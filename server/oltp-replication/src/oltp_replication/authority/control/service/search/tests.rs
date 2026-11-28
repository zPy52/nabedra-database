use super::*;
use crate::oltp_replication::authority::{
    consensus::{ConsensusFuture, ReadBarrier},
    control::tests::{Directory, Fixture, NativeModelConsensus, authorization, identities, worker},
};
use crate::oltp_replication::{
    search_rpc::{SearchControlReply, SearchMove, SearchRole},
    types::MAX_PAGE_ITEMS,
};
use std::sync::atomic::AtomicU64;

struct Leader(NativeModelConsensus);

impl Consensus<ControlCommand, ControlReply> for Leader {
    fn propose(
        &self,
        command: OwnedCommand<ControlCommand>,
    ) -> ConsensusFuture<'_, Applied<ControlReply>> {
        self.0.propose(command)
    }

    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        self.0.ensure_linearizable()
    }

    fn current_leader(&self) -> Option<u64> {
        Some(1)
    }

    fn local_node(&self) -> Option<u64> {
        Some(1)
    }
}

fn handle(fixture: &Fixture) -> Arc<ControlHandle> {
    ControlHandle::new(
        fixture.machine.clone(),
        Arc::new(Leader(NativeModelConsensus {
            machine: fixture.machine.clone(),
            next: AtomicU64::new(fixture.next),
            reads: AtomicU64::new(0),
        })),
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: Default::default(),
        },
    )
}

async fn apply(fixture: &mut Fixture, command: SearchCommand, epoch: u64) {
    assert_eq!(
        fixture
            .apply(ControlCommand::Search {
                operation_id: nabedra_rt::ids::uuid_v4(),
                unix_ms: 1_000,
                command: Box::new(command),
            })
            .await,
        ControlReply::Search(SearchControlReply::Done { epoch }),
    );
}

#[tokio::test]
async fn vs12_model_registry_reads_past_the_store_page_limit() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    let mut expected = Vec::new();
    for n in 0..=MAX_PAGE_ITEMS {
        let name = format!("m{n:03}");
        let model = EmbedModel {
            name: Text::try_from(name.as_str()).unwrap(),
            endpoint: Text::try_from("https://embed.example/").unwrap(),
            model: Text::try_from("small").unwrap(),
            dim: 8,
            credential_ref: Text::try_from("env:EMBED_KEY").unwrap(),
            max_batch: 64,
            max_rps: 10,
            allow_hosts: vec![Text::try_from("embed.example").unwrap()],
        };
        apply(&mut fixture, SearchCommand::SetModel(model.clone()), 0).await;
        expected.push(model);
    }

    let bytes = handle(&fixture).search_read(SearchRead::Models).await.unwrap();
    let mut actual: Vec<EmbedModel> = machine::decode(bytes.as_ref()).unwrap();
    actual.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn vs06_reconcile_reads_past_the_assignment_and_member_page_limits() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let database = scope.database.clone();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    let end = MAX_PAGE_ITEMS as u128;
    for id in 1..=end + 2 {
        let role = if id <= end { SearchRole::Embedder } else { SearchRole::Worker };
        let zone = if id == end + 2 { "az-2" } else { "az-1" };
        let member = SearchMember {
            database: database.clone(),
            role,
            enrollment: worker(id),
            zone: Text::try_from(zone).unwrap(),
            budget_bytes: 1 << 30,
        };
        apply(&mut fixture, SearchCommand::Enroll(member), 0).await;
    }
    let existing = Uuid::from_u128(end + 1);
    for id in 1..=end + 1 {
        let table_id = Uuid::from_u128(id);
        apply(
            &mut fixture,
            SearchCommand::BeginMove {
                table_id,
                expected_epoch: 0,
                mv: SearchMove { shard: 0, add: existing, remove: None, epoch: 1 },
            },
            0,
        )
        .await;
        apply(&mut fixture, SearchCommand::Publish { table_id, expected_epoch: 0 }, 1).await;
    }

    let handle = handle(&fixture);
    assert_eq!(handle.search_reconcile_tick().await.unwrap(), MAX_PAGE_ITEMS + 1);
    let table_id = Uuid::from_u128(end + 1);
    let bytes = handle.search_read(SearchRead::Assignment { table_id }).await.unwrap();
    let assignment: Option<SearchAssignment> = machine::decode(bytes.as_ref()).unwrap();
    let next = assignment.unwrap().next.unwrap();
    assert_eq!(next.add, Uuid::from_u128(end + 2));
    assert_eq!(next.remove, None);
    assert_eq!(handle.search_reconcile_tick().await.unwrap(), 0);
}
