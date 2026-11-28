//! `ControlCommand::Search` on real Control storage (contract-w2 2.6, spec
//! SearchAssign): members, assignments and models are durable records under
//! their own prefixes, refusals write nothing, and a repeated operation
//! replays its first reply.
use super::*;
use crate::oltp_replication::{
    authority::control::search::{assignment_key, binding_key, member_key, model_key},
    search_rpc::{
        EmbedModel, SearchAssignment, SearchCommand, SearchControlReply, SearchMember, SearchMove,
        SearchRole,
    },
};
use serde::de::DeserializeOwned;

fn search(command: SearchCommand) -> ControlCommand {
    ControlCommand::Search {
        operation_id: nabedra_rt::ids::uuid_v4(),
        unix_ms: 1_000,
        command: Box::new(command),
    }
}
fn refused(reply: &ControlReply) -> bool {
    matches!(reply, ControlReply::Search(SearchControlReply::Refused(_)))
}
const DONE: ControlReply = ControlReply::Search(SearchControlReply::Done { epoch: 0 });
/// The Control records without operation receipts: a refusal records only
/// its receipt, so a repeat replays it.
async fn records(machine: &ControlMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut all = control_dump(machine).await;
    all.retain(|(key, _)| !key.starts_with(b"ct/op/"));
    all
}

impl Fixture {
    fn search_member(&self, number: u128, role: SearchRole) -> SearchMember {
        SearchMember {
            database: self.machine.store().identity().database.clone(),
            role,
            enrollment: worker(self.base + number),
            zone: Text::try_from("az-1").unwrap(),
            budget_bytes: 1 << 30,
        }
    }
    async fn record<T: DeserializeOwned>(&self, key: Vec<u8>) -> Option<T> {
        let bytes = self
            .machine
            .store()
            .read(BUDGET, move |view| {
                let value = view.get(&key)?.map(|value| value.as_ref().to_vec());
                view.encode(&value)
            })
            .await
            .unwrap();
        let raw: Option<Vec<u8>> = decode(bytes.as_ref()).unwrap();
        raw.map(|raw| decode(&raw).unwrap())
    }
    /// Applies `command`, then the same entry again: the repeat answers the
    /// same and changes nothing.
    async fn apply_twice(&mut self, command: ControlCommand) -> ControlReply {
        let reply = self.apply(command.clone()).await;
        let before = control_dump(&self.machine).await;
        assert_eq!(self.apply(command).await, reply, "a repeated operation replays its reply");
        assert!(control_dump(&self.machine).await == before, "a repeat writes nothing");
        reply
    }
}
fn model(name: &str, dim: u32) -> EmbedModel {
    EmbedModel {
        name: Text::try_from(name).unwrap(),
        endpoint: Text::try_from("https://embed.example/v1/embeddings").unwrap(),
        model: Text::try_from("small").unwrap(),
        dim,
        credential_ref: Text::try_from("env:EMBED_KEY").unwrap(),
        max_batch: 64,
        max_rps: 10,
        allow_hosts: vec![Text::try_from("embed.example").unwrap()],
    }
}

/// Enrollment of search processes: under `ct/search-member/`, of this
/// database only, never an id of a data worker, a change of the same
/// revision or of kind refused, a newer revision accepted; each change
/// raises the enrollment revision so peer directories refresh. Retire
/// removes the member.
#[tokio::test]
async fn vs11_control_enrolls_search_members_under_their_own_prefix() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    let data = fixture.enroll(1).await;
    let member = fixture.search_member(50, SearchRole::Worker);
    let revision = fixture.state().await.enrollment_revision;
    assert_eq!(fixture.apply_twice(search(SearchCommand::Enroll(member.clone()))).await, DONE);
    assert_eq!(fixture.state().await.enrollment_revision, revision + 1);
    let id = member.enrollment.worker_id;
    assert_eq!(fixture.record::<SearchMember>(member_key(id)).await, Some(member.clone()));
    // The same enrollment again is a no-op success.
    assert_eq!(fixture.apply(search(SearchCommand::Enroll(member.clone()))).await, DONE);
    assert_eq!(fixture.state().await.enrollment_revision, revision + 1);
    let mut moved = member.clone();
    moved.zone = Text::try_from("az-2").unwrap();
    let mut embedder = member.clone();
    embedder.role = SearchRole::Embedder;
    embedder.enrollment.enrollment_revision = 2;
    let mut other_db = fixture.search_member(51, SearchRole::Worker);
    other_db.database = Text::try_from("other").unwrap();
    let mut as_data = fixture.search_member(52, SearchRole::Worker);
    as_data.enrollment.worker_id = data.worker_id;
    let mut no_budget = fixture.search_member(53, SearchRole::Worker);
    no_budget.budget_bytes = 0;
    for (name, bad) in [
        ("a change without a newer revision", moved.clone()),
        ("another kind", embedder),
        ("another database", other_db),
        ("a data worker's id", as_data),
        ("no budget", no_budget),
    ] {
        let before = records(&fixture.machine).await;
        assert!(refused(&fixture.apply(search(SearchCommand::Enroll(bad))).await), "{name}");
        assert!(records(&fixture.machine).await == before, "{name} writes nothing");
    }
    moved.enrollment.enrollment_revision = 2;
    assert_eq!(fixture.apply(search(SearchCommand::Enroll(moved.clone()))).await, DONE);
    assert_eq!(fixture.record::<SearchMember>(member_key(id)).await, Some(moved));
    assert_eq!(fixture.apply(search(SearchCommand::Retire { worker_id: id })).await, DONE);
    assert_eq!(fixture.record::<SearchMember>(member_key(id)).await, None);
    assert!(refused(&fixture.apply(search(SearchCommand::Retire { worker_id: id })).await));
}

/// SearchAssign `StartMove` and `Publish` through the log: only an enrolled
/// search worker is added; the record advances one epoch per publish; stale
/// expectations conflict with the current epoch and write nothing.
#[tokio::test]
async fn vs06_control_moves_and_publishes_assignments_durably() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    let table = nabedra_rt::ids::uuid_v4();
    let worker = fixture.search_member(60, SearchRole::Worker);
    let embedder = fixture.search_member(61, SearchRole::Embedder);
    for member in [&worker, &embedder] {
        assert_eq!(fixture.apply(search(SearchCommand::Enroll(member.clone()))).await, DONE);
    }
    let begin = |add: Uuid, expected_epoch| SearchCommand::BeginMove {
        table_id: table,
        expected_epoch,
        mv: SearchMove { shard: 0, add, remove: None, epoch: expected_epoch + 1 },
    };
    for (name, add) in [("an embedder", embedder.enrollment.worker_id), ("not enrolled", Uuid::from_u128(9))] {
        assert!(refused(&fixture.apply(search(begin(add, 0))).await), "{name}");
    }
    assert_eq!(fixture.record::<SearchAssignment>(assignment_key(table)).await, None);
    let id = worker.enrollment.worker_id;
    assert_eq!(fixture.apply_twice(search(begin(id, 0))).await, DONE);
    let pending = fixture.record::<SearchAssignment>(assignment_key(table)).await.unwrap();
    assert_eq!((pending.epoch, pending.next.map(|mv| mv.add)), (0, Some(id)));
    let publish = |expected_epoch| SearchCommand::Publish { table_id: table, expected_epoch };
    let before = records(&fixture.machine).await;
    assert_eq!(
        fixture.apply(search(publish(1))).await,
        ControlReply::Search(SearchControlReply::Conflict { epoch: 0 })
    );
    assert!(records(&fixture.machine).await == before);
    assert_eq!(
        fixture.apply_twice(search(publish(0))).await,
        ControlReply::Search(SearchControlReply::Done { epoch: 1 })
    );
    let published = fixture.record::<SearchAssignment>(assignment_key(table)).await.unwrap();
    assert_eq!((published.epoch, published.next), (1, None));
    assert_eq!(published.shards[0].replicas[0].since_epoch, 1);
    assert_eq!(
        fixture.apply(search(SearchCommand::CancelMove { table_id: table, expected_epoch: 1 })).await,
        ControlReply::Search(SearchControlReply::Conflict { epoch: 1 })
    );
}

/// Q18: a model name is bound for life to its endpoint, model and
/// dimensions. Limits, hosts and the credential reference may change; a
/// dropped model keeps its binding, so the name never comes back as another
/// model. Malformed models are refused.
#[tokio::test]
async fn vs12_a_model_name_is_bound_for_life() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    let first = model("m1", 8);
    assert_eq!(fixture.apply_twice(search(SearchCommand::SetModel(first.clone()))).await, DONE);
    let mut faster = first.clone();
    faster.max_rps = 100;
    faster.credential_ref = Text::try_from("file:/run/key").unwrap();
    assert_eq!(fixture.apply(search(SearchCommand::SetModel(faster.clone()))).await, DONE);
    assert_eq!(fixture.record::<EmbedModel>(model_key("m1")).await, Some(faster));
    let mut endpoint = first.clone();
    endpoint.endpoint = Text::try_from("https://other.example/v1/embeddings").unwrap();
    let mut plain = first.clone();
    plain.endpoint = Text::try_from("ftp://embed.example").unwrap();
    for (name, bad) in [
        ("other dimensions", model("m1", 16)),
        ("another endpoint", endpoint.clone()),
        ("a scheme other than http(s)", plain),
        ("no dimensions", model("m2", 0)),
        ("a bad name", model("-m", 8)),
    ] {
        assert!(refused(&fixture.apply(search(SearchCommand::SetModel(bad))).await), "{name}");
    }
    let drop = SearchCommand::DropModel { name: Text::try_from("m1").unwrap() };
    assert_eq!(fixture.apply(search(drop.clone())).await, DONE);
    assert_eq!(fixture.record::<EmbedModel>(model_key("m1")).await, None);
    assert!(fixture.record::<Digest>(binding_key("m1")).await.is_some(), "the binding stays");
    assert!(refused(&fixture.apply(search(drop)).await));
    assert!(refused(&fixture.apply(search(SearchCommand::SetModel(endpoint))).await));
    assert_eq!(fixture.apply(search(SearchCommand::SetModel(first))).await, DONE);
}

/// rv-j proof: `SearchRead::Models` (an embedder's read of the registry)
/// through the Control handle. `service/search.rs:198` scans 256 items per
/// page, above the store's page bound of 128 (`pool.rs:1862`), so the read
/// fails; the reconciler tick scans assignments and members the same way
/// (`service/search.rs:230,235`).
#[tokio::test]
async fn rvj_the_model_registry_read_answers() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    assert_eq!(fixture.apply(search(SearchCommand::SetModel(model("m1", 8)))).await, DONE);
    let consensus = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(fixture.next),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        consensus,
        OperatorPolicy { policy: authorization().policy, certificate_pins: Default::default() },
    );
    let read = handle.search_read(crate::oltp_replication::search_rpc::SearchRead::Models).await;
    let bytes = read.expect("the model registry read answers");
    let models: Vec<EmbedModel> = decode(bytes.as_ref()).unwrap();
    assert_eq!(models.len(), 1);
}
