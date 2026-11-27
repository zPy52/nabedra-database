//! Direct real-storage state assertions. These do not establish quorum reads;
//! the hosted multi-group integration exercises the actual Consensus adapter.
use super::{
    machine::*,
    model::*,
    scoped_queue::{
        ScopedQueueInventory, ScopedQueueOperation, ScopedQueueTransferReport,
        ScopedQueueTransition,
    },
    service::{ControlHandle, InventoryPage, OperatorPolicy},
};
use crate::{
    oltp::pool::{
        ApplyMeta, ByteClass, OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources,
    },
    oltp_replication::{
        authority::{
            consensus::{Applied, Consensus, ConsensusFuture, ReadBarrier},
            timestamp::ReadyFenceReceipt,
        },
        owned::{OwnedCommand, serialized_digest},
        types::*,
    },
};
use nabedra_oltp_types::queue::QueueScope;
use nabedra_resources::config::WorkerResources;
use crate::oltp_replication::queue_access::AuthorityReader;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use uuid::Uuid;
#[cfg(test)]
mod direct;
#[cfg(test)]
mod group_damage;
#[cfg(test)]
mod growth;
#[cfg(test)]
mod pool_drain;
#[cfg(test)]
mod restart;
#[cfg(test)]
mod search;
#[cfg(test)]
mod system_recovery;

async fn apply_to(
    machine: &ControlMachine,
    position: AppliedPosition,
    command: ControlCommand,
) -> ControlReply {
    let digest = serialized_digest(&command).unwrap();
    let command =
        OwnedCommand::new_owned(command, machine.store().resources(), ByteClass::Progress).unwrap();
    let bytes = machine
        .apply(
            ApplyMeta {
                position,
                command_digest: digest,
                membership: None,
            },
            command,
        )
        .await
        .unwrap();
    decode(bytes.as_ref()).unwrap()
}

/// Every key and value of a Control replica's applied state, in key order.
async fn control_dump(machine: &ControlMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    loop {
        let mut cursor = out.last().map(|(key, _)| key.clone());
        if let Some(cursor) = cursor.as_mut() {
            cursor.push(0);
        }
        let raw = machine
            .store()
            .read(BUDGET, move |view| {
                let mut page = Vec::new();
                let mut cursor = cursor.unwrap_or_default();
                while page.len() < 32
                    && let Some(record) = view.first_from(&[], &cursor)?
                {
                    page.push((record.key().to_vec(), record.value().to_vec()));
                    cursor = record.key().to_vec();
                    cursor.push(0);
                }
                view.encode(&page)
            })
            .await
            .unwrap();
        let page: Vec<(Vec<u8>, Vec<u8>)> = serde_json::from_slice(raw.as_ref()).unwrap();
        if page.is_empty() {
            return out;
        }
        out.extend(page);
    }
}

#[doc(hidden)]
pub struct Directory(PathBuf);
impl Directory {
    #[doc(hidden)]
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!("nabedra-control-{}", nabedra_rt::ids::uuid_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
#[doc(hidden)]
pub struct Fixture {
    #[doc(hidden)]
    pub machine: ControlMachine,
    #[doc(hidden)]
    pub next: u64,
    base: u128,
    /// GC2 mixed-version tests: a pre-feature Control replica that applies
    /// every entry too. Each apply asserts the same reply and state.
    mirror: Option<ControlMachine>,
}
impl Fixture {
    #[doc(hidden)]
    pub async fn open(directory: &Directory, scope: GroupIdentity, pool: PoolIdentity) -> Self {
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let pool = OltpPool::open_development(&directory.0, pool, shared).unwrap();
        let store = pool.group(scope).await.unwrap();
        let next = store
            .applied_state(BUDGET)
            .await
            .unwrap()
            .map_or(0, |p| p.position.index + 1);
        Self {
            machine: ControlMachine::new(store).unwrap(),
            next,
            base: (nabedra_rt::ids::uuid_v4().as_u128() >> 64) << 64,
            mirror: None,
        }
    }
    /// GC2: from now on every entry is also applied by a fresh Control
    /// replica of the pre-feature build in `directory`. It must be opened
    /// before the first entry.
    #[cfg(test)]
    async fn mirror_legacy(
        &mut self,
        directory: &Directory,
        scope: GroupIdentity,
        pool: PoolIdentity,
    ) {
        assert_eq!(self.next, 0, "the mirror replays the whole log");
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        // Another worker's replica: its own worker and pool identity.
        let mut pool = pool;
        pool.worker_id = nabedra_rt::ids::uuid_v4();
        pool.pool_id = nabedra_rt::ids::uuid_v4();
        let pool = OltpPool::open_development(&directory.0, pool, shared).unwrap();
        let store = pool.group(scope).await.unwrap();
        self.mirror = Some(ControlMachine::new(store).unwrap().legacy_v1_for_test());
    }
    #[doc(hidden)]
    pub async fn apply(&mut self, command: ControlCommand) -> ControlReply {
        let position = AppliedPosition {
            term: 1,
            node_id: 1,
            index: self.next,
        };
        self.next += 1;
        let reply = apply_to(&self.machine, position, command.clone()).await;
        if let Some(mirror) = &self.mirror {
            assert_eq!(
                apply_to(mirror, position, command).await,
                reply,
                "a pre-feature Control replica answered differently"
            );
            assert!(
                control_dump(mirror).await == control_dump(&self.machine).await,
                "a pre-feature Control replica diverged at index {}",
                position.index
            );
        }
        reply
    }
    async fn state(&self) -> State {
        let bytes = self
            .machine
            .store()
            .read(BUDGET, |view| {
                let state: State = decode(view.get(STATE)?.unwrap().as_ref())?;
                view.encode(&state)
            })
            .await
            .unwrap();
        decode(bytes.as_ref()).unwrap()
    }
    #[doc(hidden)]
    pub async fn initialize(&mut self) -> ReadyMembership {
        let result = self
            .apply(ControlCommand::Initialize {
                operation_id: nabedra_rt::ids::uuid_v4(),
                run_id: nabedra_rt::ids::uuid_v4(),
                authorization: authorization(),
            })
            .await;
        let ControlReply::Initialized { ready } = result else {
            panic!("unexpected initialization reply: {result:?}")
        };
        ready
    }
    #[doc(hidden)]
    pub async fn enroll(&mut self, number: u128) -> WorkerIdentity {
        let worker = worker(self.base + number);
        let result = self
            .apply(ControlCommand::Enroll {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: None,
                next: worker.clone(),
                authorization: authorization(),
                previous_process: None,
            })
            .await;
        assert_eq!(
            result,
            ControlReply::Enrolled(WorkerIdentity::from(&worker))
        );
        WorkerIdentity::from(&worker)
    }
    #[doc(hidden)]
    pub async fn register_group(
        &mut self,
        kind: GroupKind,
        members: Vec<ReplicaMember>,
    ) -> GroupRecord {
        let generation = nabedra_rt::ids::uuid_v4();
        let group = GroupIdentity {
            group_id: if kind == GroupKind::Control {
                self.machine.store().identity().group_id
            } else {
                nabedra_rt::ids::uuid_v4()
            },
            kind,
            ..self.machine.store().identity().clone()
        };
        assert!(matches!(
            self.apply(ControlCommand::BeginGroup {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                identity: group.clone(),
                authorization: authorization()
            })
            .await,
            ControlReply::GroupBuilding { .. }
        ));
        let mut chain = group_seed();
        for page in members.chunks(128) {
            assert!(matches!(
                self.apply(ControlCommand::GroupPage {
                    operation_id: nabedra_rt::ids::uuid_v4(),
                    generation,
                    replicas: Page::new(page.to_vec()).unwrap()
                })
                .await,
                ControlReply::GroupBuilding { .. }
            ));
            for member in page {
                chain = group_step(chain, member);
            }
        }
        let applied_at = AppliedPosition {
            term: 1,
            node_id: 3,
            index: 10,
        };
        let root = group_root(members.len() as u64, chain);
        let authority = self.state().await.ready.view.authority();
        assert!(matches!(
            self.apply(ControlCommand::SealGroup {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                proof: GroupMembershipStamp {
                    group: group.clone(),
                    generation,
                    replica_count: members.len() as u64,
                    replica_root: root,
                    applied_at,
                    authority
                }
            })
            .await,
            ControlReply::GroupRegistered { .. }
        ));
        GroupRecord {
            identity: group,
            generation,
            replica_count: members.len() as u64,
            replica_root: root,
            membership_position: applied_at,
        }
    }
    #[doc(hidden)]
    pub async fn add_ready(
        &mut self,
        target: WorkerIdentity,
        current_members: &mut Vec<WorkerIdentity>,
    ) -> ReadyMembership {
        let state = self.state().await;
        let id = nabedra_rt::ids::uuid_v4();
        let result = self
            .apply(ControlCommand::BeginJoin {
                operation_id: id,
                expected: state.ready.view,
                target,
            })
            .await;
        let ControlReply::JoinBegun { ticket, .. } = result else {
            panic!("unexpected join reply: {result:?}")
        };
        current_members.push(target);
        current_members.sort_by_key(|w| w.worker_id);
        self.publish_ready_members(ticket, current_members, state.owner.unwrap_or(target))
            .await
    }
    async fn publish_ready_members(
        &mut self,
        ticket: JoinTicket,
        members: &[WorkerIdentity],
        owner: WorkerIdentity,
    ) -> ReadyMembership {
        let id = ticket.ticket_id;
        for page in members.chunks(128) {
            assert!(matches!(
                self.apply(ControlCommand::ReadyPage {
                    operation_id: nabedra_rt::ids::uuid_v4(),
                    transition_id: id,
                    members: Page::new(page.to_vec()).unwrap()
                })
                .await,
                ControlReply::ReadyBuilding { .. }
            ));
        }
        let result = self
            .apply(ControlCommand::SealReady {
                operation_id: nabedra_rt::ids::uuid_v4(),
                transition_id: id,
            })
            .await;
        let ControlReply::ReadySealed { expected, next, .. } = result else {
            panic!("unexpected seal: {result:?}")
        };
        let receipt = ReadyFenceReceipt {
            transition_id: id,
            expected,
            next,
            applied_at: AppliedPosition {
                term: 1,
                node_id: 2,
                index: 100,
            },
        };
        self.apply(ControlCommand::TimestampFenced {
            operation_id: nabedra_rt::ids::uuid_v4(),
            transition_id: id,
            receipt,
        })
        .await;
        let proof = QueueCopyStatement {
            ticket,
            source: owner,
            snapshot_id: nabedra_rt::ids::uuid_v4(),
            snapshot_cut: 0,
            final_cut: 0,
            record_version_hash: hash(b"nabedra/ram-queue-cut/v1\0"),
        };
        assert_eq!(
            self.apply(ControlCommand::PublishReady {
                operation_id: nabedra_rt::ids::uuid_v4(),
                transition_id: id,
                proof
            })
            .await,
            ControlReply::Published {
                ready: next,
                owner: Some(owner)
            }
        );
        next
    }

    async fn scoped(
        &mut self,
        actor: WorkerIdentity,
        action: ScopedQueueOperation,
    ) -> ControlReply {
        let operation_id = nabedra_rt::ids::uuid_v4();
        let authorization = action.requires_operator().then(|| AuthorizationStamp {
            authorization_id: operation_id,
            subject: actor.certificate_sha256,
            evidence: serialized_digest(&(self.machine.store().identity(), actor, &action))
                .unwrap(),
            ..authorization()
        });
        self.apply(ControlCommand::ScopedQueue {
            operation_id,
            actor,
            action: Box::new(action),
            authorization,
        })
        .await
    }

    async fn install_scoped_transfer(
        &mut self,
        transfer: &ScopedQueueTransition,
        holders: [WorkerIdentity; 2],
    ) {
        let group_id = transfer.next.group.group_id;
        let transfer_id = transfer.id;
        let inventory = ScopedQueueInventory {
            count: 1,
            root: hash(b"scoped survivor cut"),
        };
        let mut frozen = transfer.clone();
        for worker in holders {
            assert_eq!(
                self.scoped(
                    worker,
                    ScopedQueueOperation::ReportTransfer {
                        group_id,
                        transfer_id,
                        report: ScopedQueueTransferReport::Frozen(inventory),
                    }
                )
                .await,
                ControlReply::ScopedQueueUpdated
            );
            let slot = frozen
                .previous
                .members
                .iter()
                .position(|member| *member == worker)
                .unwrap();
            frozen.frozen[slot] = Some(inventory);
        }
        let basis = frozen.basis().unwrap().unwrap();
        for worker in holders {
            assert_eq!(
                self.scoped(
                    worker,
                    ScopedQueueOperation::ReportTransfer {
                        group_id,
                        transfer_id,
                        report: ScopedQueueTransferReport::Installed { basis, inventory },
                    }
                )
                .await,
                ControlReply::ScopedQueueUpdated
            );
        }
    }
}
#[doc(hidden)]
pub fn identities() -> (GroupIdentity, PoolIdentity) {
    let cluster = nabedra_rt::ids::uuid_v4();
    (
        GroupIdentity {
            cluster_id: cluster,
            database: Text::try_from("default").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Control,
            group_format_version: GROUP_FORMAT_VERSION,
        },
        PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: cluster,
            worker_id: nabedra_rt::ids::uuid_v4(),
            pool_id: nabedra_rt::ids::uuid_v4(),
        },
    )
}
#[doc(hidden)]
pub fn authorization() -> AuthorizationStamp {
    AuthorizationStamp {
        authorization_id: nabedra_rt::ids::uuid_v4(),
        policy: Digest::from_bytes([1; 32]),
        subject: Digest::from_bytes([2; 32]),
        evidence: Digest::from_bytes([3; 32]),
    }
}
#[doc(hidden)]
pub fn worker(number: u128) -> WorkerEnrollment {
    WorkerEnrollment {
        worker_id: Uuid::from_u128(number),
        incarnation: nabedra_rt::ids::uuid_v4(),
        certificate_sha256: hash(&number.to_be_bytes()),
        endpoint: Text::try_from("127.0.0.1:1").unwrap(),
        server_name: Text::try_from("localhost").unwrap(),
        enrollment_revision: 1,
    }
}

#[tokio::test]
async fn o31_r01_r03_installed_scoped_transfer_rebases_after_ready_removal() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    let initial = fixture.initialize().await;
    let owner = fixture.enroll(1).await;
    let removed = fixture.enroll(2).await;
    let peer = fixture.enroll(3).await;
    let mut members = Vec::new();
    for worker in [owner, removed, peer] {
        fixture.add_ready(worker, &mut members).await;
        assert_eq!(
            fixture
                .scoped(
                    worker,
                    ScopedQueueOperation::Advertise {
                        version: super::scoped_queue::SCOPED_QUEUE_VERSION,
                    }
                )
                .await,
            ControlReply::ScopedQueueUpdated
        );
    }
    let group = fixture
        .register_group(
            GroupKind::Data {
                table_id: nabedra_rt::ids::uuid_v4(),
                shard_id: nabedra_rt::ids::uuid_v4(),
            },
            members
                .iter()
                .enumerate()
                .map(|(index, worker)| ReplicaMember {
                    node_id: index as u64 + 1,
                    worker_id: worker.worker_id,
                    pool_id: nabedra_rt::ids::uuid_v4(),
                })
                .collect(),
        )
        .await;
    let id = nabedra_rt::ids::uuid_v4();
    let run_id = initial.view.run_id;
    let ControlReply::ScopedQueue(previous) = fixture
        .apply(ControlCommand::EnsureScopedQueue {
            operation_id: id,
            expected: Box::new(group.clone()),
            run_id,
            authorization: AuthorizationStamp {
                authorization_id: id,
                evidence: super::machine::scoped_queue::creation_digest(&group, run_id).unwrap(),
                ..authorization()
            },
        })
        .await
    else {
        panic!("expected a published scoped queue");
    };
    let ControlReply::ScopedQueueTransfer(transfer) = fixture
        .scoped(
            owner,
            ScopedQueueOperation::BeginTransfer {
                expected: previous.clone(),
                next_owner: owner,
                replaces: None,
            },
        )
        .await
    else {
        panic!("expected a pending scoped transfer");
    };
    fixture
        .install_scoped_transfer(&transfer, [owner, peer])
        .await;

    let group_id = group.identity.group_id;
    let rebase = ScopedQueueOperation::RebaseTransfer {
        group_id,
        transfer_id: transfer.id,
        removed,
        next_owner: owner,
    };
    let publish = ScopedQueueOperation::PublishTransfer {
        group_id,
        transfer_id: transfer.id,
    };
    assert_eq!(
        fixture.scoped(owner, rebase.clone()).await,
        ControlReply::Rejected(Fault::NotReady)
    );
    let expected = fixture.state().await.ready.view;
    let ControlReply::JoinBegun { ticket, .. } = fixture
        .apply(ControlCommand::BeginRemoval {
            operation_id: nabedra_rt::ids::uuid_v4(),
            expected,
            target: removed,
            exclusion: ExclusionStamp {
                worker: removed,
                authorization: authorization(),
                ownership_fence: hash(b"removed worker"),
            },
        })
        .await
    else {
        panic!("expected a pending Ready removal");
    };
    assert_eq!(
        fixture.scoped(owner, rebase.clone()).await,
        ControlReply::Rejected(Fault::Stale)
    );
    fixture
        .publish_ready_members(ticket, &[owner, peer], owner)
        .await;
    assert_eq!(
        fixture.scoped(owner, publish.clone()).await,
        ControlReply::Rejected(Fault::NotReady)
    );
    assert_eq!(
        fixture.scoped(removed, rebase.clone()).await,
        ControlReply::Rejected(Fault::Unauthorized)
    );
    let wrong_removal = ScopedQueueOperation::RebaseTransfer {
        group_id,
        transfer_id: transfer.id,
        removed: peer,
        next_owner: owner,
    };
    assert_eq!(
        fixture.scoped(owner, wrong_removal).await,
        ControlReply::Rejected(Fault::NotReady)
    );

    let ControlReply::ScopedQueueTransfer(rebased) = fixture.scoped(owner, rebase.clone()).await
    else {
        panic!("expected an exact replacement of the obsolete transfer");
    };
    assert_eq!(rebased.previous, *previous);
    assert_ne!(rebased.id, transfer.id);
    assert_eq!(rebased.next.members, transfer.next.members);
    assert_eq!(rebased.next.unavailable_holder, Some(removed));
    assert_eq!(rebased.next.view.run_id, run_id);
    assert_eq!(
        rebased.next.view.owner_term,
        transfer.next.view.owner_term + 1
    );
    assert_eq!(
        rebased.next.queue_generation,
        transfer.next.queue_generation
    );
    assert_eq!(rebased.next.data_generation, transfer.next.data_generation);
    assert!(rebased.basis().unwrap().is_none() && !rebased.has_installed_quorum());
    assert_eq!(
        fixture.scoped(owner, rebase).await,
        ControlReply::Rejected(Fault::Stale)
    );
    assert_eq!(
        fixture.scoped(owner, publish).await,
        ControlReply::Rejected(Fault::Stale)
    );

    let publish_rebased = ScopedQueueOperation::PublishTransfer {
        group_id,
        transfer_id: rebased.id,
    };
    assert_eq!(
        fixture.scoped(owner, publish_rebased.clone()).await,
        ControlReply::Rejected(Fault::Incomplete)
    );
    fixture
        .install_scoped_transfer(&rebased, [owner, peer])
        .await;
    assert_eq!(
        fixture.scoped(owner, publish_rebased).await,
        ControlReply::ScopedQueue(Box::new(rebased.next))
    );
}

#[tokio::test]
async fn r03_control_pending_timestamp_fence_and_publication_survive_each_reopen() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    let initial = fixture.initialize().await;
    let target = fixture.enroll(1).await;
    let id = nabedra_rt::ids::uuid_v4();
    let ControlReply::JoinBegun { ticket, .. } = fixture
        .apply(ControlCommand::BeginJoin {
            operation_id: id,
            expected: initial.view,
            target,
        })
        .await
    else {
        panic!("ticket")
    };
    fixture
        .apply(ControlCommand::ReadyPage {
            operation_id: nabedra_rt::ids::uuid_v4(),
            transition_id: id,
            members: Page::new(vec![target]).unwrap(),
        })
        .await;
    let ControlReply::ReadySealed { expected, next, .. } = fixture
        .apply(ControlCommand::SealReady {
            operation_id: nabedra_rt::ids::uuid_v4(),
            transition_id: id,
        })
        .await
    else {
        panic!("seal")
    };
    let proof = QueueCopyStatement {
        ticket,
        source: target,
        snapshot_id: nabedra_rt::ids::uuid_v4(),
        snapshot_cut: 0,
        final_cut: 0,
        record_version_hash: hash(b"nabedra/ram-queue-cut/v1\0"),
    };
    assert_eq!(
        fixture
            .apply(ControlCommand::PublishReady {
                operation_id: nabedra_rt::ids::uuid_v4(),
                transition_id: id,
                proof: proof.clone()
            })
            .await,
        ControlReply::Rejected(Fault::Incomplete)
    );
    assert_eq!(fixture.state().await.ready, initial);
    drop(fixture);
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    let receipt = ReadyFenceReceipt {
        transition_id: id,
        expected,
        next,
        applied_at: AppliedPosition {
            term: 1,
            node_id: 7,
            index: 42,
        },
    };
    let ack = ControlCommand::TimestampFenced {
        operation_id: nabedra_rt::ids::uuid_v4(),
        transition_id: id,
        receipt: receipt.clone(),
    };
    let first = fixture.apply(ack.clone()).await;
    assert_eq!(
        first,
        ControlReply::TimestampFenced {
            transition_id: id,
            next
        }
    );
    drop(fixture);
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    assert_eq!(fixture.apply(ack).await, first);
    assert_eq!(fixture.state().await.ready, initial);
    let op = nabedra_rt::ids::uuid_v4();
    let publish = ControlCommand::PublishReady {
        operation_id: op,
        transition_id: id,
        proof,
    };
    let published = fixture.apply(publish.clone()).await;
    assert_eq!(
        published,
        ControlReply::Published {
            ready: next,
            owner: Some(target)
        }
    );
    assert_eq!(fixture.apply(publish).await, published);
    assert_eq!(fixture.state().await.ready, next);
    let mut members = Vec::new();
    fixture
        .machine
        .store()
        .read(BUDGET, move |view| {
            let state: State = decode(view.get(STATE)?.unwrap().as_ref())?;
            view.scan_page(
                &ready_prefix(state.ready_generation),
                None,
                128,
                |_, bytes| {
                    members.push(decode::<WorkerIdentity>(bytes)?);
                    Ok(())
                },
            )?;
            assert_eq!(members, vec![target]);
            view.encode(&())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn o06_control_enrollment_cas_requires_exclusion_and_preserves_permanent_group_nodes() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    let old = fixture.enroll(1).await;
    let mut members = Vec::new();
    fixture.add_ready(old, &mut members).await;
    let permanent = ReplicaMember {
        node_id: 41,
        worker_id: old.worker_id,
        pool_id: nabedra_rt::ids::uuid_v4(),
    };
    let group = fixture
        .register_group(GroupKind::Timestamps, vec![permanent.clone()])
        .await;
    let mut next = worker(old.worker_id.as_u128());
    next.enrollment_revision = 2;
    assert_eq!(
        fixture
            .apply(ControlCommand::Enroll {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: Some(old),
                next: next.clone(),
                authorization: authorization(),
                previous_process: None
            })
            .await,
        ControlReply::Rejected(Fault::Unauthorized)
    );
    let excluded = ExclusionStamp {
        worker: old,
        authorization: authorization(),
        ownership_fence: Digest::from_bytes([8; 32]),
    };
    assert_eq!(
        fixture
            .apply(ControlCommand::Enroll {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: Some(old),
                next: next.clone(),
                authorization: authorization(),
                previous_process: Some(excluded)
            })
            .await,
        ControlReply::Enrolled(WorkerIdentity::from(&next))
    );
    let state = fixture.state().await;
    assert_eq!(state.invalid_ready, 1);
    assert_eq!(state.ready.ready_count, 1);
    let bytes = fixture
        .machine
        .store()
        .read(BUDGET, move |view| {
            let member = view
                .get(&ready_key(state.ready_generation, old.worker_id))?
                .unwrap();
            view.copy_output(member.as_ref())
        })
        .await
        .unwrap();
    assert_eq!(decode::<WorkerIdentity>(bytes.as_ref()).unwrap(), old);
    let bytes = fixture
        .machine
        .store()
        .read(BUDGET, move |view| {
            let member = view
                .get(&member_key(group.generation, permanent.node_id))?
                .unwrap();
            assert_eq!(decode::<ReplicaMember>(member.as_ref())?, permanent);
            view.encode(&())
        })
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), b"null");
}

#[tokio::test]
async fn o14_o16_owner_service_checks_exact_receiver_in_published_membership() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool).await;
    fixture.initialize().await;
    let owner = fixture.enroll(1).await;
    let ready = fixture.add_ready(owner, &mut Vec::new()).await;
    let receiver = fixture.enroll(2).await;
    let member = ReplicaMember {
        node_id: 41,
        worker_id: receiver.worker_id,
        pool_id: nabedra_rt::ids::uuid_v4(),
    };
    let original = fixture
        .register_group(GroupKind::Timestamps, vec![member.clone()])
        .await;
    let generation = nabedra_rt::ids::uuid_v4();
    let successor = ReplicaMember {
        node_id: 42,
        pool_id: nabedra_rt::ids::uuid_v4(),
        ..member.clone()
    };
    assert!(matches!(
        fixture
            .apply(ControlCommand::BeginGroupReplacement {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                expected: original.clone(),
                authorization: authorization(),
            })
            .await,
        ControlReply::GroupBuilding { .. }
    ));
    assert!(matches!(
        fixture
            .apply(ControlCommand::GroupPage {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                replicas: Page::new(vec![successor.clone()]).unwrap(),
            })
            .await,
        ControlReply::GroupBuilding { .. }
    ));

    let model = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(fixture.next),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        model.clone(),
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: Default::default(),
        },
    );
    let queue_scope = QueueScope {
        cluster_id: scope.cluster_id,
        database: scope.database,
    };
    let check = |receiver, node, generation| {
        handle.service_role_at_replica(
            &queue_scope,
            owner,
            original.identity.clone(),
            receiver,
            node,
            generation,
        )
    };
    assert_eq!(
        check(receiver, member.node_id, original.generation)
            .await
            .unwrap()
            .owner,
        Some(owner)
    );
    assert_eq!(model.reads.swap(0, Ordering::Relaxed), 1);
    assert_eq!(
        check(receiver, successor.node_id, generation)
            .await
            .unwrap_err(),
        stale_service_route()
    );

    assert!(matches!(
        fixture
            .apply(ControlCommand::SealGroup {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                proof: GroupMembershipStamp {
                    group: original.identity.clone(),
                    generation,
                    replica_count: 1,
                    replica_root: group_root(1, group_step(group_seed(), &successor)),
                    applied_at: AppliedPosition {
                        index: original.membership_position.index + 1,
                        ..original.membership_position
                    },
                    authority: ready.view.authority(),
                },
            })
            .await,
        ControlReply::GroupRegistered { .. }
    ));

    assert_eq!(
        check(receiver, member.node_id, original.generation)
            .await
            .unwrap_err(),
        stale_service_route()
    );
    assert_eq!(
        check(receiver, member.node_id, generation)
            .await
            .unwrap_err(),
        stale_service_route()
    );
    assert_eq!(
        check(owner, successor.node_id, generation)
            .await
            .unwrap_err(),
        stale_service_route()
    );
    let wrong_incarnation = WorkerIdentity {
        incarnation: nabedra_rt::ids::uuid_v4(),
        ..receiver
    };
    assert_eq!(
        check(wrong_incarnation, successor.node_id, generation)
            .await
            .unwrap_err(),
        stale_service_route()
    );
    assert_eq!(
        check(receiver, successor.node_id, generation)
            .await
            .unwrap()
            .owner,
        Some(owner)
    );
    assert_eq!(model.reads.load(Ordering::Relaxed), 6);
}

#[tokio::test]
async fn o01_o14_initial_data_publication_resumes_exact_stage_and_preserves_sealed_placement() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool).await;
    fixture.initialize().await;
    let owner = fixture.enroll(1).await;
    let ready = fixture.add_ready(owner, &mut Vec::new()).await;
    let mut members = Vec::new();
    for index in 1..=3 {
        let worker = if index == 1 {
            owner
        } else {
            fixture.enroll(index as u128).await
        };
        members.push(ReplicaMember {
            node_id: index,
            worker_id: worker.worker_id,
            pool_id: nabedra_rt::ids::uuid_v4(),
        });
    }
    let group = GroupIdentity {
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Data {
            table_id: nabedra_rt::ids::uuid_v4(),
            shard_id: nabedra_rt::ids::uuid_v4(),
        },
        ..scope
    };
    let generation = nabedra_rt::ids::uuid_v4();
    let initial = |replicas: Vec<ReplicaMember>, generation| {
        let proof = GroupMembershipStamp {
            group: group.clone(),
            generation,
            replica_count: replicas.len() as u64,
            replica_root: group_root(
                replicas.len() as u64,
                replicas.iter().fold(group_seed(), group_step),
            ),
            applied_at: AppliedPosition {
                term: 1,
                node_id: 1,
                index: 4,
            },
            authority: ready.view.authority(),
        };
        let mut authorization = authorization();
        authorization.evidence = serialized_digest(&proof).unwrap();
        authorization.subject = owner.certificate_sha256;
        ControlCommand::RegisterInitialData {
            operation_id: nabedra_rt::ids::uuid_v4(),
            proof,
            replicas: Page::new(replicas).unwrap(),
            authorization,
        }
    };
    assert!(matches!(
        fixture
            .apply(ControlCommand::BeginGroup {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                identity: group.clone(),
                authorization: authorization(),
            })
            .await,
        ControlReply::GroupBuilding { count: 0, .. }
    ));
    assert!(matches!(
        fixture
            .apply(ControlCommand::GroupPage {
                operation_id: nabedra_rt::ids::uuid_v4(),
                generation,
                replicas: Page::new(vec![members[0].clone()]).unwrap(),
            })
            .await,
        ControlReply::GroupBuilding { count: 1, .. }
    ));
    let mut substituted = members.clone();
    substituted[0].pool_id = nabedra_rt::ids::uuid_v4();
    assert_eq!(
        fixture.apply(initial(substituted, generation)).await,
        ControlReply::Rejected(Fault::Conflict)
    );
    let reply = fixture.apply(initial(members.clone(), generation)).await;
    assert_eq!(
        reply,
        ControlReply::GroupRegistered {
            group_id: group.group_id,
            count: 3,
            root: group_root(3, members.iter().fold(group_seed(), group_step)),
        }
    );
    assert_eq!(
        fixture.apply(initial(members.clone(), generation)).await,
        reply
    );
    assert_eq!(
        fixture
            .apply(initial(members.clone(), nabedra_rt::ids::uuid_v4()))
            .await,
        ControlReply::Rejected(Fault::Conflict)
    );
    let state = fixture.state().await;
    assert_eq!(state.group_inventory_revision, 2);
    assert_eq!(state.group_count, 2);
    fixture
        .machine
        .store()
        .read(BUDGET, move |view| {
            let record: GroupRecord =
                decode(view.get(&key(GROUPS, group.group_id))?.unwrap().as_ref())?;
            assert_eq!(record.generation, generation);
            assert_eq!(record.replica_count, 3);
            for member in members {
                let node = view.get(&member_key(generation, member.node_id))?.unwrap();
                let worker = view
                    .get(&member_worker_key(generation, member.worker_id))?
                    .unwrap();
                assert_eq!(decode::<ReplicaMember>(node.as_ref())?, member);
                assert_eq!(decode::<ReplicaMember>(worker.as_ref())?, member);
            }
            view.encode(&())
        })
        .await
        .unwrap();
}

/// Model only: forwards to real native storage, not a quorum/leader shortcut
/// used in production. The hosted integration supplies actual OpenRaft.
#[doc(hidden)]
pub struct NativeModelConsensus {
    #[doc(hidden)]
    pub machine: ControlMachine,
    #[doc(hidden)]
    pub next: AtomicU64,
    #[doc(hidden)]
    pub reads: AtomicU64,
}
impl Consensus<ControlCommand, ControlReply> for NativeModelConsensus {
    fn propose(
        &self,
        command: OwnedCommand<ControlCommand>,
    ) -> ConsensusFuture<'_, Applied<ControlReply>> {
        Box::pin(async move {
            let position = AppliedPosition {
                term: 1,
                node_id: 1,
                index: self.next.fetch_add(1, Ordering::AcqRel),
            };
            let digest = serialized_digest(&command)?;
            let result = self
                .machine
                .apply(
                    ApplyMeta {
                        position,
                        command_digest: digest,
                        membership: None,
                    },
                    command,
                )
                .await?;
            let owner = Arc::new(
                self.machine
                    .store()
                    .resources()
                    .reserve_scratch(ByteClass::Progress, 4096)?,
            );
            Ok(crate::oltp_replication::authority::consensus::owned_applied(position, decode(result.as_ref())?, owner))
        })
    }
    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        Box::pin(async {
            self.reads.fetch_add(1, Ordering::AcqRel);
            let applied = self
                .machine
                .store()
                .applied_state(BUDGET)
                .await?
                .unwrap()
                .position;
            Ok(ReadBarrier { applied })
        })
    }
}

#[tokio::test]
async fn u02_control_model_inventory_pages_empty_intermediate_final_and_exhausted_without_total_cap()
 {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    let model = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(fixture.next),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        model.clone(),
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: Default::default(),
        },
    );
    let empty = handle.workers(0, None).await.unwrap();
    let empty: super::service::InventoryPage<WorkerEnrollment> = decode(empty.as_ref()).unwrap();
    assert!(empty.done && empty.items.is_empty());
    for id in 1..=300 {
        fixture.enroll(id).await;
    }
    let mut after = None;
    let mut received = 0;
    for (count, done) in [(128, false), (128, false), (44, true), (0, true)] {
        let page = handle.workers(300, after).await.unwrap();
        let page: super::service::InventoryPage<WorkerEnrollment> = decode(page.as_ref()).unwrap();
        assert_eq!(page.items.len(), count);
        assert_eq!(page.done, done);
        received += page.items.len();
        after = page.after;
    }
    assert_eq!(received, 300);
    assert_eq!(model.reads.load(Ordering::Acquire), 5);
    assert!(handle.workers(299, None).await.is_err());
    let wrong = QueueScope {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: Text::try_from("default").unwrap(),
    };
    assert!(handle.current(&wrong).await.is_err());
}

#[tokio::test]
async fn c07_terminal_reader_requires_current_enrollment_and_same_database_tso_membership() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool).await;
    fixture.initialize().await;
    let owner = fixture.enroll(1).await;
    let reader = fixture.enroll(2).await;
    fixture.add_ready(owner, &mut Vec::new()).await;
    let timestamps = fixture
        .register_group(
            GroupKind::Timestamps,
            vec![ReplicaMember {
                node_id: 7,
                worker_id: reader.worker_id,
                pool_id: nabedra_rt::ids::uuid_v4(),
            }],
        )
        .await;
    let model = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(fixture.next),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        model.clone(),
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: Default::default(),
        },
    );
    let queue_scope = QueueScope {
        cluster_id: scope.cluster_id,
        database: scope.database,
    };
    assert_eq!(handle.current(&queue_scope).await.unwrap().owner, owner);
    assert_eq!(
        handle
            .terminal_reader_view(&queue_scope, reader, timestamps.identity.group_id)
            .await
            .unwrap()
            .owner,
        owner
    );
    assert!(
        handle
            .terminal_reader_view(&queue_scope, owner, timestamps.identity.group_id)
            .await
            .is_err()
    );
    let mut obsolete = reader;
    obsolete.incarnation = nabedra_rt::ids::uuid_v4();
    assert!(
        handle
            .terminal_reader_view(&queue_scope, obsolete, timestamps.identity.group_id)
            .await
            .is_err()
    );
    assert!(
        handle
            .terminal_reader_view(&queue_scope, reader, nabedra_rt::ids::uuid_v4())
            .await
            .is_err()
    );
    assert_eq!(model.reads.load(Ordering::Acquire), 5);
}

#[tokio::test]
async fn r04_control_cold_fence_waits_for_every_group_and_old_worker_exclusion() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    fixture.initialize().await;
    let owner = fixture.enroll(1).await;
    let ready = fixture.add_ready(owner, &mut Vec::new()).await;
    let group = fixture
        .register_group(
            GroupKind::Timestamps,
            vec![ReplicaMember {
                node_id: 8,
                worker_id: owner.worker_id,
                pool_id: nabedra_rt::ids::uuid_v4(),
            }],
        )
        .await;
    let transition = nabedra_rt::ids::uuid_v4();
    let begin = ControlCommand::BeginColdFence {
        operation_id: transition,
        expected: ready,
        next_run: nabedra_rt::ids::uuid_v4(),
        authorization: authorization(),
    };
    let ControlReply::FenceBegun {
        next,
        required_groups,
        ..
    } = fixture.apply(begin.clone()).await
    else {
        panic!("cold fence")
    };
    assert_eq!(required_groups, 2);
    assert_ne!(next.view.run_id, ready.view.run_id);
    assert_eq!(fixture.state().await.ready, ready);
    assert_eq!(
        fixture
            .apply(ControlCommand::PublishFence {
                operation_id: nabedra_rt::ids::uuid_v4(),
                transition_id: transition
            })
            .await,
        ControlReply::Rejected(Fault::Incomplete)
    );
    let ack = ControlCommand::GroupFenced {
        operation_id: nabedra_rt::ids::uuid_v4(),
        receipt: GroupFenceReceipt {
            transition_id: transition,
            group: group.identity,
            expected: Some(ready.view.authority()),
            next: next.view.authority(),
            applied_at: AppliedPosition {
                term: 2,
                node_id: 8,
                index: 20,
            },
            command_digest: hash(b"fence proof"),
        },
    };
    let mut invalid = ack.clone();
    if let ControlCommand::GroupFenced { receipt, .. } = &mut invalid {
        receipt.next.run_id = ready.view.run_id;
    }
    assert_eq!(
        fixture.apply(invalid).await,
        ControlReply::Rejected(Fault::Invalid)
    );
    assert!(matches!(
        fixture.apply(ack.clone()).await,
        ControlReply::FenceProgress {
            fenced_groups: 2,
            excluded_workers: 0,
            ..
        }
    ));
    assert!(matches!(
        fixture.apply(ack).await,
        ControlReply::FenceProgress {
            fenced_groups: 2,
            excluded_workers: 0,
            ..
        }
    ));
    assert_eq!(
        fixture
            .apply(ControlCommand::PublishFence {
                operation_id: nabedra_rt::ids::uuid_v4(),
                transition_id: transition
            })
            .await,
        ControlReply::Rejected(Fault::Incomplete)
    );
    drop(fixture);
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    let proof = ExclusionStamp {
        worker: owner,
        authorization: authorization(),
        ownership_fence: hash(b"operator-verified ownership exclusion model"),
    };
    assert!(matches!(
        fixture
            .apply(ControlCommand::ExcludedPage {
                operation_id: nabedra_rt::ids::uuid_v4(),
                transition_id: transition,
                workers: Page::new(vec![proof]).unwrap()
            })
            .await,
        ControlReply::FenceProgress {
            fenced_groups: 2,
            excluded_workers: 1,
            ..
        }
    ));
    let publish = ControlCommand::PublishFence {
        operation_id: nabedra_rt::ids::uuid_v4(),
        transition_id: transition,
    };
    assert_eq!(
        fixture.apply(publish.clone()).await,
        ControlReply::Published {
            ready: next,
            owner: None
        }
    );
    assert_eq!(
        fixture.apply(publish).await,
        ControlReply::Published {
            ready: next,
            owner: None
        }
    );
    assert_eq!(
        fixture.apply(begin).await,
        ControlReply::FenceBegun {
            transition_id: transition,
            next,
            required_groups: 2
        }
    );
    assert_eq!(
        fixture
            .apply(ControlCommand::BeginColdFence {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: next,
                next_run: ready.view.run_id,
                authorization: authorization(),
            })
            .await,
        ControlReply::Rejected(Fault::Conflict)
    );
    assert_eq!(fixture.state().await.ready, next);
}

#[tokio::test]
async fn u02_control_administration_rejects_worker_lane_and_unpinned_certificate() {
    use crate::oltp_replication::multiplex::{AuthenticatedSession, Purpose};
    let directory = Directory::new();
    let (scope, pool) = identities();
    let fixture = Fixture::open(&directory, scope.clone(), pool).await;
    let local = WorkerIdentity::from(&worker(1));
    let remote = WorkerIdentity::from(&worker(2));
    let model = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(0),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        model.clone(),
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: [remote.certificate_sha256].into_iter().collect(),
        },
    );
    let wrong_lane = AuthenticatedSession::for_test(
        local,
        remote,
        scope.clone(),
        Purpose::QueueControl,
        nabedra_rt::ids::uuid_v4(),
    );
    assert!(
        handle
            .initialize(&wrong_lane, nabedra_rt::ids::uuid_v4(), nabedra_rt::ids::uuid_v4())
            .await
            .is_err()
    );
    let unknown = AuthenticatedSession::for_test(
        local,
        WorkerIdentity::from(&worker(3)),
        scope.clone(),
        Purpose::Authority,
        nabedra_rt::ids::uuid_v4(),
    );
    assert!(
        handle
            .initialize(&unknown, nabedra_rt::ids::uuid_v4(), nabedra_rt::ids::uuid_v4())
            .await
            .is_err()
    );
    assert_eq!(model.next.load(Ordering::Acquire), 0);
    let allowed =
        AuthenticatedSession::for_test(local, remote, scope, Purpose::Authority, nabedra_rt::ids::uuid_v4());
    let id = nabedra_rt::ids::uuid_v4();
    let run = nabedra_rt::ids::uuid_v4();
    let first = handle.initialize(&allowed, id, run).await.unwrap();
    assert!(matches!(first.reply, ControlReply::Initialized { .. }));
    let retry = handle.initialize(&allowed, id, run).await.unwrap();
    assert_eq!(first.reply, retry.reply);
}

#[tokio::test]
async fn c07_committed_control_apply_uses_private_budget_when_transport_progress_is_full() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let fixture = Fixture::open(&directory, scope, pool).await;
    let command = ControlCommand::Initialize {
        operation_id: nabedra_rt::ids::uuid_v4(),
        run_id: nabedra_rt::ids::uuid_v4(),
        authorization: authorization(),
    };
    let digest = serialized_digest(&command).unwrap();
    let shared = fixture.machine.store().resources();
    let command = OwnedCommand::new_owned(command, shared, ByteClass::Progress).unwrap();
    let available = shared.config().progress_bytes - shared.buffer_bytes_used(ByteClass::Progress);
    let saturated = shared
        .allocate_bytes(ByteClass::Progress, available)
        .unwrap();
    assert!(shared.allocate_bytes(ByteClass::Progress, 1).is_err());
    let result = fixture
        .machine
        .apply(
            ApplyMeta {
                position: AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index: 0,
                },
                command_digest: digest,
                membership: None,
            },
            command,
        )
        .await
        .unwrap();
    assert!(matches!(
        decode::<ControlReply>(result.as_ref()).unwrap(),
        ControlReply::Initialized { .. }
    ));
    drop(saturated);
}

#[tokio::test]
async fn u01_api_ingress_enrollment_never_grants_worker_readiness_or_replica_proofs() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope.clone(), pool).await;
    fixture.initialize().await;
    let owner = fixture.enroll(1).await;
    let ready = fixture.add_ready(owner, &mut Vec::new()).await;
    let api = ApiEnrollment {
        database: scope.database.clone(),
        enrollment: worker(800),
    };
    let previous = WorkerIdentity::from(api.transport());
    assert_eq!(
        fixture
            .apply(ControlCommand::EnrollApi {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: None,
                next: api.clone(),
                authorization: authorization(),
            })
            .await,
        ControlReply::Enrolled(previous)
    );
    let mut rotated = api.clone();
    rotated.enrollment.incarnation = nabedra_rt::ids::uuid_v4();
    rotated.enrollment.certificate_sha256 = hash(b"rotated API certificate");
    rotated.enrollment.enrollment_revision += 1;
    let current = WorkerIdentity::from(rotated.transport());
    assert_eq!(
        fixture
            .apply(ControlCommand::EnrollApi {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: Some(previous),
                next: rotated.clone(),
                authorization: authorization(),
            })
            .await,
        ControlReply::Enrolled(current)
    );
    assert_eq!(fixture.state().await.ready, ready);
    assert!(matches!(
        fixture
            .apply(ControlCommand::BeginJoin {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: ready.view,
                target: current,
            })
            .await,
        ControlReply::Rejected(_)
    ));
    assert_eq!(
        fixture
            .apply(ControlCommand::Enroll {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: None,
                next: rotated.enrollment.clone(),
                authorization: authorization(),
                previous_process: None,
            })
            .await,
        ControlReply::Rejected(Fault::Conflict)
    );
    let state = fixture.state().await;
    let model = Arc::new(NativeModelConsensus {
        machine: fixture.machine.clone(),
        next: AtomicU64::new(fixture.next),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        fixture.machine.clone(),
        model,
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: Default::default(),
        },
    );
    let queue_scope = QueueScope {
        cluster_id: scope.cluster_id,
        database: scope.database.clone(),
    };
    assert!(
        handle
            .service_role(
                &queue_scope,
                previous,
                ServiceAction::ApiIngress,
                scope.clone()
            )
            .await
            .is_err()
    );
    assert_eq!(
        handle
            .service_role(
                &queue_scope,
                current,
                ServiceAction::ApiIngress,
                scope.clone()
            )
            .await
            .unwrap()
            .owner,
        Some(owner)
    );
    for action in [
        ServiceAction::Owner,
        ServiceAction::TimestampProof {
            source_group: scope.group_id,
        },
        ServiceAction::ParticipantProof {
            source_group: scope.group_id,
        },
    ] {
        assert!(
            handle
                .service_role(&queue_scope, current, action, scope.clone())
                .await
                .is_err()
        );
    }
    let page: InventoryPage<ApiEnrollment> = decode(
        handle
            .apis(state.enrollment_revision, None)
            .await
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(&*page.items, &[rotated]);
    assert!(page.done);
    assert!(handle.worker(current.worker_id).await.unwrap().is_none());
}

// A repeated initial proposal with the same operation ID and body replays the
// stored Control reply without a second apply; the same ID with another body
// is a conflict (O16, from the #104 review).
#[tokio::test]
async fn o16_repeated_initial_proposals_replay_one_application() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    let initialize = ControlCommand::Initialize {
        operation_id: nabedra_rt::ids::uuid_v4(),
        run_id: nabedra_rt::ids::uuid_v4(),
        authorization: authorization(),
    };
    let first = fixture.apply(initialize.clone()).await;
    assert!(matches!(first, ControlReply::Initialized { .. }), "{first:?}");
    let before = control_dump(&fixture.machine).await;
    assert_eq!(fixture.apply(initialize.clone()).await, first);
    let after = control_dump(&fixture.machine).await;
    let changed: Vec<String> = after
        .iter()
        .filter(|entry| !before.contains(entry))
        .map(|(key, _)| String::from_utf8_lossy(key).into_owned())
        .collect();
    println!("keys changed by the duplicate Initialize: {changed:?}");
    assert_eq!(before.len(), after.len(), "duplicate Initialize added keys: {changed:?}");

    let next = worker(fixture.base + 1);
    let enroll = ControlCommand::Enroll {
        operation_id: nabedra_rt::ids::uuid_v4(),
        expected: None,
        next: next.clone(),
        authorization: authorization(),
        previous_process: None,
    };
    let enrolled = fixture.apply(enroll.clone()).await;
    assert_eq!(enrolled, ControlReply::Enrolled(WorkerIdentity::from(&next)));
    let revision = fixture.state().await.enrollment_revision;
    let before = control_dump(&fixture.machine).await;
    assert_eq!(fixture.apply(enroll.clone()).await, enrolled);
    assert_eq!(fixture.state().await.enrollment_revision, revision);
    let after = control_dump(&fixture.machine).await;
    assert_eq!(before.len(), after.len());

    let ControlCommand::Enroll { operation_id, .. } = enroll else { unreachable!() };
    let other = ControlCommand::Enroll {
        operation_id,
        expected: None,
        next: worker(fixture.base + 2),
        authorization: authorization(),
        previous_process: None,
    };
    assert_eq!(
        fixture.apply(other).await,
        ControlReply::Rejected(Fault::Conflict)
    );
    assert_eq!(fixture.state().await.enrollment_revision, revision);
}
