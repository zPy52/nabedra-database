//! Store-level evidence for direct commits (plan sections 4 and 5): real
//! group stores, model consensus (sequential apply positions, no Raft).
use super::*;
use crate::oltp::{
    mvcc::{Change, PreparedWrite, index::IndexValue},
    pool::{ByteClass, OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources},
};
use nabedra_oltp_types::{
    authority::SealedStatusDecision,
    owned::OwnedCommand,
    types::{DecisionStatement, Page, Text, Timestamp, TxnDecision},
};
use nabedra_resources::config::WorkerResources;
use std::path::PathBuf;
#[cfg(test)]
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
    time::Duration,
};

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn ts(value: u64) -> Timestamp {
    Timestamp::new(value).unwrap()
}
fn digest(byte: u8) -> Digest {
    Digest::from_bytes([byte; 32])
}
fn full_span() -> KeySpan {
    KeySpan {
        start: None,
        end: None,
    }
}
fn shared() -> Arc<WorkerSharedResources> {
    WorkerSharedResources::new(
        1,
        WorkerResources::default(),
        SharedResourceConfig::default(),
    )
    .unwrap()
}
fn json(shared: &WorkerSharedResources, value: serde_json::Value) -> CanonicalJson {
    CanonicalJson::from_serializable(shared, &value, crate::protocol::MAX_BODY_BYTES).unwrap()
}
const BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 32 << 20,
    batch_bytes: 32 << 20,
    output_bytes: 64 << 10,
};

/// One replica of one Data group: a machine plus its next apply index.
struct Replica {
    machine: ParticipantMachine,
    shared: Arc<WorkerSharedResources>,
    index: u64,
}
impl Replica {
    async fn submit(&mut self, command: ParticipantCommand) -> ParticipantReply {
        let index = self.index;
        self.index += 1;
        apply_at(&self.machine, &self.shared, index, command).await
    }
    async fn dump(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        dump(&self.machine).await
    }
}

async fn apply_at(
    machine: &ParticipantMachine,
    shared: &WorkerSharedResources,
    index: u64,
    command: ParticipantCommand,
) -> ParticipantReply {
    let command = OwnedCommand::new_owned(command, shared, ByteClass::Data).unwrap();
    let digest = nabedra_oltp_types::owned::serialized_digest(&command).unwrap();
    let raw = machine
        .apply(
            ApplyMeta {
                position: AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index,
                },
                command_digest: digest,
                membership: None,
            },
            command,
        )
        .await
        .unwrap();
    serde_json::from_slice(raw.as_slice()).unwrap()
}

/// Every key and value in the group's applied-state namespace. The applied
/// position and reply live in separate metadata keys, so a rejected entry
/// leaves this dump unchanged.
/// Pages of at most 32 records keep each read inside `BUDGET`.
async fn dump(machine: &ParticipantMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
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
        let page: Vec<(Vec<u8>, Vec<u8>)> = serde_json::from_slice(raw.as_slice()).unwrap();
        if page.is_empty() {
            return out;
        }
        out.extend(page);
    }
}
#[cfg(test)]
fn count_tag(dump: &[(Vec<u8>, Vec<u8>)], tag: u8) -> usize {
    dump.iter()
        .filter(|(key, _)| key.first() == Some(&tag))
        .count()
}

struct Fixture {
    replica: Replica,
    pool: OltpPool,
    pool_identity: PoolIdentity,
    group: GroupIdentity,
    authority: Authority,
    table: Uuid,
    catalog_group_id: Uuid,
    /// The exact first entry, so another replica can apply the same log.
    first_fence: ParticipantCommand,
    dir: Dir,
    /// The table has search columns (`search` tests).
    search: bool,
}
/// A replica in its own pool: keeps the pool and directory alive.
#[cfg(test)]
struct OtherReplica {
    replica: Replica,
    _pool: OltpPool,
    _dir: Dir,
}
#[cfg(test)]
impl std::ops::Deref for OtherReplica {
    type Target = Replica;
    fn deref(&self) -> &Replica {
        &self.replica
    }
}
#[cfg(test)]
impl std::ops::DerefMut for OtherReplica {
    fn deref_mut(&mut self) -> &mut Replica {
        &mut self.replica
    }
}
impl Fixture {
    /// Another worker's replica of this group, with the fence applied.
    #[cfg(test)]
    async fn replica_of(&self) -> OtherReplica {
        let mut other = self.replica_of_inactive().await;
        assert!(matches!(
            other.submit(self.activation()).await,
            ParticipantReply::DirectSchema(_)
        ));
        other
    }
    /// Another worker's replica with only the first fence applied.
    #[cfg(test)]
    async fn replica_of_inactive(&self) -> OtherReplica {
        let dir = Dir(std::env::temp_dir().join(format!("mvcc-direct-{}", nabedra_rt::ids::uuid_v4())));
        let mut identity = self.pool_identity.clone();
        identity.worker_id = nabedra_rt::ids::uuid_v4();
        identity.pool_id = nabedra_rt::ids::uuid_v4();
        // Another worker: its own storage and memory budgets.
        let shared = shared();
        let pool = OltpPool::open_development(&dir.0, identity, shared.clone()).unwrap();
        let store = pool.group(self.group.clone()).await.unwrap();
        let mut replica = Replica {
            machine: ParticipantMachine::new(store).unwrap(),
            shared,
            index: 0,
        };
        assert!(matches!(
            replica.submit(self.first_fence.clone()).await,
            ParticipantReply::Fenced(_)
        ));
        OtherReplica {
            replica,
            _pool: pool,
            _dir: dir,
        }
    }
    async fn new() -> Self {
        Self::with(nabedra_rt::ids::uuid_v4(), full_span()).await
    }
    async fn with(table: Uuid, span: KeySpan) -> Self {
        let mut f = Self::inactive(table, span).await;
        let activation = f.activation();
        assert!(matches!(
            f.submit(activation).await,
            ParticipantReply::DirectSchema(_)
        ));
        f
    }
    /// A fenced group that `data.direct.v1` has not reached yet.
    async fn inactive(table: Uuid, span: KeySpan) -> Self {
        let shared = shared();
        let dir = Dir(std::env::temp_dir().join(format!("mvcc-direct-{}", nabedra_rt::ids::uuid_v4())));
        let pool_identity = PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: nabedra_rt::ids::uuid_v4(),
            worker_id: nabedra_rt::ids::uuid_v4(),
            pool_id: nabedra_rt::ids::uuid_v4(),
        };
        let pool =
            OltpPool::open_development(&dir.0, pool_identity.clone(), shared.clone()).unwrap();
        let group = GroupIdentity {
            cluster_id: pool_identity.cluster_id,
            database: Text::try_from("default").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Data {
                table_id: table,
                shard_id: nabedra_rt::ids::uuid_v4(),
            },
            group_format_version: 1,
        };
        let store = pool.group(group.clone()).await.unwrap();
        let authority = Authority {
            run_id: nabedra_rt::ids::uuid_v4(),
            owner_term: 1,
        };
        let catalog_group_id = nabedra_rt::ids::uuid_v4();
        let first_fence = ParticipantCommand::Fence {
            transition_id: nabedra_rt::ids::uuid_v4(),
            expected: None,
            next: authority,
            schema_epoch: 1,
            routing_epoch: 1,
            span,
            catalog_group_id,
        };
        let mut f = Self {
            replica: Replica {
                machine: ParticipantMachine::new(store).unwrap(),
                shared,
                index: 0,
            },
            pool,
            pool_identity,
            group,
            authority,
            table,
            catalog_group_id,
            first_fence: first_fence.clone(),
            dir,
            search: false,
        };
        let fence = f.first_fence.clone();
        assert!(matches!(f.submit(fence).await, ParticipantReply::Fenced(_)));
        f
    }
    /// `data.direct.v1` reaches the group: the store-side gate opens, bound
    /// to the table's initial Catalog lifecycle (its id, as `table.create`
    /// writes it).
    fn activation(&self) -> ParticipantCommand {
        ParticipantCommand::DirectSchema(DirectSchemaFence {
            authority: self.authority,
            table_id: self.table,
            change: DirectSchemaChange::Activate {
                lifecycle: self.table,
            },
        })
    }
    fn fence(&self, schema_epoch: u64, routing_epoch: u64, span: KeySpan) -> ParticipantCommand {
        ParticipantCommand::Fence {
            transition_id: nabedra_rt::ids::uuid_v4(),
            expected: None,
            next: self.authority,
            schema_epoch,
            routing_epoch,
            span,
            catalog_group_id: self.catalog_group_id,
        }
    }
    fn machine(&self) -> &ParticipantMachine {
        &self.replica.machine
    }
    fn shared(&self) -> &Arc<WorkerSharedResources> {
        &self.replica.shared
    }
    async fn submit(&mut self, command: ParticipantCommand) -> ParticipantReply {
        self.replica.submit(command).await
    }
    async fn direct(&mut self, commit: &DirectCommit) -> ParticipantReply {
        self.submit(ParticipantCommand::Direct(commit.clone()))
            .await
    }
    async fn dump(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.replica.dump().await
    }
    /// A `DirectForward` of `request_id` (body `body`) anchored at `key`.
    fn forward_of(&self, key: i64, request_id: Uuid, body: u8) -> DirectForward {
        DirectForward {
            authority: self.authority,
            table_id: self.table,
            anchor: Key::Int64(key),
            request_id,
            digest: digest(body),
            applied_unix_ms: 5_000,
        }
    }
    async fn forward(&mut self, forward: &DirectForward) -> ParticipantReply {
        self.submit(ParticipantCommand::DirectForward(forward.clone()))
            .await
    }
    async fn lookup(&self, key: i64, request_id: Uuid) -> Option<DedupLookup> {
        self.machine()
            .direct_dedup(self.table, Key::Int64(key), request_id)
            .await
            .unwrap()
    }
    fn key(&self, key: i64) -> MvccKey {
        MvccKey::Row {
            table_id: self.table,
            primary_key: Key::Int64(key),
        }
    }
    fn json(&self, value: serde_json::Value) -> CanonicalJson {
        json(self.shared(), value)
    }
    fn write(
        &self,
        key: i64,
        expected: Option<u64>,
        value: Option<serde_json::Value>,
    ) -> DirectWrite {
        DirectWrite {
            key: self.key(key),
            expected_version: expected.map(ts),
            value: value.map(|value| self.json(value)),
            index: Vec::new(),
            search: None,
        }
    }
    fn commit(
        &self,
        request_id: Uuid,
        body: u8,
        commit_ts: u64,
        writes: Vec<DirectWrite>,
    ) -> DirectCommit {
        DirectCommit {
            authority: self.authority,
            table_id: self.table,
            schema_epoch: 1,
            routing_epoch: 1,
            request_id,
            digest: digest(body),
            commit_ts: ts(commit_ts),
            writes,
            result: self.json(serde_json::json!({"request": request_id, "ts": commit_ts})),
            applied_unix_ms: 1_000 + commit_ts,
        }
    }
    /// A one-row upsert of `key` committed at `commit_ts`.
    fn put(&self, key: i64, expected: Option<u64>, commit_ts: u64) -> DirectCommit {
        self.commit(
            nabedra_rt::ids::uuid_v4(),
            1,
            commit_ts,
            vec![self.write(
                key,
                expected,
                Some(serde_json::json!({"id": key, "ts": commit_ts})),
            )],
        )
    }
    async fn read(&self, s: u64, key: i64) -> serde_json::Value {
        let raw = self.machine().read_at(ts(s), self.key(key)).await.unwrap();
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
    async fn version(&self, s: u64, key: i64) -> Option<u64> {
        self.read(s, key).await["value"]["version"].as_u64()
    }
    /// Rule 3 for the fixture: this node leads term 1, which already applied.
    fn lead(&self) {
        self.machine().range_clock().lead(1);
        assert!(self.machine().range_clock().serving());
    }

    // Queued-path helpers, as in the participant store tests.
    #[cfg(test)]
    fn queued_plan(&self, read: u64, writes: &[PreparedWrite]) -> QueuedPlan {
        let txn = TxnId {
            request_id: nabedra_rt::ids::uuid_v4(),
            attempt: 0,
        };
        let request = RequestBinding {
            run_id: self.authority.run_id,
            request_id: txn.request_id,
            digest: digest(1),
            accepted_context: digest(2),
        };
        let manifest = ManifestRef {
            status_group_id: nabedra_rt::ids::uuid_v4(),
            digest: digest(3),
            participant_count: 1,
        };
        let mut plan = ParticipantPlan {
            group: self.group.clone(),
            schema_epoch: 1,
            routing_epoch: 1,
            read_ts: ts(read),
            write_root: digest(0),
            write_count: writes.len() as u64,
            page_count: 1,
        };
        let root = initial_root(txn, &plan).unwrap();
        plan.write_root = page_root(root, 0, writes).unwrap();
        QueuedPlan {
            txn,
            request,
            manifest,
            plan,
            root,
        }
    }
    #[cfg(test)]
    async fn prepare(&mut self, p: &QueuedPlan, writes: Vec<PreparedWrite>) -> ParticipantReply {
        let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
            lifecycle::LifecycleBinding {
                guard_id: nabedra_rt::ids::uuid_v4(),
                catalog_group_id: self.catalog_group_id,
                table_id: self.table,
                lifecycle_version: None,
                schema_epoch: 1,
                txn: p.txn,
                request: p.request,
                read_ts: p.plan.read_ts,
                authority: self.authority,
            },
            lifecycle::GuardMode::Shared,
        )
        .into_sealed();
        self.submit(ParticipantCommand::PreparePage {
            authority: self.authority,
            txn: p.txn,
            request: p.request,
            manifest: p.manifest,
            plan: p.plan.clone(),
            page_index: 0,
            previous_root: p.root,
            guards: Page::new(vec![guard]).unwrap(),
            writes: Page::new(writes).unwrap(),
        })
        .await
    }
    #[cfg(test)]
    async fn seal_and_resolve(&mut self, p: &QueuedPlan, commit_ts: u64) {
        assert!(matches!(
            self.submit(ParticipantCommand::Seal {
                authority: self.authority,
                txn: p.txn,
                manifest: p.manifest,
                result_digest: digest(4),
            })
            .await,
            ParticipantReply::Sealed(_)
        ));
        let proof = SealedStatusDecision::seal(DecisionStatement {
            txn: p.txn,
            request: p.request,
            manifest: p.manifest,
            decided_at: AppliedPosition {
                term: 1,
                node_id: 1,
                index: 1,
            },
            decision: TxnDecision::Commit {
                commit_ts: ts(commit_ts),
            },
        })
        .unwrap();
        assert!(matches!(
            self.submit(ParticipantCommand::Resolve {
                authority: self.authority,
                proof,
                max_items: 128,
            })
            .await,
            ParticipantReply::Resolved { complete: true, .. }
        ));
    }
    fn queued_put(
        &self,
        key: i64,
        expected: Option<u64>,
        value: serde_json::Value,
    ) -> PreparedWrite {
        let value = self.json(value);
        PreparedWrite {
            key: self.key(key),
            expected_version: expected.map(ts),
            // A table with search columns takes no plain row put.
            change: match self.search {
                true => Change::PutRow {
                    value,
                    search: Box::default(),
                },
                false => Change::Put(value),
            },
        }
    }
}
#[cfg(test)]
struct QueuedPlan {
    txn: TxnId,
    request: RequestBinding,
    manifest: ManifestRef,
    plan: ParticipantPlan,
    root: Digest,
}

/// Register like the executor: take the leader token, then (here, already)
/// the timestamp, then register.
#[cfg(test)]
fn register(clock: &RangeClock, commit_ts: u64, keys: Vec<MvccKey>) -> Result<(), ClockRejection> {
    let token = clock.leader_token().ok_or(ClockRejection::NotServing)?;
    clock.register_pending(token, ts(commit_ts), keys)
}

#[cfg(test)]
fn applied(commit_ts: u64) -> ParticipantReply {
    ParticipantReply::Direct(DirectReply::Applied {
        commit_ts: ts(commit_ts),
    })
}
#[cfg(test)]
fn forwarded_reply() -> ParticipantReply {
    ParticipantReply::Direct(DirectReply::Forwarded)
}
#[cfg(test)]
fn rejected(fault: Fault) -> ParticipantReply {
    ParticipantReply::Direct(DirectReply::Rejected(fault))
}
fn replayed(commit: &DirectCommit) -> ParticipantReply {
    ParticipantReply::Direct(DirectReply::Replayed {
        result: DirectResult(RawValue::from_string(commit.result.as_str().to_owned()).unwrap()),
    })
}

/// Submit `commit` and assert it is rejected with `fault` and writes nothing.
#[cfg(test)]
async fn assert_rejected_without_writes(f: &mut Fixture, commit: &DirectCommit, fault: Fault) {
    let before = f.dump().await;
    assert_eq!(f.direct(commit).await, rejected(fault));
    assert_eq!(
        f.dump().await,
        before,
        "a rejected direct commit wrote state"
    );
}

// ---------------------------------------------------------------------------
// Apply and dedup (O11, D03)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn o11_d03_direct_commit_writes_versions_heads_index_and_one_dedup_record() {
    let mut f = Fixture::new().await;
    let index_key = MvccKey::IndexEntry {
        table_id: f.table,
        index_id: nabedra_rt::ids::uuid_v4(),
        values: Page::new(vec![IndexValue::Int64(7)]).unwrap(),
        primary_key: Key::Int64(1),
    };
    let mut write = f.write(1, None, Some(serde_json::json!({"id": 1, "v": 7})));
    write.index.push(IndexChange {
        key: index_key.clone(),
        expected_version: None,
        value: Some(f.json(serde_json::json!({"id": 1}))),
    });
    let commit = f.commit(nabedra_rt::ids::uuid_v4(), 1, 10, vec![write]);
    assert_eq!(f.direct(&commit).await, applied(10));

    assert_eq!(f.version(10, 1).await, Some(10));
    assert_eq!(f.read(10, 1).await["value"]["value"]["v"], 7);
    assert_eq!(
        f.version(9, 1).await,
        None,
        "nothing visible below the commit"
    );
    let index = f.machine().read_at(ts(10), index_key).await.unwrap();
    let index: serde_json::Value = serde_json::from_slice(index.as_slice()).unwrap();
    assert_eq!(index["value"]["version"], 10);

    let state = f.dump().await;
    assert_eq!(count_tag(&state, DEDUP), 1, "exactly one dedup record");
    assert_eq!(count_tag(&state, EXPIRY), 1, "and its expiry index entry");
    assert_eq!(count_tag(&state, VALUE), 2, "row and index versions");
    assert_eq!(count_tag(&state, HEAD), 2, "row and index heads");
}

#[tokio::test]
async fn d03_exact_retry_replays_the_original_result_without_a_second_version() {
    let mut f = Fixture::new().await;
    let first = f.put(1, None, 10);
    assert_eq!(f.direct(&first).await, applied(10));
    let state = f.dump().await;
    // The gateway resends the same id and body; the new leader computed it
    // again from stale state with a new timestamp.
    let mut retry = first.clone();
    retry.commit_ts = ts(20);
    retry.result = f.json(serde_json::json!({"recomputed": true}));
    assert_eq!(f.direct(&retry).await, replayed(&first));
    assert_eq!(f.dump().await, state, "a replay writes nothing");
    assert_eq!(f.version(25, 1).await, Some(10));
}

#[tokio::test]
async fn d03_same_id_different_body_conflicts_on_the_same_key_and_executes_on_another_key() {
    let mut f = Fixture::new().await;
    let id = nabedra_rt::ids::uuid_v4();
    let first = f.commit(
        id,
        1,
        10,
        vec![f.write(1, None, Some(serde_json::json!({"a": 1})))],
    );
    assert_eq!(f.direct(&first).await, applied(10));
    // Same id, different body, same anchor key: the existing conflict.
    let same_key = f.commit(
        id,
        2,
        11,
        vec![f.write(1, Some(10), Some(serde_json::json!({"a": 2})))],
    );
    assert_rejected_without_writes(&mut f, &same_key, Fault::Conflict).await;
    assert_eq!(f.read(15, 1).await["value"]["value"]["a"], 1);
    // Same id, different body, another anchor: detection is per table and
    // key, so this executes (plan decision "Request IDs").
    let other_key = f.commit(
        id,
        2,
        12,
        vec![f.write(2, None, Some(serde_json::json!({"a": 3})))],
    );
    assert_eq!(f.direct(&other_key).await, applied(12));
    assert_eq!(count_tag(&f.dump().await, DEDUP), 2);
    // Its own retry replays its own result.
    assert_eq!(f.direct(&other_key).await, replayed(&other_key));
}

#[tokio::test]
async fn d03_multi_row_commit_anchors_its_dedup_record_on_the_smallest_key() {
    let mut f = Fixture::new().await;
    let commit = f.commit(
        nabedra_rt::ids::uuid_v4(),
        1,
        10,
        vec![
            f.write(3, None, Some(serde_json::json!({"id": 3}))),
            f.write(8, None, Some(serde_json::json!({"id": 8}))),
        ],
    );
    assert_eq!(f.direct(&commit).await, applied(10));
    let anchor = physical(DEDUP, &commit.dedup_key().unwrap()).unwrap();
    assert!(f.dump().await.iter().any(|(key, _)| *key == anchor));
    assert_eq!(commit.keys(), vec![f.key(3), f.key(8)]);
}

// ---------------------------------------------------------------------------
// Apply-order validation: one test per rejection, nothing written (O07)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn o07_direct_rejects_an_uninitialized_group() {
    let f = Fixture::new().await;
    let mut group = f.group.clone();
    group.group_id = nabedra_rt::ids::uuid_v4();
    let store = f.pool.group(group).await.unwrap();
    let mut empty = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: f.shared().clone(),
        index: 0,
    };
    let reply = empty
        .submit(ParticipantCommand::Direct(f.put(1, None, 10)))
        .await;
    assert_eq!(reply, rejected(Fault::NotInitialized));
    assert!(empty.dump().await.is_empty());
}

#[tokio::test]
async fn o07_direct_rejects_malformed_commands() {
    let mut f = Fixture::new().await;
    let mut nil = f.put(1, None, 10);
    nil.request_id = Uuid::nil();
    let unsorted = f.commit(
        nabedra_rt::ids::uuid_v4(),
        1,
        10,
        vec![f.write(2, None, None), f.write(1, None, None)],
    );
    let empty = f.commit(nabedra_rt::ids::uuid_v4(), 1, 10, Vec::new());
    let mut other_table = f.put(1, None, 10);
    other_table.table_id = nabedra_rt::ids::uuid_v4();
    let mut stray_index = f.write(1, None, None);
    stray_index.index.push(IndexChange {
        key: MvccKey::IndexEntry {
            table_id: f.table,
            index_id: nabedra_rt::ids::uuid_v4(),
            values: Page::new(vec![IndexValue::Int64(1)]).unwrap(),
            primary_key: Key::Int64(2),
        },
        expected_version: None,
        value: None,
    });
    let stray_index = f.commit(nabedra_rt::ids::uuid_v4(), 1, 10, vec![stray_index]);
    let mut not_a_row = f.write(1, None, None);
    not_a_row.key = MvccKey::CatalogTable { table_id: f.table };
    let not_a_row = f.commit(nabedra_rt::ids::uuid_v4(), 1, 10, vec![not_a_row]);
    for command in [nil, unsorted, empty, other_table, stray_index, not_a_row] {
        assert_rejected_without_writes(&mut f, &command, Fault::InvalidBinding).await;
    }
}

#[tokio::test]
async fn o07_direct_rejects_a_stale_authority() {
    let mut f = Fixture::new().await;
    let mut command = f.put(1, None, 10);
    command.authority.owner_term += 1;
    assert_rejected_without_writes(&mut f, &command, Fault::StaleAuthority).await;
}

#[tokio::test]
async fn o07_o08_direct_rejects_a_stale_schema_epoch() {
    let mut f = Fixture::new().await;
    let mut command = f.put(1, None, 10);
    command.schema_epoch = 2;
    assert_rejected_without_writes(&mut f, &command, Fault::StaleEpoch).await;
}

#[tokio::test]
async fn o07_direct_rejects_a_stale_routing_epoch() {
    let mut f = Fixture::new().await;
    let mut command = f.put(1, None, 10);
    command.routing_epoch = 2;
    assert_rejected_without_writes(&mut f, &command, Fault::StaleEpoch).await;
}

#[tokio::test]
async fn o07_direct_rejects_keys_outside_the_group_span() {
    let mut f = Fixture::with(
        nabedra_rt::ids::uuid_v4(),
        KeySpan {
            start: Some(Key::Int64(0)),
            end: Some(Key::Int64(100)),
        },
    )
    .await;
    let outside = f.put(100, None, 10);
    assert_rejected_without_writes(&mut f, &outside, Fault::InvalidBinding).await;
    // One key inside, one outside.
    let straddling = f.commit(
        nabedra_rt::ids::uuid_v4(),
        1,
        10,
        vec![f.write(5, None, None), f.write(-1, None, None)],
    );
    let straddling = DirectCommit {
        writes: straddling.writes.into_iter().rev().collect(),
        ..straddling
    };
    assert_rejected_without_writes(&mut f, &straddling, Fault::InvalidBinding).await;
    assert_eq!(f.direct(&f.put(99, None, 10)).await, applied(10));
}

#[tokio::test]
async fn o07_d08_direct_rejects_a_key_with_a_queued_intent() {
    let mut f = Fixture::new().await;
    let write = f.queued_put(1, None, serde_json::json!({"queued": true}));
    let plan = f.queued_plan(10, std::slice::from_ref(&write));
    assert!(matches!(
        f.prepare(&plan, vec![write]).await,
        ParticipantReply::PreparedPage { .. }
    ));
    let command = f.put(1, None, 11);
    assert_rejected_without_writes(&mut f, &command, Fault::Incomplete).await;
    // Once the queued transaction commits, the head moved: a direct commit
    // must name the queued version.
    f.seal_and_resolve(&plan, 12).await;
    assert_eq!(f.direct(&f.put(1, Some(12), 13)).await, applied(13));
}

#[tokio::test]
async fn o07_direct_rejects_a_stale_expected_version() {
    let mut f = Fixture::new().await;
    assert_eq!(f.direct(&f.put(1, None, 10)).await, applied(10));
    let missing = f.put(1, None, 11);
    assert_rejected_without_writes(&mut f, &missing, Fault::Conflict).await;
    let older = f.put(1, Some(9), 11);
    assert_rejected_without_writes(&mut f, &older, Fault::Conflict).await;
    // A deleted row keeps its tombstone's timestamp as its head version.
    let delete = f.commit(nabedra_rt::ids::uuid_v4(), 1, 11, vec![f.write(1, Some(10), None)]);
    assert_eq!(f.direct(&delete).await, applied(11));
    assert_eq!(
        f.read(12, 1).await["value"]["value"],
        serde_json::Value::Null
    );
    let as_absent = f.put(1, None, 12);
    assert_rejected_without_writes(&mut f, &as_absent, Fault::Conflict).await;
    assert_eq!(f.direct(&f.put(1, Some(11), 12)).await, applied(12));
}

#[tokio::test]
async fn o07_direct_rejects_a_commit_ts_not_above_the_newest_version() {
    let mut f = Fixture::new().await;
    assert_eq!(f.direct(&f.put(1, None, 10)).await, applied(10));
    for commit_ts in [10, 9] {
        let command = f.put(1, Some(10), commit_ts);
        assert_rejected_without_writes(&mut f, &command, Fault::Conflict).await;
    }
    assert_eq!(f.direct(&f.put(1, Some(10), 11)).await, applied(11));
}

#[tokio::test]
async fn o07_direct_validation_follows_the_plan_order() {
    let mut f = Fixture::new().await;
    assert_eq!(f.direct(&f.put(2, None, 10)).await, applied(10));
    let write = f.queued_put(1, None, serde_json::json!({}));
    let plan = f.queued_plan(10, std::slice::from_ref(&write));
    f.prepare(&plan, vec![write]).await;
    // Key 1 has an intent; key 2 has head 10. Each command breaks several
    // rules and must report the earliest one.
    let mut all = f.commit(nabedra_rt::ids::uuid_v4(), 1, 5, vec![f.write(1, Some(3), None)]);
    all.authority.owner_term += 1;
    all.schema_epoch = 9;
    assert_rejected_without_writes(&mut f, &all, Fault::StaleAuthority).await;
    all.authority = f.authority;
    assert_rejected_without_writes(&mut f, &all, Fault::StaleEpoch).await;
    all.schema_epoch = 1;
    assert_rejected_without_writes(&mut f, &all, Fault::Incomplete).await;
    let version_then_ts = f.put(2, Some(3), 5);
    assert_rejected_without_writes(&mut f, &version_then_ts, Fault::Conflict).await;
}

// ---------------------------------------------------------------------------
// Queued prepares see direct commits as committed versions (O07)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn o07_queued_prepare_conflicts_on_a_direct_commit_and_reads_it_as_committed() {
    let mut f = Fixture::new().await;
    assert_eq!(f.direct(&f.put(5, None, 11)).await, applied(11));
    // A queued plan whose snapshot predates the direct commit conflicts.
    let stale = f.queued_put(5, None, serde_json::json!({"queued": "stale"}));
    let plan = f.queued_plan(10, std::slice::from_ref(&stale));
    assert_eq!(
        f.prepare(&plan, vec![stale]).await,
        ParticipantReply::Rejected(Fault::Conflict)
    );
    // One that read the direct version prepares and commits above it.
    let fresh = f.queued_put(5, Some(11), serde_json::json!({"queued": "fresh"}));
    let plan = f.queued_plan(12, std::slice::from_ref(&fresh));
    assert!(matches!(
        f.prepare(&plan, vec![fresh]).await,
        ParticipantReply::PreparedPage { .. }
    ));
    f.seal_and_resolve(&plan, 13).await;
    assert_eq!(f.read(12, 5).await["value"]["value"]["ts"], 11);
    assert_eq!(f.read(13, 5).await["value"]["value"]["queued"], "fresh");
}

// ---------------------------------------------------------------------------
// Crash replay (O11)
// ---------------------------------------------------------------------------

#[cfg_attr(test, tokio::test)]
#[cfg_attr(not(test), tokio::main(flavor = "current_thread"))]
pub(super) async fn o11_crash_replay_of_a_direct_commit_entry_is_idempotent() {
    let f = Fixture::new().await;
    let index = MvccKey::IndexEntry {
        table_id: f.table,
        index_id: nabedra_rt::ids::uuid_v4(),
        values: Page::new(vec![IndexValue::Int64(7)]).unwrap(),
        primary_key: Key::Int64(1),
    };
    let mut write = f.write(1, None, Some(serde_json::json!({"id": 1, "value": 7})));
    write.index.push(IndexChange {
        key: index.clone(),
        expected_version: None,
        value: Some(f.json(serde_json::json!({"id": 1}))),
    });
    let commit = f.commit(nabedra_rt::ids::uuid_v4(), 1, 10, vec![write]);
    let key = f.key(1);
    let command = ParticipantCommand::Direct(commit.clone());
    let entry = || OwnedCommand::new_owned(command.clone(), f.shared(), ByteClass::Data).unwrap();
    let meta = |index| ApplyMeta {
        position: AppliedPosition {
            term: 1,
            node_id: 1,
            index,
        },
        command_digest: digest(9),
        membership: None,
    };
    let first = f
        .machine()
        .apply(meta(f.replica.index), entry())
        .await
        .unwrap();
    let state = f.dump().await;
    // Raft redelivers the same entry after a crash before the applied index
    // was observed: the stored reply returns and nothing is applied twice.
    let again = f
        .machine()
        .apply(meta(f.replica.index), entry())
        .await
        .unwrap();
    assert_eq!(again.as_slice(), first.as_slice());
    assert_eq!(f.dump().await, state);
    let result = first.as_slice().to_vec();
    drop((first, again));

    // Close everything and reopen the pool from disk.
    let Fixture {
        replica,
        pool,
        pool_identity,
        group,
        dir,
        first_fence,
        ..
    } = f;
    drop(first_fence);
    // The fence and the activation precede the commit.
    let next_index = replica.index + 1;
    let shared = replica.shared.clone();
    drop(replica);
    drop(pool);
    let pool = OltpPool::open_development(&dir.0, pool_identity, shared.clone()).unwrap();
    let mut reopened = Replica {
        machine: ParticipantMachine::new(pool.group(group).await.unwrap()).unwrap(),
        shared,
        index: next_index,
    };
    assert_eq!(reopened.dump().await, state, "the commit is durable");
    let cut = reopened
        .machine
        .store()
        .applied_state(BUDGET)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cut.position, meta(next_index - 1).position);
    assert_eq!(cut.command_digest, meta(next_index - 1).command_digest);
    for key in [key, index] {
        let visible = reopened.machine.read_at(ts(10), key).await.unwrap();
        let visible: serde_json::Value = serde_json::from_slice(visible.as_slice()).unwrap();
        assert_eq!(visible["value"]["version"], 10);
        assert_eq!(visible["value"]["value"]["id"], 1);
    }
    let repeated = OwnedCommand::new_owned(command, &reopened.shared, ByteClass::Data).unwrap();
    let repeated = reopened
        .machine
        .apply(meta(next_index - 1), repeated)
        .await
        .unwrap();
    assert_eq!(repeated.as_slice(), result);
    // The same request proposed again as a new entry replays.
    assert_eq!(
        reopened
            .submit(ParticipantCommand::Direct(DirectCommit {
                commit_ts: ts(20),
                ..commit.clone()
            }))
            .await,
        replayed(&commit)
    );
    assert_eq!(reopened.dump().await, state);
}

// ---------------------------------------------------------------------------
// Dedup records survive snapshots, splits, backups and restore (D03)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn d03_dedup_records_survive_raft_snapshot_install() {
    let mut f = Fixture::new().await;
    let commit = f.put(1, None, 10);
    assert_eq!(f.direct(&commit).await, applied(10));
    let snapshot = f.machine().store().export_snapshot(BUDGET).await.unwrap();

    let dir = Dir(std::env::temp_dir().join(format!("mvcc-direct-{}", nabedra_rt::ids::uuid_v4())));
    let mut identity = f.pool_identity.clone();
    identity.pool_id = nabedra_rt::ids::uuid_v4();
    let pool = OltpPool::open_development(&dir.0, identity, f.shared().clone()).unwrap();
    let store = pool.group(f.group.clone()).await.unwrap();
    store.install_snapshot(snapshot, BUDGET).await.unwrap();
    let mut follower = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: f.shared().clone(),
        index: f.replica.index,
    };
    assert_eq!(follower.dump().await, f.dump().await);
    let retry = DirectCommit {
        commit_ts: ts(20),
        ..commit.clone()
    };
    assert_eq!(
        follower.submit(ParticipantCommand::Direct(retry)).await,
        replayed(&commit)
    );
}

#[tokio::test]
async fn d03_o16_dedup_records_follow_their_anchor_row_through_a_split() {
    use super::super::{image_archive::ImagePage, lineage::SplitBinding};
    let mut f = Fixture::new().await;
    let left = f.put(1, None, 10);
    let right = f.put(4, None, 11);
    let late = f.put(5, None, 13);
    for (commit, ts) in [(&left, 10), (&right, 11), (&late, 13)] {
        assert_eq!(f.direct(commit).await, applied(ts));
    }

    let split_txn = TxnId {
        request_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    let split_request = RequestBinding {
        run_id: f.authority.run_id,
        request_id: split_txn.request_id,
        digest: digest(1),
        accepted_context: digest(2),
    };
    let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
        lifecycle::LifecycleBinding {
            guard_id: nabedra_rt::ids::uuid_v4(),
            catalog_group_id: f.catalog_group_id,
            table_id: f.table,
            lifecycle_version: Some(ts(1)),
            schema_epoch: 1,
            txn: split_txn,
            request: split_request,
            read_ts: ts(11),
            authority: f.authority,
        },
        lifecycle::GuardMode::Exclusive,
    )
    .into_sealed();
    let spans = [
        KeySpan {
            start: None,
            end: Some(Key::Int64(3)),
        },
        KeySpan {
            start: Some(Key::Int64(3)),
            end: None,
        },
    ];
    let mut children = Vec::new();
    let mut targets = Vec::new();
    for span in &spans {
        let mut target = f.group.clone();
        target.group_id = nabedra_rt::ids::uuid_v4();
        target.kind = GroupKind::Data {
            table_id: f.table,
            shard_id: nabedra_rt::ids::uuid_v4(),
        };
        let mut child = Replica {
            machine: ParticipantMachine::new(f.pool.group(target.clone()).await.unwrap()).unwrap(),
            shared: f.shared().clone(),
            index: 0,
        };
        assert!(matches!(
            child.submit(f.fence(1, 2, span.clone())).await,
            ParticipantReply::Fenced(_)
        ));
        assert!(matches!(
            child.submit(f.activation()).await,
            ParticipantReply::DirectSchema(_)
        ));
        targets.push(target);
        children.push(child);
    }
    let source = f.group.clone();
    let bindings = |cut: u64| -> [SplitBinding; 2] {
        [0, 1].map(|i| SplitBinding {
            split_id: guard.binding().guard_id,
            source: source.clone(),
            source_schema_epoch: 1,
            source_routing_epoch: 1,
            source_span: full_span(),
            target: targets[i].clone(),
            target_span: spans[i].clone(),
            read_ts: ts(cut),
            guard: guard.clone(),
        })
    };
    // A direct commit (13) above the cut already applied: the freeze is
    // refused, because the child images would miss it.
    assert_eq!(
        f.submit(ParticipantCommand::SplitFreeze {
            authority: f.authority,
            children: Box::new(bindings(12)),
        })
        .await,
        ParticipantReply::Rejected(Fault::DirectCommitAboveCut)
    );
    // A forward marker written before the freeze travels with its anchor
    // (key 2, left child) like a dedup record.
    let forwarded = f.forward_of(2, nabedra_rt::ids::uuid_v4(), 7);
    assert_eq!(f.forward(&forwarded).await, forwarded_reply());
    let bindings = bindings(14);
    assert!(matches!(
        f.submit(ParticipantCommand::SplitFreeze {
            authority: f.authority,
            children: Box::new(bindings.clone()),
        })
        .await,
        ParticipantReply::SplitSourceFrozen(_)
    ));
    // The frozen source takes no direct commit, and says so with a fault
    // the executor maps to Fallback rather than a route refresh (F6).
    let frozen = f.put(6, None, 15);
    assert_rejected_without_writes(&mut f, &frozen, Fault::SplitInProgress).await;
    // An exact retry still replays while frozen, and writes nothing.
    let before = f.dump().await;
    let retry = DirectCommit {
        commit_ts: ts(16),
        ..left.clone()
    };
    assert_eq!(f.direct(&retry).await, replayed(&left));
    // A forward on the frozen source returns the committed result, and
    // cannot write a marker the copy would miss.
    assert_eq!(
        f.forward(&f.forward_of(1, left.request_id, 1)).await,
        replayed(&left)
    );
    assert_eq!(
        f.forward(&f.forward_of(6, nabedra_rt::ids::uuid_v4(), 1)).await,
        rejected(Fault::SplitInProgress)
    );
    assert_eq!(f.dump().await, before);

    for (child, binding) in children.iter_mut().zip(bindings.iter()) {
        assert_eq!(
            child
                .submit(ParticipantCommand::SplitStage {
                    authority: f.authority,
                    binding: Box::new(binding.clone()),
                })
                .await,
            ParticipantReply::SplitStaged
        );
    }
    // A staged child that has not imported everything takes none either.
    let mut early = f.put(1, Some(10), 16);
    early.routing_epoch = 2;
    let before = children[0].dump().await;
    assert_eq!(
        children[0].submit(ParticipantCommand::Direct(early)).await,
        rejected(Fault::SplitInProgress)
    );
    // Nor a publication floor before its import completes (F8).
    assert_eq!(
        children[0]
            .submit(ParticipantCommand::ChildPublished(ChildPublished {
                authority: f.authority,
                published_at: ts(20),
            }))
            .await,
        ParticipantReply::Rejected(Fault::Incomplete)
    );
    assert_eq!(children[0].dump().await, before);

    let mut after = None;
    let mut after_digest = None;
    let mut root = lineage::initial_root(&bindings[0]).unwrap();
    let mut page_index = 0;
    let mut imported = [0; 2];
    loop {
        let request = OwnedCommand::new_owned(
            bindings[0].image_request(after.clone(), 64 << 10),
            f.shared(),
            ByteClass::Data,
        )
        .unwrap();
        let page = f.machine().committed_image_page(request).await.unwrap();
        let next = page.payload().unwrap().continuation().unwrap().cloned();
        let ImagePage::Images { exhausted, .. } = page.payload().unwrap() else {
            panic!("settled source blocked")
        };
        let exhausted = *exhausted;
        let mut next_root = None;
        for i in 0..2 {
            let (command, expected_root) = bindings[i]
                .import_page_from_image(f.shared(), page_index, root, after_digest, page.clone())
                .await
                .unwrap();
            next_root = Some(expected_root);
            let ParticipantReply::SplitImported {
                images, complete, ..
            } = children[i].apply_owned(command).await
            else {
                panic!("child rejected an image page")
            };
            imported[i] = images;
            assert_eq!(complete, exhausted);
        }
        root = next_root.unwrap();
        page_index += 1;
        after_digest = children[0]
            .machine
            .split_progress(&bindings[0], 64 << 10)
            .await
            .unwrap()
            .payload()
            .unwrap()
            .after_digest;
        after = next;
        if exhausted {
            break;
        }
    }
    // Rows plus their dedup records and markers, each with its anchor's
    // child.
    assert_eq!(imported, [3, 4]);
    for (child, records) in children.iter().zip([2, 2]) {
        let state = child.dump().await;
        assert_eq!(count_tag(&state, DEDUP), records);
        assert_eq!(count_tag(&state, EXPIRY), records);
    }
    // The forwarded request cannot run directly on the child either.
    let mut late_attempt = f.commit(forwarded.request_id, 7, 21, vec![f.write(2, None, None)]);
    late_attempt.routing_epoch = 2;
    let before = children[0].dump().await;
    assert_eq!(
        children[0]
            .submit(ParticipantCommand::Direct(late_attempt))
            .await,
        forwarded_reply()
    );
    assert_eq!(children[0].dump().await, before);
    // A retry routed to the child replays the source's exact result.
    for (child, commit, expected) in [
        (0usize, &left, Some(10)),
        (1, &right, Some(11)),
        (1, &late, Some(13)),
    ] {
        let mut retry = DirectCommit {
            commit_ts: ts(20),
            routing_epoch: 2,
            ..commit.clone()
        };
        retry.writes[0].expected_version = expected.map(ts);
        assert_eq!(
            children[child]
                .submit(ParticipantCommand::Direct(retry))
                .await,
            replayed(commit)
        );
    }
    // A new commit on the child cannot land at or below its import cut.
    let mut at_cut = f.put(4, Some(11), 14);
    at_cut.routing_epoch = 2;
    let before = children[1].dump().await;
    assert_eq!(
        children[1]
            .submit(ParticipantCommand::Direct(at_cut.clone()))
            .await,
        rejected(Fault::Conflict)
    );
    assert_eq!(children[1].dump().await, before);
    at_cut.commit_ts = ts(15);
    assert_eq!(
        children[1].submit(ParticipantCommand::Direct(at_cut)).await,
        applied(15)
    );
    // F8: once told the source's publication timestamp, the child keeps
    // direct commits above it, since the source served snapshots up to it.
    let published = |published_at| {
        ParticipantCommand::ChildPublished(ChildPublished {
            authority: f.authority,
            published_at: ts(published_at),
        })
    };
    let done = ParticipantReply::ChildPublished {
        published_at: ts(20),
    };
    assert_eq!(
        children[1].submit(published(14)).await,
        ParticipantReply::Rejected(Fault::InvalidBinding),
        "publication is above the cut"
    );
    assert_eq!(children[1].submit(published(20)).await, done);
    assert_eq!(children[1].submit(published(20)).await, done);
    assert_eq!(
        children[1].submit(published(21)).await,
        ParticipantReply::Rejected(Fault::Conflict)
    );
    for (commit_ts, reply) in [
        (19, rejected(Fault::Conflict)),
        (20, rejected(Fault::Conflict)),
    ] {
        let mut below = f.put(4, Some(15), commit_ts);
        below.routing_epoch = 2;
        let before = children[1].dump().await;
        assert_eq!(
            children[1].submit(ParticipantCommand::Direct(below)).await,
            reply
        );
        assert_eq!(children[1].dump().await, before);
    }
    let mut above = f.put(4, Some(15), 21);
    above.routing_epoch = 2;
    assert_eq!(
        children[1].submit(ParticipantCommand::Direct(above)).await,
        applied(21)
    );
}

#[cfg(test)]
impl Replica {
    async fn apply_owned(&mut self, command: OwnedCommand<ParticipantCommand>) -> ParticipantReply {
        let digest = nabedra_oltp_types::owned::serialized_digest(&command).unwrap();
        let index = self.index;
        self.index += 1;
        let raw = self
            .machine
            .apply(
                ApplyMeta {
                    position: AppliedPosition {
                        term: 1,
                        node_id: 1,
                        index,
                    },
                    command_digest: digest,
                    membership: None,
                },
                command,
            )
            .await
            .unwrap();
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
}

#[tokio::test]
async fn d03_o19_dedup_records_survive_backup_images_and_restore() {
    use super::super::{image_archive::ImageRequest, restore};
    let mut f = Fixture::new().await;
    let kept = f.put(1, None, 10);
    let above_cut = f.put(2, None, 30);
    assert_eq!(f.direct(&kept).await, applied(10));
    assert_eq!(f.direct(&above_cut).await, applied(30));
    let cut = 20;

    // Export committed images at the cut, as a full backup does.
    let mut images = Vec::new();
    let mut after = None;
    loop {
        let request = OwnedCommand::new_owned(
            ImageRequest {
                read_ts: ts(cut),
                schema_epoch: 1,
                routing_epoch: 1,
                span: full_span(),
                after: after.clone(),
                max_items: 128,
                max_bytes: 64 << 10,
                search: None,
            },
            f.shared(),
            ByteClass::Data,
        )
        .unwrap();
        let page = f.machine().committed_image_page(request).await.unwrap();
        let next = page.payload().unwrap().continuation().unwrap().cloned();
        let image_archive::ImagePage::Images {
            images: page_images,
            exhausted,
            ..
        } = page.payload().unwrap()
        else {
            panic!("blocked")
        };
        images.extend(page_images.iter().cloned());
        if *exhausted {
            break;
        }
        after = next;
    }
    assert_eq!(
        images.len(),
        2,
        "row 1 and its dedup record; nothing above the cut"
    );
    assert!(images.iter().all(|image| image.commit_ts == ts(10)));
    assert!(
        images
            .iter()
            .any(|image| matches!(image.key, MvccKey::DirectDedup { .. }))
    );
    for image in &images {
        restore::validate_archive_image(
            &crate::oltp::backup::history::GroupImageScope {
                group: f.group.clone(),
                schema_epoch: 1,
                routing_epoch: 1,
                span: full_span(),
            },
            &image.key,
        )
        .unwrap();
    }

    // Install them into a new group, as an offline restore does, with the
    // restore genesis that sets the group's floor to the cut.
    let mut target = f.group.clone();
    target.group_id = nabedra_rt::ids::uuid_v4();
    let store = f.pool.group(target.clone()).await.unwrap();
    let genesis = restore::ParticipantGenesis {
        restore_id: nabedra_rt::ids::uuid_v4(),
        source_cluster: f.group.cluster_id,
        target_group: target.clone(),
        catalog_group_id: f.catalog_group_id,
        schema_epoch: 1,
        routing_epoch: 1,
        span: full_span(),
        cut: ts(cut),
    };
    let install = {
        let target = target.clone();
        let images = images.clone();
        move |tx: &mut GroupTransaction<'_>| {
            tx.put_serialized(restore::RESTORE_GENESIS, &genesis)?;
            for image in &images {
                restore::install_image(tx, &target, &full_span(), image)?;
            }
            Ok(Transition::Commit(tx.copy_output(b"null")?))
        }
    };
    store
        .transaction(
            ApplyMeta {
                position: AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index: 0,
                },
                command_digest: digest(7),
                membership: None,
            },
            BUDGET,
            install,
        )
        .await
        .unwrap();
    let mut restored = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: f.shared().clone(),
        index: 1,
    };
    assert!(matches!(
        restored.submit(f.fence(1, 1, full_span())).await,
        ParticipantReply::Fenced(_)
    ));
    assert!(matches!(
        restored.submit(f.activation()).await,
        ParticipantReply::DirectSchema(_)
    ));
    let state = restored.dump().await;
    assert_eq!(count_tag(&state, DEDUP), 1);
    assert_eq!(count_tag(&state, EXPIRY), 1);
    // The restored request replays.
    let retry = DirectCommit {
        commit_ts: ts(40),
        ..kept.clone()
    };
    assert_eq!(
        restored.submit(ParticipantCommand::Direct(retry)).await,
        replayed(&kept)
    );
    // A request that committed above the cut is not restored: it executes.
    let mut again = above_cut.clone();
    again.commit_ts = ts(41);
    assert_eq!(
        restored.submit(ParticipantCommand::Direct(again)).await,
        applied(41)
    );
    // No direct commit can land at or below the restored floor (the cut).
    let below = f.put(3, None, cut);
    let before = restored.dump().await;
    assert_eq!(
        restored.submit(ParticipantCommand::Direct(below)).await,
        rejected(Fault::Conflict)
    );
    assert_eq!(restored.dump().await, before);
}

// ---------------------------------------------------------------------------
// Dedup expiry (deterministic replicated command)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn d03_direct_expire_deletes_exactly_the_records_before_the_cutoff_on_every_replica() {
    let mut a = Fixture::new().await;
    // A second replica of the same group, fenced by the same command.
    let mut b = a.replica_of().await;
    let commits: Vec<DirectCommit> = (1..=3)
        .map(|n| {
            let mut commit = a.put(n, None, 10 + n as u64);
            commit.applied_unix_ms = 100 * n as u64;
            commit
        })
        .collect();
    for commit in &commits {
        let expected = applied(commit.commit_ts.get());
        assert_eq!(a.direct(commit).await, expected);
        assert_eq!(
            b.submit(ParticipantCommand::Direct(commit.clone())).await,
            expected
        );
    }
    let authority = a.authority;
    let expire = move |cutoff, max_items| {
        ParticipantCommand::DirectExpire(DirectExpire {
            authority,
            cutoff_unix_ms: cutoff,
            retention_ms: 50,
            max_items,
        })
    };
    // Invalid and stale commands delete nothing.
    let before = a.dump().await;
    for (command, fault) in [
        (expire(250, 0), Fault::InvalidBinding),
        (
            expire(250, MAX_DIRECT_EXPIRE_ITEMS + 1),
            Fault::InvalidBinding,
        ),
        (expire(0, 1), Fault::InvalidBinding),
        (
            ParticipantCommand::DirectExpire(DirectExpire {
                authority,
                cutoff_unix_ms: 250,
                retention_ms: 0,
                max_items: 1,
            }),
            Fault::InvalidBinding,
        ),
        (
            ParticipantCommand::DirectExpire(DirectExpire {
                authority: Authority {
                    owner_term: 9,
                    ..authority
                },
                cutoff_unix_ms: 250,
                retention_ms: 50,
                max_items: 1,
            }),
            Fault::StaleAuthority,
        ),
    ] {
        assert_eq!(a.submit(command).await, ParticipantReply::Rejected(fault));
    }
    assert_eq!(a.dump().await, before);
    // Two pages: records at 100 and 200 go, oldest first; 300 stays.
    for (deleted, complete) in [(1, false), (1, true), (0, true)] {
        let reply = ParticipantReply::DirectExpired { deleted, complete };
        assert_eq!(a.submit(expire(250, 1)).await, reply);
        assert_eq!(b.submit(expire(250, 1)).await, reply);
    }
    let state = a.dump().await;
    assert_eq!(state, b.dump().await, "replicas stay identical");
    assert_eq!(count_tag(&state, DEDUP), 1);
    assert_eq!(count_tag(&state, EXPIRY), 1);
    // The kept record still replays; an expired one no longer does.
    let mut kept = commits[2].clone();
    kept.commit_ts = ts(50);
    assert_eq!(a.direct(&kept).await, replayed(&commits[2]));
    let mut expired = commits[0].clone();
    expired.commit_ts = ts(51);
    assert_eq!(a.direct(&expired).await, rejected(Fault::Conflict));
    assert_eq!(expiry_cutoff(1_000, 400), Some(600));
    assert_eq!(expiry_cutoff(100, 400), None);
}

// ---------------------------------------------------------------------------
// RangeClock (D06 store half, O09)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn d06_registration_requires_leadership_and_an_applied_entry_of_the_term() {
    let f = Fixture::new().await;
    let clock = f.machine().range_clock().clone();
    assert_eq!(
        clock.leader_token(),
        None,
        "a clock that never led refuses commits"
    );
    // The fixture applied term-1 entries. Leading term 2 holds until an
    // entry of term 2 applies; a blank entry reaches the clock through the
    // executor's Raft metrics, not the participant machine.
    clock.lead(2);
    assert!(!clock.serving());
    assert_eq!(
        register(&clock, 10, vec![f.key(1)]),
        Err(ClockRejection::NotServing)
    );
    let read = nabedra_rt::task::spawn({
        let machine = f.machine().clone();
        let key = f.key(1);
        async move { machine.read_at(ts(5), key).await.map(drop) }
    });
    nabedra_rt::time::sleep(Duration::from_millis(50)).await;
    assert!(!read.is_finished(), "a held leader serves no snapshot read");
    clock.observe_applied(AppliedPosition {
        term: 2,
        node_id: 1,
        index: 9,
    });
    read.await.unwrap().unwrap();
    assert!(clock.serving());
    assert_eq!(register(&clock, 10, vec![f.key(1)]), Ok(()));
    assert_eq!(
        register(&clock, 10, vec![f.key(1)]),
        Err(ClockRejection::Duplicate)
    );
    let token = clock.leader_token().unwrap();
    assert_eq!(token.term(), 2);
    clock.step_down();
    assert_eq!(clock.pending_len(), 0);
    assert_eq!(clock.leader_token(), None);
    assert_eq!(
        clock.register_pending(token, ts(11), vec![f.key(1)]),
        Err(ClockRejection::NotServing)
    );
}

#[tokio::test]
async fn d06_a_commit_cannot_land_at_or_below_an_already_served_snapshot() {
    let mut f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    assert_eq!(f.version(20, 1).await, None);
    assert_eq!(clock.max_read_ts(), Some(ts(20)));
    for commit_ts in [19, 20] {
        assert!(clock.needs_new_timestamp(ts(commit_ts)));
        assert_eq!(
            register(&clock, commit_ts, vec![f.key(1)]),
            Err(ClockRejection::StaleTimestamp {
                max_read_ts: ts(20)
            })
        );
    }
    assert!(!clock.needs_new_timestamp(ts(21)));
    register(&clock, 21, vec![f.key(1)]).unwrap();
    assert_eq!(f.direct(&f.put(1, None, 21)).await, applied(21));
    assert_eq!(clock.pending_len(), 0, "apply settles the pending entry");
    assert_eq!(
        f.version(20, 1).await,
        None,
        "the served snapshot is unchanged"
    );
    assert_eq!(f.version(21, 1).await, Some(21));
}

#[tokio::test]
async fn d06_t3_a_window_value_below_the_gc_safe_point_is_safe() {
    // `AdvanceFloor` may move the Timestamp GC safe point (here 60) past
    // window values still reserved or in flight (here 45). Snapshot reads
    // are never below the safe point, and Data participants delete no
    // version for it, so such a commit is safe exactly when rule 1 holds.
    let mut f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    let safe_point = 60;
    // Nothing read this range yet: the low commit lands, and every snapshot
    // a reader may still take (at or above the safe point) sees it.
    register(&clock, 45, vec![f.key(1)]).unwrap();
    assert_eq!(f.direct(&f.put(1, None, 45)).await, applied(45));
    assert_eq!(f.version(safe_point, 1).await, Some(45));
    // A snapshot at the safe point already read the range: another value
    // from that old window is refused and must be re-allocated.
    assert_eq!(f.version(safe_point, 2).await, None);
    assert!(matches!(
        register(&clock, 46, vec![f.key(2)]),
        Err(ClockRejection::StaleTimestamp { .. })
    ));
    register(&clock, 61, vec![f.key(2)]).unwrap();
    assert_eq!(f.direct(&f.put(2, None, 61)).await, applied(61));
    assert_eq!(f.version(safe_point, 2).await, None);
    assert_eq!(f.version(61, 2).await, Some(61));
}

#[tokio::test]
async fn d06_o07_reads_and_scans_wait_for_pending_commits_at_or_below_their_snapshot() {
    let mut f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    register(&clock, 15, vec![f.key(1)]).unwrap();
    register(&clock, 25, vec![f.key(2)]).unwrap();
    let scan = nabedra_rt::task::spawn({
        let machine = f.machine().clone();
        let table = f.table;
        async move { scan_versions(&machine, table, 20).await }
    });
    let above = nabedra_rt::task::spawn({
        let machine = f.machine().clone();
        let key = f.key(2);
        async move { machine.read_at(ts(10), key).await.map(drop) }
    });
    nabedra_rt::time::sleep(Duration::from_millis(50)).await;
    above.await.unwrap().unwrap();
    assert!(
        !scan.is_finished(),
        "the scan at 20 waits for the commit at 15"
    );
    assert_eq!(f.direct(&f.put(1, None, 15)).await, applied(15));
    let seen = scan.await.unwrap();
    assert_eq!(seen, BTreeMap::from([(1, 15)]));
    // A latest-version read waits only for pending commits on its own key.
    let latest = nabedra_rt::task::spawn({
        let clock = clock.clone();
        let key = f.key(2);
        async move { clock.wait_latest(&key).await }
    });
    clock.wait_latest(&f.key(1)).await.unwrap();
    nabedra_rt::time::sleep(Duration::from_millis(20)).await;
    assert!(!latest.is_finished());
    // A rejected entry settles its pending timestamp too.
    assert_eq!(
        f.direct(&f.put(2, Some(3), 25)).await,
        rejected(Fault::Conflict)
    );
    latest.await.unwrap().unwrap();
    assert_eq!(clock.pending_len(), 0);
    assert_eq!(
        scan_versions(f.machine(), f.table, 30).await,
        BTreeMap::from([(1, 15)])
    );
}

#[cfg(test)]
async fn scan_versions(machine: &ParticipantMachine, table: Uuid, s: u64) -> BTreeMap<i64, u64> {
    use super::super::scan::{ScanDomain, ScanPage, ScanRequest};
    let mut seen = BTreeMap::new();
    let mut after = None;
    loop {
        let page = machine
            .scan_page(ScanRequest {
                domain: ScanDomain::Rows { table_id: table },
                read_ts: ts(s),
                after: after.clone(),
                after_owner: None,
                lower: None,
                upper: None,
                max_items: 128,
                max_bytes: crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES,
            })
            .await
            .unwrap();
        let ScanPage::Records { records, exhausted } = page.payload().unwrap() else {
            panic!("scan blocked")
        };
        for record in records.iter() {
            let MvccKey::Row {
                primary_key: Key::Int64(key),
                ..
            } = record.key
            else {
                panic!("unexpected scan key")
            };
            if let Some(version) = record.version {
                assert!(version.get() <= s, "scan at {s} saw {version:?}");
                if record.value.is_some() {
                    seen.insert(key, version.get());
                }
            }
            after = Some(record.key.clone());
        }
        if *exhausted {
            return seen;
        }
    }
}

/// Seeded concurrent writers and scanners share one timestamp source, as
/// direct and queued callers share the Timestamp authority. Every scan at `s`
/// must return exactly the commits at or below `s`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d06_o09_concurrent_scans_see_exactly_the_commits_at_or_below_their_snapshot() {
    let f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    let source = Arc::new(AtomicU64::new(100));
    let next_index = Arc::new(tokio::sync::Mutex::new(f.replica.index));
    let mut writers = Vec::new();
    for writer in 0..4i64 {
        let machine = f.machine().clone();
        let shared = f.shared().clone();
        let source = source.clone();
        let clock = clock.clone();
        let next_index = next_index.clone();
        let template = f.put(0, None, 1);
        let key_of = f.key(0);
        writers.push(nabedra_rt::task::spawn(async move {
            let mut rng = 0x9e37_79b9_u64.wrapping_mul(writer as u64 + 1);
            let mut committed = Vec::new();
            let mut stale = 0;
            for n in 0..20 {
                let key = writer * 1000 + n;
                let MvccKey::Row { table_id, .. } = &key_of else {
                    unreachable!()
                };
                let row = MvccKey::Row {
                    table_id: *table_id,
                    primary_key: Key::Int64(key),
                };
                let commit_ts = loop {
                    let commit_ts = source.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    nabedra_rt::time::sleep(Duration::from_micros(rng % 2000)).await;
                    match register(&clock, commit_ts, vec![row.clone()]) {
                        Ok(()) => break commit_ts,
                        Err(ClockRejection::StaleTimestamp { .. }) => stale += 1,
                        Err(other) => panic!("{other:?}"),
                    }
                };
                let mut commit = template.clone();
                commit.request_id = nabedra_rt::ids::uuid_v4();
                commit.commit_ts = ts(commit_ts);
                commit.writes[0].key = row;
                let mut index = next_index.lock().await;
                let reply = apply_at(
                    &machine,
                    &shared,
                    *index,
                    ParticipantCommand::Direct(commit),
                )
                .await;
                *index += 1;
                drop(index);
                assert_eq!(reply, applied(commit_ts));
                committed.push((key, commit_ts));
            }
            (committed, stale)
        }));
    }
    let mut readers = Vec::new();
    for reader in 0..2u64 {
        let machine = f.machine().clone();
        let source = source.clone();
        let table = f.table;
        readers.push(nabedra_rt::task::spawn(async move {
            let mut rng = 0x2545_f491_u64.wrapping_mul(reader + 7);
            let mut scans = Vec::new();
            for _ in 0..30 {
                let s = source.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                nabedra_rt::time::sleep(Duration::from_micros(rng % 1500)).await;
                scans.push((s, scan_versions(&machine, table, s).await));
            }
            scans
        }));
    }
    let mut committed = BTreeMap::new();
    let mut stale = 0;
    for writer in writers {
        let (rows, retries) = writer.await.unwrap();
        stale += retries;
        committed.extend(rows);
    }
    assert_eq!(committed.len(), 80);
    let mut scans = 0;
    for reader in readers {
        for (s, seen) in reader.await.unwrap() {
            let expected: BTreeMap<i64, u64> = committed
                .iter()
                .filter(|(_, commit_ts)| **commit_ts <= s)
                .map(|(key, commit_ts)| (*key, *commit_ts))
                .collect();
            assert_eq!(seen, expected, "scan at {s}");
            scans += 1;
        }
    }
    assert_eq!(scans, 60);
    eprintln!("d06 concurrent: 80 commits, 60 scans, {stale} stale-timestamp retries");
}

// ---------------------------------------------------------------------------
// Mixed versions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn o02_direct_variants_use_new_tags_that_an_older_decoder_rejects() {
    let f = Fixture::new().await;
    let command = serde_json::to_value(ParticipantCommand::Direct(f.put(1, None, 10))).unwrap();
    assert!(command.get("direct").is_some());
    let expire = serde_json::to_value(ParticipantCommand::DirectExpire(DirectExpire {
        authority: f.authority,
        cutoff_unix_ms: 1,
        retention_ms: 1,
        max_items: 1,
    }))
    .unwrap();
    assert!(expire.get("direct_expire").is_some());
    for (command, tag) in [
        (
            ParticipantCommand::DirectForward(f.forward_of(1, nabedra_rt::ids::uuid_v4(), 1)),
            "direct_forward",
        ),
        (
            ParticipantCommand::DirectPause(DirectPause {
                authority: f.authority,
                split_id: nabedra_rt::ids::uuid_v4(),
                paused: true,
            }),
            "direct_pause",
        ),
        (
            ParticipantCommand::ChildPublished(ChildPublished {
                authority: f.authority,
                published_at: ts(1),
            }),
            "child_published",
        ),
    ] {
        assert!(command.is_direct());
        assert!(serde_json::to_value(command).unwrap().get(tag).is_some());
    }
    // The participant command decoder rejects a variant it does not know,
    // which is how an older replica fails on these tags.
    let unknown = serde_json::json!({"direct_v2": {}});
    assert!(serde_json::from_value::<ParticipantCommand>(unknown).is_err());
    // Nested in a scoped envelope, a direct command is refused at apply.
    let mut f = f;
    let before = f.dump().await;
    let scoped = ParticipantCommand::Scoped {
        scope: nabedra_oltp_types::authority::ScopedCatalogRead {
            queue_group_id: nabedra_rt::ids::uuid_v4(),
            data_generation: nabedra_rt::ids::uuid_v4(),
            queue_generation: nabedra_rt::ids::uuid_v4(),
            queue_view: nabedra_oltp_types::types::AuthorityView {
                run_id: f.authority.run_id,
                owner_term: f.authority.owner_term,
                membership_revision: 1,
            },
            request: RequestBinding {
                run_id: f.authority.run_id,
                request_id: nabedra_rt::ids::uuid_v4(),
                digest: digest(1),
                accepted_context: digest(2),
            },
            table_id: f.table,
            read_ts: ts(9),
        },
        command: OwnedCommand::new_owned(
            ParticipantCommand::Direct(f.put(1, None, 10)),
            f.shared(),
            ByteClass::Data,
        )
        .unwrap(),
    };
    assert_eq!(
        f.submit(scoped).await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
    assert_eq!(f.dump().await, before);
}

// ---------------------------------------------------------------------------
// RV-T4 fixes (T4fix)
// ---------------------------------------------------------------------------

/// F1: a leadership change fails reads that waited on a pending commit,
/// instead of letting them read without it.
#[tokio::test]
async fn d06_o09_a_leadership_change_fails_reads_waiting_on_pending_commits() {
    let f = Fixture::new().await;
    for (round, change) in [(0u64, "step_down"), (1, "lead")] {
        let (pending, s) = (15 + 100 * round, 20 + 100 * round);
        f.lead();
        let clock = f.machine().range_clock().clone();
        register(&clock, pending, vec![f.key(1)]).unwrap();
        let read = nabedra_rt::task::spawn({
            let machine = f.machine().clone();
            let key = f.key(1);
            async move { machine.read_at(ts(s), key).await.map(drop) }
        });
        let scan = nabedra_rt::task::spawn({
            let machine = f.machine().clone();
            let table = f.table;
            async move {
                machine
                    .scan_page(super::super::scan::ScanRequest {
                        domain: super::super::scan::ScanDomain::Rows { table_id: table },
                        read_ts: ts(s),
                        after: None,
                        after_owner: None,
                        lower: None,
                        upper: None,
                        max_items: 16,
                        max_bytes: crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES,
                    })
                    .await
                    .map(drop)
            }
        });
        let latest = nabedra_rt::task::spawn({
            let clock = clock.clone();
            let key = f.key(1);
            async move { clock.wait_latest(&key).await }
        });
        nabedra_rt::time::sleep(Duration::from_millis(50)).await;
        assert!(!read.is_finished() && !scan.is_finished() && !latest.is_finished());
        match change {
            // The commit at 15 may still commit under the next leader.
            "step_down" => clock.step_down(),
            // A newer term on this node drops the older term's pending
            // entries just the same.
            _ => clock.lead(5),
        }
        for (what, result) in [
            ("read", read.await.unwrap()),
            ("scan", scan.await.unwrap()),
            ("latest", latest.await.unwrap()),
        ] {
            let error = result.expect_err(what);
            assert_eq!(
                error.code,
                ErrorCode::StorageUnavailable,
                "{what} after {change}"
            );
        }
        // A read that starts after the change is not affected by it.
        clock.step_down();
        f.machine().read_at(ts(s), f.key(1)).await.unwrap();
    }
}

/// F4: a timestamp fetched under one leadership cannot register under a
/// later one, even on the same node, because another leader may have served
/// a snapshot above it meanwhile.
#[tokio::test]
async fn d06_a_timestamp_fetched_before_a_leadership_change_cannot_register() {
    let f = Fixture::new().await;
    let clock = f.machine().range_clock().clone();
    clock.lead(1);
    let token = clock.leader_token().unwrap();
    // The Timestamp RPC is in flight while leadership moves away and back:
    // another node led term 2, and this node leads term 3.
    clock.step_down();
    clock.lead(3);
    clock.observe_applied(AppliedPosition {
        term: 3,
        node_id: 1,
        index: 50,
    });
    assert!(clock.serving());
    assert_eq!(
        clock.max_read_ts(),
        None,
        "this node never saw the other read"
    );
    assert_eq!(
        clock.register_pending(token, ts(100), vec![f.key(1)]),
        Err(ClockRejection::NotServing)
    );
    // Without a step-down in between, a newer term refuses the token too.
    let token = clock.leader_token().unwrap();
    clock.lead(4);
    clock.observe_applied(AppliedPosition {
        term: 4,
        node_id: 1,
        index: 51,
    });
    assert_eq!(
        clock.register_pending(token, ts(100), vec![f.key(1)]),
        Err(ClockRejection::NotServing)
    );
    // A token taken now, before a new timestamp, registers.
    let token = clock.leader_token().unwrap();
    assert_eq!(token.term(), 4);
    assert_eq!(
        clock.register_pending(token, ts(101), vec![f.key(1)]),
        Ok(())
    );
}

/// F3: a forward marker settles a request id for the queued path, so a
/// later direct attempt answers `Forwarded` and never applies.
#[tokio::test]
async fn d03_a_forward_marker_makes_a_later_direct_attempt_fall_back() {
    let mut f = Fixture::new().await;
    let request = nabedra_rt::ids::uuid_v4();
    assert_eq!(f.lookup(1, request).await, None);
    let forward = f.forward_of(1, request, 3);
    assert_eq!(f.forward(&forward).await, forwarded_reply());
    let state = f.dump().await;
    assert_eq!(count_tag(&state, DEDUP), 1);
    assert_eq!(count_tag(&state, EXPIRY), 1);
    assert_eq!(
        f.lookup(1, request).await,
        Some(DedupLookup::Forwarded { digest: digest(3) })
    );
    // Repeating the forward is idempotent.
    assert_eq!(f.forward(&forward).await, forwarded_reply());
    assert_eq!(f.dump().await, state);
    // The direct attempt of the same request falls back and writes nothing,
    // even with a valid head and timestamp.
    let attempt = f.commit(
        request,
        3,
        10,
        vec![f.write(1, None, Some(serde_json::json!({})))],
    );
    assert_eq!(f.direct(&attempt).await, forwarded_reply());
    assert_eq!(f.dump().await, state);
    // A different body under the same id conflicts on both paths.
    let other = f.commit(request, 4, 10, vec![f.write(1, None, None)]);
    assert_rejected_without_writes(&mut f, &other, Fault::Conflict).await;
    assert_eq!(
        f.forward(&f.forward_of(1, request, 4)).await,
        rejected(Fault::Conflict)
    );
    // The marker is per anchor: another anchor is another request scope.
    let elsewhere = f.commit(request, 3, 10, vec![f.write(2, None, None)]);
    assert_eq!(f.direct(&elsewhere).await, applied(10));
    // A bad forward writes nothing.
    let before = f.dump().await;
    let mut stale = f.forward_of(3, nabedra_rt::ids::uuid_v4(), 1);
    stale.authority.owner_term += 1;
    let mut zero_clock = f.forward_of(3, nabedra_rt::ids::uuid_v4(), 1);
    zero_clock.applied_unix_ms = 0;
    let mut other_table = f.forward_of(3, nabedra_rt::ids::uuid_v4(), 1);
    other_table.table_id = nabedra_rt::ids::uuid_v4();
    for (forward, fault) in [
        (stale, Fault::StaleAuthority),
        (zero_clock, Fault::InvalidBinding),
        (other_table, Fault::InvalidBinding),
    ] {
        assert_eq!(f.forward(&forward).await, rejected(fault));
    }
    assert_eq!(f.dump().await, before);
}

/// F3: a forward after the direct commit returns its result; the lookup
/// reads the committed record behind the barrier.
#[tokio::test]
async fn d03_a_forward_after_a_direct_commit_returns_the_committed_result() {
    let mut f = Fixture::with(
        nabedra_rt::ids::uuid_v4(),
        KeySpan {
            start: Some(Key::Int64(0)),
            end: Some(Key::Int64(100)),
        },
    )
    .await;
    let commit = f.put(1, None, 10);
    assert_eq!(f.direct(&commit).await, applied(10));
    let Some(DedupLookup::Committed {
        digest: found,
        commit_ts,
        result,
    }) = f.lookup(1, commit.request_id).await
    else {
        panic!("committed record not found")
    };
    assert_eq!((found, commit_ts), (commit.digest, ts(10)));
    assert_eq!(result.get(), commit.result.as_str());
    let before = f.dump().await;
    assert_eq!(
        f.forward(&f.forward_of(1, commit.request_id, 1)).await,
        replayed(&commit)
    );
    assert_eq!(
        f.forward(&f.forward_of(1, commit.request_id, 2)).await,
        rejected(Fault::Conflict)
    );
    assert_eq!(f.dump().await, before, "no marker over a committed record");
    // Anchors outside the range, or of another table, are not answered.
    assert!(
        f.machine()
            .direct_dedup(f.table, Key::Int64(100), commit.request_id)
            .await
            .is_err()
    );
    assert!(
        f.machine()
            .direct_dedup(nabedra_rt::ids::uuid_v4(), Key::Int64(1), commit.request_id)
            .await
            .is_err()
    );
    assert_eq!(
        f.forward(&f.forward_of(100, nabedra_rt::ids::uuid_v4(), 1)).await,
        rejected(Fault::InvalidBinding)
    );
}

/// The two split bindings of the fixture's full span at `cut`.
#[cfg(test)]
fn split_bindings(
    f: &Fixture,
    split_id: Uuid,
    cut: u64,
) -> [super::super::lineage::SplitBinding; 2] {
    let txn = TxnId {
        request_id: split_id,
        attempt: 1,
    };
    let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
        lifecycle::LifecycleBinding {
            guard_id: split_id,
            catalog_group_id: f.catalog_group_id,
            table_id: f.table,
            lifecycle_version: Some(ts(1)),
            schema_epoch: 1,
            txn,
            request: RequestBinding {
                run_id: f.authority.run_id,
                request_id: split_id,
                digest: digest(1),
                accepted_context: digest(2),
            },
            // A binding's cut is above its guard's snapshot.
            read_ts: ts(2),
            authority: f.authority,
        },
        lifecycle::GuardMode::Exclusive,
    )
    .into_sealed();
    let spans = [
        KeySpan {
            start: None,
            end: Some(Key::Int64(3)),
        },
        KeySpan {
            start: Some(Key::Int64(3)),
            end: None,
        },
    ];
    [0, 1].map(|i| {
        let mut target = f.group.clone();
        // Deterministic per split, so bindings at two cuts are comparable.
        target.group_id = Uuid::from_u128(split_id.as_u128() ^ (i as u128 + 1));
        target.kind = GroupKind::Data {
            table_id: f.table,
            shard_id: Uuid::from_u128(split_id.as_u128() ^ (i as u128 + 11)),
        };
        super::super::lineage::SplitBinding {
            split_id,
            source: f.group.clone(),
            source_schema_epoch: 1,
            source_routing_epoch: 1,
            source_span: full_span(),
            target,
            target_span: spans[i].clone(),
            read_ts: ts(cut),
            guard: guard.clone(),
        }
    })
}
#[cfg(test)]
fn pause(f: &Fixture, split_id: Uuid, paused: bool) -> ParticipantCommand {
    ParticipantCommand::DirectPause(DirectPause {
        authority: f.authority,
        split_id,
        paused,
    })
}

/// F5: the replicated barrier lets a split freeze succeed while direct
/// writers keep committing, and a new leader (another replica applying the
/// same log) keeps refusing direct commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o16_d06_a_split_barrier_lets_the_freeze_succeed_under_steady_direct_load() {
    let f = Fixture::new().await;
    let mut other = f.replica_of().await;
    let source = Arc::new(AtomicU64::new(100));
    // One log: every entry is applied on both replicas in the same order.
    let log = Arc::new(tokio::sync::Mutex::new((
        f.replica.index,
        Vec::<ParticipantCommand>::new(),
    )));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut writers = Vec::new();
    for writer in 0..3i64 {
        let machine = f.machine().clone();
        let shared = f.shared().clone();
        let source = source.clone();
        let log = log.clone();
        let stop = stop.clone();
        let template = f.put(0, None, 1);
        let row_of = f.key(0);
        writers.push(nabedra_rt::task::spawn(async move {
            let (mut applied_count, mut refused) = (0u64, 0u64);
            let mut n = 0i64;
            while !stop.load(AtomicOrdering::SeqCst) {
                n += 1;
                let MvccKey::Row { table_id, .. } = &row_of else {
                    unreachable!()
                };
                let mut commit = template.clone();
                commit.request_id = nabedra_rt::ids::uuid_v4();
                // The timestamp is fetched before the entry is appended.
                commit.commit_ts = ts(source.fetch_add(1, AtomicOrdering::SeqCst) + 1);
                commit.writes[0].key = MvccKey::Row {
                    table_id: *table_id,
                    primary_key: Key::Int64(writer * 100_000 + n),
                };
                nabedra_rt::time::sleep(Duration::from_micros(200)).await;
                let mut log = log.lock().await;
                let command = ParticipantCommand::Direct(commit);
                let reply = apply_at(&machine, &shared, log.0, command.clone()).await;
                log.0 += 1;
                log.1.push(command);
                drop(log);
                match reply {
                    ParticipantReply::Direct(DirectReply::Applied { .. }) => applied_count += 1,
                    ParticipantReply::Direct(DirectReply::Rejected(Fault::SplitInProgress)) => {
                        refused += 1
                    }
                    other => panic!("{other:?}"),
                }
                tokio::task::yield_now().await;
            }
            (applied_count, refused)
        }));
    }
    let submit = |command: ParticipantCommand| {
        let machine = f.machine().clone();
        let shared = f.shared().clone();
        let log = log.clone();
        async move {
            let mut log = log.lock().await;
            let reply = apply_at(&machine, &shared, log.0, command.clone()).await;
            log.0 += 1;
            log.1.push(command);
            reply
        }
    };
    nabedra_rt::time::sleep(Duration::from_millis(100)).await;
    // Without the barrier, a cut allocated before more commits apply fails.
    let early = split_bindings(&f, nabedra_rt::ids::uuid_v4(), source.load(AtomicOrdering::SeqCst));
    // At most one commit per writer took its timestamp before the cut was read,
    // so once four more entries apply at least one of them is above the cut.
    // Wait for that instead of sleeping, which fails on a loaded host.
    let start = log.lock().await.0;
    while log.lock().await.0 < start + 4 {
        nabedra_rt::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        submit(ParticipantCommand::SplitFreeze {
            authority: f.authority,
            children: Box::new(early),
        })
        .await,
        ParticipantReply::Rejected(Fault::DirectCommitAboveCut)
    );
    // The coordinator: barrier, then the cut, then the freeze.
    let split_id = nabedra_rt::ids::uuid_v4();
    assert_eq!(
        submit(pause(&f, split_id, true)).await,
        ParticipantReply::DirectPaused {
            split_id,
            paused: true
        }
    );
    // Writers keep going (and are refused) while the cut is fetched.
    nabedra_rt::time::sleep(Duration::from_millis(50)).await;
    let cut = source.fetch_add(1, AtomicOrdering::SeqCst) + 1;
    nabedra_rt::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(
        submit(ParticipantCommand::SplitFreeze {
            authority: f.authority,
            children: Box::new(split_bindings(&f, split_id, cut)),
        })
        .await,
        ParticipantReply::SplitSourceFrozen(_)
    ));
    nabedra_rt::time::sleep(Duration::from_millis(20)).await;
    stop.store(true, AtomicOrdering::SeqCst);
    let (mut applied_total, mut refused_total) = (0, 0);
    for writer in writers {
        let (applied_count, refused) = writer.await.unwrap();
        applied_total += applied_count;
        refused_total += refused;
    }
    assert!(
        applied_total > 0 && refused_total > 0,
        "{applied_total} {refused_total}"
    );
    eprintln!("o16 barrier: {applied_total} applied, {refused_total} refused by the barrier");

    // Another replica that applies the same log, as a new leader would,
    // holds the same barrier state and refuses the same commits.
    let (_, commands) = std::mem::take(&mut *log.lock().await);
    let mut replies = Vec::new();
    for command in commands {
        replies.push(other.submit(command).await);
    }
    assert_eq!(other.dump().await, f.dump().await);
    assert!(replies.contains(&rejected(Fault::SplitInProgress)));
}

/// F5: barrier ownership. Only its split can release it, a freeze of the
/// same split takes it over, and it survives a fence.
#[tokio::test]
async fn o16_the_split_barrier_belongs_to_one_split() {
    let mut f = Fixture::new().await;
    let (a, b) = (nabedra_rt::ids::uuid_v4(), nabedra_rt::ids::uuid_v4());
    let paused = |split_id, paused| ParticipantReply::DirectPaused { split_id, paused };
    // Releasing a barrier nobody holds is a no-op.
    assert_eq!(f.submit(pause(&f, a, false)).await, paused(a, false));
    assert_eq!(f.submit(pause(&f, a, true)).await, paused(a, true));
    assert_eq!(f.submit(pause(&f, a, true)).await, paused(a, true));
    let state = f.dump().await;
    for command in [pause(&f, b, true), pause(&f, b, false)] {
        assert_eq!(
            f.submit(command).await,
            ParticipantReply::Rejected(Fault::Conflict)
        );
    }
    assert_eq!(
        f.submit(ParticipantCommand::SplitFreeze {
            authority: f.authority,
            children: Box::new(split_bindings(&f, b, 50)),
        })
        .await,
        ParticipantReply::Rejected(Fault::Conflict)
    );
    let mut stale = DirectPause {
        authority: f.authority,
        split_id: a,
        paused: false,
    };
    stale.authority.owner_term += 1;
    assert_eq!(
        f.submit(ParticipantCommand::DirectPause(stale)).await,
        ParticipantReply::Rejected(Fault::StaleAuthority)
    );
    assert_eq!(f.dump().await, state);
    // Direct commits are refused while it is held; forwards still work,
    // because the freeze that follows copies the markers.
    let blocked = f.put(1, None, 10);
    assert_rejected_without_writes(&mut f, &blocked, Fault::SplitInProgress).await;
    assert_eq!(
        f.forward(&f.forward_of(1, nabedra_rt::ids::uuid_v4(), 1)).await,
        forwarded_reply()
    );
    // A fence (new run) keeps the barrier.
    let next = Authority {
        run_id: f.authority.run_id,
        owner_term: 2,
    };
    assert!(matches!(
        f.submit(ParticipantCommand::Fence {
            transition_id: nabedra_rt::ids::uuid_v4(),
            expected: Some(f.authority),
            next,
            schema_epoch: 1,
            routing_epoch: 1,
            span: full_span(),
            catalog_group_id: f.catalog_group_id,
        })
        .await,
        ParticipantReply::Fenced(_)
    ));
    f.authority = next;
    let blocked = f.put(1, None, 11);
    assert_rejected_without_writes(&mut f, &blocked, Fault::SplitInProgress).await;
    // The holder releases it; commits resume.
    assert_eq!(f.submit(pause(&f, a, false)).await, paused(a, false));
    assert_eq!(f.direct(&f.put(1, None, 12)).await, applied(12));
    // Taken again, the freeze of the same split takes it over.
    assert_eq!(f.submit(pause(&f, a, true)).await, paused(a, true));
    assert!(matches!(
        f.submit(ParticipantCommand::SplitFreeze {
            authority: f.authority,
            children: Box::new(split_bindings(&f, a, 50)),
        })
        .await,
        ParticipantReply::SplitSourceFrozen(_)
    ));
    assert_eq!(f.submit(pause(&f, a, true)).await, paused(a, true));
    assert_eq!(
        f.submit(pause(&f, b, true)).await,
        ParticipantReply::Rejected(Fault::Conflict)
    );
    let blocked = f.put(2, None, 60);
    assert_rejected_without_writes(&mut f, &blocked, Fault::SplitInProgress).await;
}

/// F7: stamps never go below the group high-water, and an expire cutoff
/// never goes above `high-water - retention`, so neither a clock behind
/// nor a clock ahead deletes a record inside the retention window.
#[tokio::test]
async fn d03_dedup_expiry_is_clamped_to_the_group_high_water() {
    let mut f = Fixture::new().await;
    let mut commits = Vec::new();
    for (key, unix_ms) in [(1, 1_000u64), (2, 2_000), (3, 500)] {
        let mut commit = f.put(key, None, 10 + key as u64);
        commit.applied_unix_ms = unix_ms;
        assert_eq!(f.direct(&commit).await, applied(10 + key as u64));
        commits.push(commit);
    }
    // A clock of 0 is refused.
    let mut zero = f.put(4, None, 20);
    zero.applied_unix_ms = 0;
    assert_rejected_without_writes(&mut f, &zero, Fault::InvalidBinding).await;
    let authority = f.authority;
    let expire = move |cutoff_unix_ms, retention_ms| {
        ParticipantCommand::DirectExpire(DirectExpire {
            authority,
            cutoff_unix_ms,
            retention_ms,
            max_items: MAX_DIRECT_EXPIRE_ITEMS,
        })
    };
    // A leader whose clock runs far ahead proposes a cutoff that would
    // delete everything; the group's own high-water (2000) bounds it to
    // 2000 - 900 = 1100, so only the record stamped 1000 goes.
    assert_eq!(
        f.submit(expire(1_000_000, 900)).await,
        ParticipantReply::DirectExpired {
            deleted: 1,
            complete: true
        }
    );
    // The commit proposed with a clock behind (500) was stamped 2000, so a
    // cutoff of 1500 (inside the window of the 2000 records) keeps it.
    assert_eq!(
        f.submit(expire(1_500, 400)).await,
        ParticipantReply::DirectExpired {
            deleted: 0,
            complete: true
        }
    );
    let state = f.dump().await;
    assert_eq!(count_tag(&state, DEDUP), 2);
    for commit in &commits[1..] {
        let retry = DirectCommit {
            commit_ts: ts(40),
            ..commit.clone()
        };
        assert_eq!(f.direct(&retry).await, replayed(commit));
    }
    // Once the window has passed for the high-water itself, both go.
    assert_eq!(
        f.submit(expire(1_000_000, 1)).await,
        ParticipantReply::DirectExpired {
            deleted: 0,
            complete: true
        },
        "records inside the window of the high-water are kept"
    );
    let mut later = f.put(9, None, 50);
    later.applied_unix_ms = 10_000;
    assert_eq!(f.direct(&later).await, applied(50));
    assert_eq!(
        f.submit(expire(1_000_000, 1_000)).await,
        ParticipantReply::DirectExpired {
            deleted: 2,
            complete: true
        }
    );
}

/// F9: a well-formed command over a size limit answers `ResourceLimit`, so
/// the executor uses the queued path instead of failing the request.
#[tokio::test]
async fn o07_oversized_direct_commands_answer_resource_limit() {
    let mut f = Fixture::new().await;
    let rows: Vec<DirectWrite> = (0..=MAX_DIRECT_WRITES as i64)
        .map(|key| f.write(key, None, None))
        .collect();
    let too_many_rows = f.commit(nabedra_rt::ids::uuid_v4(), 1, 10, rows);
    let mut wide = f.write(1, None, None);
    wide.index = (0..=MAX_DIRECT_INDEX_CHANGES as i64)
        .map(|value| IndexChange {
            key: MvccKey::IndexEntry {
                table_id: f.table,
                index_id: nabedra_rt::ids::uuid_v4(),
                values: Page::new(vec![IndexValue::Int64(value)]).unwrap(),
                primary_key: Key::Int64(1),
            },
            expected_version: None,
            value: None,
        })
        .collect();
    let too_many_index_changes = f.commit(nabedra_rt::ids::uuid_v4(), 1, 10, vec![wide]);
    let mut large_result = f.put(1, None, 10);
    large_result.result = f.json(serde_json::json!("x".repeat(MAX_DIRECT_RESULT_BYTES)));
    for command in [too_many_rows, too_many_index_changes, large_result] {
        assert_rejected_without_writes(&mut f, &command, Fault::ResourceLimit).await;
    }
    // A malformed command is still `InvalidBinding`, even when also large.
    let mut both = f.commit(
        nabedra_rt::ids::uuid_v4(),
        1,
        10,
        (0..=MAX_DIRECT_WRITES as i64)
            .rev()
            .map(|key| f.write(key, None, None))
            .collect(),
    );
    both.request_id = Uuid::nil();
    assert_rejected_without_writes(&mut f, &both, Fault::InvalidBinding).await;
}

/// F11: a split child refuses staging when a stray dedup record or forward
/// marker exists, rather than turning a later import into recovery_unknown.
#[tokio::test]
async fn o16_a_split_child_with_a_stray_dedup_record_refuses_staging() {
    let f = Fixture::new().await;
    let [left, _] = split_bindings(&f, nabedra_rt::ids::uuid_v4(), 30);
    let mut child = Replica {
        machine: ParticipantMachine::new(f.pool.group(left.target.clone()).await.unwrap()).unwrap(),
        shared: f.shared().clone(),
        index: 0,
    };
    assert!(matches!(
        child.submit(f.fence(1, 2, left.target_span.clone())).await,
        ParticipantReply::Fenced(_)
    ));
    assert!(matches!(
        child.submit(f.activation()).await,
        ParticipantReply::DirectSchema(_)
    ));
    // The stray record: a forward marker for a key in the child's span.
    let mut stray = f.forward_of(1, nabedra_rt::ids::uuid_v4(), 1);
    stray.anchor = Key::Int64(1);
    assert_eq!(
        child.submit(ParticipantCommand::DirectForward(stray)).await,
        forwarded_reply()
    );
    let before = child.dump().await;
    assert_eq!(
        child
            .submit(ParticipantCommand::SplitStage {
                authority: f.authority,
                binding: Box::new(left),
            })
            .await,
        ParticipantReply::Rejected(Fault::Conflict)
    );
    assert_eq!(child.dump().await, before);
}

/// F10: a latest read tracks pending writers per key. It waits until every
/// pending commit that writes its key settles, ignores commits on other keys,
/// and the per-key counts do not leak after settle, `lead` or `step_down`.
#[tokio::test]
async fn d06_latest_reads_track_pending_writers_per_key() {
    let f = Fixture::new().await;
    f.lead();
    let clock = f.machine().range_clock().clone();
    register(&clock, 15, vec![f.key(1), f.key(2)]).unwrap();
    register(&clock, 16, vec![f.key(1)]).unwrap();
    register(&clock, 17, vec![f.key(3)]).unwrap();
    assert_eq!(clock.pending_key_count(), 3);
    let latest = nabedra_rt::task::spawn({
        let clock = clock.clone();
        let key = f.key(1);
        async move { clock.wait_latest(&key).await }
    });
    // Other keys, and keys nobody writes, do not wait on key 1's writers.
    clock.wait_latest(&f.key(4)).await.unwrap();
    clock.settle(ts(17));
    clock.wait_latest(&f.key(3)).await.unwrap();
    // One of key 1's two writers settles: the read keeps waiting.
    clock.settle(ts(15));
    clock.wait_latest(&f.key(2)).await.unwrap();
    nabedra_rt::time::sleep(Duration::from_millis(20)).await;
    assert!(!latest.is_finished(), "key 1 still has the writer at 16");
    clock.settle(ts(16));
    latest.await.unwrap().unwrap();
    assert_eq!((clock.pending_len(), clock.pending_key_count()), (0, 0));
    // Settling twice is harmless, and clearing on a leadership change drops
    // the counts with the entries.
    clock.settle(ts(16));
    register(&clock, 18, vec![f.key(1)]).unwrap();
    clock.step_down();
    assert_eq!((clock.pending_len(), clock.pending_key_count()), (0, 0));
    f.lead();
    register(&clock, 19, vec![f.key(1)]).unwrap();
    clock.lead(9);
    assert_eq!((clock.pending_len(), clock.pending_key_count()), (0, 0));
}

#[cfg(test)]
mod batch;
#[cfg(test)]
mod t7;
#[cfg(test)]
mod search;
