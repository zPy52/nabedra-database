//! Tables with search columns: row vectors, embedding records, closed
//! timestamps, the change feed and copies (`plans/oltp-vector.md`). Group stores
//! run on `FaultFs`, so histories are fast and crashes are real.
use super::*;
use crate::oltp::mvcc::{
    store::{
        embed::{EmbedFailed, EmbedRecord, RequeueEmbedding, SetVector},
        feed::{ChangePage, EntryChanges, RowChange},
        row::{RowImage, SearchRow},
        search::{Closed, SearchFence, SearchReply},
    },
    vector::{ColumnId, RowSearch, SearchColumn, VectorChange, VectorKind, VectorValue},
};
use nabedra_kv::fs::{FaultFs, Fs};

mod adv;
mod blobs;
mod closed;
mod copies;
mod crash;
mod embed;
mod feed;
mod fence;

/// A dense vector, an embedded dense vector, a sparse and a multi-vector column.
const PLAIN: ColumnId = 1;
const EMBEDDED: ColumnId = 2;
const SPARSE: ColumnId = 3;
const MULTI: ColumnId = 4;

fn column(id: ColumnId, kind: VectorKind, dim: u32) -> SearchColumn {
    SearchColumn {
        id,
        kind,
        dim,
        source: None,
    }
}
fn embedded(id: ColumnId) -> SearchColumn {
    SearchColumn {
        source: Some(Text::try_from("body").unwrap()),
        ..column(id, VectorKind::Dense, 2)
    }
}
fn columns() -> Vec<SearchColumn> {
    vec![
        column(PLAIN, VectorKind::Dense, 2),
        embedded(EMBEDDED),
        column(SPARSE, VectorKind::Sparse, 0),
        column(MULTI, VectorKind::Multi, 1),
    ]
}

/// Seeded and stable, so a failure prints a seed that repeats it.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    /// Any of `n` items, the newest one half of the time.
    fn recent(&mut self, n: usize) -> usize {
        if self.one_in(2) { n - 1 } else { self.below(n) }
    }
    fn one_in(&mut self, n: usize) -> bool {
        self.below(n) == 0
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
}

/// One row as a reader sees it.
#[derive(Debug, Default, PartialEq)]
struct Got {
    version: Option<u64>,
    value: Option<serde_json::Value>,
    /// Column to the version it references and its bytes.
    vectors: BTreeMap<ColumnId, (u64, Vec<u8>)>,
    source: BTreeMap<ColumnId, u64>,
    records: BTreeMap<ColumnId, EmbedRecord>,
}
fn got_image(got: &mut Got, image: &RowImage) {
    for ((column, at), vector) in image.blobs.0.iter().zip(&image.vectors) {
        assert_eq!(*column, vector.column);
        got.vectors
            .insert(*column, (at.get(), vector.value.to_bytes().unwrap()));
    }
    got.source = image.source.0.iter().map(|(c, at)| (*c, at.get())).collect();
    got.records = image
        .records
        .iter()
        .map(|image| (image.column, image.record.clone()))
        .collect();
}

struct S {
    f: Fixture,
    /// The group's direct epoch.
    epoch: u64,
    search_epoch: u64,
    clock: u64,
}
impl S {
    async fn new() -> Self {
        Self::on(Some(&FaultFs::new())).await
    }
    /// A group with `columns()`. A pool on the real file system has journal
    /// segments of production size.
    async fn on(fs: Option<&FaultFs>) -> Self {
        let mut s = Self::plain(fs).await;
        assert!(matches!(
            s.fence(columns()).await,
            SearchReply::Fenced { .. }
        ));
        s
    }
    /// A group whose table has no search columns yet.
    async fn plain(fs: Option<&FaultFs>) -> Self {
        let shared = shared();
        let dir = Dir(std::env::temp_dir().join(format!("search-{}", nabedra_rt::ids::uuid_v4())));
        std::fs::create_dir_all(&dir.0).unwrap();
        let pool_identity = PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: nabedra_rt::ids::uuid_v4(),
            worker_id: nabedra_rt::ids::uuid_v4(),
            pool_id: nabedra_rt::ids::uuid_v4(),
        };
        let pool = match fs {
            Some(fs) => {
                fs.create_dir_all_durable(&dir.0);
                open_on(&dir, fs, &pool_identity, &shared)
            }
            None => OltpPool::open_development(&dir.0, pool_identity.clone(), shared.clone()).unwrap(),
        };
        let table = nabedra_rt::ids::uuid_v4();
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
            span: full_span(),
            catalog_group_id,
        };
        let mut f = Fixture {
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
        assert!(matches!(f.submit(first_fence).await, ParticipantReply::Fenced(_)));
        let activation = f.activation();
        f.submit(activation).await;
        Self {
            f,
            epoch: 1,
            search_epoch: 0,
            clock: 100,
        }
    }
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }
    /// A fence of `epoch` that expects the epoch below it (L19's compare).
    fn fence_command(&self, epoch: u64, columns: Vec<SearchColumn>) -> ParticipantCommand {
        self.fence_from(epoch.saturating_sub(1), epoch, columns)
    }
    /// A fence of `epoch` that applies only to a group at `expected`.
    fn fence_from(&self, expected: u64, epoch: u64, columns: Vec<SearchColumn>) -> ParticipantCommand {
        ParticipantCommand::SearchFence(SearchFence {
            authority: self.f.authority,
            table_id: self.f.table,
            epoch,
            expected_epoch: Some(expected),
            lifecycle: Some(self.f.table),
            columns,
            serving: Vec::new(),
            text: Vec::new(),
        })
    }
    async fn fence(&mut self, columns: Vec<SearchColumn>) -> SearchReply {
        let command = self.fence_from(self.search_epoch, self.search_epoch + 1, columns);
        let reply = search_reply(self.f.submit(command).await);
        if let SearchReply::Fenced { epoch, direct } = &reply {
            self.f.search = true;
            self.search_epoch = *epoch;
            self.epoch = direct.unwrap().epoch;
        }
        reply
    }
    fn value(&self, kind: VectorKind, bytes: &[u8]) -> VectorValue {
        VectorValue::new(self.f.shared(), kind, bytes).unwrap()
    }
    fn dense(&self, n: u8) -> VectorValue {
        self.value(VectorKind::Dense, &[n; 8])
    }
    fn set(&self, column: ColumnId, value: VectorValue) -> VectorChange {
        VectorChange {
            column,
            value: Some(value),
        }
    }
    fn write(
        &self,
        key: i64,
        expected: Option<u64>,
        value: Option<serde_json::Value>,
        vectors: Vec<VectorChange>,
    ) -> DirectWrite {
        DirectWrite {
            search: (self.search_epoch > 0 || !vectors.is_empty()).then(|| {
                Box::new(RowSearch {
                    vectors,
                    backfill: Vec::new(),
                })
            }),
            ..self.f.write(key, expected, value)
        }
    }
    fn commit(&self, commit_ts: u64, writes: Vec<DirectWrite>) -> DirectCommit {
        DirectCommit {
            schema_epoch: self.epoch,
            ..self
                .f
                .commit(nabedra_rt::ids::uuid_v4(), 1, commit_ts, writes)
        }
    }
    async fn direct(&mut self, commit_ts: u64, write: DirectWrite) -> ParticipantReply {
        let commit = self.commit(commit_ts, vec![write]);
        self.f.direct(&commit).await
    }
    fn put_row(
        &self,
        key: i64,
        expected: Option<u64>,
        value: serde_json::Value,
        vectors: Vec<VectorChange>,
    ) -> PreparedWrite {
        PreparedWrite {
            change: Change::PutRow {
                value: self.f.json(value),
                search: Box::new(RowSearch {
                    vectors,
                    backfill: Vec::new(),
                }),
            },
            ..self.f.queued_put(key, expected, serde_json::Value::Null)
        }
    }
    async fn abort(&mut self, p: &QueuedPlan) {
        let proof = SealedStatusDecision::seal(DecisionStatement {
            txn: p.txn,
            request: p.request,
            manifest: p.manifest,
            decided_at: AppliedPosition {
                term: 1,
                node_id: 1,
                index: 1,
            },
            decision: TxnDecision::Abort {
                reason: nabedra_oltp_types::types::AbortReason::Conflict,
            },
        })
        .unwrap();
        let reply = self
            .f
            .submit(ParticipantCommand::Resolve {
                authority: self.f.authority,
                proof,
                max_items: 128,
            })
            .await;
        assert!(matches!(reply, ParticipantReply::Resolved { complete: true, .. }));
    }
    fn set_vector(&self, key: i64, expected: u64, source: u64, commit_ts: u64) -> SetVector {
        SetVector {
            authority: self.f.authority,
            table_id: self.f.table,
            schema_epoch: self.epoch,
            routing_epoch: 1,
            primary_key: Key::Int64(key),
            column: EMBEDDED,
            source_version: ts(source),
            expected_version: ts(expected),
            commit_ts: ts(commit_ts),
            value: self.dense(commit_ts as u8),
        }
    }
    async fn embed(&mut self, set: SetVector) -> SearchReply {
        search_reply(self.f.submit(ParticipantCommand::SetVector(set)).await)
    }
    async fn fail(&mut self, key: i64, source: u64, id: u64, attempt: u32) -> SearchReply {
        let failed = EmbedFailed {
            authority: self.f.authority,
            table_id: self.f.table,
            primary_key: Key::Int64(key),
            column: EMBEDDED,
            source_version: ts(source),
            id: ts(id),
            attempt,
            reason: Text::try_from("provider said no").unwrap(),
        };
        search_reply(self.f.submit(ParticipantCommand::EmbedFailed(failed)).await)
    }
    async fn close(&mut self, at: u64) -> SearchReply {
        let closed = Closed {
            authority: self.f.authority,
            ts: ts(at),
            unix_ms: at * 10,
        };
        search_reply(self.f.submit(ParticipantCommand::Closed(closed)).await)
    }
    async fn at(&self, key: i64, read: u64) -> Got {
        got(&self.f.replica.machine, self.f.key(key), read).await.unwrap()
    }
    async fn get(&self, key: i64) -> Got {
        self.at(key, u64::MAX).await
    }
    /// Every change after `after`, in pages of `entries` entries.
    async fn changes(&self, after: u64, entries: u16) -> Vec<EntryChanges> {
        changes(&self.f.replica.machine, after, entries).await.unwrap()
    }
    /// The index of the last applied entry.
    fn last(&self) -> u64 {
        self.f.replica.index - 1
    }
    /// Changes stored records behind the state machine's back.
    async fn tamper(
        &mut self,
        work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<(), ProtocolError> + Send + 'static,
    ) {
        let index = self.f.replica.index;
        self.f.replica.index += 1;
        self.f
            .machine()
            .store()
            .transaction(
                ApplyMeta {
                    position: position(index),
                    command_digest: digest(99),
                    membership: None,
                },
                BUDGET,
                move |tx| {
                    work(tx)?;
                    Ok(Transition::Commit(tx.copy_output(b"null")?))
                },
            )
            .await
            .unwrap();
    }
    async fn records(&self, tag: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut dump = self.f.dump().await;
        dump.retain(|(key, _)| key.first() == Some(&tag));
        dump
    }
    async fn count(&self, tag: u8) -> usize {
        self.records(tag).await.len()
    }
}
fn position(index: u64) -> AppliedPosition {
    AppliedPosition {
        term: 1,
        node_id: 1,
        index,
    }
}
fn open_on(
    dir: &Dir,
    fs: &FaultFs,
    identity: &PoolIdentity,
    shared: &Arc<WorkerSharedResources>,
) -> OltpPool {
    let fs: Arc<dyn Fs> = Arc::new(fs.clone());
    OltpPool::open_development_on(&dir.0, identity.clone(), shared.clone(), fs).unwrap()
}
fn search_reply(reply: ParticipantReply) -> SearchReply {
    match reply {
        ParticipantReply::Search(reply) => reply,
        other => panic!("not a search reply: {other:?}"),
    }
}
fn refused(fault: Fault) -> SearchReply {
    SearchReply::Rejected(fault)
}
async fn got(machine: &ParticipantMachine, key: MvccKey, read: u64) -> Result<Got, ProtocolError> {
    let row = machine.read_search_row(ts(read), key, 4 << 20).await?;
    let SearchRow::Row {
        version,
        value,
        search,
    } = row.payload().unwrap()
    else {
        panic!("blocked");
    };
    let mut got = Got {
        version: version.map(|at| at.get()),
        value: value
            .as_ref()
            .map(|value| serde_json::from_str(value.as_str()).unwrap()),
        ..Got::default()
    };
    if let Some(search) = search {
        got_image(&mut got, search);
    }
    Ok(got)
}
/// Every change after `after`, or `None` when some are gone.
async fn changes(
    machine: &ParticipantMachine,
    mut after: u64,
    entries: u16,
) -> Option<Vec<EntryChanges>> {
    let mut all: Vec<EntryChanges> = Vec::new();
    loop {
        let page = machine.change_page(after, entries, 8 << 20).await.unwrap();
        match page.payload().unwrap() {
            ChangePage::Changes { entries, through } => {
                assert!(through.index >= after);
                for entry in entries {
                    assert!(entry.at.index > after && entry.at.index <= through.index);
                    after = entry.at.index;
                }
                all.extend(entries.iter().cloned());
                after = through.index;
                if entries.is_empty() {
                    return Some(all);
                }
            }
            ChangePage::Gone { .. } => return None,
            other => panic!("{other:?}"),
        }
    }
}
/// Every key and value this replica stores for `group` outside its log: the
/// applied position, the state and the change records.
fn stored(pool: &OltpPool, group: Uuid) -> Vec<(Vec<u8>, Vec<u8>)> {
    let scope = [&[0x40][..], group.as_bytes()].concat();
    let end = [&scope[..], &[0xff]].concat();
    let snapshot = pool.kv_store().snapshot();
    let mut iterator = snapshot.iter(
        std::ops::Bound::Included(&scope),
        std::ops::Bound::Excluded(&end),
        false,
    );
    iterator.seek(&scope);
    let mut all = Vec::new();
    while iterator.valid() {
        all.push((iterator.key().to_vec(), iterator.value().unwrap().to_vec()));
        iterator.next();
    }
    iterator.status().unwrap();
    all.retain(|(key, _)| key[17] != b'l');
    all
}
/// A change as the model writes it: index, key, commit timestamp and the row.
type Seen = (u64, i64, u64, Option<Got>);
fn seen(entries: &[EntryChanges]) -> Vec<Seen> {
    let mut all = Vec::new();
    for entry in entries {
        for change in &entry.changes {
            match change {
                RowChange::Row {
                    key: MvccKey::Row { primary_key: Key::Int64(key), .. },
                    commit_ts,
                    row,
                } => {
                    let row = row.as_ref().map(|row| {
                        let mut got = Got {
                            version: Some(commit_ts.get()),
                            value: Some(serde_json::from_str(row.value.as_str()).unwrap()),
                            ..Got::default()
                        };
                        if let Some(search) = &row.search {
                            got_image(&mut got, search);
                        }
                        got
                    });
                    all.push((entry.at.index, *key, commit_ts.get(), row));
                }
                RowChange::Closed { ts, .. } => all.push((entry.at.index, i64::MIN, ts.get(), None)),
                other => panic!("{other:?}"),
            }
        }
    }
    all
}
