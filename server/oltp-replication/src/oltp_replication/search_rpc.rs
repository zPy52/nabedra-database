//! Vector search RPCs, protocol `nabedra-search/1` (`plans/oltp-vector.md`,
//! contract `w3/contract-w2.md` sections 2.1 to 2.6).
//!
//! This file holds the shared wire types, the role matrix and the endpoints
//! that check them; it is frozen after the skeleton (S): a field change is a
//! contract change through the lead. The handlers live in the owners' files:
//! [`feed`] (E: `Tail`, `SnapshotOpen/Page/Close`), [`barrier`] (F: `Barrier`,
//! `Close`), [`routes`] (J: `Routes`), [`embed`] (K: `EmbedLease/Set/Fail`,
//! `Requeue`). `SearchQuery` is served by the search worker process (W) under
//! the Control identity and is not bound here.
//!
//! Framing: the transport header is a [`SearchPrologue`] (method name and
//! section lengths, so preflight checks the lane and the role before any
//! payload); section 0 is the JSON [`SearchHeader`] (the request) or the JSON
//! [`SearchReplyFrame`] (the reply); section 1 carries vector bytes, referenced
//! by [`VecRef`] (Q9).
use crate::{
    oltp::mvcc::{
        MvccKey,
        catalog::{CatalogSearchColumn, ShardRoute},
        store::{KeySpan, row::Columns, search::SearchSchema},
        vector::VectorKind,
    },
    oltp_replication::multiplex::{
        AuthenticatedSession, ChargedReply, ChargedSections, Endpoint, EndpointFuture, Purpose,
        ReceivePlan, TrafficClass,
    },
    protocol::{ErrorCode, Key, Outcome, Predicate, ProtocolError, RankOp},
};
use nabedra_oltp_types::types::{
    AppliedPosition, ColumnId, CommitTs, GroupIdentity, GroupKind, Text, WorkerEnrollment,
    WorkerIdentity,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::sync::Arc;
use uuid::Uuid;

mod barrier;
mod embed;
mod feed;
mod routes;
#[cfg(test)]
mod tests;

pub const SEARCH_PROTOCOL: &str = "nabedra-search/1";
pub const SEARCH_VERSION: u16 = 1;
/// Bound of the JSON transport header ([`SearchPrologue`]).
pub const MAX_PROLOGUE_BYTES: usize = 1024;

// ---- 2.2 common header, framing ----

/// The transport header of every search call: enough for preflight to check
/// the version, lane and role and to plan the two sections.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchPrologue {
    pub version: u16,
    pub method: MethodName,
    /// Section 0: the JSON `SearchHeader`.
    pub body_bytes: u32,
    /// Section 1: vector bytes, 0 when the request has none.
    pub vector_bytes: u32,
}
/// Section 0 of a request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchHeader {
    pub version: u16,
    pub call: CallId,
    pub deadline_ms: u32,
    pub method: SearchMethod,
}
impl SearchHeader {
    /// Decodes section 0 and checks it against the prologue preflight saw.
    pub fn decode(prologue: &SearchPrologue, body: &[u8]) -> Result<Self, ProtocolError> {
        if body.len() != prologue.body_bytes as usize {
            return Err(invalid());
        }
        let header: Self = serde_json::from_slice(body).map_err(|_| invalid())?;
        if header.version != SEARCH_VERSION || header.method.name() != prologue.method {
            return Err(invalid());
        }
        Ok(header)
    }
}
/// `process` is fixed per process start, `seq` grows per call; replies echo it,
/// and a reply to an older call is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallId {
    pub process: Uuid,
    pub seq: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchMethod {
    // SearchFeed, Data voters
    Tail(TailRequest),
    SnapshotOpen(SnapshotOpen),
    SnapshotPage(SnapshotPageRequest),
    SnapshotClose(SnapshotClose),
    Barrier(BarrierRequest),
    Close(CloseInterest),
    // SearchFeed, Catalog voters
    Routes(RoutesRequest),
    // SearchQuery, search workers
    Query(QueryRequest),
    Stats(StatsRequest),
    Floors(FloorsRequest),
    Ping,
    // SearchEmbed, Data leaders
    EmbedLease(LeaseRequest),
    EmbedSet(SetRequest),
    EmbedFail(FailRequest),
    Requeue(RequeueRequest),
}
/// The method of a call without its body, for the prologue and the role matrix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MethodName {
    Tail,
    SnapshotOpen,
    SnapshotPage,
    SnapshotClose,
    Barrier,
    Close,
    Routes,
    Query,
    Stats,
    Floors,
    Ping,
    EmbedLease,
    EmbedSet,
    EmbedFail,
    Requeue,
}
impl MethodName {
    pub const ALL: [Self; 15] = [
        Self::Tail,
        Self::SnapshotOpen,
        Self::SnapshotPage,
        Self::SnapshotClose,
        Self::Barrier,
        Self::Close,
        Self::Routes,
        Self::Query,
        Self::Stats,
        Self::Floors,
        Self::Ping,
        Self::EmbedLease,
        Self::EmbedSet,
        Self::EmbedFail,
        Self::Requeue,
    ];
    /// The lane the method travels on.
    pub fn purpose(self) -> Purpose {
        match self {
            Self::Tail
            | Self::SnapshotOpen
            | Self::SnapshotPage
            | Self::SnapshotClose
            | Self::Barrier
            | Self::Close
            | Self::Routes => Purpose::SearchFeed,
            Self::Query | Self::Stats | Self::Floors | Self::Ping => Purpose::SearchQuery,
            Self::EmbedLease | Self::EmbedSet | Self::EmbedFail | Self::Requeue => {
                Purpose::SearchEmbed
            }
        }
    }
    /// Whether a group of `kind` serves the method.
    pub fn served_by(self, kind: &GroupKind) -> bool {
        match self {
            Self::Routes => *kind == GroupKind::Catalog,
            Self::Query | Self::Stats | Self::Floors | Self::Ping => *kind == GroupKind::Control,
            _ => matches!(kind, GroupKind::Data { .. }),
        }
    }
}
impl SearchMethod {
    pub fn name(&self) -> MethodName {
        match self {
            Self::Tail(_) => MethodName::Tail,
            Self::SnapshotOpen(_) => MethodName::SnapshotOpen,
            Self::SnapshotPage(_) => MethodName::SnapshotPage,
            Self::SnapshotClose(_) => MethodName::SnapshotClose,
            Self::Barrier(_) => MethodName::Barrier,
            Self::Close(_) => MethodName::Close,
            Self::Routes(_) => MethodName::Routes,
            Self::Query(_) => MethodName::Query,
            Self::Stats(_) => MethodName::Stats,
            Self::Floors(_) => MethodName::Floors,
            Self::Ping => MethodName::Ping,
            Self::EmbedLease(_) => MethodName::EmbedLease,
            Self::EmbedSet(_) => MethodName::EmbedSet,
            Self::EmbedFail(_) => MethodName::EmbedFail,
            Self::Requeue(_) => MethodName::Requeue,
        }
    }
}
/// Section 0 of a reply. Errors travel as the transport's `ProtocolError`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchReplyFrame {
    pub call: CallId,
    pub answer: SearchAnswer,
}
/// One reply per `SearchMethod`, same names.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchAnswer {
    Tail(TailReply),
    SnapshotOpen(SnapshotOpenReply),
    SnapshotPage(SnapshotPageReply),
    SnapshotClose,
    Barrier(BarrierReply),
    Close(CloseInterestReply),
    Routes(RoutesReply),
    Query(QueryReply),
    Stats(StatsReply),
    Floors(FloorsReply),
    Ping,
    EmbedLease(LeaseReply),
    EmbedSet(SetReply),
    EmbedFail(FailReply),
    Requeue(RequeueReply),
}

// ---- wire vectors (2.7) and row images ----

/// Bytes `[offset, offset + len)` of section 1. Dense `dim × f32 LE`, sparse
/// indices `n × u32 LE` and weights `n × f32 LE` as two refs, multi
/// `n × dim × f32 LE`; values as written, never normalized.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VecRef {
    pub offset: u32,
    pub len: u32,
}
impl VecRef {
    /// The referenced bytes: in bounds and a whole number of 4-byte values.
    pub fn bytes<'a>(&self, section: &'a [u8]) -> Result<&'a [u8], ProtocolError> {
        let start = self.offset as usize;
        let end = start.checked_add(self.len as usize).ok_or_else(invalid)?;
        match section.get(start..end) {
            Some(bytes) if bytes.len() % 4 == 0 => Ok(bytes),
            _ => Err(invalid()),
        }
    }
    pub fn f32s(&self, section: &[u8]) -> Result<Vec<f32>, ProtocolError> {
        Ok(self
            .bytes(section)?
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .collect())
    }
    pub fn u32s(&self, section: &[u8]) -> Result<Vec<u32>, ProtocolError> {
        Ok(self
            .bytes(section)?
            .chunks_exact(4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .collect())
    }
}
/// Builds section 1, bounded by `max_bytes` (the lane's payload bound).
#[derive(Debug, Default)]
pub struct VectorSection {
    bytes: Vec<u8>,
    max_bytes: usize,
}
impl VectorSection {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            max_bytes,
        }
    }
    pub fn push_f32(&mut self, values: &[f32]) -> Result<VecRef, ProtocolError> {
        self.push(values.len(), values.iter().map(|v| v.to_le_bytes()))
    }
    pub fn push_u32(&mut self, values: &[u32]) -> Result<VecRef, ProtocolError> {
        self.push(values.len(), values.iter().map(|v| v.to_le_bytes()))
    }
    fn push(
        &mut self,
        count: usize,
        values: impl Iterator<Item = [u8; 4]>,
    ) -> Result<VecRef, ProtocolError> {
        let len = count.checked_mul(4).ok_or_else(limit)?;
        let end = self.bytes.len().checked_add(len).ok_or_else(limit)?;
        if end > self.max_bytes || end > u32::MAX as usize {
            return Err(limit());
        }
        let offset = self.bytes.len() as u32;
        self.bytes.try_reserve(len).map_err(|_| limit())?;
        values.for_each(|v| self.bytes.extend_from_slice(&v));
        Ok(VecRef {
            offset,
            len: len as u32,
        })
    }
    pub fn len(&self) -> usize {
        self.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}
/// The canonical row JSON as stored. On the search wire it is a raw JSON value
/// (contract change: `CanonicalJson` deserializes unattached and cannot be
/// re-encoded without a storage lease).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RowJson(pub Box<RawValue>);
impl PartialEq for RowJson {
    fn eq(&self, other: &Self) -> bool {
        self.0.get() == other.0.get()
    }
}

// ---- 2.3 SearchFeed ----

/// Tail (E). SearchLatest `SendPage`; `Purge` -> `SnapshotRequired`; SearchRoute `Pull`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TailRequest {
    pub group: GroupIdentity,
    pub routing_epoch: u64,
    /// For logs and metrics only (Q7).
    pub assignment_epoch: u64,
    pub from: TailFrom,
    /// 1..=1024.
    pub max_entries: u16,
    /// 64 KiB..=MAX_FIXED_COMMAND_BYTES.
    pub max_bytes: u32,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TailFrom {
    /// The worker's applied position of this range.
    After(AppliedPosition),
    /// Only for a retagged split child: from this replica's chain start.
    Start,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TailReply {
    Changes {
        entries: Vec<WireEntry>,
        through: AppliedPosition,
        start: u64,
        frozen_at: Option<AppliedPosition>,
    },
    /// This voter has not applied `after` yet: try another voter.
    Behind { applied: AppliedPosition },
    /// `ChangePage::Gone`.
    SnapshotRequired {
        purged_through: u64,
        applied: AppliedPosition,
    },
    /// The group's routing epoch differs from the request's.
    StaleRoute { routing_epoch: u64 },
    /// No local replica of `group`, or another group generation.
    NotHosted,
    /// No search columns (`config.search` is None).
    Inactive,
    /// Value damage (L6): never served until repair or reinstall.
    Quarantined,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireEntry {
    pub at: AppliedPosition,
    pub changes: Vec<WireChange>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WireChange {
    /// `row: None` is a delete.
    Row {
        key: MvccKey,
        commit_ts: CommitTs,
        row: Option<WireRow>,
    },
    Closed {
        ts: CommitTs,
        unix_ms: u64,
    },
    Schema {
        epoch: u64,
        serving: Vec<ColumnId>,
        text: Vec<ColumnId>,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireRow {
    pub value: RowJson,
    pub blobs: Columns,
    pub source: Columns,
    pub vectors: Vec<(ColumnId, VectorKind, VecRef)>,
}
/// Snapshot for search (E). SearchLatest `StartInstall`/`FinishInstall`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotOpen {
    pub group: GroupIdentity,
    pub routing_epoch: u64,
    pub assignment_epoch: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshotOpenReply {
    Opened {
        cursor: Uuid,
        at: AppliedPosition,
        bound: CommitTs,
        /// `(ts, unix_ms)`.
        closed_floor: Option<(CommitTs, u64)>,
        schema: SearchSchema,
        frozen_at: Option<AppliedPosition>,
        idle_ms: u32,
    },
    /// Voter at its cursor limit: retry another voter or later.
    Busy,
    StaleRoute {
        routing_epoch: u64,
    },
    NotHosted,
    Inactive,
    Quarantined,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPageRequest {
    pub group: GroupIdentity,
    pub cursor: Uuid,
    pub after: Option<MvccKey>,
    pub max_items: u16,
    pub max_bytes: u32,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshotPageReply {
    Rows {
        rows: Vec<WireImageRow>,
        next: Option<MvccKey>,
        done: bool,
    },
    Expired,
}
/// The newest live version at the cursor's `at`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireImageRow {
    pub key: MvccKey,
    pub commit_ts: CommitTs,
    pub row: WireRow,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotClose {
    pub group: GroupIdentity,
    pub cursor: Uuid,
}
/// Barrier (F). One RPC per (search worker, data worker), addressed to the
/// first range's group; answers every listed range this node leads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarrierRequest {
    pub table_id: Uuid,
    pub ranges: Vec<BarrierRange>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarrierRange {
    pub group: GroupIdentity,
    pub routing_epoch: u64,
    /// Vector and text incarnations of the query.
    pub columns: Vec<ColumnId>,
}
/// Same order as the request; correlated by the header's `CallId`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarrierReply {
    pub ranges: Vec<BarrierAnswer>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum BarrierAnswer {
    /// Applied position after the wait (SearchLatest `AnswerBarrier`).
    At(AppliedPosition),
    /// Includes a deposed leader whose ReadIndex fails.
    NotLeader { hint: Option<u64> },
    /// Routing epoch differs, or frozen source whose split is published.
    StaleRoute { routing_epoch: u64 },
    /// A requested column is not in `SearchSchema.serving` ∪ `text` (DB-9).
    SchemaChanged { epoch: u64 },
    /// Status group unavailable while resolving intents.
    Unavailable,
}
/// Close interest (F). At most every 10 s per range while a worker serves
/// snapshot queries or `Floors`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseInterest {
    pub table_id: Uuid,
    /// `(group, routing_epoch)`.
    pub ranges: Vec<(GroupIdentity, u64)>,
    pub until_unix_ms: u64,
}
/// The leader's applied floor per range, `(ts, unix_ms)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseInterestReply {
    pub floors: Vec<Option<(CommitTs, u64)>>,
}
/// Routes (J), served by Catalog voters through a linearizable Catalog read.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutesRequest {
    pub table_id: Uuid,
    pub after: Option<Key>,
    pub max: u16,
}
/// A published column as the Catalog holds it (contract change: the Catalog
/// column record itself; `state` is `Ready` for every column listed).
pub type PublishedColumn = CatalogSearchColumn;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutesReply {
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub shards: Vec<ShardRoute>,
    pub next: Option<Key>,
    pub columns: Vec<PublishedColumn>,
}

// ---- 2.4 SearchQuery (W serves, I calls) ----

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub table_id: Uuid,
    pub assignment_epoch: u64,
    /// What the gateway wants covered by this worker.
    pub ranges: Vec<QueryRange>,
    pub read: ReadMode,
    /// rank: the fused set; rank_many: the batch, each its own list.
    pub rankings: Vec<WorkerRanking>,
    /// false for rank_many.
    pub fused: bool,
    /// Column names resolved and checked by the gateway.
    pub filter: Option<Predicate>,
    pub limit: u32,
    pub candidates: u32,
    pub recall: f64,
    pub exact: bool,
    pub rerank: Option<WorkerRanking>,
    pub rerank_candidates: u32,
    /// Columns to return; empty = key only.
    pub select: Vec<Text<128>>,
    /// Per ranking: table-wide BM25 counts (None for non-text).
    pub stats: Vec<Option<TextStats>>,
    pub budget: QueryBudget,
    pub class: QueryClass,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRange {
    pub group: GroupIdentity,
    pub routing_epoch: u64,
    pub span: KeySpan,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadMode {
    Latest,
    Snapshot { cut: CommitTs },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRanking {
    pub column: ColumnId,
    pub op: RankOp,
    pub probe: Probe,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Probe {
    Dense(VecRef),
    Sparse { indices: VecRef, weights: VecRef },
    Tokens { n: u32, values: VecRef },
    Text(String),
}
/// `nabedra_vector::impact::Stats`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextStats {
    pub docs: u64,
    pub words: u64,
    pub df: Vec<u64>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryBudget {
    pub cpu_us: u64,
    pub io_bytes: u64,
    pub memory_bytes: u64,
    pub deadline_ms: u32,
}
/// `nabedra_vector::runtime::Class`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryClass {
    Interactive,
    Batch,
    Exact,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryReply {
    Done {
        lists: Vec<Vec<Candidate>>,
        covered: Vec<Covered>,
        rows: Vec<RowOut>,
        queue: u32,
    },
    Refused {
        refusals: Vec<(GroupIdentity, Refusal)>,
        covered: Vec<Covered>,
        queue: u32,
    },
}
/// The worker's own view after re-tag, never an echo of the request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Covered {
    pub group: GroupIdentity,
    pub routing_epoch: u64,
    pub span: KeySpan,
    pub at: AppliedPosition,
}
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Candidate {
    /// Index into `rows`.
    pub row: u32,
    pub score: f32,
    pub rerank: Option<f32>,
}
/// Selected columns, from the pinned row image.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowOut {
    pub key: Key,
    pub commit_ts: CommitTs,
    pub columns: RowJson,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Refusal {
    NotHosted,
    Behind,
    StaleRoute,
    CutTooOld { oldest: CommitTs },
    CutTooNew { newest: CommitTs },
    NoFloor,
    SchemaChanged,
    Overloaded,
    OverBudget,
    Unavailable,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatsRequest {
    pub table_id: Uuid,
    pub ranges: Vec<QueryRange>,
    pub column: ColumnId,
    pub text: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatsReply {
    pub stats: TextStats,
    pub covered: Vec<Covered>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FloorsRequest {
    pub table_id: Uuid,
    pub ranges: Vec<QueryRange>,
}
/// Per range: `(group, floor (ts, unix_ms), bound)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FloorsReply {
    pub floors: Vec<(GroupIdentity, Option<(CommitTs, u64)>, CommitTs)>,
}

// ---- 2.5 SearchEmbed (K) ----

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseRequest {
    pub group: GroupIdentity,
    pub routing_epoch: u64,
    pub columns: Vec<ColumnId>,
    /// ≤ 128.
    pub max_items: u16,
    /// ≤ 60_000.
    pub lease_ms: u32,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LeaseReply {
    Items { items: Vec<LeasedItem>, more: bool },
    NotLeader { hint: Option<u64> },
    StaleRoute { routing_epoch: u64 },
    Inactive,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeasedItem {
    pub key: MvccKey,
    pub column: ColumnId,
    pub model: Text<128>,
    pub source_version: CommitTs,
    pub id: CommitTs,
    pub attempt: u32,
    pub text: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetRequest {
    pub group: GroupIdentity,
    pub routing_epoch: u64,
    pub key: MvccKey,
    pub column: ColumnId,
    pub source_version: CommitTs,
    pub value: VecRef,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SetReply {
    Set { commit_ts: CommitTs },
    AlreadySet,
    Obsolete,
    NotLeader { hint: Option<u64> },
    Retry,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailRequest {
    pub group: GroupIdentity,
    pub routing_epoch: u64,
    pub key: MvccKey,
    pub column: ColumnId,
    pub source_version: CommitTs,
    pub id: CommitTs,
    pub attempt: u32,
    pub reason: Text<256>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum FailReply {
    Counted { attempts: u32 },
    Failed { attempts: u32 },
    Unmatched,
    NotLeader { hint: Option<u64> },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequeueRequest {
    pub group: GroupIdentity,
    pub key: MvccKey,
    pub column: ColumnId,
    pub source_version: CommitTs,
    pub id: CommitTs,
}
/// Contract addition: the reply of `Requeue`, as `SearchReply::Requeued`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RequeueReply {
    Requeued { attempts: u32, limit: u32 },
    Unmatched,
    NotLeader { hint: Option<u64> },
}

// ---- 2.6 Control (J): wire types; logic in `authority/control/search.rs` ----

pub const SEARCH_MEMBER_PREFIX: &[u8] = b"ct/search-member/";
pub const SEARCH_ASSIGNMENT_PREFIX: &[u8] = b"ct/search-assign/";
pub const SEARCH_MODEL_PREFIX: &[u8] = b"ct/search-model/";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchMember {
    pub database: Text<128>,
    pub role: SearchRole,
    pub enrollment: WorkerEnrollment,
    pub zone: Text<64>,
    pub budget_bytes: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchRole {
    Worker,
    Embedder,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchAssignment {
    pub table_id: Uuid,
    pub epoch: u64,
    pub shards: Vec<SearchShard>,
    pub next: Option<SearchMove>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchShard {
    /// Adjacent, ascending.
    pub spans: Vec<KeySpan>,
    pub replicas: Vec<SearchReplica>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchReplica {
    pub worker_id: Uuid,
    pub since_epoch: u64,
    pub draining: Option<Draining>,
}
/// Grace 30 s; the drop names the epoch it ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Draining {
    pub ended_epoch: u64,
    pub until_unix_ms: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchMove {
    pub shard: u32,
    pub add: Uuid,
    pub remove: Option<Uuid>,
    /// The assignment's epoch + 1.
    pub epoch: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbedModel {
    pub name: Text<128>,
    pub endpoint: Text<512>,
    pub model: Text<128>,
    pub dim: u32,
    /// A file path or an environment variable name, never the secret.
    pub credential_ref: Text<256>,
    pub max_batch: u16,
    pub max_rps: u32,
    pub allow_hosts: Vec<Text<253>>,
}
/// Nested under one new `ControlCommand::Search { operation_id, command }`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchCommand {
    Enroll(SearchMember),
    Retire {
        worker_id: Uuid,
    },
    BeginMove {
        table_id: Uuid,
        expected_epoch: u64,
        mv: SearchMove,
    },
    /// epoch + 1, moves `remove` to draining.
    Publish {
        table_id: Uuid,
        expected_epoch: u64,
    },
    CancelMove {
        table_id: Uuid,
        expected_epoch: u64,
    },
    /// A name is bound for life: a change of endpoint, model or dim is refused.
    SetModel(EmbedModel),
    DropModel {
        name: Text<128>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchControlReply {
    Done { epoch: u64 },
    Conflict { epoch: u64 },
    NotEnabled,
    Refused(Text<256>),
}
/// One new `ReadRequest::Search(SearchRead)` variant.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchRead {
    Assignment { table_id: Uuid },
    Members { after: Option<Uuid>, max: u16 },
    Models,
    Status { table_id: Uuid },
}
/// Leader memory, not a log entry (IndexLifecycle `seen`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchReport {
    pub worker_id: Uuid,
    pub process: Uuid,
    pub table_id: Uuid,
    pub assignment_epoch: u64,
    /// Index in the table's assignment at `assignment_epoch`.
    pub shard: u32,
    /// Increases for each new snapshot of this shard within the process and epoch.
    /// Retries use the same id and pages.
    pub report_id: u64,
    /// Zero-based page index and the total pages in this snapshot of the shard.
    pub page: u16,
    pub pages: u16,
    pub ranges: Vec<RangeReport>,
    pub queue: u32,
    pub memory_used: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeReport {
    pub group_id: Uuid,
    pub routing_epoch: u64,
    pub applied: AppliedPosition,
    pub lag_ms: u32,
    pub columns: Vec<(ColumnId, ColumnState)>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ColumnState {
    Syncing,
    Ready,
    Failed(Text<256>),
}

// ---- 2.1 roles ----

/// The caller's enrollment role as the search endpoints see it. J maps the
/// peer directory's `Role` (`Search(db)`, `Embed(db)`, `Api(db)`, `Worker`)
/// onto it in `peer_directory/search.rs`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallerRole {
    /// All-in-one `serve`, the operator CLI on a data worker.
    Worker,
    Api(Text<128>),
    Search(Text<128>),
    Embed(Text<128>),
}
impl CallerRole {
    /// The 2.1 matrix: whether this role may call `method` on `purpose` of
    /// `group`. Every endpoint checks it in preflight. The database of an `Api`
    /// caller is checked by the search worker against the table (its query
    /// endpoint is bound under the Control identity).
    pub fn permits(&self, group: &GroupIdentity, purpose: Purpose, method: MethodName) -> bool {
        if method.purpose() != purpose || !method.served_by(&group.kind) {
            return false;
        }
        match self {
            Self::Worker => true,
            Self::Search(db) => purpose == Purpose::SearchFeed && *db == group.database,
            Self::Embed(db) => {
                matches!(
                    method,
                    MethodName::EmbedLease | MethodName::EmbedSet | MethodName::EmbedFail
                ) && *db == group.database
            }
            Self::Api(_) => purpose == Purpose::SearchQuery,
        }
    }
}
/// The role of an authenticated caller, from the local applied enrollment
/// (no network I/O). J implements it on the peer directory; tests use fakes.
pub trait RoleCheck: Send + Sync + 'static {
    fn role(&self, caller: &WorkerIdentity) -> Option<CallerRole>;
}
/// Until J's hook lands: no caller has a search role, so every search call is
/// refused with `Unauthorized` before any work.
pub struct NoSearchRoles;
impl RoleCheck for NoSearchRoles {
    fn role(&self, _: &WorkerIdentity) -> Option<CallerRole> {
        None
    }
}

// ---- endpoints ----

/// The endpoint of one search lane of one group. Preflight checks, in order:
/// the session, the prologue (version 1), the lane and the role; then the
/// owner's handler plans the sections. Nothing is served yet: every owner's
/// handler answers `UnsupportedOperation`.
pub struct SearchRpcEndpoint {
    purpose: Purpose,
    group: GroupIdentity,
    roles: Arc<dyn RoleCheck>,
}
impl SearchRpcEndpoint {
    pub fn new(purpose: Purpose, group: GroupIdentity, roles: Arc<dyn RoleCheck>) -> Arc<Self> {
        Arc::new(Self {
            purpose,
            group,
            roles,
        })
    }
    pub fn group(&self) -> &GroupIdentity {
        &self.group
    }
    /// The checks every search call passes before its handler runs.
    pub fn admit(
        &self,
        session: &AuthenticatedSession,
        header: &[u8],
    ) -> Result<SearchPrologue, ProtocolError> {
        if session.recovery_only()
            || session.purpose() != self.purpose
            || session.group() != &self.group
        {
            return Err(unauthorized());
        }
        crate::protocol::json_input(header, MAX_PROLOGUE_BYTES).map_err(|_| invalid())?;
        let prologue: SearchPrologue = serde_json::from_slice(header).map_err(|_| invalid())?;
        if prologue.version != SEARCH_VERSION {
            return Err(invalid());
        }
        let permitted = self
            .roles
            .role(&session.remote())
            .is_some_and(|role| role.permits(&self.group, self.purpose, prologue.method));
        if !permitted {
            return Err(unauthorized());
        }
        Ok(prologue)
    }
    fn class(&self) -> TrafficClass {
        match self.purpose {
            Purpose::SearchQuery => TrafficClass::SearchQuery,
            Purpose::SearchEmbed => TrafficClass::SearchEmbed,
            _ => TrafficClass::SearchFeed,
        }
    }
}
impl Endpoint for SearchRpcEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let prologue = self.admit(session, header)?;
            let plan = ReceivePlan {
                class: self.class(),
                section_lengths: [
                    u64::from(prologue.body_bytes),
                    u64::from(prologue.vector_bytes),
                    0,
                ],
                decode_budget_bytes: (prologue.body_bytes as usize).saturating_mul(4),
            };
            match prologue.method {
                MethodName::Tail
                | MethodName::SnapshotOpen
                | MethodName::SnapshotPage
                | MethodName::SnapshotClose => feed::plan(self, session, &prologue, plan),
                MethodName::Barrier | MethodName::Close => {
                    barrier::plan(self, session, &prologue, plan)
                }
                MethodName::Routes => routes::plan(self, session, &prologue, plan),
                MethodName::EmbedLease
                | MethodName::EmbedSet
                | MethodName::EmbedFail
                | MethodName::Requeue => embed::plan(self, session, &prologue, plan),
                // Served by the search worker process (W), never bound here.
                MethodName::Query | MethodName::Stats | MethodName::Floors | MethodName::Ping => {
                    Err(unsupported())
                }
            }
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: crate::oltp::pool::ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let prologue = self.admit(&session, header.as_slice())?;
            match prologue.method {
                MethodName::Tail
                | MethodName::SnapshotOpen
                | MethodName::SnapshotPage
                | MethodName::SnapshotClose => {
                    feed::handle(self, session, prologue, sections).await
                }
                MethodName::Barrier | MethodName::Close => {
                    barrier::handle(self, session, prologue, sections).await
                }
                MethodName::Routes => routes::handle(self, session, prologue, sections).await,
                MethodName::EmbedLease
                | MethodName::EmbedSet
                | MethodName::EmbedFail
                | MethodName::Requeue => embed::handle(self, session, prologue, sections).await,
                MethodName::Query | MethodName::Stats | MethodName::Floors | MethodName::Ping => {
                    Err(unsupported())
                }
            }
        })
    }
}
/// The SearchFeed endpoint of a Data group (E, F).
pub fn feed_endpoint(group: GroupIdentity, roles: Arc<dyn RoleCheck>) -> Arc<SearchRpcEndpoint> {
    SearchRpcEndpoint::new(Purpose::SearchFeed, group, roles)
}
/// The SearchFeed endpoint of a Catalog group (J: `Routes`).
pub fn routes_endpoint(group: GroupIdentity, roles: Arc<dyn RoleCheck>) -> Arc<SearchRpcEndpoint> {
    SearchRpcEndpoint::new(Purpose::SearchFeed, group, roles)
}
/// The SearchEmbed endpoint of a Data group (K).
pub fn embed_endpoint(group: GroupIdentity, roles: Arc<dyn RoleCheck>) -> Arc<SearchRpcEndpoint> {
    SearchRpcEndpoint::new(Purpose::SearchEmbed, group, roles)
}

pub(crate) fn unsupported() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::UnsupportedOperation,
        "the search operation is not supported yet",
        Outcome::NotApplied,
    )
}
pub(crate) fn unauthorized() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "search caller or lane not authorized",
        Outcome::NotApplied,
    )
}
pub(crate) fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid search request",
        Outcome::NotApplied,
    )
}
fn limit() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Overloaded,
        "search vector section over its bound",
        Outcome::NotApplied,
    )
}
