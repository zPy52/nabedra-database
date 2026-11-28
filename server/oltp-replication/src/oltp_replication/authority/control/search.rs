//! Search processes in Control (contract-w2 2.6, specs
//! `specs/vector/assign/SearchAssign.tla` and IndexLifecycle's `seen`).
//!
//! Durable records live under their own prefixes, never in `State`, so the
//! Control state format does not change: members (`ct/search-member/<id>`),
//! assignments (`ct/search-assign/<table>`), models
//! (`ct/search-model/<digest>`) and the binding of a model name, which
//! outlives a dropped model (`ct/search-model-bound/<digest>`).
//!
//! Ready reports are leader memory, never a log entry: a Control failover
//! loses them, and workers report again (`ReportAgain`).
use crate::oltp_replication::{
    search_rpc::{
        ColumnState, Draining, EmbedModel, SearchAssignment, SearchCommand, SearchControlReply,
        SearchMember, SearchMove, SearchRead, SearchReplica, SearchReport, SearchRole, SearchShard,
    },
    types::*,
};
use crate::oltp::mvcc::vector::ColumnId;
use crate::protocol::{ErrorCode, Outcome, ProtocolError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use uuid::Uuid;

pub(crate) use crate::oltp_replication::search_rpc::{
    SEARCH_ASSIGNMENT_PREFIX, SEARCH_MEMBER_PREFIX, SEARCH_MODEL_PREFIX,
};
pub(crate) const MODEL_BOUND_PREFIX: &[u8] = b"ct/search-model-bound/";
/// Ranges in one report call; a worker with more sends several (the Control
/// request header is at most 4 KiB).
pub const MAX_REPORT_RANGES: usize = 12;
const MAX_REPORT_COLUMNS: usize = 16;
/// Members in one page of `SearchRead::Members`.
pub const MAX_MEMBERS_PAGE: u16 = 32;
/// A replaced replica keeps serving this long (plan section 7).
pub const DRAIN_GRACE_MS: u64 = 30_000;
/// Search replicas per shard the reconciler keeps (plan section 7).
pub const REPLICAS: usize = 2;
/// Range reports one Control leader keeps in memory.
const MAX_MEMORY_RANGES: usize = 1 << 16;
/// Shards and replicas of one assignment.
const MAX_SHARDS: usize = 1024;
const MAX_SHARD_REPLICAS: usize = 8;

/// The one `ReadRequest::Search` of the Control endpoint: a read, a command
/// (proposed by the accepting leader) or a ready report (leader memory).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchRequest {
    Read(SearchRead),
    Command {
        operation_id: Uuid,
        command: SearchCommand,
    },
    Report(SearchReport),
}

/// The answer to `SearchRead::Status`: the assignment and, per range, the
/// incarnations some replica reported ready, from leader memory.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchStatus {
    pub assignment: Option<SearchAssignment>,
    pub ranges: Vec<RangeStatus>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeStatus {
    pub group_id: Uuid,
    pub ready: Vec<ColumnId>,
    /// The smallest lag of a replica that reported anything ready.
    pub lag_ms: Option<u32>,
}

pub(crate) fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
/// Every message is a constant well under 256 bytes.
pub(crate) fn refused(message: &'static str) -> SearchControlReply {
    Text::new(message.to_owned()).map_or(SearchControlReply::Conflict { epoch: 0 }, SearchControlReply::Refused)
}

pub(crate) fn member_key(worker_id: Uuid) -> Vec<u8> {
    super::machine::key(SEARCH_MEMBER_PREFIX, worker_id)
}
pub(crate) fn assignment_key(table_id: Uuid) -> Vec<u8> {
    super::machine::key(SEARCH_ASSIGNMENT_PREFIX, table_id)
}
fn name_id(name: &str) -> Uuid {
    let digest = Sha256::digest(name.as_bytes());
    Uuid::from_bytes(digest[..16].try_into().unwrap_or([0; 16]))
}
pub(crate) fn model_key(name: &str) -> Vec<u8> {
    super::machine::key(SEARCH_MODEL_PREFIX, name_id(name))
}
pub(crate) fn binding_key(name: &str) -> Vec<u8> {
    super::machine::key(MODEL_BOUND_PREFIX, name_id(name))
}
/// What a model name is bound to for life: its endpoint, model and dimensions.
pub(crate) fn binding(model: &EmbedModel) -> Digest {
    let mut h = Sha256::new();
    for part in [model.endpoint.as_str(), model.model.as_str()] {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part.as_bytes());
    }
    h.update(model.dim.to_be_bytes());
    Digest::from_bytes(h.finalize().into())
}

fn identifier(text: &str, max: usize) -> bool {
    let bytes = text.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= max
        && bytes[0].is_ascii_alphanumeric()
        && bytes.iter().all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(c))
}
pub(crate) fn valid_member(member: &SearchMember, database: &Text<128>) -> bool {
    member.database == *database
        && member.enrollment.validate().is_ok()
        && member.zone.len() <= 64
        && member.budget_bytes > 0
}
pub(crate) fn valid_model(model: &EmbedModel) -> bool {
    let endpoint = model.endpoint.as_str();
    identifier(model.name.as_str(), 64)
        && !model.model.is_empty()
        && (endpoint.starts_with("https://") || endpoint.starts_with("http://"))
        && (1..=4096).contains(&model.dim)
        && (1..=2048).contains(&model.max_batch)
        && model.max_rps > 0
        && !model.credential_ref.is_empty()
        && model.allow_hosts.len() <= 16
        && model.allow_hosts.iter().all(|host| !host.is_empty())
}
pub(crate) fn valid_assignment(assignment: &SearchAssignment) -> bool {
    assignment.shards.len() <= MAX_SHARDS
        && assignment.shards.iter().all(|shard| {
            !shard.spans.is_empty()
                && shard.spans.len() <= MAX_SHARDS
                && shard.replicas.len() <= MAX_SHARD_REPLICAS
        })
}

/// The first assignment of a table: one shard over every key, no replica.
pub fn initial_assignment(table_id: Uuid) -> SearchAssignment {
    SearchAssignment {
        table_id,
        epoch: 0,
        shards: vec![SearchShard {
            spans: vec![crate::oltp::mvcc::store::KeySpan {
                start: None,
                end: None,
            }],
            replicas: Vec::new(),
        }],
        next: None,
    }
}

/// SearchAssign `StartMove`: one move at a time, at the next epoch, of a
/// worker that is not in the shard yet, replacing at most one that is.
pub(crate) fn begin_move(
    current: Option<SearchAssignment>,
    table_id: Uuid,
    expected_epoch: u64,
    mv: SearchMove,
) -> Result<SearchAssignment, SearchControlReply> {
    let mut assignment = current.unwrap_or_else(|| initial_assignment(table_id));
    if assignment.epoch != expected_epoch {
        return Err(SearchControlReply::Conflict {
            epoch: assignment.epoch,
        });
    }
    let shard = usize::try_from(mv.shard)
        .ok()
        .and_then(|shard| assignment.shards.get(shard))
        .ok_or_else(|| refused("no such shard"))?;
    let held = |worker: Uuid| shard.replicas.iter().any(|replica| replica.worker_id == worker);
    if assignment.next.is_some()
        || Some(mv.epoch) != assignment.epoch.checked_add(1)
        || mv.add.is_nil()
        || held(mv.add)
        || mv.remove.is_some_and(|remove| !held(remove) || remove == mv.add)
        || shard.replicas.len() >= MAX_SHARD_REPLICAS
    {
        return Err(refused("invalid search move"));
    }
    assignment.next = Some(mv);
    Ok(assignment)
}
/// SearchAssign `Publish`: the new replica serves at the next epoch; the
/// replaced one drains and its drop names the epoch it ends. Replicas whose
/// grace passed leave the record.
pub(crate) fn publish(
    current: Option<SearchAssignment>,
    expected_epoch: u64,
    unix_ms: u64,
) -> Result<SearchAssignment, SearchControlReply> {
    let mut assignment = current.ok_or_else(|| refused("no search assignment"))?;
    let Some(mv) = assignment.next.filter(|_| assignment.epoch == expected_epoch) else {
        return Err(SearchControlReply::Conflict {
            epoch: assignment.epoch,
        });
    };
    let ended = assignment.epoch;
    let shard = usize::try_from(mv.shard)
        .ok()
        .and_then(|shard| assignment.shards.get_mut(shard))
        .ok_or_else(|| refused("no such shard"))?;
    shard
        .replicas
        .retain(|replica| replica.draining.is_none_or(|draining| draining.until_unix_ms > unix_ms));
    for replica in &mut shard.replicas {
        if Some(replica.worker_id) == mv.remove {
            replica.draining = Some(Draining {
                ended_epoch: ended,
                until_unix_ms: unix_ms.saturating_add(DRAIN_GRACE_MS),
            });
        }
    }
    shard.replicas.push(SearchReplica {
        worker_id: mv.add,
        since_epoch: mv.epoch,
        draining: None,
    });
    assignment.epoch = mv.epoch;
    assignment.next = None;
    Ok(assignment)
}
pub(crate) fn cancel(
    current: Option<SearchAssignment>,
    expected_epoch: u64,
) -> Result<SearchAssignment, SearchControlReply> {
    let mut assignment = current.ok_or_else(|| refused("no search assignment"))?;
    if assignment.epoch != expected_epoch || assignment.next.is_none() {
        return Err(SearchControlReply::Conflict {
            epoch: assignment.epoch,
        });
    }
    assignment.next = None;
    Ok(assignment)
}

/// SearchAssign `RecvReady`: every page of the new replica's shard snapshot
/// at the move's epoch is present, with every reported range ready.
pub fn move_ready(assignment: &SearchAssignment, reports: &[SearchReport]) -> bool {
    let Some(mv) = assignment.next else {
        return false;
    };
    let mut snapshot = None;
    let mut pages = BTreeSet::new();
    let mut ranges = BTreeSet::new();
    for report in reports.iter().filter(|report| {
        report.worker_id == mv.add
            && report.table_id == assignment.table_id
            && report.assignment_epoch == mv.epoch
            && report.shard == mv.shard
    }) {
        let id = (report.process, report.report_id, report.pages);
        if snapshot.get_or_insert(id) != &id
            || report.page >= report.pages
            || !pages.insert(report.page)
            || report.ranges.is_empty()
            || !report.ranges.iter().all(|range| {
                ranges.insert(range.group_id)
                    && range.columns.iter().all(|(_, state)| *state == ColumnState::Ready)
            })
        {
            return false;
        }
    }
    snapshot.is_some_and(|(_, _, total)| usize::from(total) == pages.len())
}

/// Read the workers needed for placement without loading the whole inventory.
pub(crate) trait SearchMembership {
    fn worker_zone(&mut self, worker: Uuid) -> Result<Option<Text<64>>, ProtocolError>;
    /// Visit workers in inventory order, stopping when the visitor returns true.
    fn visit_workers(&mut self, visit: impl FnMut(&SearchMember) -> bool) -> Result<(), ProtocolError>;
}

#[cfg(test)]
impl SearchMembership for &[SearchMember] {
    fn worker_zone(&mut self, worker: Uuid) -> Result<Option<Text<64>>, ProtocolError> {
        Ok(self
            .iter()
            .find(|member| member.role == SearchRole::Worker && member.enrollment.worker_id == worker)
            .map(|member| member.zone.clone()))
    }

    fn visit_workers(
        &mut self,
        mut visit: impl FnMut(&SearchMember) -> bool,
    ) -> Result<(), ProtocolError> {
        for member in *self {
            if member.role == SearchRole::Worker && visit(member) {
                break;
            }
        }
        Ok(())
    }
}

/// The next reconcile command using an in-memory inventory.
#[cfg(test)]
fn reconcile(
    assignment: &SearchAssignment,
    mut members: &[SearchMember],
    reports: &[SearchReport],
) -> Option<SearchCommand> {
    reconcile_with(assignment, &mut members, reports).unwrap()
}

/// SearchAssign `StartMove` and `Publish`: publish a ready move or fill a
/// shard that needs a replica, preferring a worker in another zone.
pub(crate) fn reconcile_with(
    assignment: &SearchAssignment,
    members: &mut impl SearchMembership,
    reports: &[SearchReport],
) -> Result<Option<SearchCommand>, ProtocolError> {
    if assignment.next.is_some() {
        return Ok(move_ready(assignment, reports).then_some(SearchCommand::Publish {
            table_id: assignment.table_id,
            expected_epoch: assignment.epoch,
        }));
    }
    let Some(epoch) = assignment.epoch.checked_add(1) else {
        return Ok(None);
    };
    for (index, shard) in assignment.shards.iter().enumerate() {
        let mut serving = 0;
        let mut gone = None;
        let mut zones = Vec::new();
        for replica in shard
            .replicas
            .iter()
            .filter(|replica| replica.draining.is_none())
        {
            serving += 1;
            match members.worker_zone(replica.worker_id)? {
                Some(zone) => zones.push(zone),
                None => {
                    gone.get_or_insert(replica.worker_id);
                }
            }
        }
        if serving >= REPLICAS && gone.is_none() {
            continue;
        }
        let (mut first, mut preferred) = (None, None);
        members.visit_workers(|member| {
            let worker = member.enrollment.worker_id;
            if shard.replicas.iter().any(|replica| replica.worker_id == worker) {
                return false;
            }
            first.get_or_insert(worker);
            if !zones.contains(&member.zone) {
                preferred = Some(worker);
                return true;
            }
            false
        })?;
        return Ok(preferred.or(first).and_then(|add| {
            Some(SearchCommand::BeginMove {
                table_id: assignment.table_id,
                expected_epoch: assignment.epoch,
                mv: SearchMove {
                    shard: u32::try_from(index).ok()?,
                    add,
                    remove: gone.filter(|_| serving >= REPLICAS),
                    epoch,
                },
            })
        }));
    }
    Ok(None)
}

/// Checks a report before it enters leader memory.
pub(crate) fn valid_report(report: &SearchReport) -> bool {
    !report.worker_id.is_nil()
        && !report.process.is_nil()
        && !report.table_id.is_nil()
        && report.shard < MAX_SHARDS as u32
        && report.page < report.pages
        && !report.ranges.is_empty()
        && report.ranges.len() <= MAX_REPORT_RANGES
        && report.ranges.iter().all(|range| {
            !range.group_id.is_nil()
                && range.columns.len() <= MAX_REPORT_COLUMNS
                && range.columns.is_sorted_by(|a, b| a.0 < b.0)
        })
}

/// Ready reports of one Control leader term (IndexLifecycle `seen`).
#[derive(Default)]
struct Memory {
    term: u64,
    /// The worker's latest snapshot for each `(table, worker, shard)`.
    reports: BTreeMap<(Uuid, Uuid, u32), ReportPages>,
    ranges: usize,
}
#[derive(Default)]
struct ReportPages {
    pages: BTreeMap<u16, SearchReport>,
    ranges: usize,
}
/// Per Control group (tests run several in one process).
static MEMORY: Mutex<BTreeMap<Uuid, Memory>> = Mutex::new(BTreeMap::new());

fn memory<T>(control: Uuid, term: u64, f: impl FnOnce(&mut Memory) -> T) -> Result<T, ProtocolError> {
    let mut all = MEMORY
        .lock()
        .map_err(|_| invalid("search report memory is poisoned"))?;
    let memory = all.entry(control).or_default();
    // A new leader term starts empty: `Failover` loses `seen`.
    if memory.term != term {
        *memory = Memory {
            term,
            ..Memory::default()
        };
    }
    Ok(f(memory))
}
/// Records a page in the leader's current term. A newer assignment,
/// restarted worker, or newer report supersedes the last snapshot.
pub(crate) fn record_report(control: Uuid, term: u64, report: SearchReport) -> Result<(), ProtocolError> {
    if !valid_report(&report) {
        return Err(invalid("invalid search report"));
    }
    memory(control, term, |memory| {
        let key = (report.table_id, report.worker_id, report.shard);
        let old = memory.reports.get(&key);
        let mut same = false;
        if let Some((_, first)) = old.and_then(|old| old.pages.first_key_value()) {
            let same_epoch = first.assignment_epoch == report.assignment_epoch;
            let same_process = first.process == report.process;
            if report.assignment_epoch < first.assignment_epoch
                || (same_epoch && same_process && report.report_id < first.report_id)
            {
                return Ok(());
            }
            same = same_epoch && same_process && first.report_id == report.report_id;
            if same && first.pages != report.pages {
                return Err(invalid("search report pages disagree on page count"));
            }
        }
        let old = memory.reports.remove(&key);
        let before = old.as_ref().map_or(0, |old| old.ranges);
        let mut next = old.filter(|_| same).unwrap_or_default();
        let page_before = next.pages.get(&report.page).map_or(0, |page| page.ranges.len());
        next.ranges = next.ranges - page_before + report.ranges.len();
        let after = memory.ranges - before + next.ranges;
        if after > MAX_MEMORY_RANGES {
            return Err(ProtocolError::new(
                ErrorCode::Overloaded,
                "Control holds too many search reports",
                Outcome::NotApplied,
            ));
        }
        next.pages.insert(report.page, report);
        memory.ranges = after;
        memory.reports.insert(key, next);
        Ok(())
    })?
}
/// The reports of `table` in the memory of the leader of `term`.
pub(crate) fn reports(control: Uuid, term: u64, table: Uuid) -> Result<Vec<SearchReport>, ProtocolError> {
    memory(control, term, |memory| {
        memory
            .reports
            .range((table, Uuid::nil(), 0)..=(table, Uuid::max(), u32::MAX))
            .flat_map(|(_, report)| report.pages.values().cloned())
            .collect()
    })
}
/// Per range of the table: the incarnations some replica reported ready.
pub fn range_status(reports: &[SearchReport]) -> Vec<RangeStatus> {
    let mut ranges: BTreeMap<Uuid, RangeStatus> = BTreeMap::new();
    for range in reports.iter().flat_map(|report| report.ranges.iter()) {
        let status = ranges.entry(range.group_id).or_insert(RangeStatus {
            group_id: range.group_id,
            ready: Vec::new(),
            lag_ms: None,
        });
        let ready: Vec<ColumnId> = range
            .columns
            .iter()
            .filter(|(_, state)| *state == ColumnState::Ready)
            .map(|(id, _)| *id)
            .collect();
        if !ready.is_empty() {
            status.lag_ms = Some(status.lag_ms.map_or(range.lag_ms, |lag| lag.min(range.lag_ms)));
        }
        for id in ready {
            if !status.ready.contains(&id) {
                status.ready.push(id);
            }
        }
        status.ready.sort_unstable();
    }
    ranges.into_values().collect()
}

pub(crate) fn command_bytes(command: &SearchCommand) -> usize {
    std::mem::size_of::<SearchCommand>()
        + match command {
            SearchCommand::Enroll(member) => {
                member.database.capacity_bytes()
                    + member.zone.capacity_bytes()
                    + member.enrollment.endpoint.capacity_bytes()
                    + member.enrollment.server_name.capacity_bytes()
            }
            SearchCommand::SetModel(model) => {
                model.name.capacity_bytes()
                    + model.endpoint.capacity_bytes()
                    + model.model.capacity_bytes()
                    + model.credential_ref.capacity_bytes()
                    + model
                        .allow_hosts
                        .iter()
                        .map(|host| host.capacity_bytes() + std::mem::size_of::<Text<253>>())
                        .sum::<usize>()
            }
            SearchCommand::DropModel { name } => name.capacity_bytes(),
            _ => 0,
        }
}

#[cfg(test)]
mod tests;
