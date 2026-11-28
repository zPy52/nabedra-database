use super::*;
use super::{index::IndexChoice, query_metrics::AccessKind};
use crate::oltp::mvcc::{
    catalog::decode_canonical,
    store::scan::{ScanDomain, ScanPage, ScanRequest},
};
use crate::{
    oltp::{pool::{ByteClass, ScratchLease}, read_attempt::ScheduledReadError},
    protocol::Query,
    query_semantics,
};
use nabedra_oltp_types::{
    owned::OwnedCommand,
    authority::ReadScope,
};
use serde::{
    Serialize,
    ser::{SerializeMap, SerializeSeq},
};
use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap},
};
mod routing;
pub(super) mod schedule;
use routing::{
    PrimaryConstraint, PrimaryRange, RoutingCursor, checked_scope_increment, validate_route,
};
use schedule::QueryExecution;

enum QueryAccess {
    Empty,
    Point {
        key: Key,
        group: GroupIdentity,
    },
    Scan {
        bounds: PrimaryRange,
        index: Option<IndexChoice>,
    },
}

impl QueryAccess {
    fn kind(&self) -> AccessKind {
        match self {
            Self::Empty => AccessKind::Empty,
            Self::Point { .. } => AccessKind::Point,
            Self::Scan {
                index: Some(index), ..
            } if index.cover.is_some() => AccessKind::CoveringIndex,
            Self::Scan { index: Some(_), .. } => AccessKind::SecondaryIndex,
            Self::Scan { bounds, .. } if bounds.bounded() => AccessKind::PrimaryRange,
            Self::Scan { .. } => AccessKind::PrimaryScan,
        }
    }
}

fn result_cap(query: &Query) -> usize {
    query
        .limit
        .unwrap_or(u64::MAX)
        .min((protocol::MAX_RESULT_ROWS + 1) as u64) as usize
}
struct OrderingContext {
    table: OwnedRecord<TableRecord>,
    query: Arc<Query>,
    _owner: Arc<ScratchLease>,
}
enum RowView {
    Base(OwnedRecord<StoredRow>),
    Cover(OwnedRecord<super::index::IndexPayload>),
}
impl std::ops::Deref for RowView {
    type Target = Row;
    fn deref(&self) -> &Row {
        match self {
            Self::Base(r) => r,
            Self::Cover(r) => &r.cover,
        }
    }
}
struct Item {
    row: RowView,
    order: Arc<OrderingContext>,
}
impl PartialEq for Item {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Item {
    fn cmp(&self, o: &Self) -> Ordering {
        query_semantics::compare_rows(
            &self.order.table.descriptor,
            &self.order.query,
            &self.row,
            &o.row,
        )
    }
}
struct State {
    heap: BinaryHeap<Item>,
    matched: u64,
    cap: usize,
    count: bool,
    cover: Option<Arc<BTreeSet<String>>>,
    access: AccessKind,
    visited_shards: u64,
    order: Arc<OrderingContext>,
    _container: Arc<ScratchLease>,
}
struct Cursor {
    key: MvccKey,
    owner: Arc<ScratchLease>,
}

enum ScanShard {
    New(OwnedRecord<RoutingPage>, usize),
    Continue(Arc<ParticipantHandle>, Cursor),
}
struct ScanInputs {
    pin: PinnedCatalog,
    table_id: Uuid,
    binding: RequestBinding,
    choice: Option<IndexChoice>,
    bounds: PrimaryRange,
    execution: Arc<QueryExecution>,
}
struct ScannedPage {
    first: bool,
    participant: Arc<ParticipantHandle>,
    response: OwnedCommand<ScanPage>,
}

fn bound_label<T>(bound: &Option<(T, bool)>) -> &'static str {
    match bound {
        None => "unbounded",
        Some((_, true)) => "inclusive",
        Some((_, false)) => "exclusive",
    }
}

impl NativePlanner {
    async fn query_access(
        &self,
        pin: &PinnedCatalog,
        table: OwnedRecord<TableRecord>,
        query: Arc<Query>,
        count: bool,
        owner: Arc<ScratchLease>,
    ) -> Result<QueryAccess, PlanError> {
        let schema = table.clone();
        let input = query.clone();
        let constraint = self
            .cpu(owner.clone(), move || {
                Ok(PrimaryConstraint::from_query(
                    &input,
                    &schema.descriptor.key,
                ))
            })
            .await?;
        match constraint {
            PrimaryConstraint::Empty => Ok(QueryAccess::Empty),
            PrimaryConstraint::Point(key) => {
                match self.catalog.route(pin, table.descriptor.id, &key).await? {
                    RouteRead::Route { page, index } => {
                        let route = &page.shards[index];
                        validate_route(&table, route)?;
                        Ok(QueryAccess::Point {
                            key,
                            group: route.group.clone(),
                        })
                    }
                    RouteRead::Blocked(intent) => Err(PlanError::Blocked(intent)),
                    RouteRead::Missing => Err(not_found().into()),
                }
            }
            PrimaryConstraint::Range(bounds) => {
                let index = self
                    .choose_index(pin, table, query, count, bounds.bounded(), owner)
                    .await?;
                Ok(QueryAccess::Scan { bounds, index })
            }
        }
    }

    pub(super) async fn explain(
        &self,
        pin: &PinnedCatalog,
        table: OwnedRecord<TableRecord>,
        query: Query,
        owner: Arc<ScratchLease>,
    ) -> Result<ChargedBytes, PlanError> {
        let query = Arc::new(query);
        let access = self
            .query_access(pin, table.clone(), query.clone(), false, owner.clone())
            .await?;
        let mut shards = 0;
        let mut first = None;
        let mut last = None;
        let mut pages = 0;
        if let QueryAccess::Scan { bounds, .. } = &access {
            let mut routes = RoutingCursor::open(&self.catalog, &table, pin, bounds).await?;
            while let Some((page, index)) = routes.next().await? {
                let group = page.shards[index].group.group_id;
                first.get_or_insert(group);
                last = Some(group);
                shards = checked_scope_increment(shards)?;
            }
            pages = routes.pages_read;
        }
        let read_ts = pin.read_ts.get();
        let shared = self.catalog.resources().clone();
        Ok(self.cpu(owner, move || {
            let key = &table.descriptor.key;
            let (access, scope, merge, data_reads) = match access {
                QueryAccess::Empty => ("empty result".into(), "shards 0".into(), "none".into(), false),
                QueryAccess::Point { group, .. } => (
                    format!("primary point seek on {key}"),
                    format!("shards 1; owning shard {}", group.group_id),
                    "none; one candidate".into(),
                    true,
                ),
                QueryAccess::Scan { bounds, index } => {
                    let access = match index {
                        Some(index) => format!(
                            "secondary {} {}; composite-prefix {}; lower {}; upper {}; covering {}",
                            index.record.definition.name,
                            if index.seeks() { "seek" } else { "scan" },
                            index.equality.len(),
                            bound_label(&index.lower),
                            bound_label(&index.upper),
                            index.cover.is_some(),
                        ),
                        None if bounds.bounded() => format!("primary range seek on {key}"),
                        None => format!("primary scan on {key}"),
                    };
                    let mut scope = format!(
                        "shards {shards}; primary-key lower {}, upper {}; linked routing pages read {pages}",
                        bound_label(&bounds.lower), bound_label(&bounds.upper),
                    );
                    if let (Some(first), Some(last)) = (first, last) {
                        use std::fmt::Write;
                        let _ = write!(scope, "; first shard {first}; last shard {last}");
                    }
                    let order = query.order_by.as_ref().map_or_else(
                        || format!("primary key {key} ascending"),
                        |order| format!("{} {}; primary-key ascending ties", order.column, if order.desc { "descending" } else { "ascending" }),
                    );
                    let merge = format!("bounded top-k ({} candidates); {order}", result_cap(&query));
                    (access, scope, merge, shards > 0)
                }
            };
            let residual = if data_reads && query.filter.is_some() { "exact predicate" } else { "none" };
            let data = if data_reads { "; Data leader quorum/applied barrier" } else { "" };
            let explanation = format!(
                "engine oltp; replicated MVCC\naccess {access}\nscope {scope}\nresidual {residual}\nmerge {merge}\nconsistency pinned timestamp {read_ts}; schema epoch {}; routing epoch {}; Catalog leader quorum/applied barrier{data}",
                table.schema_epoch, table.routing_epoch,
            );
            shared.encode(&explanation, protocol::MAX_BODY_BYTES - 128)
        }).await?)
    }

    pub(super) async fn query(
        &self,
        pin: &PinnedCatalog,
        table: OwnedRecord<TableRecord>,
        query: Query,
        count: bool,
        binding: RequestBinding,
        owner: Arc<ScratchLease>,
    ) -> Result<ChargedBytes, PlanError> {
        let query = Arc::new(query);
        let access = self
            .query_access(pin, table.clone(), query.clone(), count, owner.clone())
            .await?;
        let kind = access.kind();
        let cap = if matches!(access, QueryAccess::Point { .. }) {
            1
        } else {
            result_cap(&query)
        };
        let shared = self.catalog.resources();
        let container = Arc::new(shared.reserve_scratch(
            ByteClass::Data,
            if count {
                4096
            } else {
                cap.checked_mul(std::mem::size_of::<Item>() * 2)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or_else(not_found)?
            },
        )?);
        let order = Arc::new(OrderingContext {
            table: table.clone(),
            query,
            _owner: owner.clone(),
        });
        let mut heap = BinaryHeap::new();
        if !count {
            heap.try_reserve_exact(cap)
                .map_err(|_| failure(ErrorCode::LimitExceeded, "top-k metadata exceeds memory"))?;
        }
        let mut state = State {
            heap,
            matched: 0,
            cap,
            count,
            cover: None,
            access: kind,
            visited_shards: 0,
            order,
            _container: container,
        };
        #[cfg(any(test, feature = "test-hooks"))]
        if count && crate::oltp::test_hooks::growth::count_overflow_requested(pin.read_ts.get()) {
            state.matched = u64::MAX - 1;
        }
        let (bounds, choice) = match access {
            QueryAccess::Empty => return self.finish_query(state, owner).await,
            QueryAccess::Point { key, group } => {
                let participant = self.resolver.participant(&group).await?;
                state.visited_shards = 1;
                let (_, row) = self
                    .row_on(pin, table.descriptor.id, &key, &participant)
                    .await?;
                if let Some(row) = row {
                    state = self
                        .accept_row(state, RowView::Base(row), owner.clone())
                        .await?;
                }
                return self.finish_query(state, owner).await;
            }
            QueryAccess::Scan { bounds, index } => (bounds, index),
        };
        state.cover = choice.as_ref().and_then(|c| c.cover.clone());
        let has_base_lookup = choice.as_ref().is_some_and(|c| c.cover.is_none());
        let input = Arc::new(ScanInputs {
            pin: pin.clone(),
            table_id: table.descriptor.id,
            binding,
            choice,
            bounds,
            execution: self.query_scheduler.begin(binding, shared)?,
        });
        let mut routes = RoutingCursor::open(&self.catalog, &table, pin, &input.bounds).await?;
        let mut pages = self.query_scheduler.pages(!has_base_lookup);
        let mut routes_done = false;
        let result = async {
            while !routes_done || !pages.is_empty() {
                tokio::select! {
                    route = routes.next(), if !routes_done && pages.can_start() => {
                        if let Some((page, index)) = route? {
                            pages.start(self.clone().scan_next(input.clone(), ScanShard::New(page, index)));
                        } else {
                            routes_done = true;
                        }
                    }
                    page = pages.next(), if !pages.is_empty() => {
                        let page = page?;
                        if page.first {
                            state.visited_shards = checked_scope_increment(state.visited_shards)?;
                        }
                        let participant = page.participant.clone();
                        let (next, cursor, stop) = self
                            .reduce_page(state, page, &input, owner.clone())
                            .await?;
                        state = next;
                        if stop {
                            break;
                        }
                        if let Some(cursor) = cursor {
                            #[cfg(any(test, feature = "test-hooks"))]
                            crate::oltp::test_hooks::transaction_checkpoint(
                                "scan-page",
                                binding.request_id,
                                &state.matched,
                            ).await;
                            pages.start(self.clone().scan_next(
                                input.clone(), ScanShard::Continue(participant, cursor),
                            ));
                        }
                    }
                }
            }
            Ok::<_, PlanError>(state)
        }
        .await;
        // A blocked intent, limit, or failure stops new dispatch. Previously
        // dispatched pages still own their work through authenticated settlement.
        pages.settle().await;
        self.finish_query(result?, owner).await
    }

    async fn scan_next(
        self,
        input: Arc<ScanInputs>,
        shard: ScanShard,
    ) -> Result<ScannedPage, PlanError> {
        let grant = input.execution.reserve_page().await?;
        let first = matches!(&shard, ScanShard::New(_, _));
        let (participant, cursor) = match shard {
            ScanShard::New(page, index) => (
                self.resolver.participant(&page.shards[index].group).await?,
                None,
            ),
            ScanShard::Continue(participant, cursor) => (participant, Some(cursor)),
        };
        let key_bytes = |field: &Option<(Key, bool)>| match field {
            Some((Key::Utf8(value), _)) => value.capacity(),
            _ => 0,
        };
        let bound_bytes = match &input.choice {
            Some(choice) => {
                choice.equality.capacity()
                    * std::mem::size_of::<crate::oltp::mvcc::index::IndexValue>()
                    + choice
                        .equality
                        .iter()
                        .map(crate::oltp::mvcc::index::IndexValue::heap_bytes)
                        .sum::<usize>()
                    + choice.lower.as_ref().map_or(0, |(v, _)| v.heap_bytes())
                    + choice.upper.as_ref().map_or(0, |(v, _)| v.heap_bytes())
            }
            None => key_bytes(&input.bounds.lower) + key_bytes(&input.bounds.upper),
        };
        let _request = self.catalog.resources().reserve_scratch(
            ByteClass::Data,
            bound_bytes + cursor.as_ref().map_or(0, |cursor| cursor.key.heap_bytes()) + 4096,
        )?;
        let scope = ReadScope {
            request: input.binding,
            authority: input.pin.authority,
            read_ts: input.pin.read_ts,
        };
        let mut max_bytes = 1 << 20;
        let response = loop {
            let request = ScanRequest {
                domain: match &input.choice {
                    Some(c) => ScanDomain::Index {
                        table_id: input.table_id,
                        index_id: c.record.definition.index_id,
                        equality: c.equality.clone(),
                        complete: c.complete,
                        lower: c.lower.clone(),
                        upper: c.upper.clone(),
                    },
                    None => ScanDomain::Rows {
                        table_id: input.table_id,
                    },
                },
                read_ts: input.pin.read_ts,
                after: cursor.as_ref().map(|cursor| cursor.key.clone()),
                after_owner: cursor.as_ref().map(|cursor| cursor.owner.clone()),
                lower: if input.choice.is_none() {
                    input.bounds.lower.clone()
                } else {
                    None
                },
                upper: if input.choice.is_none() {
                    input.bounds.upper.clone()
                } else {
                    None
                },
                max_items: 128,
                max_bytes,
            };
            match participant.scheduled_scan_page(scope, request).await {
                Ok(response) => break response,
                Err(ScheduledReadError::Settled(error))
                    if max_bytes < super::super::MAX_FIXED_COMMAND_BYTES
                        && error.code == ErrorCode::LimitExceeded
                        && error.message
                            == crate::oltp::mvcc::store::scan::SCAN_RECORD_TOO_WIDE =>
                {
                    max_bytes = super::super::MAX_FIXED_COMMAND_BYTES;
                }
                Err(ScheduledReadError::Settled(error)) => return Err(error.into()),
                Err(ScheduledReadError::Uncertain { cause, read }) => {
                    input.execution.quarantine(read, grant);
                    return Err(cause.into());
                }
            }
        };
        Ok(ScannedPage {
            first,
            participant,
            response,
        })
    }

    async fn reduce_page(
        &self,
        mut state: State,
        page: ScannedPage,
        input: &ScanInputs,
        owner: Arc<ScratchLease>,
    ) -> Result<(State, Option<Cursor>, bool), PlanError> {
        let (records, exhausted) = match page.response.payload()? {
            ScanPage::Records { records, exhausted } => (records, exhausted),
            ScanPage::Blocked { txn, manifest } => {
                return Err(PlanError::Blocked(IntentRef {
                    txn: *txn,
                    manifest: *manifest,
                }));
            }
        };
        for record in records.iter() {
            let Some(value) = &record.value else { continue };
            let row = if let Some(c) = &input.choice {
                let payload: OwnedRecord<super::index::IndexPayload> =
                    decode_canonical(self.catalog.resources(), value.clone()).await?;
                if c.cover.is_some() {
                    RowView::Cover(payload)
                } else {
                    let MvccKey::IndexEntry { primary_key, .. } = &record.key else {
                        return Err(not_found().into());
                    };
                    let (version, base) = self
                        .row_on(&input.pin, input.table_id, primary_key, &page.participant)
                        .await?;
                    if version != payload.base_version.or(record.version) {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "index/base version mismatch",
                        )
                        .into());
                    }
                    RowView::Base(base.ok_or_else(not_found)?)
                }
            } else {
                RowView::Base(decode_canonical(self.catalog.resources(), value.clone()).await?)
            };
            state = self.accept_row(state, row, owner.clone()).await?;
            if state.count
                && state
                    .order
                    .query
                    .limit
                    .is_some_and(|limit| state.matched >= limit)
            {
                return Ok((state, None, true));
            }
        }
        let cursor = if *exhausted {
            None
        } else {
            let last = records.last().ok_or_else(not_found)?;
            let charge = Arc::new(
                self.catalog
                    .resources()
                    .reserve_scratch(ByteClass::Data, last.key.heap_bytes() + 512)?,
            );
            Some(
                self.cpu(owner, move || {
                    let ScanPage::Records { records, .. } = page.response.payload()? else {
                        return Err(not_found());
                    };
                    Ok(Cursor {
                        key: records.last().ok_or_else(not_found)?.key.clone(),
                        owner: charge,
                    })
                })
                .await?,
            )
        };
        Ok((state, cursor, false))
    }
    async fn accept_row(
        &self,
        mut state: State,
        row: RowView,
        owner: Arc<ScratchLease>,
    ) -> Result<State, PlanError> {
        Ok(self
            .cpu(owner, move || {
                if let Some(fields) = &state.cover {
                    if fields.iter().any(|field| !row.contains_key(field)) {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "incomplete covering index",
                        ));
                    }
                    for (column, value) in row.iter() {
                        let ty = state
                            .order
                            .table
                            .descriptor
                            .schema
                            .get(column)
                            .ok_or_else(not_found)?;
                        protocol::validate_scalar(value, *ty)?;
                    }
                } else {
                    protocol::validate_row(
                        &row,
                        &state.order.table.descriptor,
                        protocol::RowMode::Complete,
                    )?;
                }
                if query_semantics::matches_filter(&state.order.query, &row) {
                    state.matched = state.matched.checked_add(1).ok_or_else(|| {
                        failure(ErrorCode::LimitExceeded, "count exceeds u64 representation")
                    })?;
                    if !state.count {
                        if state.cap > protocol::MAX_RESULT_ROWS
                            && state.matched > protocol::MAX_RESULT_ROWS as u64
                        {
                            return Err(failure(
                                ErrorCode::LimitExceeded,
                                "result exceeds 10000 rows",
                            ));
                        }
                        let item = Item {
                            row,
                            order: state.order.clone(),
                        };
                        if state.heap.len() < state.cap {
                            state.heap.push(item)
                        } else if state.heap.peek().is_some_and(|worst| item < *worst) {
                            state.heap.pop();
                            state.heap.push(item)
                        }
                    }
                }
                Ok(state)
            })
            .await?)
    }
    async fn finish_query(
        &self,
        state: State,
        owner: Arc<ScratchLease>,
    ) -> Result<ChargedBytes, PlanError> {
        let shared = self.catalog.resources().clone();
        let metrics = self.query_metrics.clone();
        Ok(self
            .cpu(owner, move || {
                let result = if state.count {
                    shared.encode(
                        &state
                            .matched
                            .min(state.order.query.limit.unwrap_or(u64::MAX)),
                        protocol::MAX_BODY_BYTES - 128,
                    )
                } else {
                    let _container = state._container;
                    let mut rows = state.heap.into_vec();
                    rows.sort_unstable();
                    shared.encode(
                        &Rows {
                            rows: &rows,
                            select: state.order.query.select.as_deref(),
                        },
                        protocol::MAX_BODY_BYTES - 128,
                    )
                }?;
                metrics.record(
                    &state.order.table.descriptor,
                    &state.order.query,
                    state.count,
                    state.access,
                    state.visited_shards,
                );
                Ok(result)
            })
            .await?)
    }
    pub(super) async fn list(
        &self,
        pin: &PinnedCatalog,
        owner: Arc<ScratchLease>,
    ) -> Result<ChargedBytes, PlanError> {
        let mut after = None;
        let mut after_owner = None;
        let mut tables: Vec<OwnedRecord<TableRecord>> = Vec::new();
        let mut bytes = 2usize;
        loop {
            let page = self
                .catalog
                .participant()
                .scan_page(ScanRequest {
                    domain: ScanDomain::CatalogNames,
                    read_ts: pin.read_ts,
                    after,
                    after_owner,
                    lower: None,
                    upper: None,
                    max_items: 128,
                    max_bytes: super::super::MAX_FIXED_COMMAND_BYTES,
                })
                .await?;
            let ScanPage::Records { records, exhausted } = page.payload()? else {
                let ScanPage::Blocked { txn, manifest } = page.payload()? else {
                    unreachable!()
                };
                return Err(PlanError::Blocked(IntentRef {
                    txn: *txn,
                    manifest: *manifest,
                }));
            };
            for record in records.iter() {
                if let Some(raw) = &record.value {
                    let name: OwnedRecord<NameRecord> =
                        decode_canonical(self.catalog.resources(), raw.clone()).await?;
                    let (_, table) = value(self.catalog.describe(pin, name.table_id).await?)?;
                    let table = table.ok_or_else(not_found)?;
                    let copy = table.clone();
                    bytes = bytes
                        .checked_add(
                            self.cpu(owner.clone(), move || {
                                crate::oltp::query::check_body(
                                    &copy.descriptor,
                                    protocol::MAX_BODY_BYTES,
                                )
                            })
                            .await?
                                + 1,
                        )
                        .ok_or_else(not_found)?;
                    if bytes > protocol::MAX_BODY_BYTES - 128 {
                        return Err(failure(
                            ErrorCode::LimitExceeded,
                            "table listing exceeds response budget",
                        )
                        .into());
                    }
                    tables.try_reserve(1).map_err(|_| {
                        failure(
                            ErrorCode::LimitExceeded,
                            "table listing metadata allocation",
                        )
                    })?;
                    tables.push(table);
                }
            }
            if *exhausted {
                break;
            }
            let last = records.last().ok_or_else(not_found)?;
            let charge = Arc::new(
                self.catalog
                    .resources()
                    .reserve_scratch(ByteClass::Data, last.key.heap_bytes() + 512)?,
            );
            after = Some(last.key.clone());
            after_owner = Some(charge);
        }
        let shared = self.catalog.resources().clone();
        Ok(self
            .cpu(owner, move || {
                shared.encode(&TableList(&tables), protocol::MAX_BODY_BYTES - 128)
            })
            .await?)
    }
}
struct Rows<'a> {
    rows: &'a [Item],
    select: Option<&'a [String]>,
}
impl Serialize for Rows<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.rows.len()))?;
        for item in self.rows {
            seq.serialize_element(&Projection {
                row: &item.row,
                select: self.select,
            })?;
        }
        seq.end()
    }
}
struct Projection<'a> {
    row: &'a Row,
    select: Option<&'a [String]>,
}
impl Serialize for Projection<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(None)?;
        for (k, v) in self.row {
            if self.select.is_none_or(|cols| cols.contains(k)) {
                map.serialize_entry(k, v)?;
            }
        }
        map.end()
    }
}
struct TableList<'a>(&'a [OwnedRecord<TableRecord>]);
impl Serialize for TableList<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for t in self.0 {
            seq.serialize_element(&t.descriptor)?;
        }
        seq.end()
    }
}
