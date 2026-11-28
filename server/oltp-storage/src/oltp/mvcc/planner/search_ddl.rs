//! DDL of tables with search columns (plan section 7, contract-w2 2.6, spec
//! `specs/vector/lifecycle/IndexLifecycle.tla`).
//!
//! Every DDL of a search table runs under the table's Exclusive lifecycle
//! guard and fences every Data group with a `SearchFence` before the Catalog
//! commit that publishes it (`StartPublish`/`FenceRange`/`Publish`,
//! `StartDrop`/`FinishDrop`). Search epochs follow L19:
//!
//! - the coordinator probes every group through its log and picks the epoch
//!   `1 + max(Catalog epoch, every group's epoch)` (an interrupted DDL may
//!   have fenced some groups and never committed);
//! - new column ids are allocated above every group's `high` for the same
//!   reason;
//! - each fence is a compare-and-set from the epoch the group was probed at,
//!   so a late fence of an interrupted DDL that picked the same epoch never
//!   installs, and this DDL's fence never lands on columns it did not see.
//!   A `Conflict` re-probes that group; if another DDL overtook this one,
//!   the step starts over from a fresh probe, a bounded number of times.
//!
//! A step that fails before its Catalog commit stays invisible: the Catalog
//! keeps the previous columns, and the next DDL fences above it.
use super::*;
use crate::oltp::mvcc::{
    catalog::{CatalogSearch, CatalogSearchColumn, CatalogSearchKind, ColumnLife},
    store::search::{SearchFence, SearchReply, SearchSchema, TextColumn},
    vector::{ColumnId, MAX_SEARCH_COLUMNS, SearchColumn, VectorKind},
};
use crate::protocol::{ColumnType, Embed};
use nabedra_oltp_types::types::{CommitTs, Text};
use std::sync::OnceLock;
use std::time::Duration;

/// Data groups one search DDL visits. A table is at most a few thousand
/// ranges (plan section 7); this bounds the coordinator's memory.
pub(crate) const MAX_SEARCH_GROUPS: usize = 1 << 16;
/// A frozen range (a split in progress) refuses a fence; the coordinator
/// retries for about this long before it gives up (RANGES-2 stalls it).
const SPLIT_RETRIES: u32 = 24;
/// Re-probes of one group after a `Conflict`, and fresh starts of one step
/// after another DDL overtook it (L19), as DDL-FENCE's three tries.
pub(crate) const FENCE_TRIES: u32 = 3;

/// What every Data group of a search table holds after one DDL step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchFenceTemplate {
    /// The table record's next `lifecycle_id`, `None` for a drop.
    pub lifecycle: Option<Uuid>,
    pub columns: Vec<SearchColumn>,
    pub serving: Vec<ColumnId>,
    pub text: Vec<TextColumn>,
}
impl SearchFenceTemplate {
    /// The fence of `epoch` for a group probed at `expected`. The authority
    /// is filled in by the group's handle.
    fn fence(&self, table_id: Uuid, expected: u64, epoch: u64) -> SearchFence {
        SearchFence {
            authority: nabedra_oltp_types::types::Authority {
                run_id: Uuid::nil(),
                owner_term: 0,
            },
            table_id,
            epoch,
            expected_epoch: Some(expected),
            lifecycle: self.lifecycle,
            columns: self.columns.clone(),
            serving: self.serving.clone(),
            text: self.text.clone(),
        }
    }
}

/// One Data group as the DDL coordinator sees it: a `ParticipantHandle`, or
/// a fake in the planner tests.
pub(crate) trait FenceTarget: Send + Sync {
    fn group_id(&self) -> Uuid;
    /// Proposes `fence` under the group's current run authority.
    fn fence(&self, fence: SearchFence) -> ResolveFuture<'_, SearchReply>;
    /// The group's search schema in log order.
    fn probe(&self) -> ResolveFuture<'_, Option<SearchSchema>>;
}
impl FenceTarget for Arc<ParticipantHandle> {
    fn group_id(&self) -> Uuid {
        self.identity().group_id
    }
    fn fence(&self, fence: SearchFence) -> ResolveFuture<'_, SearchReply> {
        Box::pin(self.search_fence(fence))
    }
    fn probe(&self) -> ResolveFuture<'_, Option<SearchSchema>> {
        Box::pin(self.search_probe())
    }
}

/// One range's readiness as Control's leader memory holds it (IndexLifecycle
/// `seen`): the incarnations some replica reported ready, its lag, and the
/// range's embedding records.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RangeReadiness {
    pub group_id: Uuid,
    pub ready: Vec<ColumnId>,
    /// The smallest lag of a ready replica.
    pub lag_ms: Option<u32>,
    pub pending_embeddings: u64,
    pub failed_embeddings: u64,
}
/// Reads Control's ready reports of a table (`SearchRead::Status`). They are
/// leader memory: a Control failover empties them until workers report again.
pub trait SearchReadiness: Send + Sync {
    fn readiness(&self, table_id: Uuid) -> ResolveFuture<'_, Vec<RangeReadiness>>;
}
/// Shared by a planner and its scoped clones; set once at startup.
#[derive(Clone, Default)]
pub(crate) struct SearchSlot(Arc<OnceLock<Arc<dyn SearchReadiness>>>);

/// IndexLifecycle `AllReady`: every range has a replica that reported this
/// incarnation ready. A report counts only for the column it names, and only
/// for a range of the table's current routing.
pub fn all_ready(groups: &[Uuid], readiness: &[RangeReadiness], id: ColumnId) -> bool {
    !groups.is_empty()
        && groups.iter().all(|group| {
            readiness
                .iter()
                .any(|range| range.group_id == *group && range.ready.contains(&id))
        })
}

fn invalid(message: &'static str) -> ProtocolError {
    failure(ErrorCode::ValidationError, message)
}
fn conflict(message: &'static str) -> ProtocolError {
    failure(ErrorCode::Conflict, message)
}

impl CatalogSearch {
    /// A table without search columns yet.
    pub fn empty() -> Self {
        Self {
            epoch: 0,
            high: 0,
            columns: Vec::new(),
        }
    }
    /// The columns of a table created with them, ids 1, 2, ... in name order
    /// (vector columns, then full-text columns). All are ready: an empty
    /// table has nothing to build. `None` for a table without search columns.
    pub fn of_descriptor(descriptor: &TableDescriptor) -> Result<Option<Self>, ProtocolError> {
        let mut search = Self::empty();
        for (name, ty) in descriptor.schema.iter().filter(|(_, ty)| ty.is_vector()) {
            let column = vector_column(*ty, descriptor.search.embed.get(name))?;
            search = search.add_vector(name, column, ColumnLife::Ready)?;
        }
        for (column, language) in &descriptor.search.full_text {
            search = search.add_text(column, language, ColumnLife::Ready)?;
        }
        Ok((!search.columns.is_empty()).then_some(search))
    }
    fn next_id(&self) -> Result<ColumnId, ProtocolError> {
        self.high
            .checked_add(1)
            .ok_or_else(|| invalid("search column ids are exhausted"))
    }
    fn push(mut self, column: CatalogSearchColumn) -> Result<Self, ProtocolError> {
        if self.columns.len() >= MAX_SEARCH_COLUMNS {
            return Err(invalid("a table has at most 8 search columns"));
        }
        self.high = column.id;
        self.columns.push(column);
        Ok(self)
    }
    /// IndexLifecycle `AddColumn`: a new incarnation with a fresh id.
    pub fn add_vector(
        &self,
        name: &str,
        column: SearchColumn,
        state: ColumnLife,
    ) -> Result<Self, ProtocolError> {
        if self.named(name, false).is_some() {
            return Err(invalid("column already exists"));
        }
        let id = self.next_id()?;
        self.clone().push(CatalogSearchColumn {
            id,
            name: Text::try_from(name).map_err(|_| invalid("invalid column name"))?,
            kind: CatalogSearchKind::Vector(SearchColumn { id, ..column }),
            state,
        })
    }
    pub fn add_text(&self, column: &str, language: &str, state: ColumnLife) -> Result<Self, ProtocolError> {
        if self.named(column, true).is_some() {
            return Err(invalid("column already has full text"));
        }
        let id = self.next_id()?;
        let column = Text::try_from(column).map_err(|_| invalid("invalid column name"))?;
        self.clone().push(CatalogSearchColumn {
            id,
            name: column.clone(),
            kind: CatalogSearchKind::FullText {
                column,
                language: Text::try_from(language).map_err(|_| invalid("invalid language"))?,
            },
            state,
        })
    }
    /// IndexLifecycle `StartDrop`: the incarnation leaves every group; a
    /// later add of the same name is a new id.
    pub fn drop_named(&self, name: &str, text: bool) -> Result<Self, ProtocolError> {
        let id = self
            .named(name, text)
            .ok_or_else(|| invalid("no such search column"))?
            .id;
        let mut next = self.clone();
        next.columns.retain(|column| column.id != id);
        Ok(next)
    }
    /// IndexLifecycle `StartPublish`: `Building` becomes `Ready`.
    pub fn publish(&self, id: ColumnId) -> Result<Self, ProtocolError> {
        let mut next = self.clone();
        let column = next
            .columns
            .iter_mut()
            .find(|column| column.id == id)
            .ok_or_else(|| conflict("the search column was dropped"))?;
        if column.state != ColumnLife::Building {
            return Err(conflict("the search column is not building"));
        }
        column.state = ColumnLife::Ready;
        Ok(next)
    }
    pub fn named(&self, name: &str, text: bool) -> Option<&CatalogSearchColumn> {
        self.columns.iter().find(|column| {
            column.name.as_str() == name
                && matches!(column.kind, CatalogSearchKind::FullText { .. }) == text
        })
    }
    /// The incarnations still to publish.
    pub fn building(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.columns
            .iter()
            .filter(|column| column.state == ColumnLife::Building)
            .map(|column| column.id)
    }
    /// The search schema a range holds for this Catalog state: what a backup
    /// at a cut records and reads its images with (rv-oltp F1).
    pub fn schema(&self) -> SearchSchema {
        let template = self.template(None);
        SearchSchema {
            epoch: self.epoch,
            columns: template.columns,
            high: self.high,
            serving: template.serving,
            text: template.text,
        }
    }
    /// What the groups must hold for this Catalog state: every vector column
    /// is writable, only ready ones are served (DB-9), full-text columns are
    /// served once ready.
    pub fn template(&self, lifecycle: Option<Uuid>) -> SearchFenceTemplate {
        let mut template = SearchFenceTemplate {
            lifecycle,
            columns: Vec::new(),
            serving: Vec::new(),
            text: Vec::new(),
        };
        for column in &self.columns {
            let ready = column.state == ColumnLife::Ready;
            match &column.kind {
                CatalogSearchKind::Vector(vector) => {
                    template.columns.push(vector.clone());
                    if ready {
                        template.serving.push(column.id);
                    }
                }
                CatalogSearchKind::FullText { column: text, language } => {
                    template.text.push(TextColumn {
                        id: column.id,
                        column: text.clone(),
                        language: language.clone(),
                        serving: ready,
                    })
                }
            }
        }
        template.columns.sort_by_key(|column| column.id);
        template.serving.sort_unstable();
        template.text.sort_by_key(|text| text.id);
        template
    }
}
/// The stored definition of a vector column of this type.
pub fn vector_column(ty: ColumnType, embed: Option<&Embed>) -> Result<SearchColumn, ProtocolError> {
    let (kind, dim) = match ty {
        ColumnType::Vector(dim) => (VectorKind::Dense, u32::from(dim)),
        ColumnType::SparseVector => (VectorKind::Sparse, 0),
        ColumnType::VectorList(dim) => (VectorKind::Multi, u32::from(dim)),
        _ => return Err(invalid("not a vector column")),
    };
    let source = embed
        .map(|embed| Text::try_from(embed.source.as_str()).map_err(|_| invalid("invalid embed source")))
        .transpose()?;
    let column = SearchColumn {
        id: 0,
        kind,
        dim,
        source,
    };
    if !column.valid() {
        return Err(invalid("invalid vector column"));
    }
    Ok(column)
}

/// The Data groups of a search table and the schema each holds, probed
/// through its log under the Exclusive guard.
pub(crate) struct SearchGroups<G = Arc<ParticipantHandle>> {
    groups: Vec<(G, Option<SearchSchema>)>,
}
impl<G: FenceTarget> SearchGroups<G> {
    pub(crate) async fn probe(targets: Vec<G>) -> Result<Self, ProtocolError> {
        let mut groups = Self {
            groups: targets.into_iter().map(|target| (target, None)).collect(),
        };
        groups.reprobe().await?;
        Ok(groups)
    }
    async fn reprobe(&mut self) -> Result<(), ProtocolError> {
        for (target, schema) in &mut self.groups {
            *schema = target.probe().await?;
        }
        Ok(())
    }
    pub(crate) fn ids(&self) -> Vec<Uuid> {
        self.groups.iter().map(|(target, _)| target.group_id()).collect()
    }
    fn schemas(&self) -> impl Iterator<Item = &SearchSchema> {
        self.groups.iter().filter_map(|(_, schema)| schema.as_ref())
    }
    pub(crate) fn next_epoch(&self, catalog: u64) -> Result<u64, ProtocolError> {
        next_epoch(self.schemas(), catalog)
    }
    pub(crate) fn raise_high(&self, search: CatalogSearch) -> CatalogSearch {
        raise_high(self.schemas(), search)
    }
}
/// The epoch a new fence takes: above the Catalog's and every group's.
fn next_epoch<'a>(
    schemas: impl Iterator<Item = &'a SearchSchema>,
    catalog: u64,
) -> Result<u64, ProtocolError> {
    schemas
        .map(|schema| schema.epoch)
        .fold(catalog, u64::max)
        .checked_add(1)
        .ok_or_else(|| invalid("search epochs are exhausted"))
}
/// The Catalog columns with `high` raised above every id a group saw, so a
/// new column never takes an id an interrupted DDL already used.
fn raise_high<'a>(
    schemas: impl Iterator<Item = &'a SearchSchema>,
    mut search: CatalogSearch,
) -> CatalogSearch {
    search.high = schemas.map(|schema| schema.high).fold(search.high, u16::max);
    search
}

/// Why a fence step stopped before every group held its template.
pub(crate) enum FenceStop {
    /// Another DDL fenced a group at or above this step's epoch, or with an
    /// id this step did not see: start over from a fresh probe.
    Overtaken,
    Failed(PlanError),
}
impl From<ProtocolError> for FenceStop {
    fn from(error: ProtocolError) -> Self {
        Self::Failed(error.into())
    }
}

/// IndexLifecycle `SendFences`/`FenceRange` at a fixed `epoch`: every group
/// installs `template` by compare-and-set from the epoch it was probed at
/// (L19). A `Conflict` re-probes that group and retries while the group
/// stays below `epoch` with no id above the ones this step saw.
pub(crate) async fn fence_all<G: FenceTarget>(
    groups: &mut SearchGroups<G>,
    table_id: Uuid,
    epoch: u64,
    template: &SearchFenceTemplate,
) -> Result<(), FenceStop> {
    use crate::oltp::mvcc::store::Fault;
    let seen = groups.schemas().map(|schema| schema.high).max().unwrap_or(0);
    for (target, schema) in &mut groups.groups {
        let (mut conflicts, mut splits) = (0, 0);
        let mut backoff = Duration::from_millis(50);
        loop {
            let (at, high) = schema.as_ref().map_or((0, 0), |schema| (schema.epoch, schema.high));
            if at >= epoch || high > seen {
                return Err(FenceStop::Overtaken);
            }
            match target.fence(template.fence(table_id, at, epoch)).await? {
                SearchReply::Fenced { epoch: applied, direct }
                    if applied == epoch
                        && direct.is_some_and(|direct| direct.lifecycle == template.lifecycle) =>
                {
                    break;
                }
                SearchReply::Rejected(Fault::Conflict) if conflicts < FENCE_TRIES => {
                    conflicts += 1;
                    *schema = target.probe().await?;
                }
                SearchReply::Rejected(Fault::Conflict) => {
                    return Err(FenceStop::Failed(
                        conflict("a Data group kept refusing the search fence; retry the request").into(),
                    ));
                }
                SearchReply::Rejected(Fault::SplitInProgress) if splits < SPLIT_RETRIES => {
                    splits += 1;
                    nabedra_rt::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                }
                SearchReply::Rejected(Fault::SplitInProgress) => {
                    return Err(FenceStop::Failed(
                        failure(
                            ErrorCode::StorageUnavailable,
                            "a split holds a range of the table; retry the search DDL",
                        )
                        .into(),
                    ));
                }
                _ => {
                    return Err(FenceStop::Failed(
                        invalid("a Data group refused the search fence").into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// One fenced DDL step (L19): picks the epoch above the Catalog's and every
/// group's, builds the template from what the groups hold (`make`, which
/// also returns the step's next Catalog state), fences every group, and
/// starts over from a fresh probe when another DDL overtook it, at most
/// `FENCE_TRIES` times. Returns the epoch the groups hold.
pub(crate) async fn fence_search<G: FenceTarget, T>(
    groups: &mut SearchGroups<G>,
    table_id: Uuid,
    catalog_epoch: u64,
    mut make: impl FnMut(&SearchGroups<G>) -> Result<(SearchFenceTemplate, T), PlanError>,
) -> Result<(u64, T), PlanError> {
    for tries in 1.. {
        let epoch = groups.next_epoch(catalog_epoch)?;
        let (template, next) = make(groups)?;
        match fence_all(groups, table_id, epoch, &template).await {
            Ok(()) => return Ok((epoch, next)),
            Err(FenceStop::Overtaken) if tries < FENCE_TRIES => groups.reprobe().await?,
            Err(FenceStop::Overtaken) => break,
            Err(FenceStop::Failed(error)) => return Err(error),
        }
    }
    Err(conflict("another search DDL fenced the table; retry the request").into())
}

impl NativePlanner {
    /// Installs the worker's reader of Control's ready reports. Without one
    /// no column is ever published and `search_status` reports no lag.
    pub fn set_search_readiness(
        &self,
        readiness: Arc<dyn SearchReadiness>,
    ) -> Result<(), ProtocolError> {
        self.search
            .0
            .set(readiness)
            .map_err(|_| failure(ErrorCode::Conflict, "search readiness already installed"))
    }
    pub(crate) async fn readiness(&self, table_id: Uuid) -> Result<Vec<RangeReadiness>, ProtocolError> {
        match self.search.0.get() {
            Some(readiness) => readiness.readiness(table_id).await,
            None => Ok(Vec::new()),
        }
    }

    /// Every Data group the table routes to at `pin`, with its schema.
    pub(crate) async fn search_groups(
        &self,
        pin: &PinnedCatalog,
        table_id: Uuid,
    ) -> Result<SearchGroups, PlanError> {
        let mut groups = Vec::new();
        let mut next = match self.catalog.routing_root(pin, table_id).await? {
            CatalogRead::Value { record: Some(root), .. } => root.first_page,
            CatalogRead::Value { record: None, .. } => None,
            CatalogRead::Blocked(intent) => return Err(PlanError::Blocked(intent)),
        };
        while let Some(page_id) = next {
            let page = match self.catalog.routing_page(pin, table_id, page_id).await? {
                CatalogRead::Value { record: Some(page), .. } => page,
                CatalogRead::Value { record: None, .. } => return Err(not_found().into()),
                CatalogRead::Blocked(intent) => return Err(PlanError::Blocked(intent)),
            };
            for route in page.shards.iter() {
                if groups.len() >= MAX_SEARCH_GROUPS {
                    return Err(invalid("too many ranges for one search DDL").into());
                }
                groups.push(self.resolver.participant(&route.group).await?);
            }
            next = page.next_page;
        }
        Ok(SearchGroups::probe(groups).await?)
    }

    /// The fence and the next table record of a DDL that keeps the search
    /// columns (index publication and retirement): a direct schema fence for
    /// a plain table, a search fence of the same columns at a new epoch for a
    /// search table. `prepare` probes; `run` fences, after the plan built.
    pub(crate) async fn prepare_lifecycle_fence(
        &self,
        pin: &PinnedCatalog,
        table: &TableRecord,
        lifecycle: Uuid,
    ) -> Result<(LifecycleFence, TableRecord), PlanError> {
        let mut record = table.with_lifecycle(lifecycle);
        let Some(search) = table.search.as_ref() else {
            return Ok((LifecycleFence::Direct(lifecycle), record));
        };
        let groups = self.search_groups(pin, table.descriptor.id).await?;
        let epoch = groups.next_epoch(search.epoch)?;
        let template = search.template(Some(lifecycle));
        if let Some(next) = record.search.as_mut() {
            next.epoch = epoch;
        }
        Ok((LifecycleFence::Search { groups, epoch, template }, record))
    }
    /// The table record was planned with `epoch`, so this step does not
    /// start over: another DDL overtaking it fails the request as Conflict.
    pub(crate) async fn run_lifecycle_fence(
        &self,
        pin: &PinnedCatalog,
        table_id: Uuid,
        fence: LifecycleFence,
    ) -> Result<(), PlanError> {
        match fence {
            LifecycleFence::Direct(lifecycle) => {
                self.fence_direct_writers(pin, table_id, Some(lifecycle)).await?;
            }
            LifecycleFence::Search { mut groups, epoch, template } => {
                match fence_all(&mut groups, table_id, epoch, &template).await {
                    Ok(()) => {}
                    Err(FenceStop::Overtaken) => {
                        return Err(conflict("another search DDL fenced the table; retry the request").into());
                    }
                    Err(FenceStop::Failed(error)) => return Err(error),
                }
            }
        }
        Ok(())
    }

    /// `table.add_column`, `drop_column`, `add_full_text`, `drop_full_text`:
    /// one Catalog step under the Exclusive guard, fenced first. A new
    /// column is `Building` until `plan_search_publish`.
    pub(super) async fn plan_search_ddl(
        &self,
        operation: &Operation,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        table: OwnedRecord<TableRecord>,
        version: Option<CommitTs>,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let id = table.descriptor.id;
        let guard = self
            .guard(pin, txn, request, id, version, table.schema_epoch, GuardMode::Exclusive)
            .await?;
        let planned = async {
            let mut descriptor = table.descriptor.clone();
            match operation {
                Operation::AddColumn(p) => {
                    descriptor.schema.insert(p.name.clone(), p.r#type);
                    if let Some(embed) = &p.embed {
                        descriptor.search.embed.insert(p.name.clone(), embed.clone());
                    }
                }
                Operation::DropColumn(p) => {
                    descriptor.schema.remove(&p.name);
                    descriptor.search.embed.remove(&p.name);
                }
                Operation::AddFullText(p) => {
                    descriptor
                        .search
                        .full_text
                        .insert(p.column.clone(), p.language.clone());
                }
                Operation::DropFullText(p) => {
                    descriptor.search.full_text.remove(&p.column);
                }
                _ => return Err(not_found().into()),
            }
            descriptor.validate()?;
            let current = table.search.clone().unwrap_or_else(CatalogSearch::empty);
            let step = |base: CatalogSearch| -> Result<CatalogSearch, ProtocolError> {
                match operation {
                    Operation::AddColumn(p) => {
                        let column = vector_column(p.r#type, p.embed.as_ref())?;
                        base.add_vector(&p.name, column, ColumnLife::Building)
                    }
                    Operation::DropColumn(p) => base.drop_named(&p.name, false),
                    Operation::AddFullText(p) => base.add_text(&p.column, &p.language, ColumnLife::Building),
                    Operation::DropFullText(p) => base.drop_named(&p.column, true),
                    _ => Err(not_found()),
                }
            };
            let mut groups = self.search_groups(pin, id).await?;
            let next = self
                .fence_step(&mut groups, id, current.epoch, txn, |groups| {
                    Ok(step(groups.raise_high(current.clone()))?)
                })
                .await?;
            self.publish_search(&table, version, descriptor, next, txn, guard.clone(), owner.clone())
                .await
        }
        .await;
        planned.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }

    /// IndexLifecycle `StartPublish` and `Publish` for column `id`, called by
    /// the build reconciler: only if every range has a replica that reported
    /// this incarnation ready (`all_ready`). Fences `id` into `serving`, then
    /// marks it `Ready`.
    pub async fn plan_search_publish(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        table_id: Uuid,
        column: ColumnId,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let (version, table) = value(self.catalog.describe(pin, table_id).await?)?;
        let table = table.ok_or_else(not_found)?;
        let guard = self
            .guard(pin, txn, request, table_id, version, table.schema_epoch, GuardMode::Exclusive)
            .await?;
        let planned = async {
            let current = table
                .search
                .clone()
                .ok_or_else(|| conflict("the table has no search columns"))?;
            let mut groups = self.search_groups(pin, table_id).await?;
            let readiness = self.readiness(table_id).await?;
            if !all_ready(&groups.ids(), &readiness, column) {
                return Err(conflict("a range has no ready replica of the column yet").into());
            }
            let next = self
                .fence_step(&mut groups, table_id, current.epoch, txn, |groups| {
                    Ok(groups.raise_high(current.clone()).publish(column)?)
                })
                .await?;
            self.publish_search(&table, version, table.descriptor.clone(), next, txn, guard.clone(), owner.clone())
                .await
        }
        .await;
        planned.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }

    /// Fences the step whose next Catalog columns `next` computes from the
    /// probed groups, under the DDL's lifecycle; returns them at the fenced
    /// epoch.
    async fn fence_step(
        &self,
        groups: &mut SearchGroups,
        table_id: Uuid,
        catalog_epoch: u64,
        txn: TxnId,
        next: impl Fn(&SearchGroups) -> Result<CatalogSearch, PlanError>,
    ) -> Result<CatalogSearch, PlanError> {
        let lifecycle = Some(txn.request_id);
        let (epoch, mut next) = fence_search(groups, table_id, catalog_epoch, |groups| {
            let next = next(groups)?;
            Ok((next.template(lifecycle), next))
        })
        .await?;
        next.epoch = epoch;
        Ok(next)
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_search(
        &self,
        table: &TableRecord,
        version: Option<CommitTs>,
        descriptor: TableDescriptor,
        next: CatalogSearch,
        txn: TxnId,
        guard: VerifiedLifecycleGuard,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let lifecycle = txn.request_id;
        let record = TableRecord {
            descriptor: descriptor.clone(),
            search: Some(next),
            ..table.with_lifecycle(lifecycle)
        };
        let change = Change::Put(CanonicalJson::from_serializable(
            self.catalog.resources(),
            &record,
            crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES,
        )?);
        let epochs = self.catalog.participant().epochs().await?;
        Ok(MutationPlan {
            participants: vec![ParticipantWrites {
                participant: self.catalog.participant().clone(),
                schema_epoch: epochs.schema_epoch,
                routing_epoch: epochs.routing_epoch,
                writes: vec![PreparedWrite {
                    key: MvccKey::CatalogTable { table_id: descriptor.id },
                    expected_version: version,
                    change,
                }],
                guards: vec![guard.clone().into_sealed()],
            }],
            lifecycle: vec![guard],
            result: serde_json::to_value(&descriptor).map_err(|_| not_found())?,
            _owner: owner,
        })
    }

    /// `table.search_status`: the Catalog's column states with Control's
    /// ready reports. The SDK shape: `{"columns": {name: {state, lag_ms,
    /// pending_embeddings, failed_embeddings, recall}}}`.
    pub(super) async fn search_status(
        &self,
        pin: &PinnedCatalog,
        table: &TableRecord,
    ) -> Result<serde_json::Value, PlanError> {
        let Some(search) = table.search.as_ref() else {
            return Ok(serde_json::json!({ "columns": {} }));
        };
        let groups = self.route_groups(pin, table.descriptor.id).await?;
        let readiness = self.readiness(table.descriptor.id).await?;
        let (pending, failed) = readiness.iter().fold((0u64, 0u64), |(p, f), range| {
            (p.saturating_add(range.pending_embeddings), f.saturating_add(range.failed_embeddings))
        });
        let mut columns = serde_json::Map::new();
        for column in &search.columns {
            let state = match &column.state {
                ColumnLife::Ready => "ready",
                ColumnLife::Failed(_) => "failed",
                ColumnLife::Building | ColumnLife::Dropping => "building",
            };
            // The largest lag over the ranges, each at its best ready replica.
            let lag = groups
                .iter()
                .map(|group| {
                    readiness
                        .iter()
                        .find(|range| range.group_id == *group && range.ready.contains(&column.id))
                        .and_then(|range| range.lag_ms)
                })
                .try_fold(0u32, |worst, lag| lag.map(|lag| worst.max(lag)));
            let embedded = matches!(&column.kind, CatalogSearchKind::Vector(vector) if vector.source.is_some());
            columns.insert(
                column.name.as_str().to_owned(),
                serde_json::json!({
                    "state": state,
                    "lag_ms": lag.filter(|_| !groups.is_empty()),
                    "pending_embeddings": if embedded { pending } else { 0 },
                    "failed_embeddings": if embedded { failed } else { 0 },
                    "recall": null,
                }),
            );
        }
        Ok(serde_json::json!({ "columns": columns }))
    }
    /// The group ids the table routes to at `pin`, without probing them.
    async fn route_groups(&self, pin: &PinnedCatalog, table_id: Uuid) -> Result<Vec<Uuid>, PlanError> {
        let mut groups = Vec::new();
        let mut next = match self.catalog.routing_root(pin, table_id).await? {
            CatalogRead::Value { record: Some(root), .. } => root.first_page,
            CatalogRead::Value { record: None, .. } => None,
            CatalogRead::Blocked(intent) => return Err(PlanError::Blocked(intent)),
        };
        while let Some(page_id) = next {
            let page = match self.catalog.routing_page(pin, table_id, page_id).await? {
                CatalogRead::Value { record: Some(page), .. } => page,
                CatalogRead::Value { record: None, .. } => return Err(not_found().into()),
                CatalogRead::Blocked(intent) => return Err(PlanError::Blocked(intent)),
            };
            if groups.len().saturating_add(page.shards.len()) > MAX_SEARCH_GROUPS {
                return Err(invalid("too many ranges for one search status").into());
            }
            groups.extend(page.shards.iter().map(|route| route.group.group_id));
            next = page.next_page;
        }
        Ok(groups)
    }
}

/// The fence of a DDL that keeps the columns (`prepare_lifecycle_fence`).
pub(crate) enum LifecycleFence {
    Direct(Uuid),
    Search {
        groups: SearchGroups,
        epoch: u64,
        template: SearchFenceTemplate,
    },
}

#[cfg(test)]
mod tests;
