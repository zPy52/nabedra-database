//! DDL of tables with search columns (plan section 7, contract-w2 2.6, spec
//! `specs/vector/lifecycle/IndexLifecycle.tla`).
//!
//! Every DDL of a search table runs under the table's Exclusive lifecycle
//! guard and fences every Data group with a `SearchFence` before the Catalog
//! commit that publishes it (`StartPublish`/`FenceRange`/`Publish`,
//! `StartDrop`/`FinishDrop`). A Data group takes only a larger search epoch
//! (DB-20), so an epoch must never be reused with other columns:
//!
//! - the coordinator first probes every group through its log and picks an
//!   epoch above the Catalog's and every group's (an interrupted DDL may
//!   have fenced some groups and never committed);
//! - new column ids are allocated above every group's `high` for the same
//!   reason;
//! - after each fence it reads the group's schema back and refuses to go on
//!   unless the group holds exactly what this DDL sent. A late fence of an
//!   interrupted DDL with the same epoch is then caught, never published.
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
    fn matches(&self, schema: &SearchSchema, epoch: u64) -> bool {
        schema.epoch == epoch
            && schema.columns == self.columns
            && schema.serving == self.serving
            && schema.text == self.text
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
pub(crate) struct SearchGroups {
    groups: Vec<(Arc<ParticipantHandle>, Option<SearchSchema>)>,
}
impl SearchGroups {
    pub(crate) fn ids(&self) -> Vec<Uuid> {
        self.groups
            .iter()
            .map(|(participant, _)| participant.identity().group_id)
            .collect()
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
        self.probe_groups(groups).await
    }
    pub(crate) async fn probe_groups(
        &self,
        participants: Vec<Arc<ParticipantHandle>>,
    ) -> Result<SearchGroups, PlanError> {
        let mut groups = Vec::with_capacity(participants.len());
        for participant in participants {
            let schema = participant.search_probe().await?;
            groups.push((participant, schema));
        }
        Ok(SearchGroups { groups })
    }

    /// IndexLifecycle `SendFences` and `FenceRange`: fences every group with
    /// `template` at `epoch` and checks that each now holds exactly it. Call
    /// it with the table's Exclusive guard held, before the Catalog commit,
    /// with an epoch from `SearchGroups::next_epoch`. Returns the epoch.
    #[allow(clippy::result_large_err)] // `PlanError`, like the rest of the planner.
    pub(crate) async fn fence_search(
        &self,
        groups: &SearchGroups,
        table_id: Uuid,
        epoch: u64,
        template: &SearchFenceTemplate,
    ) -> Result<u64, PlanError> {
        for (participant, _) in &groups.groups {
            let fence = SearchFence {
                authority: participant.epochs().await?.authority,
                table_id,
                epoch,
                lifecycle: template.lifecycle,
                columns: template.columns.clone(),
                serving: template.serving.clone(),
                text: template.text.clone(),
            };
            let mut backoff = Duration::from_millis(50);
            let mut tries = 0;
            let direct = loop {
                match participant.search_fence(fence.clone()).await? {
                    SearchReply::Fenced { epoch: applied, direct } if applied == epoch => break direct,
                    SearchReply::Fenced { .. } => {
                        return Err(conflict("the search fence was overtaken by another DDL").into());
                    }
                    SearchReply::Rejected(crate::oltp::mvcc::store::Fault::SplitInProgress)
                        if tries < SPLIT_RETRIES =>
                    {
                        tries += 1;
                        nabedra_rt::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(1));
                    }
                    SearchReply::Rejected(crate::oltp::mvcc::store::Fault::SplitInProgress) => {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "a split holds a range of the table; retry the search DDL",
                        )
                        .into());
                    }
                    _ => return Err(invalid("a Data group refused the search fence").into()),
                }
            };
            // A repeat of the same epoch answers like a new fence: only the
            // installed schema tells whether it is this DDL's.
            let installed = participant.search_probe().await?;
            if direct.is_none_or(|direct| direct.lifecycle != template.lifecycle)
                || !installed.is_some_and(|schema| template.matches(&schema, epoch))
            {
                return Err(conflict(
                    "an interrupted search DDL used this epoch; retry the request",
                )
                .into());
            }
        }
        Ok(epoch)
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
            LifecycleFence::Search { groups, epoch, template } => {
                self.fence_search(&groups, table_id, epoch, &template).await?;
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
            let groups = self.search_groups(pin, id).await?;
            let current = table.search.clone().unwrap_or_else(CatalogSearch::empty);
            let base = groups.raise_high(current.clone());
            let mut descriptor = table.descriptor.clone();
            let next = match operation {
                Operation::AddColumn(p) => {
                    descriptor.schema.insert(p.name.clone(), p.r#type);
                    if let Some(embed) = &p.embed {
                        descriptor.search.embed.insert(p.name.clone(), embed.clone());
                    }
                    let column = vector_column(p.r#type, p.embed.as_ref())?;
                    base.add_vector(&p.name, column, ColumnLife::Building)?
                }
                Operation::DropColumn(p) => {
                    descriptor.schema.remove(&p.name);
                    descriptor.search.embed.remove(&p.name);
                    base.drop_named(&p.name, false)?
                }
                Operation::AddFullText(p) => {
                    descriptor
                        .search
                        .full_text
                        .insert(p.column.clone(), p.language.clone());
                    base.add_text(&p.column, &p.language, ColumnLife::Building)?
                }
                Operation::DropFullText(p) => {
                    descriptor.search.full_text.remove(&p.column);
                    base.drop_named(&p.column, true)?
                }
                _ => return Err(not_found().into()),
            };
            descriptor.validate()?;
            self.fence_and_publish(&table, version, descriptor, next, &groups, txn, guard.clone(), owner.clone())
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
            let groups = self.search_groups(pin, table_id).await?;
            let readiness = self.readiness(table_id).await?;
            if !all_ready(&groups.ids(), &readiness, column) {
                return Err(conflict("a range has no ready replica of the column yet").into());
            }
            let next = groups.raise_high(current).publish(column)?;
            self.fence_and_publish(
                &table,
                version,
                table.descriptor.clone(),
                next,
                &groups,
                txn,
                guard.clone(),
                owner.clone(),
            )
            .await
        }
        .await;
        planned.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn fence_and_publish(
        &self,
        table: &TableRecord,
        version: Option<CommitTs>,
        descriptor: TableDescriptor,
        mut next: CatalogSearch,
        groups: &SearchGroups,
        txn: TxnId,
        guard: VerifiedLifecycleGuard,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let lifecycle = txn.request_id;
        let catalog_epoch = table.search.as_ref().map_or(0, |search| search.epoch);
        let epoch = groups.next_epoch(catalog_epoch)?;
        self.fence_search(groups, descriptor.id, epoch, &next.template(Some(lifecycle)))
            .await?;
        next.epoch = epoch;
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
