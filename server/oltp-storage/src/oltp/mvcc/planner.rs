//! ORM operation planning. Reads produce bounded encoded payloads; mutations
//! produce fixed changes and their exact result, never a replayable Operation.
use super::{
    CanonicalJson, Change, MvccKey, PreparedWrite,
    catalog::{
        CatalogHandle, CatalogRead, IntentRef, NameRecord, OwnedRecord, PinnedCatalog, PointCatalog, RouteRead,
        RoutingPage, RoutingRoot, ShardRoute, StoredRow, TableRecord, decode_record,
    },
    store::{
        ParticipantHandle,
        lifecycle::{GuardMode, LifecycleBinding, VerifiedLifecycleGuard},
    },
};
use crate::{
    oltp::{failure, pool::ChargedBytes},
    protocol::{self, ErrorCode, Key, Operation, ProtocolError, Row, TableDescriptor},
};
use nabedra_oltp_types::{
    authority::ScopedCatalogRead,
    types::{GroupIdentity, GroupKind, Page, RequestBinding, TxnId},
};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};
use uuid::Uuid;
mod backup;
#[doc(hidden)]
pub mod direct_fence;
mod index;
#[doc(hidden)]
pub use index::index_entry_changes;
mod index_build;
mod migration;
#[cfg(any(test, feature = "test-hooks"))]
mod point_route;
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use point_route::PointRoute;
mod query;
use query::schedule::QueryScheduler;
mod query_metrics;
#[doc(hidden)]
pub mod split;
pub type ResolveFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProtocolError>> + Send + 'a>>;
pub trait ParticipantResolver: Send + Sync {
    fn participant<'a>(
        &'a self,
        group: &'a GroupIdentity,
    ) -> ResolveFuture<'a, Arc<ParticipantHandle>>;
    /// Must enroll/fence the deterministic new group and its required voters
    /// before returning. This does not publish its route in the catalog.
    fn create_data<'a>(
        &'a self,
        table: &'a TableDescriptor,
        pin: &'a PinnedCatalog,
    ) -> ResolveFuture<'a, ShardRoute>;
}
pub struct ScopedParticipantResolver {
    pub resolver: Arc<dyn ParticipantResolver>,
    pub scope: ScopedCatalogRead,
}
impl ParticipantResolver for ScopedParticipantResolver {
    fn participant<'a>(
        &'a self,
        group: &'a GroupIdentity,
    ) -> ResolveFuture<'a, Arc<ParticipantHandle>> {
        Box::pin(async move {
            if group.group_id != self.scope.queue_group_id
                || !matches!(group.kind, GroupKind::Data { table_id, .. } if table_id == self.scope.table_id)
            {
                return Err(failure(
                    ErrorCode::StorageUnavailable,
                    "pinned row belongs to another data group",
                ));
            }
            Ok(Arc::new(
                self.resolver
                    .participant(group)
                    .await?
                    .with_scoped_execution(self.scope)?,
            ))
        })
    }

    fn create_data<'a>(
        &'a self,
        _: &'a TableDescriptor,
        _: &'a PinnedCatalog,
    ) -> ResolveFuture<'a, ShardRoute> {
        Box::pin(async {
            Err(failure(
                ErrorCode::UnsupportedOperation,
                "scoped requests use an existing data group",
            ))
        })
    }
}
#[derive(Debug)]
pub enum PlanError {
    Protocol(ProtocolError),
    Blocked(IntentRef),
    LifecyclePending(LifecycleBinding),
    Guarded {
        cause: Box<PlanError>,
        guards: Vec<VerifiedLifecycleGuard>,
    },
}
impl From<ProtocolError> for PlanError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}
pub enum PlannedOperation {
    Read(ChargedBytes),
    Mutation(MutationPlan),
}
pub struct MutationPlan {
    pub participants: Vec<ParticipantWrites>,
    pub lifecycle: Vec<VerifiedLifecycleGuard>,
    pub result: serde_json::Value,
    _owner: Arc<crate::oltp::pool::ScratchLease>,
}
pub struct ParticipantWrites {
    pub participant: Arc<ParticipantHandle>,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub writes: Vec<PreparedWrite>,
    pub guards: Vec<super::store::lifecycle::SealedLifecycleGuard>,
}
#[derive(Clone)]
pub struct NativePlanner {
    catalog: CatalogHandle,
    resolver: Arc<dyn ParticipantResolver>,
    query_metrics: Arc<query_metrics::QueryMetrics>,
    query_scheduler: QueryScheduler,
    direct: direct_fence::DirectProbeSlot,
}
/// Retain only one charged route page and its selected participant during a
/// mutation plan. The cache cannot outlive or be reused with another pin.
#[derive(Default)]
struct MutationMetadata {
    route: Option<(OwnedRecord<RoutingPage>, usize, Arc<ParticipantHandle>)>,
    indexes: Option<Option<OwnedRecord<super::index::IndexRoot>>>,
}
type MutationGroups = BTreeMap<GroupIdentity, (Arc<ParticipantHandle>, Vec<PreparedWrite>)>;
struct CandidateRow {
    key: Key,
    canonical: CanonicalJson,
    insert: bool,
}
impl NativePlanner {
    pub fn new(
        catalog: CatalogHandle,
        resolver: Arc<dyn ParticipantResolver>,
        max_workers_per_request: u64,
    ) -> Result<Self, ProtocolError> {
        let query_metrics = Arc::new(query_metrics::QueryMetrics::new(catalog.resources())?);
        let query_scheduler = QueryScheduler::new(max_workers_per_request, catalog.resources())?;
        Ok(Self {
            catalog,
            resolver,
            query_metrics,
            query_scheduler,
            direct: Default::default(),
        })
    }
    pub(crate) fn with_scoped_execution(
        &self,
        scope: ScopedCatalogRead,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            catalog: CatalogHandle::new(Arc::new(
                self.catalog.participant().with_scoped_execution(scope)?,
            ))?,
            resolver: Arc::new(ScopedParticipantResolver {
                resolver: self.resolver.clone(),
                scope,
            }),
            query_metrics: self.query_metrics.clone(),
            query_scheduler: self.query_scheduler.clone(),
            direct: self.direct.clone(),
        })
    }

    pub async fn plan_scoped(
        &self,
        scope: ScopedCatalogRead,
        operation: Operation,
        pin: &PinnedCatalog,
        txn: TxnId,
        owner: Arc<crate::oltp::pool::ScratchLease>,
        point: Option<&PointCatalog>,
    ) -> Result<PlannedOperation, PlanError> {
        if !matches!(&operation, Operation::Get(_) | Operation::Update(_))
            || table_id(&operation) != Some(scope.table_id)
            || txn.request_id != scope.request.request_id
            || txn.attempt != 1
            || pin.read_ts != scope.read_ts
            || pin.authority.run_id != scope.request.run_id
        {
            return Err(failure(
                ErrorCode::ValidationError,
                "operation differs from scoped request",
            )
            .into());
        }
        self.with_scoped_execution(scope)?
            .plan(operation, pin, txn, scope.request, owner, point)
            .await
    }
    pub fn query_statistics(&self) -> query_metrics::QueryStatistics {
        self.query_metrics.snapshot()
    }
    pub(crate) fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }
    pub async fn plan(
        &self,
        operation: Operation,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        owner: Arc<crate::oltp::pool::ScratchLease>,
        point: Option<&PointCatalog>,
    ) -> Result<PlannedOperation, PlanError> {
        let operation = self
            .cpu(owner.clone(), move || {
                operation.validate()?;
                Ok(operation)
            })
            .await?;
        let encoded_operation = self
            .cpu(owner.clone(), move || {
                let bytes =
                    crate::oltp::query::check_body(&operation, super::MAX_FIXED_COMMAND_BYTES)?;
                Ok((operation, bytes))
            })
            .await?;
        let (operation, encoded) = encoded_operation;
        let owner = self.catalog.resources().ensure_scratch_budget(
            owner,
            encoded
                .checked_mul(4)
                .and_then(|n| n.checked_add(64 << 10))
                .ok_or_else(not_found)?,
        )?;
        match operation {
            Operation::Create(payload) => {
                return self
                    .create(payload, pin, txn, request, owner.clone())
                    .await
                    .map(PlannedOperation::Mutation);
            }
            Operation::Describe(payload) => {
                let (_, table) = value(self.catalog.describe_name(pin, &payload.name).await?)?;
                let table = table.ok_or_else(not_found)?;
                return Ok(PlannedOperation::Read(
                    self.catalog
                        .resources()
                        .encode(&table.descriptor, protocol::MAX_BODY_BYTES - 128)?,
                ));
            }
            Operation::List(_) => return self.list(pin, owner).await.map(PlannedOperation::Read),
            _ => {}
        }
        operation.check_queued(true)?;
        let id = table_id(&operation).ok_or_else(not_found)?;
        let point = point.filter(|point| point.table.descriptor.id == id);
        let (version, table) = match point {
            Some(point) => (point.table_version, point.table.clone()),
            None => {
                let (version, record) = value(self.catalog.describe(pin, id).await?)?;
                (version, record.ok_or_else(not_found)?)
            }
        };
        let checked_table = table.clone();
        let operation = self
            .cpu(owner.clone(), move || {
                operation.validate_for_table(&checked_table.descriptor)?;
                Ok(operation)
            })
            .await?;
        if let Operation::Drop(_) = operation {
            return self
                .drop_table(pin, txn, request, table, version, owner.clone())
                .await
                .map(PlannedOperation::Mutation);
        }
        if let Operation::Get(payload) = operation {
            let (participant, _) = self.route(pin, id, &payload.key, point).await?;
            let (_, row) = self.row_on(pin, id, &payload.key, &participant).await?;
            let shared = self.catalog.resources().clone();
            return Ok(PlannedOperation::Read(
                self.cpu(owner.clone(), move || match row {
                    Some(row) => shared.encode(&*row, protocol::MAX_BODY_BYTES - 128),
                    None => shared.copy_bytes(b"null"),
                })
                .await?,
            ));
        }
        match operation {
            Operation::Collect(p) => {
                return self
                    .query(pin, table, p.query, false, request, owner)
                    .await
                    .map(PlannedOperation::Read);
            }
            Operation::Count(p) => {
                return self
                    .query(pin, table, p.query, true, request, owner)
                    .await
                    .map(PlannedOperation::Read);
            }
            Operation::Explain(p) => {
                return self
                    .explain(pin, table, p.query, owner)
                    .await
                    .map(PlannedOperation::Read);
            }
            _ => {}
        }
        let guard = self
            .guard(
                pin,
                txn,
                request,
                id,
                version,
                table.schema_epoch,
                GuardMode::Shared,
            )
            .await?;
        let planned = async {
            let mut metadata = MutationMetadata::default();
            let mut mutation_owner = owner.clone();
            let mut used = mutation_owner.bytes();
            let sealed = guard.clone().into_sealed();
            let mut grouped: BTreeMap<GroupIdentity, (Arc<ParticipantHandle>, Vec<PreparedWrite>)> =
                BTreeMap::new();
            let mut result = serde_json::Value::Null;
            let rows = match operation {
                Operation::Insert(p) => vec![(p.row, true)],
                Operation::Upsert(p) => vec![(p.row, false)],
                Operation::InsertMany(p) => {
                    result = serde_json::json!(p.rows.len());
                    p.rows.into_iter().map(|r| (r, true)).collect()
                }
                Operation::Update(p) => {
                    let (participant, group) = self
                        .mutation_route(pin, id, &p.key, &mut metadata, point)
                        .await?;
                    let (expected, row) = self.row_on(pin, id, &p.key, &participant).await?;
                    let old_index_row = row.clone();
                    let change = if let Some(row) = row {
                        let shared = self.catalog.resources().clone();
                        let values = p.values;
                        let canonical = self
                            .cpu(owner.clone(), move || {
                                let candidate = updated_row(&row, values);
                                CanonicalJson::from_serializable(
                                    &shared,
                                    &candidate,
                                    super::MAX_FIXED_COMMAND_BYTES,
                                )
                            })
                            .await?;
                        result = serde_json::Value::Bool(true);
                        Change::Put(canonical)
                    } else {
                        result = serde_json::Value::Bool(false);
                        Change::Keep
                    };
                    let new_index_row = match &change {
                        Change::Put(c) => Some(c.clone()),
                        _ => None,
                    };
                    let mut indexes = self
                        .index_changes(
                            pin,
                            table.clone(),
                            participant.clone(),
                            &p.key,
                            old_index_row,
                            new_index_row,
                            &mut mutation_owner,
                            &mut used,
                            &mut metadata,
                        )
                        .await?;
                    grouped
                        .entry(group)
                        .or_insert_with(|| (participant.clone(), Vec::new()))
                        .1
                        .push(PreparedWrite {
                            key: MvccKey::Row {
                                table_id: id,
                                primary_key: p.key,
                            },
                            expected_version: expected,
                            change,
                        });
                    grouped
                        .get_mut(&participant.identity().clone())
                        .ok_or_else(not_found)?
                        .1
                        .append(&mut indexes);
                    Vec::new()
                }
                Operation::Delete(p) => {
                    let (participant, group) =
                        self.mutation_route(pin, id, &p.key, &mut metadata, None).await?;
                    let (expected, row) = self.row_on(pin, id, &p.key, &participant).await?;
                    let found = row.is_some();
                    result = serde_json::Value::Bool(found);
                    let mut indexes = self
                        .index_changes(
                            pin,
                            table.clone(),
                            participant.clone(),
                            &p.key,
                            row.clone(),
                            None,
                            &mut mutation_owner,
                            &mut used,
                            &mut metadata,
                        )
                        .await?;
                    grouped
                        .entry(group)
                        .or_insert_with(|| (participant.clone(), Vec::new()))
                        .1
                        .push(PreparedWrite {
                            key: MvccKey::Row {
                                table_id: id,
                                primary_key: p.key,
                            },
                            expected_version: expected,
                            change: if found { Change::Delete } else { Change::Keep },
                        });
                    grouped
                        .get_mut(&participant.identity().clone())
                        .ok_or_else(not_found)?
                        .1
                        .append(&mut indexes);
                    Vec::new()
                }
                _ => {
                    return Err(
                        failure(ErrorCode::UnsupportedOperation, "invalid row mutation").into(),
                    );
                }
            };
            let mut seen = std::collections::BTreeSet::new();
            let mut pending = Vec::with_capacity(128);
            let mut pending_participant: Option<Arc<ParticipantHandle>> = None;
            let mut pending_bytes = 4096usize;
            for (row, insert) in rows {
                let column = table.descriptor.key.clone();
                let shared = self.catalog.resources().clone();
                let (key, key_order, canonical) = self
                    .cpu(owner.clone(), move || {
                        let key = primary_key_of(&column, &row);
                        let order = key_sort(&key);
                        let canonical = CanonicalJson::from_serializable(
                            &shared,
                            &row,
                            super::MAX_FIXED_COMMAND_BYTES,
                        )?;
                        Ok((key, order, canonical))
                    })
                    .await?;
                if !seen.insert(key_order) {
                    return Err(
                        failure(ErrorCode::Conflict, "duplicate primary key in batch").into(),
                    );
                }
                let (participant, group) =
                    self.mutation_route(pin, id, &key, &mut metadata, None).await?;
                // Bound both native request ownership and its escaped JSON wire
                // encoding. Oversized individual keys retain the scalar path.
                let cost = match &key {
                    Key::Utf8(value) => {
                        value.len().checked_mul(6).and_then(|n| n.checked_add(1024))
                    }
                    _ => Some(1024),
                }
                .ok_or_else(not_found)?;
                if !pending.is_empty()
                    && (pending.len() == 128
                        || pending_bytes.checked_add(cost).is_none_or(|n| n > 1 << 20)
                        || pending_participant
                            .as_ref()
                            .is_some_and(|p| p.identity() != &group))
                {
                    self.plan_candidate_page(
                        pin,
                        table.clone(),
                        pending_participant.take().ok_or_else(not_found)?,
                        std::mem::take(&mut pending),
                        &mut grouped,
                        &mut mutation_owner,
                        &mut used,
                        &mut metadata,
                    )
                    .await?;
                    pending_bytes = 4096;
                }
                pending_bytes = pending_bytes.checked_add(cost).ok_or_else(not_found)?;
                pending.push(CandidateRow {
                    key,
                    canonical,
                    insert,
                });
                pending_participant = Some(participant);
                if cost > (1 << 20) - 4096 {
                    self.plan_candidate_page(
                        pin,
                        table.clone(),
                        pending_participant.take().ok_or_else(not_found)?,
                        std::mem::take(&mut pending),
                        &mut grouped,
                        &mut mutation_owner,
                        &mut used,
                        &mut metadata,
                    )
                    .await?;
                    pending_bytes = 4096;
                }
            }
            if !pending.is_empty() {
                self.plan_candidate_page(
                    pin,
                    table.clone(),
                    pending_participant.take().ok_or_else(not_found)?,
                    pending,
                    &mut grouped,
                    &mut mutation_owner,
                    &mut used,
                    &mut metadata,
                )
                .await?;
            }
            // Empty insert_many still validates and locks the captured table
            // dependency, then records its exact zero result through Status.
            if grouped.is_empty() {
                grouped.insert(
                    self.catalog.participant().identity().clone(),
                    (
                        self.catalog.participant().clone(),
                        vec![PreparedWrite {
                            key: MvccKey::CatalogTable { table_id: id },
                            expected_version: version,
                            change: Change::Keep,
                        }],
                    ),
                );
            }
            let catalog_epochs = self.catalog.participant().epochs().await?;
            let mut participants = Vec::new();
            for (_, (participant, mut writes)) in grouped {
                writes.sort_by(|a, b| super::store::compare_keys(&a.key, &b.key));
                let (schema_epoch, routing_epoch) = if matches!(
                    participant.identity().kind,
                    nabedra_oltp_types::types::GroupKind::Catalog
                ) {
                    (catalog_epochs.schema_epoch, catalog_epochs.routing_epoch)
                } else {
                    // A split advances the Catalog routing version while
                    // unaffected participants retain their own immutable
                    // lineage epoch. Read it once per grouped participant.
                    let epochs = participant.epochs().await?;
                    if epochs.schema_epoch != table.schema_epoch
                        || epochs.routing_epoch == 0
                        || epochs.routing_epoch > table.routing_epoch
                    {
                        return Err(failure(
                            ErrorCode::Conflict,
                            "participant routing lineage changed",
                        )
                        .into());
                    }
                    (epochs.schema_epoch, epochs.routing_epoch)
                };
                participants.push(ParticipantWrites {
                    schema_epoch,
                    routing_epoch,
                    participant,
                    writes,
                    guards: vec![sealed.clone()],
                });
            }
            Ok(PlannedOperation::Mutation(MutationPlan {
                participants,
                lifecycle: vec![guard.clone()],
                result,
                _owner: mutation_owner,
            }))
        }
        .await;
        planned.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }

    async fn create(
        &self,
        payload: protocol::CreatePayload,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let name = nabedra_oltp_types::types::Text::try_from(payload.name.as_str())
            .map_err(|_| not_found())?;
        let (name_version, existing) = value(
            self.catalog
                .read::<NameRecord>(pin, MvccKey::CatalogName { name })
                .await?,
        )?;
        if existing.is_some() {
            return Err(failure(ErrorCode::Conflict, "table name already exists").into());
        }
        let id = derived_uuid(
            b"nabedra/table-id/v1",
            self.catalog.participant().identity().group_id,
            txn.request_id,
        );
        #[cfg(any(test, feature = "test-hooks"))]
        crate::oltp::test_hooks::pool_sync::observe_create_plan(txn.request_id, id);
        let descriptor = TableDescriptor {
            id,
            name: payload.name,
            schema: payload.schema,
            key: payload.key,
            format: payload.format,
            search: payload.search,
        };
        descriptor.validate()?;
        let guard = self
            .guard(pin, txn, request, id, None, 1, GuardMode::Exclusive)
            .await?;
        let planned = async {
            let epochs = self.catalog.participant().epochs().await?;
            let route = self.resolver.create_data(&descriptor, pin).await?;
            let page = derived_uuid(b"nabedra/route-page/v1", id, txn.request_id);
            let mut writes = self
                .catalog
                .create_writes(descriptor.clone(), route, page, &guard)?;
            writes[0].expected_version = name_version;
            Ok(MutationPlan {
                participants: vec![ParticipantWrites {
                    participant: self.catalog.participant().clone(),
                    schema_epoch: epochs.schema_epoch,
                    routing_epoch: epochs.routing_epoch,
                    writes,
                    guards: vec![guard.clone().into_sealed()],
                }],
                lifecycle: vec![guard.clone()],
                result: serde_json::to_value(descriptor).map_err(|_| not_found())?,
                _owner: owner.clone(),
            })
        }
        .await;
        planned.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }

    async fn drop_table(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        table: OwnedRecord<TableRecord>,
        version: Option<nabedra_oltp_types::types::CommitTs>,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let id = table.descriptor.id;
        let guard = self
            .guard(
                pin,
                txn,
                request,
                id,
                version,
                table.schema_epoch,
                GuardMode::Exclusive,
            )
            .await?;
        let planned = async {
            let epochs = self.catalog.participant().epochs().await?;
            let name =
                nabedra_oltp_types::types::Text::try_from(table.descriptor.name.as_str())
                    .map_err(|_| not_found())?;
            let (name_version, name_record) = value(
                self.catalog
                    .read::<NameRecord>(pin, MvccKey::CatalogName { name: name.clone() })
                    .await?,
            )?;
            if name_record.is_none_or(|r| r.table_id != id) {
                return Err(not_found().into());
            }
            let (root_version, _) = value(
                self.catalog
                    .read::<RoutingRoot>(pin, MvccKey::CatalogRoutingRoot { table_id: id })
                    .await?,
            )?;
            // Plan section 7: stop direct writers in the Data log before the
            // Catalog publishes the drop. The exclusive guard is held.
            self.fence_direct_writers(pin, id, None).await?;
            let writes = vec![
                PreparedWrite {
                    key: MvccKey::CatalogName { name },
                    expected_version: name_version,
                    change: Change::Delete,
                },
                PreparedWrite {
                    key: MvccKey::CatalogTable { table_id: id },
                    expected_version: version,
                    change: Change::Delete,
                },
                PreparedWrite {
                    key: MvccKey::CatalogRoutingRoot { table_id: id },
                    expected_version: root_version,
                    change: Change::Delete,
                },
            ];
            Ok(MutationPlan {
                participants: vec![ParticipantWrites {
                    participant: self.catalog.participant().clone(),
                    schema_epoch: epochs.schema_epoch,
                    routing_epoch: epochs.routing_epoch,
                    writes,
                    guards: vec![guard.clone().into_sealed()],
                }],
                lifecycle: vec![guard.clone()],
                result: serde_json::Value::Null,
                _owner: owner.clone(),
            })
        }
        .await;
        planned.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }

    async fn cpu<T: Send + 'static>(
        &self,
        owner: Arc<crate::oltp::pool::ScratchLease>,
        work: impl FnOnce() -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let shared = self.catalog.resources();
        let native = shared
            .prepare(crate::oltp::pool::ExecutionClass::Query)
            .await?;
        let cpu = shared
            .scheduler()
            .reserve_on(
                shared.next_native_request_id()?,
                1,
                shared.scheduler_node_id(),
                0,
            )
            .await?;
        let (result, _guard) = native.run_with_guard((owner, cpu), work).await?;
        result
    }
    /// Reconstruct the exact binding of a settled queued attempt to release
    /// its guard after a coordinator interruption.
    pub async fn queued_lifecycle_binding(
        &self,
        operation: &Operation,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        granted_under: nabedra_oltp_types::types::Authority,
    ) -> Result<LifecycleBinding, PlanError> {
        let (id, version, schema_epoch) = if matches!(operation, Operation::Create(_)) {
            let id = derived_uuid(
                b"nabedra/table-id/v1",
                self.catalog.participant().identity().group_id,
                txn.request_id,
            );
            (id, None, 1)
        } else {
            let id = table_id(operation).ok_or_else(not_found)?;
            let (version, record) = value(self.catalog.describe(pin, id).await?)?;
            (id, version, record.ok_or_else(not_found)?.schema_epoch)
        };
        Ok(self.lifecycle_binding(pin, txn, request, id, version, schema_epoch, granted_under))
    }

    async fn guard(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        table_id: Uuid,
        lifecycle_version: Option<nabedra_oltp_types::types::CommitTs>,
        schema_epoch: u64,
        mode: GuardMode,
    ) -> Result<VerifiedLifecycleGuard, PlanError> {
        let current = self.catalog.participant().epochs().await?.authority;
        if current.run_id != pin.authority.run_id {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "pinned request belongs to an abandoned run",
            )
            .into());
        }
        self.guard_at(pin, txn, request, table_id, lifecycle_version, schema_epoch, mode, current)
            .await
    }

    async fn guard_at(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        table_id: Uuid,
        lifecycle_version: Option<nabedra_oltp_types::types::CommitTs>,
        schema_epoch: u64,
        mode: GuardMode,
        authority: nabedra_oltp_types::types::Authority,
    ) -> Result<VerifiedLifecycleGuard, PlanError> {
        let binding = self.lifecycle_binding(pin, txn, request, table_id, lifecycle_version, schema_epoch, authority);
        #[cfg(any(test, feature = "test-hooks"))]
        crate::oltp::test_hooks::index_faults::observe_lifecycle(&binding);
        self.catalog
            .participant()
            .acquire_lifecycle(binding.clone(), mode)
            .await?
            .ok_or(PlanError::LifecyclePending(binding))
    }

    fn lifecycle_binding(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        table_id: Uuid,
        lifecycle_version: Option<nabedra_oltp_types::types::CommitTs>,
        schema_epoch: u64,
        authority: nabedra_oltp_types::types::Authority,
    ) -> LifecycleBinding {
        LifecycleBinding {
            guard_id: derived_uuid(b"nabedra/lifecycle-guard/v1", table_id, txn.request_id),
            catalog_group_id: self.catalog.participant().identity().group_id,
            table_id,
            lifecycle_version,
            schema_epoch,
            txn,
            request,
            read_ts: pin.read_ts,
            authority,
        }
    }
    async fn route(
        &self,
        pin: &PinnedCatalog,
        table_id: Uuid,
        key: &Key,
        point: Option<&PointCatalog>,
    ) -> Result<(Arc<ParticipantHandle>, GroupIdentity), PlanError> {
        match self.route_page(pin, table_id, key, point).await? {
            RouteRead::Blocked(i) => Err(PlanError::Blocked(i)),
            RouteRead::Route { page, index } => {
                let group = page.shards[index].group.clone();
                Ok((self.resolver.participant(&group).await?, group))
            }
            RouteRead::Missing => Err(not_found().into()),
        }
    }
    async fn route_page(
        &self,
        pin: &PinnedCatalog,
        table_id: Uuid,
        key: &Key,
        point: Option<&PointCatalog>,
    ) -> Result<RouteRead, ProtocolError> {
        match point {
            Some(point) => {
                if let Some((page, index)) = &point.selected
                    && point.table.descriptor.id == table_id
                    && page.shards[*index].contains_key(key)
                {
                    return Ok(RouteRead::Route { page: page.clone(), index: *index });
                }
                self.catalog
                    .route_pages(pin, table_id, key, point.first_page)
                    .await
            }
            None => self.catalog.route(pin, table_id, key).await,
        }
    }
    async fn row_on(
        &self,
        pin: &PinnedCatalog,
        table_id: Uuid,
        key: &Key,
        participant: &ParticipantHandle,
    ) -> Result<
        (
            Option<nabedra_oltp_types::types::CommitTs>,
            Option<OwnedRecord<StoredRow>>,
        ),
        PlanError,
    > {
        #[cfg(any(test, feature = "test-hooks"))]
        let _point = nabedra_resources::perf_stage!("test.data.point").timer();
        value(
            decode_record(
                self.catalog.resources(),
                participant
                    .read_at(
                        pin.read_ts,
                        MvccKey::Row {
                            table_id,
                            primary_key: key.clone(),
                        },
                    )
                    .await?,
            )
            .await?,
        )
    }
    async fn plan_candidate_page(
        &self,
        pin: &PinnedCatalog,
        table: OwnedRecord<TableRecord>,
        participant: Arc<ParticipantHandle>,
        candidates: Vec<CandidateRow>,
        grouped: &mut MutationGroups,
        owner: &mut Arc<crate::oltp::pool::ScratchLease>,
        used: &mut usize,
        metadata: &mut MutationMetadata,
    ) -> Result<(), PlanError> {
        use super::store::{point::PointReadRequest, scan::ScanPage};
        let mut completed = 0;
        while completed < candidates.len() {
            let first = &candidates[completed];
            let oversized = matches!(&first.key, Key::Utf8(key) if key.len().saturating_mul(6).saturating_add(5120) > 1 << 20);
            if oversized {
                let (expected, old) = self
                    .row_on(pin, table.descriptor.id, &first.key, &participant)
                    .await?;
                self.append_candidate(
                    pin,
                    table.clone(),
                    participant.clone(),
                    first,
                    expected,
                    old,
                    grouped,
                    owner,
                    used,
                    metadata,
                )
                .await?;
                completed += 1;
                continue;
            }
            let keys = candidates[completed..]
                .iter()
                .map(|row| MvccKey::Row {
                    table_id: table.descriptor.id,
                    primary_key: row.key.clone(),
                })
                .collect();
            let request = nabedra_oltp_types::owned::OwnedCommand::new_owned(
                PointReadRequest {
                    read_ts: pin.read_ts,
                    keys: Page::new(keys).map_err(|_| not_found())?,
                    max_bytes: 16 << 20,
                },
                self.catalog.resources(),
                crate::oltp::pool::ByteClass::Data,
            )?;
            let page = participant.read_page(request).await?;
            match page.payload()? {
                ScanPage::Blocked { txn, manifest } => {
                    return Err(PlanError::Blocked(IntentRef {
                        txn: *txn,
                        manifest: *manifest,
                    }));
                }
                ScanPage::Records { records, exhausted } => {
                    if records.is_empty()
                        || records.len() > candidates.len() - completed
                        || *exhausted != (records.len() == candidates.len() - completed)
                    {
                        return Err(not_found().into());
                    }
                    for record in records.iter() {
                        let candidate = &candidates[completed];
                        if record.key
                            != (MvccKey::Row {
                                table_id: table.descriptor.id,
                                primary_key: candidate.key.clone(),
                            })
                        {
                            return Err(not_found().into());
                        }
                        let old = match &record.value {
                            Some(value) => Some(
                                super::catalog::decode_canonical(
                                    self.catalog.resources(),
                                    value.clone(),
                                )
                                .await?,
                            ),
                            None => None,
                        };
                        self.append_candidate(
                            pin,
                            table.clone(),
                            participant.clone(),
                            candidate,
                            record.version,
                            old,
                            grouped,
                            owner,
                            used,
                            metadata,
                        )
                        .await?;
                        completed += 1;
                    }
                }
            }
        }
        Ok(())
    }
    async fn append_candidate(
        &self,
        pin: &PinnedCatalog,
        table: OwnedRecord<TableRecord>,
        participant: Arc<ParticipantHandle>,
        candidate: &CandidateRow,
        expected: Option<nabedra_oltp_types::types::CommitTs>,
        old: Option<OwnedRecord<StoredRow>>,
        grouped: &mut MutationGroups,
        owner: &mut Arc<crate::oltp::pool::ScratchLease>,
        used: &mut usize,
        metadata: &mut MutationMetadata,
    ) -> Result<(), PlanError> {
        if candidate.insert && old.is_some() {
            return Err(failure(ErrorCode::Conflict, "primary key already exists").into());
        }
        let mut indexes = self
            .index_changes(
                pin,
                table.clone(),
                participant.clone(),
                &candidate.key,
                old,
                Some(candidate.canonical.clone()),
                owner,
                used,
                metadata,
            )
            .await?;
        let writes = &mut grouped
            .entry(participant.identity().clone())
            .or_insert_with(|| (participant.clone(), Vec::new()))
            .1;
        writes.push(PreparedWrite {
            key: MvccKey::Row {
                table_id: table.descriptor.id,
                primary_key: candidate.key.clone(),
            },
            expected_version: expected,
            change: Change::Put(candidate.canonical.clone()),
        });
        writes.append(&mut indexes);
        Ok(())
    }
    async fn mutation_route(
        &self,
        pin: &PinnedCatalog,
        table_id: Uuid,
        key: &Key,
        metadata: &mut MutationMetadata,
        point: Option<&PointCatalog>,
    ) -> Result<(Arc<ParticipantHandle>, GroupIdentity), PlanError> {
        if let Some((page, selected, participant)) = &metadata.route {
            if super::catalog::contains(&page.shards[*selected].span, key) {
                return Ok((participant.clone(), participant.identity().clone()));
            }
            if let Some(index) = page
                .shards
                .iter()
                .position(|route| super::catalog::contains(&route.span, key))
            {
                let group = page.shards[index].group.clone();
                let participant = self.resolver.participant(&group).await?;
                metadata.route = Some((page.clone(), index, participant.clone()));
                return Ok((participant, group));
            }
        }
        match self.route_page(pin, table_id, key, point).await? {
            RouteRead::Blocked(i) => Err(PlanError::Blocked(i)),
            RouteRead::Missing => Err(not_found().into()),
            RouteRead::Route { page, index } => {
                let group = page.shards[index].group.clone();
                let participant = self.resolver.participant(&group).await?;
                metadata.route = Some((page, index, participant.clone()));
                Ok((participant, group))
            }
        }
    }
}
fn value<T>(
    read: CatalogRead<T>,
) -> Result<
    (
        Option<nabedra_oltp_types::types::CommitTs>,
        Option<OwnedRecord<T>>,
    ),
    PlanError,
> {
    match read {
        CatalogRead::Value { version, record } => Ok((version, record)),
        CatalogRead::Blocked(i) => Err(PlanError::Blocked(i)),
    }
}
/// The primary key of `row`, whose key column is `column`. Shared by the
/// queued planner and the direct executor.
pub fn primary_key_of(column: &str, row: &Row) -> Key {
    crate::oltp::codec::scalar_key(&row[column])
}
/// `table.update`: `values` written over `row`. Shared by the queued
/// planner and the direct executor.
pub fn updated_row(row: &Row, values: Row) -> Row {
    let mut candidate = row.clone();
    for (k, v) in values {
        candidate.insert(k, v);
    }
    candidate
}
fn table_id(op: &Operation) -> Option<Uuid> {
    match op {
        Operation::Drop(p) => Some(p.table_id),
        Operation::Insert(p) | Operation::Upsert(p) => Some(p.table_id),
        Operation::InsertMany(p) => Some(p.table_id),
        Operation::Get(p) | Operation::Delete(p) => Some(p.table_id),
        Operation::Update(p) => Some(p.table_id),
        Operation::Collect(p) | Operation::Count(p) | Operation::Explain(p) => Some(p.table_id),
        _ => None,
    }
}
fn key_sort(key: &Key) -> Vec<u8> {
    crate::oltp::codec::Namespace(0).row(Uuid::nil(), key)
}
fn derived_uuid(domain: &[u8], namespace: Uuid, request: Uuid) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(namespace.as_bytes());
    hash.update(request.as_bytes());
    let digest = hash.finalize();
    Uuid::from_bytes(digest[..16].try_into().expect("fixed hash prefix"))
}
fn not_found() -> ProtocolError {
    failure(ErrorCode::NotFound, "table or shard not found")
}
