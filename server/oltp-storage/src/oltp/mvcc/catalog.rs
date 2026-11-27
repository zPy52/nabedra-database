//! Catalog reads share the application's pinned MVCC timestamp. The snapshot
//! token is bounded; table and routing pages stay on disk until requested.
use super::{
    CanonicalJson, Change, MvccKey, PreparedWrite, json_decode_budget,
    store::{
        KeySpan, ParticipantHandle,
        lifecycle::{GuardMode, VerifiedLifecycleGuard},
    },
    vector::{ColumnId, SearchColumn},
};
use crate::{
    oltp::{
        failure,
        pool::{ChargedBytes, ExecutionClass, ScratchLease, WorkerSharedResources},
    },
    protocol::{ErrorCode, Key, ProtocolError, TableDescriptor},
};
use nabedra_oltp_types::{
    authority::{IdentityReservation, QueueReadToken, ScopedCatalogRead, ScopedQueuePlacement},
    types::{
        Authority, CommitTs, Digest, GroupIdentity, GroupKind, ManifestRef, Page, ReadTs,
        RequestBinding, Text, TxnId,
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::sync::Arc;
use uuid::Uuid;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedCatalog {
    pub catalog: GroupIdentity,
    pub read_ts: ReadTs,
    pub authority: Authority,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRecord {
    pub descriptor: TableDescriptor,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    /// Routing-only versions retain this identity. Schema/index transitions
    /// replace it, so an old pin cannot omit a newly Building index.
    #[serde(default, skip_serializing_if = "Uuid::is_nil")]
    pub lifecycle_id: Uuid,
    /// The table's search columns (contract-w2 2.6); `None` for a table
    /// without them. Not written when `None`, so such a table's record keeps
    /// its bytes (the golden tests stay as they are).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search: Option<CatalogSearch>,
}
/// The Catalog's view of a table's search columns. `epoch` is raised in a
/// committed Catalog write before any `SearchFence` uses it (DB-20).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSearch {
    pub epoch: u64,
    /// The highest column id the table ever had; ids are never reused.
    pub high: ColumnId,
    pub columns: Vec<CatalogSearchColumn>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSearchColumn {
    pub id: ColumnId,
    pub name: Text<128>,
    pub kind: CatalogSearchKind,
    pub state: ColumnLife,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CatalogSearchKind {
    Vector(SearchColumn),
    FullText {
        column: Text<128>,
        language: Text<32>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ColumnLife {
    Building,
    Ready,
    Dropping,
    Failed(Text<256>),
}
impl TableRecord {
    pub(super) fn with_lifecycle(&self, id: Uuid) -> Self {
        Self {
            lifecycle_id: id,
            ..self.clone()
        }
    }
}
pub(super) fn lifecycle_anchor(
    table_id: Uuid,
    schema_epoch: u64,
    version: Option<CommitTs>,
    stored: Uuid,
) -> Result<Uuid, ProtocolError> {
    if !stored.is_nil() {
        return Ok(stored);
    }
    let version = version.ok_or_else(invalid)?;
    let digest = nabedra_oltp_types::owned::serialized_digest(&(
        b"nabedra/legacy-table-lifecycle/v1".as_slice(),
        table_id,
        schema_epoch,
        version,
    ))?;
    Uuid::from_slice(&digest.as_bytes()[..16]).map_err(|_| invalid())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NameRecord {
    pub table_id: Uuid,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingRoot {
    pub first_page: Option<Uuid>,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardRoute {
    pub group: GroupIdentity,
    pub span: KeySpan,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
}
impl ShardRoute {
    pub fn contains_key(&self, key: &Key) -> bool {
        contains(&self.span, key)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingPage {
    pub shards: Page<ShardRoute>,
    pub next_page: Option<Uuid>,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct IntentRef {
    pub txn: TxnId,
    pub manifest: ManifestRef,
}
#[derive(Clone)]
pub struct CatalogHandle {
    participant: Arc<ParticipantHandle>,
}
pub struct OwnedRecord<T> {
    inner: Arc<RecordInner<T>>,
}
struct RecordInner<T> {
    value: T,
    _owner: Arc<ScratchLease>,
}
impl<T> Clone for OwnedRecord<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<T> std::ops::Deref for OwnedRecord<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner.value
    }
}
pub enum CatalogRead<T> {
    Value {
        version: Option<CommitTs>,
        record: Option<OwnedRecord<T>>,
    },
    Blocked(IntentRef),
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WireRead<'a> {
    Value {
        version: Option<CommitTs>,
        #[serde(borrow)]
        value: Option<&'a RawValue>,
    },
    Blocked {
        txn: TxnId,
        manifest: ManifestRef,
    },
}
impl CatalogHandle {
    pub fn new(participant: Arc<ParticipantHandle>) -> Result<Self, ProtocolError> {
        if !matches!(participant.identity().kind, GroupKind::Catalog) {
            return Err(invalid());
        }
        Ok(Self { participant })
    }
    pub fn participant(&self) -> &Arc<ParticipantHandle> {
        &self.participant
    }
    pub fn resources(&self) -> &Arc<WorkerSharedResources> {
        self.participant.resources()
    }
    pub fn snapshot(
        &self,
        read_ts: ReadTs,
        authority: Authority,
    ) -> Result<PinnedCatalog, ProtocolError> {
        authority.validate().map_err(|_| invalid())?;
        Ok(PinnedCatalog {
            catalog: self.participant.identity().clone(),
            read_ts,
            authority,
        })
    }
    fn check(&self, snapshot: &PinnedCatalog) -> Result<(), ProtocolError> {
        if snapshot.catalog != *self.participant.identity() {
            Err(invalid())
        } else {
            Ok(())
        }
    }
    pub async fn read<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        snapshot: &PinnedCatalog,
        key: MvccKey,
    ) -> Result<CatalogRead<T>, ProtocolError> {
        self.check(snapshot)?;
        decode_record(
            self.resources(),
            self.participant.read_at(snapshot.read_ts, key).await?,
        )
        .await
    }
    pub async fn describe(
        &self,
        snapshot: &PinnedCatalog,
        table_id: Uuid,
    ) -> Result<CatalogRead<TableRecord>, ProtocolError> {
        self.read(snapshot, MvccKey::CatalogTable { table_id })
            .await
    }
    pub(crate) async fn describe_name(
        &self,
        snapshot: &PinnedCatalog,
        name: &str,
    ) -> Result<CatalogRead<TableRecord>, ProtocolError> {
        let name = Text::try_from(name).map_err(|_| invalid())?;
        match self
            .read::<NameRecord>(snapshot, MvccKey::CatalogName { name })
            .await?
        {
            CatalogRead::Blocked(intent) => Ok(CatalogRead::Blocked(intent)),
            CatalogRead::Value {
                record: Some(record),
                ..
            } => self.describe(snapshot, record.table_id).await,
            CatalogRead::Value {
                version,
                record: None,
            } => Ok(CatalogRead::Value {
                version,
                record: None,
            }),
        }
    }
    pub async fn routing_root(
        &self,
        snapshot: &PinnedCatalog,
        table_id: Uuid,
    ) -> Result<CatalogRead<RoutingRoot>, ProtocolError> {
        self.read(snapshot, MvccKey::CatalogRoutingRoot { table_id })
            .await
    }
    pub async fn routing_page(
        &self,
        snapshot: &PinnedCatalog,
        table_id: Uuid,
        page_id: Uuid,
    ) -> Result<CatalogRead<RoutingPage>, ProtocolError> {
        self.read(snapshot, MvccKey::CatalogRoutingPage { table_id, page_id })
            .await
    }
    /// Linked bounded pages permit unlimited shard growth. A malformed cycle
    /// cannot be published by the guarded routing writer; no whole routing map
    /// is allocated by a point lookup.
    pub async fn route(
        &self,
        snapshot: &PinnedCatalog,
        table_id: Uuid,
        key: &Key,
    ) -> Result<RouteRead, ProtocolError> {
        let next = match self.routing_root(snapshot, table_id).await? {
            CatalogRead::Blocked(i) => return Ok(RouteRead::Blocked(i)),
            CatalogRead::Value {
                record: Some(root), ..
            } => root.first_page,
            _ => return Ok(RouteRead::Missing),
        };
        self.route_pages(snapshot, table_id, key, next).await
    }
    pub async fn scoped_route(
        &self,
        placement: &ScopedQueuePlacement,
        request: RequestBinding,
        token: &QueueReadToken,
        key: &Key,
    ) -> Result<RouteRead, ProtocolError> {
        placement.validate(&placement.group)?;
        let GroupKind::Data { table_id, .. } = placement.group.kind else {
            return Err(invalid());
        };
        let catalog = self.participant.identity();
        if placement.group.cluster_id != catalog.cluster_id
            || placement.group.database != catalog.database
            || request.run_id != placement.view.run_id
            || request.request_id.is_nil()
            || token.identity.run_id != request.run_id
            || token.identity.request_id != request.request_id
            || token.identity.digest != request.digest
            || token.identity.nonce.is_nil()
            || token.storage_pin.pin_id.is_nil()
            || token.storage_pin.allocator_epoch == 0
        {
            return Err(invalid());
        }
        let scope = ScopedCatalogRead {
            queue_group_id: placement.group.group_id,
            data_generation: placement.data_generation,
            queue_generation: placement.queue_generation,
            queue_view: placement.view,
            request,
            table_id,
            read_ts: token.storage_pin.read_ts,
        };
        let (table_version, table) = match self
            .read_scoped::<TableRecord>(scope, MvccKey::CatalogTable { table_id })
            .await?
        {
            CatalogRead::Blocked(intent) => return Ok(RouteRead::Blocked(intent)),
            CatalogRead::Value {
                version,
                record: Some(table),
            } => (version, table),
            CatalogRead::Value { record: None, .. } => return Ok(RouteRead::Missing),
        };
        let (routing_version, root) = match self
            .read_scoped::<RoutingRoot>(scope, MvccKey::CatalogRoutingRoot { table_id })
            .await?
        {
            CatalogRead::Blocked(intent) => return Ok(RouteRead::Blocked(intent)),
            CatalogRead::Value {
                version,
                record: Some(root),
            } => (version, root),
            CatalogRead::Value { record: None, .. } => return Ok(RouteRead::Missing),
        };
        let capture = CatalogCapture {
            pin: self.snapshot(scope.read_ts, placement.view.authority())?,
            catalog_root: token.catalog_root,
            table_id: Some(table_id),
            name_version: None,
            table_version,
            routing_version,
            schema_epoch: table.schema_epoch,
            routing_epoch: table.routing_epoch,
            point: None,
        };
        if table.descriptor.id != table_id
            || table.schema_epoch != token.schema_epoch
            || table.routing_epoch != token.routing_epoch
            || root.schema_epoch != table.schema_epoch
            || root.routing_epoch != table.routing_epoch
            || capture.lineage_digest()? != token.catalog_root
        {
            return Err(invalid());
        }
        key.validate(
            *table
                .descriptor
                .schema
                .get(&table.descriptor.key)
                .ok_or_else(invalid)?,
        )?;
        let route = self
            .read_route_pages(&capture.pin, table_id, key, root.first_page, Some(scope))
            .await?;
        if let RouteRead::Route { page, index } = &route {
            let route = &page.shards[*index];
            if route.group != placement.group
                || route.schema_epoch != table.schema_epoch
                || route.routing_epoch == 0
                || route.routing_epoch > table.routing_epoch
            {
                return Err(invalid());
            }
        }
        Ok(route)
    }
    async fn read_scoped<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        scope: ScopedCatalogRead,
        key: MvccKey,
    ) -> Result<CatalogRead<T>, ProtocolError> {
        decode_record(
            self.resources(),
            self.participant.read_scoped_catalog(scope, key).await?,
        )
        .await
    }
    pub async fn route_pages(
        &self,
        snapshot: &PinnedCatalog,
        table_id: Uuid,
        key: &Key,
        next: Option<Uuid>,
    ) -> Result<RouteRead, ProtocolError> {
        self.read_route_pages(snapshot, table_id, key, next, None)
            .await
    }
    async fn read_route_pages(
        &self,
        snapshot: &PinnedCatalog,
        table_id: Uuid,
        key: &Key,
        mut next: Option<Uuid>,
        scope: Option<ScopedCatalogRead>,
    ) -> Result<RouteRead, ProtocolError> {
        while let Some(page_id) = next {
            let read = match scope {
                Some(scope) => {
                    self.read_scoped::<RoutingPage>(
                        scope,
                        MvccKey::CatalogRoutingPage { table_id, page_id },
                    )
                    .await?
                }
                None => self.routing_page(snapshot, table_id, page_id).await?,
            };
            match read {
                CatalogRead::Blocked(i) => return Ok(RouteRead::Blocked(i)),
                CatalogRead::Value {
                    record: Some(page), ..
                } => {
                    for (index, route) in page.shards.iter().enumerate() {
                        if contains(&route.span, key) {
                            return Ok(RouteRead::Route { page, index });
                        }
                    }
                    next = page.next_page;
                }
                _ => return Err(invalid()),
            }
        }
        Ok(RouteRead::Missing)
    }
    /// Create publishes four MVCC keys together through the ordinary Status
    /// manifest. The new Data group must already be enrolled and fenced before
    /// this plan is proposed; there is no direct catalog visibility write.
    pub(crate) fn create_writes(
        &self,
        descriptor: TableDescriptor,
        route: ShardRoute,
        page_id: Uuid,
        guard: &VerifiedLifecycleGuard,
    ) -> Result<Vec<PreparedWrite>, ProtocolError> {
        descriptor.validate()?;
        let binding = guard.binding();
        if binding.table_id != descriptor.id
            || binding.catalog_group_id != self.participant.identity().group_id
            || binding.lifecycle_version.is_some()
            || guard.mode() != GuardMode::Exclusive
            || page_id.is_nil()
            || !matches!(route.group.kind,GroupKind::Data{table_id,..}if table_id==descriptor.id)
            || route.group.cluster_id != self.participant.identity().cluster_id
            || route.group.database != self.participant.identity().database
        {
            return Err(invalid());
        }
        let table_id = descriptor.id;
        let name = Text::try_from(descriptor.name.as_str()).map_err(|_| invalid())?;
        let root = RoutingRoot {
            first_page: Some(page_id),
            schema_epoch: route.schema_epoch,
            routing_epoch: route.routing_epoch,
        };
        let page = RoutingPage {
            shards: Page::new(vec![route.clone()]).map_err(|_| invalid())?,
            next_page: None,
        };
        let record = TableRecord {
            descriptor,
            schema_epoch: route.schema_epoch,
            routing_epoch: route.routing_epoch,
            lifecycle_id: table_id,
            // TODO(J): a table created with search columns records them here.
            search: None,
        };
        let entries = [
            (
                MvccKey::CatalogName { name },
                self.canonical(&NameRecord { table_id })?,
            ),
            (MvccKey::CatalogTable { table_id }, self.canonical(&record)?),
            (
                MvccKey::CatalogRoutingRoot { table_id },
                self.canonical(&root)?,
            ),
            (
                MvccKey::CatalogRoutingPage { table_id, page_id },
                self.canonical(&page)?,
            ),
        ];
        Ok(entries
            .into_iter()
            .map(|(key, value)| PreparedWrite {
                key,
                expected_version: None,
                change: Change::Put(value),
            })
            .collect())
    }
    fn canonical(&self, value: &impl Serialize) -> Result<CanonicalJson, ProtocolError> {
        CanonicalJson::from_serializable(self.resources(), value, crate::protocol::MAX_BODY_BYTES)
    }
}
pub enum RouteRead {
    Route {
        page: OwnedRecord<RoutingPage>,
        index: usize,
    },
    Blocked(IntentRef),
    Missing,
}
impl RouteRead {
    pub fn route(&self) -> Option<&ShardRoute> {
        match self {
            Self::Route { page, index } => page.shards.get(*index),
            _ => None,
        }
    }
}
pub(crate) async fn decode_record<T: DeserializeOwned + Send + Sync + 'static>(
    shared: &Arc<WorkerSharedResources>,
    raw: ChargedBytes,
) -> Result<CatalogRead<T>, ProtocolError> {
    let owner = Arc::new(
        shared
            .reserve_scratch_wait(
                crate::oltp::pool::ByteClass::Data,
                json_decode_budget(raw.as_slice())?,
                0,
            )
            .await?,
    );
    shared
        .execute(ExecutionClass::Query, 0, 0, move || {
            let decoded: WireRead<'_> =
                serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
            match decoded {
                WireRead::Blocked { txn, manifest } => {
                    Ok(CatalogRead::Blocked(IntentRef { txn, manifest }))
                }
                WireRead::Value { version, value } => {
                    let record = value
                        .map(|v| serde_json::from_str(v.get()).map_err(|_| invalid()))
                        .transpose()?;
                    Ok(CatalogRead::Value {
                        version,
                        record: record.map(|value| OwnedRecord {
                            inner: Arc::new(RecordInner {
                                value,
                                _owner: owner,
                            }),
                        }),
                    })
                }
            }
        })
        .await
}
pub async fn decode_canonical<T: DeserializeOwned + Send + Sync + 'static>(
    shared: &Arc<WorkerSharedResources>,
    raw: CanonicalJson,
) -> Result<OwnedRecord<T>, ProtocolError> {
    let owner = Arc::new(
        shared
            .reserve_scratch_wait(
                crate::oltp::pool::ByteClass::Data,
                json_decode_budget(raw.as_str().as_bytes())?,
                0,
            )
            .await?,
    );
    shared
        .execute(ExecutionClass::Query, 0, 0, move || {
            let value = serde_json::from_str(raw.as_str()).map_err(|_| invalid())?;
            Ok(OwnedRecord {
                inner: Arc::new(RecordInner {
                    value,
                    _owner: owner,
                }),
            })
        })
        .await
}

/// A stored row in any column types. `Row`'s own `Deserialize` takes the four
/// scalars only (the request grammar), so a row of a table with vector, list
/// or nullable columns decodes through `protocol::decode_row` instead.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredRow(pub crate::protocol::Row);
impl<'de> Deserialize<'de> for StoredRow {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(d)?;
        crate::protocol::decode_row(raw.get())
            .map(Self)
            .map_err(|_| serde::de::Error::custom("malformed stored row"))
    }
}
impl Serialize for StoredRow {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}
impl std::ops::Deref for StoredRow {
    type Target = crate::protocol::Row;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(super) fn contains(span: &KeySpan, key: &Key) -> bool {
    span.start
        .as_ref()
        .is_none_or(|a| primary(a, key) != std::cmp::Ordering::Greater)
        && span
            .end
            .as_ref()
            .is_none_or(|b| primary(key, b) == std::cmp::Ordering::Less)
}
pub(super) fn primary(a: &Key, b: &Key) -> std::cmp::Ordering {
    match (a, b) {
        (Key::Int64(a), Key::Int64(b)) => a.cmp(b),
        (Key::Utf8(a), Key::Utf8(b)) => a.cmp(b),
        (Key::Int64(_), _) => std::cmp::Ordering::Less,
        _ => std::cmp::Ordering::Greater,
    }
}
fn invalid() -> ProtocolError {
    failure(
        ErrorCode::ValidationError,
        "invalid pinned catalog or routing lineage",
    )
}

/// The root commits to an actual namespaced timestamp cut and the requested
/// table's immutable identities/versions. Capturing it does not clone all tables.
pub struct CatalogCapture {
    pub pin: PinnedCatalog,
    pub catalog_root: nabedra_oltp_types::types::Digest,
    pub table_id: Option<Uuid>,
    pub name_version: Option<CommitTs>,
    pub table_version: Option<CommitTs>,
    pub routing_version: Option<CommitTs>,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    point: Option<PointCatalog>,
}

/// Local, charged catalog records from the same snapshot as the accepted
/// context. Transfer and recovery reconstruct them from the retained pin.
pub struct PointCatalog {
    pub(super) table: OwnedRecord<TableRecord>,
    pub(super) table_version: Option<CommitTs>,
    pub(super) first_page: Option<Uuid>,
    pub(super) selected: Option<(OwnedRecord<RoutingPage>, usize)>,
}

impl CatalogCapture {
    fn lineage_digest(&self) -> Result<Digest, ProtocolError> {
        // Authority terms fence execution while the immutable cut survives a
        // same-run takeover.
        nabedra_oltp_types::owned::serialized_digest(&(
            b"nabedra/pinned-catalog-lineage/v1".as_slice(),
            &self.pin.catalog,
            self.pin.read_ts,
            self.pin.authority.run_id,
            self.table_id,
            self.name_version,
            self.table_version,
            self.routing_version,
            self.schema_epoch,
            self.routing_epoch,
        ))
    }

    pub fn has_point(&self) -> bool {
        self.point.is_some()
    }

    pub fn point_for(
        &self,
        pin: &PinnedCatalog,
        root: nabedra_oltp_types::types::Digest,
    ) -> Option<&PointCatalog> {
        if self.catalog_root != root
            || self.pin.catalog != pin.catalog
            || self.pin.read_ts != pin.read_ts
            || self.pin.authority != pin.authority
        {
            return None;
        }
        self.point.as_ref()
    }
}
pub enum CaptureRead {
    Captured(CatalogCapture),
    Blocked(IntentRef),
}
pub enum ScopedCapture {
    Captured(CatalogCapture),
    ScopeSuperseded,
}
impl CatalogHandle {
    pub async fn scoped_admission_route(
        &self,
        placement: &ScopedQueuePlacement,
        authority: Authority,
        request: RequestBinding,
        key: &Key,
    ) -> Result<RouteRead, ProtocolError> {
        let GroupKind::Data { table_id, .. } = placement.group.kind else {
            return Err(invalid());
        };
        let read_ts = ReadTs::new(u64::MAX).map_err(|_| invalid())?;
        let scope = ScopedCatalogRead {
            queue_group_id: placement.group.group_id,
            data_generation: placement.data_generation,
            queue_generation: placement.queue_generation,
            queue_view: placement.view,
            request,
            table_id,
            read_ts,
        };
        let catalog = Self::new(Arc::new(self.participant.with_scoped_execution(scope)?))?;
        let current = catalog.snapshot(read_ts, authority)?;
        catalog.route(&current, table_id, key).await
    }

    pub async fn capture_scoped_lineage(
        &self,
        placement: &ScopedQueuePlacement,
        reservation: IdentityReservation,
        identity: &nabedra_oltp_types::types::PinIdentity,
        storage_pin: nabedra_oltp_types::types::StoragePin,
        key: &crate::protocol::Key,
        authority: Authority,
    ) -> Result<ScopedCapture, ProtocolError> {
        let GroupKind::Data { table_id, .. } = placement.group.kind else {
            return Err(invalid());
        };
        let request = RequestBinding {
            run_id: identity.run_id,
            request_id: identity.request_id,
            digest: identity.digest,
            accepted_context: Digest::from_bytes([0; 32]),
        };
        if !reservation.binds(request)
            || reservation.reservation_id != identity.nonce
            || request.run_id != placement.view.run_id
            || !reservation.scoped.is_some_and(|policy| {
                policy.queue_group_id == placement.group.group_id
                    && policy.queue_generation == placement.queue_generation
            })
        {
            return Err(invalid());
        }
        let scope = ScopedCatalogRead {
            queue_group_id: placement.group.group_id,
            data_generation: placement.data_generation,
            queue_generation: placement.queue_generation,
            queue_view: placement.view,
            request,
            table_id,
            read_ts: storage_pin.read_ts,
        };
        let catalog = Self::new(Arc::new(self.participant.with_scoped_execution(scope)?))?;
        let pin = catalog.snapshot(storage_pin.read_ts, authority)?;
        let operation = crate::protocol::Operation::Get(crate::protocol::KeyPayload {
            table_id,
            key: key.clone(),
        });
        let unavailable = || {
            ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "scoped catalog lineage or data route is unavailable",
                crate::protocol::Outcome::Unknown,
            )
        };
        let CaptureRead::Captured(mut capture) = catalog.capture_lineage(&pin, &operation).await?
        else {
            return Err(unavailable());
        };
        let Some(point) = capture.point.as_mut() else {
            return Ok(ScopedCapture::ScopeSuperseded);
        };
        match catalog
            .route_pages(&pin, table_id, key, point.first_page)
            .await?
        {
            RouteRead::Route { page, index } if page.shards[index].group == placement.group => {
                point.selected = Some((page, index));
                Ok(ScopedCapture::Captured(capture))
            }
            RouteRead::Route { .. } | RouteRead::Missing => Ok(ScopedCapture::ScopeSuperseded),
            RouteRead::Blocked(_) => Err(unavailable()),
        }
    }

    pub async fn capture_lineage(
        &self,
        pin: &PinnedCatalog,
        operation: &crate::protocol::Operation,
    ) -> Result<CaptureRead, ProtocolError> {
        self.check(pin)?;
        let namespace = self.participant.epochs().await?;
        if namespace.authority != pin.authority {
            return Err(invalid());
        }
        let mut capture = CatalogCapture {
            pin: pin.clone(),
            catalog_root: nabedra_oltp_types::types::Digest::from_bytes([0; 32]),
            table_id: None,
            name_version: None,
            table_version: None,
            routing_version: None,
            schema_epoch: namespace.schema_epoch,
            routing_epoch: namespace.routing_epoch,
            point: None,
        };
        use crate::protocol::Operation;
        let keep_point = matches!(operation, Operation::Get(_) | Operation::Update(_));
        let name = match operation {
            Operation::Create(p) => Some(p.name.as_str()),
            Operation::Describe(p) => Some(p.name.as_str()),
            _ => None,
        };
        if let Some(name) = name {
            match self
                .read::<NameRecord>(
                    pin,
                    MvccKey::CatalogName {
                        name: Text::try_from(name).map_err(|_| invalid())?,
                    },
                )
                .await?
            {
                CatalogRead::Blocked(i) => return Ok(CaptureRead::Blocked(i)),
                CatalogRead::Value { version, record } => {
                    capture.name_version = version;
                    capture.table_id = record.map(|r| r.table_id);
                }
            }
        } else {
            capture.table_id = match operation {
                Operation::Drop(p) => Some(p.table_id),
                Operation::Insert(p) | Operation::Upsert(p) => Some(p.table_id),
                Operation::InsertMany(p) => Some(p.table_id),
                Operation::Get(p) | Operation::Delete(p) => Some(p.table_id),
                Operation::Update(p) => Some(p.table_id),
                Operation::Collect(p) | Operation::Count(p) | Operation::Explain(p) => {
                    Some(p.table_id)
                }
                _ => None,
            };
        }
        if let Some(table_id) = capture.table_id {
            let mut point_table = None;
            match self.describe(pin, table_id).await? {
                CatalogRead::Blocked(i) => return Ok(CaptureRead::Blocked(i)),
                CatalogRead::Value { version, record } => {
                    capture.table_version = version;
                    if let Some(table) = record {
                        capture.schema_epoch = table.schema_epoch;
                        capture.routing_epoch = table.routing_epoch;
                        if keep_point && table.descriptor.id == table_id {
                            point_table = Some(table);
                        }
                    }
                }
            }
            match self.routing_root(pin, table_id).await? {
                CatalogRead::Blocked(i) => return Ok(CaptureRead::Blocked(i)),
                CatalogRead::Value { version, record } => {
                    capture.routing_version = version;
                    if let Some(root) = record {
                        if root.schema_epoch != capture.schema_epoch
                            || root.routing_epoch != capture.routing_epoch
                        {
                            return Err(invalid());
                        }
                        if let Some(table) = point_table {
                            capture.point = Some(PointCatalog {
                                table,
                                table_version: capture.table_version,
                                first_page: root.first_page,
                                selected: None,
                            });
                        }
                    }
                }
            }
        }
        capture.catalog_root = capture.lineage_digest()?;
        Ok(CaptureRead::Captured(capture))
    }
}
