use super::*;
use crate::oltp::mvcc::index::{
    IndexDefinition, IndexNameRecord, IndexPage, IndexPhase, IndexRecord, IndexRoot,
};
use crate::oltp::pool::ScratchLease;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexPayload {
    pub base_version: Option<nabedra_oltp_types::types::CommitTs>,
    pub cover: Row,
}
/// The entries one index changes when a row goes from `old` to `new`.
/// Shared by the queued planner and the direct executor (plan section 6).
pub struct IndexEntryChanges {
    /// The old entry's key, when the change moves or removes it. An absent
    /// old entry still receives a tombstone: a delayed backfill must never
    /// resurrect its stale key.
    pub delete: Option<MvccKey>,
    /// The new entry and its covering payload.
    pub put: Option<(MvccKey, CanonicalJson)>,
}
/// Computes `IndexEntryChanges` for one validated, writable index.
pub fn index_entry_changes(
    definition: &IndexDefinition,
    table: &TableDescriptor,
    primary: &Key,
    old: Option<&Row>,
    new: Option<&CanonicalJson>,
    resources: &crate::oltp::pool::WorkerSharedResources,
) -> Result<IndexEntryChanges, ProtocolError> {
    let entry = |row: &Row| {
        definition
            .values(table, row)
            .map(|values| MvccKey::IndexEntry {
                table_id: table.id,
                index_id: definition.index_id,
                values,
                primary_key: primary.clone(),
            })
    };
    let old_key = old.map(entry).transpose()?;
    let new_row = new
        .map(|raw| crate::protocol::decode_row(raw.as_str()).map_err(|_| not_found()))
        .transpose()?;
    let new_key = new_row.as_ref().map(entry).transpose()?;
    let put = match (new_key, new_row.as_ref()) {
        (Some(key), Some(row)) => Some((
            key,
            CanonicalJson::from_serializable(
                resources,
                &IndexPayload {
                    base_version: None,
                    cover: definition.cover(table, row),
                },
                super::super::MAX_FIXED_COMMAND_BYTES,
            )?,
        )),
        _ => None,
    };
    let delete = old_key.filter(|old| put.as_ref().is_none_or(|(new, _)| new != old));
    Ok(IndexEntryChanges { delete, put })
}
impl NativePlanner {
    /// Stream definition pages; there is no table-wide index-count allocation.
    pub(super) async fn index_changes(
        &self,
        pin: &PinnedCatalog,
        table: OwnedRecord<TableRecord>,
        participant: Arc<ParticipantHandle>,
        key: &Key,
        old: Option<OwnedRecord<StoredRow>>,
        new: Option<CanonicalJson>,
        owner: &mut Arc<ScratchLease>,
        used: &mut usize,
        metadata: &mut MutationMetadata,
    ) -> Result<Vec<PreparedWrite>, PlanError> {
        let mut result = Vec::new();
        if metadata.indexes.is_none() {
            let (_, root) = value(
                self.catalog
                    .read::<IndexRoot>(
                        pin,
                        MvccKey::CatalogIndexesRoot {
                            table_id: table.descriptor.id,
                        },
                    )
                    .await?,
            )?;
            metadata.indexes = Some(root);
        }
        let mut next = metadata
            .indexes
            .as_ref()
            .and_then(|root| root.as_ref())
            .and_then(|r| r.first_page);
        while let Some(page_id) = next {
            let (_, page) = value(
                self.catalog
                    .read::<IndexPage>(
                        pin,
                        MvccKey::CatalogIndexesPage {
                            table_id: table.descriptor.id,
                            page_id,
                        },
                    )
                    .await?,
            )?;
            let page = page.ok_or_else(not_found)?;
            for index_id in page.indexes.iter() {
                let (_, record) = value(
                    self.catalog
                        .read::<IndexRecord>(
                            pin,
                            MvccKey::CatalogIndex {
                                table_id: table.descriptor.id,
                                index_id: *index_id,
                            },
                        )
                        .await?,
                )?;
                let record = record.ok_or_else(not_found)?;
                if !record.phase.writable() {
                    continue;
                }
                if record.schema_epoch != table.schema_epoch {
                    return Err(failure(ErrorCode::Conflict, "index schema changed").into());
                }
                let old_copy = old.clone();
                let new_copy = new.clone();
                let metadata = table.clone();
                let extra = self
                    .cpu(owner.clone(), move || {
                        let mut cost = 64 << 10;
                        if let Some(old) = old_copy {
                            cost += crate::oltp::query::row_charge(
                                &metadata.descriptor,
                                crate::oltp::query::check_body(
                                    &*old,
                                    super::super::MAX_FIXED_COMMAND_BYTES,
                                )?,
                            )?;
                        }
                        if let Some(new) = new_copy {
                            cost += crate::oltp::query::row_charge(
                                &metadata.descriptor,
                                new.as_str().len(),
                            )?;
                        }
                        Ok(cost)
                    })
                    .await?;
                *used = used.checked_add(extra).ok_or_else(not_found)?;
                let required = used
                    .checked_add((1 << 20) - 1)
                    .map(|n| n & !((1 << 20) - 1))
                    .ok_or_else(not_found)?;
                *owner = self
                    .catalog
                    .resources()
                    .ensure_scratch_budget(owner.clone(), required)?;
                let old_row = old.clone();
                let raw_new = new.clone();
                let primary = key.clone();
                let schema = table.clone();
                let definition = record.clone();
                let resources = self.catalog.resources().clone();
                let changes = self
                    .cpu(owner.clone(), move || {
                        definition.definition.validate(&schema.descriptor)?;
                        index_entry_changes(
                            &definition.definition,
                            &schema.descriptor,
                            &primary,
                            old_row.as_deref().map(|row| &row.0),
                            raw_new.as_ref(),
                            &resources,
                        )
                    })
                    .await?;
                if let Some(old_key) = changes.delete {
                    let expected = self
                        .index_version(pin, &participant, old_key.clone())
                        .await?;
                    result.push(PreparedWrite {
                        key: old_key,
                        expected_version: expected,
                        change: Change::Delete,
                    });
                }
                if let Some((new_key, cover)) = changes.put {
                    let expected = self
                        .index_version(pin, &participant, new_key.clone())
                        .await?;
                    result.push(PreparedWrite {
                        key: new_key,
                        expected_version: expected,
                        change: Change::Put(cover),
                    });
                }
            }
            next = page.next_page;
        }
        Ok(result)
    }
    async fn index_version(
        &self,
        pin: &PinnedCatalog,
        participant: &ParticipantHandle,
        key: MvccKey,
    ) -> Result<Option<nabedra_oltp_types::types::CommitTs>, PlanError> {
        let read = participant.read_at(pin.read_ts, key).await?;
        let (version, _) =
            value(decode_record::<serde::de::IgnoredAny>(self.catalog.resources(), read).await?)?;
        Ok(version)
    }
    pub(crate) async fn acquire_index_guard(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        table_id: Uuid,
        mode: GuardMode,
    ) -> Result<VerifiedLifecycleGuard, PlanError> {
        let (version, table) = value(self.catalog.describe(pin, table_id).await?)?;
        let table = table.ok_or_else(not_found)?;
        self.guard_at(
            pin,
            txn,
            request,
            table_id,
            version,
            table.schema_epoch,
            mode,
            pin.authority,
        )
        .await
    }

    pub async fn index_publication_binding(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        table_id: Uuid,
    ) -> Result<LifecycleBinding, PlanError> {
        let (version, table) = value(self.catalog.describe(pin, table_id).await?)?;
        let table = table.ok_or_else(not_found)?;
        Ok(self.lifecycle_binding(pin, txn, request, table_id, version, table.schema_epoch, pin.authority))
    }

    pub(crate) async fn validate_index_declaration(
        &self,
        pin: &PinnedCatalog,
        definition: &IndexDefinition,
    ) -> Result<(), PlanError> {
        let (_, table) = value(self.catalog.describe(pin, definition.table_id).await?)?;
        definition.validate(&table.ok_or_else(not_found)?.descriptor)?;
        let (_, name) = value(self.catalog.read::<IndexNameRecord>(pin, MvccKey::CatalogIndexName {
            table_id: definition.table_id,
            name: nabedra_oltp_types::types::Text::try_from(definition.name.as_str()).map_err(|_| not_found())?,
        }).await?)?;
        let (_, record) = value(self.catalog.read::<IndexRecord>(pin, MvccKey::CatalogIndex {
            table_id: definition.table_id,
            index_id: definition.index_id,
        }).await?)?;
        if name.is_some() || record.is_some() {
            return Err(failure(ErrorCode::Conflict, "index name or identity already exists").into());
        }
        Ok(())
    }
    /// The caller acquires this exclusive guard BEFORE allocating the source
    /// retention pin. Earlier writers must settle before the source cut exists.
    pub(crate) async fn plan_declare_index(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        definition: IndexDefinition,
        phase: IndexPhase,
        guard: VerifiedLifecycleGuard,
        owner: Arc<ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let (table_version,table)=async {
        let (table_version, table) = value(self.catalog.describe(pin, definition.table_id).await?)?;
        let table = table.ok_or_else(not_found)?;
        definition.validate(&table.descriptor)?;
        if !matches!(&phase,IndexPhase::Building{pin:source_pin,source,..}
            if source_pin.read_ts>pin.read_ts && source.read_ts==source_pin.read_ts && source.catalog==pin.catalog && source.authority.run_id==pin.authority.run_id){
            return Err(failure(ErrorCode::Conflict,"index source pin predates exclusive acquisition; use a new build attempt").into());
        }
        if guard.mode() != GuardMode::Exclusive
            || guard.binding().txn != txn
            || guard.binding().request != request
            || guard.binding().table_id != definition.table_id
            || guard.binding().read_ts != pin.read_ts
            || guard.binding().lifecycle_version != table_version
        {
            return Err(failure(ErrorCode::Conflict, "index declaration guard mismatch").into());
        }
            Ok((table_version,table))
        }.await.map_err(|cause|PlanError::Guarded{cause:Box::new(cause),guards:vec![guard.clone()]})?;
        let planned = async {
            let (table_id, index_id) = (definition.table_id, definition.index_id);
            let (name_version, name) = value(
                self.catalog
                    .read::<IndexNameRecord>(
                        pin,
                        MvccKey::CatalogIndexName {
                            table_id,
                            name: nabedra_oltp_types::types::Text::try_from(
                                definition.name.as_str(),
                            )
                            .map_err(|_| not_found())?,
                        },
                    )
                    .await?,
            )?;
            if name.is_some() {
                return Err(failure(ErrorCode::Conflict, "index name already exists").into());
            }
            let (root_version, root) = value(
                self.catalog
                    .read::<IndexRoot>(pin, MvccKey::CatalogIndexesRoot { table_id })
                    .await?,
            )?;
            let page_id = derived_uuid(b"nabedra/index-page/v1", index_id, txn.request_id);
            let mut root = root.as_ref().map(|r| (**r).clone()).unwrap_or(IndexRoot {
                first_page: None,
                last_page: None,
                epoch: 0,
            });
            let mut writes = Vec::new();
            let mut page = IndexPage {
                indexes: Page::new(vec![index_id]).map_err(|_| not_found())?,
                next_page: None,
            };
            let mut page_version = None;
            let mut target = page_id;
            if let Some(last) = root.last_page {
                let (version, old) = value(
                    self.catalog
                        .read::<IndexPage>(
                            pin,
                            MvccKey::CatalogIndexesPage {
                                table_id,
                                page_id: last,
                            },
                        )
                        .await?,
                )?;
                let old = old.ok_or_else(not_found)?;
                if old.indexes.len() < 128 {
                    let mut items = old.indexes.to_vec();
                    items.push(index_id);
                    page.indexes = Page::new(items).map_err(|_| not_found())?;
                    target = last;
                    page_version = version;
                } else {
                    let linked = IndexPage {
                        indexes: old.indexes.clone(),
                        next_page: Some(page_id),
                    };
                    writes.push(PreparedWrite {
                        key: MvccKey::CatalogIndexesPage {
                            table_id,
                            page_id: last,
                        },
                        expected_version: version,
                        change: self.index_put(&linked)?,
                    });
                    root.last_page = Some(page_id);
                }
            } else {
                root.first_page = Some(page_id);
                root.last_page = Some(page_id);
            }
            root.epoch = root.epoch.checked_add(1).ok_or_else(not_found)?;
            let record = IndexRecord {
                definition: definition.clone(),
                phase,
                schema_epoch: table.schema_epoch,
                routing_epoch: table.routing_epoch,
            };
            writes.extend([
                PreparedWrite {
                    key: MvccKey::CatalogIndexesRoot { table_id },
                    expected_version: root_version,
                    change: self.index_put(&root)?,
                },
                PreparedWrite {
                    key: MvccKey::CatalogIndexesPage {
                        table_id,
                        page_id: target,
                    },
                    expected_version: page_version,
                    change: self.index_put(&page)?,
                },
                PreparedWrite {
                    key: MvccKey::CatalogIndexName {
                        table_id,
                        name: nabedra_oltp_types::types::Text::try_from(
                            definition.name.as_str(),
                        )
                        .map_err(|_| not_found())?,
                    },
                    expected_version: name_version,
                    change: self.index_put(&IndexNameRecord { index_id })?,
                },
                PreparedWrite {
                    key: MvccKey::CatalogIndex { table_id, index_id },
                    expected_version: None,
                    change: self.index_put(&record)?,
                },
                // The unchanged descriptor receives a new MVCC version, so an
                // old captured schema cannot acquire a new unindexed guard.
                PreparedWrite {
                    key: MvccKey::CatalogTable { table_id },
                    expected_version: table_version,
                    change: self.index_put(&table.with_lifecycle(txn.request_id))?,
                },
            ]);
            let epochs = self.catalog.participant().epochs().await?;
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
                _owner: owner,
            })
        }
        .await;
        planned.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }
    pub(super) fn index_put(&self, value: &impl Serialize) -> Result<Change, ProtocolError> {
        Ok(Change::Put(CanonicalJson::from_serializable(
            self.catalog.resources(),
            value,
            super::super::MAX_FIXED_COMMAND_BYTES,
        )?))
    }
}

pub(super) struct IndexChoice {
    pub record: OwnedRecord<IndexRecord>,
    pub equality: Page<crate::oltp::mvcc::index::IndexValue>,
    pub lower: Option<(crate::oltp::mvcc::index::IndexValue, bool)>,
    pub upper: Option<(crate::oltp::mvcc::index::IndexValue, bool)>,
    pub cover: Option<Arc<BTreeSet<String>>>,
    pub complete: bool,
    score: usize,
    _owner: Arc<ScratchLease>,
}
impl IndexChoice {
    pub(super) fn seeks(&self) -> bool {
        !self.equality.is_empty() || self.lower.is_some() || self.upper.is_some()
    }
}
impl NativePlanner {
    pub(super) async fn choose_index(
        &self,
        pin: &PinnedCatalog,
        table: OwnedRecord<TableRecord>,
        query: Arc<crate::protocol::Query>,
        count: bool,
        primary_range: bool,
        owner: Arc<ScratchLease>,
    ) -> Result<Option<IndexChoice>, PlanError> {
        let (_, root) = value(
            self.catalog
                .read::<IndexRoot>(
                    pin,
                    MvccKey::CatalogIndexesRoot {
                        table_id: table.descriptor.id,
                    },
                )
                .await?,
        )?;
        let Some(first_page) = root.and_then(|r| r.first_page) else {
            return Ok(None);
        };
        let schema = table.clone();
        let request = query.clone();
        let required = self
            .cpu(owner.clone(), move || {
                Ok(Arc::new(required_columns(
                    &schema.descriptor,
                    &request,
                    count,
                )))
            })
            .await?;
        let mut next = Some(first_page);
        let mut best: Option<IndexChoice> = None;
        while let Some(page_id) = next {
            let (_, page) = value(
                self.catalog
                    .read::<IndexPage>(
                        pin,
                        MvccKey::CatalogIndexesPage {
                            table_id: table.descriptor.id,
                            page_id,
                        },
                    )
                    .await?,
            )?;
            let page = page.ok_or_else(not_found)?;
            for id in page.indexes.iter() {
                let (_, record) = value(
                    self.catalog
                        .read::<IndexRecord>(
                            pin,
                            MvccKey::CatalogIndex {
                                table_id: table.descriptor.id,
                                index_id: *id,
                            },
                        )
                        .await?,
                )?;
                let record = record.ok_or_else(not_found)?;
                if !record.phase.ready() {
                    continue;
                }
                if record.schema_epoch != table.schema_epoch
                    || record.definition.table_id != table.descriptor.id
                    || record.definition.index_id != *id
                {
                    return Err(failure(
                        ErrorCode::StorageUnavailable,
                        "invalid Ready index at the pinned Catalog epoch",
                    )
                    .into());
                }
                let captured = table.clone();
                let request = query.clone();
                let fields = required.clone();
                let held = owner.clone();
                let candidate = self
                    .cpu(owner.clone(), move || {
                        record.definition.validate(&captured.descriptor)?;
                        let physical = record.definition.physical_columns(&captured.descriptor);
                        let mut comparisons = Vec::new();
                        if let Some(p) = &request.filter {
                            conjuncts(p, &mut comparisons)
                        }
                        let mut equality = Vec::new();
                        let (mut lower, mut upper) = (None, None);
                        for column in physical {
                            if let Some(eq) = comparisons.iter().find(|p| {
                                p.column == column.as_str()
                                    && p.op == protocol::ComparisonOp::Eq
                                    && exact_index_scalar(
                                        captured.descriptor.schema[column.as_str()],
                                        &p.value,
                                    )
                            }) {
                                equality.push(crate::oltp::mvcc::index::IndexValue::from_scalar(
                                    &eq.value,
                                )?);
                                continue;
                            }
                            for p in comparisons.iter().filter(|p| {
                                p.column == column.as_str()
                                    && exact_index_scalar(
                                        captured.descriptor.schema[column.as_str()],
                                        &p.value,
                                    )
                            }) {
                                use protocol::ComparisonOp::*;
                                let candidate =
                                    crate::oltp::mvcc::index::IndexValue::from_scalar(&p.value)?;
                                match p.op {
                                    Gt | Ge => {
                                        let inclusive = p.op == Ge;
                                        if lower.as_ref().is_none_or(|(old, inc)| {
                                            candidate > *old
                                                || (candidate == *old && *inc && !inclusive)
                                        }) {
                                            lower = Some((candidate, inclusive));
                                        }
                                    }
                                    Lt | Le => {
                                        let inclusive = p.op == Le;
                                        if upper.as_ref().is_none_or(|(old, inc)| {
                                            candidate < *old
                                                || (candidate == *old && *inc && !inclusive)
                                        }) {
                                            upper = Some((candidate, inclusive));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            break;
                        }
                        let covered = fields.iter().all(|field| {
                            field == &captured.descriptor.key
                                || record
                                    .definition
                                    .columns
                                    .iter()
                                    .chain(record.definition.covering.iter())
                                    .any(|c| c.as_str() == field)
                        });
                        let seek_score = equality.len() * 4
                            + usize::from(lower.is_some() || upper.is_some()) * 2;
                        if seek_score == 0 && (!covered || primary_range) {
                            return Ok(None);
                        }
                        let complete = equality.len() == physical.len();
                        Ok(Some(IndexChoice {
                            record,
                            equality: Page::new(equality).map_err(|_| not_found())?,
                            lower,
                            upper,
                            cover: covered.then_some(fields),
                            complete,
                            score: seek_score + usize::from(covered),
                            _owner: held,
                        }))
                    })
                    .await?;
                if let Some(candidate) = candidate {
                    if best.as_ref().is_none_or(|old| {
                        candidate.score > old.score
                            || (candidate.score == old.score
                                && candidate.record.definition.index_id
                                    < old.record.definition.index_id)
                    }) {
                        best = Some(candidate)
                    }
                }
            }
            next = page.next_page;
        }
        Ok(best)
    }
}
fn conjuncts<'a>(predicate: &'a protocol::Predicate, out: &mut Vec<&'a protocol::Comparison>) {
    match predicate {
        protocol::Predicate::Comparison(p) => out.push(p),
        protocol::Predicate::Logical(protocol::LogicalPredicate::And { args }) => {
            for p in args {
                conjuncts(p, out)
            }
        }
        _ => {}
    }
}
fn required_columns(
    table: &TableDescriptor,
    query: &protocol::Query,
    count: bool,
) -> BTreeSet<String> {
    fn predicate(p: &protocol::Predicate, fields: &mut BTreeSet<String>) {
        match p {
            protocol::Predicate::Comparison(p) => {
                fields.insert(p.column.clone());
            }
            protocol::Predicate::Logical(
                protocol::LogicalPredicate::And { args } | protocol::LogicalPredicate::Or { args },
            ) => {
                for p in args {
                    predicate(p, fields)
                }
            }
            protocol::Predicate::Logical(protocol::LogicalPredicate::Not { arg }) => {
                predicate(arg, fields)
            }
            protocol::Predicate::Text(p) => {
                fields.insert(p.column().to_owned());
            }
        }
    }
    let mut fields = BTreeSet::new();
    fields.insert(table.key.clone());
    if !count {
        match &query.select {
            Some(cols) => fields.extend(cols.iter().cloned()),
            None => fields.extend(table.schema.keys().cloned()),
        }
        if let Some(order) = &query.order_by {
            fields.insert(order.column.clone());
        }
    }
    if let Some(filter) = &query.filter {
        predicate(filter, &mut fields)
    }
    fields
}

impl NativePlanner {
    pub async fn index_participant(
        &self,
        group: &GroupIdentity,
    ) -> Result<Arc<ParticipantHandle>, ProtocolError> {
        self.resolver.participant(group).await
    }
}

/// Mixed integer/float predicates retain the shared exact comparator as a
/// residual. Seeking a differently tagged or rounded scalar could exclude a
/// valid boundary, including float(2^63) > int64::MAX.
fn exact_index_scalar(kind: protocol::ColumnType, scalar: &protocol::Scalar) -> bool {
    matches!(
        (kind, scalar),
        (protocol::ColumnType::Int64, protocol::Scalar::Int64(_))
            | (protocol::ColumnType::Float64, protocol::Scalar::Float64(_))
            | (protocol::ColumnType::Bool, protocol::Scalar::Bool(_))
            | (protocol::ColumnType::Utf8, protocol::Scalar::Utf8(_))
    )
}
