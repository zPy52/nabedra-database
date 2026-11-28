//! Fixed Catalog changes for the operator's online-index coordinator.
use super::*;
use crate::oltp::mvcc::index::{
    IndexAttemptClosedRecord, IndexClosedKind, IndexPhase, IndexProgress, IndexRecord, closure_id,
    progress_id,
};
use nabedra_oltp_types::types::MaintenanceOwnerRef;
impl NativePlanner {
    pub(crate) async fn plan_index_progress(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        maintenance: MaintenanceOwnerRef,
        progress: IndexProgress,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::oltp::test_hooks::index_faults::checkpoint(
            "backfill",
            maintenance.index_id,
            &progress,
        )
        .await;
        let guard = self
            .acquire_index_guard(pin, txn, request, maintenance.table_id, GuardMode::Shared)
            .await?;
        let result = async {
            let (_, record) = value(self.catalog.read::<IndexRecord>(pin, MvccKey::CatalogIndex { table_id: maintenance.table_id, index_id: maintenance.index_id }).await?)?;
            let record = record.ok_or_else(not_found)?;
            if !matches!(record.phase, IndexPhase::Building { build_id, attempt, .. } if build_id == maintenance.build_id && attempt == maintenance.attempt)
                || progress.build_id != maintenance.build_id || progress.attempt != maintenance.attempt || !progress.done {
                return Err(failure(ErrorCode::Conflict, "index build changed").into());
            }
            let key = MvccKey::CatalogIndexProgress { table_id: maintenance.table_id, progress_id: progress_id(maintenance.index_id, progress.group_id) };
            let (version, _) = value(self.catalog.read::<IndexProgress>(pin, key.clone()).await?)?;
            self.catalog_index_plan(vec![PreparedWrite { key, expected_version: version, change: self.index_put(&progress)? }], guard.clone(), owner).await
        }.await;
        result.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }
    /// Publish only after every source shard has a durable completed marker.
    /// The exclusive guard keeps the schema and writer set fixed while this
    /// bounded page traversal checks the captured routing lineage.
    pub(crate) async fn plan_finish_index(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        maintenance: MaintenanceOwnerRef,
        kind: IndexClosedKind,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let guard = self
            .acquire_index_guard(
                pin,
                txn,
                request,
                maintenance.table_id,
                GuardMode::Exclusive,
            )
            .await?;
        let result = async {
            let key = MvccKey::CatalogIndex {
                table_id: maintenance.table_id,
                index_id: maintenance.index_id,
            };
            let (version, record) =
                value(self.catalog.read::<IndexRecord>(pin, key.clone()).await?)?;
            let record = record.ok_or_else(not_found)?;
            let (build_id, attempt, source_pin, source) = match &record.phase {
                IndexPhase::Building {
                    build_id,
                    attempt,
                    pin,
                    source,
                } => (*build_id, *attempt, *pin, source.clone()),
                _ => return Err(failure(ErrorCode::Conflict, "index is not building").into()),
            };
            if build_id != maintenance.build_id || attempt != maintenance.attempt {
                return Err(not_found().into());
            }
            if kind == IndexClosedKind::Ready {
                let (_, source_root) = value(
                    self.catalog
                        .routing_root(&source, maintenance.table_id)
                        .await?,
                )?;
                let source_root = source_root.ok_or_else(not_found)?;
                let (_, current_root) =
                    value(self.catalog.routing_root(pin, maintenance.table_id).await?)?;
                let current_root = current_root.ok_or_else(not_found)?;
                if source_root.schema_epoch != current_root.schema_epoch
                    || source_root.routing_epoch != current_root.routing_epoch
                    || record.schema_epoch != source_root.schema_epoch
                    || record.routing_epoch != source_root.routing_epoch
                {
                    return Err(failure(
                        ErrorCode::Conflict,
                        "index routing changed during backfill",
                    )
                    .into());
                }
                let mut next = source_root.first_page;
                while let Some(page_id) = next {
                    let (_, page) = value(
                        self.catalog
                            .routing_page(&source, maintenance.table_id, page_id)
                            .await?,
                    )?;
                    let page = page.ok_or_else(not_found)?;
                    for shard in page.shards.iter() {
                        let (_, progress) = value(
                            self.catalog
                                .read::<IndexProgress>(
                                    pin,
                                    MvccKey::CatalogIndexProgress {
                                        table_id: maintenance.table_id,
                                        progress_id: progress_id(
                                            maintenance.index_id,
                                            shard.group.group_id,
                                        ),
                                    },
                                )
                                .await?,
                        )?;
                        if !progress.is_some_and(|p| {
                            p.done
                                && p.build_id == build_id
                                && p.attempt == attempt
                                && p.span == shard.span
                        }) {
                            return Err(failure(
                                ErrorCode::Conflict,
                                "index backfill is incomplete",
                            )
                            .into());
                        }
                        #[cfg(any(test, feature = "test-hooks"))]
                        if crate::oltp::test_hooks::index_holds::checkpoint(
                            maintenance.index_id,
                            crate::oltp::test_hooks::index_holds::Checkpoint::Validation,
                            || None,
                        )
                        .await || crate::oltp::test_hooks::index_faults::checkpoint(
                            "validation",
                            maintenance.index_id,
                            &serde_json::json!({
                                "record": &*record,
                                "verified_shard": shard,
                                "txn": txn,
                                "request": request,
                                "guard": guard.binding(),
                            }),
                        )
                        .await
                        {
                            return Err(failure(
                                ErrorCode::StorageUnavailable,
                                "index fixture rejected Ready validation",
                            )
                            .into());
                        }
                    }
                    next = page.next_page;
                }
            }
            let phase = match kind {
                IndexClosedKind::Ready => IndexPhase::Ready {
                    build_id,
                    attempt,
                    pin: source_pin,
                },
                IndexClosedKind::Failed => IndexPhase::Failed {
                    build_id,
                    attempt,
                    pin: source_pin,
                },
                IndexClosedKind::Retired => IndexPhase::Retired {
                    build_id,
                    attempt,
                    pin: source_pin,
                },
                IndexClosedKind::Superseded => {
                    return Err(failure(
                        ErrorCode::ValidationError,
                        "supersede requires a new fenced attempt",
                    )
                    .into());
                }
            };
            let closed_key = MvccKey::CatalogIndexClosed {
                table_id: maintenance.table_id,
                closure_id: closure_id(&maintenance)?,
            };
            let (closed_version, _) = value(
                self.catalog
                    .read::<IndexAttemptClosedRecord>(pin, closed_key.clone())
                    .await?,
            )?;
            let updated = IndexRecord {
                definition: record.definition.clone(),
                phase,
                schema_epoch: record.schema_epoch,
                routing_epoch: record.routing_epoch,
            };
            let (table_version, table) =
                value(self.catalog.describe(pin, maintenance.table_id).await?)?;
            let table = table.ok_or_else(not_found)?;
            #[cfg(any(test, feature = "test-hooks"))]
            if kind == IndexClosedKind::Ready {
                crate::oltp::test_hooks::index_faults::checkpoint(
                    "ready-plan",
                    maintenance.index_id,
                    &serde_json::json!({
                        "owner": maintenance,
                        "txn": txn,
                        "request": request,
                        "guard": guard.binding(),
                    }),
                )
                .await;
            }
            // A search table fences with a SearchFence of the same columns.
            let (fence, record) = self
                .prepare_lifecycle_fence(pin, &table, txn.request_id)
                .await?;
            let plan = self.catalog_index_plan(
                vec![
                    PreparedWrite {
                        key: MvccKey::CatalogTable {
                            table_id: maintenance.table_id,
                        },
                        expected_version: table_version,
                        change: self.index_put(&record)?,
                    },
                    PreparedWrite {
                        key,
                        expected_version: version,
                        change: self.index_put(&updated)?,
                    },
                    PreparedWrite {
                        key: closed_key,
                        expected_version: closed_version,
                        change: self.index_put(&IndexAttemptClosedRecord {
                            owner: maintenance,
                            pin: source_pin,
                            closed_by: txn,
                            kind,
                        })?,
                    },
                ],
                guard.clone(),
                owner,
            )
            .await?;
            // Fence before publishing the new lifecycle. A rejected
            // validation must leave Data on the current Catalog lifecycle.
            self.run_lifecycle_fence(pin, maintenance.table_id, fence)
                .await?;
            Ok(plan)
        }
        .await;
        result.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }
    async fn catalog_index_plan(
        &self,
        writes: Vec<PreparedWrite>,
        guard: VerifiedLifecycleGuard,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let epochs = self.catalog.participant().epochs().await?;
        Ok(MutationPlan {
            participants: vec![ParticipantWrites {
                participant: self.catalog.participant().clone(),
                schema_epoch: epochs.schema_epoch,
                routing_epoch: epochs.routing_epoch,
                writes,
                guards: vec![guard.clone().into_sealed()],
            }],
            lifecycle: vec![guard],
            result: serde_json::Value::Null,
            _owner: owner,
        })
    }
}

impl NativePlanner {
    pub(crate) async fn plan_restart_index(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        previous: MaintenanceOwnerRef,
        phase: IndexPhase,
        fenced_table: TableRecord,
        guard: VerifiedLifecycleGuard,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let result = async {
            if guard.mode() != GuardMode::Exclusive
                || guard.binding().txn != txn
                || guard.binding().request != request
                || guard.binding().table_id != previous.table_id
            {
                return Err(not_found().into());
            }
            let key = MvccKey::CatalogIndex {
                table_id: previous.table_id,
                index_id: previous.index_id,
            };
            let (version, record) =
                value(self.catalog.read::<IndexRecord>(pin, key.clone()).await?)?;
            let record = record.ok_or_else(not_found)?;
            let (build_id, attempt, source_pin) = record.phase.attempt_binding();
            let IndexPhase::Building {
                build_id: new_build,
                attempt: new_attempt,
                pin: new_pin,
                source,
            } = &phase
            else {
                return Err(not_found().into());
            };
            if build_id != previous.build_id
                || attempt != previous.attempt
                || !matches!(record.phase, IndexPhase::Building { .. })
                || *new_build != build_id
                || *new_attempt <= attempt
                || new_pin.read_ts <= pin.read_ts
                || source.read_ts != new_pin.read_ts
                || source.authority.run_id != pin.authority.run_id
            {
                return Err(failure(
                    ErrorCode::Conflict,
                    "index restart does not replace the fenced attempt",
                )
                .into());
            }
            let (table_version, table) =
                value(self.catalog.describe(pin, previous.table_id).await?)?;
            table.ok_or_else(not_found)?;
            let closed_key = MvccKey::CatalogIndexClosed {
                table_id: previous.table_id,
                closure_id: closure_id(&previous)?,
            };
            let (closed_version, closed) = value(
                self.catalog
                    .read::<IndexAttemptClosedRecord>(pin, closed_key.clone())
                    .await?,
            )?;
            if closed.is_some() {
                return Err(failure(ErrorCode::Conflict, "index attempt is already closed").into());
            }
            let updated = IndexRecord {
                definition: record.definition.clone(),
                phase,
                schema_epoch: record.schema_epoch,
                routing_epoch: record.routing_epoch,
            };
            self.catalog_index_plan(
                vec![
                    PreparedWrite {
                        key,
                        expected_version: version,
                        change: self.index_put(&updated)?,
                    },
                    PreparedWrite {
                        key: MvccKey::CatalogTable {
                            table_id: previous.table_id,
                        },
                        expected_version: table_version,
                        change: self.index_put(&fenced_table)?,
                    },
                    PreparedWrite {
                        key: closed_key,
                        expected_version: closed_version,
                        change: self.index_put(&IndexAttemptClosedRecord {
                            owner: previous,
                            pin: source_pin,
                            closed_by: txn,
                            kind: IndexClosedKind::Superseded,
                        })?,
                    },
                ],
                guard.clone(),
                owner,
            )
            .await
        }
        .await;
        result.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }
    /// A tombstone-bound cleanup guard can close a build after table deletion.
    /// Its native permission admits only terminal index metadata and a Keep of
    /// that exact tombstone. It cannot publish schema or authorize Data writes.
    pub async fn plan_retire_index(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        maintenance: MaintenanceOwnerRef,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let key = MvccKey::CatalogIndex {
            table_id: maintenance.table_id,
            index_id: maintenance.index_id,
        };
        let (version, record) = value(self.catalog.read::<IndexRecord>(pin, key.clone()).await?)?;
        let record = record.ok_or_else(not_found)?;
        let (build_id, attempt, source_pin) = record.phase.attempt_binding();
        if build_id != maintenance.build_id || attempt != maintenance.attempt {
            return Err(not_found().into());
        }
        let (table_version, table) =
            value(self.catalog.describe(pin, maintenance.table_id).await?)?;
        let mode = if table.is_some() {
            GuardMode::Exclusive
        } else {
            if table_version.is_none() {
                return Err(not_found().into());
            }
            GuardMode::MetadataCleanup
        };
        let guard = self
            .guard_at(
                pin,
                txn,
                request,
                maintenance.table_id,
                table_version,
                record.schema_epoch,
                mode,
                pin.authority,
            )
            .await?;
        let result = async {
            // Plan section 7: a live table's lifecycle changes with the
            // retirement; a dropped table was fenced by its drop.
            let table_record = match table.as_ref() {
                Some(table) => {
                    let (fence, record) = self
                        .prepare_lifecycle_fence(pin, table, txn.request_id)
                        .await?;
                    self.run_lifecycle_fence(pin, maintenance.table_id, fence)
                        .await?;
                    Some(record)
                }
                None => None,
            };
            let closed_key = MvccKey::CatalogIndexClosed {
                table_id: maintenance.table_id,
                closure_id: closure_id(&maintenance)?,
            };
            let (closed_version, closed) = value(
                self.catalog
                    .read::<IndexAttemptClosedRecord>(pin, closed_key.clone())
                    .await?,
            )?;
            let retired = IndexRecord {
                definition: record.definition.clone(),
                phase: IndexPhase::Retired {
                    build_id,
                    attempt,
                    pin: source_pin,
                },
                schema_epoch: record.schema_epoch,
                routing_epoch: record.routing_epoch,
            };
            let mut writes = vec![
                PreparedWrite {
                    key,
                    expected_version: version,
                    change: self.index_put(&retired)?,
                },
                PreparedWrite {
                    key: MvccKey::CatalogTable {
                        table_id: maintenance.table_id,
                    },
                    expected_version: table_version,
                    change: match table_record {
                        Some(record) => self.index_put(&record)?,
                        None => Change::Keep,
                    },
                },
            ];
            // Closure records are immutable: an already Ready/Failed attempt
            // retains its original settlement and idempotent TSO release proof.
            if closed.is_none() {
                writes.push(PreparedWrite {
                    key: closed_key,
                    expected_version: closed_version,
                    change: self.index_put(&IndexAttemptClosedRecord {
                        owner: maintenance,
                        pin: source_pin,
                        closed_by: txn,
                        kind: IndexClosedKind::Retired,
                    })?,
                });
            }
            self.catalog_index_plan(writes, guard.clone(), owner).await
        }
        .await;
        result.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }
}
