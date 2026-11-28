//! Operator-only index composition over the production Status/MVCC services.
//! Metadata jobs have no invented ORM body and never enter an application WAL.
use super::*;
use crate::oltp::mvcc::{
    catalog::PinnedCatalog,
    index::{
        IndexClosedKind, IndexDefinition, IndexRecord,
        build::{
            IndexAttempt, IndexBackend, IndexCommitOutcome, IndexService, declaration_job,
            ready_job,
        },
    },
    planner::{MutationPlan, ResolveFuture},
    store::{
        ParticipantReply,
        index::VerifiedIndexAttemptClosed,
        lifecycle::{GuardMode, LifecycleBinding, LifecycleReply, VerifiedLifecycleGuard},
    },
};
use crate::oltp_replication::{
    authority::{
        status::{RequestKind, StatusFault, StatusReply, TransactionInspection, VerifiedSettlement},
        timestamp::IndexMetadataPin,
    },
    owned::serialized_digest,
};
struct RuntimeIndexes {
    runtime: Arc<WorkerRuntime>,
    authority: Option<Authority>,
}
/// The table of a search column DDL, which L24 repairs first. `table.drop`
/// is not one: it fences above every group whatever they hold.
pub(super) fn search_ddl_table(operation: &protocol::Operation) -> Option<Uuid> {
    use protocol::Operation;
    match operation {
        Operation::AddColumn(p) => Some(p.table_id),
        Operation::DropColumn(p) => Some(p.table_id),
        Operation::AddFullText(p) => Some(p.table_id),
        Operation::DropFullText(p) => Some(p.table_id),
        _ => None,
    }
}

impl WorkerRuntime {
    // Reuse the internal Catalog metadata transaction and settlement path
    // used by online indexes. The reserved owner domain gives repair its
    // own durable request, independent of the subsequent DDL's result.
    pub(super) async fn repair_search_ddl(
        self: &Arc<Self>,
        table_id: Uuid,
        expected: Option<Authority>,
    ) -> Result<(), ProtocolError> {
        let authority = self.index_authority(expected).await?;
        let backend = RuntimeIndexes { runtime: self.clone(), authority: Some(authority) };
        let catalog = &self.services.catalog;
        let probe_pin = catalog.snapshot(ReadTs::new(u64::MAX).map_err(|_| unknown())?, authority)?;
        let scope = catalog.participant().identity().group_id;
        let owner_id = repair_owner(scope, table_id)?;
        let gate = self.gate(owner_id)?;
        let _guard = gate.lock().await;

        // rv-j3 N1: every attempt takes the next id of the run's sequence for
        // the table, so failures never use the ids up. A finished id is
        // skipped; one that stopped before it finished resumes. The gate
        // serializes attempts; the cursor only skips finished ids.
        let mut attempt = self.search_repairs.lock().map_err(|_| unknown())?.get(&owner_id).copied().unwrap_or(1);
        let (mut tries, mut settled) = (0u8, true);
        loop {
            let (id, digest, maintenance) = search_repair_attempt(authority.run_id, scope, table_id, attempt)?;
            let reservation_digest = metadata_digest(id, digest, maintenance)?;
            if let Some(IdentityLookup::Terminal(terminal)) = self.services.status.lookup_identity(id, Some(reservation_digest)).await? {
                // rv-j3 N3: finish an attempt that stopped before it released
                // its guard and pin. A failure leaves the cursor here, so a
                // later repair tries again; this one goes on.
                if let Err(error) = self.settle_search_repair(authority, maintenance, terminal).await {
                    tracing::warn!(table=%table_id, attempt, code=?error.code, message=%error.message,
                        "a stopped search repair keeps its metadata pin");
                    settled = false;
                }
                attempt = attempt.checked_add(1).ok_or_else(unknown)?;
                if settled {
                    self.search_repairs.lock().map_err(|_| unknown())?.insert(owner_id, attempt);
                }
                continue;
            }
            if tries == 3 {
                break;
            }
            tries += 1;
            match self.planner.search_repair_epoch(&probe_pin, table_id).await {
                Ok(Some(_)) => {}
                Ok(None) => return Ok(()),
                Err(PlanError::Blocked(intent)) => {
                    self.help_intent(intent).await?;
                    continue;
                }
                Err(PlanError::Protocol(error)) => return Err(error),
                Err(_) => return Err(unknown()),
            }
            let metadata = backend.begin(id, digest, maintenance, Some(id)).await?;
            let plan = match self.planner.plan_search_repair(
                &metadata.pin, metadata.txn, metadata.request, table_id, metadata.owner.clone()
            ).await {
                Ok(plan) => plan,
                Err(error) => {
                    let (cause, guards) = match error {
                        PlanError::Guarded { cause, guards } => (*cause, guards),
                        other => (other, Vec::new()),
                    };
                    let (error, pending) = match cause {
                        PlanError::Protocol(error) => (error, None),
                        PlanError::LifecyclePending(binding) => (
                            ProtocolError::new(ErrorCode::Conflict, "a lifecycle change is in progress", Outcome::NotApplied),
                            Some(binding),
                        ),
                        PlanError::Blocked(_) | PlanError::Guarded { .. } => (unknown(), None),
                    };
                    let (shared, unguarded) = (pending.clone(), guards.is_empty());
                    backend.fail(metadata, guards, pending, error.clone()).await?;
                    // rv-j3 N2: before the retry, help what kept the guard:
                    // the shared holders it waited on, or another DDL's
                    // exclusive guard. A stopped holder is finished and its
                    // guard released; a live one is left to its actor.
                    if let Some(binding) = shared {
                        backend.help_lifecycle(binding).await?;
                    } else if unguarded && error.code == ErrorCode::Conflict
                        && let Some(holder) = catalog.participant().lifecycle_exclusive(table_id).await?
                    {
                        self.help_lifecycle(holder).await?;
                    }
                    if error.code == ErrorCode::Conflict {
                        continue;
                    }
                    return Err(error);
                }
            };
            match backend.commit(metadata, plan).await? {
                IndexCommitOutcome::Committed => return Ok(()),
                IndexCommitOutcome::Aborted(error) if error.code == ErrorCode::Conflict => continue,
                IndexCommitOutcome::Aborted(error) => return Err(error),
            }
        }
        Err(ProtocolError::new(ErrorCode::Conflict, "search DDL recovery conflicted; retry the request", Outcome::NotApplied))
    }

    /// A finished repair attempt whose actor stopped before it released its
    /// Exclusive guard and metadata pin: resolve it, release the guard (as a
    /// helper does) and then the pin, which otherwise holds the retention floor.
    async fn settle_search_repair(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        terminal: TerminalIdentity,
    ) -> Result<(), ProtocolError> {
        let services = &self.services;
        let (reservation, request) = repair_reservation(&terminal)?;
        let txn = TxnId { request_id: reservation.request_id, attempt: 1 };
        let observation = services.timestamps.index_metadata_pin(authority, owner, txn, reservation, request).await?;
        if observation.released {
            return Ok(());
        }
        if let Some(holder) = services.catalog.participant().lifecycle_exclusive(owner.table_id).await?
            && holder.txn == txn
        {
            self.help_lifecycle(holder).await?;
        }
        let settled = match terminal {
            TerminalIdentity::Committed { decision, .. } => {
                self.transactions.recover(authority, txn).await?.ok_or_else(unknown)?;
                services.status.settled_proof(txn, decision.manifest).await?
            }
            _ => services.status.failure_settled_proof(txn, request).await?,
        };
        let identity = PinIdentity {
            run_id: reservation.original_run,
            request_id: reservation.request_id,
            digest: reservation.digest,
            nonce: reservation.reservation_id,
        };
        services.timestamps.release_index_metadata(authority, identity, observation.pin, settled).await
    }

    pub(super) fn index_service(self: &Arc<Self>, authority: Option<Authority>) -> IndexService {
        IndexService::new(Arc::new(RuntimeIndexes {
            runtime: self.clone(),
            authority,
        }))
    }

    pub(super) async fn index_authority(
        &self,
        expected: Option<Authority>,
    ) -> Result<Authority, ProtocolError> {
        let current = self.services.control.current(&self.services.scope).await?;
        let authority = current.ready.view.authority();
        if current.owner != self.services.local
            || !self.accepting.load(Ordering::Acquire)
            || expected.is_some_and(|expected| expected != authority)
        {
            return Err(unavailable());
        }
        Ok(authority)
    }

    pub(crate) async fn create_index(
        self: &Arc<Self>,
        definition: IndexDefinition,
        build_id: Uuid,
        attempt: u64,
    ) -> Result<(), ProtocolError> {
        self.index_service(None).create(definition, build_id, attempt).await
    }
    pub(crate) async fn resume_index(
        self: &Arc<Self>,
        record: &IndexRecord,
    ) -> Result<(), ProtocolError> {
        self.index_service(None).resume(record).await
    }
    pub(crate) async fn restart_index(
        self: &Arc<Self>,
        record: &IndexRecord,
        next_attempt: u64,
    ) -> Result<(), ProtocolError> {
        self.index_service(None).restart(record, next_attempt).await
    }
    pub(crate) async fn retire_index(
        self: &Arc<Self>,
        record: &IndexRecord,
    ) -> Result<(), ProtocolError> {
        self.index_service(None).retire(record).await
    }

    pub(super) async fn reconcile_ready_index(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<(), ProtocolError> {
        let services = &self.services;
        let catalog = services.catalog.participant();
        let (job, digest) = ready_job(owner)?;
        let (before, original) = self.index_commit(authority, txn, manifest, metadata_digest(job, digest, owner)?).await?;
        let TxnDecision::Commit { commit_ts } = original.decision else {
            return Err(unknown());
        };
        if before.created_authority.run_id != authority.run_id {
            // A cold run can observe an already resolved publication. Old-run
            // cleanup belongs to the native cold recovery path.
            let proof = catalog.strong_index_attempt_closed(owner).await?;
            return check_ready_closure(&proof, txn, commit_ts, manifest);
        }
        let (identity, observation) = self.index_metadata_observation(authority, owner, &original).await?;
        if !observation.released {
            let binding = self.original_index_binding(owner, &before, &original, observation.pin).await?;
            if before.created_authority != authority {
                self.recover_index_publication(owner, binding, identity, observation.pin).await?;
                return Ok(());
            }
            if before.resolved_count == 0 && !catalog.lifecycle_guard(&binding).await?
                .is_some_and(|guard| guard.mode() == GuardMode::Exclusive)
            {
                return Err(unknown());
            }
            let decision = self.transactions.recover(authority, txn).await?.ok_or_else(unknown)?;
            if decision.statement() != &original {
                return Err(unknown());
            }
            let proof = catalog.strong_index_attempt_closed(owner).await?;
            check_ready_closure(&proof, txn, commit_ts, manifest)?;
            let settled = services.status.settled_proof(txn, manifest).await?;
            if let Some(guard) = catalog.lifecycle_guard(&binding).await? {
                if guard.mode() != GuardMode::Exclusive {
                    return Err(unknown());
                }
                let released = catalog.release_lifecycle(authority, guard, settled.clone()).await?;
                if !matches!(released.reply, ParticipantReply::Lifecycle(LifecycleReply::Released)) {
                    return Err(unknown());
                }
            }
            services.timestamps.release_index_metadata(authority, identity, observation.pin, settled).await?;
            return services.timestamps.release_maintenance(authority, proof).await;
        }
        let proof = catalog.strong_index_attempt_closed(owner).await?;
        check_ready_closure(&proof, txn, commit_ts, manifest)?;
        services.timestamps.release_maintenance(authority, proof).await
    }

    pub(super) async fn reconcile_index_declaration(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        intent: IntentRef,
        definition: &IndexDefinition,
    ) -> Result<(), ProtocolError> {
        let (job, digest) = declaration_job(owner, definition)?;
        let (before, original) = self.index_commit(authority, intent.txn, intent.manifest, metadata_digest(job, digest, owner)?).await?;
        if before.created_authority.run_id != authority.run_id {
            return Err(unknown());
        }
        let (identity, observation) = self.index_metadata_observation(authority, owner, &original).await?;
        if observation.released {
            return Ok(());
        }
        let binding = self.original_index_binding(owner, &before, &original, observation.pin).await?;
        let catalog = self.services.catalog.participant();
        let guard = catalog.lifecycle_guard(&binding).await?;
        if guard.as_ref().is_some_and(|guard| guard.mode() != GuardMode::Exclusive)
            || (before.resolved_count == 0 && guard.is_none())
        {
            return Err(unknown());
        }
        let decision = self.transactions.recover(authority, intent.txn).await?.ok_or_else(unknown)?;
        if decision.statement() != &original {
            return Err(unknown());
        }
        let settled = self.services.status.settled_proof(intent.txn, intent.manifest).await?;
        if let Some(guard) = guard {
            let released = catalog.release_lifecycle(authority, guard, settled.clone()).await?;
            if !matches!(released.reply, ParticipantReply::Lifecycle(LifecycleReply::Released)) {
                return Err(unknown());
            }
        }
        self.services.timestamps.release_index_metadata(authority, identity, observation.pin, settled).await
    }

    async fn index_commit(
        &self,
        authority: Authority,
        txn: TxnId,
        manifest: ManifestRef,
        digest: Digest,
    ) -> Result<(TransactionInspection, DecisionStatement), ProtocolError> {
        self.index_authority(Some(authority)).await?;
        let before = self.services.status.inspect_transaction(txn).await?.ok_or_else(unknown)?;
        let original = before.decision.as_ref().ok_or_else(unknown)?;
        if before.current_authority != authority
            || (before.created_authority.run_id == authority.run_id
                && before.created_authority.owner_term > authority.owner_term)
            || txn.attempt != 1 || original.txn != txn || original.manifest != manifest
            || original.manifest.participant_count != 1 || original.request.digest != digest
            || !matches!(original.decision, TxnDecision::Commit { .. })
        {
            return Err(unknown());
        }
        let original = original.clone();
        Ok((before, original))
    }

    async fn index_metadata_observation(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        original: &DecisionStatement,
    ) -> Result<(PinIdentity, IndexMetadataPin), ProtocolError> {
        let services = &self.services;
        let txn = original.txn;
        let reservation = match services.status.lookup_identity(txn.request_id, Some(original.request.digest)).await? {
            Some(IdentityLookup::Terminal(TerminalIdentity::Committed { reservation, decision, .. }))
                if &decision == original => reservation,
            _ => return Err(unknown()),
        };
        let observation = services.timestamps.index_metadata_pin(authority, owner, txn, reservation, original.request).await?;
        Ok((PinIdentity {
            run_id: reservation.original_run,
            request_id: reservation.request_id,
            digest: reservation.digest,
            nonce: reservation.reservation_id,
        }, observation))
    }

    async fn original_index_binding(
        &self,
        owner: MaintenanceOwnerRef,
        before: &TransactionInspection,
        original: &DecisionStatement,
        pin: StoragePin,
    ) -> Result<LifecycleBinding, ProtocolError> {
        let catalog = self.services.catalog.snapshot(pin.read_ts, before.created_authority)?;
        self.planner.index_publication_binding(&catalog, original.txn, original.request, owner.table_id)
            .await.map_err(|_| unknown())
    }

    /// Finish a Ready publication whose original owner stopped after COMMIT.
    /// The original metadata identity and guard bind cleanup to that attempt.
    pub(crate) async fn recover_index_publication(
        &self,
        owner: MaintenanceOwnerRef,
        binding: LifecycleBinding,
        identity: PinIdentity,
        pin: StoragePin,
    ) -> Result<VerifiedIndexAttemptClosed, ProtocolError> {
        let services = &self.services;
        let current = services.control.current(&services.scope).await?;
        let authority = current.ready.view.authority();
        let catalog = services.catalog.participant();
        if current.owner != services.local
            || binding.authority.run_id != authority.run_id
            || binding.authority.owner_term >= authority.owner_term
            || binding.table_id != owner.table_id
            || binding.catalog_group_id != owner.catalog_group_id
            || binding.catalog_group_id != catalog.identity().group_id
            || identity.request_id != binding.txn.request_id
            || identity.run_id != binding.request.run_id
            || identity.digest != binding.request.digest
            || pin.read_ts != binding.read_ts
        {
            return Err(unknown());
        }
        let before = services
            .status
            .inspect_transaction(binding.txn)
            .await?
            .ok_or_else(unknown)?;
        let original = before.decision.ok_or_else(unknown)?;
        let TxnDecision::Commit { commit_ts } = original.decision else {
            return Err(unknown());
        };
        if before.current_authority != authority
            || before.created_authority != binding.authority
            || original.request != binding.request
            || original.manifest.participant_count != 1
        {
            return Err(unknown());
        }
        if before.resolved_count == 0
            && !catalog
                .lifecycle_guard(&binding)
                .await?
                .is_some_and(|guard| guard.mode() == GuardMode::Exclusive)
        {
            return Err(unknown());
        }
        let decision = self
            .transactions
            .recover(authority, binding.txn)
            .await?
            .ok_or_else(unknown)?;
        if decision.statement() != &original {
            return Err(unknown());
        }
        let proof = catalog.strong_index_attempt_closed(owner).await?;
        let closure = proof.archive_evidence();
        if closure.statement.kind != IndexClosedKind::Ready
            || closure.txn != binding.txn
            || closure.manifest != original.manifest
            || closure.commit_ts != commit_ts
        {
            return Err(unknown());
        }
        let settled = services
            .status
            .settled_proof(binding.txn, original.manifest)
            .await?;
        catalog
            .recover_index_publication(authority, owner, binding, settled.clone())
            .await?;
        services
            .timestamps
            .release_index_metadata(authority, identity, pin, settled)
            .await?;
        services
            .timestamps
            .release_maintenance(authority, proof.clone())
            .await?;
        Ok(proof)
    }
}
impl IndexBackend for RuntimeIndexes {
    fn planner(&self) -> &NativePlanner {
        &self.runtime.planner
    }
    fn checkpoint(&self) -> ResolveFuture<'_, ()> {
        Box::pin(async move {
            self.runtime.index_authority(self.authority).await?;
            Ok(())
        })
    }
    fn repair_search_ddl(&self, table_id: Uuid) -> ResolveFuture<'_, ()> {
        Box::pin(self.runtime.repair_search_ddl(table_id, self.authority))
    }
    fn begin(
        &self,
        job: Uuid,
        digest: Digest,
        maintenance: MaintenanceOwnerRef,
        fixed_identity: Option<Uuid>,
    ) -> ResolveFuture<'_, IndexAttempt> {
        Box::pin(async move {
            let services = &self.runtime.services;
            let current = services.control.current(&services.scope).await?;
            let authority = current.ready.view.authority();
            if current.owner != services.local
                || !self.runtime.accepting.load(Ordering::Acquire)
                || self.authority.is_some_and(|expected| expected != authority)
            {
                return Err(unavailable());
            }
            // A metadata execution is a new internal transaction. Its context
            // binds the stable logical job and fenced build attempt. A resumed
            // completed progress step may repeat safely without rewriting or
            // evicting the previous transaction's durable UUID decision.
            let id = fixed_identity.unwrap_or_else(nabedra_rt::ids::uuid_v4);
            let digest = metadata_digest(job, digest, maintenance)?;
            let owner = Arc::new(services.shared.reserve_scratch(ByteClass::Data, 4 << 20)?);
            let reservation = match services
                .status
                .reserve_identity(authority, id, digest, RequestKind::Mutation)
                .await?
                .reply
            {
                StatusReply::Identity(IdentityLookup::Reserved(reservation))
                    if reservation.request_id == id && reservation.digest == digest
                        && reservation.original_run == authority.run_id => reservation,
                StatusReply::Identity(IdentityLookup::Terminal(TerminalIdentity::Failed { outcome, .. })) => {
                    return Err(outcome.result().err().unwrap_or_else(unknown));
                }
                StatusReply::Rejected(StatusFault::DigestConflict) => {
                    return Err(ProtocolError::new(ErrorCode::Conflict,
                        "index declaration UUID already binds different parameters", Outcome::NotApplied));
                }
                _ => return Err(unknown()),
            };
            let txn = TxnId {
                request_id: id,
                attempt: 1,
            };
            let storage_pin = services
                .timestamps
                .begin_index_metadata(authority, maintenance, txn, reservation)
                .await?;
            let identity = PinIdentity {
                run_id: reservation.original_run,
                request_id: id,
                digest,
                nonce: reservation.reservation_id,
            };
            let pin = services.catalog.snapshot(storage_pin.read_ts, authority)?;
            let mut capture = None;
            for _ in 0..8 {
                // Capture only the table's actual catalog lineage. This helper
                // operation is neither admitted nor persisted as executable work.
                match services
                    .catalog
                    .capture_lineage(
                        &pin,
                        &protocol::Operation::Drop(protocol::TablePayload {
                            table_id: maintenance.table_id,
                        }),
                    )
                    .await?
                {
                    CaptureRead::Captured(captured) => {
                        capture = Some(captured);
                        break;
                    }
                    CaptureRead::Blocked(intent) => self.runtime.help_intent(intent).await?,
                }
            }
            let capture = capture.ok_or_else(unknown)?;
            let request = RequestBinding {
                run_id: authority.run_id,
                request_id: id,
                digest,
                accepted_context: serialized_digest(&(
                    b"index-metadata-context/v1",
                    job,
                    maintenance,
                    &pin,
                    capture.catalog_root,
                    capture.schema_epoch,
                    capture.routing_epoch,
                ))?,
            };
            services
                .timestamps
                .bind_snapshot_context(current.ready.view, identity.clone(), storage_pin, request)
                .await?;
            #[cfg(test)]
            super::super::recovery_tests::index_faults::observe_attempt(txn, &identity, storage_pin);
            Ok(IndexAttempt {
                pin,
                txn,
                request,
                owner,
                identity,
                storage_pin,
            })
        })
    }
    fn commit(
        &self,
        attempt: IndexAttempt,
        plan: MutationPlan,
    ) -> ResolveFuture<'_, IndexCommitOutcome> {
        Box::pin(async move {
            let current = self
                .runtime
                .services
                .control
                .current(&self.runtime.services.scope)
                .await?;
            let authority = current.ready.view.authority();
            if current.owner != self.runtime.services.local || authority != attempt.pin.authority {
                return Err(unknown());
            }
            let guards = plan.lifecycle.clone();
            #[cfg(test)]
            super::super::recovery_tests::index_holds::bind(
                attempt.pin.catalog.group_id,
                attempt.txn,
            );
            let decision = self
                .runtime
                .transactions
                .commit(
                    authority,
                    attempt.txn,
                    attempt.request,
                    attempt.pin.read_ts,
                    plan,
                )
                .await?;
            self.runtime
                .transactions
                .resolve(authority, &decision)
                .await?;
            let settled = self
                .runtime
                .services
                .status
                .settled_proof(attempt.txn, decision.statement().manifest)
                .await?;
            let catalog = self.runtime.services.catalog.participant();
            // Commit may have returned a durable decision after a failed Resolve.
            // The second Resolve must also finish any guard it left behind.
            for guard in guards {
                if let Some(current) = catalog.lifecycle_guard(guard.binding()).await? {
                    if current.mode() != guard.mode() {
                        return Err(unknown());
                    }
                    let released = catalog
                        .release_lifecycle(authority, current, settled.clone())
                        .await?;
                    if !matches!(
                        released.reply,
                        ParticipantReply::Lifecycle(LifecycleReply::Released)
                    ) {
                        return Err(unknown());
                    }
                }
            }
            self.runtime
                .services
                .timestamps
                .release_index_metadata(authority, attempt.identity, attempt.storage_pin, settled)
                .await?;
            match decision.outcome().ok_or_else(unknown)?.result() {
                Ok(_) => Ok(IndexCommitOutcome::Committed),
                Err(error)
                    if matches!(decision.statement().decision, TxnDecision::Abort { .. }) =>
                {
                    Ok(IndexCommitOutcome::Aborted(error))
                }
                Err(error) => Err(error),
            }
        })
    }
    fn source_pin(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
    ) -> ResolveFuture<'_, (StoragePin, PinnedCatalog)> {
        Box::pin(async move {
            let pin = self
                .runtime
                .services
                .timestamps
                .begin_maintenance(authority, owner)
                .await?;
            Ok((
                pin,
                self.runtime
                    .services
                    .catalog
                    .snapshot(pin.read_ts, authority)?,
            ))
        })
    }
    fn refresh(&self, table: Uuid, key: protocol::Key, job: Uuid) -> ResolveFuture<'_, ()> {
        Box::pin(async move {
            use std::time::Duration;

            let request = Arc::new(protocol::Request {
                request_id: job,
                operation: protocol::Operation::Update(protocol::UpdatePayload {
                    table_id: table,
                    key,
                    values: protocol::Row::new(),
                }),
            });
            let resources = self.runtime.services.shared.clone();
            let mut retries = 0;
            let mut backoff = Duration::from_millis(50);
            let accepted = loop {
                let request = request.clone();
                let body = resources
                    .execute(ExecutionClass::Query, 0, 0, move || {
                        serde_json::to_vec(&*request).map_err(|_| unknown())
                    })
                    .await?;
                match self
                    .runtime
                    .admit_with_authority(body, self.authority)
                    .await
                {
                    Err(error)
                        if retries < 7
                            && error.code == ErrorCode::Overloaded
                            && error.outcome == Outcome::NotApplied =>
                    {
                        // A definitive admission refusal can be retried with
                        // the same job identity. Receipts and uncertain
                        // outcomes take their normal path below.
                        #[cfg(test)]
                        crate::oltp_runtime::recovery_tests::index_holds::checkpoint(
                            job,
                            crate::oltp_runtime::recovery_tests::index_holds::Checkpoint::RefreshAdmissionRefused,
                            || None,
                        ).await;
                        nabedra_rt::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_millis(250));
                        retries += 1;
                    }
                    result => break result?,
                }
            };
            nabedra_rt::time::timeout(Duration::from_secs(30), async {
                loop {
                    if !self.runtime.accepting.load(Ordering::Acquire) {
                        return Err(unavailable());
                    }
                    let receipt = self.runtime.observe(accepted.run_id, job).await?;
                    match receipt.state {
                        RequestState::Succeeded => return Ok(()),
                        RequestState::Failed => {
                            return Err(receipt.error.clone().unwrap_or_else(unknown));
                        }
                        RequestState::Abandoned => return Err(unavailable()),
                        _ => nabedra_rt::time::sleep(Duration::from_millis(10)).await,
                    }
                }
            }).await.map_err(|_| unknown())?
        })
    }
    fn close_source(
        &self,
        authority: Authority,
        proof: VerifiedIndexAttemptClosed,
    ) -> ResolveFuture<'_, ()> {
        Box::pin(async move {
            self.runtime
                .services
                .timestamps
                .release_maintenance(authority, proof)
                .await
        })
    }
    fn help(&self, intent: IntentRef) -> ResolveFuture<'_, ()> {
        Box::pin(async move { self.runtime.help_intent(intent).await })
    }
    fn help_lifecycle(
        &self,
        binding: crate::oltp::mvcc::store::lifecycle::LifecycleBinding,
    ) -> ResolveFuture<'_, ()> {
        Box::pin(async move {
            if let Some(holder) = self
                .runtime
                .services
                .catalog
                .participant()
                .lifecycle_holder(binding.table_id)
                .await?
            {
                self.runtime.help_lifecycle(holder).await?;
            }
            Ok(())
        })
    }
    fn fail(
        &self,
        attempt: IndexAttempt,
        guards: Vec<VerifiedLifecycleGuard>,
        pending: Option<LifecycleBinding>,
        error: ProtocolError,
    ) -> ResolveFuture<'_, ()> {
        Box::pin(async move {
            if error.outcome != Outcome::NotApplied {
                return Err(error);
            }
            let current = self
                .runtime
                .services
                .control
                .current(&self.runtime.services.scope)
                .await?;
            let authority = current.ready.view.authority();
            if current.owner != self.runtime.services.local || authority != attempt.pin.authority {
                return Err(unknown());
            }
            self.runtime
                .services
                .status
                .terminal_failure(authority, attempt.request, error)
                .await?;
            let settled = self
                .runtime
                .services
                .status
                .failure_settled_proof(attempt.txn, attempt.request)
                .await?;
            let catalog = self.runtime.services.catalog.participant();
            for guard in guards {
                let released = catalog
                    .release_lifecycle(authority, guard, settled.clone())
                    .await?;
                if !matches!(released.reply, ParticipantReply::Lifecycle(LifecycleReply::Released)) {
                    return Err(unknown());
                }
            }
            if let Some(binding) = pending {
                let cancelled = catalog
                    .cancel_pending_lifecycle(authority, binding, settled.clone()).await?;
                if !matches!(cancelled.reply, ParticipantReply::Lifecycle(LifecycleReply::Released)) {
                    return Err(unknown());
                }
            }
            self.runtime
                .services
                .timestamps
                .release_index_metadata(authority, attempt.identity, attempt.storage_pin, settled)
                .await
        })
    }
}

fn repair_reservation(terminal: &TerminalIdentity) -> Result<(IdentityReservation, RequestBinding), ProtocolError> {
    match terminal {
        TerminalIdentity::Committed { reservation, decision, .. } => Ok((*reservation, decision.request)),
        TerminalIdentity::Failed { reservation, request, .. } => Ok((*reservation, *request)),
        TerminalIdentity::ReadExpired { .. } => Err(unknown()),
    }
}

fn repair_owner(scope: Uuid, table_id: Uuid) -> Result<Uuid, ProtocolError> {
    Ok(repair_uuid(serialized_digest(&(b"search-repair-owner/v1", scope, table_id))?))
}

/// The `attempt`th (from 1) repair transaction of `table_id` in run `run_id`:
/// its id, digest and owner. They are fixed, so a stopped attempt resumes on
/// its id and a later repair finds it.
fn search_repair_attempt(
    run_id: Uuid,
    scope: Uuid,
    table_id: Uuid,
    attempt: u64,
) -> Result<(Uuid, Digest, MaintenanceOwnerRef), ProtocolError> {
    let owner_id = repair_owner(scope, table_id)?;
    let maintenance = MaintenanceOwnerRef {
        catalog_group_id: scope,
        table_id,
        index_id: owner_id,
        build_id: owner_id,
        attempt,
    };
    let digest = serialized_digest(&(b"search-repair/v2", run_id, maintenance))?;
    Ok((repair_uuid(digest), digest, maintenance))
}

fn repair_uuid(digest: Digest) -> Uuid {
    let mut id = [0; 16];
    id.copy_from_slice(&digest.as_bytes()[..16]);
    // Custom UUID version and RFC variant, outside randomly issued v4 IDs.
    id[6] = (id[6] & 0x0f) | 0x80;
    id[8] = (id[8] & 0x3f) | 0x80;
    Uuid::from_bytes(id)
}

fn metadata_digest(job: Uuid, digest: Digest, owner: MaintenanceOwnerRef) -> Result<Digest, ProtocolError> {
    serialized_digest(&(b"index-metadata/v1", job, digest, owner))
}

fn check_ready_closure(
    proof: &VerifiedIndexAttemptClosed,
    txn: TxnId,
    commit_ts: CommitTs,
    manifest: ManifestRef,
) -> Result<(), ProtocolError> {
    let closure = proof.archive_evidence();
    if closure.statement.kind != IndexClosedKind::Ready || closure.txn != txn
        || closure.manifest != manifest || closure.commit_ts != commit_ts
    {
        return Err(unknown());
    }
    Ok(())
}

#[cfg(test)]
impl WorkerRuntime {
    /// The production index backend, for tests that wrap it.
    pub(super) fn index_backend_for_test(self: &Arc<Self>) -> Arc<dyn IndexBackend> {
        Arc::new(RuntimeIndexes { runtime: self.clone(), authority: None })
    }

    /// Whether the metadata pin of the table's `attempt`th search repair in
    /// this run is released; None while the attempt has no Status outcome.
    pub(super) async fn search_repair_pin_released(
        &self,
        table_id: Uuid,
        attempt: u64,
    ) -> Result<Option<bool>, ProtocolError> {
        let authority = self.index_authority(None).await?;
        let scope = self.services.catalog.participant().identity().group_id;
        let (id, digest, owner) = search_repair_attempt(authority.run_id, scope, table_id, attempt)?;
        let lookup = self.services.status.lookup_identity(id, Some(metadata_digest(id, digest, owner)?)).await?;
        let Some(IdentityLookup::Terminal(terminal)) = lookup else {
            return Ok(None);
        };
        let (reservation, request) = repair_reservation(&terminal)?;
        let txn = TxnId { request_id: id, attempt: 1 };
        let pin = self.services.timestamps.index_metadata_pin(authority, owner, txn, reservation, request).await?;
        Ok(Some(pin.released))
    }

    pub(crate) async fn run_index_fault_command(
        self: &Arc<Self>,
        command: super::super::recovery_tests::index_faults::Command,
    ) -> Result<serde_json::Value, ProtocolError> {
        use super::super::recovery_tests::index_faults::Command;
        use crate::oltp::mvcc::{MvccKey, catalog::CatalogRead};
        use serde_json::json;

        let current = self.services.control.current(&self.services.scope).await?;
        let authority = current.ready.view.authority();
        let catalog = self.services.catalog.participant();
        let status = &self.services.status;
        let timestamps = &self.services.timestamps;
        match command {
            Command::Create {
                definition,
                build_id,
                attempt,
            } => {
                self.create_index(definition, build_id, attempt).await?;
            }
            Command::Resume { record } => self.resume_index(&record).await?,
            Command::Authority { data } => {
                if data.len() > 8 {
                    return Err(unknown());
                }
                let mut epochs = Vec::new();
                for group in data {
                    let participant = self.planner.index_participant(&group).await?;
                    epochs.push(json!({"group": group, "epochs": participant.epochs().await?}));
                }
                return Ok(json!({
                    "queue": current,
                    "local": self.services.local,
                    "catalog": catalog.epochs().await?,
                    "tso": timestamps.current_authority().await?,
                    "data": epochs,
                }));
            }
            Command::ValidationFailure { owner, guard } => {
                let settled = status
                    .failure_settled_proof(guard.txn, guard.request)
                    .await?;
                return Ok(json!({
                    "settlement": settled.statement(),
                    "guard_released": catalog.lifecycle_guard(&guard).await?.is_none(),
                    "closure_error": catalog.strong_index_attempt_closed(owner).await.err(),
                }));
            }
            Command::RecoverPublication {
                owner,
                txn,
                guard,
                identity,
                pin,
            } => {
                if guard.txn != txn {
                    return Err(unknown());
                }
                let before = status.inspect_transaction(txn).await?.ok_or_else(unknown)?;
                let held = catalog.lifecycle_guard(&guard).await?;
                let observed_guard = held.map(VerifiedLifecycleGuard::into_sealed);
                let statement = before.decision.as_ref().ok_or_else(unknown)?;
                let proof = self
                    .recover_index_publication(owner, guard.clone(), identity, pin)
                    .await?;
                let settled = status.settled_proof(txn, statement.manifest).await?;
                return Ok(json!({
                    "before": before,
                    "decision": statement,
                    "settlement": settled.statement(),
                    "closure": proof.archive_evidence(),
                    "guard_before": observed_guard,
                    "guard_released": catalog.lifecycle_guard(&guard).await?.is_none(),
                }));
            }
            Command::Closure { owner } => {
                let proof = catalog.strong_index_attempt_closed(owner).await?;
                return Ok(json!(proof.archive_evidence()));
            }
            Command::Retire { owner } => {
                let before = catalog.strong_index_attempt_closed(owner).await?;
                let pin = self
                    .services
                    .catalog
                    .snapshot(before.archive_evidence().commit_ts, authority)?;
                let read = self
                    .services
                    .catalog
                    .read::<IndexRecord>(
                        &pin,
                        MvccKey::CatalogIndex {
                            table_id: owner.table_id,
                            index_id: owner.index_id,
                        },
                    )
                    .await?;
                let CatalogRead::Value {
                    record: Some(record),
                    ..
                } = read
                else {
                    return Err(unknown());
                };
                self.retire_index(&record).await?;
                let after = catalog.strong_index_attempt_closed(owner).await?;
                return Ok(
                    json!({"before": before.archive_evidence(), "after": after.archive_evidence()}),
                );
            }
            Command::Transaction { txn } => {
                let state = status.inspect_transaction(txn).await?.ok_or_else(unknown)?;
                let decision = state.decision.as_ref().ok_or_else(unknown)?;
                let settlement = status.settled_proof(txn, decision.manifest).await?;
                return Ok(json!({"state": state, "settlement": settlement.statement()}));
            }
        }
        Ok(json!({"completed": true}))
    }
}

#[cfg(test)]
mod acceptance_tests;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ColumnType, Query, Row, Scalar, TableDescriptor};
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;
    pub(super) async fn call(app: &axum::Router, operation: &str, payload: Value) -> Value {
        let body = json!({"request_id":nabedra_rt::ids::uuid_v4(),"operation":operation,"payload":payload});
        let response = app
            .clone()
            .oneshot(
                Request::post("/v1/operations")
                    .header("authorization", "Bearer index-test")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), crate::protocol::MAX_BODY_BYTES)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            status,
            axum::http::StatusCode::OK,
            "operation={operation} payload={} response={value}",
            body["payload"]
        );
        value["data"].clone()
    }
    pub(super) async fn check(
        app: &axum::Router,
        table: &TableDescriptor,
        rows: &[Row],
        query: Query,
    ) {
        let actual = call(
            app,
            "query.collect",
            json!({"table_id":table.id,"query":query}),
        )
        .await;
        let mut expected: Vec<Row> = rows
            .iter()
            .filter(|r| crate::query_semantics::matches_filter(&query, r))
            .cloned()
            .collect();
        expected.sort_by(|a, b| crate::query_semantics::compare_rows(table, &query, a, b));
        let count = expected.len() as u64;
        expected.truncate(query.limit.unwrap_or(u64::MAX).min(usize::MAX as u64) as usize);
        if let Some(select) = &query.select {
            for row in &mut expected {
                row.retain(|key, _| select.contains(key));
            }
        }
        assert_eq!(
            actual,
            serde_json::to_value(expected).unwrap(),
            "query {query:?}"
        );
        assert_eq!(
            call(
                app,
                "query.count",
                json!({"table_id":table.id,"query":query})
            )
            .await,
            json!(count.min(query.limit.unwrap_or(u64::MAX)))
        );
    }
    async fn reject_mixed_predicate(app: &axum::Router, table: Uuid) {
        let body = serde_json::json!({"request_id":nabedra_rt::ids::uuid_v4(),"operation":"query.collect","payload":{"table_id":table,"query":{"select":null,"filter":{"column":"price","op":"gt","value":i64::MAX},"order_by":null,"limit":null}}});
        let response = app
            .clone()
            .oneshot(
                Request::post("/v1/operations")
                    .header("authorization", "Bearer index-test")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        let bytes = to_bytes(response.into_body(), crate::protocol::MAX_BODY_BYTES)
            .await
            .unwrap();
        let error: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(error["error"]["code"], "validation_error");
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_online_index_public_query_oracle() {
        nabedra_rt::time::timeout(std::time::Duration::from_secs(120),async {
            let path=std::env::temp_dir().join(format!("nabedra-index-runtime-{}",nabedra_rt::ids::uuid_v4()));
            let runtime=WorkerRuntime::open(&path,"index-test","index-test",ExecutionConfig{max_pending_requests:32,max_queue_bytes_per_replica:8<<20,max_retained_result_bytes_per_worker:8<<20,..Default::default()},WorkerResources{max_threads:2,memory_limit_bytes:2<<30,..Default::default()}).await.unwrap();
            let app=runtime.application("index-test".into()).unwrap();
            let descriptor=call(&app,"table.create",json!({"name":"items","schema":{"id":"int64","category":"int64","price":"float64","name":"utf8"},"key":"id","format":"parquet"})).await;
            let table:TableDescriptor=serde_json::from_value(descriptor).unwrap();
            assert_eq!(table.schema["price"],ColumnType::Float64);
            let mut rows=Vec::new();
            for (i,id) in [i64::MIN,-11,-2,-1,0,1,2,3,4,5,8,13,i64::MAX].into_iter().enumerate() {
                let price=match i {0=>-f64::MAX,1=>-0.0,2=>0.0,11=>2f64.powi(63),12=>f64::MAX,_=>(i%3) as f64};
                let row=Row::from([("id".into(),Scalar::Int64(id)),("category".into(),Scalar::Int64((i%2) as i64)),("price".into(),Scalar::Float64(price)),("name".into(),Scalar::Utf8(format!("row-{i}\0suffix")))]);
                call(&app,"table.insert",json!({"table_id":table.id,"row":row})).await;rows.push(row);
            }
            let queries:Vec<Query>=vec![
                json!({"select":null,"filter":null,"order_by":null,"limit":null}),
                json!({"select":["id","name"],"filter":{"column":"category","op":"eq","value":1},"order_by":{"column":"price","desc":true},"limit":4}),
                json!({"select":["id","price"],"filter":{"op":"or","args":[{"column":"price","op":"eq","value":0.0},{"op":"not","arg":{"column":"category","op":"eq","value":0}}]},"order_by":{"column":"price","desc":true},"limit":7}),
                json!({"select":null,"filter":{"column":"price","op":"gt","value":2f64.powi(63)-1024.0},"order_by":null,"limit":null}),
                json!({"select":["id"],"filter":{"column":"price","op":"eq","value":-0.0},"order_by":null,"limit":2}),
                json!({"select":null,"filter":{"column":"category","op":"eq","value":999},"order_by":null,"limit":null}),
                json!({"select":null,"filter":null,"order_by":{"column":"price","desc":true},"limit":0}),
            ].into_iter().map(|q|serde_json::from_value(q).unwrap()).collect();
            for query in &queries {check(&app,&table,&rows,query.clone()).await;}
            reject_mixed_predicate(&app,table.id).await;
            runtime.create_index(IndexDefinition{index_id:nabedra_rt::ids::uuid_v4(),table_id:table.id,name:Text::try_from("category_price").unwrap(),columns:Page::new(vec![Text::try_from("category").unwrap(),Text::try_from("price").unwrap()]).unwrap(),covering:Page::new(vec![Text::try_from("name").unwrap()]).unwrap()},nabedra_rt::ids::uuid_v4(),1).await.unwrap();
            for query in &queries {check(&app,&table,&rows,query.clone()).await;}
            reject_mixed_predicate(&app,table.id).await;
            let explanation=call(&app,"query.explain",json!({"table_id":table.id,"query":queries[1]})).await;
            assert!(explanation.as_str().unwrap().contains("secondary category_price"));
            assert!(explanation.as_str().unwrap().contains("covering true"));
            call(&app,"table.update",json!({"table_id":table.id,"key":1,"values":{"category":1,"price":0.0,"name":"changed"}})).await;
            let row=rows.iter_mut().find(|r|r["id"]==Scalar::Int64(1)).unwrap();row.insert("category".into(),Scalar::Int64(1));row.insert("price".into(),Scalar::Float64(0.0));row.insert("name".into(),Scalar::Utf8("changed".into()));
            call(&app,"table.delete",json!({"table_id":table.id,"key":-2})).await;rows.retain(|r|r["id"]!=Scalar::Int64(-2));
            for query in queries {check(&app,&table,&rows,query).await;}
            runtime.drain().await.unwrap();drop(app);drop(runtime);std::fs::remove_dir_all(path).unwrap();
        }).await.expect("online index runtime watchdog");
    }
}
