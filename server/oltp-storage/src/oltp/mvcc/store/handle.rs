//! Strong participant capabilities. These wrappers cannot be deserialized by
//! callers; only the leader's quorum barrier plus durable snapshot creates one.
use super::*;
use super::consensus::{Applied, Consensus, ConsensusFuture};
use crate::oltp::{
    pool::{ExecutionClass, ScratchLease},
    read_attempt::ScheduledReadError,
};
use nabedra_oltp_types::authority::{
    ParticipantProofKind, ReadScope, ScopedCatalogRead, ScopedQueueTransition, StatusDecisionProof,
};

/// Authenticated strong-read context bound to one participant reply.
pub trait ParticipantReplyProof {
    fn group(&self) -> &GroupIdentity;
    fn owner(&self) -> Arc<ScratchLease>;
    fn verify_payload(
        &self,
        kind: ParticipantProofKind,
        value: &impl Serialize,
    ) -> Result<(), ProtocolError>;
}

const META: TransactionBudget = TransactionBudget {
    scratch_bytes: 128 << 10,
    batch_bytes: 0,
    output_bytes: 16 << 10,
};
pub type ParticipantFuture<'a, T> = ConsensusFuture<'a, T>;
pub type ScheduledReadFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    OwnedCommand<super::scan::ScanPage>, ScheduledReadError,
                >,
            > + Send
            + 'a,
    >,
>;
mod direct;
mod search_fence;
mod timestamp_point;
pub use direct::LatestRead;
pub use timestamp_point::PointEvidenceRead;

pub trait RemoteParticipant: Send + Sync {
    fn next_stored_index(&self, _after: Option<Uuid>) -> ParticipantFuture<'_, Option<Uuid>> {
        Box::pin(async { Err(invalid()) })
    }
    fn resolve_timestamp_point(
        &self,
        _authority: Authority,
        _locator: TimestampDecisionLocator,
    ) -> ParticipantFuture<'_, Applied<ParticipantReply>> {
        Box::pin(async { Err(invalid()) })
    }
    fn with_scoped_execution(
        &self,
        _scope: ScopedCatalogRead,
    ) -> Result<Arc<dyn RemoteParticipant>, ProtocolError> {
        Err(invalid())
    }
    fn propose(
        &self,
        command: OwnedCommand<ParticipantCommand>,
    ) -> ParticipantFuture<'_, Applied<ParticipantReply>>;
    fn read_at(&self, read_ts: ReadTs, key: MvccKey) -> ParticipantFuture<'_, ChargedBytes>;
    fn read_scoped_catalog(
        &self,
        _scope: ScopedCatalogRead,
        _key: MvccKey,
    ) -> ParticipantFuture<'_, ChargedBytes> {
        Box::pin(async { Err(invalid()) })
    }
    fn read_page(
        &self,
        request: OwnedCommand<super::point::PointReadRequest>,
    ) -> ParticipantFuture<'_, OwnedCommand<super::scan::ScanPage>>;
    fn committed_image_page(
        &self,
        request: OwnedCommand<super::image_archive::ImageRequest>,
    ) -> ParticipantFuture<'_, OwnedCommand<super::image_archive::ImagePage>>;
    fn archive_effect_page<'a>(
        &'a self,
        proof: &'a dyn StatusDecisionProof,
        after: u64,
        max_items: u16,
        max_bytes: usize,
    ) -> ParticipantFuture<'a, OwnedCommand<super::archive::ArchiveEffects>>;
    fn scheduled_scan_page(
        &self,
        scope: ReadScope,
        request: super::scan::ScanRequest,
    ) -> ScheduledReadFuture<'_>;
    fn scan_page(
        &self,
        request: super::scan::ScanRequest,
    ) -> ParticipantFuture<'_, OwnedCommand<super::scan::ScanPage>>;
    fn activation(&self) -> ParticipantFuture<'_, Option<ParticipantEpochs>>;
    fn strong_seal(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ParticipantFuture<'_, VerifiedParticipantSeal>;
    fn strong_resolution(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ParticipantFuture<'_, VerifiedParticipantResolution>;
    fn acquire_lifecycle(
        &self,
        binding: super::lifecycle::LifecycleBinding,
        mode: super::lifecycle::GuardMode,
    ) -> ParticipantFuture<'_, Option<super::lifecycle::VerifiedLifecycleGuard>>;
    fn lifecycle_holder(
        &self,
        table_id: Uuid,
    ) -> ParticipantFuture<'_, Option<super::lifecycle::LifecycleBinding>>;
    fn lifecycle_guard(
        &self,
        binding: super::lifecycle::LifecycleBinding,
    ) -> ParticipantFuture<'_, Option<super::lifecycle::VerifiedLifecycleGuard>>;
    /// The table's exclusive lifecycle guard (T7b). Only the service client
    /// implements it.
    fn lifecycle_exclusive(
        &self,
        _table_id: Uuid,
    ) -> ParticipantFuture<'_, Option<super::lifecycle::LifecycleBinding>> {
        Box::pin(async { Err(invalid()) })
    }
    fn scoped_lifecycle_guard(
        &self,
        _txn: TxnId,
    ) -> ParticipantFuture<'_, Option<super::lifecycle::VerifiedLifecycleGuard>> {
        Box::pin(async { Err(invalid()) })
    }
    fn strong_index_attempt_closed(
        &self,
        owner: nabedra_oltp_types::types::MaintenanceOwnerRef,
    ) -> ParticipantFuture<'_, super::index::VerifiedIndexAttemptClosed>;
    fn fence_current(
        &self,
        transition_id: Uuid,
        expected: Authority,
        next: Authority,
    ) -> ParticipantFuture<'_, nabedra_oltp_types::types::GroupFenceReceipt>;
    fn fence_scoped_queue<'a>(
        &'a self,
        transfer: &'a ScopedQueueTransition,
    ) -> ParticipantFuture<'a, ()>;
}
#[derive(Clone)]
pub struct ParticipantHandle {
    machine: Option<ParticipantMachine>,
    consensus: Option<Arc<dyn Consensus<ParticipantCommand, ParticipantReply>>>,
    identity: GroupIdentity,
    resources: Arc<crate::oltp::pool::WorkerSharedResources>,
    pub(super) remote: Option<Arc<dyn RemoteParticipant>>,
}
#[derive(Clone)]
pub struct VerifiedParticipantSeal {
    evidence: PreparedParticipant,
    _owner: Arc<ScratchLease>,
}
impl std::fmt::Debug for VerifiedParticipantSeal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.evidence.fmt(f)
    }
}
impl std::fmt::Debug for VerifiedParticipantResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.evidence.fmt(f)
    }
}
impl VerifiedParticipantSeal {
    pub fn from_authenticated(
        context: impl ParticipantReplyProof,
        evidence: PreparedParticipant,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<Self, ProtocolError> {
        if context.group() != &evidence.participant.group
            || evidence.txn != txn
            || evidence.manifest != manifest
        {
            return Err(invalid());
        }
        context.verify_payload(
            ParticipantProofKind::Seal,
            &evidence,
        )?;
        Ok(Self {
            evidence,
            _owner: context.owner(),
        })
    }

    pub fn evidence(&self) -> &PreparedParticipant {
        &self.evidence
    }
    pub(crate) fn into_evidence(self) -> PreparedParticipant {
        self.evidence
    }
}
#[derive(Clone)]
pub struct VerifiedParticipantResolution {
    evidence: ResolutionEvidence,
    _owner: Arc<ScratchLease>,
}
impl VerifiedParticipantResolution {
    pub fn from_authenticated(
        context: impl ParticipantReplyProof,
        evidence: ResolutionEvidence,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<Self, ProtocolError> {
        if context.group() != &evidence.group
            || evidence.txn != txn
            || evidence.manifest != manifest
        {
            return Err(invalid());
        }
        context.verify_payload(
            ParticipantProofKind::Resolution,
            &evidence,
        )?;
        Ok(Self {
            evidence,
            _owner: context.owner(),
        })
    }

    pub fn evidence(&self) -> &ResolutionEvidence {
        &self.evidence
    }
    pub fn into_evidence(self) -> ResolutionEvidence {
        self.evidence
    }
}
impl ParticipantHandle {
    pub fn new(
        machine: ParticipantMachine,
        consensus: Arc<dyn Consensus<ParticipantCommand, ParticipantReply>>,
    ) -> Self {
        let identity = machine.store.identity().clone();
        let resources = machine.store.resources().clone();
        Self {
            machine: Some(machine),
            consensus: Some(consensus),
            identity,
            resources,
            remote: None,
        }
    }
    pub fn remote(
        identity: GroupIdentity,
        resources: Arc<crate::oltp::pool::WorkerSharedResources>,
        remote: Arc<dyn RemoteParticipant>,
    ) -> Result<Self, ProtocolError> {
        identity.validate().map_err(|_| invalid())?;
        if !matches!(identity.kind, GroupKind::Data { .. } | GroupKind::Catalog) {
            return Err(invalid());
        }
        Ok(Self {
            machine: None,
            consensus: None,
            identity,
            resources,
            remote: Some(remote),
        })
    }
    pub(super) fn local_machine(&self) -> &ParticipantMachine {
        self.machine.as_ref().expect("local participant dispatch")
    }
    pub(super) fn local_consensus(
        &self,
    ) -> &Arc<dyn Consensus<ParticipantCommand, ParticipantReply>> {
        self.consensus.as_ref().expect("local participant dispatch")
    }
    pub fn identity(&self) -> &GroupIdentity {
        &self.identity
    }
    pub fn resources(&self) -> &Arc<crate::oltp::pool::WorkerSharedResources> {
        &self.resources
    }
    pub async fn propose(
        &self,
        command: OwnedCommand<ParticipantCommand>,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.propose(command).await;
        }

        self.local_consensus().propose(command).await
    }
    pub async fn propose_verified_scoped(
        &self,
        scope: ScopedCatalogRead,
        command: OwnedCommand<ParticipantCommand>,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        if self.remote.is_some()
            || !scoped_queue::bound_to_scope(self.identity(), scope, command.payload()?)
        {
            return Err(invalid());
        }
        let class = if command.payload()?.uses_prepare_lane() {
            crate::oltp::pool::ByteClass::Data
        } else {
            crate::oltp::pool::ByteClass::Progress
        };
        self.propose(OwnedCommand::new_owned(
            ParticipantCommand::Scoped { scope, command },
            self.resources(),
            class,
        )?)
        .await
    }
    pub async fn prepare_page(
        &self,
        command: OwnedCommand<ParticipantCommand>,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        if !matches!(command.payload()?, ParticipantCommand::PreparePage { .. }) {
            return Err(invalid());
        }
        self.propose(command).await
    }
    pub async fn seal(
        &self,
        authority: Authority,
        txn: TxnId,
        manifest: ManifestRef,
        result_digest: Digest,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.propose(OwnedCommand::new_owned(
            ParticipantCommand::Seal {
                authority,
                txn,
                manifest,
                result_digest,
            },
            self.resources(),
            crate::oltp::pool::ByteClass::Progress,
        )?)
        .await
    }
    pub async fn resolve(
        &self,
        authority: Authority,
        decision: impl StatusDecisionProof,
        max_items: u16,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.propose(OwnedCommand::new_owned(
            ParticipantCommand::Resolve {
                authority,
                proof: decision.into_sealed(),
                max_items,
            },
            self.resources(),
            crate::oltp::pool::ByteClass::Progress,
        )?)
        .await
    }
    /// A durable witness can replace the remote Status reads only on the
    /// current participant leader, while that exact decision is still paging.
    pub async fn has_resolution_witness(
        &self,
        authority: Authority,
        proof: &SealedStatusDecision,
    ) -> Result<bool, ProtocolError> {
        let statement = proof.statement().clone();
        proof.validate_for(statement.txn, &statement.manifest, &statement.request)?;
        let digest = proof.digest();
        let group = self.identity().clone();
        self.local_consensus().ensure_linearizable().await?;
        let raw = self
            .local_machine()
            .store
            .read_metadata(META, move |view| {
                let Some(witness) = view.pinned(&txn_key(RESOLUTION_WITNESS, statement.txn))?
                else {
                    return view.encode(&false);
                };
                let config = view.pinned(CONFIG)?.ok_or_else(recovery_unknown)?;
                let state = view
                    .pinned(&txn_key(b't', statement.txn))?
                    .ok_or_else(recovery_unknown)?;
                if config.len() > METADATA_BYTES || state.len() > METADATA_BYTES {
                    return Err(recovery_unknown());
                }
                let config: Config =
                    serde_json::from_slice(&config).map_err(|_| recovery_unknown())?;
                let state: Txn = serde_json::from_slice(&state).map_err(|_| recovery_unknown())?;
                // An older voter can finish a page without removing a key
                // carried over in a newer voter's checkpoint.
                if config.authority != authority || state.resolved == state.writes {
                    return view.encode(&false);
                }
                if decode_resolution_witness(&witness)? != digest {
                    return Err(invalid());
                }
                if state.plan.group != group
                    || state.request != statement.request
                    || state.manifest != statement.manifest
                    || state.decision != Some(statement.decision)
                    || state.resolved == 0
                    || state.resolved > state.writes
                {
                    return Err(recovery_unknown());
                }
                view.encode(&true)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }
    pub async fn read_at(
        &self,
        read_ts: ReadTs,
        key: MvccKey,
    ) -> Result<ChargedBytes, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.read_at(read_ts, key).await;
        }

        self.local_consensus().ensure_linearizable().await?;
        self.local_machine().read_at(read_ts, key).await
    }
    pub fn with_scoped_execution(
        &self,
        scope: ScopedCatalogRead,
    ) -> Result<Self, ProtocolError> {
        let remote = self
            .remote
            .as_ref()
            .ok_or_else(invalid)?
            .with_scoped_execution(scope)?;
        Self::remote(self.identity.clone(), self.resources.clone(), remote)
    }

    pub(crate) async fn read_scoped_catalog(
        &self,
        scope: ScopedCatalogRead,
        key: MvccKey,
    ) -> Result<ChargedBytes, ProtocolError> {
        let remote = self.remote.as_ref().ok_or_else(invalid)?;
        remote.read_scoped_catalog(scope, key).await
    }

    pub async fn read_verified_scoped_catalog(
        &self,
        scope: ScopedCatalogRead,
        key: MvccKey,
    ) -> Result<ChargedBytes, ProtocolError> {
        if self.identity().kind != GroupKind::Catalog
            || !matches!(
                &key,
                MvccKey::CatalogTable { table_id }
                    | MvccKey::CatalogRoutingRoot { table_id }
                    | MvccKey::CatalogRoutingPage { table_id, .. }
                    | MvccKey::CatalogIndex { table_id, .. }
                    | MvccKey::CatalogIndexesRoot { table_id }
                    | MvccKey::CatalogIndexesPage { table_id, .. }
                    if *table_id == scope.table_id
            )
        {
            return Err(invalid());
        }
        self.read_at(scope.read_ts, key).await
    }
    pub async fn read_page(
        &self,
        request: OwnedCommand<super::point::PointReadRequest>,
    ) -> Result<OwnedCommand<super::scan::ScanPage>, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.read_page(request).await;
        }
        self.local_consensus().ensure_linearizable().await?;
        self.local_machine().read_page(request).await
    }
    pub async fn strong_seal(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<VerifiedParticipantSeal, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.strong_seal(txn, manifest).await;
        }

        self.local_consensus().ensure_linearizable().await?;
        let raw = self
            .local_machine()
            .store
            .read_metadata(META, move |view| {
                let state = view.pinned(&txn_key(b't', txn))?.ok_or_else(invalid)?;
                if state.len() > METADATA_BYTES {
                    return Err(recovery_unknown());
                }
                let state: Txn = serde_json::from_slice(&state).map_err(|_| recovery_unknown())?;
                if state.manifest != manifest
                    || state.decision.is_some()
                    || state.point_home.is_some()
                {
                    return Err(invalid());
                }
                let evidence = state.sealed.ok_or_else(invalid)?;
                view.encode(&evidence)
            })
            .await?;
        let owner = Arc::new(
            self.resources()
                .reserve_reply_decode(super::super::json_decode_budget(raw.as_slice())?)
                .await?,
        );
        let resources = self.resources().clone();
        let decode_owner = owner.clone();
        let evidence = resources
            .execute(ExecutionClass::Control, 0, 0, move || {
                let _owner = decode_owner;
                serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
            })
            .await?;
        Ok(VerifiedParticipantSeal {
            evidence,
            _owner: owner,
        })
    }
    pub async fn strong_resolution(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<VerifiedParticipantResolution, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.strong_resolution(txn, manifest).await;
        }

        self.local_consensus().ensure_linearizable().await?;
        let raw = self
            .local_machine()
            .store
            .read_metadata(META, move |view| {
                let state = view.pinned(&txn_key(b'd', txn))?.ok_or_else(invalid)?;
                if state.len() > METADATA_BYTES {
                    return Err(recovery_unknown());
                }
                let evidence: ResolutionEvidence =
                    serde_json::from_slice(&state).map_err(|_| recovery_unknown())?;
                if evidence.txn != txn || evidence.manifest != manifest {
                    return Err(invalid());
                }
                view.encode(&evidence)
            })
            .await?;
        let owner = Arc::new(
            self.resources()
                .reserve_reply_decode(super::super::json_decode_budget(raw.as_slice())?)
                .await?,
        );
        let resources = self.resources().clone();
        let decode_owner = owner.clone();
        let evidence = resources
            .execute(ExecutionClass::Control, 0, 0, move || {
                let _owner = decode_owner;
                serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
            })
            .await?;
        Ok(VerifiedParticipantResolution {
            evidence,
            _owner: owner,
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ParticipantEpochs {
    pub authority: Authority,
    pub catalog_group_id: Uuid,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    /// The group's `data.direct.v1` state (T7), read from the same config.
    /// Absent until activation, so the encoding is unchanged before it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_schema: Option<super::direct::DirectSchema>,
}
impl ParticipantHandle {
    pub async fn activation(&self) -> Result<Option<ParticipantEpochs>, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.activation().await;
        }

        self.local_consensus().ensure_linearizable().await?;
        let raw = self
            .local_machine()
            .store
            .read_metadata(META, |view| {
                let raw = view.pinned(CONFIG)?;
                let epochs = raw
                    .map(|raw| {
                        if raw.len() > METADATA_BYTES {
                            return Err(recovery_unknown());
                        }
                        serde_json::from_slice::<ParticipantEpochs>(&raw)
                            .map_err(|_| recovery_unknown())
                    })
                    .transpose()?;
                view.encode(&epochs)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }
    pub async fn epochs(&self) -> Result<ParticipantEpochs, ProtocolError> {
        self.activation().await?.ok_or_else(invalid)
    }
}

impl ParticipantHandle {
    pub async fn fence_initial(
        &self,
        transition_id: Uuid,
        next: Authority,
        catalog_group_id: Uuid,
        schema_epoch: u64,
        routing_epoch: u64,
        span: KeySpan,
    ) -> Result<nabedra_oltp_types::types::GroupFenceReceipt, ProtocolError> {
        self.fence_command(ParticipantCommand::Fence {
            transition_id,
            expected: None,
            next,
            catalog_group_id,
            schema_epoch,
            routing_epoch,
            span,
        })
        .await
    }
    pub async fn fence_current(
        &self,
        transition_id: Uuid,
        expected: Authority,
        next: Authority,
    ) -> Result<nabedra_oltp_types::types::GroupFenceReceipt, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.fence_current(transition_id, expected, next).await;
        }

        self.local_consensus().ensure_linearizable().await?;
        let raw = self
            .local_machine()
            .store
            .read_metadata(META, |view| {
                view.pinned(CONFIG)?
                    .map(|raw| view.copy_output(&raw))
                    .transpose()?
                    .ok_or_else(invalid)
            })
            .await?;
        let config: Config =
            serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())?;
        // A lost successful response may be retried after the new fence. The
        // transition's durable receipt, not a fresh authority guess, decides it.
        if config.authority != expected && config.authority != next {
            return Err(invalid());
        }
        self.fence_command(ParticipantCommand::Fence {
            transition_id,
            expected: Some(expected),
            next,
            catalog_group_id: config.catalog_group_id,
            schema_epoch: config.schema_epoch,
            routing_epoch: config.routing_epoch,
            span: config.span,
        })
        .await
    }
    /// The group's direct-commit schema state (`data.direct.v1`, plan
    /// section 7): `None` until direct commits were activated on it. Read
    /// behind the linearizable barrier on the local replica. Tests read it;
    /// the executor reads the epochs through `direct_epochs`.
    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn direct_schema(
        &self,
    ) -> Result<Option<super::direct::DirectSchema>, ProtocolError> {
        if self.remote.is_some() {
            return Err(invalid());
        }
        self.local_consensus().ensure_linearizable().await?;
        let raw = self
            .local_machine()
            .store
            .read_metadata(META, |view| {
                let schema = view
                    .pinned(CONFIG)?
                    .map(|raw| {
                        if raw.len() > METADATA_BYTES {
                            return Err(recovery_unknown());
                        }
                        serde_json::from_slice::<Config>(&raw)
                            .map(|config| config.direct_schema)
                            .map_err(|_| recovery_unknown())
                    })
                    .transpose()?
                    .flatten();
                view.encode(&schema)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }
    /// Activates direct commits on this Data group or fences them for a DDL,
    /// under the group's current run authority. Only callers that observed
    /// Control's `data.direct.v1` activation may call it: an older replica
    /// cannot decode the command.
    pub async fn change_direct_schema(
        &self,
        change: super::direct::DirectSchemaChange,
    ) -> Result<super::direct::DirectSchema, ProtocolError> {
        let GroupKind::Data { table_id, .. } = self.identity().kind else {
            return Err(invalid());
        };
        let mut tries = 0;
        loop {
            tries += 1;
            let authority = self.epochs().await?.authority;
            let applied = self
                .propose(OwnedCommand::new_owned(
                    ParticipantCommand::DirectSchema(super::direct::DirectSchemaFence {
                        authority,
                        table_id,
                        change,
                    }),
                    self.resources(),
                    crate::oltp::pool::ByteClass::Progress,
                )?)
                .await?;
            match applied.reply {
                ParticipantReply::DirectSchema(schema) => return Ok(schema),
                // A run fence moved between the epoch read and the apply.
                ParticipantReply::Rejected(Fault::StaleAuthority) if tries < 3 => continue,
                _ => return Err(invalid()),
            }
        }
    }
    async fn fence_command(
        &self,
        command: ParticipantCommand,
    ) -> Result<nabedra_oltp_types::types::GroupFenceReceipt, ProtocolError> {
        let applied = self
            .propose(OwnedCommand::new_owned(
                command,
                self.resources(),
                crate::oltp::pool::ByteClass::Progress,
            )?)
            .await?;
        match applied.reply {
            ParticipantReply::Fenced(receipt) => Ok(receipt),
            _ => Err(invalid()),
        }
    }
}
