//! Strong Control reads share the authenticated worker listener. This endpoint
//! grants no raw Raft proposal or caller-supplied proof capability.
use super::{
    ControlCommand, ControlHandle, ControlReply, DataWorkerGrowthProof, Fault, GroupRecord,
    InventoryPage, SCOPED_QUEUE_VERSION, ScopedQueueCapability, ScopedQueuePlacement,
    ScopedQueueTransferReport, ScopedQueueTransition, ServiceAction, ServiceAuthorization,
    TimestampPointRoute, scoped_queue::ScopedQueueOperation, unsupported_service_route,
};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, ExecutionClass, ScratchLease, WorkerSharedResources},
    oltp_deployment::live::InventoryPin,
    oltp_replication::{
        authority::{
            consensus::Consensus,
            status::{StatusReply, StatusTopology, identity::ScopedStatusEpoch},
            timestamp::{ScopedTimestampEpoch, TimestampReply},
        },
        multiplex::*,
        peer_directory::MAX_VIEW_REVOCATIONS,
        service_rpc::{ParticipantClient, StatusClient, TimestampClient},
        queue_access::AuthorityReader,
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use nabedra_oltp_types::queue::{QueueAuthority, QueueScope};
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use std::{
    mem::size_of,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use nabedra_rt::time;
use uuid::Uuid;

const HEADER_LIMIT: usize = 4096;
const READ_FAILOVER_TIMEOUT: Duration = Duration::from_secs(5);
const READ_PEER_TIMEOUT: Duration = Duration::from_millis(750);
const READ_RETRY_DELAY: Duration = Duration::from_millis(50);
/// A revocation page holds at most 128 identities of about 200 JSON bytes.
const REVOCATION_PAGE_BYTES: usize = 64 << 10;

#[cfg(test)]
mod route_tests;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum ReadRequest {
    Current,
    ColdPin {
        authority: Authority,
        prior_run: Uuid,
    },
    QueueRemoval {
        owner: WorkerIdentity,
        ticket: JoinTicket,
    },
    EnrollmentRevision,
    GroupInventoryRevision,
    RestartPreparation {
        revision: u64,
    },
    PreparedDataWorker {
        revision: u64,
    },
    WorkerExcluded {
        worker: WorkerIdentity,
    },
    /// REP6: one page of committed system revocations, for Data-only
    /// workers' revocation views.
    Revocations {
        after: Option<Uuid>,
    },
    OwnWorkerDrain,
    OwnGrowthSample {
        pin: InventoryPin,
        claim_id: Uuid,
    },
    PoolReplicaRetired {
        stamp: Box<super::ReplicaRetiredStamp>,
    },
    RemoteReplicaMove {
        id: Uuid,
    },
    RemoteReplicaVoter {
        id: Uuid,
        node: u64,
    },
    Group {
        group_id: Uuid,
    },
    ScopedQueue {
        group_id: Uuid,
    },
    ScopedQueueCapability,
    /// `data.direct.v1`: activation and the caller's advert. An older
    /// Control leader rejects the unknown request; callers treat that as
    /// "not active".
    DirectCapability,
    ChangeDirectCapability {
        operation_id: Uuid,
        action: super::DirectCapabilityOperation,
    },
    /// GC2: the direct family's cluster features (`DirectFeatures`). An
    /// older Control leader rejects the unknown request; callers treat that
    /// as "not enabled".
    DirectFeatures,
    QueuedSnapshotRetryEnabled,
    /// CAP1: one registered cluster feature (`ClusterFeatureState`). An
    /// older Control leader rejects the unknown request, and one that does
    /// not register the name answers an error; callers treat both as "not
    /// enabled".
    ClusterFeature {
        name: String,
    },
    ScopedQueueTransition {
        group_id: Uuid,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_group: Option<Uuid>,
    },
    ScopedQueuePredecessor {
        group_id: Uuid,
    },
    ScopedQueueEpochRetired {
        group_id: Uuid,
        queue_generation: Uuid,
        view: AuthorityView,
    },
    ChangeScopedQueue {
        operation_id: Uuid,
        action: ScopedQueueOperation,
    },
    SplitSource {
        expected: Box<GroupRecord>,
        authority: Authority,
    },
    ServiceRole {
        remote: WorkerIdentity,
        action: ServiceAction,
        target: Box<GroupIdentity>,
    },
    ServiceRoleAtReplica {
        remote: WorkerIdentity,
        target: Box<GroupIdentity>,
        node_id: u64,
        generation: Uuid,
    },
    TimestampPointProof {
        remote: WorkerIdentity,
        target: Box<GroupIdentity>,
        node_id: u64,
        route: TimestampPointRoute,
        decision_source: Option<(ServiceAction, Digest)>,
    },
    TerminalReader {
        remote: WorkerIdentity,
        timestamps_group_id: Uuid,
    },
    Apis {
        revision: u64,
        after: Option<Uuid>,
    },
    Workers {
        revision: u64,
        after: Option<Uuid>,
    },
    Ready {
        expected: ReadyMembership,
        after: Option<Uuid>,
    },
    Groups {
        revision: u64,
        after: Option<Uuid>,
    },
    GroupMembers {
        expected: Box<GroupRecord>,
        after: Option<u64>,
    },
    /// NS14 rule 2: the caller cannot serve this replica. An older Control
    /// rejects the variant; the worker keeps retrying.
    GroupDamaged {
        report: Box<super::GroupDamageReport>,
    },
    GroupDamage {
        group_id: Uuid,
        node_id: u64,
    },
    /// Search reads, commands and ready reports (contract-w2 2.6).
    Search(super::search::SearchRequest),
}

impl ReadRequest {
    fn supports_bounded_failover(&self) -> bool {
        matches!(
            self,
            Self::Current
                | Self::EnrollmentRevision
                | Self::Workers { .. }
                | Self::Apis { .. }
                | Self::RestartPreparation { .. }
                | Self::PreparedDataWorker { .. }
                | Self::ColdPin { .. }
                | Self::GroupInventoryRevision
                | Self::WorkerExcluded { .. }
                | Self::Groups { .. }
                | Self::Group { .. }
                | Self::GroupMembers { .. }
                | Self::ScopedQueue { .. }
                | Self::ScopedQueueCapability
                | Self::DirectCapability
                | Self::DirectFeatures
                | Self::QueuedSnapshotRetryEnabled
                | Self::ClusterFeature { .. }
                | Self::ScopedQueueTransition { .. }
                | Self::ScopedQueuePredecessor { .. }
                | Self::ScopedQueueEpochRetired { .. }
                | Self::ServiceRole { .. }
                | Self::ServiceRoleAtReplica { .. }
                | Self::TimestampPointProof { .. }
        )
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadReply {
    nonce: Uuid,
    bytes: u64,
}

#[doc(hidden)]
pub struct ControlEndpoint {
    handle: Arc<ControlHandle>,
    shared: Arc<WorkerSharedResources>,
    scoped_queue_fences: OnceLock<ScopedQueueFenceReaders>,
}

struct ScopedQueueFenceReaders {
    status: Vec<Arc<StatusClient>>,
    timestamps: Arc<TimestampClient>,
    catalog: Arc<ParticipantClient>,
    transport: WorkerClient,
    control: Arc<ControlClient>,
}
impl ControlEndpoint {
    pub(super) fn control(&self) -> &ControlHandle {
        &self.handle
    }

    pub(super) fn is_operator(&self, worker: WorkerIdentity) -> bool {
        self.handle.is_operator(&worker)
    }

    pub(super) async fn read_eks_replacement(
        &self,
        worker: WorkerIdentity,
        token_digest: Digest,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.handle.own_eks_replacement(worker, token_digest).await
    }
    pub(super) async fn authorize_data_worker_join(
        &self,
        worker: WorkerIdentity,
        token: Option<Digest>,
    ) -> Result<(), ProtocolError> {
        self.handle.authorize_data_worker_join(worker, token).await
    }
    #[doc(hidden)]
    pub fn new(
        handle: Arc<ControlHandle>,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Arc<Self>, ProtocolError> {
        if !Arc::ptr_eq(handle.resources(), &shared) {
            return Err(invalid("Control endpoint has a foreign worker budget"));
        }
        Ok(Arc::new(Self {
            handle,
            shared,
            scoped_queue_fences: OnceLock::new(),
        }))
    }

    #[doc(hidden)]
    pub fn install_scoped_queue_fence_readers(
        &self,
        topology: StatusTopology,
        status: Vec<Arc<StatusClient>>,
        timestamps: Arc<TimestampClient>,
        catalog: GroupIdentity,
        transport: WorkerClient,
        control: Arc<ControlClient>,
    ) -> Result<(), ProtocolError> {
        let scope = self.handle.identity();
        let same_scope = |group: &GroupIdentity| {
            group.cluster_id == scope.cluster_id && group.database == scope.database
        };
        if !topology
            .groups()
            .iter()
            .eq(status.iter().map(|client| client.identity()))
            || status.iter().any(|client| {
                !same_scope(client.identity()) || !Arc::ptr_eq(client.resources(), &self.shared)
            })
            || !same_scope(timestamps.identity())
            || timestamps.identity().kind != GroupKind::Timestamps
            || !Arc::ptr_eq(timestamps.resources(), &self.shared)
            || !same_scope(&catalog)
            || catalog.kind != GroupKind::Catalog
            || control.group != *scope
            || !Arc::ptr_eq(&control.shared, &self.shared)
            || transport.local_identity() != control.client.local_identity()
        {
            return Err(invalid("Control endpoint has foreign scoped fence readers"));
        }
        let catalog = ParticipantClient::new(
            catalog,
            self.shared.clone(),
            transport.clone(),
            control.clone(),
            ServiceAction::Owner,
        )?;
        self.scoped_queue_fences
            .set(ScopedQueueFenceReaders {
                status,
                timestamps,
                catalog,
                transport,
                control,
            })
            .map_err(|_| invalid("Control endpoint already has scoped fence readers"))
    }

    async fn verify_scoped_queue_publication(
        &self,
        owner: WorkerIdentity,
        group_id: Uuid,
        transfer_id: Uuid,
    ) -> Result<(), ProtocolError> {
        use super::scoped_queue::stale;

        let raw = self
            .handle
            .read_scoped_queue_transition(owner, group_id, None)
            .await?;
        let transfer = super::machine::decode::<Option<ScopedQueueTransition>>(raw.as_slice())?;
        let Some(transfer) = transfer.filter(|transfer| transfer.id == transfer_id) else {
            // An exact retry of a committed operation can replay its Control
            // receipt without re-reading the Status fences.
            return if self
                .handle
                .scoped_queue_publication_replayed(owner, group_id, transfer_id)
                .await?
            {
                Ok(())
            } else {
                Err(stale())
            };
        };
        if transfer.next.owner != owner
            || transfer.basis()?.is_none()
            || !transfer.has_installed_quorum()
        {
            return Err(stale());
        }
        let readers = self.scoped_queue_fences.get().ok_or_else(stale)?;
        let epoch = ScopedStatusEpoch::from_placement(&transfer.next);
        for reader in &readers.status {
            if !matches!(
                reader.scoped_queue_fence_receipt(&transfer).await?,
                Some(StatusReply::ScopedQueueFenced {
                    transition_id, epoch: applied, applied_at
                }) if transition_id == transfer.id && applied == epoch
                    && applied_at.term != 0 && applied_at.node_id != 0 && applied_at.index != 0
            ) {
                return Err(stale());
            }
        }
        let epoch = ScopedTimestampEpoch::from_placement(&transfer.next)?;
        if !matches!(
            readers.timestamps.scoped_queue_fence_receipt(&transfer).await?,
            Some(TimestampReply::ScopedQueueFenced { transition_id, epoch: applied, applied_at })
                if transition_id == transfer.id && applied == epoch
                    && applied_at.term != 0 && applied_at.node_id != 0 && applied_at.index != 0
        ) {
            return Err(stale());
        }
        readers.catalog.verify_scoped_queue_fence(&transfer).await?;
        let data = ParticipantClient::new(
            transfer.next.group.clone(),
            self.shared.clone(),
            readers.transport.clone(),
            readers.control.clone(),
            ServiceAction::Owner,
        )?;
        data.verify_scoped_queue_fence(&transfer).await?;
        Ok(())
    }
    fn parse(
        &self,
        session: &AuthenticatedSession,
        header: &[u8],
    ) -> Result<ReadRequest, ProtocolError> {
        if session.recovery_only()
            || session.purpose() != Purpose::Authority
            || session.group() != self.handle.identity()
            || header.len() > HEADER_LIMIT
        {
            return Err(invalid("invalid Control authority session"));
        }
        crate::protocol::json_input(header, HEADER_LIMIT)
            .map_err(|_| invalid("invalid Control authority header"))?;
        serde_json::from_slice(header).map_err(|_| invalid("invalid Control authority request"))
    }
}
impl Endpoint for ControlEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            self.parse(session, header)?;
            // No payload or fresh recursive Control lookup is needed to read
            // Control itself. TLS authenticates locally applied enrollment.
            Ok(ReceivePlan {
                class: TrafficClass::Authority,
                section_lengths: [0; 3],
                decode_budget_bytes: 0,
            })
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        self.handle_with_leader(session, header, sections, None)
    }
}
impl ControlEndpoint {
    pub(super) fn handle_with_leader<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: ChargedBytes,
        sections: ChargedSections,
        leader: Option<&'a dyn Consensus<ControlCommand, ControlReply>>,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let request = self.parse(&session, header.as_ref())?;
            if (0..3).any(|i| sections.section(i).is_some()) {
                return Err(invalid("Control read has unexpected payload"));
            }
            let scope = QueueScope {
                cluster_id: session.group().cluster_id,
                database: session.group().database.clone(),
            };
            let bytes = match request {
                ReadRequest::Search(request) => {
                    self.handle.search_rpc(&session, request, leader).await?
                }
                ReadRequest::Group { group_id } => {
                    let group = self.handle.group(group_id).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &group, HEADER_LIMIT)?
                }
                ReadRequest::ScopedQueue { group_id } => {
                    self.handle.read_scoped_queue(group_id).await?
                }
                ReadRequest::ScopedQueueCapability => {
                    let state = self.handle.scoped_queue_capability().await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &state, HEADER_LIMIT)?
                }
                ReadRequest::DirectCapability => {
                    let state = self.handle.direct_capability(session.remote()).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &state, HEADER_LIMIT)?
                }
                ReadRequest::QueuedSnapshotRetryEnabled => {
                    let enabled = self.handle.queued_snapshot_retry_enabled().await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &enabled, HEADER_LIMIT)?
                }
                ReadRequest::DirectFeatures => {
                    let state = self.handle.direct_features(session.remote()).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &state, HEADER_LIMIT)?
                }
                ReadRequest::ClusterFeature { name } => {
                    let state = self.handle.cluster_feature(session.remote(), &name).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &state, HEADER_LIMIT)?
                }
                ReadRequest::ChangeDirectCapability {
                    operation_id,
                    action,
                } => {
                    let reply = self
                        .handle
                        .apply_direct_capability_rpc(&session, operation_id, action, leader)
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &reply, HEADER_LIMIT)?
                }
                ReadRequest::ScopedQueueTransition {
                    group_id,
                    source_group,
                } => {
                    self.handle
                        .read_scoped_queue_transition(session.remote(), group_id, source_group)
                        .await?
                }
                ReadRequest::ScopedQueuePredecessor { group_id } => {
                    self.handle
                        .read_scoped_queue_predecessor(session.remote(), group_id)
                        .await?
                }
                ReadRequest::ScopedQueueEpochRetired {
                    group_id,
                    queue_generation,
                    view,
                } => {
                    self.handle
                        .read_scoped_queue_epoch_retired(
                            session.remote(),
                            group_id,
                            queue_generation,
                            view,
                        )
                        .await?
                }
                ReadRequest::ChangeScopedQueue {
                    operation_id,
                    action,
                } => {
                    if operation_id.is_nil() {
                        return Err(invalid("missing scoped queue operation ID"));
                    }
                    if let ScopedQueueOperation::PublishTransfer {
                        group_id,
                        transfer_id,
                    } = &action
                    {
                        self.verify_scoped_queue_publication(
                            session.remote(),
                            *group_id,
                            *transfer_id,
                        )
                        .await?;
                    }
                    let reply = self
                        .handle
                        .apply_scoped_queue_rpc(&session, operation_id, action, leader)
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &reply, HEADER_LIMIT)?
                }
                ReadRequest::SplitSource {
                    expected,
                    authority,
                } => self.handle.split_source(*expected, authority).await?,
                ReadRequest::GroupDamaged { report } => {
                    // The transport identity is the reporter (rule 2).
                    let record = self
                        .handle
                        .report_group_damage(session.remote(), *report)
                        .await?;
                    self.shared.encode_in(
                        ByteClass::Progress,
                        &ControlReply::GroupDamage(Box::new(record)),
                        HEADER_LIMIT,
                    )?
                }
                ReadRequest::GroupDamage { group_id, node_id } => {
                    let record = self.handle.group_damage(group_id, node_id).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &record, HEADER_LIMIT)?
                }
                ReadRequest::ServiceRole {
                    remote,
                    action,
                    target,
                } => {
                    let grant = self
                        .handle
                        .service_role(&scope, remote, action, *target)
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &grant, HEADER_LIMIT)?
                }
                ReadRequest::ServiceRoleAtReplica {
                    remote,
                    target,
                    node_id,
                    generation,
                } => {
                    let grant = self
                        .handle
                        .service_role_at_replica(
                            &scope,
                            remote,
                            *target,
                            session.remote(),
                            node_id,
                            generation,
                        )
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &grant, HEADER_LIMIT)?
                }
                ReadRequest::TimestampPointProof {
                    remote,
                    target,
                    node_id,
                    route,
                    decision_source,
                } => {
                    self.handle
                        .timestamp_point_route(
                            remote,
                            session.remote(),
                            *target,
                            node_id,
                            route,
                            decision_source,
                        )
                        .await?
                }
                ReadRequest::EnrollmentRevision => {
                    let revision = self
                        .handle
                        .inspect()
                        .await?
                        .ok_or_else(unavailable)?
                        .enrollment_revision;
                    self.shared
                        .encode_in(ByteClass::Progress, &revision, HEADER_LIMIT)?
                }
                ReadRequest::GroupInventoryRevision => {
                    let revision = self
                        .handle
                        .inspect()
                        .await?
                        .ok_or_else(unavailable)?
                        .group_inventory_revision;
                    self.shared
                        .encode_in(ByteClass::Progress, &revision, HEADER_LIMIT)?
                }
                ReadRequest::RestartPreparation { revision } => {
                    let preparation = self.handle.restart_preparation(revision).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &preparation, HEADER_LIMIT)?
                }
                ReadRequest::PreparedDataWorker { revision } => {
                    let enrollment = self.handle.prepared_data_worker(revision).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &enrollment, HEADER_LIMIT)?
                }
                ReadRequest::WorkerExcluded { worker } => {
                    let excluded = self.handle.worker_replaced(worker).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &excluded, HEADER_LIMIT)?
                }
                ReadRequest::Revocations { after } => self.handle.revocations(after).await?,
                ReadRequest::OwnWorkerDrain => {
                    self.handle.own_worker_drain(session.remote()).await?
                }
                ReadRequest::OwnGrowthSample { pin, claim_id } => {
                    self.handle
                        .data_worker_growth_proof(session.remote(), claim_id, pin)
                        .await?
                }
                ReadRequest::PoolReplicaRetired { stamp } => {
                    if stamp.worker != session.remote() {
                        return Err(invalid("replica retirement is for a different worker"));
                    }
                    let retired = self.handle.verify_pool_replica_retired(*stamp).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &retired, HEADER_LIMIT)?
                }
                ReadRequest::RemoteReplicaMove { id } => {
                    self.handle.remote_move_view(id, session.remote()).await?
                }
                ReadRequest::RemoteReplicaVoter { id, node } => {
                    self.handle
                        .replica_move_voter_read(id, node, session.remote())
                        .await?
                }
                ReadRequest::Current => {
                    let current = self.handle.current(&scope).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &current, HEADER_LIMIT)?
                }
                ReadRequest::ColdPin {
                    authority,
                    prior_run,
                } => {
                    let published = self
                        .handle
                        .published_cold_fence(authority, Some(prior_run))
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &published, HEADER_LIMIT)?
                }
                ReadRequest::QueueRemoval { owner, ticket } => {
                    let current = self.handle.removal_view(&scope, owner, ticket).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &current, HEADER_LIMIT)?
                }
                ReadRequest::TerminalReader {
                    remote,
                    timestamps_group_id,
                } => {
                    let current = self
                        .handle
                        .terminal_reader_view(&scope, remote, timestamps_group_id)
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &current, HEADER_LIMIT)?
                }
                ReadRequest::Apis { revision, after } => self.handle.apis(revision, after).await?,
                ReadRequest::Workers { revision, after } => {
                    self.handle.workers(revision, after).await?
                }
                ReadRequest::Ready { expected, after } => {
                    self.handle.ready_page_read(expected, after).await?
                }
                ReadRequest::Groups { revision, after } => {
                    self.handle.groups(revision, after).await?
                }
                ReadRequest::GroupMembers { expected, after } => {
                    self.handle.group_members(*expected, after).await?
                }
            };
            let reply = self.shared.encode_in(
                ByteClass::Progress,
                &ReadReply {
                    nonce: session.nonce(),
                    bytes: bytes.len() as u64,
                },
                HEADER_LIMIT,
            )?;
            Ok(ChargedReply {
                header: reply,
                sections: [Some(bytes), None, None],
            })
        })
    }
}

/// The manager supplies its current locally applied permanent Control voters.
/// Candidates are paged/cursor based; no total-cluster-size cap is introduced.
#[doc(hidden)]
pub trait ControlRoutes: Send + Sync + 'static {
    fn preferred(&self) -> Option<Uuid> {
        None
    }
    fn next(&self, after: Option<Uuid>) -> Result<Option<Uuid>, ProtocolError>;

    fn quorum_voters(&self) -> Result<Vec<Uuid>, ProtocolError> {
        let mut voters = Vec::new();
        while let Some(worker) = self.next(voters.last().copied())? {
            if worker.is_nil()
                || voters.last().is_some_and(|after| worker <= *after)
                || voters.len() == 5
            {
                return Err(invalid("invalid Control voters"));
            }
            voters.push(worker);
        }
        Ok(voters)
    }
}
/// Bootstrap pins are the initial trust set. A quorum of that set can
/// confirm the next published Control placement.
#[doc(hidden)]
pub struct PublishedControlRoutes {
    current: Mutex<(Vec<Uuid>, Option<GroupRecord>)>,
}

impl PublishedControlRoutes {
    #[doc(hidden)]
    pub fn new(mut voters: Vec<Uuid>) -> Result<Arc<Self>, ProtocolError> {
        voters.sort_unstable();
        if !matches!(voters.len(), 1 | 3 | 5)
            || voters[0].is_nil()
            || voters.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(invalid("invalid Control bootstrap voters"));
        }
        Ok(Arc::new(Self {
            current: Mutex::new((voters, None)),
        }))
    }

    async fn refresh(&self, control: &ControlClient) -> Result<(), ProtocolError> {
        let (voters, published) = self.current.lock().map_err(|_| unavailable())?.clone();
        let record =
            control
                .quorum_read(
                    &ReadRequest::Group {
                        group_id: control.group.group_id,
                    },
                    64 << 10,
                    |reply| async move {
                        Ok((*reply.decode_section::<Option<GroupRecord>>(0)?).clone())
                    },
                )
                .await?
                .filter(|record| record.identity == control.group)
                .ok_or_else(unavailable)?;
        if published.as_ref() == Some(&record) {
            return Ok(());
        }
        if published.as_ref().is_some_and(|previous| {
            record.membership_position.index <= previous.membership_position.index
        }) || record.replica_count != voters.len() as u64
        {
            return Err(invalid("unexpected published Control placement"));
        }

        // The quorum agreed to the member root. Fetch and verify the member
        // page against that exact publication.
        let (members, _) = control.load_replica_members(&record, voters.len()).await?;
        validate_voters(&members).map_err(|_| invalid("invalid Control voters"))?;
        let mut next: Vec<_> = members.iter().map(|member| member.worker_id).collect();
        next.sort_unstable();
        if next
            .iter()
            .filter(|id| voters.binary_search(id).is_ok())
            .count()
            <= voters.len() / 2
        {
            return Err(invalid(
                "published Control voters do not retain a previous majority",
            ));
        }
        let mut current = self.current.lock().map_err(|_| unavailable())?;
        if current.1 == published {
            *current = (next, Some(record));
        }
        Ok(())
    }

    #[doc(hidden)]
    pub async fn run_refresh(
        self: Arc<Self>,
        control: Arc<ControlClient>,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut tick = time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        while !*stop.borrow() {
            tokio::select! {
                _ = stop.changed() => return,
                _ = tick.tick() => {}
            }
            tokio::select! {
                _ = stop.changed() => return,
                result = self.refresh(&control) => {
                    if let Err(error) = result {
                        tracing::debug!(target: "nabedra_database_server::oltp_replication::authority::control::endpoint", code = ?error.code, "published Control voter refresh incomplete");
                    }
                }
            }
        }
    }
}

impl ControlRoutes for PublishedControlRoutes {
    fn next(&self, after: Option<Uuid>) -> Result<Option<Uuid>, ProtocolError> {
        let current = self.current.lock().map_err(|_| unavailable())?;
        let index = after.map_or(0, |old| current.0.partition_point(|worker| *worker <= old));
        Ok(current.0.get(index).copied())
    }

    fn quorum_voters(&self) -> Result<Vec<Uuid>, ProtocolError> {
        Ok(self.current.lock().map_err(|_| unavailable())?.0.clone())
    }
}

#[doc(hidden)]
pub struct ControlClient {
    client: WorkerClient,
    shared: Arc<WorkerSharedResources>,
    group: GroupIdentity,
    routes: Arc<dyn ControlRoutes>,
    last_read_route: Mutex<Option<(Option<Uuid>, Uuid)>>,
    replica_routes: Option<Box<Mutex<ReplicaRouteCache>>>,
    service_grants: crate::oltp_replication::service_rpc::GrantCache,
}
impl ControlClient {
    /// The Control group this client reads (API ingress enrollments live
    /// there; `Method::Direct` authorizes them against it).
    pub(crate) fn control_group(&self) -> &GroupIdentity {
        &self.group
    }
    /// Receiver-side service-role grants for callers of this worker.
    pub(crate) fn service_grants(&self) -> &crate::oltp_replication::service_rpc::GrantCache {
        &self.service_grants
    }
    /// The locally applied Control enrollment revision, when known.
    pub(crate) fn applied_revision(&self) -> Option<u64> {
        self.client.applied_revision()
    }
    #[doc(hidden)]
    pub async fn scoped_queue_capability(
        &self,
    ) -> Result<ScopedQueueCapability, ProtocolError> {
        let reply = self.read(&ReadRequest::ScopedQueueCapability).await?;
        let capability = *reply.decode_section::<ScopedQueueCapability>(0)?;
        if capability.version != SCOPED_QUEUE_VERSION
            || capability.run_id.is_nil()
            || capability.activation.is_some()
                && capability.activation != capability.configuration_id
        {
            return Err(invalid("invalid Control scoped queue capability"));
        }
        Ok(capability)
    }

    /// The strong Control view of `data.direct.v1` for this caller.
    #[doc(hidden)]
    pub async fn direct_capability(&self) -> Result<super::DirectCapability, ProtocolError> {
        let reply = self.read(&ReadRequest::DirectCapability).await?;
        let capability = *reply.decode_section::<super::DirectCapability>(0)?;
        if capability.version != super::DIRECT_VERSION {
            return Err(invalid("invalid Control direct capability"));
        }
        Ok(capability)
    }

    /// GC2: the strong Control view of the direct cluster features for this
    /// caller. An older Control answers an error: not enabled.
    #[doc(hidden)]
    pub async fn direct_features(&self) -> Result<super::DirectFeatures, ProtocolError> {
        let reply = self.read(&ReadRequest::DirectFeatures).await?;
        Ok(*reply.decode_section::<super::DirectFeatures>(0)?)
    }

    /// CAP1: the strong Control view of one registered cluster feature for
    /// this caller. An older Control answers an error: not enabled.
    pub(crate) async fn cluster_feature(
        &self,
        name: &str,
    ) -> Result<super::ClusterFeatureState, ProtocolError> {
        let reply = self
            .read(&ReadRequest::ClusterFeature {
                name: name.to_owned(),
            })
            .await?;
        Ok(*reply.decode_section::<super::ClusterFeatureState>(0)?)
    }

    #[doc(hidden)]
    pub async fn queued_snapshot_retry_enabled(&self) -> Result<bool, ProtocolError> {
        let reply = self.read(&ReadRequest::QueuedSnapshotRetryEnabled).await?;
        Ok(*reply.decode_section::<bool>(0)?)
    }

    /// GC2: advertises `feature` for `local`, this caller's enrolled
    /// identity, as the `Advertise { version: 1 }` every Control build
    /// applies alike (see `control::direct`). A repeated advert answers a
    /// conflict (its receipt exists), which is ignored: read
    /// `direct_features` for the outcome.
    #[doc(hidden)]
    pub async fn advertise_direct_feature(
        &self,
        feature: &str,
        local: crate::oltp_replication::types::WorkerIdentity,
    ) {
        let _ = self
            .change_direct_capability(
                super::feature_operation_id(feature, local),
                super::DirectCapabilityOperation::Advertise {
                    version: super::DIRECT_VERSION,
                },
            )
            .await;
    }

    /// Advertises `data.direct.v1` for this caller's enrolled identity, or
    /// (`Activate`) asks Control to re-evaluate activation.
    #[doc(hidden)]
    pub async fn change_direct_capability(
        &self,
        operation_id: Uuid,
        action: super::DirectCapabilityOperation,
    ) -> Result<super::DirectCapability, ProtocolError> {
        let reply = self
            .read(&ReadRequest::ChangeDirectCapability {
                operation_id,
                action,
            })
            .await?;
        match &*reply.decode_section::<ControlReply>(0)? {
            ControlReply::DirectCapability(capability)
                if capability.version == super::DIRECT_VERSION =>
            {
                Ok(*capability)
            }
            ControlReply::Rejected(Fault::Unauthorized) => Err(ProtocolError::new(
                ErrorCode::Unauthorized,
                "Control rejected the direct capability caller",
                Outcome::NotApplied,
            )),
            _ => Err(invalid("unexpected Control direct capability reply")),
        }
    }

    #[doc(hidden)]
    pub async fn advertise_scoped_queue(
        &self,
        operation_id: Uuid,
    ) -> Result<(), ProtocolError> {
        self.update_scoped_queue(
            operation_id,
            ScopedQueueOperation::Advertise {
                version: SCOPED_QUEUE_VERSION,
            },
        )
        .await
    }

    #[doc(hidden)]
    pub async fn set_scoped_queue_admission(
        &self,
        operation_id: Uuid,
        expected: ScopedQueueCapability,
        enabled: bool,
    ) -> Result<(), ProtocolError> {
        self.update_scoped_queue(
            operation_id,
            ScopedQueueOperation::SetAdmission { expected, enabled },
        )
        .await
    }

    #[doc(hidden)]
    pub async fn ensure_scoped_queue(
        &self,
        operation_id: Uuid,
        group: &GroupIdentity,
    ) -> Result<ScopedQueuePlacement, ProtocolError> {
        let reply = self
            .change_scoped_queue(
                operation_id,
                ScopedQueueOperation::Ensure {
                    group: group.clone(),
                },
            )
            .await?;
        let ControlReply::ScopedQueue(placement) = reply else {
            return Err(invalid("unexpected Control scoped queue reply"));
        };
        placement.validate(group)?;
        Ok(*placement)
    }

    #[doc(hidden)]
    pub async fn scoped_queue_transition(
        &self,
        group_id: Uuid,
    ) -> Result<Option<ScopedQueueTransition>, ProtocolError> {
        self.scoped_queue_transition_read(group_id, None).await
    }

    pub(crate) async fn scoped_queue_transition_for_replica(
        &self,
        group_id: Uuid,
        source_group: Uuid,
    ) -> Result<Option<ScopedQueueTransition>, ProtocolError> {
        self.scoped_queue_transition_read(group_id, Some(source_group))
            .await
    }

    async fn scoped_queue_transition_read(
        &self,
        group_id: Uuid,
        source_group: Option<Uuid>,
    ) -> Result<Option<ScopedQueueTransition>, ProtocolError> {
        let reply = self
            .read(&ReadRequest::ScopedQueueTransition {
                group_id,
                source_group,
            })
            .await?;
        let transfer = reply.decode_section::<Option<ScopedQueueTransition>>(0)?;
        if let Some(transfer) = transfer.as_ref() {
            let group = &transfer.previous.group;
            if group.group_id != group_id
                || group.cluster_id != self.group.cluster_id
                || group.database != self.group.database
            {
                return Err(invalid("foreign Control scoped queue transfer"));
            }
            transfer.previous.validate(group)?;
            transfer.next.validate(group)?;
        }
        Ok((*transfer).clone())
    }

    /// Operator recovery input only. A fenced predecessor confers no authority.
    #[doc(hidden)]
    pub async fn scoped_queue_predecessor(
        &self,
        group_id: Uuid,
    ) -> Result<Option<ScopedQueuePlacement>, ProtocolError> {
        self.scoped_placement(ReadRequest::ScopedQueuePredecessor { group_id }, group_id)
            .await
    }

    /// Checks the committed epoch even when a newer transfer fences routing.
    #[doc(hidden)]
    pub async fn scoped_queue_epoch_retired(
        &self,
        group_id: Uuid,
        queue_generation: Uuid,
        view: AuthorityView,
    ) -> Result<bool, ProtocolError> {
        let reply = self
            .read(&ReadRequest::ScopedQueueEpochRetired {
                group_id,
                queue_generation,
                view,
            })
            .await?;
        Ok(*reply.decode_section::<bool>(0)?)
    }

    #[doc(hidden)]
    pub async fn begin_scoped_queue_transfer(
        &self,
        operation_id: Uuid,
        expected: ScopedQueuePlacement,
        next_owner: WorkerIdentity,
        replaces: Option<Uuid>,
    ) -> Result<ScopedQueueTransition, ProtocolError> {
        let group_id = expected.group.group_id;
        let reply = self
            .change_scoped_queue(
                operation_id,
                ScopedQueueOperation::BeginTransfer {
                    expected: Box::new(expected),
                    next_owner,
                    replaces,
                },
            )
            .await?;
        if !matches!(reply, ControlReply::ScopedQueueTransfer(_)) {
            return Err(invalid("unexpected Control scoped queue transfer reply"));
        }
        self.scoped_queue_transition(group_id)
            .await?
            .filter(|current| current.id == operation_id)
            .ok_or_else(super::scoped_queue::stale)
    }

    #[doc(hidden)]
    pub async fn report_scoped_queue_transfer(
        &self,
        operation_id: Uuid,
        group_id: Uuid,
        transfer_id: Uuid,
        report: ScopedQueueTransferReport,
    ) -> Result<(), ProtocolError> {
        self.update_scoped_queue(
            operation_id,
            ScopedQueueOperation::ReportTransfer {
                group_id,
                transfer_id,
                report,
            },
        )
        .await
    }

    #[doc(hidden)]
    pub async fn rebase_scoped_queue_transfer(
        &self,
        operation_id: Uuid,
        group_id: Uuid,
        transfer_id: Uuid,
        removed: WorkerIdentity,
        next_owner: WorkerIdentity,
    ) -> Result<(), ProtocolError> {
        let reply = self
            .change_scoped_queue(
                operation_id,
                ScopedQueueOperation::RebaseTransfer {
                    group_id,
                    transfer_id,
                    removed,
                    next_owner,
                },
            )
            .await?;
        if !matches!(reply, ControlReply::ScopedQueueTransfer(transfer)
            if transfer.id == operation_id && transfer.previous.group.group_id == group_id
                && transfer.next.owner == next_owner && transfer.next.unavailable_holder == Some(removed))
        {
            return Err(invalid("unexpected Control scoped queue rebase reply"));
        }
        Ok(())
    }

    #[doc(hidden)]
    pub async fn publish_scoped_queue_transfer(
        &self,
        operation_id: Uuid,
        group_id: Uuid,
        transfer_id: Uuid,
    ) -> Result<ScopedQueuePlacement, ProtocolError> {
        let reply = self
            .change_scoped_queue(
                operation_id,
                ScopedQueueOperation::PublishTransfer {
                    group_id,
                    transfer_id,
                },
            )
            .await?;
        let ControlReply::ScopedQueue(placement) = reply else {
            return Err(invalid("unexpected Control scoped queue publication reply"));
        };
        if placement.group.group_id != group_id
            || placement.group.cluster_id != self.group.cluster_id
            || placement.group.database != self.group.database
        {
            return Err(invalid("foreign Control scoped queue publication"));
        }
        placement.validate(&placement.group)?;
        Ok(*placement)
    }

    async fn update_scoped_queue(
        &self,
        operation_id: Uuid,
        action: ScopedQueueOperation,
    ) -> Result<(), ProtocolError> {
        match self.change_scoped_queue(operation_id, action).await? {
            ControlReply::ScopedQueueUpdated => Ok(()),
            _ => Err(invalid("unexpected Control scoped queue update reply")),
        }
    }

    /// NS14 rule 2 from a worker that does not host Control. Idempotent.
    pub(crate) async fn report_group_damage(
        &self,
        report: super::GroupDamageReport,
    ) -> Result<super::GroupDamageRecord, ProtocolError> {
        let reply = self
            .read(&ReadRequest::GroupDamaged {
                report: Box::new(report),
            })
            .await?;
        let reply = reply.decode_section::<ControlReply>(0)?;
        match &*reply {
            ControlReply::GroupDamage(record) => Ok((**record).clone()),
            _ => Err(invalid("unexpected Control reply to a damage report")),
        }
    }
    /// NS14: Control's record of one replica's damage.
    pub(crate) async fn group_damage(
        &self,
        group_id: Uuid,
        node_id: u64,
    ) -> Result<Option<super::GroupDamageRecord>, ProtocolError> {
        let reply = self
            .read(&ReadRequest::GroupDamage { group_id, node_id })
            .await?;
        Ok((*reply.decode_section::<Option<super::GroupDamageRecord>>(0)?).clone())
    }

    async fn change_scoped_queue(
        &self,
        operation_id: Uuid,
        action: ScopedQueueOperation,
    ) -> Result<ControlReply, ProtocolError> {
        let reply = self
            .read(&ReadRequest::ChangeScopedQueue {
                operation_id,
                action,
            })
            .await?;
        let reply = reply.decode_section::<ControlReply>(0)?;
        match &*reply {
            ControlReply::Rejected(Fault::Unauthorized) => Err(ProtocolError::new(
                ErrorCode::Unauthorized,
                "scoped queue authority rejected the caller",
                Outcome::NotApplied,
            )),
            ControlReply::Rejected(Fault::Invalid) => {
                Err(invalid("invalid scoped queue operation"))
            }
            ControlReply::Rejected(Fault::Conflict) => Err(ProtocolError::new(
                ErrorCode::Conflict,
                "scoped queue operation conflicts with existing state",
                Outcome::NotApplied,
            )),
            ControlReply::Rejected(_) => Err(super::scoped_queue::stale()),
            _ => Ok((*reply).clone()),
        }
    }

    #[doc(hidden)]
    pub async fn scoped_queue(
        &self,
        group: &GroupIdentity,
    ) -> Result<Option<ScopedQueuePlacement>, ProtocolError> {
        if group.cluster_id != self.group.cluster_id
            || group.database != self.group.database
            || group.validate().is_err()
            || !matches!(group.kind, GroupKind::Data { .. })
        {
            return Err(invalid("foreign scoped queue group"));
        }
        let placement = self.scoped_queue_by_id(group.group_id).await?;
        if let Some(placement) = &placement {
            placement.validate(group)?;
        }
        Ok(placement)
    }

    #[doc(hidden)]
    pub async fn scoped_queue_by_id(
        &self,
        group_id: Uuid,
    ) -> Result<Option<ScopedQueuePlacement>, ProtocolError> {
        self.scoped_placement(ReadRequest::ScopedQueue { group_id }, group_id)
            .await
    }

    async fn scoped_placement(
        &self,
        request: ReadRequest,
        group_id: Uuid,
    ) -> Result<Option<ScopedQueuePlacement>, ProtocolError> {
        if group_id.is_nil() {
            return Err(invalid("invalid scoped queue group ID"));
        }
        let reply = self.read(&request).await?;
        let reply = self.reserve_section_decode(reply).await?;
        let placement = reply.decode_section::<Option<ScopedQueuePlacement>>(0)?;
        if let Some(placement) = &*placement {
            let group = &placement.group;
            if group.group_id != group_id
                || group.cluster_id != self.group.cluster_id
                || group.database != self.group.database
            {
                return Err(invalid("foreign scoped queue placement"));
            }
            placement.validate(group)?;
        }
        Ok((*placement).clone())
    }

    pub(crate) async fn published_cold_fence(
        &self,
        authority: Authority,
        prior_run: Uuid,
    ) -> Result<Option<Uuid>, ProtocolError> {
        let reply = self
            .read(&ReadRequest::ColdPin {
                authority,
                prior_run,
            })
            .await?;
        Ok(*reply.decode_section::<Option<Uuid>>(0)?)
    }

    #[doc(hidden)]
    pub async fn own_growth_sample(
        &self,
        pin: InventoryPin,
        claim_id: Uuid,
    ) -> Result<Decoded<DataWorkerGrowthProof>, ProtocolError> {
        let reply = self
            .read(&ReadRequest::OwnGrowthSample { pin, claim_id })
            .await?;
        let proof = reply.decode_section::<DataWorkerGrowthProof>(0)?;
        let local = self.client.local_identity();
        if proof.pin != pin
            || proof.authority.ready != pin.ready
            || proof.primary.claim_id != claim_id
            || proof.primary.mount.worker != local
            || proof
                .added
                .as_ref()
                .is_some_and(|added| added.mount.worker != local)
        {
            return Err(invalid("Control growth sample scope differs"));
        }
        Ok(proof)
    }

    #[doc(hidden)]
    pub async fn split_source(
        &self,
        expected: &GroupRecord,
        authority: Authority,
    ) -> Result<
        crate::oltp_replication::authority::timestamp::split::SplitSourcePlacement,
        ProtocolError,
    > {
        let reply = self
            .read(&ReadRequest::SplitSource {
                expected: Box::new(expected.clone()),
                authority,
            })
            .await?;
        let reply = self.reserve_section_decode(reply).await?;
        let decoded = reply.decode_section::<crate::oltp_replication::authority::timestamp::split::SplitSourcePlacement>(0)?;
        if decoded.record != *expected {
            return Err(invalid("automatic split Control source generation changed"));
        }
        decoded.validate(&expected.identity)?;
        Ok((*decoded).clone())
    }

    #[doc(hidden)]
    pub async fn own_worker_drain(
        &self,
    ) -> Result<Option<super::WorkerDrainRecord>, ProtocolError> {
        let reply = self.read(&ReadRequest::OwnWorkerDrain).await?;
        let record = reply.decode_section::<Option<super::WorkerDrainRecord>>(0)?;
        if record
            .as_ref()
            .is_some_and(|record| record.intent.worker != self.client.local_identity())
        {
            return Err(invalid("Control worker drain identity differs"));
        }
        Ok((*record).clone())
    }

    pub(crate) async fn verify_pool_replica_retired(
        &self,
        stamp: super::ReplicaRetiredStamp,
    ) -> Result<bool, ProtocolError> {
        let reply = self
            .read(&ReadRequest::PoolReplicaRetired {
                stamp: Box::new(stamp),
            })
            .await?;
        Ok(*reply.decode_section::<bool>(0)?)
    }

    pub(crate) async fn remote_replica_move(
        &self,
        id: Uuid,
    ) -> Result<Decoded<super::RemoteMoveView>, ProtocolError> {
        let reply = self.read(&ReadRequest::RemoteReplicaMove { id }).await?;
        let reply = self.reserve_section_decode(reply).await?;
        let record = reply.decode_section::<super::RemoteMoveView>(0)?;
        let local = self.client.local_identity();
        if record.plan.id != id
            || (record.plan.request.worker != local
                && !record
                    .plan
                    .remote_source
                    .as_ref()
                    .is_some_and(|source| source.worker == local))
        {
            return Err(invalid("Control remote placement identity differs"));
        }
        Ok(record)
    }

    pub(crate) async fn remote_replica_voter(
        &self,
        id: Uuid,
        node: u64,
    ) -> Result<Decoded<(super::ReplicaMove, WorkerIdentity)>, ProtocolError> {
        use super::ReplicaMovePhase;
        let reply = self
            .read(&ReadRequest::RemoteReplicaVoter { id, node })
            .await?;
        let reply = self.reserve_section_decode(reply).await?;
        let record = reply.decode_section::<(super::ReplicaMove, WorkerIdentity)>(0)?;
        let (plan, voter) = &*record;
        let local = self.client.local_identity();
        let members = match plan.phase {
            ReplicaMovePhase::Moving => &plan.old,
            ReplicaMovePhase::Published { .. } => &plan.next,
            _ => return Err(invalid("Control remote voter phase differs")),
        };
        let learner_check = plan.phase == ReplicaMovePhase::Moving
            && plan.remote_target()
            && plan.request.worker == local;
        if plan.id != id
            || (local != *voter && !learner_check)
            || !members
                .iter()
                .any(|member| member.node_id == node && member.worker_id == voter.worker_id)
        {
            return Err(invalid("Control remote voter identity differs"));
        }
        Ok(record)
    }

    #[doc(hidden)]
    pub fn new(
        client: WorkerClient,
        shared: Arc<WorkerSharedResources>,
        group: GroupIdentity,
        routes: Arc<dyn ControlRoutes>,
    ) -> Result<Arc<Self>, ProtocolError> {
        group
            .validate()
            .map_err(|_| invalid("invalid Control RPC group"))?;
        if group.kind != GroupKind::Control {
            return Err(invalid("wrong Control RPC group kind"));
        }
        let replica_routes = ReplicaRouteCache::new(&shared);
        Ok(Arc::new(Self {
            client,
            shared,
            group,
            routes,
            last_read_route: Mutex::new(None),
            replica_routes,
            service_grants: Default::default(),
        }))
    }
    async fn read(&self, request: &ReadRequest) -> Result<ReceivedReply, ProtocolError> {
        let header = self
            .shared
            .encode_in(ByteClass::Progress, request, HEADER_LIMIT)?;
        // PERF1: Control reads this process issues, by request kind.
        let _timer =
            nabedra_resources::stage_timing::json_variant("control.read.", header.as_slice())
                .timer();
        let placement_role = matches!(request, ReadRequest::ServiceRoleAtReplica { .. });
        let role_dependency = placement_role
            || matches!(
                request,
                ReadRequest::ServiceRole { .. } | ReadRequest::WorkerExcluded { .. }
            );
        let exact_sample = matches!(request, ReadRequest::OwnGrowthSample { .. });
        let deadline = request
            .supports_bounded_failover()
            .then(|| nabedra_rt::time::now() + READ_FAILOVER_TIMEOUT);
        let use_route_hint = deadline.is_some()
            || matches!(
                request,
                ReadRequest::ChangeScopedQueue { .. } | ReadRequest::GroupDamaged { .. }
            );
        let mut last = unavailable();
        loop {
            let configured_preferred = self.routes.preferred();
            let preferred = match configured_preferred {
                None if use_route_hint => self.preferred_read_route()?,
                value => value,
            };
            let mut first = preferred;
            let mut after = None;
            let mut attempted = false;
            let mut legacy_placement_role = false;
            loop {
                let cursor = after;
                let (worker, preferred_attempt) = if let Some(worker) = first.take() {
                    (worker, true)
                } else {
                    let Some(worker) = self.routes.next(after)? else {
                        break;
                    };
                    if after.is_some_and(|previous| worker <= previous) {
                        return Err(invalid("Control routes are not strictly ordered"));
                    }
                    after = Some(worker);
                    if Some(worker) == preferred {
                        continue;
                    }
                    (worker, false)
                };
                if worker.is_nil() {
                    return Err(invalid("nil Control route"));
                }
                attempted = true;
                let exchange = async {
                    if role_dependency {
                        // This endpoint uses Control consensus and its local state.
                        // It does not make another outgoing Authority read.
                        self.client
                            .request_authority_dependency(
                                &self.group,
                                worker,
                                header.clone(),
                                64 << 10,
                            )
                            .await
                    } else {
                        self.client
                            .request_classified(
                                &self.group,
                                worker,
                                Purpose::Authority,
                                header.clone(),
                                [None, None, None],
                                64 << 10,
                            )
                            .await
                    }
                };
                let result = if let Some(deadline) = deadline {
                    let now = nabedra_rt::time::now();
                    if now >= deadline {
                        return Err(last);
                    }
                    time::timeout_at(deadline.min(now + READ_PEER_TIMEOUT), exchange)
                        .await
                        .unwrap_or(Err(RequestError::Unreachable))
                } else {
                    exchange.await
                };
                let reply = match result {
                    Ok(reply) => reply,
                    Err(RequestError::PeerRejected {
                        worker: peer,
                        error,
                    }) if exact_sample || deadline.is_some() => {
                        if peer.worker_id != worker {
                            return Err(invalid(
                                "Control error response transport binding differs",
                            ));
                        }
                        if placement_role && error == invalid("invalid Control authority request") {
                            // Older Control voters reject this closed request in their parser.
                            legacy_placement_role = true;
                            continue;
                        }
                        let retryable = if exact_sample {
                            growth_sample_peer_unavailable(&error)
                        } else {
                            control_read_peer_unavailable(&error)
                        };
                        if !retryable {
                            return Err(error);
                        }
                        last = error;
                        continue;
                    }
                    Err(failure) => {
                        if (exact_sample || deadline.is_some())
                            && !matches!(failure, RequestError::Unreachable)
                            && !matches!(
                                &failure,
                                RequestError::Protocol(error) if peer_enrollment_unavailable(error)
                                    && deadline.is_some()
                            )
                        {
                            // A local lane refusal or invalid exchange cannot
                            // improve by selecting another peer.
                            return Err(failure.into());
                        }
                        last = failure.into();
                        continue;
                    }
                };
                self.validate_read_reply(&reply, worker)?;
                if use_route_hint
                    && configured_preferred.is_none()
                    && !preferred_attempt
                    && let Ok(mut route) = self.last_read_route.lock()
                {
                    *route = Some((cursor, worker));
                }
                return Ok(reply);
            }
            if legacy_placement_role {
                return Err(unsupported_service_route());
            }
            let Some(deadline) = deadline else {
                return Err(last);
            };
            if !attempted || nabedra_rt::time::now() >= deadline {
                return Err(last);
            }
            // A new election may make a peer usable after the first round.
            time::sleep_until(deadline.min(nabedra_rt::time::now() + READ_RETRY_DELAY)).await;
        }
    }

    fn validate_read_reply(
        &self,
        reply: &ReceivedReply,
        worker: Uuid,
    ) -> Result<(), ProtocolError> {
        if reply.remote_identity().worker_id != worker
            || reply.group() != &self.group
            || reply.purpose() != Purpose::Authority
            || reply.sections[1].is_some()
            || reply.sections[2].is_some()
        {
            return Err(invalid("Control response transport binding differs"));
        }
        let decoded = reply.decode_header::<ReadReply>()?;
        if decoded.nonce != reply.nonce()
            || reply.sections[0].as_ref().map(|bytes| bytes.len() as u64) != Some(decoded.bytes)
        {
            return Err(invalid("Control response payload differs"));
        }
        Ok(())
    }

    fn preferred_read_route(&self) -> Result<Option<Uuid>, ProtocolError> {
        let hint = self.last_read_route.lock().ok().and_then(|route| *route);
        let Some((cursor, worker)) = hint else {
            return Ok(None);
        };
        // Recheck membership before using a previously responsive route.
        Ok((self.routes.next(cursor)? == Some(worker)).then_some(worker))
    }

    async fn authority(
        &self,
        scope: &QueueScope,
        request: ReadRequest,
    ) -> Result<QueueAuthority, ProtocolError> {
        if scope.cluster_id != self.group.cluster_id || scope.database != self.group.database {
            return Err(invalid("foreign Control query scope"));
        }
        let reply = self.read(&request).await?;
        let current = reply.decode_section::<QueueAuthority>(0)?;
        current
            .ready
            .view
            .validate()
            .map_err(|_| invalid("invalid Control ready view"))?;
        current
            .owner
            .validate()
            .map_err(|_| invalid("invalid Control owner"))?;
        if current.ready.ready_count == 0 {
            return Err(unavailable());
        }
        Ok(*current)
    }
    pub(crate) async fn worker_replaced(
        &self,
        worker: WorkerIdentity,
    ) -> Result<bool, ProtocolError> {
        let reply = self.read(&ReadRequest::WorkerExcluded { worker }).await?;
        Ok(*reply.decode_section::<bool>(0)?)
    }
    /// An identity pinned in old bootstrap configuration must not vouch
    /// for itself. A Data-only worker checks its bootstrap pins against a
    /// majority of the distinct Control pins once, at startup.
    pub(crate) async fn worker_exclusion_quorum(
        &self,
        identity: WorkerIdentity,
    ) -> Result<bool, ProtocolError> {
        self.quorum_read(
            &ReadRequest::WorkerExcluded { worker: identity },
            64 << 10,
            |reply| async move { Ok(*reply.decode_section::<bool>(0)?) },
        )
        .await
    }

    /// REP6: every committed system revocation (the set Control hosts
    /// enforce), each page agreed by a majority of the current Control voters. A
    /// Data-only worker seeds its revocation view with this at startup and
    /// refreshes it once per second (`REVOCATION_REFRESH`), never per
    /// exchange.
    pub(crate) async fn revocations_quorum(&self) -> Result<Vec<WorkerIdentity>, ProtocolError> {
        let mut revoked = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .quorum_read(
                    &ReadRequest::Revocations { after },
                    REVOCATION_PAGE_BYTES,
                    |reply| async move {
                        let bytes = reply.sections[0]
                            .as_ref()
                            .ok_or_else(|| invalid("Control revocation page is missing"))?;
                        let budget = crate::oltp::mvcc::json_decode_budget(bytes.as_slice())?;
                        let reply = reply.reserve_reply_decode(&self.shared, budget).await?;
                        let page = reply.decode_section::<InventoryPage<WorkerIdentity>>(0)?;
                        let page = (*page).clone();
                        Ok((page.items.into_inner(), page.after, page.done))
                    },
                )
                .await?;
            let (items, cursor, done) = page;
            for worker in &items {
                if after.is_some_and(|previous| worker.worker_id <= previous)
                    || revoked.len() == MAX_VIEW_REVOCATIONS
                {
                    return Err(invalid("invalid Control revocation page"));
                }
                after = Some(worker.worker_id);
                revoked.push(*worker);
            }
            if cursor != after || (!done && items.is_empty()) {
                return Err(invalid("invalid Control revocation page"));
            }
            if done {
                return Ok(revoked);
            }
        }
    }

    /// `request` sent to every current Control voter; the first value a majority of
    /// them return (each a linearizable read on its replica) wins. Snapshot
    /// the voter set so a concurrent publication cannot mix two quorums.
    async fn quorum_read<T, F, D>(
        &self,
        request: &ReadRequest,
        reply_bytes: usize,
        decode: D,
    ) -> Result<T, ProtocolError>
    where
        T: PartialEq,
        F: std::future::Future<Output = Result<T, ProtocolError>>,
        D: Fn(ReceivedReply) -> F,
    {
        let header = self
            .shared
            .encode_in(ByteClass::Progress, request, HEADER_LIMIT)?;
        let voters = self.routes.quorum_voters()?;
        if !matches!(voters.len(), 1 | 3 | 5) {
            return Err(unavailable());
        }
        let quorum = voters.len() / 2 + 1;
        // D01/REP6 evidence: majority reads and the RPCs they send, by kind.
        let _call = nabedra_resources::stage_timing::json_variant(
            "control.quorum_read.",
            header.as_slice(),
        )
        .timer();
        let rpc = nabedra_resources::stage_timing::json_variant(
            "control.quorum_rpc.",
            header.as_slice(),
        );
        let decode = &decode;
        let mut requests = FuturesUnordered::new();
        for voter in voters {
            let header = header.clone();
            requests.push(async move {
                let _rpc = rpc.timer();
                let reply = self
                    .client
                    .request_authority_dependency(&self.group, voter, header, reply_bytes)
                    .await?;
                self.validate_read_reply(&reply, voter)?;
                decode(reply).await
            });
        }
        time::timeout(READ_FAILOVER_TIMEOUT, async move {
            let mut agreement: Vec<(T, usize)> = Vec::new();
            while let Some(result) = requests.next().await {
                let Ok(value) = result else { continue };
                let votes = match agreement.iter_mut().find(|(seen, _)| *seen == value) {
                    Some((_, votes)) => {
                        *votes += 1;
                        *votes
                    }
                    None => {
                        agreement.push((value, 1));
                        1
                    }
                };
                if votes == quorum {
                    let index = agreement
                        .iter()
                        .position(|(_, count)| *count == quorum)
                        .ok_or_else(unavailable)?;
                    return Ok(agreement.swap_remove(index).0);
                }
            }
            Err(unavailable())
        })
        .await
        .map_err(|_| unavailable())?
    }

    #[doc(hidden)]
    pub async fn group(&self, group_id: Uuid) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Group { group_id }).await
    }
    pub(crate) async fn group_members(
        &self,
        expected: GroupRecord,
        after: Option<u64>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::GroupMembers {
            expected: Box::new(expected),
            after,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn service_role(
        &self,
        scope: &QueueScope,
        remote: WorkerIdentity,
        action: ServiceAction,
        target: GroupIdentity,
    ) -> Result<ServiceAuthorization, ProtocolError> {
        if scope.cluster_id != self.group.cluster_id || scope.database != self.group.database {
            return Err(invalid("foreign service authorization scope"));
        }
        let reply = self
            .read(&ReadRequest::ServiceRole {
                remote,
                action,
                target: Box::new(target),
            })
            .await?;
        Ok(*reply.decode_section::<ServiceAuthorization>(0)?)
    }

    pub(crate) async fn verify_timestamp_point_route(
        &self,
        session: &AuthenticatedSession,
        node_id: u64,
        route: TimestampPointRoute,
    ) -> Result<(), ProtocolError> {
        self.read_timestamp_point_route(session, node_id, route, None)
            .await
    }

    pub(crate) async fn verify_timestamp_point_decision_route(
        &self,
        session: &AuthenticatedSession,
        node_id: u64,
        route: TimestampPointRoute,
        action: ServiceAction,
        source_digest: Digest,
    ) -> Result<(), ProtocolError> {
        self.read_timestamp_point_route(session, node_id, route, Some((action, source_digest)))
            .await
    }

    async fn read_timestamp_point_route(
        &self,
        session: &AuthenticatedSession,
        node_id: u64,
        route: TimestampPointRoute,
        decision_source: Option<(ServiceAction, Digest)>,
    ) -> Result<(), ProtocolError> {
        let reply = self
            .read(&ReadRequest::TimestampPointProof {
                remote: session.remote(),
                target: Box::new(session.group().clone()),
                node_id,
                route,
                decision_source,
            })
            .await?;
        Ok(*reply.decode_section::<()>(0)?)
    }
    pub(crate) async fn service_role_at_replica(
        &self,
        scope: &QueueScope,
        remote: WorkerIdentity,
        target: GroupIdentity,
        node_id: u64,
        generation: Uuid,
    ) -> Result<(), ProtocolError> {
        if scope.cluster_id != self.group.cluster_id || scope.database != self.group.database {
            return Err(invalid("foreign service authorization scope"));
        }
        let reply = self
            .read(&ReadRequest::ServiceRoleAtReplica {
                remote,
                target: Box::new(target),
                node_id,
                generation,
            })
            .await?;
        let grant = reply.decode_section::<ServiceAuthorization>(0)?;
        if grant.owner != Some(remote) {
            return Err(invalid(
                "receiver placement grant differs from request owner",
            ));
        }
        Ok(())
    }
    #[doc(hidden)]
    pub async fn ready_page(
        &self,
        expected: ReadyMembership,
        after: Option<Uuid>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Ready { expected, after }).await
    }
}

fn peer_enrollment_unavailable(error: &ProtocolError) -> bool {
    error.code == ErrorCode::StorageUnavailable
        && error.outcome == Outcome::Unknown
        && error.message == "peer enrollment view is unavailable"
}

fn control_read_peer_unavailable(error: &ProtocolError) -> bool {
    growth_sample_peer_unavailable(error)
        || peer_enrollment_unavailable(error)
        || error.code == ErrorCode::Overloaded
}

fn growth_sample_peer_unavailable(error: &ProtocolError) -> bool {
    // A stale pin and a failed ReadIndex both use StorageUnavailable. Only
    // recognized quorum/transport failures allow another authenticated read.
    match (error.code, error.outcome) {
        (ErrorCode::StorageUnavailable, Outcome::Unknown) => matches!(
            error.message.as_str(),
            "Control leader quorum or local application unavailable"
                | "typed Raft quorum, application or leader authority unavailable"
                | "worker transport unavailable"
        ),
        (ErrorCode::Overloaded, Outcome::NotApplied) => matches!(
            error.message.as_str(),
            "worker handler lane full" | "worker preflight lane full"
        ),
        _ => false,
    }
}
impl AuthorityReader for ControlClient {
    fn current<'a>(&'a self, scope: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(scope, ReadRequest::Current))
    }
    fn removal_view<'a>(
        &'a self,
        scope: &'a QueueScope,
        owner: WorkerIdentity,
        ticket: JoinTicket,
    ) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(scope, ReadRequest::QueueRemoval { owner, ticket }))
    }
    fn excludes_process(&self, worker: WorkerIdentity) -> EndpointFuture<'_, bool> {
        Box::pin(self.worker_replaced(worker))
    }
    fn terminal_reader_view<'a>(
        &'a self,
        scope: &'a QueueScope,
        remote: WorkerIdentity,
        timestamps_group_id: Uuid,
    ) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(
            scope,
            ReadRequest::TerminalReader {
                remote,
                timestamps_group_id,
            },
        ))
    }
}
fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "Control quorum is unavailable",
        Outcome::NotApplied,
    )
}

pub(crate) type ReplicaRoutes = Arc<VerifiedReplicaRoutes>;

/// One verified membership cut. The route vector and record remain charged to
/// worker working memory through eviction and the last outstanding caller.
#[doc(hidden)]
pub struct VerifiedReplicaRoutes {
    pub record: GroupRecord,
    pub members: Vec<ReplicaMember>,
    hint: Mutex<ReplicaRouteHint>,
    _owner: ScratchLease,
}

#[derive(Default)]
struct ReplicaRouteHint {
    last_successful_node: Option<u64>,
    receiver: Option<(u64, WorkerIdentity)>,
    probe_after: Option<Instant>,
    /// When a strong Control read last confirmed this record (PERF1).
    confirmed_at: Option<Instant>,
}

impl VerifiedReplicaRoutes {
    fn confirmed(&self) {
        if let Ok(mut hint) = self.hint.lock() {
            hint.confirmed_at = Some(nabedra_rt::time::now_std());
        }
    }

    fn confirmed_within(&self, max_age: Duration) -> bool {
        self.hint
            .lock()
            .ok()
            .and_then(|hint| hint.confirmed_at)
            .is_some_and(|at| nabedra_rt::time::elapsed(at) <= max_age)
    }

    #[doc(hidden)]
    pub fn last_successful_node(&self) -> Option<u64> {
        self.hint.lock().ok()?.last_successful_node
    }

    pub(crate) fn record_successful_node(&self, node_id: u64) {
        if let Ok(mut hint) = self.hint.lock() {
            hint.last_successful_node = Some(node_id);
        }
    }

    #[doc(hidden)]
    pub fn hinted_receiver(&self, node_id: u64) -> Option<WorkerIdentity> {
        let (node, receiver) = self.hint.lock().ok()?.receiver?;
        (node == node_id).then_some(receiver)
    }

    pub(crate) fn begin_receiver_probe(&self) -> bool {
        let Ok(mut hint) = self.hint.lock() else {
            return false;
        };
        let now = nabedra_rt::time::now_std();
        if hint.probe_after.is_some_and(|after| after > now) {
            return false;
        }
        hint.probe_after = Some(now + Duration::from_secs(1));
        true
    }

    pub(crate) fn confirm_hinted_receiver(&self, node_id: u64, receiver: WorkerIdentity) {
        if let Ok(mut hint) = self.hint.lock() {
            hint.receiver = Some((node_id, receiver));
        }
    }
}

const ROUTE_CACHE_ENTRIES: usize = 64;
const ROUTE_CACHE_ENTRY_BYTES: usize = 4 << 10;

struct ReplicaRouteCache {
    entries: [Option<ReplicaRoutes>; ROUTE_CACHE_ENTRIES],
    next: usize,
    _metadata: ScratchLease,
}
impl ReplicaRouteCache {
    fn new(shared: &WorkerSharedResources) -> Option<Box<Mutex<Self>>> {
        let metadata = shared
            .reserve_scratch(ByteClass::Data, size_of::<Mutex<Self>>() + 64)
            .ok()?;
        Some(Box::new(Mutex::new(Self {
            entries: [const { None }; ROUTE_CACHE_ENTRIES],
            next: 0,
            _metadata: metadata,
        })))
    }

    fn slot(&mut self, group: &GroupIdentity) -> Option<&mut Option<ReplicaRoutes>> {
        self.entries.iter_mut().find(|slot| {
            slot.as_ref()
                .is_some_and(|route| route.record.identity == *group)
        })
    }

    fn store(&mut self, route: ReplicaRoutes) -> ReplicaRoutes {
        if let Some(existing) = self
            .slot(&route.record.identity)
            .and_then(|slot| slot.as_ref())
        {
            // A concurrent verification may already have filled this slot.
            // Each caller uses only the record from its own strong read.
            return if existing.record == route.record {
                existing.clone()
            } else {
                route
            };
        }
        if let Some(slot) = self.entries.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(route.clone());
        } else {
            self.entries[self.next] = Some(route.clone());
            self.next = (self.next + 1) % ROUTE_CACHE_ENTRIES;
        }
        route
    }
}
impl ControlClient {
    #[doc(hidden)]
    pub fn cached_replica_routes(&self, group: &GroupIdentity) -> Option<ReplicaRoutes> {
        self.route_cache()?.slot(group)?.clone()
    }
    pub(crate) async fn enrollment_revision(&self) -> Result<u64, ProtocolError> {
        let reply = self.read(&ReadRequest::EnrollmentRevision).await?;
        Ok(*reply.decode_section::<u64>(0)?)
    }
    #[doc(hidden)]
    pub async fn group_inventory_revision(&self) -> Result<u64, ProtocolError> {
        let reply = self.read(&ReadRequest::GroupInventoryRevision).await?;
        Ok(*reply.decode_section::<u64>(0)?)
    }
    #[doc(hidden)]
    pub async fn groups(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Groups { revision, after }).await
    }
    pub(crate) async fn restart_preparation(
        &self,
        revision: u64,
    ) -> Result<Option<super::RestartPreparation>, ProtocolError> {
        let reply = self
            .read(&ReadRequest::RestartPreparation { revision })
            .await?;
        Ok((*reply.decode_section::<Option<super::RestartPreparation>>(0)?).clone())
    }
    pub(crate) async fn prepared_data_worker(
        &self,
        revision: u64,
    ) -> Result<Option<WorkerEnrollment>, ProtocolError> {
        let reply = self
            .read(&ReadRequest::PreparedDataWorker { revision })
            .await?;
        Ok((*reply.decode_section::<Option<WorkerEnrollment>>(0)?).clone())
    }
    pub(crate) async fn workers(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Workers { revision, after }).await
    }
    pub(crate) async fn apis(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Apis { revision, after }).await
    }
    pub(crate) async fn search(
        &self,
        request: super::search::SearchRequest,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Search(request)).await
    }
    #[doc(hidden)]
    pub async fn replica_routes(
        &self,
        expected: &GroupIdentity,
    ) -> Result<ReplicaRoutes, ProtocolError> {
        self.replica_routes_inner(expected, 0).await
    }

    pub(crate) async fn timestamp_point_routes(
        &self,
        expected: &GroupIdentity,
    ) -> Result<ReplicaRoutes, ProtocolError> {
        self.replica_routes_inner(expected, 8).await
    }

    async fn replica_routes_inner(
        &self,
        expected: &GroupIdentity,
        progress_member_limit: usize,
    ) -> Result<ReplicaRoutes, ProtocolError> {
        if expected.cluster_id != self.group.cluster_id || expected.database != self.group.database
        {
            return Err(invalid("foreign replica routing scope"));
        }
        let start = nabedra_rt::time::now_std();
        let reply = self.group(expected.group_id).await?;
        let reply = self.reserve_section_decode(reply).await?;
        let decoded = reply.decode_section::<Option<GroupRecord>>(0)?;
        let record = decoded
            .as_ref()
            .filter(|record| record.identity == *expected && record.replica_count != 0);
        let (cached, changed) = {
            let mut cache = self.route_cache();
            if let Some(slot) = cache.as_mut().and_then(|cache| cache.slot(expected)) {
                if slot
                    .as_ref()
                    .is_some_and(|route| record == Some(&route.record))
                {
                    (slot.clone(), false)
                } else {
                    *slot = None;
                    (None, true)
                }
            } else {
                (None, false)
            }
        };
        if let Some(route) = cached {
            route.confirmed();
            tracing::debug!(
                target: "nabedra_database_server::oltp_replication::service_rpc",
                cache = "hit",
                pages = 0,
                ms = nabedra_rt::time::elapsed(start).as_millis(),
                "verified replica routes"
            );
            return Ok(route);
        }
        let record = record.ok_or_else(|| {
            if decoded.is_some() {
                invalid("replica routing identity differs")
            } else {
                unavailable()
            }
        })?;
        let count = usize::try_from(record.replica_count).map_err(|_| unavailable())?;
        let bytes = count
            .checked_mul(size_of::<ReplicaMember>() + 128)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(unavailable)?;
        let (owner, retain) = match self.shared.reserve_scratch(ByteClass::Data, bytes) {
            Ok(owner) => (owner, true),
            Err(error) => {
                let retried = if let Some(mut cache) = self.route_cache()
                    && cache.entries.iter().any(Option::is_some)
                {
                    cache.entries.fill(None);
                    drop(cache);
                    self.shared.reserve_scratch(ByteClass::Data, bytes)
                } else {
                    Err(error)
                };
                match retried {
                    Ok(owner) => (owner, false),
                    Err(error) if count > progress_member_limit => return Err(error),
                    Err(_) => (
                        self.shared.reserve_scratch(ByteClass::Progress, bytes)?,
                        false,
                    ),
                }
            }
        };
        let record = record.clone();
        // Release the record's independent reply decoder before requesting pages.
        drop(decoded);
        drop(reply);

        let record_ms = nabedra_rt::time::elapsed(start).as_millis();
        let (members, pages) = self.load_replica_members(&record, count).await?;
        let retained = members
            .capacity()
            .checked_mul(size_of::<ReplicaMember>())
            .and_then(|bytes| {
                bytes.checked_add(
                    record.identity.database.capacity_bytes()
                        + size_of::<VerifiedReplicaRoutes>()
                        + 2 * size_of::<usize>()
                        + 128,
                )
            })
            .ok_or_else(unavailable)?;
        owner.shrink_unused(retained)?;
        let route = Arc::new(VerifiedReplicaRoutes {
            record,
            members,
            hint: Mutex::new(ReplicaRouteHint {
                confirmed_at: Some(nabedra_rt::time::now_std()),
                ..ReplicaRouteHint::default()
            }),
            _owner: owner,
        });
        let cacheable = retain && retained <= ROUTE_CACHE_ENTRY_BYTES;
        tracing::debug!(
            target: "nabedra_database_server::oltp_replication::service_rpc",
            cache = if changed { "changed" } else { "miss" },
            cacheable,
            pages,
            record_ms,
            ms = nabedra_rt::time::elapsed(start).as_millis(),
            "verified replica routes"
        );
        Ok(match self.route_cache() {
            Some(mut cache) if cacheable => cache.store(route),
            _ => route,
        })
    }

    /// The cached routes of `expected` if a strong Control read confirmed
    /// their record within `max_age`; no Control read. Only for calls whose
    /// outcome does not depend on routing: a stale member list can only make
    /// the call fail, never answer it wrongly (PERF1).
    pub(crate) fn recent_replica_routes(
        &self,
        expected: &GroupIdentity,
        max_age: Duration,
    ) -> Option<ReplicaRoutes> {
        let mut cache = self.route_cache()?;
        let route = cache.slot(expected)?.clone()?;
        route.confirmed_within(max_age).then_some(route)
    }

    #[doc(hidden)]
    pub fn invalidate_replica_routes(&self, route: &ReplicaRoutes) {
        let invalidated = {
            let mut cache = self.route_cache();
            cache
                .as_mut()
                .and_then(|cache| cache.slot(&route.record.identity))
                .is_some_and(|slot| {
                    if slot
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(current, route))
                    {
                        *slot = None;
                        true
                    } else {
                        false
                    }
                })
        };
        if invalidated {
            tracing::debug!(
                target: "nabedra_database_server::oltp_replication::service_rpc",
                cache = "invalidated",
                "service exhausted verified replica routes"
            );
        }
    }

    fn route_cache(&self) -> Option<std::sync::MutexGuard<'_, ReplicaRouteCache>> {
        self.replica_routes.as_ref()?.lock().ok()
    }

    async fn reserve_section_decode(
        &self,
        reply: ReceivedReply,
    ) -> Result<ReceivedReply, ProtocolError> {
        let raw = reply.sections[0].as_ref().ok_or_else(unavailable)?.clone();
        let decode = self
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                crate::oltp::mvcc::json_decode_budget(raw.as_ref())
            })
            .await?;
        reply.reserve_reply_decode(&self.shared, decode).await
    }

    async fn load_replica_members(
        &self,
        record: &GroupRecord,
        count: usize,
    ) -> Result<(Vec<ReplicaMember>, usize), ProtocolError> {
        let mut members = Vec::new();
        members
            .try_reserve_exact(count)
            .map_err(|_| unavailable())?;
        let mut after = None;
        let mut root = super::machine::group_seed();
        let mut pages = 0;
        loop {
            let reply = self.group_members(record.clone(), after).await?;
            let reply = self.reserve_section_decode(reply).await?;
            let page = self
                .shared
                .execute(ExecutionClass::Control, 0, 0, move || {
                    reply.decode_section::<super::service::ReplicaInventoryPage>(0)
                })
                .await?;
            if page.group != *record || (!page.done && page.items.is_empty()) {
                return Err(invalid("replica routing page differs"));
            }
            pages += 1;
            for member in page.items.iter() {
                member
                    .validate()
                    .map_err(|_| invalid("invalid replica routing member"))?;
                if after.is_some_and(|previous| member.node_id <= previous)
                    || members.len() >= count
                {
                    return Err(invalid("replica routing order or count differs"));
                }
                root = super::machine::group_step(root, member);
                after = Some(member.node_id);
                members.push(member.clone());
            }
            if page.after != after {
                return Err(invalid("replica routing cursor differs"));
            }
            if page.done {
                break;
            }
        }
        if members.len() != count
            || super::machine::group_root(record.replica_count, root) != record.replica_root
        {
            return Err(invalid("replica routing root differs"));
        }
        Ok((members, pages))
    }
}
