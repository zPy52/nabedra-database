use super::super::timestamp::ReadyFenceReceipt;
use super::scoped_queue::ScopedQueueOperation;
use crate::{
    oltp::pool::ScratchLease,
    oltp_replication::{ReadySetHasher, owned::OwnedPayload, types::*},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use std::{fmt, sync::Arc};
use uuid::Uuid;

/// Produced by the leader's configured operator authorization boundary. A
/// worker RPC can submit an intent, but cannot create a raw Control proposal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct AuthorizationStamp {
    pub authorization_id: Uuid,
    pub policy: Digest,
    pub subject: Digest,
    pub evidence: Digest,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct ExclusionStamp {
    pub worker: WorkerIdentity,
    pub authorization: AuthorizationStamp,
    pub ownership_fence: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct GroupRecord {
    pub identity: GroupIdentity,
    pub generation: Uuid,
    pub replica_count: u64,
    pub replica_root: Digest,
    pub membership_position: AppliedPosition,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupMembershipStamp {
    pub group: GroupIdentity,
    pub generation: Uuid,
    pub replica_count: u64,
    pub replica_root: Digest,
    pub applied_at: AppliedPosition,
    pub authority: Authority,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[doc(hidden)]
pub enum ReadyChange {
    Join,
    Remove,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[doc(hidden)]
pub enum ReadyPhase {
    Building,
    AwaitingTimestampFence,
    AwaitingQueueProof,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct ReadyPending {
    pub transition_id: Uuid,
    pub expected: ReadyMembership,
    pub target: WorkerIdentity,
    pub change: ReadyChange,
    pub generation: Uuid,
    pub source_after: Option<Uuid>,
    pub target_seen: bool,
    pub root: ReadySetHasher,
    pub next: Option<ReadyMembership>,
    pub phase: ReadyPhase,
    pub timestamp_fence: Option<ReadyFenceReceipt>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupBuild {
    pub generation: Uuid,
    pub identity: GroupIdentity,
    pub count: u64,
    pub last_node: Option<u64>,
    pub chain: Digest,
    #[serde(default)]
    pub expected: Option<GroupRecord>,
    #[serde(default)]
    pub retained_count: u64,
    #[serde(default)]
    pub retired_count: u64,
}

pub(crate) use nabedra_oltp_types::authority::ReplicaRetiredStamp;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct FencePending {
    pub transition_id: Uuid,
    pub expected: ReadyMembership,
    pub next: ReadyMembership,
    pub next_owner: Option<WorkerIdentity>,
    pub cold: bool,
    pub authorization: AuthorizationStamp,
    pub group_inventory_revision: u64,
    pub required_groups: u64,
    pub fenced_groups: u64,
    pub excluded_workers: u64,
    #[serde(default)]
    pub extra_exclusion: Option<WorkerIdentity>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[doc(hidden)]
pub enum Pending {
    Ready(ReadyPending),
    Fence(FencePending),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct State {
    pub policy: Digest,
    pub ready: ReadyMembership,
    pub owner: Option<WorkerIdentity>,
    pub ready_generation: Uuid,
    pub enrollment_revision: u64,
    pub group_inventory_revision: u64,
    pub group_count: u64,
    pub invalid_ready: u64,
    pub pending: Option<Pending>,
    #[serde(default)]
    pub restart: Option<super::restart::RestartPreparation>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerRecord {
    pub enrollment: WorkerEnrollment,
    pub previous_process: Option<ExclusionStamp>,
}

/// An API ingress has a current TLS enrollment and database scope, but no
/// queue-holder, pool, Raft-replica or readiness authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct ApiEnrollment {
    pub database: Text<128>,
    pub enrollment: WorkerEnrollment,
}
impl ApiEnrollment {
    #[doc(hidden)]
    pub fn transport(&self) -> &WorkerEnrollment {
        &self.enrollment
    }
    #[doc(hidden)]
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.enrollment
            .validate()
            .map_err(|_| unavailable("invalid API enrollment"))?;
        if !crate::oltp::valid_database(self.database.as_str()) {
            return Err(unavailable("invalid API database scope"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[doc(hidden)]
pub enum ControlCommand {
    SystemRecovery {
        operation_id: Uuid,
        command: Box<super::SystemRecoveryCommand>,
    },
    /// `data.direct.v1` adverts and activation. New in this build: see
    /// `control::direct` for the upgrade order.
    DirectCapability {
        operation_id: Uuid,
        actor: WorkerIdentity,
        action: super::DirectCapabilityOperation,
        /// The proposer's wall clock (Unix ms). An activation records it, so
        /// every process measures the queued-history window (T7c) from the
        /// same instant.
        #[serde(default)]
        unix_ms: u64,
    },
    ScopedQueue {
        operation_id: Uuid,
        actor: WorkerIdentity,
        action: Box<ScopedQueueOperation>,
        authorization: Option<AuthorizationStamp>,
    },
    EnsureScopedQueue {
        operation_id: Uuid,
        expected: Box<GroupRecord>,
        run_id: Uuid,
        authorization: AuthorizationStamp,
    },
    BeginWorkerDrain {
        operation_id: Uuid,
        worker: WorkerIdentity,
        authorization: AuthorizationStamp,
        intent: super::WorkerDrainIntent,
    },
    BeginWorkerDrainRemoval {
        operation_id: Uuid,
        worker: WorkerIdentity,
        authorization: AuthorizationStamp,
        id: Uuid,
        expected: ReadyMembership,
        placement_revision: u64,
        policy: Uuid,
        growth_revision: u64,
    },
    CloseDataWorker {
        operation_id: Uuid,
        proof: Box<super::WorkerCloseAttestation>,
    },
    EksWorkerReplacement {
        operation_id: Uuid,
        worker: WorkerIdentity,
        authorization: AuthorizationStamp,
        request: Box<super::SignedEksReplacement>,
    },
    CompleteEksWorkerReplacement {
        operation_id: Uuid,
        worker: WorkerIdentity,
        mounted: Box<super::worker_drain::EksMountedReplacement>,
    },
    PoolDrain {
        operation_id: Uuid,
        worker: WorkerIdentity,
        authorization: AuthorizationStamp,
        command: super::pool_drain::PoolDrainCommand,
    },
    PrepareDataWorker {
        operation_id: Uuid,
        intent: Box<super::DataWorkerIntent>,
        authorization: AuthorizationStamp,
    },
    CompleteDataWorker {
        operation_id: Uuid,
        intent: Box<super::DataWorkerIntent>,
        mount: crate::oltp_deployment::controller::MountedGrowthStamp,
        proof: super::restart::RestartMountProof,
    },
    BeginDataWorkerJoin {
        operation_id: Uuid,
        claim_id: Uuid,
        worker: WorkerIdentity,
        expected: ReadyMembership,
    },
    RegisterRestartMount {
        operation_id: Uuid,
        worker: WorkerIdentity,
        pool_id: Uuid,
        generation: u64,
        public_key: Digest,
        authorization: AuthorizationStamp,
    },
    PrepareRestart {
        operation_id: Uuid,
        intent: super::restart::RestartIntent,
        authorization: AuthorizationStamp,
    },
    CompleteRestart {
        operation_id: Uuid,
        intent: super::restart::RestartIntent,
        proof: super::restart::RestartMountProof,
        exclusion: ExclusionStamp,
    },
    ReplicaRetired {
        operation_id: Uuid,
        authorization: AuthorizationStamp,
        proof: ReplicaRetiredStamp,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        remote: Option<super::restart::RestartMountProof>,
    },
    BeginReplicaMove {
        operation_id: Uuid,
        authorization: AuthorizationStamp,
        plan: Box<super::placement::ReplicaMove>,
    },
    ArmReplicaMove {
        operation_id: Uuid,
        transition_id: Uuid,
        worker: WorkerIdentity,
        authorization: AuthorizationStamp,
        guard: Box<crate::oltp::mvcc::store::lifecycle::SealedLifecycleGuard>,
    },
    CancelReplicaMove {
        operation_id: Uuid,
        transition_id: Uuid,
        worker: WorkerIdentity,
        authorization: AuthorizationStamp,
    },
    PublishReplicaMove {
        operation_id: Uuid,
        authorization: AuthorizationStamp,
        proof: Box<super::placement::ReplicaMovePublication>,
    },
    Growth {
        operation_id: Uuid,
        authorization: AuthorizationStamp,
        command: crate::oltp_deployment::controller::GrowthCommand,
    },
    Initialize {
        operation_id: Uuid,
        run_id: Uuid,
        authorization: AuthorizationStamp,
    },
    Enroll {
        operation_id: Uuid,
        expected: Option<WorkerIdentity>,
        next: WorkerEnrollment,
        authorization: AuthorizationStamp,
        previous_process: Option<ExclusionStamp>,
    },
    EnrollApi {
        operation_id: Uuid,
        expected: Option<WorkerIdentity>,
        next: ApiEnrollment,
        authorization: AuthorizationStamp,
    },
    BeginGroup {
        operation_id: Uuid,
        generation: Uuid,
        identity: GroupIdentity,
        authorization: AuthorizationStamp,
    },
    RegisterInitialData {
        operation_id: Uuid,
        proof: GroupMembershipStamp,
        replicas: Page<ReplicaMember>,
        authorization: AuthorizationStamp,
    },
    BeginGroupReplacement {
        operation_id: Uuid,
        generation: Uuid,
        expected: GroupRecord,
        authorization: AuthorizationStamp,
    },
    GroupPage {
        operation_id: Uuid,
        generation: Uuid,
        replicas: Page<ReplicaMember>,
    },
    SealGroup {
        operation_id: Uuid,
        generation: Uuid,
        proof: GroupMembershipStamp,
    },
    BeginJoin {
        operation_id: Uuid,
        expected: AuthorityView,
        target: WorkerIdentity,
    },
    BeginRemoval {
        operation_id: Uuid,
        expected: AuthorityView,
        target: WorkerIdentity,
        exclusion: ExclusionStamp,
    },
    /// Queue eligibility only. The completed group fences and surviving RAM
    /// proof are checked separately from physical storage-owner exclusion.
    BeginQueueRemoval {
        operation_id: Uuid,
        expected: AuthorityView,
        target: WorkerIdentity,
        owner_fence: Uuid,
        authorization: AuthorizationStamp,
    },
    ReadyPage {
        operation_id: Uuid,
        transition_id: Uuid,
        members: Page<WorkerIdentity>,
    },
    SealReady {
        operation_id: Uuid,
        transition_id: Uuid,
    },
    TimestampFenced {
        operation_id: Uuid,
        transition_id: Uuid,
        receipt: ReadyFenceReceipt,
    },
    PublishReady {
        operation_id: Uuid,
        transition_id: Uuid,
        proof: QueueCopyStatement,
    },
    BeginOwnerFence {
        operation_id: Uuid,
        expected: ReadyMembership,
        next_owner: WorkerIdentity,
        authorization: AuthorizationStamp,
    },
    BeginColdFence {
        operation_id: Uuid,
        expected: ReadyMembership,
        next_run: Uuid,
        authorization: AuthorizationStamp,
    },
    ExcludedPage {
        operation_id: Uuid,
        transition_id: Uuid,
        workers: Page<ExclusionStamp>,
    },
    GroupFenced {
        operation_id: Uuid,
        receipt: GroupFenceReceipt,
    },
    PublishFence {
        operation_id: Uuid,
        transition_id: Uuid,
    },
    /// NS14 rule 2: a worker cannot serve this replica (storage damage or a
    /// refused pool). Proposed only behind `GroupDamageGate`.
    GroupDamaged {
        operation_id: Uuid,
        worker: WorkerIdentity,
        report: Box<super::GroupDamageReport>,
    },
    /// NS14b rule 3: Control's scheduler claims a damaged replica's group for
    /// one fresh identity and placement. Proposed only behind `GroupDamageGate`.
    GroupDamageReplacementBegin {
        operation_id: Uuid,
        authorization: AuthorizationStamp,
        plan: Box<super::group_damage::GroupDamageReplacementPlan>,
    },
    /// NS14 rule 3: the membership change that removed the damaged replica
    /// committed; NS14b publishes the new placement with it. Proposed only
    /// behind `GroupDamageGate`.
    GroupDamageReplaced {
        operation_id: Uuid,
        authorization: AuthorizationStamp,
        proof: Box<super::GroupDamageReplacement>,
    },
    /// Search members, assignments and models (contract-w2 2.6). `unix_ms`
    /// is the accepting leader's clock, so apply stays deterministic.
    Search {
        operation_id: Uuid,
        unix_ms: u64,
        command: Box<crate::oltp_replication::search_rpc::SearchCommand>,
    },
}
impl ControlCommand {
    pub fn operation_id(&self) -> Uuid {
        match self {
            Self::SystemRecovery { operation_id, .. }
            | Self::DirectCapability { operation_id, .. }
            | Self::ScopedQueue { operation_id, .. }
            | Self::EnsureScopedQueue { operation_id, .. }
            | Self::PoolDrain { operation_id, .. }
            | Self::BeginWorkerDrain { operation_id, .. }
            | Self::BeginWorkerDrainRemoval { operation_id, .. }
            | Self::CloseDataWorker { operation_id, .. }
            | Self::EksWorkerReplacement { operation_id, .. }
            | Self::CompleteEksWorkerReplacement { operation_id, .. }
            | Self::PrepareDataWorker { operation_id, .. }
            | Self::CompleteDataWorker { operation_id, .. }
            | Self::BeginDataWorkerJoin { operation_id, .. }
            | Self::RegisterRestartMount { operation_id, .. }
            | Self::PrepareRestart { operation_id, .. }
            | Self::CompleteRestart { operation_id, .. }
            | Self::ReplicaRetired { operation_id, .. }
            | Self::BeginReplicaMove { operation_id, .. }
            | Self::ArmReplicaMove { operation_id, .. }
            | Self::CancelReplicaMove { operation_id, .. }
            | Self::PublishReplicaMove { operation_id, .. }
            | Self::Growth { operation_id, .. }
            | Self::Initialize { operation_id, .. }
            | Self::Enroll { operation_id, .. }
            | Self::EnrollApi { operation_id, .. }
            | Self::BeginGroup { operation_id, .. }
            | Self::RegisterInitialData { operation_id, .. }
            | Self::BeginGroupReplacement { operation_id, .. }
            | Self::GroupPage { operation_id, .. }
            | Self::SealGroup { operation_id, .. }
            | Self::BeginJoin { operation_id, .. }
            | Self::BeginRemoval { operation_id, .. }
            | Self::BeginQueueRemoval { operation_id, .. }
            | Self::ReadyPage { operation_id, .. }
            | Self::SealReady { operation_id, .. }
            | Self::TimestampFenced { operation_id, .. }
            | Self::PublishReady { operation_id, .. }
            | Self::BeginOwnerFence { operation_id, .. }
            | Self::BeginColdFence { operation_id, .. }
            | Self::ExcludedPage { operation_id, .. }
            | Self::GroupFenced { operation_id, .. }
            | Self::PublishFence { operation_id, .. }
            | Self::GroupDamaged { operation_id, .. }
            | Self::GroupDamageReplacementBegin { operation_id, .. }
            | Self::GroupDamageReplaced { operation_id, .. }
            | Self::Search { operation_id, .. } => *operation_id,
        }
    }
}
impl fmt::Display for ControlCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "control {}", self.operation_id())
    }
}
impl OwnedPayload for ControlCommand {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(match self {
            Self::SystemRecovery { command, .. } => {
                size_of::<super::SystemRecoveryCommand>() + command.heap_bytes()
            }
            Self::ScopedQueue { action, .. } => {
                size_of::<ScopedQueueOperation>() + action.heap_bytes()
            }
            Self::EnsureScopedQueue { expected, .. } => {
                size_of::<GroupRecord>() + expected.identity.database.capacity_bytes()
            }
            Self::PoolDrain { command, .. } => command.heap_bytes(),
            Self::BeginWorkerDrain { intent, .. } => intent.heap_bytes(),
            Self::CloseDataWorker { proof, .. } => proof.heap_bytes(),
            Self::EksWorkerReplacement { request, .. } => size_of::<super::SignedEksReplacement>() + request.action.heap_bytes(),
            Self::CompleteEksWorkerReplacement { mounted, .. } => size_of::<super::worker_drain::EksMountedReplacement>() + mounted.heap_bytes(),
            Self::PrepareDataWorker { intent, .. } => {
                size_of::<super::DataWorkerIntent>() + intent.heap_bytes()
            }
            Self::CompleteDataWorker { intent, mount, .. } => {
                size_of::<super::DataWorkerIntent>() + intent.heap_bytes() + mount.provider_volume_id.capacity()
            }
            Self::ReplicaRetired { proof, .. } => proof.group.database.capacity_bytes(),
            Self::BeginReplicaMove { plan, .. } => {
                size_of::<super::placement::ReplicaMove>()
                    + plan.request.group.database.capacity_bytes()
                    + plan.expected.identity.database.capacity_bytes()
            }
            Self::PublishReplicaMove { proof, .. } => {
                size_of::<super::placement::ReplicaMovePublication>()
                    + proof.membership.group.database.capacity_bytes()
                    + proof.activation.group.database.capacity_bytes()
            }
            Self::ArmReplicaMove { .. } => size_of::<crate::oltp::mvcc::store::lifecycle::SealedLifecycleGuard>(),
            Self::Growth { command, .. } => command.heap_bytes(),
            Self::Enroll { next, .. } => next.endpoint.len() + next.server_name.len(),
            Self::PrepareRestart { intent, .. } | Self::CompleteRestart { intent, .. } => {
                intent.next.endpoint.capacity_bytes() + intent.next.server_name.capacity_bytes()
            }
            Self::EnrollApi { next, .. } => {
                next.database.len()
                    + next.enrollment.endpoint.len()
                    + next.enrollment.server_name.len()
            }
            Self::BeginGroup { identity, .. } => identity.database.len(),
            Self::RegisterInitialData {
                proof, replicas, ..
            } => {
                proof.group.database.capacity_bytes()
                    + replicas.capacity() * size_of::<ReplicaMember>()
            }
            Self::BeginGroupReplacement { expected, .. } => {
                expected.identity.database.capacity_bytes()
            }
            Self::GroupPage { replicas, .. } => replicas.len() * size_of::<ReplicaMember>(),
            Self::ReadyPage { members, .. } => members.len() * size_of::<WorkerIdentity>(),
            Self::ExcludedPage { workers, .. } => workers.len() * size_of::<ExclusionStamp>(),
            Self::GroupFenced { receipt, .. } => receipt.group.database.len(),
            Self::SealGroup { proof, .. } => proof.group.database.len(),
            Self::GroupDamaged { report, .. } => {
                size_of::<super::GroupDamageReport>() + report.group.database.capacity_bytes()
            }
            Self::GroupDamageReplaced { proof, .. } => {
                size_of::<super::GroupDamageReplacement>() + proof.group.database.capacity_bytes()
            }
            Self::GroupDamageReplacementBegin { plan, .. } => {
                size_of::<super::group_damage::GroupDamageReplacementPlan>()
                    + 2 * plan.group.database.capacity_bytes()
                    + plan.members.capacity() * size_of::<ReplicaMember>()
            }
            Self::Search { command, .. } => super::search::command_bytes(command),
            _ => 0,
        })
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[doc(hidden)]
pub enum Fault {
    NotInitialized,
    Invalid,
    Unauthorized,
    Stale,
    Conflict,
    Busy,
    NotFound,
    Incomplete,
    NotReady,
    /// `data.direct.v1` is active and the worker did not advertise it, so it
    /// cannot become a voter or learner. Emitted only after activation.
    DirectCapabilityRequired,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[doc(hidden)]
pub enum ControlReply {
    SystemReplacementEnabled(AppliedPosition),
    SystemReplacementMounted,
    SystemReplacementAbandoned,
    SystemReplacementDataQueued,
    SystemReplacementPublished(GroupRecord),
    SystemReplacementPrepared(Box<super::ReplacementRecord>),
    DirectCapability(super::DirectCapability),
    ScopedQueueUpdated,
    ScopedQueueTransfer(Box<super::ScopedQueueTransition>),
    ScopedQueue(Box<super::ScopedQueuePlacement>),
    WorkerDrain(Box<super::WorkerDrainRecord>),
    EksReplacement(Box<super::worker_drain::EksReplacementRecord>),
    PoolDrain(Box<super::pool_drain::PoolDrainRecord>),
    RestartMountRegistered(WorkerIdentity),
    RestartPrepared(WorkerIdentity),
    DataWorkerPrepared(WorkerIdentity),
    GrowthConfigured(crate::oltp_deployment::controller::GrowthPolicy),
    GroupRetirement {
        generation: Uuid,
        remaining: u64,
    },
    ReplicaMove(Box<super::placement::ReplicaMove>),
    /// NS14: one replica's damage record.
    GroupDamage(Box<super::GroupDamageRecord>),
    Growth(crate::oltp_deployment::controller::GrowthRecord),
    Initialized {
        ready: ReadyMembership,
    },
    Enrolled(WorkerIdentity),
    GroupBuilding {
        generation: Uuid,
        count: u64,
    },
    GroupRegistered {
        group_id: Uuid,
        count: u64,
        root: Digest,
    },
    JoinBegun {
        ticket: JoinTicket,
        bootstrap: bool,
    },
    ReadyBuilding {
        transition_id: Uuid,
        count: u64,
    },
    ReadySealed {
        transition_id: Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    },
    TimestampFenced {
        transition_id: Uuid,
        next: ReadyMembership,
    },
    Published {
        ready: ReadyMembership,
        owner: Option<WorkerIdentity>,
    },
    FenceBegun {
        transition_id: Uuid,
        next: ReadyMembership,
        required_groups: u64,
    },
    FenceProgress {
        transition_id: Uuid,
        fenced_groups: u64,
        excluded_workers: u64,
    },
    Search(crate::oltp_replication::search_rpc::SearchControlReply),
    Rejected(Fault),
}

pub(super) fn unavailable(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::StorageUnavailable, message, Outcome::Unknown)
}

#[doc(hidden)]
pub use nabedra_oltp_types::authority::ScopedCatalogRead;

/// The source group is bound independently of a physical worker, which can
/// host many logical replicas. A wire caller never receives raw proposal power.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[doc(hidden)]
pub enum ServiceAction {
    Owner,
    ScopedCatalogRead(ScopedCatalogRead),
    ScopedQueueExecute(ScopedCatalogRead),
    ScopedQueueIngress {
        queue_group_id: Uuid,
        queue_generation: Uuid,
        queue_view: AuthorityView,
        owner: WorkerIdentity,
    },
    ScopedQueueReserve {
        queue_group_id: Uuid,
        queue_generation: Uuid,
        queue_view: AuthorityView,
        request_id: Uuid,
    },
    ScopedQueueAdmit {
        queue_group_id: Uuid,
        queue_generation: Uuid,
        queue_view: AuthorityView,
        request_id: Uuid,
        activation: Uuid,
    },
    ScopedQueueRead {
        queue_group_id: Uuid,
        queue_generation: Uuid,
        queue_view: AuthorityView,
        request_id: Uuid,
    },
    ScopedQueueFenceReceipt {
        queue_group_id: Uuid,
        transfer_id: Uuid,
    },
    QueueRetirement,
    ApiIngress,
    StatusProof { source_group: Uuid },
    TimestampProof { source_group: Uuid },
    ParticipantProof { source_group: Uuid },
    ControlFence { transition_id: Uuid },
    Operator,
    /// A Data range leader executing direct commits (T7): Catalog reads for
    /// its table schema and `AllocateDirect` on the Timestamp group. A worker
    /// acting as gateway (T6) also uses it for `Method::Direct` and
    /// `DirectForward` on Data groups. Granted only while `data.direct.v1`
    /// is active, to a worker that advertised it, so no older peer receives
    /// this tag.
    DirectLeader,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct ServiceAuthorization {
    pub ready: ReadyMembership,
    pub owner: Option<WorkerIdentity>,
    pub effective_authority: Authority,
    pub pending_ready: Option<ReadyMembership>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scoped_ingress: Option<ScopedIngressGrant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct ScopedIngressGrant {
    group_id: Uuid,
    table_id: Uuid,
    shard_id: Uuid,
    data_generation: Uuid,
    queue_generation: Uuid,
    view: AuthorityView,
    owner: WorkerIdentity,
    members: [WorkerIdentity; 3],
    unavailable_holder: Option<WorkerIdentity>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimestampPointRoute {
    pub source_group: Uuid,
    pub source_generation: Uuid,
    pub target_generation: Uuid,
    pub authority: Authority,
}

impl ScopedIngressGrant {
    pub(super) fn from_placement(queue: &super::ScopedQueuePlacement) -> Result<Self, ProtocolError> {
        let GroupKind::Data { table_id, shard_id } = queue.group.kind else {
            return Err(stale_service_route());
        };
        Ok(Self {
            group_id: queue.group.group_id,
            table_id,
            shard_id,
            data_generation: queue.data_generation,
            queue_generation: queue.queue_generation,
            view: queue.view,
            owner: queue.owner,
            members: queue.members,
            unavailable_holder: queue.unavailable_holder,
        })
    }

    #[doc(hidden)]
    pub fn placement(self, control: &GroupIdentity) -> Result<super::ScopedQueuePlacement, ProtocolError> {
        let queue = super::ScopedQueuePlacement {
            group: GroupIdentity {
                cluster_id: control.cluster_id,
                database: control.database.clone(),
                group_id: self.group_id,
                kind: GroupKind::Data { table_id: self.table_id, shard_id: self.shard_id },
                group_format_version: GROUP_FORMAT_VERSION,
            },
            data_generation: self.data_generation,
            queue_generation: self.queue_generation,
            view: self.view,
            owner: self.owner,
            members: self.members,
            unavailable_holder: self.unavailable_holder,
        };
        queue.validate(&queue.group)?;
        Ok(queue)
    }
}

pub(crate) fn stale_service_route() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "service receiver placement changed",
        Outcome::NotApplied,
    )
}

pub(crate) fn unsupported_service_route() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::UnsupportedOperation,
        "service receiver placement check unavailable",
        Outcome::NotApplied,
    )
}
