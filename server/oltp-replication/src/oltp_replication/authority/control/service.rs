use super::{
    ControlMachine,
    machine::{self, APIS, BUDGET, GROUPS, STATE, WORKERS},
    model::*,
};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, GroupRead},
    oltp_replication::{
        authority::consensus::{Applied, Consensus},
        multiplex::{AuthenticatedSession, EndpointFuture, Purpose},
        owned::OwnedCommand,
        queue_access::{AuthorityReader, JoinObservation, QueueCopySource, RemovalObservation},
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use nabedra_oltp_types::queue::{QueueAuthority, QueueScope};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;
mod data_worker;
mod direct;
mod group_damage;
mod growth;
mod placement;
mod pool_drain;
mod replacement;
mod restart;
mod scoped_queue;
mod search;
mod system_recovery;
mod worker_drain;

/// Authentication configuration, supplied by the operator rather than a worker
/// payload. Empty pins deny every administration operation.
#[doc(hidden)]
pub struct OperatorPolicy {
    pub policy: Digest,
    pub certificate_pins: BTreeSet<Digest>,
}
/// Created by the hosted manager after local TLS material and physical worker
/// ownership are validated for an explicit local operator bootstrap command.
/// Remote worker input cannot deserialize or construct this capability.
#[doc(hidden)]
pub struct LocalOperatorPermit {
    local: WorkerIdentity,
    shared: Arc<crate::oltp::pool::WorkerSharedResources>,
    _ownership: Arc<dyn Send + Sync>,
}
impl LocalOperatorPermit {
    /// For a local Docker/directory restart, a manager-held lifetime directory
    /// lock excludes the prior process before its database can be reopened.
    /// This is not an EC2/CSI or remote-host exclusion capability.
    pub(in crate::oltp_replication) fn exclude_previous_before_open(
        &self,
        policy: &OperatorPolicy,
        operation_id: Uuid,
        previous: WorkerIdentity,
    ) -> Result<VerifiedExclusion, ProtocolError> {
        if operation_id.is_nil()
            || previous.worker_id != self.local.worker_id
            || previous.incarnation == self.local.incarnation
            || previous.enrollment_revision >= self.local.enrollment_revision
            || !policy
                .certificate_pins
                .contains(&self.local.certificate_sha256)
            || self.shared.worker_id() != Some(self.local.worker_id)
        {
            return Err(unauthorized());
        }
        let evidence = crate::oltp_replication::owned::serialized_digest(&(previous, self.local))?;
        Ok(VerifiedExclusion {
            stamp: ExclusionStamp {
                worker: previous,
                authorization: AuthorizationStamp {
                    authorization_id: operation_id,
                    policy: policy.policy,
                    subject: self.local.certificate_sha256,
                    evidence,
                },
                ownership_fence: evidence,
            },
        })
    }
    pub(in crate::oltp_replication) fn from_local_bootstrap(
        local: WorkerIdentity,
        shared: Arc<crate::oltp::pool::WorkerSharedResources>,
        ownership: Arc<dyn Send + Sync>,
    ) -> Result<Self, ProtocolError> {
        local.validate().map_err(|_| unauthorized())?;
        if shared.worker_id() != Some(local.worker_id) {
            return Err(unauthorized());
        }
        Ok(Self {
            local,
            shared,
            _ownership: ownership,
        })
    }
}
#[doc(hidden)]
pub struct VerifiedExclusion {
    stamp: ExclusionStamp,
}
impl VerifiedExclusion {
    pub(crate) fn stamp(&self) -> &ExclusionStamp {
        &self.stamp
    }
}
impl crate::oltp::pool::mount::PoolExclusionProof for VerifiedExclusion {
    fn excluded_worker(&self) -> WorkerIdentity {
        self.stamp().worker
    }
}
#[doc(hidden)]
pub struct VerifiedMembership {
    stamp: GroupMembershipStamp,
}
impl VerifiedMembership {
    #[doc(hidden)]
    pub fn from_consensus(
        observation: crate::oltp_replication::manager::GroupMembershipObservation,
        generation: Uuid,
        authority: Authority,
    ) -> Result<Self, ProtocolError> {
        if generation.is_nil() || authority.validate().is_err() || observation.replicas().is_empty()
        {
            return Err(unauthorized());
        }
        let mut chain = machine::group_seed();
        let mut last = None;
        for replica in observation.replicas() {
            if last.is_some_and(|old| replica.node_id <= old) {
                return Err(unauthorized());
            }
            last = Some(replica.node_id);
            chain = machine::group_step(chain, replica);
        }
        Ok(Self {
            stamp: GroupMembershipStamp {
                group: observation.group().clone(),
                generation,
                replica_count: observation.replicas().len() as u64,
                replica_root: machine::group_root(observation.replicas().len() as u64, chain),
                applied_at: observation.applied_at(),
                authority,
            },
        })
    }
}
#[doc(hidden)]
pub struct VerifiedQueueCopy {
    statement: QueueCopyStatement,
}
#[doc(hidden)]
pub struct RemovalCopyVerifier {
    statement: QueueCopyStatement,
    ready: crate::oltp_replication::ReadySetHasher,
}
impl RemovalCopyVerifier {
    #[doc(hidden)]
    pub fn observe_local(
        &mut self,
        replica: &impl QueueCopySource,
    ) -> Result<(), ProtocolError> {
        if replica
            .removal_statement(self.statement.ticket)
            .map_err(nabedra_protocol::QueueError::protocol)?
            != self.statement
        {
            return Err(unauthorized());
        }
        self.ready
            .push(replica.local_identity())
            .map_err(|_| unauthorized())
    }
    #[doc(hidden)]
    pub fn observe(
        &mut self,
        observation: RemovalObservation,
    ) -> Result<(), ProtocolError> {
        let (worker, statement, nonce) = observation.into_parts();
        if nonce.is_nil() || statement != self.statement {
            return Err(unauthorized());
        }
        self.ready.push(worker).map_err(|_| unauthorized())
    }
    #[doc(hidden)]
    pub fn finish(
        self,
        source: &impl QueueCopySource,
    ) -> Result<VerifiedQueueCopy, ProtocolError> {
        let (root, count) = source
            .removal_ready_root(self.statement.ticket)
            .map_err(nabedra_protocol::QueueError::protocol)?;
        if !source.removal_frozen(self.statement.ticket)
            || source
                .removal_statement(self.statement.ticket)
                .map_err(nabedra_protocol::QueueError::protocol)?
                != self.statement
            || self.ready.finish() != root
            || self.ready.count() != count
        {
            return Err(unauthorized());
        }
        Ok(VerifiedQueueCopy {
            statement: self.statement,
        })
    }
}
impl VerifiedQueueCopy {
    #[doc(hidden)]
    pub fn begin_removal(
        source: &impl QueueCopySource,
        ticket: JoinTicket,
    ) -> Result<RemovalCopyVerifier, ProtocolError> {
        if source.local_identity() != source.authority().owner || !source.removal_frozen(ticket) {
            return Err(unauthorized());
        }
        Ok(RemovalCopyVerifier {
            statement: source
                .removal_statement(ticket)
                .map_err(nabedra_protocol::QueueError::protocol)?,
            ready: crate::oltp_replication::ReadySetHasher::default(),
        })
    }
    #[doc(hidden)]
    pub fn from_join(
        source: &impl QueueCopySource,
        current: QueueAuthority,
        target: JoinObservation,
    ) -> Result<Self, ProtocolError> {
        let statement = source
            .join_statement(current)
            .map_err(nabedra_protocol::QueueError::protocol)?;
        let (worker, observed, nonce) = target.into_parts();
        if nonce.is_nil()
            || source.local_identity() != statement.source
            || worker != statement.ticket.target
            || statement != observed
        {
            return Err(unauthorized());
        }
        Ok(Self { statement })
    }
    #[doc(hidden)]
    pub fn from_empty_worker(
        ticket: JoinTicket,
        replica: &impl QueueCopySource,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            statement: replica
                .empty_bootstrap_statement(ticket)
                .map_err(nabedra_protocol::QueueError::protocol)?,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct InventoryPage<T> {
    pub revision: u64,
    pub items: Page<T>,
    pub after: Option<Uuid>,
    pub done: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct ReplicaInventoryPage {
    pub group: GroupRecord,
    pub items: Page<ReplicaMember>,
    pub after: Option<u64>,
    pub done: bool,
}

/// This handle owns the private proposal capability. Public worker methods
/// receive AuthorityReader only; they cannot submit raw Control commands.
#[doc(hidden)]
pub struct ControlHandle {
    machine: ControlMachine,
    consensus: Arc<dyn Consensus<ControlCommand, ControlReply>>,
    policy: OperatorPolicy,
    registered_directory:
        std::sync::OnceLock<Arc<crate::oltp_replication::peer_directory::AppliedPeerDirectory>>,
    /// NS14 mixed-version gate for the group damage commands.
    group_damage_gate: std::sync::RwLock<Option<Arc<dyn super::group_damage::GroupDamageGate>>>,
}
impl ControlHandle {
    #[doc(hidden)]
    pub fn new(
        machine: ControlMachine,
        consensus: Arc<dyn Consensus<ControlCommand, ControlReply>>,
        policy: OperatorPolicy,
    ) -> Arc<Self> {
        Arc::new(Self {
            machine,
            consensus,
            policy,
            registered_directory: Default::default(),
            group_damage_gate: std::sync::RwLock::new(None),
        })
    }
    #[doc(hidden)]
    pub fn use_registered_directory(
        &self,
        directory: Arc<crate::oltp_replication::peer_directory::AppliedPeerDirectory>,
    ) -> Result<(), ProtocolError> {
        self.registered_directory
            .set(directory)
            .map_err(|_| unauthorized())
    }
    pub(crate) fn system_placement_changed(&self) -> bool {
        self.registered_directory
            .get()
            .is_some_and(|directory| directory.system_placement_changed())
    }
    pub(crate) fn resources(&self) -> &Arc<crate::oltp::pool::WorkerSharedResources> {
        self.machine.store().resources()
    }
    #[doc(hidden)]
    pub fn identity(&self) -> &GroupIdentity {
        self.machine.store().identity()
    }
    /// The Control log index this worker's replica has applied, from memory,
    /// or None without a running local replica. A change hint only.
    #[doc(hidden)]
    pub fn applied_index(&self) -> Option<u64> {
        self.consensus.applied_index()
    }
    async fn submit(
        &self,
        command: ControlCommand,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let command = OwnedCommand::new_owned(
            command,
            self.machine.store().resources(),
            ByteClass::Progress,
        )?;
        self.consensus.propose(command).await
    }
    fn operator(
        &self,
        session: &AuthenticatedSession,
        evidence: Digest,
        operation_id: Uuid,
    ) -> Result<AuthorizationStamp, ProtocolError> {
        if session.group() != self.identity()
            || session.purpose() != Purpose::Authority
            || session.recovery_only()
            || !self.is_operator(&session.remote())
        {
            return Err(unauthorized());
        }
        Ok(AuthorizationStamp {
            authorization_id: operation_id,
            policy: self.policy.policy,
            subject: session.remote().certificate_sha256,
            evidence,
        })
    }
    fn local_operator(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        evidence: Digest,
    ) -> Result<AuthorizationStamp, ProtocolError> {
        if !Arc::ptr_eq(&permit.shared, self.resources()) || !self.is_operator(&permit.local) {
            return Err(unauthorized());
        }
        Ok(AuthorizationStamp {
            authorization_id: operation_id,
            policy: self.policy.policy,
            subject: permit.local.certificate_sha256,
            evidence,
        })
    }
    #[doc(hidden)]
    pub async fn initialize_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        run_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization =
            self.local_operator(permit, operation_id, machine::hash(run_id.as_bytes()))?;
        self.submit(ControlCommand::Initialize {
            operation_id,
            run_id,
            authorization,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn enroll_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        expected: Option<WorkerIdentity>,
        next: WorkerEnrollment,
        previous_process: Option<VerifiedExclusion>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        if WorkerIdentity::from(&next) != permit.local {
            return Err(unauthorized());
        }
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&next)?,
        )?;
        self.submit(ControlCommand::Enroll {
            operation_id,
            expected,
            next,
            authorization,
            previous_process: previous_process.map(|proof| proof.stamp),
        })
        .await
    }
    #[doc(hidden)]
    pub fn exclude_local_previous(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        previous: WorkerIdentity,
    ) -> Result<VerifiedExclusion, ProtocolError> {
        if previous.worker_id != permit.local.worker_id || previous == permit.local {
            return Err(unauthorized());
        }
        let evidence =
            crate::oltp_replication::owned::serialized_digest(&(previous, permit.local))?;
        let authorization = self.local_operator(permit, operation_id, evidence)?;
        Ok(VerifiedExclusion {
            stamp: ExclusionStamp {
                worker: previous,
                authorization,
                ownership_fence: evidence,
            },
        })
    }
    #[doc(hidden)]
    pub async fn excluded_local_page(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        transition_id: Uuid,
        proofs: Vec<VerifiedExclusion>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(transition_id.as_bytes()),
        )?;
        let workers = Page::new(proofs.into_iter().map(|proof| proof.stamp).collect())
            .map_err(|_| unauthorized())?;
        self.submit(ControlCommand::ExcludedPage {
            operation_id,
            transition_id,
            workers,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn begin_group_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        generation: Uuid,
        identity: GroupIdentity,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&identity)?,
        )?;
        self.submit(ControlCommand::BeginGroup {
            operation_id,
            generation,
            identity,
            authorization,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn register_initial_data_local(
        &self,
        permit: &LocalOperatorPermit,
        generation: Uuid,
        authority: Authority,
        observation: crate::oltp_replication::manager::GroupMembershipObservation,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let replicas = Page::new(observation.replicas().to_vec()).map_err(|_| unauthorized())?;
        let proof = VerifiedMembership::from_consensus(observation, generation, authority)?.stamp;
        let operation_id = nabedra_rt::ids::uuid_v4();
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&proof)?,
        )?;
        self.submit(ControlCommand::RegisterInitialData {
            operation_id,
            proof,
            replicas,
            authorization,
        })
        .await
    }
    pub(crate) async fn begin_group_replacement_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        generation: Uuid,
        expected: GroupRecord,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let evidence = crate::oltp_replication::owned::serialized_digest(&expected)?;
        let authorization = self.local_operator(permit, operation_id, evidence)?;
        self.submit(ControlCommand::BeginGroupReplacement {
            operation_id,
            generation,
            expected,
            authorization,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn group_page_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        generation: Uuid,
        replicas: Page<ReplicaMember>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(permit, operation_id, machine::hash(generation.as_bytes()))?;
        self.submit(ControlCommand::GroupPage {
            operation_id,
            generation,
            replicas,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn seal_group_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        proof: VerifiedMembership,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(proof.stamp.generation.as_bytes()),
        )?;
        self.submit(ControlCommand::SealGroup {
            operation_id,
            generation: proof.stamp.generation,
            proof: proof.stamp,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn begin_join_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        expected: AuthorityView,
        target: WorkerIdentity,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(target.worker_id.as_bytes()),
        )?;
        // Every caller joins its own process, which runs this build and so
        // decodes every direct entry: advertise `data.direct.v1` first, or an
        // active capability refuses the join. Control answers `Unauthorized`
        // (and records nothing) when the identity is not enrolled yet.
        if target == permit.local {
            self.submit(ControlCommand::DirectCapability {
                operation_id: nabedra_rt::ids::uuid_v4(),
                actor: target,
                action: super::DirectCapabilityOperation::Advertise {
                    version: super::DIRECT_VERSION,
                },
                unix_ms: super::direct::wall_ms(),
            })
            .await?;
            for feature in super::CLUSTER_FEATURES.map(|feature| feature.name) {
                let _ = self
                    .submit(ControlCommand::DirectCapability {
                        operation_id: super::feature_operation_id(feature, target),
                        actor: target,
                        action: super::DirectCapabilityOperation::Advertise {
                            version: super::DIRECT_VERSION,
                        },
                        unix_ms: super::direct::wall_ms(),
                    })
                    .await;
            }
        }
        self.submit(ControlCommand::BeginJoin {
            operation_id,
            expected,
            target,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn ready_page_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        transition_id: Uuid,
        members: Page<WorkerIdentity>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(transition_id.as_bytes()),
        )?;
        self.submit(ControlCommand::ReadyPage {
            operation_id,
            transition_id,
            members,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn seal_ready_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        transition_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(transition_id.as_bytes()),
        )?;
        self.submit(ControlCommand::SealReady {
            operation_id,
            transition_id,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn begin_owner_fence_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        expected: ReadyMembership,
        next_owner: WorkerIdentity,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&next_owner)?,
        )?;
        self.submit(ControlCommand::BeginOwnerFence {
            operation_id,
            expected,
            next_owner,
            authorization,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn begin_queue_removal_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        expected: AuthorityView,
        target: WorkerIdentity,
        owner_fence: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&(expected, target, owner_fence))?,
        )?;
        self.submit(ControlCommand::BeginQueueRemoval {
            operation_id,
            expected,
            target,
            owner_fence,
            authorization,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn begin_cold_fence_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        expected: ReadyMembership,
        next_run: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization =
            self.local_operator(permit, operation_id, machine::hash(next_run.as_bytes()))?;
        self.submit(ControlCommand::BeginColdFence {
            operation_id,
            expected,
            next_run,
            authorization,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn record_group_fence(
        &self,
        operation_id: Uuid,
        receipt: GroupFenceReceipt,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.submit(ControlCommand::GroupFenced {
            operation_id,
            receipt,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn publish_fence_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        transition_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(transition_id.as_bytes()),
        )?;
        self.submit(ControlCommand::PublishFence {
            operation_id,
            transition_id,
        })
        .await
    }
    pub(crate) async fn initialize(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        run_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization =
            self.operator(session, machine::hash(run_id.as_bytes()), operation_id)?;
        self.submit(ControlCommand::Initialize {
            operation_id,
            run_id,
            authorization,
        })
        .await
    }
    pub(crate) async fn enroll_api(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        expected: Option<WorkerIdentity>,
        next: ApiEnrollment,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization = self.operator(
            session,
            crate::oltp_replication::owned::serialized_digest(&next)?,
            operation_id,
        )?;
        self.submit(ControlCommand::EnrollApi {
            operation_id,
            expected,
            next,
            authorization,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn enroll_api_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        expected: Option<WorkerIdentity>,
        next: ApiEnrollment,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&next)?,
        )?;
        self.submit(ControlCommand::EnrollApi {
            operation_id,
            expected,
            next,
            authorization,
        })
        .await
    }
    pub(crate) async fn enroll(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        expected: Option<WorkerIdentity>,
        next: WorkerEnrollment,
        previous_process: Option<VerifiedExclusion>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let evidence = crate::oltp_replication::owned::serialized_digest(&next)?;
        let authorization = self.operator(session, evidence, operation_id)?;
        self.submit(ControlCommand::Enroll {
            operation_id,
            expected,
            next,
            authorization,
            previous_process: previous_process.map(|proof| proof.stamp),
        })
        .await
    }
    pub(crate) async fn begin_group(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        generation: Uuid,
        identity: GroupIdentity,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization = self.operator(
            session,
            crate::oltp_replication::owned::serialized_digest(&identity)?,
            operation_id,
        )?;
        self.submit(ControlCommand::BeginGroup {
            operation_id,
            generation,
            identity,
            authorization,
        })
        .await
    }
    pub(crate) async fn group_page(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        generation: Uuid,
        replicas: Page<ReplicaMember>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.operator(session, machine::hash(generation.as_bytes()), operation_id)?;
        self.submit(ControlCommand::GroupPage {
            operation_id,
            generation,
            replicas,
        })
        .await
    }
    pub(crate) async fn seal_group(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        proof: VerifiedMembership,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.operator(
            session,
            machine::hash(proof.stamp.generation.as_bytes()),
            operation_id,
        )?;
        self.submit(ControlCommand::SealGroup {
            operation_id,
            generation: proof.stamp.generation,
            proof: proof.stamp,
        })
        .await
    }
    pub(crate) async fn begin_join(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        expected: AuthorityView,
        target: WorkerIdentity,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.operator(
            session,
            machine::hash(target.worker_id.as_bytes()),
            operation_id,
        )?;
        self.submit(ControlCommand::BeginJoin {
            operation_id,
            expected,
            target,
        })
        .await
    }
    pub(crate) async fn ready_page(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        transition_id: Uuid,
        members: Page<WorkerIdentity>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.operator(
            session,
            machine::hash(transition_id.as_bytes()),
            operation_id,
        )?;
        self.submit(ControlCommand::ReadyPage {
            operation_id,
            transition_id,
            members,
        })
        .await
    }
    pub(crate) async fn seal_ready(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        transition_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.operator(
            session,
            machine::hash(transition_id.as_bytes()),
            operation_id,
        )?;
        self.submit(ControlCommand::SealReady {
            operation_id,
            transition_id,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn timestamp_fenced(
        &self,
        operation_id: Uuid,
        receipt: super::super::timestamp::ReadyFenceReceipt,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        // Called only with the timestamp handle's durable receipt, never a
        // deserialized worker boolean. The apply transition checks every field.
        self.submit(ControlCommand::TimestampFenced {
            operation_id,
            transition_id: receipt.transition_id,
            receipt,
        })
        .await
    }
    #[doc(hidden)]
    pub async fn publish_ready(
        &self,
        operation_id: Uuid,
        proof: VerifiedQueueCopy,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.submit(ControlCommand::PublishReady {
            operation_id,
            transition_id: proof.statement.ticket.ticket_id,
            proof: proof.statement,
        })
        .await
    }

    /// A linearizable read of the local machine. Each call site is counted
    /// as `control.barrier.site.<file>:<line>` (PERF2c).
    #[track_caller]
    fn read(
        &self,
        read: impl FnOnce(&mut GroupRead<'_>) -> Result<ChargedBytes, ProtocolError> + Send + 'static,
    ) -> impl std::future::Future<Output = Result<ChargedBytes, ProtocolError>> + Send + '_ {
        let site = nabedra_resources::stage_timing::site(
            "control.barrier.site.",
            std::panic::Location::caller(),
        );
        async move {
            let _timer = site.timer();
            self.consensus.ensure_linearizable().await?;
            self.machine.store().read(BUDGET, read).await
        }
    }
    #[doc(hidden)]
    pub async fn excluded(
        &self,
        transition_id: Uuid,
        worker: WorkerIdentity,
    ) -> Result<bool, ProtocolError> {
        let raw = self
            .read(move |view| {
                let key = machine::key(
                    &machine::key(b"ct/exclusion/", transition_id),
                    worker.worker_id,
                );
                let record: Option<ExclusionStamp> = load(view, &key)?;
                view.encode(&record.is_some_and(|record| record.worker == worker))
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    #[doc(hidden)]
    pub async fn worker_replaced(
        &self,
        worker: WorkerIdentity,
    ) -> Result<bool, ProtocolError> {
        let raw = self
            .read(move |view| {
                if view
                    .get(&machine::system_recovery::revoked_id(worker.worker_id))?
                    .is_some()
                    || view
                        .get(&machine::system_recovery::revoked_certificate(
                            worker.certificate_sha256,
                        ))?
                        .is_some()
                    || load::<WorkerIdentity>(view, &machine::process_exclusion_key(worker))?
                        == Some(worker)
                {
                    return view.encode(&true);
                }
                let record: Option<WorkerRecord> =
                    load(view, &machine::key(WORKERS, worker.worker_id))?;
                view.encode(&record.is_some_and(|record| {
                    record.enrollment.enrollment_revision > worker.enrollment_revision
                        && record
                            .previous_process
                            .is_some_and(|excluded| excluded.worker == worker)
                }))
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    /// REP6: one page of committed system revocations, the set Control
    /// hosts enforce (`system_trust`). `revision` is the enrollment revision
    /// the page was read at; callers agree on the items only.
    pub(crate) async fn revocations(
        &self,
        after: Option<Uuid>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let state = state(view)?;
            let prefix = machine::system_recovery::REVOKED_WORKERS;
            let mut items = Vec::new();
            let mut cursor = after;
            let progress = view.scan_page(
                prefix,
                after.map(|id| machine::key(prefix, id)).as_deref(),
                crate::oltp_replication::types::MAX_PAGE_ITEMS,
                |key, bytes| {
                    let worker: WorkerIdentity = machine::decode(bytes)?;
                    if key[prefix.len()..] != *worker.worker_id.as_bytes() {
                        return Err(stale());
                    }
                    cursor = Some(worker.worker_id);
                    items.push(worker);
                    Ok(())
                },
            )?;
            view.encode(&InventoryPage {
                revision: state.enrollment_revision,
                items: Page::new(items).map_err(|_| stale())?,
                after: cursor,
                done: progress.exhausted,
            })
        })
        .await
    }
    #[doc(hidden)]
    pub async fn worker(
        &self,
        worker_id: Uuid,
    ) -> Result<Option<WorkerEnrollment>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let record: Option<WorkerRecord> = load(view, &machine::key(WORKERS, worker_id))?;
                view.encode(&record.map(|record| record.enrollment))
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    #[doc(hidden)]
    pub async fn group(&self, group_id: Uuid) -> Result<Option<GroupRecord>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let record: Option<GroupRecord> = load(view, &machine::key(GROUPS, group_id))?;
                view.encode(&record)
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    #[doc(hidden)]
    pub async fn inspect(&self) -> Result<Option<State>, ProtocolError> {
        let bytes = self
            .read(|view| {
                let state: Option<State> = load(view, STATE)?;
                view.encode(&state)
            })
            .await?;
        machine::decode(bytes.as_ref())
    }
    #[doc(hidden)]
    pub async fn published_cold_fence(
        &self,
        authority: Authority,
        prior_run: Option<Uuid>,
    ) -> Result<Option<Uuid>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let state = state(view)?;
                if state.ready.view.authority() != authority || state.pending.is_some() {
                    return Err(stale());
                }
                let published: Option<(Uuid, Authority, Authority)> =
                    load(view, machine::COLD_PUBLICATION)?;
                let published = published.filter(|(id, prior, next)| {
                    !id.is_nil()
                        && prior.run_id != next.run_id
                        && prior.owner_term.checked_add(1) == Some(next.owner_term)
                        && next.run_id == authority.run_id
                        && next.owner_term <= authority.owner_term
                });
                if let Some(prior) = prior_run.filter(|_| published.is_some()) {
                    // The caller also verifies this pin came from an earlier applied
                    // allocator epoch. Used runs cannot be resurrected by Control.
                    if prior == authority.run_id
                        || load::<bool>(view, &machine::key(machine::RUNS, prior))? != Some(true)
                    {
                        return view.encode(&None::<Uuid>);
                    }
                }
                view.encode(&published.map(|(transition_id, _, _)| transition_id))
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    pub(crate) async fn apis(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let state = state(view)?;
            if state.enrollment_revision != revision {
                return Err(stale());
            }
            let mut items = Vec::new();
            let mut cursor = after;
            let mut bytes = 4096usize;
            let mut skipped = false;
            let progress = view.scan_page(
                APIS,
                after.map(|id| machine::key(APIS, id)).as_deref(),
                128,
                |key, raw| {
                    if skipped || bytes.checked_add(raw.len()).is_none_or(|n| n > 60 << 10) {
                        skipped = true;
                        return Ok(());
                    }
                    bytes += raw.len();
                    let api: ApiEnrollment = machine::decode(raw)?;
                    cursor = Some(Uuid::from_slice(&key[APIS.len()..]).map_err(|_| stale())?);
                    items.push(api);
                    Ok(())
                },
            )?;
            view.encode(&InventoryPage {
                revision,
                items: Page::new(items).map_err(|_| stale())?,
                after: cursor,
                done: progress.exhausted && !skipped,
            })
        })
        .await
    }
    #[doc(hidden)]
    pub async fn workers(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let state = state(view)?;
            if state.enrollment_revision != revision {
                return Err(stale());
            }
            let mut items = Vec::new();
            let mut cursor = after;
            let mut page_bytes = 4096usize;
            let mut skipped = false;
            let progress = view.scan_page(
                WORKERS,
                after.map(|id| machine::key(WORKERS, id)).as_deref(),
                128,
                |key, bytes| {
                    if skipped
                        || page_bytes
                            .checked_add(bytes.len())
                            .is_none_or(|n| n > 60 << 10)
                    {
                        skipped = true;
                        return Ok(());
                    }
                    page_bytes += bytes.len();
                    let worker: WorkerRecord = machine::decode(bytes)?;
                    cursor = Some(Uuid::from_slice(&key[WORKERS.len()..]).map_err(|_| stale())?);
                    items.push(worker.enrollment);
                    Ok(())
                },
            )?;
            view.encode(&InventoryPage {
                revision,
                items: Page::new(items).map_err(|_| stale())?,
                after: cursor,
                done: progress.exhausted && !skipped,
            })
        })
        .await
    }
    #[doc(hidden)]
    pub async fn groups(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let state = state(view)?;
            if state.group_inventory_revision != revision {
                return Err(stale());
            }
            let mut items = Vec::new();
            let mut cursor = after;
            let mut page_bytes = 4096usize;
            let mut skipped = false;
            let key = after.map(|id| machine::key(GROUPS, id));
            let progress = view.scan_page(GROUPS, key.as_deref(), 128, |_, bytes| {
                if skipped
                    || page_bytes
                        .checked_add(bytes.len())
                        .is_none_or(|n| n > 60 << 10)
                {
                    skipped = true;
                    return Ok(());
                }
                page_bytes += bytes.len();
                let group: GroupRecord = machine::decode(bytes)?;
                cursor = Some(group.identity.group_id);
                items.push(group);
                Ok(())
            })?;
            view.encode(&InventoryPage {
                revision,
                items: Page::new(items).map_err(|_| stale())?,
                after: cursor,
                done: progress.exhausted && !skipped,
            })
        })
        .await
    }
    #[doc(hidden)]
    pub async fn group_members(
        &self,
        expected: GroupRecord,
        after: Option<u64>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            if load::<GroupRecord>(view, &machine::key(GROUPS, expected.identity.group_id))?
                != Some(expected.clone())
            {
                return Err(stale());
            }
            let prefix = machine::member_prefix(expected.generation);
            let key = after.map(|node| machine::member_key(expected.generation, node));
            let mut items = Vec::new();
            let mut cursor = after;
            let progress = view.scan_page(&prefix, key.as_deref(), 128, |_, bytes| {
                let member: ReplicaMember = machine::decode(bytes)?;
                cursor = Some(member.node_id);
                items.push(member);
                Ok(())
            })?;
            view.encode(&ReplicaInventoryPage {
                group: expected,
                items: Page::new(items).map_err(|_| stale())?,
                after: cursor,
                done: progress.exhausted,
            })
        })
        .await
    }
    #[doc(hidden)]
    pub async fn ready_page_read(
        &self,
        expected: ReadyMembership,
        after: Option<Uuid>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let state = state(view)?;
            if state.ready != expected {
                return Err(stale());
            }
            let prefix = machine::ready_prefix(state.ready_generation);
            let cursor_key = after.map(|id| machine::ready_key(state.ready_generation, id));
            let mut items = Vec::new();
            let mut cursor = after;
            let progress = view.scan_page(&prefix, cursor_key.as_deref(), 128, |_, bytes| {
                let member: WorkerIdentity = machine::decode(bytes)?;
                cursor = Some(member.worker_id);
                items.push(member);
                Ok(())
            })?;
            view.encode(&InventoryPage {
                revision: expected.view.membership_revision,
                items: Page::new(items).map_err(|_| stale())?,
                after: cursor,
                done: progress.exhausted,
            })
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
        self.service_role_inner(scope, remote, action, target, None)
            .await
    }
    pub(super) async fn service_role_at_replica(
        &self,
        scope: &QueueScope,
        remote: WorkerIdentity,
        target: GroupIdentity,
        receiver: WorkerIdentity,
        node_id: u64,
        generation: Uuid,
    ) -> Result<ServiceAuthorization, ProtocolError> {
        if node_id == 0 || generation.is_nil() || target.kind == GroupKind::Control {
            return Err(unauthorized());
        }
        self.service_role_inner(
            scope,
            remote,
            ServiceAction::Owner,
            target,
            Some((receiver, node_id, generation)),
        )
        .await
    }

    pub(super) async fn timestamp_point_route(
        &self,
        remote: WorkerIdentity,
        receiver: WorkerIdentity,
        target: GroupIdentity,
        node_id: u64,
        route: TimestampPointRoute,
        decision_source: Option<(ServiceAction, Digest)>,
    ) -> Result<ChargedBytes, ProtocolError> {
        let control = self.identity();
        let target_kind = match decision_source {
            None => matches!(
                target.kind,
                GroupKind::Data { .. } | GroupKind::Status { .. }
            ),
            Some((ServiceAction::StatusProof { .. }, _)) => {
                matches!(target.kind, GroupKind::Timestamps | GroupKind::Data { .. })
            }
            Some((ServiceAction::ParticipantProof { .. }, _)) => {
                target.kind == GroupKind::Timestamps
            }
            _ => false,
        };
        if target.cluster_id != control.cluster_id
            || target.database != control.database
            || target.validate().is_err()
            || !target_kind
            || node_id == 0
            || route.source_group.is_nil()
            || route.source_generation.is_nil()
            || route.target_generation.is_nil()
            || route.authority.validate().is_err()
        {
            return Err(unauthorized());
        }
        self.read(move |view| {
            let state = state(view)?;
            if state.ready.view.authority() != route.authority
                || state.pending.is_some()
                || state.restart.is_some()
            {
                return Err(stale_service_route());
            }
            let source: GroupRecord =
                load(view, &machine::key(GROUPS, route.source_group))?.ok_or_else(unauthorized)?;
            let permitted = match decision_source {
                None => {
                    source.identity.kind == GroupKind::Timestamps
                        || matches!(target.kind, GroupKind::Status { .. })
                            && matches!(source.identity.kind, GroupKind::Data { .. })
                }
                Some((action, digest)) => {
                    matches!(
                        (action, &source.identity.kind),
                        (ServiceAction::StatusProof { source_group }, GroupKind::Status { .. })
                        | (ServiceAction::ParticipantProof { source_group }, GroupKind::Data { .. })
                            if source_group == route.source_group
                    ) && crate::oltp_replication::owned::serialized_digest(&source.identity)?
                        == digest
                }
            };
            if !permitted
                || source.identity.group_id != route.source_group
                || source.identity.cluster_id != target.cluster_id
                || source.identity.database != target.database
                || source.replica_count == 0
                || source.identity.validate().is_err()
            {
                return Err(unauthorized());
            }
            if source.generation != route.source_generation {
                return Err(stale_service_route());
            }
            let issuer: ReplicaMember = load(
                view,
                &machine::member_worker_key(source.generation, remote.worker_id),
            )?
            .ok_or_else(unauthorized)?;
            if issuer.validate().is_err() || issuer.worker_id != remote.worker_id {
                return Err(unauthorized());
            }
            for identity in [remote, receiver] {
                let worker: WorkerRecord = load(view, &machine::key(WORKERS, identity.worker_id))?
                    .ok_or_else(unauthorized)?;
                if WorkerIdentity::from(&worker.enrollment) != identity {
                    return Err(unauthorized());
                }
            }
            let served: GroupRecord = load(view, &machine::key(GROUPS, target.group_id))?
                .filter(|record: &GroupRecord| {
                    record.identity == target
                        && record.generation == route.target_generation
                        && record.replica_count != 0
                })
                .ok_or_else(stale_service_route)?;
            let member: ReplicaMember =
                load(view, &machine::member_key(served.generation, node_id))?
                    .ok_or_else(stale_service_route)?;
            if member.validate().is_err()
                || member.node_id != node_id
                || member.worker_id != receiver.worker_id
            {
                return Err(stale_service_route());
            }
            view.encode(&())
        })
        .await
    }
    async fn service_role_inner(
        &self,
        scope: &QueueScope,
        remote: WorkerIdentity,
        action: ServiceAction,
        target: GroupIdentity,
        placement: Option<(WorkerIdentity, u64, Uuid)>,
    ) -> Result<ServiceAuthorization, ProtocolError> {
        if scope.cluster_id != self.identity().cluster_id
            || scope.database != self.identity().database
            || target.cluster_id != scope.cluster_id
            || target.database != scope.database
            || target.validate().is_err()
        {
            return Err(unauthorized());
        }
        let control = self.identity().clone();
        let database = scope.database.clone();
        let operator = self.is_operator(&remote);
        let raw = self
            .read(move |view| {
                let state = state(view)?;
                if matches!(
                    action,
                    ServiceAction::ApiIngress | ServiceAction::ScopedQueueIngress { .. }
                ) {
                    let api: ApiEnrollment = load(view, &machine::key(APIS, remote.worker_id))?
                        .ok_or_else(unauthorized)?;
                    // The Data endpoint binds the exact hosted group. For
                    // direct ingress, Control only proves the API's current
                    // enrollment in the database and direct activation.
                    let direct = action == ServiceAction::ApiIngress
                        && matches!(target.kind, GroupKind::Data { .. })
                        && view.get(machine::direct::activation_key())?.is_some();
                    if WorkerIdentity::from(api.transport()) != remote
                        || api.database != database
                        || (target != control && !direct)
                    {
                        return Err(unauthorized());
                    }
                    let (owner, scoped_ingress) = if let ServiceAction::ScopedQueueIngress {
                        queue_group_id,
                        queue_generation,
                        queue_view,
                        owner,
                    } = action
                    {
                        let advertised = load::<WorkerIdentity>(
                            view,
                            &machine::scoped_queue::capability::reader_key(remote),
                        )?;
                        if advertised != Some(remote) {
                            return Err(unauthorized());
                        }
                        let queue = scoped_queue::current_placement(
                            view,
                            &control,
                            &state,
                            queue_group_id,
                        )?
                        .ok_or_else(stale_service_route)?;
                        if queue.queue_generation != queue_generation
                            || queue.view != queue_view
                            || queue.owner != owner
                        {
                            return Err(stale_service_route());
                        }
                        (
                            Some(queue.owner),
                            Some(ScopedIngressGrant::from_placement(&queue)?),
                        )
                    } else {
                        if !direct
                            && (state.owner.is_none()
                                || state.ready.ready_count == 0
                                || state.invalid_ready != 0
                                || state.pending.is_some())
                        {
                            return Err(stale());
                        }
                        (state.owner, None)
                    };
                    return view.encode(&ServiceAuthorization {
                        ready: state.ready,
                        owner,
                        effective_authority: state.ready.view.authority(),
                        pending_ready: None,
                        scoped_ingress,
                    });
                }
                let worker: WorkerRecord = load(view, &machine::key(WORKERS, remote.worker_id))?
                    .ok_or_else(unauthorized)?;
                if WorkerIdentity::from(&worker.enrollment) != remote {
                    return Err(unauthorized());
                }
                let target_record: Option<GroupRecord> =
                    load(view, &machine::key(GROUPS, target.group_id))?;
                if placement.is_none()
                    && action != ServiceAction::Operator
                    && target_record
                        .as_ref()
                        .is_none_or(|record| record.identity != target)
                {
                    return Err(unauthorized());
                }
                let mut grant = ServiceAuthorization {
                    ready: state.ready,
                    owner: state.owner,
                    effective_authority: state.ready.view.authority(),
                    pending_ready: None,
                    scoped_ingress: None,
                };
                let source = match action {
                    ServiceAction::ApiIngress | ServiceAction::ScopedQueueIngress { .. } => {
                        return Err(unauthorized());
                    }
                    ServiceAction::DirectLeader => {
                        let advertised =
                            load::<WorkerIdentity>(view, &machine::direct::reader_key(remote))?;
                        if view.get(machine::direct::activation_key())?.is_none()
                            || advertised != Some(remote)
                            || !matches!(
                                target.kind,
                                GroupKind::Catalog | GroupKind::Timestamps | GroupKind::Data { .. }
                            )
                        {
                            return Err(unauthorized());
                        }
                        None
                    }
                    ServiceAction::Owner => {
                        if state.owner != Some(remote) {
                            return Err(unauthorized());
                        }
                        if state.ready.ready_count == 0
                            || state.invalid_ready != 0
                            || state.pending.is_some()
                        {
                            return Err(stale());
                        }
                        None
                    }
                    ServiceAction::ScopedCatalogRead(read)
                    | ServiceAction::ScopedQueueExecute(read) => {
                        let execute = matches!(action, ServiceAction::ScopedQueueExecute(_));
                        if read.request.request_id.is_nil()
                            || read.request.run_id != read.queue_view.run_id
                            || read.table_id.is_nil()
                        {
                            return Err(unauthorized());
                        }
                        let queue = scoped_queue::current_placement(
                            view,
                            &control,
                            &state,
                            read.queue_group_id,
                        )?
                        .ok_or_else(unauthorized)?;
                        let target_allowed = if execute {
                            matches!(
                                target.kind,
                                GroupKind::Catalog
                                    | GroupKind::Timestamps
                                    | GroupKind::Status { .. }
                            ) || target == queue.group
                        } else {
                            target.kind == GroupKind::Catalog
                        };
                        if queue.data_generation != read.data_generation
                            || queue.queue_generation != read.queue_generation
                            || queue.view != read.queue_view
                            || !queue.has_holder(remote)
                            || execute && queue.owner != remote
                            || !target_allowed
                            || !matches!(queue.group.kind, GroupKind::Data { table_id, .. }
                                if table_id == read.table_id)
                        {
                            return Err(unauthorized());
                        }
                        grant.owner = Some(queue.owner);
                        if !execute {
                            grant.effective_authority = queue.view.authority();
                        }
                        None
                    }
                    ServiceAction::ScopedQueueReserve {
                        queue_group_id,
                        queue_generation,
                        queue_view,
                        request_id,
                    }
                    | ServiceAction::ScopedQueueAdmit {
                        queue_group_id,
                        queue_generation,
                        queue_view,
                        request_id,
                        ..
                    }
                    | ServiceAction::ScopedQueueRead {
                        queue_group_id,
                        queue_generation,
                        queue_view,
                        request_id,
                    } => {
                        let owner_only = !matches!(action, ServiceAction::ScopedQueueRead { .. });
                        if request_id.is_nil()
                            || !(matches!(target.kind, GroupKind::Status { .. })
                                || target.kind == GroupKind::Timestamps)
                        {
                            return Err(unauthorized());
                        }
                        if let ServiceAction::ScopedQueueAdmit { activation, .. } = action {
                            use machine::scoped_queue::capability::Admission;
                            let admission = load(view, Admission::key())?;
                            let gate = Admission::capability(admission, &state);
                            if !matches!(target.kind, GroupKind::Status { .. })
                                || gate.activation != Some(activation)
                            {
                                return Err(unauthorized());
                            }
                        }
                        let queue = scoped_queue::current_placement(
                            view,
                            &control,
                            &state,
                            queue_group_id,
                        )?
                        .ok_or_else(unauthorized)?;
                        if queue.queue_generation != queue_generation
                            || queue.view != queue_view
                            || !queue.has_holder(remote)
                            || owner_only && queue.owner != remote
                        {
                            return Err(unauthorized());
                        }
                        grant.owner = Some(queue.owner);
                        None
                    }
                    ServiceAction::ScopedQueueFenceReceipt {
                        queue_group_id,
                        transfer_id,
                    } => {
                        use machine::scoped_queue::transfer_key;
                        if state.pending.is_some()
                            || state.restart.is_some()
                            || !load::<super::ScopedQueueTransition>(
                                view,
                                &transfer_key(queue_group_id),
                            )?
                            .is_some_and(|transfer| {
                                transfer.id == transfer_id
                                    && transfer.next.view.run_id == state.ready.view.run_id
                                    && (matches!(
                                        target.kind,
                                        GroupKind::Status { .. }
                                            | GroupKind::Timestamps
                                            | GroupKind::Catalog
                                    ) || target == transfer.next.group)
                            })
                        {
                            return Err(unauthorized());
                        }
                        Some((control.group_id, 3))
                    }
                    ServiceAction::QueueRetirement => {
                        let member: Option<WorkerIdentity> = load(
                            view,
                            &machine::ready_key(state.ready_generation, remote.worker_id),
                        )?;
                        if !matches!(target.kind, GroupKind::Status { .. })
                            || state.owner.is_none()
                            || state.invalid_ready != 0
                            || matches!(state.pending, Some(Pending::Fence(_)))
                            || member != Some(remote)
                        {
                            return Err(unauthorized());
                        }
                        None
                    }
                    ServiceAction::StatusProof { source_group } => Some((source_group, 0)),
                    ServiceAction::TimestampProof { source_group } => Some((source_group, 1)),
                    ServiceAction::ParticipantProof { source_group } => Some((source_group, 2)),
                    ServiceAction::ControlFence { transition_id } => {
                        match &state.pending {
                            Some(Pending::Fence(pending))
                                if pending.transition_id == transition_id =>
                            {
                                grant.effective_authority = pending.next.view.authority();
                                grant.pending_ready = Some(pending.next);
                            }
                            Some(Pending::Ready(pending))
                                if pending.transition_id == transition_id
                                    && target.kind == GroupKind::Timestamps =>
                            {
                                let next = pending.next.ok_or_else(unauthorized)?;
                                grant.effective_authority = next.view.authority();
                                grant.pending_ready = Some(next);
                            }
                            _ => return Err(unauthorized()),
                        }
                        Some((control.group_id, 3))
                    }
                    ServiceAction::Operator => {
                        if !operator
                            || view
                                .get(&machine::key(
                                    machine::data_worker::REGISTERED,
                                    remote.worker_id,
                                ))?
                                .is_some()
                        {
                            return Err(unauthorized());
                        }
                        None
                    }
                };
                if let Some((source, kind)) = source {
                    let record: GroupRecord =
                        load(view, &machine::key(GROUPS, source))?.ok_or_else(unauthorized)?;
                    let permitted = match kind {
                        0 => matches!(record.identity.kind, GroupKind::Status { .. }),
                        1 => record.identity.kind == GroupKind::Timestamps,
                        2 => matches!(
                            record.identity.kind,
                            GroupKind::Catalog | GroupKind::Data { .. }
                        ),
                        _ => record.identity == control,
                    };
                    if !permitted
                        || record.replica_count == 0
                        || load::<ReplicaMember>(
                            view,
                            &machine::member_worker_key(record.generation, remote.worker_id),
                        )?
                        .is_none()
                    {
                        return Err(unauthorized());
                    }
                }
                if let Some((receiver, node_id, generation)) = placement {
                    let record = target_record
                        .as_ref()
                        .filter(|record| {
                            record.identity == target
                                && record.generation == generation
                                && record.replica_count != 0
                        })
                        .ok_or_else(stale_service_route)?;
                    let member: ReplicaMember =
                        load(view, &machine::member_key(record.generation, node_id))?
                            .ok_or_else(stale_service_route)?;
                    let enrolled: WorkerRecord =
                        load(view, &machine::key(WORKERS, receiver.worker_id))?
                            .ok_or_else(stale_service_route)?;
                    if member.validate().is_err()
                        || member.node_id != node_id
                        || member.worker_id != receiver.worker_id
                        || WorkerIdentity::from(&enrolled.enrollment) != receiver
                    {
                        return Err(stale_service_route());
                    }
                }
                view.encode(&grant)
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    async fn authority(
        &self,
        scope: &QueueScope,
        reader: Option<(WorkerIdentity, Uuid)>,
        admitting: bool,
    ) -> Result<QueueAuthority, ProtocolError> {
        if scope.cluster_id != self.identity().cluster_id
            || scope.database != self.identity().database
        {
            return Err(unauthorized());
        }
        let bytes = self
            .read(move |view| {
                let state = state(view)?;
                if admitting && matches!(state.pending, Some(Pending::Ready(ref pending)) if pending.change == ReadyChange::Remove) {
                    return Err(ProtocolError::new(
                        ErrorCode::StorageUnavailable,
                        "worker removal is fencing ready admission",
                        Outcome::Unknown,
                    ));
                }
                if admitting && state.restart.is_some() {
                    return Err(ProtocolError::new(
                        ErrorCode::StorageUnavailable,
                        "retained worker is joining the current queue",
                        Outcome::Unknown,
                    ));
                }
                let owner = state.owner.ok_or_else(stale)?;
                if state.ready.ready_count == 0
                    || state.invalid_ready != 0
                    || matches!(state.pending, Some(Pending::Fence(_)))
                {
                    return Err(stale());
                }
                if let Some((remote, group_id)) = reader {
                    let worker: WorkerRecord =
                        load(view, &machine::key(WORKERS, remote.worker_id))?
                            .ok_or_else(unauthorized)?;
                    let group: GroupRecord =
                        load(view, &machine::key(GROUPS, group_id))?.ok_or_else(unauthorized)?;
                    if WorkerIdentity::from(&worker.enrollment) != remote
                        || group.identity.kind != GroupKind::Timestamps
                        || group.replica_count == 0
                        || load::<ReplicaMember>(
                            view,
                            &machine::member_worker_key(group.generation, remote.worker_id),
                        )?
                        .is_none()
                    {
                        return Err(unauthorized());
                    }
                }
                view.encode(&(state.ready, owner))
            })
            .await?;
        let (ready, owner) = machine::decode(bytes.as_ref())?;
        Ok(QueueAuthority { ready, owner })
    }

    async fn queue_removal_view(
        &self,
        scope: &QueueScope,
        owner: WorkerIdentity,
        ticket: JoinTicket,
    ) -> Result<QueueAuthority, ProtocolError> {
        if scope.cluster_id != self.identity().cluster_id
            || scope.database != self.identity().database
        {
            return Err(unauthorized());
        }
        let bytes = self
            .read(move |view| {
                let state = state(view)?;
                if state.owner != Some(owner)
                    || state.ready != ticket.source_ready
                    || state.invalid_ready != 0
                    || !matches!(state.pending, Some(Pending::Ready(pending))
                    if pending.change == ReadyChange::Remove
                        && pending.phase == ReadyPhase::AwaitingQueueProof
                        && pending.transition_id == ticket.ticket_id
                        && pending.target == ticket.target
                        && pending.expected == ticket.source_ready)
                {
                    return Err(stale());
                }
                view.encode(&QueueAuthority {
                    ready: state.ready,
                    owner,
                })
            })
            .await?;
        machine::decode(bytes.as_ref())
    }
}
/// Paces a polling loop's linearizable Control reads (PERF2c). After a pass
/// that found nothing to do, the next pass is due only once this worker's
/// Control replica has applied a new entry, the caller's `key` changed, or
/// `max_idle` passed. Every pass that runs still reads through a ReadIndex
/// barrier: the gate only skips passes, it never serves a read. A change
/// committed elsewhere reaches the local replica within a replication or
/// heartbeat round, and `max_idle` bounds the delay when it cannot (a
/// lagging or removed voter). Without a local replica every pass is due.
#[doc(hidden)]
pub struct IdlePasses<K> {
    quiet: Option<(u64, K, tokio::time::Instant)>,
    max_idle: std::time::Duration,
}
impl<K: PartialEq> IdlePasses<K> {
    #[doc(hidden)]
    pub fn new(max_idle: std::time::Duration) -> Self {
        Self {
            quiet: None,
            max_idle,
        }
    }
    #[doc(hidden)]
    pub fn due(&self, applied: Option<u64>, key: &K, now: tokio::time::Instant) -> bool {
        match (&self.quiet, applied) {
            (Some((index, quiet_key, since)), Some(applied)) => {
                applied != *index
                    || key != quiet_key
                    || now.saturating_duration_since(*since) >= self.max_idle
            }
            _ => true,
        }
    }
    /// Records the outcome of a pass that started at `started`, when the
    /// replica had applied `applied` and the caller's key was `key`. Only a
    /// pass that found nothing to do (`quiet`) lets later passes be skipped.
    #[doc(hidden)]
    pub fn record(
        &mut self,
        quiet: bool,
        applied: Option<u64>,
        key: K,
        started: tokio::time::Instant,
    ) {
        self.quiet = match (quiet, applied) {
            (true, Some(applied)) => Some((applied, key, started)),
            _ => None,
        };
    }
}
impl AuthorityReader for ControlHandle {
    fn current<'a>(&'a self, scope: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(scope, None, false))
    }
    fn removal_view<'a>(
        &'a self,
        scope: &'a QueueScope,
        owner: WorkerIdentity,
        ticket: JoinTicket,
    ) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.queue_removal_view(scope, owner, ticket))
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
        Box::pin(self.authority(scope, Some((remote, timestamps_group_id)), false))
    }
}
pub(super) fn load<T: serde::de::DeserializeOwned>(
    view: &mut GroupRead<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    view.get(key)?
        .map(|v| machine::decode(v.as_ref()))
        .transpose()
}
fn state(view: &mut GroupRead<'_>) -> Result<State, ProtocolError> {
    load(view, STATE)?.ok_or_else(stale)
}
fn stale() -> ProtocolError {
    unavailable("Control authority or inventory is not ready")
}
fn unauthorized() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "Control role is not authorized",
        Outcome::NotApplied,
    )
}

#[cfg(test)]
mod idle_passes_tests {
    use super::IdlePasses;
    use std::time::Duration;

    /// PERF2c: a quiet pass lets later passes be skipped only while the local
    /// Control replica applied nothing new, the key is unchanged and less than
    /// `max_idle` passed. A busy or failed pass, or no local replica, keeps
    /// every pass due.
    #[test]
    fn perf2c_idle_passes_skip_only_quiet_unchanged_passes() {
        let start = nabedra_rt::time::now();
        let mut idle = IdlePasses::new(Duration::from_secs(2));
        // the first pass is due
        assert!(idle.due(Some(7), &1, start));
        idle.record(true, Some(7), 1, start);
        assert!(!idle.due(Some(7), &1, start + Duration::from_millis(250)));
        assert!(!idle.due(Some(7), &1, start + Duration::from_millis(1999)));
        // max idle
        assert!(idle.due(Some(7), &1, start + Duration::from_secs(2)));
        // new entry
        assert!(idle.due(Some(8), &1, start + Duration::from_millis(250)));
        // key changed
        assert!(idle.due(Some(7), &2, start + Duration::from_millis(250)));
        // no replica
        assert!(idle.due(None, &1, start + Duration::from_millis(250)));

        idle.record(false, Some(7), 1, start);
        // busy pass
        assert!(idle.due(Some(7), &1, start + Duration::from_millis(250)));
        idle.record(true, None, 1, start);
        // no replica
        assert!(idle.due(Some(7), &1, start + Duration::from_millis(250)));
    }
}
