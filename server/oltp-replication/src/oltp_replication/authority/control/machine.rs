use super::model::*;
use crate::{
    oltp::pool::{
        ApplyMeta, ChargedBytes, GroupStore, GroupTransaction, TransactionBudget, Transition,
    },
    oltp_replication::{ReadySetHasher, owned::OwnedCommand, types::*},
    protocol::ProtocolError,
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use uuid::Uuid;
pub(super) mod data_worker;
pub(super) mod direct;
pub(super) mod group_damage;
pub(super) mod replacement;
pub(super) mod restart;
pub(super) mod placement;
pub(super) mod pool_drain;
pub(super) mod scoped_queue;
pub(super) mod search;
pub(super) mod system_recovery;
mod system_trust;
pub(super) mod worker_drain;

pub(super) const STATE: &[u8] = b"ct/state";
pub(super) const APIS: &[u8] = b"ct/api/";
pub(super) const WORKERS: &[u8] = b"ct/worker/";
pub(super) const GROUPS: &[u8] = b"ct/group/";
pub(super) const RUNS: &[u8] = b"ct/used-run/";
pub(super) const COLD_PUBLICATION: &[u8] = b"ct/published-cold-fence";
pub(super) const BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 2 << 20,
    batch_bytes: 2 << 20,
    output_bytes: 256 << 10,
};
#[derive(Clone)]
#[doc(hidden)]
pub struct ControlMachine {
    store: GroupStore,
    replacement_trust: Option<system_trust::ReplacementTrust>,
    /// GC2/CAP1 mixed-version tests: apply as a build that knows only the
    /// first this many cluster features.
    #[cfg(test)]
    known_features: usize,
}
impl ControlMachine {
    #[doc(hidden)]
    pub fn new(store: GroupStore) -> Result<Self, ProtocolError> {
        if store.identity().kind != GroupKind::Control {
            return Err(unavailable("wrong Control group kind"));
        }
        Ok(Self {
            store,
            replacement_trust: None,
            #[cfg(test)]
            known_features: super::direct::CLUSTER_FEATURES.len(),
        })
    }
    /// GC2: this replica applies every entry as a pre-feature build would.
    #[cfg(test)]
    pub(crate) fn legacy_v1_for_test(self) -> Self {
        self.known_features_for_test(0)
    }
    /// CAP1: this replica applies every entry as a build that knows only the
    /// first `count` registered cluster features would.
    #[cfg(test)]
    pub(crate) fn known_features_for_test(mut self, count: usize) -> Self {
        self.known_features = count;
        self
    }
    #[doc(hidden)]
    pub fn store(&self) -> &GroupStore {
        &self.store
    }
    pub(crate) async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<ControlCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        command.ensure_charged()?;
        let scope = self.store.identity().clone();
        let position = meta.position;
        let digest = meta.command_digest;
        let payload = command.payload()?;
        let refresh = matches!(
            payload,
            ControlCommand::SystemRecovery { .. }
                | ControlCommand::Enroll { .. }
                | ControlCommand::CompleteRestart { .. }
                | ControlCommand::PublishFence { .. }
                | ControlCommand::Search { .. }
        );
        let mount = match payload {
            ControlCommand::SystemRecovery { command, .. } => match command.as_ref() {
                super::SystemRecoveryCommand::Mount { proof, .. } => Some(proof.id),
                _ => None,
            },
            _ => None,
        };
        let directory = self
            .replacement_trust
            .as_ref()
            .map(|trust| trust.directory.as_ref());
        let reload = directory
            .filter(|_| mount.is_some())
            .map(|directory| directory.reloading_revocations());
        let revoked = reload
            .as_ref()
            .map(|_| std::sync::Arc::new(std::sync::OnceLock::new()));
        let revoke = revoked.clone();
        #[cfg(test)]
        let known_features = self.known_features;
        let result = self
            .store
            .transaction(meta, BUDGET, move |tx| {
                if crate::oltp_replication::owned::serialized_digest(&command)? != digest {
                    return Err(unavailable("Control command digest differs"));
                }
                #[cfg(test)]
                direct::KNOWN_FEATURES.with(|known| known.set(known_features));
                let transition = command
                    .payload()
                    .and_then(|payload| apply(tx, &scope, position, digest, payload));
                #[cfg(test)]
                direct::KNOWN_FEATURES
                    .with(|known| known.set(super::direct::CLUSTER_FEATURES.len()));
                let transition = transition?;
                if let (Some(id), Some(revoke)) = (mount, revoke)
                    && let Transition::Commit(reply) = &transition
                    && decode::<ControlReply>(reply.as_ref())?
                        == ControlReply::SystemReplacementMounted
                    && let Some(record) =
                        load::<super::ReplacementRecord>(tx, &system_recovery::by_id(id))?
                {
                    let _ = revoke.set(record.intent.previous);
                }
                Ok(transition)
            })
            .await?;
        if let Some(directory) = directory
            && let Some(worker) = revoked.as_ref().and_then(|change| change.get())
        {
            directory.record_revocation(*worker);
        }
        drop(reload);
        if refresh {
            self.refresh_or_retry().await;
        }
        Ok(result)
    }
}

#[derive(Serialize, serde::Deserialize)]
pub(super) struct Receipt {
    pub(super) digest: Digest,
    pub(super) reply: ControlReply,
    #[serde(default)]
    pub(super) growth_digest: Option<Digest>,
}
#[derive(Serialize, serde::Deserialize, PartialEq, Eq)]
struct FenceAck {
    group: GroupIdentity,
    expected: Authority,
    next: Authority,
    applied_at: AppliedPosition,
    command_digest: Digest,
}

fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    position: AppliedPosition,
    digest: Digest,
    command: &ControlCommand,
) -> Result<Transition, ProtocolError> {
    let op = command.operation_id();
    if op.is_nil() {
        return reject(tx, Fault::Invalid);
    }
    let receipt_key = key(b"ct/op/", op);
    let growth_digest = match command {
        ControlCommand::Growth { command, .. } => Some(
            crate::oltp_replication::owned::serialized_digest(&(op, command))?,
        ),
        _ => None,
    };
    if let Some(old) = load::<Receipt>(tx, &receipt_key)? {
        let authorized_growth_replay =
            if old.growth_digest.is_some() && old.growth_digest == growth_digest {
                match command {
                    ControlCommand::Growth { authorization, .. } => load::<State>(tx, STATE)?
                        .is_some_and(|state| stamp(authorization, state.policy)),
                    _ => false,
                }
            } else {
                false
            };
        return if old.digest == digest || authorized_growth_replay {
            commit(tx, &old.reply)
        } else {
            reject(tx, Fault::Conflict)
        };
    }
    let state = load::<State>(tx, STATE)?;
    let reply = match (state, command) {
        (
            None,
            ControlCommand::Initialize {
                run_id,
                authorization,
                ..
            },
        ) => {
            if run_id.is_nil() || !stamp(authorization, authorization.policy) {
                ControlReply::Rejected(Fault::Invalid)
            } else {
                let ready = ReadyMembership {
                    view: AuthorityView {
                        run_id: *run_id,
                        owner_term: 1,
                        membership_revision: 1,
                    },
                    ready_root: ReadySetHasher::default().finish(),
                    ready_count: 0,
                };
                let state = State {
                    policy: authorization.policy,
                    ready,
                    owner: None,
                    ready_generation: op,
                    enrollment_revision: 0,
                    group_inventory_revision: 1,
                    group_count: 1,
                    invalid_ready: 0,
                    pending: None,
                    restart: None,
                };
                save(tx, STATE, &state)?;
                save(tx, &key(RUNS, *run_id), &true)?;
                save(
                    tx,
                    &key(GROUPS, scope.group_id),
                    &GroupRecord {
                        identity: scope.clone(),
                        generation: op,
                        replica_count: 0,
                        replica_root: group_root(0, group_seed()),
                        membership_position: position,
                    },
                )?;
                ControlReply::Initialized { ready }
            }
        }
        (None, _) => ControlReply::Rejected(Fault::NotInitialized),
        (Some(_), ControlCommand::Initialize { .. }) => ControlReply::Rejected(Fault::Conflict),
        (Some(mut state), _) => {
            let reply = apply_initialized(tx, scope, position, digest, command, &mut state)?;
            if !matches!(reply, ControlReply::Rejected(_)) {
                pool_drain::advance_revision(tx, command)?;
                save(tx, STATE, &state)?;
            }
            reply
        }
    };
    if matches!(reply, ControlReply::Rejected(_)) {
        // GroupTransaction cannot expose candidate writes after a rejection.
        // Semantic paths below preflight their page before writing anything.
        return reject(
            tx,
            match reply {
                ControlReply::Rejected(fault) => fault,
                _ => unreachable!(),
            },
        );
    }
    save(
        tx,
        &receipt_key,
        &Receipt {
            digest,
            reply: reply.clone(),
            growth_digest,
        },
    )?;
    commit(tx, &reply)
}

fn apply_initialized(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    position: AppliedPosition,
    command_digest: Digest,
    command: &ControlCommand,
    state: &mut State,
) -> Result<ControlReply, ProtocolError> {
    use ControlCommand::*;
    if state.restart.is_some()
        && matches!(
            command,
            BeginGroup { .. }
                | RegisterInitialData { .. }
                | BeginGroupReplacement { .. }
                | GroupPage { .. }
                | SealGroup { .. }
                | ReplicaRetired { .. }
                | BeginReplicaMove { .. }
                | PublishReplicaMove { .. }
                | ArmReplicaMove { .. }
                | CancelReplicaMove { .. }
                | GroupDamageReplacementBegin { .. }
                | GroupDamageReplaced { .. }
        )
    {
        return fault(Fault::Busy);
    }
    if matches!(command,
        PrepareRestart { .. } | PrepareDataWorker { .. } | CompleteDataWorker { .. }
        | BeginDataWorkerJoin { .. } | BeginJoin { .. } | BeginRemoval { .. } | BeginQueueRemoval { .. }
        | BeginOwnerFence { .. } | BeginColdFence { .. } | Enroll { .. }
    ) && load::<Uuid>(tx, placement::ACTIVE)?.is_some() {
        return fault(Fault::Busy);
    }
    match command {
        SystemRecovery { operation_id, command } =>
            system_recovery::apply(tx, scope, state, position, *operation_id, command),
        DirectCapability {
            operation_id,
            actor,
            action,
            unix_ms,
        } => direct::apply(tx, state, position, *operation_id, *actor, action, *unix_ms),
        ScopedQueue { operation_id, actor, action, authorization } => {
            scoped_queue::apply(tx, scope, state, *operation_id, *actor, action, authorization.as_ref())
        }
        EnsureScopedQueue {
            operation_id,
            expected,
            run_id,
            authorization,
        } => scoped_queue::ensure(tx, scope, state, *operation_id, expected, *run_id, authorization),
        BeginWorkerDrain { operation_id, worker, authorization, intent } =>
            worker_drain::begin(tx, state, *operation_id, *worker, authorization, intent),
        BeginWorkerDrainRemoval { operation_id, worker, authorization, id, expected, placement_revision, policy, growth_revision } =>
            worker_drain::begin_removal(tx, state, *operation_id, *worker, authorization, *id, *expected, *placement_revision, *policy, *growth_revision),
        CloseDataWorker { proof, .. } => worker_drain::close(tx, scope, state, proof),
        EksWorkerReplacement { operation_id, worker, authorization, request } =>
            replacement::apply(tx, scope, state, *operation_id, *worker, authorization, request),
        CompleteEksWorkerReplacement { worker, mounted, .. } =>
            replacement::complete(tx, scope, state, *worker, mounted),
        PoolDrain { operation_id, worker, authorization, command } =>
            pool_drain::apply(tx, scope, state, *operation_id, *worker, authorization, command),
        PrepareDataWorker { intent, authorization, .. } =>
            data_worker::prepare(tx, scope, state, intent, authorization),
        CompleteDataWorker { intent, mount, proof, .. } =>
            data_worker::complete(tx, scope, state, intent, mount, proof),
        BeginDataWorkerJoin { operation_id, claim_id, worker, expected } =>
            data_worker::begin_join(tx, state, *operation_id, *worker, *claim_id, *expected),
        RegisterRestartMount { worker, pool_id, generation, public_key, authorization, .. } =>
            restart::register_mount(tx, state, *worker, *pool_id, *generation, *public_key, authorization),
        PrepareRestart { operation_id, intent, authorization } => {
            if tx.get(data_worker::PREPARED)?.is_some() {
                return fault(Fault::Busy);
            }
            restart::prepare(tx, scope, state, *operation_id, intent, authorization)
        }
        CompleteRestart { operation_id, intent, proof, exclusion } =>
            restart::complete(tx, scope, state, *operation_id, intent, proof, exclusion),
        BeginReplicaMove { operation_id, plan, authorization } =>
            placement::begin(tx, scope, state, *operation_id, plan, authorization),
        ArmReplicaMove { transition_id, worker, authorization, guard, .. } =>
            placement::arm(tx, state, *transition_id, *worker, authorization, guard),
        CancelReplicaMove { transition_id, worker, authorization, .. } =>
            placement::cancel(tx, state, *transition_id, *worker, authorization),
        PublishReplicaMove { proof, authorization, .. } =>
            placement::publish(tx, scope, state, proof, authorization),
        GroupDamaged { worker, report, .. } =>
            group_damage::report(tx, scope, position, *worker, report),
        GroupDamageReplacementBegin { authorization, plan, .. } =>
            group_damage::begin(tx, scope, state, position, authorization, plan),
        GroupDamageReplaced { authorization, proof, .. } =>
            group_damage::replaced(tx, scope, state, authorization, proof),
        Search { unix_ms, command, .. } => search::apply(tx, scope, state, *unix_ms, command),
        ReplicaRetired {
            authorization,
            proof,
            remote,
            ..
        } => {
            if let Some(plan) = load(tx, &placement::move_key(proof.generation))? {
                return placement::retire(tx, scope, state, plan, proof, remote.as_ref(), authorization);
            }
            if remote.is_some() || !stamp(authorization, state.policy) {
                return fault(Fault::Unauthorized);
            }
            let Some(mut build) =
                load::<GroupBuild>(tx, &key(b"ct/group-build/", proof.generation))?
            else {
                return fault(Fault::NotFound);
            };
            let Some(expected) = build.expected.as_ref() else {
                return fault(Fault::Invalid);
            };
            let Some(published) = load::<GroupRecord>(tx, &key(GROUPS, proof.group.group_id))?
            else {
                return fault(Fault::NotFound);
            };
            if published.identity != proof.group
                || published.generation != proof.generation
                || proof.worker.worker_id != proof.replica.worker_id
                || proof.worker.validate().is_err()
                || !position_valid(proof.applied_at)
                || proof.applied_at.index < published.membership_position.index
                || load::<ReplicaMember>(
                    tx,
                    &member_key(expected.generation, proof.replica.node_id),
                )?
                .as_ref()
                    != Some(&proof.replica)
                || load::<ReplicaMember>(tx, &member_key(proof.generation, proof.replica.node_id))?
                    .is_some()
            {
                return fault(Fault::Invalid);
            }
            let retired = pool_drain::retired_key(proof.generation, proof.replica.node_id);
            if let Some(old) = load::<ReplicaRetiredStamp>(tx, &retired)? {
                if old != *proof {
                    return fault(Fault::Conflict);
                }
            } else {
                if load::<Uuid>(tx, &key(b"ct/group-replacement/", proof.group.group_id))?
                    != Some(proof.generation)
                {
                    return fault(Fault::Conflict);
                }
                build.retired_count = build.retired_count.checked_add(1).ok_or_else(unknown)?;
                if build.retired_count > expected.replica_count.saturating_sub(build.retained_count)
                {
                    return fault(Fault::Invalid);
                }
                save(tx, &retired, proof)?;
                save(tx, &key(b"ct/group-build/", proof.generation), &build)?;
            }
            let remaining = expected
                .replica_count
                .checked_sub(build.retained_count)
                .and_then(|n| n.checked_sub(build.retired_count))
                .ok_or_else(unknown)?;
            if remaining == 0 {
                tx.delete(&key(b"ct/group-replacement/", proof.group.group_id))?;
            }
            Ok(ControlReply::GroupRetirement {
                generation: proof.generation,
                remaining,
            })
        }
        Growth {
            authorization,
            command,
            ..
        } => {
            if !stamp(authorization, state.policy) {
                return fault(Fault::Unauthorized);
            }
            super::growth::apply(tx, command, state)
        }
        Initialize { .. } => Ok(ControlReply::Rejected(Fault::Conflict)),
        Enroll {
            expected,
            next,
            authorization,
            previous_process,
            ..
        } => {
            if system_recovery::blocked_enrollment(tx, next)?
                || tx.get(&key(data_worker::REGISTERED, next.worker_id))?.is_some()
                || load::<super::DataWorkerIntent>(tx, data_worker::PREPARED)?
                    .is_some_and(|intent| intent.enrollment.worker_id == next.worker_id)
            {
                return fault(Fault::Unauthorized);
            }
            if matches!(state.pending, Some(Pending::Ready(_))) {
                return fault(Fault::Busy);
            }
            if state.restart.as_ref().is_some_and(|restart| restart.intent.previous.worker_id == next.worker_id)
                && !matches!(&state.pending, Some(Pending::Fence(fence)) if fence.cold)
            {
                return fault(Fault::Busy);
            }
            if !stamp(authorization, state.policy) || next.validate().is_err() {
                return fault(Fault::Unauthorized);
            }
            if load::<ApiEnrollment>(tx, &key(APIS, next.worker_id))?.is_some() {
                return fault(Fault::Conflict);
            }
            let old = load::<WorkerRecord>(tx, &key(WORKERS, next.worker_id))?;
            if old.as_ref().map(|v| WorkerIdentity::from(&v.enrollment)) != *expected {
                return fault(Fault::Stale);
            }
            let revision = old.as_ref().map_or(Ok(1), |v| {
                v.enrollment
                    .enrollment_revision
                    .checked_add(1)
                    .ok_or_else(unknown)
            })?;
            if next.enrollment_revision < revision {
                return fault(Fault::Invalid);
            }
            if let Some(old) = &old {
                let identity = WorkerIdentity::from(&old.enrollment);
                if next.incarnation == identity.incarnation
                    || !previous_process.as_ref().is_some_and(|proof| {
                        proof.worker == identity && exclusion(proof, state.policy)
                    })
                {
                    return fault(Fault::Unauthorized);
                }
                if load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, next.worker_id))?
                    == Some(identity)
                {
                    state.invalid_ready = state.invalid_ready.checked_add(1).ok_or_else(unknown)?;
                }
            } else if previous_process.is_some() {
                return fault(Fault::Invalid);
            }
            if let Some(proof) = previous_process {
                record_process_exclusion(tx, proof)?;
            }
            state.enrollment_revision = state
                .enrollment_revision
                .checked_add(1)
                .ok_or_else(unknown)?;
            save(
                tx,
                &key(WORKERS, next.worker_id),
                &WorkerRecord {
                    enrollment: next.clone(),
                    previous_process: previous_process.clone(),
                },
            )?;
            Ok(ControlReply::Enrolled(WorkerIdentity::from(next)))
        }
        EnrollApi {
            expected,
            next,
            authorization,
            ..
        } => {
            if !stamp(authorization, state.policy)
                || next.validate().is_err()
                || next.database != scope.database
            {
                return fault(Fault::Unauthorized);
            }
            if system_recovery::blocked_enrollment(tx, next.transport())? {
                return fault(Fault::Unauthorized);
            }
            let next_identity = WorkerIdentity::from(next.transport());
            if load::<WorkerRecord>(tx, &key(WORKERS, next_identity.worker_id))?.is_some() {
                return fault(Fault::Conflict);
            }
            let old = load::<ApiEnrollment>(tx, &key(APIS, next_identity.worker_id))?;
            if old
                .as_ref()
                .map(|old| WorkerIdentity::from(old.transport()))
                != *expected
            {
                return fault(Fault::Stale);
            }
            if old.as_ref().is_some_and(|old| {
                old.enrollment.incarnation == next.enrollment.incarnation
                    || next.enrollment.enrollment_revision <= old.enrollment.enrollment_revision
            }) {
                return fault(Fault::Invalid);
            }
            state.enrollment_revision = state
                .enrollment_revision
                .checked_add(1)
                .ok_or_else(unknown)?;
            save(tx, &key(APIS, next_identity.worker_id), next)?;
            Ok(ControlReply::Enrolled(next_identity))
        }
        BeginGroup {
            generation,
            identity,
            authorization,
            ..
        } => {
            if state.pending.is_some() || tx.get(&placement::move_key(*generation))?.is_some() {
                return fault(Fault::Busy);
            }
            if !stamp(authorization, state.policy)
                || !same_scope(scope, identity)
                || identity.validate().is_err()
                || generation.is_nil()
                || identity.kind == GroupKind::Control && identity.group_id != scope.group_id
            {
                return fault(Fault::Unauthorized);
            }
            if let Some(existing) = load::<GroupBuild>(tx, &key(b"ct/group-build/", *generation))? {
                return if existing.identity == *identity {
                    Ok(ControlReply::GroupBuilding {
                        generation: *generation,
                        count: existing.count,
                    })
                } else {
                    fault(Fault::Conflict)
                };
            }
            if load::<GroupRecord>(tx, &key(GROUPS, identity.group_id))?
                .is_some_and(|g| g.replica_count != 0)
            {
                return fault(Fault::Conflict);
            }
            save(
                tx,
                &key(b"ct/group-build/", *generation),
                &GroupBuild {
                    generation: *generation,
                    identity: identity.clone(),
                    count: 0,
                    last_node: None,
                    chain: group_seed(),
                    expected: None,
                    retained_count: 0,
                    retired_count: 0,
                },
            )?;
            Ok(ControlReply::GroupBuilding {
                generation: *generation,
                count: 0,
            })
        }
        RegisterInitialData {
            proof,
            replicas,
            authorization,
            ..
        } => register_initial_data(tx, scope, state, proof, replicas, authorization),
        BeginGroupReplacement {
            generation,
            expected,
            authorization,
            ..
        } => {
            if state.pending.is_some() || tx.get(&placement::move_key(*generation))?.is_some() {
                return fault(Fault::Busy);
            }
            if !stamp(authorization, state.policy)
                || !same_scope(scope, &expected.identity)
                || generation.is_nil()
                || *generation == expected.generation
                || expected.replica_count == 0
            {
                return fault(Fault::Unauthorized);
            }
            if let Some(build) = load::<GroupBuild>(tx, &key(b"ct/group-build/", *generation))? {
                return if build.expected.as_ref() == Some(expected) {
                    Ok(ControlReply::GroupBuilding {
                        generation: *generation,
                        count: build.count,
                    })
                } else {
                    fault(Fault::Conflict)
                };
            }
            if load::<GroupRecord>(tx, &key(GROUPS, expected.identity.group_id))?.as_ref()
                != Some(expected)
            {
                return fault(Fault::Stale);
            }
            // One durable replacement claim per group. The old paged record
            // remains authoritative until a new actual membership is sealed.
            let claim = key(b"ct/group-replacement/", expected.identity.group_id);
            if load::<Uuid>(tx, &claim)?.is_some_and(|old| old != *generation) {
                return fault(Fault::Busy);
            }
            save(tx, &claim, generation)?;
            save(
                tx,
                &key(b"ct/group-build/", *generation),
                &GroupBuild {
                    generation: *generation,
                    identity: expected.identity.clone(),
                    count: 0,
                    last_node: None,
                    chain: group_seed(),
                    expected: Some(expected.clone()),
                    retained_count: 0,
                    retired_count: 0,
                },
            )?;
            Ok(ControlReply::GroupBuilding {
                generation: *generation,
                count: 0,
            })
        }
        GroupPage {
            generation,
            replicas,
            ..
        } => {
            if state.pending.is_some() {
                return fault(Fault::Busy);
            }
            let Some(mut build) = load::<GroupBuild>(tx, &key(b"ct/group-build/", *generation))?
            else {
                return fault(Fault::NotFound);
            };
            if load::<GroupRecord>(tx, &key(GROUPS, build.identity.group_id))?
                .is_some_and(|record| record.generation == *generation)
            {
                return fault(Fault::Conflict);
            }
            let mut within = BTreeSet::new();
            for member in replicas.iter() {
                if pool_drain::blocks_pool(tx, member.pool_id)? || worker_drain::blocks_worker(tx, member.worker_id)? {
                    return fault(Fault::Busy);
                }
                if !direct::admits_worker_id(tx, member.worker_id)? {
                    return fault(Fault::DirectCapabilityRequired);
                }
                if member.validate().is_err()
                    || build.last_node.is_some_and(|old| member.node_id <= old)
                    || !within.insert(member.worker_id)
                    || load::<WorkerRecord>(tx, &key(WORKERS, member.worker_id))?.is_none()
                    || (!matches!(build.identity.kind, GroupKind::Data { .. })
                        && tx.get(&key(data_worker::REGISTERED, member.worker_id))?.is_some())
                    || tx
                        .get(&member_worker_key(*generation, member.worker_id))?
                        .is_some()
                {
                    return fault(Fault::Invalid);
                }
                build.last_node = Some(member.node_id);
                build.count = build.count.checked_add(1).ok_or_else(unknown)?;
                build.chain = group_step(build.chain, member);
                if let Some(expected) = &build.expected {
                    if let Some(old) =
                        load::<ReplicaMember>(tx, &member_key(expected.generation, member.node_id))?
                    {
                        if old != *member {
                            return fault(Fault::Invalid);
                        }
                        build.retained_count =
                            build.retained_count.checked_add(1).ok_or_else(unknown)?;
                    }
                }
            }
            for member in replicas.iter() {
                save(tx, &member_key(*generation, member.node_id), member)?;
                save(
                    tx,
                    &member_worker_key(*generation, member.worker_id),
                    member,
                )?;
            }
            save(tx, &key(b"ct/group-build/", *generation), &build)?;
            Ok(ControlReply::GroupBuilding {
                generation: *generation,
                count: build.count,
            })
        }
        SealGroup {
            generation, proof, ..
        } => {
            if state.pending.is_some() {
                return fault(Fault::Busy);
            }
            let Some(build) = load::<GroupBuild>(tx, &key(b"ct/group-build/", *generation))? else {
                return fault(Fault::NotFound);
            };
            if build.count == 0
                || proof.group != build.identity
                || proof.generation != *generation
                || proof.replica_count != build.count
                || proof.replica_root != group_root(build.count, build.chain)
                || proof.authority != state.ready.view.authority()
                || !position_valid(proof.applied_at)
            {
                return fault(Fault::Invalid);
            }
            let old = load::<GroupRecord>(tx, &key(GROUPS, build.identity.group_id))?;
            if let Some(old) = &old {
                if old.generation == *generation
                    && old.identity == build.identity
                    && old.replica_count == build.count
                    && old.replica_root == proof.replica_root
                {
                    return Ok(ControlReply::GroupRegistered {
                        group_id: build.identity.group_id,
                        count: build.count,
                        root: proof.replica_root,
                    });
                }
            }
            match &build.expected {
                Some(expected)
                    if old.as_ref() == Some(expected)
                        && proof.applied_at.index >= expected.membership_position.index => {}
                None if old.as_ref().is_none_or(|group| group.replica_count == 0) => {}
                _ => return fault(Fault::Conflict),
            }
            save(
                tx,
                &key(GROUPS, build.identity.group_id),
                &GroupRecord {
                    identity: build.identity.clone(),
                    generation: *generation,
                    replica_count: build.count,
                    replica_root: proof.replica_root,
                    membership_position: proof.applied_at,
                },
            )?;
            if old.is_none() {
                state.group_count = state.group_count.checked_add(1).ok_or_else(unknown)?;
            }
            // Keep the durable transition exclusive through actual retirement
            // of every removed local replica. Publication alone cannot unlock
            // another move or authorize detaching any storage.
            if build
                .expected
                .as_ref()
                .is_some_and(|old| old.replica_count == build.retained_count)
            {
                tx.delete(&key(b"ct/group-replacement/", build.identity.group_id))?;
            }
            state.group_inventory_revision = state
                .group_inventory_revision
                .checked_add(1)
                .ok_or_else(unknown)?;
            Ok(ControlReply::GroupRegistered {
                group_id: build.identity.group_id,
                count: build.count,
                root: proof.replica_root,
            })
        }
        BeginJoin {
            operation_id,
            expected,
            target,
        } => {
            if tx.get(&key(data_worker::REGISTERED, target.worker_id))?.is_some() {
                return fault(Fault::Unauthorized);
            }
            begin_ready(tx, state, *operation_id, *expected, *target, ReadyChange::Join, None, false)
        }
        BeginRemoval {
            operation_id,
            expected,
            target,
            exclusion: proof,
        } => begin_ready(
            tx,
            state,
            *operation_id,
            *expected,
            *target,
            ReadyChange::Remove,
            Some(proof),
            false,
        ),
        BeginQueueRemoval {
            operation_id,
            expected,
            target,
            owner_fence,
            authorization,
        } => {
            let published: Option<(Uuid, Authority)> = load(tx, b"ct/published-owner-fence")?;
            if !stamp(authorization, state.policy)
                || published != Some((*owner_fence, state.ready.view.authority()))
            {
                return fault(Fault::Unauthorized);
            }
            begin_ready(
                tx,
                state,
                *operation_id,
                *expected,
                *target,
                ReadyChange::Remove,
                None,
                true,
            )
        }
        ReadyPage {
            transition_id,
            members,
            ..
        } => ready_page(tx, state, *transition_id, members),
        SealReady { transition_id, .. } => seal_ready(tx, state, *transition_id),
        TimestampFenced {
            transition_id,
            receipt,
            ..
        } => {
            let Some(Pending::Ready(pending)) = &mut state.pending else {
                return fault(Fault::NotFound);
            };
            if pending.transition_id != *transition_id
                || pending.phase != ReadyPhase::AwaitingTimestampFence
                || receipt.transition_id != *transition_id
                || receipt.expected != pending.expected
                || Some(receipt.next) != pending.next
                || !position_valid(receipt.applied_at)
            {
                return fault(Fault::Stale);
            }
            pending.timestamp_fence = Some(receipt.clone());
            pending.phase = ReadyPhase::AwaitingQueueProof;
            Ok(ControlReply::TimestampFenced {
                transition_id: *transition_id,
                next: receipt.next,
            })
        }
        PublishReady {
            transition_id,
            proof,
            ..
        } => publish_ready(tx, state, *transition_id, proof),
        BeginOwnerFence {
            operation_id,
            expected,
            next_owner,
            authorization,
        } => {
            if state.pending.is_some() {
                return fault(Fault::Busy);
            }
            if *expected != state.ready
                || !stamp(authorization, state.policy)
                || !current_ready(tx, state, *next_owner)?
                || tx.get(&key(data_worker::REGISTERED, next_owner.worker_id))?.is_some()
            {
                return fault(Fault::Stale);
            }
            let mut next = *expected;
            next.view.owner_term = next.view.owner_term.checked_add(1).ok_or_else(unknown)?;
            begin_fence(
                tx,
                scope,
                state,
                *operation_id,
                next,
                Some(*next_owner),
                false,
                None,
                authorization.clone(),
                position,
                command_digest,
            )
        }
        BeginColdFence {
            operation_id,
            expected,
            next_run,
            authorization,
        } => {
            // An explicitly authorized cold fence supersedes an interrupted
            // join only after the target is also positively excluded. The TSO
            // and every data/status group still fence the old authority before
            // Control publishes the new run.
            let extra_exclusion = match &state.pending {
                Some(Pending::Fence(_)) => return fault(Fault::Busy),
                Some(Pending::Ready(pending)) if pending.change == ReadyChange::Join => {
                    Some(pending.target)
                }
                _ => None,
            };
            if *expected != state.ready
                || next_run.is_nil()
                || *next_run == state.ready.view.run_id
                || !stamp(authorization, state.policy)
            {
                return fault(Fault::Unauthorized);
            }
            if load::<bool>(tx, &key(RUNS, *next_run))?.is_some() {
                return fault(Fault::Conflict);
            }
            if state.restart.take().is_some() {
                state.enrollment_revision = state.enrollment_revision.checked_add(1).ok_or_else(unknown)?;
            }
            if tx.get(data_worker::PREPARED)?.is_some() {
                tx.delete(data_worker::PREPARED)?;
                state.enrollment_revision = state.enrollment_revision.checked_add(1).ok_or_else(unknown)?;
            }
            let next = ReadyMembership {
                view: AuthorityView {
                    run_id: *next_run,
                    owner_term: state
                        .ready
                        .view
                        .owner_term
                        .checked_add(1)
                        .ok_or_else(unknown)?,
                    membership_revision: state
                        .ready
                        .view
                        .membership_revision
                        .checked_add(1)
                        .ok_or_else(unknown)?,
                },
                ready_root: ReadySetHasher::default().finish(),
                ready_count: 0,
            };
            save(tx, &key(RUNS, state.ready.view.run_id), &true)?;
            save(tx, &key(RUNS, *next_run), &true)?;
            begin_fence(
                tx,
                scope,
                state,
                *operation_id,
                next,
                None,
                true,
                extra_exclusion,
                authorization.clone(),
                position,
                command_digest,
            )
        }
        ExcludedPage {
            transition_id,
            workers,
            ..
        } => {
            let Some(Pending::Fence(pending)) = &mut state.pending else {
                return fault(Fault::NotFound);
            };
            if pending.transition_id != *transition_id || !pending.cold {
                return fault(Fault::Stale);
            }
            let mut within = BTreeSet::new();
            let mut additions = Vec::new();
            for proof in workers.iter() {
                if !within.insert(proof.worker.worker_id)
                    || !exclusion(proof, state.policy)
                    || load::<WorkerIdentity>(
                        tx,
                        &ready_key(state.ready_generation, proof.worker.worker_id),
                    )? != Some(proof.worker)
                        && pending.extra_exclusion != Some(proof.worker)
                {
                    return fault(Fault::Unauthorized);
                }
                let target = exclusion_key(*transition_id, proof.worker.worker_id);
                match load::<ExclusionStamp>(tx, &target)? {
                    Some(old) if old != *proof => return fault(Fault::Conflict),
                    Some(_) => {}
                    None => additions.push((target, proof)),
                }
            }
            for (target, proof) in additions {
                save(tx, &target, proof)?;
                record_process_exclusion(tx, proof)?;
                pending.excluded_workers = pending
                    .excluded_workers
                    .checked_add(1)
                    .ok_or_else(unknown)?;
            }
            Ok(ControlReply::FenceProgress {
                transition_id: *transition_id,
                fenced_groups: pending.fenced_groups,
                excluded_workers: pending.excluded_workers,
            })
        }
        GroupFenced { receipt, .. } => {
            let Some(Pending::Fence(pending)) = &mut state.pending else {
                return fault(Fault::NotFound);
            };
            if pending.transition_id != receipt.transition_id
                || receipt.expected != Some(pending.expected.view.authority())
                || receipt.next != pending.next.view.authority()
                || !same_scope(scope, &receipt.group)
                || !position_valid(receipt.applied_at)
                || !nonzero(receipt.command_digest)
                || load::<GroupRecord>(tx, &key(GROUPS, receipt.group.group_id))?
                    .is_none_or(|g| g.identity != receipt.group)
            {
                return fault(Fault::Invalid);
            }
            let target = fence_key(receipt.transition_id, receipt.group.group_id);
            let ack = FenceAck {
                group: receipt.group.clone(),
                expected: pending.expected.view.authority(),
                next: receipt.next,
                applied_at: receipt.applied_at,
                command_digest: receipt.command_digest,
            };
            match load::<FenceAck>(tx, &target)? {
                Some(old) if old != ack => return fault(Fault::Conflict),
                Some(_) => {}
                None => {
                    save(tx, &target, &ack)?;
                    pending.fenced_groups =
                        pending.fenced_groups.checked_add(1).ok_or_else(unknown)?;
                }
            }
            Ok(ControlReply::FenceProgress {
                transition_id: receipt.transition_id,
                fenced_groups: pending.fenced_groups,
                excluded_workers: pending.excluded_workers,
            })
        }
        PublishFence { transition_id, .. } => {
            let Some(Pending::Fence(pending)) = &state.pending else {
                return fault(Fault::NotFound);
            };
            if pending.transition_id != *transition_id
                || pending.group_inventory_revision != state.group_inventory_revision
                || pending.fenced_groups != pending.required_groups
                || pending.cold
                    && pending.excluded_workers
                        != state
                            .ready
                            .ready_count
                            .checked_add(u64::from(pending.extra_exclusion.is_some()))
                            .ok_or_else(unknown)?
            {
                return fault(Fault::Incomplete);
            }
            let pending = pending.clone();
            state.ready = pending.next;
            state.owner = pending.next_owner;
            if pending.cold {
                system_recovery::retire_on_cold_fence(tx)?;
                // A cold fence begun by an earlier binary can still publish.
                save(tx, &key(RUNS, pending.expected.view.run_id), &true)?;
                save(tx, &key(RUNS, pending.next.view.run_id), &true)?;
                save(
                    tx,
                    COLD_PUBLICATION,
                    &(
                        *transition_id,
                        pending.expected.view.authority(),
                        pending.next.view.authority(),
                    ),
                )?;
                state.ready_generation = *transition_id;
                state.invalid_ready = 0;
            } else {
                save(
                    tx,
                    b"ct/published-owner-fence",
                    &(*transition_id, pending.next.view.authority()),
                )?;
            }
            state.pending = None;
            Ok(ControlReply::Published {
                ready: state.ready,
                owner: state.owner,
            })
        }
    }
}

fn begin_ready(
    tx: &mut GroupTransaction<'_>,
    state: &mut State,
    id: Uuid,
    expected: AuthorityView,
    target: WorkerIdentity,
    change: ReadyChange,
    removed: Option<&ExclusionStamp>,
    logical_fence: bool,
) -> Result<ControlReply, ProtocolError> {
    if state.pending.is_some() {
        return fault(Fault::Busy);
    }
    if state.ready.view != expected || target.validate().is_err() {
        return fault(Fault::Stale);
    }
    if change == ReadyChange::Join && system_recovery::waits_for_groups(tx, target.worker_id)? {
        return fault(Fault::Busy);
    }
    if change == ReadyChange::Join && !direct::admits(tx, target)? {
        return fault(Fault::DirectCapabilityRequired);
    }
    if change == ReadyChange::Join && worker_drain::blocks_worker(tx, target.worker_id)?
        && !replacement::can_join(tx, target)?
    {
        return fault(Fault::Busy);
    }
    if change == ReadyChange::Join
        && state.restart.as_ref().is_some_and(|restart| {
            !restart.enrolled || WorkerIdentity::from(&restart.intent.next) != target
        })
    {
        return fault(Fault::Busy);
    }
    let enrolled = load::<WorkerRecord>(tx, &key(WORKERS, target.worker_id))?;
    if enrolled
        .as_ref()
        .map(|w| WorkerIdentity::from(&w.enrollment))
        != Some(target)
    {
        return fault(Fault::Unauthorized);
    }
    let existing =
        load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, target.worker_id))?;
    match change {
        ReadyChange::Join if existing.is_some() => return fault(Fault::Conflict),
        ReadyChange::Remove
            if existing != Some(target)
                || state.ready.ready_count <= 1
                || state.owner == Some(target)
                || !logical_fence
                    && !removed.is_some_and(|proof| {
                        proof.worker == target && exclusion(proof, state.policy)
                    }) =>
        {
            return fault(Fault::Unauthorized);
        }
        _ => {}
    }
    if let Some(proof) = removed {
        record_process_exclusion(tx, proof)?;
    }
    state.pending = Some(Pending::Ready(ReadyPending {
        transition_id: id,
        expected: state.ready,
        target,
        change,
        generation: id,
        source_after: None,
        target_seen: false,
        root: ReadySetHasher::default(),
        next: None,
        phase: ReadyPhase::Building,
        timestamp_fence: None,
    }));
    Ok(ControlReply::JoinBegun {
        ticket: JoinTicket {
            ticket_id: id,
            target,
            source_ready: state.ready,
        },
        bootstrap: state.ready.ready_count == 0,
    })
}

fn ready_page(
    tx: &mut GroupTransaction<'_>,
    state: &mut State,
    id: Uuid,
    members: &Page<WorkerIdentity>,
) -> Result<ControlReply, ProtocolError> {
    let Some(Pending::Ready(pending)) = &mut state.pending else {
        return fault(Fault::NotFound);
    };
    if pending.transition_id != id || pending.phase != ReadyPhase::Building {
        return fault(Fault::Stale);
    }
    let mut next = pending.clone();
    for member in members.iter() {
        let enrolled = load::<WorkerRecord>(tx, &key(WORKERS, member.worker_id))?;
        if enrolled
            .as_ref()
            .map(|w| WorkerIdentity::from(&w.enrollment))
            != Some(*member)
            || next.root.push(*member).is_err()
        {
            return fault(Fault::Invalid);
        }
        let mut old = next_ready(tx, state.ready_generation, next.source_after)?;
        if next.change == ReadyChange::Remove && old == Some(next.target) {
            next.source_after = Some(next.target.worker_id);
            old = next_ready(tx, state.ready_generation, next.source_after)?;
            next.target_seen = true;
        }
        if next.change == ReadyChange::Join && *member == next.target {
            if next.target_seen {
                return fault(Fault::Conflict);
            }
            next.target_seen = true;
        } else {
            if old != Some(*member) {
                return fault(Fault::Invalid);
            }
            next.source_after = Some(member.worker_id);
        }
    }
    for member in members.iter() {
        save(tx, &ready_key(next.generation, member.worker_id), member)?;
    }
    *pending = next;
    Ok(ControlReply::ReadyBuilding {
        transition_id: id,
        count: pending.root.count(),
    })
}
fn seal_ready(
    tx: &mut GroupTransaction<'_>,
    state: &mut State,
    id: Uuid,
) -> Result<ControlReply, ProtocolError> {
    let Some(Pending::Ready(pending)) = &mut state.pending else {
        return fault(Fault::NotFound);
    };
    if pending.transition_id != id || pending.phase != ReadyPhase::Building {
        return fault(Fault::Stale);
    }
    let mut old = next_ready(tx, state.ready_generation, pending.source_after)?;
    if pending.change == ReadyChange::Remove && old == Some(pending.target) {
        pending.target_seen = true;
        pending.source_after = Some(pending.target.worker_id);
        old = next_ready(tx, state.ready_generation, pending.source_after)?;
    }
    let expected = match pending.change {
        ReadyChange::Join => state.ready.ready_count.checked_add(1),
        ReadyChange::Remove => state.ready.ready_count.checked_sub(1),
    }
    .ok_or_else(unknown)?;
    if old.is_some() || !pending.target_seen || pending.root.count() != expected {
        return fault(Fault::Incomplete);
    }
    let mut view = state.ready.view;
    view.membership_revision = view
        .membership_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    let next = ReadyMembership {
        view,
        ready_root: pending.root.finish(),
        ready_count: pending.root.count(),
    };
    pending.next = Some(next);
    pending.phase = ReadyPhase::AwaitingTimestampFence;
    Ok(ControlReply::ReadySealed {
        transition_id: id,
        expected: pending.expected,
        next,
    })
}
fn publish_ready(
    tx: &mut GroupTransaction<'_>,
    state: &mut State,
    id: Uuid,
    proof: &QueueCopyStatement,
) -> Result<ControlReply, ProtocolError> {
    let Some(Pending::Ready(pending)) = &state.pending else {
        return fault(Fault::NotFound);
    };
    if pending.transition_id != id
        || pending.phase != ReadyPhase::AwaitingQueueProof
        || pending.timestamp_fence.is_none()
        || proof.ticket
            != (JoinTicket {
                ticket_id: id,
                target: pending.target,
                source_ready: pending.expected,
            })
        || proof.snapshot_id.is_nil()
        || proof.final_cut < proof.snapshot_cut
        || !nonzero(proof.record_version_hash)
    {
        return fault(Fault::Incomplete);
    }
    let expected_source = state.owner.unwrap_or(pending.target);
    if proof.source != expected_source {
        return fault(Fault::Unauthorized);
    }
    if state.ready.ready_count == 0
        && (proof.snapshot_cut != 0
            || proof.final_cut != 0
            || proof.record_version_hash != hash(b"nabedra/ram-queue-cut/v1\0"))
    {
        return fault(Fault::Invalid);
    }
    let pending = pending.clone();
    state.ready = pending.next.ok_or_else(unknown)?;
    state.ready_generation = pending.generation;
    if state.owner.is_none() {
        state.owner = Some(pending.target);
    }
    state.invalid_ready = 0;
    state.pending = None;
    worker_drain::publish_queue_removed(tx, &pending, state.ready)?;
    if let Some(fault) = data_worker::publish_mount(tx, state, &pending)? {
        return Ok(ControlReply::Rejected(fault));
    }
    if pending.change == ReadyChange::Join && state.restart.as_ref().is_some_and(|restart| {
        restart.enrolled && WorkerIdentity::from(&restart.intent.next) == pending.target
    }) {
        state.restart = None;
        state.enrollment_revision = state.enrollment_revision.checked_add(1).ok_or_else(unknown)?;
    }
    Ok(ControlReply::Published {
        ready: state.ready,
        owner: state.owner,
    })
}
fn begin_fence(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    id: Uuid,
    next: ReadyMembership,
    owner: Option<WorkerIdentity>,
    cold: bool,
    extra_exclusion: Option<WorkerIdentity>,
    authorization: AuthorizationStamp,
    position: AppliedPosition,
    command_digest: Digest,
) -> Result<ControlReply, ProtocolError> {
    let pending = FencePending {
        transition_id: id,
        expected: state.ready,
        next,
        next_owner: owner,
        cold,
        authorization,
        group_inventory_revision: state.group_inventory_revision,
        required_groups: state.group_count,
        fenced_groups: 1,
        excluded_workers: 0,
        extra_exclusion,
    };
    save(
        tx,
        &fence_key(id, scope.group_id),
        &FenceAck {
            group: scope.clone(),
            expected: pending.expected.view.authority(),
            next: pending.next.view.authority(),
            applied_at: position,
            command_digest,
        },
    )?;
    state.pending = Some(Pending::Fence(pending));
    Ok(ControlReply::FenceBegun {
        transition_id: id,
        next,
        required_groups: state.group_count,
    })
}

fn current_ready(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    Ok(
        load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, worker.worker_id))?
            == Some(worker)
            && load::<WorkerRecord>(tx, &key(WORKERS, worker.worker_id))?
                .is_some_and(|r| WorkerIdentity::from(&r.enrollment) == worker),
    )
}
fn next_ready(
    tx: &mut GroupTransaction<'_>,
    generation: Uuid,
    after: Option<Uuid>,
) -> Result<Option<WorkerIdentity>, ProtocolError> {
    let prefix = ready_prefix(generation);
    let cursor = after.map(|worker| ready_key(generation, worker));
    let mut next = None;
    tx.scan_page(&prefix, cursor.as_deref(), 1, |_, bytes| {
        next = Some(decode(bytes)?);
        Ok(())
    })?;
    Ok(next)
}
pub(super) fn key(prefix: &[u8], id: Uuid) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(id.as_bytes());
    key
}
pub(super) fn process_exclusion_key(worker: WorkerIdentity) -> Vec<u8> {
    let mut key = key(b"ct/process-exclusion/", worker.worker_id);
    key.extend_from_slice(&worker.enrollment_revision.to_be_bytes());
    key
}
fn record_process_exclusion(tx: &mut GroupTransaction<'_>, stamp: &ExclusionStamp) -> Result<(), ProtocolError> {
    save(tx, &process_exclusion_key(stamp.worker), &stamp.worker)
}
pub(super) fn ready_prefix(generation: Uuid) -> Vec<u8> {
    key(b"ct/ready/", generation)
}
pub(super) fn ready_key(generation: Uuid, worker: Uuid) -> Vec<u8> {
    key(&ready_prefix(generation), worker)
}
pub(super) fn member_prefix(generation: Uuid) -> Vec<u8> {
    key(b"ct/member/", generation)
}
pub(super) fn member_key(generation: Uuid, node: u64) -> Vec<u8> {
    let mut key = member_prefix(generation);
    key.extend_from_slice(&node.to_be_bytes());
    key
}
fn member_key_nested(prefix: &[u8], generation: Uuid, node: u64) -> Vec<u8> {
    let mut result = key(prefix, generation);
    result.extend_from_slice(&node.to_be_bytes());
    result
}
pub(super) fn member_worker_key(generation: Uuid, worker: Uuid) -> Vec<u8> {
    key(&key(b"ct/member-worker/", generation), worker)
}
fn fence_key(transition: Uuid, group: Uuid) -> Vec<u8> {
    key(&key(b"ct/fence/", transition), group)
}
fn exclusion_key(transition: Uuid, worker: Uuid) -> Vec<u8> {
    key(&key(b"ct/exclusion/", transition), worker)
}
fn register_initial_data(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    proof: &GroupMembershipStamp,
    replicas: &[ReplicaMember],
    authorization: &AuthorizationStamp,
) -> Result<ControlReply, ProtocolError> {
    if state.pending.is_some()
        || tx.get(&placement::move_key(proof.generation))?.is_some()
        || tx
            .get(&key(b"ct/group-replacement/", proof.group.group_id))?
            .is_some()
    {
        return fault(Fault::Busy);
    }
    if !stamp(authorization, state.policy)
        || authorization.evidence != crate::oltp_replication::owned::serialized_digest(proof)?
        || !state
            .owner
            .is_some_and(|owner| owner.certificate_sha256 == authorization.subject)
        || !matches!(proof.group.kind, GroupKind::Data { .. })
        || !same_scope(scope, &proof.group)
        || proof.group.validate().is_err()
        || proof.generation.is_nil()
        || proof.authority != state.ready.view.authority()
        || !position_valid(proof.applied_at)
        || proof.applied_at.index == 0
    {
        return fault(Fault::Unauthorized);
    }
    let count = replicas.len() as u64;
    let chain = replicas.iter().fold(group_seed(), group_step);
    if validate_voters(replicas).is_err()
        || replicas
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
        || count != proof.replica_count
        || proof.replica_root != group_root(count, chain)
    {
        return fault(Fault::Invalid);
    }
    for member in replicas {
        if !direct::admits_worker_id(tx, member.worker_id)? {
            return fault(Fault::DirectCapabilityRequired);
        }
    }
    let reply = ControlReply::GroupRegistered {
        group_id: proof.group.group_id,
        count,
        root: proof.replica_root,
    };
    let group_key = key(GROUPS, proof.group.group_id);
    if let Some(existing) = load::<GroupRecord>(tx, &group_key)? {
        return if existing.identity == proof.group
            && existing.generation == proof.generation
            && existing.replica_count == count
            && existing.replica_root == proof.replica_root
        {
            Ok(reply)
        } else {
            fault(Fault::Conflict)
        };
    }
    let build_key = key(b"ct/group-build/", proof.generation);
    let prior_build = load::<GroupBuild>(tx, &build_key)?;
    let staged = if let Some(build) = &prior_build {
        if build.identity != proof.group
            || build.generation != proof.generation
            || build.expected.is_some()
            || build.count > count
            || build.retained_count != 0
            || build.retired_count != 0
        {
            return fault(Fault::Conflict);
        }
        let prefix = &replicas[..build.count as usize];
        if build.last_node != prefix.last().map(|member| member.node_id)
            || build.chain != prefix.iter().fold(group_seed(), group_step)
        {
            return fault(Fault::Conflict);
        }
        prefix.len()
    } else {
        0
    };
    for (index, member) in replicas.iter().enumerate() {
        if pool_drain::blocks_pool(tx, member.pool_id)? || worker_drain::blocks_worker(tx, member.worker_id)? {
            return fault(Fault::Busy);
        }
        if load::<WorkerRecord>(tx, &key(WORKERS, member.worker_id))?.is_none() {
            return fault(Fault::Invalid);
        }
        let expected = (index < staged).then_some(member);
        let node = member_key(proof.generation, member.node_id);
        let worker = member_worker_key(proof.generation, member.worker_id);
        if load::<ReplicaMember>(tx, &node)?.as_ref() != expected
            || load::<ReplicaMember>(tx, &worker)?.as_ref() != expected
        {
            return fault(Fault::Conflict);
        }
    }
    for member in &replicas[staged..] {
        save(tx, &member_key(proof.generation, member.node_id), member)?;
        save(
            tx,
            &member_worker_key(proof.generation, member.worker_id),
            member,
        )?;
    }
    if prior_build.is_some() {
        tx.delete(&build_key)?;
    }
    save(
        tx,
        &group_key,
        &GroupRecord {
            identity: proof.group.clone(),
            generation: proof.generation,
            replica_count: count,
            replica_root: proof.replica_root,
            membership_position: proof.applied_at,
        },
    )?;
    state.group_count = state.group_count.checked_add(1).ok_or_else(unknown)?;
    state.group_inventory_revision = state
        .group_inventory_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    Ok(reply)
}

#[doc(hidden)]
pub fn group_seed() -> Digest {
    hash(b"nabedra/control/group-members/v1")
}
#[doc(hidden)]
pub fn group_step(prior: Digest, member: &ReplicaMember) -> Digest {
    let mut h = Sha256::new();
    h.update(b"nabedra/control/group-member/v1");
    h.update(prior.as_bytes());
    h.update(member.node_id.to_be_bytes());
    h.update(member.worker_id.as_bytes());
    h.update(member.pool_id.as_bytes());
    Digest::from_bytes(h.finalize().into())
}
#[doc(hidden)]
pub fn group_root(count: u64, chain: Digest) -> Digest {
    let mut h = Sha256::new();
    h.update(b"nabedra/control/group-root/v1");
    h.update(count.to_be_bytes());
    h.update(chain.as_bytes());
    Digest::from_bytes(h.finalize().into())
}
pub(super) fn hash(bytes: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(bytes).into())
}
fn nonzero(value: Digest) -> bool {
    value.as_bytes().iter().any(|b| *b != 0)
}
fn stamp(value: &AuthorizationStamp, policy: Digest) -> bool {
    !value.authorization_id.is_nil()
        && value.policy == policy
        && nonzero(policy)
        && nonzero(value.subject)
        && nonzero(value.evidence)
}
fn exclusion(value: &ExclusionStamp, policy: Digest) -> bool {
    value.worker.validate().is_ok()
        && stamp(&value.authorization, policy)
        && nonzero(value.ownership_fence)
}
fn same_scope(control: &GroupIdentity, other: &GroupIdentity) -> bool {
    control.cluster_id == other.cluster_id && control.database == other.database
}
fn position_valid(position: AppliedPosition) -> bool {
    position.term > 0 && position.node_id > 0
}
pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ProtocolError> {
    serde_json::from_slice(bytes).map_err(|_| unavailable("corrupt Control metadata"))
}
fn load<T: DeserializeOwned>(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    tx.get(key)?.map(|v| decode(v.as_ref())).transpose()
}
fn save<T: Serialize>(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
    value: &T,
) -> Result<(), ProtocolError> {
    let bytes = tx.encode_value(value)?;
    tx.put(key, bytes.as_ref())
}
fn fault(value: Fault) -> Result<ControlReply, ProtocolError> {
    Ok(ControlReply::Rejected(value))
}
fn reject(tx: &mut GroupTransaction<'_>, value: Fault) -> Result<Transition, ProtocolError> {
    Ok(Transition::Reject(
        tx.encode(&ControlReply::Rejected(value))?,
    ))
}
fn commit(
    tx: &mut GroupTransaction<'_>,
    value: &ControlReply,
) -> Result<Transition, ProtocolError> {
    Ok(Transition::Commit(tx.encode(value)?))
}
fn unknown() -> ProtocolError {
    unavailable("Control transition cannot be proven")
}

impl crate::oltp_replication::hosted::TypedMachine<super::super::config::ControlConfig>
    for ControlMachine
{
    fn installing_snapshot(&self) -> impl Send {
        self.replacement_trust
            .as_ref()
            .map(|trust| trust.directory.reloading_revocations())
    }
    async fn snapshot_installed(&self) -> Result<(), ProtocolError> {
        if let Some(trust) = &self.replacement_trust {
            trust.directory.require_revocation_snapshot();
        }
        self.refresh_or_retry().await;
        Ok(())
    }
    fn store(&self) -> &GroupStore {
        ControlMachine::store(self)
    }
    async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<ControlCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        ControlMachine::apply(self, meta, command).await
    }
}
