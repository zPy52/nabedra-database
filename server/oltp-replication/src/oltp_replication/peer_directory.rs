//! Transport authentication uses locally installed Control enrollment. Refresh
//! happens outside Raft handlers, so authenticating Control cannot recursively
//! require a Control quorum call through the same saturated listener.
use crate::oltp::{
    mvcc::json_decode_budget,
    pool::{ExecutionClass, ScratchLease, WorkerSharedResources},
};
use crate::oltp_replication::{
    authority::control::{ApiEnrollment, ControlClient, InventoryPage, ReplacementRecord},
    types::{Digest, GroupIdentity, GroupKind, Text, WorkerEnrollment, WorkerIdentity},
};
use crate::protocol::{ErrorCode, Outcome, ProtocolError};
use nabedra_transport::{EndpointFuture, PeerDirectory, Purpose, ReceivedReply, TransportTrust};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

// Keep the existing RPC trace filter and test subscribers working after the move.
mod search;

const TRACE_TARGET: &str = "nabedra_database_server::oltp_replication::multiplex::directory";

/// REP6: a Data-only worker refreshes its revocation view this often.
pub(crate) const REVOCATION_REFRESH: Duration = Duration::from_secs(1);
/// One refresh waits at most this long for a Control majority.
const REVOCATION_READ_TIMEOUT: Duration = Duration::from_secs(2);
/// A view without a successful majority refresh for longer than this is
/// stale. It keeps enforcing what it learned and keeps serving (Data
/// availability does not depend on Control's); see `docs/oltp-recovery.md`.
pub(crate) const REVOCATION_STALE_AFTER: Duration = Duration::from_secs(5);
/// Control retains at most 1,024 revocations plus the one REP4 could commit
/// past its limit (`system_trust::MAX_RETAINED_REVOCATIONS`).
pub(crate) const MAX_VIEW_REVOCATIONS: usize = 1025;

/// Committed system revocations as a majority of Control voters report
/// them. A Data-only worker has no Control replica of its own.
#[doc(hidden)]
pub trait RevocationSource: Send + Sync + 'static {
    fn revocations(&self) -> EndpointFuture<'_, Vec<WorkerIdentity>>;
}

impl RevocationSource for ControlClient {
    fn revocations(&self) -> EndpointFuture<'_, Vec<WorkerIdentity>> {
        Box::pin(self.revocations_quorum())
    }
}

/// The freshness of a Data-only worker's revocation view.
struct RemoteRevocations {
    refreshed: Mutex<Instant>,
    stale: AtomicBool,
}

enum Role {
    Worker,
    Api(Text<128>),
    Search(Text<128>),
    Embed(Text<128>),
    BootstrapControl(GroupIdentity),
    PreparedControl,
}
struct Entry {
    enrollment: WorkerEnrollment,
    role: Role,
    seen: u64,
    _charge: Arc<ScratchLease>,
}
struct SystemTrust {
    revision: u64,
    hardened: bool,
    deferred_data: bool,
    operators: BTreeMap<Uuid, WorkerEnrollment>,
    candidate: Option<(ReplacementRecord, bool)>,
    _charge: ScratchLease,
}

#[derive(Default)]
struct Revocations {
    workers: BTreeSet<Uuid>,
    certificates: BTreeSet<Digest>,
}

pub(crate) struct RevocationReload<'a>(&'a AppliedPeerDirectory);

impl Drop for RevocationReload<'_> {
    fn drop(&mut self) {
        self.0.reloading_revocations.store(false, Ordering::Release);
        // A revocation recorded during the reload wakes sessions only now:
        // while reloading, `resolve` refuses every peer.
        self.0.wake_sessions();
    }
}

#[doc(hidden)]
pub struct AppliedPeerDirectory {
    cluster: Uuid,
    control: GroupIdentity,
    shared: Arc<WorkerSharedResources>,
    entries: RwLock<BTreeMap<Uuid, Entry>>,
    prepared: RwLock<Option<Entry>>,
    system: RwLock<Option<SystemTrust>>,
    learner_groups: RwLock<BTreeMap<Uuid, BTreeSet<Uuid>>>,
    revision: AtomicU64,
    epoch: AtomicU64,
    cold_bootstrap: AtomicBool,
    reloading_revocations: AtomicBool,
    revocations_ready: AtomicBool,
    revocations: RwLock<Revocations>,
    revocation_memory: std::sync::OnceLock<ScratchLease>,
    remote: std::sync::OnceLock<RemoteRevocations>,
    trust_changes: tokio::sync::watch::Sender<u64>,
    refresh: tokio::sync::Mutex<()>,
    #[cfg(test)]
    pub(crate) fail_replacement_trust_install: AtomicBool,
}
impl AppliedPeerDirectory {
    /// Bootstrap pins are an explicit operator-supplied Control peer set. They
    /// grant consensus/enrollment reads only, never application or queue access.
    /// Retained node reuse still requires the manager's actual exclusion proof.
    #[doc(hidden)]
    pub fn from_bootstrap(
        control: GroupIdentity,
        peers: Vec<WorkerEnrollment>,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Arc<Self>, ProtocolError> {
        control.validate().map_err(|_| invalid())?;
        if control.kind != GroupKind::Control || peers.is_empty() || peers.len() > 128 {
            return Err(invalid());
        }
        let directory = Arc::new(Self {
            cluster: control.cluster_id,
            control: control.clone(),
            shared,
            entries: RwLock::new(BTreeMap::new()),
            prepared: RwLock::new(None),
            system: RwLock::new(None),
            learner_groups: RwLock::new(BTreeMap::new()),
            revision: AtomicU64::new(0),
            epoch: AtomicU64::new(1),
            cold_bootstrap: AtomicBool::new(false),
            reloading_revocations: AtomicBool::new(false),
            revocations_ready: AtomicBool::new(true),
            revocations: RwLock::new(Revocations::default()),
            revocation_memory: std::sync::OnceLock::new(),
            remote: std::sync::OnceLock::new(),
            trust_changes: tokio::sync::watch::Sender::new(0),
            refresh: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            fail_replacement_trust_install: AtomicBool::new(false),
        });
        for peer in peers {
            if directory
                .entries
                .read()
                .map_err(|_| unavailable())?
                .contains_key(&peer.worker_id)
            {
                return Err(invalid());
            }
            directory.install(peer, Role::BootstrapControl(control.clone()), 0)?;
        }
        Ok(directory)
    }
    /// REP6: seeds a Data-only worker's revocation view from a Control
    /// majority. Each exchange then checks the view in memory only
    /// ([`PeerDirectory::revoked`], [`PeerDirectory::verify`]); the view is
    /// kept current by [`Self::run_revocation_refresh`].
    #[doc(hidden)]
    pub async fn seed_remote_revocations(
        &self,
        source: &dyn RevocationSource,
    ) -> Result<(), ProtocolError> {
        self.reserve_revocations(MAX_VIEW_REVOCATIONS)?;
        let revoked = source.revocations().await?;
        self.learn_revocations(revoked)?;
        let now = nabedra_rt::time::now_std();
        if let Err(view) = self.remote.set(RemoteRevocations {
            refreshed: Mutex::new(now),
            stale: AtomicBool::new(false),
        }) {
            drop(view);
            self.mark_refreshed(now);
        }
        Ok(())
    }

    /// Refreshes the view every [`REVOCATION_REFRESH`] with one majority
    /// read. A failed refresh keeps every revocation already learned (Control
    /// never deletes one) and keeps the worker serving; past
    /// [`REVOCATION_STALE_AFTER`] it logs that the view is stale.
    #[doc(hidden)]
    pub async fn run_revocation_refresh(
        self: Arc<Self>,
        source: Arc<dyn RevocationSource>,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut interval = nabedra_rt::time::interval(REVOCATION_REFRESH);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            if *stop.borrow() {
                return;
            }
            tokio::select! {
                changed = stop.changed() => if changed.is_err() || *stop.borrow() { return },
                _ = interval.tick() => {}
            }
            let read = nabedra_rt::time::timeout(REVOCATION_READ_TIMEOUT, source.revocations());
            let result = tokio::select! {
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { return }
                    continue
                }
                result = read => result.unwrap_or_else(|_| Err(unavailable())),
            };
            self.refreshed(result.and_then(|revoked| self.learn_revocations(revoked)));
        }
    }

    fn refreshed(&self, result: Result<usize, ProtocolError>) {
        use nabedra_resources::perf_stage;
        let Some(remote) = self.remote.get() else {
            return;
        };
        match result {
            Ok(learned) => {
                perf_stage!("peer.revocations.refresh").record(Duration::ZERO);
                self.mark_refreshed(nabedra_rt::time::now_std());
                if learned > 0 {
                    tracing::info!(target: TRACE_TARGET, learned, "Data-only revocation view learned revocations");
                }
            }
            Err(error) => {
                perf_stage!("peer.revocations.refresh_failed").record(Duration::ZERO);
                let age = self.revocation_view_age().unwrap_or_default();
                if age > REVOCATION_STALE_AFTER && !remote.stale.swap(true, Ordering::AcqRel) {
                    tracing::warn!(target: TRACE_TARGET, code=?error.code, age_ms=age.as_millis() as u64,
                        "Data-only revocation view is stale: no Control majority refresh; \
                         enforcing the revocations already learned");
                }
            }
        }
    }

    fn mark_refreshed(&self, now: Instant) {
        if let Some(remote) = self.remote.get() {
            *remote
                .refreshed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = now;
            if remote.stale.swap(false, Ordering::AcqRel) {
                tracing::info!(target: TRACE_TARGET, "Data-only revocation view refreshed from a Control majority again");
            }
        }
    }

    /// Time since the last successful majority refresh of a Data-only
    /// worker's revocation view; `None` where Control is local.
    pub(crate) fn revocation_view_age(&self) -> Option<Duration> {
        let remote = self.remote.get()?;
        Some(nabedra_rt::time::elapsed(
            *remote
                .refreshed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        ))
    }

    /// Records each revocation not yet known and drops its directory entry.
    fn learn_revocations(&self, revoked: Vec<WorkerIdentity>) -> Result<usize, ProtocolError> {
        if revoked.len() > MAX_VIEW_REVOCATIONS {
            return Err(invalid());
        }
        let mut learned = 0;
        for worker in revoked {
            if self.record_revocation(worker) {
                self.remove_replaced(worker)?;
                learned += 1;
            }
        }
        Ok(learned)
    }

    #[doc(hidden)]
    pub async fn exclude_replaced_bootstrap_peers(
        &self,
        control: &ControlClient,
        peers: &[WorkerIdentity],
    ) -> Result<(), ProtocolError> {
        for &peer in peers {
            if control.worker_exclusion_quorum(peer).await? {
                self.remove_replaced(peer)?;
            }
        }
        Ok(())
    }

    fn remove_replaced(&self, peer: WorkerIdentity) -> Result<(), ProtocolError> {
        let mut entries = self.entries.write().map_err(|_| unavailable())?;
        if entries
            .get(&peer.worker_id)
            .is_some_and(|entry| WorkerIdentity::from(&entry.enrollment) == peer)
        {
            entries.remove(&peer.worker_id);
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn set_cold_bootstrap(&self, cold: bool) {
        self.cold_bootstrap.store(cold, Ordering::Release);
    }

    // A complete cold restart must first rebuild the current Control quorum
    // with the retained disks. The configured replacement incarnations only
    // bridge the gap until the existing cold fence authorizes their enrollment.
    fn cold_peer(&self, enrolled: &WorkerEnrollment) -> Option<WorkerEnrollment> {
        if !self.cold_bootstrap.load(Ordering::Acquire) {
            return None;
        }
        let entries = self.entries.read().ok()?;
        let entry = entries.get(&enrolled.worker_id)?;
        (matches!(entry.role, Role::BootstrapControl(_))
            && entry.enrollment.enrollment_revision > enrolled.enrollment_revision)
            .then(|| entry.enrollment.clone())
    }

    pub(crate) fn allow_replacement_bootstrap(
        &self,
        record: &ReplacementRecord,
    ) -> Result<(), ProtocolError> {
        let mut groups = self.learner_groups.write().map_err(|_| unavailable())?;
        for plan in &record.groups {
            for member in &plan.old {
                if member.worker_id != record.intent.previous.worker_id {
                    groups
                        .entry(member.worker_id)
                        .or_default()
                        .insert(plan.expected.identity.group_id);
                }
            }
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn install_system_trust(
        &self,
        revision: u64,
        operators: Vec<WorkerEnrollment>,
        candidate: Option<(ReplacementRecord, bool)>,
        hardened: bool,
        deferred_data: bool,
    ) -> Result<(), ProtocolError> {
        #[cfg(test)]
        if self.fail_replacement_trust_install.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let charge = self.shared.reserve_scratch(
            crate::oltp::pool::ByteClass::Progress,
            4096 + candidate
                .as_ref()
                .map_or(0, |(record, _)| record.groups.len() * 2048),
        )?;
        *self.system.write().map_err(|_| unavailable())? = Some(SystemTrust {
            revision,
            hardened,
            deferred_data,
            operators: operators
                .into_iter()
                .map(|entry| (entry.worker_id, entry))
                .collect(),
            candidate,
            _charge: charge,
        });
        Ok(())
    }

    pub(crate) fn reserve_revocations(&self, limit: usize) -> Result<(), ProtocolError> {
        if self.revocation_memory.get().is_none() {
            let lease = self
                .shared
                .reserve_scratch(crate::oltp::pool::ByteClass::Progress, limit * 128)?;
            let _ = self.revocation_memory.set(lease);
        }
        Ok(())
    }

    pub(crate) fn reloading_revocations(&self) -> RevocationReload<'_> {
        self.reloading_revocations.store(true, Ordering::Release);
        RevocationReload(self)
    }

    pub(crate) fn require_revocation_snapshot(&self) {
        self.revocations_ready.store(false, Ordering::Release);
    }

    pub(crate) fn install_revocations(&self, revoked: Vec<WorkerIdentity>) {
        let mut learned = false;
        for worker in revoked {
            learned |= self.record_revocation(worker);
        }
        self.revocations_ready.store(true, Ordering::Release);
        if learned {
            self.wake_sessions();
        }
    }

    /// Records `worker` as revoked. A new revocation wakes every session
    /// ([`PeerDirectory::trust_changes`]) so a revoked peer's sessions close
    /// now rather than at their next directory check.
    pub(crate) fn record_revocation(&self, worker: WorkerIdentity) -> bool {
        let learned = {
            let mut revoked = self
                .revocations
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let id = revoked.workers.insert(worker.worker_id);
            revoked.certificates.insert(worker.certificate_sha256) || id
        };
        if learned {
            self.wake_sessions();
        }
        learned
    }

    /// Wakes every session to recheck its peer ([`PeerDirectory::trust_changes`]).
    /// Not while trust is unavailable: sessions would see every peer refused.
    fn wake_sessions(&self) {
        if self.trust_available() {
            self.trust_changes
                .send_modify(|generation| *generation = generation.wrapping_add(1));
        }
    }

    fn trust_available(&self) -> bool {
        self.revocations_ready.load(Ordering::Acquire)
            && !self.reloading_revocations.load(Ordering::Acquire)
    }

    pub(crate) fn replacement_hardening_active(&self) -> bool {
        self.system
            .read()
            .is_ok_and(|system| system.as_ref().is_some_and(|state| state.hardened))
    }

    #[doc(hidden)]
    pub fn has_deferred_data_repairs(&self) -> bool {
        self.system
            .read()
            .is_ok_and(|system| system.as_ref().is_some_and(|state| state.deferred_data))
    }

    #[doc(hidden)]
    pub fn has_mounted_replacement(&self) -> bool {
        self.system.read().is_ok_and(|system| {
            system.as_ref().is_some_and(|state| {
                state
                    .candidate
                    .as_ref()
                    .is_some_and(|(_, mounted)| *mounted)
            })
        })
    }

    pub(crate) fn system_placement_changed(&self) -> bool {
        self.revocations
            .read()
            .is_ok_and(|revoked| !revoked.workers.is_empty())
    }

    pub(crate) fn current_operator(&self, identity: WorkerIdentity) -> Option<bool> {
        if !self.trust_available()
            || self.denied(identity.worker_id, Some(identity.certificate_sha256))
        {
            return Some(false);
        }
        match self.system.read() {
            Ok(state) => state.as_ref().map(|state| {
                state
                    .operators
                    .get(&identity.worker_id)
                    .is_some_and(|entry| {
                        WorkerIdentity::from(entry) == identity
                            || self.cold_peer(entry).as_ref().map(WorkerIdentity::from)
                                == Some(identity)
                    })
            }),
            Err(_) => Some(false),
        }
    }

    fn denied(&self, worker: Uuid, certificate: Option<Digest>) -> bool {
        self.revocations.read().map_or(true, |revoked| {
            revoked.workers.contains(&worker)
                || certificate.is_some_and(|digest| revoked.certificates.contains(&digest))
        })
    }

    fn install(
        &self,
        enrollment: WorkerEnrollment,
        role: Role,
        seen: u64,
    ) -> Result<(), ProtocolError> {
        enrollment.validate().map_err(|_| invalid())?;
        let bytes = std::mem::size_of::<Entry>()
            + 256
            + enrollment.endpoint.capacity_bytes()
            + enrollment.server_name.capacity_bytes()
            + search::role_bytes(&role);
        let mut entries = self.entries.write().map_err(|_| unavailable())?;
        if let Some(old) = entries.get_mut(&enrollment.worker_id) {
            if search::relabels(&old.role, &role) {
                return Err(invalid());
            }
            if old.enrollment.enrollment_revision > enrollment.enrollment_revision {
                old.seen = seen;
                return Ok(());
            }
            if old.enrollment.enrollment_revision == enrollment.enrollment_revision {
                if old.enrollment != enrollment {
                    return Err(invalid());
                }
                if !matches!(old.role, Role::BootstrapControl(_)) {
                    old.seen = seen;
                    return Ok(());
                }
            }
        }
        // Reserve the replacement before publishing it. A failed refresh keeps
        // the prior verified entry; it never relabels an API as a worker.
        let charge = Arc::new(
            self.shared
                .reserve_scratch(crate::oltp::pool::ByteClass::Progress, bytes)?,
        );
        entries.insert(
            enrollment.worker_id,
            Entry {
                enrollment,
                role,
                seen,
                _charge: charge,
            },
        );
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn install_worker_for_test(
        &self,
        enrollment: WorkerEnrollment,
    ) -> Result<(), ProtocolError> {
        self.install(enrollment, Role::Worker, self.epoch.load(Ordering::Relaxed))
    }
    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
    #[doc(hidden)]
    pub async fn refresh(&self, control: &ControlClient) -> Result<u64, ProtocolError> {
        let _order = self.refresh.lock().await;
        let revision = control.enrollment_revision().await?;
        if revision != 0 && revision == self.revision() {
            return Ok(revision);
        }
        let epoch = self
            .epoch
            .load(Ordering::Relaxed)
            .checked_add(1)
            .ok_or_else(unavailable)?;
        self.epoch.store(epoch, Ordering::Relaxed);
        let mut after = None;
        loop {
            let response = control.workers(revision, after).await?;
            let (page, _owner) = self.decode::<WorkerEnrollment>(response).await?;
            check_page(&page, revision, after)?;
            for worker in page.items.iter() {
                if after.is_some_and(|old| worker.worker_id <= old) {
                    return Err(invalid());
                }
                after = Some(worker.worker_id);
                self.install(worker.clone(), Role::Worker, epoch)?;
            }
            if page.after != after {
                return Err(invalid());
            }
            if page.done {
                break;
            }
        }
        after = None;
        loop {
            let response = control.apis(revision, after).await?;
            let (page, _owner) = self.decode::<ApiEnrollment>(response).await?;
            check_page(&page, revision, after)?;
            for api in page.items.iter() {
                if after.is_some_and(|old| api.enrollment.worker_id <= old) {
                    return Err(invalid());
                }
                after = Some(api.enrollment.worker_id);
                self.install(
                    api.enrollment.clone(),
                    Role::Api(api.database.clone()),
                    epoch,
                )?;
            }
            if page.after != after {
                return Err(invalid());
            }
            if page.done {
                break;
            }
        }
        self.refresh_members(control, epoch).await?;
        let prepared = control.restart_preparation(revision).await?;
        let restart = prepared
            .filter(|prepared| !prepared.enrolled)
            .map(|prepared| {
                prepared.intent.validate()?;
                Ok::<_, ProtocolError>(prepared.intent.next)
            })
            .transpose()?;
        let data = control.prepared_data_worker(revision).await?;
        if restart.is_some() && data.is_some() {
            return Err(invalid());
        }
        let candidate = restart
            .or(data)
            .map(|enrollment| {
                enrollment.validate().map_err(|_| invalid())?;
                let bytes = std::mem::size_of::<Entry>()
                    + 256
                    + enrollment.endpoint.capacity_bytes()
                    + enrollment.server_name.capacity_bytes();
                let charge = self
                    .shared
                    .reserve_scratch(crate::oltp::pool::ByteClass::Progress, bytes)?;
                Ok::<_, ProtocolError>(Entry {
                    enrollment,
                    role: Role::PreparedControl,
                    seen: epoch,
                    _charge: Arc::new(charge),
                })
            })
            .transpose()?;
        *self.prepared.write().map_err(|_| unavailable())? = candidate;
        self.entries
            .write()
            .map_err(|_| unavailable())?
            .retain(|_, entry| entry.seen == epoch);
        self.revision.store(revision, Ordering::Release);
        Ok(revision)
    }
    /// The worker owns and joins this task. Refresh is independent of query and
    /// Raft handlers, and stopping it does not infer exclusion of any process.
    #[doc(hidden)]
    pub async fn run_refresh(
        self: Arc<Self>,
        control: Arc<ControlClient>,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut interval = nabedra_rt::time::interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if *stop.borrow() {
                return;
            }
            tokio::select! {
                changed=stop.changed()=>{if changed.is_err()||*stop.borrow(){return}},
                _=interval.tick()=>{
                    let result=tokio::select! {
                        changed=stop.changed()=>{if changed.is_err()||*stop.borrow(){return}continue},
                        result=self.refresh(&control)=>result,
                    };
                    if let Err(error)=result{tracing::warn!(target: TRACE_TARGET, code=?error.code,outcome=?error.outcome,"Control enrollment refresh incomplete; prior verified peers retained");}
                }
            }
        }
    }
    async fn decode<T: DeserializeOwned + Send + 'static>(
        &self,
        reply: ReceivedReply,
    ) -> Result<(InventoryPage<T>, Arc<ScratchLease>), ProtocolError> {
        let raw = reply.sections[0].as_ref().ok_or_else(invalid)?.clone();
        let bytes = self
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                json_decode_budget(raw.as_slice())
            })
            .await?;
        let reply = reply.reserve_reply_decode(&self.shared, bytes).await?;
        self.shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let raw = reply.sections[0].as_ref().ok_or_else(invalid)?;
                let page = serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
                Ok((page, reply.scratch_lease()))
            })
            .await
    }
}
impl PeerDirectory for AppliedPeerDirectory {
    fn applied_revision(&self) -> Option<u64> {
        let revision = self
            .system
            .read()
            .ok()
            .and_then(|system| system.as_ref().map(|state| state.revision))
            .unwrap_or(0);
        Some(self.revision().max(revision))
    }
    fn revoked(&self, peer: WorkerIdentity) -> bool {
        !self.trust_available() || self.denied(peer.worker_id, Some(peer.certificate_sha256))
    }
    fn trust_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.trust_changes.subscribe())
    }
    fn is_api(&self, peer: WorkerIdentity) -> bool {
        self.entries.read().is_ok_and(|entries| {
            entries.get(&peer.worker_id).is_some_and(|entry| {
                matches!(entry.role, Role::Api(_))
                    && WorkerIdentity::from(&entry.enrollment) == peer
            })
        })
    }
    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        if !self.trust_available() {
            return Err(unavailable());
        }
        if self.denied(worker, None) {
            return Err(unavailable());
        }
        if let Some((record, _)) = self
            .system
            .read()
            .map_err(|_| unavailable())?
            .as_ref()
            .and_then(|state| state.candidate.as_ref())
            && record.intent.next.worker_id == worker
        {
            return Ok(record.intent.next.clone());
        }
        // During a retained restart the prepared entry must override the
        // previous operator, so applied operators are a later fallback.
        if let Some(candidate) = self
            .prepared
            .read()
            .map_err(|_| unavailable())?
            .as_ref()
            .filter(|candidate| candidate.enrollment.worker_id == worker)
        {
            return Ok(candidate.enrollment.clone());
        }
        if let Some(enrollment) = self
            .system
            .read()
            .map_err(|_| unavailable())?
            .as_ref()
            .and_then(|state| state.operators.get(&worker))
        {
            return Ok(self
                .cold_peer(enrollment)
                .unwrap_or_else(|| enrollment.clone()));
        }
        self.entries
            .read()
            .map_err(|_| unavailable())?
            .get(&worker)
            .map(|entry| entry.enrollment.clone())
            .ok_or_else(unavailable)
    }
    fn verify(
        &self,
        peer: WorkerIdentity,
        group: &GroupIdentity,
        purpose: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        if group.cluster_id != self.cluster {
            return Err(invalid());
        }
        if !self.trust_available() {
            return Err(unavailable());
        }
        if self.denied(peer.worker_id, Some(peer.certificate_sha256)) {
            return Err(invalid());
        }
        if let Some(state) = self.system.read().map_err(|_| unavailable())?.as_ref()
            && let Some((record, mounted)) = &state.candidate
            && record.intent.next.worker_id == peer.worker_id
        {
            if WorkerIdentity::from(&record.intent.next) != peer {
                return Err(invalid());
            }
            let admitted = (group == &self.control && purpose == Purpose::SystemRecovery)
                || (*mounted
                    && matches!(
                        purpose,
                        Purpose::Raft | Purpose::Snapshot | Purpose::RaftProgress
                    )
                    && record
                        .groups
                        .iter()
                        .any(|plan| plan.expected.identity == *group))
                || (*mounted
                    && state
                        .operators
                        .get(&peer.worker_id)
                        .is_some_and(|entry| WorkerIdentity::from(entry) == peer)
                    && group == &self.control
                    && matches!(purpose, Purpose::Authority | Purpose::ControlDelegation));
            return if admitted {
                Ok(TransportTrust::AppliedEnrollment)
            } else {
                Err(unavailable())
            };
        }
        if let Some(candidate) = self
            .prepared
            .read()
            .map_err(|_| unavailable())?
            .as_ref()
            .filter(|candidate| candidate.enrollment.worker_id == peer.worker_id)
        {
            if WorkerIdentity::from(&candidate.enrollment) != peer {
                return Err(invalid());
            }
            return if group == &self.control && purpose == Purpose::ControlDelegation {
                Ok(TransportTrust::PreparedControl)
            } else if group == &self.control && purpose == Purpose::SystemRecovery {
                Ok(TransportTrust::AppliedEnrollment)
            } else {
                // The worker's voter placements still point here during a
                // retained restart. Until enrollment, route callers must
                // continue to the surviving replicas.
                Err(unavailable())
            };
        }
        if let Some(operator) = self
            .system
            .read()
            .map_err(|_| unavailable())?
            .as_ref()
            .and_then(|state| state.operators.get(&peer.worker_id))
        {
            if WorkerIdentity::from(operator) == peer {
                return Ok(TransportTrust::AppliedEnrollment);
            }
            if self.cold_peer(operator).as_ref().map(WorkerIdentity::from) == Some(peer)
                && group == &self.control
                && matches!(
                    purpose,
                    Purpose::Raft
                        | Purpose::Authority
                        | Purpose::ControlDelegation
                        | Purpose::SystemRecovery
                )
            {
                return Ok(TransportTrust::BootstrapControl);
            }
            return Err(invalid());
        }
        let entries = self.entries.read().map_err(|_| unavailable())?;
        let entry = entries.get(&peer.worker_id).ok_or_else(unavailable)?;
        if WorkerIdentity::from(&entry.enrollment) != peer {
            return Err(invalid());
        }
        if let Some(admitted) = search::verify(&entry.role, group, purpose) {
            return admitted.then_some(TransportTrust::AppliedEnrollment).ok_or_else(invalid);
        }
        match &entry.role {
            Role::Worker => Ok(TransportTrust::AppliedEnrollment),
            Role::Api(database)
                if group.kind == GroupKind::Control
                    && group.database == *database
                    && matches!(
                        purpose,
                        Purpose::Authority | Purpose::ApiIngress | Purpose::ApiObservation
                    ) =>
            {
                Ok(TransportTrust::AppliedEnrollment)
            }
            // An API gateway sends `Method::Direct` / `DirectForward` to the range
            // leader of its own database's Data groups (T5/T6). Only `ApiIngress`;
            // the receiver still checks the method and the Control grant (T9 finding:
            // without this arm every API gateway point operation ended unknown).
            Role::Api(database)
                if matches!(group.kind, GroupKind::Data { .. })
                    && group.database == *database
                    && purpose == Purpose::ApiIngress =>
            {
                Ok(TransportTrust::AppliedEnrollment)
            }
            Role::BootstrapControl(_)
                if (matches!(
                    purpose,
                    Purpose::Raft | Purpose::Snapshot | Purpose::RaftProgress
                ) || (group == &self.control && purpose == Purpose::SystemRecovery))
                    && self
                        .learner_groups
                        .read()
                        .map_err(|_| unavailable())?
                        .get(&peer.worker_id)
                        .is_some_and(|groups| groups.contains(&group.group_id)) =>
            {
                Ok(TransportTrust::AppliedEnrollment)
            }
            Role::BootstrapControl(control)
                if group == control
                    && matches!(
                        purpose,
                        Purpose::Raft
                            | Purpose::Authority
                            | Purpose::ControlDelegation
                            | Purpose::SystemRecovery
                    ) =>
            {
                Ok(TransportTrust::BootstrapControl)
            }
            _ => Err(invalid()),
        }
    }
}
fn check_page<T>(
    page: &InventoryPage<T>,
    revision: u64,
    after: Option<Uuid>,
) -> Result<(), ProtocolError> {
    if page.revision != revision
        || (!page.done && page.items.is_empty())
        || page
            .after
            .is_some_and(|cursor| after.is_some_and(|old| cursor <= old))
            && !page.items.is_empty()
    {
        return Err(invalid());
    }
    Ok(())
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "peer identity or service role differs from applied Control enrollment",
        Outcome::NotApplied,
    )
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "peer enrollment view is unavailable",
        Outcome::Unknown,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use nabedra_resources::config::WorkerResources;
    use crate::{
        oltp::pool::SharedResourceConfig,
        oltp_replication::types::{Digest, Text},
    };
    fn enrollment() -> WorkerEnrollment {
        WorkerEnrollment {
            worker_id: nabedra_rt::ids::uuid_v4(),
            incarnation: nabedra_rt::ids::uuid_v4(),
            certificate_sha256: Digest::from_bytes([3; 32]),
            endpoint: Text::try_from("127.0.0.1:1234").unwrap(),
            server_name: Text::try_from("localhost").unwrap(),
            enrollment_revision: 1,
        }
    }

    #[test]
    fn r03_prepared_restart_routes_past_the_unavailable_voter() {
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let control = GroupIdentity {
            cluster_id: nabedra_rt::ids::uuid_v4(),
            database: Text::try_from("tenant").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Control,
            group_format_version: 1,
        };
        let old = enrollment();
        let directory = AppliedPeerDirectory::from_bootstrap(
            control.clone(),
            vec![old.clone()],
            shared.clone(),
        )
        .unwrap();
        let next = WorkerEnrollment {
            incarnation: nabedra_rt::ids::uuid_v4(),
            enrollment_revision: 2,
            ..old.clone()
        };
        *directory.prepared.write().unwrap() = Some(Entry {
            enrollment: next.clone(),
            role: Role::PreparedControl,
            seen: 1,
            _charge: Arc::new(
                shared
                    .reserve_scratch(crate::oltp::pool::ByteClass::Progress, 1024)
                    .unwrap(),
            ),
        });
        let target = WorkerIdentity::from(&directory.resolve(old.worker_id).unwrap());
        assert_eq!(target, WorkerIdentity::from(&next));
        assert_eq!(
            directory.verify(target, &control, Purpose::ControlDelegation),
            Ok(TransportTrust::PreparedControl)
        );
        assert_eq!(
            directory.verify(target, &control, Purpose::Raft),
            Err(unavailable())
        );
        assert_eq!(
            directory.verify(
                WorkerIdentity::from(&old),
                &control,
                Purpose::ControlDelegation
            ),
            Err(invalid())
        );
        let data = GroupIdentity {
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Data {
                table_id: nabedra_rt::ids::uuid_v4(),
                shard_id: nabedra_rt::ids::uuid_v4(),
            },
            ..control
        };
        for purpose in [Purpose::Raft, Purpose::Authority, Purpose::ServiceMutation] {
            assert_eq!(directory.verify(target, &data, purpose), Err(unavailable()));
        }
    }

    #[test]
    fn o15_api_transport_role_cannot_become_a_voter_or_queue_copy_source() {
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let baseline = shared.control_bytes_used();
        let control = GroupIdentity {
            cluster_id: nabedra_rt::ids::uuid_v4(),
            database: Text::try_from("tenant").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Control,
            group_format_version: 1,
        };
        let worker = enrollment();
        let directory = AppliedPeerDirectory::from_bootstrap(
            control.clone(),
            vec![worker.clone()],
            shared.clone(),
        )
        .unwrap();
        let identity = WorkerIdentity::from(&worker);
        assert_eq!(
            directory.verify(identity, &control, Purpose::Raft).unwrap(),
            TransportTrust::BootstrapControl
        );
        assert!(
            directory
                .verify(identity, &control, Purpose::ApiIngress)
                .is_err()
        );
        let mut data = control.clone();
        data.kind = GroupKind::Data {
            table_id: nabedra_rt::ids::uuid_v4(),
            shard_id: nabedra_rt::ids::uuid_v4(),
        };
        data.group_id = nabedra_rt::ids::uuid_v4();
        assert!(directory.verify(identity, &data, Purpose::Raft).is_err());
        // This unit checks the cache's defense in depth. Production upgrades
        // come only from refresh's actual Control ReadIndex inventory pages.
        directory.install(worker, Role::Worker, 1).unwrap();
        assert_eq!(
            directory.verify(identity, &data, Purpose::Raft).unwrap(),
            TransportTrust::AppliedEnrollment
        );
        let mut api = enrollment();
        directory
            .install(api.clone(), Role::Api(control.database.clone()), 1)
            .unwrap();
        let old = WorkerIdentity::from(&api);
        for purpose in [
            Purpose::Raft,
            Purpose::Snapshot,
            Purpose::Storage,
            Purpose::QueueControl,
            Purpose::QueueData,
        ] {
            assert!(directory.verify(old, &control, purpose).is_err());
        }
        for purpose in [
            Purpose::Authority,
            Purpose::ApiIngress,
            Purpose::ApiObservation,
        ] {
            assert_eq!(
                directory.verify(old, &control, purpose).unwrap(),
                TransportTrust::AppliedEnrollment
            );
        }
        // T9: an API reaches its database's Data groups with `ApiIngress` only.
        assert_eq!(
            directory.verify(old, &data, Purpose::ApiIngress).unwrap(),
            TransportTrust::AppliedEnrollment
        );
        for purpose in [
            Purpose::Raft,
            Purpose::Snapshot,
            Purpose::Storage,
            Purpose::Authority,
            Purpose::ApiObservation,
            Purpose::Settlement,
        ] {
            assert!(directory.verify(old, &data, purpose).is_err());
        }
        let mut foreign_data = data.clone();
        foreign_data.database = Text::try_from("other").unwrap();
        assert!(
            directory
                .verify(old, &foreign_data, Purpose::ApiIngress)
                .is_err()
        );
        let mut catalog = data.clone();
        catalog.kind = GroupKind::Catalog;
        assert!(
            directory
                .verify(old, &catalog, Purpose::ApiIngress)
                .is_err()
        );
        let mut foreign = control.clone();
        foreign.database = Text::try_from("other").unwrap();
        assert!(
            directory
                .verify(old, &foreign, Purpose::ApiIngress)
                .is_err()
        );
        assert!(directory.install(api.clone(), Role::Worker, 2).is_err());
        api.incarnation = nabedra_rt::ids::uuid_v4();
        api.enrollment_revision = 2;
        api.certificate_sha256 = Digest::from_bytes([4; 32]);
        directory
            .install(api.clone(), Role::Api(control.database.clone()), 2)
            .unwrap();
        assert!(
            directory
                .verify(old, &control, Purpose::ApiIngress)
                .is_err()
        );
        assert!(
            directory
                .verify(WorkerIdentity::from(&api), &control, Purpose::ApiIngress)
                .is_ok()
        );
        api.endpoint = Text::try_from("127.0.0.1:2345").unwrap();
        assert!(
            directory
                .install(api, Role::Api(control.database.clone()), 2)
                .is_err()
        );
        assert!(shared.control_bytes_used() > baseline);
        drop(directory);
        assert_eq!(shared.control_bytes_used(), baseline);
    }
}
