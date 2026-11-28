//! The actual API runtime composes durable authority/participants with the
//! worker-memory queue. Bodies are never submitted to Raft or a request WAL.
mod admission_cleanup;
pub(crate) mod archive;
mod automatic_split;
pub(super) mod cold_history;
pub(crate) mod direct_capability;
mod deployment;
mod direct_gateway;
mod index_admin;
mod index_backend;
mod index_reclaim;
mod migration;
mod placement;
mod pool_drain;
mod query_statistics;
mod queued_attempt;
mod recovery;
pub(super) mod retirement;
#[cfg(test)]
mod shard_route;
mod split;
pub(super) mod system_recovery;
mod worker_drain;
use super::{
    admission::{DurableAdmission, ReservationResult},
    application::{
        ApplicationObservation, ApplicationReadView, INLINE_RECEIPT_BYTES, ObservedApplication,
    },
    bootstrap::LocalServices,
    http_runtime::{HttpRuntime, OwnedReceipt},
    replica::*,
    transaction::{RETRY_PROGRESS_TIMEOUT, TransactionCoordinator},
    worker_endpoint::AuthorityReader,
};
use crate::{
    config::{ExecutionConfig, WorkerResources},
    executor::Executor,
    oltp::{
        mvcc::{
            catalog::{CaptureRead, CatalogCapture, IntentRef},
            planner::{NativePlanner, PlanError, PlannedOperation},
        },
        pool::{
            ByteClass, ChargedBytes, ExecutionClass, ScratchLease, SharedResourceConfig,
            WorkerSharedResources,
        },
    },
    oltp_replication::{
        authority::status::{IdentityLookup, IdentityReservation, RequestKind, TerminalIdentity},
        multiplex::EndpointFuture,
        types::*,
    },
    protocol::{self, ErrorCode, Outcome, ProtocolError},
    request_queue::{QueueError, Receipt, RequestRecord, RequestState},
    response::{self, EncodedResponse},
    scheduling::{MemoryCharge, ResourceScheduler},
};
use queued_attempt::QueuedAttempt;
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use uuid::Uuid;

pub(crate) struct WorkerRuntime {
    services: LocalServices,
    admission: DurableAdmission,
    planner: NativePlanner,
    transactions: TransactionCoordinator,
    scoped: Option<Arc<super::scoped_worker::ScopedWorker>>,
    ingress: Executor,
    /// HTTP admission for direct-gateway requests (PERF1).
    direct_ingress: Executor,
    actors: Executor,
    control_ingress: Executor,
    index_builds: Executor,
    supervision: Executor,
    active: Mutex<BTreeSet<Uuid>>,
    recovery_cursor: Mutex<Option<Uuid>>,
    retirement_cursor: Mutex<Option<u64>>,
    recovery_probe: Mutex<recovery::RecoveryProbe>,
    gates: Mutex<BTreeMap<Uuid, Weak<AsyncMutex<()>>>>,
    changed: Notify,
    admission_cleanup: admission_cleanup::AdmissionCleanup,
    accepting: AtomicBool,
    /// Set while `data.direct.v1` is active (T7). Direct commits have no
    /// Status decision, so incremental backups would miss them.
    direct_commits_active: AtomicBool,
    /// The direct-path gateway of this worker's HTTP API (T6).
    direct_gateway: std::sync::OnceLock<Arc<super::application::direct::DirectGateway>>,
    /// This worker's `data.direct.v1` view and `direct_oltp` switch (T7).
    direct: std::sync::OnceLock<Arc<direct_capability::DirectSwitch>>,
    authorization: OriginalAuthorization,
    _actor_memory: MemoryCharge,
    retirement: Mutex<Option<tokio::task::JoinHandle<()>>>,
    cold_cleanup: Mutex<Option<tokio::task::JoinHandle<()>>>,
    automatic_split: Mutex<automatic_split::Observation>,
}
struct DecodedInput {
    request: protocol::Request,
    context: AcceptedContext,
    owner: Arc<ScratchLease>,
}
impl WorkerRuntime {
    pub(crate) async fn open(
        path: &Path,
        database: &str,
        token: &str,
        execution: ExecutionConfig,
        resources: WorkerResources,
    ) -> Result<Arc<Self>, ProtocolError> {
        Self::open_configured(path, database, token, execution, resources, false).await
    }
    pub(crate) async fn open_configured(
        path: &Path,
        database: &str,
        token: &str,
        execution: ExecutionConfig,
        resources: WorkerResources,
        cold_start: bool,
    ) -> Result<Arc<Self>, ProtocolError> {
        if resources.max_threads == 0 || resources.max_threads > 256 {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "OLTP query compute ceiling must be between 1 and 256",
                Outcome::NotApplied,
            ));
        }
        resources.validate_execution(&execution).map_err(|_| {
            ProtocolError::new(
                ErrorCode::ValidationError,
                "invalid typed worker execution resources",
                Outcome::NotApplied,
            )
        })?;
        let services =
            LocalServices::open(path, database, resources, &execution, cold_start).await?;
        Self::from_services(services, token, execution).await
    }
    pub(crate) async fn open_registered(
        config: crate::oltp_replication::manager::RegisteredConfig,
        token: &str,
        execution: ExecutionConfig,
        resources: WorkerResources,
        native: SharedResourceConfig,
    ) -> Result<Arc<Self>, ProtocolError> {
        resources
            .validate_execution(&execution)
            .map_err(|_| unavailable())?;
        let services =
            LocalServices::open_registered(config, resources, native, &execution).await?;
        Self::from_services(services, token, execution).await
    }
    async fn from_services(
        mut services: LocalServices,
        token: &str,
        execution: ExecutionConfig,
    ) -> Result<Arc<Self>, ProtocolError> {
        let admission = DurableAdmission::new(
            services.shared.clone(),
            services.control.clone(),
            services.status.clone(),
            services.timestamps.clone(),
            services.scope.clone(),
            services.queue.clone(),
        )?;
        let planner = NativePlanner::new(
            services.catalog.clone(),
            services.resolver.clone(),
            execution.max_workers_per_request,
        )?;
        let transactions = TransactionCoordinator::new(
            services.shared.clone(),
            services.status.clone(),
            services.timestamps.clone(),
            services.resolver.clone(),
            services.catalog.participant().clone(),
        );
        let authorization = OriginalAuthorization {
            database: services.scope.database.clone(),
            principal: Digest::from_bytes(Sha256::digest(token.as_bytes()).into()),
            policy_revision: 1,
            ingress_id: services.local.worker_id,
            ingress_incarnation: services.local.incarnation,
            credential_expires_at_ms: None,
        };
        let actor_memory = services.shared.scheduler().charge_memory(
            services.shared.scheduler_node_id(),
            WorkerSharedResources::request_actor_bytes(&execution).ok_or_else(unknown)?,
        )?;
        let actor_capacity = execution.max_pending_requests as usize;
        let actors = Executor::new(actor_capacity, 0)?;
        let ingress = Executor::new(2, 16)?;
        let scoped = services
            .scoped
            .take()
            .map(|scoped| {
                super::scoped_worker::ScopedWorker::new(
                    scoped,
                    services.control.identity().clone(),
                    token,
                    execution.max_workers_per_request,
                    execution.result_retention_ms,
                    ingress.clone(),
                    actors.clone(),
                )
            })
            .transpose()?;
        let (admission_cleanup, cleanup_queue, cleanup_stop) =
            admission_cleanup::AdmissionCleanup::new(actor_capacity);
        let runtime = Arc::new(Self {
            services,
            admission,
            planner,
            transactions,
            ingress,
            direct_ingress: Executor::new(super::http_runtime::DIRECT_INGRESS_SLOTS, 0)?,
            control_ingress: Executor::new(2, 0)?,
            index_builds: Executor::new(1, 0)?,
            supervision: Executor::new(1, 0)?,
            actors,
            scoped,
            active: Mutex::new(BTreeSet::new()),
            recovery_cursor: Mutex::new(None),
            retirement_cursor: Mutex::new(None),
            recovery_probe: Mutex::new(recovery::RecoveryProbe::default()),
            gates: Mutex::new(BTreeMap::new()),
            changed: Notify::new(),
            admission_cleanup,
            accepting: AtomicBool::new(true),
            direct_commits_active: AtomicBool::new(false),
            direct_gateway: std::sync::OnceLock::new(),
            direct: std::sync::OnceLock::new(),
            authorization,
            _actor_memory: actor_memory,
            retirement: Mutex::new(None),
            cold_cleanup: Mutex::new(None),
            automatic_split: Mutex::new(automatic_split::Observation::default()),
        });
        runtime.install_direct_gateway();
        let weak = Arc::downgrade(&runtime);
        if let Some(issuer) = &runtime.services.admission_issuer {
            for client in &runtime.services.remote_status {
                client.install_admission_issuer(issuer.clone())?;
            }
        }
        let retirement = nabedra_rt::task::spawn(async move {
            let reclaim = index_reclaim::reclaim_indexes(weak.clone());
            let retire = async move {
                loop {
                    nabedra_rt::time::sleep(std::time::Duration::from_millis(100)).await;
                    let Some(runtime) = weak.upgrade() else {
                        break;
                    };
                    if !runtime.accepting.load(Ordering::Acquire) {
                        break;
                    }
                    let _ = runtime.tick_supervision();
                    let _ = nabedra_rt::time::timeout(
                        std::time::Duration::from_secs(2),
                        runtime.dispatch_retained(),
                    )
                    .await;
                    let _ = nabedra_rt::time::timeout(
                        std::time::Duration::from_secs(2),
                        runtime.retire_expired(),
                    )
                    .await;
                }
            };
            tokio::join!(retire, reclaim);
        });
        *runtime.retirement.lock().map_err(|_| unknown())? = Some(retirement);
        let endpoint = super::application::ApplicationEndpoint::new(&runtime);
        let registration = runtime
            .services
            .manager
            .registry()
            .reserve(runtime.services.control.identity().clone())?;
        registration.bind(
            crate::oltp_replication::multiplex::Purpose::ApiIngress,
            endpoint.clone(),
        )?;
        registration.bind(
            crate::oltp_replication::multiplex::Purpose::ApiObservation,
            endpoint,
        )?;
        if let Some(scoped) = &runtime.scoped {
            let operation = super::authority_fence::transition_id(
                "scoped-queue-capability-v2",
                &runtime.services.local.incarnation,
            )?;
            runtime
                .services
                .control_client
                .advertise_scoped_queue(operation)
                .await?;
            scoped.start_maintenance()?;
        }
        {
            let direct = direct_capability::DirectSwitch::new(
                runtime.services.control_client.clone(),
                runtime.services.local,
                execution.direct_oltp,
            );
            runtime.planner.set_direct_probe(direct.clone())?;
            // T4fix F2: direct commits have no Status decision, so incremental
            // backups are refused from activation on.
            let weak = Arc::downgrade(&runtime);
            direct.on_active(move || {
                if let Some(runtime) = weak.upgrade() {
                    runtime.set_direct_commits_active(true);
                }
            });
            // T5's executor serves `Method::Direct` once active (T7).
            direct.serve_direct_when_active(direct_capability::DirectLeaderServices {
                shared: runtime.services.shared.clone(),
                transport: runtime.services.manager.client(),
                catalog: runtime.services.catalog.participant().identity().clone(),
                timestamps: runtime.services.timestamps.identity().clone(),
                local_timestamps: runtime.services.direct_timestamps.clone(),
            });
            // CAP1: watch the cluster features this worker's own tasks and
            // hosted leaders gate on, from activation on (no feature is
            // enabled before it).
            let features = runtime.services.manager.cluster_features().clone();
            let control = runtime.services.control_client.clone();
            let local = runtime.services.local;
            direct.on_active(move || {
                crate::oltp_replication::authority::control::watch_cluster_features(
                    &control,
                    local,
                    &[
                        crate::oltp_replication::authority::control::INDEX_RECLAIM_FEATURE,
                        crate::oltp_replication::authority::control::VECTOR_FEATURE,
                    ],
                    &features,
                );
            });
            direct.start();
            runtime.direct.set(direct).map_err(|_| unknown())?;
        }
        if runtime.services.cold_history.is_some() {
            *runtime.cold_cleanup.lock().map_err(|_| unknown())? =
                Some(cold_history::spawn(&runtime));
        }
        runtime
            .admission_cleanup
            .start(&runtime, cleanup_queue, cleanup_stop)?;
        Ok(runtime)
    }
    pub(crate) fn application(
        self: &Arc<Self>,
        token: String,
    ) -> Result<axum::Router, ProtocolError> {
        if Digest::from_bytes(Sha256::digest(token.as_bytes()).into())
            != self.authorization.principal
        {
            return Err(ProtocolError::new(
                ErrorCode::Unauthorized,
                "HTTP bearer scope differs from runtime",
                Outcome::NotApplied,
            ));
        }
        super::http::router_for(
            self.clone(),
            token,
            self.services.scope.database.as_str().to_owned(),
        )
    }
    pub(crate) fn application_with_admin(
        self: &Arc<Self>,
        token: String,
        admin: String,
    ) -> Result<axum::Router, ProtocolError> {
        self.authorize_application(
            self.services.control.identity(),
            &token,
            &self.services.scope.database,
        )?;
        super::http::router_for_admin(
            self.clone(),
            token,
            self.services.scope.database.as_str().into(),
            Some(admin),
        )
    }
    pub(super) async fn operator_connection(&self) -> Result<serde_json::Value, ProtocolError> {
        let current = self.services.control.current(&self.services.scope).await?;
        let worker = self
            .services
            .control
            .worker(self.services.local.worker_id)
            .await?
            .ok_or_else(unknown)?;
        if WorkerIdentity::from(&worker) != self.services.local {
            return Err(unknown());
        }
        let mut peers = Vec::new();
        let group = self
            .services
            .control
            .group(self.services.control.identity().group_id)
            .await?
            .ok_or_else(unknown)?;
        let mut after = None;
        loop {
            let bytes = self
                .services
                .control
                .group_members(group.clone(), after)
                .await?;
            let page: crate::oltp_replication::authority::control::ReplicaInventoryPage =
                serde_json::from_slice(bytes.as_ref()).map_err(|_| unknown())?;
            for member in page.items.iter() {
                peers.push(
                    self.services
                        .control
                        .worker(member.worker_id)
                        .await?
                        .ok_or_else(unknown)?,
                );
            }
            if page.done {
                break;
            }
            after = page.after;
        }
        Ok(
            serde_json::json!({"cluster_id":self.services.scope.cluster_id,"database":self.services.scope.database,
            "control_group_id":self.services.control.identity().group_id,"control_peers":peers,
            "run_id":current.ready.view.run_id,"ready_workers":current.ready.ready_count}),
        )
    }
    pub(super) async fn enroll_api_operator(
        &self,
        id: Uuid,
        expected: Option<WorkerIdentity>,
        next: crate::oltp_replication::authority::control::ApiEnrollment,
    ) -> Result<
        crate::oltp_replication::authority::consensus::Applied<
            crate::oltp_replication::authority::control::ControlReply,
        >,
        ProtocolError,
    > {
        let applied = self
            .services
            .control
            .enroll_api_local(&self.services.operator, id, expected, next)
            .await?;
        self.services
            .refresh_enrollment()
            .await
            .map_err(|mut error| {
                error.outcome = Outcome::Unknown;
                error
            })?;
        Ok(applied)
    }
    pub(super) async fn prepare_worker_restart(
        &self,
        intent: crate::oltp_replication::authority::control::RestartIntent,
    ) -> Result<EncodedResponse, ProtocolError> {
        use crate::oltp_replication::authority::control::ControlReply;
        let request = intent.id;
        let applied = self
            .services
            .control
            .prepare_restart(&self.services.operator, intent)
            .await?;
        let (position, reply, _owner) = applied.into_parts()?;
        match reply {
            ControlReply::RestartPrepared(identity) => {
                self.services.refresh_enrollment().await?;
                Ok(response::encode_success(
                    request,
                    &serde_json::json!({"identity": identity, "applied_at": position}),
                    true,
                ))
            }
            ControlReply::Rejected(fault) => Err(ProtocolError::new(
                ErrorCode::Conflict,
                format!("Control could not prepare retained worker restart: {fault:?}"),
                Outcome::NotApplied,
            )),
            _ => Err(unknown()),
        }
    }
    pub(crate) fn authorize_application(
        &self,
        group: &GroupIdentity,
        bearer: &str,
        database: &Text<128>,
    ) -> Result<(), ProtocolError> {
        if group != self.services.control.identity()
            || database != &self.services.scope.database
            || bearer.len() > 4096
            || Digest::from_bytes(Sha256::digest(bearer.as_bytes()).into())
                != self.authorization.principal
        {
            return Err(ProtocolError::new(
                ErrorCode::Unauthorized,
                "unauthorized API database scope",
                Outcome::NotApplied,
            ));
        }
        Ok(())
    }
    pub(crate) async fn authorize_api(
        &self,
        identity: WorkerIdentity,
    ) -> Result<QueueAuthority, ProtocolError> {
        let grant = self
            .services
            .control
            .service_role(
                &self.services.scope,
                identity,
                crate::oltp_replication::authority::control::ServiceAction::ApiIngress,
                self.services.control.identity().clone(),
            )
            .await?;
        if grant.owner != Some(self.services.local) || !self.accepting.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        Ok(QueueAuthority {
            ready: grant.ready,
            owner: self.services.local,
        })
    }

    pub(super) fn scoped_worker(&self) -> Option<&Arc<super::scoped_worker::ScopedWorker>> {
        self.scoped.as_ref()
    }

    pub(super) fn scoped_operator_client(
        &self,
    ) -> Result<&crate::oltp_replication::authority::control::ControlClient, ProtocolError> {
        self.scoped.as_ref().ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "scoped queue support is disabled on this worker",
                Outcome::NotApplied,
            )
        })?;
        Ok(&self.services.control_client)
    }

    pub(super) async fn resolve_scoped_point(
        &self,
        authority: QueueAuthority,
        point: &super::application::PointRequest,
        digest: Digest,
    ) -> Result<super::application::PointRouting, ProtocolError> {
        use super::application::{PointRouting, ScopedTarget};
        if self.scoped.is_none() {
            return Ok(PointRouting::Legacy);
        }
        let status = self
            .services
            .status
            .lookup_identity(point.request_id, Some(digest))
            .await?;
        let control = &self.services.control_client;
        if let Some(status) = status {
            let reservation = match &status {
                IdentityLookup::Reserved(reservation)
                | IdentityLookup::TimestampPoint(reservation) => reservation,
                IdentityLookup::Abandoned(_) => return Err(QueueError::Abandoned.protocol()),
                IdentityLookup::Terminal(terminal) => {
                    super::scoped_worker::terminal_reservation(terminal)
                }
                IdentityLookup::Rejected(rejection) => {
                    return Err(rejection.failure.result().err().ok_or_else(unknown)?);
                }
            };
            let Some(policy) = reservation.scoped else {
                return Ok(PointRouting::Legacy);
            };
            if reservation.original_run != authority.ready.view.run_id {
                if matches!(status, IdentityLookup::Terminal(_)) {
                    // The legacy receipt reader uses the exact durable outcome
                    // for a fenced prior run with no executable RAM queue.
                    return Ok(PointRouting::Legacy);
                }
                if matches!(status, IdentityLookup::TimestampPoint(_)) {
                    return Err(unknown());
                }
                return Err(QueueError::Abandoned.protocol());
            }
            match control.scoped_queue_by_id(policy.queue_group_id).await {
                Ok(Some(placement))
                    if placement.view.run_id == reservation.original_run
                        && placement.queue_generation == policy.queue_generation =>
                {
                    return Ok(PointRouting::Owner(ScopedTarget::from(&placement)));
                }
                Ok(_)
                | Err(ProtocolError {
                    code: ErrorCode::StorageUnavailable,
                    ..
                }) => {}
                Err(error) => return Err(error),
            }
            if self
                .services
                .status
                .retired_scoped_receipt(
                    reservation.original_run,
                    reservation.request_id,
                    Some(reservation.digest),
                )
                .await?
                .is_some()
            {
                return Ok(PointRouting::Legacy);
            }
            return Err(unknown());
        }
        if control
            .scoped_queue_capability()
            .await?
            .activation
            .is_none()
        {
            return Ok(PointRouting::Legacy);
        }
        let hint = self.services.catalog.snapshot(
            Timestamp::new(u64::MAX).map_err(|_| unknown())?,
            authority.ready.view.authority(),
        )?;
        let route = self
            .services
            .catalog
            .route(&hint, point.table_id, &point.key)
            .await?;
        let Some(shard) = route.route() else {
            return Ok(PointRouting::Legacy);
        };
        let placement = match control.scoped_queue(&shard.group).await? {
            Some(placement) => placement,
            None => {
                let operation = super::authority_fence::transition_id(
                    "scoped-queue-create-v2",
                    &(shard.group.group_id, authority.ready.view.run_id),
                )?;
                control.ensure_scoped_queue(operation, &shard.group).await?
            }
        };
        if placement.view.run_id != authority.ready.view.run_id || placement.group != shard.group {
            return Err(super::application::stale_route());
        }
        Ok(PointRouting::range(
            shard.clone(),
            ScopedTarget::from(&placement),
        ))
    }

    pub(super) async fn resolve_scoped_receipt(
        &self,
        authority: QueueAuthority,
        run: Uuid,
        id: Uuid,
    ) -> Result<Option<super::application::ScopedReceiptRoute>, ProtocolError> {
        if self.scoped.is_none() || run != authority.ready.view.run_id {
            return Ok(None);
        }
        let lookup = self.durable_lookup(id).await?;
        let reservation = match lookup.as_ref() {
            Some(
                IdentityLookup::Reserved(reservation) | IdentityLookup::TimestampPoint(reservation),
            ) => reservation,
            Some(IdentityLookup::Terminal(terminal)) => {
                super::scoped_worker::terminal_reservation(terminal)
            }
            _ => return Ok(None),
        };
        let Some(policy) = reservation.scoped else {
            return Ok(None);
        };
        if reservation.original_run != run {
            return Err(QueueError::Abandoned.protocol());
        }
        let placement = match self
            .services
            .control_client
            .scoped_queue_by_id(policy.queue_group_id)
            .await
        {
            Ok(Some(placement))
                if placement.view.run_id == run
                    && placement.queue_generation == policy.queue_generation =>
            {
                placement
            }
            Ok(_)
            | Err(ProtocolError {
                code: ErrorCode::StorageUnavailable,
                ..
            }) => {
                let retired = self
                    .services
                    .status
                    .retired_scoped_receipt(run, id, Some(reservation.digest))
                    .await;
                match retired {
                    Ok(Some(_)) => return Ok(None),
                    // The API enrollment was verified before routing. The
                    // internal Status role can retire with the old owner.
                    Ok(None) | Err(ProtocolError { code: ErrorCode::Unauthorized, .. }) => {
                        return Err(unknown());
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        Ok(Some(super::application::ScopedReceiptRoute {
            target: super::application::ScopedTarget::from(&placement),
            digest: reservation.digest,
        }))
    }
    pub(crate) async fn submit_charged(
        self: &Arc<Self>,
        body: ChargedBytes,
        origin: WorkerIdentity,
    ) -> Result<RequestRecord, ProtocolError> {
        let admission = self.ingress.try_admit()?;
        let runtime = self.clone();
        admission
            .spawn(move || async move {
                let _copy = runtime
                    .services
                    .shared
                    .reserve_scratch(ByteClass::Data, body.len())?;
                let mut authorization = runtime.authorization.clone();
                authorization.ingress_id = origin.worker_id;
                authorization.ingress_incarnation = origin.incarnation;
                runtime
                    .submit_original(body.as_ref().to_vec(), authorization, None)
                    .await
            })?
            .receive()
            .await
    }
    pub(super) fn application_copy_confirmation(
        &self,
        run: Uuid,
        id: Uuid,
    ) -> Result<Option<RecordVersion>, ProtocolError> {
        let replica = self.services.replica.lock().map_err(|_| unknown())?;
        Ok(replica
            .record(id)
            .filter(|record| record.run_id == run)
            .and_then(|record| replica.terminal_copy_confirmation(replica.authority(), record)))
    }
    pub(super) async fn application_observation(
        &self,
        view: ApplicationReadView,
        run: Uuid,
        id: Uuid,
        include_result: bool,
    ) -> Result<ObservedApplication, ProtocolError> {
        #[cfg(test)]
        if super::recovery_tests::lose_receipt_observation(id).await {
            return Err(unknown());
        }
        let limit = if include_result {
            INLINE_RECEIPT_BYTES
        } else {
            0
        };
        self.application_snapshot(view, run, id, limit, ByteClass::Progress, None)
            .await
    }
    async fn application_snapshot(
        &self,
        view: ApplicationReadView,
        run: Uuid,
        id: Uuid,
        result_limit: usize,
        result_class: ByteClass,
        expected: Option<&ApplicationObservation>,
    ) -> Result<ObservedApplication, ProtocolError> {
        let ApplicationReadView {
            authority,
            terminal,
        } = view;
        let confirmed = |replica: &WorkerReplica, record: &RequestRecord| {
            terminal.is_some() && replica.terminal_copy_confirmation(authority, record) == terminal
        };
        let (local_error, reconciling) = {
            let replica = self.services.replica.lock().map_err(|_| unknown())?;
            if replica.authority() != authority {
                return Err(unknown());
            }
            let local = replica.retained_record(id, now()).and_then(|record| {
                if record.run_id != run {
                    return Err(QueueError::Abandoned);
                }
                let observation = replica.observation(id)?;
                let response_hash = if record.state.terminal() {
                    Some(replica.terminal_response_hash(id)?)
                } else {
                    None
                };
                let metadata = ApplicationObservation {
                    record: observation,
                    response_hash,
                    durable_proof: false,
                };
                Ok((record, metadata))
            });
            match local {
                Ok((record, metadata)) if record.state != RequestState::Reconciling => {
                    if record.state.terminal() && !confirmed(&replica, record) {
                        return pending_terminal_copy(metadata.record, expected);
                    }
                    return attach_result(metadata, expected, || {
                        receipt_body(
                            result_class,
                            replica
                                .copy_response_bytes(record, result_limit, result_class)
                                .map_err(QueueError::protocol),
                        )
                    });
                }
                Ok((record, metadata)) => (
                    QueueError::Unavailable,
                    record.pinned_catalog.is_some().then_some(metadata),
                ),
                Err(error) => (error, None),
            }
        };
        #[cfg(test)]
        super::recovery_tests::observe_application_status_lookup(run, id);
        match self.durable_lookup(id).await {
            Ok(Some(IdentityLookup::Terminal(terminal))) => {
                #[cfg(test)]
                super::recovery_tests::transaction_checkpoint("status-lookup", id, &terminal).await;
                let terminal = self
                    .application_terminal(terminal, run, authority.ready.view.run_id)
                    .await?;
                let reservation = terminal_reservation(&terminal)?;
                if reservation.original_run != run {
                    return Err(QueueError::Abandoned.protocol());
                }
                {
                    let replica = self.services.replica.lock().map_err(|_| unknown())?;
                    if replica.authority() != authority {
                        return Err(unknown());
                    }
                    if let Some(record) = replica.record(id)
                        && record.run_id == run
                        && !confirmed(&replica, record)
                    {
                        // A live record retains its version and the worker's
                        // replication task advances it under the current view.
                        let record = replica.observation(id).map_err(QueueError::protocol)?;
                        return pending_terminal_copy(record, expected);
                    }
                }
                let encoded = terminal_response(id, &terminal)?;
                attach_result(
                    ApplicationObservation {
                        record: terminal_record_with_status(reservation, encoded.status),
                        response_hash: Some(Digest::from_bytes(
                            Sha256::digest(&encoded.body).into(),
                        )),
                        durable_proof: true,
                    },
                    expected,
                    || {
                        receipt_body(
                            result_class,
                            (encoded.body.len() <= result_limit)
                                .then(|| {
                                    self.services
                                        .shared
                                        .copy_bytes_in(result_class, &encoded.body)
                                })
                                .transpose(),
                        )
                    },
                )
            }
            Ok(Some(IdentityLookup::Abandoned(_))) => Err(QueueError::Abandoned.protocol()),
            Ok(Some(IdentityLookup::TimestampPoint(reservation))) => {
                if reservation.original_run != run {
                    return Err(QueueError::Abandoned.protocol());
                }
                Err(unknown())
            }
            Ok(Some(IdentityLookup::Rejected(rejection))) => {
                Err(rejection.failure.result().err().ok_or_else(unknown)?)
            }
            status => attach_reconciling_read(reconciling, status, local_error, expected),
        }
    }
    pub(super) async fn application_result(
        &self,
        view: ApplicationReadView,
        observed: &ApplicationObservation,
    ) -> Result<ChargedBytes, ProtocolError> {
        let current = self
            .application_snapshot(
                view,
                observed.record.run_id,
                observed.record.request_id,
                crate::protocol::MAX_BODY_BYTES,
                ByteClass::Data,
                Some(observed),
            )
            .await?;
        current.body.ok_or_else(unknown)
    }
    pub(crate) fn cluster_id(&self) -> Uuid {
        self.services.scope.cluster_id
    }
    pub(crate) fn resources(&self) -> &Arc<crate::oltp::pool::WorkerSharedResources> {
        &self.services.shared
    }
    pub(crate) async fn admit(
        self: &Arc<Self>,
        body: Vec<u8>,
    ) -> Result<RequestRecord, ProtocolError> {
        self.admit_with_authority(body, None).await
    }
    async fn admit_with_authority(
        self: &Arc<Self>,
        body: Vec<u8>,
        expected: Option<Authority>,
    ) -> Result<RequestRecord, ProtocolError> {
        let runtime = self.clone();
        self.ingress
            .try_admit()?
            .spawn(move || async move {
                let authorization = runtime.authorization.clone();
                runtime.submit_original(body, authorization, expected).await
            })?
            .receive()
            .await
    }
    pub(crate) async fn observe(&self, run: Uuid, id: Uuid) -> Result<OwnedReceipt, ProtocolError> {
        self.receipt_owned(run, id).await
    }
    fn gate(&self, id: Uuid) -> Result<Arc<AsyncMutex<()>>, ProtocolError> {
        let mut gates = self.gates.lock().map_err(|_| unknown())?;
        gates.retain(|_, gate| gate.strong_count() != 0);
        if let Some(gate) = gates.get(&id).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        let gate = Arc::new(AsyncMutex::new(()));
        gates.insert(id, Arc::downgrade(&gate));
        Ok(gate)
    }
    pub(crate) fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
        if let Some(scoped) = &self.scoped {
            scoped.stop_accepting();
        }
        self.admission_cleanup.stop();
        self.ingress.stop_accepting();
        self.direct_ingress.stop_accepting();
        self.actors.stop_accepting();
        self.control_ingress.stop_accepting();
        self.index_builds.stop_accepting();
        self.supervision.stop_accepting();
    }
    pub(crate) async fn drain(&self) -> Result<(), ProtocolError> {
        self.accepting.store(false, Ordering::Release);
        if let Some(scoped) = &self.scoped {
            scoped.drain().await;
        }
        self.admission_cleanup.stop();
        self.direct_ingress.drain().await;
        self.ingress.drain().await;
        self.actors.drain().await;
        self.control_ingress.drain().await;
        self.index_builds.drain().await;
        self.supervision.drain().await;
        self.admission_cleanup.join().await?;
        if let Some(direct) = self.direct.get() {
            direct.stop().await;
        }
        let retirement = self.retirement.lock().map_err(|_| unknown())?.take();
        if let Some(retirement) = retirement {
            retirement.await.map_err(|_| unknown())?;
        }
        let cold_cleanup = self.cold_cleanup.lock().map_err(|_| unknown())?.take();
        if let Some(cold_cleanup) = cold_cleanup {
            cold_cleanup.await.map_err(|_| unknown())?;
        }
        self.services.shutdown().await
    }
    pub(super) async fn submit_owned(
        self: Arc<Self>,
        body: Vec<u8>,
    ) -> Result<RequestRecord, ProtocolError> {
        let authorization = self.authorization.clone();
        self.submit_original(body, authorization, None).await
    }
    /// T7c: the exactly-once rule across the `data.direct.v1` activation
    /// boundary, enforced where the queued path reserves a request id.
    ///
    /// While the capability is active an eligible mutation is settled at
    /// its anchor (`DirectForward`) before Status reserves it, whatever the
    /// sending gateway did: a gateway that has not yet observed activation
    /// routes queued without a marker. A replayed direct result means the
    /// id already committed directly, so it must not be queued.
    ///
    /// Before this switch observes activation, Control is asked first
    /// (`QueuedHistory`): it either is or becomes active (settle as above),
    /// or durably records pre-activation queued history before this
    /// reservation. Any later activation then opens the queued-history
    /// window, in which no leader executes an unknown id directly (see
    /// `DirectSwitch::history_settling`), so a request reserved here
    /// without a marker can only be answered by the queued path: its
    /// original result, a conflict, or 404 abandoned after a cold start.
    /// The scoped queue's admission calls this too.
    ///
    /// Gets are skipped: no settle, no `QueuedHistory` note (user decision
    /// 2026-11-21, RV-FINAL M3). A read has no exactly-once identity; it
    /// cannot apply twice, and running it again is a new read. Every
    /// mutation guarantee above rests on mutation records only.
    pub(super) async fn settle_before_reserve(&self, body: &[u8]) -> Result<(), ProtocolError> {
        if !super::application::direct::settled_before_queue(body) {
            return Ok(());
        }
        let (Some(switch), Some(gateway)) = (self.direct_switch(), self.direct_gateway_handle())
        else {
            return Ok(());
        };
        if !switch.note_queued_history().await? {
            return Ok(());
        }
        match gateway.route(body, true).await? {
            super::application::direct::Routed::Queued => Ok(()),
            super::application::direct::Routed::Done(_) => Err(ProtocolError::new(
                ErrorCode::OutcomeUnknown,
                "request id already committed on the direct path; retry it to read the result",
                Outcome::Unknown,
            )),
        }
    }
    async fn submit_original(
        self: Arc<Self>,
        body: Vec<u8>,
        authorization: OriginalAuthorization,
        expected: Option<Authority>,
    ) -> Result<RequestRecord, ProtocolError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        if self
            .recovery_probe
            .lock()
            .map_err(|_| unknown())?
            .removal
            .is_some()
        {
            return Err(QueueError::Unavailable.protocol());
        }
        // The HTTP ingress owns this entire future independently of its socket.
        let prepared = self.admission.prepare(body, authorization, now()).await?;
        let id = prepared.request_id;
        #[cfg(test)]
        super::recovery_tests::count_submission(id);
        let gate = self
            .admission_cleanup
            .lock(self.gate(id)?, id, prepared.digest)
            .await?;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        // Acquire the UUID gate before Reserve, so a retry observes a rejection
        // that landed while the previous admission held that gate.
        let actor = self.actors.try_admit()?;
        self.settle_before_reserve(prepared.body()).await?;
        let reserved = match self.admission.reserve(prepared, expected).await? {
            ReservationResult::Reserved(reserved) => reserved,
            ReservationResult::Abandoned(_) => return Err(QueueError::Abandoned.protocol()),
            ReservationResult::Terminal(terminal) => return terminal_record(&terminal),
        };
        if expected.is_some() {
            self.index_authority(expected).await?;
        }
        let existing = {
            let replica = self.services.replica.lock().map_err(|_| unknown())?;
            if replica
                .record(id)
                .is_some_and(|record| record.body.as_slice() != reserved.body.as_ref())
            {
                return Err(QueueError::Conflict.protocol());
            }
            replica.record(id).map(|record| record.accepted)
        };
        let mut captured_point = None;
        if let Some(accepted) = existing {
            let current = self.services.control.current(&self.services.scope).await?;
            let result = if accepted {
                self.services.queue.ensure_copies(current, id).await
            } else {
                self.resume_staged(current, id).await
            };
            result.map_err(uncertain_admission)?;
        } else {
            let cleanup = self.admission_cleanup.reserve(&self.services.shared)?;
            let mut pin = None;
            let mut request_binding = None;
            let accepted = async {
                let (operation, owner) = self.decode_raw(reserved.body.clone()).await?;
                if let Some(table_id) = index_backend::search_ddl_table(&operation.operation) {
                    self.repair_search_ddl(table_id, expected).await?;
                }
                let storage_pin = self.admission.begin_snapshot(&reserved).await?;
                pin = Some(storage_pin);
                #[cfg(test)]
                super::recovery_tests::pause("preaccept", id).await;
                let current = self.services.control.current(&self.services.scope).await?;
                let catalog = self
                    .services
                    .catalog
                    .snapshot(storage_pin.read_ts, current.ready.view.authority())?;
                let capture = loop {
                    match self
                        .services
                        .catalog
                        .capture_lineage(&catalog, &operation.operation)
                        .await?
                    {
                        CaptureRead::Captured(capture) => break capture,
                        CaptureRead::Blocked(intent) => {
                            self.help_intent(intent).await?;
                            nabedra_rt::time::sleep(std::time::Duration::from_millis(2)).await;
                        }
                    }
                };
                let read = QueueReadToken {
                    identity: reserved.pin_identity(),
                    storage_pin,
                    catalog_root: capture.catalog_root,
                    schema_epoch: capture.schema_epoch,
                    routing_epoch: capture.routing_epoch,
                };
                let context = self.admission.context(&reserved, read)?;
                if expected.is_some() {
                    self.index_authority(expected).await?;
                }
                self.admission
                    .queue()
                    .stage(
                        current,
                        reserved.body.as_ref().to_vec(),
                        context.clone(),
                        now(),
                    )
                    .await?;
                #[cfg(test)]
                super::recovery_tests::pause("tentative", id).await;
                let binding = self
                    .admission
                    .bind_staged(current.ready.view, &context)
                    .await?;
                request_binding = Some(binding);
                #[cfg(test)]
                super::recovery_tests::pause("bound", id).await;
                let result = self.admission.queue().accept_staged(current, id).await;
                if result.is_ok() && capture.has_point() {
                    captured_point = Some(capture);
                }
                drop((operation, owner));
                result
            }
            .await;
            if let Err(failure) = accepted {
                // The owner always installs locally before sending any copy.
                // Absence under this UUID gate therefore proves no copy phase
                // began. Unknown errors never create a cleanup capability.
                let absent = self
                    .services
                    .replica
                    .lock()
                    .map_err(|_| unknown())?
                    .record(id)
                    .is_none();
                if !absent {
                    return Err(uncertain_admission(failure));
                }
                if failure.outcome == Outcome::NotApplied {
                    let fresh = self.services.control.current(&self.services.scope).await?;
                    if fresh.owner == self.services.local
                        && fresh.ready.view.run_id == reserved.reservation.original_run
                    {
                        let cap = VerifiedNeverAccepted {
                            scope: self.services.scope.clone(),
                            view: fresh.ready.view,
                            reservation: reserved.reservation,
                            identity: reserved.pin_identity(),
                            pin,
                            request: request_binding,
                            failure: failure.clone(),
                            admission_gate: Arc::new(gate),
                            live_admission: std::sync::OnceLock::new(),
                        };
                        let (slot, pending) = self.admission_cleanup.hold(cleanup, cap)?;
                        if self.settle_never_accepted(&pending.cap).await.is_ok() {
                            return Err(failure);
                        }
                        drop(slot.send(pending));
                        return Err(unknown());
                    }
                    return Err(uncertain_admission(failure));
                }
                return Err(failure);
            }
        }
        let observation = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .observation(id)
            .map_err(QueueError::protocol)?;
        let start = self.active.lock().map_err(|_| unknown())?.insert(id);
        drop(gate);
        if start && !observation.state.terminal() {
            let runtime = self.clone();
            match actor.spawn(move || async move {
                let mut settlement = Settlement {
                    runtime: runtime.clone(),
                    id,
                    armed: true,
                };
                runtime
                    .execute(id, captured_point, None)
                    .await
                    .inspect_err(|error| {
                        eprintln!(
                            "nabedra-db: request {id} execution failed: {:?}: {}",
                            error.code, error.message
                        );
                    })?;
                settlement.armed = false;
                Ok(())
            }) {
                Ok(operation) => drop(operation),
                Err(error) => {
                    self.mark_unknown(id);
                    return Err(error);
                }
            }
        } else if start {
            self.active.lock().map_err(|_| unknown())?.remove(&id);
        }
        if self.services.replication_factor > 1 {
            self.services
                .control
                .admission_authority(&self.services.scope)
                .await?;
        }
        Ok(observation)
    }
    async fn resume_staged(
        &self,
        current: QueueAuthority,
        id: Uuid,
    ) -> Result<RecordVersion, ProtocolError> {
        let context = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .staged_context(id)
            .map_err(QueueError::protocol)?
            .ok_or_else(unknown)?;
        let original: AcceptedContext =
            serde_json::from_slice(context.as_ref()).map_err(|_| unknown())?;
        self.services.queue.ensure_staged(current, id).await?;
        self.admission
            .bind_staged(current.ready.view, &original)
            .await?;
        self.services.queue.accept_staged(current, id).await
    }
    async fn decode_raw(
        &self,
        body: ChargedBytes,
    ) -> Result<(protocol::Request, Arc<ScratchLease>), ProtocolError> {
        let shared = self.services.shared.clone();
        let resources = shared.clone();
        let vector = self
            .services
            .manager
            .cluster_features()
            .enabled(crate::oltp_replication::authority::control::VECTOR_FEATURE);
        shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                let budget = crate::oltp::mvcc::request_decode_budget(body.as_ref())?
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(64 << 10))
                    .and_then(|n| {
                        super::worker_endpoint::request_raw_clone_bytes(body.as_ref())
                            .ok()
                            .and_then(|extra| n.checked_add(extra))
                    })
                    .ok_or_else(unknown)?;
                let owner = Arc::new(resources.reserve_scratch(ByteClass::Data, budget)?);
                let request = protocol::parse_oltp_request(body.as_ref())?;
                request.operation.check_queued(vector)?;
                Ok((request, owner))
            })
            .await
    }
    async fn decode(&self, id: Uuid) -> Result<DecodedInput, ProtocolError> {
        let (body, context, record) = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .input(id)
            .map_err(QueueError::protocol)?;
        let (request, owner) = self.decode_raw(body).await?;
        let parsed =
            serde_json::from_slice::<AcceptedContext>(context.as_ref()).map_err(|_| unknown())?;
        if request.request_id != record.request_id
            || parsed
                .read
                .as_ref()
                .is_none_or(|read| read.identity.run_id != record.run_id)
        {
            return Err(unknown());
        }
        Ok(DecodedInput {
            request,
            context: parsed,
            owner,
        })
    }
    async fn binding(&self, id: Uuid) -> Result<(DecodedInput, RequestBinding), ProtocolError> {
        let input = self.decode(id).await?;
        let token = input.context.read.as_ref().ok_or_else(unknown)?;
        let binding = RequestBinding {
            run_id: token.identity.run_id,
            request_id: id,
            digest: token.identity.digest,
            accepted_context: crate::oltp_replication::owned::serialized_digest(&input.context)?,
        };
        Ok((input, binding))
    }
    async fn execute(
        &self,
        id: Uuid,
        captured_point: Option<CatalogCapture>,
        resumed: Option<QueuedAttempt>,
    ) -> Result<(), ProtocolError> {
        #[cfg(test)]
        super::recovery_tests::pause("queued", id).await;
        let running = self.services.queue.start(id).await?;
        let (input, request) = self.binding(id).await?;
        let read = input.context.read.as_ref().ok_or_else(unknown)?;
        let current = self.services.control.current(&self.services.scope).await?;
        let authority = current.ready.view.authority();
        let mut attempt = resumed.unwrap_or_else(|| QueuedAttempt::original(id, read.storage_pin));
        let initial_txn = attempt.txn;
        let mutation = input.request.operation.is_mutation();
        let mut captured_point = captured_point;
        let encoded = loop {
            let txn = attempt.txn;
            let pin = self
                .services
                .catalog
                .snapshot(attempt.pin.read_ts, authority)?;
            #[cfg(test)]
            if txn.attempt == 1 {
                super::recovery_tests::transaction_checkpoint("snapshot", id, &pin.read_ts).await;
            }
            // The pinned Running copy barrier has finished.
            if txn == initial_txn
                && let Ok(mut replica) = self.services.replica.lock()
            {
                replica.observe_dispatch(current, running);
            }
            let point = captured_point
                .as_ref()
                .and_then(|catalog| catalog.point_for(&pin, read.catalog_root));
            let mut guards = BTreeMap::new();
            let planned = self
                .plan_queued(&input, &pin, txn, request, point, &mut guards)
                .await?;
            drop(captured_point.take());
            break match planned {
                Ok(PlannedOperation::Read(bytes)) => {
                    let shared = self.services.shared.clone();
                    shared
                        .execute(ExecutionClass::Query, 0, 0, move || {
                            let raw: &serde_json::value::RawValue =
                                serde_json::from_slice(bytes.as_ref()).map_err(|_| unknown())?;
                            Ok(response::encode_success(id, raw, false))
                        })
                        .await?
                }
                Ok(PlannedOperation::Mutation(plan)) => {
                    let decision = Box::pin(self.transactions.commit_queued(
                        authority,
                        txn,
                        request,
                        pin.read_ts,
                        plan,
                        self.direct_switch().map(Arc::as_ref),
                    ))
                    .await?;
                    if decision.statement().decision.retries_snapshot() {
                        attempt = self
                            .advance_queued_attempt(current, &read.identity, &decision)
                            .await?;
                        continue;
                    }
                    match decision.outcome().ok_or_else(unknown)?.result() {
                        Ok(value) => response::encode_success(id, &value, true),
                        Err(error) => response::encode_error(Some(id), error),
                    }
                }
                Err(PlanError::Protocol(error)) => {
                    if mutation {
                        let terminal = self
                            .services
                            .status
                            .terminal_failure(authority, request, error.clone())
                            .await?;
                        #[cfg(test)]
                        super::recovery_tests::transaction_checkpoint(
                            "status-failure",
                            id,
                            &terminal,
                        )
                        .await;
                        if !guards.is_empty() {
                            let settled = self
                                .services
                                .status
                                .failure_settled_proof(txn, request)
                                .await?;
                            for (_, guard) in guards {
                                self.services
                                    .catalog
                                    .participant()
                                    .release_lifecycle(authority, guard, settled.clone())
                                    .await?;
                            }
                        }
                        terminal_response(id, &terminal)?
                    } else {
                        response::encode_error(Some(id), error)
                    }
                }
                Err(_) => return Err(unknown()),
            };
        };
        self.services
            .queue
            .finish(id, encoded.status, encoded.body, now())
            .await?;
        self.changed.notify_waiters();
        let statement = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .terminal_statement(id)
            .map_err(QueueError::protocol)?;
        // A failed cleanup does not undo an acknowledged decision/result. Keep
        // the complete record and retry through the bounded retirement loop.
        let _ = self.services.timestamps.release(statement).await;
        Ok(())
    }
    async fn plan_queued(
        &self,
        input: &DecodedInput,
        pin: &crate::oltp::mvcc::catalog::PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        point: Option<&crate::oltp::mvcc::catalog::PointCatalog>,
        guards: &mut BTreeMap<Uuid, crate::oltp::mvcc::store::lifecycle::VerifiedLifecycleGuard>,
    ) -> Result<Result<PlannedOperation, PlanError>, ProtocolError> {
        let mutation = input.request.operation.is_mutation();
        let mut blocked_since = None;
        let mut split_retry: Option<(tokio::time::Instant, ProtocolError)> = None;
        let mut split_backoff = std::time::Duration::from_millis(50);
        let planned = loop {
            let planning = self.planner.plan(
                input.request.operation.clone(),
                pin,
                txn,
                request,
                input.owner.clone(),
                point,
            );
            let result = match &split_retry {
                Some((deadline, error)) => match nabedra_rt::time::timeout_at(*deadline, planning).await
                {
                    Ok(result) => result,
                    Err(_) => break Err(PlanError::Protocol(error.clone())),
                },
                None => planning.await,
            };
            let error = match result {
                Ok(plan) => break Ok(plan),
                Err(error) => error,
            };
            let mut error = error;
            loop {
                match error {
                    PlanError::Guarded {
                        cause,
                        guards: held,
                    } => {
                        for guard in held {
                            guards.insert(guard.binding().guard_id, guard);
                        }
                        error = *cause;
                    }
                    _ => break,
                }
            }
            match error {
                PlanError::Protocol(error)
                    if !mutation && error.code == ErrorCode::SplitPublicationPending =>
                {
                    #[cfg(test)]
                    if split_retry.is_none() {
                        super::recovery_tests::pause("split_read_pending", txn.request_id).await;
                    }
                    // Each plan drops its native and query grants before waiting.
                    // Re-read Catalog at the original accepted timestamp.
                    let now = nabedra_rt::time::now();
                    let (deadline, _) = split_retry
                        .get_or_insert_with(|| (now + std::time::Duration::from_secs(10), error));
                    nabedra_rt::time::sleep_until((now + split_backoff).min(*deadline)).await;
                    split_backoff = (split_backoff * 2).min(std::time::Duration::from_millis(250));
                    continue;
                }
                PlanError::Blocked(intent) => {
                    #[cfg(test)]
                    super::recovery_tests::transaction_checkpoint(
                        "blocked",
                        txn.request_id,
                        &intent,
                    )
                    .await;
                    self.help_intent(intent).await?;
                }
                PlanError::LifecyclePending(binding) => {
                    #[cfg(test)]
                    super::recovery_tests::transaction_checkpoint(
                        "lifecycle-blocked",
                        txn.request_id,
                        &binding,
                    )
                    .await;
                    if let Some(holder) = self
                        .services
                        .catalog
                        .participant()
                        .lifecycle_holder(binding.table_id)
                        .await?
                    {
                        self.help_lifecycle(holder).await?;
                    }
                }
                other => break Err(other),
            }
            if txn.attempt > 1
                && {
                    let since = *blocked_since.get_or_insert_with(nabedra_rt::time::now);
                    nabedra_rt::time::elapsed(since) >= RETRY_PROGRESS_TIMEOUT
                }
            {
                return Err(unknown());
            }
            nabedra_rt::time::sleep(std::time::Duration::from_millis(2)).await;
        };
        Ok(planned)
    }
    async fn help_intent(&self, intent: IntentRef) -> Result<(), ProtocolError> {
        let authority = self
            .services
            .control
            .current(&self.services.scope)
            .await?
            .ready
            .view
            .authority();
        if let Some(decision) = self
            .recover_queued_transaction(authority, intent.txn)
            .await?
        {
            if decision.statement().manifest != intent.manifest {
                return Err(unknown());
            }
        }
        Ok(())
    }
    async fn help_lifecycle(
        &self,
        binding: crate::oltp::mvcc::store::lifecycle::LifecycleBinding,
    ) -> Result<(), ProtocolError> {
        let authority = self
            .services
            .control
            .current(&self.services.scope)
            .await?
            .ready
            .view
            .authority();
        let decision = self
            .recover_queued_transaction(authority, binding.txn)
            .await?;
        let settled = if let Some(decision) = decision {
            let statement = decision.statement();
            self.services
                .status
                .settled_proof(statement.txn, statement.manifest)
                .await?
        } else {
            // A planning actor may own a guard before a manifest exists. Only
            // its immutable Status failure proves that no Prepare can follow.
            let identity = self.durable_lookup(binding.request.request_id).await?;
            if !matches!(
                identity,
                Some(IdentityLookup::Terminal(TerminalIdentity::Failed { .. }))
            ) {
                return Ok(());
            }
            self.services
                .status
                .failure_settled_proof(binding.txn, binding.request)
                .await?
        };
        let participant = self.services.catalog.participant();
        if let Some(guard) = participant.lifecycle_guard(&binding).await? {
            participant
                .release_lifecycle(authority, guard, settled)
                .await?;
        } else {
            participant
                .cancel_pending_lifecycle(authority, binding, settled)
                .await?;
        }
        Ok(())
    }
    fn mark_unknown(&self, id: Uuid) {
        if let Ok(mut replica) = self.services.replica.lock() {
            let current = replica.authority();
            let _ = replica.reconcile(current, id);
        }
        self.changed.notify_waiters();
    }
    async fn durable_lookup(&self, id: Uuid) -> Result<Option<IdentityLookup>, ProtocolError> {
        self.services.status.lookup_identity(id, None).await
    }

    async fn application_terminal(
        &self,
        terminal: TerminalIdentity,
        requested_run: Uuid,
        current_run: Uuid,
    ) -> Result<TerminalIdentity, ProtocolError> {
        let reservation = *super::scoped_worker::terminal_reservation(&terminal);
        if reservation.original_run != requested_run {
            return Err(QueueError::Abandoned.protocol());
        }
        if reservation.scoped.is_none() {
            return Ok(terminal);
        }
        let status = &self.services.status;
        if requested_run == current_run {
            return status
                .retired_scoped_receipt(
                    requested_run,
                    reservation.request_id,
                    Some(reservation.digest),
                )
                .await?
                .map(|receipt| receipt.terminal)
                .ok_or_else(unknown);
        }
        match status
            .prior_scoped_receipt(
                requested_run,
                reservation.request_id,
                Some(reservation.digest),
            )
            .await?
        {
            Some(IdentityLookup::Terminal(terminal)) => Ok(terminal),
            Some(IdentityLookup::Abandoned(_)) => Err(QueueError::Abandoned.protocol()),
            _ => Err(unknown()),
        }
    }
    async fn terminal_receipt_ready(
        &self,
        run: Uuid,
        id: Uuid,
        version: Option<u64>,
    ) -> Result<bool, ProtocolError> {
        if self.services.replication_factor == 1 {
            return Ok(true);
        }
        let current = self.services.control.current(&self.services.scope).await?;
        if current.ready.view.run_id != run {
            // Old-run outcomes can be recovered from durable Status history.
            return Ok(version.is_none());
        }
        if current.owner == self.services.local {
            let replica = self.services.replica.lock().map_err(|_| unknown())?;
            if replica.authority() != current {
                return Err(unknown());
            }
            return match replica.record(id) {
                Some(record) if record.run_id == run => Ok(replica
                    .terminal_copy_confirmation(current, record)
                    .is_some_and(|confirmed| version.is_none_or(|v| confirmed.version == v))),
                Some(_) => Err(unknown()),
                None => Ok(version.is_none()),
            };
        }
        match self.services.queue.owner_record_state(current, id).await {
            Ok(state) => Ok(state.state.terminal()
                && state.copies_complete
                && version.is_none_or(|v| state.version == v)),
            Err(error) if error == QueueError::NotFound.protocol() => Ok(version.is_none()),
            Err(error) => Err(error),
        }
    }
    async fn receipt_owned(&self, run: Uuid, id: Uuid) -> Result<OwnedReceipt, ProtocolError> {
        let committing = || {
            OwnedReceipt::from(Receipt {
                request_id: id,
                run_id: run,
                state: RequestState::Committing,
                result: None,
                error: None,
            })
        };
        let local = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .response(run, id, now());
        match local {
            Ok((record, Some(bytes))) => {
                if !self
                    .terminal_receipt_ready(run, id, Some(record.version))
                    .await?
                {
                    return Ok(committing());
                }
                let shared = self.services.shared.clone();
                let resources = shared.clone();
                return shared
                    .execute(ExecutionClass::Control, 0, 0, move || {
                        let owner = Arc::new(
                            resources.reserve_scratch(
                                ByteClass::Data,
                                crate::oltp::mvcc::json_decode_budget(bytes.as_ref())?
                                    .checked_mul(2)
                                    .ok_or_else(unknown)?,
                            )?,
                        );
                        let value: serde_json::Value =
                            serde_json::from_slice(bytes.as_ref()).map_err(|_| unknown())?;
                        Ok(OwnedReceipt {
                            _owner: Some(owner),
                            receipt: Receipt {
                                request_id: id,
                                run_id: run,
                                state: record.state,
                                result: value.get("data").filter(|value| !value.is_null()).cloned(),
                                error: value
                                    .get("error")
                                    .map(|value| {
                                        serde_json::from_value(value.clone()).map_err(|_| unknown())
                                    })
                                    .transpose()?,
                            },
                        })
                    })
                    .await;
            }
            Ok((record, None)) if record.state != RequestState::Reconciling => {
                return Ok(OwnedReceipt::from(Receipt {
                    request_id: id,
                    run_id: run,
                    state: record.state,
                    result: None,
                    error: None,
                }));
            }
            _ => {}
        }
        match self.durable_lookup(id).await? {
            Some(IdentityLookup::Terminal(terminal)) => {
                let terminal = if super::scoped_worker::terminal_reservation(&terminal)
                    .scoped
                    .is_some()
                {
                    let current = self.services.control.current(&self.services.scope).await?;
                    self.application_terminal(terminal, run, current.ready.view.run_id)
                        .await?
                } else {
                    terminal
                };
                let record = terminal_record(&terminal)?;
                if record.run_id != run {
                    return Err(QueueError::Abandoned.protocol());
                }
                if !self.terminal_receipt_ready(run, id, None).await? {
                    return Ok(committing());
                }
                let response = terminal_response(id, &terminal)?;
                let owner = Arc::new(
                    self.services.shared.reserve_scratch(
                        ByteClass::Data,
                        crate::oltp::mvcc::json_decode_budget(&response.body)?
                            .checked_mul(2)
                            .ok_or_else(unknown)?,
                    )?,
                );
                let value: serde_json::Value =
                    serde_json::from_slice(&response.body).map_err(|_| unknown())?;
                Ok(OwnedReceipt {
                    _owner: Some(owner),
                    receipt: Receipt {
                        request_id: id,
                        run_id: run,
                        state: record.state,
                        result: value.get("data").filter(|value| !value.is_null()).cloned(),
                        error: value
                            .get("error")
                            .map(|value| {
                                serde_json::from_value(value.clone()).map_err(|_| unknown())
                            })
                            .transpose()?,
                    },
                })
            }
            Some(IdentityLookup::Rejected(rejection)) => {
                Err(rejection.failure.result().err().ok_or_else(unknown)?)
            }
            Some(IdentityLookup::Abandoned(_)) => Err(QueueError::Abandoned.protocol()),
            Some(
                IdentityLookup::Reserved(reservation) | IdentityLookup::TimestampPoint(reservation),
            ) if reservation.original_run != run => Err(QueueError::Abandoned.protocol()),
            Some(IdentityLookup::TimestampPoint(_)) => Err(unknown()),
            _ => match local {
                Ok((record, _)) => Ok(OwnedReceipt::from(Receipt {
                    request_id: id,
                    run_id: run,
                    state: record.state,
                    result: None,
                    error: None,
                })),
                Err(error) => Err(error.protocol()),
            },
        }
    }
}
impl HttpRuntime for WorkerRuntime {
    fn ingress(&self) -> &Executor {
        &self.ingress
    }
    fn direct_ingress(&self) -> &Executor {
        &self.direct_ingress
    }
    fn direct_gateway(&self) -> Option<Arc<super::application::direct::DirectGateway>> {
        self.direct_gateway_handle().cloned()
    }
    fn scheduler(&self) -> ResourceScheduler {
        self.services.shared.scheduler()
    }
    fn scheduler_node_id(&self) -> u64 {
        self.services.shared.scheduler_node_id()
    }
    fn submit(
        self: Arc<Self>,
        body: Vec<u8>,
        parsing_charge: MemoryCharge,
        _point: Option<super::application::PointRequest>,
    ) -> EndpointFuture<'static, RequestRecord> {
        Box::pin(async move {
            let submitted = self.submit_owned(body).await;
            drop(parsing_charge);
            submitted
        })
    }
    fn receipt(&self, run: Uuid, id: Uuid) -> EndpointFuture<'_, OwnedReceipt> {
        Box::pin(self.receipt_owned(run, id))
    }
    fn wait<'a>(&'a self, record: &'a RequestRecord) -> EndpointFuture<'a, EncodedResponse> {
        Box::pin(async move {
            let mut copy_deadline = None;
            let mut interval = std::time::Duration::from_millis(50);
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let observe = self.receipt_owned(record.run_id, record.request_id);
                let receipt = match copy_deadline {
                    Some(deadline) => nabedra_rt::time::timeout_at(deadline, observe)
                        .await
                        .map_err(|_| unavailable())?,
                    None => observe.await,
                }?;
                if receipt.state == RequestState::Reconciling {
                    return Err(unknown());
                }
                if receipt.state.terminal() {
                    return Ok(match receipt.receipt.error {
                        Some(error) => response::encode_error(Some(record.request_id), error),
                        None => response::encode_success(
                            record.request_id,
                            &receipt.receipt.result,
                            false,
                        ),
                    });
                }
                if receipt.state == RequestState::Committing || copy_deadline.is_some() {
                    // A remote owner's confirmation does not notify this worker.
                    let now = nabedra_rt::time::now();
                    let deadline =
                        *copy_deadline.get_or_insert(now + super::RECEIPT_OBSERVATION_TIMEOUT);
                    if now >= deadline {
                        return Err(unavailable());
                    }
                    tokio::select! {
                        () = &mut changed => {},
                        () = nabedra_rt::time::sleep_until(deadline.min(now + interval)) => {},
                    }
                    interval = (interval * 2).min(std::time::Duration::from_millis(500));
                } else {
                    changed.await;
                }
            }
        })
    }
    fn admin_memory(&self) -> Result<Arc<ScratchLease>, ProtocolError> {
        Ok(Arc::new(
            self.services
                .shared
                .reserve_scratch(ByteClass::Progress, 128 << 10)?,
        ))
    }
    fn administer(self: Arc<Self>, body: Vec<u8>) -> EndpointFuture<'static, EncodedResponse> {
        Box::pin(async move {
            let owner = self.admin_memory()?;
            self.control_ingress
                .try_admit()?
                .spawn(move || async move {
                    let _owner = owner;
                    self.enroll_api_admin(body).await
                })?
                .receive()
                .await
        })
    }
    fn stats(&self) -> EndpointFuture<'_, EncodedResponse> {
        Box::pin(async move {
            #[cfg(test)]
            {
                super::recovery_tests::observe_data_voters(&self.services.manager);
                super::recovery_tests::observe_control_leader(
                    &self.services.manager,
                    self.services.control.identity().group_id,
                );
            }
            let (queue, accept_to_dispatch) = {
                let replica = self.services.replica.lock().map_err(|_| unknown())?;
                #[cfg(test)]
                super::recovery_tests::observe_record(&replica);
                (replica.stats(), replica.queue_latency_stats())
            };
            let scheduler = self.services.shared.scheduler().usage();
            #[allow(unused_mut)]
            let mut stats = serde_json::json!({
                "engine":"oltp", "queue":queue, "scheduler":scheduler,
                "accept_to_dispatch":accept_to_dispatch,
                "block_cache":self.services.shared.block_cache_usage(),
                "storage":self.services.shared.storage_stats(),
                "storage_reservations":self.services.shared.storage_reservations(),
                // Rule 6: secondary pools refused at open, with the error class.
                "refused_pools":self.services.pools.refused_report(),
                "working_decode_bytes":self.services.shared.working_decode_bytes(),
                "native_thread_budget":self.services.shared.thread_budget(),
                // `null` until `data.direct.v1` activates the executor.
                "direct":match self.direct_switch().and_then(|switch| switch.direct_stats()) {
                    Some(stats) => serde_json::to_value(stats).map_err(|_| unknown())?,
                    None => crate::oltp_replication::service_rpc::direct::installed(&self.services.shared)
                        .map(|service| service.stats())
                        .unwrap_or(serde_json::Value::Null),
                },
                "direct_capability":self.direct_switch().map(|switch| serde_json::json!({
                    "active":switch.is_direct_active(), "direct_oltp":switch.direct_oltp(),
                    "history_settling":switch.history_settling(),
                    // GC2: `data.direct.batch` enabled (DirectBatch entries).
                    "batch":switch.is_batch_enabled(),
                })),
                "direct_gateway":self.direct_gateway_handle().map(|gateway| gateway.stats()),
                // Local lane refusals (NS11 finding 8).
                "transport":self.services.manager.client().lane_refusals(),
                // RV-APPLY: transient retries and stopped cores.
                "raft":self.services.manager.raft_health(),
                // PERF1: per-stage latency histograms of this process.
                "stage_timing":super::stage_timing::snapshot(),
            });
            #[cfg(test)]
            {
                if let Some(dir) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")
                    && std::fs::remove_file(std::path::Path::new(&dir).join("reset-growth-decode"))
                        .is_ok()
                {
                    self.services.shared.reset_working_decode_peak();
                }
                stats["test_working_memory"] = serde_json::json!({
                    "decode_peak_bytes": self.services.shared.peak_working_decode_bytes(),
                    "task_limit_bytes": self.services.shared.task_memory_limit_bytes(),
                });
                if let Some(source) = &self.services.direct_timestamps
                    && let Some(probe) =
                        super::recovery_tests::timestamp_probe(source.as_ref()).await
                {
                    stats["test_timestamp"] = probe;
                }
                if let Some(group) = super::recovery_tests::group_leader_probe() {
                    stats["test_group_leader"] =
                        serde_json::json!(self.services.manager.test_group_leader(group));
                }
                if let Some(request) = super::recovery_tests::pool_sync::status_probe() {
                    let lookup = match self.durable_lookup(request).await {
                        Ok(Some(IdentityLookup::Terminal(terminal))) => {
                            let result = match &terminal {
                                TerminalIdentity::Committed { outcome, .. } => {
                                    Some(outcome.result()?)
                                }
                                _ => None,
                            };
                            serde_json::json!({"terminal": terminal, "result": result})
                        }
                        Ok(Some(_)) => serde_json::json!({"pending": true}),
                        Ok(None) => serde_json::Value::Null,
                        Err(error) => serde_json::json!({"error": error}),
                    };
                    stats["test_pool_sync"] = serde_json::json!({
                        "pools_requiring_recovery": self.services.pools.pool_ids()?.into_iter()
                            .filter_map(|id| self.services.pools.get(id).ok().flatten())
                            .filter(|pool| pool.requires_recovery()).count(),
                        "status": lookup,
                    });
                }
                if let Some(observed) =
                    super::recovery_tests::maintenance::observe(&self.services).await?
                {
                    stats["test_maintenance"] = observed;
                }
            }
            Ok(EncodedResponse {
                status: 200,
                body: serde_json::to_vec(&stats).map_err(|_| unknown())?,
            })
        })
    }
    fn health(&self) -> EndpointFuture<'_, EncodedResponse> {
        Box::pin(async move {
            if !self.accepting.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            let current = self.services.control.current(&self.services.scope).await?;
            let factor = self.services.replication_factor;
            let timing = if factor > 1 {
                Some(super::bootstrap::registered_raft_config())
            } else {
                None
            };
            let timing = timing.map(|config| serde_json::json!({"heartbeat_ms":config.heartbeat_interval,"election_min_ms":config.election_timeout_min,"election_max_ms":config.election_timeout_max}));
            Ok(EncodedResponse {status:200,body:serde_json::to_vec(&serde_json::json!({"status":"ready","engine":"oltp","topology":if factor==1 {"typed-groups-rf1"} else {"typed-groups-distributed"},"replication_factor":factor,"consensus_timing":timing,"run_id":current.ready.view.run_id,"cluster_id":self.services.scope.cluster_id,"ready_workers":current.ready.ready_count,"native_thread_budget":self.services.shared.thread_budget()})).map_err(|_|unknown())?})
        })
    }
}
struct Settlement {
    runtime: Arc<WorkerRuntime>,
    id: Uuid,
    armed: bool,
}
impl Drop for Settlement {
    fn drop(&mut self) {
        if self.armed {
            self.runtime.mark_unknown(self.id)
        }
        if let Ok(mut active) = self.runtime.active.lock() {
            active.remove(&self.id);
        }
        self.runtime.changed.notify_waiters();
    }
}
pub(super) fn terminal_response(
    id: Uuid,
    terminal: &TerminalIdentity,
) -> Result<EncodedResponse, ProtocolError> {
    match terminal {
        TerminalIdentity::Committed { outcome, .. } | TerminalIdentity::Failed { outcome, .. } => {
            Ok(match outcome.result() {
                Ok(value) => response::encode_success(id, &value, true),
                Err(error) => response::encode_error(Some(id), error),
            })
        }
        TerminalIdentity::ReadExpired { .. } => Err(QueueError::NotFound.protocol()),
    }
}
fn terminal_reservation(
    terminal: &TerminalIdentity,
) -> Result<&IdentityReservation, ProtocolError> {
    match terminal {
        TerminalIdentity::Committed { reservation, .. }
        | TerminalIdentity::Failed { reservation, .. } => Ok(reservation),
        TerminalIdentity::ReadExpired { .. } => Err(QueueError::NotFound.protocol()),
    }
}
fn terminal_record(terminal: &TerminalIdentity) -> Result<RequestRecord, ProtocolError> {
    let reservation = terminal_reservation(terminal)?;
    let response = terminal_response(reservation.request_id, terminal)?;
    Ok(terminal_record_with_status(reservation, response.status))
}
pub(super) fn terminal_record_with_status(
    reservation: &IdentityReservation,
    status: u16,
) -> RequestRecord {
    RequestRecord {
        context: None,
        run_id: reservation.original_run,
        scheduler_term: 1,
        membership_revision: 1,
        request_id: reservation.request_id,
        digest: reservation.digest.to_string(),
        sequence: 1,
        version: 1,
        body: Vec::new(),
        accepted: true,
        state: if status == 200 {
            RequestState::Succeeded
        } else {
            RequestState::Failed
        },
        pinned_catalog: None,
        response_status: Some(status),
        response_body: None,
        completed_at_ms: Some(now()),
        result_expired: false,
        replicas: Default::default(),
    }
}
fn attach_reconciling_read(
    reconciling: Option<ApplicationObservation>,
    status: Result<Option<IdentityLookup>, ProtocolError>,
    local_error: QueueError,
    expected: Option<&ApplicationObservation>,
) -> Result<ObservedApplication, ProtocolError> {
    // Takeover can keep a read in Reconciling until its old grants settle.
    // Require both the durable read identity and the accepted RAM copy with its pin.
    let Some(IdentityLookup::Reserved(reservation)) = status? else {
        return Err(local_error.protocol());
    };
    let metadata = reconciling
        .filter(|observation| {
            let record = &observation.record;
            reservation.kind == RequestKind::Read
                && reservation.original_run == record.run_id
                && reservation.request_id == record.request_id
                && Digest::parse(&record.digest).is_ok_and(|digest| digest == reservation.digest)
        })
        .ok_or_else(|| local_error.protocol())?;
    attach_result(metadata, expected, || Ok(None))
}

fn pending_terminal_copy(
    mut record: RequestRecord,
    expected: Option<&ApplicationObservation>,
) -> Result<ObservedApplication, ProtocolError> {
    record.state = RequestState::Committing;
    record.response_status = None;
    record.completed_at_ms = None;
    attach_result(
        ApplicationObservation {
            record,
            response_hash: None,
            durable_proof: false,
        },
        expected,
        || Ok(None),
    )
}

pub(super) fn attach_result(
    metadata: ApplicationObservation,
    expected: Option<&ApplicationObservation>,
    body: impl FnOnce() -> Result<Option<ChargedBytes>, ProtocolError>,
) -> Result<ObservedApplication, ProtocolError> {
    if let Some(expected) = expected {
        if metadata.response_hash.is_none()
            || metadata.response_hash != expected.response_hash
            || (!metadata.durable_proof
                && !expected.durable_proof
                && metadata.record.version < expected.record.version)
            || metadata.record.run_id != expected.record.run_id
            || metadata.record.request_id != expected.record.request_id
            || metadata.record.digest != expected.record.digest
            || metadata.record.state != expected.record.state
            || metadata.record.response_status != expected.record.response_status
        {
            return Err(unknown());
        }
    }
    Ok(ObservedApplication {
        metadata,
        body: body()?,
    })
}

fn receipt_body(
    class: ByteClass,
    result: Result<Option<ChargedBytes>, ProtocolError>,
) -> Result<Option<ChargedBytes>, ProtocolError> {
    match result {
        Err(mut error)
            if matches!(error.code, ErrorCode::Overloaded | ErrorCode::LimitExceeded) =>
        {
            if matches!(class, ByteClass::Progress) {
                // The metadata can still use the protected observation lane.
                return Ok(None);
            }
            error.outcome = Outcome::Unknown;
            Err(error)
        }
        result => result,
    }
}

#[cfg(test)]
#[test]
fn q04_r01_r02_reconciling_observation_requires_exact_reserved_read() {
    let reservation = IdentityReservation {
        request_id: nabedra_rt::ids::uuid_v4(),
        digest: Digest::from_bytes([1; 32]),
        original_run: nabedra_rt::ids::uuid_v4(),
        reservation_id: nabedra_rt::ids::uuid_v4(),
        kind: RequestKind::Read,
        scoped: None,
    };
    let mut record = terminal_record_with_status(&reservation, 200);
    record.state = RequestState::Reconciling;
    record.response_status = None;
    record.completed_at_ms = None;
    let pending = ApplicationObservation {
        record,
        response_hash: None,
        durable_proof: false,
    };
    let reserved = |reservation| Ok(Some(IdentityLookup::Reserved(reservation)));
    let local_error = QueueError::Unavailable;
    let observed = attach_reconciling_read(
        Some(pending.clone()),
        reserved(reservation),
        local_error,
        None,
    )
    .unwrap();
    assert_eq!(observed.metadata.record.state, RequestState::Reconciling);
    assert_eq!(
        observed.metadata.record.digest,
        reservation.digest.to_string()
    );
    assert!(observed.body.is_none());
    for mismatched in [
        IdentityReservation {
            kind: RequestKind::Mutation,
            ..reservation
        },
        IdentityReservation {
            original_run: nabedra_rt::ids::uuid_v4(),
            ..reservation
        },
        IdentityReservation {
            request_id: nabedra_rt::ids::uuid_v4(),
            ..reservation
        },
        IdentityReservation {
            digest: Digest::from_bytes([2; 32]),
            ..reservation
        },
    ] {
        assert_eq!(
            attach_reconciling_read(
                Some(pending.clone()),
                reserved(mismatched),
                local_error,
                None
            )
            .err(),
            Some(local_error.protocol())
        );
    }
    let unavailable = ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "Status lookup unavailable",
        Outcome::Unknown,
    );
    assert_eq!(
        attach_reconciling_read(
            Some(pending.clone()),
            Err(unavailable.clone()),
            local_error,
            None
        )
        .err(),
        Some(unavailable)
    );
    assert_eq!(
        attach_reconciling_read(Some(pending.clone()), Ok(None), local_error, None).err(),
        Some(local_error.protocol())
    );
    assert_eq!(
        attach_reconciling_read(None, reserved(reservation), QueueError::NotFound, None).err(),
        Some(QueueError::NotFound.protocol())
    );
    assert!(
        attach_reconciling_read(
            Some(pending.clone()),
            reserved(reservation),
            local_error,
            Some(&pending)
        )
        .is_err()
    );
}

#[cfg(test)]
#[test]
fn q04_q06_result_snapshot_checks_request_digest_status_and_terminal_hash() {
    let reservation = IdentityReservation {
        request_id: nabedra_rt::ids::uuid_v4(),
        digest: Digest::from_bytes([1; 32]),
        original_run: nabedra_rt::ids::uuid_v4(),
        reservation_id: nabedra_rt::ids::uuid_v4(),
        kind: RequestKind::Mutation,
        scoped: None,
    };
    let original = ApplicationObservation {
        record: terminal_record_with_status(&reservation, 200),
        response_hash: Some(Digest::from_bytes([2; 32])),
        durable_proof: true,
    };
    assert!(attach_result(original.clone(), Some(&original), || Ok(None)).is_ok());
    let mut wrong_digest = original.clone();
    wrong_digest.record.digest = Digest::from_bytes([3; 32]).to_string();
    let mut wrong_payload = original.clone();
    wrong_payload.response_hash = Some(Digest::from_bytes([3; 32]));
    let mut wrong_status = original.clone();
    wrong_status.record.response_status = Some(202);
    let mut wrong_run = original.clone();
    wrong_run.record.run_id = nabedra_rt::ids::uuid_v4();
    let mut wrong_request = original.clone();
    wrong_request.record.request_id = nabedra_rt::ids::uuid_v4();
    for altered in [
        wrong_digest,
        wrong_payload,
        wrong_status,
        wrong_run,
        wrong_request,
    ] {
        assert!(
            attach_result(original.clone(), Some(&altered), || panic!(
                "released wrong result"
            ))
            .is_err()
        );
    }
    let expired = TerminalIdentity::ReadExpired {
        reservation: IdentityReservation {
            kind: RequestKind::Read,
            ..reservation
        },
        request: RequestBinding {
            run_id: reservation.original_run,
            request_id: reservation.request_id,
            digest: reservation.digest,
            accepted_context: Digest::from_bytes([4; 32]),
        },
        terminal_version: 2,
        response_hash: original.response_hash.unwrap(),
        recorded_at: AppliedPosition {
            term: 1,
            node_id: 1,
            index: 1,
        },
    };
    let error = terminal_reservation(&expired).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
    assert_eq!(error.message, "receipt unavailable or expired");
    let capacity = QueueError::Overloaded.protocol();
    assert!(
        receipt_body(ByteClass::Progress, Err(capacity.clone()))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        receipt_body(ByteClass::Data, Err(capacity))
            .unwrap_err()
            .outcome,
        Outcome::Unknown
    );
}

fn now() -> u64 {
    nabedra_rt::time::wall_now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
fn unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "original accepted request requires reconciliation",
        Outcome::Unknown,
    )
}
fn uncertain_admission(mut error: ProtocolError) -> ProtocolError {
    error.outcome = Outcome::Unknown;
    error
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "worker runtime is not accepting requests",
        Outcome::NotApplied,
    )
}

/// The sole current owner holds this UUID's admission gate and has established
/// that no local request record/copy phase exists. Only this runtime can create
/// the capability; parsing a client payload cannot create rejection authority.
pub(crate) struct VerifiedNeverAccepted {
    scope: QueueScope,
    view: AuthorityView,
    reservation: crate::oltp_replication::authority::status::IdentityReservation,
    identity: PinIdentity,
    pin: Option<StoragePin>,
    request: Option<RequestBinding>,
    failure: ProtocolError,
    pub(super) admission_gate: Arc<tokio::sync::OwnedMutexGuard<()>>,
    pub(super) live_admission: std::sync::OnceLock<Arc<super::admission_tickets::LiveAdmission>>,
}
impl crate::oltp_replication::queue_access::NeverAcceptedAdmission for VerifiedNeverAccepted {
    fn scope(&self) -> &QueueScope {
        &self.scope
    }
    fn view(&self) -> AuthorityView {
        self.view
    }
    fn reservation(
        &self,
    ) -> crate::oltp_replication::authority::status::IdentityReservation {
        self.reservation
    }
    fn identity(&self) -> &PinIdentity {
        &self.identity
    }
    fn pin(&self) -> Option<StoragePin> {
        self.pin
    }
    fn request(&self) -> Option<RequestBinding> {
        self.request
    }
    fn failure(&self) -> &ProtocolError {
        &self.failure
    }
}
