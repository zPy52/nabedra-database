//! rv-j3 N1-N3: liveness of the L24 search DDL repair on a lone runtime.
//! An index declaration of the table is the other DDL: it holds the table's
//! Exclusive guard and fences the range ahead of the Catalog before it takes
//! its source pin, where the wrapper below stops it.
use super::*;
use crate::oltp::mvcc::{
    catalog::PinnedCatalog,
    index::{
        IndexDefinition,
        build::{IndexAttempt, IndexBackend, IndexCommitOutcome, IndexService},
    },
    planner::{MutationPlan, ResolveFuture},
    store::{
        index::VerifiedIndexAttemptClosed,
        lifecycle::{LifecycleBinding, VerifiedLifecycleGuard},
    },
};
use crate::oltp_replication::types::{Page, Text};

/// The production backend, stopped in `source_pin` until the test releases
/// it; the declaration then fails. With `crash`, its actor stops after the
/// Status failure and before it releases its guard and pin.
struct Stalled {
    runtime: Arc<WorkerRuntime>,
    inner: Arc<dyn IndexBackend>,
    entered: Notify,
    release: Notify,
    crash: bool,
}
impl IndexBackend for Stalled {
    fn planner(&self) -> &NativePlanner {
        self.inner.planner()
    }
    fn checkpoint(&self) -> ResolveFuture<'_, ()> {
        self.inner.checkpoint()
    }
    fn repair_search_ddl(&self, table_id: Uuid) -> ResolveFuture<'_, ()> {
        self.inner.repair_search_ddl(table_id)
    }
    fn begin(&self, job: Uuid, digest: Digest, maintenance: MaintenanceOwnerRef, fixed: Option<Uuid>) -> ResolveFuture<'_, IndexAttempt> {
        self.inner.begin(job, digest, maintenance, fixed)
    }
    fn commit(&self, attempt: IndexAttempt, plan: MutationPlan) -> ResolveFuture<'_, IndexCommitOutcome> {
        self.inner.commit(attempt, plan)
    }
    fn source_pin(&self, _: Authority, _: MaintenanceOwnerRef) -> ResolveFuture<'_, (StoragePin, PinnedCatalog)> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            Err(ProtocolError::new(ErrorCode::StorageUnavailable, "the test stops the build", Outcome::NotApplied))
        })
    }
    fn refresh(&self, table: Uuid, key: protocol::Key, job: Uuid) -> ResolveFuture<'_, ()> {
        self.inner.refresh(table, key, job)
    }
    fn close_source(&self, authority: Authority, proof: VerifiedIndexAttemptClosed) -> ResolveFuture<'_, ()> {
        self.inner.close_source(authority, proof)
    }
    fn help(&self, intent: IntentRef) -> ResolveFuture<'_, ()> {
        self.inner.help(intent)
    }
    fn help_lifecycle(&self, binding: LifecycleBinding) -> ResolveFuture<'_, ()> {
        self.inner.help_lifecycle(binding)
    }
    fn fail(
        &self,
        attempt: IndexAttempt,
        guards: Vec<VerifiedLifecycleGuard>,
        pending: Option<LifecycleBinding>,
        error: ProtocolError,
    ) -> ResolveFuture<'_, ()> {
        if !self.crash {
            return self.inner.fail(attempt, guards, pending, error);
        }
        Box::pin(async move {
            let status = &self.runtime.services.status;
            status.terminal_failure(attempt.pin.authority, attempt.request, error).await?;
            Err(ProtocolError::new(ErrorCode::StorageUnavailable, "the actor stopped", Outcome::Unknown))
        })
    }
}

/// Starts an index declaration of `table` and returns once it holds the
/// table's guard and fenced the range ahead of the Catalog.
async fn stalled_declaration(
    runtime: &Arc<WorkerRuntime>,
    table: Uuid,
    crash: bool,
) -> (Arc<Stalled>, tokio::task::JoinHandle<Result<(), ProtocolError>>) {
    let stalled = Arc::new(Stalled {
        runtime: runtime.clone(),
        inner: runtime.index_backend_for_test(),
        entered: Notify::new(),
        release: Notify::new(),
        crash,
    });
    let service = IndexService::new(stalled.clone());
    let definition = IndexDefinition {
        index_id: nabedra_rt::ids::uuid_v4(),
        table_id: table,
        name: Text::try_from("by_body").unwrap(),
        columns: Page::new(vec![Text::try_from("body").unwrap()]).unwrap(),
        covering: Page::new(Vec::new()).unwrap(),
    };
    let build = tokio::spawn(async move { service.create(definition, nabedra_rt::ids::uuid_v4(), 1).await });
    nabedra_rt::time::timeout(std::time::Duration::from_secs(30), stalled.entered.notified())
        .await
        .expect("the declaration reaches its source pin");
    (stalled, build)
}
fn add(table: Uuid, name: &str) -> Value {
    request("table.add_column", json!({"table_id":table,"name":name,"type":{"type":"vector","dim":3}}))
}
async fn refused(runtime: &Arc<WorkerRuntime>, request: Value) -> ProtocolError {
    runtime.admit(serde_json::to_vec(&request).unwrap()).await.expect_err("the DDL is refused")
}

/// rv-j3 N1: a repair that fails takes a fresh id next time. A live DDL holds
/// the guard after it fenced the range, so every repair conflicts; after more
/// than three failed repairs it fails and the next column DDL rolls its fence
/// forward. Before, the run had three repair ids per epoch, all spent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rvj3_a_table_recovers_after_more_than_three_failed_repairs() {
    let path = std::env::temp_dir().join(format!("nabedra-repair-ids-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let (runtime, table, group) = ddl_table(&path).await;
    let (stalled, build) = stalled_declaration(&runtime, table, false).await;
    assert_eq!(ids(&schema(&runtime, &group).await), (2, vec![1], vec![1], vec![]));
    for _ in 0..2 {
        let error = refused(&runtime, add(table, "w")).await;
        assert_eq!(error.code, ErrorCode::Conflict, "{error:?}");
    }
    stalled.release.notify_one();
    assert!(build.await.unwrap().is_err());

    run(&runtime, add(table, "w")).await;
    assert_eq!(ids(&schema(&runtime, &group).await), (3, vec![1, 2], vec![1], vec![]));
    assert_eq!(status(&runtime, table).await["w"]["state"], "building");
    runtime.drain().await.unwrap();
}

/// rv-j3 N2: a DDL that stopped after its Status failure, still holding the
/// Exclusive guard over a range it fenced ahead, no longer blocks the next
/// column DDL: on the guard's Conflict the repair helps the exclusive holder,
/// which releases a failed holder's guard, and retries. Before, every
/// repair conflicted until the run changed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rvj3_a_crashed_guard_holder_does_not_block_the_next_column_ddl() {
    let path = std::env::temp_dir().join(format!("nabedra-repair-help-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let (runtime, table, group) = ddl_table(&path).await;
    let (stalled, build) = stalled_declaration(&runtime, table, true).await;
    stalled.release.notify_one();
    assert_eq!(build.await.unwrap().unwrap_err().outcome, Outcome::Unknown);
    assert_eq!(ids(&schema(&runtime, &group).await), (2, vec![1], vec![1], vec![]));

    run(&runtime, add(table, "w")).await;
    assert_eq!(ids(&schema(&runtime, &group).await), (3, vec![1, 2], vec![1], vec![]));
    runtime.drain().await.unwrap();
}
