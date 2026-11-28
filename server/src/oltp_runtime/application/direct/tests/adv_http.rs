//! Adversarial tests of the HTTP gateway for the new operations (task C1:
//! VEC-G14, VEC-G09). The runtime is a stub that counts what reaches the
//! queue, so a refusal is told from an accepted request.
use crate::{
    executor::Executor,
    oltp_replication::multiplex::EndpointFuture,
    oltp_runtime::{
        http::router_for,
        http_runtime::{DIRECT_INGRESS_SLOTS, HttpRuntime, OwnedReceipt},
    },
    protocol::{self, ErrorCode, ErrorResponse, Outcome, ProtocolError},
    request_queue::RequestRecord,
    response::EncodedResponse,
    scheduling::{MemoryCharge, ResourceScheduler},
};
use axum::{body::Body, http::Request as HttpRequest};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tower::ServiceExt as _;
use uuid::Uuid;

const TOKEN: &str = "adv-token";
const DATABASE: &str = "advdb";

/// A runtime without a direct gateway. A request that reaches its queue is
/// counted and refused with `STOPPED`.
struct Stub {
    ingress: Executor,
    direct_ingress: Executor,
    scheduler: ResourceScheduler,
    submits: AtomicUsize,
}
impl Stub {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            ingress: Executor::new(2, 2).unwrap(),
            direct_ingress: Executor::new(DIRECT_INGRESS_SLOTS, 0).unwrap(),
            scheduler: ResourceScheduler::new(vec![(0, crate::config::WorkerResources::default())])
                .unwrap(),
            submits: AtomicUsize::new(0),
        })
    }
}
const STOPPED: &str = "the stub queue stops here";
fn stopped() -> ProtocolError {
    ProtocolError::new(ErrorCode::Overloaded, STOPPED, Outcome::NotApplied)
}
impl HttpRuntime for Stub {
    fn ingress(&self) -> &Executor {
        &self.ingress
    }
    fn direct_ingress(&self) -> &Executor {
        &self.direct_ingress
    }
    fn scheduler(&self) -> ResourceScheduler {
        self.scheduler.clone()
    }
    fn scheduler_node_id(&self) -> u64 {
        0
    }
    fn submit(
        self: Arc<Self>,
        _body: Vec<u8>,
        _charge: MemoryCharge,
        _point: Option<crate::oltp_runtime::application::PointRequest>,
    ) -> EndpointFuture<'static, RequestRecord> {
        Box::pin(async move {
            self.submits.fetch_add(1, Ordering::SeqCst);
            Err(stopped())
        })
    }
    fn receipt(&self, _run: Uuid, _id: Uuid) -> EndpointFuture<'_, OwnedReceipt> {
        Box::pin(async { Err(stopped()) })
    }
    fn wait<'a>(&'a self, _record: &'a RequestRecord) -> EndpointFuture<'a, EncodedResponse> {
        Box::pin(async { Err(stopped()) })
    }
    fn health(&self) -> EndpointFuture<'_, EncodedResponse> {
        Box::pin(async { Err(stopped()) })
    }
}

/// The answer to one request on `path`, and how many reached the queue.
async fn send(path: &str, operation: &str, payload: &str) -> (u16, ProtocolError, usize) {
    let runtime = Stub::new();
    let app = router_for(runtime.clone(), TOKEN.into(), DATABASE.into()).unwrap();
    let body = format!(
        r#"{{"request_id":"{}","operation":"{operation}","payload":{payload}}}"#,
        nabedra_rt::ids::uuid_v4()
    );
    let reply = app
        .oneshot(
            HttpRequest::post(path)
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .header("content-length", body.len())
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = reply.status().as_u16();
    let bytes = axum::body::to_bytes(reply.into_body(), protocol::MAX_BODY_BYTES)
        .await
        .unwrap();
    let reply: ErrorResponse = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&bytes)));
    (status, reply.error, runtime.submits.load(Ordering::SeqCst))
}

const TABLE: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";

/// VEC-G09: a group of rows needs the direct path. Where the gateway has none
/// (or the request is queued), it is refused and nothing reaches the queue:
/// the queued path never runs the rows of a `*_many` call whole.
#[tokio::test]
async fn adv_http_the_many_operations_are_never_queued_whole() {
    for path in ["/v1/operations", "/v1/requests"] {
        for (operation, payload) in [
            (
                "table.upsert_many",
                format!(r#"{{"table_id":"{TABLE}","rows":[{{"id":1,"value":1}}]}}"#),
            ),
            (
                "table.delete_many",
                format!(r#"{{"table_id":"{TABLE}","keys":[1,2]}}"#),
            ),
        ] {
            let (status, error, submits) = send(path, operation, &payload).await;
            assert_eq!(
                error.code,
                ErrorCode::UnsupportedOperation,
                "{path} {operation}: {error:?} {status}"
            );
            assert_eq!(error.outcome, Outcome::NotApplied);
            assert_eq!(submits, 0, "{path} {operation} reached the queue");
        }
    }
}

/// VEC-G14: while `data.vector.v1` is off, no table that needs it is created
/// and no operation on search is queued; a plain table is unaffected.
#[tokio::test]
async fn adv_http_vector_tables_and_search_operations_wait_for_the_feature() {
    let vector = r#"{"id":"utf8","v":{"type":"vector","dim":2}}"#;
    let cases = [
        (vector, ""),
        (r#"{"id":"utf8","s":{"type":"vector","sparse":true}}"#, ""),
        (
            r#"{"id":"utf8","m":{"type":"list","item":{"type":"vector","dim":2}}}"#,
            "",
        ),
        (r#"{"id":"utf8","t":{"type":"list","item":"utf8"}}"#, ""),
        (
            r#"{"id":"utf8","b":"utf8"}"#,
            r#","full_text":{"b":"english"}"#,
        ),
        (r#"{"id":"utf8","b":"utf8"}"#, r#","partition_by":"b""#),
    ];
    for path in ["/v1/operations", "/v1/requests"] {
        for (schema, extra) in cases {
            let payload =
                format!(r#"{{"name":"t","schema":{schema},"key":"id","format":"parquet"{extra}}}"#);
            let (_, error, submits) = send(path, "table.create", &payload).await;
            assert_eq!(
                error.code,
                ErrorCode::UnsupportedOperation,
                "{path} {schema}{extra}: {error:?}"
            );
            assert!(error.message.contains("data.vector.v1"), "{error:?}");
            assert_eq!(submits, 0, "{path} {schema}{extra}");
        }
        // The same create without search columns is queued as before.
        let plain =
            r#"{"name":"t","schema":{"id":"utf8","n":"int64"},"key":"id","format":"parquet"}"#;
        let (_, error, submits) = send(path, "table.create", plain).await;
        assert_eq!((error.message.as_str(), submits), (STOPPED, 1), "{path}");
        // Operations this build decodes but does not serve are refused alike.
        for (operation, payload) in [
            (
                "table.add_column",
                format!(
                    r#"{{"table_id":"{TABLE}","name":"w","type":{{"type":"vector","dim":2}}}}"#
                ),
            ),
            (
                "table.drop_column",
                format!(r#"{{"table_id":"{TABLE}","name":"w"}}"#),
            ),
            (
                "table.add_full_text",
                format!(r#"{{"table_id":"{TABLE}","column":"b","language":"english"}}"#),
            ),
            (
                "table.drop_full_text",
                format!(r#"{{"table_id":"{TABLE}","column":"b"}}"#),
            ),
            (
                "table.search_status",
                format!(r#"{{"table_id":"{TABLE}"}}"#),
            ),
            (
                "query.rank",
                format!(
                    r#"{{"table_id":"{TABLE}","query":{{"select":null,"filter":null,"limit":1}},"rank":{{"rankings":[{{"column":"b","op":"match","value":"x"}}]}}}}"#
                ),
            ),
            (
                "query.rank_many",
                format!(
                    r#"{{"table_id":"{TABLE}","query":{{"select":null,"filter":null,"limit":1}},"rank":{{"rankings":[]}},"batch":[{{"column":"b","op":"match","value":"x"}}]}}"#
                ),
            ),
        ] {
            let (_, error, submits) = send(path, operation, &payload).await;
            // J serves the column operations and status through the queue.
            if operation.starts_with("table.") {
                assert_eq!((error.message.as_str(), submits), (STOPPED, 1), "{path} {operation}");
                continue;
            }
            assert_eq!(
                error.code,
                ErrorCode::UnsupportedOperation,
                "{path} {operation}: {error:?}"
            );
            assert_eq!(submits, 0, "{path} {operation}");
        }
    }
}

/// An undecodable body of a new operation is a validation error that names no
/// partial effect, and reaches nothing.
#[tokio::test]
async fn adv_http_malformed_vector_requests_are_refused_before_the_queue() {
    let bad = [
        ("table.upsert_many", format!(r#"{{"table_id":"{TABLE}","rows":[{{"id":1,"v":{{"f32":"AAA"}}}}]}}"#)),
        ("table.upsert_many", format!(r#"{{"table_id":"{TABLE}","rows":[{{"id":1,"v":{{"f32":"AAAAAA==","x":1}}}}]}}"#)),
        ("table.upsert_many", format!(r#"{{"table_id":"{TABLE}","rows":[{{"id":1}},{{"id":1}}]}}"#)),
        ("table.upsert_many", format!(r#"{{"table_id":"{TABLE}","rows":{{}}}}"#)),
        ("table.delete_many", format!(r#"{{"table_id":"{TABLE}","keys":[1,1]}}"#)),
        ("table.delete_many", format!(r#"{{"table_id":"{TABLE}","keys":[null]}}"#)),
        ("table.create", r#"{"name":"t","schema":{"id":"utf8","v":{"type":"vector","dim":0}},"key":"id","format":"parquet"}"#.to_string()),
        ("table.create", r#"{"name":"t","schema":{"id":"utf8","v":{"type":"vector","dim":4097}},"key":"id","format":"parquet"}"#.to_string()),
    ];
    for path in ["/v1/operations", "/v1/requests"] {
        for (operation, payload) in &bad {
            let (_, error, submits) = send(path, operation, payload).await;
            assert!(
                matches!(
                    error.code,
                    ErrorCode::ValidationError | ErrorCode::UnsupportedOperation
                ),
                "{path} {operation} {payload}: {error:?}"
            );
            assert_eq!(error.outcome, Outcome::NotApplied);
            assert_eq!(submits, 0, "{path} {operation} {payload}");
        }
    }
}
