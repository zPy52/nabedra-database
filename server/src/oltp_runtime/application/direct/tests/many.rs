//! `table.upsert_many` and `table.delete_many` on real three-voter groups
//! (VEC-G09, VS15): one exact outcome per row, and each row applies once.
use super::*;
use crate::protocol::{RowOutcome, RowOutcomes, row_request_id};

/// The outcomes of one call. The reply always lists one per row.
async fn many(
    gateway: &DirectGateway,
    body: &[u8],
    queue: Option<&dyn QueuedRows>,
) -> Result<Vec<RowOutcome>, (ErrorCode, Outcome)> {
    let request: Value = serde_json::from_slice(body).unwrap();
    match gateway.route_with(body, false, queue).await {
        Ok(Routed::Done(encoded)) => {
            assert_eq!(encoded.status, 200);
            let mut envelope: Value = serde_json::from_slice(&encoded.body).unwrap();
            assert_eq!(envelope["request_id"], request["request_id"]);
            let outcomes: RowOutcomes = serde_json::from_value(envelope["data"].take()).unwrap();
            let payload = &request["payload"];
            let rows = payload.get("rows").or(payload.get("keys")).unwrap();
            assert_eq!(outcomes.outcomes.len(), rows.as_array().unwrap().len());
            Ok(outcomes.outcomes)
        }
        Ok(Routed::Queued) => panic!("a many call is never queued whole"),
        Err(error) => Err((error.code, error.outcome)),
    }
}
fn applied(outcomes: &[RowOutcome]) -> bool {
    outcomes
        .iter()
        .all(|outcome| *outcome == RowOutcome::Applied)
}
fn unknown_rows(outcomes: &[RowOutcome]) -> bool {
    outcomes
        .iter()
        .all(|outcome| matches!(outcome, RowOutcome::OutcomeUnknown { .. }))
}
fn refused_rows(outcomes: &[RowOutcome], code: ErrorCode) -> bool {
    outcomes.iter().all(
        |outcome| matches!(outcome, RowOutcome::NotApplied { error } if error.code == code && error.outcome == Outcome::NotApplied),
    )
}
fn rows(ids: impl IntoIterator<Item = i64>, value: i64) -> Value {
    ids.into_iter()
        .map(|id| json!({"id": id, "value": value + id}))
        .collect()
}
impl Cluster {
    async fn batching() -> Self {
        let cluster = Self::new().await;
        cluster.gate.0.store(true, Ordering::Release);
        cluster
    }
    fn upsert_many(&self, call: Uuid, rows: Value) -> Vec<u8> {
        self.body(call, "table.upsert_many", json!({ "rows": rows }))
    }
    fn delete_many(&self, call: Uuid, keys: Value) -> Vec<u8> {
        self.body(call, "table.delete_many", json!({ "keys": keys }))
    }
    async fn versions(&self, group: usize, ids: std::ops::Range<i64>) -> Vec<Option<CommitTs>> {
        let leader = self.leader(group).await;
        let mut out = Vec::new();
        for id in ids {
            out.push(self.latest(group, leader, id).await.0);
        }
        out
    }
    async fn row_settled(&self, group: usize, call: Uuid, id: i64) -> bool {
        let request_id = row_request_id(call, &Key::Int64(id)).unwrap();
        self.settled(group, id, request_id).await
    }
}

/// A queued path that answers every row with `status` and keeps its body.
struct Queue {
    bodies: Mutex<Vec<Vec<u8>>>,
    fail: Option<ProtocolError>,
}
impl Queue {
    fn new(fail: Option<ProtocolError>) -> Self {
        Self {
            bodies: Mutex::default(),
            fail,
        }
    }
    fn bodies(&self) -> Vec<Vec<u8>> {
        self.bodies.lock().unwrap().clone()
    }
}
impl QueuedRows for Queue {
    fn run(&self, body: Vec<u8>) -> EndpointFuture<'_, EncodedResponse> {
        let id = serde_json::from_slice::<Value>(&body).unwrap()["request_id"].clone();
        self.bodies.lock().unwrap().push(body);
        let id: Uuid = serde_json::from_value(id).unwrap();
        let response = match &self.fail {
            Some(error) => response::encode_error(Some(id), error.clone()),
            None => response::encode_success(id, &(), true),
        };
        Box::pin(async move { Ok(response) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_rows_are_grouped_by_range_and_a_resend_applies_nothing_twice() {
    let mut cluster = Cluster::batching().await;
    let left = cluster.add_group(span(None, Some(100)), 1, &[1]).await;
    let right = cluster.add_group(span(Some(100), None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[left, right])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster.gateway(direct_on(), source.clone(), transport.clone());
    let call = nabedra_rt::ids::uuid_v4();
    // Rows of both ranges, interleaved and out of key order.
    let ids: Vec<i64> = (0..100).flat_map(|i| [199 - i, i]).collect();
    let body = cluster.upsert_many(call, rows(ids.clone(), 1_000));
    assert!(applied(&many(&gateway, &body, None).await.unwrap()));
    let stats = cluster.nodes[0].executor.stats();
    assert_eq!(stats.requests, 2, "one request per range");
    for id in 0..200 {
        let group = if id < 100 { left } else { right };
        assert_eq!(
            cluster.latest(group, 0, id).await.1,
            Some(json!({"id": id, "value": 1_000 + id}))
        );
    }
    assert!(cluster.row_settled(left, call, 0).await);
    assert!(cluster.row_settled(right, call, 199).await);

    // The same call again, and its rows in other chunks, other order and
    // other JSON: every row keeps its one version.
    let before = cluster.versions(left, 0..100).await;
    let after = cluster.versions(right, 100..200).await;
    assert!(applied(&many(&gateway, &body, None).await.unwrap()));
    for ids in [(0..50).rev().collect::<Vec<i64>>(), (40..130).collect()] {
        let mut text = format!(
            r#"{{"operation": "table.upsert_many", "request_id": "{call}", "payload": {{ "rows": ["#
        );
        for (position, id) in ids.iter().enumerate() {
            let comma = if position == 0 { "" } else { "," };
            text.push_str(&format!(
                r#"{comma} {{ "value": {}, "id" : {id} }}"#,
                1_000 + id
            ));
        }
        text.push_str(&format!(r#"], "table_id": "{}" }} }}"#, cluster.table));
        assert!(applied(
            &many(&gateway, text.as_bytes(), None).await.unwrap()
        ));
    }
    assert_eq!(cluster.versions(left, 0..100).await, before);
    assert_eq!(cluster.versions(right, 100..200).await, after);

    // The call's id with another row for the same key is a conflict for that
    // row only. A new call writes both.
    let mixed = json!([{"id": 1, "value": 1}, {"id": 300, "value": 2}]);
    let outcomes = many(&gateway, &cluster.upsert_many(call, mixed.clone()), None)
        .await
        .unwrap();
    assert!(
        refused_rows(&outcomes[..1], ErrorCode::Conflict),
        "{outcomes:?}"
    );
    assert_eq!(outcomes[1], RowOutcome::Applied);
    assert_eq!(cluster.versions(left, 0..100).await, before);
    let fresh = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), mixed);
    assert!(applied(&many(&gateway, &fresh, None).await.unwrap()));
    assert_ne!(cluster.versions(left, 0..100).await, before);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_delete_many_deletes_once_and_a_missing_key_is_applied() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let gateway = cluster.gateway(direct_on(), source, cluster.transport());
    let upsert = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows(0..10, 0));
    assert!(applied(&many(&gateway, &upsert, None).await.unwrap()));
    let call = nabedra_rt::ids::uuid_v4();
    let delete = cluster.delete_many(call, json!([3, 99, 4]));
    assert!(applied(&many(&gateway, &delete, None).await.unwrap()));
    for (id, gone) in [(2, false), (3, true), (4, true), (5, false)] {
        assert_eq!(cluster.latest(group, 0, id).await.1.is_none(), gone, "{id}");
    }
    // A row written after the delete survives the delete's resend.
    let again = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows([3], 7));
    assert!(applied(&many(&gateway, &again, None).await.unwrap()));
    assert!(applied(&many(&gateway, &delete, None).await.unwrap()));
    assert_eq!(
        cluster.latest(group, 0, 3).await.1,
        Some(json!({"id": 3, "value": 10}))
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_a_lost_reply_is_resent_and_never_reported_as_not_applied() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster.gateway(direct_on(), source.clone(), transport.clone());
    let warm = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows([1_000], 0));
    assert!(applied(&many(&gateway, &warm, None).await.unwrap()));
    // The leader executed every row and the reply is lost.
    let call = nabedra_rt::ids::uuid_v4();
    let before = cluster.nodes[0].executor.stats();
    transport.lose.store(1, Ordering::SeqCst);
    let body = cluster.upsert_many(call, rows(0..64, 0));
    assert!(applied(&many(&gateway, &body, None).await.unwrap()));
    let after = cluster.nodes[0].executor.stats();
    assert_eq!(
        after.entries_applied - before.entries_applied,
        64,
        "each row once"
    );
    assert_eq!(after.replayed - before.replayed, 64, "then from its record");
    assert_eq!(gateway.stats().resends, 1);

    // The reply is lost, then the leader refuses the caller. The rows are
    // there, so nothing else than unknown is true.
    let call = nabedra_rt::ids::uuid_v4();
    let deny = Arc::downgrade(&transport);
    *transport.after_lose.lock().unwrap() = Some(Box::new(move || {
        deny.upgrade().unwrap().deny.store(true, Ordering::SeqCst);
    }));
    transport.lose.store(1, Ordering::SeqCst);
    let body = cluster.upsert_many(call, rows(100..164, 0));
    let outcomes = many(&gateway, &body, None).await.unwrap();
    assert!(unknown_rows(&outcomes), "{outcomes:?}");
    assert!(cluster.latest(group, 0, 100).await.1.is_some());
    // Refused before any attempt executed: definitely not applied.
    let body = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows(200..264, 0));
    let outcomes = many(&gateway, &body, None).await.unwrap();
    assert!(
        refused_rows(&outcomes, ErrorCode::Unauthorized),
        "{outcomes:?}"
    );
    assert_eq!(cluster.latest(group, 0, 200).await, (None, None));

    // After the loss the table changes, so the leader refuses the rows before
    // its dedup lookup. Settling each row finds what committed.
    transport.deny.store(false, Ordering::SeqCst);
    let call = nabedra_rt::ids::uuid_v4();
    let (schema, table) = (cluster.schema.clone(), cluster.table);
    *transport.after_lose.lock().unwrap() = Some(Box::new(move || {
        let mut changed = descriptor(table);
        changed.schema.insert("value".to_string(), ColumnType::Utf8);
        schema.insert(DirectTable {
            descriptor: changed,
            schema_epoch: 1,
            indexes: Vec::new(),
            lifecycle_id: table,
        });
    }));
    transport.lose.store(1, Ordering::SeqCst);
    let body = cluster.upsert_many(call, rows(300..332, 0));
    assert!(applied(&many(&gateway, &body, None).await.unwrap()));
    assert_eq!(gateway.stats().forward_replayed, 32);
    // Without a loss the same refusal is definite, per row.
    let body = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows(400..432, 0));
    let outcomes = many(&gateway, &body, None).await.unwrap();
    assert!(
        refused_rows(&outcomes, ErrorCode::ValidationError),
        "{outcomes:?}"
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_rows_reach_one_outcome_when_the_leader_stops_during_the_call() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1, 2, 3]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster
        .gateway(direct_on(), source.clone(), transport.clone())
        .with_deadline(Duration::from_secs(20));
    let warm = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows([10_000], 0));
    assert!(applied(&many(&gateway, &warm, None).await.unwrap()));
    let leader = cluster.groups[group].replicas[cluster.leader(group).await].node;
    let call = nabedra_rt::ids::uuid_v4();
    let body = cluster.upsert_many(call, rows(0..2_000, 5));
    let running = {
        let (gateway, body) = (gateway.clone(), body.clone());
        nabedra_rt::task::spawn(async move { many(&gateway, &body, None).await.unwrap() })
    };
    // Some rows committed, then the leader is gone for good.
    let first = cluster.groups[group].replicas[leader as usize - 1]
        .handle
        .clone();
    let key = cluster.row_key(0);
    watchdog(|| async {
        match first.direct_latest(key.clone()).await.unwrap() {
            LatestRead::Value { version, .. } => version,
            _ => None,
        }
    })
    .await;
    transport.down.lock().unwrap().insert(leader);
    cluster.stop_node(leader).await;
    let outcomes = running.await.unwrap();
    assert!(
        applied(&outcomes),
        "{:?}",
        outcomes.iter().find(|o| **o != RowOutcome::Applied)
    );
    let survivor = cluster.leader(group).await;
    for id in 0..2_000 {
        assert_eq!(
            cluster.latest(group, survivor, id).await.1,
            Some(json!({"id": id, "value": 5 + id}))
        );
    }
    let before = cluster.versions(group, 0..2_000).await;
    assert!(applied(&many(&gateway, &body, None).await.unwrap()));
    assert_eq!(cluster.versions(group, 0..2_000).await, before);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_without_a_leader_no_row_is_applied_or_reported_wrongly() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster
        .gateway(direct_on(), source, transport.clone())
        .with_deadline(Duration::from_millis(600));
    transport.down.lock().unwrap().extend([1, 2, 3]);
    let body = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows(0..10, 0));
    let outcomes = many(&gateway, &body, None).await.unwrap();
    assert!(unknown_rows(&outcomes), "{outcomes:?}");
    // Every voter answers that it does not lead: nothing executed.
    transport.down.lock().unwrap().clear();
    transport
        .retired
        .lock()
        .unwrap()
        .insert(cluster.groups[group].identity.group_id);
    let body = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows(20..30, 0));
    let outcomes = many(&gateway, &body, None).await.unwrap();
    assert!(
        refused_rows(&outcomes, ErrorCode::StorageUnavailable),
        "{outcomes:?}"
    );
    assert_eq!(cluster.latest(group, 0, 20).await, (None, None));
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_a_split_regroups_the_rows_of_a_stale_route() {
    let mut cluster = Cluster::batching().await;
    let left = cluster.add_group(span(None, Some(500)), 2, &[1]).await;
    let right = cluster.add_group(span(Some(500), None), 2, &[1]).await;
    let mut before = cluster.fill(&[left]);
    before.shards[0].span = span(None, None);
    before.shards[0].routing_epoch = 1;
    let after = cluster.fill(&[left, right]);
    let source = Source::new(vec![before, after], cluster.voters());
    let gateway = cluster.gateway(direct_on(), source, cluster.transport());
    let call = nabedra_rt::ids::uuid_v4();
    let body = cluster.upsert_many(call, rows((0..1_000).step_by(10), 0));
    assert!(applied(&many(&gateway, &body, None).await.unwrap()));
    for id in (0..1_000).step_by(10) {
        let group = if id < 500 { left } else { right };
        assert!(cluster.row_settled(group, call, id).await, "{id}");
        assert!(cluster.latest(group, 0, id).await.1.is_some(), "{id}");
    }
    assert!(gateway.stats().stale_routes > 0);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_a_row_that_cannot_run_directly_is_settled_and_queued_alone() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let mut wide = descriptor(cluster.table);
    wide.schema.insert("value".to_string(), ColumnType::Utf8);
    cluster.schema.insert(DirectTable {
        descriptor: wide,
        schema_epoch: 1,
        indexes: Vec::new(),
        lifecycle_id: cluster.table,
    });
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let gateway = cluster.gateway(direct_on(), source, cluster.transport());
    let call = nabedra_rt::ids::uuid_v4();
    // At the row limit, so its entry is above the direct limit.
    let big = "x".repeat((1 << 20) - 22);
    let body = cluster.upsert_many(
        call,
        json!([{"id": 1, "value": "a"}, {"id": 2, "value": big}, {"id": 3, "value": "c"}]),
    );
    let queue = Queue::new(None);
    assert!(applied(&many(&gateway, &body, Some(&queue)).await.unwrap()));
    let bodies = queue.bodies();
    assert_eq!(bodies.len(), 1);
    let expected = protocol_upsert(call, cluster.table, json!({"id": 2, "value": big}));
    assert_eq!(bodies[0], expected, "the canonical body the marker settled");
    assert_eq!(cluster.latest(group, 0, 2).await, (None, None));
    assert!(cluster.latest(group, 0, 1).await.1.is_some());
    assert!(cluster.latest(group, 0, 3).await.1.is_some());
    assert!(cluster.row_settled(group, call, 2).await);
    // The marker holds: a resend queues the same request again and never
    // applies it directly. The queue's answer is the row's outcome.
    let conflict = ProtocolError::new(ErrorCode::Conflict, "busy", Outcome::NotApplied);
    let queue = Queue::new(Some(conflict));
    let outcomes = many(&gateway, &body, Some(&queue)).await.unwrap();
    assert_eq!(outcomes[0], RowOutcome::Applied);
    assert!(
        refused_rows(&outcomes[1..2], ErrorCode::Conflict),
        "{outcomes:?}"
    );
    assert_eq!(queue.bodies(), bodies);
    // No queued path here, and an earlier call queued the row.
    let outcomes = many(&gateway, &body, None).await.unwrap();
    assert_eq!(outcomes[0], RowOutcome::Applied);
    assert!(unknown_rows(&outcomes[1..2]), "{outcomes:?}");
    cluster.shutdown().await;
}
fn protocol_upsert(call: Uuid, table: Uuid, row: Value) -> Vec<u8> {
    let row = crate::protocol::decode_upsert_row(&row.to_string()).unwrap();
    let request = crate::protocol::upsert_row_request(call, table, "id", row).unwrap();
    serde_json::to_vec(&request).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_rollback_mode_settles_every_row_before_it_is_queued() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let switch = direct_on();
    let gateway = cluster.gateway(switch.clone(), source, cluster.transport());
    let call = nabedra_rt::ids::uuid_v4();
    let direct = cluster.upsert_many(call, rows(0..4, 0));
    assert!(applied(&many(&gateway, &direct, None).await.unwrap()));
    let before = cluster.versions(group, 0..4).await;
    for history in [false, true] {
        switch.set_direct_oltp(history);
        switch.set_history(history);
        let queue = Queue::new(None);
        let ids = if history { 20..28 } else { 10..18 };
        // Rows 0 to 3 committed directly: their records answer, not the queue.
        let body = cluster.upsert_many(call, rows((0..4).chain(ids.clone()), 0));
        assert!(applied(&many(&gateway, &body, Some(&queue)).await.unwrap()));
        assert_eq!(queue.bodies().len(), 8);
        for id in ids {
            assert!(cluster.row_settled(group, call, id).await);
            assert_eq!(cluster.latest(group, 0, id).await, (None, None));
        }
        assert_eq!(cluster.versions(group, 0..4).await, before);
        // Without a queued path nothing is started.
        assert_eq!(
            many(&gateway, &body, None).await,
            Err((ErrorCode::UnsupportedOperation, Outcome::NotApplied))
        );
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_vs17_bad_calls_are_refused_before_anything_is_sent() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster.gateway(direct_on(), source, transport.clone());
    let id = nabedra_rt::ids::uuid_v4();
    let ok = json!({"id": 1, "value": 1});
    let validation = Err((ErrorCode::ValidationError, Outcome::NotApplied));
    for body in [
        // A key twice, also far apart.
        cluster.upsert_many(
            id,
            json!([ok, {"id": 2, "value": 1}, {"id": 1, "value": 2}]),
        ),
        cluster.upsert_many(id, json!([ok, ok])),
        cluster.delete_many(id, json!([1, 2, 1])),
        // Malformed rows, keys and payloads, after a good row.
        cluster.upsert_many(id, json!([ok, {"value": 1}])),
        cluster.upsert_many(id, json!([ok, {"id": null, "value": 1}])),
        cluster.upsert_many(id, json!([ok, {"id": "1", "value": 1}])),
        cluster.upsert_many(id, json!([ok, {"id": 1.0, "value": 1}])),
        cluster.upsert_many(id, json!([ok, {"id": 2, "value": {"f32": "AAA"}}])),
        cluster.upsert_many(id, json!([ok, {"id": 2, "value": {"f32": "AADAfw=="}}])),
        cluster.upsert_many(id, json!([ok, {"id": 2, "value": [1]}])),
        cluster.upsert_many(id, json!([ok, 1])),
        cluster.upsert_many(id, json!({})),
        cluster.upsert_many(id, json!(null)),
        cluster.delete_many(id, json!([1, "1"])),
        cluster.delete_many(id, json!([1, null])),
        cluster.delete_many(id, json!([1, 1.0])),
        cluster.upsert_many(Uuid::nil(), json!([ok])),
        cluster.body(id, "table.upsert_many", json!({"rows": [ok], "x": 1})),
        cluster.body(id, "table.upsert_many", json!({"keys": [1]})),
        cluster.body(id, "table.delete_many", json!({"rows": [ok]})),
    ] {
        assert_eq!(
            many(&gateway, &body, None).await,
            validation,
            "{}",
            String::from_utf8_lossy(&body)
        );
    }
    let limit = Err((ErrorCode::LimitExceeded, Outcome::NotApplied));
    for body in [
        cluster.upsert_many(id, rows(0..10_001, 0)),
        cluster.delete_many(id, (0..10_001).collect()),
    ] {
        assert_eq!(many(&gateway, &body, None).await, limit);
    }
    assert_eq!(transport.sent.load(Ordering::Relaxed), 0);
    // Asynchronous calls and streamed bodies have no direct answer to wait for.
    let body = cluster.upsert_many(id, json!([ok]));
    let error = gateway.route(&body, true).await.err().unwrap();
    assert_eq!(error.code, ErrorCode::UnsupportedOperation);
    assert_eq!(error.outcome, Outcome::NotApplied);
    assert_eq!(transport.sent.load(Ordering::Relaxed), 0);
    // Nothing to do is done.
    assert_eq!(
        many(&gateway, &cluster.upsert_many(id, json!([])), None).await,
        Ok(vec![])
    );
    assert_eq!(
        many(&gateway, &cluster.delete_many(id, json!([])), None).await,
        Ok(vec![])
    );
    // A row of the wrong type is refused by its leader, alone.
    let body = cluster.upsert_many(
        id,
        json!([ok, {"id": 2, "value": "text"}, {"id": 3}, {"id": 4, "value": null}, {"id": 5, "value": 5}]),
    );
    let outcomes = many(&gateway, &body, None).await.unwrap();
    assert_eq!(outcomes[0], RowOutcome::Applied);
    assert!(
        refused_rows(&outcomes[1..4], ErrorCode::ValidationError),
        "{outcomes:?}"
    );
    assert_eq!(outcomes[4], RowOutcome::Applied);
    for (id, present) in [(1, true), (2, false), (3, false), (4, false), (5, true)] {
        assert_eq!(cluster.latest(group, 0, id).await.1.is_some(), present);
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs17_a_call_at_the_limits_is_sent_in_bounded_chunks() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster
        .gateway(direct_on(), source, transport)
        .with_deadline(Duration::from_secs(30));
    let body = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows(0..10_000, 0));
    let outcomes = many(&gateway, &body, None).await.unwrap();
    assert!(
        applied(&outcomes),
        "{:?}",
        outcomes.iter().find(|o| **o != RowOutcome::Applied)
    );
    let stats = cluster.nodes[0].executor.stats();
    assert_eq!(stats.requests, 10, "1,024 rows each");
    for id in [0, 1_023, 1_024, 9_999] {
        assert!(cluster.latest(group, 0, id).await.1.is_some());
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vs20_vector_tables_and_unserved_operations_are_refused_explicitly() {
    let cluster = Cluster::new().await;
    let switch = direct_on();
    let source = Source::new(Vec::new(), Vec::new());
    let gateway = cluster.gateway(switch.clone(), source, cluster.transport());
    let id = nabedra_rt::ids::uuid_v4();
    let create = |schema: Value, options: Value| {
        let mut payload = json!({"name": "t", "schema": schema, "key": "id", "format": "parquet"});
        for (name, value) in options.as_object().unwrap() {
            payload[name] = value.clone();
        }
        serde_json::to_vec(
            &json!({"request_id": id, "operation": "table.create", "payload": payload}),
        )
        .unwrap()
    };
    let vector = json!({"type": "vector", "dim": 2});
    let gated = [
        create(json!({"id": "utf8", "v": vector}), json!({})),
        create(
            json!({"id": "utf8", "v": {"type": "vector", "sparse": true}}),
            json!({}),
        ),
        create(
            json!({"id": "utf8", "v": {"type": "list", "item": vector}}),
            json!({}),
        ),
        create(
            json!({"id": "utf8", "v": {"type": "list", "item": "utf8"}}),
            json!({}),
        ),
        create(
            json!({"id": "utf8"}),
            json!({"full_text": {"id": "english"}}),
        ),
        create(json!({"id": "utf8"}), json!({"partition_by": "id"})),
    ];
    let plain = create(json!({"id": "utf8"}), json!({}));
    for asynchronous in [false, true] {
        for enabled in [false, true] {
            switch.set_vector(enabled);
            assert!(matches!(
                gateway.route(&plain, asynchronous).await,
                Ok(Routed::Queued)
            ));
            for body in &gated {
                match gateway.route(body, asynchronous).await {
                    Ok(Routed::Queued) => assert!(enabled),
                    Err(error) => {
                        assert!(!enabled);
                        assert_eq!(error.code, ErrorCode::UnsupportedOperation);
                        assert_eq!(error.outcome, Outcome::NotApplied);
                        assert!(error.message.contains("data.vector.v1"), "{error}");
                    }
                    Ok(Routed::Done(_)) => panic!("a create is never direct"),
                }
            }
        }
    }
    let query = json!({"select": null, "filter": null, "limit": 10});
    let ranking = json!({"column": "v", "op": "dot", "value": {"f32": "AACAPw=="}});
    for (operation, payload) in [
        ("table.add_column", json!({"name": "v", "type": vector})),
        ("table.drop_column", json!({"name": "v"})),
        (
            "table.add_full_text",
            json!({"column": "c", "language": "english"}),
        ),
        ("table.drop_full_text", json!({"column": "c"})),
        ("table.search_status", json!({})),
        (
            "query.rank",
            json!({"query": query, "rank": {"rankings": [ranking]}}),
        ),
        (
            "query.rank_many",
            json!({"query": query, "rank": {"rankings": []}, "batch": [ranking]}),
        ),
    ] {
        let body = cluster.body(id, operation, payload.clone());
        // J's column operations and status are Catalog steps: queued.
        if operation.starts_with("table.") {
            assert!(matches!(gateway.route(&body, false).await, Ok(Routed::Queued)), "{operation}");
        } else {
            let error = gateway.route(&body, false).await.err().unwrap();
            assert_eq!(error.code, ErrorCode::UnsupportedOperation, "{operation}");
            assert_eq!(error.outcome, Outcome::NotApplied);
        }
        let mut bad = payload;
        bad["x"] = json!(1);
        let error = gateway
            .route(&cluster.body(id, operation, bad), false)
            .await;
        assert_eq!(
            error.err().unwrap().code,
            ErrorCode::ValidationError,
            "{operation}"
        );
    }
    // Off, the queued path answers as before this build.
    switch.set_active(false);
    assert!(matches!(
        gateway.route(&gated[0], false).await,
        Ok(Routed::Queued)
    ));
    cluster.shutdown().await;
}

#[path = "adv_many.rs"]
mod adv;

#[path = "adv_http.rs"]
mod adv_http;
