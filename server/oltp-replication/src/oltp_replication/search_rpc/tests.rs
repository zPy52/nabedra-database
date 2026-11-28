//! Contract tests of `nabedra-search/1` (contract-w2 section 3): every wire
//! type of 2.2 to 2.7 round-trips through JSON and refuses an unknown field, the
//! role matrix of 2.1 holds with empty handlers, and the vector section is
//! bounded. Each owner keeps them green.
use super::*;
use crate::oltp::mvcc::{
    catalog::{CatalogSearch, CatalogSearchKind, ColumnLife},
    store::search::TextColumn,
    vector::SearchColumn,
};
use nabedra_oltp_types::types::Digest;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt::Debug};

/// Where an unknown field goes: a struct's own object, the body of an enum
/// variant, or nowhere (a unit or string-like value).
#[derive(Clone, Copy)]
enum Shape {
    S,
    V,
    N,
}
use Shape::{N, S, V};

fn check<T: Serialize + DeserializeOwned + PartialEq + Debug>(shape: Shape, value: T) {
    let json = serde_json::to_value(&value).unwrap();
    let back: T = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(back, value, "round trip {json}");
    assert_eq!(serde_json::to_value(&back).unwrap(), json);
    let mut bad = json.clone();
    let object = match (shape, &mut bad) {
        (S, Value::Object(map)) => map,
        (V, Value::Object(map)) if map.len() == 1 => match map.values_mut().next() {
            Some(Value::Object(body)) => body,
            _ => panic!("variant without a body: {json}"),
        },
        (N, _) => return,
        _ => panic!("unexpected shape: {json}"),
    };
    object.insert("zz_unknown".into(), json!(1));
    assert!(
        serde_json::from_value::<T>(bad.clone()).is_err(),
        "accepted an unknown field: {bad}"
    );
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn text<const L: usize>(s: &str) -> Text<L> {
    Text::try_from(s).unwrap()
}
fn ts(n: u64) -> CommitTs {
    CommitTs::new(n).unwrap()
}
fn pos(index: u64) -> AppliedPosition {
    AppliedPosition {
        term: 2,
        node_id: 3,
        index,
    }
}
fn group(kind: GroupKind) -> GroupIdentity {
    GroupIdentity {
        cluster_id: uuid(1),
        database: text("db"),
        group_id: uuid(2),
        kind,
        group_format_version: 1,
    }
}
fn data() -> GroupIdentity {
    group(GroupKind::Data {
        table_id: uuid(3),
        shard_id: uuid(4),
    })
}
fn key() -> MvccKey {
    MvccKey::Row {
        table_id: uuid(3),
        primary_key: Key::Utf8("a".into()),
    }
}
fn span() -> KeySpan {
    KeySpan {
        start: Some(Key::Utf8("a".into())),
        end: None,
    }
}
fn vref() -> VecRef {
    VecRef { offset: 8, len: 12 }
}
fn json_row() -> RowJson {
    RowJson(RawValue::from_string(r#"{"id":"a","n":1}"#.into()).unwrap())
}
fn row() -> WireRow {
    WireRow {
        value: json_row(),
        blobs: Columns(BTreeMap::from([(1, ts(4))])),
        source: Columns::default(),
        vectors: vec![(1, VectorKind::Dense, vref())],
    }
}
fn column() -> SearchColumn {
    SearchColumn {
        id: 1,
        kind: VectorKind::Dense,
        dim: 3,
        source: Some(text("body")),
    }
}
fn schema() -> SearchSchema {
    SearchSchema {
        epoch: 4,
        columns: vec![column()],
        high: 2,
        serving: vec![1],
        text: vec![TextColumn {
            id: 2,
            column: text("body"),
            language: text("english"),
            serving: false,
        }],
    }
}
fn published() -> PublishedColumn {
    CatalogSearchColumn {
        id: 1,
        name: text("embedding"),
        kind: CatalogSearchKind::Vector(column()),
        state: ColumnLife::Ready,
    }
}
fn query_range() -> QueryRange {
    QueryRange {
        group: data(),
        routing_epoch: 1,
        span: span(),
    }
}
fn covered() -> Covered {
    Covered {
        group: data(),
        routing_epoch: 1,
        span: span(),
        at: pos(9),
    }
}
fn ranking() -> WorkerRanking {
    WorkerRanking {
        column: 1,
        op: RankOp::Cosine,
        probe: Probe::Dense(vref()),
    }
}
fn stats() -> TextStats {
    TextStats {
        docs: 10,
        words: 99,
        df: vec![1, 2],
    }
}
fn enrollment() -> WorkerEnrollment {
    WorkerEnrollment {
        worker_id: uuid(7),
        incarnation: uuid(8),
        certificate_sha256: Digest::from_bytes([9; 32]),
        endpoint: text("10.0.0.1:7000"),
        server_name: text("search-0"),
        enrollment_revision: 1,
    }
}
fn query() -> QueryRequest {
    QueryRequest {
        table_id: uuid(3),
        assignment_epoch: 2,
        ranges: vec![query_range()],
        read: ReadMode::Snapshot { cut: ts(5) },
        rankings: vec![ranking()],
        fused: true,
        filter: Some(
            serde_json::from_value(json!({"op":"eq","column":"tenant","value":"x"})).unwrap(),
        ),
        limit: 10,
        candidates: 100,
        recall: 0.95,
        exact: false,
        rerank: Some(ranking()),
        rerank_candidates: 200,
        select: vec![text("title")],
        stats: vec![Some(stats()), None],
        budget: QueryBudget {
            cpu_us: 1,
            io_bytes: 2,
            memory_bytes: 3,
            deadline_ms: 4,
        },
        class: QueryClass::Interactive,
    }
}
fn tail() -> TailRequest {
    TailRequest {
        group: data(),
        routing_epoch: 1,
        assignment_epoch: 2,
        from: TailFrom::After(pos(5)),
        max_entries: 64,
        max_bytes: 1 << 16,
    }
}
fn member() -> SearchMember {
    SearchMember {
        database: text("db"),
        role: SearchRole::Worker,
        enrollment: enrollment(),
        zone: text("a"),
        budget_bytes: 1 << 30,
    }
}
fn model() -> EmbedModel {
    EmbedModel {
        name: text("m"),
        endpoint: text("https://embed.local/v1/embeddings"),
        model: text("e5"),
        dim: 3,
        credential_ref: text("env:EMBED_KEY"),
        max_batch: 32,
        max_rps: 10,
        allow_hosts: vec![text("embed.local")],
    }
}
fn mv() -> SearchMove {
    SearchMove {
        shard: 0,
        add: uuid(7),
        remove: Some(uuid(8)),
        epoch: 3,
    }
}

fn methods() -> Vec<SearchMethod> {
    vec![
        SearchMethod::Tail(tail()),
        SearchMethod::SnapshotOpen(SnapshotOpen {
            group: data(),
            routing_epoch: 1,
            assignment_epoch: 2,
        }),
        SearchMethod::SnapshotPage(SnapshotPageRequest {
            group: data(),
            cursor: uuid(5),
            after: Some(key()),
            max_items: 10,
            max_bytes: 1 << 16,
        }),
        SearchMethod::SnapshotClose(SnapshotClose {
            group: data(),
            cursor: uuid(5),
        }),
        SearchMethod::Barrier(BarrierRequest {
            table_id: uuid(3),
            ranges: vec![BarrierRange {
                group: data(),
                routing_epoch: 1,
                columns: vec![1, 2],
            }],
        }),
        SearchMethod::Close(CloseInterest {
            table_id: uuid(3),
            ranges: vec![(data(), 1)],
            until_unix_ms: 99,
        }),
        SearchMethod::Routes(RoutesRequest {
            table_id: uuid(3),
            after: Some(Key::Int64(4)),
            max: 10,
        }),
        SearchMethod::Query(query()),
        SearchMethod::Stats(StatsRequest {
            table_id: uuid(3),
            ranges: vec![query_range()],
            column: 2,
            text: "reset password".into(),
        }),
        SearchMethod::Floors(FloorsRequest {
            table_id: uuid(3),
            ranges: vec![query_range()],
        }),
        SearchMethod::Ping,
        SearchMethod::EmbedLease(LeaseRequest {
            group: data(),
            routing_epoch: 1,
            columns: vec![1],
            max_items: 16,
            lease_ms: 30_000,
        }),
        SearchMethod::EmbedSet(SetRequest {
            group: data(),
            routing_epoch: 1,
            key: key(),
            column: 1,
            source_version: ts(4),
            value: vref(),
        }),
        SearchMethod::EmbedFail(FailRequest {
            group: data(),
            routing_epoch: 1,
            key: key(),
            column: 1,
            source_version: ts(4),
            id: ts(5),
            attempt: 2,
            reason: text("429"),
        }),
        SearchMethod::Requeue(RequeueRequest {
            group: data(),
            key: key(),
            column: 1,
            source_version: ts(4),
            id: ts(5),
        }),
    ]
}

fn answers() -> Vec<SearchAnswer> {
    vec![
        SearchAnswer::Tail(TailReply::Inactive),
        SearchAnswer::SnapshotOpen(SnapshotOpenReply::Busy),
        SearchAnswer::SnapshotPage(SnapshotPageReply::Expired),
        SearchAnswer::SnapshotClose,
        SearchAnswer::Barrier(BarrierReply {
            ranges: vec![BarrierAnswer::Unavailable],
        }),
        SearchAnswer::Close(CloseInterestReply {
            floors: vec![None, Some((ts(3), 4))],
        }),
        SearchAnswer::Routes(RoutesReply {
            schema_epoch: 1,
            routing_epoch: 2,
            shards: vec![ShardRoute {
                group: data(),
                span: span(),
                schema_epoch: 1,
                routing_epoch: 2,
            }],
            next: None,
            columns: vec![published()],
        }),
        SearchAnswer::Query(QueryReply::Done {
            lists: vec![vec![Candidate {
                row: 0,
                score: 0.5,
                rerank: Some(0.25),
            }]],
            covered: vec![covered()],
            rows: vec![RowOut {
                key: Key::Utf8("a".into()),
                commit_ts: ts(4),
                columns: json_row(),
            }],
            queue: 1,
        }),
        SearchAnswer::Stats(StatsReply {
            stats: stats(),
            covered: vec![covered()],
        }),
        SearchAnswer::Floors(FloorsReply {
            floors: vec![(data(), Some((ts(3), 4)), ts(2))],
        }),
        SearchAnswer::Ping,
        SearchAnswer::EmbedLease(LeaseReply::Inactive),
        SearchAnswer::EmbedSet(SetReply::Retry),
        SearchAnswer::EmbedFail(FailReply::Unmatched),
        SearchAnswer::Requeue(RequeueReply::Unmatched),
    ]
}

#[test]
fn vs00_every_header_and_reply_frame_round_trips_and_refuses_unknown_fields() {
    let methods = methods();
    assert_eq!(methods.len(), MethodName::ALL.len());
    for (method, name) in methods.into_iter().zip(MethodName::ALL) {
        assert_eq!(method.name(), name);
        check(N, name);
        check(
            S,
            SearchPrologue {
                version: SEARCH_VERSION,
                method: name,
                body_bytes: 10,
                vector_bytes: 0,
            },
        );
        let shape = if name == MethodName::Ping { N } else { V };
        check(shape, method.clone());
        check(
            S,
            SearchHeader {
                version: SEARCH_VERSION,
                call: CallId {
                    process: uuid(6),
                    seq: 7,
                },
                deadline_ms: 100,
                method,
            },
        );
    }
    for answer in answers() {
        check(
            S,
            SearchReplyFrame {
                call: CallId {
                    process: uuid(6),
                    seq: 7,
                },
                answer,
            },
        );
    }
    check(
        S,
        CallId {
            process: uuid(6),
            seq: 7,
        },
    );
}

#[test]
fn vs00_feed_types_round_trip_and_refuse_unknown_fields() {
    check(S, tail());
    check(V, TailFrom::After(pos(1)));
    check(N, TailFrom::Start);
    for reply in [
        TailReply::Changes {
            entries: vec![WireEntry {
                at: pos(6),
                changes: vec![
                    WireChange::Row {
                        key: key(),
                        commit_ts: ts(4),
                        row: Some(row()),
                    },
                    WireChange::Row {
                        key: key(),
                        commit_ts: ts(5),
                        row: None,
                    },
                    WireChange::Closed {
                        ts: ts(5),
                        unix_ms: 9,
                    },
                    WireChange::Schema {
                        epoch: 2,
                        serving: vec![1],
                        text: vec![2],
                    },
                ],
            }],
            through: pos(7),
            start: 1,
            frozen_at: Some(pos(7)),
        },
        TailReply::Behind { applied: pos(2) },
        TailReply::SnapshotRequired {
            purged_through: 4,
            applied: pos(9),
        },
        TailReply::StaleRoute { routing_epoch: 3 },
    ] {
        check(V, reply);
    }
    for reply in [
        TailReply::NotHosted,
        TailReply::Inactive,
        TailReply::Quarantined,
    ] {
        check(N, reply);
    }
    check(
        S,
        WireEntry {
            at: pos(1),
            changes: vec![],
        },
    );
    check(S, row());
    check(
        V,
        SnapshotOpenReply::Opened {
            cursor: uuid(5),
            at: pos(5),
            bound: ts(3),
            closed_floor: Some((ts(3), 4)),
            schema: schema(),
            frozen_at: None,
            idle_ms: 30_000,
        },
    );
    check(V, SnapshotOpenReply::StaleRoute { routing_epoch: 2 });
    for reply in [
        SnapshotOpenReply::Busy,
        SnapshotOpenReply::NotHosted,
        SnapshotOpenReply::Inactive,
        SnapshotOpenReply::Quarantined,
    ] {
        check(N, reply);
    }
    check(
        V,
        SnapshotPageReply::Rows {
            rows: vec![WireImageRow {
                key: key(),
                commit_ts: ts(4),
                row: row(),
            }],
            next: Some(key()),
            done: false,
        },
    );
    check(
        S,
        WireImageRow {
            key: key(),
            commit_ts: ts(4),
            row: row(),
        },
    );
    check(
        S,
        BarrierRange {
            group: data(),
            routing_epoch: 1,
            columns: vec![],
        },
    );
    check(
        S,
        BarrierReply {
            ranges: vec![BarrierAnswer::At(pos(3))],
        },
    );
    check(V, BarrierAnswer::At(pos(3)));
    check(V, BarrierAnswer::NotLeader { hint: Some(2) });
    check(V, BarrierAnswer::StaleRoute { routing_epoch: 2 });
    check(V, BarrierAnswer::SchemaChanged { epoch: 2 });
    check(N, BarrierAnswer::Unavailable);
    check(
        S,
        CloseInterestReply {
            floors: vec![Some((ts(2), 3))],
        },
    );
    check(S, published());
    check(S, vref());
}

#[test]
fn vs00_query_types_round_trip_and_refuse_unknown_fields() {
    check(S, query());
    check(S, query_range());
    check(N, ReadMode::Latest);
    check(V, ReadMode::Snapshot { cut: ts(2) });
    check(S, ranking());
    for probe in [
        Probe::Dense(vref()),
        Probe::Sparse {
            indices: vref(),
            weights: vref(),
        },
        Probe::Tokens {
            n: 2,
            values: vref(),
        },
    ] {
        check(V, probe);
    }
    check(N, Probe::Text("text".into()));
    check(S, stats());
    check(N, QueryClass::Exact);
    check(
        V,
        QueryReply::Refused {
            refusals: vec![(data(), Refusal::CutTooOld { oldest: ts(2) })],
            covered: vec![covered()],
            queue: 3,
        },
    );
    check(S, covered());
    check(
        S,
        Candidate {
            row: 1,
            score: 1.5,
            rerank: None,
        },
    );
    check(
        S,
        RowOut {
            key: Key::Int64(1),
            commit_ts: ts(2),
            columns: json_row(),
        },
    );
    check(V, Refusal::CutTooOld { oldest: ts(2) });
    check(V, Refusal::CutTooNew { newest: ts(2) });
    for refusal in [
        Refusal::NotHosted,
        Refusal::Behind,
        Refusal::StaleRoute,
        Refusal::NoFloor,
        Refusal::SchemaChanged,
        Refusal::Overloaded,
        Refusal::OverBudget,
        Refusal::Unavailable,
    ] {
        check(N, refusal);
    }
}

#[test]
fn vs00_embed_types_round_trip_and_refuse_unknown_fields() {
    check(
        V,
        LeaseReply::Items {
            items: vec![LeasedItem {
                key: key(),
                column: 1,
                model: text("m"),
                source_version: ts(4),
                id: ts(5),
                attempt: 0,
                text: "hello".into(),
            }],
            more: true,
        },
    );
    check(V, LeaseReply::NotLeader { hint: None });
    check(V, LeaseReply::StaleRoute { routing_epoch: 1 });
    check(V, SetReply::Set { commit_ts: ts(6) });
    check(V, SetReply::NotLeader { hint: Some(1) });
    for reply in [SetReply::AlreadySet, SetReply::Obsolete, SetReply::Retry] {
        check(N, reply);
    }
    check(V, FailReply::Counted { attempts: 1 });
    check(V, FailReply::Failed { attempts: 3 });
    check(V, FailReply::NotLeader { hint: None });
    check(
        V,
        RequeueReply::Requeued {
            attempts: 0,
            limit: 3,
        },
    );
    check(V, RequeueReply::NotLeader { hint: None });
}

#[test]
fn vs00_control_and_catalog_types_round_trip_and_refuse_unknown_fields() {
    check(S, member());
    check(N, SearchRole::Embedder);
    check(
        S,
        SearchAssignment {
            table_id: uuid(3),
            epoch: 2,
            shards: vec![SearchShard {
                spans: vec![span()],
                replicas: vec![SearchReplica {
                    worker_id: uuid(7),
                    since_epoch: 1,
                    draining: Some(Draining {
                        ended_epoch: 2,
                        until_unix_ms: 5,
                    }),
                }],
            }],
            next: Some(mv()),
        },
    );
    check(
        S,
        Draining {
            ended_epoch: 2,
            until_unix_ms: 5,
        },
    );
    check(S, mv());
    check(S, model());
    check(V, SearchCommand::Enroll(member()));
    check(V, SearchCommand::Retire { worker_id: uuid(7) });
    check(
        V,
        SearchCommand::BeginMove {
            table_id: uuid(3),
            expected_epoch: 2,
            mv: mv(),
        },
    );
    check(
        V,
        SearchCommand::Publish {
            table_id: uuid(3),
            expected_epoch: 2,
        },
    );
    check(
        V,
        SearchCommand::CancelMove {
            table_id: uuid(3),
            expected_epoch: 2,
        },
    );
    check(V, SearchCommand::SetModel(model()));
    check(V, SearchCommand::DropModel { name: text("m") });
    check(V, SearchControlReply::Done { epoch: 1 });
    check(V, SearchControlReply::Conflict { epoch: 1 });
    check(N, SearchControlReply::NotEnabled);
    check(N, SearchControlReply::Refused(text("no")));
    check(V, SearchRead::Assignment { table_id: uuid(3) });
    check(
        V,
        SearchRead::Members {
            after: None,
            max: 10,
        },
    );
    check(N, SearchRead::Models);
    check(V, SearchRead::Status { table_id: uuid(3) });
    check(
        S,
        SearchReport {
            worker_id: uuid(7),
            process: uuid(8),
            table_id: uuid(3),
            assignment_epoch: 2,
            shard: 1,
            report_id: 4,
            page: 2,
            pages: 3,
            ranges: vec![RangeReport {
                group_id: uuid(2),
                routing_epoch: 1,
                applied: pos(4),
                lag_ms: 5,
                columns: vec![
                    (1, ColumnState::Ready),
                    (2, ColumnState::Syncing),
                    (3, ColumnState::Failed(text("disk"))),
                ],
            }],
            queue: 0,
            memory_used: 1,
        },
    );
    // Catalog and replicated state (2.6, 2.7).
    check(
        S,
        CatalogSearch {
            epoch: 3,
            high: 2,
            columns: vec![
                published(),
                CatalogSearchColumn {
                    id: 2,
                    name: text("body_text"),
                    kind: CatalogSearchKind::FullText {
                        column: text("body"),
                        language: text("english"),
                    },
                    state: ColumnLife::Failed(text("build")),
                },
            ],
        },
    );
    check(
        V,
        CatalogSearchKind::FullText {
            column: text("body"),
            language: text("english"),
        },
    );
    for life in [
        ColumnLife::Building,
        ColumnLife::Ready,
        ColumnLife::Dropping,
    ] {
        check(N, life);
    }
    check(S, schema());
    check(
        S,
        TextColumn {
            id: 2,
            column: text("body"),
            language: text("simple"),
            serving: true,
        },
    );
}

/// L15 (no backward compatibility): `serving` and `text` are required, so a
/// schema or fence without them is refused rather than read as empty.
#[test]
fn vs00_search_schema_requires_serving_and_text() {
    let mut json = serde_json::to_value(schema()).unwrap();
    for field in ["serving", "text"] {
        let mut missing = json.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<SearchSchema>(missing).is_err(),
            "{field}"
        );
    }
    json["serving"] = json!([]);
    json["text"] = json!([]);
    let empty: SearchSchema = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&empty).unwrap(),
        json,
        "empty lists are written"
    );
}

// ---- vector section (Q9) ----

#[test]
fn vs00_vector_section_round_trips_and_is_bounded() {
    let mut section = VectorSection::new(32);
    let dense = section.push_f32(&[1.0, -2.5, f32::MIN_POSITIVE]).unwrap();
    let indices = section.push_u32(&[7, 9]).unwrap();
    assert_eq!(dense, VecRef { offset: 0, len: 12 });
    assert_eq!(indices, VecRef { offset: 12, len: 8 });
    assert!(section.push_f32(&[0.0; 4]).is_err(), "over the bound");
    assert_eq!(section.len(), 20);
    let bytes = section.into_bytes();
    assert_eq!(
        dense.f32s(&bytes).unwrap(),
        vec![1.0, -2.5, f32::MIN_POSITIVE]
    );
    assert_eq!(indices.u32s(&bytes).unwrap(), vec![7, 9]);
    assert_eq!(&bytes[12..16], &7u32.to_le_bytes(), "little endian");
    for bad in [
        VecRef { offset: 16, len: 8 },
        VecRef { offset: 0, len: 3 },
        VecRef {
            offset: u32::MAX,
            len: 4,
        },
    ] {
        assert!(bad.bytes(&bytes).is_err(), "{bad:?}");
    }
}

// ---- 2.1 role matrix with empty handlers ----

/// The pairs 2.1 lists; every other (role, method) is refused before `handle`.
fn listed(role: &str, method: MethodName) -> bool {
    use MethodName::*;
    match role {
        "worker" => true,
        "search" => matches!(
            method,
            Tail | SnapshotOpen | SnapshotPage | SnapshotClose | Barrier | Close | Routes
        ),
        "embed" => matches!(method, EmbedLease | EmbedSet | EmbedFail),
        "api" => matches!(method, Query | Stats | Floors | Ping),
        _ => unreachable!(),
    }
}
fn roles(database: &str) -> [(&'static str, CallerRole); 4] {
    [
        ("worker", CallerRole::Worker),
        ("api", CallerRole::Api(text(database))),
        ("search", CallerRole::Search(text(database))),
        ("embed", CallerRole::Embed(text(database))),
    ]
}
fn home(method: MethodName) -> GroupIdentity {
    match method {
        MethodName::Routes => group(GroupKind::Catalog),
        MethodName::Query | MethodName::Stats | MethodName::Floors | MethodName::Ping => {
            group(GroupKind::Control)
        }
        _ => data(),
    }
}
const PURPOSES: [Purpose; 3] = [
    Purpose::SearchFeed,
    Purpose::SearchQuery,
    Purpose::SearchEmbed,
];

#[test]
fn vs11_role_matrix_refuses_every_pair_not_listed() {
    let kinds = [
        group(GroupKind::Control),
        group(GroupKind::Catalog),
        group(GroupKind::Timestamps),
        data(),
    ];
    for method in MethodName::ALL {
        for (name, role) in roles("db") {
            for target in &kinds {
                for purpose in PURPOSES {
                    let home = home(method);
                    let want = listed(name, method)
                        && purpose == method.purpose()
                        && target.kind == home.kind;
                    assert_eq!(
                        role.permits(target, purpose, method),
                        want,
                        "{name} {method:?} {purpose:?} {:?}",
                        target.kind
                    );
                }
            }
        }
        // Another database's search or embed caller reads nothing of `db`.
        for (name, role) in roles("other") {
            let allowed = role.permits(&home(method), method.purpose(), method);
            let expected = match name {
                "worker" | "api" => listed(name, method),
                _ => false,
            };
            assert_eq!(allowed, expected, "{name} of another database, {method:?}");
        }
    }
}

struct FixedRole(Option<CallerRole>);
impl RoleCheck for FixedRole {
    fn role(&self, _: &WorkerIdentity) -> Option<CallerRole> {
        self.0.clone()
    }
}
fn session(group: &GroupIdentity, purpose: Purpose) -> AuthenticatedSession {
    let identity = WorkerIdentity::from(&enrollment());
    AuthenticatedSession::for_test(identity, identity, group.clone(), purpose, uuid(11))
}
fn prologue(method: MethodName, version: u16) -> Vec<u8> {
    serde_json::to_vec(&SearchPrologue {
        version,
        method,
        body_bytes: 2,
        vector_bytes: 0,
    })
    .unwrap()
}

/// The endpoints S binds refuse a caller the matrix does not list with
/// `Unauthorized`, a bad version with `ValidationError`, and answer every
/// permitted call `UnsupportedOperation` until its owner serves it.
#[tokio::test]
async fn vs11_bound_endpoints_check_lane_and_role_then_answer_unsupported() {
    for method in MethodName::ALL {
        let group = home(method);
        let purpose = method.purpose();
        for (name, role) in roles("db") {
            let endpoint =
                SearchRpcEndpoint::new(purpose, group.clone(), Arc::new(FixedRole(Some(role))));
            let got = endpoint
                .preflight(&session(&group, purpose), &prologue(method, 1))
                .await
                .err()
                .map(|error| error.code);
            let want = if listed(name, method) {
                ErrorCode::UnsupportedOperation
            } else {
                ErrorCode::Unauthorized
            };
            assert_eq!(got, Some(want), "{name} {method:?}");
            let version = endpoint
                .preflight(&session(&group, purpose), &prologue(method, 2))
                .await
                .err()
                .map(|error| error.code);
            assert_eq!(version, Some(ErrorCode::ValidationError), "{method:?}");
        }
        // No role, or a session on another lane: refused before the prologue.
        let endpoint = SearchRpcEndpoint::new(purpose, group.clone(), Arc::new(NoSearchRoles));
        let refused = endpoint
            .preflight(&session(&group, purpose), &prologue(method, 1))
            .await
            .err()
            .map(|error| error.code);
        assert_eq!(refused, Some(ErrorCode::Unauthorized), "{method:?}");
        let endpoint = SearchRpcEndpoint::new(
            purpose,
            group.clone(),
            Arc::new(FixedRole(Some(CallerRole::Worker))),
        );
        let other = PURPOSES.into_iter().find(|p| *p != purpose).unwrap();
        let lane = endpoint
            .preflight(&session(&group, other), &prologue(method, 1))
            .await
            .err()
            .map(|error| error.code);
        assert_eq!(lane, Some(ErrorCode::Unauthorized), "{method:?}");
    }
}

#[test]
fn vs00_header_decode_checks_version_method_and_length() {
    let header = SearchHeader {
        version: SEARCH_VERSION,
        call: CallId {
            process: uuid(6),
            seq: 1,
        },
        deadline_ms: 10,
        method: SearchMethod::Ping,
    };
    let body = serde_json::to_vec(&header).unwrap();
    let prologue = |method, body_bytes| SearchPrologue {
        version: SEARCH_VERSION,
        method,
        body_bytes,
        vector_bytes: 0,
    };
    let len = body.len() as u32;
    assert_eq!(
        SearchHeader::decode(&prologue(MethodName::Ping, len), &body).unwrap(),
        header
    );
    assert!(SearchHeader::decode(&prologue(MethodName::Tail, len), &body).is_err());
    assert!(SearchHeader::decode(&prologue(MethodName::Ping, len + 1), &body).is_err());
    let old = serde_json::to_vec(&SearchHeader {
        version: 2,
        ..header
    })
    .unwrap();
    assert!(SearchHeader::decode(&prologue(MethodName::Ping, old.len() as u32), &old).is_err());
}
