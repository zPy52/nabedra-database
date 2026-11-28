//! Adversarial tests of the rules of task C1 (VEC-G10, VEC-G11, VEC-G14,
//! OLTP-G12): the exact wire forms of vector values, the limits and the
//! regexes, the error classes, the scalar grammar's bytes, and arbitrary
//! input. Black box: public decoders only. Each test names the rule it
//! attacks; removing the rule makes it fail.
use nabedra_protocol::protocol::*;
use serde_json::{Value, json};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
// SAFETY: forwards every call to `System` unchanged and only counts bytes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, layout) }
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, layout, new) };
        if !q.is_null() {
            if new >= layout.size() {
                let grown = new - layout.size();
                let now = CURRENT.fetch_add(grown, Ordering::Relaxed) + grown;
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                CURRENT.fetch_sub(layout.size() - new, Ordering::Relaxed);
            }
        }
        q
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

static LOCK: Mutex<()> = Mutex::new(());
fn lock() -> std::sync::MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}
fn peak<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let base = CURRENT.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let out = f();
    (out, PEAK.load(Ordering::Relaxed).saturating_sub(base))
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn finite(&mut self) -> f32 {
        loop {
            let v = f32::from_bits(self.next() as u32);
            if v.is_finite() {
                return v;
            }
        }
    }
}

const ID: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";

fn b64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = u32::from(b[0]) << 16 | u32::from(b[1]) << 8 | u32::from(b[2]);
        for i in 0..4 {
            out.push(if i <= chunk.len() {
                A[(n >> (18 - 6 * i)) as usize & 63] as char
            } else {
                '='
            });
        }
    }
    out
}
fn f32s(values: &[f32]) -> String {
    b64(&values
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect::<Vec<u8>>())
}
fn u32s(values: &[u32]) -> String {
    b64(&values
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect::<Vec<u8>>())
}
fn envelope(op: &str, payload: &str) -> String {
    format!(r#"{{"request_id":"{ID}","operation":"{op}","payload":{payload}}}"#)
}
fn parse(op: &str, payload: &str) -> Result<Request, ProtocolError> {
    parse_oltp_request(envelope(op, payload).as_bytes())
}
fn create(schema: &str, extra: &str) -> String {
    format!(r#"{{"name":"t","schema":{schema},"key":"id","format":"parquet"{extra}}}"#)
}
fn descriptor(json: Value) -> TableDescriptor {
    serde_json::from_value(json).unwrap()
}
fn table() -> TableDescriptor {
    descriptor(json!({
        "id": ID, "name": "t", "key": "id", "format": "parquet",
        "schema": {"id": "utf8", "body": "utf8", "tags": {"type": "list", "item": "utf8"},
                   "v": {"type": "vector", "dim": 3}, "s": {"type": "vector", "sparse": true},
                   "m": {"type": "list", "item": {"type": "vector", "dim": 2}}},
        "full_text": {"body": "english"}
    }))
}
fn row(extra: Value) -> Value {
    let mut row = json!({"id": "a", "body": "b", "tags": [], "v": null, "s": null, "m": null});
    for (key, value) in extra.as_object().unwrap() {
        row[key] = value.clone();
    }
    row
}
/// Decoded and checked against `table()`.
fn upsert(rows: &[Value]) -> Result<(), ProtocolError> {
    let payload = json!({"table_id": ID, "rows": rows}).to_string();
    parse("table.upsert_many", &payload)?
        .operation
        .validate_for_table(&table())
}
fn code(result: Result<impl std::fmt::Debug, ProtocolError>) -> ErrorCode {
    let error = result.expect_err("accepted");
    assert_eq!(error.outcome, Outcome::NotApplied, "{error:?}");
    error.code
}

// ---- wire forms ----

/// VEC-G10: a vector is written as the contract says and read back bit for bit.
#[test]
fn rule_wire_forms_of_vectors_are_exact_and_round_trip_bit_for_bit() {
    let _g = lock();
    let mut rng = Rng(0x5eed_0001);
    for round in 0..400 {
        let dim = 1 + rng.below(if round % 50 == 0 { 4096 } else { 40 });
        let dense: Vec<f32> = (0..dim).map(|_| rng.finite()).collect();
        let mut indices: Vec<u32> = (0..rng.below(30)).map(|_| rng.next() as u32).collect();
        indices.sort_unstable();
        indices.dedup();
        let weights: Vec<f32> = indices.iter().map(|_| rng.finite()).collect();
        let (count, mdim) = (rng.below(9), 1 + rng.below(6));
        let multi: Vec<f32> = (0..count * mdim).map(|_| rng.finite()).collect();
        let cases = [
            (
                Vector::Dense(dense.clone()),
                format!(r#"{{"f32":"{}"}}"#, f32s(&dense)),
            ),
            (
                Vector::Sparse {
                    indices: indices.clone(),
                    weights: weights.clone(),
                },
                format!(
                    r#"{{"sparse":{{"i":"{}","f32":"{}"}}}}"#,
                    u32s(&indices),
                    f32s(&weights)
                ),
            ),
            (
                Vector::Multi {
                    values: multi.clone(),
                    count: count as u16,
                },
                format!(r#"{{"f32s":"{}","n":{count}}}"#, f32s(&multi)),
            ),
        ];
        for (vector, wire) in cases {
            assert_eq!(serde_json::to_string(&vector).unwrap(), wire, "encoding");
            let decoded = decode_row(&format!(r#"{{"x":{wire}}}"#)).unwrap();
            let Some(Scalar::Vector(back)) = decoded.get("x") else {
                panic!("{wire}")
            };
            // Bit equality: -0.0, subnormals and every payload survive.
            let bits = |v: &Vector| match v {
                Vector::Dense(x) => (x.iter().map(|f| f.to_bits()).collect::<Vec<_>>(), vec![], 0),
                Vector::Sparse { indices, weights } => (
                    weights.iter().map(|f| f.to_bits()).collect(),
                    indices.clone(),
                    1,
                ),
                Vector::Multi { values, count } => (
                    values.iter().map(|f| f.to_bits()).collect(),
                    vec![u32::from(*count)],
                    2,
                ),
            };
            assert_eq!(bits(back), bits(&vector), "{wire}");
        }
    }
}

/// VEC-G10: a value that decoding refuses cannot be written either, so no
/// client library and no internal caller emits what the server would refuse.
#[test]
fn rule_serialization_refuses_every_invalid_value() {
    let _g = lock();
    for bad in [
        Vector::Dense(vec![f32::NAN]),
        Vector::Dense(vec![1.0, f32::INFINITY]),
        Vector::Dense(vec![f32::NEG_INFINITY]),
        Vector::Dense(vec![]),
        Vector::Dense(vec![0.0; 4097]),
        Vector::Sparse {
            indices: vec![2, 1],
            weights: vec![1.0, 1.0],
        },
        Vector::Sparse {
            indices: vec![1, 1],
            weights: vec![1.0, 1.0],
        },
        Vector::Sparse {
            indices: vec![1],
            weights: vec![],
        },
        Vector::Sparse {
            indices: vec![1],
            weights: vec![f32::NAN],
        },
        Vector::Multi {
            values: vec![1.0],
            count: 2,
        },
        Vector::Multi {
            values: vec![1.0, 2.0, 3.0],
            count: 2,
        },
        Vector::Multi {
            values: vec![],
            count: 1,
        },
        Vector::Multi {
            values: vec![1.0],
            count: 1025,
        },
        Vector::Multi {
            values: vec![f32::NAN],
            count: 1,
        },
    ] {
        assert!(serde_json::to_string(&bad).is_err(), "{bad:?}");
        assert!(bad.validate().is_err(), "{bad:?}");
    }
    for good in [
        Vector::Dense(vec![0.0; 4096]),
        Vector::Dense(vec![-0.0]),
        Vector::Sparse {
            indices: vec![],
            weights: vec![],
        },
        Vector::Multi {
            values: vec![],
            count: 0,
        },
        Vector::Multi {
            values: vec![1.0; 1024],
            count: 1024,
        },
    ] {
        assert!(serde_json::to_string(&good).is_ok(), "{good:?}");
    }
}

/// The dimension of a value is the dimension of its column, for every column.
#[test]
fn rule_dimension_must_equal_the_column_dimension() {
    let _g = lock();
    let v = |n: usize| json!({"f32": f32s(&vec![1.0; n])});
    assert!(upsert(&[row(json!({"v": v(3)}))]).is_ok());
    for n in [1, 2, 4, 6, 4096] {
        assert_eq!(
            code(upsert(&[row(json!({"v": v(n)}))])),
            ErrorCode::ValidationError,
            "dense {n}"
        );
    }
    let m = |n: usize, k: usize| json!({"f32s": f32s(&vec![1.0; n]), "n": k});
    assert!(upsert(&[row(json!({"m": m(6, 3)}))]).is_ok());
    assert!(upsert(&[row(json!({"m": m(0, 0)}))]).is_ok());
    for (n, k) in [(6, 2), (6, 6), (3, 3), (4, 1), (2, 2), (1, 1), (7, 3)] {
        assert_eq!(
            code(upsert(&[row(json!({"m": m(n, k)}))])),
            ErrorCode::ValidationError,
            "multi {n}/{k}"
        );
    }
    // A kind in the wrong column.
    assert!(upsert(&[row(json!({"s": v(3)}))]).is_err());
    assert!(upsert(&[row(json!({"v": json!({"sparse": {"i": "", "f32": ""}})}))]).is_err());
    assert!(upsert(&[row(json!({"m": v(2)}))]).is_err());
}

/// Multi: n is 0..=1024, 1025 is a limit, the data must hold n whole vectors.
#[test]
fn rule_multi_count_is_zero_to_1024_and_a_limit_beyond() {
    let _g = lock();
    let wide = descriptor(
        json!({"id": ID, "name": "t", "key": "id", "format": "parquet",
        "schema": {"id": "utf8", "m": {"type": "list", "item": {"type": "vector", "dim": 1}}}}),
    );
    let check = |n: usize| {
        let payload = json!({"table_id": ID, "rows": [{"id": "a", "m": {"f32s": f32s(&vec![1.0; n]), "n": n}}]});
        parse("table.upsert_many", &payload.to_string())?
            .operation
            .validate_for_table(&wide)
    };
    for n in [0, 1, 2, 1023, 1024] {
        assert!(check(n).is_ok(), "{n}");
    }
    for n in [1025, 1026, 4096] {
        assert_eq!(code(check(n)), ErrorCode::LimitExceeded, "{n}");
    }
}

/// Sparse indices are strictly ascending: one inversion or repeat anywhere
/// refuses the value, and the largest index is a value.
#[test]
fn rule_sparse_indices_strictly_ascend() {
    let _g = lock();
    let mut rng = Rng(0x5eed_0002);
    let sparse = |i: &[u32]| json!({"sparse": {"i": u32s(i), "f32": f32s(&vec![1.0; i.len()])}});
    for _ in 0..300 {
        let mut indices: Vec<u32> = (0..2 + rng.below(40)).map(|_| rng.next() as u32).collect();
        indices.sort_unstable();
        indices.dedup();
        if indices.len() < 2 {
            continue;
        }
        assert!(upsert(&[row(json!({"s": sparse(&indices)}))]).is_ok());
        let at = rng.below(indices.len() - 1);
        let mut swapped = indices.clone();
        swapped.swap(at, at + 1);
        assert!(
            upsert(&[row(json!({"s": sparse(&swapped)}))]).is_err(),
            "{swapped:?}"
        );
        let mut repeated = indices.clone();
        repeated[at + 1] = repeated[at];
        assert!(
            upsert(&[row(json!({"s": sparse(&repeated)}))]).is_err(),
            "{repeated:?}"
        );
    }
    assert!(upsert(&[row(json!({"s": sparse(&[0, u32::MAX])}))]).is_ok());
}

/// A float that is not finite is refused in all three kinds and in a query.
#[test]
fn rule_every_float_is_finite_in_rows_and_queries() {
    let _g = lock();
    let mut rng = Rng(0x5eed_0003);
    for _ in 0..200 {
        let bad = [
            f32::NAN,
            -f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::from_bits(0x7f80_0001 | (rng.next() as u32 & 0x007f_ffff)),
            f32::from_bits(0xff80_0001 | (rng.next() as u32 & 0x007f_ffff)),
        ][rng.below(6)];
        let at = rng.below(3);
        let mut dense = [rng.finite(), rng.finite(), rng.finite()];
        dense[at] = bad;
        assert!(upsert(&[row(json!({"v": {"f32": f32s(&dense)}}))]).is_err());
        assert!(
            upsert(&[row(
                json!({"s": {"sparse": {"i": u32s(&[1]), "f32": f32s(&[bad])}}})
            )])
            .is_err()
        );
        assert!(upsert(&[row(json!({"m": {"f32s": f32s(&[1.0, bad]), "n": 1}}))]).is_err());
        let rank = json!({"table_id": ID, "query": {"select": null, "filter": null, "limit": 3},
            "rank": {"rankings": [{"column": "v", "op": "dot", "value": {"f32": f32s(&dense)}}]}});
        let decoded = parse("query.rank", &rank.to_string())
            .and_then(|r| r.operation.validate_for_table(&table()));
        assert!(decoded.is_err());
    }
}

/// Tags are JSON strings only; `null` is for vector columns only.
#[test]
fn rule_tags_hold_strings_and_null_is_for_vectors_only() {
    let _g = lock();
    let many: Vec<String> = (0..2000).map(|i| format!("tag{i}")).collect();
    assert!(upsert(&[row(json!({"tags": many}))]).is_ok());
    assert!(upsert(&[row(json!({"tags": ["", "é", "\u{1F600}", "a\u{0}b"]}))]).is_ok());
    for bad in [
        json!([1]),
        json!([null]),
        json!([["a"]]),
        json!("a"),
        json!(null),
        json!({}),
        json!([true]),
    ] {
        assert!(upsert(&[row(json!({"tags": bad}))]).is_err());
    }
    for column in ["id", "body"] {
        assert!(upsert(&[row(json!({column: null}))]).is_err(), "{column}");
    }
    assert!(upsert(&[row(json!({"v": null, "s": null, "m": null}))]).is_ok());
}

// ---- limits, regexes and error classes ----

fn schema_with(vectors: usize, sparse: usize, multi: usize, tags: usize) -> String {
    let mut s = String::from(r#"{"id":"utf8","body":"utf8""#);
    for i in 0..vectors {
        s += &format!(r#","d{i}":{{"type":"vector","dim":2}}"#);
    }
    for i in 0..sparse {
        s += &format!(r#","s{i}":{{"type":"vector","sparse":true}}"#);
    }
    for i in 0..multi {
        s += &format!(r#","m{i}":{{"type":"list","item":{{"type":"vector","dim":2}}}}"#);
    }
    for i in 0..tags {
        s += &format!(r#","t{i}":{{"type":"list","item":"utf8"}}"#);
    }
    s + "}"
}

/// A table has at most 8 search columns: dense, sparse, multi and full-text
/// columns count; tags do not; an embedded column is one column.
#[test]
fn rule_eight_search_columns_count_every_kind_and_only_those() {
    let _g = lock();
    let ft = r#","full_text":{"body":"english"}"#;
    for (v, s, m, t, full, ok) in [
        (8, 0, 0, 0, false, true),
        (0, 8, 0, 0, false, true),
        (0, 0, 8, 0, false, true),
        (3, 3, 2, 0, false, true),
        (7, 0, 0, 0, true, true),
        (3, 2, 2, 0, true, true),
        (0, 0, 0, 40, false, true),
        (8, 0, 0, 20, false, true),
        (9, 0, 0, 0, false, false),
        (0, 9, 0, 0, false, false),
        (0, 0, 9, 0, false, false),
        (8, 0, 0, 0, true, false),
        (4, 3, 1, 0, true, false),
        (0, 0, 0, 0, true, true),
    ] {
        let result = parse(
            "table.create",
            &create(&schema_with(v, s, m, t), if full { ft } else { "" }),
        );
        assert_eq!(
            result.is_ok(),
            ok,
            "v{v} s{s} m{m} t{t} ft{full}: {:?}",
            result.err()
        );
        if !ok {
            assert_eq!(code(result), ErrorCode::LimitExceeded);
        }
    }
    // An embedded column counts once.
    let embed = r#","embed":{"d0":{"source":"body","model":"m"}}"#;
    assert!(parse("table.create", &create(&schema_with(8, 0, 0, 0), embed)).is_ok());
    // A descriptor is held to the same limit, and so is add_column.
    let nine = descriptor(
        json!({"id": ID, "name": "t", "key": "id", "format": "parquet",
        "schema": serde_json::from_str::<Value>(&schema_with(8, 0, 0, 0)).unwrap()}),
    );
    assert!(nine.validate().is_ok());
    let add =
        json!({"table_id": ID, "name": "x", "type": {"type": "vector", "dim": 2}}).to_string();
    let result =
        parse("table.add_column", &add).and_then(|r| r.operation.validate_for_table(&nine));
    assert_eq!(code(result), ErrorCode::LimitExceeded);
    let seven = descriptor(
        json!({"id": ID, "name": "t", "key": "id", "format": "parquet",
        "schema": serde_json::from_str::<Value>(&schema_with(7, 0, 0, 0)).unwrap()}),
    );
    let result =
        parse("table.add_column", &add).and_then(|r| r.operation.validate_for_table(&seven));
    assert!(result.is_ok(), "{result:?}");
}

/// Embedding model names are `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}`: every ASCII
/// character in the first and the second place, and the length edges.
#[test]
fn rule_embedding_model_names_match_their_regex() {
    let _g = lock();
    let accept = |model: &str| {
        let extra = json!({"embed": {"v": {"source": "id", "model": model}}}).to_string();
        let extra = format!(",{}", &extra[1..extra.len() - 1]);
        parse(
            "table.create",
            &create(r#"{"id":"utf8","v":{"type":"vector","dim":2}}"#, &extra),
        )
        .is_ok()
    };
    for c in (0u8..128).map(char::from) {
        let first = c.is_ascii_alphanumeric();
        let rest = c.is_ascii_alphanumeric() || "_.-".contains(c);
        assert_eq!(accept(&format!("{c}")), first, "first {c:?}");
        assert_eq!(accept(&format!("a{c}")), rest, "second {c:?}");
    }
    assert!(!accept(""));
    assert!(accept(&"a".repeat(64)));
    assert!(!accept(&"a".repeat(65)));
    assert!(accept(&format!("a{}", "-".repeat(63))));
    assert!(!accept("é") && !accept("aé") && !accept("a b") && !accept("a\n") && !accept("ａ"));
    // The same through add_column.
    let add = |model: &str| {
        let payload = json!({"table_id": ID, "name": "x", "type": {"type": "vector", "dim": 2},
            "embed": {"source": "body", "model": model}});
        parse("table.add_column", &payload.to_string())
            .and_then(|r| r.operation.validate_for_table(&table()))
    };
    assert!(add("docs-v2").is_ok());
    assert!(add("-docs").is_err() && add("docs v2").is_err() && add(&"a".repeat(65)).is_err());
}

/// Create and add-full-text admit only languages with an analyzer.
#[test]
fn rule_full_text_languages_have_analyzers() {
    let _g = lock();
    let ok_create = |language: &str| {
        let extra = json!({"full_text": {"body": language}}).to_string();
        let extra = format!(",{}", &extra[1..extra.len() - 1]);
        parse(
            "table.create",
            &create(r#"{"id":"utf8","body":"utf8"}"#, &extra),
        )
        .is_ok()
    };
    let t = descriptor(
        json!({"id": ID, "name": "t", "key": "id", "format": "parquet",
        "schema": {"id": "utf8", "body": "utf8"}}),
    );
    let ok_add = |language: &str| {
        let payload = json!({"table_id": ID, "column": "body", "language": language});
        parse("table.add_full_text", &payload.to_string())
            .and_then(|r| r.operation.validate_for_table(&t))
            .is_ok()
    };
    for language in [
        "arabic",
        "danish",
        "dutch",
        "english",
        "finnish",
        "french",
        "german",
        "greek",
        "hungarian",
        "italian",
        "norwegian",
        "portuguese",
        "romanian",
        "russian",
        "simple",
        "spanish",
        "swedish",
        "tamil",
        "turkish",
    ] {
        assert!(ok_create(language), "create {language}");
        assert!(ok_add(language), "add {language}");
    }
    for language in ["", "English", "en-us", "en1", "pt_br", "esperanto", "_"] {
        assert!(!ok_create(language), "create {language}");
        assert!(!ok_add(language), "add {language}");
    }
}

/// `_score` is reserved on tables with search columns only.
#[test]
fn rule_score_is_reserved_only_with_search_columns() {
    let _g = lock();
    assert!(
        parse(
            "table.create",
            &create(r#"{"id":"utf8","_score":"float64"}"#, "")
        )
        .is_ok()
    );
    assert!(
        parse(
            "table.create",
            &create(
                r#"{"id":"utf8","_score":"float64","v":{"type":"vector","dim":2}}"#,
                ""
            )
        )
        .is_err()
    );
    assert!(
        parse(
            "table.create",
            &create(
                r#"{"id":"utf8","_score":"float64","body":"utf8"}"#,
                r#","full_text":{"body":"english"}"#
            )
        )
        .is_err()
    );
    let add = json!({"table_id": ID, "name": "_score", "type": {"type": "vector", "dim": 2}});
    assert!(parse("table.add_column", &add.to_string()).is_err());
}

/// `partition_by` names a utf8 or int64 column; `full_text` a utf8 column;
/// `embed` a dense column from a utf8 source.
#[test]
fn rule_option_targets_have_the_right_types() {
    let _g = lock();
    let schema = r#"{"id":"utf8","n":"int64","f":"float64","b":"bool","body":"utf8","v":{"type":"vector","dim":2},"s":{"type":"vector","sparse":true},"t":{"type":"list","item":"utf8"}}"#;
    let ok = |extra: &str| parse("table.create", &create(schema, extra)).is_ok();
    for (column, expected) in [
        ("id", true),
        ("n", true),
        ("body", true),
        ("f", false),
        ("b", false),
        ("v", false),
        ("t", false),
        ("zz", false),
    ] {
        assert_eq!(
            ok(&format!(r#","partition_by":"{column}""#)),
            expected,
            "partition_by {column}"
        );
    }
    for (column, expected) in [
        ("id", true),
        ("body", true),
        ("n", false),
        ("v", false),
        ("t", false),
        ("zz", false),
    ] {
        assert_eq!(
            ok(&format!(r#","full_text":{{"{column}":"english"}}"#)),
            expected,
            "full_text {column}"
        );
    }
    for (column, source, expected) in [
        ("v", "body", true),
        ("v", "n", false),
        ("v", "v", false),
        ("v", "t", false),
        ("v", "zz", false),
        ("s", "body", false),
        ("body", "id", false),
        ("zz", "body", false),
    ] {
        let extra = format!(r#","embed":{{"{column}":{{"source":"{source}","model":"m"}}}}"#);
        assert_eq!(ok(&extra), expected, "embed {column} from {source}");
    }
    // An embedded column cannot be written, in any request.
    let extra = r#","embed":{"v":{"source":"body","model":"m"}}"#;
    assert!(ok(extra));
    let t = descriptor(
        json!({"id": ID, "name": "t", "key": "id", "format": "parquet",
        "schema": {"id": "utf8", "body": "utf8", "v": {"type": "vector", "dim": 2}},
        "embed": {"v": {"source": "body", "model": "m"}}}),
    );
    let write = |op: &str, payload: Value| {
        parse(op, &payload.to_string()).and_then(|r| r.operation.validate_for_table(&t))
    };
    let with = json!({"id": "a", "body": "x", "v": {"f32": f32s(&[1.0, 2.0])}});
    let without = json!({"id": "a", "body": "x"});
    assert!(
        write(
            "table.upsert_many",
            json!({"table_id": ID, "rows": [with.clone()]})
        )
        .is_err()
    );
    assert!(write("table.insert", json!({"table_id": ID, "row": with})).is_err());
    assert!(
        write(
            "table.upsert_many",
            json!({"table_id": ID, "rows": [without.clone()]})
        )
        .is_ok()
    );
    assert!(
        write(
            "table.update",
            json!({"table_id": ID, "key": "a", "values": {"v": null}})
        )
        .is_err()
    );
}

/// The error class of each refusal: a limit is `limit_exceeded` (413), the
/// rest are `validation_error`, all definite.
#[test]
fn rule_error_classes_of_refusals() {
    let _g = lock();
    let ok_row = row(json!({}));
    let rows = |n: usize| json!({"table_id": ID, "rows": vec![ok_row.clone(); n]});
    // Rows: the limit is a limit even before the key check.
    let many: Vec<Value> = (0..10_001)
        .map(|i| row(json!({"id": format!("k{i}")})))
        .collect();
    let payload = json!({"table_id": ID, "rows": many}).to_string();
    assert_eq!(
        code(parse("table.upsert_many", &payload)),
        ErrorCode::LimitExceeded
    );
    let keys: Vec<String> = (0..10_001).map(|i| format!("k{i}")).collect();
    let payload = json!({"table_id": ID, "keys": keys}).to_string();
    assert_eq!(
        code(parse("table.delete_many", &payload)),
        ErrorCode::LimitExceeded
    );
    // The same key twice in one call.
    assert_eq!(
        code(upsert(&[row(json!({})), row(json!({}))])),
        ErrorCode::ValidationError
    );
    let payload = json!({"table_id": ID, "keys": ["a", "a"]}).to_string();
    assert_eq!(
        code(parse("table.delete_many", &payload)),
        ErrorCode::ValidationError
    );
    let _ = rows;
    // Values.
    let dense = |n: usize| json!({"v": {"f32": f32s(&vec![1.0; n])}});
    assert_eq!(code(upsert(&[row(dense(2))])), ErrorCode::ValidationError);
    assert_eq!(
        code(parse(
            "table.upsert_many",
            &json!({"table_id": ID, "rows": [row(dense(4097))]}).to_string()
        )),
        ErrorCode::LimitExceeded
    );
    assert_eq!(
        code(upsert(&[row(json!({"v": {"f32": "AAA"}}))])),
        ErrorCode::ValidationError
    );
    // A row above 1 MiB.
    let big = json!({"tags": vec!["x".repeat(1000); 1100]});
    assert_eq!(code(upsert(&[row(big)])), ErrorCode::LimitExceeded);
    // A schema above 128 columns.
    let wide: Vec<String> = (0..129).map(|i| format!(r#""c{i}":"utf8""#)).collect();
    let schema = format!(r#"{{"id":"utf8",{}}}"#, wide.join(","));
    assert!(parse("table.create", &create(&schema, "")).is_err());
    // Ranked queries.
    let rank = |limit: u64, extra: &str| {
        format!(
            r#"{{"table_id":"{ID}","query":{{"select":null,"filter":null,"limit":{limit}}},"rank":{{"rankings":[{{"column":"body","op":"match","value":"x"}}]{extra}}}}}"#
        )
    };
    assert!(parse("query.rank", &rank(1000, "")).is_ok());
    assert_eq!(
        code(parse("query.rank", &rank(1001, ""))),
        ErrorCode::LimitExceeded
    );
    let many = format!(
        r#","x":0,"rankings":[{}]"#,
        [r#"{"column":"body","op":"match","value":"x"}"#; 3].join(",")
    );
    let _ = many;
    let batch = |n: usize| {
        format!(
            r#"{{"table_id":"{ID}","query":{{"select":null,"filter":null,"limit":1}},"rank":{{"rankings":[]}},"batch":[{}]}}"#,
            vec![r#"{"column":"body","op":"match","value":"x"}"#; n].join(",")
        )
    };
    assert!(parse("query.rank_many", &batch(256)).is_ok());
    assert!(parse("query.rank_many", &batch(257)).is_err());
    assert!(parse("query.rank_many", &batch(0)).is_err());
}

// ---- the scalar grammar's bytes ----

/// A table without search columns keeps its bytes: descriptors, create
/// payloads, rows and requests serialize to the literals of the base commit,
/// and the vector-aware grammar reads them to the same values.
#[test]
fn rule_tables_without_search_columns_keep_their_bytes() {
    let _g = lock();
    let plain = format!(
        r#"{{"id":"{ID}","name":"users","schema":{{"active":"bool","age":"int64","id":"utf8","score":"float64"}},"key":"id","format":"parquet"}}"#
    );
    let descriptor: TableDescriptor = serde_json::from_str(&plain).unwrap();
    assert_eq!(serde_json::to_string(&descriptor).unwrap(), plain);
    let created = r#"{"name":"users","schema":{"active":"bool","age":"int64","id":"utf8","score":"float64"},"key":"id","format":"parquet"}"#;
    let create: CreatePayload = serde_json::from_str(created).unwrap();
    assert_eq!(serde_json::to_string(&create).unwrap(), created);
    for text in [
        r#"{"active":true,"age":24,"id":"a","score":1.5}"#,
        r#"{"active":false,"age":-1,"id":"é\u0001","score":-0.0}"#,
    ] {
        let row: Row = serde_json::from_str(text).unwrap();
        assert_eq!(decode_row(text).unwrap(), row);
        assert_eq!(
            serde_json::to_string(&row).unwrap(),
            serde_json::to_string(&decode_row(text).unwrap()).unwrap()
        );
    }
    let literal = r#"{"active":true,"age":24,"id":"a","score":1.5}"#;
    assert_eq!(
        serde_json::to_string(&serde_json::from_str::<Row>(literal).unwrap()).unwrap(),
        literal
    );
    // Absent options are absent, never null or empty.
    let value = serde_json::to_value(&descriptor).unwrap();
    for key in ["partition_by", "full_text", "embed"] {
        assert!(value.get(key).is_none(), "{key}");
    }
    // Options are not accepted as null or empty.
    for extra in [
        r#","partition_by":null"#,
        r#","full_text":{}"#,
        r#","embed":{}"#,
        r#","full_text":null"#,
    ] {
        let text = format!("{}{}}}", &created[..created.len() - 1], extra);
        assert!(
            serde_json::from_str::<CreatePayload>(&text).is_err(),
            "{extra}"
        );
    }
}

/// The two grammars agree on everything the old one accepted, for requests
/// mutated at random from valid scalar ones: same request, byte for byte.
#[test]
fn rule_scalar_requests_parse_alike_in_both_grammars() {
    let _g = lock();
    let t = format!(r#""table_id":"{ID}""#);
    let row = r#"{"active":true,"age":24,"id":"a","score":1.5}"#;
    let query = format!(
        r#"{{{t},"query":{{"select":["id"],"filter":{{"op":"and","args":[{{"op":"gt","column":"age","value":18}},{{"op":"not","arg":{{"op":"eq","column":"score","value":1e2}}}}]}},"order_by":{{"column":"age","desc":true}},"limit":10}}}}"#
    );
    let seeds = vec![
        ("table.create", r#"{"name":"users","schema":{"id":"utf8","age":"int64"},"key":"id","format":"parquet"}"#.to_string()),
        ("table.describe", r#"{"name":"users"}"#.into()),
        ("table.list", "{}".into()),
        ("table.drop", format!("{{{t}}}")),
        ("table.insert", format!(r#"{{{t},"row":{row}}}"#)),
        ("table.insert_many", format!(r#"{{{t},"rows":[{row},{row}]}}"#)),
        ("table.get", format!(r#"{{{t},"key":"a"}}"#)),
        ("table.update", format!(r#"{{{t},"key":"a","values":{{"age":25}}}}"#)),
        ("table.upsert", format!(r#"{{{t},"row":{row}}}"#)),
        ("table.delete", format!(r#"{{{t},"key":-7}}"#)),
        ("query.collect", query.clone()),
        ("query.count", query.clone()),
        ("query.explain", query),
    ];
    let new_words = [
        "vector",
        "list",
        "contains",
        "starts_with",
        "sparse",
        "f32",
        "partition_by",
        "full_text",
        "embed",
        "upsert_many",
        "delete_many",
        "rank",
        "add_column",
        "drop_column",
        "search_status",
        "add_full_text",
        "drop_full_text",
    ];
    let mut rng = Rng(0x5eed_0004);
    let (mut same, mut refused) = (0, 0);
    for round in 0..6000 {
        let (op, payload) = &seeds[round % seeds.len()];
        let mut body = envelope(op, payload).into_bytes();
        for _ in 0..rng.below(4) {
            mutate(&mut body, &mut rng);
        }
        let old = parse_request(&body);
        let new = parse_oltp_request(&body);
        match (&old, &new) {
            (Ok(a), Ok(b)) => {
                assert_eq!(a, b, "{}", String::from_utf8_lossy(&body));
                assert_eq!(
                    serde_json::to_string(a).unwrap(),
                    serde_json::to_string(b).unwrap()
                );
                same += 1;
            }
            (Ok(_), Err(e)) => panic!(
                "the new grammar refuses what the old accepts: {e:?}: {}",
                String::from_utf8_lossy(&body)
            ),
            (Err(_), Ok(_)) => {
                let text = String::from_utf8_lossy(&body);
                assert!(
                    new_words.iter().any(|w| text.contains(w)),
                    "accepted without a new feature: {text}"
                );
            }
            (Err(_), Err(_)) => refused += 1,
        }
    }
    assert!(same > 500 && refused > 500, "{same} {refused}");
}

fn mutate(body: &mut Vec<u8>, rng: &mut Rng) {
    if body.is_empty() {
        return;
    }
    let at = rng.below(body.len());
    match rng.below(9) {
        0 => body[at] ^= 1 << rng.below(8),
        1 => body[at] = rng.next() as u8,
        2 => {
            body.remove(at);
        }
        3 => {
            let piece = [
                b'"', b'{', b'}', b'[', b']', b',', b':', b'\\', b'0', b'-', b'e', b'.', b' ',
                0xc3, 0xff,
            ][rng.below(15)];
            body.insert(at, piece);
        }
        4 => body.truncate(at),
        5 => {
            let end = (at + 1 + rng.below(24)).min(body.len());
            let piece = body[at..end].to_vec();
            let to = rng.below(body.len());
            body.splice(to..to, piece);
        }
        6 => {
            let end = (at + 1 + rng.below(8)).min(body.len());
            body.drain(at..end);
        }
        7 => {
            let token: &[&[u8]] = &[
                b"null",
                b"1e400",
                b"-0",
                b"18446744073709551616",
                b"\"\\ud800\"",
                b"{}",
                b"[]",
                b"true",
                b"\"\\u0000\"",
                b"99999999999999999999",
            ];
            let piece = token[rng.below(token.len())];
            let end = (at + 1 + rng.below(6)).min(body.len());
            body.splice(at..end, piece.iter().copied());
        }
        _ => {
            if let Some(p) = body.iter().skip(at).position(|b| b.is_ascii_digit()) {
                body[at + p] = b'0' + rng.below(10) as u8;
            }
        }
    }
}

// ---- arbitrary input ----

/// The decode budget the gateway charges before it parses a body.
fn budget(body: &[u8]) -> usize {
    let (mut in_string, mut escaped, mut structural) = (false, false, 0usize);
    for &byte in body {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
        } else if byte == b'"' {
            in_string = true;
        } else if b"{}[],:".contains(&byte) {
            structural += 1;
        }
    }
    body.len() * 2 + structural * 256 + 4096
}

fn corpus() -> Vec<(&'static str, String)> {
    let two = f32s(&[1.0, 2.0, 3.0]);
    let sp = format!(
        r#"{{"sparse":{{"i":"{}","f32":"{}"}}}}"#,
        u32s(&[1, 5]),
        f32s(&[1.0, 2.0])
    );
    let mu = format!(r#"{{"f32s":"{}","n":2}}"#, f32s(&[1.0, 2.0, 3.0, 4.0]));
    let r = format!(
        r#"{{"id":"a","body":"b","tags":["x","y"],"v":{{"f32":"{two}"}},"s":{sp},"m":{mu}}}"#
    );
    let t = format!(r#""table_id":"{ID}""#);
    let rank = |extra: &str| {
        format!(
            r#"{{{t},"query":{{"select":["id"],"filter":{{"op":"contains_any","column":"tags","values":["a","b"]}},"limit":5}},"rank":{{"rankings":[{{"column":"v","op":"cosine","value":{{"f32":"{two}"}}}},{{"column":"body","op":"match","value":"q"}}],"fusion":[1.0,0.5]{extra}}}}}"#
        )
    };
    vec![
        (
            "table.upsert_many",
            format!(
                r#"{{{t},"rows":[{r},{}]}}"#,
                r.replace(r#""id":"a""#, r#""id":"b""#)
            ),
        ),
        (
            "table.delete_many",
            format!(r#"{{{t},"keys":["a","b","c"]}}"#),
        ),
        ("table.insert", format!(r#"{{{t},"row":{r}}}"#)),
        (
            "table.update",
            format!(r#"{{{t},"key":"a","values":{{"v":{{"f32":"{two}"}}}}}}"#),
        ),
        (
            "table.create",
            create(
                r#"{"id":"utf8","body":"utf8","v":{"type":"vector","dim":3},"s":{"type":"vector","sparse":true},"m":{"type":"list","item":{"type":"vector","dim":2}},"tags":{"type":"list","item":"utf8"}}"#,
                r#","partition_by":"id","full_text":{"body":"english"},"embed":{"v":{"source":"body","model":"docs-v1"}}"#,
            ),
        ),
        (
            "table.add_column",
            format!(
                r#"{{{t},"name":"w","type":{{"type":"vector","dim":3}},"embed":{{"source":"body","model":"m"}}}}"#
            ),
        ),
        ("table.drop_column", format!(r#"{{{t},"name":"v"}}"#)),
        (
            "table.add_full_text",
            format!(r#"{{{t},"column":"body","language":"english"}}"#),
        ),
        (
            "table.drop_full_text",
            format!(r#"{{{t},"column":"body"}}"#),
        ),
        ("table.search_status", format!("{{{t}}}")),
        ("query.rank", rank("")),
        (
            "query.rank",
            rank(
                r#","rerank":{"ranking":{"column":"m","op":"dot","value":{"f32s":"AAAAAAAAAAA=","n":1}},"candidates":10}"#,
            ),
        ),
        (
            "query.rank_many",
            format!(
                r#"{{{t},"query":{{"select":null,"filter":{{"op":"starts_with","column":"id","value":"x"}},"limit":5}},"rank":{{"rankings":[]}},"batch":[{{"column":"v","op":"dot","value":{{"f32":"{two}"}}}}]}}"#
            ),
        ),
        (
            "query.collect",
            format!(
                r#"{{{t},"query":{{"select":null,"filter":{{"op":"contains","column":"tags","value":"x"}},"order_by":null,"limit":null}}}}"#
            ),
        ),
    ]
}

/// VEC-G11 and OLTP-G12: whatever the bytes, decoding returns and never
/// holds more memory than the gateway charged for the body; everything the
/// decoder accepts, it accepts again after it is written out.
#[test]
fn rule_arbitrary_bytes_never_panic_and_stay_inside_the_decode_budget() {
    let _g = lock();
    let seeds = corpus();
    let mut rng = Rng(0x5eed_0005);
    let (mut accepted, mut refused) = (0usize, 0usize);
    for round in 0..30_000 {
        let (op, payload) = &seeds[round % seeds.len()];
        let mut body = envelope(op, payload).into_bytes();
        for _ in 0..1 + rng.below(5) {
            mutate(&mut body, &mut rng);
        }
        let (result, used) = peak(|| parse_oltp_request(&body));
        assert!(
            used <= budget(&body),
            "{used} bytes for {}: {}",
            body.len(),
            String::from_utf8_lossy(&body)
        );
        match result {
            Ok(request) => {
                accepted += 1;
                // Writing an accepted request out and reading it back is the identity.
                let again = serde_json::to_vec(&request).unwrap();
                let back = parse_oltp_request(&again)
                    .unwrap_or_else(|e| panic!("{e:?}: {}", String::from_utf8_lossy(&again)));
                assert_eq!(back, request);
                let _ = request.operation.validate_for_table(&table());
            }
            Err(error) => {
                refused += 1;
                assert_eq!(error.outcome, Outcome::NotApplied);
            }
        }
    }
    assert!(accepted > 300 && refused > 1000, "{accepted} {refused}");
    // Raw bytes, and rows and values on their own.
    for _ in 0..30_000 {
        let len = rng.below(200);
        let bytes: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let _ = parse_oltp_request(&bytes);
        let _ = decode_row(&text);
        let _ = decode_upsert_row(&text);
        let _ = decode_row(&format!(r#"{{"v":{text}}}"#));
        let _ = decode_row(&format!(r#"{{"v":{{"f32":"{text}"}}}}"#));
        let _ = decode_row(&format!(
            r#"{{"v":{{"sparse":{{"i":"{text}","f32":"{text}"}}}}}}"#
        ));
    }
}

/// Every prefix of every valid request is refused without a panic, and the
/// complete ones are accepted.
#[test]
fn rule_every_prefix_of_a_valid_request_is_refused_cleanly() {
    let _g = lock();
    for (op, payload) in corpus() {
        let body = envelope(op, &payload).into_bytes();
        let full = parse_oltp_request(&body);
        assert!(full.is_ok(), "{op}: {full:?}");
        for end in 0..body.len() {
            let (result, used) = peak(|| parse_oltp_request(&body[..end]));
            assert!(result.is_err(), "{op}: prefix {end}");
            assert!(used <= budget(&body[..end]));
        }
    }
}

// ---- the gate and the reply ----

/// VEC-G14: a table that needs the feature data.vector.v1 is not created while the
/// feature is off, whichever way it needs it; a plain one is. Operations this
/// build decodes but does not serve are refused whether the feature is on or
/// not, and a group of rows is never queued whole.
#[test]
fn rule_the_gate_decides_create_and_the_unserved_operations() {
    let _g = lock();
    let creates = [
        create(r#"{"id":"utf8","v":{"type":"vector","dim":2}}"#, ""),
        create(r#"{"id":"utf8","s":{"type":"vector","sparse":true}}"#, ""),
        create(
            r#"{"id":"utf8","m":{"type":"list","item":{"type":"vector","dim":2}}}"#,
            "",
        ),
        create(r#"{"id":"utf8","t":{"type":"list","item":"utf8"}}"#, ""),
        create(
            r#"{"id":"utf8","b":"utf8"}"#,
            r#","full_text":{"b":"english"}"#,
        ),
        create(r#"{"id":"utf8","b":"utf8"}"#, r#","partition_by":"b""#),
    ];
    for payload in &creates {
        let operation = parse("table.create", payload).unwrap().operation;
        for check in [Operation::check_served, Operation::check_queued] {
            let off = check(&operation, false).expect_err(payload);
            assert_eq!(
                (off.code, off.outcome),
                (ErrorCode::UnsupportedOperation, Outcome::NotApplied)
            );
            assert!(off.message.contains("data.vector.v1"), "{off:?}");
            assert!(check(&operation, true).is_ok(), "{payload}");
        }
    }
    let plain = parse("table.create", &create(r#"{"id":"utf8","n":"int64"}"#, ""))
        .unwrap()
        .operation;
    for check in [Operation::check_served, Operation::check_queued] {
        assert!(check(&plain, false).is_ok() && check(&plain, true).is_ok());
    }
    let t = format!(r#""table_id":"{ID}""#);
    let rank = format!(
        r#"{{{t},"query":{{"select":null,"filter":null,"limit":1}},"rank":{{"rankings":[{{"column":"b","op":"match","value":"x"}}]}}}}"#
    );
    let many = format!(
        r#"{{{t},"query":{{"select":null,"filter":null,"limit":1}},"rank":{{"rankings":[]}},"batch":[{{"column":"b","op":"match","value":"x"}}]}}"#
    );
    let unserved = [
        (
            "table.add_column",
            format!(r#"{{{t},"name":"w","type":{{"type":"vector","dim":2}}}}"#),
        ),
        ("table.drop_column", format!(r#"{{{t},"name":"w"}}"#)),
        (
            "table.add_full_text",
            format!(r#"{{{t},"column":"b","language":"english"}}"#),
        ),
        ("table.drop_full_text", format!(r#"{{{t},"column":"b"}}"#)),
        ("table.search_status", format!("{{{t}}}")),
        ("query.rank", rank),
        ("query.rank_many", many),
    ];
    for (op, payload) in unserved {
        let operation = parse(op, &payload).unwrap().operation;
        for vector in [false, true] {
            for check in [Operation::check_served, Operation::check_queued] {
                // J serves the column operations and `table.search_status`.
                if op.starts_with("table.") {
                    assert!(check(&operation, vector).is_ok(), "{op}");
                    continue;
                }
                let error = check(&operation, vector).expect_err(op);
                assert_eq!(
                    (error.code, error.outcome),
                    (ErrorCode::UnsupportedOperation, Outcome::NotApplied),
                    "{op}"
                );
            }
        }
    }
    let upsert = parse(
        "table.upsert_many",
        &json!({"table_id": ID, "rows": []}).to_string(),
    )
    .unwrap()
    .operation;
    let delete = parse(
        "table.delete_many",
        &json!({"table_id": ID, "keys": []}).to_string(),
    )
    .unwrap()
    .operation;
    for operation in [&upsert, &delete] {
        assert!(
            operation.check_served(true).is_ok(),
            "the direct path serves it"
        );
        for vector in [false, true] {
            let error = operation.check_queued(vector).expect_err("queued whole");
            assert_eq!(error.code, ErrorCode::UnsupportedOperation);
        }
    }
}

/// The reply of a group: an outcome names its class and its error agrees.
#[test]
fn rule_row_outcomes_agree_with_their_errors() {
    let _g = lock();
    let error = |outcome: Outcome| {
        serde_json::to_value(ProtocolError::new(ErrorCode::ValidationError, "m", outcome)).unwrap()
    };
    let applied: RowOutcome = serde_json::from_value(json!({"o": "applied"})).unwrap();
    assert_eq!(applied, RowOutcome::Applied);
    for (o, outcome, ok) in [
        ("not_applied", Outcome::NotApplied, true),
        ("not_applied", Outcome::Unknown, false),
        ("outcome_unknown", Outcome::Unknown, true),
        ("outcome_unknown", Outcome::NotApplied, false),
    ] {
        let parsed = serde_json::from_value::<RowOutcome>(json!({"o": o, "error": error(outcome)}));
        assert_eq!(parsed.is_ok(), ok, "{o} with {outcome:?}");
    }
    for bad in [
        json!({"o": "applied", "error": error(Outcome::NotApplied)}),
        json!({"o": "not_applied"}),
        json!({"o": "outcome_unknown"}),
        json!({"o": "done"}),
        json!({}),
    ] {
        assert!(
            serde_json::from_value::<RowOutcome>(bad.clone()).is_err(),
            "{bad}"
        );
    }
    // The class follows the error on every conversion and on the wire.
    for outcome in [Outcome::NotApplied, Outcome::Unknown] {
        let converted =
            RowOutcome::from(ProtocolError::new(ErrorCode::OutcomeUnknown, "m", outcome));
        let wire = serde_json::to_value(&converted).unwrap();
        assert_eq!(
            wire["o"],
            if outcome == Outcome::Unknown {
                "outcome_unknown"
            } else {
                "not_applied"
            }
        );
        assert_eq!(
            serde_json::from_value::<RowOutcome>(wire).unwrap(),
            converted
        );
    }
}
