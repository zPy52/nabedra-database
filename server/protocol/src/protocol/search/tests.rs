use super::*;
use serde_json::json;

const ID: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";

/// A seeded generator, so a failure names its case.
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
    fn float(&mut self) -> f32 {
        loop {
            let value = f32::from_bits(self.next() as u32);
            if value.is_finite() {
                return value;
            }
        }
    }
    fn floats(&mut self, n: usize) -> Vec<f32> {
        (0..n).map(|_| self.float()).collect()
    }
    fn vector(&mut self) -> Vector {
        match self.below(3) {
            0 => {
                let dim = 1 + self.below(40);
                Vector::Dense(self.floats(dim))
            }
            1 => {
                let n = self.below(20);
                let mut indices: Vec<u32> = (0..n).map(|_| self.next() as u32).collect();
                indices.sort_unstable();
                indices.dedup();
                let weights = self.floats(indices.len());
                Vector::Sparse { indices, weights }
            }
            _ => {
                let (count, dim) = (self.below(5), 1 + self.below(9));
                Vector::Multi {
                    values: self.floats(count * dim),
                    count: count as u16,
                }
            }
        }
    }
    fn ty(&mut self) -> ColumnType {
        let dim = 1 + self.below(4096) as u16;
        [
            ColumnType::Utf8,
            ColumnType::Int64,
            ColumnType::Float64,
            ColumnType::Bool,
            ColumnType::Vector(dim),
            ColumnType::SparseVector,
            ColumnType::VectorList(dim),
            ColumnType::Utf8List,
        ][self.below(8)]
    }
}

fn value(text: &str) -> Result<Scalar, ProtocolError> {
    decode_row(&format!(r#"{{"v":{text}}}"#)).map(|mut row| row.remove("v").unwrap())
}
fn parse_op(op: &str, payload: serde_json::Value) -> Result<Operation, ProtocolError> {
    parse_oltp_request(
        &serde_json::to_vec(&json!({"request_id":ID,"operation":op,"payload":payload})).unwrap(),
    )
    .map(|request| request.operation)
}
fn code<T: fmt::Debug>(result: Result<T, ProtocolError>) -> ErrorCode {
    let error = result.unwrap_err();
    assert_eq!(error.outcome, Outcome::NotApplied);
    error.code
}
fn b64(bytes: &[u8]) -> String {
    let mut padded = bytes.to_vec();
    padded.resize(bytes.len().next_multiple_of(4), 0);
    assert_eq!(padded.len(), bytes.len(), "test data is whole words");
    encode_words(padded.as_chunks::<4>().0.iter().copied(), padded.len() / 4)
}
fn floats(values: &[f32]) -> String {
    encode_floats(values)
}

// ---- base64 ----

#[test]
fn base64_matches_the_standard_vectors() {
    // RFC 4648 section 10, on whole words.
    for (bytes, text) in [
        (&b""[..], ""),
        (b"foob", "Zm9vYg=="),
        (b"foobar\xfb\xff", "Zm9vYmFy+/8="),
        (b"\x00\x00\x00\x00", "AAAAAA=="),
        (
            b"\x00\x00\x80\x3f\x00\x00\x00\x40\x00\x00\x40\x40",
            "AACAPwAAAEAAAEBA",
        ),
    ] {
        assert_eq!(b64(bytes), text);
        assert_eq!(decode_words(text, 4).unwrap().concat(), bytes);
    }
    assert_eq!(floats(&[1.0, 2.0, 3.0]), "AACAPwAAAEAAAEBA");
}

#[test]
fn base64_round_trips_every_length_and_bit_pattern() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for n in 0..200 {
        let words: Vec<[u8; 4]> = (0..n).map(|_| (rng.next() as u32).to_le_bytes()).collect();
        let text = encode_words(words.iter().copied(), n);
        assert_eq!(decode_words(&text, n).unwrap(), words);
        if n > 0 {
            assert_eq!(code(decode_words(&text, n - 1)), ErrorCode::LimitExceeded);
        }
    }
}

#[test]
fn base64_rejects_every_other_spelling() {
    for text in [
        "Zm9vYg",           // no padding
        "Zm9vYg=",          // short padding
        "Zm9vYh==",         // trailing bits
        "Zm9vYmFy+/9=",     // trailing bits, one pad
        "Zm9vYmFy-_8=",     // URL alphabet
        "Zm9v Yg==",        // whitespace
        "Zm9vYg==\n",       // newline
        "====",             // only padding
        "A===",             // one sextet
        "Zm9vYg==Zm9vYg==", // padding inside
        "Zm9v",             // three bytes: not a whole word
        "Zm8=",             // two bytes
        "Zg==",             // one byte
        "Zm9vYé==",         // not ASCII
    ] {
        assert_eq!(
            code(decode_words(text, 100)),
            ErrorCode::ValidationError,
            "{text:?}"
        );
    }
}

#[test]
fn base64_accepts_only_what_it_encodes() {
    // Any accepted text is the one encoding of its bytes.
    let mut rng = Rng(7);
    let mut accepted = 0;
    for _ in 0..20_000 {
        let n = 4 * (1 + rng.below(3));
        let text: String = (0..n).map(|_| b"AAB/=-"[rng.below(6)] as char).collect();
        if let Ok(words) = decode_words(&text, 100) {
            accepted += 1;
            assert_eq!(encode_words(words.iter().copied(), words.len()), text);
        }
    }
    assert!(accepted > 10, "the generator reaches accepted texts");
}

// ---- values ----

#[test]
fn values_round_trip_bit_for_bit() {
    let mut rng = Rng(42);
    for _ in 0..500 {
        let vector = rng.vector();
        let text = serde_json::to_string(&Scalar::Vector(Box::new(vector.clone()))).unwrap();
        let Scalar::Vector(decoded) = value(&text).unwrap() else {
            panic!("{text}");
        };
        let bits = |v: &Vector| match v {
            Vector::Dense(values) | Vector::Multi { values, .. } => {
                values.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
            }
            Vector::Sparse { weights, .. } => weights.iter().map(|v| v.to_bits()).collect(),
        };
        assert_eq!(bits(&decoded), bits(&vector));
        assert_eq!(*decoded, vector);
        assert_eq!(
            serde_json::to_string(&Scalar::Vector(decoded)).unwrap(),
            text
        );
    }
    for (text, expected) in [
        ("null", Scalar::Null),
        ("[]", Scalar::Utf8List([].into())),
        (
            r#"["a","","a","ñ"]"#,
            Scalar::Utf8List(["a".into(), "".into(), "a".into(), "ñ".into()].into()),
        ),
        ("1", Scalar::Int64(1)),
        ("1.0", Scalar::Float64(1.0)),
    ] {
        assert_eq!(value(text).unwrap(), expected);
        assert_eq!(serde_json::to_string(&expected).unwrap(), text);
    }
}

#[test]
fn values_have_the_wire_form_of_the_contract() {
    for (vector, text) in [
        (
            Vector::Dense(vec![1.0, 2.0, 3.0]),
            r#"{"f32":"AACAPwAAAEAAAEBA"}"#,
        ),
        (
            Vector::Sparse {
                indices: vec![1, 7],
                weights: vec![1.0, -0.0],
            },
            r#"{"sparse":{"i":"AQAAAAcAAAA=","f32":"AACAPwAAAIA="}}"#,
        ),
        (
            Vector::Multi {
                values: vec![1.0, 2.0],
                count: 2,
            },
            r#"{"f32s":"AACAPwAAAEA=","n":2}"#,
        ),
        (
            Vector::Multi {
                values: vec![],
                count: 0,
            },
            r#"{"f32s":"","n":0}"#,
        ),
    ] {
        assert_eq!(serde_json::to_string(&vector).unwrap(), text);
        assert_eq!(value(text).unwrap(), Scalar::Vector(Box::new(vector)));
    }
    // JSON may escape any character.
    assert_eq!(
        value(r#"{"f32":"AACAPwAAAEAAAEBA"}"#).unwrap(),
        Scalar::Vector(Box::new(Vector::Dense(vec![1.0, 2.0, 3.0])))
    );
}

#[test]
fn vs17_values_reject_every_malformed_form() {
    let one = floats(&[1.0]);
    let two = floats(&[1.0, 2.0]);
    let i = |values: &[u32]| encode_words(values.iter().map(|v| v.to_le_bytes()), values.len());
    let sparse =
        |indices: &[u32], w: &str| format!(r#"{{"sparse":{{"i":"{}","f32":"{w}"}}}}"#, i(indices));
    for text in [
        "{}".to_string(),
        r#"{"f32":""}"#.into(), // dimension 0
        r#"{"f32":null}"#.into(),
        r#"{"f32":[1.0]}"#.into(),
        r#"{"f32":1}"#.into(),
        format!(r#"{{"f32":"{one}","x":1}}"#),
        format!(r#"{{"f32":"{one}","f32":"{one}"}}"#),
        format!(r#"{{"f32":"{one}","n":1}}"#),
        format!(r#"{{"f32":"{one}","f32s":"{one}","n":1}}"#),
        format!(r#"{{"f32":"{}"}}"#, floats(&[1.0]).trim_end_matches('=')),
        format!(r#"{{"f32":"{}"}}"#, b64(&f32::NAN.to_le_bytes())),
        format!(r#"{{"f32":"{}"}}"#, b64(&f32::INFINITY.to_le_bytes())),
        format!(r#"{{"f32":"{}"}}"#, b64(&f32::NEG_INFINITY.to_le_bytes())),
        // multi
        format!(r#"{{"f32s":"{two}"}}"#),
        r#"{"n":2}"#.to_string(),
        format!(r#"{{"f32s":"{two}","n":null}}"#),
        format!(r#"{{"f32s":"{two}","n":3}}"#),
        format!(r#"{{"f32s":"{two}","n":0}}"#),
        format!(r#"{{"f32s":"{two}","n":-1}}"#),
        format!(r#"{{"f32s":"{two}","n":1.0}}"#),
        format!(r#"{{"f32s":"{two}","n":65536}}"#),
        r#"{"f32s":"","n":1}"#.into(), // dimension 0
        // sparse
        r#"{"sparse":null}"#.into(),
        r#"{"sparse":{}}"#.into(),
        r#"{"sparse":["",""]}"#.into(),
        format!(r#"{{"sparse":{{"i":"{}","f32":"{one}","x":1}}}}"#, i(&[1])),
        sparse(&[1, 2], &one),
        sparse(&[2, 1], &two),
        sparse(&[1, 1], &two),
        sparse(&[1], &b64(&f32::NAN.to_le_bytes())),
        // lists
        "[1]".into(),
        "[null]".into(),
        r#"["a",["b"]]"#.into(),
        r#"[{}]"#.into(),
        // private serde_json markers
        r#"{"$serde_json::private::Number":"1"}"#.into(),
        r#"{"$serde_json::private::RawValue":"1"}"#.into(),
    ] {
        assert_eq!(code(value(&text)), ErrorCode::ValidationError, "{text}");
    }
}

#[test]
fn vs17_values_are_bounded_before_they_are_decoded() {
    let dense = |n: usize| format!(r#"{{"f32":"{}"}}"#, floats(&vec![1.0; n]));
    assert!(value(&dense(4096)).is_ok());
    assert_eq!(code(value(&dense(4097))), ErrorCode::LimitExceeded);
    let multi =
        |n: usize, dim: usize| format!(r#"{{"f32s":"{}","n":{n}}}"#, floats(&vec![1.0; n * dim]));
    assert!(value(&multi(1024, 1)).is_ok());
    assert_eq!(code(value(&multi(1025, 1))), ErrorCode::LimitExceeded);
    assert_eq!(code(value(&multi(1, 4097))), ErrorCode::ValidationError);
    assert_eq!(code(value(&multi(1024, 257))), ErrorCode::LimitExceeded);
    let huge = format!(r#"{{"f32":"{}"}}"#, "A".repeat(6 * MAX_ROW_BYTES));
    assert_eq!(code(value(&huge)), ErrorCode::LimitExceeded);
}

#[test]
fn upsert_rows_decode_one_object_with_unique_columns() {
    assert_eq!(
        code(decode_upsert_row(r#"{"id":1,"id":1}"#)),
        ErrorCode::ValidationError
    );
    for text in ["[]", "null", "1", r#"{"a":1} x"#, r#"{"a":1"#, ""] {
        assert_eq!(code(decode_row(text)), ErrorCode::ValidationError, "{text}");
        assert_eq!(code(decode_upsert_row(text)), ErrorCode::ValidationError);
    }
}

#[test]
fn serde_decoding_of_scalars_stays_scalar() {
    for text in ["null", "[]", r#"["a"]"#, r#"{"f32":"AACAPw=="}"#] {
        assert!(serde_json::from_str::<Scalar>(text).is_err());
    }
}

// ---- column types ----

#[test]
fn column_types_have_the_wire_form_of_the_contract() {
    for (ty, text) in [
        (ColumnType::Utf8, r#""utf8""#),
        (ColumnType::Int64, r#""int64""#),
        (ColumnType::Float64, r#""float64""#),
        (ColumnType::Bool, r#""bool""#),
        (ColumnType::Vector(1024), r#"{"type":"vector","dim":1024}"#),
        (
            ColumnType::SparseVector,
            r#"{"type":"vector","sparse":true}"#,
        ),
        (
            ColumnType::VectorList(128),
            r#"{"type":"list","item":{"type":"vector","dim":128}}"#,
        ),
        (ColumnType::Utf8List, r#"{"type":"list","item":"utf8"}"#),
    ] {
        assert_eq!(serde_json::to_string(&ty).unwrap(), text);
        assert_eq!(serde_json::from_str::<ColumnType>(text).unwrap(), ty);
    }
    let mut rng = Rng(3);
    for _ in 0..200 {
        let ty = rng.ty();
        let text = serde_json::to_string(&ty).unwrap();
        assert_eq!(serde_json::from_str::<ColumnType>(&text).unwrap(), ty);
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(serde_json::from_value::<ColumnType>(value).unwrap(), ty);
    }
}

#[test]
fn vs17_column_types_reject_every_other_form() {
    for text in [
        r#""vector""#,
        r#""list""#,
        r#""Utf8""#,
        "null",
        "1",
        "[]",
        "{}",
        r#"{"utf8":null}"#,
        r#"{"type":"utf8"}"#,
        r#"{"type":"vector"}"#,
        r#"{"dim":3}"#,
        r#"{"type":"vector","dim":0}"#,
        r#"{"type":"vector","dim":4097}"#,
        r#"{"type":"vector","dim":65537}"#,
        r#"{"type":"vector","dim":-1}"#,
        r#"{"type":"vector","dim":3.0}"#,
        r#"{"type":"vector","dim":"3"}"#,
        r#"{"type":"vector","dim":null}"#,
        r#"{"type":"vector","sparse":false}"#,
        r#"{"type":"vector","sparse":null}"#,
        r#"{"type":"vector","sparse":true,"dim":3}"#,
        r#"{"type":"vector","dim":3,"metric":"cosine"}"#,
        r#"{"type":"vector","dim":3,"dim":3}"#,
        r#"{"type":"list"}"#,
        r#"{"type":"list","item":"int64"}"#,
        r#"{"type":"list","item":null}"#,
        r#"{"type":"list","item":"utf8","dim":3}"#,
        r#"{"type":"list","item":{"type":"vector","sparse":true}}"#,
        r#"{"type":"list","item":{"type":"vector","dim":0}}"#,
        r#"{"type":"list","item":{"type":"vector","dim":4097}}"#,
        r#"{"type":"list","item":{"type":"list","item":"utf8"}}"#,
        r#"{"type":"list","item":{"type":"vector","dim":3,"x":1}}"#,
        r#"{"$serde_json::private::RawValue":"\"utf8\""}"#,
    ] {
        assert!(serde_json::from_str::<ColumnType>(text).is_err(), "{text}");
    }
}

// ---- schemas ----

fn create(schema: serde_json::Value, options: serde_json::Value) -> serde_json::Value {
    let mut payload = json!({"name":"chunks","schema":schema,"key":"id","format":"parquet"});
    for (name, value) in options.as_object().unwrap() {
        payload[name] = value.clone();
    }
    payload
}
fn chunks() -> TableDescriptor {
    serde_json::from_value(json!({
        "id": ID, "name": "chunks", "key": "id", "format": "parquet",
        "schema": {
            "id": "utf8", "tenant": "utf8", "year": "int64", "body": "utf8",
            "acl": {"type":"list","item":"utf8"},
            "embedding": {"type":"vector","dim":2},
            "auto": {"type":"vector","dim":2},
            "sparse": {"type":"vector","sparse":true},
            "tokens": {"type":"list","item":{"type":"vector","dim":2}},
        },
        "partition_by": "tenant",
        "full_text": {"body": "english"},
        "embed": {"auto": {"source": "body", "model": "docs-v1"}},
    }))
    .unwrap()
}

#[test]
fn search_options_round_trip_and_are_absent_when_empty() {
    let table = chunks();
    table.validate().unwrap();
    assert!(table.uses_search());
    let text = serde_json::to_string(&table).unwrap();
    assert_eq!(
        text,
        format!(
            r#"{{"id":"{ID}","name":"chunks","schema":{{"acl":{{"type":"list","item":"utf8"}},"auto":{{"type":"vector","dim":2}},"body":"utf8","embedding":{{"type":"vector","dim":2}},"id":"utf8","sparse":{{"type":"vector","sparse":true}},"tenant":"utf8","tokens":{{"type":"list","item":{{"type":"vector","dim":2}}}},"year":"int64"}},"key":"id","format":"parquet","partition_by":"tenant","full_text":{{"body":"english"}},"embed":{{"auto":{{"source":"body","model":"docs-v1"}}}}}}"#
        )
    );
    assert_eq!(
        serde_json::from_str::<TableDescriptor>(&text).unwrap(),
        table
    );
    let value = serde_json::to_value(&table).unwrap();
    assert_eq!(
        serde_json::from_value::<TableDescriptor>(value).unwrap(),
        table
    );

    for options in [
        json!({"full_text": {}}),
        json!({"embed": {}}),
        json!({"partition_by": null}),
        json!({"full_text": null}),
        json!({"embed": null}),
        json!({"full_text": {"body": null}}),
        json!({"full_text": ["body"]}),
        json!({"embed": {"v": {"source": "body"}}}),
        json!({"embed": {"v": ["body", "m"]}}),
        json!({"embed": {"v": {"source": "body", "model": "m", "x": 1}}}),
        json!({"partition": "tenant"}),
    ] {
        assert_eq!(
            code(parse_op(
                "table.create",
                create(json!({"id":"utf8"}), options.clone())
            )),
            ErrorCode::ValidationError,
            "{options}"
        );
    }
    for raw in [
        r#"{"name":"t","schema":{"id":"utf8"},"key":"id","format":"parquet","full_text":{"id":"english","id":"english"}}"#.to_string(),
        format!(r#"{{"name":"t","schema":{{"id":"utf8"}},"key":"id","format":"parquet","full_text":{{{}}}}}"#,
            (0..129).map(|i| format!(r#""c{i}":"x""#)).collect::<Vec<_>>().join(",")),
    ] {
        assert!(serde_json::from_str::<CreatePayload>(&raw).is_err());
    }
}

#[test]
fn schema_rules_reject_every_violation() {
    let vector = json!({"type":"vector","dim":2});
    let ok = |schema, options| parse_op("table.create", create(schema, options)).map(|_| ());
    ok(
        json!({"id":"utf8","tenant":"int64","body":"utf8","v":vector}),
        json!({"partition_by":"tenant","full_text":{"body":"english"},"embed":{"v":{"source":"body","model":"a.b-C_1"}}}),
    )
    .unwrap();
    for (why, schema, options) in [
        ("key", json!({"id": vector}), json!({})),
        (
            "key",
            json!({"id": {"type":"list","item":"utf8"}}),
            json!({}),
        ),
        (
            "reserved",
            json!({"id":"utf8","_score":"float64","v":vector}),
            json!({}),
        ),
        (
            "reserved",
            json!({"id":"utf8","_score":"float64"}),
            json!({"full_text":{"id":"english"}}),
        ),
        ("reserved", json!({"id":"utf8","_score":vector}), json!({})),
        (
            "partition_by missing",
            json!({"id":"utf8"}),
            json!({"partition_by":"x"}),
        ),
        (
            "partition_by vector",
            json!({"id":"utf8","v":vector}),
            json!({"partition_by":"v"}),
        ),
        (
            "partition_by list",
            json!({"id":"utf8","v":{"type":"list","item":"utf8"}}),
            json!({"partition_by":"v"}),
        ),
        (
            "full_text missing",
            json!({"id":"utf8"}),
            json!({"full_text":{"x":"english"}}),
        ),
        (
            "full_text int",
            json!({"id":"utf8","x":"int64"}),
            json!({"full_text":{"x":"english"}}),
        ),
        (
            "full_text list",
            json!({"id":"utf8","x":{"type":"list","item":"utf8"}}),
            json!({"full_text":{"x":"english"}}),
        ),
        (
            "language",
            json!({"id":"utf8"}),
            json!({"full_text":{"id":""}}),
        ),
        (
            "language",
            json!({"id":"utf8"}),
            json!({"full_text":{"id":"English"}}),
        ),
        (
            "language",
            json!({"id":"utf8"}),
            json!({"full_text":{"id":"x".repeat(33)}}),
        ),
        (
            "embed missing",
            json!({"id":"utf8"}),
            json!({"embed":{"v":{"source":"id","model":"m"}}}),
        ),
        (
            "embed scalar",
            json!({"id":"utf8"}),
            json!({"embed":{"id":{"source":"id","model":"m"}}}),
        ),
        (
            "embed sparse",
            json!({"id":"utf8","v":{"type":"vector","sparse":true}}),
            json!({"embed":{"v":{"source":"id","model":"m"}}}),
        ),
        (
            "embed multi",
            json!({"id":"utf8","v":{"type":"list","item":vector}}),
            json!({"embed":{"v":{"source":"id","model":"m"}}}),
        ),
        (
            "embed source missing",
            json!({"id":"utf8","v":vector}),
            json!({"embed":{"v":{"source":"x","model":"m"}}}),
        ),
        (
            "embed source int",
            json!({"id":"int64","v":vector}),
            json!({"embed":{"v":{"source":"id","model":"m"}}}),
        ),
        (
            "embed model",
            json!({"id":"utf8","v":vector}),
            json!({"embed":{"v":{"source":"id","model":""}}}),
        ),
        (
            "embed model",
            json!({"id":"utf8","v":vector}),
            json!({"embed":{"v":{"source":"id","model":"-m"}}}),
        ),
        (
            "embed model",
            json!({"id":"utf8","v":vector}),
            json!({"embed":{"v":{"source":"id","model":"a b"}}}),
        ),
        (
            "embed model",
            json!({"id":"utf8","v":vector}),
            json!({"embed":{"v":{"source":"id","model":"x".repeat(65)}}}),
        ),
    ] {
        assert_eq!(
            code(ok(schema.clone(), options)),
            ErrorCode::ValidationError,
            "{why} {schema}"
        );
    }
    // `_score` stays an ordinary name on a table without search columns.
    ok(json!({"id":"utf8","_score":"float64"}), json!({})).unwrap();
    ok(
        json!({"id":"utf8","_score":"float64","l":{"type":"list","item":"utf8"}}),
        json!({}),
    )
    .unwrap();
}

#[test]
fn a_table_has_at_most_eight_search_columns() {
    let schema = |vectors: usize| {
        let mut schema = json!({"id":"utf8","body":"utf8","tags":{"type":"list","item":"utf8"}});
        for i in 0..vectors {
            schema[format!("v{i}")] = json!({"type":"vector","dim":1});
        }
        schema
    };
    let full_text = json!({"full_text":{"body":"english"}});
    for (vectors, options, ok) in [
        (8, json!({}), true),
        (9, json!({}), false),
        (7, full_text.clone(), true),
        (8, full_text, false),
    ] {
        let result = parse_op("table.create", create(schema(vectors), options));
        assert_eq!(result.is_ok(), ok, "{vectors}");
    }
}

// ---- rows ----

fn row() -> serde_json::Value {
    json!({
        "id":"a","tenant":"t","year":2024,"body":"text","acl":["g"],
        "embedding":{"f32":floats(&[1.0, 2.0])},
        "sparse":{"sparse":{"i":"","f32":""}},
        "tokens":{"f32s":floats(&[1.0, 2.0, 3.0, 4.0]),"n":2},
    })
}

#[test]
fn rows_are_validated_against_every_column_type() {
    let table = chunks();
    let check = |op: &str, payload| parse_op(op, payload)?.validate_for_table(&table);
    for op in ["table.insert", "table.upsert"] {
        check(op, json!({"table_id":ID,"row":row()})).unwrap();
    }
    for op in ["table.insert_many", "table.upsert_many"] {
        check(op, json!({"table_id":ID,"rows":[row()]})).unwrap();
    }
    let mut nulls = row();
    for column in ["embedding", "sparse", "tokens"] {
        nulls[column] = json!(null);
    }
    check("table.upsert", json!({"table_id":ID,"row":nulls})).unwrap();
    for (column, bad) in [
        ("embedding", json!({"f32":floats(&[1.0])})),
        ("embedding", json!({"f32":floats(&[1.0, 2.0, 3.0])})),
        ("embedding", json!({"f32s":floats(&[1.0, 2.0]),"n":1})),
        ("embedding", json!({"sparse":{"i":"","f32":""}})),
        ("embedding", json!([1.0, 2.0])),
        ("embedding", json!("AACAPwAAAEA=")),
        ("sparse", json!({"f32":floats(&[1.0, 2.0])})),
        ("tokens", json!({"f32s":floats(&[1.0, 2.0, 3.0]),"n":3})),
        ("tokens", json!({"f32":floats(&[1.0, 2.0])})),
        ("acl", json!(null)),
        ("acl", json!("g")),
        ("acl", json!({"f32":floats(&[1.0, 2.0])})),
        ("body", json!(null)),
        ("body", json!(["x"])),
        ("year", json!(null)),
        ("year", json!({"f32":floats(&[1.0, 2.0])})),
        // An embedded column is never written, not even as null.
        ("auto", json!({"f32":floats(&[1.0, 2.0])})),
        ("auto", json!(null)),
        ("_score", json!(1.0)),
    ] {
        let mut bad_row = row();
        bad_row[column] = bad.clone();
        for (op, payload) in [
            ("table.insert", json!({"table_id":ID,"row":bad_row})),
            ("table.upsert", json!({"table_id":ID,"row":bad_row})),
            (
                "table.insert_many",
                json!({"table_id":ID,"rows":[row(), bad_row]}),
            ),
            (
                "table.upsert_many",
                json!({"table_id":ID,"rows":[row(), bad_row]}),
            ),
            (
                "table.update",
                json!({"table_id":ID,"key":"a","values":{column: bad}}),
            ),
        ] {
            assert_eq!(
                code(check(op, payload)),
                ErrorCode::ValidationError,
                "{op} {column} {bad}"
            );
        }
    }
    for column in ["embedding", "sparse", "tokens", "acl", "body"] {
        let mut missing = row();
        missing.as_object_mut().unwrap().remove(column);
        assert_eq!(
            code(check("table.upsert", json!({"table_id":ID,"row":missing}))),
            ErrorCode::ValidationError,
            "{column}"
        );
    }
    check(
        "table.update",
        json!({"table_id":ID,"key":"a","values":{"embedding":null,"acl":[]}}),
    )
    .unwrap();

    // A stored row may hold the embedded column.
    let mut stored = decode_row(&row().to_string()).unwrap();
    validate_row(&stored, &table, RowMode::Stored).unwrap();
    stored.insert("auto".into(), Scalar::Null);
    validate_row(&stored, &table, RowMode::Stored).unwrap();
    assert!(validate_row(&stored, &table, RowMode::Complete).is_err());
    stored.remove("body");
    assert!(validate_row(&stored, &table, RowMode::Stored).is_err());
}

#[test]
fn vs17_a_row_of_a_search_table_is_at_most_one_mebibyte() {
    let table: TableDescriptor = serde_json::from_value(json!({
        "id":ID,"name":"t","key":"id","format":"parquet",
        "schema":{"id":"utf8","f":"float64","m":{"type":"list","item":{"type":"vector","dim":4096}}},
    }))
    .unwrap();
    // 60 vectors of 16 KiB each, as raw bytes. Their base64 is 1.25 MiB.
    let vectors = json!({"f32s":floats(&vec![1.0; 60 * 4096]),"n":60});
    let rest = 60 * 4096 * 4 + r#"{"f":0,"id":"","m":{"f32s":"","n":60}}"#.len();
    for (over, allowed) in [(0, true), (1, false)] {
        let row =
            json!({"id":"x".repeat(MAX_ROW_BYTES - rest + over),"f":1.2345678e-200,"m":vectors});
        for (op, payload) in [
            ("table.insert", json!({"table_id":ID,"row":row})),
            ("table.upsert", json!({"table_id":ID,"row":row})),
            ("table.insert_many", json!({"table_id":ID,"rows":[row]})),
            ("table.upsert_many", json!({"table_id":ID,"rows":[row]})),
        ] {
            let checked = parse_op(op, payload).unwrap().validate_for_table(&table);
            assert_eq!(checked.is_ok(), allowed, "{op} {over}");
            if !allowed {
                assert_eq!(code(checked), ErrorCode::LimitExceeded);
            }
        }
    }
    let update = json!({"table_id":ID,"key":"a","values":{"f":1.0,"m":{"f32s":"","n":0}}});
    parse_op("table.update", update)
        .unwrap()
        .validate_for_table(&table)
        .unwrap();
    // Tables without search columns keep today's limit, lists included.
    let plain: TableDescriptor = serde_json::from_value(
        json!({"id":ID,"name":"t","schema":{"id":"utf8","l":{"type":"list","item":"utf8"}},"key":"id","format":"parquet"}),
    )
    .unwrap();
    let row = json!({"id":"x".repeat(MAX_ROW_BYTES),"l":["x".repeat(MAX_ROW_BYTES)]});
    for (op, payload) in [
        ("table.insert", json!({"table_id":ID,"row":row})),
        ("table.upsert_many", json!({"table_id":ID,"rows":[row]})),
    ] {
        parse_op(op, payload)
            .unwrap()
            .validate_for_table(&plain)
            .unwrap();
    }
}

// ---- the scalar grammar ----

#[test]
fn the_scalar_grammar_knows_nothing_new() {
    let query = |filter| json!({"table_id":ID,"query":{"select":null,"filter":filter,"order_by":null,"limit":null}});
    let vector = json!({"f32":floats(&[1.0])});
    for (op, payload, expected) in [
        (
            "table.create",
            create(
                json!({"id":"utf8","v":{"type":"vector","dim":1}}),
                json!({}),
            ),
            ErrorCode::ValidationError,
        ),
        (
            "table.create",
            create(
                json!({"id":"utf8","v":{"type":"list","item":"utf8"}}),
                json!({}),
            ),
            ErrorCode::ValidationError,
        ),
        (
            "table.create",
            create(json!({"id":"utf8"}), json!({"partition_by":"id"})),
            ErrorCode::ValidationError,
        ),
        (
            "table.create",
            create(json!({"id":"utf8"}), json!({"full_text":{"id":"english"}})),
            ErrorCode::ValidationError,
        ),
        (
            "table.insert",
            json!({"table_id":ID,"row":{"id":"a","v":vector}}),
            ErrorCode::ValidationError,
        ),
        (
            "table.insert",
            json!({"table_id":ID,"row":{"id":"a","v":null}}),
            ErrorCode::ValidationError,
        ),
        (
            "table.upsert",
            json!({"table_id":ID,"row":{"id":"a","v":["x"]}}),
            ErrorCode::ValidationError,
        ),
        (
            "table.insert_many",
            json!({"table_id":ID,"rows":[{"id":"a","v":["x"]}]}),
            ErrorCode::ValidationError,
        ),
        (
            "table.update",
            json!({"table_id":ID,"key":"a","values":{"v":null}}),
            ErrorCode::ValidationError,
        ),
        (
            "query.collect",
            query(json!({"op":"contains","column":"a","value":"x"})),
            ErrorCode::ValidationError,
        ),
        (
            "query.count",
            query(json!({"op":"not","arg":{"op":"starts_with","column":"a","value":"x"}})),
            ErrorCode::ValidationError,
        ),
        (
            "query.explain",
            query(json!({"op":"contains_any","column":"a","values":["x"]})),
            ErrorCode::ValidationError,
        ),
        (
            "table.upsert_many",
            json!({"table_id":ID,"rows":[]}),
            ErrorCode::UnsupportedOperation,
        ),
        (
            "table.delete_many",
            json!({"table_id":ID,"keys":[]}),
            ErrorCode::UnsupportedOperation,
        ),
        (
            "table.search_status",
            json!({"table_id":ID}),
            ErrorCode::UnsupportedOperation,
        ),
        (
            "table.drop_column",
            json!({"table_id":ID,"name":"v"}),
            ErrorCode::UnsupportedOperation,
        ),
        ("query.rank", json!({}), ErrorCode::UnsupportedOperation),
    ] {
        let raw =
            serde_json::to_vec(&json!({"request_id":ID,"operation":op,"payload":payload})).unwrap();
        assert_eq!(code(parse_request(&raw)), expected, "{op} {payload}");
        assert!(serde_json::from_slice::<Request>(&raw).is_err(), "{op}");
    }
}

// ---- filters ----

#[test]
fn text_filters_decode_strictly_and_match() {
    let query = |filter: &serde_json::Value| {
        parse_op(
            "query.collect",
            json!({"table_id":ID,"query":{"select":null,"filter":filter,"order_by":null,"limit":null}}),
        )
    };
    let row = decode_row(&row().to_string()).unwrap();
    for (filter, matches) in [
        (json!({"op":"contains","column":"acl","value":"g"}), true),
        (json!({"op":"contains","column":"acl","value":"G"}), false),
        (
            json!({"op":"contains_any","column":"acl","values":["x","g"]}),
            true,
        ),
        (
            json!({"op":"contains_any","column":"acl","values":["x"]}),
            false,
        ),
        (
            json!({"op":"contains_any","column":"acl","values":[]}),
            false,
        ),
        (
            json!({"op":"starts_with","column":"body","value":"te"}),
            true,
        ),
        (json!({"op":"starts_with","column":"body","value":""}), true),
        (json!({"op":"starts_with","column":"id","value":"b"}), false),
        (
            json!({"op":"not","arg":{"op":"contains","column":"acl","value":"x"}}),
            true,
        ),
        (
            json!({"op":"and","args":[{"op":"starts_with","column":"id","value":"a"},{"op":"ge","column":"year","value":2024}]}),
            true,
        ),
    ] {
        let operation = query(&filter).unwrap();
        operation.validate_for_table(&chunks()).unwrap();
        let Operation::Collect(p) = operation else {
            panic!()
        };
        assert_eq!(serde_json::to_value(&p.query.filter).unwrap(), filter);
        assert_eq!(
            crate::query_semantics::matches_filter(&p.query, &row),
            matches,
            "{filter}"
        );
    }
    for filter in [
        json!({"op":"contains","column":"acl"}),
        json!({"op":"contains","column":"acl","value":1}),
        json!({"op":"contains","column":"acl","value":null}),
        json!({"op":"contains","column":"acl","value":["g"]}),
        json!({"op":"contains","column":"acl","value":"g","x":1}),
        json!({"op":"contains","column":"acl","values":["g"]}),
        json!({"op":"contains","column":"bad-name","value":"g"}),
        json!({"op":"contains_any","column":"acl","value":"g"}),
        json!({"op":"contains_any","column":"acl","values":"g"}),
        json!({"op":"contains_any","column":"acl","values":[1]}),
        json!({"op":"starts_with","column":"body"}),
        json!({"op":"ends_with","column":"body","value":"x"}),
        json!({"op":"eq","column":"id","value":null}),
        json!({"op":"eq","column":"acl","value":["g"]}),
    ] {
        assert_eq!(code(query(&filter)), ErrorCode::ValidationError, "{filter}");
    }
    // Types are checked against the table.
    for filter in [
        json!({"op":"contains","column":"body","value":"g"}),
        json!({"op":"contains","column":"embedding","value":"g"}),
        json!({"op":"contains","column":"absent","value":"g"}),
        json!({"op":"contains_any","column":"id","values":["g"]}),
        json!({"op":"starts_with","column":"acl","value":"g"}),
        json!({"op":"starts_with","column":"year","value":"2"}),
        json!({"op":"eq","column":"acl","value":"g"}),
        json!({"op":"eq","column":"embedding","value":"g"}),
    ] {
        assert_eq!(
            code(query(&filter).unwrap().validate_for_table(&chunks())),
            ErrorCode::ValidationError,
            "{filter}"
        );
    }
    for column in ["acl", "embedding", "sparse", "tokens"] {
        let order = parse_op(
            "query.collect",
            json!({"table_id":ID,"query":{"select":null,"filter":null,"order_by":{"column":column,"desc":false},"limit":null}}),
        );
        assert!(
            order.unwrap().validate_for_table(&chunks()).is_err(),
            "{column}"
        );
    }
}

// ---- operations ----

#[test]
fn vs15_row_identities_depend_on_call_and_key_only() {
    let call: Uuid = ID.parse().unwrap();
    // The reference value, computed with another SHA-256 implementation.
    for (key, expected) in [
        (
            Key::Utf8("doc7#0".into()),
            "ce86273c-5e29-8399-98eb-f18f7c9ce73b",
        ),
        (Key::Int64(-7), "85853730-ce48-84e0-8cd2-086778568add"),
    ] {
        let id = row_request_id(call, &key).unwrap();
        assert_eq!(id.get_version_num(), 8);
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(id.to_string(), expected);
    }
    let id = |call: u128, key: Key| row_request_id(Uuid::from_u128(call), &key).unwrap();
    let mut seen = BTreeSet::new();
    for call in 0..20 {
        for key in 0..20 {
            assert!(seen.insert(id(call, Key::Int64(key))));
            assert!(seen.insert(id(call, Key::Utf8(key.to_string()))));
        }
    }
}

#[test]
fn vs15_a_row_runs_as_one_canonical_request() {
    let (call, table): (Uuid, Uuid) = (ID.parse().unwrap(), Uuid::from_u128(1));
    let spaced = r#"{ "v" : {"f32":"\u0041ACAPwAAAEA="}, "n": 1.50, "id": "doc7#0" }"#;
    let plain = r#"{"id":"doc7#0","n":1.5,"v":{"f32":"AACAPwAAAEA="}}"#;
    for row in [spaced, plain] {
        let request =
            upsert_row_request(call, table, "id", decode_upsert_row(row).unwrap()).unwrap();
        // This encoding is the digest of the row's dedup record and forward
        // marker, so it must never change.
        assert_eq!(
            serde_json::to_string(&request).unwrap(),
            r#"{"request_id":"ce86273c-5e29-8399-98eb-f18f7c9ce73b","operation":"table.upsert","payload":{"table_id":"00000000-0000-0000-0000-000000000001","row":{"id":"doc7#0","n":1.5,"v":{"f32":"AACAPwAAAEA="}}}}"#
        );
    }
    let request = delete_row_request(call, table, Key::Int64(-7)).unwrap();
    assert_eq!(
        serde_json::to_string(&request).unwrap(),
        r#"{"request_id":"85853730-ce48-84e0-8cd2-086778568add","operation":"table.delete","payload":{"table_id":"00000000-0000-0000-0000-000000000001","key":-7}}"#
    );
    for row in [
        r#"{"v":1}"#,
        r#"{"id":1.0}"#,
        r#"{"id":true}"#,
        r#"{"id":null}"#,
    ] {
        let row = decode_upsert_row(row).unwrap();
        assert!(upsert_row_request(call, table, "id", row).is_err());
    }
}

#[test]
fn many_operations_decode_strictly() {
    for (op, payload) in [
        ("table.upsert_many", json!({"table_id":ID,"rows":[]})),
        (
            "table.upsert_many",
            json!({"table_id":ID,"rows":[row(), {"id":"b"}]}),
        ),
        (
            "table.delete_many",
            json!({"table_id":ID,"keys":["a", 1, "1"]}),
        ),
        (
            "table.add_column",
            json!({"table_id":ID,"name":"v2","type":{"type":"vector","dim":3}}),
        ),
        (
            "table.add_column",
            json!({"table_id":ID,"name":"v2","type":{"type":"vector","dim":3},"embed":{"source":"body","model":"m"}}),
        ),
        (
            "table.drop_column",
            json!({"table_id":ID,"name":"embedding"}),
        ),
        (
            "table.add_full_text",
            json!({"table_id":ID,"column":"tenant","language":"english"}),
        ),
        (
            "table.drop_full_text",
            json!({"table_id":ID,"column":"body"}),
        ),
        ("table.search_status", json!({"table_id":ID})),
    ] {
        let operation = parse_op(op, payload.clone()).unwrap();
        assert_eq!(operation.name(), op);
        assert_eq!(
            serde_json::to_value(&operation).unwrap(),
            json!({"operation":op,"payload":payload})
        );
        assert_eq!(operation.is_mutation(), op != "table.search_status");
    }
    for (op, payload) in [
        ("table.upsert_many", json!({"table_id":ID})),
        ("table.upsert_many", json!({"table_id":ID,"rows":{}})),
        ("table.upsert_many", json!({"table_id":ID,"rows":[1]})),
        ("table.upsert_many", json!({"table_id":ID,"rows":[[]]})),
        ("table.upsert_many", json!({"table_id":ID,"rows":[],"x":1})),
        ("table.upsert_many", json!({"table_id":"x","rows":[]})),
        (
            "table.upsert_many",
            json!({"table_id":ID,"rows":[{"id":"a","v":{"f32":"AACA"}}]}),
        ),
        ("table.delete_many", json!({"table_id":ID})),
        ("table.delete_many", json!({"table_id":ID,"keys":[true]})),
        ("table.delete_many", json!({"table_id":ID,"keys":[null]})),
        ("table.delete_many", json!({"table_id":ID,"keys":[1.0]})),
        (
            "table.delete_many",
            json!({"table_id":ID,"keys":[],"rows":[]}),
        ),
        (
            "table.delete_many",
            json!({"table_id":ID,"keys":["a","b","a"]}),
        ),
        ("table.delete_many", json!({"table_id":ID,"keys":[1,1]})),
        ("table.add_column", json!({"table_id":ID,"name":"v2"})),
        (
            "table.add_column",
            json!({"table_id":ID,"name":"v2","type":"utf8"}),
        ),
        (
            "table.add_column",
            json!({"table_id":ID,"name":"v2","type":{"type":"list","item":"utf8"}}),
        ),
        (
            "table.add_column",
            json!({"table_id":ID,"name":"_score","type":{"type":"vector","dim":3}}),
        ),
        (
            "table.add_column",
            json!({"table_id":ID,"name":"2v","type":{"type":"vector","dim":3}}),
        ),
        (
            "table.add_column",
            json!({"table_id":ID,"name":"v2","type":{"type":"vector","dim":3},"embed":null}),
        ),
        (
            "table.add_column",
            json!({"table_id":ID,"name":"v2","type":{"type":"vector","dim":3},"embed":["body","m"]}),
        ),
        ("table.drop_column", json!({"table_id":ID,"column":"v"})),
        (
            "table.add_full_text",
            json!({"table_id":ID,"column":"body"}),
        ),
        (
            "table.add_full_text",
            json!({"table_id":ID,"column":"body","language":null}),
        ),
        (
            "table.add_full_text",
            json!({"table_id":ID,"column":"body","language":"EN"}),
        ),
        (
            "table.drop_full_text",
            json!({"table_id":ID,"column":"body","language":"english"}),
        ),
        ("table.search_status", json!({})),
        ("table.search_status", json!([ID])),
    ] {
        assert_eq!(
            code(parse_op(op, payload.clone())),
            ErrorCode::ValidationError,
            "{op} {payload}"
        );
    }
    assert_eq!(
        code(parse_op("query.rerank", json!({}))),
        ErrorCode::UnsupportedOperation
    );
}

#[test]
fn vs17_batches_stop_at_ten_thousand_rows() {
    let rows = vec![json!({"id":"a"}); MAX_BATCH_ROWS + 1];
    let keys: Vec<_> = (0..=MAX_BATCH_ROWS).collect();
    for op in ["table.upsert_many", "table.insert_many"] {
        assert!(parse_op(op, json!({"table_id":ID,"rows":rows[1..]})).is_ok());
        assert_eq!(
            code(parse_op(op, json!({"table_id":ID,"rows":rows}))),
            ErrorCode::LimitExceeded
        );
        let mut more = rows.clone();
        more.push(json!({"id":"a"}));
        assert!(parse_op(op, json!({"table_id":ID,"rows":more})).is_err());
    }
    assert!(parse_op("table.delete_many", json!({"table_id":ID,"keys":keys[1..]})).is_ok());
    assert_eq!(
        code(parse_op(
            "table.delete_many",
            json!({"table_id":ID,"keys":keys})
        )),
        ErrorCode::LimitExceeded
    );
    let body = vec![b' '; MAX_BODY_BYTES + 1];
    assert_eq!(code(parse_oltp_request(&body)), ErrorCode::LimitExceeded);
}

#[test]
fn column_operations_are_checked_against_the_table() {
    let check = |op, payload| parse_op(op, payload).unwrap().validate_for_table(&chunks());
    let vector = json!({"type":"vector","dim":3});
    for (op, payload) in [
        (
            "table.add_column",
            json!({"table_id":ID,"name":"v2","type":vector,"embed":{"source":"body","model":"m"}}),
        ),
        ("table.drop_column", json!({"table_id":ID,"name":"tokens"})),
        (
            "table.add_full_text",
            json!({"table_id":ID,"column":"tenant","language":"english"}),
        ),
        (
            "table.drop_full_text",
            json!({"table_id":ID,"column":"body"}),
        ),
        ("table.search_status", json!({"table_id":ID})),
        ("table.delete_many", json!({"table_id":ID,"keys":["a"]})),
    ] {
        check(op, payload).unwrap();
    }
    for (op, payload) in [
        (
            "table.add_column",
            json!({"table_id":ID,"name":"body","type":vector}),
        ),
        (
            "table.add_column",
            json!({"table_id":ID,"name":"v2","type":vector,"embed":{"source":"year","model":"m"}}),
        ),
        (
            "table.add_column",
            json!({"table_id":ID,"name":"v2","type":{"type":"vector","sparse":true},"embed":{"source":"body","model":"m"}}),
        ),
        ("table.drop_column", json!({"table_id":ID,"name":"body"})),
        ("table.drop_column", json!({"table_id":ID,"name":"absent"})),
        (
            "table.add_full_text",
            json!({"table_id":ID,"column":"body","language":"english"}),
        ),
        (
            "table.add_full_text",
            json!({"table_id":ID,"column":"year","language":"english"}),
        ),
        (
            "table.drop_full_text",
            json!({"table_id":ID,"column":"tenant"}),
        ),
        ("table.delete_many", json!({"table_id":ID,"keys":[1]})),
        (
            "table.search_status",
            json!({"table_id":"00000000-0000-0000-0000-000000000001"}),
        ),
    ] {
        assert_eq!(
            code(check(op, payload.clone())),
            ErrorCode::ValidationError,
            "{op} {payload}"
        );
    }
}

fn rank(change: impl FnOnce(&mut serde_json::Value)) -> serde_json::Value {
    let mut payload = json!({
        "table_id": ID,
        "query": {"select":["id"],"filter":{"op":"contains","column":"acl","value":"g"},"limit":10},
        "rank": {
            "rankings": [
                {"column":"embedding","op":"cosine","value":{"f32":floats(&[1.0, 0.0])}},
                {"column":"body","op":"match","value":"reset password"},
                {"column":"sparse","op":"dot","value":{"sparse":{"i":"","f32":""}}},
            ],
            "fusion":"rrf","candidates":100,"recall":0.95,"exact":false,"consistency":"latest",
            "rerank":{"ranking":{"column":"tokens","op":"cosine","value":{"f32s":floats(&[1.0, 0.0]),"n":1}},"candidates":200},
        },
    });
    change(&mut payload);
    payload
}

#[test]
fn rank_decodes_round_trips_and_defaults() {
    for payload in [
        rank(|_| ()),
        rank(|p| p["rank"]["fusion"] = json!([0.5, 1.0, 0.0])),
        rank(|p| p["rank"]["rerank"] = json!(null)),
        rank(|p| p["rank"]["consistency"] = json!("snapshot")),
        rank(|p| p["query"] = json!({"select":null,"filter":null,"limit":1000})),
        rank(|p| p["rank"]["rankings"][0] = json!({"column":"auto","op":"l2","value":"text"})),
        rank(|p| p["rank"]["rankings"][0]["op"] = json!("l2")),
        rank(|p| {
            p["rank"]["rankings"][0] =
                json!({"column":"tokens","op":"dot","value":{"f32s":floats(&[0.0, 0.0]),"n":1}})
        }),
    ] {
        let operation = parse_op("query.rank", payload.clone()).unwrap();
        operation.validate_for_table(&chunks()).unwrap();
        assert_eq!(
            serde_json::to_value(&operation).unwrap(),
            json!({"operation":"query.rank","payload":payload})
        );
        assert!(!operation.is_mutation());
    }
    let sparse = rank(|p| {
        p["rank"] = json!({"rankings": p["rank"]["rankings"], "rerank": {"ranking": p["rank"]["rerank"]["ranking"]}});
    });
    let Operation::Rank(p) = parse_op("query.rank", sparse).unwrap() else {
        panic!()
    };
    let Operation::Rank(full) = parse_op("query.rank", rank(|_| ())).unwrap() else {
        panic!()
    };
    assert_eq!(p, full, "the contract's defaults");

    let many = rank(|p| {
        p["batch"] = p["rank"]["rankings"].clone();
        p["rank"]["rankings"] = json!([]);
    });
    let operation = parse_op("query.rank_many", many.clone()).unwrap();
    operation.validate_for_table(&chunks()).unwrap();
    assert_eq!(
        serde_json::to_value(&operation).unwrap(),
        json!({"operation":"query.rank_many","payload":many})
    );
}

#[test]
fn vs17_rank_rejects_every_malformed_form() {
    let ranking = |value| json!({"column":"embedding","op":"dot","value":value});
    let cases: Vec<(&str, serde_json::Value)> = vec![
        ("unknown", rank(|p| p["x"] = json!(1))),
        ("unknown", rank(|p| p["query"]["x"] = json!(1))),
        ("unknown", rank(|p| p["query"]["order_by"] = json!(null))),
        ("unknown", rank(|p| p["rank"]["x"] = json!(1))),
        (
            "unknown",
            rank(|p| p["rank"]["rankings"][0]["x"] = json!(1)),
        ),
        ("unknown", rank(|p| p["rank"]["rerank"]["x"] = json!(1))),
        (
            "positional",
            rank(|p| p["rank"]["rankings"][0] = json!(["embedding", "dot", "x"])),
        ),
        ("positional", rank(|p| p["rank"] = json!([]))),
        ("positional", rank(|p| p["query"] = json!([null, null, 1]))),
        ("limit", rank(|p| p["query"]["limit"] = json!(null))),
        ("limit", rank(|p| p["query"]["limit"] = json!(-1))),
        ("limit", rank(|p| p["query"]["limit"] = json!(1.0))),
        ("select", rank(|p| p["query"]["select"] = json!([]))),
        (
            "select",
            rank(|p| p["query"]["select"] = json!(["id", "id"])),
        ),
        ("empty", rank(|p| p["rank"]["rankings"] = json!([]))),
        (
            "many",
            rank(|p| p["rank"]["rankings"] = json!(vec![ranking(json!("x")); 257])),
        ),
        (
            "op",
            rank(|p| p["rank"]["rankings"][0]["op"] = json!("euclidean")),
        ),
        (
            "op",
            rank(|p| p["rank"]["rankings"][0]["op"] = json!({"dot":null})),
        ),
        (
            "value",
            rank(|p| p["rank"]["rankings"][0]["value"] = json!(null)),
        ),
        (
            "value",
            rank(|p| p["rank"]["rankings"][0]["value"] = json!(1)),
        ),
        (
            "value",
            rank(|p| p["rank"]["rankings"][0]["value"] = json!([1.0])),
        ),
        (
            "value",
            rank(|p| p["rank"]["rankings"][0]["value"] = json!({"f32":"AACA"})),
        ),
        (
            "match vector",
            rank(|p| p["rank"]["rankings"][0]["op"] = json!("match")),
        ),
        (
            "zero",
            rank(|p| p["rank"]["rankings"][0]["value"] = json!({"f32":floats(&[0.0, -0.0])})),
        ),
        (
            "zero",
            rank(|p| p["rank"]["rankings"][2]["op"] = json!("cosine")),
        ),
        (
            "zero",
            rank(|p| {
                p["rank"]["rerank"]["ranking"]["value"] =
                    json!({"f32s":floats(&[1.0, 0.0, 0.0, 0.0]),"n":2})
            }),
        ),
        ("fusion", rank(|p| p["rank"]["fusion"] = json!("sum"))),
        ("fusion", rank(|p| p["rank"]["fusion"] = json!(null))),
        ("fusion", rank(|p| p["rank"]["fusion"] = json!([1.0]))),
        (
            "fusion",
            rank(|p| p["rank"]["fusion"] = json!([1.0, 1.0, -1.0])),
        ),
        (
            "fusion",
            rank(|p| p["rank"]["fusion"] = json!(["1", "1", "1"])),
        ),
        ("candidates", rank(|p| p["rank"]["candidates"] = json!(0))),
        (
            "candidates",
            rank(|p| p["rank"]["candidates"] = json!(4_294_967_296_u64)),
        ),
        (
            "candidates",
            rank(|p| p["rank"]["candidates"] = json!(null)),
        ),
        (
            "candidates",
            rank(|p| p["rank"]["rerank"]["candidates"] = json!(0)),
        ),
        ("recall", rank(|p| p["rank"]["recall"] = json!(0.0))),
        ("recall", rank(|p| p["rank"]["recall"] = json!(1.01))),
        ("recall", rank(|p| p["rank"]["recall"] = json!("0.9"))),
        ("exact", rank(|p| p["rank"]["exact"] = json!(1))),
        (
            "consistency",
            rank(|p| p["rank"]["consistency"] = json!("eventual")),
        ),
        (
            "rerank",
            rank(|p| p["rank"]["rerank"] = json!({"candidates":1})),
        ),
        ("batch", rank(|p| p["batch"] = json!([]))),
    ];
    for (why, payload) in cases {
        assert_eq!(
            code(parse_op("query.rank", payload.clone())),
            if why == "many" {
                ErrorCode::LimitExceeded
            } else {
                ErrorCode::ValidationError
            },
            "{why} {payload}"
        );
    }
    assert_eq!(
        code(parse_op(
            "query.rank",
            rank(|p| p["query"]["limit"] = json!(1001))
        )),
        ErrorCode::LimitExceeded
    );
    let many = |rankings, n: usize| {
        rank(|p| {
            p["rank"]["rankings"] = rankings;
            p["batch"] = json!(vec![ranking(json!("x")); n]);
        })
    };
    assert!(parse_op("query.rank_many", many(json!([]), 256)).is_ok());
    assert_eq!(
        code(parse_op("query.rank_many", many(json!([]), 257))),
        ErrorCode::LimitExceeded
    );
    for payload in [
        many(json!([]), 0),
        many(json!([ranking(json!("x"))]), 1),
        rank(|_| ()),
    ] {
        assert_eq!(
            code(parse_op("query.rank_many", payload)),
            ErrorCode::ValidationError
        );
    }
}

#[test]
fn rank_is_checked_against_the_table() {
    let check = |column: &str, op: &str, value: serde_json::Value| {
        parse_op(
            "query.rank",
            rank(|p| p["rank"]["rankings"] = json!([{"column":column,"op":op,"value":value}])),
        )
        .unwrap()
        .validate_for_table(&chunks())
    };
    let dense = json!({"f32":floats(&[1.0, 2.0])});
    for (column, op, value) in [
        ("embedding", "dot", json!({"f32":floats(&[1.0])})),
        (
            "embedding",
            "dot",
            json!({"f32s":floats(&[1.0, 2.0]),"n":1}),
        ),
        ("embedding", "dot", json!("text")), // no model
        ("embedding", "match", json!("text")),
        ("sparse", "l2", json!({"sparse":{"i":"","f32":""}})),
        ("sparse", "dot", dense.clone()),
        ("tokens", "l2", json!({"f32s":floats(&[1.0, 2.0]),"n":1})),
        ("tokens", "dot", dense.clone()),
        ("tenant", "match", json!("x")), // no full text
        ("body", "cosine", json!("x")),
        ("year", "dot", dense.clone()),
        ("acl", "match", json!("x")),
        ("absent", "dot", dense),
    ] {
        assert_eq!(
            code(check(column, op, value)),
            ErrorCode::ValidationError,
            "{column} {op}"
        );
    }
}

#[test]
fn row_outcomes_have_the_wire_form_of_the_contract() {
    let error = |outcome| ProtocolError::new(ErrorCode::Conflict, "m", outcome);
    let outcomes = RowOutcomes {
        outcomes: vec![
            RowOutcome::Applied,
            error(Outcome::NotApplied).into(),
            error(Outcome::Unknown).into(),
        ],
    };
    let text = r#"{"outcomes":[{"o":"applied"},{"o":"not_applied","error":{"code":"conflict","message":"m","outcome":"not_applied"}},{"o":"outcome_unknown","error":{"code":"conflict","message":"m","outcome":"unknown"}}]}"#;
    assert_eq!(serde_json::to_string(&outcomes).unwrap(), text);
    assert_eq!(serde_json::from_str::<RowOutcomes>(text).unwrap(), outcomes);
}

#[test]
fn vs17_arbitrary_bytes_never_panic() {
    let mut rng = Rng(99);
    let seeds: Vec<Vec<u8>> = [
        ("query.rank", rank(|_| ())),
        ("table.upsert_many", json!({"table_id":ID,"rows":[row()]})),
        ("table.create", serde_json::to_value(chunks()).unwrap()),
    ]
    .into_iter()
    .map(|(op, payload)| {
        serde_json::to_vec(&json!({"request_id":ID,"operation":op,"payload":payload})).unwrap()
    })
    .collect();
    let mut accepted = 0;
    for round in 0..30_000 {
        let mut bytes = seeds[round % seeds.len()].clone();
        for _ in 0..rng.below(3) {
            let at = rng.below(bytes.len());
            match rng.below(3) {
                0 => bytes[at] = rng.next() as u8,
                1 => {
                    bytes.remove(at);
                }
                _ => bytes.insert(at, b"{}[]\",:0n="[rng.below(10)]),
            }
        }
        if let Ok(request) = parse_oltp_request(&bytes) {
            accepted += 1;
            // Whatever decodes encodes to a request that decodes to the same.
            let again = serde_json::to_vec(&request).unwrap();
            assert_eq!(parse_oltp_request(&again).unwrap(), request);
            let _ = request.operation.validate_for_table(&chunks());
        }
    }
    assert!(accepted > 1_000);
}

/// contract-w2 4.2: the `served` table refuses every search operation until its
/// owner flips the operation's line, on the direct and the queued path, with the
/// cluster feature on or off.
#[test]
fn vs00_check_served_refuses_every_search_operation_until_its_owner_flips_it() {
    let many = rank(|p| {
        p["batch"] = p["rank"]["rankings"].clone();
        p["rank"]["rankings"] = json!([]);
    });
    for (op, payload) in [
        (
            "table.add_column",
            json!({"table_id":ID,"name":"v2","type":{"type":"vector","dim":3}}),
        ),
        (
            "table.drop_column",
            json!({"table_id":ID,"name":"embedding"}),
        ),
        (
            "table.add_full_text",
            json!({"table_id":ID,"column":"tenant","language":"english"}),
        ),
        (
            "table.drop_full_text",
            json!({"table_id":ID,"column":"body"}),
        ),
        ("table.search_status", json!({"table_id":ID})),
        ("query.rank", rank(|_| ())),
        ("query.rank_many", many.clone()),
    ] {
        let operation = parse_op(op, payload).unwrap();
        // J flipped the column operations and `table.search_status`.
        let served = op.starts_with("table.");
        for vector in [true, false] {
            for result in [
                operation.check_served(vector),
                operation.check_queued(vector),
            ] {
                if served {
                    assert!(result.is_ok(), "{op}: {result:?}");
                    continue;
                }
                let error = result.unwrap_err();
                assert_eq!(error.code, ErrorCode::UnsupportedOperation, "{op}");
                assert_eq!(error.message, "the operation is not supported yet", "{op}");
            }
        }
    }
}
