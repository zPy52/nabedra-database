//! VEC-G10: vector values next to their rows (`specs/vector/blobs`).
use super::*;
use crate::oltp::mvcc::{
    store::row::{self, Columns, STAGED, VECTOR, unreferenced, unreferenced_in},
    vector::MAX_VECTOR_BYTES,
};

fn row(key: i64, body: &str) -> serde_json::Value {
    serde_json::json!({"id": key, "body": body})
}

#[tokio::test]
async fn vs16_a_row_and_its_vectors_are_one_entry_and_every_byte_survives() {
    let mut s = S::new().await;
    let sparse: Vec<u8> = (0..48).collect();
    let vectors = vec![
        s.set(PLAIN, s.dense(7)),
        s.set(SPARSE, s.value(VectorKind::Sparse, &sparse)),
        s.set(MULTI, s.value(VectorKind::Multi, &[9; 12])),
    ];
    let write = s.write(1, None, Some(serde_json::json!({"id": 1})), vectors);
    let before = s.last();
    // As a follower decodes the entry from its log.
    let commit = ParticipantCommand::Direct(s.commit(10, vec![write]));
    let entry = OwnedCommand::new_owned(commit, s.f.shared(), ByteClass::Data).unwrap();
    let encoded = s.f.shared().encode(&entry, 1 << 20).unwrap();
    let entry = OwnedCommand::decode_json(&encoded, s.f.shared(), ByteClass::Data, 1 << 20).unwrap();
    assert_eq!(s.f.replica.apply_owned(entry).await, applied(10));
    assert_eq!(s.last(), before + 1, "one entry");
    let row = s.get(1).await;
    assert_eq!(
        row.vectors,
        BTreeMap::from([
            (PLAIN, (10, vec![7; 8])),
            (SPARSE, (10, sparse)),
            (MULTI, (10, vec![9; 12])),
        ])
    );
    // `kind u8 ‖ CRC32C u32 ‖ bytes` under `'x' ‖ pk ‖ column ‖ commit_ts`.
    let stored = s.records(VECTOR).await;
    assert_eq!(stored.len(), 3);
    let logical = row::logical_key(&s.f.key(1)).unwrap();
    let key = [&[b'x'][..], &logical, &1u16.to_be_bytes(), &10u64.to_be_bytes()].concat();
    let mut frame = vec![1];
    frame.extend_from_slice(
        &nabedra_kv::crc::crc32c(&[&[1u8][..], &[7; 8]].concat()).to_le_bytes(),
    );
    frame.extend_from_slice(&[7; 8]);
    assert_eq!(stored[0], (key, frame));
}

#[tokio::test]
async fn vs16_a_metadata_update_references_the_vector_and_does_not_rewrite_it() {
    let mut s = S::new().await;
    let first = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, first).await;
    let stored = s.records(VECTOR).await;
    // A direct update, and a queued one.
    let update = s.write(1, Some(10), Some(row(1, "b")), Vec::new());
    assert_eq!(s.direct(11, update).await, applied(11));
    let writes = vec![s.f.queued_put(1, Some(11), row(1, "c"))];
    let plan = s.f.queued_plan(20, &writes);
    s.f.prepare(&plan, writes).await;
    s.f.seal_and_resolve(&plan, 21).await;
    assert_eq!(s.records(VECTOR).await, stored);
    for (read, version) in [(10, 10), (15, 11), (30, 21)] {
        let row = s.at(1, read).await;
        assert_eq!(row.version, Some(version));
        assert_eq!(row.vectors[&PLAIN], (10, vec![1; 8]));
    }
    // A new vector of one column leaves the others' references alone.
    let next = s.write(1, Some(21), Some(row(1, "c")), vec![s.set(MULTI, s.value(VectorKind::Multi, &[3; 4]))]);
    assert_eq!(s.direct(30, next).await, applied(30));
    let row_now = s.get(1).await;
    assert_eq!(row_now.vectors[&PLAIN], (10, vec![1; 8]));
    assert_eq!(row_now.vectors[&MULTI], (30, vec![3; 4]));
    // Null clears the reference, and the old versions keep theirs.
    let clear = VectorChange {
        column: PLAIN,
        value: None,
    };
    let write = s.write(1, Some(30), Some(row(1, "c")), vec![clear]);
    assert_eq!(s.direct(31, write).await, applied(31));
    assert!(!s.get(1).await.vectors.contains_key(&PLAIN));
    assert_eq!(s.at(1, 30).await.vectors[&PLAIN], (10, vec![1; 8]));
}

/// Mutant `no_expected_version`: an update commits against the version it read.
#[tokio::test]
async fn vs16_an_update_of_an_older_version_never_brings_its_vector_back() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    // Reads version 10, and another writer replaces the vector meanwhile.
    let late = s.write(1, Some(10), Some(row(1, "late")), Vec::new());
    let write = s.write(1, Some(10), Some(row(1, "b")), vec![s.set(PLAIN, s.dense(2))]);
    assert_eq!(s.direct(11, write).await, applied(11));
    let before = s.f.dump().await;
    assert_eq!(s.direct(12, late).await, rejected(Fault::Conflict));
    assert_eq!(s.f.dump().await, before);
    let mut queued = vec![s.f.queued_put(1, Some(10), row(1, "late"))];
    let plan = s.f.queued_plan(12, &queued);
    assert_eq!(
        s.f.prepare(&plan, std::mem::take(&mut queued)).await,
        ParticipantReply::Rejected(Fault::Conflict)
    );
    // Delete and insert again: nothing of the old row returns.
    let delete = s.write(1, Some(11), None, Vec::new());
    assert_eq!(s.direct(13, delete).await, applied(13));
    let late = s.write(1, Some(11), Some(row(1, "late")), Vec::new());
    assert_eq!(s.direct(14, late).await, rejected(Fault::Conflict));
    let insert = s.write(1, Some(13), Some(row(1, "new")), Vec::new());
    assert_eq!(s.direct(15, insert).await, applied(15));
    assert!(s.get(1).await.vectors.is_empty());
}

#[tokio::test]
async fn vs16_batch_members_store_vectors_and_a_rejected_member_stores_nothing() {
    let mut s = S::new().await;
    let put = |s: &S, key, at, n| {
        let write = s.write(key, None, Some(row(key, "a")), vec![s.set(PLAIN, s.dense(n))]);
        s.commit(at, vec![write])
    };
    let members = vec![put(&s, 1, 10, 1), put(&s, 2, 5, 2), put(&s, 1, 11, 3), put(&s, 3, 12, 4)];
    s.close(8).await;
    let reply = s
        .f
        .submit(ParticipantCommand::DirectBatch(DirectBatch { members }))
        .await;
    let applied = |at| DirectMemberReply::Applied { commit_ts: ts(at) };
    assert_eq!(
        reply,
        ParticipantReply::Direct(DirectReply::Batch(vec![
            applied(10),
            // Below the closed floor, then a key an earlier member wrote.
            DirectMemberReply::Rejected(Fault::Conflict),
            DirectMemberReply::Rejected(Fault::Conflict),
            applied(12),
        ]))
    );
    assert_eq!(s.count(VECTOR).await, 2);
    assert_eq!(s.get(1).await.vectors[&PLAIN], (10, vec![1; 8]));
    assert_eq!(s.get(3).await.vectors[&PLAIN], (12, vec![4; 8]));
    assert_eq!(s.get(2).await, Got::default());
}

#[tokio::test]
async fn vs16_a_transaction_stages_its_vectors_and_resolve_gives_them_the_commit_timestamp() {
    let mut s = S::new().await;
    let old = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, old).await;
    let writes = |s: &S| {
        vec![
            s.put_row(1, Some(10), row(1, "b"), vec![s.set(PLAIN, s.dense(2))]),
            s.put_row(2, None, row(2, "c"), vec![s.set(SPARSE, s.value(VectorKind::Sparse, &[5; 16]))]),
        ]
    };
    for commit in [false, true] {
        let prepared = writes(&s);
        let plan = s.f.queued_plan(20, &prepared);
        assert!(matches!(
            s.f.prepare(&plan, prepared).await,
            ParticipantReply::PreparedPage { writes: 2, .. }
        ));
        assert_eq!(s.count(VECTOR).await, 1, "no version before the decision");
        assert_eq!(s.count(STAGED).await, 4, "two instructions, two vectors");
        assert_eq!(s.at(1, 20).await.vectors[&PLAIN], (10, vec![1; 8]));
        if commit {
            s.f.seal_and_resolve(&plan, 21).await;
        } else {
            s.abort(&plan).await;
        }
        assert_eq!(s.count(STAGED).await, 0);
    }
    assert_eq!(s.get(1).await.vectors[&PLAIN], (21, vec![2; 8]));
    assert_eq!(s.at(1, 20).await.vectors[&PLAIN], (10, vec![1; 8]));
    assert_eq!(s.get(2).await.vectors[&SPARSE], (21, vec![5; 16]));
    assert_eq!(s.get(2).await.records[&EMBEDDED], EmbedRecord::fresh(ts(21)));
}

/// Mutants `gc_ignores_reference` and `gc_takes_staged`.
#[tokio::test]
async fn vs16_collection_finds_only_unreferenced_vectors_and_never_a_staged_one() {
    let mut s = S::new().await;
    // Vector 10 stays referenced through two updates, 13 replaces it.
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    for (expected, at) in [(10, 11), (11, 12)] {
        let write = s.write(1, Some(expected), Some(row(1, "b")), Vec::new());
        s.direct(at, write).await;
    }
    let write = s.write(1, Some(12), Some(row(1, "c")), vec![s.set(PLAIN, s.dense(2))]);
    s.direct(13, write).await;
    let staged = vec![s.put_row(1, Some(13), row(1, "d"), vec![s.set(PLAIN, s.dense(3))])];
    let plan = s.f.queued_plan(20, &staged);
    s.f.prepare(&plan, staged).await;
    let find = |s: &S| {
        let key = s.f.key(1);
        let store = s.f.machine().store().clone();
        async move {
            let raw = store
                .read(BUDGET, move |view| {
                    let found = unreferenced_in(view, &key)?;
                    view.encode(&found)
                })
                .await
                .unwrap();
            serde_json::from_slice::<Vec<(ColumnId, u64)>>(raw.as_slice()).unwrap()
        }
    };
    assert_eq!(find(&s).await, []);
    // The versions that a newer one at or below a safe point of 12 supersedes.
    let drop_version = |at: u64| {
        let key = [&[VALUE][..], &row::logical_key(&s.f.key(1)).unwrap(), &(!at).to_be_bytes()].concat();
        move |tx: &mut GroupTransaction<'_>| tx.delete(&key)
    };
    let (ten, eleven, twelve) = (drop_version(10), drop_version(11), drop_version(12));
    s.tamper(ten).await;
    s.tamper(eleven).await;
    assert_eq!(find(&s).await, [], "version 12 still uses vector 10");
    s.tamper(twelve).await;
    assert_eq!(find(&s).await, [(PLAIN, 10)]);
    s.f.seal_and_resolve(&plan, 21).await;
    assert_eq!(s.get(1).await.vectors[&PLAIN], (21, vec![3; 8]));
    assert_eq!(find(&s).await, [(PLAIN, 10)]);
}

/// The rule against the spec's `Referenced`, over random versions.
#[test]
fn vs16_the_reference_rule_matches_its_definition() {
    for seed in 0..500 {
        let mut rng = Rng(seed);
        let mut versions = Vec::new();
        for _ in 0..rng.below(6) {
            let blobs = (0..rng.below(4))
                .map(|_| (rng.below(3) as ColumnId, ts(1 + rng.next() % 6)))
                .collect();
            versions.push(Columns(blobs));
        }
        let vectors: Vec<_> = (0..3)
            .flat_map(|column| (1..7).map(move |at| (column, ts(at))))
            .collect();
        let expected: Vec<_> = vectors
            .iter()
            .copied()
            .filter(|(column, at)| !versions.iter().any(|v| v.get(*column) == Some(*at)))
            .collect();
        assert_eq!(unreferenced(&versions, vectors), expected, "seed {seed}");
    }
}

#[tokio::test]
async fn vs17_a_write_that_names_wrong_vectors_is_refused_and_writes_nothing() {
    let mut s = S::new().await;
    let dense = s.dense(1);
    let short = s.value(VectorKind::Dense, &[1; 4]);
    // Base64 of the right length, with one character that is not.
    let garbage: VectorValue =
        serde_json::from_value(serde_json::json!({"kind": "dense", "data": "AQEBAQE!AQE="})).unwrap();
    let owner = s.f.shared().reserve_scratch(ByteClass::Data, 64 << 10).unwrap();
    garbage.attach_charge(Arc::new(owner)).unwrap();
    let bad: Vec<Vec<VectorChange>> = vec![
        vec![s.set(99, dense.clone())],
        vec![s.set(EMBEDDED, dense.clone())],
        vec![s.set(PLAIN, short)],
        vec![s.set(PLAIN, garbage)],
        vec![s.set(PLAIN, s.value(VectorKind::Multi, &[1; 8]))],
        vec![s.set(MULTI, s.value(VectorKind::Multi, &[1; 4])), s.set(PLAIN, dense.clone())],
        vec![s.set(PLAIN, dense.clone()), s.set(PLAIN, dense.clone())],
        vec![VectorChange { column: 99, value: None }],
    ];
    for (n, vectors) in bad.into_iter().enumerate() {
        let before = s.f.dump().await;
        let write = s.write(1, None, Some(row(1, "a")), vectors.clone());
        assert_eq!(s.direct(10, write).await, rejected(Fault::InvalidBinding), "case {n}");
        let prepared = vec![s.put_row(1, None, row(1, "a"), vectors)];
        let plan = s.f.queued_plan(20, &prepared);
        assert_eq!(
            s.f.prepare(&plan, prepared).await,
            ParticipantReply::Rejected(Fault::InvalidBinding),
            "case {n}"
        );
        assert_eq!(s.f.dump().await, before, "case {n}");
    }
    // Backfill names an embedded column, a delete names none.
    let mut write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, dense.clone())]);
    write.search.as_mut().unwrap().backfill = vec![PLAIN];
    assert_eq!(s.direct(10, write).await, rejected(Fault::InvalidBinding));
    let delete = s.write(1, None, None, vec![s.set(PLAIN, dense.clone())]);
    assert_eq!(s.direct(10, delete).await, rejected(Fault::InvalidBinding));
    // A table without search columns takes no vector.
    let mut plain = S::plain(Some(&FaultFs::new())).await;
    let write = plain.write(1, None, Some(row(1, "a")), vec![plain.set(PLAIN, dense)]);
    assert_eq!(plain.direct(10, write).await, rejected(Fault::InvalidBinding));
}

#[tokio::test]
async fn vs17_an_entry_over_the_native_batch_answers_resource_limit() {
    let mut s = S::on(None).await;
    let big = s.value(VectorKind::Sparse, &vec![7; MAX_VECTOR_BYTES]);
    let writes: Vec<_> = (0..33)
        .map(|key| s.write(key, None, Some(row(key, "a")), vec![s.set(SPARSE, big.clone())]))
        .collect();
    let commit = s.commit(10, writes);
    let before = s.f.dump().await;
    assert_eq!(s.f.direct(&commit).await, rejected(Fault::ResourceLimit));
    assert_eq!(s.f.dump().await, before);
    let commit = s.commit(10, commit.writes[..4].to_vec());
    assert_eq!(s.f.direct(&commit).await, applied(10));
    assert_eq!(s.get(3).await.vectors[&SPARSE].1.len(), MAX_VECTOR_BYTES);
}

/// A damaged or missing vector is an explicit error for every reader, never
/// null, and the group follows the damage rules: it never writes again.
#[tokio::test]
async fn vs16_a_damaged_vector_is_never_null_and_quarantines_its_group() {
    for missing in [false, true] {
        let mut s = S::new().await;
        let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
        s.direct(10, write).await;
        let after = s.last() - 1;
        let (key, mut frame) = s.records(VECTOR).await.remove(0);
        frame[7] ^= 4;
        s.tamper(move |tx| if missing { tx.delete(&key) } else { tx.put(&key, &frame) })
            .await;
        let mut damage = s.f.pool.watch_damage();
        let corrupt = |error: ProtocolError| {
            assert!(error.message.starts_with("storage corrupt"), "{}", error.message);
        };
        // Images (a split or a backup) and point reads fail.
        let request = ImageRequestOf(&s).request();
        corrupt(s.f.machine().committed_image_page(request).await.err().unwrap());
        corrupt(got(s.f.machine(), s.f.key(1), 10).await.err().unwrap());
        // The feed fails for a damaged and for a missing value alike.
        corrupt(s.f.machine().change_page(after, 10, 1 << 20).await.err().unwrap());
        let group = s.f.group.group_id;
        assert!(damage.borrow_and_update().contains(&group));
        assert!(s.f.pool.is_group_quarantined(group));
        assert!(s.f.pool.group_damage(group).is_some());
        let commit = s.commit(11, vec![s.write(2, None, Some(row(2, "b")), Vec::new())]);
        let command =
            OwnedCommand::new_owned(ParticipantCommand::Direct(commit), s.f.shared(), ByteClass::Data)
                .unwrap();
        let refused = s
            .f
            .machine()
            .apply(
                ApplyMeta {
                    position: position(s.f.replica.index),
                    command_digest: digest(1),
                    membership: None,
                },
                command,
            )
            .await;
        assert!(refused.err().unwrap().message.contains("quarantined"));
    }
}
struct ImageRequestOf<'a>(&'a S);
impl ImageRequestOf<'_> {
    fn request(&self) -> OwnedCommand<image_archive::ImageRequest> {
        OwnedCommand::new_owned(
            image_archive::ImageRequest {
                read_ts: ts(10),
                schema_epoch: 1,
                routing_epoch: 1,
                span: full_span(),
                after: None,
                max_items: 128,
                max_bytes: 1 << 20,
                search: None,
            },
            self.0.f.shared(),
            ByteClass::Data,
        )
        .unwrap()
    }
}

/// Random direct, batched and queued histories of three rows against a model
/// that keeps every version whole. Every version reads back byte for byte.
#[tokio::test]
async fn vs16_random_histories_read_back_every_version_byte_for_byte() {
    type Model = BTreeMap<ColumnId, (u64, Vec<u8>)>;
    for seed in 0..24 {
        let mut rng = Rng(seed);
        let mut s = S::new().await;
        // Per key: every version with its vectors, `None` for a delete.
        let mut model: BTreeMap<i64, Vec<(u64, Option<Model>)>> = BTreeMap::new();
        for step in 0..30 {
            let key = rng.below(3) as i64;
            let versions = model.entry(key).or_default();
            let head = versions.last().map(|(at, _)| *at);
            let live = versions.last().and_then(|(_, vectors)| vectors.clone());
            let at = s.tick() + 100;
            if live.is_some() && rng.one_in(6) {
                let write = s.write(key, head, None, Vec::new());
                assert_eq!(s.direct(at, write).await, applied(at), "seed {seed} step {step}");
                versions.push((at, None));
                continue;
            }
            let mut next = live.unwrap_or_default();
            let mut named = Vec::new();
            for (column, kind, len) in [
                (PLAIN, VectorKind::Dense, 8),
                (SPARSE, VectorKind::Sparse, 8 * rng.below(40)),
                (MULTI, VectorKind::Multi, 4 * rng.below(5)),
            ] {
                match rng.below(4) {
                    0 => {
                        next.remove(&column);
                        named.push(VectorChange { column, value: None });
                    }
                    1 => {
                        let bytes = rng.bytes(len);
                        named.push(s.set(column, s.value(kind, &bytes)));
                        next.insert(column, (at, bytes));
                    }
                    _ => {}
                }
            }
            let value = serde_json::json!({"id": key, "step": step});
            match rng.below(4) {
                0 => {
                    let prepared = vec![s.put_row(key, head, value, named)];
                    let plan = s.f.queued_plan(at - 1, &prepared);
                    if rng.one_in(3) {
                        s.f.prepare(&plan, prepared).await;
                        s.abort(&plan).await;
                        continue;
                    }
                    s.f.prepare(&plan, prepared).await;
                    s.f.seal_and_resolve(&plan, at).await;
                }
                1 => {
                    // A stale update beside it changes nothing.
                    let write = s.write(key, head, Some(value), named);
                    let other = s.write(9, Some(1), Some(serde_json::json!({"id": 9})), Vec::new());
                    let members = vec![s.commit(at, vec![write]), s.commit(at, vec![other])];
                    let reply = s
                        .f
                        .submit(ParticipantCommand::DirectBatch(DirectBatch { members }))
                        .await;
                    let ParticipantReply::Direct(DirectReply::Batch(replies)) = reply else {
                        panic!("{reply:?}");
                    };
                    assert!(matches!(replies[0], DirectMemberReply::Applied { .. }));
                    assert!(matches!(replies[1], DirectMemberReply::Rejected(Fault::Conflict)));
                }
                _ => {
                    let write = s.write(key, head, Some(value), named);
                    assert_eq!(s.direct(at, write).await, applied(at), "seed {seed} step {step}");
                }
            }
            versions.push((at, Some(next)));
        }
        for (key, versions) in &model {
            for (at, vectors) in versions {
                let row = s.at(*key, *at).await;
                assert_eq!(row.version, Some(*at), "seed {seed} key {key}");
                assert_eq!(row.vectors, vectors.clone().unwrap_or_default(), "seed {seed} key {key} at {at}");
            }
        }
    }
}

/// The stored and logged encodings of a table with search columns. Other builds
/// and replicas read them, so a change here is a format change.
#[tokio::test]
async fn vs20_search_encodings_are_stable_and_an_older_decoder_refuses_them() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    let entry = serde_json::to_value(&write).unwrap();
    assert_eq!(
        entry["search"],
        serde_json::json!({"vectors": [{"column": 1, "value": {"kind": "dense", "data": "AQEBAQEBAQE="}}]})
    );
    // An older build knows no `search`, in a write, a row change or a command.
    let plain = serde_json::to_value(s.f.write(1, None, None)).unwrap();
    assert!(plain.get("search").is_none());
    let prepared = s.put_row(1, None, row(1, "a"), vec![]);
    assert!(serde_json::to_value(&prepared).unwrap()["change"].get("put_row").is_some());
    let authority = s.f.authority;
    let (table_id, primary_key) = (s.f.table, Key::Int64(1));
    let commands = [
        (s.fence_command(1, columns()), "search_fence"),
        (
            ParticipantCommand::Closed(Closed { authority, ts: ts(1), unix_ms: 1 }),
            "closed",
        ),
        (ParticipantCommand::SetVector(s.set_vector(1, 1, 1, 2)), "set_vector"),
        (
            ParticipantCommand::EmbedFailed(EmbedFailed {
                authority,
                table_id,
                primary_key: primary_key.clone(),
                column: EMBEDDED,
                source_version: ts(1),
                id: ts(1),
                attempt: 0,
                reason: Text::try_from("no").unwrap(),
            }),
            "embed_failed",
        ),
        (
            ParticipantCommand::RequeueEmbedding(RequeueEmbedding {
                authority,
                table_id,
                primary_key,
                column: EMBEDDED,
                source_version: ts(1),
                id: ts(1),
            }),
            "requeue_embedding",
        ),
    ];
    for (command, tag) in commands {
        assert!(command.is_direct(), "{tag} is never nested");
        let encoded = serde_json::to_vec(&command).unwrap();
        let decoded: ParticipantCommand = serde_json::from_slice(&encoded).unwrap();
        assert!(serde_json::from_slice::<serde_json::Value>(&encoded).unwrap().get(tag).is_some());
        let owner = s.f.shared().reserve_scratch(ByteClass::Data, 64 << 10).unwrap();
        decoded.attach_fields(Arc::new(owner)).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), encoded);
    }
    s.direct(10, write).await;
    let logical = row::logical_key(&s.f.key(1)).unwrap();
    let dump: BTreeMap<_, _> = s.f.dump().await.into_iter().collect();
    let version = [&[VALUE][..], &logical, &(!10u64).to_be_bytes()].concat();
    assert_eq!(
        std::str::from_utf8(&dump[&version]).unwrap(),
        r#"{"value":{"body":"a","id":1},"blobs":{"1":10},"source":{"2":10}}"#
    );
    let record = [&b"e"[..], &logical, &2u16.to_be_bytes()].concat();
    assert_eq!(
        std::str::from_utf8(&dump[&record]).unwrap(),
        r#"{"source_version":10,"id":10,"attempts":0,"limit":10}"#
    );
    // A tombstone carries nothing, so it has the bytes of any other table's.
    let delete = s.write(1, Some(10), None, Vec::new());
    s.direct(11, delete).await;
    let dump: BTreeMap<_, _> = s.f.dump().await.into_iter().collect();
    let version = [&[VALUE][..], &logical, &(!11u64).to_be_bytes()].concat();
    assert_eq!(dump[&version], br#"{"value":null}"#);
}

/// A replica snapshot whose vector does not have its column's type is refused,
/// and the follower keeps what it had. A dropped column's vector installs, and
/// is not served.
#[tokio::test]
async fn vs16_a_snapshot_install_refuses_a_vector_that_does_not_fit_its_column() {
    for (fits, kind, bytes) in [
        (true, VectorKind::Dense, vec![9u8; 8]),
        (false, VectorKind::Sparse, vec![9u8; 8]),
        (false, VectorKind::Dense, vec![9u8; 12]),
    ] {
        let mut s = S::new().await;
        let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
        s.direct(10, write).await;
        let (key, _) = s.records(VECTOR).await.remove(0);
        let value = s.value(kind, &bytes);
        let mut frame = vec![0; 5 + bytes.len()];
        value.frame(&mut frame).unwrap();
        s.tamper(move |tx| tx.put(&key, &frame)).await;
        let snapshot = s.f.machine().store().export_snapshot(BUDGET).await.unwrap();
        let mut follower = s.f.replica_of_inactive().await;
        let before = follower.dump().await;
        let store = follower.machine.store().clone();
        let installed = store.install_snapshot(snapshot, BUDGET).await;
        if fits {
            installed.unwrap();
            follower.index = s.f.replica.index;
            assert_eq!(follower.dump().await, s.f.dump().await);
            continue;
        }
        let error = installed.err().unwrap();
        assert!(error.message.starts_with("storage corrupt"), "{}", error.message);
        assert_eq!(follower.dump().await, before);
        // A reader of the source refuses it as damage, too.
        let refused = got(s.f.machine(), s.f.key(1), 10).await.err().unwrap();
        assert!(refused.message.starts_with("storage corrupt"), "{}", refused.message);
    }
}

type Rows = (Vec<(Vec<u8>, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>)>);
async fn rows(s: &S) -> Rows {
    (s.records(VALUE).await, s.records(VECTOR).await, s.records(STAGED).await)
}
/// A queued row put of a search range names its vectors, like a direct write:
/// a plain `Put` (an older build's shape) is refused, on any page, and nothing
/// of its transaction lands.
#[tokio::test]
async fn vs16_a_queued_plain_put_on_a_search_range_fails_closed() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    let plain = |s: &S, key| PreparedWrite {
        change: Change::Put(s.f.json(row(key, "plain"))),
        ..s.f.queued_put(key, if key == 1 { Some(10) } else { None }, serde_json::Value::Null)
    };
    // One page.
    let before = rows(&s).await;
    let writes = vec![plain(&s, 1)];
    let plan = s.f.queued_plan(20, &writes);
    assert_eq!(s.f.prepare(&plan, writes).await, ParticipantReply::Rejected(Fault::InvalidBinding));
    let after = rows(&s).await;
    assert_eq!(after, before);
    // Two pages: the first names its vector, the second is a plain put.
    let pages = [
        vec![s.put_row(1, Some(10), row(1, "b"), vec![s.set(PLAIN, s.dense(2))])],
        vec![plain(&s, 2)],
    ];
    let txn = TxnId { request_id: nabedra_rt::ids::uuid_v4(), attempt: 0 };
    let mut plan = s.f.queued_plan(20, &pages[0]);
    plan.txn = txn;
    plan.request.request_id = txn.request_id;
    plan.plan.page_count = 2;
    plan.plan.write_count = 2;
    let roots = {
        let first = initial_root(txn, &plan.plan).unwrap();
        let second = page_root(first, 0, &pages[0]).unwrap();
        [first, second]
    };
    plan.plan.write_root = page_root(roots[1], 1, &pages[1]).unwrap();
    let mut replies = Vec::new();
    for (index, writes) in pages.into_iter().enumerate() {
        let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
            lifecycle::LifecycleBinding {
                guard_id: nabedra_rt::ids::uuid_v4(),
                catalog_group_id: s.f.catalog_group_id,
                table_id: s.f.table,
                lifecycle_version: None,
                schema_epoch: 1,
                txn,
                request: plan.request,
                read_ts: plan.plan.read_ts,
                authority: s.f.authority,
            },
            lifecycle::GuardMode::Shared,
        )
        .into_sealed();
        replies.push(
            s.f.submit(ParticipantCommand::PreparePage {
                authority: s.f.authority,
                txn,
                request: plan.request,
                manifest: plan.manifest,
                plan: plan.plan.clone(),
                page_index: index as u64,
                previous_root: roots[index],
                guards: Page::new(vec![guard]).unwrap(),
                writes: Page::new(writes).unwrap(),
            })
            .await,
        );
    }
    assert!(matches!(replies[0], ParticipantReply::PreparedPage { writes: 1, .. }), "{replies:?}");
    assert_eq!(replies[1], ParticipantReply::Rejected(Fault::InvalidBinding));
    s.abort(&plan).await;
    let after = rows(&s).await;
    assert_eq!(after, before, "the row, its vectors and nothing staged");
    assert_eq!(s.get(1).await.vectors[&PLAIN], (10, vec![1; 8]));
}

/// L6: a vector damage quarantine is durable. The running replica stops
/// appending, voting and serving at once; after a crash and a restart the pool
/// finds the group's record, reports it, and never opens it again.
#[tokio::test]
async fn vs16_a_vector_damage_quarantine_survives_a_restart() {
    let fs = FaultFs::new();
    let mut s = S::on(Some(&fs)).await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    let (key, mut frame) = s.records(VECTOR).await.remove(0);
    frame[6] ^= 1;
    s.tamper(move |tx| tx.put(&key, &frame)).await;
    let group = s.f.group.clone();
    let store = s.f.machine().store().clone();
    let error = got(s.f.machine(), s.f.key(1), 10).await.err().unwrap();
    assert!(error.message.starts_with("storage corrupt"), "{}", error.message);
    let quarantined = |error: ProtocolError| {
        assert!(error.message.contains("quarantined"), "{}", error.message);
    };
    // A vote or an append is a log write; an acknowledgment needs an apply.
    let vote = store
        .log_transaction(BUDGET, |tx| {
            tx.put(b"vote", b"term 2")?;
            Ok(Transition::Commit(tx.copy_output(b"")?))
        })
        .await;
    quarantined(vote.err().unwrap());
    let commit = s.commit(11, vec![s.write(2, None, Some(row(2, "b")), Vec::new())]);
    let entry =
        OwnedCommand::new_owned(ParticipantCommand::Direct(commit), s.f.shared(), ByteClass::Data).unwrap();
    let meta = ApplyMeta {
        position: position(s.f.replica.index),
        command_digest: digest(1),
        membership: None,
    };
    quarantined(s.f.machine().apply(meta, entry).await.err().unwrap());
    // Its host stops on the published damage (rule 1); the group is not served
    // again, since no restart opens it.
    assert!(s.f.pool.watch_damage().borrow().contains(&group.group_id));

    let S { f, .. } = s;
    let Fixture {
        replica,
        pool,
        pool_identity,
        dir,
        ..
    } = f;
    let shared = replica.shared.clone();
    drop((replica, store, pool));
    let fs = fs.crash();
    let pool = open_on(&dir, &fs, &pool_identity, &shared);
    assert!(pool.quarantined_groups().contains(&group.group_id));
    assert!(pool.is_group_quarantined(group.group_id));
    assert!(pool.group_damage(group.group_id).is_some(), "Control is told");
    quarantined(pool.retained_group(group.clone()).await.err().unwrap());
    quarantined(pool.group(group.clone()).await.err().unwrap());
    // Released after its replacement committed, the record forgets it.
    let removed = crate::oltp::pool::DamagedReplicaRemoved {
        group: group.clone(),
        removed: nabedra_oltp_types::types::ReplicaMember {
            node_id: 2,
            worker_id: pool_identity.worker_id,
            pool_id: pool_identity.pool_id,
        },
        committed_at: position(40),
    };
    pool.release_damaged_group(removed).await.unwrap();
    assert!(!pool.is_group_quarantined(group.group_id));
    drop(pool);
    let pool = open_on(&dir, &fs.crash(), &pool_identity, &shared);
    assert!(pool.quarantined_groups().is_empty());
    pool.retained_group(group).await.err().unwrap();
}


#[tokio::test]
async fn vs16_concurrent_value_damage_reports_survive_restart() {
    let fs = FaultFs::new();
    let S { f, .. } = S::on(Some(&fs)).await;
    let Fixture {
        replica,
        pool,
        pool_identity,
        dir,
        ..
    } = f;
    let shared = replica.shared.clone();
    let groups: Vec<_> = (1..=8).map(uuid::Uuid::from_u128).collect();
    for pair in groups.chunks_exact(2) {
        tokio::join!(
            pool.report_value_damage(pair[0]),
            pool.report_value_damage(pair[1]),
        );
    }
    drop((replica, pool));
    let reopened = open_on(&dir, &fs.crash(), &pool_identity, &shared);
    for group in groups {
        assert!(reopened.is_group_quarantined(group), "lost quarantine for {group}");
    }
}

#[tokio::test]
async fn vs14_a_search_range_refuses_a_plain_schema_fence() {
    let mut s = S::new().await;
    let before = s.f.dump().await;
    let command = ParticipantCommand::DirectSchema(DirectSchemaFence {
        authority: s.f.authority,
        table_id: s.f.table,
        change: DirectSchemaChange::Fence {
            lifecycle: Some(nabedra_rt::ids::uuid_v4()),
        },
    });
    assert_eq!(
        s.f.submit(command).await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
    assert_eq!(s.f.dump().await, before);
    assert!(matches!(s.fence(columns()).await, SearchReply::Fenced { .. }));
}
