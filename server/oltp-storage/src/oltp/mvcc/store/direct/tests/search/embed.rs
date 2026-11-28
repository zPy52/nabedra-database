//! VEC-G07: background embedding at apply (`specs/vector/embed`).
use super::*;
use crate::oltp::mvcc::store::embed::EMBED_ATTEMPTS;

fn row(key: i64, body: &str) -> serde_json::Value {
    serde_json::json!({"id": key, "body": body})
}
fn record(source: u64, id: u64, attempts: u32, limit: u32) -> EmbedRecord {
    EmbedRecord {
        source_version: ts(source),
        id: ts(id),
        attempts,
        limit,
        failed: None,
    }
}
async fn put(s: &mut S, key: i64, expected: Option<u64>, at: u64, body: &str) {
    let write = s.write(key, expected, Some(row(key, body)), Vec::new());
    assert_eq!(s.direct(at, write).await, applied(at));
}

/// Mutant `prefix_stale_vector`.
#[tokio::test]
async fn vs12_a_changed_text_drops_the_vector_and_the_failure_and_waits_again() {
    let mut s = S::new().await;
    put(&mut s, 1, None, 10, "a").await;
    let got = s.get(1).await;
    assert_eq!((got.source, got.vectors), (BTreeMap::from([(EMBEDDED, 10)]), BTreeMap::new()));
    assert_eq!(got.records, BTreeMap::from([(EMBEDDED, record(10, 10, 0, 10))]));
    assert_eq!(s.embed(s.set_vector(1, 10, 10, 11)).await, SearchReply::Set { commit_ts: ts(11) });
    let got = s.get(1).await;
    assert_eq!(got.version, Some(11), "a row version of its own");
    assert_eq!(got.vectors[&EMBEDDED], (11, vec![11; 8]));
    assert_eq!(got.value, Some(row(1, "a")));
    assert!(got.records.is_empty());
    // Keeping the text keeps the vector, directly and through the queue.
    let write = s.write(1, Some(11), Some(serde_json::json!({"id": 1, "body": "a", "v": 1})), Vec::new());
    s.direct(12, write).await;
    let writes = vec![s.f.queued_put(1, Some(12), row(1, "a"))];
    let plan = s.f.queued_plan(12, &writes);
    s.f.prepare(&plan, writes).await;
    s.f.seal_and_resolve(&plan, 13).await;
    let got = s.get(1).await;
    assert_eq!((&got.vectors[&EMBEDDED], got.source[&EMBEDDED]), (&(11, vec![11; 8]), 10));
    // Changing it drops the vector and starts a new record, both ways.
    put(&mut s, 1, Some(13), 14, "b").await;
    let got = s.get(1).await;
    assert!(got.vectors.is_empty());
    assert_eq!(got.records[&EMBEDDED], record(14, 14, 0, 10));
    for attempt in 0..EMBED_ATTEMPTS {
        s.fail(1, 14, 14, attempt).await;
    }
    assert!(s.get(1).await.records[&EMBEDDED].failed.is_some());
    let writes = vec![s.f.queued_put(1, Some(14), row(1, "a"))];
    let plan = s.f.queued_plan(14, &writes);
    s.f.prepare(&plan, writes).await;
    s.f.seal_and_resolve(&plan, 15).await;
    let got = s.get(1).await;
    assert!(got.vectors.is_empty(), "the text changed back, its vector is gone");
    assert_eq!(got.records[&EMBEDDED], record(15, 15, 0, 10));
    // A null text has nothing to embed, and neither does a deleted row.
    let write = s.write(1, Some(15), Some(serde_json::json!({"id": 1, "body": null})), Vec::new());
    s.direct(16, write).await;
    let got = s.get(1).await;
    assert!(got.records.is_empty() && got.source.is_empty());
    put(&mut s, 1, Some(16), 17, "c").await;
    let write = s.write(1, Some(17), None, Vec::new());
    s.direct(18, write).await;
    assert_eq!(s.count(crate::oltp::mvcc::store::embed::EMBED).await, 0);
}

/// Mutants `fence_by_key_only`, `apply_on_deleted_row` and `fence_without_column`.
#[tokio::test]
async fn vs12_an_embedding_for_another_text_row_or_column_changes_nothing() {
    let mut s = S::new().await;
    put(&mut s, 1, None, 10, "a").await;
    let late = s.set_vector(1, 10, 10, 30);
    // The text changes, changes back, and the row is deleted and inserted again.
    for (expected, at, body) in [(10, 11, Some("b")), (11, 12, Some("a")), (12, 13, None), (13, 14, Some("a"))] {
        let write = s.write(1, Some(expected), body.map(|body| row(1, body)), Vec::new());
        assert_eq!(s.direct(at, write).await, applied(at));
        for expected in [10, at] {
            let before = s.f.dump().await;
            let set = SetVector { expected_version: ts(expected), ..late.clone() };
            assert_eq!(s.embed(set).await, SearchReply::Obsolete, "after {at}");
            assert_eq!(s.f.dump().await, before);
        }
    }
    let missing = s.set_vector(7, 10, 10, 30);
    assert_eq!(s.embed(missing).await, SearchReply::Obsolete);
    // The column is dropped and added again for another model.
    let mut next = columns();
    next[1] = embedded(5);
    next.sort_by_key(|column| column.id);
    assert!(matches!(s.fence(next).await, SearchReply::Fenced { .. }));
    let set = s.set_vector(1, 14, 14, 30);
    assert_eq!(s.embed(set.clone()).await, SearchReply::Obsolete);
    // Nor does it land as the new column, which backfill has not reached.
    let set = SetVector { column: 5, ..set };
    assert_eq!(s.embed(set).await, SearchReply::Obsolete);
    assert!(s.get(1).await.vectors.is_empty());
    assert_eq!(s.fail(1, 14, 14, 0).await, SearchReply::Unmatched);
}

/// Mutant `prefix_setvector_unversioned`: it compares and bumps the row version.
#[tokio::test]
async fn vs12_an_embedding_conflicts_with_an_update_and_an_update_with_an_embedding() {
    let mut s = S::new().await;
    put(&mut s, 1, None, 10, "a").await;
    let late = s.set_vector(1, 10, 10, 12);
    let write = s.write(1, Some(10), Some(serde_json::json!({"id": 1, "body": "a", "v": 1})), Vec::new());
    s.direct(11, write).await;
    let before = s.f.dump().await;
    assert_eq!(s.embed(late).await, refused(Fault::Conflict));
    assert_eq!(s.f.dump().await, before);
    // An update that read the row before the embedding cannot drop it.
    let stale = s.write(1, Some(11), Some(serde_json::json!({"id": 1, "body": "a", "v": 2})), Vec::new());
    let set = s.set_vector(1, 11, 10, 12);
    assert_eq!(s.embed(set.clone()).await, SearchReply::Set { commit_ts: ts(12) });
    assert_eq!(s.direct(13, stale).await, rejected(Fault::Conflict));
    // A second delivery is a no-op, also at another version and timestamp.
    let before = s.f.dump().await;
    for (expected, at) in [(11, 12), (12, 14), (12, 15)] {
        let again = SetVector { expected_version: ts(expected), commit_ts: ts(at), ..set.clone() };
        assert_eq!(s.embed(again).await, SearchReply::AlreadySet);
    }
    assert_eq!(s.f.dump().await, before);
    assert_eq!(s.get(1).await.vectors[&EMBEDDED], (12, vec![12; 8]));
}

/// The other conditions of an ordinary direct commit.
#[tokio::test]
async fn vs12_an_embedding_obeys_every_direct_commit_rule() {
    let mut s = S::new().await;
    put(&mut s, 1, None, 10, "a").await;
    let good = s.set_vector(1, 10, 10, 20);
    let mut cases = vec![
        (SetVector { authority: Authority { run_id: s.f.authority.run_id, owner_term: 2 }, ..good.clone() }, Fault::StaleAuthority),
        (SetVector { schema_epoch: s.epoch - 1, ..good.clone() }, Fault::StaleEpoch),
        (SetVector { routing_epoch: 2, ..good.clone() }, Fault::StaleEpoch),
        (SetVector { table_id: nabedra_rt::ids::uuid_v4(), ..good.clone() }, Fault::InvalidBinding),
        (SetVector { value: s.value(VectorKind::Dense, &[1; 12]), ..good.clone() }, Fault::InvalidBinding),
        (SetVector { value: s.value(VectorKind::Multi, &[1; 8]), ..good.clone() }, Fault::InvalidBinding),
        (SetVector { commit_ts: ts(10), ..good.clone() }, Fault::Conflict),
    ];
    // Below and at the closed floor.
    assert_eq!(s.close(15).await, SearchReply::Closed { floor: ts(15) });
    cases.push((SetVector { commit_ts: ts(15), ..good.clone() }, Fault::Conflict));
    for (n, (set, fault)) in cases.into_iter().enumerate() {
        let before = s.f.dump().await;
        assert_eq!(s.embed(set).await, refused(fault), "case {n}");
        assert_eq!(s.f.dump().await, before, "case {n}");
    }
    // A column without an embedder.
    let plain = SetVector { column: PLAIN, ..good.clone() };
    assert_eq!(s.embed(plain).await, SearchReply::Obsolete);
    // An open intent, the split barrier, and a DDL fence.
    let prepared = vec![s.f.queued_put(1, Some(10), row(1, "a"))];
    let plan = s.f.queued_plan(16, &prepared);
    s.f.prepare(&plan, prepared).await;
    assert_eq!(s.embed(good.clone()).await, refused(Fault::Incomplete));
    s.abort(&plan).await;
    let split = nabedra_rt::ids::uuid_v4();
    let paused = pause(&s.f, split, true);
    s.f.submit(paused).await;
    assert_eq!(s.embed(good.clone()).await, refused(Fault::SplitInProgress));
    let resumed = pause(&s.f, split, false);
    s.f.submit(resumed).await;
    s.fence(columns()).await;
    assert_eq!(s.embed(good.clone()).await, refused(Fault::StaleEpoch));
    let good = SetVector { schema_epoch: s.epoch, ..good };
    assert_eq!(s.embed(good).await, SearchReply::Set { commit_ts: ts(20) });
    // The commit raises the freeze bound like any direct commit.
    let bindings = split_bindings(&s.f, split, 19);
    let freeze = ParticipantCommand::SplitFreeze {
        authority: s.f.authority,
        children: Box::new(bindings),
    };
    assert_eq!(s.f.submit(freeze).await, ParticipantReply::Rejected(Fault::DirectCommitAboveCut));
}

/// Mutants `prefix_count_every_report`, `prefix_unfenced_failure` and
/// `prefix_requeue_resets_attempts`.
#[tokio::test]
async fn vs13_an_attempt_counts_once_and_requeue_goes_on() {
    let mut s = S::new().await;
    put(&mut s, 1, None, 10, "a").await;
    assert_eq!(s.fail(1, 10, 10, 0).await, SearchReply::Counted { attempts: 1 });
    for (source, id, attempt) in [(10, 10, 0), (10, 10, 2), (9, 10, 1), (10, 9, 1), (11, 11, 1)] {
        let before = s.f.dump().await;
        assert_eq!(s.fail(1, source, id, attempt).await, SearchReply::Unmatched);
        assert_eq!(s.f.dump().await, before);
    }
    assert_eq!(s.fail(2, 10, 10, 1).await, SearchReply::Unmatched, "another row");
    for attempt in 1..EMBED_ATTEMPTS - 1 {
        assert_eq!(s.fail(1, 10, 10, attempt).await, SearchReply::Counted { attempts: attempt + 1 });
    }
    let last = EMBED_ATTEMPTS - 1;
    assert_eq!(s.fail(1, 10, 10, last).await, SearchReply::EmbedFailed { attempts: EMBED_ATTEMPTS });
    let failed = s.get(1).await.records[&EMBEDDED].clone();
    assert_eq!(failed.failed.unwrap().as_str(), "provider said no");
    assert_eq!(s.fail(1, 10, 10, EMBED_ATTEMPTS).await, SearchReply::Unmatched, "it failed already");
    let requeue = |id| RequeueEmbedding {
        authority: s.f.authority,
        table_id: s.f.table,
        primary_key: Key::Int64(1),
        column: EMBEDDED,
        source_version: ts(10),
        id: ts(id),
    };
    let (wrong, right) = (requeue(9), requeue(10));
    let submit = ParticipantCommand::RequeueEmbedding;
    assert_eq!(search_reply(s.f.submit(submit(wrong)).await), SearchReply::Unmatched);
    let reply = search_reply(s.f.submit(submit(right.clone())).await);
    assert_eq!(reply, SearchReply::Requeued { attempts: 10, limit: 20 });
    assert_eq!(search_reply(s.f.submit(submit(right)).await), SearchReply::Unmatched, "it waits");
    assert_eq!(s.get(1).await.records[&EMBEDDED], record(10, 10, 10, 20));
    // Late reports of the first ten attempts match nothing.
    for attempt in 0..EMBED_ATTEMPTS {
        assert_eq!(s.fail(1, 10, 10, attempt).await, SearchReply::Unmatched);
    }
    assert_eq!(s.fail(1, 10, 10, 10).await, SearchReply::Counted { attempts: 11 });
    // A vector that arrives for a failed or waiting record takes effect.
    assert_eq!(s.embed(s.set_vector(1, 10, 10, 20)).await, SearchReply::Set { commit_ts: ts(20) });
    assert!(s.get(1).await.records.is_empty());
}

/// Leases live in leader memory only: handing a record out changes nothing.
/// (Mutants `delete_pending_on_lease` and `embedded_never_retried`.)
#[tokio::test]
async fn vs13_the_records_page_in_key_order_and_reading_them_keeps_them() {
    let mut s = S::new().await;
    for key in 0..5 {
        put(&mut s, key, None, 10 + key as u64, "a").await;
    }
    let before = s.f.dump().await;
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = s.f.machine().embed_records(after, 2).await.unwrap();
        let Some(last) = page.last() else { break };
        after = Some((last.key.clone(), last.column));
        found.extend(page);
    }
    let keys: Vec<_> = found.iter().map(|item| (item.key.clone(), item.column)).collect();
    assert_eq!(keys, (0..5).map(|key| (s.f.key(key), EMBEDDED)).collect::<Vec<_>>());
    assert_eq!(found[3].record, record(13, 13, 0, 10));
    assert_eq!(s.f.dump().await, before);
    assert_eq!(s.f.machine().embed_records(None, 128).await.unwrap(), found);
}

#[tokio::test]
async fn vs14_a_column_id_is_one_incarnation_and_a_late_fence_changes_nothing() {
    let mut s = S::new().await;
    let epoch = s.epoch;
    put(&mut s, 1, None, 10, "a").await;
    // A late fence of an old Control leader expects an epoch the group left
    // (DB-20, L19); the installed one repeated is a no-op.
    let before = s.f.dump().await;
    let late = s.fence_command(1, vec![]);
    assert_eq!(search_reply(s.f.submit(late).await), refused(Fault::Conflict));
    let repeated = s.fence_command(1, columns());
    assert!(matches!(
        search_reply(s.f.submit(repeated).await),
        SearchReply::Fenced { epoch: 1, direct: Some(direct) } if direct.epoch == epoch
    ));
    assert_eq!(s.f.dump().await, before);
    // An id keeps its definition and never returns.
    let mut changed = columns();
    changed[0].dim = 3;
    assert_eq!(s.fence(changed).await, refused(Fault::InvalidBinding));
    let mut dropped = columns();
    dropped.remove(1);
    assert!(matches!(s.fence(dropped).await, SearchReply::Fenced { .. }));
    assert_eq!(s.fence(columns()).await, refused(Fault::InvalidBinding));
    // A write that read the old columns is refused.
    let mut old = s.commit(11, vec![s.write(1, Some(10), Some(row(1, "b")), Vec::new())]);
    old.schema_epoch = epoch;
    assert_eq!(s.f.direct(&old).await, rejected(Fault::StaleEpoch));
    // The dropped column's record goes with the next write of its row.
    let write = s.write(1, Some(10), Some(row(1, "b")), Vec::new());
    assert_eq!(s.direct(12, write).await, applied(12));
    assert_eq!(s.count(crate::oltp::mvcc::store::embed::EMBED).await, 0);
    assert_eq!(s.get(1).await.source, BTreeMap::new());
    for bad in [
        vec![column(9, VectorKind::Dense, 0)],
        vec![column(9, VectorKind::Dense, 4097)],
        vec![column(9, VectorKind::Sparse, 3)],
        vec![SearchColumn { kind: VectorKind::Sparse, dim: 0, ..embedded(9) }],
        vec![column(9, VectorKind::Dense, 1), column(9, VectorKind::Dense, 1)],
        (9..18).map(|id| column(id, VectorKind::Dense, 1)).collect(),
    ] {
        assert_eq!(s.fence(bad).await, refused(Fault::InvalidBinding));
    }
}

// ---------------------------------------------------------------------------
// EmbedApply.tla as a model, over random histories
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
struct Row {
    text: u8,
    source: Option<u64>,
    version: u64,
}
#[derive(Clone, Copy, Debug, PartialEq)]
struct Task {
    text: u8,
    source: u64,
    column: ColumnId,
    attempts: u32,
    id: u64,
}
#[derive(Debug)]
struct Model {
    row: Option<Row>,
    /// The head version, a tombstone included.
    head: Option<u64>,
    column: ColumnId,
    filled: bool,
    vector: Option<(u8, ColumnId)>,
    pending: Option<EmbedRecord>,
    failed: Option<EmbedRecord>,
    tasks: Vec<Task>,
    vectors: Vec<Task>,
    errors: Vec<Task>,
    errs: BTreeMap<u64, u32>,
    counted: BTreeMap<u64, u32>,
    update: Option<u64>,
    set: Option<(Task, u64)>,
}
fn made_from(text: u8, column: ColumnId) -> Vec<u8> {
    [[text; 4], [column as u8; 4]].concat()
}

#[tokio::test]
async fn vs12_vs13_random_histories_follow_the_embed_apply_spec() {
    let mut sets = 0;
    let mut failures = 0;
    let mut replies: BTreeMap<String, u32> = BTreeMap::new();
    for seed in 0..50 {
        let mut rng = Rng(seed);
        let mut s = S::plain(Some(&FaultFs::new())).await;
        s.fence(vec![embedded(1)]).await;
        let mut m = Model {
            row: None,
            head: None,
            column: 1,
            filled: true,
            vector: None,
            pending: None,
            failed: None,
            tasks: Vec::new(),
            vectors: Vec::new(),
            errors: Vec::new(),
            errs: BTreeMap::new(),
            counted: BTreeMap::new(),
            update: None,
            set: None,
        };
        for step in 0..240 {
            let label = format!("seed {seed} step {step}");
            match rng.below(24) {
                0 => {
                    let text = rng.below(2) as u8;
                    let at = s.tick();
                    let value = serde_json::json!({"id": 1, "body": text.to_string(), "v": step});
                    let reply = if rng.one_in(2) {
                        let write = s.write(1, m.head, Some(value), Vec::new());
                        s.direct(at, write).await == applied(at)
                    } else {
                        let writes = vec![s.f.queued_put(1, m.head, value)];
                        let plan = s.f.queued_plan(at - 1, &writes);
                        let prepared = s.f.prepare(&plan, writes).await;
                        s.f.seal_and_resolve(&plan, at).await;
                        matches!(prepared, ParticipantReply::PreparedPage { .. })
                    };
                    assert!(reply, "{label}");
                    match &mut m.row {
                        Some(row) if row.text == text => row.version = at,
                        row => {
                            *row = Some(Row { text, source: Some(at), version: at });
                            m.vector = None;
                            m.pending = Some(EmbedRecord::fresh(ts(at)));
                            m.failed = None;
                            m.filled = true;
                        }
                    }
                    m.head = Some(at);
                }
                2 if m.row.is_some() && rng.one_in(3) => {
                    let at = s.tick();
                    let write = s.write(1, m.head, None, Vec::new());
                    assert_eq!(s.direct(at, write).await, applied(at), "{label}");
                    (m.row, m.vector, m.pending, m.failed) = (None, None, None, None);
                    m.head = Some(at);
                }
                3 if m.row.is_some() => m.update = m.head,
                4 => {
                    let Some(read) = m.update.take() else { continue };
                    let Some(row) = &mut m.row else { continue };
                    let at = s.tick();
                    let value = serde_json::json!({"id": 1, "body": row.text.to_string(), "u": step});
                    let write = s.write(1, Some(read), Some(value), Vec::new());
                    let reply = s.direct(at, write).await;
                    if row.version == read {
                        assert_eq!(reply, applied(at), "{label}");
                        row.version = at;
                        m.head = Some(at);
                    } else {
                        assert_eq!(reply, rejected(Fault::Conflict), "{label}");
                    }
                }
                5 if rng.one_in(8) => {
                    m.column += 1;
                    let column = embedded(m.column);
                    assert!(matches!(s.fence(vec![column]).await, SearchReply::Fenced { .. }));
                    (m.vector, m.pending, m.failed, m.update) = (None, None, None, None);
                    m.filled = m.row.is_none();
                    if let Some(row) = &mut m.row {
                        row.source = None;
                    }
                }
                6 if m.row.is_some() && rng.one_in(2) => {
                    // Backfill, also when there is nothing to do.
                    let row = m.row.as_mut().unwrap();
                    let at = s.tick();
                    let value = serde_json::json!({"id": 1, "body": row.text.to_string(), "b": step});
                    let mut write = s.write(1, Some(row.version), Some(value), Vec::new());
                    write.search = Some(Box::new(RowSearch {
                        vectors: Vec::new(),
                        backfill: vec![m.column],
                    }));
                    assert_eq!(s.direct(at, write).await, applied(at), "{label}");
                    if m.vector.is_none() && m.pending.is_none() && m.failed.is_none() {
                        row.source = Some(at);
                        m.pending = Some(EmbedRecord::fresh(ts(at)));
                    }
                    row.version = at;
                    m.head = Some(at);
                    m.filled = true;
                }
                1 | 7 | 8 => {
                    if let (Some(pending), Some(row)) = (&m.pending, &m.row) {
                        m.tasks.push(Task {
                            text: row.text,
                            source: pending.source_version.get(),
                            column: m.column,
                            attempts: pending.attempts,
                            id: pending.id.get(),
                        });
                    }
                }
                9 | 16 | 17 if !m.tasks.is_empty() => {
                    let task = m.tasks[rng.recent(m.tasks.len())];
                    m.vectors.push(task);
                }
                10 | 11 if !m.tasks.is_empty() => {
                    // One provider error per task. Its report may arrive again.
                    let task = m.tasks.remove(rng.recent(m.tasks.len()));
                    *m.errs.entry(task.source).or_default() += 1;
                    m.errors.push(task);
                }
                12 | 18 | 19 if !m.vectors.is_empty() => {
                    let task = m.vectors[rng.recent(m.vectors.len())];
                    m.set = Some((task, m.row.map_or(1, |row| row.version)));
                }
                13 | 20 | 21 => {
                    let Some((task, read)) = m.set.take() else { continue };
                    let at = s.tick();
                    let set = SetVector {
                        column: task.column,
                        value: s.value(VectorKind::Dense, &made_from(task.text, task.column)),
                        ..s.set_vector(1, read, task.source, at)
                    };
                    let reply = s.embed(set).await;
                    let row = m.row.filter(|row| {
                        row.source == Some(task.source) && task.column == m.column
                    });
                    let expected = match row {
                        None => SearchReply::Obsolete,
                        Some(_) if m.vector.is_some() && m.pending.is_none() => SearchReply::AlreadySet,
                        Some(row) if row.version != read => refused(Fault::Conflict),
                        Some(_) => {
                            m.vector = Some((task.text, task.column));
                            (m.pending, m.failed) = (None, None);
                            m.row.as_mut().unwrap().version = at;
                            m.head = Some(at);
                            sets += 1;
                            SearchReply::Set { commit_ts: ts(at) }
                        }
                    };
                    assert_eq!(reply, expected, "{label} {m:?}");
                    let name = format!("{reply:?}").split(['(', ' ']).next().unwrap().to_owned();
                    *replies.entry(name).or_default() += 1;
                }
                14 | 22 if !m.errors.is_empty() => {
                    let task = m.errors[rng.recent(m.errors.len())];
                    let failed = EmbedFailed {
                        authority: s.f.authority,
                        table_id: s.f.table,
                        primary_key: Key::Int64(1),
                        column: task.column,
                        source_version: ts(task.source),
                        id: ts(task.id),
                        attempt: task.attempts,
                        reason: Text::try_from("no").unwrap(),
                    };
                    let reply = search_reply(s.f.submit(ParticipantCommand::EmbedFailed(failed)).await);
                    let matches = task.column == m.column
                        && m.pending.as_ref().is_some_and(|pending| {
                            (pending.source_version.get(), pending.id.get(), pending.attempts)
                                == (task.source, task.id, task.attempts)
                        });
                    let expected = if matches {
                        let mut record = m.pending.take().unwrap();
                        *m.counted.entry(task.source).or_default() += 1;
                        record.attempts += 1;
                        let attempts = record.attempts;
                        if attempts >= record.limit {
                            record.failed = Some(Text::try_from("no").unwrap());
                            m.failed = Some(record);
                            failures += 1;
                            SearchReply::EmbedFailed { attempts }
                        } else {
                            m.pending = Some(record);
                            SearchReply::Counted { attempts }
                        }
                    } else {
                        SearchReply::Unmatched
                    };
                    assert_eq!(reply, expected, "{label} {m:?}");
                    let name = format!("{reply:?}").split(['(', ' ']).next().unwrap().to_owned();
                    *replies.entry(name).or_default() += 1;
                    // Run the record close to its limit, so histories reach it.
                    if let Some(record) = m.pending.as_mut().filter(|record| record.attempts == 1) {
                        while record.attempts + 1 < record.limit {
                            let (source, id) = (record.source_version.get(), record.id.get());
                            *m.errs.entry(source).or_default() += 1;
                            *m.counted.entry(source).or_default() += 1;
                            let failed = EmbedFailed {
                                authority: s.f.authority,
                                table_id: s.f.table,
                                primary_key: Key::Int64(1),
                                column: m.column,
                                source_version: ts(source),
                                id: ts(id),
                                attempt: record.attempts,
                                reason: Text::try_from("no").unwrap(),
                            };
                            let reply = s.f.submit(ParticipantCommand::EmbedFailed(failed)).await;
                            record.attempts += 1;
                            let attempts = record.attempts;
                            assert_eq!(search_reply(reply), SearchReply::Counted { attempts });
                        }
                    }
                }
                15 => {
                    let Some(mut record) = m.failed.clone() else { continue };
                    let requeue = RequeueEmbedding {
                        authority: s.f.authority,
                        table_id: s.f.table,
                        primary_key: Key::Int64(1),
                        column: m.column,
                        source_version: record.source_version,
                        id: record.id,
                    };
                    s.f.submit(ParticipantCommand::RequeueEmbedding(requeue)).await;
                    record.failed = None;
                    record.limit = record.attempts + EMBED_ATTEMPTS;
                    (m.pending, m.failed) = (Some(record), None);
                }
                _ => {}
            }
            let got = s.get(1).await;
            assert_eq!(got.version, m.head, "{label}");
            let vector = got.vectors.get(&m.column).map(|(_, bytes)| bytes.clone());
            assert_eq!(vector, m.vector.map(|(text, column)| made_from(text, column)), "{label}");
            if let Some(row) = &m.row {
                assert_eq!(got.source.get(&m.column).copied(), row.source, "{label}");
                let record = m.pending.as_ref().or(m.failed.as_ref());
                assert_eq!(got.records.get(&m.column), record, "{label} {m:?}");
                // VectorMatchesSource, PendingWhenNeeded.
                assert!(m.vector.is_none_or(|vector| vector == (row.text, m.column)), "{label}");
                assert!(!m.filled || m.vector.is_some() || record.is_some_and(|record| {
                    Some(record.source_version.get()) == row.source
                }), "{label}");
            } else {
                // DeletedStaysDeleted.
                assert!(got.value.is_none() && got.vectors.is_empty(), "{label}");
                assert_eq!(s.f.machine().embed_records(None, 8).await.unwrap(), [], "{label}");
            }
            // CountedOnce.
            assert!(m.counted.iter().all(|(v, n)| *n <= m.errs[v]), "{label}");
        }
    }
    assert!(sets > 30 && failures > 2 && replies.len() == 7, "{replies:?}");
}
