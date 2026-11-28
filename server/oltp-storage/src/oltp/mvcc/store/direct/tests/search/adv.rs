//! Adversarial tests of C2, written by someone who did not write the code: the
//! closed floor on every commit path, change records, damaged and staged vector
//! records, embedding records, copies, and crashes at every write.
use super::*;
use crate::oltp::mvcc::store::{
    direct::{DirectMemberReply, DirectReply},
    embed::{EMBED, EMBED_ATTEMPTS, RequeueEmbedding},
    image_archive::{CommittedImage, ImagePage, ImageRequest},
    lineage::{self, SplitBinding, SplitPage},
    restore,
    row::{STAGED, VECTOR},
};

use crate::oltp::mvcc::vector::MAX_VECTOR_BYTES;

fn row(key: i64, body: &str) -> serde_json::Value {
    serde_json::json!({"id": key, "body": body})
}
fn plain(key: i64) -> serde_json::Value {
    serde_json::json!({"id": key})
}
/// The index of the entry that started the feed.
const FEED_START: u64 = 2;

fn requeue(s: &S, key: i64, source: u64, id: u64) -> ParticipantCommand {
    ParticipantCommand::RequeueEmbedding(RequeueEmbedding {
        authority: s.f.authority,
        table_id: s.f.table,
        primary_key: Key::Int64(key),
        column: EMBEDDED,
        source_version: ts(source),
        id: ts(id),
    })
}
async fn seal(s: &mut S, p: &QueuedPlan) {
    let command = ParticipantCommand::Seal {
        authority: s.f.authority,
        txn: p.txn,
        manifest: p.manifest,
        result_digest: digest(4),
    };
    assert!(matches!(s.f.submit(command).await, ParticipantReply::Sealed(_)));
}
async fn resolve(s: &mut S, p: &QueuedPlan, decision: TxnDecision, max_items: u16) -> ParticipantReply {
    let proof = SealedStatusDecision::seal(DecisionStatement {
        txn: p.txn,
        request: p.request,
        manifest: p.manifest,
        decided_at: position(1),
        decision,
    })
    .unwrap();
    let command = ParticipantCommand::Resolve { authority: s.f.authority, proof, max_items };
    s.f.submit(command).await
}
fn commit_at(at: u64) -> TxnDecision {
    TxnDecision::Commit { commit_ts: ts(at) }
}
fn abort_decision() -> TxnDecision {
    TxnDecision::Abort { reason: nabedra_oltp_types::types::AbortReason::Conflict }
}
/// Applies one entry on `machine`, as the Raft layer does.
async fn apply_entry(
    machine: &ParticipantMachine,
    shared: &WorkerSharedResources,
    index: u64,
    command: &ParticipantCommand,
) -> Result<ParticipantReply, ProtocolError> {
    let command = OwnedCommand::new_owned(command.clone(), shared, ByteClass::Data).unwrap();
    let meta = ApplyMeta {
        position: position(index),
        command_digest: nabedra_oltp_types::owned::serialized_digest(&command).unwrap(),
        membership: None,
    };
    let raw = machine.apply(meta, command).await?;
    Ok(serde_json::from_slice(raw.as_slice()).unwrap())
}

// ---------------------------------------------------------------------------
// (2) The closed floor against every commit path
// ---------------------------------------------------------------------------

/// Direct, DirectBatch members and SetVector at, below and above a floor.
#[tokio::test]
async fn adv_g02_every_direct_member_and_set_vector_obeys_the_floor() {
    let mut s = S::new().await;
    for key in [5, 6] {
        let write = s.write(key, None, Some(row(key, "t")), Vec::new());
        assert_eq!(s.direct(20 + key as u64, write).await, applied(20 + key as u64));
    }
    assert_eq!(s.close(50).await, SearchReply::Closed { floor: ts(50) });
    let vector = |s: &S, n: u8| vec![s.set(PLAIN, s.dense(n))];
    let members = vec![
        s.commit(49, vec![s.write(1, None, Some(plain(1)), vector(&s, 1))]),
        s.commit(51, vec![s.write(2, None, Some(plain(2)), vector(&s, 2))]),
        s.commit(50, vec![s.write(3, None, Some(plain(3)), vector(&s, 3))]),
        s.commit(52, vec![s.write(4, None, Some(plain(4)), vector(&s, 4))]),
        s.commit(
            50,
            vec![
                s.write(7, None, Some(plain(7)), vector(&s, 7)),
                s.write(8, None, Some(plain(8)), vector(&s, 8)),
            ],
        ),
    ];
    let reply = s.f.submit(ParticipantCommand::DirectBatch(DirectBatch { members })).await;
    let ParticipantReply::Direct(DirectReply::Batch(replies)) = reply else { panic!("{reply:?}") };
    let conflict = DirectMemberReply::Rejected(Fault::Conflict);
    let took = |at| DirectMemberReply::Applied { commit_ts: ts(at) };
    assert_eq!(replies, vec![conflict.clone(), took(51), conflict.clone(), took(52), conflict]);
    for key in [1, 3, 7, 8] {
        assert_eq!(s.get(key).await, Got::default(), "row {key}");
    }
    assert_eq!(s.count(VECTOR).await, 2, "vectors of the two applied members only");
    let feed = seen(&s.changes(FEED_START, 8).await);
    let keys: Vec<_> = feed.iter().map(|change| (change.1, change.2)).collect();
    assert_eq!(keys, vec![(5, 25), (6, 26), (i64::MIN, 50), (2, 51), (4, 52)]);

    let before = s.f.dump().await;
    for at in [49, 50] {
        assert_eq!(s.embed(s.set_vector(5, 25, 25, at)).await, refused(Fault::Conflict), "{at}");
    }
    assert_eq!(s.f.dump().await, before);
    assert_eq!(s.embed(s.set_vector(5, 25, 25, 53)).await, SearchReply::Set { commit_ts: ts(53) });
}
/// A prepared transaction resolved below a floor that closed before it prepared.
/// The plan leaves this to the protocol (the commit timestamp is taken after every
/// participant prepared); what does the store do with one that is not?
#[tokio::test]
async fn adv_g02_a_resolve_below_the_floor_is_not_a_silent_landing() {
    let mut s = S::new().await;
    assert_eq!(s.close(50).await, SearchReply::Closed { floor: ts(50) });
    let writes = vec![s.f.queued_put(1, None, plain(1))];
    let plan = s.f.queued_plan(20, &writes);
    s.f.prepare(&plan, writes).await;
    seal(&mut s, &plan).await;
    // It cannot be refused (atomicity) and must not land: the apply fails and the
    // group is quarantined, so this replica never serves or acknowledges again.
    let proof = SealedStatusDecision::seal(DecisionStatement {
        txn: plan.txn,
        request: plan.request,
        manifest: plan.manifest,
        decided_at: position(1),
        decision: commit_at(40),
    })
    .unwrap();
    let command = ParticipantCommand::Resolve { authority: s.f.authority, proof, max_items: 128 };
    let index = s.f.replica.index;
    let reply = apply_entry(s.f.machine(), s.f.shared(), index, &command).await;
    let error = reply.err().expect("a Resolve below the floor applied");
    assert!(error.message.starts_with("storage corrupt"), "{}", error.message);
    assert!(s.f.pool.is_group_quarantined(s.f.group.group_id));
    // Nothing of it was written: the intent waits, and no version exists.
    let versions: Vec<_> = s.records(VALUE).await;
    assert!(versions.is_empty(), "a version landed below the closed floor 50: {versions:?}");
}

/// Closed twice, below the floor, redelivered, and after a leader change.
#[tokio::test]
async fn adv_g02_closed_twice_below_and_after_a_leader_change_changes_nothing() {
    let mut s = S::new().await;
    assert_eq!(s.close(50).await, SearchReply::Closed { floor: ts(50) });
    let feed = seen(&s.changes(FEED_START, 8).await);
    assert_eq!(feed.len(), 1);
    let dump = s.f.dump().await;
    for at in [50, 50, 49, 1] {
        assert_eq!(s.close(at).await, SearchReply::Closed { floor: ts(50) }, "{at}");
    }
    assert_eq!(s.f.dump().await, dump);
    assert_eq!(seen(&s.changes(FEED_START, 8).await), feed, "no record for a close that closed nothing");
    // The same entry delivered again leaves its one record.
    let closed = Closed { authority: s.f.authority, ts: ts(60), unix_ms: 600 };
    let meta = ApplyMeta { position: position(s.f.replica.index), command_digest: digest(5), membership: None };
    for _ in 0..3 {
        let command = ParticipantCommand::Closed(closed);
        let command = OwnedCommand::new_owned(command, s.f.shared(), ByteClass::Data).unwrap();
        s.f.machine().apply(meta.clone(), command).await.unwrap();
    }
    s.f.replica.index += 1;
    assert_eq!(seen(&s.changes(FEED_START, 8).await).len(), 2);
    // A leader change: the old run's Closed is stale, the new run keeps the floor.
    let next = Authority { run_id: nabedra_rt::ids::uuid_v4(), owner_term: 2 };
    let fence = ParticipantCommand::Fence {
        transition_id: nabedra_rt::ids::uuid_v4(),
        expected: Some(s.f.authority),
        next,
        schema_epoch: 1,
        routing_epoch: 1,
        span: full_span(),
        catalog_group_id: s.f.catalog_group_id,
    };
    assert!(matches!(s.f.submit(fence).await, ParticipantReply::Fenced(_)));
    assert_eq!(s.close(70).await, refused(Fault::StaleAuthority));
    s.f.authority = next;
    assert_eq!(s.close(55).await, SearchReply::Closed { floor: ts(60) });
    assert_eq!(seen(&s.changes(FEED_START, 8).await).len(), 2);
    let write = s.write(1, None, Some(plain(1)), Vec::new());
    assert_eq!(s.direct(60, write).await, rejected(Fault::Conflict));
}

/// A replica that installs a snapshot keeps the floor.
#[tokio::test]
async fn adv_g02_the_floor_survives_a_replica_snapshot() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(plain(1)), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    assert_eq!(s.close(50).await, SearchReply::Closed { floor: ts(50) });
    let snapshot = s.f.machine().store().export_snapshot(BUDGET).await.unwrap();
    let dir = Dir(std::env::temp_dir().join(format!("adv-{}", nabedra_rt::ids::uuid_v4())));
    let mut identity = s.f.pool_identity.clone();
    identity.pool_id = nabedra_rt::ids::uuid_v4();
    let pool = OltpPool::open_development(&dir.0, identity, s.f.shared().clone()).unwrap();
    let store = pool.group(s.f.group.clone()).await.unwrap();
    store.install_snapshot(snapshot, BUDGET).await.unwrap();
    let mut follower = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: s.f.shared().clone(),
        index: s.f.replica.index,
    };
    let commit = s.commit(50, vec![s.write(2, None, Some(plain(2)), Vec::new())]);
    assert_eq!(follower.submit(ParticipantCommand::Direct(commit.clone())).await, rejected(Fault::Conflict));
    let commit = DirectCommit { commit_ts: ts(51), ..commit };
    assert_eq!(follower.submit(ParticipantCommand::Direct(commit)).await, applied(51));
}

// ---------------------------------------------------------------------------
// (3) Change records
// ---------------------------------------------------------------------------

async fn feed_log(s: &mut S) {
    for n in 0..9i64 {
        match n % 3 {
            0 => {
                let write = s.write(n, None, Some(row(n, "a")), vec![s.set(PLAIN, s.dense(n as u8))]);
                s.direct(10 + n as u64, write).await;
            }
            1 => {
                // Rejected: a stale expected version leaves no record and no hole.
                let write = s.write(n, Some(3), Some(plain(n)), Vec::new());
                s.direct(10 + n as u64, write).await;
            }
            _ => {
                s.close(10 + n as u64).await;
            }
        }
    }
}
async fn purge_changes(s: &S, from: u64, through: u64) {
    s.f.machine()
        .store()
        .log_transaction(BUDGET, move |tx| {
            tx.purge_changes(from, through)?;
            Ok(Transition::Commit(tx.copy_output(b"")?))
        })
        .await
        .unwrap();
}

/// A record names the one before it, and that one may be purged, truncated or
/// never have existed. Every position gets exactly the rows after it, or an
/// explicit gap, and a gap only when a record it needs is gone.
#[tokio::test]
async fn adv_dbr5_every_position_gets_the_rows_after_it_or_a_gap_that_has_a_reason() {
    let reference = {
        let mut s = S::new().await;
        feed_log(&mut s).await;
        let all = seen(&s.changes(FEED_START, 64).await);
        (all, s.last())
    };
    let (all, last) = reference;
    assert!(all.len() >= 5, "{all:?}");
    let first = all[0].0;
    let newest = all.last().unwrap().0;
    let ranges = [(0, 3), (3, first), (first, first), (first + 1, newest), (newest, newest), (0, last), (last, 2), (0, u64::MAX)];
    for (from, through) in ranges {
        let mut s = S::new().await;
        feed_log(&mut s).await;
        purge_changes(&s, from, through).await;
        for after in FEED_START..=last {
            let purged_needed = all.iter().any(|change| change.0 > after && change.0 >= from && change.0 <= through);
            match changes(&s.f.replica.machine, after, 3).await {
                Some(found) => {
                    let expected: Vec<_> = all.iter().filter(|change| change.0 > after).collect();
                    assert!(!purged_needed || expected.is_empty(), "purge {from}..={through} after {after}: skipped");
                    let found = seen(&found);
                    assert_eq!(found.iter().collect::<Vec<_>>(), expected, "purge {from}..={through} after {after}");
                }
                None => assert!(purged_needed, "purge {from}..={through} after {after}: gone for nothing"),
            }
        }
    }
}

/// A page read while entries apply sees whole entries in order, and what it
/// sees is what a quiet reader sees afterwards.
#[tokio::test]
async fn adv_dbr5_a_page_read_while_entries_apply_sees_only_whole_entries() {
    let s = S::new().await;
    let machine = s.f.machine();
    let shared = s.f.shared();
    let mut log = Vec::new();
    let mut ts_now = 100;
    for n in 0..40i64 {
        ts_now += 1;
        let key = n % 4;
        let head = (n >= 4).then_some(ts_now - 4);
        let vector = vec![s.set(PLAIN, s.dense(n as u8)), s.set(SPARSE, s.value(VectorKind::Sparse, &vec![n as u8; 3000]))];
        log.push(ParticipantCommand::Direct(s.commit(ts_now, vec![s.write(key, head, Some(row(key, "a")), vector)])));
    }
    let start = s.f.replica.index;
    let apply_all = async {
        for (offset, command) in log.iter().enumerate() {
            let reply = apply_entry(machine, shared, start + offset as u64, command).await.unwrap();
            assert!(matches!(reply, ParticipantReply::Direct(DirectReply::Applied { .. })), "{reply:?}");
            tokio::task::yield_now().await;
        }
    };
    let read_all = async {
        let mut pages: Vec<Vec<EntryChanges>> = Vec::new();
        for _ in 0..60 {
            let after = FEED_START;
            let page = machine.change_page(after, 3, 4 << 20).await.unwrap();
            if let ChangePage::Changes { entries, through } = page.payload().unwrap() {
                let mut previous = after;
                for entry in entries {
                    assert!(entry.at.index > previous && !entry.changes.is_empty());
                    previous = entry.at.index;
                }
                assert!(entries.last().is_none_or(|entry| through.index >= entry.at.index));
                pages.push(entries.to_vec());
            }
            tokio::task::yield_now().await;
        }
        pages
    };
    let ((), pages) = tokio::join!(apply_all, read_all);
    let full = seen(&changes(machine, FEED_START, 1024).await.unwrap());
    assert_eq!(full.len(), 40);
    for page in pages {
        let found = seen(&page);
        assert_eq!(found, full[..found.len()], "a page is a prefix of the final feed");
    }
}

/// A very large entry (a batch whose rows carry the largest vectors) is served
/// whole or refused with a limit, and a reader with the largest budget gets it.
#[tokio::test]
async fn adv_dbr5_a_very_large_entry_is_served_with_the_largest_budget() {
    let mut s = S::on(None).await;
    let big = s.value(VectorKind::Sparse, &vec![7; MAX_VECTOR_BYTES]);
    let writes: Vec<_> = (0..6)
        .map(|key| s.write(key, None, Some(row(key, "a")), vec![s.set(SPARSE, big.clone())]))
        .collect();
    let commit = s.commit(10, writes);
    assert_eq!(s.f.direct(&commit).await, applied(10));
    let small = s.f.machine().change_page(FEED_START, 8, 1 << 20).await.err().unwrap();
    assert_eq!(small.code, ErrorCode::LimitExceeded);
    let page = s.f.machine().change_page(FEED_START, 8, 16 << 20).await.unwrap();
    let ChangePage::Changes { entries, .. } = page.payload().unwrap() else { panic!() };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].changes.len(), 6);
}

// ---------------------------------------------------------------------------
// (4) 'x' records
// ---------------------------------------------------------------------------

type Tamper = (&'static str, fn(&mut Vec<u8>));
fn tampers() -> Vec<Tamper> {
    vec![
        ("kind byte", |frame| frame[0] ^= 1),
        ("crc byte 0", |frame| frame[1] ^= 0x80),
        ("crc byte 1", |frame| frame[2] ^= 1),
        ("crc byte 2", |frame| frame[3] ^= 0x10),
        ("crc byte 3", |frame| frame[4] ^= 0x01),
        ("first data byte", |frame| frame[5] ^= 1),
        ("last data byte", |frame| *frame.last_mut().unwrap() ^= 0x80),
        ("truncated by one", |frame| {
            frame.pop();
        }),
        ("truncated to the header", |frame| frame.truncate(5)),
        ("truncated to the kind", |frame| frame.truncate(1)),
        ("empty", |frame| frame.clear()),
        ("extended by one", |frame| frame.push(0)),
        ("two values", |frame| {
            let copy = frame.clone();
            frame.extend(copy);
        }),
    ]
}
#[derive(Clone, Copy, Debug)]
enum Path {
    Point,
    Images,
    Feed,
}

/// Every damage class, on every read path: an explicit corruption error, never
/// null or a skipped vector, and the group is quarantined.
#[tokio::test]
async fn adv_vec_g07_every_damage_class_is_an_error_on_every_read_path() {
    for (name, tamper) in tampers() {
        for path in [Path::Point, Path::Images, Path::Feed] {
            let mut s = S::new().await;
            let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
            s.direct(10, write).await;
            // An older version damaged, and the newest intact.
            let write = s.write(1, Some(10), Some(row(1, "a")), vec![s.set(PLAIN, s.dense(2))]);
            s.direct(11, write).await;
            let after = s.last() - 2;
            let (key, mut frame) = s.records(VECTOR).await.remove(0);
            tamper(&mut frame);
            s.tamper(move |tx| tx.put(&key, &frame)).await;
            let corrupt = |error: ProtocolError| {
                assert!(error.message.starts_with("storage corrupt"), "{name} {path:?}: {}", error.message);
            };
            match path {
                Path::Point => corrupt(got(s.f.machine(), s.f.key(1), 10).await.err().expect(name)),
                Path::Images => {
                    let request = OwnedCommand::new_owned(
                        ImageRequest {
                            read_ts: ts(10),
                            schema_epoch: 1,
                            routing_epoch: 1,
                            span: full_span(),
                            after: None,
                            max_items: 128,
                            max_bytes: 1 << 20,
                            search: None,
                        },
                        s.f.shared(),
                        ByteClass::Data,
                    )
                    .unwrap();
                    corrupt(s.f.machine().committed_image_page(request).await.err().expect(name));
                }
                Path::Feed => corrupt(s.f.machine().change_page(after, 8, 1 << 20).await.err().expect(name)),
            }
            assert!(s.f.pool.is_group_quarantined(s.f.group.group_id), "{name} {path:?}");
            // The intact newest version of the same row is still readable by key
            // only through a path that does not need the damaged value.
            let page = s.f.machine().change_page(after + 1, 8, 1 << 20).await;
            if let Ok(page) = page {
                assert!(matches!(page.payload().unwrap(), ChangePage::Changes { .. } | ChangePage::Gone { .. }));
            }
        }
    }
}

/// A vector whose CRC is valid but whose kind or length does not fit the
/// column (a misdirected or buggy write) is served as is. What do readers do?
#[tokio::test]
async fn adv_vec_g07_a_vector_that_does_not_fit_its_column_is_never_served() {
    for (name, kind, bytes) in [
        ("wrong kind", VectorKind::Sparse, vec![1u8; 8]),
        ("wrong length", VectorKind::Dense, vec![1u8; 12]),
        ("empty", VectorKind::Dense, Vec::new()),
    ] {
        let mut s = S::new().await;
        let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
        s.direct(10, write).await;
        let (key, _) = s.records(VECTOR).await.remove(0);
        let value = s.value(kind, &bytes);
        let mut frame = vec![0; 5 + bytes.len()];
        value.frame(&mut frame).unwrap();
        s.tamper(move |tx| tx.put(&key, &frame)).await;
        let served = got(s.f.machine(), s.f.key(1), 10).await;
        assert!(served.is_err(), "{name}: a vector that does not fit column {PLAIN} was served: {served:?}");
    }
}

/// Resolve of a staged vector that is damaged is an explicit error that leaves the
/// transaction intact, and an abort needs no staged value.
#[tokio::test]
async fn adv_vec_g07_a_damaged_staged_vector_stops_the_commit_and_not_the_abort() {
    for abort in [false, true] {
        let mut s = S::new().await;
        let writes = vec![s.put_row(1, None, plain(1), vec![s.set(PLAIN, s.dense(1))])];
        let plan = s.f.queued_plan(20, &writes);
        s.f.prepare(&plan, writes).await;
        seal(&mut s, &plan).await;
        let (key, mut frame) = s.records(STAGED).await.into_iter().max_by_key(|(key, _)| key.len()).unwrap();
        frame[6] ^= 1;
        let tampered = key.clone();
        s.tamper(move |tx| tx.put(&tampered, &frame)).await;
        if abort {
            let reply = resolve(&mut s, &plan, abort_decision(), 128).await;
            assert!(matches!(reply, ParticipantReply::Resolved { complete: true, .. }), "{reply:?}");
            assert_eq!(s.count(STAGED).await, 0);
            assert_eq!(s.count(VECTOR).await, 0);
        } else {
            let proof = SealedStatusDecision::seal(DecisionStatement {
                txn: plan.txn,
                request: plan.request,
                manifest: plan.manifest,
                decided_at: position(1),
                decision: commit_at(30),
            })
            .unwrap();
            let command = ParticipantCommand::Resolve { authority: s.f.authority, proof, max_items: 128 };
            let error = apply_entry(s.f.machine(), s.f.shared(), s.f.replica.index, &command).await.err().unwrap();
            assert!(error.message.starts_with("storage corrupt"), "{}", error.message);
            assert!(s.f.pool.is_group_quarantined(s.f.group.group_id));
            assert_eq!(s.count(VECTOR).await, 0, "nothing was moved under the commit timestamp");
        }
    }
}

/// Staged vectors of aborted and abandoned transactions are never exposed, and
/// an abort in pages collects every one of them.
#[tokio::test]
async fn adv_vec_g07_staged_vectors_are_never_exposed_and_an_abort_collects_all() {
    let mut s = S::new().await;
    let writes: Vec<_> = (1..=3).map(|key| s.put_row(key, None, plain(key), vec![s.set(PLAIN, s.dense(key as u8))])).collect();
    let plan = s.f.queued_plan(20, &writes);
    s.f.prepare(&plan, writes).await;
    assert_eq!(s.count(VECTOR).await, 0);
    assert!(s.count(STAGED).await >= 6);
    // Abandoned (not resolved yet): no reader, copy or collector sees a vector.
    for key in 1..=3 {
        let row = s.f.machine().read_search_row(ts(25), s.f.key(key), 1 << 20).await.unwrap();
        assert!(matches!(row.payload().unwrap(), SearchRow::Blocked { .. }));
        let row = s.f.machine().read_search_row(ts(15), s.f.key(key), 1 << 20).await.unwrap();
        let SearchRow::Row { version, search, .. } = row.payload().unwrap() else { panic!() };
        assert!(version.is_none() && search.is_none());
    }
    let page = s.f.machine().change_page(s.last(), 8, 1 << 20).await.unwrap();
    assert!(matches!(page.payload().unwrap(), ChangePage::Changes { entries, .. } if entries.is_empty()));
    assert_eq!(s.f.machine().embed_records(None, 128).await.unwrap().len(), 0);
    let key = s.f.key(1);
    let raw = s
        .f
        .machine()
        .store()
        .read(BUDGET, move |view| {
            let found = crate::oltp::mvcc::store::row::unreferenced_in(view, &key)?;
            view.encode(&found)
        })
        .await
        .unwrap();
    let found: Vec<(u16, u64)> = serde_json::from_slice(raw.as_slice()).unwrap();
    assert_eq!(found, Vec::new());
    // A split cannot freeze, and a floor cannot close, over it.
    assert_eq!(s.close(30).await, refused(Fault::Incomplete));
    // Abort in pages of one: staged records of later ordinals live until their page.
    seal(&mut s, &plan).await;
    let reply = resolve(&mut s, &plan, abort_decision(), 1).await;
    assert!(matches!(reply, ParticipantReply::Resolved { complete: false, next_ordinal: 1 }), "{reply:?}");
    assert!(s.count(STAGED).await >= 4);
    for _ in 0..2 {
        resolve(&mut s, &plan, abort_decision(), 1).await;
    }
    assert_eq!(s.count(STAGED).await, 0, "every staged record is collected by the abort");
    assert_eq!(s.count(VECTOR).await, 0);
    for key in 1..=3 {
        assert_eq!(s.get(key).await, Got::default());
    }
    // A commit after an abort of the same transaction is refused.
    let reply = resolve(&mut s, &plan, commit_at(40), 128).await;
    assert!(!matches!(reply, ParticipantReply::Resolved { complete: true, .. }) || s.count(VECTOR).await == 0, "{reply:?}");
    assert_eq!(s.get(1).await, Got::default());
}

// ---------------------------------------------------------------------------
// (5) Embedding records
// ---------------------------------------------------------------------------

/// The source text goes a, b, a (and delete, insert): a report made for the first
/// text matches nothing, whatever it names.
#[tokio::test]
async fn adv_vec_g07_aba_of_the_source_text_matches_no_old_report() {
    for via_delete in [false, true] {
        let mut s = S::new().await;
        let put = |s: &mut S, expected: Option<u64>, at: u64, body: Option<&str>| {
            let write = s.write(1, expected, body.map(|body| row(1, body)), Vec::new());
            (at, write)
        };
        let (at, write) = put(&mut s, None, 10, Some("a"));
        s.direct(at, write).await;
        let (at, write) = if via_delete { put(&mut s, Some(10), 11, None) } else { put(&mut s, Some(10), 11, Some("b")) };
        s.direct(at, write).await;
        let (at, write) = put(&mut s, Some(11), 12, Some("a"));
        s.direct(at, write).await;
        assert_eq!(s.get(1).await.source[&EMBEDDED], 12);
        // The embedding of the first "a", in every spelling.
        for (expected, source) in [(10, 10), (12, 10), (11, 10), (12, 11)] {
            assert_eq!(s.embed(s.set_vector(1, expected, source, 20)).await, SearchReply::Obsolete, "{expected} {source}");
        }
        assert_eq!(s.fail(1, 10, 10, 0).await, SearchReply::Unmatched);
        assert_eq!(s.fail(1, 12, 10, 0).await, SearchReply::Unmatched);
        assert_eq!(s.fail(1, 10, 12, 0).await, SearchReply::Unmatched);
        assert_eq!(s.fail(1, 12, 12, 0).await, SearchReply::Counted { attempts: 1 });
        // The second report of the same attempt counts nothing.
        assert_eq!(s.fail(1, 12, 12, 0).await, SearchReply::Unmatched);
        assert_eq!(s.embed(s.set_vector(1, 12, 12, 21)).await, SearchReply::Set { commit_ts: ts(21) });
        // The record is gone, so a late report matches nothing, and the same
        // embedding delivered again took effect already.
        assert_eq!(s.fail(1, 12, 12, 1).await, SearchReply::Unmatched);
        assert_eq!(s.embed(s.set_vector(1, 21, 12, 22)).await, SearchReply::AlreadySet);
        assert_eq!(s.count(EMBED).await, 0);
        assert_eq!(s.get(1).await.vectors[&EMBEDDED], (21, vec![21; 8]));
    }
}

/// `'e'` is one key for a pending and a failure record: every transition between
/// them, and the late reports around a requeue.
#[tokio::test]
async fn adv_vec_g07_pending_and_failure_share_one_key_and_every_late_report_misses() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), Vec::new());
    s.direct(10, write).await;
    // A requeue of a record that is not failed matches nothing.
    assert_eq!(s.f.submit(requeue(&s, 1, 10, 10)).await, ParticipantReply::Search(SearchReply::Unmatched));
    for attempt in 0..EMBED_ATTEMPTS - 1 {
        assert_eq!(s.fail(1, 10, 10, attempt).await, SearchReply::Counted { attempts: attempt + 1 });
    }
    assert_eq!(s.fail(1, 10, 10, EMBED_ATTEMPTS - 1).await, SearchReply::EmbedFailed { attempts: EMBED_ATTEMPTS });
    assert_eq!(s.count(EMBED).await, 1);
    // A metadata update that keeps the text keeps the failure.
    let write = s.write(1, Some(10), Some(serde_json::json!({"id": 1, "body": "a", "v": 2})), Vec::new());
    s.direct(11, write).await;
    assert!(s.get(1).await.records[&EMBEDDED].failed.is_some());
    // Late reports of every attempt miss a failure.
    for attempt in 0..=EMBED_ATTEMPTS + 1 {
        assert_eq!(s.fail(1, 10, 10, attempt).await, SearchReply::Unmatched, "{attempt}");
    }
    // Wrong identity requeues miss; the right one goes on from the attempt.
    for (source, id) in [(10, 11), (11, 10), (11, 11)] {
        assert_eq!(s.f.submit(requeue(&s, 1, source, id)).await, ParticipantReply::Search(SearchReply::Unmatched));
    }
    assert_eq!(
        s.f.submit(requeue(&s, 1, 10, 10)).await,
        ParticipantReply::Search(SearchReply::Requeued { attempts: EMBED_ATTEMPTS, limit: 2 * EMBED_ATTEMPTS })
    );
    assert_eq!(s.f.submit(requeue(&s, 1, 10, 10)).await, ParticipantReply::Search(SearchReply::Unmatched));
    // A report of the attempt that failed it, delivered again after the requeue.
    assert_eq!(s.fail(1, 10, 10, EMBED_ATTEMPTS - 1).await, SearchReply::Unmatched);
    for attempt in EMBED_ATTEMPTS..2 * EMBED_ATTEMPTS - 1 {
        assert_eq!(s.fail(1, 10, 10, attempt).await, SearchReply::Counted { attempts: attempt + 1 });
    }
    assert_eq!(s.fail(1, 10, 10, 2 * EMBED_ATTEMPTS - 1).await, SearchReply::EmbedFailed { attempts: 2 * EMBED_ATTEMPTS });
    assert_eq!(
        s.f.submit(requeue(&s, 1, 10, 10)).await,
        ParticipantReply::Search(SearchReply::Requeued { attempts: 2 * EMBED_ATTEMPTS, limit: 3 * EMBED_ATTEMPTS })
    );
    // An embedding applies to a failed record too and clears it.
    assert_eq!(s.embed(s.set_vector(1, 11, 10, 30)).await, SearchReply::Set { commit_ts: ts(30) });
    assert_eq!(s.count(EMBED).await, 0);
    assert_eq!(s.f.submit(requeue(&s, 1, 10, 10)).await, ParticipantReply::Search(SearchReply::Unmatched));
}

/// A metadata update that read the row before an embedding landed, through the
/// queue and directly, never drops the vector or the new version.
#[tokio::test]
async fn adv_vec_g07_a_metadata_update_racing_an_embedding_loses_or_keeps_the_vector() {
    // The embedding lands first: the update that read the older version conflicts.
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), Vec::new());
    s.direct(10, write).await;
    assert_eq!(s.embed(s.set_vector(1, 10, 10, 11)).await, SearchReply::Set { commit_ts: ts(11) });
    let stale = s.write(1, Some(10), Some(serde_json::json!({"id": 1, "body": "a", "v": 1})), Vec::new());
    assert_eq!(s.direct(12, stale).await, rejected(Fault::Conflict));
    let writes = vec![s.f.queued_put(1, Some(10), serde_json::json!({"id": 1, "body": "a", "v": 1}))];
    let plan = s.f.queued_plan(10, &writes);
    let reply = s.f.prepare(&plan, writes).await;
    assert_eq!(reply, ParticipantReply::Rejected(Fault::Conflict));
    assert_eq!(s.get(1).await.vectors[&EMBEDDED], (11, vec![11; 8]));
    // The update lands first: an embedding that read the older version conflicts,
    // and a retry on the new version applies and keeps the update.
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), Vec::new());
    s.direct(10, write).await;
    let write = s.write(1, Some(10), Some(serde_json::json!({"id": 1, "body": "a", "v": 1})), Vec::new());
    s.direct(11, write).await;
    assert_eq!(s.embed(s.set_vector(1, 10, 10, 12)).await, refused(Fault::Conflict));
    assert_eq!(s.embed(s.set_vector(1, 11, 10, 12)).await, SearchReply::Set { commit_ts: ts(12) });
    let got = s.get(1).await;
    assert_eq!((got.value, got.version), (Some(serde_json::json!({"id": 1, "body": "a", "v": 1})), Some(12)));
    // An update in a prepared transaction blocks the embedding until it resolves.
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), Vec::new());
    s.direct(10, write).await;
    let writes = vec![s.f.queued_put(1, Some(10), serde_json::json!({"id": 1, "body": "a", "v": 1}))];
    let plan = s.f.queued_plan(10, &writes);
    s.f.prepare(&plan, writes).await;
    assert_eq!(s.embed(s.set_vector(1, 10, 10, 12)).await, refused(Fault::Incomplete));
    seal(&mut s, &plan).await;
    resolve(&mut s, &plan, commit_at(13), 128).await;
    assert_eq!(s.embed(s.set_vector(1, 13, 10, 14)).await, SearchReply::Set { commit_ts: ts(14) });
    assert_eq!(s.get(1).await.source[&EMBEDDED], 10, "the source version is the text's, not the update's");
}

// ---------------------------------------------------------------------------
// (6) Copies
// ---------------------------------------------------------------------------

/// A replica snapshot is one engine snapshot: writes after its cut, including
/// vectors, embeddings and closes, never leak into its file.
#[tokio::test]
async fn adv_vec_g10_a_replica_snapshot_is_its_cut_under_concurrent_writes() {
    let mut s = S::new().await;
    for key in 0..4i64 {
        let write = s.write(key, None, Some(row(key, "a")), vec![s.set(PLAIN, s.dense(key as u8))]);
        s.direct(10 + key as u64, write).await;
    }
    s.embed(s.set_vector(1, 11, 11, 20)).await;
    let cut = s.f.machine().store().cut_snapshot(BUDGET, true).await.unwrap();
    let at_cut = s.f.dump().await;
    let applied_at_cut = cut.header.applied;
    // Writes of every kind after the cut, before its file is made.
    for key in 0..4i64 {
        let head = s.get(key).await.version;
        let write = s.write(key, head, Some(row(key, "b")), vec![s.set(PLAIN, s.dense(99))]);
        s.direct(30 + key as u64, write).await;
    }
    s.close(40).await;
    s.fail(2, 12, 12, 0).await;
    let write = s.write(9, None, Some(row(9, "z")), vec![s.set(SPARSE, s.value(VectorKind::Sparse, &[3; 4000]))]);
    s.direct(50, write).await;
    let snapshot = cut.file(BUDGET).await.unwrap();
    let dir = Dir(std::env::temp_dir().join(format!("adv-{}", nabedra_rt::ids::uuid_v4())));
    let mut identity = s.f.pool_identity.clone();
    identity.pool_id = nabedra_rt::ids::uuid_v4();
    let pool = OltpPool::open_development(&dir.0, identity, s.f.shared().clone()).unwrap();
    let store = pool.group(s.f.group.clone()).await.unwrap();
    store.install_snapshot(snapshot, BUDGET).await.unwrap();
    let follower = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: s.f.shared().clone(),
        index: applied_at_cut.unwrap().index + 1,
    };
    assert_eq!(follower.dump().await, at_cut);
    assert_eq!(follower.machine.embed_records(None, 128).await.unwrap().len(), 3);
}

/// A snapshot that holds a prepared transaction: the follower resolves it, with
/// the vectors it staged, to exactly what the leader gets.
#[tokio::test]
async fn adv_vec_g10_a_snapshot_with_a_prepared_transaction_resolves_on_the_follower() {
    let mut s = S::new().await;
    let writes = vec![
        s.put_row(1, None, plain(1), vec![s.set(PLAIN, s.dense(1))]),
        s.put_row(2, None, plain(2), vec![s.set(SPARSE, s.value(VectorKind::Sparse, &[2; 16]))]),
    ];
    let plan = s.f.queued_plan(20, &writes);
    s.f.prepare(&plan, writes).await;
    seal(&mut s, &plan).await;
    let snapshot = s.f.machine().store().export_snapshot(BUDGET).await.unwrap();
    let dir = Dir(std::env::temp_dir().join(format!("adv-{}", nabedra_rt::ids::uuid_v4())));
    let mut identity = s.f.pool_identity.clone();
    identity.pool_id = nabedra_rt::ids::uuid_v4();
    let pool = OltpPool::open_development(&dir.0, identity, s.f.shared().clone()).unwrap();
    let store = pool.group(s.f.group.clone()).await.unwrap();
    store.install_snapshot(snapshot, BUDGET).await.unwrap();
    let mut follower = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: s.f.shared().clone(),
        index: s.f.replica.index,
    };
    let proof = SealedStatusDecision::seal(DecisionStatement {
        txn: plan.txn,
        request: plan.request,
        manifest: plan.manifest,
        decided_at: position(1),
        decision: commit_at(30),
    })
    .unwrap();
    let command = ParticipantCommand::Resolve { authority: s.f.authority, proof, max_items: 128 };
    let leader = s.f.submit(command.clone()).await;
    assert_eq!(follower.submit(command).await, leader);
    assert_eq!(follower.dump().await, s.f.dump().await);
    assert_eq!(got(&follower.machine, s.f.key(1), 30).await.unwrap().vectors[&PLAIN], (30, vec![1; 8]));
    assert_eq!(s.count(STAGED).await, 0);
}

fn fence_for(s: &S, epoch: u64, columns: Vec<SearchColumn>) -> ParticipantCommand {
    s.fence_command(epoch, columns)
}
async fn image_list(s: &S, cut: u64) -> Vec<CommittedImage> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let request = OwnedCommand::new_owned(
            ImageRequest {
                read_ts: ts(cut),
                schema_epoch: 1,
                routing_epoch: 1,
                span: full_span(),
                after,
                max_items: 128,
                max_bytes: 64 << 10,
                search: None,
            },
            s.f.shared(),
            ByteClass::Data,
        )
        .unwrap();
        let page = s.f.machine().committed_image_page(request).await.unwrap();
        after = page.payload().unwrap().continuation().unwrap().cloned();
        let ImagePage::Images { images, exhausted, .. } = page.payload().unwrap() else { panic!() };
        all.extend(images.iter().cloned());
        if *exhausted {
            return all;
        }
    }
}
async fn restore_into(s: &S, images: &[CommittedImage], cut: u64) -> Replica {
    let mut target = s.f.group.clone();
    target.group_id = nabedra_rt::ids::uuid_v4();
    let store = s.f.pool.group(target.clone()).await.unwrap();
    let genesis = restore::ParticipantGenesis {
        restore_id: nabedra_rt::ids::uuid_v4(),
        source_cluster: s.f.group.cluster_id,
        target_group: target.clone(),
        catalog_group_id: s.f.catalog_group_id,
        schema_epoch: 1,
        routing_epoch: 1,
        span: full_span(),
        cut: ts(cut),
    };
    let images = images.to_vec();
    store
        .transaction(
            ApplyMeta { position: position(0), command_digest: digest(7), membership: None },
            BUDGET,
            move |tx| {
                tx.put_serialized(restore::RESTORE_GENESIS, &genesis)?;
                let mut sorted: Vec<_> = images.iter().collect();
                sorted.sort_by(|a, b| compare_keys(&a.key, &b.key).then(a.commit_ts.cmp(&b.commit_ts)));
                restore::install_rows(tx, &target, &full_span(), ts(cut), &sorted)?;
                Ok(Transition::Commit(tx.copy_output(b"null")?))
            },
        )
        .await
        .unwrap();
    let mut restored = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: s.f.shared().clone(),
        index: 1,
    };
    let fence = s.f.fence(1, 1, full_span());
    assert!(matches!(restored.submit(fence).await, ParticipantReply::Fenced(_)));
    restored.submit(s.f.activation()).await;
    restored
}

/// A restored range holds vectors before Control tells it the columns: nothing
/// writes, closes or embeds until its first SearchFence, and then all of it works.
#[tokio::test]
async fn adv_vec_g10_a_restored_range_takes_nothing_before_its_first_search_fence() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    let write = s.write(2, None, Some(row(2, "t")), Vec::new());
    s.direct(11, write).await;
    let images = image_list(&s, 20).await;
    let mut restored = restore_into(&s, &images, 20).await;
    let inactive = |fault| ParticipantReply::Search(SearchReply::Rejected(fault));
    let before = restored.dump().await;
    let commit = s.commit(30, vec![s.write(3, None, Some(plain(3)), Vec::new())]);
    assert_eq!(restored.submit(ParticipantCommand::Direct(commit.clone())).await, rejected(Fault::DirectInactive));
    let batch = DirectBatch { members: vec![commit] };
    let reply = restored.submit(ParticipantCommand::DirectBatch(batch)).await;
    assert!(
        matches!(reply, ParticipantReply::Direct(DirectReply::Batch(ref members)) if members == &[DirectMemberReply::Rejected(Fault::DirectInactive)]),
        "{reply:?}"
    );
    let closed = Closed { authority: s.f.authority, ts: ts(25), unix_ms: 1 };
    assert_eq!(restored.submit(ParticipantCommand::Closed(closed)).await, inactive(Fault::DirectInactive));
    let set = ParticipantCommand::SetVector(s.set_vector(2, 11, 11, 31));
    assert_eq!(restored.submit(set).await, inactive(Fault::DirectInactive));
    assert_eq!(restored.submit(requeue(&s, 2, 11, 11)).await, inactive(Fault::DirectInactive));
    let writes = vec![s.f.queued_put(3, None, plain(3))];
    let plan = s.f.queued_plan(25, &writes);
    let prepared = restored
        .submit(ParticipantCommand::PreparePage {
            authority: s.f.authority,
            txn: plan.txn,
            request: plan.request,
            manifest: plan.manifest,
            plan: plan.plan.clone(),
            page_index: 0,
            previous_root: plan.root,
            guards: Page::new(vec![
                lifecycle::VerifiedLifecycleGuard::model_for_test(
                    lifecycle::LifecycleBinding {
                        guard_id: nabedra_rt::ids::uuid_v4(),
                        catalog_group_id: s.f.catalog_group_id,
                        table_id: s.f.table,
                        lifecycle_version: None,
                        schema_epoch: 1,
                        txn: plan.txn,
                        request: plan.request,
                        read_ts: plan.plan.read_ts,
                        authority: s.f.authority,
                    },
                    lifecycle::GuardMode::Shared,
                )
                .into_sealed(),
            ])
            .unwrap(),
            writes: Page::new(writes).unwrap(),
        })
        .await;
    assert_eq!(prepared, ParticipantReply::Rejected(Fault::Incomplete));
    assert_eq!(restored.dump().await, before, "nothing was written");
    // No vector is served before the range knows its columns, since it cannot be
    // checked against them; the rows and vectors are there once it does.
    let unknown = got(&restored.machine, s.f.key(1), 20).await.err().unwrap();
    assert!(unknown.message.contains("not known yet"), "{}", unknown.message);
    // The first fence names the columns; now reads, writes, closes and embeddings apply.
    let fence = fence_for(&s, 1, columns());
    assert!(matches!(search_reply(restored.submit(fence).await), SearchReply::Fenced { epoch: 1, .. }));
    assert_eq!(got(&restored.machine, s.f.key(1), 20).await.unwrap().vectors[&PLAIN], (10, vec![1; 8]));
    let direct = match search_reply(restored.submit(fence_for(&s, 2, columns())).await) {
        SearchReply::Fenced { epoch: 2, direct: Some(direct) } => direct,
        other => panic!("{other:?}"),
    };
    let mut commit = s.commit(30, vec![s.write(3, None, Some(plain(3)), vec![s.set(PLAIN, s.dense(3))])]);
    commit.schema_epoch = direct.epoch;
    assert_eq!(restored.submit(ParticipantCommand::Direct(commit)).await, applied(30));
    let closed = Closed { authority: s.f.authority, ts: ts(35), unix_ms: 1 };
    assert_eq!(
        search_reply(restored.submit(ParticipantCommand::Closed(closed)).await),
        SearchReply::Closed { floor: ts(35) }
    );
    // The derived record of row 2 (text, no vector) is the restore's own.
    let records = restored.machine.embed_records(None, 128).await.unwrap();
    assert_eq!(records.len(), 2, "both rows have a text and no embedding");
    assert_eq!(records.iter().map(|item| item.record.source_version).collect::<Vec<_>>(), vec![ts(10), ts(11)]);
    assert!(records.iter().all(|item| item.record.id > ts(20)));
    let mut set = s.set_vector(2, 11, 11, 40);
    set.schema_epoch = direct.epoch;
    assert_eq!(search_reply(restored.submit(ParticipantCommand::SetVector(set)).await), SearchReply::Set { commit_ts: ts(40) });
}

/// A copy installs only vectors that fit their column: the kind and length the
/// column has, under a column the group has.
#[tokio::test]
async fn adv_vec_g10_a_copy_never_installs_a_vector_that_does_not_fit_its_column() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    type Work = Box<dyn Fn(&mut RowImage, &S)>;
    let cases: Vec<(&'static str, Work)> = vec![
        ("control", Box::new(|_, _| {})),
        ("sparse bytes for a dense column", Box::new(|search, s| search.vectors[0].value = s.value(VectorKind::Sparse, &[1; 8]))),
        ("three floats for two", Box::new(|search, s| search.vectors[0].value = s.value(VectorKind::Dense, &[1; 12]))),
        ("no floats", Box::new(|search, s| search.vectors[0].value = s.value(VectorKind::Dense, &[]))),
        (
            "a column the group has not",
            Box::new(|search, s| {
                search.blobs.0 = std::collections::BTreeMap::from([(9, ts(10))]);
                search.vectors[0] = crate::oltp::mvcc::store::row::VectorImage { column: 9, value: s.dense(1) };
            }),
        ),
        (
            "a vector for an embedded column",
            Box::new(|search, s| {
                search.blobs.0.insert(EMBEDDED, ts(10));
                search.vectors.push(crate::oltp::mvcc::store::row::VectorImage { column: EMBEDDED, value: s.dense(2) });
            }),
        ),
    ];
    let _ = &s;
    for (name, work) in cases {
        let mut s2 = S::new().await;
        let write = s2.write(1, None, Some(row(1, "a")), vec![s2.set(PLAIN, s2.dense(1))]);
        s2.direct(10, write).await;
        let mut split = freeze_at(&mut s2, 20).await;
        let page = s2
            .f
            .machine()
            .committed_image_page(
                OwnedCommand::new_owned(split.bindings[0].image_request(None, 1 << 20), s2.f.shared(), ByteClass::Data).unwrap(),
            )
            .await
            .unwrap();
        let ImagePage::Images { after, search, images, .. } = page.payload().unwrap() else { panic!() };
        let mut images = images.to_vec();
        work(images.iter_mut().find_map(|image| image.search.as_mut()).unwrap(), &s2);
        let binding = split.bindings[0].clone();
        let command = ParticipantCommand::SplitImportPage {
            authority: s2.f.authority,
            page: Box::new(SplitPage {
                binding: binding.clone(),
                page_index: 0,
                previous_root: lineage::initial_root(&binding).unwrap(),
                previous_after: None,
                images: Page::new(images).unwrap(),
                after: after.clone(),
                exhausted: true,
                search: search.clone(),
            }),
        };
        let before = split.children[0].dump().await;
        let reply = split.children[0].submit(command).await;
        if name == "control" {
            assert!(matches!(reply, ParticipantReply::SplitImported { complete: true, .. }), "{reply:?}");
            continue;
        }
        assert_eq!(reply, ParticipantReply::Rejected(Fault::InvalidBinding), "{name}");
        assert_eq!(split.children[0].dump().await, before, "{name}");
    }
}
struct Frozen {
    bindings: [SplitBinding; 2],
    children: Vec<Replica>,
}
async fn freeze_at(s: &mut S, cut: u64) -> Frozen {
    let bindings = split_bindings(&s.f, nabedra_rt::ids::uuid_v4(), cut);
    let frozen = s
        .f
        .submit(ParticipantCommand::SplitFreeze { authority: s.f.authority, children: Box::new(bindings.clone()) })
        .await;
    assert!(matches!(frozen, ParticipantReply::SplitSourceFrozen(_)), "{frozen:?}");
    let mut children = Vec::new();
    for binding in &bindings {
        let store = s.f.pool.group(binding.target.clone()).await.unwrap();
        let mut child = Replica { machine: ParticipantMachine::new(store).unwrap(), shared: s.f.shared().clone(), index: 0 };
        let fence = s.f.fence(1, 2, binding.target_span.clone());
        assert!(matches!(child.submit(fence).await, ParticipantReply::Fenced(_)));
        child.submit(s.f.activation()).await;
        let stage = ParticipantCommand::SplitStage { authority: s.f.authority, binding: Box::new(binding.clone()) };
        assert_eq!(child.submit(stage).await, ParticipantReply::SplitStaged);
        children.push(child);
    }
    Frozen { bindings, children }
}

/// A split is refused while a transaction holds an intent on a search range.
#[tokio::test]
async fn adv_vec_g10_a_split_waits_for_staged_transactions() {
    let mut s = S::new().await;
    let writes = vec![s.put_row(1, None, plain(1), vec![s.set(PLAIN, s.dense(1))])];
    let plan = s.f.queued_plan(20, &writes);
    s.f.prepare(&plan, writes).await;
    let bindings = split_bindings(&s.f, nabedra_rt::ids::uuid_v4(), 30);
    let before = s.f.dump().await;
    let reply = s
        .f
        .submit(ParticipantCommand::SplitFreeze { authority: s.f.authority, children: Box::new(bindings) })
        .await;
    assert_eq!(reply, ParticipantReply::Rejected(Fault::Incomplete));
    assert_eq!(s.f.dump().await, before);
}

// ---------------------------------------------------------------------------
// (7) Crash and power loss at every write of every new command
// ---------------------------------------------------------------------------

fn prepared_commands(s: &S, plan: &QueuedPlan, writes: Vec<PreparedWrite>, decision: TxnDecision) -> Vec<ParticipantCommand> {
    let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
        lifecycle::LifecycleBinding {
            guard_id: nabedra_rt::ids::uuid_v4(),
            catalog_group_id: s.f.catalog_group_id,
            table_id: s.f.table,
            lifecycle_version: None,
            schema_epoch: 1,
            txn: plan.txn,
            request: plan.request,
            read_ts: plan.plan.read_ts,
            authority: s.f.authority,
        },
        lifecycle::GuardMode::Shared,
    )
    .into_sealed();
    vec![
        ParticipantCommand::PreparePage {
            authority: s.f.authority,
            txn: plan.txn,
            request: plan.request,
            manifest: plan.manifest,
            plan: plan.plan.clone(),
            page_index: 0,
            previous_root: plan.root,
            guards: Page::new(vec![guard]).unwrap(),
            writes: Page::new(writes).unwrap(),
        },
        ParticipantCommand::Seal {
            authority: s.f.authority,
            txn: plan.txn,
            manifest: plan.manifest,
            result_digest: digest(4),
        },
        ParticipantCommand::Resolve {
            authority: s.f.authority,
            proof: SealedStatusDecision::seal(DecisionStatement {
                txn: plan.txn,
                request: plan.request,
                manifest: plan.manifest,
                decided_at: position(1),
                decision,
            })
            .unwrap(),
            max_items: 128,
        },
    ]
}

fn failed_command(s: &S, key: i64, source: u64, attempt: u32) -> ParticipantCommand {
    ParticipantCommand::EmbedFailed(EmbedFailed {
        authority: s.f.authority,
        table_id: s.f.table,
        primary_key: Key::Int64(key),
        column: EMBEDDED,
        source_version: ts(source),
        id: ts(source),
        attempt,
        reason: Text::try_from("no").unwrap(),
    })
}

/// One log with every new command, applied on a reference group, with its replies.
async fn every_command() -> (S, Vec<ParticipantCommand>, Vec<ParticipantReply>) {
    let mut s = S::plain(Some(&FaultFs::new())).await;
    let mut log: Vec<ParticipantCommand> = Vec::new();
    let mut replies: Vec<ParticipantReply> = Vec::new();
    macro_rules! run {
        ($command:expr) => {{
            let command: ParticipantCommand = $command;
            replies.push(s.f.submit(command.clone()).await);
            log.push(command);
        }};
    }
    run!(s.fence_command(1, columns()));
    let ParticipantReply::Search(SearchReply::Fenced { epoch, direct }) = replies[0].clone() else { panic!() };
    s.search_epoch = epoch;
    s.epoch = direct.unwrap().epoch;
    let sparse = s.value(VectorKind::Sparse, &[5; 40]);
    let multi = s.value(VectorKind::Multi, &[6; 8]);
    run!(ParticipantCommand::Direct(s.commit(
        101,
        vec![s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1)), s.set(SPARSE, sparse)])]
    )));
    run!(ParticipantCommand::Direct(s.commit(102, vec![s.write(2, None, Some(row(2, "b")), vec![s.set(MULTI, multi.clone())])])));
    run!(ParticipantCommand::SetVector(s.set_vector(1, 101, 101, 103)));
    run!(failed_command(&s, 2, 102, 0));
    let members = vec![
        s.commit(104, vec![s.write(3, None, Some(row(3, "c")), vec![s.set(PLAIN, s.dense(3))])]),
        s.commit(105, vec![s.write(2, Some(99), Some(row(2, "x")), Vec::new())]),
        s.commit(106, vec![s.write(1, Some(103), Some(row(1, "a2")), vec![s.set(PLAIN, s.dense(9))])]),
    ];
    run!(ParticipantCommand::DirectBatch(DirectBatch { members }));
    let writes = vec![s.put_row(3, Some(104), row(3, "c"), vec![s.set(PLAIN, s.dense(4))])];
    let plan = s.f.queued_plan(107, &writes);
    for command in prepared_commands(&s, &plan, writes, commit_at(108)) {
        run!(command);
    }
    let writes = vec![s.put_row(2, Some(102), row(2, "b"), vec![s.set(MULTI, multi.clone())])];
    let plan = s.f.queued_plan(108, &writes);
    for command in prepared_commands(&s, &plan, writes, abort_decision()) {
        run!(command);
    }
    run!(ParticipantCommand::Closed(Closed { authority: s.f.authority, ts: ts(109), unix_ms: 1_090 }));
    run!(ParticipantCommand::Direct(s.commit(110, vec![s.write(3, Some(108), None, Vec::new())])));
    run!(failed_command(&s, 2, 102, 1));
    run!(requeue(&s, 2, 102, 102));
    let narrower = vec![column(PLAIN, VectorKind::Dense, 2), embedded(EMBEDDED)];
    run!(s.fence_command(2, narrower));
    let ParticipantReply::Search(SearchReply::Fenced { direct, .. }) = replies.last().unwrap().clone() else { panic!() };
    s.epoch = direct.unwrap().epoch;
    run!(ParticipantCommand::Direct(s.commit(111, vec![s.write(1, Some(106), Some(row(1, "a3")), vec![s.set(PLAIN, s.dense(5))])])));
    run!(ParticipantCommand::Direct(s.commit(112, vec![s.write(2, Some(102), Some(row(2, "b2")), Vec::new())])));
    for (n, reply) in replies.iter().enumerate() {
        assert!(!matches!(reply, ParticipantReply::Rejected(_)), "entry {n}: {reply:?}");
    }
    (s, log, replies)
}

#[tokio::test]
async fn adv_oltp_g01_a_crash_at_every_write_of_every_entry_replays_to_the_same_state() {
    let (reference, log, replies) = every_command().await;
    let group = reference.f.group.clone();
    let expected = stored(&reference.f.pool, group.group_id);
    let feed = seen(&reference.changes(FEED_START, 64).await);
    assert!(feed.len() >= 8);
    let mut entries = vec![reference.f.first_fence.clone(), reference.f.activation()];
    entries.extend(log);
    let mut crashes = 0;
    for deferred in [false, true] {
        for power in [false, true] {
            for entry in 2..entries.len() {
                for point in 1..=80u64 {
                    let mut fs = FaultFs::new();
                    let dir = Dir(std::env::temp_dir().join(format!("adv-crash-{}", nabedra_rt::ids::uuid_v4())));
                    std::fs::create_dir_all(&dir.0).unwrap();
                    fs.create_dir_all_durable(&dir.0);
                    let mut identity = reference.f.pool_identity.clone();
                    identity.worker_id = nabedra_rt::ids::uuid_v4();
                    identity.pool_id = nabedra_rt::ids::uuid_v4();
                    let crashed = {
                        let shared = shared();
                        let pool = open_on(&dir, &fs, &identity, &shared);
                        let store = pool.group(group.clone()).await.unwrap();
                        let machine = ParticipantMachine::new(store.clone()).unwrap();
                        let token = deferred.then(|| store.defer_apply_sync(&store).unwrap());
                        for index in 0..entry {
                            apply_entry(&machine, &shared, index as u64, &entries[index]).await.unwrap();
                        }
                        fs.crash_after(point);
                        let result = apply_entry(&machine, &shared, entry as u64, &entries[entry]).await;
                        let crashed = fs.is_crashed();
                        assert!(crashed || result.is_ok(), "entry {entry}: an error without a fault: {result:?}");
                        drop((token, machine, store, pool));
                        crashed
                    };
                    if !crashed {
                        break;
                    }
                    crashes += 1;
                    fs = if power { fs.power_loss(point ^ entry as u64) } else { fs.crash() };
                    let shared = shared();
                    let pool = open_on(&dir, &fs, &identity, &shared);
                    let store = pool.group(group.clone()).await.unwrap();
                    let machine = ParticipantMachine::new(store.clone()).unwrap();
                    let token = deferred.then(|| store.defer_apply_sync(&store).unwrap());
                    let applied = store.applied_state(BUDGET).await.unwrap();
                    let next = applied.map_or(0, |applied| applied.position.index + 1) as usize;
                    // The last applied entry may be delivered again.
                    let first = if point % 2 == 0 { next.saturating_sub(1) } else { next };
                    for index in first..entries.len() {
                        let reply = apply_entry(&machine, &shared, index as u64, &entries[index]).await;
                        let reply = reply.unwrap_or_else(|error| {
                            panic!("deferred {deferred} power {power} entry {entry} point {point} index {index}: {error:?}")
                        });
                        if index >= 2 {
                            assert_eq!(
                                reply,
                                replies[index - 2],
                                "deferred {deferred} power {power} entry {entry} point {point} index {index}"
                            );
                        }
                    }
                    let state = stored(&pool, group.group_id);
                    assert!(state == expected, "deferred {deferred} power {power} entry {entry} point {point}");
                    assert_eq!(
                        seen(&changes(&machine, FEED_START, 64).await.unwrap()),
                        feed,
                        "feed: entry {entry} point {point}"
                    );
                    drop((token, machine, store, pool));
                }
            }
        }
    }
    assert!(crashes > 100, "{crashes}");
}

// ---------------------------------------------------------------------------
// (8) Older and newer entries
// ---------------------------------------------------------------------------

/// A field or variant this build does not know is refused whole, at every level
/// an older build would meet it, and a tampered row version too.
#[tokio::test]
async fn adv_compat_a_field_or_variant_nobody_knows_is_refused_never_dropped() {
    let s = S::new().await;
    let write = s.write(1, None, Some(plain(1)), vec![s.set(PLAIN, s.dense(1))]);
    let commit = s.commit(10, vec![write]);
    let good = serde_json::to_value(ParticipantCommand::Direct(commit)).unwrap();
    assert!(serde_json::from_value::<ParticipantCommand>(good.clone()).is_ok());
    let paths: [&[&str]; 5] = [
        &["direct"],
        &["direct", "writes", "0"],
        &["direct", "writes", "0", "search"],
        &["direct", "writes", "0", "search", "vectors", "0"],
        &["direct", "writes", "0", "search", "vectors", "0", "value"],
    ];
    for path in paths {
        let mut doc = good.clone();
        let mut at = &mut doc;
        for step in path {
            at = match step.parse::<usize>() {
                Ok(index) => &mut at[index],
                Err(_) => &mut at[*step],
            };
        }
        at["unknown_field"] = serde_json::json!(1);
        assert!(serde_json::from_value::<ParticipantCommand>(doc).is_err(), "{path:?}");
    }
    for variant in ["set_vecto", "quantize", "closed_at", "search_fence_v2"] {
        let doc = serde_json::json!({ variant: {} });
        assert!(serde_json::from_value::<ParticipantCommand>(doc).is_err(), "{variant}");
    }
    // A row version with a field of a newer build is an explicit error for readers.
    let mut s = S::new().await;
    let write = s.write(1, None, Some(plain(1)), Vec::new());
    s.direct(10, write).await;
    let (key, _) = s.records(VALUE).await.remove(0);
    s.tamper(move |tx| tx.put(&key, br#"{"value":{"id":1},"future":1}"#)).await;
    assert!(got(s.f.machine(), s.f.key(1), 10).await.is_err());
}

/// The new commands, one JSON line each, for a build that does not know them.
#[tokio::test]
async fn adv_compat_dump_new_entries() {
    let Ok(path) = std::env::var("ADV_NEW_ENTRIES_OUT") else {
        return;
    };
    let (_, log, _) = every_command().await;
    let lines: Vec<String> = log.iter().map(|command| serde_json::to_string(command).unwrap()).collect();
    std::fs::write(path, lines.join("\n")).unwrap();
}

/// A transaction of two pages stages by ordinal across pages: no row gets the
/// vector of a row of the other page, and the abort collects both pages.
#[tokio::test]
async fn adv_vec_g07_a_two_page_transaction_stages_each_vector_under_its_own_ordinal() {
    for (abort, step) in [(false, 128u16), (false, 1), (true, 1), (true, 3)] {
        let mut s = S::new().await;
        let pages: Vec<Vec<PreparedWrite>> = vec![
            (1..=2).map(|key| s.put_row(key, None, plain(key), vec![s.set(PLAIN, s.dense(key as u8))])).collect(),
            (3..=4).map(|key| s.put_row(key, None, plain(key), vec![s.set(PLAIN, s.dense(key as u8))])).collect(),
        ];
        let txn = TxnId { request_id: nabedra_rt::ids::uuid_v4(), attempt: 0 };
        let request = RequestBinding {
            run_id: s.f.authority.run_id,
            request_id: txn.request_id,
            digest: digest(1),
            accepted_context: digest(2),
        };
        let manifest = ManifestRef { status_group_id: nabedra_rt::ids::uuid_v4(), digest: digest(3), participant_count: 1 };
        let mut plan = ParticipantPlan {
            group: s.f.group.clone(),
            schema_epoch: 1,
            routing_epoch: 1,
            read_ts: ts(20),
            write_root: digest(0),
            write_count: 4,
            page_count: 2,
        };
        let mut roots = vec![initial_root(txn, &plan).unwrap()];
        for (index, page) in pages.iter().enumerate() {
            roots.push(page_root(roots[index], index as u64, page).unwrap());
        }
        plan.write_root = roots[2];
        for (index, page) in pages.iter().enumerate() {
            let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
                lifecycle::LifecycleBinding {
                    guard_id: nabedra_rt::ids::uuid_v4(),
                    catalog_group_id: s.f.catalog_group_id,
                    table_id: s.f.table,
                    lifecycle_version: None,
                    schema_epoch: 1,
                    txn,
                    request,
                    read_ts: ts(20),
                    authority: s.f.authority,
                },
                lifecycle::GuardMode::Shared,
            )
            .into_sealed();
            let reply = s
                .f
                .submit(ParticipantCommand::PreparePage {
                    authority: s.f.authority,
                    txn,
                    request,
                    manifest,
                    plan: plan.clone(),
                    page_index: index as u64,
                    previous_root: roots[index],
                    guards: Page::new(vec![guard]).unwrap(),
                    writes: Page::new(page.clone()).unwrap(),
                })
                .await;
            assert!(matches!(reply, ParticipantReply::PreparedPage { .. }), "{reply:?}");
        }
        let queued = QueuedPlan { txn, request, manifest, plan, root: roots[0] };
        seal(&mut s, &queued).await;
        let decision = if abort { abort_decision() } else { commit_at(30) };
        for _ in 0..4 {
            resolve(&mut s, &queued, decision, step).await;
        }
        if abort {
            assert_eq!(s.count(STAGED).await, 0, "step {step}");
            assert_eq!(s.count(VECTOR).await, 0);
            for key in 1..=4 {
                assert_eq!(s.get(key).await, Got::default());
            }
        } else {
            assert_eq!(s.count(STAGED).await, 0);
            for key in 1..=4 {
                assert_eq!(s.get(key).await.vectors[&PLAIN], (30, vec![key as u8; 8]), "row {key} step {step}");
            }
        }
    }
}

/// Backfill gives an existing row a source version and a record only when it has
/// none of the vector, the record or the failure of this incarnation.
#[tokio::test]
async fn adv_vec_g07_backfill_touches_only_rows_that_wait_for_nothing() {
    let mut s = S::new().await;
    // Row 1 has a vector, row 2 a failure, row 3 a pending record, row 4 no text,
    // row 5 nothing yet (it was written before the column existed).
    let before_column = vec![column(PLAIN, VectorKind::Dense, 2)];
    let mut s0 = S::plain(Some(&FaultFs::new())).await;
    assert!(matches!(s0.fence(before_column).await, SearchReply::Fenced { .. }));
    for key in 1..=5 {
        let body = if key == 4 { serde_json::json!({"id": key, "body": null}) } else { row(key, "t") };
        let write = s0.write(key, None, Some(body), Vec::new());
        assert_eq!(s0.direct(10 + key as u64, write).await, applied(10 + key as u64));
    }
    assert_eq!(s0.count(EMBED).await, 0, "no embedded column yet");
    assert!(matches!(s0.fence(columns()).await, SearchReply::Fenced { .. }));
    for key in 1..=5 {
        let head = s0.get(key).await;
        let mut write = s0.write(key, head.version, head.value.map(|value| value), Vec::new());
        write.search = Some(Box::new(RowSearch { vectors: Vec::new(), backfill: vec![EMBEDDED] }));
        let reply = s0.direct(30 + key as u64, write).await;
        assert_eq!(reply, applied(30 + key as u64), "row {key}");
    }
    let records = s0.f.machine().embed_records(None, 128).await.unwrap();
    let keys: Vec<_> = records.iter().map(|item| item.key.clone()).collect();
    assert_eq!(keys.len(), 4, "rows 1, 2, 3 and 5 have a text: {keys:?}");
    assert!(records.iter().all(|item| item.record.source_version == item.record.id && item.record.attempts == 0));
    assert_eq!(s0.get(4).await.source, BTreeMap::new());
    // A second backfill of the same rows changes no record, and a vector or failure
    // of this incarnation stops it.
    assert_eq!(s0.embed(s0.set_vector(1, 31, 31, 40)).await, SearchReply::Set { commit_ts: ts(40) });
    for attempt in 0..EMBED_ATTEMPTS {
        s0.fail(2, 32, 32, attempt).await;
    }
    let before = s0.f.machine().embed_records(None, 128).await.unwrap();
    for key in 1..=3 {
        let head = s0.get(key).await;
        let mut write = s0.write(key, head.version, head.value, Vec::new());
        write.search = Some(Box::new(RowSearch { vectors: Vec::new(), backfill: vec![EMBEDDED] }));
        s0.direct(50 + key as u64, write).await;
    }
    let after = s0.f.machine().embed_records(None, 128).await.unwrap();
    assert_eq!(after, before.iter().filter(|item| item.key != s0.f.key(1)).cloned().collect::<Vec<_>>());
    assert_eq!(s0.get(1).await.vectors[&EMBEDDED].0, 40);
    let _ = &mut s;
}

/// A dropped embedded column leaves no record behind after the next write of its
/// row, and nothing can be embedded for it any more.
#[tokio::test]
async fn adv_vec_g14_a_dropped_embedded_column_leaves_no_record_and_takes_no_report() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    let write = s.write(2, None, Some(row(2, "b")), Vec::new());
    s.direct(11, write).await;
    assert_eq!(s.embed(s.set_vector(2, 11, 11, 12)).await, SearchReply::Set { commit_ts: ts(12) });
    assert_eq!(s.count(EMBED).await, 1);
    let narrower = vec![column(PLAIN, VectorKind::Dense, 2), column(SPARSE, VectorKind::Sparse, 0)];
    assert!(matches!(s.fence(narrower).await, SearchReply::Fenced { .. }));
    // Reports for the dropped column match nothing.
    assert_eq!(s.embed(s.set_vector(1, 10, 10, 20)).await, SearchReply::Obsolete);
    assert_eq!(s.fail(1, 10, 10, 0).await, SearchReply::Unmatched);
    assert_eq!(s.f.submit(requeue(&s, 1, 10, 10)).await, ParticipantReply::Search(SearchReply::Unmatched));
    // The next write of each row collects its record and does not carry the
    // dropped column's vector or source version.
    let head = s.get(1).await;
    let write = s.write(1, head.version, Some(row(1, "a")), Vec::new());
    assert_eq!(s.direct(30, write).await, applied(30));
    let head = s.get(2).await;
    let write = s.write(2, head.version, Some(row(2, "b")), Vec::new());
    assert_eq!(s.direct(31, write).await, applied(31));
    assert_eq!(s.count(EMBED).await, 0);
    let got = s.get(2).await;
    assert!(got.source.is_empty() && !got.vectors.contains_key(&EMBEDDED), "{got:?}");
    assert_eq!(s.get(1).await.vectors[&PLAIN], (10, vec![1; 8]), "a kept column keeps its vector");
    // The old version keeps the dropped column's vector until a collector takes
    // it, and no reader is served it: a dropped column is never served (VEC-G08).
    assert!(!s.at(2, 12).await.vectors.contains_key(&EMBEDDED));
    let logical = crate::oltp::mvcc::store::row::logical_key(&s.f.key(2)).unwrap();
    let kept = [&[VECTOR][..], &logical, &EMBEDDED.to_be_bytes(), &12u64.to_be_bytes()].concat();
    assert!(s.records(VECTOR).await.iter().any(|(key, _)| *key == kept), "the vector of (EMBEDDED, 12) is kept");
    // The id never comes back, with the same definition or another.
    for definition in [embedded(EMBEDDED), column(EMBEDDED, VectorKind::Dense, 2)] {
        let columns = vec![column(PLAIN, VectorKind::Dense, 2), definition];
        assert_eq!(s.fence(columns).await, refused(Fault::InvalidBinding));
    }
}
