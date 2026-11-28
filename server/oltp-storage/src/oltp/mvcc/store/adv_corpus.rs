//! Adversarial corpus for "tables without search columns are unchanged".
//!
//! The same file runs on the base branch (`nabedra/oltp-vector`) and on the
//! storage branch. It uses only APIs both have, applies one fixed history of
//! every operation that exists on the base, and writes a line per section with a
//! SHA-256 of what it produced: replies, log entry bytes and digests, every
//! stored record, image pages, archive pages, a replica snapshot file, a split
//! and a restore. `ADV_CORPUS_OUT` names the file. A script diffs the two files.
use super::{
    direct::{
        DirectBatch, DirectCommit, DirectExpire, DirectForward, DirectPause, DirectSchemaChange,
        DirectSchemaFence, DirectWrite, IndexChange,
    },
    image_archive::{ImagePage, ImageRequest},
    lineage::SplitBinding,
    *,
};
use crate::oltp::{
    mvcc::{CanonicalJson, index::IndexValue},
    pool::{ByteClass, OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources},
};
use nabedra_oltp_types::types::{DecisionStatement, Text, Timestamp};
use nabedra_resources::config::WorkerResources;
use sha2::Sha256;
use std::fmt::Write as _;

fn id(n: u128) -> Uuid {
    Uuid::from_u128(0x4000_8000_0000_0000_0000 + n)
}
fn ts(value: u64) -> Timestamp {
    Timestamp::new(value).unwrap()
}
fn digest(byte: u8) -> Digest {
    Digest::from_bytes([byte; 32])
}
fn row_key(key: i64) -> MvccKey {
    MvccKey::Row {
        table_id: id(1),
        primary_key: Key::Int64(key),
    }
}
fn authority() -> Authority {
    Authority {
        run_id: id(2),
        owner_term: 1,
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn sha(parts: &[&[u8]]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    hex(&hash.finalize())
}

struct Corpus {
    shared: Arc<WorkerSharedResources>,
    out: String,
    entries: Sha256,
    replies: Sha256,
    digests: Sha256,
    lines: Vec<String>,
}
struct Replica {
    machine: ParticipantMachine,
    index: u64,
}
impl Corpus {
    fn json(&self, value: serde_json::Value) -> CanonicalJson {
        CanonicalJson::from_serializable(&self.shared, &value, 1 << 20).unwrap()
    }
    fn write(&self, key: i64, expected: Option<u64>, value: Option<serde_json::Value>) -> DirectWrite {
        serde_json::from_value::<DirectWrite>(serde_json::json!({
            "key": row_key(key),
            "expected_version": expected,
            "value": null,
            "index": [],
        }))
        .map(|write| DirectWrite {
            value: value.map(|value| self.json(value)),
            ..write
        })
        .unwrap()
    }
    fn commit(&self, request: u128, commit_ts: u64, writes: Vec<DirectWrite>) -> DirectCommit {
        DirectCommit {
            authority: authority(),
            table_id: id(1),
            schema_epoch: 1,
            routing_epoch: 1,
            request_id: id(request),
            digest: digest(9),
            commit_ts: ts(commit_ts),
            writes,
            result: self.json(serde_json::json!({"request": request as u64})),
            applied_unix_ms: 1_000 + commit_ts,
        }
    }
    async fn submit(&mut self, replica: &mut Replica, command: ParticipantCommand) -> ParticipantReply {
        let command = OwnedCommand::new_owned(command, &self.shared, ByteClass::Data).unwrap();
        let encoded = serde_json::to_vec(&command).unwrap();
        self.lines.push(String::from_utf8(encoded.clone()).unwrap());
        self.entries.update((encoded.len() as u64).to_be_bytes());
        self.entries.update(&encoded);
        let digest = nabedra_oltp_types::owned::serialized_digest(&command).unwrap();
        self.digests.update(serde_json::to_vec(&digest).unwrap());
        let position = AppliedPosition {
            term: 1,
            node_id: 1,
            index: replica.index,
        };
        replica.index += 1;
        let raw = replica
            .machine
            .apply(
                ApplyMeta {
                    position,
                    command_digest: digest,
                    membership: None,
                },
                command,
            )
            .await
            .unwrap();
        self.replies.update((raw.as_slice().len() as u64).to_be_bytes());
        self.replies.update(raw.as_slice());
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
    fn line(&mut self, name: &str, value: String) {
        writeln!(self.out, "{name} {value}").unwrap();
    }
}

/// Every key and value of the group's scope, log entries included.
fn stored(pool: &OltpPool, group: Uuid) -> String {
    let mut scope = vec![0x40];
    scope.extend_from_slice(group.as_bytes());
    let mut end = scope.clone();
    end.push(0xff);
    let snapshot = pool.kv_store().snapshot();
    let mut iterator = snapshot.iter(
        std::ops::Bound::Included(&scope),
        std::ops::Bound::Excluded(&end),
        false,
    );
    iterator.seek(&scope);
    let mut hash = Sha256::new();
    let mut records = 0;
    while iterator.valid() {
        let value = iterator.value().unwrap();
        for bytes in [iterator.key(), &value] {
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
        records += 1;
        iterator.next();
    }
    iterator.status().unwrap();
    format!("{records} {}", hex(&hash.finalize()))
}

#[tokio::test]
async fn adv_corpus_of_tables_without_search_columns() {
    let Ok(path) = std::env::var("ADV_CORPUS_OUT") else {
        return;
    };
    let shared = WorkerSharedResources::new(
        1,
        WorkerResources::default(),
        SharedResourceConfig::default(),
    )
    .unwrap();
    let dir = std::env::temp_dir().join(format!("adv-corpus-{}", nabedra_rt::ids::uuid_v4()));
    let identity = PoolIdentity {
        engine: "oltp".into(),
        pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
        cluster_id: id(3),
        worker_id: id(4),
        pool_id: id(5),
    };
    let pool = OltpPool::open_development(&dir, identity.clone(), shared.clone()).unwrap();
    let group = |n: u128| GroupIdentity {
        cluster_id: id(3),
        database: Text::try_from("default").unwrap(),
        group_id: id(n),
        kind: GroupKind::Data {
            table_id: id(1),
            shard_id: id(n + 100),
        },
        group_format_version: 1,
    };
    let mut c = Corpus {
        shared: shared.clone(),
        out: String::new(),
        entries: Sha256::new(),
        replies: Sha256::new(),
        digests: Sha256::new(),
        lines: Vec::new(),
    };
    let fence = |routing_epoch, span| ParticipantCommand::Fence {
        transition_id: id(6),
        expected: None,
        next: authority(),
        schema_epoch: 1,
        routing_epoch,
        span,
        catalog_group_id: id(7),
    };
    let activate = || {
        ParticipantCommand::DirectSchema(DirectSchemaFence {
            authority: authority(),
            table_id: id(1),
            change: DirectSchemaChange::Activate { lifecycle: id(1) },
        })
    };
    let mut source = Replica {
        machine: ParticipantMachine::new(pool.group(group(10)).await.unwrap()).unwrap(),
        index: 0,
    };
    let full = KeySpan {
        start: None,
        end: None,
    };
    c.submit(&mut source, fence(1, full.clone())).await;
    c.submit(&mut source, activate()).await;
    let state = stored(&pool, id(10));
    c.line("after-activation", state);

    // Direct: index entry, batch with conflicts, forward, update, delete, expiry.
    let mut first = c.write(1, None, Some(serde_json::json!({"id": 1, "body": "a", "v": 7, "t": ["x", "y"]})));
    first.index.push(IndexChange {
        key: MvccKey::IndexEntry {
            table_id: id(1),
            index_id: id(8),
            values: Page::new(vec![IndexValue::Int64(7)]).unwrap(),
            primary_key: Key::Int64(1),
        },
        expected_version: None,
        value: Some(c.json(serde_json::json!({"id": 1}))),
    });
    let second = c.write(2, None, Some(serde_json::json!({"id": 2, "body": "b", "f": 1.5, "n": null})));
    let commit = c.commit(20, 10, vec![first, second]);
    c.submit(&mut source, ParticipantCommand::Direct(commit.clone())).await;
    // Replay, and the same id with another digest.
    c.submit(&mut source, ParticipantCommand::Direct(commit.clone())).await;
    let mut other = commit.clone();
    other.digest = digest(1);
    c.submit(&mut source, ParticipantCommand::Direct(other)).await;
    let members = vec![
        c.commit(21, 11, vec![c.write(3, None, Some(serde_json::json!({"id": 3})))]),
        c.commit(22, 12, vec![c.write(1, Some(9), Some(serde_json::json!({"id": 1})))]),
        c.commit(23, 13, vec![c.write(2, Some(10), None)]),
        c.commit(25, 9, vec![c.write(5, None, Some(serde_json::json!({"id": 5})))]),
    ];
    c.submit(&mut source, ParticipantCommand::DirectBatch(DirectBatch { members })).await;
    c.submit(
        &mut source,
        ParticipantCommand::DirectForward(DirectForward {
            authority: authority(),
            table_id: id(1),
            anchor: Key::Int64(3),
            request_id: id(24),
            digest: digest(8),
            applied_unix_ms: 5_000,
        }),
    )
    .await;
    c.submit(
        &mut source,
        ParticipantCommand::DirectExpire(DirectExpire {
            authority: authority(),
            cutoff_unix_ms: 1_011,
            retention_ms: 1,
            max_items: 8,
        }),
    )
    .await;
    // Large row, a unicode key text and a row with every JSON type.
    let big = "z".repeat(300_000);
    c.submit(
        &mut source,
        ParticipantCommand::Direct(c.commit(
            26,
            14,
            vec![c.write(6, None, Some(serde_json::json!({"id": 6, "blob": big, "u": "\u{1F600}", "b": true})))],
        )),
    )
    .await;
    c.line("after-direct", stored(&pool, id(10)));

    // Queued: a three-row transaction resolved one row per entry, and an aborted one.
    let writes = vec![
        PreparedWrite {
            key: row_key(1),
            expected_version: Some(ts(10)),
            change: Change::Put(c.json(serde_json::json!({"id": 1, "body": "c"}))),
        },
        PreparedWrite {
            key: row_key(2),
            expected_version: Some(ts(13)),
            change: Change::Put(c.json(serde_json::json!({"id": 2, "again": true}))),
        },
        PreparedWrite {
            key: row_key(3),
            expected_version: Some(ts(11)),
            change: Change::Delete,
        },
        PreparedWrite {
            key: row_key(4),
            expected_version: None,
            change: Change::Put(c.json(serde_json::json!({"id": 4}))),
        },
        PreparedWrite {
            key: row_key(5),
            expected_version: Some(ts(9)),
            change: Change::Keep,
        },
    ];
    let mut statements = Vec::new();
    let second = vec![PreparedWrite {
        key: row_key(6),
        expected_version: Some(ts(14)),
        change: Change::Put(c.json(serde_json::json!({"id": 6, "aborted": true}))),
    }];
    for (n, (abort, writes)) in [(false, writes.clone()), (true, second)]
        .into_iter()
        .enumerate()
    {
        let txn = TxnId {
            request_id: id(30 + n as u128),
            attempt: 0,
        };
        let request = RequestBinding {
            run_id: id(2),
            request_id: txn.request_id,
            digest: digest(1),
            accepted_context: digest(2),
        };
        let manifest = ManifestRef {
            status_group_id: id(31),
            digest: digest(3),
            participant_count: 1,
        };
        let mut plan = ParticipantPlan {
            group: group(10),
            schema_epoch: 1,
            routing_epoch: 1,
            read_ts: ts(16 + 10 * n as u64),
            write_root: digest(0),
            write_count: writes.len() as u64,
            page_count: 1,
        };
        let root = initial_root(txn, &plan).unwrap();
        plan.write_root = page_root(root, 0, &writes).unwrap();
        let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
            lifecycle::LifecycleBinding {
                guard_id: id(32 + n as u128),
                catalog_group_id: id(7),
                table_id: id(1),
                lifecycle_version: None,
                schema_epoch: 1,
                txn,
                request,
                read_ts: plan.read_ts,
                authority: authority(),
            },
            lifecycle::GuardMode::Shared,
        )
        .into_sealed();
        let count = writes.len() as u64;
        let prepared = c
            .submit(
                &mut source,
                ParticipantCommand::PreparePage {
                    authority: authority(),
                    txn,
                    request,
                    manifest,
                    plan,
                    page_index: 0,
                    previous_root: root,
                    guards: Page::new(vec![guard]).unwrap(),
                    writes: Page::new(writes).unwrap(),
                },
            )
            .await;
        assert!(matches!(prepared, ParticipantReply::PreparedPage { .. }), "{prepared:?}");
        c.line(&format!("prepared-{n}"), stored(&pool, id(10)));
        c.submit(
            &mut source,
            ParticipantCommand::Seal {
                authority: authority(),
                txn,
                manifest,
                result_digest: digest(4),
            },
        )
        .await;
        let decision = if abort {
            TxnDecision::Abort {
                reason: nabedra_oltp_types::types::AbortReason::Conflict,
            }
        } else {
            TxnDecision::Commit { commit_ts: ts(17) }
        };
        let statement = DecisionStatement {
            txn,
            request,
            manifest,
            decided_at: AppliedPosition {
                term: 1,
                node_id: 1,
                index: 1,
            },
            decision,
        };
        let proof = SealedStatusDecision::seal(statement.clone()).unwrap();
        for _ in 0..count.min(6) {
            c.submit(
                &mut source,
                ParticipantCommand::Resolve {
                    authority: authority(),
                    proof: proof.clone(),
                    max_items: 1,
                },
            )
            .await;
        }
        c.line(&format!("resolved-{n}"), stored(&pool, id(10)));
        statements.push(statement);
    }
    // Archive page of the committed transaction, one effect at a time.
    for after in 0..5 {
        let page = source
            .machine
            .archive_effect_page_from_statement(&statements[0], after, 1, 64 << 10)
            .await;
        match page {
            Ok(page) => c.line(
                &format!("archive-{after}"),
                sha(&[&serde_json::to_vec(page.payload().unwrap()).unwrap()]),
            ),
            Err(error) => c.line(&format!("archive-{after}"), format!("error {:?}", error.code)),
        }
    }

    // Committed images at two cuts, in small pages.
    for cut in [12u64, 17, 40] {
        let mut after = None;
        let mut hash = Sha256::new();
        loop {
            let request = OwnedCommand::new_owned(
                ImageRequest {
                    read_ts: ts(cut),
                    schema_epoch: 1,
                    routing_epoch: 1,
                    span: full.clone(),
                    after: after.clone(),
                    max_items: 2,
                    max_bytes: 1 << 20,
                    search: None,
                },
                &shared,
                ByteClass::Data,
            )
            .unwrap();
            let page = source.machine.committed_image_page(request).await.unwrap();
            hash.update(serde_json::to_vec(page.payload().unwrap()).unwrap());
            let next = page.payload().unwrap().continuation().unwrap().cloned();
            let ImagePage::Images { exhausted, .. } = page.payload().unwrap() else {
                panic!("blocked")
            };
            if *exhausted {
                break;
            }
            after = next;
        }
        c.line(&format!("images-{cut}"), hex(&hash.finalize()));
    }

    // A replica snapshot file and its installation.
    let snapshot = source
        .machine
        .store()
        .export_snapshot(TransactionBudget {
            scratch_bytes: 32 << 20,
            batch_bytes: 32 << 20,
            output_bytes: 64 << 10,
        })
        .await
        .unwrap();
    let bytes = std::fs::read(&snapshot.path).unwrap();
    c.line("snapshot-file", format!("{} {}", bytes.len(), sha(&[&bytes])));
    let dir2 = std::env::temp_dir().join(format!("adv-corpus-{}", nabedra_rt::ids::uuid_v4()));
    let mut identity2 = identity.clone();
    identity2.worker_id = id(14);
    identity2.pool_id = id(15);
    let shared2 = WorkerSharedResources::new(1, WorkerResources::default(), SharedResourceConfig::default()).unwrap();
    let pool2 = OltpPool::open_development(&dir2, identity2, shared2).unwrap();
    let store2 = pool2.group(group(10)).await.unwrap();
    store2
        .install_snapshot(
            snapshot,
            TransactionBudget {
                scratch_bytes: 32 << 20,
                batch_bytes: 32 << 20,
                output_bytes: 64 << 10,
            },
        )
        .await
        .unwrap();
    c.line("snapshot-installed", stored_by_class(&pool2, id(10)));
    c.line("source-by-class", stored_by_class(&pool, id(10)));
    c.line("source-after-snapshot", stored(&pool, id(10)));

    // A restore of the images at cut 17 into another group.
    let mut images = Vec::new();
    let mut after = None;
    loop {
        let request = OwnedCommand::new_owned(
            ImageRequest {
                read_ts: ts(17),
                schema_epoch: 1,
                routing_epoch: 1,
                span: full.clone(),
                after: after.clone(),
                max_items: 128,
                max_bytes: 1 << 20,
                search: None,
            },
            &shared,
            ByteClass::Data,
        )
        .unwrap();
        let page = source.machine.committed_image_page(request).await.unwrap();
        let next = page.payload().unwrap().continuation().unwrap().cloned();
        let ImagePage::Images { images: page_images, exhausted, .. } = page.payload().unwrap() else {
            panic!("blocked")
        };
        images.extend(page_images.iter().cloned());
        if *exhausted {
            break;
        }
        after = next;
    }
    let target = group(20);
    let target_store = pool.group(target.clone()).await.unwrap();
    let genesis = restore::ParticipantGenesis {
        restore_id: id(21),
        source_cluster: id(3),
        target_group: target.clone(),
        catalog_group_id: id(7),
        schema_epoch: 1,
        routing_epoch: 1,
        span: full.clone(),
        cut: ts(17),
    };
    let install = {
        let target = target.clone();
        let images = images.clone();
        let full = full.clone();
        move |tx: &mut GroupTransaction<'_>| {
            tx.put_serialized(restore::RESTORE_GENESIS, &genesis)?;
            for image in &images {
                restore::install_image(tx, &target, &full, image)?;
            }
            Ok(Transition::Commit(tx.copy_output(b"null")?))
        }
    };
    target_store
        .transaction(
            ApplyMeta {
                position: AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index: 0,
                },
                command_digest: digest(7),
                membership: None,
            },
            TransactionBudget {
                scratch_bytes: 32 << 20,
                batch_bytes: 32 << 20,
                output_bytes: 64 << 10,
            },
            install,
        )
        .await
        .unwrap();
    let mut restored = Replica {
        machine: ParticipantMachine::new(target_store).unwrap(),
        index: 1,
    };
    c.submit(&mut restored, fence(1, full.clone())).await;
    c.submit(&mut restored, activate()).await;
    c.submit(
        &mut restored,
        ParticipantCommand::Direct(c.commit(50, 60, vec![c.write(1, Some(17), Some(serde_json::json!({"id": 1, "after": "restore"})))])),
    )
    .await;
    c.line("restored", stored(&pool, id(20)));

    // A split, as in the golden test, with a second page boundary.
    let split_guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
        lifecycle::LifecycleBinding {
            guard_id: id(40),
            catalog_group_id: id(7),
            table_id: id(1),
            lifecycle_version: None,
            schema_epoch: 1,
            txn: TxnId {
                request_id: id(30),
                attempt: 0,
            },
            request: RequestBinding {
                run_id: id(2),
                request_id: id(30),
                digest: digest(1),
                accepted_context: digest(2),
            },
            read_ts: ts(15),
            authority: authority(),
        },
        lifecycle::GuardMode::Exclusive,
    )
    .into_sealed();
    c.submit(
        &mut source,
        ParticipantCommand::DirectPause(DirectPause {
            authority: authority(),
            split_id: id(40),
            paused: true,
        }),
    )
    .await;
    let spans = [
        KeySpan {
            start: None,
            end: Some(Key::Int64(3)),
        },
        KeySpan {
            start: Some(Key::Int64(3)),
            end: None,
        },
    ];
    let bindings = [0usize, 1].map(|i| SplitBinding {
        split_id: id(40),
        source: group(10),
        source_schema_epoch: 1,
        source_routing_epoch: 1,
        source_span: full.clone(),
        target: group(11 + i as u128),
        target_span: spans[i].clone(),
        read_ts: ts(20),
        guard: split_guard.clone(),
    });
    let frozen = c
        .submit(
            &mut source,
            ParticipantCommand::SplitFreeze {
                authority: authority(),
                children: Box::new(bindings.clone()),
            },
        )
        .await;
    c.line("frozen", format!("{frozen:?}").chars().take(40).collect());
    let mut children = Vec::new();
    for (i, binding) in bindings.iter().enumerate() {
        let mut child = Replica {
            machine: ParticipantMachine::new(pool.group(group(11 + i as u128)).await.unwrap()).unwrap(),
            index: 0,
        };
        c.submit(&mut child, fence(2, spans[i].clone())).await;
        c.submit(&mut child, activate()).await;
        c.submit(
            &mut child,
            ParticipantCommand::SplitStage {
                authority: authority(),
                binding: Box::new(binding.clone()),
            },
        )
        .await;
        children.push(child);
    }
    let request = OwnedCommand::new_owned(
        bindings[0].image_request(None, 1 << 20),
        &shared,
        ByteClass::Data,
    )
    .unwrap();
    let page = source.machine.committed_image_page(request).await.unwrap();
    assert!(matches!(page.payload().unwrap(), ImagePage::Images { exhausted: true, .. }));
    c.line("split-page", sha(&[&serde_json::to_vec(page.payload().unwrap()).unwrap()]));
    for (binding, child) in bindings.iter().zip(&mut children) {
        let root = lineage::initial_root(binding).unwrap();
        let (command, _) = binding
            .import_page_from_image(&shared, 0, root, None, page.clone())
            .await
            .unwrap();
        let imported = c.submit_owned(child, command).await;
        assert!(matches!(imported, ParticipantReply::SplitImported { complete: true, .. }), "{imported:?}");
    }
    c.line("split-source", stored(&pool, id(10)));
    c.line("split-left", stored(&pool, id(11)));
    c.line("split-right", stored(&pool, id(12)));

    let (entries, replies, digests) = (
        hex(&std::mem::take(&mut c.entries).finalize()),
        hex(&std::mem::take(&mut c.replies).finalize()),
        hex(&std::mem::take(&mut c.digests).finalize()),
    );
    c.line("entries", entries);
    c.line("replies", replies);
    c.line("command-digests", digests);
    std::fs::write(format!("{path}.entries"), c.lines.join("\n")).unwrap();
    std::fs::write(path, &c.out).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dir2);
}

impl Corpus {
    async fn submit_owned(
        &mut self,
        replica: &mut Replica,
        command: OwnedCommand<ParticipantCommand>,
    ) -> ParticipantReply {
        let encoded = serde_json::to_vec(&command).unwrap();
        self.lines.push(String::from_utf8(encoded.clone()).unwrap());
        self.entries.update((encoded.len() as u64).to_be_bytes());
        self.entries.update(&encoded);
        let digest = nabedra_oltp_types::owned::serialized_digest(&command).unwrap();
        self.digests.update(serde_json::to_vec(&digest).unwrap());
        let position = AppliedPosition {
            term: 1,
            node_id: 1,
            index: replica.index,
        };
        replica.index += 1;
        let raw = replica
            .machine
            .apply(
                ApplyMeta {
                    position,
                    command_digest: digest,
                    membership: None,
                },
                command,
            )
            .await
            .unwrap();
        self.replies.update(raw.as_slice());
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
}

/// Decodes the entries another build wrote, one JSON command per line of
/// `ADV_FOREIGN_IN`, and says per line whether this build takes it and whether it
/// writes it back byte for byte. `ADV_FOREIGN_OUT` gets one verdict per line.
#[tokio::test]
async fn adv_decode_foreign_entries() {
    let (Ok(input), Ok(output)) = (std::env::var("ADV_FOREIGN_IN"), std::env::var("ADV_FOREIGN_OUT")) else {
        return;
    };
    let mut verdicts = String::new();
    for (n, line) in std::fs::read_to_string(input).unwrap().lines().enumerate() {
        let name: String = line.chars().skip(1).take_while(|c| *c != '"').collect();
        let verdict = match serde_json::from_str::<ParticipantCommand>(line) {
            Ok(command) => match serde_json::to_string(&command) {
                Ok(back) if back == line => "ok-identical".to_owned(),
                Ok(_) => "ok-DIFFERENT".to_owned(),
                Err(_) => "ok-unencodable".to_owned(),
            },
            Err(_) => "refused".to_owned(),
        };
        writeln!(verdicts, "{n} {name} {verdict}").unwrap();
    }
    std::fs::write(output, verdicts).unwrap();
}

/// Every record of the group's scope, hashed per kind of record.
fn stored_by_class(pool: &OltpPool, group: Uuid) -> String {
    let mut scope = vec![0x40];
    scope.extend_from_slice(group.as_bytes());
    let mut end = scope.clone();
    end.push(0xff);
    let snapshot = pool.kv_store().snapshot();
    let mut iterator = snapshot.iter(
        std::ops::Bound::Included(&scope),
        std::ops::Bound::Excluded(&end),
        false,
    );
    iterator.seek(&scope);
    let mut classes: std::collections::BTreeMap<u8, (usize, Sha256)> = std::collections::BTreeMap::new();
    while iterator.valid() {
        let value = iterator.value().unwrap();
        let key = iterator.key().to_vec();
        let class = key.get(17).copied().unwrap_or(0);
        let entry = classes.entry(class).or_insert_with(|| (0, Sha256::new()));
        entry.0 += 1;
        for bytes in [&key[..], &value[..]] {
            entry.1.update((bytes.len() as u64).to_be_bytes());
            entry.1.update(bytes);
        }
        iterator.next();
    }
    iterator.status().unwrap();
    classes
        .into_iter()
        .map(|(class, (count, hash))| format!("{}:{count}:{}", class as char, &hex(&hash.finalize())[..12]))
        .collect::<Vec<_>>()
        .join(" ")
}
