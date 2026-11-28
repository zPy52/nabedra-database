//! VEC-G10: splits, backups, restore and replica snapshots (`specs/vector/blobs`).
use super::*;
use crate::oltp::mvcc::store::search::SearchSchema;
use crate::oltp::{
    backup::history::{HistoryFrame, OwnedImage},
    mvcc::store::{
        embed::{EMBED, EMBED_ATTEMPTS},
        image_archive::{CommittedImage, ImagePage, ImageRequest},
        lineage::{self, SplitBinding, SplitPage},
        restore,
        row::VECTOR,
        search::SplitSearch,
    },
};

fn row(key: i64, body: &str) -> serde_json::Value {
    serde_json::json!({"id": key, "body": body})
}
/// Six rows: plain, updated, deleted, waiting, failed and embedded.
async fn fill(s: &mut S) {
    let vectors = vec![s.set(PLAIN, s.dense(1)), s.set(MULTI, s.value(VectorKind::Multi, &[2; 8]))];
    let write = s.write(1, None, Some(serde_json::json!({"id": 1})), vectors);
    s.direct(10, write).await;
    let write = s.write(2, None, Some(row(2, "a")), vec![s.set(PLAIN, s.dense(3))]);
    s.direct(11, write).await;
    let write = s.write(2, Some(11), Some(row(2, "a")), vec![s.set(SPARSE, s.value(VectorKind::Sparse, &[4; 24]))]);
    s.direct(12, write).await;
    let write = s.write(3, None, Some(row(3, "gone")), vec![s.set(PLAIN, s.dense(5))]);
    s.direct(13, write).await;
    let write = s.write(3, Some(13), None, Vec::new());
    s.direct(14, write).await;
    for key in 4..7 {
        let write = s.write(key, None, Some(row(key, "t")), vec![s.set(PLAIN, s.dense(key as u8))]);
        s.direct(20 + key as u64, write).await;
    }
    assert_eq!(s.fail(4, 24, 24, 0).await, SearchReply::Counted { attempts: 1 });
    for attempt in 0..EMBED_ATTEMPTS {
        s.fail(5, 25, 25, attempt).await;
    }
    assert!(matches!(s.embed(s.set_vector(6, 26, 26, 30)).await, SearchReply::Set { .. }));
}
fn request(s: &S, cut: u64, max_bytes: usize, after: Option<MvccKey>) -> OwnedCommand<ImageRequest> {
    request_at(s, cut, max_bytes, after, None)
}
/// A backup's request, which names the search columns of its cut.
fn request_at(
    s: &S,
    cut: u64,
    max_bytes: usize,
    after: Option<MvccKey>,
    search: Option<SearchSchema>,
) -> OwnedCommand<ImageRequest> {
    OwnedCommand::new_owned(
        ImageRequest {
            read_ts: ts(cut),
            schema_epoch: 1,
            routing_epoch: 1,
            span: full_span(),
            after,
            max_items: 128,
            max_bytes,
            search: search.map(Box::new),
        },
        s.f.shared(),
        ByteClass::Data,
    )
    .unwrap()
}
async fn images(s: &S, cut: u64) -> Vec<CommittedImage> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = s.f.machine().committed_image_page(request(s, cut, 64 << 10, after)).await.unwrap();
        after = page.payload().unwrap().continuation().unwrap().cloned();
        let ImagePage::Images { images, exhausted, .. } = page.payload().unwrap() else { panic!() };
        all.extend(images.iter().cloned());
        if *exhausted {
            return all;
        }
    }
}

struct Split {
    bindings: [SplitBinding; 2],
    children: Vec<Replica>,
}
async fn freeze(s: &mut S, cut: u64) -> Split {
    let bindings = split_bindings(&s.f, nabedra_rt::ids::uuid_v4(), cut);
    let frozen = s
        .f
        .submit(ParticipantCommand::SplitFreeze {
            authority: s.f.authority,
            children: Box::new(bindings.clone()),
        })
        .await;
    assert!(matches!(frozen, ParticipantReply::SplitSourceFrozen(_)), "{frozen:?}");
    let mut children = Vec::new();
    for binding in &bindings {
        let store = s.f.pool.group(binding.target.clone()).await.unwrap();
        let mut child = Replica {
            machine: ParticipantMachine::new(store).unwrap(),
            shared: s.f.shared().clone(),
            index: 0,
        };
        let fence = s.f.fence(1, 2, binding.target_span.clone());
        assert!(matches!(child.submit(fence).await, ParticipantReply::Fenced(_)));
        child.submit(s.f.activation()).await;
        let stage = ParticipantCommand::SplitStage {
            authority: s.f.authority,
            binding: Box::new(binding.clone()),
        };
        assert_eq!(child.submit(stage).await, ParticipantReply::SplitStaged);
        children.push(child);
    }
    Split { bindings, children }
}
/// The command as another replica decodes it from its log, within the budget of
/// its own bytes.
fn logged(s: &S, command: &OwnedCommand<ParticipantCommand>) -> OwnedCommand<ParticipantCommand> {
    let encoded = s.f.shared().encode(command, 16 << 20).unwrap();
    let decoded = OwnedCommand::decode_json(&encoded, s.f.shared(), ByteClass::Data, 48 << 20).unwrap();
    assert_eq!(
        s.f.shared().encode(&decoded, 16 << 20).unwrap().as_slice(),
        encoded.as_slice()
    );
    decoded
}
/// Copies everything, in pages of `max_bytes`.
async fn import(s: &S, split: &mut Split, max_bytes: usize) {
    let mut root = lineage::initial_root(&split.bindings[0]).unwrap();
    let (mut after, mut digest) = (None, None);
    for page_index in 0.. {
        let page = s
            .f
            .machine()
            .committed_image_page(
                OwnedCommand::new_owned(
                    split.bindings[0].image_request(after, max_bytes),
                    s.f.shared(),
                    ByteClass::Data,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        after = page.payload().unwrap().continuation().unwrap().cloned();
        let ImagePage::Images { exhausted, .. } = page.payload().unwrap() else { panic!() };
        let mut next = root;
        for (binding, child) in split.bindings.iter().zip(&mut split.children) {
            let (command, expected) = binding
                .import_page_from_image(s.f.shared(), page_index, root, digest, page.clone())
                .await
                .unwrap();
            let reply = child.apply_owned(logged(s, &command)).await;
            let ParticipantReply::SplitImported { complete, .. } = reply else { panic!("{reply:?}") };
            assert_eq!(complete, *exhausted);
            next = expected;
        }
        root = next;
        digest = split.children[0]
            .machine
            .split_progress(&split.bindings[0], 64 << 10)
            .await
            .unwrap()
            .payload()
            .unwrap()
            .after_digest;
        if *exhausted {
            return;
        }
    }
}

/// Mutants `split_drops_blobs`, `prefix_split_rederives`, `copy_drops_row` and
/// `prefix_copy_takes_collectable`.
#[tokio::test]
async fn vs08_vs16_a_split_copies_rows_vectors_and_records_exactly() {
    for max_bytes in [64 << 10, 1 << 20] {
        let mut s = S::new().await;
        fill(&mut s).await;
        // Closed above the cut, between the barrier and the freeze.
        assert_eq!(s.close(45).await, SearchReply::Closed { floor: ts(45) });
        let mut split = freeze(&mut s, 41).await;
        // The frozen source takes no entry that would change what was copied.
        assert_eq!(s.fail(4, 24, 24, 1).await, refused(Fault::SplitInProgress));
        let set = s.set_vector(4, 24, 24, 50);
        assert_eq!(s.embed(set).await, refused(Fault::SplitInProgress));
        // A child serves nothing before its copy is complete.
        assert!(got(&split.children[0].machine, s.f.key(1), 41).await.is_err());
        import(&s, &mut split, max_bytes).await;
        for key in 1..7 {
            let child = &split.children[usize::from(key >= 3)].machine;
            let copied = got(child, s.f.key(key), 41).await.unwrap();
            assert_eq!(copied, s.at(key, 41).await, "row {key}");
            if key != 3 {
                assert!(!copied.vectors.is_empty(), "row {key}");
            }
        }
        // Byte for byte, and only the newest version and its vectors.
        for tag in [EMBED, VECTOR] {
            let mut copied = Vec::new();
            for child in &split.children {
                copied.extend(child.dump().await.into_iter().filter(|(key, _)| key[0] == tag));
            }
            let mut source = s.records(tag).await;
            if tag == VECTOR {
                let deleted = [&[VECTOR][..], &crate::oltp::mvcc::store::row::logical_key(&s.f.key(3)).unwrap()].concat();
                source.retain(|(key, _)| !key.starts_with(&deleted));
            }
            assert_eq!(copied, source);
        }
        let right = &mut split.children[1];
        assert_eq!(count_tag(&right.dump().await, VALUE), 4, "one version of each row");
        // The child inherits the columns and the floor, and goes on.
        let mut write = s.commit(44, vec![s.write(4, Some(24), Some(row(4, "t")), Vec::new())]);
        write.routing_epoch = 2;
        write.schema_epoch = 1;
        for at in [41, 45] {
            write.commit_ts = ts(at);
            let reply = right.submit(ParticipantCommand::Direct(write.clone())).await;
            assert_eq!(reply, rejected(Fault::Conflict));
        }
        write.commit_ts = ts(46);
        assert_eq!(right.submit(ParticipantCommand::Direct(write)).await, applied(46));
        assert_eq!(got(&right.machine, s.f.key(4), 50).await.unwrap().vectors[&PLAIN], (24, vec![4; 8]));
        // Its feed starts after the copy.
        let origin = right.index - 4;
        assert!(changes(&right.machine, 1, 8).await.is_none());
        assert_eq!(seen(&changes(&right.machine, origin.max(right.index - 3), 8).await.unwrap()).len(), 1);
    }
}

#[tokio::test]
async fn vs16_a_split_page_without_every_vector_is_refused() {
    let mut s = S::new().await;
    fill(&mut s).await;
    let mut split = freeze(&mut s, 41).await;
    let page = s.f.machine().committed_image_page(request(&s, 41, 1 << 20, None)).await.unwrap();
    let ImagePage::Images { images, after, search, .. } = page.payload().unwrap() else { panic!() };
    let schema: Option<Box<SplitSearch>> = search.clone();
    assert!(schema.is_some());
    let binding = split.bindings[0].clone();
    let good = |images: Vec<CommittedImage>, search| SplitPage {
        binding: binding.clone(),
        page_index: 0,
        previous_root: lineage::initial_root(&binding).unwrap(),
        previous_after: None,
        images: Page::new(images).unwrap(),
        after: after.clone(),
        exhausted: true,
        search,
    };
    let edit = |work: &dyn Fn(&mut CommittedImage)| {
        let mut images = images.to_vec();
        work(&mut images[0]);
        images
    };
    let vectors = |image: &mut CommittedImage, work: &dyn Fn(&mut RowImage)| {
        work(image.search.as_mut().unwrap());
    };
    type Case = (&'static str, Vec<CommittedImage>, Option<Box<SplitSearch>>);
    let cases: Vec<Case> = vec![
        ("no vectors", edit(&|image| vectors(image, &|search| search.vectors.clear())), schema.clone()),
        ("one vector", edit(&|image| vectors(image, &|search| { search.vectors.pop(); })), schema.clone()),
        ("another column", edit(&|image| vectors(image, &|search| search.vectors[0].column = 9)), schema.clone()),
        ("a later version", edit(&|image| vectors(image, &|search| { search.blobs.0.insert(PLAIN, ts(99)); })), schema.clone()),
        ("vectors of no row", edit(&|image| image.value = None), schema.clone()),
        ("a foreign record", edit(&|image| vectors(image, &|search| {
            search.records.push(crate::oltp::mvcc::store::row::RecordImage { column: PLAIN, record: EmbedRecord::fresh(ts(1)) });
        })), schema.clone()),
        ("no columns", images.to_vec(), None),
    ];
    for (name, images, search) in cases {
        let before = split.children[0].dump().await;
        let reply = split.children[0]
            .submit(ParticipantCommand::SplitImportPage {
                authority: s.f.authority,
                page: Box::new(good(images, search)),
            })
            .await;
        assert_eq!(reply, ParticipantReply::Rejected(Fault::InvalidBinding), "{name}");
        assert_eq!(split.children[0].dump().await, before, "{name}");
    }
    let reply = split.children[0]
        .submit(ParticipantCommand::SplitImportPage {
            authority: s.f.authority,
            page: Box::new(good(images.to_vec(), schema)),
        })
        .await;
    assert!(matches!(reply, ParticipantReply::SplitImported { complete: true, .. }));
}

/// Installs images into a new group as an offline restore does.
async fn restore(s: &S, images: &[CommittedImage], cut: u64, batch: usize) -> Replica {
    restore_with_schema(s, images, cut, batch, None).await
}

async fn restore_with_schema(
    s: &S,
    images: &[CommittedImage],
    cut: u64,
    batch: usize,
    schema: Option<SearchSchema>,
) -> Replica {
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
    let mut index = 0;
    for batch in images.chunks(batch) {
        let (target, genesis, batch) = (target.clone(), genesis.clone(), batch.to_vec());
        let schema = schema.clone();
        store
            .transaction(
                ApplyMeta {
                    position: position(index),
                    command_digest: digest(7),
                    membership: None,
                },
                BUDGET,
                move |tx| {
                    tx.put_serialized(restore::RESTORE_GENESIS, &genesis)?;
                    if let Some(schema) = schema {
                        tx.put_serialized(restore::RESTORE_SEARCH, &schema)?;
                    }
                    let mut sorted: Vec<_> = batch.iter().collect();
                    sorted.sort_by(|a, b| compare_keys(&a.key, &b.key).then(a.commit_ts.cmp(&b.commit_ts)));
                    restore::install_rows(tx, &target, &full_span(), ts(cut), &sorted)?;
                    Ok(Transition::Commit(tx.copy_output(b"null")?))
                },
            )
            .await
            .unwrap();
        index += 1;
    }
    let mut restored = Replica {
        machine: ParticipantMachine::new(store).unwrap(),
        shared: s.f.shared().clone(),
        index,
    };
    let fence = s.f.fence(1, 1, full_span());
    assert!(matches!(restored.submit(fence).await, ParticipantReply::Fenced(_)));
    restored.submit(s.f.activation()).await;
    restored
}

/// Mutants `prefix_copy_without_pending`, `prefix_rederive_keeps_identity` and
/// `copy_drops_row`.
#[tokio::test]
async fn vs16_a_backup_carries_rows_and_vectors_and_a_restore_derives_its_records() {
    let mut s = S::new().await;
    fill(&mut s).await;
    // Writes above the cut, which the backup does not take.
    let write = s.write(1, Some(10), Some(serde_json::json!({"id": 1})), vec![s.set(PLAIN, s.dense(9))]);
    s.direct(60, write).await;
    let write = s.write(4, Some(24), Some(row(4, "changed")), Vec::new());
    s.direct(61, write).await;
    let backup = images(&s, 50).await;
    assert!(backup.iter().all(|image| {
        image.search.as_ref().is_none_or(|search| search.records.is_empty())
    }));
    for batch in [1, 3, 128] {
        let mut restored = restore(&s, &backup, 50, batch).await;
        // No vector is served before the range knows its columns.
        let unknown = got(&restored.machine, s.f.key(1), 50).await.err().unwrap();
        assert!(unknown.message.contains("not known yet"), "{}", unknown.message);
        // Writes wait for the columns, so nothing drops a vector.
        let mut commit = s.commit(70, vec![s.write(1, Some(10), Some(serde_json::json!({"id": 1, "v": 1})), Vec::new())]);
        commit.schema_epoch = 1;
        let before = restored.dump().await;
        let reply = restored.submit(ParticipantCommand::Direct(commit.clone())).await;
        assert_eq!(reply, rejected(Fault::DirectInactive));
        assert_eq!(restored.dump().await, before);
        let fence = s.fence_from(0, 7, columns());
        assert!(matches!(search_reply(restored.submit(fence).await), SearchReply::Fenced { .. }));
        for key in 1..7 {
            let mut copied = got(&restored.machine, s.f.key(key), 50).await.unwrap();
            let mut original = s.at(key, 50).await;
            // Rows 2, 4 and 5 have a text and no vector, the failed one included.
            let pending = matches!(key, 2 | 4 | 5);
            let records = std::mem::take(&mut copied.records);
            let expected = pending.then(|| {
                let source = ts(original.source[&EMBEDDED]);
                (EMBEDDED, EmbedRecord::derived(source, ts(51)))
            });
            assert_eq!(records, expected.into_iter().collect(), "row {key}");
            original.records.clear();
            assert_eq!(copied, original, "row {key}");
        }
        commit.schema_epoch = 2;
        assert_eq!(restored.submit(ParticipantCommand::Direct(commit)).await, applied(70));
        assert_eq!(got(&restored.machine, s.f.key(1), 70).await.unwrap().vectors[&PLAIN], (10, vec![1; 8]));
        // A late report for the record of the source counts nothing.
        let late = |id| {
            ParticipantCommand::EmbedFailed(EmbedFailed {
                authority: s.f.authority,
                table_id: s.f.table,
                primary_key: Key::Int64(4),
                column: EMBEDDED,
                source_version: ts(24),
                id: ts(id),
                attempt: 0,
                reason: Text::try_from("no").unwrap(),
            })
        };
        let (old, new) = (late(24), late(51));
        assert_eq!(search_reply(restored.submit(old).await), SearchReply::Unmatched);
        assert_eq!(search_reply(restored.submit(new).await), SearchReply::Counted { attempts: 1 });
    }
}

/// Several versions of a row, in any batches: the newest one decides.
#[tokio::test]
async fn vs16_a_restore_of_a_history_keeps_every_version_and_the_newest_records() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    let write = s.write(1, Some(10), Some(row(1, "b")), Vec::new());
    s.direct(11, write).await;
    s.embed(s.set_vector(1, 11, 11, 12)).await;
    let write = s.write(2, None, Some(row(2, "a")), Vec::new());
    s.direct(13, write).await;
    let write = s.write(2, Some(13), None, Vec::new());
    s.direct(14, write).await;
    let mut history = Vec::new();
    for cut in [10, 11, 12, 13, 14] {
        history.extend(images(&s, cut).await.into_iter().filter(|image| {
            matches!(image.key, MvccKey::Row { .. }) && image.commit_ts == ts(cut)
        }));
    }
    assert_eq!(history.len(), 5);
    for (batch, reversed) in [(1, false), (1, true), (2, false), (5, true)] {
        let mut images = history.clone();
        if reversed {
            images.reverse();
        }
        let mut restored = restore(&s, &images, 20, batch).await;
        let fence = s.fence_from(0, 7, columns());
        assert!(matches!(search_reply(restored.submit(fence).await), SearchReply::Fenced { .. }));
        for at in [10, 11, 12, 13, 14] {
            for key in [1, 2] {
                let mut original = s.at(key, at).await;
                let copied = got(&restored.machine, s.f.key(key), at).await;
                original.records.clear();
                // A restored group serves from its cut on.
                assert!(copied.is_err());
            }
        }
        assert_eq!(got(&restored.machine, s.f.key(1), 20).await.unwrap(), s.at(1, 20).await);
        assert_eq!(restored.machine.embed_records(None, 8).await.unwrap(), []);
        let dump = restored.dump().await;
        assert_eq!(count_tag(&dump, VALUE), 5);
        let vectors: Vec<_> = dump.into_iter().filter(|(key, _)| key[0] == VECTOR).collect();
        assert_eq!(vectors, s.records(VECTOR).await);
    }
    // A damaged archive is refused: an image without its vector.
    let mut images = history.clone();
    images[0].search.as_mut().unwrap().vectors.clear();
    let store = s.f.pool.group(GroupIdentity { group_id: nabedra_rt::ids::uuid_v4(), ..s.f.group.clone() }).await.unwrap();
    let refused = store
        .transaction(
            ApplyMeta { position: position(0), command_digest: digest(1), membership: None },
            BUDGET,
            move |tx| {
                let group = GroupIdentity { ..images_group(&images) };
                restore::install_rows(tx, &group, &full_span(), ts(20), &[&images[0]])?;
                Ok(Transition::Commit(tx.copy_output(b"null")?))
            },
        )
        .await;
    assert!(refused.is_err());
}
fn images_group(images: &[CommittedImage]) -> GroupIdentity {
    let MvccKey::Row { table_id, .. } = images[0].key else { panic!() };
    GroupIdentity {
        cluster_id: nabedra_rt::ids::uuid_v4(),
        database: Text::try_from("default").unwrap(),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind: GroupKind::Data { table_id, shard_id: nabedra_rt::ids::uuid_v4() },
        group_format_version: 1,
    }
}

/// The archive frames keep vectors charged and byte for byte.
#[tokio::test]
async fn vs16_an_archive_frame_round_trips_an_image_with_its_vectors() {
    let mut s = S::new().await;
    fill(&mut s).await;
    for image in images(&s, 50).await {
        let frame = OwnedCommand::new_owned(HistoryFrame::Image(image.clone()), s.f.shared(), ByteClass::Data).unwrap();
        let encoded = s.f.shared().encode(&frame, 1 << 20).unwrap();
        let decoded = OwnedCommand::<HistoryFrame>::decode_json(&encoded, s.f.shared(), ByteClass::Data, 8 << 20).unwrap();
        let owned = OwnedImage::from_base(decoded, s.f.shared()).unwrap();
        let copied = owned.image().unwrap();
        assert_eq!((&copied.key, copied.commit_ts, &copied.value, &copied.search), (&image.key, image.commit_ts, &image.value, &image.search));
    }
}

/// Mutant `copy_unpinned`: no copy reads below what collection may have taken.
#[tokio::test]
async fn vs16_a_copy_below_the_collected_floor_is_refused() {
    let mut s = S::new().await;
    fill(&mut s).await;
    let reclaim = |safe: u64, s: &S| {
        ParticipantCommand::ReclaimIndex(index_reclaim::ReclaimIndex {
            authority: s.f.authority,
            catalog_group_id: s.f.catalog_group_id,
            table_id: s.f.table,
            index_id: Uuid::from_u128(5),
            retired_at: ts(1),
            safe_ts: ts(safe),
            routing_version: None,
        })
    };
    // An index entry, so reclaiming it raises the group's floor to 45.
    let mut write = s.write(8, None, Some(row(8, "t")), Vec::new());
    write.index.push(IndexChange {
        key: MvccKey::IndexEntry {
            table_id: s.f.table,
            index_id: Uuid::from_u128(5),
            values: Page::new(vec![IndexValue::Int64(1)]).unwrap(),
            primary_key: Key::Int64(8),
        },
        expected_version: None,
        value: None,
    });
    s.direct(40, write).await;
    let command = reclaim(45, &s);
    assert!(matches!(s.f.submit(command).await, ParticipantReply::IndexReclaimed { deleted: 1.., .. }));
    for cut in [10, 44] {
        assert!(s.f.machine().committed_image_page(request(&s, cut, 1 << 20, None)).await.is_err());
        assert!(got(s.f.machine(), s.f.key(1), cut).await.is_err());
    }
    let frozen = s
        .f
        .submit(ParticipantCommand::SplitFreeze {
            authority: s.f.authority,
            children: Box::new(split_bindings(&s.f, nabedra_rt::ids::uuid_v4(), 44)),
        })
        .await;
    assert_eq!(frozen, ParticipantReply::Rejected(Fault::InvalidBinding));
    assert_eq!(images(&s, 45).await.iter().filter(|image| matches!(image.key, MvccKey::Row { .. })).count(), 7);
}

/// Split sizing counts vectors, so a range of vectors splits in time and its
/// image pages are sized for them.
#[tokio::test]
async fn vs08_split_usage_counts_vectors() {
    let mut s = S::new().await;
    for key in 0..40 {
        let value = s.value(VectorKind::Sparse, &vec![key as u8; 100 << 10]);
        let write = s.write(key, None, Some(row(key, "t")), vec![s.set(SPARSE, value)]);
        s.direct(10 + key as u64, write).await;
    }
    let mut scan = None;
    let usage = loop {
        match s.f.machine().observe_split_usage(s.f.authority, 1 << 20, scan).await.unwrap() {
            split_usage::ShardUsage::Scanning(next) => scan = Some(next),
            split_usage::ShardUsage::Observed { bytes, largest_image, .. } => break (bytes, largest_image),
            _ => panic!(),
        }
    };
    assert!(usage.0 > 40 * (100 << 10) && usage.0 < 41 * (100 << 10) + (64 << 10), "{usage:?}");
    assert!(usage.1 > (100 << 10) * 4 / 3, "{usage:?}");
    // A page of that size takes the largest image.
    let page = s.f.machine().committed_image_page(request(&s, 60, usage.1, None)).await.unwrap();
    assert!(matches!(page.payload().unwrap(), ImagePage::Images { images, .. } if !images.is_empty()));
}

/// L7: a restore checks every vector against the backup's search columns, and
/// a mismatch refuses the batch as a schema error before anything is written,
/// without quarantining anything.
#[tokio::test]
async fn vs16_a_restore_refuses_vectors_that_do_not_fit_the_backups_columns() {
    let mut s = S::new().await;
    fill(&mut s).await;
    let backup = images(&s, 50).await;
    let images: Vec<&CommittedImage> = backup.iter().collect();
    let schema = |columns| crate::oltp::mvcc::store::search::SearchSchema {
        epoch: 1,
        columns,
        high: MULTI,
        serving: Vec::new(),
        text: Vec::new(),
    };
    restore::check_schema(Some(&schema(columns())), &images).unwrap();
    // Another dimension, another kind, a column the backup does not have, none.
    let mut wider = columns();
    wider[0].dim = 3;
    let mut sparse = columns();
    sparse[3] = column(MULTI, VectorKind::Sparse, 0);
    let mut fewer = columns();
    fewer.remove(0);
    for (name, columns) in [("dimension", Some(wider)), ("kind", Some(sparse)), ("column", Some(fewer)), ("none", None)] {
        let mut target = s.f.group.clone();
        target.group_id = nabedra_rt::ids::uuid_v4();
        let store = s.f.pool.group(target.clone()).await.unwrap();
        let backup = backup.clone();
        let schema = columns.map(schema);
        let refused = store
            .transaction(
                ApplyMeta { position: position(0), command_digest: digest(7), membership: None },
                BUDGET,
                move |tx| {
                    let images: Vec<_> = backup.iter().collect();
                    // As `install_images` does: check, then install.
                    restore::check_schema(schema.as_ref(), &images)?;
                    restore::install_rows(tx, &target, &full_span(), ts(50), &images)?;
                    Ok(Transition::Commit(tx.copy_output(b"null")?))
                },
            )
            .await
            .err()
            .unwrap();
        assert_eq!(refused.code, ErrorCode::ValidationError, "{name}");
        assert!(refused.message.contains("search columns"), "{name}: {}", refused.message);
        let replica = Replica { machine: ParticipantMachine::new(store).unwrap(), shared: s.f.shared().clone(), index: 0 };
        assert!(replica.dump().await.is_empty(), "{name}: nothing installed");
        assert!(!s.f.pool.is_group_quarantined(replica.machine.store().identity().group_id), "{name}");
    }
    // Rows without vectors need no columns.
    let plain: Vec<_> = backup.iter().filter(|image| image.search.is_none()).collect();
    restore::check_schema(None, &plain).unwrap();
}


#[tokio::test]
async fn vs16_the_first_search_fence_matches_the_restored_columns() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    let backed_up = images(&s, 10).await;
    let schema = SearchSchema { epoch: 1, columns: columns(), high: 8, serving: Vec::new(), text: Vec::new() };
    let mut restored = restore_with_schema(&s, &backed_up, 10, 1, Some(schema)).await;

    let mut wrong = columns();
    wrong[0].dim += 1;
    let rejected = restored.submit(s.fence_from(0, 7, wrong)).await;
    assert_eq!(search_reply(rejected), SearchReply::Rejected(Fault::InvalidBinding));
    let ready = restored.submit(s.fence_from(0, 7, columns())).await;
    assert!(matches!(search_reply(ready), SearchReply::Fenced { epoch: 7, .. }));
    assert!(got(&restored.machine, s.f.key(1), 10).await.is_ok());

    let mut reused = columns();
    reused.push(column(8, VectorKind::Dense, 2));
    let rejected = restored.submit(s.fence_command(8, reused)).await;
    assert_eq!(search_reply(rejected), SearchReply::Rejected(Fault::InvalidBinding));
}

#[test]
fn vs16_a_restore_refuses_malformed_search_metadata_before_reading_rows() {
    let checked = SearchSchema {
        epoch: 1,
        columns: columns(),
        high: MULTI,
        serving: Vec::new(),
        text: Vec::new(),
    };
    assert!(restore::check_schema(Some(&checked), &[]).is_ok());
    let mut no_dimension = checked.clone();
    no_dimension.columns.last_mut().unwrap().dim = 0;
    let mut unordered = checked.clone();
    unordered.columns.swap(0, 1);
    let mut repeated = checked.clone();
    repeated.columns.insert(0, repeated.columns[0].clone());
    let mut low = checked.clone();
    low.high -= 1;
    let mut inactive = checked;
    inactive.epoch = 0;
    for schema in [no_dimension, unordered, repeated, low, inactive] {
        assert_eq!(
            restore::check_schema(Some(&schema), &[]).unwrap_err().code,
            ErrorCode::ValidationError
        );
    }
}

#[tokio::test]
async fn vs16_archive_of_a_split_keeps_the_root_without_embedding_attempts() {
    use crate::oltp::mvcc::store::split_closure::{fold_image, image_seed};

    let mut s = S::new().await;
    fill(&mut s).await;
    let _split = freeze(&mut s, 41).await;
    let source = images(&s, 41).await;
    let split_image = source.iter().find(|image| {
        image.search.as_ref().is_some_and(|search| !search.records.is_empty())
    }).unwrap();
    let mut archived_image = split_image.clone();
    archived_image.search.as_mut().unwrap().records.clear();
    let root = image_seed().unwrap();
    let published = fold_image(root, split_image).unwrap();
    assert_eq!(published, fold_image(root, &archived_image).unwrap());
    archived_image.search.as_mut().unwrap().source.0.clear();
    assert_ne!(published, fold_image(root, &archived_image).unwrap());
}

#[tokio::test]
async fn vs16_a_restore_refuses_an_undeclared_embedding_source() {
    let mut s = S::new().await;
    fill(&mut s).await;
    let backup = images(&s, 41).await;
    let image = backup.iter().find(|image| {
        image.search.as_ref().is_some_and(|search| {
            search.source.0.contains_key(&EMBEDDED)
                && !search.blobs.0.contains_key(&EMBEDDED)
        })
    }).unwrap();
    let mut schema = SearchSchema { epoch: 1, columns: columns(), high: MULTI, serving: Vec::new(), text: Vec::new() };
    assert!(restore::check_schema(Some(&schema), &[image]).is_ok());
    schema.columns.retain(|column| column.id != EMBEDDED);
    let error = restore::check_schema(Some(&schema), &[image]).unwrap_err();
    assert_eq!(error.code, ErrorCode::ValidationError);
}

/// The images and search columns of a backup page at `cut`, read with the
/// columns `search` of the cut (a backup) or without (a split copy).
async fn page_at(s: &S, cut: u64, search: Option<SearchSchema>) -> (Vec<CommittedImage>, Option<SearchSchema>) {
    let page = s.f.machine().committed_image_page(request_at(s, cut, 1 << 20, None, search)).await.unwrap();
    let ImagePage::Images { images, search, exhausted, .. } = page.payload().unwrap() else { panic!() };
    assert!(*exhausted);
    (images.iter().cloned().collect(), search.as_ref().map(|search| search.schema.clone()))
}
fn vector_columns(images: &[CommittedImage], key: MvccKey) -> Vec<ColumnId> {
    let image = images.iter().find(|image| image.key == key).unwrap();
    image.search.iter().flat_map(|search| search.vectors.iter().map(|vector| vector.column)).collect()
}

/// VEC-G10, rv-oltp F1 (was `VEC-BUG-backup-cut-current-search-schema`): a
/// backup at a cut takes the newest version at or below the cut with its
/// vector. An online `drop_column` fences the range after the cut and before
/// the backup reads its pages; the backup's request names the columns of
/// its cut, so the row keeps its vector and the page keeps the column. A
/// read without them (a split copy) follows the current columns: red for a
/// mutant that ignores the request's columns.
#[tokio::test]
async fn vs40_a_backup_below_a_later_drop_column_keeps_the_rows_vector() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    let (_, at_cut) = page_at(&s, 10, None).await;
    let at_cut = at_cut.unwrap();
    assert!(at_cut.columns.iter().any(|column| column.id == PLAIN));
    // Dropped after the cut.
    let mut kept = columns();
    kept.retain(|column| column.id != PLAIN);
    assert!(matches!(s.fence(kept).await, SearchReply::Fenced { .. }));
    let (images, schema) = page_at(&s, 10, Some(at_cut.clone())).await;
    assert_eq!(vector_columns(&images, s.f.key(1)), [PLAIN], "the row held a PLAIN vector at the cut");
    assert_eq!(schema.as_ref(), Some(&at_cut), "the page's columns are those of the cut");
    let (current, _) = page_at(&s, 10, None).await;
    assert!(vector_columns(&current, s.f.key(1)).is_empty(), "a split copy follows the range's columns");
}

/// The same for an online `add_column` after the cut: the page carries the
/// cut's columns, so the restored range's first fence, with the columns of
/// the Catalog at the cut, is taken.
#[tokio::test]
async fn vs41_a_restore_at_a_cut_before_a_later_add_column_becomes_ready() {
    let mut s = S::new().await;
    let write = s.write(1, None, Some(row(1, "a")), vec![s.set(PLAIN, s.dense(1))]);
    s.direct(10, write).await;
    let (_, at_cut) = page_at(&s, 10, None).await;
    let mut wider = columns();
    wider.push(column(MULTI + 1, VectorKind::Dense, 2));
    assert!(matches!(s.fence(wider).await, SearchReply::Fenced { .. }));
    let (backup, schema) = page_at(&s, 10, at_cut).await;
    let mut restored = restore_with_schema(&s, &backup, 10, 128, schema).await;
    let first = restored.submit(s.fence_from(0, 7, columns())).await;
    assert!(
        matches!(search_reply(first), SearchReply::Fenced { epoch: 7, .. }),
        "the columns of the cut are taken"
    );
}

/// rv-oltp F1: the cut's columns must be this range's history: each one the
/// range holds with the same definition, or one it dropped since. A column
/// the range never had, or another definition of a held id, is refused and
/// the backup fails rather than record other columns.
#[tokio::test]
async fn vs40_a_backup_refuses_cut_columns_that_are_not_the_ranges() {
    let s = S::new().await;
    let (_, at_cut) = page_at(&s, 10, None).await;
    let at_cut = at_cut.unwrap();
    let mut unknown = at_cut.clone();
    unknown.columns.push(column(at_cut.high + 1, VectorKind::Dense, 2));
    unknown.high += 1;
    let mut other = at_cut.clone();
    other.columns[0].dim += 1;
    for (name, cut) in [("a column the range never had", unknown), ("another definition", other)] {
        let error = s.f.machine().committed_image_page(request_at(&s, 10, 1 << 20, None, Some(cut))).await.err().unwrap();
        assert_eq!(error.code, ErrorCode::StorageUnavailable, "{name}");
    }
}

/// A backup with an explicitly plain cut omits columns fenced after it.
#[tokio::test]
async fn rvj_a_backup_of_a_table_plain_at_its_cut_omits_the_later_first_column() {
    let mut s = S::plain(Some(&FaultFs::new())).await;
    let write = s.write(1, None, Some(row(1, "a")), Vec::new());
    assert_eq!(s.direct(10, write).await, applied(10));
    assert_eq!(page_at(&s, 10, None).await.1, None);
    assert!(matches!(s.fence(vec![column(1, VectorKind::Dense, 2)]).await, SearchReply::Fenced { .. }));

    // Epoch zero represents an explicitly plain Catalog cut.
    let cut = SearchSchema {
        epoch: 0, high: 0, columns: Vec::new(), serving: Vec::new(), text: Vec::new(),
    };
    let (backup, schema) = page_at(&s, 10, Some(cut)).await;
    assert_eq!(schema, None);
    let mut restored = restore_with_schema(&s, &backup, 10, 128, schema).await;
    let plain_ddl = ParticipantCommand::DirectSchema(DirectSchemaFence {
        authority: s.f.authority,
        table_id: s.f.table,
        change: DirectSchemaChange::Fence { lifecycle: Some(nabedra_rt::ids::uuid_v4()) },
    });
    assert!(matches!(restored.submit(plain_ddl).await, ParticipantReply::DirectSchema(_)));
}
