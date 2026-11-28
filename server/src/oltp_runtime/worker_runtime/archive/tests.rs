//! Focused O19/O28 and R04 assertions through actual local Raft services,
//! immutable filesystem publication and native genesis. No source request replay.
use super::*;
use crate::request_queue::{Receipt, RequestState};
use serde_json::{Value, json};
mod plain_cut;
mod search;
async fn run(runtime: &Arc<WorkerRuntime>, request: Value) -> (Vec<u8>, Receipt) {
    let body = serde_json::to_vec(&request).unwrap();
    let record = runtime.admit(body.clone()).await.unwrap();
    let until = nabedra_rt::time::now() + std::time::Duration::from_secs(30);
    loop {
        let receipt = runtime
            .observe(record.run_id, record.request_id)
            .await
            .unwrap();
        if receipt.state.terminal() {
            assert_eq!(
                receipt.state,
                RequestState::Succeeded,
                "{:?}",
                receipt.error
            );
            return (body, receipt.receipt.clone());
        }
        assert!(
            nabedra_rt::time::now() < until,
            "actual request never settled"
        );
        nabedra_rt::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
fn request(operation: &str, payload: Value) -> Value {
    json!({"request_id":nabedra_rt::ids::uuid_v4(),"operation":operation,"payload":payload})
}
fn history_path(path: &Path) -> PathBuf {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".history-")
        })
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "inspection child for oltp_docker_archive.py"]
async fn actual_restored_pool_child() {
    use crate::{
        oltp::pool::{
            OltpPool, SharedResourceConfig, TransactionBudget, VerifiedPoolMount,
            WorkerSharedResources,
        },
        oltp_replication::{entry::stored_membership, manager::RegisteredConfig},
    };
    #[derive(Deserialize)]
    struct Config {
        node: RegisteredConfig,
        resources: WorkerResources,
    }
    const BUDGET: TransactionBudget = TransactionBudget {
        scratch_bytes: 256 << 10,
        batch_bytes: 0,
        output_bytes: 128 << 10,
    };
    let config: Config = serde_json::from_slice(
        &std::fs::read(std::env::var("NABEDRA_OLTP_TEST_RESTORE_POOL_CONFIG").unwrap()).unwrap(),
    )
    .unwrap();
    let source: Uuid = std::env::var("NABEDRA_OLTP_TEST_ARCHIVE_OLD_UUID")
        .unwrap()
        .parse()
        .unwrap();
    let fresh: Uuid = std::env::var("NABEDRA_OLTP_TEST_ARCHIVE_NEW_UUID")
        .unwrap()
        .parse()
        .unwrap();
    let mut data: GroupIdentity =
        serde_json::from_str(&std::env::var("NABEDRA_OLTP_TEST_ARCHIVE_DATA").unwrap()).unwrap();
    data.cluster_id = config.node.placements.catalog.identity.cluster_id;
    let shared = WorkerSharedResources::new(
        config.node.scheduler_node_id,
        config.resources,
        SharedResourceConfig {
            query_threads: config.resources.max_threads as usize,
            ..Default::default()
        },
    )
    .unwrap();
    let expected = config.node.mount.unwrap();
    let identity = expected.identity.clone();
    let mount = VerifiedPoolMount::verify(expected, None).unwrap();
    let pool = OltpPool::open_registered(mount, identity, shared).unwrap();
    let mut groups = Vec::new();
    for (group, voters) in config
        .node
        .placements
        .all()
        .into_iter()
        .map(|placement| (placement.identity.clone(), &placement.voters))
        .chain(std::iter::once((
            data,
            &config.node.placements.catalog.voters,
        )))
    {
        let store = pool.group(group.clone()).await.unwrap();
        let state = store.applied_state(BUDGET).await.unwrap().unwrap();
        let membership = stored_membership(state.membership.as_ref().unwrap().as_slice()).unwrap();
        let membership = membership.membership();
        let joint = membership.get_joint_config();
        assert_eq!(joint.len(), 1);
        assert_eq!(
            joint[0],
            voters
                .iter()
                .map(|voter| voter.node_id)
                .collect::<std::collections::BTreeSet<_>>()
        );
        for (node, expected) in membership.nodes() {
            assert_eq!(
                voters.iter().find(|voter| voter.node_id == *node),
                Some(expected)
            );
        }
        groups.push(json!({"group":group, "applied":state.position, "voters":voters}));
    }
    let status = pool
        .group(config.node.placements.status.identity.clone())
        .await
        .unwrap();
    let raw = status
        .read_metadata(BUDGET, move |view| {
            let mut records = Vec::new();
            for id in [source, fresh] {
                let mut key = b"status/identity/".to_vec();
                key.extend(id.as_bytes());
                let bytes = view.pinned(&key)?.ok_or_else(unknown)?;
                records.push(serde_json::from_slice::<Value>(&bytes).map_err(|_| unknown())?);
            }
            view.encode(&records)
        })
        .await
        .unwrap();
    let decisions: Vec<Value> = serde_json::from_slice(raw.as_slice()).unwrap();
    assert!(decisions[0]["restore_origin"].is_object());
    assert!(decisions[1]["restore_origin"].is_null());
    let fresh_commit = &decisions[1]["terminal"]["committed"]["decision"];
    assert_eq!(fresh_commit["txn"]["request_id"], json!(fresh));
    assert_eq!(
        fresh_commit["request"]["run_id"],
        json!(config.node.initial_run)
    );
    let position = status
        .applied_state(BUDGET)
        .await
        .unwrap()
        .unwrap()
        .position;
    assert!(fresh_commit["decided_at"]["index"].as_u64().unwrap() <= position.index);
    println!(
        "NABEDRA_RESTORE_POOL {}",
        json!({"mount":pool.observe_registered_mount().unwrap(),
        "groups":groups,"old_decision":decisions[0],"fresh_decision":decisions[1]})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o19_native_committed_images_pitr_uuid_and_checked_refusals() {
    struct TestDirectory(PathBuf);
    impl TestDirectory {
        fn path(&self) -> &Path {
            &self.0
        }
    }
    let directory = TestDirectory(
        std::env::temp_dir().join(format!("nabedra-native-archive-{}", nabedra_rt::ids::uuid_v4())),
    );
    std::fs::create_dir(directory.path()).unwrap();
    let source_path = directory.path().join("source");
    let execution = ExecutionConfig {
        max_pending_requests: 4,
        ..Default::default()
    };
    let resources = WorkerResources {
        max_threads: 2,
        ..Default::default()
    };
    let source = WorkerRuntime::open(
        &source_path,
        "test",
        "archive-test",
        execution.clone(),
        resources,
    )
    .await
    .unwrap();
    source.without_direct_for_test().await;
    let (_, created) = run(&source, request("table.create", json!({"name":"history","schema":{"id":"utf8","value":"int64"},"key":"id","format":"parquet"}))).await;
    let table = created.result.as_ref().unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (insert_body, inserted) = run(
        &source,
        request(
            "table.insert_many",
            json!({"table_id":table,"rows":[{"id":"a","value":1},{"id":"b","value":2}]}),
        ),
    )
    .await;
    assert_eq!(inserted.result, Some(json!(2)));
    let (_, obsolete) = run(
        &source,
        request(
            "table.create",
            json!({
                "name":"obsolete","schema":{"id":"utf8"},"key":"id","format":"parquet"
            }),
        ),
    )
    .await;
    let obsolete = obsolete.result.as_ref().unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let pending = request(
        "table.insert",
        json!({"table_id":table,"row":{"id":"pending","value":99}}),
    );
    let pending_body = serde_json::to_vec(&pending).unwrap();
    let pending_id: Uuid = serde_json::from_value(pending["request_id"].clone()).unwrap();
    let authority = source
        .services
        .control
        .current(&source.services.scope)
        .await
        .unwrap()
        .ready
        .view
        .authority();
    source
        .services
        .status
        .reserve_identity(
            authority,
            pending_id,
            Digest::from_bytes(Sha256::digest(&pending_body).into()),
            crate::oltp_replication::authority::status::RequestKind::Mutation,
        )
        .await
        .unwrap();
    // Export one captured identity cut while reservations and a terminal
    // outcome change between its bounded pages. Positions reconstruct the
    // old reserved state without retaining the whole identity inventory.
    for number in 1u128..=65 {
        let id = Uuid::from_u128(number);
        source
            .services
            .status
            .reserve_identity(
                authority,
                id,
                Digest::from_bytes(Sha256::digest(id.as_bytes()).into()),
                crate::oltp_replication::authority::status::RequestKind::Read,
            )
            .await
            .unwrap();
    }
    let race_id = Uuid::from_u128(u128::MAX);
    let race_digest = Digest::from_bytes(Sha256::digest(race_id.as_bytes()).into());
    source
        .services
        .status
        .reserve_identity(
            authority,
            race_id,
            race_digest,
            crate::oltp_replication::authority::status::RequestKind::Mutation,
        )
        .await
        .unwrap();
    let race_owner = BackupOwnerRef {
        backup_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    let race_pin = source
        .services
        .timestamps
        .begin_backup(authority, race_owner)
        .await
        .unwrap();
    let race_archive = nabedra_rt::ids::uuid_v4();
    let race_cut = source
        .services
        .timestamps
        .archive_closure(
            authority,
            race_owner,
            race_pin,
            race_archive,
            None,
            source.services.status.clone(),
        )
        .await
        .unwrap();
    let mut page = source
        .services
        .status
        .native_archive_page(&race_cut, None)
        .await
        .unwrap();
    assert!(
        !page.exhausted,
        "identity fixture did not cross a bounded page"
    );
    source
        .services
        .status
        .terminal_failure(
            authority,
            RequestBinding {
                run_id: authority.run_id,
                request_id: race_id,
                digest: race_digest,
                accepted_context: Digest::from_bytes([17; 32]),
            },
            ProtocolError::new(
                ErrorCode::ValidationError,
                "metadata-only test failure after captured cut",
                Outcome::NotApplied,
            ),
        )
        .await
        .unwrap();
    let late_id = Uuid::from_u128(66);
    source
        .services
        .status
        .reserve_identity(
            authority,
            late_id,
            Digest::from_bytes(Sha256::digest(late_id.as_bytes()).into()),
            crate::oltp_replication::authority::status::RequestKind::Read,
        )
        .await
        .unwrap();
    let mut old_count = 0u64;
    let mut old_root=crate::oltp_replication::authority::status::identity::transaction::archive_images::identity_inventory_seed(source.services.status.identity(),race_cut.status_applied_at()).unwrap();
    let mut saw_reserved_race = false;
    loop {
        for image in page.items.iter() {
            if let Some((id,root))=crate::oltp_replication::authority::status::identity::transaction::archive_images::identity_metadata_step(&source.services.shared,old_root,image).unwrap(){
                assert_ne!(id,late_id,"post-cut reservation leaked into identity coverage");
                if id==race_id {let value:Value=serde_json::from_str(&image.value).unwrap();assert!(value["terminal"].is_null(),"post-cut terminal replaced captured reserved state");saw_reserved_race=true;}
                old_count+=1;old_root=root;
            }
        }
        if page.exhausted {
            break;
        }
        page = source
            .services
            .status
            .native_archive_page(&race_cut, page.after.clone())
            .await
            .unwrap();
    }
    assert!(saw_reserved_race);
    assert_eq!(race_cut.statement().identity_count, Some(old_count));
    assert_eq!(race_cut.statement().identity_root, Some(old_root));
    source
        .services
        .timestamps
        .cancel_backup(
            authority,
            crate::oltp::backup::lifecycle::VerifiedBackupCancelled::after_settlement(
                crate::oltp::backup::lifecycle::BackupCancelledStatement {
                    owner: race_owner,
                    pin: race_pin,
                    archive_id: race_archive,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    // A settled failed publication closes its own attempt, rather than
    // accumulating a permanent storage pin on every new failed owner.
    let failed_owner = BackupOwnerRef {
        backup_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    let failed_path = directory.path().join("failed-publication");
    ensure_private_directory(&failed_path).unwrap();
    let failed_digest = serialized_digest(&(
        b"nabedra/native-backup-id/v1".as_slice(),
        source.cluster_id(),
        failed_owner,
    ))
    .unwrap();
    let failed_archive = Uuid::from_slice(&failed_digest.as_bytes()[..16]).unwrap();
    let conflicted = failed_path
        .join(source.cluster_id().to_string())
        .join(failed_archive.to_string());
    ensure_private_directory(conflicted.parent().unwrap()).unwrap();
    ensure_private_directory(&conflicted).unwrap();
    std::fs::write(
        conflicted.join("part-00000000000000000000"),
        b"conflicting immutable bytes",
    )
    .unwrap();
    assert!(
        source
            .backup_native(&failed_path, failed_owner, None)
            .await
            .is_err()
    );
    let failed_key = ArchiveKey {
        cluster_id: source.cluster_id(),
        archive_id: failed_archive,
    };
    let repository = ArchiveRepository::new(
        FilesystemRepository::open(
            &std::fs::canonicalize(&failed_path).unwrap(),
            source.services.shared.clone(),
        )
        .unwrap(),
        ArchiveKeys::load(None).unwrap(),
    );
    assert!(
        ArchiveReader::open(repository.clone(), failed_key)
            .await
            .is_err()
    );
    let cancellation =
        publication::read_cancellation(&repository, failed_key, failed_owner, None)
            .await
            .unwrap()
            .statement();
    assert_eq!(cancellation.archive_id, failed_archive);
    // Discard the first cancel acknowledgement and replay through the
    // no-active-pin recovery path using only the exact stored record.
    source
        .reconcile_native_cancellation(&repository, failed_key, failed_owner, None)
        .await
        .unwrap();
    let repeated_cancellation = publication::read_cancellation(
        &repository,
        failed_key,
        failed_owner,
        Some(cancellation.pin),
    )
    .await
    .unwrap()
    .statement();
    assert_eq!(repeated_cancellation.pin, cancellation.pin);
    assert!(
        source
            .services
            .timestamps
            .begin_backup(authority, failed_owner)
            .await
            .is_err(),
        "failed attempt pin remained active"
    );
    let owner = BackupOwnerRef {
        backup_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    let base_path = directory.path().join("base");
    let base = source.backup_native(&base_path, owner, None).await.unwrap();
    // Reconcile an exact completed owner rather than allocating a new cut.
    let retried = source.backup_native(&base_path, owner, None).await.unwrap();
    assert_eq!(base.pin, retried.pin);
    assert_eq!(base.repository_root, retried.repository_root);
    let base_repository = ArchiveRepository::new(
        FilesystemRepository::open(
            &std::fs::canonicalize(&base_path).unwrap(),
            source.services.shared.clone(),
        )
        .unwrap(),
        ArchiveKeys::load(None).unwrap(),
    );
    let matching_owner = BackupCancelledStatement {
        owner,
        pin: base.pin,
        archive_id: base.key.archive_id,
    };
    assert!(
        publication::settle_cancellation(&base_repository, base.key, matching_owner)
            .await
            .is_err(),
        "the published root cannot authorize cancellation"
    );
    let published = ArchiveReader::open(base_repository, base.key)
        .await
        .unwrap();
    assert_eq!(published.root().root_sha256(), base.repository_root);
    run(&source, request("table.drop", json!({"table_id":obsolete}))).await;
    let (_, added) = run(
        &source,
        request(
            "table.create",
            json!({
                "name":"empty_after_base","schema":{"id":"utf8"},"key":"id","format":"parquet"
            }),
        ),
    )
    .await;
    let added = added.result.as_ref().unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (_, updated) = run(
        &source,
        request(
            "table.update",
            json!({"table_id":table,"key":"a","values":{"value":7}}),
        ),
    )
    .await;
    assert_eq!(updated.result, Some(json!(true)));
    let first_path = directory.path().join("first");
    let first = source
        .backup_native(
            &first_path,
            BackupOwnerRef {
                attempt: 2,
                ..owner
            },
            Some(base.clone()),
        )
        .await
        .unwrap();
    let (future_body, future) = run(
        &source,
        request(
            "table.insert",
            json!({"table_id":table,"row":{"id":"future","value":77}}),
        ),
    )
    .await;
    let last_path = directory.path().join("last");
    let last = source
        .backup_native(
            &last_path,
            BackupOwnerRef {
                attempt: 3,
                ..owner
            },
            Some(first.clone()),
        )
        .await
        .unwrap();
    // A gap cannot pass even though both individual histories are complete.
    let mut plan = crate::oltp::backup::restore::RestorePlanValidator::new(last.pin.read_ts);
    for (index, path) in [&base_path, &last_path].iter().enumerate() {
        let mut reader = HistoryReader::new(
            identity_metadata_step,
            File::open(history_path(path)).unwrap(),
        )
        .unwrap();
        while reader.next(&source.services.shared).unwrap().is_some() {}
        let result = plan.include(&reader.verified().unwrap());
        if index == 0 {
            result.unwrap();
        } else {
            assert!(result.is_err(), "missing segment accepted");
        }
    }
    // A checksummed writer refuses an omitted participant before it can write
    // a matching transaction footer. This checks semantic completeness.
    let mut reader = HistoryReader::new(
        identity_metadata_step,
        File::open(history_path(&base_path)).unwrap(),
    )
    .unwrap();
    let mut writer = HistoryWriter::new(identity_metadata_step, Vec::new()).unwrap();
    let mut omitted = false;
    let mut refused = false;
    while let Some(frame) = reader.next(&source.services.shared).unwrap() {
        if matches!(
            frame.payload().unwrap(),
            HistoryFrame::ParticipantManifest(_)
        ) && !omitted
        {
            omitted = true;
            continue;
        }
        if matches!(frame.payload().unwrap(), HistoryFrame::EndArchive { .. }) {
            break;
        }
        if writer
            .write(frame.payload().unwrap(), &source.services.shared)
            .is_err()
        {
            refused = true;
            break;
        }
    }
    assert!(omitted && refused, "missing participant accepted");
    // Recomputing every frame checksum/footer must not conceal a lost
    // metadata-only reserved UUID. The authoritative Status inventory binds it.
    let mut identity_reader = HistoryReader::new(
        identity_metadata_step,
        File::open(history_path(&base_path)).unwrap(),
    )
    .unwrap();
    let mut identity_writer = HistoryWriter::new(identity_metadata_step, Vec::new()).unwrap();
    let mut omitted_identity = false;
    let mut omitted_identity_metadata = false;
    while let Some(frame) = identity_reader.next(&source.services.shared).unwrap() {
        match frame.payload().unwrap() {
            HistoryFrame::Identity { identity, .. }
                if identity.reservation.request_id == pending_id =>
            {
                omitted_identity = true;
                continue;
            }
            HistoryFrame::Metadata(image)
                if image.key.starts_with(b"status/identity/")
                    && image.key.ends_with(pending_id.as_bytes()) =>
            {
                omitted_identity_metadata = true;
                continue;
            }
            HistoryFrame::EndArchive { .. } => break,
            _ => identity_writer
                .write(frame.payload().unwrap(), &source.services.shared)
                .unwrap(),
        }
    }
    assert!(omitted_identity && omitted_identity_metadata);
    assert!(
        identity_writer.finish(&source.services.shared).is_err(),
        "missing reserved identity accepted with recomputed checksums"
    );
    let original = std::fs::read(history_path(&base_path)).unwrap();
    let mut truncated = HistoryReader::new(
        identity_metadata_step,
        std::io::Cursor::new(&original[..original.len() - 1]),
    )
    .unwrap();
    let mut truncation_refused = false;
    loop {
        match truncated.next(&source.services.shared) {
            Err(_) => {
                truncation_refused = true;
                break;
            }
            Ok(None) => break,
            Ok(Some(_)) => {}
        }
    }
    assert!(truncation_refused, "truncated footer accepted");
    let mut corrupted = original.clone();
    corrupted[100] ^= 1;
    let mut corrupted =
        HistoryReader::new(identity_metadata_step, std::io::Cursor::new(corrupted)).unwrap();
    assert!(
        corrupted.next(&source.services.shared).is_err(),
        "checksum corruption accepted"
    );
    source.stop_accepting();
    source.drain().await.unwrap();
    drop(source);
    let mounted = directory.path().join("mounted-target");
    std::fs::create_dir(&mounted).unwrap();
    let restore_started = nabedra_rt::time::now_std();
    let summary = WorkerRuntime::restore_native(
        &[base_path.clone(), first_path, last_path],
        Some(first.pin.read_ts),
        &mounted,
        "test",
        "archive-test",
        execution.clone(),
        resources,
    )
    .await
    .unwrap();
    let restore_elapsed = nabedra_rt::time::elapsed(restore_started);
    println!(
        "{}",
        serde_json::json!({"scope":"local-native-O19-checkpoint","restore_seconds":restore_elapsed.as_secs_f64(),"target_cut":summary.target_cut,"segments":summary.segments,"images":summary.images,"identities":summary.identities})
    );
    assert_eq!(summary.target_cut, first.pin.read_ts);
    assert_eq!(summary.segments, 3);
    let target = WorkerRuntime::open_configured(
        &mounted,
        "test",
        "archive-test",
        execution,
        resources,
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        target
            .services
            .replica
            .lock()
            .unwrap()
            .stats()
            .accepted_records,
        0
    );
    assert_ne!(target.cluster_id(), base.scope.logical_cluster);
    let (_, descriptor) = run(
        &target,
        request(
            "table.describe",
            json!({
                "name":"empty_after_base"
            }),
        ),
    )
    .await;
    assert_eq!(descriptor.result.as_ref().unwrap()["id"], added);
    run(
        &target,
        request(
            "table.insert",
            json!({
                "table_id":added,"row":{"id":"first"}
            }),
        ),
    )
    .await;
    let old_id = Uuid::parse_str(&obsolete).unwrap();
    let mut reader = HistoryReader::new(
        identity_metadata_step,
        File::open(history_path(&base_path)).unwrap(),
    )
    .unwrap();
    let old_group = loop {
        let frame = reader.next(&target.services.shared).unwrap().unwrap();
        if let HistoryFrame::BeginGroup(scope) = frame.payload().unwrap()
            && matches!(scope.group.kind, GroupKind::Data {table_id, ..} if table_id == old_id)
        {
            break scope.group.group_id;
        }
    };
    assert!(
        target
            .services
            .control
            .group(old_group)
            .await
            .unwrap()
            .is_none()
    );
    let reconciled = target
        .observe(inserted.run_id, inserted.request_id)
        .await
        .unwrap();
    assert_eq!(reconciled.result, inserted.result);
    assert_eq!(reconciled.request_id, inserted.request_id);
    assert_eq!(reconciled.state, RequestState::Succeeded);
    drop(reconciled);
    let existing = target.admit(insert_body).await.unwrap();
    assert_eq!(existing.request_id, inserted.request_id);
    assert_eq!(existing.state, RequestState::Succeeded);
    assert!(
        target.admit(pending_body).await.is_err(),
        "reserved old UUID executed"
    );
    assert!(
        target.admit(future_body).await.is_err(),
        "post-cut UUID executed"
    );
    let (_, value) = run(
        &target,
        request("table.get", json!({"table_id":table,"key":"a"})),
    )
    .await;
    assert_eq!(value.result, Some(json!({"id":"a","value":7})));
    let (_, value) = run(
        &target,
        request("table.get", json!({"table_id":table,"key":"future"})),
    )
    .await;
    // Receipt conversion intentionally omits JSON null in both normal and
    // restored runs; an absent row is represented by result=None.
    assert_eq!(value.result, None);
    let old_future = target.observe(future.run_id, future.request_id).await;
    assert!(old_future.is_err() || old_future.unwrap().state != RequestState::Succeeded);
    run(
        &target,
        request(
            "table.insert",
            json!({"table_id":table,"row":{"id":"new","value":88}}),
        ),
    )
    .await;
    target.stop_accepting();
    target.drain().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o12_o19_native_index_closure_pitr_and_repeat_restore() {
    use crate::oltp::mvcc::{
        index::{IndexClosedKind, IndexDefinition, IndexPhase, IndexRecord},
        store::index::{ArchivedIndexClosure, IndexClosureArchiveOrigin},
    };

    async fn authority(runtime: &WorkerRuntime) -> Authority {
        runtime
            .services
            .control
            .current(&runtime.services.scope)
            .await
            .unwrap()
            .ready
            .view
            .authority()
    }

    async fn index_at(
        runtime: &WorkerRuntime,
        cut: CommitTs,
        index: &IndexDefinition,
    ) -> IndexRecord {
        let snapshot = runtime
            .services
            .catalog
            .snapshot(cut, authority(runtime).await)
            .unwrap();
        match runtime
            .services
            .catalog
            .read::<IndexRecord>(
                &snapshot,
                MvccKey::CatalogIndex {
                    table_id: index.table_id,
                    index_id: index.index_id,
                },
            )
            .await
            .unwrap()
        {
            CatalogRead::Value {
                record: Some(record),
                ..
            } => (*record).clone(),
            _ => panic!("index missing at archive cut {cut:?}"),
        }
    }

    fn owner() -> BackupOwnerRef {
        BackupOwnerRef {
            backup_id: nabedra_rt::ids::uuid_v4(),
            attempt: 1,
        }
    }
    fn next_owner(previous: &NativeBackupManifest) -> BackupOwnerRef {
        BackupOwnerRef {
            backup_id: previous.owner.backup_id,
            attempt: previous.owner.attempt + 1,
        }
    }

    fn archived_closure(path: &Path, runtime: &WorkerRuntime) -> ArchivedIndexClosure {
        let mut reader = HistoryReader::new(
            identity_metadata_step,
            File::open(history_path(path)).unwrap(),
        )
        .unwrap();
        let mut closure = None;
        while let Some(frame) = reader.next(&runtime.services.shared).unwrap() {
            if let HistoryFrame::CatalogIndexClosure(evidence) = frame.payload().unwrap() {
                assert!(closure.replace(evidence.clone()).is_none());
            }
        }
        reader.verified().unwrap();
        closure.expect("original Catalog closure missing in archive")
    }

    let path = std::env::temp_dir().join(format!("nabedra-index-archive-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    let execution = ExecutionConfig {
        max_pending_requests: 8,
        ..Default::default()
    };
    let resources = WorkerResources {
        max_threads: 2,
        ..Default::default()
    };
    let source = WorkerRuntime::open(
        &path.join("source"),
        "test",
        "archive-index",
        execution.clone(),
        resources,
    )
    .await
    .unwrap();
    source.without_direct_for_test().await;
    let (_, table) = run(
        &source,
        request(
            "table.create",
            json!({
                "name":"archive_index", "schema":{"id":"utf8","value":"int64"},
                "key":"id","format":"parquet"
            }),
        ),
    )
    .await;
    let table_id: Uuid = serde_json::from_value(table.result.unwrap()["id"].clone()).unwrap();
    let (original_body, original_receipt) = run(
        &source,
        request(
            "table.insert_many",
            json!({
                "table_id":table_id, "rows":[{"id":"old","value":7}]
            }),
        ),
    )
    .await;
    let base_path = path.join("base");
    let base = source
        .backup_native(&base_path, owner(), None)
        .await
        .unwrap();

    let definition = IndexDefinition {
        table_id,
        index_id: nabedra_rt::ids::uuid_v4(),
        name: Text::try_from("by_value").unwrap(),
        columns: Page::new(vec![Text::try_from("value").unwrap()]).unwrap(),
        covering: Page::new(Vec::new()).unwrap(),
    };
    let maintenance = MaintenanceOwnerRef {
        catalog_group_id: base.scope.catalog_group,
        table_id,
        index_id: definition.index_id,
        build_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    source
        .create_index(
            definition.clone(),
            maintenance.build_id,
            maintenance.attempt,
        )
        .await
        .unwrap();
    let ready_path = path.join("ready");
    let ready = source
        .backup_native(&ready_path, next_owner(&base), Some(base))
        .await
        .unwrap();
    let ready_index = index_at(&source, ready.scope.cut, &definition).await;
    assert!(matches!(ready_index.phase, IndexPhase::Ready { .. }));
    let catalog = source.services.catalog.participant();
    let proof = catalog
        .strong_index_attempt_closed(maintenance)
        .await
        .unwrap();
    let original = proof.archive_evidence().clone();
    assert_eq!(original.statement.kind, IndexClosedKind::Ready);
    assert!(proof.restored_observation().is_none());
    assert_eq!(archived_closure(&ready_path, &source), original);
    {
        let mut reader = HistoryReader::new(
            identity_metadata_step,
            File::open(history_path(&ready_path)).unwrap(),
        )
        .unwrap();
        let mut writer = HistoryWriter::new(identity_metadata_step, Vec::new()).unwrap();
        let mut removed = false;
        let mut refused = false;
        while let Some(frame) = reader.next(&source.services.shared).unwrap() {
            let frame = frame.payload().unwrap();
            if matches!(frame, HistoryFrame::CatalogIndexClosure(_)) {
                removed = true;
            } else if writer.write(frame, &source.services.shared).is_err() {
                assert!(removed);
                refused = true;
                break;
            }
        }
        assert!(
            removed && refused,
            "accepted a closure row without its original evidence"
        );
    }

    let ready_base_path = path.join("ready-as-base");
    let ready_base = source
        .backup_native(&ready_base_path, owner(), None)
        .await
        .unwrap();
    assert_eq!(archived_closure(&ready_base_path, &source), original);
    VerifiedNativeRestore::verify(
        std::slice::from_ref(&ready_base_path),
        None,
        "test",
        None,
        source.services.shared.clone(),
        None,
        ArchiveKeys::load(None).unwrap(),
    )
    .await
    .unwrap();
    {
        let shared = &source.services.shared;
        let mut graph = super::restore_graph::RestoreGraph::new(shared.clone()).unwrap();
        let mut reader = HistoryReader::new(
            identity_metadata_step,
            File::open(history_path(&ready_base_path)).unwrap(),
        )
        .unwrap();
        let mut group = None;
        let mut removed = 0;
        while let Some(frame) = reader.next(shared).unwrap() {
            match frame.payload().unwrap() {
                HistoryFrame::BeginGroup(scope) => {
                    graph.group(&ready_base.scope, scope, true).unwrap();
                    group = Some(scope.clone());
                }
                HistoryFrame::EndGroup { .. } => group = None,
                HistoryFrame::Image(image) => {
                    if matches!(
                        image.key,
                        MvccKey::CatalogIndex { .. } | MvccKey::CatalogIndexClosed { .. }
                    ) {
                        removed += 1;
                    } else {
                        graph
                            .image(
                                group.as_ref().unwrap(),
                                &image.key,
                                image.value.as_ref(),
                                image.commit_ts,
                                true,
                            )
                            .unwrap();
                    }
                }
                _ => {}
            }
        }
        assert_eq!(removed, 2);
        assert!(
            graph.close(ready_base.scope.cut).is_err(),
            "dangling index inventory accepted"
        );
    }

    source.retire_index(&ready_index).await.unwrap();
    let retired_path = path.join("retired");
    let retired = source
        .backup_native(&retired_path, next_owner(&ready), Some(ready.clone()))
        .await
        .unwrap();
    let retired_index = index_at(&source, retired.scope.cut, &definition).await;
    assert!(matches!(retired_index.phase, IndexPhase::Retired { .. }));
    let retired_proof = catalog
        .strong_index_attempt_closed(maintenance)
        .await
        .unwrap();
    assert_eq!(retired_proof.archive_evidence(), &original);
    source
        .services
        .timestamps
        .release_maintenance(authority(&source).await, proof)
        .await
        .unwrap();
    source.drain().await.unwrap();
    drop(source);

    let paths = [base_path, ready_path, retired_path];
    let mut expected = original.clone();
    expected.first_archive = Some(IndexClosureArchiveOrigin {
        archive_id: ready.scope.archive_id,
        history_root: ready.history_root,
        cut: ready.scope.cut,
    });

    // Restore at Ready even though the complete input ends at Retired. Then
    // restore the retired view and export it as the base of a separate restore.
    let mut observed_targets = Vec::new();
    let mut repeat_path = None;
    for (name, input, cut, is_ready) in [
        ("pitr-ready", paths.to_vec(), Some(ready.scope.cut), true),
        ("pitr-retired", paths.to_vec(), None, false),
        ("repeat-retired", Vec::new(), None, false),
    ] {
        let input = if input.is_empty() {
            vec![repeat_path.take().unwrap()]
        } else {
            input
        };
        let destination = path.join(name);
        let started = nabedra_rt::time::now_std();
        let restored = WorkerRuntime::restore_native(
            &input,
            cut,
            &destination,
            "test",
            "archive-index",
            execution.clone(),
            resources,
        )
        .await
        .unwrap();
        let restore_seconds = nabedra_rt::time::elapsed(started).as_secs_f64();
        assert_eq!(
            restored.target_cut,
            cut.unwrap_or_else(|| {
                WorkerRuntime::native_backup_manifest(input.last().unwrap())
                    .unwrap()
                    .scope
                    .cut
            })
        );
        let target = WorkerRuntime::open_configured(
            &destination,
            "test",
            "archive-index",
            execution.clone(),
            resources,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            target
                .services
                .replica
                .lock()
                .unwrap()
                .stats()
                .accepted_records,
            0
        );
        assert_ne!(target.cluster_id(), original.source.cluster_id);
        assert!(
            observed_targets
                .iter()
                .all(|earlier| earlier != &target.cluster_id())
        );
        observed_targets.push(target.cluster_id());

        let index = index_at(&target, restored.target_cut, &definition).await;
        assert_eq!(index.phase.ready(), is_ready);
        if !is_ready {
            assert!(matches!(index.phase, IndexPhase::Retired { .. }));
        }
        let root_proof = target
            .services
            .catalog
            .participant()
            .strong_index_attempt_closed(maintenance)
            .await
            .unwrap();
        assert_eq!(root_proof.archive_evidence(), &expected);
        let observation = root_proof.restored_observation().unwrap().clone();
        assert_eq!(
            &observation.target,
            target.services.catalog.participant().identity()
        );
        assert_ne!(observation.target, original.source);
        assert_eq!(
            root_proof.terminal_position(),
            original.statement.terminal_position
        );
        let released = authority(&target).await;
        for _ in 0..2 {
            target
                .services
                .timestamps
                .release_maintenance(released, root_proof.clone())
                .await
                .unwrap();
        }
        let expired = target
            .services
            .timestamps
            .begin_maintenance(released, maintenance)
            .await
            .unwrap_err();
        assert!(
            expired.message.contains("PinExpired"),
            "{}",
            expired.message
        );

        let reconciled = target.admit(original_body.clone()).await.unwrap();
        assert_eq!(reconciled.state, RequestState::Succeeded);
        let receipt = target
            .observe(original_receipt.run_id, original_receipt.request_id)
            .await
            .unwrap();
        assert_eq!(receipt.result, original_receipt.result);
        drop(receipt);
        let (_, row) = run(
            &target,
            request(
                "table.get",
                json!({
                    "table_id":table_id, "key":"old"
                }),
            ),
        )
        .await;
        assert_eq!(row.result, Some(json!({"id":"old","value":7})));
        let (_, explanation) = run(
            &target,
            request(
                "query.explain",
                json!({
                    "table_id":table_id,
                    "query":{"select":["id"],"filter":{"column":"value","op":"eq","value":7},
                        "order_by":null,"limit":null}
                }),
            ),
        )
        .await;
        assert_eq!(
            explanation
                .result
                .unwrap()
                .as_str()
                .unwrap()
                .contains("secondary by_value"),
            is_ready
        );

        println!(
            "{}",
            json!({
                "case":name, "restore_seconds":restore_seconds, "cut":restored.target_cut,
                "original":original.statement, "target_observation":observation
            })
        );
        if name == "pitr-retired" {
            let (_, fresh) = run(
                &target,
                request(
                    "table.insert_many",
                    json!({
                        "table_id":table_id, "rows":[{"id":"fresh","value":8}]
                    }),
                ),
            )
            .await;
            assert_eq!(fresh.result, Some(json!(1)));
            let output = path.join("repeat");
            target.backup_native(&output, owner(), None).await.unwrap();
            assert_eq!(archived_closure(&output, &target), expected);
            repeat_path = Some(output);
        } else if name == "repeat-retired" {
            let (_, row) = run(
                &target,
                request(
                    "table.get",
                    json!({
                        "table_id":table_id, "key":"fresh"
                    }),
                ),
            )
            .await;
            assert_eq!(row.result, Some(json!({"id":"fresh","value":8})));
        }
        target.drain().await.unwrap();
    }
    std::fs::remove_dir_all(path).unwrap();
}

/// RV-T4 F2 gate: while direct commits are active, an incremental backup is
/// refused with an error that says to take a full backup, and a full backup
/// still runs (its committed images carry `'D'`/`'E'`; see
/// `d03_o19_dedup_records_survive_backup_images_and_restore`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o19_incremental_backups_are_refused_while_direct_commits_are_active() {
    let path = std::env::temp_dir().join(format!("nabedra-direct-backup-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(path.clone());
    let source = WorkerRuntime::open(
        &path.join("source"),
        "test",
        "direct-backup",
        ExecutionConfig {
            max_pending_requests: 8,
            ..Default::default()
        },
        WorkerResources {
            max_threads: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    source.without_direct_for_test().await;
    assert!(!source.direct_commits_active(), "off by default");
    source.set_direct_commits_active(true);
    let backup_id = nabedra_rt::ids::uuid_v4();
    let base = source
        .backup_native(
            &path.join("base"),
            BackupOwnerRef {
                backup_id,
                attempt: 1,
            },
            None,
        )
        .await
        .expect("a full backup runs while direct commits are active");
    let refused = source
        .backup_native(
            &path.join("incremental"),
            BackupOwnerRef {
                backup_id,
                attempt: 2,
            },
            Some(base.clone()),
        )
        .await
        .expect_err("an incremental backup would miss direct commits");
    assert_eq!(refused.code, ErrorCode::UnsupportedOperation);
    assert!(
        refused.message.contains("take a full backup"),
        "{}",
        refused.message
    );
    assert!(!path.join("incremental").exists(), "nothing was written");
    // Without direct commits, the same incremental backup runs.
    source.set_direct_commits_active(false);
    source
        .backup_native(
            &path.join("incremental"),
            BackupOwnerRef {
                backup_id,
                attempt: 2,
            },
            Some(base),
        )
        .await
        .unwrap();
}

/// T7: a lone worker is every member, so its own advert activates
/// `data.direct.v1` in Control; the activation hook turns on the T4fix F2
/// gate, and incremental backups are refused from then on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o02_o19_t7_a_lone_worker_activates_data_direct_v1_and_refuses_incremental_backups() {
    let path = std::env::temp_dir().join(format!("nabedra-direct-activation-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(path.clone());
    let source = WorkerRuntime::open(
        &path.join("source"),
        "test",
        "direct-activation",
        ExecutionConfig {
            max_pending_requests: 8,
            ..Default::default()
        },
        WorkerResources {
            max_threads: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let switch = source.direct_switch().expect("every runtime has a switch").clone();
    // True in every release build; false only in T9's `NABEDRA_TEST_DIRECT_OLTP=off` run.
    assert_eq!(
        switch.direct_oltp(),
        crate::config::direct_oltp_default(),
        "direct_oltp follows its default"
    );
    let deadline = nabedra_rt::time::now() + std::time::Duration::from_secs(60);
    while !switch.is_direct_active() {
        assert!(nabedra_rt::time::now() < deadline, "data.direct.v1 never activated");
        nabedra_rt::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(switch.requires_forward());
    assert_eq!(switch.routes_direct(), switch.direct_oltp());
    assert!(source.direct_commits_active(), "activation set the backup gate");
    assert!(switch.refresh().await.unwrap(), "Control reports it active");
    let backup_id = nabedra_rt::ids::uuid_v4();
    let base = source
        .backup_native(
            &path.join("base"),
            BackupOwnerRef {
                backup_id,
                attempt: 1,
            },
            None,
        )
        .await
        .expect("a full backup runs while direct commits are active");
    // Model the interval before this worker's watcher updates the archive
    // gate. Control has already activated the capability.
    source.set_direct_commits_active(false);
    let refused = source
        .backup_native(
            &path.join("incremental"),
            BackupOwnerRef {
                backup_id,
                attempt: 2,
            },
            Some(base),
        )
        .await
        .expect_err("incremental backups are refused once active");
    assert_eq!(refused.code, ErrorCode::UnsupportedOperation);
    assert!(
        refused.message.contains("take a full backup"),
        "{refused:?}"
    );
    assert!(!path.join("incremental").exists());
}
