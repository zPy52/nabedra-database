use super::*;

/// VEC-G10, rv-j F3: an existing backup pin still exports the plain table
/// after an online add fences the range. Its restore accepts a plain DDL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vs40_a_plain_backup_cut_omits_the_first_column_added_after_it() {
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let path =
        std::env::temp_dir().join(format!("nabedra-plain-cut-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let execution = || ExecutionConfig {
        max_pending_requests: 8,
        ..Default::default()
    };
    let resources = || WorkerResources {
        max_threads: 2,
        ..Default::default()
    };
    let source = WorkerRuntime::open(
        &path.join("source"),
        "test",
        "plain-cut",
        execution(),
        resources(),
    )
    .await
    .unwrap();
    source.without_direct_for_test().await;
    source
        .services
        .manager
        .cluster_features()
        .enable(crate::oltp_replication::authority::control::VECTOR_FEATURE);
    let (_, created) = run(
        &source,
        request(
            "table.create",
            json!({
                "name": "docs", "schema": {"id": "utf8"}, "key": "id", "format": "parquet"
            }),
        ),
    )
    .await;
    let table: Uuid = created.result.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    run(
        &source,
        request(
            "table.insert",
            json!({"table_id": table, "row": {"id": "a"}}),
        ),
    )
    .await;

    let owner = BackupOwnerRef {
        backup_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    let authority = source
        .services
        .control
        .current(&source.services.scope)
        .await
        .unwrap()
        .ready
        .view
        .authority();
    let pin = source
        .services
        .timestamps
        .begin_backup(authority, owner)
        .await
        .unwrap();
    run(
        &source,
        request(
            "table.add_column",
            json!({
                "table_id": table, "name": "v", "type": {"type": "vector", "dim": 2}
            }),
        ),
    )
    .await;

    let backup = path.join("backup");
    let manifest = source.backup_native(&backup, owner, None).await.unwrap();
    assert_eq!(manifest.pin, pin);
    let mut reader = HistoryReader::new(
        identity_metadata_step,
        File::open(history_path(&backup)).unwrap(),
    )
    .unwrap();
    while let Some(frame) = reader.next(&source.services.shared).unwrap() {
        assert!(!matches!(
            frame.payload().unwrap(),
            HistoryFrame::GroupSearch(_)
        ));
    }
    source.drain().await.unwrap();
    drop(source);

    let destination = path.join("restored");
    WorkerRuntime::restore_native(
        &[backup],
        None,
        &destination,
        "test",
        "plain-cut",
        execution(),
        resources(),
    )
    .await
    .unwrap();
    let target = WorkerRuntime::open_configured(
        &destination,
        "test",
        "plain-cut",
        execution(),
        resources(),
        true,
    )
    .await
    .unwrap();
    run(&target, request("table.drop", json!({"table_id": table}))).await;
    target.drain().await.unwrap();
}
