//! C1/C2 integration (review-C2 (3) and (5), test-C1 "C2 integration seams"):
//! a table with a vector column on a real lone runtime, whose Data group took
//! its search columns, through the queued path, a native backup and a restore.
//! Control does not send `SearchFence` yet (task J), so the tests propose it
//! on the group as the DDL coordinator will.
use super::*;
use crate::oltp::{
    mvcc::{
        store::{
            ParticipantCommand, ParticipantReply,
            search::{SearchFence, SearchReply, SearchSchema},
        },
        vector::{SearchColumn, VectorKind},
    },
    pool::ByteClass,
};
use crate::oltp_replication::authority::control::VECTOR_FEATURE;

struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn execution() -> ExecutionConfig {
    ExecutionConfig {
        max_pending_requests: 8,
        ..Default::default()
    }
}
fn resources() -> WorkerResources {
    WorkerResources {
        max_threads: 2,
        ..Default::default()
    }
}
fn owner() -> BackupOwnerRef {
    BackupOwnerRef {
        backup_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    }
}
fn dense(dim: u32) -> SearchColumn {
    SearchColumn {
        id: 1,
        kind: VectorKind::Dense,
        dim,
        source: None,
    }
}
/// What a backup holds for the one Data group of `table`.
struct Archived {
    group: GroupIdentity,
    search: Option<SearchSchema>,
    /// Each row image's key and value, in archive order.
    images: Vec<(String, Option<String>)>,
}
fn archived(runtime: &WorkerRuntime, backup: &Path, table: Uuid) -> Archived {
    let mut reader = HistoryReader::new(
        identity_metadata_step,
        File::open(history_path(backup)).unwrap(),
    )
    .unwrap();
    let mut found: Option<Archived> = None;
    let mut inside = false;
    while let Some(frame) = reader.next(&runtime.services.shared).unwrap() {
        match frame.payload().unwrap() {
            HistoryFrame::BeginGroup(scope) => {
                inside = matches!(scope.group.kind, GroupKind::Data { table_id, .. } if table_id == table);
                if inside {
                    assert!(found.is_none(), "one range");
                    found = Some(Archived {
                        group: scope.group.clone(),
                        search: None,
                        images: Vec::new(),
                    });
                }
            }
            HistoryFrame::GroupSearch(schema) if inside => {
                let found = found.as_mut().unwrap();
                assert!(found.search.is_none(), "one search frame per group");
                assert!(found.images.is_empty(), "the columns come before the rows");
                found.search = Some((**schema).clone());
            }
            HistoryFrame::Image(image) if inside => found.as_mut().unwrap().images.push((
                serde_json::to_string(&image.key).unwrap(),
                image.value.as_ref().map(|value| value.as_str().to_owned()),
            )),
            _ => {}
        }
    }
    found.expect("the table's range is in the backup")
}
async fn fence(
    runtime: &WorkerRuntime,
    group: &GroupIdentity,
    table: Uuid,
    epoch: u64,
    columns: Vec<SearchColumn>,
) -> ParticipantReply {
    let handle = runtime.services.resolver.participant(group).await.unwrap();
    let authority = handle.epochs().await.unwrap().authority;
    let command = OwnedCommand::new_owned(
        ParticipantCommand::SearchFence(SearchFence {
            authority,
            table_id: table,
            epoch,
            // A created table's first lifecycle id is its table id.
            lifecycle: Some(table),
            columns,
            serving: Vec::new(),
            text: Vec::new(),
        }),
        handle.resources(),
        ByteClass::Progress,
    )
    .unwrap();
    handle.propose(command).await.unwrap().reply
}
fn fenced(reply: &ParticipantReply) -> bool {
    matches!(reply, ParticipantReply::Search(SearchReply::Fenced { .. }))
}
const ROWS: &str = r#"[{"id":"a","v":{"f32":"AACAPwAAAEA="}},{"id":"b","v":null}]"#;

/// A lone runtime with `data.vector.v1` observed and queued writes, a table
/// with a dense column and two rows, and its Data group (from a first backup).
async fn search_table(path: &Path) -> (Arc<WorkerRuntime>, Uuid, GroupIdentity) {
    let source = WorkerRuntime::open(
        &path.join("source"),
        "test",
        "archive-search",
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
        .enable(VECTOR_FEATURE);
    let (_, created) = run(
        &source,
        request(
            "table.create",
            json!({"name":"docs","schema":{"id":"utf8","v":{"type":"vector","dim":2}},
                "key":"id","format":"parquet"}),
        ),
    )
    .await;
    let table: Uuid = created.result.as_ref().unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let rows: Value = serde_json::from_str(ROWS).unwrap();
    let (_, inserted) = run(
        &source,
        request("table.insert_many", json!({"table_id":table,"rows":rows})),
    )
    .await;
    assert_eq!(inserted.result, Some(json!(2)));
    let plain = path.join("plain");
    source.backup_native(&plain, owner(), None).await.unwrap();
    let before = archived(&source, &plain, table);
    // Before its first fence the range is plain: no search frame (DB-5 bytes).
    assert!(before.search.is_none());
    assert_eq!(before.images.len(), 2);
    (source, table, before.group)
}

/// Review-C2 (3) and (5): once its range has search columns, every image page
/// of the range carries them, so a full backup holds one `GroupSearch` frame
/// before the rows. A restore installs it: the restored range takes a first
/// fence only with the backup's columns, and keeps every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vs16_a_search_table_survives_a_native_backup_and_restore_with_its_columns() {
    let path = std::env::temp_dir().join(format!("nabedra-search-archive-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let (source, table, group) = search_table(&path).await;
    assert!(fenced(&fence(&source, &group, table, 1, vec![dense(2)]).await));
    let backup = path.join("search");
    source.backup_native(&backup, owner(), None).await.unwrap();
    let archive = archived(&source, &backup, table);
    assert_eq!(archive.group, group);
    let schema = archive.search.expect("a GroupSearch frame");
    assert_eq!((schema.epoch, schema.columns.clone(), schema.high), (1, vec![dense(2)], 1));
    let rows: Vec<Value> = archive
        .images
        .iter()
        .map(|(_, value)| serde_json::from_str(value.as_deref().unwrap()).unwrap())
        .collect();
    assert_eq!(rows, serde_json::from_str::<Vec<Value>>(ROWS).unwrap());
    source.drain().await.unwrap();
    drop(source);

    let destination = path.join("restored");
    WorkerRuntime::restore_native(&[backup], None, &destination, "test", "archive-search", execution(), resources())
        .await
        .unwrap();
    let target = WorkerRuntime::open_configured(&destination, "test", "archive-search", execution(), resources(), true)
        .await
        .unwrap();
    // Other columns than the backup's are refused; the backup's are taken.
    let group = map_native_group(&group, target.cluster_id());
    let other = fence(&target, &group, table, 2, vec![dense(3)]).await;
    assert!(!fenced(&other), "{other:?}");
    let none = fence(&target, &group, table, 2, Vec::new()).await;
    assert!(!fenced(&none), "{none:?}");
    assert!(fenced(&fence(&target, &group, table, 2, vec![dense(2)]).await));
    // The rows, with the columns the restore installed, as a backup of the
    // restored range shows them.
    let again = path.join("again");
    target.backup_native(&again, owner(), None).await.unwrap();
    let restored = archived(&target, &again, table);
    assert_eq!(restored.group, group);
    let schema = restored.search.expect("the restored range keeps its columns");
    assert_eq!((schema.epoch, schema.columns), (2, vec![dense(2)]));
    assert_eq!(restored.images, archive.images);
    target.drain().await.unwrap();
}

/// C1 duty (review-C2 (5)): a queued row put of a search range is a `PutRow`.
/// The planner still builds `Change::Put`, which apply refuses there, so no
/// queued write of a search table can land once its range has columns. Today
/// it also waits for the Catalog to publish the fence's epoch (task J), which
/// this test does not model: both must land before it passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "VEC-BUG-queued-put-row"]
async fn vs15_a_queued_row_write_of_a_fenced_search_range_is_applied() {
    let path = std::env::temp_dir().join(format!("nabedra-search-queued-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let (source, table, group) = search_table(&path).await;
    assert!(fenced(&fence(&source, &group, table, 1, vec![dense(2)]).await));
    for (operation, payload) in [
        ("table.insert", json!({"table_id":table,"row":{"id":"c","v":{"f32":"AAAAAAAAgD8="}}})),
        ("table.upsert", json!({"table_id":table,"row":{"id":"a","v":null}})),
        ("table.update", json!({"table_id":table,"key":"b","values":{"v":{"f32":"AACAPwAAgD8="}}})),
        ("table.delete", json!({"table_id":table,"key":"c"})),
    ] {
        let body = serde_json::to_vec(&request(operation, payload)).unwrap();
        let record = source.admit(body).await.unwrap();
        let until = nabedra_rt::time::now() + std::time::Duration::from_secs(30);
        let receipt = loop {
            let receipt = source.observe(record.run_id, record.request_id).await.unwrap();
            if receipt.state.terminal() || nabedra_rt::time::now() >= until {
                break receipt;
            }
            nabedra_rt::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        assert_eq!(receipt.state, RequestState::Succeeded, "{operation}: {:?}", receipt.error);
    }
    source.drain().await.unwrap();
}

/// int0 duty (task J). A queued read of a vector table: its stored rows hold
/// vectors and nulls, which the strict scalar grammar of `Row` refused, so a
/// queued `get` failed with "invalid pinned catalog or routing lineage",
/// before the range's fence as after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vs15_a_queued_get_of_a_vector_table_is_served_before_and_after_its_fence() {
    let path = std::env::temp_dir().join(format!("nabedra-search-get-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let (source, table, group) = search_table(&path).await;
    let get = |key: &str| request("table.get", json!({"table_id":table,"key":key}));
    let row = json!({"id":"a","v":{"f32":"AACAPwAAAEA="}});
    assert_eq!(run(&source, get("a")).await.1.result, Some(row.clone()));
    assert_eq!(run(&source, get("b")).await.1.result, Some(json!({"id":"b","v":null})));
    assert!(fenced(&fence(&source, &group, table, 1, vec![dense(2)]).await));
    assert_eq!(run(&source, get("a")).await.1.result, Some(row));
    source.drain().await.unwrap();
}
