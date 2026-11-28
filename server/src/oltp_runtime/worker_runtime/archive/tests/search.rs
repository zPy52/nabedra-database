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
            search::{SearchFence, SearchReply, SearchSchema, VectorName},
        },
        vector::{SearchColumn, VectorKind},
    },
    pool::ByteClass,
};
use crate::oltp_replication::authority::control::VECTOR_FEATURE;

mod repair;

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
    // L19: from the epoch the range holds now.
    let expected = handle.search_probe().await.unwrap().map_or(0, |schema| schema.epoch);
    // L24: a fence names its vector columns as the planner does, so the next
    // DDL can roll it forward. Every table here calls its vector `v`.
    let names = columns
        .iter()
        .map(|column| VectorName { id: column.id, name: "v".try_into().unwrap(), model: None })
        .collect();
    let command = OwnedCommand::new_owned(
        ParticipantCommand::SearchFence(SearchFence {
            authority,
            table_id: table,
            epoch,
            expected_epoch: Some(expected),
            // A created table's first lifecycle id is its table id.
            lifecycle: Some(table),
            columns,
            serving: Vec::new(),
            text: Vec::new(),
            names,
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
#[ignore = "VEC-BUG-queued-put-row: table.create now fences its range (J), so the seed rows need X PutRow"]
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
#[ignore = "VEC-BUG-queued-put-row: table.create now fences its range (J), so the seed rows need X PutRow"]
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

// ---- Search DDL on a lone runtime (task J, contract-w2 2.6, IndexLifecycle) ----

/// A lone runtime with `data.vector.v1` on and a table `docs` created with a
/// dense column `v`, and the table's Data group (from a backup).
async fn ddl_table(path: &Path) -> (Arc<WorkerRuntime>, Uuid, GroupIdentity) {
    ddl_table_with_schema(path, json!({"id":"utf8","body":"utf8","v":{"type":"vector","dim":2}})).await
}
async fn ddl_table_with_schema(path: &Path, columns: Value) -> (Arc<WorkerRuntime>, Uuid, GroupIdentity) {
    let runtime = WorkerRuntime::open(&path.join("ddl"), "test", "search-ddl", execution(), resources())
        .await
        .unwrap();
    runtime.without_direct_for_test().await;
    runtime.services.manager.cluster_features().enable(VECTOR_FEATURE);
    let (_, created) = run(
        &runtime,
        request(
            "table.create",
            json!({"name":"docs","schema":columns,
                "key":"id","format":"parquet"}),
        ),
    )
    .await;
    let table: Uuid = created.result.as_ref().unwrap()["id"].as_str().unwrap().parse().unwrap();
    let backup = path.join("created");
    runtime.backup_native(&backup, owner(), None).await.unwrap();
    let group = archived(&runtime, &backup, table).group;
    (runtime, table, group)
}
/// The group's search schema in log order.
async fn schema(runtime: &WorkerRuntime, group: &GroupIdentity) -> SearchSchema {
    let handle = runtime.services.resolver.participant(group).await.unwrap();
    handle.search_probe().await.unwrap().expect("a search range")
}
/// Epoch, column ids, published ids, and full-text ids with their flag.
type Ids = (u64, Vec<u16>, Vec<u16>, Vec<(u16, bool)>);
fn ids(schema: &SearchSchema) -> Ids {
    (
        schema.epoch,
        schema.columns.iter().map(|column| column.id).collect(),
        schema.serving.clone(),
        schema.text.iter().map(|text| (text.id, text.serving)).collect(),
    )
}
async fn status(runtime: &Arc<WorkerRuntime>, table: Uuid) -> Value {
    let (_, reply) = run(runtime, request("table.search_status", json!({"table_id":table}))).await;
    reply.result.unwrap()["columns"].clone()
}

/// IndexLifecycle `AddColumn`, `StartDrop`/`FinishDrop` and a re-add on a
/// real lone runtime: `table.create` fences its range at epoch 1 with every
/// column published; each column operation fences the range at a larger
/// epoch before the Catalog commit, a new column is writable and not served
/// (`building` in `table.search_status`), a dropped one leaves the range, and
/// a re-added name is a new id that never names the old incarnation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vs14_column_operations_fence_the_range_before_the_catalog_names_them() {
    let path = std::env::temp_dir().join(format!("nabedra-search-ddl-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let (runtime, table, group) = ddl_table(&path).await;
    assert_eq!(ids(&schema(&runtime, &group).await), (1, vec![1], vec![1], vec![]));
    assert_eq!(status(&runtime, table).await["v"]["state"], "ready");

    let add = |name: &str| request("table.add_column", json!({"table_id":table,"name":name,"type":{"type":"vector","dim":3}}));
    run(&runtime, add("w")).await;
    assert_eq!(ids(&schema(&runtime, &group).await), (2, vec![1, 2], vec![1], vec![]));
    let columns = status(&runtime, table).await;
    assert_eq!((columns["v"]["state"].clone(), columns["w"]["state"].clone()), (json!("ready"), json!("building")));

    run(&runtime, request("table.add_full_text", json!({"table_id":table,"column":"body","language":"english"}))).await;
    assert_eq!(ids(&schema(&runtime, &group).await), (3, vec![1, 2], vec![1], vec![(3, false)]));

    run(&runtime, request("table.drop_column", json!({"table_id":table,"name":"v"}))).await;
    assert_eq!(ids(&schema(&runtime, &group).await), (4, vec![2], vec![], vec![(3, false)]));
    assert!(status(&runtime, table).await.get("v").is_none());
    let (_, described) = run(&runtime, request("table.describe", json!({"name":"docs"}))).await;
    assert!(described.result.unwrap()["schema"].get("v").is_none(), "the Catalog dropped the name");

    // A re-add is a new incarnation.
    run(&runtime, add("v")).await;
    let again = schema(&runtime, &group).await;
    assert_eq!(ids(&again), (5, vec![2, 4], vec![], vec![(3, false)]));
    assert_eq!(again.high, 4);

    run(&runtime, request("table.drop_full_text", json!({"table_id":table,"column":"body"}))).await;
    assert_eq!(ids(&schema(&runtime, &group).await), (6, vec![2, 4], vec![], vec![]));
    runtime.drain().await.unwrap();
}

/// IndexLifecycle `StartDrop`/`FinishDrop` for the table, red for mutant
/// `drop_without_fence`: `table.drop` of a search table fences its range
/// with no columns before the Catalog publishes the drop, so no worker can
/// pass a barrier for a column of the dropped table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vs14_a_table_drop_fences_its_ranges_first() {
    let path = std::env::temp_dir().join(format!("nabedra-search-drop-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let (runtime, table, group) = ddl_table(&path).await;
    run(&runtime, request("table.drop", json!({"table_id":table}))).await;
    let dropped = schema(&runtime, &group).await;
    assert_eq!(ids(&dropped), (2, vec![], vec![], vec![]));
    assert_eq!(dropped.high, 1, "ids stay used");
    runtime.drain().await.unwrap();
}

/// An unrelated DDL rolls a fenced drop forward before allocating its own column.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rvj_an_interrupted_drop_column_is_adopted_by_the_next_ddl() {
    let path = std::env::temp_dir().join(format!("nabedra-search-adopt-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let (runtime, table, group) = ddl_table(&path).await;
    assert!(fenced(&fence(&runtime, &group, table, 2, Vec::new()).await));
    assert_eq!(status(&runtime, table).await["v"]["state"], "ready");

    run(&runtime, request("table.add_column", json!({
        "table_id":table,"name":"w","type":{"type":"vector","dim":3}
    }))).await;
    let columns = status(&runtime, table).await;
    assert!(columns.get("v").is_none());
    assert_eq!(columns["w"]["state"], "building");
    assert_eq!(ids(&schema(&runtime, &group).await), (3, vec![2], vec![], vec![]));
    runtime.drain().await.unwrap();
}

/// A plain Catalog record cannot hide an interrupted first search fence from index or drop DDL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vs14_an_interrupted_first_column_is_adopted_by_index_and_drop_ddl() {
    use crate::oltp::mvcc::index::IndexDefinition;
    use crate::oltp_replication::types::{Page, Text};

    for index in [false, true] {
        let path = std::env::temp_dir().join(format!("nabedra-first-fence-{}", nabedra_rt::ids::uuid_v4()));
        std::fs::create_dir(&path).unwrap();
        let _cleanup = Cleanup(path.clone());
        let (runtime, table, group) = ddl_table_with_schema(&path, json!({"id":"utf8","body":"utf8"})).await;
        assert!(fenced(&fence(&runtime, &group, table, 1, vec![dense(2)]).await));
        assert_eq!(status(&runtime, table).await, json!({}));
        if index {
            runtime.create_index(
                IndexDefinition {
                    index_id: nabedra_rt::ids::uuid_v4(),
                    table_id: table,
                    name: Text::try_from("by_body").unwrap(),
                    columns: Page::new(vec![Text::try_from("body").unwrap()]).unwrap(),
                    covering: Page::new(Vec::new()).unwrap(),
                },
                nabedra_rt::ids::uuid_v4(),
                1,
            ).await.unwrap();
            assert_eq!(status(&runtime, table).await["v"]["state"], "building");
            assert_eq!(ids(&schema(&runtime, &group).await), (3, vec![1], vec![], vec![]));
        } else {
            run(&runtime, request("table.drop", json!({"table_id":table}))).await;
            assert_eq!(ids(&schema(&runtime, &group).await), (2, vec![], vec![], vec![]));
        }
        runtime.drain().await.unwrap();
    }
}
