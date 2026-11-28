//! C1 duty (review-C2 (5)): once a range has search columns, every row write
//! of its table names its vectors (`DirectWrite::search`), deletes included.
//! Apply refuses a row write without it there. Control does not send
//! `SearchFence` yet (task J), so the test proposes it as the DDL coordinator
//! will.
use super::*;
use crate::oltp::{
    mvcc::{
        store::{
            row::SearchRow,
            search::{SearchFence, SearchReply},
        },
        vector::{SearchColumn, VectorKind},
    },
    pool::ByteClass,
};
use crate::oltp_replication::{owned::OwnedCommand, types::Timestamp};
use serde_json::json;

struct VectorGate;
impl DirectGate for VectorGate {
    fn direct_active(&self) -> bool {
        true
    }
    fn direct_batch(&self) -> bool {
        true
    }
    fn vector(&self) -> bool {
        true
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "VEC-BUG-direct-search-unset"]
async fn vs15_every_direct_row_write_of_a_fenced_search_range_is_applied() {
    let counter: Arc<dyn DirectTimestampSource> = Arc::new(Counter(AtomicU64::new(1_000)));
    let cluster = Cluster::new(Options {
        batch: true,
        timestamps: Some(counter.clone()),
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let voter = &cluster.voters[leader];
    let fence = OwnedCommand::new_owned(
        ParticipantCommand::SearchFence(SearchFence {
            authority: cluster.authority,
            table_id: cluster.table,
            epoch: 1,
            expected_epoch: Some(0),
            lifecycle: Some(cluster.table),
            columns: vec![SearchColumn {
                id: 1,
                kind: VectorKind::Dense,
                dim: 2,
                source: None,
            }],
            serving: Vec::new(),
            text: Vec::new(),
            names: Vec::new(),
        }),
        voter.handle.resources(),
        ByteClass::Progress,
    )
    .unwrap();
    let reply = voter.handle.propose(fence).await.unwrap().reply;
    assert!(
        matches!(reply, ParticipantReply::Search(SearchReply::Fenced { .. })),
        "{reply:?}"
    );
    let mut table = descriptor(cluster.table);
    table.schema.insert("v".to_string(), ColumnType::Vector(2));
    cluster.schema.insert(DirectTable {
        descriptor: table,
        schema_epoch: 1,
        lifecycle_id: cluster.table,
        indexes: Vec::new(),
    });
    let executor = DirectExecutor::new(
        voter.shared.clone(),
        DirectConfig::default(),
        Arc::new(VectorGate),
        cluster.schema.clone(),
        DirectTimestampClient::new(None, counter),
    );
    let row = |id: i64| json!({"id": id, "value": id, "tag": "t", "score": 0.5, "v": {"f32": "AACAPwAAAEA="}});
    let batch = |n: usize| format!("[{}]", vec!["null"; n].join(","));
    for (operation, payload, expected) in [
        ("table.insert", json!({"row": row(1)}), "null".to_string()),
        ("table.upsert", json!({"row": row(2)}), "null".to_string()),
        ("table.update", json!({"key": 1, "values": {"v": null}}), "true".to_string()),
        ("table.delete", json!({"key": 2}), "true".to_string()),
        ("table.upsert_many", json!({"rows": [row(3), row(4)]}), batch(2)),
        ("table.delete_many", json!({"keys": [4]}), batch(1)),
    ] {
        let id = nabedra_rt::ids::uuid_v4();
        let body = cluster.body(id, operation, payload);
        let answer = executor
            .execute(&voter.handle, cluster.request(leader, &body, id))
            .await;
        let answer = match answer {
            DirectResponse::Done(data) if operation.ends_with("_many") => {
                let replies: Vec<DirectResponse> = serde_json::from_str(data.0.get()).unwrap();
                let flat: Vec<String> = replies
                    .iter()
                    .map(|reply| match reply {
                        DirectResponse::Done(data) => data.0.get().to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect();
                format!("[{}]", flat.join(","))
            }
            DirectResponse::Done(data) => data.0.get().to_owned(),
            other => format!("{other:?}"),
        };
        assert_eq!(answer, expected, "{operation}");
    }
    // Row 3 holds its vector beside the row, row 1 none after its update, and
    // rows 2 and 4 are gone.
    for (id, present, vectors) in [(1, true, 0), (2, false, 0), (3, true, 1), (4, false, 0)] {
        let read = voter
            .handle
            .search_row_local(Timestamp::new(u64::MAX).unwrap(), cluster.row_key(id), 1 << 20)
            .await
            .unwrap();
        match read.payload().unwrap() {
            SearchRow::Row { value, search, .. } => {
                assert_eq!(value.is_some(), present, "row {id}");
                let held = search.as_ref().map_or(0, |search| search.vectors.len());
                assert_eq!(held, vectors, "row {id}");
            }
            other => panic!("row {id}: {other:?}"),
        }
    }
    cluster.shutdown().await;
}
