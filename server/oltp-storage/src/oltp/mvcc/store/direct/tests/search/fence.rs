//! IndexLifecycle `FenceRange` at apply (`specs/vector/lifecycle`): DB-20 and
//! the published (`serving`) and full-text (`text`) columns of DB-9.
use super::*;
use crate::oltp::mvcc::store::search::{SearchSchema, TextColumn};

fn text(id: ColumnId, column: &str, serving: bool) -> TextColumn {
    TextColumn {
        id,
        column: Text::try_from(column).unwrap(),
        language: Text::try_from("english").unwrap(),
        serving,
    }
}
fn fence(
    s: &S,
    epoch: u64,
    columns: Vec<SearchColumn>,
    serving: Vec<ColumnId>,
    text: Vec<TextColumn>,
) -> ParticipantCommand {
    let ParticipantCommand::SearchFence(mut fence) = s.fence_command(epoch, columns) else {
        unreachable!()
    };
    fence.serving = serving;
    fence.text = text;
    ParticipantCommand::SearchFence(fence)
}
async fn schema(s: &mut S) -> Option<SearchSchema> {
    let probe = s.fence_command(0, Vec::new());
    let ParticipantCommand::SearchFence(mut probe) = probe else { unreachable!() };
    probe.lifecycle = None;
    match search_reply(s.f.submit(ParticipantCommand::SearchFence(probe)).await) {
        SearchReply::Schema(schema) => schema.map(|schema| *schema),
        other => panic!("{other:?}"),
    }
}

/// DB-9 data: a fence installs the published vector columns and the
/// full-text columns with their published flag, and the probe reads them
/// back in log order without writing anything.
#[tokio::test]
async fn vs14_a_fence_installs_serving_and_text_columns() {
    let mut s = S::plain(Some(&FaultFs::new())).await;
    assert_eq!(schema(&mut s).await, None);
    let before = s.f.dump().await;
    assert_eq!(schema(&mut s).await, None);
    assert_eq!(s.f.dump().await, before, "a probe writes nothing");
    let command = fence(&s, 3, columns(), vec![PLAIN, SPARSE], vec![text(6, "body", false)]);
    assert!(matches!(search_reply(s.f.submit(command).await), SearchReply::Fenced { epoch: 3, .. }));
    let got = schema(&mut s).await.unwrap();
    assert_eq!((got.epoch, got.serving.clone(), got.high), (3, vec![PLAIN, SPARSE], 6));
    assert_eq!(got.text, vec![text(6, "body", false)]);
    // Publishing the text column and the other vectors changes only `serving`.
    let command = fence(&s, 4, columns(), vec![PLAIN, EMBEDDED, SPARSE, MULTI], vec![text(6, "body", true)]);
    assert!(matches!(search_reply(s.f.submit(command).await), SearchReply::Fenced { epoch: 4, .. }));
    let got = schema(&mut s).await.unwrap();
    assert_eq!(got.serving, vec![PLAIN, EMBEDDED, SPARSE, MULTI]);
    assert!(got.text[0].serving);
}

/// Malformed fences change nothing: a published column that is not a
/// column, ids out of order, a text id shared with a vector column, an
/// unknown analyzer, more than eight columns of both kinds.
#[tokio::test]
async fn vs14_a_malformed_fence_is_refused() {
    let mut s = S::new().await;
    let many: Vec<TextColumn> = (5..10).map(|id| text(id, "body", false)).collect();
    let mut unknown = text(6, "body", false);
    unknown.language = Text::try_from("klingon").unwrap();
    for (name, serving, texts) in [
        ("serving outside columns", vec![9], Vec::new()),
        ("serving unsorted", vec![SPARSE, PLAIN], Vec::new()),
        ("text shares a vector id", Vec::new(), vec![text(PLAIN, "body", false)]),
        ("text id zero", Vec::new(), vec![text(0, "body", false)]),
        ("text unsorted", Vec::new(), vec![text(7, "a", false), text(6, "b", false)]),
        ("unknown analyzer", Vec::new(), vec![unknown.clone()]),
        ("nine columns", Vec::new(), many.clone()),
    ] {
        let before = s.f.dump().await;
        let command = fence(&s, s.search_epoch + 1, columns(), serving, texts);
        assert_eq!(search_reply(s.f.submit(command).await), refused(Fault::InvalidBinding), "{name}");
        assert_eq!(s.f.dump().await, before, "{name}");
    }
    assert!(matches!(s.fence(columns()).await, SearchReply::Fenced { .. }));
}

/// A full-text id names one incarnation like a vector id: its column and
/// analyzer never change, and a dropped id never returns, as text or as a
/// vector column.
#[tokio::test]
async fn vs14_a_dropped_or_changed_text_column_id_is_never_reused() {
    let mut s = S::new().await;
    let at = |s: &S| s.search_epoch + 1;
    let command = fence(&s, at(&s), columns(), Vec::new(), vec![text(6, "body", false)]);
    assert!(matches!(search_reply(s.f.submit(command).await), SearchReply::Fenced { .. }));
    s.search_epoch += 1;
    let mut other = text(6, "body", false);
    other.language = Text::try_from("french").unwrap();
    for (name, columns, texts) in [
        ("another analyzer", columns(), vec![other]),
        ("another column", columns(), vec![text(6, "title", false)]),
    ] {
        let command = fence(&s, at(&s), columns, Vec::new(), texts);
        assert_eq!(search_reply(s.f.submit(command).await), refused(Fault::InvalidBinding), "{name}");
    }
    // Dropped.
    let command = fence(&s, at(&s), columns(), Vec::new(), Vec::new());
    assert!(matches!(search_reply(s.f.submit(command).await), SearchReply::Fenced { .. }));
    s.search_epoch += 1;
    let mut vector = columns();
    vector.push(column(6, VectorKind::Dense, 2));
    for (name, columns, texts) in [
        ("as text", columns(), vec![text(6, "body", false)]),
        ("as a vector", vector, Vec::new()),
    ] {
        let command = fence(&s, at(&s), columns, Vec::new(), texts);
        assert_eq!(search_reply(s.f.submit(command).await), refused(Fault::InvalidBinding), "{name}");
    }
    // A re-add is a new id.
    let command = fence(&s, at(&s), columns(), Vec::new(), vec![text(7, "body", false)]);
    assert!(matches!(search_reply(s.f.submit(command).await), SearchReply::Fenced { .. }));
}

/// DB-20, red for mutant `prefix_fence_not_monotone`: fences of random epochs
/// arrive late and repeated; the group takes a fence only if its epoch is
/// larger, so it always holds the schema of the largest epoch it saw.
#[tokio::test]
async fn vs14_a_group_takes_only_a_larger_search_epoch() {
    for seed in 0..6u64 {
        let mut s = S::plain(Some(&FaultFs::new())).await;
        let mut rng = Rng(seed);
        // Fence `e` publishes a different subset of the columns, so the
        // installed schema names the epoch that installed it.
        let sent: Vec<u64> = (0..12).map(|_| 1 + rng.below(6) as u64).collect();
        let mut largest: u64 = 0;
        for epoch in sent {
            let serving: Vec<ColumnId> = columns()
                .iter()
                .map(|column| column.id)
                .filter(|id| (epoch >> (id - 1)) & 1 == 1)
                .collect();
            let reply = search_reply(s.f.submit(fence(&s, epoch, columns(), serving, Vec::new())).await);
            largest = largest.max(epoch);
            assert!(
                matches!(reply, SearchReply::Fenced { epoch: e, .. } if e == largest),
                "seed {seed}: {reply:?}"
            );
            let got = schema(&mut s).await.unwrap();
            let expected: Vec<ColumnId> = columns()
                .iter()
                .map(|column| column.id)
                .filter(|id| (largest >> (id - 1)) & 1 == 1)
                .collect();
            assert_eq!((got.epoch, got.serving), (largest, expected), "seed {seed}");
        }
    }
}
