//! IndexLifecycle `FenceRange` at apply (`specs/vector/lifecycle`): DB-20, the
//! compare-and-set of L19, and the published (`serving`) and full-text
//! (`text`) columns of DB-9.
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
    expected: u64,
    epoch: u64,
    columns: Vec<SearchColumn>,
    serving: Vec<ColumnId>,
    text: Vec<TextColumn>,
) -> ParticipantCommand {
    let ParticipantCommand::SearchFence(mut fence) = s.fence_from(expected, epoch, columns) else {
        unreachable!()
    };
    fence.serving = serving;
    fence.text = text;
    ParticipantCommand::SearchFence(fence)
}
fn probe(s: &S) -> ParticipantCommand {
    let ParticipantCommand::SearchFence(mut probe) = s.fence_from(0, 0, Vec::new()) else { unreachable!() };
    probe.lifecycle = None;
    probe.expected_epoch = None;
    ParticipantCommand::SearchFence(probe)
}
async fn schema(s: &mut S) -> Option<SearchSchema> {
    match search_reply(s.f.submit(probe(s)).await) {
        SearchReply::Schema(probe) => probe.schema,
        other => panic!("{other:?}"),
    }
}
/// The published subset that fence `epoch` of the generated runs installs,
/// so the installed schema names the epoch that installed it.
fn serving_of(epoch: u64) -> Vec<ColumnId> {
    columns()
        .iter()
        .map(|column| column.id)
        .filter(|id| (epoch >> (id - 1)) & 1 == 1)
        .collect()
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
    let command = fence(&s, 0, 3, columns(), vec![PLAIN, SPARSE], vec![text(6, "body", false)]);
    assert!(matches!(search_reply(s.f.submit(command).await), SearchReply::Fenced { epoch: 3, .. }));
    let got = schema(&mut s).await.unwrap();
    assert_eq!((got.epoch, got.serving.clone(), got.high), (3, vec![PLAIN, SPARSE], 6));
    assert_eq!(got.text, vec![text(6, "body", false)]);
    // Publishing the text column and the other vectors changes only `serving`.
    let command = fence(&s, 3, 4, columns(), vec![PLAIN, EMBEDDED, SPARSE, MULTI], vec![text(6, "body", true)]);
    assert!(matches!(search_reply(s.f.submit(command).await), SearchReply::Fenced { epoch: 4, .. }));
    let got = schema(&mut s).await.unwrap();
    assert_eq!(got.serving, vec![PLAIN, EMBEDDED, SPARSE, MULTI]);
    assert!(got.text[0].serving);
}

/// Malformed fences change nothing: a published column that is not a
/// column, ids out of order, a text id shared with a vector column, an
/// unknown analyzer, more than eight columns of both kinds, an epoch not
/// above the one it expects, a probe that names columns.
#[tokio::test]
async fn vs14_a_malformed_fence_is_refused() {
    let mut s = S::new().await;
    let many: Vec<TextColumn> = (5..10).map(|id| text(id, "body", false)).collect();
    let mut unknown = text(6, "body", false);
    unknown.language = Text::try_from("klingon").unwrap();
    let (at, next) = (s.search_epoch, s.search_epoch + 1);
    let mut commands = Vec::new();
    for (name, serving, texts) in [
        ("serving outside columns", vec![9], Vec::new()),
        ("serving unsorted", vec![SPARSE, PLAIN], Vec::new()),
        ("text shares a vector id", Vec::new(), vec![text(PLAIN, "body", false)]),
        ("text id zero", Vec::new(), vec![text(0, "body", false)]),
        ("text unsorted", Vec::new(), vec![text(7, "a", false), text(6, "b", false)]),
        ("unknown analyzer", Vec::new(), vec![unknown.clone()]),
        ("nine columns", Vec::new(), many.clone()),
    ] {
        commands.push((name, fence(&s, at, next, columns(), serving, texts)));
    }
    commands.push(("epoch at its expected one", fence(&s, at, at, columns(), Vec::new(), Vec::new())));
    commands.push(("epoch below its expected one", fence(&s, next, at, columns(), Vec::new(), Vec::new())));
    let ParticipantCommand::SearchFence(mut named) = probe(&s) else { unreachable!() };
    named.columns = columns();
    commands.push(("a probe with columns", ParticipantCommand::SearchFence(named)));
    for (name, command) in commands {
        let before = s.f.dump().await;
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
    let at = |s: &S, columns, texts| fence(s, s.search_epoch, s.search_epoch + 1, columns, Vec::new(), texts);
    let command = at(&s, columns(), vec![text(6, "body", false)]);
    assert!(matches!(search_reply(s.f.submit(command).await), SearchReply::Fenced { .. }));
    s.search_epoch += 1;
    let mut other = text(6, "body", false);
    other.language = Text::try_from("french").unwrap();
    for (name, columns, texts) in [
        ("another analyzer", columns(), vec![other]),
        ("another column", columns(), vec![text(6, "title", false)]),
    ] {
        let command = at(&s, columns, texts);
        assert_eq!(search_reply(s.f.submit(command).await), refused(Fault::InvalidBinding), "{name}");
    }
    // Dropped.
    let command = at(&s, columns(), Vec::new());
    assert!(matches!(search_reply(s.f.submit(command).await), SearchReply::Fenced { .. }));
    s.search_epoch += 1;
    let mut vector = columns();
    vector.push(column(6, VectorKind::Dense, 2));
    for (name, columns, texts) in [
        ("as text", columns(), vec![text(6, "body", false)]),
        ("as a vector", vector, Vec::new()),
    ] {
        let command = at(&s, columns, texts);
        assert_eq!(search_reply(s.f.submit(command).await), refused(Fault::InvalidBinding), "{name}");
    }
    // A re-add is a new id.
    let command = at(&s, columns(), vec![text(7, "body", false)]);
    assert!(matches!(search_reply(s.f.submit(command).await), SearchReply::Fenced { .. }));
}

/// L19, red for the CAS mutant (apply takes any larger epoch): two DDL
/// coordinators probed the group at the same epoch and picked the same next
/// epoch with other columns. The first fence installs; the second expects
/// an epoch the group left, so it is refused and changes nothing, whether it
/// names the same epoch or a larger one. Its sender re-probes.
#[tokio::test]
async fn vs14_l19_a_fence_applies_only_at_its_expected_epoch() {
    let mut s = S::new().await;
    let at = s.search_epoch;
    let first = fence(&s, at, at + 1, columns(), vec![PLAIN], Vec::new());
    assert!(matches!(search_reply(s.f.submit(first).await), SearchReply::Fenced { epoch, .. } if epoch == at + 1));
    for (name, epoch) in [("same epoch", at + 1), ("larger epoch", at + 5)] {
        let before = s.f.dump().await;
        let second = fence(&s, at, epoch, columns(), vec![SPARSE], Vec::new());
        assert_eq!(search_reply(s.f.submit(second).await), refused(Fault::Conflict), "{name}");
        assert_eq!(s.f.dump().await, before, "{name}");
    }
    let got = schema(&mut s).await.unwrap();
    assert_eq!((got.epoch, got.serving), (at + 1, vec![PLAIN]));
    // After a re-probe the same sender fences above it.
    let third = fence(&s, at + 1, at + 2, columns(), vec![SPARSE], Vec::new());
    assert!(matches!(search_reply(s.f.submit(third).await), SearchReply::Fenced { epoch, .. } if epoch == at + 2));
}

/// L19: repeating the installed fence (a lost reply, a retried proposal) is
/// a no-op that answers like the first apply; the same epoch with another
/// lifecycle or other columns is a conflict.
#[tokio::test]
async fn vs14_l19_a_repeated_fence_is_a_no_op_retry() {
    let mut s = S::new().await;
    let at = s.search_epoch;
    let command = fence(&s, at, at + 1, columns(), vec![PLAIN], Vec::new());
    let first = search_reply(s.f.submit(command.clone()).await);
    assert!(matches!(first, SearchReply::Fenced { epoch, direct: Some(_) } if epoch == at + 1), "{first:?}");
    let before = s.f.dump().await;
    assert_eq!(search_reply(s.f.submit(command.clone()).await), first);
    assert_eq!(s.f.dump().await, before, "a repeat writes nothing");
    let ParticipantCommand::SearchFence(mut lifecycle) = command else { unreachable!() };
    let mut columns = lifecycle.clone();
    lifecycle.lifecycle = Some(nabedra_rt::ids::uuid_v4());
    columns.serving = vec![SPARSE];
    for (name, other) in [("another lifecycle", lifecycle), ("other columns", columns)] {
        assert_eq!(
            search_reply(s.f.submit(ParticipantCommand::SearchFence(other)).await),
            refused(Fault::Conflict),
            "{name}"
        );
        assert_eq!(s.f.dump().await, before, "{name}");
    }
}

/// DB-20 with L19, red for mutants `prefix_fence_not_monotone` and the CAS
/// mutant: fences of random expected and new epochs arrive late and
/// repeated. A group takes a fence only if it expects the installed epoch
/// (or repeats the installed fence), so its epoch never decreases and it
/// always holds the schema of the fence that installed its epoch.
#[tokio::test]
async fn vs14_a_group_takes_a_fence_only_at_its_expected_epoch() {
    for seed in 0..6u64 {
        let mut s = S::plain(Some(&FaultFs::new())).await;
        let mut rng = Rng(seed);
        let mut installed: u64 = 0;
        let mut sent = Vec::new();
        for _ in 0..16 {
            // A new fence of a coordinator that probed recently, or a late
            // or repeated one.
            let command = match (rng.below(3), sent.is_empty()) {
                (0, false) => sent[rng.below(sent.len())],
                _ => {
                    let expected = installed.saturating_sub(rng.below(2) as u64);
                    (expected, expected + 1 + rng.below(3) as u64)
                }
            };
            sent.push(command);
            let (expected, epoch) = command;
            let reply = search_reply(
                s.f.submit(fence(&s, expected, epoch, columns(), serving_of(epoch), Vec::new()))
                    .await,
            );
            let takes = expected == installed;
            let repeat = epoch == installed;
            match reply {
                SearchReply::Fenced { epoch: e, .. } => {
                    assert!(takes || repeat, "seed {seed}: {command:?} at {installed}");
                    assert_eq!(e, epoch, "seed {seed}");
                    installed = epoch;
                }
                SearchReply::Rejected(Fault::Conflict) => {
                    assert!(!takes && !repeat, "seed {seed}: {command:?} at {installed}")
                }
                other => panic!("seed {seed}: {other:?}"),
            }
            let got = schema(&mut s).await;
            let got = got.map_or((0, Vec::new()), |got| (got.epoch, got.serving));
            let want = if installed == 0 { (0, Vec::new()) } else { (installed, serving_of(installed)) };
            assert_eq!(got, want, "seed {seed}");
        }
    }
}
