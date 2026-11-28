//! SearchAssign's Control steps and IndexLifecycle's `seen` as pure functions
//! (`specs/vector/assign`, `specs/vector/lifecycle`).
use super::*;
use crate::oltp_replication::search_rpc::RangeReport;

fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn member(n: u128, zone: &str, role: SearchRole) -> SearchMember {
    SearchMember {
        database: Text::try_from("default").unwrap(),
        role,
        enrollment: WorkerEnrollment {
            worker_id: id(n),
            incarnation: id(n + 1000),
            certificate_sha256: Digest::from_bytes([n as u8; 32]),
            endpoint: Text::try_from("127.0.0.1:1").unwrap(),
            server_name: Text::try_from("localhost").unwrap(),
            enrollment_revision: 1,
        },
        zone: Text::try_from(zone).unwrap(),
        budget_bytes: 1 << 30,
    }
}
fn report(worker: u128, table: Uuid, epoch: u64, columns: Vec<(ColumnId, ColumnState)>) -> SearchReport {
    SearchReport {
        worker_id: id(worker),
        process: id(worker + 500),
        table_id: table,
        assignment_epoch: epoch,
        shard: 0,
        report_id: 1,
        page: 0,
        pages: 1,
        ranges: vec![RangeReport {
            group_id: id(77),
            routing_epoch: 1,
            applied: AppliedPosition { term: 1, node_id: 1, index: 9 },
            lag_ms: 5,
            columns,
        }],
        queue: 0,
        memory_used: 0,
    }
}
fn mv(add: u128, remove: Option<u128>, epoch: u64) -> SearchMove {
    SearchMove { shard: 0, add: id(add), remove: remove.map(id), epoch }
}

/// SearchAssign `StartMove`, `Publish` and the drop: one move at a time at
/// epoch + 1; the published replica serves from the move's epoch; the
/// replaced one drains and its drop names the epoch it ends; drained
/// replicas leave after the grace.
#[test]
fn vs06_a_move_publishes_at_the_next_epoch_and_the_drop_names_the_ended_epoch() {
    let table = id(1);
    let first = begin_move(None, table, 0, mv(10, None, 1)).unwrap();
    assert_eq!(begin_move(Some(first.clone()), table, 0, mv(11, None, 1)), Err(refused("invalid search move")));
    let one = publish(Some(first), 0, 1_000).unwrap();
    assert_eq!((one.epoch, one.shards[0].replicas.len()), (1, 1));
    assert_eq!(one.shards[0].replicas[0].since_epoch, 1);
    // Stale expectations conflict and name the current epoch.
    assert_eq!(begin_move(Some(one.clone()), table, 0, mv(11, None, 1)), Err(SearchControlReply::Conflict { epoch: 1 }));
    assert_eq!(publish(Some(one.clone()), 1, 1_000), Err(SearchControlReply::Conflict { epoch: 1 }));
    for (name, bad) in [
        ("an epoch other than epoch + 1", mv(11, None, 3)),
        ("a worker already in the shard", mv(10, None, 2)),
        ("removing a worker not in the shard", mv(11, Some(12), 2)),
        ("adding the removed worker", mv(10, Some(10), 2)),
    ] {
        assert_eq!(begin_move(Some(one.clone()), table, 1, bad), Err(refused("invalid search move")), "{name}");
    }
    let replace = begin_move(Some(one), table, 1, mv(11, Some(10), 2)).unwrap();
    let two = publish(Some(replace), 1, 2_000).unwrap();
    let old = two.shards[0].replicas.iter().find(|r| r.worker_id == id(10)).unwrap();
    assert_eq!(old.draining, Some(Draining { ended_epoch: 1, until_unix_ms: 2_000 + DRAIN_GRACE_MS }));
    // A later publish after the grace drops the drained replica.
    let three = begin_move(Some(two.clone()), table, 2, mv(12, None, 3)).unwrap();
    let kept = publish(Some(three.clone()), 2, 1_999 + DRAIN_GRACE_MS).unwrap();
    assert!(kept.shards[0].replicas.iter().any(|r| r.worker_id == id(10)), "still draining before the deadline");
    let gone = publish(Some(three), 2, 2_000 + DRAIN_GRACE_MS).unwrap();
    assert!(!gone.shards[0].replicas.iter().any(|r| r.worker_id == id(10)));
    // Cancel needs a pending move at the expected epoch.
    assert_eq!(cancel(Some(two.clone()), 2), Err(SearchControlReply::Conflict { epoch: 2 }));
    let pending = begin_move(Some(two), table, 2, mv(12, None, 3)).unwrap();
    assert_eq!(cancel(Some(pending), 2).unwrap().next, None);
}

/// SearchAssign `RecvReady`/`Publish`, red for mutant
/// `publish_before_caught_up`: the reconciler publishes a move only after
/// the added worker reported, for the move's epoch, every range ready. A
/// report of another worker, epoch or table, a syncing or failed column, or
/// a report without ranges does not count.
#[test]
fn vs06_a_move_publishes_only_after_the_new_replica_reported_caught_up() {
    let table = id(1);
    let members = [member(10, "a", SearchRole::Worker), member(11, "b", SearchRole::Worker)];
    let pending = begin_move(None, table, 0, mv(10, None, 1)).unwrap();
    let ready = vec![(1, ColumnState::Ready), (2, ColumnState::Ready)];
    let mut empty = report(10, table, 1, ready.clone());
    empty.ranges.clear();
    for (name, reports) in [
        ("no report", vec![]),
        ("another worker", vec![report(11, table, 1, ready.clone())]),
        ("another epoch", vec![report(10, table, 0, ready.clone())]),
        ("another shard", vec![SearchReport { shard: 1, ..report(10, table, 1, ready.clone()) }]),
        ("another table", vec![report(10, id(2), 1, ready.clone())]),
        ("a syncing column", vec![report(10, table, 1, vec![(1, ColumnState::Ready), (2, ColumnState::Syncing)])]),
        ("a failed column", vec![report(10, table, 1, vec![(1, ColumnState::Failed(Text::try_from("x").unwrap()))])]),
        ("no ranges", vec![empty]),
    ] {
        assert!(!move_ready(&pending, &reports), "{name}");
        assert_eq!(reconcile(&pending, &members, &reports), None, "{name}");
    }
    let mut first = report(10, table, 1, ready.clone());
    first.pages = 2;
    first.ranges = (0..MAX_REPORT_RANGES as u128)
        .map(|n| RangeReport { group_id: id(100 + n), ..first.ranges[0].clone() })
        .collect();
    let mut last = report(10, table, 1, ready);
    last.page = 1;
    last.pages = 2;
    let control = nabedra_rt::ids::uuid_v4();
    // A retry of the last page cannot stand in for the other page.
    for page in [&last, &last] {
        record_report(control, 1, page.clone()).unwrap();
        assert_eq!(reconcile(&pending, &members, &reports(control, 1, table).unwrap()), None);
    }
    record_report(control, 1, first).unwrap();
    let reports = reports(control, 1, table).unwrap();
    assert!(move_ready(&pending, &reports));
    assert_eq!(
        reconcile(&pending, &members, &reports),
        Some(SearchCommand::Publish { table_id: table, expected_epoch: 0 })
    );
}

/// The reconciler gives a shard `REPLICAS` search workers, the second in
/// another zone when one exists, never an embedder, and replaces a replica
/// whose worker left; with nothing to do it proposes nothing.
#[test]
fn vs06_the_reconciler_fills_shards_across_zones_and_replaces_lost_workers() {
    let table = id(1);
    let members = vec![
        member(10, "a", SearchRole::Worker),
        member(11, "a", SearchRole::Worker),
        member(12, "b", SearchRole::Worker),
        member(13, "c", SearchRole::Embedder),
    ];
    let mut assignment = initial_assignment(table);
    let mut epoch = 0;
    let mut added = Vec::new();
    while let Some(command) = reconcile(&assignment, &members, &[]) {
        let SearchCommand::BeginMove { expected_epoch, mv, .. } = command else { panic!("{command:?}") };
        assert_eq!((expected_epoch, mv.epoch), (epoch, epoch + 1));
        added.push(mv.add);
        assignment = publish(Some(begin_move(Some(assignment), table, epoch, mv).unwrap()), epoch, 0).unwrap();
        epoch += 1;
    }
    assert_eq!(added.len(), REPLICAS);
    assert_eq!(added[0], id(10));
    assert_eq!(added[1], id(12), "another zone first");
    // Worker 12 leaves: it is replaced, and the move removes it.
    let left: Vec<SearchMember> = members.iter().filter(|m| m.enrollment.worker_id != id(12)).cloned().collect();
    let Some(SearchCommand::BeginMove { mv, .. }) = reconcile(&assignment, &left, &[]) else { panic!() };
    assert_eq!((mv.add, mv.remove), (id(11), Some(id(12))));
    // Only embedders: nothing to add.
    assert_eq!(reconcile(&initial_assignment(table), &members[3..], &[]), None);
}

/// IndexLifecycle `seen` and `Failover`: reports live in the memory of one
/// leader term; a new term starts empty and a repeated report counts again
/// (`ReportAgain`). A report of the same process merges its ranges; one of
/// a new process replaces the worker's earlier one; malformed reports are
/// refused.
#[test]
fn vs14_reports_are_leader_memory_and_a_failover_loses_them() {
    let control = nabedra_rt::ids::uuid_v4();
    let table = id(1);
    let mut first = report(10, table, 1, vec![(1, ColumnState::Ready)]);
    first.pages = 2;
    record_report(control, 1, first.clone()).unwrap();
    let mut other_range = first.clone();
    other_range.page = 1;
    other_range.ranges[0].group_id = id(78);
    record_report(control, 1, other_range).unwrap();
    let held = reports(control, 1, table).unwrap();
    assert_eq!(held.len(), 2, "both pages of the snapshot are held");
    let status = range_status(&held);
    assert_eq!(status.iter().map(|s| (s.group_id, s.ready.clone())).collect::<Vec<_>>(), [(id(77), vec![1]), (id(78), vec![1])]);
    // Failover.
    assert!(reports(control, 2, table).unwrap().is_empty(), "a new leader term holds nothing");
    record_report(control, 2, first.clone()).unwrap();
    assert_eq!(reports(control, 2, table).unwrap().len(), 1, "the repeated report counts");
    // A new process of the worker replaces its report.
    let mut restarted = first.clone();
    restarted.process = id(9);
    restarted.ranges[0].columns = vec![(1, ColumnState::Syncing)];
    record_report(control, 2, restarted).unwrap();
    assert!(range_status(&reports(control, 2, table).unwrap())[0].ready.is_empty());
    for (name, bad) in [
        ("nil worker", SearchReport { worker_id: Uuid::nil(), ..first.clone() }),
        ("zero page count", SearchReport { pages: 0, ..first.clone() }),
        ("page index out of range", SearchReport { page: first.pages, ..first.clone() }),
        ("empty page", SearchReport { ranges: vec![], ..first.clone() }),
        ("too many ranges", SearchReport { ranges: vec![first.ranges[0].clone(); MAX_REPORT_RANGES + 1], ..first.clone() }),
        ("unsorted columns", report(10, table, 1, vec![(2, ColumnState::Ready), (1, ColumnState::Ready)])),
    ] {
        assert!(record_report(control, 2, bad).is_err(), "{name}");
    }
}

/// `report_without_column` at Control: a range's status lists only the
/// incarnations a replica reported ready, by id, never a syncing one, and
/// its lag is the smallest of a replica that reported something ready.
#[test]
fn vs14_range_status_counts_a_report_only_for_the_columns_it_names() {
    let table = id(1);
    let mut slow = report(10, table, 1, vec![(3, ColumnState::Ready)]);
    slow.ranges[0].lag_ms = 50;
    let fast = report(11, table, 1, vec![(2, ColumnState::Syncing), (4, ColumnState::Ready)]);
    let syncing = report(12, table, 1, vec![(3, ColumnState::Syncing)]);
    let mut lagging = syncing.clone();
    lagging.ranges[0].lag_ms = 1;
    let status = range_status(&[slow, fast, lagging]);
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].ready, vec![3, 4]);
    assert_eq!(status[0].lag_ms, Some(5), "the syncing replica's lag does not count");
}

/// A late page of a superseded snapshot cannot complete the current snapshot.
/// A complete snapshot still waits if its thirteenth range is syncing.
#[test]
fn rvj_a_move_waits_for_every_page_of_the_same_report() {
    let table = id(1);
    let members = [member(10, "a", SearchRole::Worker)];
    let pending = begin_move(None, table, 0, mv(10, None, 1)).unwrap();
    let mut first = report(10, table, 1, vec![(1, ColumnState::Ready)]);
    first.pages = 2;
    first.ranges = (0..MAX_REPORT_RANGES as u128)
        .map(|n| RangeReport { group_id: id(100 + n), ..first.ranges[0].clone() })
        .collect();
    let mut last = report(10, table, 1, vec![(1, ColumnState::Ready)]);
    last.page = 1;
    last.pages = 2;
    let control = nabedra_rt::ids::uuid_v4();
    record_report(control, 1, first.clone()).unwrap();
    assert_eq!(reconcile(&pending, &members, &reports(control, 1, table).unwrap()), None);

    first.report_id += 1;
    record_report(control, 1, first.clone()).unwrap();
    let inconsistent = SearchReport { pages: 3, ..first.clone() };
    assert!(record_report(control, 1, inconsistent).is_err());
    record_report(control, 1, last.clone()).unwrap();
    assert_eq!(reconcile(&pending, &members, &reports(control, 1, table).unwrap()), None);

    last.report_id = first.report_id;
    last.ranges[0].columns = vec![(1, ColumnState::Syncing)];
    record_report(control, 1, last.clone()).unwrap();
    assert_eq!(reconcile(&pending, &members, &reports(control, 1, table).unwrap()), None);

    first.report_id += 1;
    last.report_id = first.report_id;
    last.ranges[0].columns = vec![(1, ColumnState::Ready)];
    record_report(control, 1, first).unwrap();
    record_report(control, 1, last).unwrap();
    assert_eq!(
        reconcile(&pending, &members, &reports(control, 1, table).unwrap()),
        Some(SearchCommand::Publish { table_id: table, expected_epoch: 0 })
    );
}

/// An overloaded page leaves the previous page and the range count intact.
#[test]
fn rvj_a_refused_report_keeps_the_workers_earlier_report() {
    let control = nabedra_rt::ids::uuid_v4();
    let (table, other) = (id(1), id(2));
    let ready = vec![(1, ColumnState::Ready)];
    let one = SearchReport { pages: 2, ..report(10, table, 1, ready.clone()) };
    record_report(control, 1, one.clone()).unwrap();
    // Fill leader memory to the bound with other workers' reports.
    let mut left = MAX_MEMORY_RANGES - 1;
    let mut worker = 1_000u128;
    while left > 0 {
        let n = left.min(MAX_REPORT_RANGES);
        let mut filler = report(worker, other, 1, ready.clone());
        filler.ranges = (0..n as u128)
            .map(|r| RangeReport { group_id: id(1_000_000 + worker * 16 + r), ..filler.ranges[0].clone() })
            .collect();
        record_report(control, 1, filler).unwrap();
        left -= n;
        worker += 1;
    }
    let mut more = one.clone();
    more.page = 1;
    more.ranges[0].group_id = id(78);
    let error = record_report(control, 1, more.clone()).unwrap_err();
    assert_eq!(error.code, ErrorCode::Overloaded);
    assert_eq!(reports(control, 1, table).unwrap(), [one.clone()]);
    let counts = memory(control, 1, |memory| {
        let actual: usize = memory.reports.values()
            .flat_map(|report| report.pages.values())
            .map(|page| page.ranges.len())
            .sum();
        (memory.ranges, actual)
    }).unwrap();
    assert_eq!(counts, (MAX_MEMORY_RANGES, MAX_MEMORY_RANGES));

    // A new, smaller snapshot frees space and permits retry of the refused page.
    let smaller = SearchReport { report_id: 2, ..report(1_000, other, 1, ready) };
    record_report(control, 1, smaller).unwrap();
    record_report(control, 1, more.clone()).unwrap();
    assert_eq!(reports(control, 1, table).unwrap(), [one, more]);
    assert!(reports(control, 2, table).unwrap().is_empty());
}

#[test]
fn vs06_one_workers_reports_cannot_fill_control_memory() {
    const LIMIT: usize = 1 << 13;
    let control = nabedra_rt::ids::uuid_v4();
    let ready = vec![(1, ColumnState::Ready)];
    let mut left = LIMIT;
    let mut table = 1;
    while left > 0 {
        let n = left.min(MAX_REPORT_RANGES);
        let mut page = report(10, id(table), 1, ready.clone());
        page.ranges = (0..n as u128)
            .map(|r| RangeReport { group_id: id(1_000 + r), ..page.ranges[0].clone() })
            .collect();
        record_report(control, 1, page).unwrap();
        left -= n;
        table += 1;
    }
    let extra = SearchReport { process: id(99), ..report(10, id(table), 1, ready.clone()) };
    let error = record_report(control, 1, extra).unwrap_err();
    assert_eq!(error.code, ErrorCode::Overloaded, "other tables and a new process count against the worker");
    assert!(reports(control, 1, id(table)).unwrap().is_empty());

    let new_worker = report(11, id(table), 1, ready.clone());
    record_report(control, 1, new_worker.clone()).unwrap();
    assert_eq!(reports(control, 1, id(table)).unwrap(), [new_worker]);

    let smaller = SearchReport { report_id: 2, ..report(10, id(1), 1, ready.clone()) };
    record_report(control, 1, smaller).unwrap();
    record_report(control, 1, report(10, id(table), 1, ready)).unwrap();
    assert_eq!(reports(control, 1, id(table)).unwrap().len(), 2);
    assert!(reports(control, 2, id(table)).unwrap().is_empty());
}
