use super::*;
use crate::oltp::{
    mvcc::CanonicalJson,
    pool::{ByteClass, OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources},
};
use nabedra_oltp_types::types::{DecisionStatement, Text, Timestamp};
#[cfg(test)]
use nabedra_oltp_types::types::AbortReason;
use nabedra_resources::config::WorkerResources;
use std::path::PathBuf;
struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    machine: ParticipantMachine,
    shared: Arc<WorkerSharedResources>,
    authority: Authority,
    index: u64,
    table: Uuid,
    catalog_group_id: Uuid,
    _dir: Dir,
}
impl Fixture {
    #[cfg(test)]
    async fn new() -> Self {
        Self::open(None).await
    }

    async fn open(fs: Option<&nabedra_kv::fs::FaultFs>) -> Self {
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let dir = Dir(std::env::temp_dir().join(format!("mvcc-participant-{}", nabedra_rt::ids::uuid_v4())));
        let identity = PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: nabedra_rt::ids::uuid_v4(),
            worker_id: nabedra_rt::ids::uuid_v4(),
            pool_id: nabedra_rt::ids::uuid_v4(),
        };
        let pool = match fs {
            Some(fs) => {
                fs.create_dir_all_durable(&dir.0);
                OltpPool::open_development_on(
                    &dir.0,
                    identity.clone(),
                    shared.clone(),
                    Arc::new(fs.clone()),
                )
            }
            None => OltpPool::open_development(&dir.0, identity.clone(), shared.clone()),
        }
        .unwrap();
        let table = nabedra_rt::ids::uuid_v4();
        let group = pool
            .group(GroupIdentity {
                cluster_id: identity.cluster_id,
                database: Text::try_from("default").unwrap(),
                group_id: nabedra_rt::ids::uuid_v4(),
                kind: GroupKind::Data {
                    table_id: table,
                    shard_id: nabedra_rt::ids::uuid_v4(),
                },
                group_format_version: 1,
            })
            .await
            .unwrap();
        let authority = Authority {
            run_id: nabedra_rt::ids::uuid_v4(),
            owner_term: 1,
        };
        let mut f = Self {
            catalog_group_id: nabedra_rt::ids::uuid_v4(),
            machine: ParticipantMachine::new(group).unwrap(),
            shared,
            authority,
            index: 0,
            table,
            _dir: dir,
        };
        assert!(matches!(
            f.submit(ParticipantCommand::Fence {
                transition_id: nabedra_rt::ids::uuid_v4(),
                expected: None,
                next: authority,
                schema_epoch: 1,
                routing_epoch: 1,
                catalog_group_id: f.catalog_group_id,
                span: KeySpan {
                    start: None,
                    end: None
                }
            })
            .await,
            ParticipantReply::Fenced(_)
        ));
        f
    }
    async fn submit(&mut self, command: ParticipantCommand) -> ParticipantReply {
        let command = OwnedCommand::new_owned(command, &self.shared, ByteClass::Data).unwrap();
        let position = AppliedPosition {
            term: 1,
            node_id: 1,
            index: self.index,
        };
        self.index += 1;
        let raw = self
            .machine
            .apply(
                ApplyMeta {
                    position,
                    command_digest: Digest::from_bytes([position.index as u8; 32]),
                    membership: None,
                },
                command,
            )
            .await
            .unwrap();
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
    #[cfg(test)]
    fn row(&self, key: i64, expected: Option<u64>, change: Change) -> PreparedWrite {
        PreparedWrite {
            key: MvccKey::Row {
                table_id: self.table,
                primary_key: Key::Int64(key),
            },
            expected_version: expected.map(ts),
            change,
        }
    }
    fn put(&self, value: serde_json::Value) -> Change {
        Change::Put(
            CanonicalJson::from_serializable(&self.shared, &value, crate::protocol::MAX_BODY_BYTES)
                .unwrap(),
        )
    }
    fn plan(&self, read: u64, pages: &[Vec<PreparedWrite>]) -> Plan {
        let txn = TxnId {
            request_id: nabedra_rt::ids::uuid_v4(),
            attempt: 0,
        };
        let request = RequestBinding {
            run_id: self.authority.run_id,
            request_id: txn.request_id,
            digest: Digest::from_bytes([1; 32]),
            accepted_context: Digest::from_bytes([2; 32]),
        };
        let manifest = ManifestRef {
            status_group_id: nabedra_rt::ids::uuid_v4(),
            digest: Digest::from_bytes([3; 32]),
            participant_count: 1,
        };
        let mut plan = ParticipantPlan {
            group: self.machine.store.identity().clone(),
            schema_epoch: 1,
            routing_epoch: 1,
            read_ts: ts(read),
            write_root: Digest::from_bytes([0; 32]),
            write_count: pages.iter().map(|p| p.len() as u64).sum(),
            page_count: pages.len() as u64,
        };
        let mut root = initial_root(txn, &plan).unwrap();
        let mut roots = Vec::new();
        for (i, p) in pages.iter().enumerate() {
            roots.push(root);
            root = page_root(root, i as u64, p).unwrap();
        }
        plan.write_root = root;
        Plan {
            txn,
            request,
            manifest,
            plan,
            roots,
        }
    }
    async fn prepare(
        &mut self,
        p: &Plan,
        page: usize,
        writes: Vec<PreparedWrite>,
    ) -> ParticipantReply {
        self.submit(ParticipantCommand::PreparePage {
            authority: self.authority,
            txn: p.txn,
            request: p.request,
            manifest: p.manifest,
            plan: p.plan.clone(),
            page_index: page as u64,
            previous_root: p.roots[page],
            guards: Page::new(vec![
                lifecycle::VerifiedLifecycleGuard::model_for_test(
                    lifecycle::LifecycleBinding {
                        guard_id: nabedra_rt::ids::uuid_v4(),
                        catalog_group_id: self.catalog_group_id,
                        table_id: self.table,
                        lifecycle_version: None,
                        schema_epoch: 1,
                        txn: p.txn,
                        request: p.request,
                        read_ts: p.plan.read_ts,
                        authority: self.authority,
                    },
                    lifecycle::GuardMode::Shared,
                )
                .into_sealed(),
            ])
            .unwrap(),
            writes: Page::new(writes).unwrap(),
        })
        .await
    }
    async fn seal(&mut self, p: &Plan) -> ParticipantReply {
        self.submit(ParticipantCommand::Seal {
            authority: self.authority,
            txn: p.txn,
            manifest: p.manifest,
            result_digest: Digest::from_bytes([4; 32]),
        })
        .await
    }
    async fn resolve(
        &mut self,
        p: &Plan,
        decision: TxnDecision,
        max_items: u16,
    ) -> ParticipantReply {
        let statement = DecisionStatement {
            txn: p.txn,
            request: p.request,
            manifest: p.manifest,
            decided_at: AppliedPosition {
                term: 2,
                node_id: 2,
                index: 10,
            },
            decision,
        };
        let proof = SealedStatusDecision::seal(statement).unwrap();
        self.submit(ParticipantCommand::Resolve {
            authority: self.authority,
            proof,
            max_items,
        })
        .await
    }
    #[cfg(test)]
    async fn read(&self, s: u64, key: Key) -> serde_json::Value {
        let raw = self
            .machine
            .read_at(
                ts(s),
                MvccKey::Row {
                    table_id: self.table,
                    primary_key: key,
                },
            )
            .await
            .unwrap();
        serde_json::from_slice(raw.as_slice()).unwrap()
    }
}
struct Plan {
    txn: TxnId,
    request: RequestBinding,
    manifest: ManifestRef,
    plan: ParticipantPlan,
    roots: Vec<Digest>,
}
fn ts(value: u64) -> Timestamp {
    Timestamp::new(value).unwrap()
}

#[tokio::test]
async fn o01_o22_m02_incremental_prepare_page_budget_retains_exact_limits() {
    let fixture = Fixture::new().await;
    let txn = TxnId {
        request_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    let manifest = ManifestRef {
        status_group_id: nabedra_rt::ids::uuid_v4(),
        digest: Digest::from_bytes([255; 32]),
        participant_count: u64::MAX,
    };
    let reference_cost = |write: &PreparedWrite| {
        let (change, value) = match &write.change {
            Change::Keep => (IntentChange::Keep, None),
            Change::Delete => (IntentChange::Delete, None),
            Change::Put(value) | Change::PutRow { value, .. } => (
                IntentChange::Put,
                Some(serde_json::from_str::<&RawValue>(value.as_str()).unwrap()),
            ),
        };
        let key = key_length(&write.key).unwrap();
        let intent = serde_json::to_vec(&Intent {
            read_ts: ReadTs::new(u64::MAX).unwrap(),
            txn,
            manifest,
            change,
            value,
        })
        .unwrap()
        .len();
        (3 * key + intent + 1024, 2 * key + intent + (256 << 10))
    };
    let check =
        |budget: &mut PreparePageBudget<'_>, write: &PreparedWrite, expected: &mut usize| {
            let (batch, scratch) = reference_cost(write);
            let fits = *expected + batch <= APPLY.batch_bytes && scratch <= APPLY.scratch_bytes;
            assert_eq!(budget.try_add(write).unwrap(), fits);
            if fits {
                *expected += batch;
            }
            assert_eq!(budget.batch_bytes, *expected);
            fits
        };

    let mut budget = PreparePageBudget::new(&txn, &manifest);
    let mut expected = 16 << 10;
    for key in 0..128 {
        let change = if key % 2 == 0 {
            Change::Keep
        } else {
            Change::Delete
        };
        assert!(check(
            &mut budget,
            &fixture.row(key, Some(1), change),
            &mut expected
        ));
    }

    let mut budget = PreparePageBudget::new(&txn, &manifest);
    let mut expected = 16 << 10;
    let wide = fixture.put(serde_json::json!({ "value": "x".repeat(4 << 20) }));
    let mut writes: Vec<_> = (0..8)
        .map(|key| fixture.row(key, None, wide.clone()))
        .collect();
    for write in &writes {
        assert!(check(&mut budget, write, &mut expected));
    }
    let row = |bytes| {
        fixture.row(
            8,
            None,
            fixture.put(serde_json::json!({ "value": "x".repeat(bytes) })),
        )
    };
    let remaining = APPLY.batch_bytes - expected - reference_cost(&row(0)).0;
    assert!(!check(&mut budget, &row(remaining + 1), &mut expected));
    assert!(check(&mut budget, &row(remaining), &mut expected));
    assert_eq!(expected, APPLY.batch_bytes);

    writes.push(row(remaining));
    assert!(page_fits(&txn, &manifest, &writes).unwrap());
    *writes.last_mut().unwrap() = row(remaining + 1);
    assert!(!page_fits(&txn, &manifest, &writes).unwrap());
}
fn committed(value: u64) -> TxnDecision {
    TxnDecision::Commit {
        commit_ts: ts(value),
    }
}

#[tokio::test]
async fn o16_o28_source_image_pages_charge_exact_native_and_wire_owners() {
    use super::image_archive::{ImagePage, ImageRequest};

    let mut f = Fixture::new().await;
    let wide = format!("z{}", "w\0".repeat(6 << 10));
    let keys = (0..60)
        .map(|index| format!("a{index:03}"))
        .chain([wide])
        .collect::<Vec<_>>();
    let writes = keys
        .iter()
        .map(|key| PreparedWrite {
            key: MvccKey::Row {
                table_id: f.table,
                primary_key: Key::Utf8(key.clone()),
            },
            expected_version: None,
            change: f.put(serde_json::json!({"n": 1})),
        })
        .collect::<Vec<_>>();
    let plan = f.plan(10, &[writes.clone()]);
    f.prepare(&plan, 0, writes).await;
    f.seal(&plan).await;
    f.resolve(&plan, committed(11), 128).await;

    let mut after = None;
    let mut actual = Vec::new();
    let mut pages = 0;
    loop {
        let image = f
            .machine
            .committed_image_page(
                OwnedCommand::new_owned(
                    ImageRequest {
                        read_ts: ts(12),
                        schema_epoch: 1,
                        routing_epoch: 1,
                        span: KeySpan {
                            start: None,
                            end: None,
                        },
                        after: after.clone(),
                        max_items: 128,
                        max_bytes: 64 << 10,
                        search: None,
                    },
                    &f.shared,
                    ByteClass::Data,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let wire = f
            .shared
            .encode_in(ByteClass::Data, &image, 64 << 10)
            .unwrap();
        let decoded =
            OwnedCommand::<ImagePage>::decode_json(&wire, &f.shared, ByteClass::Data, 128 << 20)
                .unwrap();
        drop(wire);
        assert_eq!(
            nabedra_oltp_types::owned::serialized_digest(&decoded).unwrap(),
            nabedra_oltp_types::owned::serialized_digest(&image).unwrap()
        );
        let page = decoded.payload().unwrap();
        let ImagePage::Images {
            images, exhausted, ..
        } = page
        else {
            panic!("settled source blocked")
        };
        assert!(!images.is_empty());
        for image in images.iter() {
            let MvccKey::Row {
                primary_key: Key::Utf8(key),
                ..
            } = &image.key
            else {
                panic!("unexpected copied key")
            };
            assert_eq!(image.value.as_ref().unwrap().as_str(), r#"{"n":1}"#);
            assert_eq!(image.commit_ts, ts(11));
            actual.push(key.clone());
        }
        pages += 1;
        if *exhausted {
            break;
        }
        let next = page.continuation().unwrap().cloned().unwrap();
        assert!(
            after
                .as_ref()
                .is_none_or(|prior| super::compare_keys(prior, &next).is_lt())
        );
        after = Some(next);
        assert!(pages < keys.len());
    }
    assert!(pages >= 3);
    assert_eq!(actual, keys);
}
#[cfg(test)]
fn aborted() -> TxnDecision {
    TxnDecision::Abort {
        reason: AbortReason::Conflict,
    }
}

#[tokio::test]
async fn o11_o12_reclaim_reopens_mid_page_sequence_and_releases_run_bytes() {
    index_reclamation_storage_evidence().await;
}

pub async fn index_reclamation_storage_evidence() -> (u64, u64) {
    use nabedra_kv::fs::{FaultFs, Fs, OpenMode};
    let fs = FaultFs::new();
    let mut f = Fixture::open(Some(&fs)).await;
    let retired = Uuid::from_u128(1);
    let live = Uuid::from_u128(2);
    let index_key = |id, primary| MvccKey::IndexEntry {
        table_id: f.table,
        index_id: id,
        values: Page::new(vec![super::super::index::IndexValue::Int64(1)]).unwrap(),
        primary_key: Key::Int64(primary),
    };
    let live_key = index_key(live, 0);
    let mut writes = (0..65)
        .map(|id| PreparedWrite {
            key: index_key(retired, id),
            expected_version: None,
            change: f.put(serde_json::json!({"cover":"x".repeat(4096)})),
        })
        .collect::<Vec<_>>();
    writes.push(PreparedWrite {
        key: live_key.clone(),
        expected_version: None,
        change: f.put(serde_json::json!({"cover":"live"})),
    });
    let plan = f.plan(10, &[writes.clone()]);
    assert!(matches!(
        f.prepare(&plan, 0, writes).await,
        ParticipantReply::PreparedPage { .. }
    ));
    f.seal(&plan).await;
    f.resolve(&plan, committed(11), 128).await;
    let run_bytes = |fs: &FaultFs, path: &std::path::Path| -> u64 {
        fs.list(&path.join("kv/runs"))
            .unwrap()
            .iter()
            .filter(|path| path.extension().is_some_and(|ext| ext == "run"))
            .map(|path| fs.open(path, OpenMode::Read).unwrap().len().unwrap())
            .sum()
    };
    async fn flush_and_compact(f: &Fixture) {
        let pool = f.machine.store.pool().clone();
        nabedra_rt::task::spawn_blocking(move || {
            pool.kv_store().flush().unwrap();
            pool.kv_store().compact().unwrap();
        })
        .await
        .unwrap();
    }

    flush_and_compact(&f).await;
    let before = run_bytes(&fs, &f._dir.0);
    assert!(
        before >= 256 << 10,
        "retired payload should already reside on runs"
    );
    let reclaim = index_reclaim::ReclaimIndex {
        authority: f.authority,
        catalog_group_id: f.catalog_group_id,
        table_id: f.table,
        index_id: retired,
        retired_at: ts(15),
        safe_ts: ts(16),
        routing_version: None,
    };
    assert_eq!(
        f.submit(ParticipantCommand::ReclaimIndex(
            index_reclaim::ReclaimIndex {
                safe_ts: reclaim.retired_at,
                ..reclaim
            }
        ))
        .await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
    let command = ParticipantCommand::ReclaimIndex(reclaim);
    assert_eq!(
        f.submit(command.clone()).await,
        ParticipantReply::IndexReclaimed {
            deleted: 32,
            complete: false
        }
    );
    fs.halt();
    let fs = fs.crash();
    let identity = f.machine.store.pool().identity().clone();
    let group = f.machine.store.identity().clone();
    let Fixture {
        machine,
        shared,
        authority,
        index,
        table,
        catalog_group_id,
        _dir,
    } = f;
    drop(machine);
    let pool =
        OltpPool::open_development_on(&_dir.0, identity, shared.clone(), Arc::new(fs.clone()))
            .unwrap();
    let mut f = Fixture {
        machine: ParticipantMachine::new(pool.retained_group(group).await.unwrap()).unwrap(),
        shared,
        authority,
        index,
        table,
        catalog_group_id,
        _dir,
    };
    let mut deleted = 0;
    for _ in 0..4 {
        let reply = f.submit(command.clone()).await;
        let ParticipantReply::IndexReclaimed {
            deleted: count,
            complete,
        } = reply
        else {
            panic!("reclamation did not resume after reopening the durable cut: {reply:?}");
        };
        assert!(count <= 32);
        deleted += count;
        flush_and_compact(&f).await;
        if complete {
            break;
        }
    }
    assert_eq!(
        deleted, 98,
        "the first 32 of 65 heads and 65 versions survived the crash as deleted"
    );
    let visible: serde_json::Value = serde_json::from_slice(
        f.machine
            .read_at(ts(16), live_key)
            .await
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    assert_eq!(visible["value"]["value"]["cover"], "live");
    assert_eq!(
        f.submit(command).await,
        ParticipantReply::IndexReclaimed {
            deleted: 0,
            complete: true
        }
    );
    let after = run_bytes(&fs, &f._dir.0);
    assert!(
        after <= 64 << 10,
        "run bytes after reclamation: {after}, before: {before}"
    );
    (before, after)
}

/// O16/O21/O28 storage assertions. Catalog/Status authority is modeled here;
/// distributed split publication requires its separate production oracle.
#[tokio::test]
async fn o16_o21_o28_split_images_preserve_versions_indexes_and_exact_page_retry() {
    use super::{image_archive::ImagePage, lineage::SplitBinding};
    use nabedra_oltp_types::authority::{SealedSettlement, SettlementStatement};
    struct Child {
        machine: ParticipantMachine,
        index: u64,
    }
    impl Child {
        async fn apply(
            &mut self,
            shared: &WorkerSharedResources,
            command: ParticipantCommand,
        ) -> ParticipantReply {
            let command = OwnedCommand::new_owned(command, shared, ByteClass::Data).unwrap();
            self.apply_owned(command).await
        }
        async fn apply_owned(
            &mut self,
            command: OwnedCommand<ParticipantCommand>,
        ) -> ParticipantReply {
            let digest = nabedra_oltp_types::owned::serialized_digest(&command).unwrap();
            let position = AppliedPosition {
                term: 1,
                node_id: 1,
                index: self.index,
            };
            self.index += 1;
            let raw = self
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
            serde_json::from_slice(raw.as_slice()).unwrap()
        }
    }
    let mut f = Fixture::new().await;
    let index_id = nabedra_rt::ids::uuid_v4();
    let index_key = MvccKey::IndexEntry {
        table_id: f.table,
        index_id,
        values: Page::new(vec![super::super::index::IndexValue::Int64(1)]).unwrap(),
        primary_key: Key::Int64(1),
    };
    let mut writes = vec![
        f.row(
            1,
            None,
            f.put(serde_json::json!({"id":1,"v":"a".repeat(40 << 10)})),
        ),
        f.row(2, None, Change::Delete),
        f.row(3, None, Change::Keep),
        f.row(
            4,
            None,
            f.put(serde_json::json!({"id":4,"v":"b".repeat(40 << 10)})),
        ),
        PreparedWrite {
            key: index_key.clone(),
            expected_version: None,
            change: f.put(serde_json::json!({"id":1})),
        },
        PreparedWrite {
            key: MvccKey::IndexEntry {
                table_id: f.table,
                index_id,
                values: Page::new(vec![super::super::index::IndexValue::Int64(1)]).unwrap(),
                primary_key: Key::Int64(4),
            },
            expected_version: None,
            change: f.put(serde_json::json!({"id":4})),
        },
    ];
    writes.sort_by(|a, b| super::compare_keys(&a.key, &b.key));
    let plan = f.plan(10, &[writes.clone()]);
    f.prepare(&plan, 0, writes).await;
    f.seal(&plan).await;
    f.resolve(&plan, committed(11), 128).await;
    eprintln!("split storage stage=source_settled");
    let split_txn = TxnId {
        request_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    let split_request = RequestBinding {
        request_id: split_txn.request_id,
        ..plan.request
    };
    let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
        lifecycle::LifecycleBinding {
            guard_id: nabedra_rt::ids::uuid_v4(),
            catalog_group_id: f.catalog_group_id,
            table_id: f.table,
            lifecycle_version: Some(ts(1)),
            schema_epoch: 1,
            txn: split_txn,
            request: split_request,
            read_ts: ts(11),
            authority: f.authority,
        },
        lifecycle::GuardMode::Exclusive,
    )
    .into_sealed();
    let mut children = Vec::new();
    let mut bindings = Vec::new();
    for span in [
        KeySpan {
            start: None,
            end: Some(Key::Int64(3)),
        },
        KeySpan {
            start: Some(Key::Int64(3)),
            end: None,
        },
    ] {
        let mut target = f.machine.store.identity().clone();
        target.group_id = nabedra_rt::ids::uuid_v4();
        target.kind = GroupKind::Data {
            table_id: f.table,
            shard_id: nabedra_rt::ids::uuid_v4(),
        };
        let mut child = Child {
            machine: ParticipantMachine::new(
                f.machine.store.pool().group(target.clone()).await.unwrap(),
            )
            .unwrap(),
            index: 0,
        };
        assert!(matches!(
            child
                .apply(
                    &f.shared,
                    ParticipantCommand::Fence {
                        transition_id: nabedra_rt::ids::uuid_v4(),
                        expected: None,
                        next: f.authority,
                        schema_epoch: 1,
                        routing_epoch: 2,
                        span: span.clone(),
                        catalog_group_id: f.catalog_group_id,
                    }
                )
                .await,
            ParticipantReply::Fenced(_)
        ));
        bindings.push(SplitBinding {
            split_id: guard.binding().guard_id,
            source: f.machine.store.identity().clone(),
            source_schema_epoch: 1,
            source_routing_epoch: 1,
            source_span: KeySpan {
                start: None,
                end: None,
            },
            target,
            target_span: span,
            read_ts: ts(12),
            guard: guard.clone(),
        });
        children.push(child);
    }
    let bindings: [SplitBinding; 2] = bindings.try_into().unwrap();
    let freeze = ParticipantCommand::SplitFreeze {
        authority: f.authority,
        children: Box::new(bindings.clone()),
    };
    let stale_write = vec![f.row(7, None, f.put(serde_json::json!({"id":7})))];
    let stale_plan = f.plan(11, &[stale_write.clone()]);
    assert!(matches!(
        f.prepare(&stale_plan, 0, stale_write.clone()).await,
        ParticipantReply::PreparedPage { .. }
    ));
    assert_eq!(
        f.submit(freeze.clone()).await,
        ParticipantReply::Rejected(Fault::Incomplete),
        "source fence must wait for unresolved prepares"
    );
    f.resolve(
        &stale_plan,
        TxnDecision::Abort {
            reason: AbortReason::Conflict,
        },
        128,
    )
    .await;

    let settlement = |txn, request, decision| {
        let manifest = plan.manifest;
        let position = AppliedPosition {
            term: 1,
            node_id: 1,
            index: 80,
        };
        SealedSettlement::seal(SettlementStatement {
            txn,
            request,
            manifest,
            resolved_count: manifest.participant_count,
            settled_at: position,
            decision: DecisionStatement {
                txn,
                request,
                manifest,
                decided_at: position,
                decision,
            },
        })
        .unwrap()
    };
    let mut cancelled_bindings = bindings.clone();
    let cancelled_id = nabedra_rt::ids::uuid_v4();
    let cancelled_txn = TxnId {
        request_id: nabedra_rt::ids::uuid_v4(),
        attempt: 1,
    };
    let cancelled_request = RequestBinding {
        request_id: cancelled_txn.request_id,
        ..split_request
    };
    let cancelled_guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
        lifecycle::LifecycleBinding {
            guard_id: cancelled_id,
            txn: cancelled_txn,
            request: cancelled_request,
            ..guard.binding().clone()
        },
        lifecycle::GuardMode::Exclusive,
    )
    .into_sealed();
    for binding in &mut cancelled_bindings {
        binding.split_id = cancelled_id;
        binding.guard = cancelled_guard.clone();
    }
    let cancelled_freeze = ParticipantCommand::SplitFreeze {
        authority: f.authority,
        children: Box::new(cancelled_bindings),
    };
    assert!(matches!(
        f.submit(cancelled_freeze.clone()).await,
        ParticipantReply::SplitSourceFrozen(_)
    ));
    let aborted = TxnDecision::Abort {
        reason: AbortReason::AbandonedByFence,
    };
    let finish_cancelled = ParticipantCommand::SplitFinishSource {
        authority: f.authority,
        split_id: cancelled_id,
        settled: settlement(cancelled_txn, cancelled_request, aborted),
    };
    assert_eq!(
        f.submit(finish_cancelled.clone()).await,
        ParticipantReply::SplitSourceFinished(aborted)
    );
    assert_eq!(
        f.read(14, Key::Int64(1)).await["value"]["version"],
        11,
        "the old route reopens on a settled abort"
    );
    assert_eq!(
        f.submit(cancelled_freeze).await,
        ParticipantReply::Rejected(Fault::AlreadyDecided)
    );

    let frozen = f.submit(freeze.clone()).await;
    let ParticipantReply::SplitSourceFrozen(statement) = &frozen else {
        panic!("source fence was not applied");
    };
    assert_eq!(statement.split_id, bindings[0].split_id);
    assert_eq!(statement.read_ts, ts(12));
    assert_eq!(f.submit(freeze).await, frozen, "lost fence reply");
    let reclaim = index_reclaim::ReclaimIndex {
        authority: f.authority,
        catalog_group_id: f.catalog_group_id,
        table_id: f.table,
        index_id,
        retired_at: ts(11),
        safe_ts: ts(12),
        routing_version: None,
    };
    assert_eq!(
        f.submit(ParticipantCommand::ReclaimIndex(reclaim)).await,
        ParticipantReply::Rejected(Fault::Incomplete),
        "even a post-retirement split must hold its source image stable"
    );
    assert_eq!(
        f.submit(finish_cancelled).await,
        ParticipantReply::SplitSourceFinished(aborted),
        "an old abort retry must leave the new freeze in place"
    );
    let next_write = f.plan(11, &[stale_write.clone()]);
    assert_eq!(
        f.prepare(&next_write, 0, stale_write).await,
        ParticipantReply::Rejected(Fault::Incomplete)
    );

    let point = MvccKey::Row {
        table_id: f.table,
        primary_key: Key::Int64(1),
    };
    assert_eq!(
        f.machine
            .read_at(ts(13), point.clone())
            .await
            .err()
            .unwrap()
            .code,
        ErrorCode::SplitPublicationPending,
        "an uncertain cutover cannot serve newer reads from the old route"
    );
    for (child, binding) in children.iter_mut().zip(bindings.iter()) {
        let stage = ParticipantCommand::SplitStage {
            authority: f.authority,
            binding: Box::new(binding.clone()),
        };
        assert_eq!(
            child.apply(&f.shared, stage.clone()).await,
            ParticipantReply::SplitStaged
        );
        assert_eq!(
            child.apply(&f.shared, stage).await,
            ParticipantReply::SplitStaged
        );
    }
    assert_eq!(
        children[0]
            .machine
            .read_at(ts(12), point.clone())
            .await
            .err()
            .unwrap()
            .code,
        ErrorCode::StorageUnavailable,
        "a staged child cannot serve an incomplete range"
    );
    let mut after = None;
    let mut after_digest = None;
    let mut root = lineage::initial_root(&bindings[0]).unwrap();
    assert_eq!(root, lineage::initial_root(&bindings[1]).unwrap());
    let mut page_index = 0;
    let mut totals = [0; 2];
    loop {
        let request = OwnedCommand::new_owned(
            bindings[0].image_request(after.clone(), 64 << 10),
            &f.shared,
            ByteClass::Data,
        )
        .unwrap();
        let page = f.machine.committed_image_page(request).await.unwrap();
        eprintln!("split storage stage=source_page page={page_index}");
        let next = page.payload().unwrap().continuation().unwrap().cloned();
        let ImagePage::Images { exhausted, .. } = page.payload().unwrap() else {
            panic!("settled source blocked")
        };
        let mut assembled_root = None;
        for i in 0..2 {
            let (command, expected_root) = bindings[i]
                .import_page_from_image(&f.shared, page_index, root, after_digest, page.clone())
                .await
                .unwrap();
            assert_eq!(*assembled_root.get_or_insert(expected_root), expected_root);
            let reply = children[i].apply_owned(command.clone()).await;
            let ParticipantReply::SplitImported {
                pages,
                images,
                source_images,
                root: actual,
                complete,
            } = reply.clone()
            else {
                panic!("child rejected {reply:?}")
            };
            assert_eq!(pages, page_index + 1);
            assert_eq!(complete, *exhausted);
            assert_eq!(actual, expected_root);
            assert!(source_images >= images);
            totals[i] = images;
            assert_eq!(
                children[i].apply_owned(command.clone()).await,
                reply,
                "lost reply must reconcile exact page"
            );
            let ParticipantCommand::SplitImportPage { page: input, .. } =
                command.payload().unwrap()
            else {
                panic!("split page builder produced a different command");
            };
            let mut altered = input.as_ref().clone();
            altered.previous_root = Digest::from_bytes([91; 32]);
            assert_eq!(
                children[i]
                    .apply(
                        &f.shared,
                        ParticipantCommand::SplitImportPage {
                            authority: f.authority,
                            page: Box::new(altered)
                        }
                    )
                    .await,
                ParticipantReply::Rejected(Fault::InvalidBinding)
            );
            if page_index == 0 && i == 0 {
                let lagging = children[1]
                    .machine
                    .split_progress(&bindings[1], 64 << 10)
                    .await
                    .unwrap();
                let lagging = lagging.payload().unwrap();
                assert_eq!(lagging.pages, 0);
                let retry_source = f
                    .machine
                    .committed_image_page(
                        OwnedCommand::new_owned(
                            bindings[0].image_request(lagging.after.clone(), 64 << 10),
                            &f.shared,
                            ByteClass::Data,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                let (retry, retry_root) = bindings[0]
                    .import_page_from_image(
                        &f.shared,
                        lagging.pages,
                        lagging.root,
                        lagging.after_digest,
                        retry_source,
                    )
                    .await
                    .unwrap();
                assert_eq!(retry_root, expected_root);
                assert_eq!(
                    children[0].apply_owned(retry).await,
                    reply,
                    "rebuild the exact lost page using the lagging child's durable cursor"
                );
            }
        }
        root = assembled_root.unwrap();
        page_index += 1;
        for i in 0..2 {
            let progress = children[i]
                .machine
                .split_progress(&bindings[i], 64 << 10)
                .await
                .unwrap();
            let progress = progress.payload().unwrap();
            assert_eq!(progress.root, root);
            assert_eq!(progress.pages, page_index);
            assert_eq!(progress.after, next);
            assert_eq!(progress.complete, *exhausted);
            if i == 0 {
                after_digest = progress.after_digest;
            } else {
                assert_eq!(progress.after_digest, after_digest);
            }
        }
        after = next;
        eprintln!("split storage stage=children_imported page={page_index}");
        if *exhausted {
            break;
        }
    }
    assert!(page_index > 1, "source must adapt pages to byte budget");
    assert_eq!(
        totals,
        [3, 2],
        "row, tombstone and index must stay with primary span"
    );
    eprintln!("split storage stage=scalar_comparison");
    for (key, target) in [(1, 0), (2, 0), (4, 1)] {
        let logical = MvccKey::Row {
            table_id: f.table,
            primary_key: Key::Int64(key),
        };
        let actual = children[target]
            .machine
            .read_at(ts(12), logical.clone())
            .await
            .unwrap();
        let actual_value: serde_json::Value = serde_json::from_slice(actual.as_slice()).unwrap();
        drop(actual);
        let expected = f.machine.read_at(ts(12), logical).await.unwrap();
        let expected_value: serde_json::Value =
            serde_json::from_slice(expected.as_slice()).unwrap();
        drop(expected);
        assert_eq!(actual_value, expected_value);
    }
    let actual = children[0]
        .machine
        .read_at(ts(12), index_key.clone())
        .await
        .unwrap();
    let actual_value: serde_json::Value = serde_json::from_slice(actual.as_slice()).unwrap();
    drop(actual);
    let expected = f.machine.read_at(ts(12), index_key).await.unwrap();
    let expected_value: serde_json::Value = serde_json::from_slice(expected.as_slice()).unwrap();
    drop(expected);
    assert_eq!(actual_value, expected_value);
    eprintln!("split storage stage=complete");
    let early_settlement = ParticipantCommand::SplitFinishSource {
        authority: f.authority,
        split_id: bindings[0].split_id,
        settled: settlement(split_txn, split_request, committed(12)),
    };
    assert_eq!(
        f.submit(early_settlement).await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
    let finished = ParticipantCommand::SplitFinishSource {
        authority: f.authority,
        split_id: bindings[0].split_id,
        settled: settlement(split_txn, split_request, committed(15)),
    };
    assert_eq!(
        f.submit(finished.clone()).await,
        ParticipantReply::SplitSourceFinished(committed(15))
    );
    assert_eq!(
        f.submit(finished).await,
        ParticipantReply::SplitSourceFinished(committed(15))
    );
    assert_eq!(
        f.read(14, Key::Int64(1)).await["value"]["version"],
        11,
        "reads pinned after the image cut but before publication still work"
    );
    assert_eq!(
        f.machine
            .read_at(ts(15), point.clone())
            .await
            .err()
            .unwrap()
            .code,
        ErrorCode::StorageUnavailable
    );
    assert_eq!(
        children[0]
            .machine
            .read_at(ts(11), point)
            .await
            .err()
            .unwrap()
            .code,
        ErrorCode::StorageUnavailable,
        "only the old source can answer a pin before the imported cut"
    );
    assert_eq!(
        f.read(11, Key::Int64(1)).await["value"]["version"],
        11,
        "old pinned source survives"
    );
    for child in &mut children {
        let reclaim = index_reclaim::ReclaimIndex {
            routing_version: Some(ts(15)),
            ..reclaim
        };
        assert_eq!(
            child
                .apply(&f.shared, ParticipantCommand::ReclaimIndex(reclaim))
                .await,
            ParticipantReply::Rejected(Fault::Incomplete)
        );
        assert_eq!(
            child
                .apply(
                    &f.shared,
                    ParticipantCommand::ReclaimIndex(index_reclaim::ReclaimIndex {
                        safe_ts: ts(15),
                        ..reclaim
                    })
                )
                .await,
            ParticipantReply::IndexReclaimed {
                deleted: 2,
                complete: true
            }
        );
    }
}

/// Deterministic participant-storage evidence with an exact decision statement.
#[tokio::test]
async fn o19_o28_archive_pages_preserve_put_delete_keep_and_require_full_resolution() {
    let mut f = Fixture::new().await;
    let writes = vec![
        f.row(1, None, f.put(serde_json::json!({"id":1,"v":"archived"}))),
        f.row(2, None, Change::Delete),
        f.row(3, None, Change::Keep),
    ];
    let p = f.plan(10, &[writes.clone()]);
    f.prepare(&p, 0, writes).await;
    f.seal(&p).await;
    let statement = DecisionStatement {
        txn: p.txn,
        request: p.request,
        manifest: p.manifest,
        decided_at: AppliedPosition {
            term: 2,
            node_id: 2,
            index: 10,
        },
        decision: committed(11),
    };
    f.resolve(&p, committed(11), 1).await;
    assert!(
        f.machine
            .archive_effect_page_from_statement(&statement, 0, 1, 64 << 10)
            .await
            .is_err()
    );
    f.resolve(&p, committed(11), 128).await;
    let first = f
        .machine
        .archive_effect_page_from_statement(&statement, 0, 1, 64 << 10)
        .await
        .unwrap();
    let page = first.payload().unwrap();
    assert_eq!(page.next_ordinal, 1);
    assert!(!page.complete);
    assert_eq!(page.effects.len(), 1);
    assert_eq!(
        page.effects[0].key,
        MvccKey::Row {
            table_id: f.table,
            primary_key: Key::Int64(1)
        }
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(page.effects[0].value.as_ref().unwrap().as_str())
            .unwrap(),
        serde_json::json!({"id":1,"v":"archived"})
    );
    let second = f
        .machine
        .archive_effect_page_from_statement(&statement, 1, 1, 64 << 10)
        .await
        .unwrap();
    let page = second.payload().unwrap();
    assert_eq!(page.next_ordinal, 2);
    assert!(!page.complete);
    assert_eq!(page.effects.len(), 1);
    assert!(page.effects[0].value.is_none());
    let last = f
        .machine
        .archive_effect_page_from_statement(&statement, 2, 1, 64 << 10)
        .await
        .unwrap();
    let page = last.payload().unwrap();
    assert_eq!(page.next_ordinal, 3);
    assert!(page.complete);
    assert!(
        page.effects.is_empty(),
        "Keep must not invent a committed version"
    );
    assert!(
        f.machine
            .archive_effect_page_from_statement(&statement, 4, 1, 64 << 10)
            .await
            .is_err()
    );
    drop((first, second, last));
    assert_eq!(f.shared.control_bytes_used(), 0);
}

/// O04/O07/O09/M01 deterministic native-storage regression. Consensus barriers
/// and authenticated RPC are exercised separately by the gateway fixture.
#[tokio::test]
async fn o04_o07_o09_point_pages_match_scalar_reads_and_block_absent_key_intents() {
    use super::{point::PointReadRequest, scan::ScanPage};
    let mut f = Fixture::new().await;
    let writes = vec![
        f.row(
            1,
            None,
            f.put(serde_json::json!({"id":1,"v":"a".repeat(40 << 10)})),
        ),
        f.row(2, None, Change::Delete),
        f.row(3, None, Change::Keep),
        f.row(
            4,
            None,
            f.put(serde_json::json!({"id":4,"v":"b".repeat(40 << 10)})),
        ),
    ];
    let p = f.plan(10, &[writes.clone()]);
    assert!(matches!(
        f.prepare(&p, 0, writes).await,
        ParticipantReply::PreparedPage { .. }
    ));
    f.seal(&p).await;
    f.resolve(&p, committed(11), 128).await;
    let request = |keys: Vec<i64>, read_ts| {
        OwnedCommand::new_owned(
            PointReadRequest {
                read_ts: ts(read_ts),
                keys: Page::new(
                    keys.into_iter()
                        .map(|key| MvccKey::Row {
                            table_id: f.table,
                            primary_key: Key::Int64(key),
                        })
                        .collect(),
                )
                .unwrap(),
                max_bytes: 64 << 10,
            },
            &f.shared,
            ByteClass::Data,
        )
        .unwrap()
    };
    let first = f
        .machine
        .read_page(request(vec![1, 2, 3, 4, 5], 11))
        .await
        .unwrap();
    let ScanPage::Records { records, exhausted } = first.payload().unwrap() else {
        panic!("unexpected blocked settled page")
    };
    assert!(!exhausted);
    assert_eq!(records.len(), 3, "byte budget must return the exact prefix");
    for (record, key) in records.iter().zip([1, 2, 3]) {
        let scalar = f.read(11, Key::Int64(key)).await;
        assert_eq!(
            serde_json::to_value(record.version).unwrap(),
            scalar["value"]["version"]
        );
        let value = record
            .value
            .as_ref()
            .map(|v| serde_json::from_str::<serde_json::Value>(v.as_str()).unwrap());
        assert_eq!(
            serde_json::to_value(value).unwrap(),
            scalar["value"]["value"]
        );
    }
    assert_eq!(
        records[1].version,
        Some(ts(11)),
        "tombstone version must survive"
    );
    assert_eq!(records[2].version, None, "Keep must retain exact absence");
    drop(first);
    let last = f.machine.read_page(request(vec![4, 5], 11)).await.unwrap();
    let ScanPage::Records { records, exhausted } = last.payload().unwrap() else {
        panic!("unexpected blocked suffix")
    };
    assert!(*exhausted);
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].version, None);
    assert!(records[1].value.is_none());
    drop(last);
    // Release the request builder's immutable fixture borrow before mutation.
    let pending = vec![f.row(5, None, f.put(serde_json::json!({"id":5,"v":"pending"})))];
    let p = f.plan(11, &[pending.clone()]);
    f.prepare(&p, 0, pending).await;
    let read = |read_ts| {
        OwnedCommand::new_owned(
            PointReadRequest {
                read_ts: ts(read_ts),
                keys: Page::new(vec![MvccKey::Row {
                    table_id: f.table,
                    primary_key: Key::Int64(5),
                }])
                .unwrap(),
                max_bytes: 64 << 10,
            },
            &f.shared,
            ByteClass::Data,
        )
        .unwrap()
    };
    let blocked = f.machine.read_page(read(12)).await.unwrap();
    assert!(
        matches!(blocked.payload().unwrap(), ScanPage::Blocked { txn, manifest } if *txn == p.txn && *manifest == p.manifest)
    );
    let pinned = f.machine.read_page(read(11)).await.unwrap();
    let ScanPage::Records { records, exhausted } = pinned.payload().unwrap() else {
        panic!("later intent blocked original snapshot")
    };
    assert!(*exhausted);
    assert_eq!(records[0].version, None);
    assert!(records[0].value.is_none());
    drop((blocked, pinned));
    f.resolve(&p, aborted(), 128).await;
    assert_eq!(f.shared.control_bytes_used(), 0);
}

#[tokio::test]
async fn o07_o09_fixed_pages_seal_exact_plan_and_delayed_commit_stays_discoverable() {
    let mut f = Fixture::new().await;
    let writes = vec![
        f.row(-1, None, f.put(serde_json::json!({"id":-1,"v":"a"}))),
        f.row(2, None, f.put(serde_json::json!({"id":2,"v":"b"}))),
    ];
    let p = f.plan(10, &[writes.clone()]);
    assert_eq!(
        f.read(10, Key::Int64(-1)).await,
        serde_json::json!({"value":{"version":null,"value":null}})
    );
    assert_eq!(
        f.prepare(&p, 0, writes.clone()).await,
        ParticipantReply::PreparedPage {
            next_page: 1,
            writes: 2
        }
    );
    assert!(f.read(20, Key::Int64(-1)).await.get("blocked").is_some());
    assert_eq!(
        f.prepare(&p, 0, writes.clone()).await,
        ParticipantReply::PreparedPage {
            next_page: 1,
            writes: 2
        }
    );
    let mut changed = writes;
    changed[0].change = Change::Delete;
    assert_eq!(
        f.prepare(&p, 0, changed).await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
    assert!(matches!(f.seal(&p).await, ParticipantReply::Sealed(_)));
    assert_eq!(
        f.resolve(&p, committed(11), 1).await,
        ParticipantReply::Resolved {
            next_ordinal: 1,
            complete: false
        }
    );
    assert_eq!(
        f.read(10, Key::Int64(-1)).await["value"]["version"],
        serde_json::Value::Null
    );
    assert_eq!(f.read(11, Key::Int64(-1)).await["value"]["value"]["v"], "a");
    assert!(f.read(11, Key::Int64(2)).await.get("blocked").is_some());
    assert_eq!(
        f.resolve(&p, committed(11), 1).await,
        ParticipantReply::Resolved {
            next_ordinal: 2,
            complete: true
        }
    );
    assert_eq!(f.read(11, Key::Int64(2)).await["value"]["value"]["v"], "b");
    assert_eq!(
        f.resolve(&p, committed(11), 128).await,
        ParticipantReply::Resolved {
            next_ordinal: 2,
            complete: true
        }
    );
    assert_eq!(f.shared.control_bytes_used(), 0);
}

/// Exercises native participant state with modeled Status and quorum replies.
#[tokio::test]
async fn o06_o07_o10_partial_resolution_witness_requires_exact_decision_and_current_quorum() {
    use crate::oltp::mvcc::store::consensus::{Applied, Consensus, ReadBarrier};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct ModeledQuorum(AtomicBool);
    impl Consensus<ParticipantCommand, ParticipantReply> for ModeledQuorum {
        fn propose(
            &self,
            _: OwnedCommand<ParticipantCommand>,
        ) -> ParticipantFuture<'_, Applied<ParticipantReply>> {
            unreachable!("this fixture applies commands through the participant machine")
        }

        fn ensure_linearizable(&self) -> ParticipantFuture<'_, ReadBarrier> {
            Box::pin(async {
                if !self.0.load(Ordering::Relaxed) {
                    return Err(recovery_unknown());
                }
                Ok(ReadBarrier {
                    applied: AppliedPosition {
                        term: 1,
                        node_id: 1,
                        index: 0,
                    },
                })
            })
        }
    }

    let mut f = Fixture::new().await;
    let writes: Vec<_> = (1..=3)
        .map(|id| f.row(id, None, f.put(serde_json::json!({"id": id}))))
        .collect();
    let p = f.plan(10, &[writes.clone()]);
    assert!(matches!(
        f.prepare(&p, 0, writes).await,
        ParticipantReply::PreparedPage { .. }
    ));
    assert!(matches!(f.seal(&p).await, ParticipantReply::Sealed(_)));
    let statement = DecisionStatement {
        txn: p.txn,
        request: p.request,
        manifest: p.manifest,
        decided_at: AppliedPosition {
            term: 2,
            node_id: 2,
            index: 10,
        },
        decision: committed(11),
    };
    let as_proof = |statement| {
        SealedStatusDecision::seal(statement).unwrap()
    };
    let proof = as_proof(statement.clone());
    let quorum = Arc::new(ModeledQuorum(AtomicBool::new(true)));
    let handle = ParticipantHandle::new(f.machine.clone(), quorum.clone());
    let owner = f.authority;
    assert!(!handle.has_resolution_witness(owner, &proof).await.unwrap());
    assert_eq!(
        f.resolve(&p, committed(11), 1).await,
        ParticipantReply::Resolved {
            next_ordinal: 1,
            complete: false
        }
    );
    assert!(handle.has_resolution_witness(owner, &proof).await.unwrap());
    assert!(f.read(11, Key::Int64(2)).await.get("blocked").is_some());

    for changed in [
        DecisionStatement {
            decided_at: AppliedPosition {
                index: 11,
                ..statement.decided_at
            },
            ..statement.clone()
        },
        DecisionStatement {
            decision: committed(12),
            ..statement.clone()
        },
        DecisionStatement {
            decision: aborted(),
            ..statement.clone()
        },
        DecisionStatement {
            request: RequestBinding {
                accepted_context: Digest::from_bytes([5; 32]),
                ..statement.request
            },
            ..statement.clone()
        },
        DecisionStatement {
            manifest: ManifestRef {
                digest: Digest::from_bytes([6; 32]),
                ..statement.manifest
            },
            ..statement
        },
    ] {
        let forged = as_proof(changed);
        assert_eq!(
            handle
                .has_resolution_witness(owner, &forged)
                .await
                .unwrap_err()
                .code,
            ErrorCode::ValidationError
        );
        assert_eq!(
            f.submit(ParticipantCommand::Resolve {
                authority: owner,
                proof: forged,
                max_items: 1
            })
            .await,
            ParticipantReply::Rejected(Fault::InvalidBinding)
        );
    }
    quorum.0.store(false, Ordering::Relaxed);
    assert_eq!(
        handle
            .has_resolution_witness(owner, &proof)
            .await
            .unwrap_err()
            .code,
        ErrorCode::OutcomeUnknown
    );
    quorum.0.store(true, Ordering::Relaxed);

    // Model a legacy snapshot with one applied resolution page and no witness.
    let witness_key = txn_key(RESOLUTION_WITNESS, p.txn);
    let position = AppliedPosition {
        term: 1,
        node_id: 1,
        index: f.index,
    };
    f.index += 1;
    f.machine
        .store
        .transaction(
            ApplyMeta {
                position,
                command_digest: Digest::from_bytes([0; 32]),
                membership: None,
            },
            APPLY,
            move |tx| {
                assert!(tx.pinned(&witness_key)?.is_some());
                tx.delete(&witness_key)?;
                commit(
                    tx,
                    &ParticipantReply::Resolved {
                        next_ordinal: 1,
                        complete: false,
                    },
                )
            },
        )
        .await
        .unwrap();
    assert!(!handle.has_resolution_witness(owner, &proof).await.unwrap());
    assert_eq!(
        f.resolve(&p, committed(11), 1).await,
        ParticipantReply::Resolved {
            next_ordinal: 2,
            complete: false
        }
    );
    assert!(handle.has_resolution_witness(owner, &proof).await.unwrap());

    let next = Authority {
        owner_term: owner.owner_term + 1,
        ..owner
    };
    assert!(matches!(
        f.submit(ParticipantCommand::Fence {
            transition_id: nabedra_rt::ids::uuid_v4(),
            expected: Some(owner),
            next,
            schema_epoch: 1,
            routing_epoch: 1,
            catalog_group_id: f.catalog_group_id,
            span: KeySpan {
                start: None,
                end: None
            },
        })
        .await,
        ParticipantReply::Fenced(_)
    ));
    assert!(!handle.has_resolution_witness(owner, &proof).await.unwrap());
    assert!(handle.has_resolution_witness(next, &proof).await.unwrap());
    assert_eq!(
        f.resolve(&p, committed(11), 1).await,
        ParticipantReply::Rejected(Fault::StaleAuthority)
    );
    f.authority = next;
    assert_eq!(
        f.resolve(&p, committed(11), 129).await,
        ParticipantReply::Rejected(Fault::InvalidBinding)
    );
    assert_eq!(
        f.resolve(&p, committed(11), 1).await,
        ParticipantReply::Resolved {
            next_ordinal: 3,
            complete: true
        }
    );
    assert!(!handle.has_resolution_witness(next, &proof).await.unwrap());
    let remaining = f
        .machine
        .store
        .read_metadata(
            TransactionBudget {
                scratch_bytes: 8 << 10,
                batch_bytes: 0,
                output_bytes: 64,
            },
            move |view| {
                let exists = view.pinned(&witness_key)?.is_some();
                view.encode(&exists)
            },
        )
        .await
        .unwrap();
    assert_eq!(remaining.as_slice(), b"false");
    drop(remaining);
    assert_eq!(f.read(11, Key::Int64(3)).await["value"]["version"], 11);
    assert_eq!(f.shared.control_bytes_used(), 0);
}

#[tokio::test]
async fn o08_t06_tombstone_exact_version_and_keep_validate_target_without_new_version() {
    let mut f = Fixture::new().await;
    let first = vec![f.row(1, None, Change::Delete)];
    let p = f.plan(1, &[first.clone()]);
    f.prepare(&p, 0, first).await;
    f.seal(&p).await;
    f.resolve(&p, committed(2), 128).await;
    assert_eq!(
        f.read(2, Key::Int64(1)).await,
        serde_json::json!({"value":{"version":2,"value":null}})
    );
    let stale = vec![f.row(1, None, Change::Keep)];
    let bad = f.plan(2, &[stale.clone()]);
    assert_eq!(
        f.prepare(&bad, 0, stale).await,
        ParticipantReply::Rejected(Fault::Conflict)
    );
    let keep = vec![f.row(1, Some(2), Change::Keep)];
    let p = f.plan(2, &[keep.clone()]);
    assert_eq!(
        f.prepare(&p, 0, keep).await,
        ParticipantReply::PreparedPage {
            next_page: 1,
            writes: 1
        }
    );
    f.seal(&p).await;
    f.resolve(&p, committed(3), 128).await;
    assert_eq!(f.read(3, Key::Int64(1)).await["value"]["version"], 2);
    assert_eq!(f.shared.control_bytes_used(), 0);
}

#[tokio::test]
async fn o07_o15_r05_abort_before_late_prepare_and_partial_prepare_retains_rollback_marker() {
    let mut f = Fixture::new().await;
    let writes = vec![f.row(1, None, Change::Delete)];
    let p = f.plan(1, &[writes.clone()]);
    assert_eq!(
        f.resolve(&p, aborted(), 128).await,
        ParticipantReply::Resolved {
            next_ordinal: 0,
            complete: true
        }
    );
    assert_eq!(
        f.prepare(&p, 0, writes).await,
        ParticipantReply::Rejected(Fault::RolledBack)
    );
    let pages = vec![
        vec![f.row(1, None, Change::Delete)],
        vec![f.row(2, None, Change::Delete)],
    ];
    let partial = f.plan(1, &pages);
    f.prepare(&partial, 0, pages[0].clone()).await;
    assert_eq!(
        f.seal(&partial).await,
        ParticipantReply::Rejected(Fault::Incomplete)
    );
    assert_eq!(
        f.resolve(&partial, aborted(), 128).await,
        ParticipantReply::Resolved {
            next_ordinal: 1,
            complete: true
        }
    );
    assert_eq!(
        f.prepare(&partial, 1, pages[1].clone()).await,
        ParticipantReply::Rejected(Fault::RolledBack)
    );
    assert!(f.read(20, Key::Int64(1)).await.get("value").is_some());
    let old = f.authority;
    f.authority = Authority {
        run_id: nabedra_rt::ids::uuid_v4(),
        owner_term: 2,
    };
    assert!(matches!(
        f.submit(ParticipantCommand::Fence {
            transition_id: nabedra_rt::ids::uuid_v4(),
            expected: Some(old),
            next: f.authority,
            catalog_group_id: f.catalog_group_id,
            schema_epoch: 1,
            routing_epoch: 1,
            span: KeySpan {
                start: None,
                end: None
            }
        })
        .await,
        ParticipantReply::Fenced(_)
    ));
    assert_eq!(
        f.submit(ParticipantCommand::Seal {
            authority: old,
            txn: partial.txn,
            manifest: partial.manifest,
            result_digest: Digest::from_bytes([4; 32])
        })
        .await,
        ParticipantReply::Rejected(Fault::StaleAuthority)
    );
    assert_eq!(f.shared.control_bytes_used(), 0);
}

#[tokio::test]
async fn o07_m01_wide_fixed_prepare_and_resolve_fit_private_apply_without_row_tree_decode() {
    let mut f = Fixture::new().await;
    let key = "k".repeat(7 << 20);
    let change = f.put(serde_json::json!({"id":key}));
    let write = PreparedWrite {
        key: MvccKey::Row {
            table_id: f.table,
            primary_key: Key::Utf8(key.clone()),
        },
        expected_version: None,
        change,
    };
    let p = f.plan(1, &[vec![write.clone()]]);
    assert_eq!(
        f.prepare(&p, 0, vec![write]).await,
        ParticipantReply::PreparedPage {
            next_page: 1,
            writes: 1
        }
    );
    assert!(matches!(f.seal(&p).await, ParticipantReply::Sealed(_)));
    assert_eq!(
        f.resolve(&p, committed(2), 128).await,
        ParticipantReply::Resolved {
            next_ordinal: 1,
            complete: true
        }
    );
    let read = f.read(2, Key::Utf8(key.clone())).await;
    assert_eq!(read["value"]["value"]["id"].as_str(), Some(key.as_str()));
    assert_eq!(f.shared.apply_bytes_used(), 0);
    assert_eq!(f.shared.control_bytes_used(), 0);
}
