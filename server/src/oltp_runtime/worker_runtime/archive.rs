//! Native committed-image backup/PITR. No application operation enters restore.
use super::*;
use crate::{
    oltp::{
        backup::{
            BackupOwnerRef, BackupPublishedStatement, VerifiedBackupPublished,
            history::{GroupImageScope, HistoryFrame, HistoryReader, HistoryScope, HistoryWriter},
            lifecycle::BackupCancelledStatement,
        },
        mvcc::{
            MvccKey,
            catalog::{CatalogRead, NameRecord, OwnedRecord, PinnedCatalog, decode_canonical},
            store::{
                KeySpan, ParticipantHandle,
                image_archive::{ImagePage, ImageRequest},
                index::index_closure_row,
            },
        },
    },
    oltp_backup_repository::{
        ArchiveKey, ArchiveKeys, ArchiveReader, ArchiveRepository, FilesystemRepository,
        UploadSession,
    },
    oltp_replication::{
        authority::status::{
            StatusService, identity::transaction::archive_images::identity_metadata_step,
        },
        owned::{OwnedCommand, serialized_digest},
    },
};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    path::PathBuf,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeBackupManifest {
    pub format_version: u32,
    pub owner: BackupOwnerRef,
    pub pin: StoragePin,
    pub key: ArchiveKey,
    pub repository_root: Digest,
    pub history_root: Digest,
    pub scope: HistoryScope,
    pub frames: u64,
}
impl NativeBackupManifest {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.format_version != 2
            || self.key.archive_id != self.scope.archive_id
            || self.key.cluster_id != self.scope.logical_cluster
            || self.pin.read_ts != self.scope.cut
            || self.owner.backup_id.is_nil()
            || self.owner.attempt == 0
            || publication::native_archive_id(self.key.cluster_id, self.owner)?
                != self.key.archive_id
        {
            return Err(unknown());
        }
        Ok(())
    }
}
mod point_history;
mod restore;
mod restore_graph;
mod split_baseline;
pub(crate) use restore::VerifiedNativeRestore;
mod publication;
pub(crate) use publication::NativeBackupPublication;

fn backup_error(stage: &str, mut error: ProtocolError) -> ProtocolError {
    error.message = format!("native backup {stage}: {}", error.message);
    error
}

struct Export {
    writer: HistoryWriter<File>,
    shared: Arc<crate::oltp::pool::WorkerSharedResources>,
}
impl Export {
    async fn frame(&mut self, frame: HistoryFrame) -> Result<(), ProtocolError> {
        // Bounded serialization is charged before writer growth. File writes
        // are synchronous and short; no native database slot crosses RPCs.
        self.writer.write(&frame, &self.shared)
    }
}
impl WorkerRuntime {
    pub(crate) fn native_backup_manifest(
        path: &Path,
    ) -> Result<NativeBackupManifest, ProtocolError> {
        read_native_locator(path)
    }
    /// The detached owner settles all publication I/O even when its caller is
    /// cancelled. The same owner deterministically reuses the archive identity.
    pub(crate) async fn backup_native(
        self: &Arc<Self>,
        destination: &Path,
        owner: BackupOwnerRef,
        previous: Option<NativeBackupManifest>,
    ) -> Result<NativeBackupManifest, ProtocolError> {
        if owner.backup_id.is_nil() || owner.attempt == 0 {
            return Err(unknown());
        }
        if previous.is_some() {
            self.check_incremental_backup().await?;
        }
        if let Some(repository) = &self.services.archive_repository {
            repository.require_write()?;
            if repository.directory(self.native_archive_id(owner)?) != destination {
                return Err(unknown());
            }
            ensure_private_directory(destination)?;
        }
        let runtime = self.clone();
        let destination = destination.to_path_buf();
        nabedra_rt::task::spawn(async move {
            let gate = runtime.gate(owner.backup_id)?;
            let _gate = gate.lock_owned().await;
            ensure_private_directory(&destination)?;
            let destination = match &runtime.services.archive_repository {
                Some(_) => destination,
                None => std::fs::canonicalize(&destination).map_err(|_| unknown())?,
            };
            let repository = match &runtime.services.archive_repository {
                Some(configured) => configured.repository(&destination)?,
                None => ArchiveRepository::new(
                    FilesystemRepository::open(&destination, runtime.services.shared.clone())?,
                    ArchiveKeys::load(None)?,
                ),
            };
            let key = ArchiveKey {
                cluster_id: runtime.cluster_id(),
                archive_id: runtime.native_archive_id(owner)?,
            };
            let mut active_pin = None;
            let result = runtime
                .clone()
                .backup_native_owned(
                    destination,
                    owner,
                    previous,
                    repository.clone(),
                    key,
                    &mut active_pin,
                )
                .await;
            if result.is_err() {
                runtime
                    .reconcile_native_cancellation(&repository, key, owner, active_pin)
                    .await?;
            }
            result
        })
        .await
        .map_err(|_| unknown())?
    }
    /// Whether direct commits are active, which refuses incremental backups.
    /// The `data.direct.v1` switch: `is_direct_active()`, `direct_oltp()`
    /// and `routes_direct()` for the leader executor (T5) and gateway (T6).
    pub(crate) fn direct_switch(&self) -> Option<&Arc<super::direct_capability::DirectSwitch>> {
        self.direct.get()
    }

    async fn check_incremental_backup(&self) -> Result<(), ProtocolError> {
        // Incremental segments export only Status-decided transactions. The
        // local watcher may lag the durable activation in Control.
        if self.direct_commits_active()
            || self.direct_switch().ok_or_else(unknown)?.refresh().await?
        {
            return Err(crate::oltp::failure(
                ErrorCode::UnsupportedOperation,
                "incremental backups are unavailable while direct commits (data.direct.v1) \
                 are active: take a full backup",
            ));
        }
        Ok(())
    }
    /// Incremental backups exist only while `data.direct.v1` is inactive (a
    /// cluster with an older member). A single test worker activates it on
    /// startup, so tests of incremental export stop the watcher first.
    #[cfg(test)]
    pub(crate) async fn without_direct_for_test(&self) {
        if let Some(direct) = self.direct.get() {
            direct.disable_for_test();
            direct.stop().await;
        }
        self.set_direct_commits_active(false);
    }
    pub(crate) fn direct_commits_active(&self) -> bool {
        self.direct_commits_active
            .load(std::sync::atomic::Ordering::Acquire)
    }
    /// Set by the `data.direct.v1` activation (T7). Off by default.
    pub(crate) fn set_direct_commits_active(&self, active: bool) {
        self.direct_commits_active
            .store(active, std::sync::atomic::Ordering::Release);
    }
    async fn reconcile_native_cancellation(
        &self,
        repository: &ArchiveRepository,
        key: ArchiveKey,
        owner: BackupOwnerRef,
        active_pin: Option<StoragePin>,
    ) -> Result<(), ProtocolError> {
        let proof = match active_pin {
            Some(pin) => publication::settle_cancellation(
                repository,
                key,
                BackupCancelledStatement {
                    owner,
                    pin,
                    archive_id: key.archive_id,
                },
            )
            .await
            .map_err(|_| {
                ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "archive publication or cancellation requires reconciliation",
                    Outcome::Unknown,
                )
            })?,
            None => {
                // A retry after an applied cancellation has no active pin.
                // Replay only the canonical record read at this owner's key.
                let Ok(proof) = publication::read_cancellation(repository, key, owner, None).await
                else {
                    return Ok(());
                };
                proof
            }
        };
        let current = self.services.control.current(&self.services.scope).await?;
        self.services
            .timestamps
            .cancel_backup(current.ready.view.authority(), proof)
            .await
    }
    fn native_archive_id(&self, owner: BackupOwnerRef) -> Result<Uuid, ProtocolError> {
        publication::native_archive_id(self.cluster_id(), owner)
    }
    async fn backup_native_owned(
        self: Arc<Self>,
        destination: PathBuf,
        owner: BackupOwnerRef,
        previous: Option<NativeBackupManifest>,
        repository: ArchiveRepository,
        key: ArchiveKey,
        active_pin: &mut Option<StoragePin>,
    ) -> Result<NativeBackupManifest, ProtocolError> {
        if owner.backup_id.is_nil() || owner.attempt == 0 {
            return Err(unknown());
        }
        let authority = self
            .services
            .control
            .current(&self.services.scope)
            .await?
            .ready
            .view
            .authority();
        write_local_record(
            &destination,
            "attempt.json",
            &serde_json::to_vec(&owner).map_err(|_| unknown())?,
            4096,
        )?;
        let initial_status_group = self.services.status.identity().group_id;
        let status_partitions: Vec<_> = self
            .services
            .remote_status
            .iter()
            .skip(1)
            .map(|status| status.identity().group_id)
            .collect();
        if let Ok(mut reader) = ArchiveReader::open(repository.clone(), key).await {
            let recorded = publication::recorded_manifest(&reader)?;
            if recorded.owner != owner
                || recorded.scope.initial_status_group != initial_status_group
                || recorded.scope.status_partitions != status_partitions
                || recorded.scope.previous_segment != previous.as_ref().map(|old| old.history_root)
            {
                return Err(unknown());
            }
            while reader.next().await?.is_some() {}
            reader.verify_complete()?;
            self.retain_native_archive(authority, &recorded).await?;
            self.release_native_backup(authority, &recorded).await?;
            write_locator(&destination, &recorded)?;
            return Ok(recorded);
        }
        let pin = self
            .services
            .timestamps
            .begin_backup(authority, owner)
            .await
            .map_err(|error| backup_error("pin", error))?;
        *active_pin = Some(pin);
        // Activation may have committed since the first check. A direct
        // timestamp allocated after this cut is above it, so check Control
        // after the cut is fixed and before exporting incremental history.
        if previous.is_some() {
            self.check_incremental_backup().await?;
        }
        let catalog = self.services.catalog.snapshot(pin.read_ts, authority)?;
        let archive_id = key.archive_id;
        let scope = HistoryScope {
            format_version: if status_partitions.is_empty() { 1 } else { 2 },
            archive_id,
            logical_cluster: self.cluster_id(),
            database: self.services.scope.database.clone(),
            control_group: self.services.control.identity().group_id,
            timestamp_group: self.services.timestamps.identity().group_id,
            catalog_group: catalog.catalog.group_id,
            initial_status_group,
            status_partitions,
            base_exclusive: previous.as_ref().map(|old| old.pin.read_ts),
            cut: pin.read_ts,
            maximum_timestamp: pin.read_ts,
            previous_segment: previous.as_ref().map(|old| old.history_root),
        };
        if previous.as_ref().is_some_and(|old| {
            old.owner.backup_id != owner.backup_id
                || old.owner.attempt.checked_add(1) != Some(owner.attempt)
                || old.scope.logical_cluster != scope.logical_cluster
                || old.scope.database != scope.database
                || !old.scope.status_groups().eq(scope.status_groups())
                || old.pin.read_ts >= pin.read_ts
        }) {
            return Err(unknown());
        }
        // A complete root is authoritative after a lost reply. Verify every
        // chunk before granting pin release, even on a publication retry.
        if let Ok(mut reader) = ArchiveReader::open(repository.clone(), key).await {
            let recorded = publication::recorded_manifest(&reader)?;
            if recorded.owner != owner || recorded.pin != pin || recorded.scope != scope {
                return Err(unknown());
            }
            while reader.next().await?.is_some() {}
            reader.verify_complete()?;
            *active_pin = None;
            self.retain_native_archive(authority, &recorded).await?;
            self.release_native_backup(authority, &recorded).await?;
            write_locator(&destination, &recorded)?;
            return Ok(recorded);
        }
        let staging = TemporaryArchiveFile(destination.join(format!(".stage-{}", nabedra_rt::ids::uuid_v4())));
        let file = OpenOptions::new()
            .create_new(true)
            .mode(0o600)
            .read(true)
            .write(true)
            .open(&staging.0)
            .map_err(|_| unknown())?;
        let retain_history = self
            .services
            .archive_repository
            .as_ref()
            .is_none_or(|config| config.retain_local_history());
        if !retain_history {
            std::fs::remove_file(&staging.0).map_err(|_| unknown())?;
        }
        let mut export = Export {
            writer: HistoryWriter::new(identity_metadata_step, file)?,
            shared: self.services.shared.clone(),
        };
        export.frame(HistoryFrame::Scope(scope.clone())).await?;
        if previous.is_none() {
            let epochs = self
                .services
                .catalog
                .participant()
                .activation()
                .await?
                .ok_or_else(unknown)?;
            self.export_native_group(
                &mut export,
                self.services.catalog.participant().clone(),
                GroupImageScope {
                    group: catalog.catalog.clone(),
                    schema_epoch: epochs.schema_epoch,
                    routing_epoch: epochs.routing_epoch,
                    span: KeySpan {
                        start: None,
                        end: None,
                    },
                },
                &catalog,
            )
            .await
            .map_err(|error| backup_error("catalog images", error))?;
            self.export_native_routes(&catalog, &mut export)
                .await
                .map_err(|error| backup_error("data ranges", error))?;
        } else {
            self.export_native_split_baselines(&catalog, &scope, &mut export)
                .await?;
        }
        // Pin the Control inventory and require each configured Status service
        // to contribute its own closure and identity history.
        let control = self.services.control.inspect().await?.ok_or_else(unknown)?;
        let mut exported = std::collections::BTreeSet::new();
        let mut after = None;
        loop {
            let raw = self
                .services
                .control
                .groups(control.group_inventory_revision, after)
                .await
                .map_err(|error| backup_error("control inventory", error))?;
            let page: crate::oltp_replication::authority::control::InventoryPage<
                crate::oltp_replication::authority::control::GroupRecord,
            > = serde_json::from_slice(raw.as_ref()).map_err(|_| unknown())?;
            for group in page
                .items
                .iter()
                .filter(|group| matches!(group.identity.kind, GroupKind::Status { .. }))
            {
                let status: Arc<dyn StatusService> = if self.services.remote_status.is_empty() {
                    self.services.status.clone()
                } else {
                    self.services
                        .remote_status
                        .iter()
                        .find(|client| client.identity() == &group.identity)
                        .ok_or_else(unknown)?
                        .clone()
                };
                if group.identity != *status.identity()
                    || exported.contains(&group.identity.group_id)
                {
                    return Err(unknown());
                }
                self.export_native_status(
                    &mut export,
                    authority,
                    owner,
                    pin,
                    archive_id,
                    scope.base_exclusive,
                    status,
                    exported.is_empty(),
                )
                .await
                .map_err(|error| backup_error("status history", error))?;
                exported.insert(group.identity.group_id);
            }
            if page.done {
                break;
            }
            if page.after == after {
                return Err(unknown());
            }
            after = page.after;
        }
        if !exported.iter().copied().eq(scope
            .status_groups()
            .collect::<std::collections::BTreeSet<_>>())
        {
            return Err(unknown());
        }
        let (mut file, history_root) = export.writer.finish(&self.services.shared)?;
        file.sync_all().map_err(|_| unknown())?;
        file.rewind().map_err(|_| unknown())?;
        let shared = self.services.shared.clone();
        let mut validator = HistoryReader::new(identity_metadata_step, &mut file)?;
        let split_graph = restore_graph::RestoreGraph::new(shared.clone())?;
        let mut split_transcript = restore_graph::SplitTranscript::default();
        let mut point_transcript = point_history::PointTranscript::default();
        while let Some(frame) = validator
            .next(&shared)
            .map_err(|error| backup_error("history frame", error))?
        {
            split_transcript.observe(&split_graph, &frame, scope.cut)?;
            point_transcript.observe(frame.payload()?, &shared)?;
        }
        point_transcript.finish()?;
        split_graph.verify_split_inventory(false)?;
        let verified = validator.verified()?;
        if retain_history {
            std::fs::rename(
                &staging.0,
                destination.join(format!(".history-{archive_id}.partial")),
            )
            .map_err(|_| unknown())?;
        }
        let mut manifest = NativeBackupManifest {
            format_version: 2,
            owner,
            pin,
            key,
            repository_root: Digest::from_bytes([0; 32]),
            history_root,
            scope,
            frames: verified.frames(),
        };
        let mut upload = UploadSession::new(repository, key, 1 << 20)?;
        file.rewind().map_err(|_| unknown())?;
        loop {
            let mut bytes = shared.allocate_bytes(ByteClass::Data, 1 << 20)?;
            let mut used = 0;
            while used < bytes.as_mut_slice().len() {
                let n = file
                    .read(&mut bytes.as_mut_slice()[used..])
                    .map_err(|_| unknown())?;
                if n == 0 {
                    break;
                }
                used += n;
            }
            if used == 0 {
                break;
            }
            let body = if used == bytes.as_mut_slice().len() {
                bytes.freeze()
            } else {
                shared.copy_bytes(&bytes.as_mut_slice()[..used])?
            };
            upload.append(body).await?;
        }
        let history = shared.encode_in(ByteClass::Data, &manifest, 64 << 10)?;
        let published = upload
            .publish(history, manifest.scope.previous_segment)
            .await
            .map_err(|error| backup_error("repository publication", error))?;
        *active_pin = None;
        manifest.repository_root = published.root_sha256();
        // Continuous retention is established before the completed base pin is
        // released. The retention owner has a fixed identity across retries.
        self.retain_native_archive(authority, &manifest).await?;
        self.release_native_backup(authority, &manifest).await?;
        write_locator(&destination, &manifest)?;
        Ok(manifest)
    }
    async fn retain_native_archive(
        &self,
        authority: Authority,
        manifest: &NativeBackupManifest,
    ) -> Result<(), ProtocolError> {
        self.services
            .timestamps
            .retain_archive(
                authority,
                VerifiedBackupPublished::published(BackupPublishedStatement {
                    owner: manifest.owner,
                    pin: manifest.pin,
                    archive_id: manifest.key.archive_id,
                    manifest_sha256: manifest.repository_root,
                    catalog_root: manifest.history_root,
                })?,
                manifest.scope.previous_segment,
            )
            .await
    }
    async fn release_native_backup(
        &self,
        authority: Authority,
        manifest: &NativeBackupManifest,
    ) -> Result<(), ProtocolError> {
        self.services
            .timestamps
            .release_backup(
                authority,
                VerifiedBackupPublished::published(BackupPublishedStatement {
                    owner: manifest.owner,
                    pin: manifest.pin,
                    archive_id: manifest.key.archive_id,
                    manifest_sha256: manifest.repository_root,
                    catalog_root: manifest.history_root,
                })?,
            )
            .await
    }
    async fn export_native_group(
        &self,
        export: &mut Export,
        participant: Arc<ParticipantHandle>,
        scope: GroupImageScope,
        pin: &PinnedCatalog,
    ) -> Result<(), ProtocolError> {
        self.export_native_image_stream(export, participant, scope, pin.read_ts, Some(pin))
            .await
    }

    async fn export_native_image_stream(
        &self,
        export: &mut Export,
        participant: Arc<ParticipantHandle>,
        scope: GroupImageScope,
        cut: CommitTs,
        catalog_cut: Option<&PinnedCatalog>,
    ) -> Result<(), ProtocolError> {
        let mut root = if catalog_cut.is_some() {
            export
                .frame(HistoryFrame::BeginGroup(scope.clone()))
                .await?;
            Some(serialized_digest(&(b"nabedra/history-group/v1", &scope))?)
        } else {
            None
        };
        // rv-oltp F1: a backup's images follow the search columns of the
        // Catalog at its cut, which its restore fences with, not the range's
        // current ones (a column operation may fence the range after the cut).
        let search = match (catalog_cut, &scope.group.kind) {
            (Some(pin), GroupKind::Data { table_id, .. }) => self
                .native_catalog::<crate::oltp::mvcc::catalog::TableRecord>(
                    pin,
                    MvccKey::CatalogTable { table_id: *table_id },
                )
                .await?
                .and_then(|table| table.search.as_ref().map(|search| Box::new(search.schema()))),
            _ => None,
        };
        let mut index = None;
        let mut count = 0u64;
        let mut searched = false;
        let mut after = None;
        let mut after_owner = None;
        let mut pending_deadline = None;
        let mut pending_backoff = std::time::Duration::from_millis(25);
        loop {
            let _cursor_owner = &after_owner;
            let command = OwnedCommand::new_owned(
                ImageRequest {
                    read_ts: cut,
                    schema_epoch: scope.schema_epoch,
                    routing_epoch: scope.routing_epoch,
                    span: scope.span.clone(),
                    after: after.clone(),
                    max_items: 128,
                    max_bytes: crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES,
                    search: search.clone(),
                },
                &self.services.shared,
                ByteClass::Data,
            )?;
            let page = match participant.committed_image_page(command).await {
                Ok(page) => {
                    pending_deadline = None;
                    pending_backoff = std::time::Duration::from_millis(25);
                    page
                }
                Err(error) if error.code == ErrorCode::SplitPublicationPending => {
                    // A base cut between copy and publication still owns the
                    // old route. Retry the same cut once the source settles.
                    let now = nabedra_rt::time::now();
                    let deadline = *pending_deadline
                        .get_or_insert_with(|| now + std::time::Duration::from_secs(30));
                    if now >= deadline {
                        return Err(error);
                    }
                    nabedra_rt::time::sleep_until((now + pending_backoff).min(deadline)).await;
                    pending_backoff =
                        (pending_backoff * 2).min(std::time::Duration::from_millis(250));
                    continue;
                }
                Err(error) => return Err(error),
            };
            match page.payload()? {
                ImagePage::Blocked { txn, manifest } => {
                    self.help_intent(IntentRef {
                        txn: *txn,
                        manifest: *manifest,
                    })
                    .await?
                }
                ImagePage::Images {
                    group,
                    read_ts,
                    images,
                    after: _,
                    exhausted,
                    search,
                } => {
                    if group != &scope.group || *read_ts != cut {
                        return Err(unknown());
                    }
                    // The restore checks every vector against these columns (L7).
                    if let Some(search) = search.as_ref().filter(|_| !searched) {
                        let schema = Box::new(search.schema.clone());
                        let frame = if root.is_some() {
                            HistoryFrame::GroupSearch(schema)
                        } else {
                            HistoryFrame::SplitSearch(schema)
                        };
                        export.frame(frame).await?;
                    }
                    searched = true;
                    for image in images.iter() {
                        if let (
                            Some(pin),
                            MvccKey::IndexEntry {
                                table_id, index_id, ..
                            },
                        ) = (catalog_cut, &image.key)
                        {
                            // Index images sort by UUID. At a full backup cut
                            // after retirement, their presence in Data can
                            // change while pages are being exported.
                            if index.is_none_or(|(prior, _)| prior != *index_id) {
                                use crate::oltp::mvcc::index::{IndexPhase, IndexRecord};
                                let retired = self
                                    .native_catalog::<IndexRecord>(
                                        pin,
                                        MvccKey::CatalogIndex {
                                            table_id: *table_id,
                                            index_id: *index_id,
                                        },
                                    )
                                    .await?
                                    .is_some_and(|record| {
                                        matches!(record.phase, IndexPhase::Retired { .. })
                                    });
                                index = Some((*index_id, retired));
                            }
                            if index.is_some_and(|(_, retired)| retired) {
                                continue;
                            }
                        }
                        if let Some(prior) = root {
                            root = Some(serialized_digest(&(
                                b"nabedra/history-image/v1",
                                prior,
                                image,
                            ))?);
                            count = count.checked_add(1).ok_or_else(unknown)?;
                            export.frame(HistoryFrame::Image(image.clone())).await?;
                            Self::export_native_index_closure(
                                export,
                                &participant,
                                &image.key,
                                image.value.as_ref(),
                            )
                            .await?;
                            self.export_native_split_publication(export, &participant, image, cut)
                                .await?;
                        } else {
                            export
                                .frame(HistoryFrame::SplitImage(image.clone()))
                                .await?;
                        }
                    }
                    if *exhausted {
                        break;
                    }
                    let continuation = page.payload()?.continuation()?.ok_or_else(unknown)?;
                    if Some(continuation) == after.as_ref() {
                        return Err(unknown());
                    }
                    let next_owner = self.services.shared.reserve_scratch(
                        ByteClass::Data,
                        continuation
                            .heap_bytes()
                            .checked_add(1024)
                            .ok_or_else(unknown)?,
                    )?;
                    after = Some(continuation.clone());
                    after_owner = Some(next_owner);
                }
            }
        }
        match root {
            Some(root) => {
                export
                    .frame(HistoryFrame::EndGroup {
                        group_id: scope.group.group_id,
                        image_count: count,
                        root,
                    })
                    .await
            }
            None => {
                export
                    .frame(HistoryFrame::EndSplitChild {
                        group_id: scope.group.group_id,
                    })
                    .await
            }
        }
    }
    async fn export_native_index_closure(
        export: &mut Export,
        catalog: &ParticipantHandle,
        key: &MvccKey,
        value: Option<&crate::oltp::mvcc::CanonicalJson>,
    ) -> Result<(), ProtocolError> {
        if let Some(row) = index_closure_row(key, value)? {
            let proof = catalog.strong_index_attempt_closed(row.owner).await?;
            export
                .frame(HistoryFrame::CatalogIndexClosure(
                    proof.archive_evidence().clone(),
                ))
                .await?;
        }
        Ok(())
    }
    async fn native_catalog<T: serde::de::DeserializeOwned + Send + Sync + 'static>(
        &self,
        pin: &PinnedCatalog,
        key: MvccKey,
    ) -> Result<Option<OwnedRecord<T>>, ProtocolError> {
        loop {
            match self.services.catalog.read(pin, key.clone()).await? {
                CatalogRead::Value { record, .. } => return Ok(record),
                CatalogRead::Blocked(intent) => self.help_intent(intent).await?,
            }
        }
    }
    async fn export_native_routes(
        &self,
        pin: &PinnedCatalog,
        export: &mut Export,
    ) -> Result<(), ProtocolError> {
        use crate::oltp::mvcc::store::scan::{ScanDomain, ScanPage, ScanRequest};
        let mut after = None;
        let mut after_owner = None;
        loop {
            let page = self
                .services
                .catalog
                .participant()
                .scan_page(ScanRequest {
                    domain: ScanDomain::CatalogNames,
                    read_ts: pin.read_ts,
                    after: after.clone(),
                    after_owner: after_owner.clone(),
                    lower: None,
                    upper: None,
                    max_items: 64,
                    max_bytes: 128 << 10,
                })
                .await?;
            match page.payload()? {
                ScanPage::Blocked { txn, manifest } => {
                    self.help_intent(IntentRef {
                        txn: *txn,
                        manifest: *manifest,
                    })
                    .await?;
                }
                ScanPage::Records { records, exhausted } => {
                    for record in records.iter() {
                        let Some(value) = &record.value else {
                            continue;
                        };
                        let name: OwnedRecord<NameRecord> =
                            decode_canonical(&self.services.shared, value.clone()).await?;
                        let root = self
                            .native_catalog::<crate::oltp::mvcc::catalog::RoutingRoot>(
                                pin,
                                MvccKey::CatalogRoutingRoot {
                                    table_id: name.table_id,
                                },
                            )
                            .await?
                            .ok_or_else(unknown)?;
                        let mut next = root.first_page;
                        while let Some(page_id) = next {
                            let routes = self
                                .native_catalog::<crate::oltp::mvcc::catalog::RoutingPage>(
                                    pin,
                                    MvccKey::CatalogRoutingPage {
                                        table_id: name.table_id,
                                        page_id,
                                    },
                                )
                                .await?
                                .ok_or_else(unknown)?;
                            for route in routes.shards.iter() {
                                let participant =
                                    self.services.resolver.participant(&route.group).await?;
                                self.export_native_group(
                                    export,
                                    participant,
                                    GroupImageScope {
                                        group: route.group.clone(),
                                        schema_epoch: route.schema_epoch,
                                        routing_epoch: route.routing_epoch,
                                        span: route.span.clone(),
                                    },
                                    pin,
                                )
                                .await?;
                            }
                            next = routes.next_page;
                        }
                    }
                    if *exhausted {
                        break;
                    }
                    after = Some(records.last().ok_or_else(unknown)?.key.clone());
                    after_owner = Some(page.decode_owner()?);
                }
            }
        }
        Ok(())
    }
    async fn export_native_status(
        &self,
        export: &mut Export,
        authority: Authority,
        owner: BackupOwnerRef,
        pin: StoragePin,
        archive_id: Uuid,
        base: Option<CommitTs>,
        status: Arc<dyn StatusService>,
        include_timestamps: bool,
    ) -> Result<(), ProtocolError> {
        let closure = self
            .services
            .timestamps
            .archive_closure(authority, owner, pin, archive_id, base, status.clone())
            .await
            .map_err(|error| backup_error("timestamp closure", error))?;
        export
            .frame(HistoryFrame::StatusClosure(closure.statement().clone()))
            .await?;
        let mut after = None;
        loop {
            let page = status
                .identity_history_page(&closure, after)
                .await
                .map_err(|error| backup_error("identity page", error))?;
            for identity in page.items.iter() {
                export
                    .frame(HistoryFrame::Identity {
                        status: status.identity().clone(),
                        applied_at: closure.status_applied_at(),
                        identity: identity.clone(),
                    })
                    .await?;
                if identity.point_projection.is_some() {
                    let _owner = self
                        .services
                        .shared
                        .reserve_scratch(ByteClass::Data, 32 << 10)?;
                    let image = point_history::verify_identity(closure.statement(), identity)?
                        .ok_or_else(unknown)?;
                    export.frame(HistoryFrame::Metadata(image)).await?;
                }
            }
            if page.exhausted {
                break;
            }
            if page.after == after {
                return Err(unknown());
            }
            after = page.after;
        }
        let _metadata_cursor_owner = self
            .services
            .shared
            .reserve_scratch(ByteClass::Data, 16 << 10)?;
        if include_timestamps {
            let mut cursor = None;
            loop {
                let page = self
                    .services
                    .timestamps
                    .native_archive_page(&closure, cursor.clone())
                    .await
                    .map_err(|error| backup_error("timestamp metadata page", error))?;
                for image in page.items.iter() {
                    export.frame(HistoryFrame::Metadata(image.clone())).await?;
                }
                if page.exhausted {
                    break;
                }
                if page.after == cursor {
                    return Err(unknown());
                }
                cursor = page.after;
            }
        }
        let mut metadata_after = None;
        loop {
            let page = status
                .native_archive_page(&closure, metadata_after.clone())
                .await
                .map_err(|error| backup_error("status metadata page", error))?;
            for image in page.items.iter() {
                export.frame(HistoryFrame::Metadata(image.clone())).await?;
            }
            if page.exhausted {
                break;
            }
            if page.after == metadata_after {
                return Err(unknown());
            }
            metadata_after = page.after;
        }
        let mut after = None;
        loop {
            let page = status
                .committed_page(&closure, base, after)
                .await
                .map_err(|error| backup_error("committed decisions page", error))?;
            for decision in page.items.iter() {
                let txn = decision.statement.txn;
                let manifest = decision.statement.manifest;
                let proof = status
                    .lookup_decision(txn, manifest)
                    .await?
                    .ok_or_else(unknown)?;
                export
                    .frame(HistoryFrame::BeginTransaction(decision.clone()))
                    .await?;
                let mut cursor = None;
                let mut participants = 0u64;
                loop {
                    let plans = status
                        .manifest_page(txn, manifest, cursor)
                        .await
                        .map_err(|error| backup_error("participant manifest page", error))?;
                    for plan in plans.items.iter() {
                        if base.is_none() {
                            export
                                .frame(HistoryFrame::ParticipantManifest(plan.clone()))
                                .await?;
                            participants = participants.checked_add(1).ok_or_else(unknown)?;
                            continue;
                        }
                        let participant = self.services.resolver.participant(&plan.group).await?;
                        let mut ordinal = 0;
                        loop {
                            let effects = participant
                                .archive_effect_page(
                                    proof.clone(),
                                    ordinal,
                                    128,
                                    crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES,
                                )
                                .await?;
                            let effect = effects.payload()?;
                            if effect.plan != *plan {
                                return Err(unknown());
                            }
                            export
                                .frame(HistoryFrame::ParticipantEffects(effect.clone()))
                                .await?;
                            for row in effect.effects.iter() {
                                Self::export_native_index_closure(
                                    export,
                                    &participant,
                                    &row.key,
                                    row.value.as_ref(),
                                )
                                .await?;
                            }
                            if effect.complete {
                                break;
                            }
                            if effect.next_ordinal <= ordinal {
                                return Err(unknown());
                            }
                            ordinal = effect.next_ordinal;
                        }
                        participants = participants.checked_add(1).ok_or_else(unknown)?;
                    }
                    if plans.done {
                        break;
                    }
                    if plans.after == cursor {
                        return Err(unknown());
                    }
                    cursor = plans.after;
                }
                export
                    .frame(HistoryFrame::EndTransaction {
                        txn,
                        participants,
                        manifest,
                    })
                    .await?;
            }
            if page.exhausted {
                break;
            }
            if page.after == after {
                return Err(unknown());
            }
            after = page.after;
        }
        Ok(())
    }
}
fn ensure_private_directory(path: &Path) -> Result<(), ProtocolError> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(_) => Err(unknown()),
    }
}
struct TemporaryArchiveFile(PathBuf);
impl Drop for TemporaryArchiveFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn write_local_record(
    directory: &Path,
    name: &str,
    bytes: &[u8],
    limit: usize,
) -> Result<(), ProtocolError> {
    let path = directory.join(name);
    let temporary = TemporaryArchiveFile(directory.join(format!(".record-{}", nabedra_rt::ids::uuid_v4())));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary.0)
        .map_err(|_| unknown())?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| unknown())?;
    match std::fs::hard_link(&temporary.0, &path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if read_local_record(&path, limit)? != bytes {
                return Err(unknown());
            }
        }
        Err(_) => return Err(unknown()),
    }
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|_| unknown())
}

fn write_locator(path: &Path, manifest: &NativeBackupManifest) -> Result<(), ProtocolError> {
    let bytes = serde_json::to_vec(manifest).map_err(|_| unknown())?;
    write_local_record(path, "manifest.json", &bytes, 64 << 10)
}

#[derive(Serialize)]
pub(crate) struct NativeRestoreSummary {
    pub source_cluster: Uuid,
    pub target_cluster: Uuid,
    pub target_cut: CommitTs,
    pub segments: u64,
    pub images: u64,
    pub identities: u64,
    pub history_root: Digest,
}
impl WorkerRuntime {
    pub(crate) async fn restore_native(
        backups: &[PathBuf],
        target_cut: Option<CommitTs>,
        destination: &Path,
        database: &str,
        token: &str,
        execution: ExecutionConfig,
        resources: WorkerResources,
    ) -> Result<NativeRestoreSummary, ProtocolError> {
        use crate::oltp::{
            backup::restore::OfflineRestoreTarget,
            pool::{OltpPool, SharedResourceConfig, WorkerSharedResources},
        };
        let shared = WorkerSharedResources::new(
            1,
            resources,
            SharedResourceConfig {
                query_threads: resources.max_threads as usize,
                ..Default::default()
            },
        )?;
        let verified = VerifiedNativeRestore::verify(
            backups,
            target_cut,
            database,
            None,
            shared.clone(),
            None,
            ArchiveKeys::load(None)?,
        )
        .await?;
        let target = OfflineRestoreTarget::claim(destination, &verified.plan)?;
        crate::oltp_replication::manager::LocalWorker::prepare_restore(&target)?;
        let pool = OltpPool::open_development(
            &target.stage().join("pool"),
            target.statement().target_pool.clone(),
            shared.clone(),
        )?;
        let (genesis, summary) = verified.install(target, &pool).await?;
        let statement = genesis.statement();
        drop(pool);
        shared.drain().await;
        drop(shared);
        // The standard target bootstrap establishes real new-run fences and
        // starts with an empty RAM queue. No HTTP listener exists during this phase.
        let runtime = Self::open(genesis.stage(), database, token, execution, resources).await?;
        let current = runtime
            .services
            .control
            .current(&runtime.services.scope)
            .await?;
        let authority = current.ready.view.authority();
        if runtime.cluster_id() != statement.target_pool.cluster_id
            || runtime.services.timestamps.current_authority().await? != Some(authority)
            || runtime.services.status.current_authority().await? != Some(authority)
            || runtime
                .services
                .catalog
                .participant()
                .epochs()
                .await?
                .authority
                != authority
        {
            return Err(unknown());
        }
        if runtime
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .stats()
            .accepted_records
            != 0
        {
            return Err(unknown());
        }
        let probe_owner = BackupOwnerRef {
            backup_id: nabedra_rt::ids::uuid_v4(),
            attempt: 1,
        };
        let probe = runtime
            .services
            .timestamps
            .begin_backup(authority, probe_owner)
            .await?;
        if probe.read_ts.get() <= statement.maximum_timestamp.get() {
            return Err(unknown());
        }
        runtime
            .services
            .timestamps
            .cancel_backup(
                authority,
                crate::oltp::backup::lifecycle::VerifiedBackupCancelled::after_settlement(
                    crate::oltp::backup::lifecycle::BackupCancelledStatement {
                        owner: probe_owner,
                        pin: probe,
                        archive_id: nabedra_rt::ids::uuid_v4(),
                    },
                )?,
            )
            .await?;
        runtime.stop_accepting();
        runtime.drain().await?;
        drop(runtime);
        genesis.publish()?;
        Ok(summary)
    }
}
fn map_native_group(source: &GroupIdentity, target_cluster: Uuid) -> GroupIdentity {
    let mut target = source.clone();
    target.cluster_id = target_cluster;
    target
}
fn read_local_record(path: &Path, limit: usize) -> Result<Vec<u8>, ProtocolError> {
    use std::os::unix::fs::MetadataExt;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| unknown())?;
    let metadata = file.metadata().map_err(|_| unknown())?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(unknown());
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| unknown())?;
    if bytes.len() > limit {
        return Err(unknown());
    }
    Ok(bytes)
}
pub(crate) fn read_native_locator(path: &Path) -> Result<NativeBackupManifest, ProtocolError> {
    let bytes = read_local_record(&path.join("manifest.json"), 64 << 10)?;
    let manifest: NativeBackupManifest = serde_json::from_slice(&bytes).map_err(|_| unknown())?;
    manifest.validate()?;
    Ok(manifest)
}

#[cfg(test)]
mod tests;

async fn install_native_maintenance(
    pool: &crate::oltp::pool::OltpPool,
    target: Arc<crate::oltp::backup::restore::OfflineRestoreTarget>,
    source: GroupIdentity,
    key: &MvccKey,
    value: &crate::oltp::mvcc::CanonicalJson,
) -> Result<(), ProtocolError> {
    if !matches!(key, MvccKey::CatalogIndex { .. }) {
        return Ok(());
    }
    let statement = target.statement();
    let group = GroupIdentity {
        cluster_id: statement.target_pool.cluster_id,
        database: statement.database.clone(),
        group_id: statement.timestamp_group,
        kind: GroupKind::Timestamps,
        group_format_version: GROUP_FORMAT_VERSION,
    };
    let store = pool.group(group).await?;
    crate::oltp_replication::authority::timestamp::archive::metadata::install_index_maintenance(
        &store, target, source, key, value,
    )
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveCreate {
    output: PathBuf,
    owner: BackupOwnerRef,
    previous: Option<PathBuf>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveStatus {
    output: PathBuf,
}
impl WorkerRuntime {
    pub(in crate::oltp_runtime) fn validate_archive_admin(
        operation: &str,
        payload: &str,
    ) -> Result<bool, ProtocolError> {
        fn path(path: &Path) -> bool {
            path.is_absolute() && path.as_os_str().len() <= 4096
        }
        match operation {
            "archive.create" => {
                let request: ArchiveCreate =
                    serde_json::from_str(payload).map_err(|_| unknown())?;
                if !path(&request.output)
                    || request.owner.backup_id.is_nil()
                    || request.owner.attempt == 0
                    || request
                        .previous
                        .as_ref()
                        .is_some_and(|previous| !path(previous))
                {
                    return Err(unknown());
                }
                Ok(true)
            }
            "archive.status" => {
                let request: ArchiveStatus =
                    serde_json::from_str(payload).map_err(|_| unknown())?;
                if !path(&request.output) {
                    return Err(unknown());
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }
    pub(in crate::oltp_runtime) async fn archive_admin(
        self: Arc<Self>,
        request_id: Uuid,
        operation: &str,
        payload: &str,
    ) -> Result<response::EncodedResponse, ProtocolError> {
        let current = self.services.control.current(&self.services.scope).await?;
        if current.owner != self.services.local {
            return Err(unavailable());
        }
        if !self.services.remote_status.is_empty() && self.services.archive_repository.is_none() {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "registered archive operations require an archive repository",
                Outcome::NotApplied,
            ));
        }
        let manifest = match operation {
            "archive.create" => {
                let request: ArchiveCreate =
                    serde_json::from_str(payload).map_err(|_| unknown())?;
                let previous = match request.previous {
                    Some(path) => Some(self.archive_locator(&path).await?),
                    None => None,
                };
                self.backup_native(&request.output, request.owner, previous)
                    .await?
            }
            "archive.status" => {
                let request: ArchiveStatus =
                    serde_json::from_str(payload).map_err(|_| unknown())?;
                self.archive_locator(&request.output).await?
            }
            _ => return Err(unknown()),
        };
        Ok(response::encode_success(
            request_id,
            &manifest,
            operation == "archive.create",
        ))
    }

    async fn archive_locator(&self, path: &Path) -> Result<NativeBackupManifest, ProtocolError> {
        let configured = self.services.archive_repository.as_ref();
        if configured.is_some_and(|repository| !repository.contains(path)) {
            return Err(unknown());
        }
        let locator = read_native_locator(path)?;
        if let Some(repository) = configured {
            repository.verify_locator(path, &locator).await?;
        }
        Ok(locator)
    }
}
