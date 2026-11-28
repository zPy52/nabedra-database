//! Replicated child images remain unpublished until their complete source
//! traversal is durable. The source participant and old Catalog routes stay
//! readable for transactions pinned before the split publication.
pub(crate) use super::image_archive::CommittedImage as SplitImage;
use super::*;
use super::{
    image_archive::{ImagePage, ImageRequest},
    lifecycle::{GuardMode, SealedLifecycleGuard},
};
use crate::oltp::pool::{ByteClass, ExecutionClass, ScratchLease, WorkerSharedResources};
use nabedra_oltp_types::authority::SealedSettlement;

/// Authenticated response for one phase of a split.
pub trait SplitReplyProof {
    fn verify(
        self,
        source: bool,
        group: &GroupIdentity,
        value: &impl Serialize,
    ) -> Result<Arc<ScratchLease>, ProtocolError>;
}

const STATE: &[u8] = b"mvcc/split-import/v1";
const CURSOR: &[u8] = b"mvcc/split-import-cursor/v1";
const RECEIPTS: &[u8] = b"mvcc/split-pages/v1/";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitSourceStatement {
    pub group: GroupIdentity,
    pub split_id: Uuid,
    pub txn: TxnId,
    pub request: RequestBinding,
    pub read_ts: ReadTs,
    pub children: [Digest; 2],
    pub frozen_at: AppliedPosition,
}

/// Only a native source Raft read or its authenticated reply creates this proof.
pub struct VerifiedSplitSource {
    proof: OwnedCommand<SplitSourceState>,
}
impl VerifiedSplitSource {
    pub fn from_authenticated(
        context: impl SplitReplyProof,
        value: (SplitSourceStatement, Option<CommitTs>),
        children: &[SplitBinding; 2],
    ) -> Result<Self, ProtocolError> {
        let [left, right] = children;
        let (statement, published_at) = &value;
        let guard = left.guard.binding();
        if statement.group != left.source || statement.split_id != left.split_id
            || statement.txn != guard.txn || statement.request != guard.request
            || statement.read_ts != left.read_ts || statement.frozen_at.index == 0
            || statement.children != [child_digest(left)?, child_digest(right)?]
            || published_at.is_some_and(|timestamp| timestamp <= statement.read_ts)
        {
            return Err(invalid());
        }
        let owner = context.verify(true, &left.source, &value)?;
        let (statement, published_at) = value;
        Ok(Self { proof: OwnedCommand::new_with_owner(SplitSourceState { statement, published_at }, owner)? })
    }
    fn state(&self) -> &SplitSourceState {
        self.proof
            .payload()
            .expect("verified native split source remains owned")
    }
    pub fn statement(&self) -> &SplitSourceStatement {
        &self.state().statement
    }
    pub fn published_at(&self) -> Option<CommitTs> {
        self.state().published_at
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SplitSourceState {
    pub(super) statement: SplitSourceStatement,
    pub(super) published_at: Option<CommitTs>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SplitChildVisibility {
    pub(super) read_ts: ReadTs,
    pub(super) complete: bool,
    /// The source's publication timestamp, once the coordinator sent it
    /// (`ChildPublished`). Direct commits stay above it (RV-T4 F8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) published_at: Option<CommitTs>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitBinding {
    pub split_id: Uuid,
    pub source: GroupIdentity,
    pub source_schema_epoch: u64,
    pub source_routing_epoch: u64,
    pub source_span: KeySpan,
    pub target: GroupIdentity,
    pub target_span: KeySpan,
    pub read_ts: ReadTs,
    pub guard: SealedLifecycleGuard,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitPage {
    pub binding: SplitBinding,
    pub page_index: u64,
    pub previous_root: Digest,
    pub previous_after: Option<Digest>,
    pub images: Page<SplitImage>,
    /// The last image owns the usual continuation. An explicit key permits
    /// advancing past a scanned head without a version at this cut.
    pub after: Option<MvccKey>,
    pub exhausted: bool,
    /// The source's search columns and closed floor, if it has any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<Box<search::SplitSearch>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportState {
    binding: Digest,
    pages: u64,
    images: u64,
    source_images: u64,
    root: Digest,
    #[serde(default)]
    image_root: Option<Digest>,
    after: Option<Digest>,
    complete: bool,
    completed_at: Option<AppliedPosition>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitImportProgress {
    pub binding: Digest,
    pub pages: u64,
    pub root: Digest,
    pub after_digest: Option<Digest>,
    pub after: Option<MvccKey>,
    pub complete: bool,
}
impl OwnedPayload for SplitImportProgress {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(self.after.as_ref().map_or(0, MvccKey::heap_bytes))
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    command: Digest,
    pages: u64,
    images: u64,
    source_images: u64,
    root: Digest,
    complete: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitImportedStatement {
    pub binding: SplitBinding,
    pub pages: u64,
    pub images: u64,
    pub source_images: u64,
    pub source_root: Digest,
    pub image_root: Digest,
    pub completed_at: AppliedPosition,
}
/// Constructed by a strong participant read or its authenticated RPC reply.
pub struct VerifiedSplitImported {
    statement: OwnedCommand<SplitImportedStatement>,
}
impl VerifiedSplitImported {
    pub fn from_authenticated(
        context: impl SplitReplyProof,
        statement: SplitImportedStatement,
        binding: &SplitBinding,
    ) -> Result<Self, ProtocolError> {
        if statement.binding != *binding || statement.completed_at.index == 0 || statement.pages == 0
            || statement.images > statement.source_images
        {
            return Err(invalid());
        }
        let owner = context.verify(false, &binding.target, &statement)?;
        Ok(Self { statement: OwnedCommand::new_with_owner(statement, owner)? })
    }
    pub fn statement(&self) -> &SplitImportedStatement {
        self.statement
            .payload()
            .expect("verified native split statement remains owned")
    }
}
impl ParticipantHandle {
    pub async fn strong_split_progress_local(
        &self,
        binding: &SplitBinding,
        cursor_limit: usize,
    ) -> Result<OwnedCommand<SplitImportProgress>, ProtocolError> {
        if self.remote.is_some() {
            return Err(invalid());
        }
        self.local_consensus().ensure_linearizable().await?;
        self.local_machine()
            .split_progress(binding, cursor_limit)
            .await
    }
}
impl ParticipantMachine {
    pub(super) async fn split_progress(
        &self,
        binding: &SplitBinding,
        cursor_limit: usize,
    ) -> Result<OwnedCommand<SplitImportProgress>, ProtocolError> {
        if binding.target != *self.store.identity()
            || cursor_limit > super::super::MAX_FIXED_COMMAND_BYTES
        {
            return Err(invalid());
        }
        let digest = child_digest(binding)?;
        let owner = Arc::new(
            self.store.resources().reserve_scratch(
                ByteClass::Data,
                cursor_limit
                    .checked_mul(4)
                    .and_then(|bytes| bytes.checked_add(16 << 10))
                    .ok_or_else(invalid)?,
            )?,
        );
        self.store
            .read_owned(
                TransactionBudget {
                    scratch_bytes: 16 << 10,
                    batch_bytes: 0,
                    output_bytes: 0,
                },
                owner,
                move |view, owner| {
                    let raw = view.pinned(STATE)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let state: ImportState =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    if state.binding != digest {
                        return Err(invalid());
                    }
                    let cursor = view.pinned(CURSOR)?;
                    if cursor.as_ref().is_some_and(|raw| raw.len() > cursor_limit) {
                        return Err(failure(
                            ErrorCode::LimitExceeded,
                            "split cursor exceeds read budget",
                        ));
                    }
                    if cursor.as_deref().map(cursor_digest) != state.after {
                        return Err(recovery_unknown());
                    }
                    let after = cursor.as_deref().map(super::scan::decode_key).transpose()?;
                    owner.shrink_unused(
                        after
                            .as_ref()
                            .map_or(0, MvccKey::heap_bytes)
                            .checked_add(16 << 10)
                            .ok_or_else(invalid)?,
                    )?;
                    OwnedCommand::new_with_owner(
                        SplitImportProgress {
                            binding: state.binding,
                            pages: state.pages,
                            root: state.root,
                            after_digest: state.after,
                            after,
                            complete: state.complete,
                        },
                        owner,
                    )
                },
            )
            .await
    }
}

impl ParticipantHandle {
    pub async fn strong_split_source_local(
        &self,
        split_id: Uuid,
    ) -> Result<VerifiedSplitSource, ProtocolError> {
        if self.remote.is_some() || split_id.is_nil() {
            return Err(invalid());
        }
        self.local_consensus().ensure_linearizable().await?;
        let owner = Arc::new(
            self.resources()
                .reserve_scratch_wait(crate::oltp::pool::ByteClass::Progress, 16 << 10, 0)
                .await?,
        );
        let proof = self
            .local_machine()
            .store()
            .read_owned(
                TransactionBudget {
                    scratch_bytes: 16 << 10,
                    batch_bytes: 0,
                    output_bytes: 0,
                },
                owner,
                move |view, owner| {
                    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let config: Config =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    let source = config.split_source.ok_or_else(invalid)?;
                    if source.statement.split_id != split_id {
                        return Err(invalid());
                    }
                    OwnedCommand::new_with_owner(source, owner)
                },
            )
            .await?;
        Ok(VerifiedSplitSource { proof })
    }

    pub async fn strong_split_imported_local(
        &self,
        binding: SplitBinding,
    ) -> Result<VerifiedSplitImported, ProtocolError> {
        if self.remote.is_some() || binding.target != *self.identity() {
            return Err(invalid());
        }
        self.local_consensus().ensure_linearizable().await?;
        let owner = Arc::new(
            self.resources()
                .reserve_scratch_wait(crate::oltp::pool::ByteClass::Data, 64 << 10, 0)
                .await?,
        );
        let statement = self
            .local_machine()
            .store()
            .read_owned(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 0,
                },
                owner,
                move |view, owner| {
                    let raw = view.pinned(STATE)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let state: ImportState =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    if state.binding != child_digest(&binding)? || !state.complete {
                        return Err(invalid());
                    }
                    OwnedCommand::new_with_owner(
                        SplitImportedStatement {
                            binding,
                            pages: state.pages,
                            images: state.images,
                            source_images: state.source_images,
                            source_root: state.root,
                            image_root: state.image_root.ok_or_else(|| {
                                failure(
                                    ErrorCode::StorageUnavailable,
                                    "split import predates the committed-image fold",
                                )
                            })?,
                            completed_at: state.completed_at.ok_or_else(recovery_unknown)?,
                        },
                        owner,
                    )
                },
            )
            .await?;
        Ok(VerifiedSplitImported { statement })
    }
}
impl OwnedPayload for SplitSourceState {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(self.statement.group.database.capacity_bytes())
    }
    fn attach_fields(&self, _: Arc<crate::oltp::pool::ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
impl OwnedPayload for SplitImportedStatement {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        self.binding.heap_bytes()
    }
    fn attach_fields(&self, _: Arc<crate::oltp::pool::ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
pub fn child_digest(binding: &SplitBinding) -> Result<Digest, ProtocolError> {
    hash(b"nabedra/split-child/v1\0", binding)
}

pub(super) fn validate_read(config: &Config, read_ts: ReadTs) -> Result<(), ProtocolError> {
    if read_ts.get() < config.gc_floor {
        return Err(invalid());
    }
    if config.split_source.as_ref().is_some_and(|source| {
        source.published_at.is_none() && read_ts > source.statement.read_ts
    }) {
        return Err(failure(
            ErrorCode::SplitPublicationPending,
            "split source awaits routing publication",
        ));
    }
    if config
        .split_source
        .as_ref()
        .and_then(|source| source.published_at)
        .is_some_and(|commit_ts| read_ts >= commit_ts)
        || config
            .split_child
            .is_some_and(|child| !child.complete || read_ts < child.read_ts)
    {
        return Err(failure(
            ErrorCode::StorageUnavailable,
            "split routing does not cover this read timestamp",
        ));
    }
    Ok(())
}

pub(super) fn freeze_source(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: &Config,
    position: AppliedPosition,
    children: &[SplitBinding; 2],
) -> Result<Transition, ProtocolError> {
    let [left, right] = children;
    let guard = left.guard.binding();
    if left.source != *group
        || left.source_span != config.span
        || left.source_schema_epoch != config.schema_epoch
        || left.source_routing_epoch != config.routing_epoch
        || config.split_child.is_some_and(|child| !child.complete)
        || initial_root(left)? != initial_root(right)?
        || left.target.group_id == right.target.group_id
        || left.target.kind == right.target.kind
        || left.target_span.start != config.span.start
        || right.target_span.end != config.span.end
        || left.target_span.end.is_none()
        || left.target_span.end != right.target_span.start
        || config.routing_epoch.checked_add(1).is_none()
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let target_epoch = config.routing_epoch + 1;
    for child in children {
        let target_config = Config {
            span: child.target_span.clone(),
            routing_epoch: target_epoch,
            split_source: None,
            split_child: None,
            ..config.clone()
        };
        if !valid(child, &child.target, &target_config) {
            return reject(tx, Fault::InvalidBinding);
        }
    }
    let mut statement = SplitSourceStatement {
        group: group.clone(),
        split_id: left.split_id,
        txn: guard.txn,
        request: guard.request,
        read_ts: left.read_ts,
        children: [child_digest(left)?, child_digest(right)?],
        frozen_at: position,
    };
    if let Some(existing) = &config.split_source {
        statement.frozen_at = existing.statement.frozen_at;
        return if existing.statement == statement {
            commit(tx, &ParticipantReply::SplitSourceFrozen(statement))
        } else {
            reject(tx, Fault::Conflict)
        };
    }
    if tx.pinned(&source_receipt(left.split_id))?.is_some() {
        return reject(tx, Fault::AlreadyDecided);
    }
    // Another split holds the direct barrier.
    if config
        .direct_pause
        .is_some_and(|holder| holder != left.split_id)
    {
        return reject(tx, Fault::Conflict);
    }
    if tx.first_from(&[INTENT], &[INTENT])?.is_some() {
        return reject(tx, Fault::Incomplete);
    }
    // Direct commits take no lifecycle guard. One applied above the cut
    // would be missing from the child images, so this cut cannot freeze.
    // The coordinator avoids this by taking the direct barrier
    // (`DirectPause`) before it allocates the cut.
    if super::direct::high(tx)?.is_some_and(|high| high > left.read_ts) {
        return reject(tx, Fault::DirectCommitAboveCut);
    }
    let mut next = config.clone();
    next.split_source = Some(SplitSourceState {
        statement: statement.clone(),
        published_at: None,
    });
    // The frozen source refuses direct commits by itself; an abort reopens
    // the range, so the barrier is not kept.
    next.direct_pause = None;
    if encoded_length(&next, METADATA_BYTES).is_err() {
        return reject(tx, Fault::ResourceLimit);
    }
    tx.put_serialized(CONFIG, &next)?;
    commit(tx, &ParticipantReply::SplitSourceFrozen(statement))
}

pub(super) fn finish_source(
    tx: &mut GroupTransaction<'_>,
    mut config: Config,
    split_id: Uuid,
    settled: &SealedSettlement,
) -> Result<Transition, ProtocolError> {
    let key = source_receipt(split_id);
    let proof = settled.statement();
    let decision = proof.decision.decision;
    let digest = nabedra_oltp_types::owned::serialized_digest(settled)?;
    if let Some(prior) = load::<Digest>(tx, &key)? {
        return if prior == digest {
            commit(tx, &ParticipantReply::SplitSourceFinished(decision))
        } else {
            reject(tx, Fault::InvalidBinding)
        };
    }
    let source = config.split_source.as_mut()
        .filter(|source| source.statement.split_id == split_id);
    let Some(source) = source else {
        // A durable abort can race a delayed Freeze after the children were
        // bound. Persist its receipt even when Freeze never reached this source.
        // The exact split endpoint verifies the attempt and quorum settlement.
        if matches!(decision, TxnDecision::Abort { .. })
            && proof.txn.request_id == split_id && proof.request.request_id == split_id
            && settled.validate_for(proof.txn, &proof.request, &proof.manifest).is_ok()
        {
            // An aborted split releases its direct barrier.
            if config.direct_pause == Some(split_id) {
                config.direct_pause = None;
                tx.put_serialized(CONFIG, &config)?;
            }
            tx.put_serialized(&key, &digest)?;
            return commit(tx, &ParticipantReply::SplitSourceFinished(decision));
        }
        return reject(tx, Fault::InvalidBinding);
    };
    if source.statement.split_id != split_id
        || settled
            .validate_for(
                source.statement.txn,
                &source.statement.request,
                &proof.manifest,
            )
            .is_err()
    {
        return reject(tx, Fault::InvalidBinding);
    }
    match decision {
        TxnDecision::Commit { commit_ts }
            if proof.manifest.participant_count == 1 && commit_ts > source.statement.read_ts =>
        {
            source.published_at = Some(commit_ts);
        }
        TxnDecision::Commit { .. } => return reject(tx, Fault::InvalidBinding),
        TxnDecision::Abort { .. } => config.split_source = None,
    }
    if encoded_length(&config, METADATA_BYTES).is_err() {
        return reject(tx, Fault::ResourceLimit);
    }
    tx.put_serialized(CONFIG, &config)?;
    tx.put_serialized(&key, &digest)?;
    commit(tx, &ParticipantReply::SplitSourceFinished(decision))
}

fn source_receipt(split_id: Uuid) -> [u8; 17] {
    let mut key = [b'S'; 17];
    key[1..].copy_from_slice(split_id.as_bytes());
    key
}

pub(super) fn stage_child(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    mut config: Config,
    binding: &SplitBinding,
) -> Result<Transition, ProtocolError> {
    if !valid(binding, group, &config) || config.split_source.is_some() {
        return reject(tx, Fault::InvalidBinding);
    }
    let digest = child_digest(binding)?;
    if let Some(existing) = load::<ImportState>(tx, STATE)? {
        return if existing.binding == digest && config.split_child.is_some() {
            commit(tx, &ParticipantReply::SplitStaged)
        } else {
            reject(tx, Fault::InvalidBinding)
        };
    }
    if config.split_child.is_some()
        || tx.first_from(&[HEAD], &[HEAD])?.is_some()
        || tx.first_from(b"t", b"t")?.is_some()
        || tx
            .first_from(&[super::direct::DEDUP], &[super::direct::DEDUP])?
            .is_some()
        || tx
            .first_from(&[super::direct::EXPIRY], &[super::direct::EXPIRY])?
            .is_some()
        || tx.first_from(&[row::VECTOR], &[row::VECTOR])?.is_some()
        || tx.first_from(&[embed::EMBED], &[embed::EMBED])?.is_some()
    {
        return reject(tx, Fault::Conflict);
    }
    let state = ImportState {
        binding: digest,
        pages: 0,
        images: 0,
        source_images: 0,
        root: initial_root(binding)?,
        image_root: Some(super::split_closure::image_seed()?),
        after: None,
        complete: false,
        completed_at: None,
    };
    if encoded_length(&state, METADATA_BYTES).is_err() {
        return reject(tx, Fault::ResourceLimit);
    }
    config.split_child = Some(SplitChildVisibility {
        read_ts: binding.read_ts,
        complete: false,
        published_at: None,
    });
    if encoded_length(&config, METADATA_BYTES).is_err() {
        return reject(tx, Fault::ResourceLimit);
    }
    tx.put_serialized(CONFIG, &config)?;
    tx.put_serialized(STATE, &state)?;
    commit(tx, &ParticipantReply::SplitStaged)
}
pub(crate) fn initial_root(binding: &SplitBinding) -> Result<Digest, ProtocolError> {
    hash(
        b"nabedra/split-images/v1\0",
        &(
            binding.split_id,
            &binding.source,
            binding.source_schema_epoch,
            binding.source_routing_epoch,
            &binding.source_span,
            binding.read_ts,
            &binding.guard,
        ),
    )
}
pub(crate) fn page_root(page: &SplitPage) -> Result<Digest, ProtocolError> {
    let root = image_page_root(page)?;
    match &page.search {
        Some(search) => hash(b"nabedra/split-page-search/v1\0", &(root, search)),
        None => Ok(root),
    }
}
fn image_page_root(page: &SplitPage) -> Result<Digest, ProtocolError> {
    hash(
        b"nabedra/split-page/v1\0",
        &(
            initial_root(&page.binding)?,
            page.page_index,
            page.previous_root,
            &page.previous_after,
            &page.images,
            &page.after,
            page.exhausted,
        ),
    )
}
fn within(child: &KeySpan, source: &KeySpan) -> bool {
    valid_span(child)
        && source.start.as_ref().is_none_or(|lower| {
            child
                .start
                .as_ref()
                .is_some_and(|start| compare_primary(lower, start) != Ordering::Greater)
        })
        && source.end.as_ref().is_none_or(|upper| {
            child
                .end
                .as_ref()
                .is_some_and(|end| compare_primary(end, upper) != Ordering::Greater)
        })
        && child != source
}
fn valid(binding: &SplitBinding, group: &GroupIdentity, config: &Config) -> bool {
    let table = match (&binding.source.kind, &group.kind) {
        (
            GroupKind::Data {
                table_id: source,
                shard_id: source_shard,
            },
            GroupKind::Data {
                table_id: target,
                shard_id: target_shard,
            },
        ) if source == target && source_shard != target_shard => *source,
        _ => return false,
    };
    let guard = binding.guard.binding();
    binding.split_id != Uuid::nil()
        && binding.source.validate().is_ok()
        && group.validate().is_ok()
        && valid_span(&binding.source_span)
        && binding.target == *group
        && binding.source.group_id != group.group_id
        && binding.source.cluster_id == group.cluster_id
        && binding.source.database == group.database
        && binding.source_schema_epoch == config.schema_epoch
        && binding.source_routing_epoch.checked_add(1) == Some(config.routing_epoch)
        && binding.target_span == config.span
        && within(&binding.target_span, &binding.source_span)
        && binding.read_ts > guard.read_ts
        && binding.read_ts.get() >= config.gc_floor
        && binding.guard.mode() == GuardMode::Exclusive
        && guard.catalog_group_id == config.catalog_group_id
        && guard.table_id == table
        && guard.schema_epoch == config.schema_epoch
        && guard.authority == config.authority
        && binding.guard.valid().unwrap_or(false)
}
pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    mut config: Config,
    position: AppliedPosition,
    command: Digest,
    page: &SplitPage,
) -> Result<Transition, ProtocolError> {
    if !valid(&page.binding, group, &config)
        || config.split_child.is_none()
        || page.images.len() > 128
        || encoded_length(page, super::super::MAX_FIXED_COMMAND_BYTES).is_err()
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let mut receipt_key = [0; 28];
    receipt_key[..RECEIPTS.len()].copy_from_slice(RECEIPTS);
    receipt_key[RECEIPTS.len()..].copy_from_slice(&page.page_index.to_be_bytes());
    if let Some(receipt) = load::<Receipt>(tx, &receipt_key)? {
        return if receipt.command == command {
            commit(
                tx,
                &ParticipantReply::SplitImported {
                    pages: receipt.pages,
                    images: receipt.images,
                    source_images: receipt.source_images,
                    root: receipt.root,
                    complete: receipt.complete,
                },
            )
        } else {
            reject(tx, Fault::InvalidBinding)
        };
    }
    let mut state = load::<ImportState>(tx, STATE)?.ok_or_else(recovery_unknown)?;
    if state.binding != child_digest(&page.binding)?
        || state.complete
        || state.pages != page.page_index
        || state.root != page.previous_root
        || state.after != page.previous_after
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let prior = tx.pinned(CURSOR)?;
    if prior.as_deref().map(cursor_digest) != state.after {
        return Err(recovery_unknown());
    }
    let after = page.continuation();
    if after.is_none() && !page.exhausted
        || page.after.as_ref().is_some_and(|after| {
            page.images
                .last()
                .is_some_and(|last| compare_keys(&last.key, after) != Ordering::Less)
        })
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let mut previous = None;
    let mut selected = 0u64;
    for image in page.images.iter() {
        if previous.is_some_and(|old| compare_keys(old, &image.key) != Ordering::Less)
            || image.commit_ts > page.binding.read_ts
            || !owns_image(&page.binding.source, &page.binding.source_span, &image.key)
            || !image.complete()
            || image.search.as_ref().is_some_and(|search| {
                !page.search.as_ref().is_some_and(|split| search.fits(&split.schema))
            })
        {
            return reject(tx, Fault::InvalidBinding);
        }
        let length = key_length(&image.key)?;
        if let (None, Some(prior)) = (previous, prior.as_deref()) {
            let ordered = tx.with_scratch_buffer(length, |_, buffer| {
                encode_key(&image.key, buffer);
                Ok(prior < &*buffer)
            })?;
            if !ordered {
                return reject(tx, Fault::InvalidBinding);
            }
        }
        previous = Some(&image.key);
        if !owns_image(group, &config.span, &image.key) {
            continue;
        }
        if let Some(root) = state.image_root {
            state.image_root = Some(super::split_closure::fold_image(root, image)?);
        }
        if matches!(image.key, MvccKey::DirectDedup { .. }) {
            // A retry routed to this child must replay the source's result.
            super::direct::import_image(tx, image)?;
            selected = selected.checked_add(1).ok_or_else(invalid)?;
            continue;
        }
        tx.with_scratch_buffer(length + 9, |tx, key| {
            encode_key(&image.key, &mut key[1..length + 1]);
            key[0] = HEAD;
            if tx.pinned(&key[..length + 1])?.is_some() {
                return Err(recovery_unknown());
            }
            key[0] = VALUE;
            key[length + 1..].copy_from_slice(&(!image.commit_ts.get()).to_be_bytes());
            if tx.pinned(key)?.is_some() {
                return Err(recovery_unknown());
            }
            let value = image
                .value
                .as_ref()
                .map(|value| serde_json::from_str(value.as_str()).map_err(|_| invalid()))
                .transpose()?;
            if let Some(search) = &image.search {
                // The frozen source took no entry since its cut, so its records
                // are copied as they are (DB-22).
                let logical = &key[1..length + 1];
                row::install_vectors(tx, logical, search)?;
                for image in &search.records {
                    embed::put(tx, logical, image.column, &image.record)?;
                }
            }
            tx.put_serialized(key, &row::imaged(value, image.search.as_deref()))?;
            key[0] = HEAD;
            tx.put_serialized(
                &key[..length + 1],
                &Head {
                    version: Some(image.commit_ts),
                    intent: None,
                },
            )
        })?;
        selected = selected.checked_add(1).ok_or_else(invalid)?;
    }
    if let Some(after) = after {
        if !owns_image(&page.binding.source, &page.binding.source_span, after) {
            return reject(tx, Fault::InvalidBinding);
        }
        let next = tx.with_scratch_buffer(key_length(after)?, |tx, buffer| {
            encode_key(after, buffer);
            let ordered = prior
                .as_deref()
                .is_none_or(|prior| prior < &*buffer || page.exhausted && prior == &*buffer);
            if !ordered {
                return Ok(None);
            }
            // Keep the full continuation separately so it can be read within
            // a caller's cursor budget without inflating all proof reads.
            let root = cursor_digest(buffer);
            if state.after != Some(root) {
                tx.put(CURSOR, buffer)?;
            }
            Ok(Some(root))
        })?;
        let Some(next) = next else {
            return reject(tx, Fault::InvalidBinding);
        };
        state.after = Some(next);
    }
    state.pages = state.pages.checked_add(1).ok_or_else(invalid)?;
    state.images = state.images.checked_add(selected).ok_or_else(invalid)?;
    state.source_images = state
        .source_images
        .checked_add(page.images.len() as u64)
        .ok_or_else(invalid)?;
    state.root = page_root(page)?;
    state.complete = page.exhausted;
    state.completed_at = page.exhausted.then_some(position);
    if encoded_length(&state, METADATA_BYTES).is_err() {
        return reject(tx, Fault::ResourceLimit);
    }
    if let Some(search) = &page.search {
        config.search = Some(search.schema.clone());
        config.closed_floor = search.closed_floor;
        // A search worker gets the imported rows from a range snapshot.
        feed::start(tx, position)?;
    }
    if page.exhausted {
        config
            .split_child
            .as_mut()
            .ok_or_else(recovery_unknown)?
            .complete = true;
    }
    if page.exhausted || page.search.is_some() {
        if encoded_length(&config, METADATA_BYTES).is_err() {
            return reject(tx, Fault::ResourceLimit);
        }
        tx.put_serialized(CONFIG, &config)?;
    }
    tx.put_serialized(STATE, &state)?;
    tx.put_serialized(
        &receipt_key,
        &Receipt {
            command,
            pages: state.pages,
            images: state.images,
            source_images: state.source_images,
            root: state.root,
            complete: state.complete,
        },
    )?;
    commit(
        tx,
        &ParticipantReply::SplitImported {
            pages: state.pages,
            images: state.images,
            source_images: state.source_images,
            root: state.root,
            complete: state.complete,
        },
    )
}
fn cursor_digest(encoded: &[u8]) -> Digest {
    let mut hash = Sha256::new();
    hash.update(b"nabedra/split-cursor/v1\0");
    hash.update(encoded);
    Digest::from_bytes(hash.finalize().into())
}

impl SplitBinding {
    pub fn image_request(&self, after: Option<MvccKey>, max_bytes: usize) -> ImageRequest {
        ImageRequest {
            read_ts: self.read_ts,
            schema_epoch: self.source_schema_epoch,
            routing_epoch: self.source_routing_epoch,
            span: self.source_span.clone(),
            after,
            max_items: 128,
            max_bytes,
            search: None,
        }
    }

    /// Build one charged Raft command from a page read from the exact source.
    /// The endpoint must authenticate the source and re-read its live fence
    /// before calling this function.
    pub async fn import_page_from_image(
        &self,
        shared: &Arc<WorkerSharedResources>,
        page_index: u64,
        previous_root: Digest,
        previous_after: Option<Digest>,
        source: OwnedCommand<ImagePage>,
    ) -> Result<(OwnedCommand<ParticipantCommand>, Digest), ProtocolError> {
        let bytes = source
            .payload()?
            .heap_bytes()?
            .checked_add(self.heap_bytes()?)
            .and_then(|bytes| bytes.checked_add(16 << 10))
            .ok_or_else(invalid)?;
        let owner = Arc::new(shared.reserve_scratch(ByteClass::Data, bytes)?);
        let binding = self.clone();
        shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                let image = source.payload()?;
                let ImagePage::Images {
                    group,
                    read_ts,
                    images,
                    after,
                    exhausted,
                    search,
                } = image
                else {
                    return Err(failure(
                        ErrorCode::StorageUnavailable,
                        "split source has an unresolved image intent",
                    ));
                };
                if *group != binding.source || *read_ts != binding.read_ts {
                    return Err(invalid());
                }
                let authority = binding.guard.binding().authority;
                let page = SplitPage {
                    binding,
                    page_index,
                    previous_root,
                    previous_after,
                    images: Page::new(images.to_vec()).map_err(|_| invalid())?,
                    after: after.clone(),
                    exhausted: *exhausted,
                    search: search.clone(),
                };
                let root = page_root(&page)?;
                let command = ParticipantCommand::SplitImportPage {
                    authority,
                    page: Box::new(page),
                };
                encoded_length(&command, super::super::MAX_FIXED_COMMAND_BYTES).map_err(|_| {
                    failure(
                        ErrorCode::LimitExceeded,
                        "split import page exceeds command budget",
                    )
                })?;
                Ok((OwnedCommand::new_with_owner(command, owner)?, root))
            })
            .await
    }

    pub(super) fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let key_bytes = |key: &Option<Key>| match key {
            Some(Key::Utf8(text)) => text.capacity(),
            _ => 0,
        };
        [
            self.source.database.capacity_bytes(),
            self.target.database.capacity_bytes(),
            key_bytes(&self.source_span.start),
            key_bytes(&self.source_span.end),
            key_bytes(&self.target_span.start),
            key_bytes(&self.target_span.end),
        ]
        .into_iter()
        .try_fold(0usize, |bytes, next| {
            bytes.checked_add(next).ok_or_else(invalid)
        })
    }
}
impl SplitPage {
    pub(crate) fn continuation(&self) -> Option<&MvccKey> {
        self.after
            .as_ref()
            .or_else(|| self.images.last().map(|image| &image.key))
    }

    pub(super) fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let base = self.binding.heap_bytes()?
            + self.images.capacity() * std::mem::size_of::<SplitImage>()
            + self.after.as_ref().map_or(0, MvccKey::heap_bytes)
            + self.search.as_ref().map_or(0, |search| search.heap_bytes());
        self.images.iter().try_fold(base, |bytes, image| {
            bytes
                .checked_add(image.key.heap_bytes())
                .and_then(|bytes| bytes.checked_add(image.value_bytes().ok()?))
                .ok_or_else(invalid)
        })
    }
    pub(super) fn attach_fields(
        &self,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<(), ProtocolError> {
        for image in self.images.iter() {
            image.attach_charge(owner.clone())?;
        }
        Ok(())
    }
    pub(super) fn fields_attached(&self) -> bool {
        self.images.iter().all(SplitImage::is_charged)
    }
}
