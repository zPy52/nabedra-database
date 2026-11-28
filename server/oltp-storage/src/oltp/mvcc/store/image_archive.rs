//! A retained cut exports committed images, never executable intents. Pages
//! cover the participant's whole logical namespace, including Catalog keys.
use super::*;
use crate::oltp::pool::{ByteClass, ScratchLease};

const IMAGE_METADATA_BYTES: usize = 512;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedImage {
    pub key: MvccKey,
    pub commit_ts: CommitTs,
    pub value: Option<super::super::CanonicalJson>,
    /// The row version's vectors, only for a table with search columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<Box<row::RowImage>>,
}
impl CommittedImage {
    /// Uncharged heap of the value and the vectors, without the key.
    pub(crate) fn value_bytes(&self) -> Result<usize, ProtocolError> {
        let value = self.value.as_ref().map_or(Ok(0), |value| value.heap_bytes())?;
        let search = self.search.as_ref().map_or(Ok(0), |search| search.heap_bytes())?;
        value.checked_add(search).ok_or_else(invalid)
    }
    pub(crate) fn attach_charge(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        if let Some(value) = &self.value {
            value.attach_charge(owner.clone())?;
        }
        self.search
            .as_ref()
            .map_or(Ok(()), |search| search.attach_charge(owner))
    }
    pub(crate) fn is_charged(&self) -> bool {
        self.value.as_ref().is_none_or(|value| value.is_charged())
            && self.search.as_ref().is_none_or(|search| search.is_charged())
    }
    /// A row image holds every vector its row references, and only a live row
    /// has any.
    pub(super) fn complete(&self) -> bool {
        self.search.as_ref().is_none_or(|search| {
            self.value.is_some()
                && matches!(self.key, MvccKey::Row { .. })
                && search.complete(self.commit_ts)
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageRequest {
    pub read_ts: ReadTs,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub span: KeySpan,
    pub after: Option<MvccKey>,
    pub max_items: u16,
    pub max_bytes: usize,
    /// A backup's search columns at its cut: the Catalog's at the cut, which
    /// its restore fences with. The page's columns and the images' vectors
    /// follow them, not the range's current columns, so a column operation
    /// that fences the range after the cut changes nothing in the backup
    /// (VEC-G10, rv-oltp F1). For a plain Data table at the cut, use
    /// `plain_search_cut()`. `None` copies the range's current schema for a
    /// split, whose lifecycle guard prevents a concurrent fence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<Box<search::SearchSchema>>,
}
impl ImageRequest {
    /// Explicitly plain Catalog image at a backup cut. A fenced Catalog
    /// search schema always has a nonzero epoch.
    pub fn plain_search_cut() -> search::SearchSchema {
        search::SearchSchema {
            epoch: 0,
            columns: Vec::new(),
            high: 0,
            serving: Vec::new(),
            text: Vec::new(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ImagePage {
    Blocked {
        txn: TxnId,
        manifest: ManifestRef,
    },
    Images {
        group: GroupIdentity,
        read_ts: ReadTs,
        images: Page<CommittedImage>,
        after: Option<MvccKey>,
        exhausted: bool,
        /// On the pages of a frozen split source with search columns.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        search: Option<Box<search::SplitSearch>>,
    },
}
impl ImagePage {
    /// The last image already owns its complete key. Omit that duplicate on
    /// the wire and borrow it as the continuation. An explicit `after` covers
    /// traversed heads with no committed value visible at this cut.
    pub fn continuation(&self) -> Result<Option<&MvccKey>, ProtocolError> {
        match self {
            Self::Blocked { .. } => Err(invalid()),
            Self::Images {
                images,
                after,
                exhausted,
                ..
            } => {
                let cursor = after
                    .as_ref()
                    .or_else(|| images.last().map(|image| &image.key));
                if !*exhausted && cursor.is_none() {
                    return Err(invalid());
                }
                Ok(cursor)
            }
        }
    }
}
impl OwnedPayload for ImageRequest {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        fn key(value: &Key) -> usize {
            match value {
                Key::Utf8(value) => value.capacity(),
                _ => 0,
            }
        }
        self.after
            .as_ref()
            .map_or(0, MvccKey::heap_bytes)
            .checked_add(self.span.start.as_ref().map_or(0, key))
            .and_then(|n| n.checked_add(self.span.end.as_ref().map_or(0, key)))
            .and_then(|n| n.checked_add(self.search.as_ref().map_or(0, |search| search.heap_bytes())))
            .ok_or_else(invalid)
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
impl OwnedPayload for ImagePage {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        match self {
            Self::Blocked { .. } => Ok(0),
            Self::Images {
                group,
                images,
                after,
                search,
                ..
            } => images.iter().try_fold(
                group.database.capacity_bytes()
                    + after.as_ref().map_or(0, MvccKey::heap_bytes)
                    + search.as_ref().map_or(0, |search| search.heap_bytes())
                    + images.capacity() * std::mem::size_of::<CommittedImage>(),
                |n, image| {
                    let search = match &image.search {
                        Some(search) => search.heap_bytes()?,
                        None => 0,
                    };
                    n.checked_add(image.key.heap_bytes())
                        .and_then(|n| n.checked_add(IMAGE_METADATA_BYTES))
                        .and_then(|n| n.checked_add(search))
                        .ok_or_else(invalid)
                },
            ),
        }
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        if let Self::Images { images, .. } = self {
            for image in images.iter() {
                image.attach_charge(owner.clone())?;
            }
        }
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        match self {
            Self::Blocked { .. } => true,
            Self::Images { images, .. } => images.iter().all(CommittedImage::is_charged),
        }
    }
}
impl ParticipantMachine {
    pub(crate) async fn committed_image_page(
        &self,
        request: OwnedCommand<ImageRequest>,
    ) -> Result<OwnedCommand<ImagePage>, ProtocolError> {
        let input = request.payload()?;
        if input.max_items == 0
            || input.max_items > 128
            || input.max_bytes < 64 << 10
            || input.max_bytes > super::super::MAX_FIXED_COMMAND_BYTES
            || !valid_span(&input.span)
        {
            return Err(invalid());
        }
        self.observe_snapshot(input.read_ts).await?;
        let owner = Arc::new(
            self.store
                .resources()
                .reserve_scratch_wait(
                    ByteClass::Data,
                    input
                        .max_bytes
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(256 << 10))
                        .ok_or_else(invalid)?,
                    0,
                )
                .await?,
        );
        let group = self.store.identity().clone();
        let page = self
            .store
            .read_owned(
                TransactionBudget {
                    scratch_bytes: 32 << 20,
                    batch_bytes: 0,
                    output_bytes: 0,
                },
                owner,
                move |view, owner| {
                    let request = request.payload()?;
                    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let config: Config =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    if config.schema_epoch != request.schema_epoch
                        || config.routing_epoch != request.routing_epoch
                        || config.span != request.span
                    {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "archive participant lineage or retained cut changed",
                        ));
                    }
                    lineage::validate_read(&config, request.read_ts)?;
                    let cut_search = request.search.as_deref();
                    let plain_cut = cut_search == Some(&ImageRequest::plain_search_cut());
                    let config = match cut_search {
                        Some(_) if plain_cut => Config { search: None, ..config },
                        Some(cut) => search::at_cut(config, cut)?,
                        None => config,
                    };
                    #[cfg(feature = "sim")]
                    super::snapshot_probe(&group, request.read_ts);
                    // Heads first, then direct-commit dedup records, which
                    // sort after every head key (`MvccKey::DirectDedup`).
                    let mut prefix = match &request.after {
                        Some(MvccKey::DirectDedup { .. }) => [super::direct::DEDUP],
                        _ => [HEAD],
                    };
                    let mut cursor = match &request.after {
                        Some(after) => {
                            if !owns_image(&group, &config.span, after) {
                                return Err(invalid());
                            }
                            let mut key = vec![prefix[0]; key_length(after)? + 2];
                            let end = key.len() - 1;
                            encode_key(after, &mut key[1..end]);
                            key[end] = 0;
                            key
                        }
                        None => prefix.to_vec(),
                    };
                    let search = search::SplitSearch::of(&config);
                    let records = search::SplitSearch::frozen_at(&config, request.read_ts);
                    let mut images = Vec::with_capacity(request.max_items as usize);
                    let mut used = 4096
                        + images.capacity() * std::mem::size_of::<CommittedImage>()
                        + cursor.capacity()
                        + search.as_ref().map_or(0, |search| search.heap_bytes())
                        + request.after.as_ref().map_or(0, MvccKey::heap_bytes);
                    let mut wire_used = 4096usize;
                    let mut next = request.after.clone();
                    let mut visited = 0;
                    while visited < request.max_items {
                        let Some(head_row) = view.first_from(&prefix, &cursor)? else {
                            if prefix[0] == HEAD {
                                prefix = [super::direct::DEDUP];
                                cursor.clear();
                                cursor.push(super::direct::DEDUP);
                                continue;
                            }
                            break;
                        };
                        visited += 1;
                        let logical = &head_row.key()[1..];
                        let key = super::archive::decode_logical(logical)?;
                        if !owns_image(&group, &config.span, &key) {
                            return Err(recovery_unknown());
                        }
                        let dedup = prefix[0] == super::direct::DEDUP;
                        if dedup != matches!(key, MvccKey::DirectDedup { .. })
                            || (!dedup && head_row.value().len() > METADATA_BYTES)
                        {
                            return Err(recovery_unknown());
                        }
                        let head: Head = if dedup {
                            Head::default()
                        } else {
                            serde_json::from_slice(head_row.value())
                                .map_err(|_| recovery_unknown())?
                        };
                        let length = logical.len();
                        let outcome = view.with_scratch_buffer(length + 9, |view, buffer| {
                            if dedup {
                                let value = head_row.value();
                                let cost =
                                    encoded_length(&key, super::super::MAX_FIXED_COMMAND_BYTES)?
                                        .checked_add(value.len())
                                        .and_then(|n| n.checked_add(2048))
                                        .ok_or_else(invalid)?;
                                if wire_used
                                    .checked_add(cost)
                                    .is_none_or(|n| n > request.max_bytes)
                                {
                                    return Ok(None);
                                }
                                let Some(image) = super::direct::image_from_record(
                                    key.clone(),
                                    value,
                                    request.read_ts,
                                    owner.clone(),
                                )?
                                else {
                                    return Ok(Some(Ok(None)));
                                };
                                wire_used += cost;
                                used = used
                                    .checked_add(key.heap_bytes())
                                    .and_then(|n| n.checked_add(value.len()))
                                    .and_then(|n| n.checked_add(IMAGE_METADATA_BYTES))
                                    .ok_or_else(invalid)?;
                                return Ok(Some(Ok(Some(image))));
                            }
                            buffer[1..length + 1].copy_from_slice(logical);
                            let end = length + 1;
                            if let Some(txn) = head.intent {
                                buffer[0] = INTENT;
                                let raw =
                                    view.pinned(&buffer[..end])?.ok_or_else(recovery_unknown)?;
                                let intent: Intent<'_> =
                                    serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                                if intent.txn != txn {
                                    return Err(recovery_unknown());
                                }
                                if intent.read_ts < request.read_ts {
                                    return Ok(Some(Err((txn, intent.manifest))));
                                }
                            }
                            buffer[0] = VALUE;
                            buffer[end..].copy_from_slice(&(!request.read_ts.get()).to_be_bytes());
                            let Some(version) = view.first_from(&buffer[..end], buffer)? else {
                                return Ok(Some(Ok(None)));
                            };
                            if version.key().len() != buffer.len() {
                                return Err(recovery_unknown());
                            }
                            let commit_ts = CommitTs::new(!u64::from_be_bytes(
                                version.key()[end..]
                                    .try_into()
                                    .map_err(|_| recovery_unknown())?,
                            ))
                            .map_err(|_| recovery_unknown())?;
                            if commit_ts > request.read_ts {
                                return Err(recovery_unknown());
                            }
                            let value = row::Version::decode(version.value())?;
                            let value = if plain_cut {
                                row::Version::plain(value.value)
                            } else {
                                value
                            };
                            let vectors =
                                row::image_cost(view, logical, &value, records)?
                                    .ok_or_else(row::damaged_value)?;
                            let cost = encoded_length(&key, super::super::MAX_FIXED_COMMAND_BYTES)?
                                .checked_add(value.value.map_or(0, |v| v.get().len()))
                                .and_then(|n| n.checked_add(vectors))
                                .and_then(|n| n.checked_add(2048))
                                .ok_or_else(invalid)?;
                            if wire_used
                                .checked_add(cost)
                                .is_none_or(|n| n > request.max_bytes)
                            {
                                return Ok(None);
                            }
                            wire_used += cost;
                            used = used
                                .checked_add(key.heap_bytes())
                                .and_then(|n| {
                                    n.checked_add(value.value.map_or(0, |v| v.get().len()))
                                })
                                .and_then(|n| n.checked_add(vectors))
                                .and_then(|n| n.checked_add(IMAGE_METADATA_BYTES))
                                .ok_or_else(invalid)?;
                            let image =
                                row::image(view, &config, logical, &value, records, &owner)?;
                            let value = value
                                .value
                                .map(|value| {
                                    super::super::CanonicalJson::from_stored_json(
                                        value.get().as_bytes(),
                                        owner.clone(),
                                    )
                                })
                                .transpose()?;
                            Ok(Some(Ok(Some(CommittedImage {
                                key: key.clone(),
                                commit_ts,
                                value,
                                search: image,
                            }))))
                        })?;
                        let emitted = match outcome {
                            Some(Err((txn, manifest))) => {
                                drop(images);
                                owner.shrink_unused(4096)?;
                                return OwnedCommand::new_with_owner(
                                    ImagePage::Blocked { txn, manifest },
                                    owner,
                                );
                            }
                            Some(Ok(Some(image))) => {
                                images.push(image);
                                true
                            }
                            Some(Ok(None)) => false,
                            None => {
                                if images.is_empty() {
                                    return Err(failure(
                                        ErrorCode::LimitExceeded,
                                        "archive image exceeds bounded page budget",
                                    ));
                                }
                                break;
                            }
                        };
                        if !emitted
                            && wire_used
                                .checked_add(encoded_length(
                                    &key,
                                    super::super::MAX_FIXED_COMMAND_BYTES,
                                )?)
                                .is_none_or(|n| n > request.max_bytes)
                        {
                            if images.is_empty() {
                                return Err(failure(
                                    ErrorCode::LimitExceeded,
                                    "archive continuation exceeds bounded page budget",
                                ));
                            }
                            break;
                        }
                        let needed = head_row.key().len() + 1;
                        let additional = needed.saturating_sub(cursor.capacity());
                        let retained = used
                            .checked_add(additional)
                            .and_then(|n| {
                                n.checked_sub(next.as_ref().map_or(0, MvccKey::heap_bytes))
                            })
                            .and_then(|n| n.checked_add(key.heap_bytes()))
                            .ok_or_else(invalid)?;
                        if retained > owner.bytes() {
                            return Err(invalid());
                        }
                        cursor.clear();
                        cursor.try_reserve_exact(needed).map_err(|_| invalid())?;
                        cursor.extend_from_slice(head_row.key());
                        cursor.push(0);
                        used = retained;
                        next = Some(key);
                    }
                    let exhausted = view.first_from(&prefix, &cursor)?.is_none()
                        && (prefix[0] == super::direct::DEDUP
                            || view
                                .first_from(&[super::direct::DEDUP], &[super::direct::DEDUP])?
                                .is_none());
                    if next.as_ref() == images.last().map(|image| &image.key) {
                        next = None;
                    }
                    owner.shrink_unused(used)?;
                    OwnedCommand::new_with_owner(
                        ImagePage::Images {
                            group,
                            read_ts: request.read_ts,
                            images: Page::new(images).map_err(|_| invalid())?,
                            after: next,
                            exhausted,
                            search,
                        },
                        owner,
                    )
                },
            )
            .await;
        self.note_damage(page).await
    }
}
impl ParticipantHandle {
    pub async fn committed_image_page(
        &self,
        request: OwnedCommand<ImageRequest>,
    ) -> Result<OwnedCommand<ImagePage>, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.committed_image_page(request).await;
        }
        self.local_consensus().ensure_linearizable().await?;
        self.local_machine().committed_image_page(request).await
    }
}
