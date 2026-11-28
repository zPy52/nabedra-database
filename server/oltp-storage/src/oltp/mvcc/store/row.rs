//! Row versions and the vectors they reference (`plans/oltp-vector.md`,
//! section 2, spec `specs/vector/blobs/RowBlobs.tla`).
//!
//! `'x' ‖ row key ‖ column ‖ commit_ts` holds one vector as it was written. A
//! row version of a table with search columns names the vector versions it uses
//! (`blobs`) and the source version of each embedded column (`source`). Both
//! maps are absent when empty, so every other table keeps its bytes.
use super::{embed::EmbedRecord, search::SearchSchema, *};
use crate::protocol::ErrorCode;
use crate::oltp::{
    mvcc::vector::{ColumnId, FRAME_BYTES, MAX_SEARCH_COLUMNS, RowSearch, VectorValue, open_frame},
    pool::{GroupRead, ScratchLease},
};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const VECTOR: u8 = b'x';
/// Vectors and search instructions of an open transaction: `'y' ‖ txn ‖
/// ordinal`, then the column. Nothing but Resolve removes them.
pub(super) const STAGED: u8 = b'y';
const DAMAGED_VALUE: &str = "storage corrupt: a stored vector value is missing or damaged";
const BELOW_FLOOR: &str = "storage corrupt: a transaction resolved at or below the closed floor";
pub(super) const COLUMNS_UNKNOWN: &str = "the search columns of this restored range are not known yet";

/// A timestamp per search column.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Columns(pub BTreeMap<ColumnId, CommitTs>);
impl Columns {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn get(&self, column: ColumnId) -> Option<CommitTs> {
        self.0.get(&column).copied()
    }
}

/// The stored value of one `'v'` record.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Version<'a> {
    #[serde(borrow)]
    pub(super) value: Option<&'a RawValue>,
    #[serde(default, skip_serializing_if = "Columns::is_empty")]
    pub(super) blobs: Columns,
    #[serde(default, skip_serializing_if = "Columns::is_empty")]
    pub(super) source: Columns,
}
impl<'a> Version<'a> {
    pub(super) fn plain(value: Option<&'a RawValue>) -> Self {
        Self {
            value,
            blobs: Columns::default(),
            source: Columns::default(),
        }
    }
    pub(super) fn decode(raw: &'a [u8]) -> Result<Self, ProtocolError> {
        serde_json::from_slice(raw).map_err(|_| recovery_unknown())
    }
    /// Whether this version carries anything besides its row.
    fn searched(&self) -> bool {
        !self.blobs.is_empty() || !self.source.is_empty()
    }
    /// Keeps what `old` holds for `column`: its vector and its source version.
    pub(super) fn keep(&mut self, old: &Version<'_>, column: ColumnId) {
        if let Some(version) = old.blobs.get(column) {
            self.blobs.0.insert(column, version);
        }
        if let Some(version) = old.source.get(column) {
            self.source.0.insert(column, version);
        }
    }
}

/// `tag ‖ row key ‖ column`, then the commit timestamp of a vector.
pub(super) fn column_key(
    tag: u8,
    logical: &[u8],
    column: ColumnId,
    version: Option<CommitTs>,
) -> Result<Vec<u8>, ProtocolError> {
    let mut key = tagged(tag, logical, 10)?;
    key.extend_from_slice(&column.to_be_bytes());
    if let Some(version) = version {
        key.extend_from_slice(&version.get().to_be_bytes());
    }
    Ok(key)
}
fn version_key(logical: &[u8], version: CommitTs) -> Result<Vec<u8>, ProtocolError> {
    let mut key = tagged(VALUE, logical, 8)?;
    key.extend_from_slice(&(!version.get()).to_be_bytes());
    Ok(key)
}
pub(super) fn tagged(tag: u8, logical: &[u8], extra: usize) -> Result<Vec<u8>, ProtocolError> {
    let mut key = Vec::new();
    key.try_reserve_exact(logical.len() + 1 + extra)
        .map_err(|_| invalid())?;
    key.push(tag);
    key.extend_from_slice(logical);
    Ok(key)
}
pub(super) fn logical_key(key: &MvccKey) -> Result<Vec<u8>, ProtocolError> {
    let length = key_length(key)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|_| invalid())?;
    bytes.resize(length, 0);
    encode_key(key, &mut bytes);
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

/// The text of `column` in a row, exactly as canonical JSON spells it. `None`
/// for null and for a missing column.
fn text<'a>(row: &'a RawValue, column: &str) -> Option<&'a str> {
    struct Find<'c>(&'c str);
    impl<'de> serde::de::Visitor<'de> for Find<'_> {
        type Value = Option<&'de RawValue>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a row")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut found = None;
            while let Some(name) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                if name == self.0 {
                    found = Some(map.next_value()?);
                } else {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
            Ok(found)
        }
    }
    let mut decoder = serde_json::Deserializer::from_str(row.get());
    serde::Deserializer::deserialize_map(&mut decoder, Find(column))
        .ok()
        .flatten()
        .map(RawValue::get)
        .filter(|text| *text != "null")
}

/// One write of one row of a search range.
pub(super) struct RowWrite<'a> {
    pub(super) schema: &'a SearchSchema,
    pub(super) logical: &'a [u8],
    /// The head version this write replaces.
    pub(super) previous: Option<CommitTs>,
    pub(super) commit_ts: CommitTs,
    /// Columns this write names, and whether it sets a vector or null.
    pub(super) named: &'a [(ColumnId, bool)],
    pub(super) backfill: &'a [ColumnId],
}
impl RowWrite<'_> {
    /// Writes and deletes the row's embedding records and returns the row
    /// version to store (`EmbedApply.tla`, `Write`, `Delete`, `Backfill`). A
    /// changed source text clears the vector and any failure and starts a new
    /// record. An unchanged one keeps all three.
    pub(super) fn plan<'v>(
        &self,
        tx: &mut GroupTransaction<'_>,
        value: Option<&'v RawValue>,
    ) -> Result<Version<'v>, ProtocolError> {
        let raw = match self.previous {
            Some(version) => Some(stored(tx, self.logical, version)?),
            None => None,
        };
        let old = match &raw {
            Some(raw) => Version::decode(raw)?,
            None => Version::plain(None),
        };
        let mut next = Version::plain(value);
        for column in &self.schema.columns {
            let id = column.id;
            if let Some(source) = &column.source {
                let before = old.value.and_then(|row| text(row, source));
                let after = value.and_then(|row| text(row, source));
                if value.is_some() && old.value.is_some() && before == after {
                    next.keep(&old, id);
                    if after.is_some()
                        && self.backfill.contains(&id)
                        && old.blobs.get(id).is_none()
                        && embed::load(tx, self.logical, id)?.is_none()
                    {
                        next.source.0.insert(id, self.commit_ts);
                        embed::put(tx, self.logical, id, &EmbedRecord::fresh(self.commit_ts))?;
                    }
                } else if after.is_some() {
                    next.source.0.insert(id, self.commit_ts);
                    embed::put(tx, self.logical, id, &EmbedRecord::fresh(self.commit_ts))?;
                } else if old.source.get(id).is_some() {
                    embed::delete(tx, self.logical, id)?;
                }
            } else if value.is_some() {
                match self.named.iter().find(|(named, _)| *named == id) {
                    Some((_, true)) => {
                        next.blobs.0.insert(id, self.commit_ts);
                    }
                    Some((_, false)) => {}
                    None if old.value.is_some() => next.keep(&old, id),
                    None => {}
                }
            }
        }
        // A dropped column's record goes with the first write after the drop.
        for id in old.source.0.keys() {
            if self.schema.column(*id).is_none() {
                embed::delete(tx, self.logical, *id)?;
            }
        }
        Ok(next)
    }
}
/// The columns a write names.
pub(super) fn named(search: Option<&RowSearch>) -> Vec<(ColumnId, bool)> {
    search.map_or_else(Vec::new, |search| {
        search
            .vectors
            .iter()
            .map(|change| (change.column, change.value.is_some()))
            .collect()
    })
}
/// The vectors of one write under `key(column)`. The caller checked
/// `search.valid`.
pub(super) fn put_vectors(
    tx: &mut GroupTransaction<'_>,
    search: &RowSearch,
    key: impl Fn(ColumnId) -> Result<Vec<u8>, ProtocolError>,
) -> Result<(), ProtocolError> {
    for change in &search.vectors {
        if let Some(value) = &change.value {
            put_vector(tx, &key(change.column)?, value)?;
        }
    }
    Ok(())
}
pub(super) fn put_vector(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
    value: &VectorValue,
) -> Result<(), ProtocolError> {
    let length = value.byte_len().ok_or_else(invalid)? + FRAME_BYTES;
    tx.with_scratch_buffer(length, |tx, frame| {
        value.frame(frame).ok_or_else(invalid)?;
        tx.put(key, frame)
    })
}
pub(super) fn put_version(
    tx: &mut GroupTransaction<'_>,
    logical: &[u8],
    at: CommitTs,
    version: &Version<'_>,
) -> Result<(), ProtocolError> {
    tx.put_serialized(&version_key(logical, at)?, version)?;
    tx.put_serialized(
        &tagged(HEAD, logical, 0)?,
        &Head {
            version: Some(at),
            intent: None,
        },
    )
}
/// The stored version a head names.
pub(super) fn stored(
    tx: &mut GroupTransaction<'_>,
    logical: &[u8],
    at: CommitTs,
) -> Result<bytes::Bytes, ProtocolError> {
    tx.pinned(&version_key(logical, at)?)?
        .ok_or_else(recovery_unknown)
}
/// Native batch bytes of one row's vectors, metadata, embedding records and
/// change record, on top of its version and head.
pub(super) fn write_cost(search: Option<&RowSearch>, key: usize) -> usize {
    const RECORD: usize = 256;
    let records = key
        .saturating_add(RECORD)
        .saturating_mul(MAX_SEARCH_COLUMNS + 2);
    search
        .into_iter()
        .flat_map(RowSearch::values)
        .map(|value| value.byte_len().unwrap_or(0))
        .fold(records, |bytes, value| {
            bytes
                .saturating_add(value)
                .saturating_add(key)
                .saturating_add(256)
        })
}

// ---------------------------------------------------------------------------
// Transactions: Prepare stages, Resolve writes
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Staged {
    named: Vec<(ColumnId, bool)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    backfill: Vec<ColumnId>,
}
fn staged_key(txn: TxnId, ordinal: u64, column: Option<ColumnId>) -> Vec<u8> {
    let mut key = ordinal_key(STAGED, txn, ordinal).to_vec();
    if let Some(column) = column {
        key.extend_from_slice(&column.to_be_bytes());
    }
    key
}
pub(super) fn stage(
    tx: &mut GroupTransaction<'_>,
    txn: TxnId,
    ordinal: u64,
    search: &RowSearch,
) -> Result<(), ProtocolError> {
    tx.put_serialized(
        &staged_key(txn, ordinal, None),
        &Staged {
            named: named(Some(search)),
            backfill: search.backfill.clone(),
        },
    )?;
    put_vectors(tx, search, |column| {
        Ok(staged_key(txn, ordinal, Some(column)))
    })
}
/// Resolves one prepared write of a search range. A commit moves the staged
/// vectors under its timestamp and returns the version to store. An abort
/// (`None`) drops them.
pub(super) fn resolve<'v>(
    tx: &mut GroupTransaction<'_>,
    txn: TxnId,
    ordinal: u64,
    commit: Option<(RowWrite<'_>, Option<&'v RawValue>)>,
) -> Result<Version<'v>, ProtocolError> {
    let key = staged_key(txn, ordinal, None);
    let staged = match load::<Staged>(tx, &key)? {
        Some(staged) => {
            tx.delete(&key)?;
            staged
        }
        None => Staged {
            named: Vec::new(),
            backfill: Vec::new(),
        },
    };
    for (column, _) in staged.named.iter().filter(|(_, set)| *set) {
        let key = staged_key(txn, ordinal, Some(*column));
        if let Some((write, _)) = &commit {
            let raw = tx.pinned(&key)?.ok_or_else(damaged_value)?;
            open_frame(&raw).ok_or_else(damaged_value)?;
            let vector = column_key(VECTOR, write.logical, *column, Some(write.commit_ts))?;
            tx.put(&vector, &raw)?;
        }
        tx.delete(&key)?;
    }
    let Some((write, value)) = commit else {
        return Ok(Version::plain(None));
    };
    RowWrite {
        named: &staged.named,
        backfill: &staged.backfill,
        ..write
    }
    .plan(tx, value)
}

// ---------------------------------------------------------------------------
// Images: splits, backups, the feed and reads
// ---------------------------------------------------------------------------

/// One committed row version's vectors, in an image, a page or a reply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowImage {
    pub blobs: Columns,
    /// One per entry of `blobs`, in the same order.
    pub vectors: Vec<VectorImage>,
    #[serde(default, skip_serializing_if = "Columns::is_empty")]
    pub source: Columns,
    /// The embedding records of the row, only in the images of a split.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub records: Vec<RecordImage>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorImage {
    pub column: ColumnId,
    pub value: VectorValue,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordImage {
    pub column: ColumnId,
    pub record: EmbedRecord,
}
impl RowImage {
    /// A copy is installed only with every vector its row references, and
    /// nothing else (VEC-G10).
    pub(super) fn complete(&self, commit_ts: CommitTs) -> bool {
        self.vectors.len() == self.blobs.0.len()
            && self.blobs.0.iter().zip(&self.vectors).all(
                |((column, version), image)| {
                    *column == image.column
                        && *version <= commit_ts
                        && image.value.checked_len().is_some()
                },
            )
            && self.source.0.values().all(|version| *version <= commit_ts)
            && self.records.is_sorted_by(|a, b| a.column < b.column)
            && self.records.iter().all(|image| {
                self.blobs.get(image.column).is_none()
                    && self.source.get(image.column) == Some(image.record.source_version)
            })
    }
    pub(crate) fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let base = 1024
            + self.vectors.capacity() * std::mem::size_of::<VectorImage>()
            + self.records.capacity() * (std::mem::size_of::<RecordImage>() + 256);
        self.vectors.iter().try_fold(base, |bytes, image| {
            bytes
                .checked_add(image.value.heap_bytes()?)
                .ok_or_else(invalid)
        })
    }
    pub(crate) fn attach_charge(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        self.vectors
            .iter()
            .try_for_each(|image| image.value.attach_charge(owner.clone()))
    }
    pub(crate) fn is_charged(&self) -> bool {
        self.vectors.iter().all(|image| image.value.is_charged())
    }
    /// The columns whose vector is still to be made: the row has a source text
    /// and no vector (`RowBlobs.tla`, `NeedsVector`).
    pub(super) fn pending(&self) -> impl Iterator<Item = (ColumnId, CommitTs)> + '_ {
        self.source
            .0
            .iter()
            .filter(|(column, _)| self.blobs.get(**column).is_none())
            .map(|(column, version)| (*column, *version))
    }
}

/// Memory and wire bytes of `version`'s image, before it is read. `None` when a
/// referenced vector is not there.
pub(super) fn image_cost(
    view: &mut GroupRead<'_>,
    logical: &[u8],
    version: &Version<'_>,
    records: bool,
) -> Result<Option<usize>, ProtocolError> {
    if !version.searched() {
        return Ok(Some(0));
    }
    let source = if records { 768 } else { 64 };
    let mut bytes = 2048 + version.source.0.len() * source;
    for (column, at) in &version.blobs.0 {
        let key = column_key(VECTOR, logical, *column, Some(*at))?;
        let Some(raw) = view.pinned(&key)? else {
            return Ok(None);
        };
        bytes = bytes
            .checked_add(VectorValue::retained_bytes(raw.len()))
            .ok_or_else(invalid)?;
    }
    Ok(Some(bytes))
}
/// The vectors (and, for a split, the records) of one stored row version, or
/// `None` for one without any. The caller precharged `image_cost`. Only the
/// columns of `config`'s schema are served: a dropped column's vector is not,
/// and a vector that does not have its column's type is damage (VEC-G10).
pub(super) fn image(
    view: &mut GroupRead<'_>,
    config: &Config,
    logical: &[u8],
    version: &Version<'_>,
    records: bool,
    owner: &Arc<ScratchLease>,
) -> Result<Option<Box<RowImage>>, ProtocolError> {
    if !version.searched() {
        return Ok(None);
    }
    // A restored range learns its columns from its first search fence.
    let schema = search::ready(config).ok_or_else(|| {
        failure(ErrorCode::StorageUnavailable, COLUMNS_UNKNOWN)
    })?;
    let mut blobs = Columns::default();
    let mut vectors = Vec::with_capacity(version.blobs.0.len());
    for (column, at) in &version.blobs.0 {
        let Some(definition) = schema.column(*column) else {
            continue;
        };
        let key = column_key(VECTOR, logical, *column, Some(*at))?;
        let raw = view.pinned(&key)?.ok_or_else(damaged_value)?;
        let (kind, bytes) = open_frame(&raw)
            .filter(|(kind, bytes)| definition.fits(*kind, bytes.len()))
            .ok_or_else(damaged_value)?;
        blobs.0.insert(*column, *at);
        vectors.push(VectorImage {
            column: *column,
            value: VectorValue::from_stored(kind, bytes, owner.clone())?,
        });
    }
    let mut source = version.source.clone();
    source.0.retain(|column, _| schema.column(*column).is_some());
    let mut found = Vec::new();
    if records {
        for column in source.0.keys() {
            if let Some(record) = embed::read(view, logical, *column)? {
                found.push(RecordImage {
                    column: *column,
                    record,
                });
            }
        }
    }
    Ok(Some(Box::new(RowImage {
        blobs,
        vectors,
        source,
        records: found,
    })))
}

/// A copy from another replica that installs its records as they are checks
/// every vector against the columns of the copy's own config, which sorts
/// before every vector (VEC-G10).
#[derive(Default)]
pub(crate) struct InstallCheck {
    schema: Option<SearchSchema>,
}
impl InstallCheck {
    pub(crate) fn record(&mut self, key: &[u8], value: &[u8]) -> Result<(), ProtocolError> {
        if key == CONFIG {
            let config: Config = serde_json::from_slice(value).map_err(|_| damaged_value())?;
            self.schema = config.search;
            return Ok(());
        }
        if key.first() != Some(&VECTOR) {
            return Ok(());
        }
        let column = key
            .len()
            .checked_sub(10)
            .and_then(|at| key.get(at..at + 2))
            .map(|column| u16::from_be_bytes([column[0], column[1]]))
            .ok_or_else(damaged_value)?;
        let (kind, bytes) = open_frame(value).ok_or_else(damaged_value)?;
        // A dropped column's vector stays until collection, and is never served.
        let definition = self
            .schema
            .as_ref()
            .filter(|schema| schema.epoch != 0)
            .and_then(|schema| schema.column(column));
        if definition.is_some_and(|definition| !definition.fits(kind, bytes.len())) {
            return Err(damaged_value());
        }
        Ok(())
    }
}
impl RowImage {
    /// Every vector and source version belongs to a column of the right kind.
    pub(super) fn fits(&self, schema: &SearchSchema) -> bool {
        self.source
            .0
            .keys()
            .all(|id| schema.column(*id).is_some_and(|column| column.source.is_some()))
            && self.vectors.iter().all(|image| {
                schema.column(image.column).is_some_and(|column| {
                    image
                        .value
                        .checked_len()
                        .is_some_and(|len| column.fits(image.value.kind, len))
                })
            })
    }
}
/// A Resolve whose commit timestamp is at or below the closed floor. It cannot be
/// refused, which would break the transaction's atomicity, and must not land, as
/// a search read at the floor was served without it. The coordinator takes the
/// timestamp after every participant prepared, and `Closed` waits for open
/// intents, so this is a broken protocol: the apply fails and the group is
/// quarantined (defense in depth).
pub(super) fn resolved_below_floor() -> ProtocolError {
    failure(ErrorCode::StorageUnavailable, BELOW_FLOOR)
}
/// Writes an image's vectors. The caller checked `complete`.
pub(super) fn install_vectors(
    tx: &mut GroupTransaction<'_>,
    logical: &[u8],
    image: &RowImage,
) -> Result<(), ProtocolError> {
    for (vector, at) in image.vectors.iter().zip(image.blobs.0.values()) {
        let key = column_key(VECTOR, logical, vector.column, Some(*at))?;
        // Another version of the row may have brought the same vector.
        if let Some(existing) = tx.pinned(&key)? {
            let length = vector.value.byte_len().ok_or_else(invalid)? + FRAME_BYTES;
            let same = tx.with_scratch_buffer(length, |_, frame| {
                vector.value.frame(frame).ok_or_else(invalid)?;
                Ok(existing.as_ref() == &*frame)
            })?;
            if !same {
                return Err(recovery_unknown());
            }
        } else {
            put_vector(tx, &key, &vector.value)?;
        }
    }
    Ok(())
}
/// The row version an image stands for.
pub(super) fn imaged<'a>(
    value: Option<&'a RawValue>,
    image: Option<&RowImage>,
) -> Version<'a> {
    match image {
        Some(image) => Version {
            value,
            blobs: image.blobs.clone(),
            source: image.source.clone(),
        },
        None => Version::plain(value),
    }
}

/// A damaged vector is never null: every reader gets this error, and the group
/// follows the damage rules of the storage engine.
pub(super) fn damaged_value() -> ProtocolError {
    failure(ErrorCode::StorageUnavailable, DAMAGED_VALUE)
}
impl ParticipantMachine {
    /// Quarantines the group when a read or an apply met a damaged vector.
    pub(super) async fn note_damage<T>(
        &self,
        result: Result<T, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        if result
            .as_ref()
            .is_err_and(|error| [DAMAGED_VALUE, BELOW_FLOOR].contains(&error.message.as_str()))
        {
            self.store
                .pool()
                .report_value_damage(self.store.identity().group_id)
                .await;
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

/// The reference rule for MVCC collection, which any collector must use: of the
/// stored vector versions of one row, those that no retained version of the row
/// references. It may remove them later without a second look, because a write
/// references only its own vector or what the newest version references, and the
/// newest version is always retained. The vectors of an open transaction are
/// staged under another tag, so they are never among them. Which row versions
/// are retained is the collector's safe point, which a copy pins at its cut.
pub fn unreferenced<'a>(
    retained: impl IntoIterator<Item = &'a Columns>,
    vectors: impl IntoIterator<Item = (ColumnId, CommitTs)>,
) -> Vec<(ColumnId, CommitTs)> {
    let used: BTreeSet<(ColumnId, CommitTs)> = retained
        .into_iter()
        .flat_map(|blobs| blobs.0.iter().map(|(column, at)| (*column, *at)))
        .collect();
    vectors
        .into_iter()
        .filter(|vector| !used.contains(vector))
        .collect()
}
/// `unreferenced` over the stored versions and vectors of one row.
pub fn unreferenced_in(
    view: &mut GroupRead<'_>,
    key: &MvccKey,
) -> Result<Vec<(ColumnId, CommitTs)>, ProtocolError> {
    let logical = logical_key(key)?;
    let mut retained = Vec::new();
    let mut vectors = Vec::new();
    for (tag, suffix) in [(VALUE, 8), (VECTOR, 10)] {
        let prefix = tagged(tag, &logical, 0)?;
        let mut cursor = prefix.clone();
        while let Some(record) = view.first_from(&prefix, &cursor)? {
            let rest = &record.key()[prefix.len()..];
            // Row keys are prefix-free, so every record here is of this row.
            if rest.len() != suffix {
                return Err(recovery_unknown());
            }
            if tag == VALUE {
                retained.push(Version::decode(record.value())?.blobs);
            } else {
                let at = rest[2..].try_into().map_err(|_| recovery_unknown())?;
                let at = CommitTs::new(u64::from_be_bytes(at)).map_err(|_| recovery_unknown())?;
                vectors.push((u16::from_be_bytes([rest[0], rest[1]]), at));
            }
            cursor = [record.key(), &[0]].concat();
        }
    }
    Ok(unreferenced(&retained, vectors))
}

// ---------------------------------------------------------------------------
// Point reads
// ---------------------------------------------------------------------------

/// A row with its vectors and records at one timestamp.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchRow {
    /// A transaction holds an intent on the row.
    Blocked { txn: TxnId, manifest: ManifestRef },
    Row {
        version: Option<CommitTs>,
        value: Option<super::super::CanonicalJson>,
        search: Option<Box<RowImage>>,
    },
}
impl OwnedPayload for SearchRow {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let Self::Row { value, search, .. } = self else {
            return Ok(0);
        };
        value
            .as_ref()
            .map_or(Ok(0), |value| value.heap_bytes())?
            .checked_add(search.as_ref().map_or(Ok(0), |search| search.heap_bytes())?)
            .ok_or_else(invalid)
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        let Self::Row { value, search, .. } = self else {
            return Ok(());
        };
        if let Some(value) = value {
            value.attach_charge(owner.clone())?;
        }
        search
            .as_ref()
            .map_or(Ok(()), |search| search.attach_charge(owner))
    }
    fn fields_attached(&self) -> bool {
        let Self::Row { value, search, .. } = self else {
            return true;
        };
        value.as_ref().is_none_or(|value| value.is_charged())
            && search.as_ref().is_none_or(|search| search.is_charged())
    }
}
impl ParticipantMachine {
    /// The newest version of a row at or below `read_ts` with its vectors and
    /// its embedding records. The caller took the read barrier, as for
    /// `read_at`. `u64::MAX` reads the latest version like `direct_latest`: it
    /// waits for the key's registered commits and never raises `max_read_ts`.
    pub async fn read_search_row(
        &self,
        read_ts: ReadTs,
        key: MvccKey,
        max_bytes: usize,
    ) -> Result<OwnedCommand<SearchRow>, ProtocolError> {
        if max_bytes > super::super::MAX_FIXED_COMMAND_BYTES {
            return Err(invalid());
        }
        if read_ts.get() == u64::MAX {
            self.clock.wait_latest(&key).await?;
        } else {
            self.observe_snapshot(read_ts).await?;
        }
        let owner = Arc::new(
            self.store
                .resources()
                .reserve_scratch_wait(
                    crate::oltp::pool::ByteClass::Data,
                    2 * max_bytes + (64 << 10),
                    0,
                )
                .await?,
        );
        let group = self.store.identity().clone();
        let result = self
            .store
            .read_owned(
                TransactionBudget {
                    scratch_bytes: 32 << 20,
                    batch_bytes: 0,
                    output_bytes: 0,
                },
                owner,
                move |view, owner| {
                    let row = read_row(view, &group, read_ts, &key, max_bytes, &owner)?;
                    OwnedCommand::new_with_owner(row, owner)
                },
            )
            .await;
        self.note_damage(result).await
    }
}
impl ParticipantHandle {
    /// `ParticipantMachine::read_search_row` on a local replica.
    pub async fn search_row_local(
        &self,
        read_ts: ReadTs,
        key: MvccKey,
        max_bytes: usize,
    ) -> Result<OwnedCommand<SearchRow>, ProtocolError> {
        if self.remote.is_some() {
            return Err(invalid());
        }
        self.local_machine()
            .read_search_row(read_ts, key, max_bytes)
            .await
    }
}
fn read_row(
    view: &mut GroupRead<'_>,
    group: &GroupIdentity,
    read_ts: ReadTs,
    key: &MvccKey,
    max_bytes: usize,
    owner: &Arc<ScratchLease>,
) -> Result<SearchRow, ProtocolError> {
    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
    if raw.len() > METADATA_BYTES {
        return Err(recovery_unknown());
    }
    let config: Config = serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
    lineage::validate_read(&config, read_ts)?;
    if !owns(group, &config.span, key) {
        return Err(invalid());
    }
    let logical = logical_key(key)?;
    if let Some(raw) = view.pinned(&tagged(HEAD, &logical, 0)?)? {
        let head: Head = serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
        if let Some(txn) = head.intent {
            let raw = view
                .pinned(&tagged(INTENT, &logical, 0)?)?
                .ok_or_else(recovery_unknown)?;
            let intent: Intent<'_> =
                serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
            if intent.txn != txn {
                return Err(recovery_unknown());
            }
            if intent.read_ts < read_ts {
                return Ok(SearchRow::Blocked {
                    txn,
                    manifest: intent.manifest,
                });
            }
        }
    }
    let prefix = tagged(VALUE, &logical, 0)?;
    let Some(record) = view.first_from(&prefix, &version_key(&logical, read_ts)?)? else {
        return Ok(SearchRow::Row {
            version: None,
            value: None,
            search: None,
        });
    };
    let at = record.key()[prefix.len()..]
        .try_into()
        .map(|at| !u64::from_be_bytes(at))
        .map_err(|_| recovery_unknown())?;
    let version = Version::decode(record.value())?;
    let bytes = image_cost(view, &logical, &version, true)?
        .ok_or_else(damaged_value)?
        .checked_add(version.value.map_or(0, |value| value.get().len() + 256))
        .ok_or_else(invalid)?;
    if bytes > max_bytes {
        return Err(failure(
            ErrorCode::LimitExceeded,
            "row exceeds its read budget",
        ));
    }
    let search = image(view, &config, &logical, &version, true, owner)?;
    Ok(SearchRow::Row {
        version: Some(CommitTs::new(at).map_err(|_| recovery_unknown())?),
        search,
        value: version
            .value
            .map(|value| {
                super::super::CanonicalJson::from_stored_json(value.get().as_bytes(), owner.clone())
            })
            .transpose()?,
    })
}
/// The exact version `at` of a row, or `None` when it is gone.
pub(super) fn exact(
    view: &mut GroupRead<'_>,
    logical: &[u8],
    at: CommitTs,
) -> Result<Option<bytes::Bytes>, ProtocolError> {
    view.pinned(&version_key(logical, at)?)
}
