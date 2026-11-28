//! The replicated search state of one Data group: its search columns and its
//! closed floor (`plans/oltp-vector.md`, sections 3 and 7, spec
//! `specs/vector/snapshot/SearchSnapshot.tla`).
//!
//! Every command here is proposed only while `data.vector.v1` is active: a
//! replica of an older build cannot decode it.
use super::{feed::Changes, *};
use crate::oltp::mvcc::vector::{ColumnId, MAX_SEARCH_COLUMNS, SearchColumn};
use nabedra_oltp_types::types::Text;

/// The columns whose vectors this group stores. A table with full-text columns
/// only has an empty list. A dropped column's id never returns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchSchema {
    /// Control's search schema epoch. 0 after a restore, until the first fence:
    /// the rows have vectors whose columns are not known, so writes wait.
    pub epoch: u64,
    /// In ascending id order.
    pub columns: Vec<SearchColumn>,
    /// The highest id this group ever had.
    pub high: ColumnId,
    /// The published vector columns, a subset of `columns` (DB-9: a barrier
    /// serves only these).
    pub serving: Vec<ColumnId>,
    /// The full-text columns, ids from the same never-reused space (`high`).
    pub text: Vec<TextColumn>,
    /// Catalog identities of vector columns, retained for DDL recovery.
    pub names: Vec<VectorName>,
}
/// The Catalog metadata absent from the storage definition of a vector.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorName {
    pub id: ColumnId,
    pub name: Text<64>,
    pub model: Option<Text<64>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchProbe {
    pub schema: Option<SearchSchema>,
    pub lifecycle: Option<Uuid>,
}

/// One full-text column of a search range (contract-w2 2.7). It has no
/// stored vector; a worker builds its impact index from the row's text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextColumn {
    pub id: ColumnId,
    /// The text column of the table it indexes.
    pub column: Text<128>,
    /// One of `nabedra_vector::text::LANGUAGES` (L9).
    pub language: Text<32>,
    /// Published: a barrier serves it (DB-9).
    pub serving: bool,
}
impl SearchSchema {
    pub(crate) fn valid(&self) -> bool {
        self.epoch != 0
            && self.columns.len() <= MAX_SEARCH_COLUMNS
            && self.columns.iter().all(SearchColumn::valid)
            && self.columns.is_sorted_by(|a, b| a.id < b.id)
            && self.columns.last().is_none_or(|column| column.id <= self.high)
    }

    pub(super) fn column(&self, id: ColumnId) -> Option<&SearchColumn> {
        self.columns.iter().find(|column| column.id == id)
    }
    pub(super) fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + columns_bytes(&self.columns) + extra_bytes(&self.serving, &self.text) + names_bytes(&self.names)
    }
}

/// The group's config with the search columns of a backup's cut (rv-oltp
/// F1). The Catalog commits a column operation only after it fenced every
/// group, so each column of the cut is one the group holds with the same
/// definition, or one it dropped since (id at most `high`), whose vectors
/// stay stored. Anything else is not this group's history: refused.
pub(super) fn at_cut(mut config: Config, cut: &SearchSchema) -> Result<Config, ProtocolError> {
    let changed = || failure(ErrorCode::StorageUnavailable, "the search columns of the backup's cut are not this range's");
    let current = ready(&config).ok_or_else(|| failure(ErrorCode::StorageUnavailable, row::COLUMNS_UNKNOWN))?;
    let known = |id: ColumnId| id != 0 && id <= current.high;
    let columns = cut.columns.iter().all(|column| match current.column(column.id) {
        Some(held) => held == column,
        None => known(column.id),
    });
    let text = cut.text.iter().all(|text| {
        match current.text.iter().find(|held| held.id == text.id) {
            Some(held) => held.column == text.column && held.language == text.language,
            None => known(text.id),
        }
    });
    if !cut.valid() || !columns || !text {
        return Err(changed());
    }
    config.search = Some(cut.clone());
    Ok(config)
}
/// What a split copies besides rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitSearch {
    pub schema: SearchSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_floor: Option<CommitTs>,
}
impl SplitSearch {
    /// What every image page of a search range carries: a split child takes
    /// it over, and a backup records it for the restore's check (L7).
    pub(super) fn of(config: &Config) -> Option<Box<Self>> {
        config.search.as_ref().map(|schema| {
            Box::new(Self {
                schema: schema.clone(),
                closed_floor: config.closed_floor,
            })
        })
    }
    /// A page of a frozen split source at its cut also carries the records,
    /// which the split copies as they are.
    pub(super) fn frozen_at(config: &Config, read_ts: ReadTs) -> bool {
        config
            .split_source
            .as_ref()
            .is_some_and(|source| source.statement.read_ts == read_ts)
    }
    pub(super) fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>() - std::mem::size_of::<SearchSchema>() + self.schema.heap_bytes()
    }
}

/// Sets the group's search columns and raises its direct epoch in one entry, so
/// no commit that read the previous columns applies after it.
///
/// L19: a compare-and-set on the group's search epoch, with the semantics of
/// DDL-FENCE's `DirectSchemaChange::Fence`. `expected_epoch: None` is the DDL
/// coordinator's probe through the log. `Some(e)` installs `epoch` (above
/// `e`) only while the group is at `e`; repeating the installed fence is a
/// no-op; anything else is `Rejected(Conflict)`. So the epoch only grows, and
/// a late fence of an interrupted DDL that picked the same epoch never
/// installs other columns (DB-20).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchFence {
    pub authority: Authority,
    pub table_id: Uuid,
    pub epoch: u64,
    /// The group's search epoch this fence was computed from (0 before the
    /// first fence and after a restore); `None` for a probe.
    pub expected_epoch: Option<u64>,
    /// As for `DirectSchemaChange::Fence`.
    pub lifecycle: Option<Uuid>,
    pub columns: Vec<SearchColumn>,
    /// As `SearchSchema::serving`: ascending, a subset of `columns`.
    pub serving: Vec<ColumnId>,
    /// As `SearchSchema::text`: ascending ids, none shared with `columns`.
    pub text: Vec<TextColumn>,
    pub names: Vec<VectorName>,
}
impl SearchFence {
    pub(super) fn heap_bytes(&self) -> usize {
        columns_bytes(&self.columns) + extra_bytes(&self.serving, &self.text) + names_bytes(&self.names)
    }
    /// The shape rules a fence must meet before apply compares it with the
    /// group's schema: ascending unique ids, a published subset, at most
    /// `MAX_SEARCH_COLUMNS` columns of both kinds, known analyzers.
    pub(crate) fn well_formed(&self) -> bool {
        let languages = crate::protocol::FULL_TEXT_LANGUAGES;
        self.expected_epoch.is_some_and(|expected| self.epoch > expected)
            && self.columns.len().saturating_add(self.text.len()) <= MAX_SEARCH_COLUMNS
            && self.columns.iter().all(SearchColumn::valid)
            && self.columns.is_sorted_by(|a, b| a.id < b.id)
            && self.serving.is_sorted_by(|a, b| a < b)
            && self
                .serving
                .iter()
                .all(|id| self.columns.iter().any(|column| column.id == *id))
            && self.text.is_sorted_by(|a, b| a.id < b.id)
            && self.text.iter().all(|text| {
                text.id != 0
                    && !text.column.as_str().is_empty()
                    && languages.contains(&text.language.as_str())
                    && self.columns.iter().all(|column| column.id != text.id)
            })
            && self.columns.iter().all(|column| column.id != 0)
            && (self.names.is_empty() || self.names.len() == self.columns.len()
                && self.names.iter().zip(&self.columns).all(|(name, column)| {
                    name.id == column.id
                        && crate::protocol::validate_identifier(name.name.as_str()).is_ok()
                        && name.model.is_some() == column.source.is_some()
                }))
    }
    /// No expected epoch and nothing else: read the group's schema through
    /// the log.
    pub(crate) fn is_probe(&self) -> bool {
        self.expected_epoch.is_none()
            && self.epoch == 0
            && self.columns.is_empty()
            && self.serving.is_empty()
            && self.text.is_empty()
            && self.lifecycle.is_none()
            && self.names.is_empty()
    }
    pub(crate) fn matches_schema(&self, schema: &SearchSchema) -> bool {
        schema.epoch == self.epoch
            && schema.columns == self.columns
            && schema.serving == self.serving
            && schema.text == self.text
            && schema.names == self.names
    }
    /// An id keeps its definition and cannot return after being dropped.
    pub(crate) fn reuses_id(&self, current: &SearchSchema) -> bool {
        self.columns.iter().any(|column| {
            match current.column(column.id) {
                Some(existing) => existing != column,
                None => column.id <= current.high,
            }
        }) || self.text.iter().any(|text| {
            match current.text.iter().find(|old| old.id == text.id) {
                Some(old) => old.column != text.column || old.language != text.language,
                None => text.id <= current.high,
            }
        }) || self.names.iter().any(|name| {
            current.names.iter().any(|old| old.id == name.id && old != name)
        })
    }
    /// The highest id this fence names.
    fn last(&self) -> ColumnId {
        let vector = self.columns.last().map_or(0, |column| column.id);
        vector.max(self.text.last().map_or(0, |text| text.id))
    }
}
fn columns_bytes(columns: &Vec<SearchColumn>) -> usize {
    columns.capacity() * std::mem::size_of::<SearchColumn>()
        + columns
            .iter()
            .filter_map(|column| column.source.as_ref())
            .map(|source| source.capacity_bytes())
            .sum::<usize>()
}
fn names_bytes(names: &Vec<VectorName>) -> usize {
    names.capacity() * std::mem::size_of::<VectorName>()
        + names.iter().map(|name| {
            name.name.capacity_bytes() + name.model.as_ref().map_or(0, Text::capacity_bytes)
        }).sum::<usize>()
}
fn extra_bytes(serving: &Vec<ColumnId>, text: &Vec<TextColumn>) -> usize {
    serving.capacity() * std::mem::size_of::<ColumnId>()
        + text.capacity() * std::mem::size_of::<TextColumn>()
        + text
            .iter()
            .map(|text| text.column.capacity_bytes() + text.language.capacity_bytes())
            .sum::<usize>()
}
/// Closes `ts`: no later commit at or below it applies in this range. The leader
/// proposes a timestamp that the Timestamp group already handed out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Closed {
    pub authority: Authority,
    pub ts: CommitTs,
    /// Leader wall clock for `ts`. Only the age of a snapshot query reads it.
    pub unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchReply {
    Fenced {
        epoch: u64,
        direct: Option<direct::DirectSchema>,
    },
    /// The schema and lifecycle installed at the probe's log position.
    Schema(Box<SearchProbe>),
    Closed {
        floor: CommitTs,
    },
    Set {
        commit_ts: CommitTs,
    },
    /// An earlier delivery of the same embedding took effect.
    AlreadySet,
    /// The row, its text or the column it was made for is gone.
    Obsolete,
    /// The attempt counted, and the record still waits.
    Counted {
        attempts: u32,
    },
    /// The attempt counted, and the record failed.
    EmbedFailed {
        attempts: u32,
    },
    Requeued {
        attempts: u32,
        limit: u32,
    },
    /// No such record, source version or attempt.
    Unmatched,
    Rejected(Fault),
}
pub(super) fn reply(
    tx: &mut GroupTransaction<'_>,
    reply: SearchReply,
) -> Result<Transition, ProtocolError> {
    let wrote = !matches!(
        reply,
        SearchReply::Rejected(_)
            | SearchReply::Schema(_)
            | SearchReply::AlreadySet
            | SearchReply::Obsolete
            | SearchReply::Unmatched
    );
    let encoded = tx.encode(&ParticipantReply::Search(reply))?;
    Ok(if wrote {
        Transition::Commit(encoded)
    } else {
        Transition::Reject(encoded)
    })
}
pub(super) fn rejected(
    tx: &mut GroupTransaction<'_>,
    fault: Fault,
) -> Result<Transition, ProtocolError> {
    reply(tx, SearchReply::Rejected(fault))
}

/// The group's columns, once they are known.
pub(super) fn ready(config: &Config) -> Option<&SearchSchema> {
    config.search.as_ref().filter(|schema| schema.epoch != 0)
}
/// The range stores vectors, but its columns are not known yet.
pub(super) fn unknown(config: &Config) -> bool {
    config.search.is_some() && ready(config).is_none()
}
/// A split froze the range, or has not finished copying it.
pub(super) fn frozen(config: &Config) -> bool {
    config.split_source.is_some() || config.split_child.is_some_and(|child| !child.complete)
}
/// The search state of a group that a restore filled with vectors or records.
pub(super) fn restored(
    tx: &mut GroupTransaction<'_>,
) -> Result<Option<SearchSchema>, ProtocolError> {
    if let Some(mut schema) = load::<SearchSchema>(tx, restore::RESTORE_SEARCH)? {
        schema.epoch = 0;
        return Ok(Some(schema));
    }
    for tag in [row::VECTOR, embed::EMBED] {
        if tx.first_from(&[tag], &[tag])?.is_some() {
            return Ok(Some(SearchSchema {
                epoch: 0,
                columns: Vec::new(),
                high: 0,
                serving: Vec::new(),
                text: Vec::new(),
                names: Vec::new(),
            }));
        }
    }
    Ok(None)
}

pub(super) fn fence(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<Config>,
    position: AppliedPosition,
    command: &SearchFence,
) -> Result<Transition, ProtocolError> {
    let Some(mut config) = config else {
        return rejected(tx, Fault::NotInitialized);
    };
    if !matches!(&group.kind, GroupKind::Data { table_id, .. } if *table_id == command.table_id)
        || !(command.well_formed() || command.is_probe())
    {
        return rejected(tx, Fault::InvalidBinding);
    }
    if config.authority != command.authority {
        return rejected(tx, Fault::StaleAuthority);
    }
    // A probe reads the schema in log order and writes nothing: the DDL
    // coordinator picks an epoch above every group's and checks what a
    // fence installed (`planner::search_ddl`).
    if command.is_probe() {
        return reply(tx, SearchReply::Schema(Box::new(SearchProbe {
            schema: config.search,
            lifecycle: config.direct_schema.and_then(|direct| direct.lifecycle),
        })));
    }
    if frozen(&config) {
        return rejected(tx, Fault::SplitInProgress);
    }
    let (epoch, high) = config
        .search
        .as_ref()
        .map_or((0, 0), |schema| (schema.epoch, schema.high));
    // L19: a repeat of the installed fence answers like its first apply.
    let installed = config.search.as_ref().is_some_and(|schema| command.matches_schema(schema)) && config
        .direct_schema
        .is_some_and(|direct| direct.lifecycle == command.lifecycle);
    if installed {
        let direct = config.direct_schema;
        return reply(tx, SearchReply::Fenced { epoch, direct });
    }
    if command.expected_epoch != Some(epoch) {
        return rejected(tx, Fault::Conflict);
    }
    // At the first fence a restored range binds the same column incarnations
    // that were used to validate its images, including full-text-only ranges.
    if epoch == 0
        && let Some(backup) = load::<SearchSchema>(tx, restore::RESTORE_SEARCH)?
        && command.columns != backup.columns
    {
        return rejected(tx, Fault::InvalidBinding);
    }
    if ready(&config).is_some_and(|current| command.reuses_id(current)) {
        return rejected(tx, Fault::InvalidBinding);
    }
    let Some(direct) = direct::fenced(&config, command.lifecycle) else {
        return rejected(tx, Fault::ResourceLimit);
    };
    let first = config.search.is_none();
    config.search = Some(SearchSchema {
        epoch: command.epoch,
        columns: command.columns.clone(),
        high: command.last().max(high),
        serving: command.serving.clone(),
        text: command.text.clone(),
        names: command.names.clone(),
    });
    config.direct_schema = Some(direct);
    if encoded_length(&config, METADATA_BYTES).is_err() {
        return rejected(tx, Fault::ResourceLimit);
    }
    tx.put_serialized(CONFIG, &config)?;
    if first {
        feed::start(tx, position)?;
    }
    reply(
        tx,
        SearchReply::Fenced {
            epoch: command.epoch,
            direct: Some(direct),
        },
    )
}

pub(super) fn closed(
    tx: &mut GroupTransaction<'_>,
    config: Option<Config>,
    position: AppliedPosition,
    command: &Closed,
) -> Result<Transition, ProtocolError> {
    let Some(mut config) = config else {
        return rejected(tx, Fault::NotInitialized);
    };
    if config.authority != command.authority {
        return rejected(tx, Fault::StaleAuthority);
    }
    if ready(&config).is_none() {
        return rejected(tx, Fault::DirectInactive);
    }
    if frozen(&config) {
        return rejected(tx, Fault::SplitInProgress);
    }
    // A prepared transaction may commit at a timestamp that was already taken.
    if tx.first_from(&[INTENT], &[INTENT])?.is_some() {
        return rejected(tx, Fault::Incomplete);
    }
    if let Some(floor) = config.closed_floor.filter(|floor| *floor >= command.ts) {
        return reply(tx, SearchReply::Closed { floor });
    }
    config.closed_floor = Some(command.ts);
    if encoded_length(&config, METADATA_BYTES).is_err() {
        return rejected(tx, Fault::ResourceLimit);
    }
    tx.put_serialized(CONFIG, &config)?;
    let mut changes = Changes::new(&config);
    changes.closed(command.ts, command.unix_ms)?;
    changes.flush(tx, position)?;
    reply(tx, SearchReply::Closed { floor: command.ts })
}
