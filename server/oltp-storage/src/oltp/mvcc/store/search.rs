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
        std::mem::size_of::<Self>()
            + columns_bytes(&self.schema.columns)
            + extra_bytes(&self.schema.serving, &self.schema.text)
    }
}

/// Sets the group's search columns and raises its direct epoch in one entry, so
/// no commit that read the previous columns applies after it. It takes only a
/// larger `epoch`: a late or repeated fence of an old Control leader changes
/// nothing (DB-20).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchFence {
    pub authority: Authority,
    pub table_id: Uuid,
    pub epoch: u64,
    /// As for `DirectSchemaChange::Fence`.
    pub lifecycle: Option<Uuid>,
    pub columns: Vec<SearchColumn>,
    /// As `SearchSchema::serving`: ascending, a subset of `columns`.
    pub serving: Vec<ColumnId>,
    /// As `SearchSchema::text`: ascending ids, none shared with `columns`.
    pub text: Vec<TextColumn>,
}
impl SearchFence {
    pub(super) fn heap_bytes(&self) -> usize {
        columns_bytes(&self.columns) + extra_bytes(&self.serving, &self.text)
    }
    /// The shape rules a fence must meet before apply compares it with the
    /// group's schema: ascending unique ids, a published subset, at most
    /// `MAX_SEARCH_COLUMNS` columns of both kinds, known analyzers.
    pub(crate) fn well_formed(&self) -> bool {
        let languages = crate::protocol::FULL_TEXT_LANGUAGES;
        self.epoch != 0
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
    }
    /// Epoch 0 with nothing else: read the group's schema through the log.
    pub(crate) fn is_probe(&self) -> bool {
        self.epoch == 0
            && self.columns.is_empty()
            && self.serving.is_empty()
            && self.text.is_empty()
            && self.lifecycle.is_none()
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
    /// The answer to a probe (`SearchFence::is_probe`): the group's schema,
    /// `None` before its first fence.
    Schema(Option<Box<SearchSchema>>),
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
        let schema = config.search.map(Box::new);
        return reply(tx, SearchReply::Schema(schema));
    }
    if frozen(&config) {
        return rejected(tx, Fault::SplitInProgress);
    }
    let (epoch, high) = config
        .search
        .as_ref()
        .map_or((0, 0), |schema| (schema.epoch, schema.high));
    if command.epoch <= epoch {
        let direct = config.direct_schema;
        return reply(tx, SearchReply::Fenced { epoch, direct });
    }
    // At the first fence a restored range binds the same column incarnations
    // that were used to validate its images, including full-text-only ranges.
    if epoch == 0
        && let Some(backup) = load::<SearchSchema>(tx, restore::RESTORE_SEARCH)?
        && command.columns != backup.columns
    {
        return rejected(tx, Fault::InvalidBinding);
    }
    // An id names one incarnation of either kind: it keeps its definition and
    // never returns once dropped. `serving` may grow or shrink freely.
    let current = ready(&config);
    let reused = command.columns.iter().any(|column| {
        match current.and_then(|schema| schema.column(column.id)) {
            Some(existing) => existing != column,
            None => epoch != 0 && column.id <= high,
        }
    }) || command.text.iter().any(|text| {
        match current.and_then(|schema| schema.text.iter().find(|old| old.id == text.id)) {
            Some(old) => old.column != text.column || old.language != text.language,
            None => epoch != 0 && text.id <= high,
        }
    });
    if reused {
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
