//! Control's search reads, commands and ready reports (contract-w2 2.1, 2.6).
//! The accepting replica checks the caller's role before it reads anything;
//! commands are proposed by the accepting leader, reports stay in its memory.
use super::*;
use crate::oltp_replication::{
    authority::control::search::{
        MAX_MEMBERS_PAGE, SearchRequest, SearchStatus, assignment_key, member_key, range_status,
        record_report, reconcile, reports,
    },
    search_rpc::{
        CallerRole, EmbedModel, RoleCheck, SEARCH_ASSIGNMENT_PREFIX, SEARCH_MEMBER_PREFIX,
        SEARCH_MODEL_PREFIX, SearchAssignment, SearchCommand, SearchMember, SearchRead,
        SearchReport,
    },
};

/// Replies of search reads and commands (a members page is at most 32
/// enrollments, a status at most one entry per range).
const REPLY_BYTES: usize = 4 << 20;
/// Models and assignments one read visits.
const MAX_SCAN: usize = 256;

impl ControlHandle {
    /// The caller's role from the applied peer directory. A process enrolled
    /// as a data worker is `Worker`; without a directory (a lone bootstrap)
    /// only operators are known.
    fn search_role(&self, session: &AuthenticatedSession) -> Option<CallerRole> {
        let remote = session.remote();
        match self.registered_directory.get() {
            Some(directory) => directory.role(&remote),
            None => self.is_operator(&remote).then_some(CallerRole::Worker),
        }
    }

    /// `ReadRequest::Search`. The 2.1 matrix for Control: a search worker
    /// reads everything, enrolls itself again and reports; an embedder reads
    /// models and enrolls itself again; an API reads assignments, members and
    /// models; a data worker reads everything, and an operator also commands.
    pub(crate) async fn search_rpc(
        &self,
        session: &AuthenticatedSession,
        request: SearchRequest,
        leader: Option<&dyn Consensus<ControlCommand, ControlReply>>,
    ) -> Result<ChargedBytes, ProtocolError> {
        if session.recovery_only()
            || session.purpose() != Purpose::Authority
            || session.group() != self.identity()
        {
            return Err(unauthorized());
        }
        let role = self.search_role(session).ok_or_else(unauthorized)?;
        let database = &self.identity().database;
        let own = |db: &Text<128>| db == database;
        let remote = session.remote();
        let permitted = match (&role, &request) {
            (CallerRole::Worker, SearchRequest::Read(_)) => true,
            (CallerRole::Worker, SearchRequest::Command { .. }) => self.is_operator(&remote),
            (CallerRole::Search(db), SearchRequest::Read(_)) => own(db),
            (CallerRole::Search(db), SearchRequest::Report(report)) => {
                own(db) && report.worker_id == remote.worker_id
            }
            (CallerRole::Search(db) | CallerRole::Embed(db), SearchRequest::Command { command, .. }) => {
                own(db)
                    && matches!(command, SearchCommand::Enroll(member)
                        if WorkerIdentity::from(&member.enrollment) == remote)
            }
            (CallerRole::Embed(db), SearchRequest::Read(SearchRead::Models)) => own(db),
            (
                CallerRole::Api(db),
                SearchRequest::Read(
                    SearchRead::Assignment { .. } | SearchRead::Members { .. } | SearchRead::Models,
                ),
            ) => own(db),
            _ => false,
        };
        if !permitted {
            return Err(unauthorized());
        }
        match request {
            SearchRequest::Read(read) => self.search_read(read).await,
            SearchRequest::Command {
                operation_id,
                command,
            } => {
                let reply = self.search_command(operation_id, command, leader).await?;
                self.resources()
                    .encode_in(ByteClass::Progress, &reply, REPLY_BYTES)
            }
            SearchRequest::Report(report) => {
                self.search_report(report).await?;
                self.resources().encode_in(ByteClass::Progress, &(), REPLY_BYTES)
            }
        }
    }

    pub(crate) async fn search_command(
        &self,
        operation_id: Uuid,
        command: SearchCommand,
        leader: Option<&dyn Consensus<ControlCommand, ControlReply>>,
    ) -> Result<ControlReply, ProtocolError> {
        if operation_id.is_nil() {
            return Err(unauthorized());
        }
        let command = ControlCommand::Search {
            operation_id,
            unix_ms: super::super::direct::wall_ms(),
            command: Box::new(command),
        };
        let applied = match leader {
            Some(leader) => {
                let owned = OwnedCommand::new_owned(command, self.resources(), ByteClass::Progress)?;
                leader.propose(owned).await?
            }
            None => self.submit(command).await?,
        };
        Ok(applied.reply)
    }

    /// The operator path of a data worker (`nabedra-db search assign|model`).
    #[doc(hidden)]
    pub async fn search_command_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        command: SearchCommand,
    ) -> Result<ControlReply, ProtocolError> {
        self.local_operator(permit, operation_id, machine::hash(operation_id.as_bytes()))?;
        self.search_command(operation_id, command, None).await
    }

    /// The leader's term, or an error on a replica that is not the leader:
    /// reports are leader memory.
    async fn leader_term(&self) -> Result<u64, ProtocolError> {
        let barrier = self.consensus.ensure_linearizable().await?;
        let local = self.consensus.local_node();
        if local.is_none() || self.consensus.current_leader() != local {
            return Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "not the Control leader; send search reports to the leader",
                Outcome::NotApplied,
            ));
        }
        Ok(barrier.applied.term)
    }
    /// IndexLifecycle `FinishSync`/`ReportAgain`, SearchAssign `Report`.
    pub(crate) async fn search_report(&self, report: SearchReport) -> Result<(), ProtocolError> {
        let term = self.leader_term().await?;
        record_report(self.identity().group_id, term, report)
    }
    /// The reports of `table` this leader holds; none on a follower.
    pub(crate) async fn search_reports(&self, table: Uuid) -> Result<Vec<SearchReport>, ProtocolError> {
        match self.leader_term().await {
            Ok(term) => reports(self.identity().group_id, term, table),
            Err(_) => Ok(Vec::new()),
        }
    }

    pub(crate) async fn search_read(&self, read: SearchRead) -> Result<ChargedBytes, ProtocolError> {
        match read {
            SearchRead::Assignment { table_id } => {
                self.read(move |view| {
                    let assignment: Option<SearchAssignment> = load(view, &assignment_key(table_id))?;
                    view.encode(&assignment)
                })
                .await
            }
            SearchRead::Members { after, max } => {
                let max = usize::from(max.clamp(1, MAX_MEMBERS_PAGE));
                self.read(move |view| {
                    let revision = state(view)?.enrollment_revision;
                    let mut items: Vec<SearchMember> = Vec::new();
                    let mut cursor = after;
                    let progress = view.scan_page(
                        SEARCH_MEMBER_PREFIX,
                        after.map(member_key).as_deref(),
                        max,
                        |key, bytes| {
                            items.push(machine::decode(bytes)?);
                            cursor = Some(
                                Uuid::from_slice(&key[SEARCH_MEMBER_PREFIX.len()..]).map_err(|_| stale())?,
                            );
                            Ok(())
                        },
                    )?;
                    view.encode(&InventoryPage {
                        revision,
                        items: Page::new(items).map_err(|_| stale())?,
                        after: cursor,
                        done: progress.exhausted,
                    })
                })
                .await
            }
            SearchRead::Models => {
                self.read(|view| {
                    let mut models: Vec<EmbedModel> = Vec::new();
                    view.scan_page(SEARCH_MODEL_PREFIX, None, MAX_SCAN, |_, bytes| {
                        models.push(machine::decode(bytes)?);
                        Ok(())
                    })?;
                    view.encode(&models)
                })
                .await
            }
            SearchRead::Status { table_id } => {
                let reports = self.search_reports(table_id).await?;
                let ranges = range_status(&reports);
                self.read(move |view| {
                    let assignment = load(view, &assignment_key(table_id))?;
                    view.encode(&SearchStatus { assignment, ranges })
                })
                .await
            }
        }
    }

    /// One pass of the assignment reconciler on the Control leader (modelled
    /// on `DamageScheduler::tick`): for each table with an assignment,
    /// publish a move whose new replica reported ready, or start the move a
    /// shard needs. Operation ids derive from the table and epoch, so a
    /// repeated pass, or one of a new leader, proposes the same command.
    /// Returns the commands proposed.
    #[doc(hidden)]
    pub async fn search_reconcile_tick(&self) -> Result<usize, ProtocolError> {
        let term = self.leader_term().await?;
        let raw = self
            .read(|view| {
                let mut assignments: Vec<SearchAssignment> = Vec::new();
                view.scan_page(SEARCH_ASSIGNMENT_PREFIX, None, MAX_SCAN, |_, bytes| {
                    assignments.push(machine::decode(bytes)?);
                    Ok(())
                })?;
                let mut members: Vec<SearchMember> = Vec::new();
                view.scan_page(SEARCH_MEMBER_PREFIX, None, MAX_SCAN, |_, bytes| {
                    members.push(machine::decode(bytes)?);
                    Ok(())
                })?;
                view.encode(&(assignments, members))
            })
            .await?;
        let (assignments, members): (Vec<SearchAssignment>, Vec<SearchMember>) =
            machine::decode(raw.as_ref())?;
        let mut proposed = 0;
        for assignment in assignments {
            let reports = reports(self.identity().group_id, term, assignment.table_id)?;
            let Some(command) = reconcile(&assignment, &members, &reports) else {
                continue;
            };
            let operation_id = reconcile_operation(&assignment, &command)?;
            self.search_command(operation_id, command, None).await?;
            proposed += 1;
        }
        Ok(proposed)
    }
}

fn reconcile_operation(
    assignment: &SearchAssignment,
    command: &SearchCommand,
) -> Result<Uuid, ProtocolError> {
    let digest = crate::oltp_replication::owned::serialized_digest(&(
        b"nabedra/search-reconcile/v1".as_slice(),
        assignment.table_id,
        assignment.epoch,
        command,
    ))?;
    Uuid::from_slice(&digest.as_bytes()[..16]).map_err(|_| stale())
}

#[cfg(test)]
mod tests;
