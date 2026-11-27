//! Apply of `ControlCommand::Search` (contract-w2 2.6, spec SearchAssign).
//! The accepting leader checked the caller's role (`service/search.rs`); apply
//! checks every record rule, writes only on success, and answers a refusal
//! as `ControlReply::Search(Refused | Conflict)`, which replays like any reply.
use super::*;
use crate::oltp_replication::{
    authority::control::search::{
        assignment_key, begin_move, binding, binding_key, cancel, member_key, model_key, publish,
        refused, valid_assignment, valid_member, valid_model,
    },
    search_rpc::{EmbedModel, SearchAssignment, SearchCommand, SearchControlReply, SearchMember, SearchRole},
};

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    unix_ms: u64,
    command: &SearchCommand,
) -> Result<ControlReply, ProtocolError> {
    let reply = match command {
        SearchCommand::Enroll(member) => enroll(tx, scope, state, member)?,
        SearchCommand::Retire { worker_id } => {
            if load::<SearchMember>(tx, &member_key(*worker_id))?.is_none() {
                refused("no such search member")
            } else {
                tx.delete(&member_key(*worker_id))?;
                state.enrollment_revision =
                    state.enrollment_revision.checked_add(1).ok_or_else(unknown)?;
                SearchControlReply::Done { epoch: 0 }
            }
        }
        SearchCommand::BeginMove {
            table_id,
            expected_epoch,
            mv,
        } => {
            let current = load::<SearchAssignment>(tx, &assignment_key(*table_id))?;
            let worker = load::<SearchMember>(tx, &member_key(mv.add))?;
            if !worker.is_some_and(|member| member.role == SearchRole::Worker) {
                refused("the added worker is not an enrolled search worker")
            } else {
                store(tx, begin_move(current, *table_id, *expected_epoch, *mv))?
            }
        }
        SearchCommand::Publish {
            table_id,
            expected_epoch,
        } => {
            let current = load::<SearchAssignment>(tx, &assignment_key(*table_id))?;
            store(tx, publish(current, *expected_epoch, unix_ms))?
        }
        SearchCommand::CancelMove {
            table_id,
            expected_epoch,
        } => {
            let current = load::<SearchAssignment>(tx, &assignment_key(*table_id))?;
            store(tx, cancel(current, *expected_epoch))?
        }
        SearchCommand::SetModel(model) => set_model(tx, model)?,
        SearchCommand::DropModel { name } => {
            if load::<EmbedModel>(tx, &model_key(name.as_str()))?.is_none() {
                refused("no such embedding model")
            } else {
                // The binding stays: the name keeps its endpoint, model and
                // dimensions for life.
                tx.delete(&model_key(name.as_str()))?;
                SearchControlReply::Done { epoch: 0 }
            }
        }
    };
    Ok(ControlReply::Search(reply))
}

fn store(
    tx: &mut GroupTransaction<'_>,
    next: Result<SearchAssignment, SearchControlReply>,
) -> Result<SearchControlReply, ProtocolError> {
    Ok(match next {
        Ok(next) if valid_assignment(&next) => {
            save(tx, &assignment_key(next.table_id), &next)?;
            SearchControlReply::Done { epoch: next.epoch }
        }
        Ok(_) => refused("the search assignment is too large"),
        Err(reply) => reply,
    })
}

/// A search worker or embedder of this database. Its id is never a data
/// worker's or an API's, and a later enrollment carries a larger revision.
fn enroll(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    member: &SearchMember,
) -> Result<SearchControlReply, ProtocolError> {
    let id = member.enrollment.worker_id;
    if !valid_member(member, &scope.database) {
        return Ok(refused("invalid search member"));
    }
    if tx.get(&key(WORKERS, id))?.is_some() || tx.get(&key(APIS, id))?.is_some() {
        return Ok(refused("the id is enrolled as a worker or an API"));
    }
    if let Some(old) = load::<SearchMember>(tx, &member_key(id))? {
        if old == *member {
            return Ok(SearchControlReply::Done { epoch: 0 });
        }
        if old.role != member.role
            || old.enrollment.enrollment_revision >= member.enrollment.enrollment_revision
        {
            return Ok(refused("the search member changed without a newer enrollment"));
        }
    }
    save(tx, &member_key(id), member)?;
    state.enrollment_revision = state.enrollment_revision.checked_add(1).ok_or_else(unknown)?;
    Ok(SearchControlReply::Done { epoch: 0 })
}

/// Q18: a model name is bound for life to its endpoint, model and
/// dimensions; its limits, hosts and credential reference may change.
fn set_model(
    tx: &mut GroupTransaction<'_>,
    model: &EmbedModel,
) -> Result<SearchControlReply, ProtocolError> {
    if !valid_model(model) {
        return Ok(refused("invalid embedding model"));
    }
    let bound = binding(model);
    match load::<Digest>(tx, &binding_key(model.name.as_str()))? {
        Some(old) if old != bound => {
            return Ok(refused("a model name is bound for life to its endpoint, model and dimensions"));
        }
        Some(_) => {}
        None => save(tx, &binding_key(model.name.as_str()), &bound)?,
    }
    save(tx, &model_key(model.name.as_str()), model)?;
    Ok(SearchControlReply::Done { epoch: 0 })
}
