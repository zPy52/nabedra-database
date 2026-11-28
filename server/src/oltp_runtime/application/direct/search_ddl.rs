//! Owner: J. The gateway side of `table.add_column`, `drop_column`,
//! `add_full_text`, `drop_full_text` and `table.search_status`
//! (contract-w2 2.6).
//!
//! Each is a Catalog step: the planner fences every Data group of the table
//! under its Exclusive lifecycle guard before the Catalog commit
//! (`planner::search_ddl`, L19), so the gateway sends them to the queued
//! path, which owns their outcome and its OD-3 rule (a request the queue
//! accepted never answers `not_applied` after a possible send). The gateway
//! adds no direct path for them.
use super::{DirectGateway, Routed};
use crate::protocol::{ProtocolError, Request};

pub(super) async fn route(
    _gateway: &DirectGateway,
    _request: &Request,
) -> Result<Routed, ProtocolError> {
    Ok(Routed::Queued)
}
