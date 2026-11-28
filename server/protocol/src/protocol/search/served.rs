//! Which search operations this build serves (contract-w2 4.2). One line per
//! operation; the task that implements it flips its line and nothing else:
//! I owns `RANK` and `RANK_MANY`, J the column operations and `SEARCH_STATUS`.
//! K adds `EMBEDDING_STATUS` with `table.embedding_status` (2.5).
//!
//! An operation that is not served is refused with `UnsupportedOperation`
//! before any routing, on the direct and the queued path alike.

pub(super) const ADD_COLUMN: bool = true; // J
pub(super) const DROP_COLUMN: bool = true; // J
pub(super) const ADD_FULL_TEXT: bool = true; // J
pub(super) const DROP_FULL_TEXT: bool = true; // J
pub(super) const SEARCH_STATUS: bool = true; // J
pub(super) const RANK: bool = false; // I
pub(super) const RANK_MANY: bool = false; // I
