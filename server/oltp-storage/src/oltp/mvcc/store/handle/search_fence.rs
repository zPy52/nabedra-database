//! The DDL coordinator's two calls on a Data group of a search table
//! (contract-w2 2.6): a `SearchFence` (a compare-and-set on the group's
//! search epoch, L19), and a probe that reads the group's
//! search schema through its log. Both are proposals, so a remote handle
//! serves them like a local one.
use super::*;
use crate::oltp::mvcc::store::search::{SearchFence, SearchProbe, SearchReply, SearchSchema};

impl ParticipantHandle {
    /// Proposes `fence` under the group's current run authority (the field
    /// is overwritten). A run fence that moves between the read and the apply
    /// is retried twice.
    pub async fn search_fence(&self, mut fence: SearchFence) -> Result<SearchReply, ProtocolError> {
        let GroupKind::Data { table_id, .. } = self.identity().kind else {
            return Err(invalid());
        };
        if fence.table_id != table_id {
            return Err(invalid());
        }
        for tries in 1.. {
            fence.authority = self.epochs().await?.authority;
            let applied = self
                .propose(OwnedCommand::new_owned(
                    ParticipantCommand::SearchFence(fence.clone()),
                    self.resources(),
                    crate::oltp::pool::ByteClass::Progress,
                )?)
                .await?;
            match applied.reply {
                ParticipantReply::Search(SearchReply::Rejected(Fault::StaleAuthority))
                    if tries < 3 => {}
                ParticipantReply::Search(reply) => return Ok(reply),
                _ => return Err(invalid()),
            }
        }
        Err(invalid())
    }

    /// The group's search schema in log order, `None` before its first fence.
    pub async fn search_probe(&self) -> Result<Option<SearchSchema>, ProtocolError> {
        Ok(self.search_lifecycle_probe().await?.schema)
    }

    pub async fn search_lifecycle_probe(&self) -> Result<SearchProbe, ProtocolError> {
        let GroupKind::Data { table_id, .. } = self.identity().kind else {
            return Err(invalid());
        };
        let probe = SearchFence {
            authority: self.epochs().await?.authority,
            table_id,
            epoch: 0,
            expected_epoch: None,
            lifecycle: None,
            columns: Vec::new(),
            serving: Vec::new(),
            text: Vec::new(),
            names: Vec::new(),
        };
        match self.search_fence(probe).await? {
            SearchReply::Schema(schema) => Ok(*schema),
            _ => Err(invalid()),
        }
    }
}
