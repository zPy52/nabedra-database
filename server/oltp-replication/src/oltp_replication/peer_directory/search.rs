//! Search processes in the applied peer directory (contract-w2 2.1): their
//! enrollment roles, the lanes each may open, and the role every search
//! endpoint checks (`RoleCheck`).
use super::*;
use crate::oltp_replication::{
    authority::control::search::{MAX_MEMBERS_PAGE, SearchRequest},
    search_rpc::{CallerRole, RoleCheck, SearchMember, SearchRead, SearchRole},
};

/// Whether replacing an entry of role `old` with `new` is refused: a worker
/// never becomes an API (and back), and a search worker or an embedder never
/// changes its kind of enrollment.
pub(super) fn relabels(old: &Role, new: &Role) -> bool {
    let search = |role: &Role| match role {
        Role::Search(_) => Some(true),
        Role::Embed(_) => Some(false),
        _ => None,
    };
    matches!(
        (old, new),
        (Role::Worker, Role::Api(_)) | (Role::Api(_), Role::Worker) | (Role::BootstrapControl(_), Role::Api(_))
    ) || ((search(old).is_some() || search(new).is_some()) && search(old) != search(new))
}
pub(super) fn role_bytes(role: &Role) -> usize {
    match role {
        Role::Worker | Role::PreparedControl => 0,
        Role::Api(database) | Role::Search(database) | Role::Embed(database) => {
            database.capacity_bytes()
        }
        Role::BootstrapControl(group) => group.database.capacity_bytes(),
    }
}

/// The 2.1 lanes of a search worker and an embedder; `None` for other roles.
/// A search worker reads its database's Data and Catalog groups and talks to
/// Control; an embedder talks to its Data leaders and Control. Neither may
/// open a Raft, queue or application lane, so neither can propose.
pub(super) fn verify(role: &Role, group: &GroupIdentity, purpose: Purpose) -> Option<bool> {
    let (database, data_lane, catalog) = match role {
        Role::Search(database) => (database, Purpose::SearchFeed, true),
        Role::Embed(database) => (database, Purpose::SearchEmbed, false),
        // A gateway queries search workers, which bind their query endpoint
        // under the Control identity.
        Role::Api(database) => {
            return (group.kind == GroupKind::Control
                && group.database == *database
                && purpose == Purpose::SearchQuery)
                .then_some(true);
        }
        _ => return None,
    };
    if group.database != *database {
        return Some(false);
    }
    Some(match group.kind {
        GroupKind::Control => purpose == Purpose::Authority,
        GroupKind::Data { .. } => purpose == data_lane,
        GroupKind::Catalog => catalog && purpose == Purpose::SearchFeed,
        _ => false,
    })
}

impl AppliedPeerDirectory {
    /// Installs every enrolled search member, paged like the workers.
    pub(super) async fn refresh_members(
        &self,
        control: &ControlClient,
        epoch: u64,
    ) -> Result<(), ProtocolError> {
        let mut after = None;
        loop {
            let read = SearchRequest::Read(SearchRead::Members {
                after,
                max: MAX_MEMBERS_PAGE,
            });
            let response = control.search(read).await?;
            let (page, _owner) = self.decode::<SearchMember>(response).await?;
            for member in page.items.iter() {
                let id = member.enrollment.worker_id;
                if after.is_some_and(|old| id <= old) {
                    return Err(invalid());
                }
                after = Some(id);
                let role = match member.role {
                    SearchRole::Worker => Role::Search(member.database.clone()),
                    SearchRole::Embedder => Role::Embed(member.database.clone()),
                };
                self.install(member.enrollment.clone(), role, epoch)?;
            }
            if page.after != after {
                return Err(invalid());
            }
            if page.done {
                return Ok(());
            }
        }
    }
}

/// The role every search endpoint checks before it reads a payload. An
/// operator of the system trust is a `Worker`; a revoked or unknown
/// identity, or a certificate that differs from the enrollment, has none.
impl RoleCheck for AppliedPeerDirectory {
    fn role(&self, caller: &WorkerIdentity) -> Option<CallerRole> {
        if !self.trust_available() || self.denied(caller.worker_id, Some(caller.certificate_sha256)) {
            return None;
        }
        if self.current_operator(*caller) == Some(true) {
            return Some(CallerRole::Worker);
        }
        let entries = self.entries.read().ok()?;
        let entry = entries.get(&caller.worker_id)?;
        if WorkerIdentity::from(&entry.enrollment) != *caller {
            return None;
        }
        match &entry.role {
            Role::Worker => Some(CallerRole::Worker),
            Role::Api(database) => Some(CallerRole::Api(database.clone())),
            Role::Search(database) => Some(CallerRole::Search(database.clone())),
            Role::Embed(database) => Some(CallerRole::Embed(database.clone())),
            Role::BootstrapControl(_) | Role::PreparedControl => None,
        }
    }
}

#[cfg(test)]
mod tests;
