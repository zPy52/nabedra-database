//! contract-w2 2.1 at the peer directory: the lanes a search worker and an
//! embedder may open, their roles never change kind, and `RoleCheck` answers
//! only for the enrolled identity.
use super::*;
use crate::oltp::pool::SharedResourceConfig;
use nabedra_resources::config::WorkerResources;

const PURPOSES: [Purpose; 22] = [
    Purpose::Raft,
    Purpose::Snapshot,
    Purpose::Authority,
    Purpose::QueueControl,
    Purpose::QueueData,
    Purpose::Storage,
    Purpose::ApiIngress,
    Purpose::ApiObservation,
    Purpose::Settlement,
    Purpose::ServiceMutation,
    Purpose::GroupLifecycle,
    Purpose::ControlDelegation,
    Purpose::ShardSplit,
    Purpose::ShardSplitProof,
    Purpose::ShardSplitImage,
    Purpose::ReplicaPlacement,
    Purpose::RaftProgress,
    Purpose::GroupDamage,
    Purpose::SystemRecovery,
    Purpose::SearchFeed,
    Purpose::SearchQuery,
    Purpose::SearchEmbed,
];
fn text<const N: usize>(value: &str) -> Text<N> {
    Text::try_from(value).unwrap()
}
fn group(database: &str, kind: GroupKind) -> GroupIdentity {
    GroupIdentity {
        cluster_id: Uuid::from_u128(1),
        database: text(database),
        group_id: nabedra_rt::ids::uuid_v4(),
        kind,
        group_format_version: 1,
    }
}
fn kinds() -> [GroupKind; 5] {
    [
        GroupKind::Control,
        GroupKind::Catalog,
        GroupKind::Data {
            table_id: nabedra_rt::ids::uuid_v4(),
            shard_id: nabedra_rt::ids::uuid_v4(),
        },
        GroupKind::Status { bucket: 0 },
        GroupKind::Timestamps,
    ]
}
fn enrollment(revision: u64) -> WorkerEnrollment {
    WorkerEnrollment {
        worker_id: Uuid::from_u128(7),
        incarnation: Uuid::from_u128(revision as u128),
        certificate_sha256: Digest::from_bytes([3; 32]),
        endpoint: text("127.0.0.1:1234"),
        server_name: text("localhost"),
        enrollment_revision: revision,
    }
}

/// VS11 (2.1 matrix, lanes): a search worker opens only SearchFeed to the
/// Data and Catalog groups of its database and Authority to Control; an
/// embedder only SearchEmbed to Data and Authority to Control; a gateway only
/// SearchQuery under Control. No search role reaches Raft, queues, storage
/// or application lanes, or another database.
#[test]
fn vs11_search_roles_open_only_their_lanes() {
    for kind in kinds() {
        for purpose in PURPOSES {
            for database in ["db", "other"] {
                let group = group(database, kind.clone());
                let own = database == "db";
                let search = verify(&Role::Search(text("db")), &group, purpose);
                let embed = verify(&Role::Embed(text("db")), &group, purpose);
                let api = verify(&Role::Api(text("db")), &group, purpose);
                let want_search = own
                    && matches!(
                        (&kind, purpose),
                        (GroupKind::Control, Purpose::Authority)
                            | (GroupKind::Data { .. } | GroupKind::Catalog, Purpose::SearchFeed)
                    );
                let want_embed = own
                    && matches!(
                        (&kind, purpose),
                        (GroupKind::Control, Purpose::Authority) | (GroupKind::Data { .. }, Purpose::SearchEmbed)
                    );
                assert_eq!(search, Some(want_search), "search {kind:?} {purpose:?} {database}");
                assert_eq!(embed, Some(want_embed), "embed {kind:?} {purpose:?} {database}");
                // A gateway's search lane is the only one this hook answers;
                // every other lane of an API keeps the existing rules.
                let want_api = (own && kind == GroupKind::Control && purpose == Purpose::SearchQuery).then_some(true);
                assert_eq!(api, want_api, "api {kind:?} {purpose:?} {database}");
            }
        }
    }
    assert_eq!(verify(&Role::Worker, &group("db", GroupKind::Catalog), Purpose::SearchFeed), None);
}

/// An enrollment keeps its kind: a search worker never becomes an embedder,
/// a data worker or an API, and none of those becomes a search member.
#[test]
fn vs11_a_search_enrollment_never_changes_kind() {
    let roles = [
        Role::Worker,
        Role::Api(text("db")),
        Role::Search(text("db")),
        Role::Embed(text("db")),
    ];
    for (a, old) in roles.iter().enumerate() {
        for (b, new) in roles.iter().enumerate() {
            let search = |role: &Role| matches!(role, Role::Search(_) | Role::Embed(_));
            if search(old) || search(new) {
                assert_eq!(relabels(old, new), a != b, "role {a} -> role {b}");
            }
        }
    }
    // The existing rules hold.
    assert!(relabels(&Role::Worker, &Role::Api(text("db"))));
    assert!(!relabels(&Role::Worker, &Role::Worker));
}

/// `RoleCheck`: the role of the enrolled identity, none for another
/// certificate or incarnation, a revoked identity, or a bootstrap peer.
#[test]
fn vs11_role_check_answers_only_for_the_enrolled_identity() {
    let shared = WorkerSharedResources::new(1, WorkerResources::default(), SharedResourceConfig::default()).unwrap();
    let control = group("db", GroupKind::Control);
    let mut peer = enrollment(1);
    peer.worker_id = Uuid::from_u128(99);
    let directory = AppliedPeerDirectory::from_bootstrap(control, vec![peer.clone()], shared).unwrap();
    assert_eq!(directory.role(&WorkerIdentity::from(&peer)), None, "a bootstrap peer");
    let member = enrollment(1);
    directory.install(member.clone(), Role::Search(text("db")), 1).unwrap();
    let identity = WorkerIdentity::from(&member);
    assert_eq!(directory.role(&identity), Some(CallerRole::Search(text("db"))));
    let mut forged = identity;
    forged.certificate_sha256 = Digest::from_bytes([4; 32]);
    assert_eq!(directory.role(&forged), None, "another certificate");
    // The same id may not come back as an embedder, even newer.
    assert!(directory.install(enrollment(2), Role::Embed(text("db")), 1).is_err());
    assert_eq!(directory.role(&identity), Some(CallerRole::Search(text("db"))));
    directory.install_revocations(vec![identity]);
    assert_eq!(directory.role(&identity), None, "revoked");
}
