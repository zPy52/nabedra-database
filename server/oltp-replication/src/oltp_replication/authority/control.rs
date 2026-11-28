//! Metadata-only Control state and private leader service. Application requests,
//! rows, query text, and pending queue bodies never enter these commands.
mod data_worker;
mod direct;
mod endpoint;
mod features;
mod forwarding;
mod group_damage;
mod growth;
mod machine;
mod model;
mod placement;
mod pool_drain;
mod restart;
mod scoped_queue;
pub(crate) mod search;
mod service;
mod system_recovery;
mod worker_drain;

pub(crate) use data_worker::DataWorkerGrowthProof;
#[doc(hidden)]
pub use data_worker::DataWorkerIntent;
pub(crate) use direct::{ClusterFeatureState, DirectActivation, DirectFeatures, FeatureActivation, cluster_feature, feature_operation_id};
#[doc(hidden)]
pub use direct::{CLUSTER_FEATURES, DIRECT_BATCH_FEATURE, DIRECT_VERSION, DirectCapability, DirectCapabilityOperation, INDEX_RECLAIM_FEATURE, QUEUED_SNAPSHOT_RETRY_FEATURE, VECTOR_FEATURE};
pub(crate) use endpoint::ReplicaRoutes;
#[doc(hidden)]
pub use endpoint::{ControlClient, ControlEndpoint, ControlRoutes, PublishedControlRoutes};
#[doc(hidden)]
pub use features::{EnabledFeatures, spawn_watch as watch_cluster_features};
#[doc(hidden)]
pub use forwarding::{DataWorkerClient, ForwardedControl, HostedControlEndpoint};
pub(crate) use group_damage::{GroupDamageReason, GroupDamageRecord, GroupDamageReplacement, GroupDamageReplacementPlan, GroupDamageReport, PoolRefusal, MAX_ACTIVE_REPLACEMENTS as MAX_ACTIVE_DAMAGE_REPLACEMENTS};
#[doc(hidden)]
pub use group_damage::DirectCapabilityDamageGate;
#[allow(unused_imports)]
pub(crate) use group_damage::{GroupDamageGate, GroupDamagePhase};
#[doc(hidden)]
pub use machine::ControlMachine;
#[doc(hidden)]
pub use machine::{group_root, group_seed, group_step};
pub(crate) use model::*;
#[doc(hidden)]
pub use model::ControlCommand;
#[doc(hidden)]
pub use model::ApiEnrollment;
#[doc(hidden)]
pub use model::ControlReply;
#[doc(hidden)]
pub use model::Fault;
#[doc(hidden)]
pub use model::FencePending;
#[doc(hidden)]
pub use model::GroupRecord;
#[doc(hidden)]
pub use model::Pending;
#[doc(hidden)]
pub use model::ReadyChange;
#[doc(hidden)]
pub use model::ReadyPending;
#[doc(hidden)]
pub use model::ReadyPhase;
#[doc(hidden)]
pub use model::ScopedCatalogRead;
#[doc(hidden)]
pub use model::ServiceAction;
#[doc(hidden)]
pub use model::State;
pub(crate) use placement::{RemoteActivationAttestation, RemoteMoveView, RemotePlacementReply, RemoteRetirementAttestation, RemoteSourceBinding, ReplicaActivationEvidence, ReplicaMoveLineage, VerifiedRemoteActivation, VerifiedRemoteRetirement, decode_reply as decode_remote_placement_reply};
#[doc(hidden)]
pub use placement::{ReplicaMove, ReplicaMovePhase, ReplicaMoveRequest};
pub(crate) use pool_drain::{DrainMount, PoolDrainRecord, RetainedPoolDetachPermit};
#[doc(hidden)]
pub use pool_drain::PoolDrainPhase;
pub(crate) use worker_drain::{WorkerCloseAttestation, WorkerDrainRecord};
#[doc(hidden)]
pub use worker_drain::{WorkerDrainIntent, WorkerDrainPhase};
#[doc(hidden)]
pub use worker_drain::{EksReplacementAction, EksReplacementPermit, SignedEksReplacement};
pub(crate) use restart::RestartPreparation;
#[doc(hidden)]
pub use restart::{RestartIntent, RestartMountKey};
pub(crate) use scoped_queue::SCOPED_QUEUE_VERSION;
#[doc(hidden)]
pub use scoped_queue::{ScopedQueueCapability, ScopedQueuePlacement, ScopedQueueTransferReport, ScopedQueueTransition};
#[allow(unused_imports)]
#[doc(hidden)]
pub use scoped_queue::ScopedQueueInventory;
pub(crate) use service::VerifiedExclusion;
#[doc(hidden)]
pub use service::{ControlHandle, InventoryPage, LocalOperatorPermit, OperatorPolicy, ReplicaInventoryPage, VerifiedMembership, VerifiedQueueCopy};
#[doc(hidden)]
pub use service::IdlePasses;
pub(crate) use system_recovery::{EXECUTION_CAPABILITY as SYSTEM_REPLACEMENT_EXECUTION, HARDENING_CAPABILITY as SYSTEM_REPLACEMENT_HARDENING, ReplacementRecord, SystemRecoveryCommand};
#[doc(hidden)]
pub use system_recovery::{CAPABILITY as SYSTEM_REPLACEMENT_CAPABILITY, MAX_GROUPS as MAX_SYSTEM_REPLACEMENT_GROUPS, ReplacementGroup, ReplacementIntent, ReplacementMountProof};
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub mod tests;
