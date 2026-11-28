//! UUID-preserving metadata creation for an isolated offline import target.
//! No public ORM operation accepts a caller-selected immutable table UUID.
use super::*;
impl NativePlanner {
    pub async fn plan_import_table(
        &self,
        descriptor: TableDescriptor,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        descriptor.validate()?;
        if descriptor.id.is_nil() {
            return Err(failure(ErrorCode::ValidationError, "import table UUID is nil").into());
        }
        let name = nabedra_oltp_types::types::Text::try_from(descriptor.name.as_str())
            .map_err(|_| not_found())?;
        let (name_version, existing) = value(
            self.catalog
                .read::<NameRecord>(pin, MvccKey::CatalogName { name })
                .await?,
        )?;
        let (id_version, by_id) = value(self.catalog.describe(pin, descriptor.id).await?)?;
        if existing.is_some() || by_id.is_some() || name_version.is_some() || id_version.is_some() {
            return Err(failure(
                ErrorCode::Conflict,
                "offline import target already contains table history",
            )
            .into());
        }
        let id = descriptor.id;
        let guard = self
            .guard(pin, txn, request, id, None, 1, GuardMode::Exclusive)
            .await?;
        let result = async {
            let route = self.resolver.create_data(&descriptor, pin).await?;
            let page = derived_uuid(b"nabedra/import-route-page/v1", id, txn.request_id);
            let writes = self
                .catalog
                .create_writes(descriptor.clone(), route, page, &guard, None)?;
            let epochs = self.catalog.participant().epochs().await?;
            Ok(MutationPlan {
                participants: vec![ParticipantWrites {
                    participant: self.catalog.participant().clone(),
                    schema_epoch: epochs.schema_epoch,
                    routing_epoch: epochs.routing_epoch,
                    writes,
                    guards: vec![guard.clone().into_sealed()],
                }],
                lifecycle: vec![guard.clone()],
                result: serde_json::to_value(descriptor).map_err(|_| not_found())?,
                _owner: owner,
            })
        }
        .await;
        result.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }
}
