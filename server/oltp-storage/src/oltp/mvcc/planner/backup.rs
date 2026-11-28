//! Logical backup traversal reads every catalog, route and row at one retained
//! timestamp. Unresolved older intents are helped outside native execution.
use super::*;
use crate::oltp::mvcc::{
    catalog::decode_canonical,
    index::{IndexPage, IndexPhase, IndexRecord, IndexRoot},
    store::scan::{ScanDomain, ScanPage, ScanRequest},
};
use crate::{
    migration_interchange::{ArchiveSummary, ArchiveWriter, IndexDescriptor},
    oltp::{backup::BackupBackend, pool::ByteClass},
};
use nabedra_oltp_types::types::Digest;
use std::{fs::File, path::PathBuf};
impl NativePlanner {
    pub(crate) async fn export_logical_snapshot(
        &self,
        pin: &PinnedCatalog,
        mut writer: ArchiveWriter<File>,
        help: &dyn BackupBackend,
    ) -> Result<(File, ArchiveSummary), ProtocolError> {
        let owner = Arc::new(
            self.catalog
                .resources()
                .reserve_scratch(ByteClass::Data, 64 << 20)?,
        );
        let (mut after, mut after_owner) = (None, None);
        loop {
            let (scan_after, scan_owner) = crate::oltp::mvcc::store::scan::clone_scan_cursor(
                self.catalog.resources(),
                &after,
                &after_owner,
            )?;
            let names = self
                .catalog
                .participant()
                .scan_page(ScanRequest {
                    domain: ScanDomain::CatalogNames,
                    read_ts: pin.read_ts,
                    after: scan_after,
                    after_owner: scan_owner,
                    lower: None,
                    upper: None,
                    max_items: 128,
                    max_bytes: 1 << 20,
                })
                .await?;
            match names.payload()? {
                ScanPage::Blocked { txn, manifest } => {
                    help.help(IntentRef {
                        txn: *txn,
                        manifest: *manifest,
                    })
                    .await?;
                    continue;
                }
                ScanPage::Records { records, exhausted } => {
                    for name in records.iter().filter_map(|record| record.value.clone()) {
                        let name: OwnedRecord<NameRecord> =
                            decode_canonical(self.catalog.resources(), name).await?;
                        let table =
                            backup_value(|| self.catalog.describe(pin, name.table_id), help)
                                .await?
                                .ok_or_else(not_found)?;
                        let descriptor = table.descriptor.clone();
                        writer = self
                            .cpu(owner.clone(), move || {
                                writer.begin_table(descriptor)?;
                                Ok(writer)
                            })
                            .await?;
                        let root =
                            backup_value(|| self.catalog.routing_root(pin, name.table_id), help)
                                .await?
                                .ok_or_else(not_found)?;
                        let mut next = root.first_page;
                        while let Some(page_id) = next {
                            let page = backup_value(
                                || self.catalog.routing_page(pin, name.table_id, page_id),
                                help,
                            )
                            .await?
                            .ok_or_else(not_found)?;
                            for route in page.shards.iter() {
                                let participant = self.resolver.participant(&route.group).await?;
                                let (mut cursor, mut cursor_owner) = (None, None);
                                loop {
                                    let (scan_cursor, scan_owner) =
                                        crate::oltp::mvcc::store::scan::clone_scan_cursor(
                                            self.catalog.resources(),
                                            &cursor,
                                            &cursor_owner,
                                        )?;
                                    let response = participant
                                        .scan_page(ScanRequest {
                                            domain: ScanDomain::Rows {
                                                table_id: name.table_id,
                                            },
                                            read_ts: pin.read_ts,
                                            after: scan_cursor,
                                            after_owner: scan_owner,
                                            lower: route.span.start.clone().map(|key| (key, true)),
                                            upper: route.span.end.clone().map(|key| (key, false)),
                                            max_items: 128,
                                            max_bytes: 32 << 20,
                                        })
                                        .await?;
                                    match response.payload()? {
                                        ScanPage::Blocked { txn, manifest } => {
                                            help.help(IntentRef {
                                                txn: *txn,
                                                manifest: *manifest,
                                            })
                                            .await?
                                        }
                                        ScanPage::Records { records, exhausted } => {
                                            for raw in records
                                                .iter()
                                                .filter_map(|record| record.value.clone())
                                            {
                                                let decoded: OwnedRecord<StoredRow> =
                                                    decode_canonical(self.catalog.resources(), raw)
                                                        .await?;
                                                writer = self
                                                    .cpu(owner.clone(), move || {
                                                        writer.row(decoded.0.clone())?;
                                                        Ok(writer)
                                                    })
                                                    .await?;
                                            }
                                            if *exhausted {
                                                break;
                                            }
                                            cursor = Some(
                                                records.last().ok_or_else(not_found)?.key.clone(),
                                            );
                                            cursor_owner = Some(response.decode_owner()?);
                                        }
                                    }
                                }
                            }
                            next = page.next_page;
                        }
                        // Only published definitions enter logical restore.
                        // Building records remain maintained in the source but
                        // do not authorize queries at this backup cut.
                        let mut next = backup_value(
                            || {
                                self.catalog.read::<IndexRoot>(
                                    pin,
                                    MvccKey::CatalogIndexesRoot {
                                        table_id: name.table_id,
                                    },
                                )
                            },
                            help,
                        )
                        .await?
                        .and_then(|root| root.first_page);
                        while let Some(page_id) = next {
                            let page = backup_value(
                                || {
                                    self.catalog.read::<IndexPage>(
                                        pin,
                                        MvccKey::CatalogIndexesPage {
                                            table_id: name.table_id,
                                            page_id,
                                        },
                                    )
                                },
                                help,
                            )
                            .await?
                            .ok_or_else(not_found)?;
                            for id in page.indexes.iter() {
                                let record = backup_value(
                                    || {
                                        self.catalog.read::<IndexRecord>(
                                            pin,
                                            MvccKey::CatalogIndex {
                                                table_id: name.table_id,
                                                index_id: *id,
                                            },
                                        )
                                    },
                                    help,
                                )
                                .await?
                                .ok_or_else(not_found)?;
                                if !matches!(record.phase, IndexPhase::Ready { .. }) {
                                    continue;
                                }
                                writer = self
                                    .cpu(owner.clone(), move || {
                                        writer.index(IndexDescriptor {
                                            index_id: record.definition.index_id,
                                            name: record.definition.name.as_str().into(),
                                            columns: record
                                                .definition
                                                .columns
                                                .iter()
                                                .map(|c| c.as_str().into())
                                                .collect(),
                                            covering: record
                                                .definition
                                                .covering
                                                .iter()
                                                .map(|c| c.as_str().into())
                                                .collect(),
                                        })?;
                                        Ok(writer)
                                    })
                                    .await?;
                            }
                            next = page.next_page;
                        }
                        writer = self
                            .cpu(owner.clone(), move || {
                                writer.end_table()?;
                                Ok(writer)
                            })
                            .await?;
                    }
                    if *exhausted {
                        break;
                    }
                    after = Some(records.last().ok_or_else(not_found)?.key.clone());
                    after_owner = Some(names.decode_owner()?);
                }
            }
        }
        self.cpu(owner, move || writer.finish()).await
    }
    pub async fn hash_backup_file(&self, path: PathBuf) -> Result<Digest, ProtocolError> {
        let owner = Arc::new(
            self.catalog
                .resources()
                .reserve_scratch(ByteClass::Data, 128 << 10)?,
        );
        let mut file = self
            .cpu(owner.clone(), move || {
                File::open(path).map_err(|_| not_found())
            })
            .await?;
        let mut hash = Sha256::new();
        loop {
            let (next_file, next_hash, done) = self
                .cpu(owner.clone(), move || {
                    let done = crate::oltp::backup::hash_chunk(&mut file, &mut hash)?;
                    Ok((file, hash, done))
                })
                .await?;
            file = next_file;
            hash = next_hash;
            if done {
                return Ok(Digest::from_bytes(hash.finalize().into()));
            }
        }
    }
}
async fn backup_value<T, F, Fut>(
    mut fetch: F,
    help: &dyn BackupBackend,
) -> Result<Option<OwnedRecord<T>>, ProtocolError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<CatalogRead<T>, ProtocolError>>,
{
    loop {
        match fetch().await? {
            CatalogRead::Value { record, .. } => return Ok(record),
            CatalogRead::Blocked(intent) => help.help(intent).await?,
        }
    }
}
