//! IndexLifecycle's Control and Catalog steps as pure functions
//! (`specs/vector/lifecycle`).
use super::*;
use crate::protocol::{SearchOptions, TableFormat};

fn descriptor() -> TableDescriptor {
    let mut search = SearchOptions::default();
    search.full_text.insert("title".into(), "english".into());
    search.embed.insert(
        "e".into(),
        Embed {
            source: "body".into(),
            model: "m1".into(),
        },
    );
    TableDescriptor {
        id: nabedra_rt::ids::uuid_v4(),
        name: "docs".into(),
        schema: [
            ("id", ColumnType::Utf8),
            ("body", ColumnType::Utf8),
            ("title", ColumnType::Utf8),
            ("v", ColumnType::Vector(3)),
            ("e", ColumnType::Vector(2)),
            ("s", ColumnType::SparseVector),
            ("m", ColumnType::VectorList(4)),
        ]
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect(),
        key: "id".into(),
        format: TableFormat::Parquet,
        search,
    }
}
fn dense(dim: u32) -> SearchColumn {
    vector_column(ColumnType::Vector(dim as u16), None).unwrap()
}
fn ids(search: &CatalogSearch) -> Vec<(String, ColumnId)> {
    search
        .columns
        .iter()
        .map(|column| (column.name.as_str().to_owned(), column.id))
        .collect()
}

/// `table.create`: ids 1, 2, ... in name order, vector columns first, all
/// published; the fence serves them all and names the embed source.
#[test]
fn vs14_a_created_table_publishes_every_column_with_fresh_ids() {
    let search = CatalogSearch::of_descriptor(&descriptor()).unwrap().unwrap();
    assert_eq!(
        ids(&search),
        [("e", 1), ("m", 2), ("s", 3), ("v", 4), ("title", 5)].map(|(n, i)| (n.to_owned(), i))
    );
    assert_eq!(search.high, 5);
    let template = search.template(None);
    assert_eq!(template.serving, vec![1, 2, 3, 4]);
    assert_eq!(template.columns[0].source.as_ref().unwrap().as_str(), "body");
    assert_eq!(
        template.columns.iter().map(|c| (c.kind, c.dim)).collect::<Vec<_>>(),
        [(VectorKind::Dense, 2), (VectorKind::Multi, 4), (VectorKind::Sparse, 0), (VectorKind::Dense, 3)]
    );
    assert_eq!(template.text.len(), 1);
    assert!(template.text[0].serving);
    // A plain table has none.
    let mut plain = descriptor();
    plain.schema.retain(|_, ty| ty.is_scalar());
    plain.search = SearchOptions::default();
    assert_eq!(CatalogSearch::of_descriptor(&plain).unwrap(), None);
}

/// IndexLifecycle `AddColumn`, `StartDrop` and a re-add: a building column is
/// writable and not served; a dropped name leaves every group; a re-added
/// name is a new incarnation that never serves the old one.
#[test]
fn vs14_add_drop_and_re_add_make_new_incarnations() {
    let search = CatalogSearch::empty();
    let one = search.add_vector("v", dense(2), ColumnLife::Building).unwrap();
    assert_eq!(one.template(None).columns.len(), 1);
    assert!(one.template(None).serving.is_empty(), "building is not served");
    assert!(one.add_vector("v", dense(2), ColumnLife::Building).is_err());
    let published = one.publish(1).unwrap();
    assert_eq!(published.template(None).serving, vec![1]);
    assert!(published.publish(1).is_err(), "published once");
    let dropped = published.drop_named("v", false).unwrap();
    assert!(dropped.template(None).columns.is_empty());
    assert!(dropped.publish(1).is_err(), "a dropped incarnation never publishes");
    let again = dropped.add_vector("v", dense(2), ColumnLife::Building).unwrap();
    assert_eq!(ids(&again), [("v".to_owned(), 2)]);
    // Full text by the same rules, in the same id space.
    let text = again.add_text("v", "english", ColumnLife::Building).unwrap();
    assert_eq!(text.named("v", true).unwrap().id, 3);
    assert!(!text.template(None).text[0].serving);
    assert!(text.drop_named("v", true).unwrap().named("v", false).is_some());
    // At most eight columns of both kinds.
    let mut full = CatalogSearch::empty();
    for i in 0..8 {
        full = full.add_vector(&format!("c{i}"), dense(2), ColumnLife::Ready).unwrap();
    }
    assert!(full.add_text("t", "english", ColumnLife::Building).is_err());
}

/// IndexLifecycle `AllReady`, red for mutants `publish_before_all_ready`
/// (one ready range is enough) and `report_without_column` (a report counts
/// for a column it does not name).
#[test]
fn vs14_a_column_publishes_only_when_every_range_reported_it_ready() {
    let (a, b) = (nabedra_rt::ids::uuid_v4(), nabedra_rt::ids::uuid_v4());
    let range = |group_id, ready: Vec<ColumnId>| RangeReadiness {
        group_id,
        ready,
        ..RangeReadiness::default()
    };
    let groups = [a, b];
    assert!(!all_ready(&groups, &[range(a, vec![3])], 3), "one range of two");
    assert!(!all_ready(&groups, &[range(a, vec![3]), range(b, vec![2])], 3), "another incarnation");
    assert!(!all_ready(&groups, &[range(a, vec![3]), range(b, Vec::new())], 3), "syncing");
    let stranger = nabedra_rt::ids::uuid_v4();
    assert!(!all_ready(&groups, &[range(a, vec![3]), range(stranger, vec![3])], 3), "a range of other routing");
    assert!(all_ready(&groups, &[range(b, vec![2, 3]), range(a, vec![3])], 3));
    assert!(!all_ready(&[], &[], 3), "a table without ranges never publishes");
}

/// DB-20 at the coordinator: the next epoch is above the Catalog's and every
/// group's, and new ids are above every id a group saw, whatever an
/// interrupted DDL left behind.
#[test]
fn vs14_the_next_epoch_and_id_are_above_every_group() {
    let schema = |epoch, high| SearchSchema {
        epoch,
        columns: Vec::new(),
        high,
        serving: Vec::new(),
        text: Vec::new(),
    };
    for seed in 0..64u64 {
        let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % 9
        };
        let catalog = next();
        let found: Vec<Option<SearchSchema>> = (0..next())
            .map(|_| (next() > 2).then(|| schema(next(), next() as u16)))
            .collect();
        let epoch = found
            .iter()
            .flatten()
            .map(|s| s.epoch)
            .fold(catalog, u64::max)
            + 1;
        let high = found.iter().flatten().map(|s| s.high).fold(1, u16::max);
        assert_eq!(epochs(&found).next_epoch(catalog).unwrap(), epoch, "seed {seed}");
        let mut base = CatalogSearch::empty();
        base.high = 1;
        let raised = epochs(&found).raise_high(base);
        assert_eq!(raised.high, high, "seed {seed}");
        let added = raised.add_vector("n", dense(2), ColumnLife::Building).unwrap();
        assert!(found.iter().flatten().all(|s| added.high > s.high), "seed {seed}");
    }
}
/// Groups with these schemas and no handles (the pure helpers read only the
/// schemas).
fn epochs(found: &[Option<SearchSchema>]) -> SchemasOnly {
    SchemasOnly(found.to_vec())
}
struct SchemasOnly(Vec<Option<SearchSchema>>);
impl SchemasOnly {
    fn next_epoch(&self, catalog: u64) -> Result<u64, ProtocolError> {
        next_epoch(self.0.iter().flatten(), catalog)
    }
    fn raise_high(&self, search: CatalogSearch) -> CatalogSearch {
        raise_high(self.0.iter().flatten(), search)
    }
}
