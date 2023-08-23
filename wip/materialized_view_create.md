* Set `until`
* Call `active_compute().create_dataflow()`
* Call `initialize_compute_read_policies()`

```rust
// Pick the least valid read timestamp as the as-of for the view
// dataflow. This makes the materialized view include the maximum possible
// amount of historical detail.
let id_bundle = self
    .index_oracle(cluster_id)
    .sufficient_collections(&expr_depends_on);
let as_of = self.least_valid_read(&id_bundle);

// Create a dataflow that materializes the view query and sinks
// it to storage.
let CatalogItem::MaterializedView(mv) = txn.catalog.get_entry(&id).item() else {
    unreachable!()
};

let mut builder = txn.dataflow_builder(cluster_id);
let df = builder.build_materialized_view(
    id,
    internal_view_id,
    debug_name,
    &mv.optimized_expr,
    &mv.desc,
)?;

// Announce the creation of the materialized view source.
self.controller
    .storage
    .create_collections(vec![(
        id,
        CollectionDescription {
            desc,
            data_source: DataSource::Other(DataSourceOther::Compute),
            since: Some(as_of.clone()),
            status_collection_id: None,
        },
    )])
    .await
    .unwrap_or_terminate("cannot fail to append");

self.initialize_storage_read_policies(
    vec![id],
    Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS),
)
.await;


df.set_as_of(as_of);

// If the only outputs of the dataflow are sinks, we might
// be able to turn off the computation early, if they all
// have non-trivial `up_to`s.
if df.index_exports.is_empty() {
    df.until = Antichain::from_elem(Timestamp::MIN);
    for (_, sink) in &df.sink_exports {
        df.until.join_assign(&sink.up_to);
    }
}

let plan = self.finalize_dataflow(df, instance)?;

self.controller
    .active_compute()
    .create_dataflow(instance, plan)
    .unwrap_or_terminate("dataflow creation cannot fail");

self.initialize_compute_read_policies(
    df.export_ids().collect(),
    instance,
    Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS),
)
.await;
```