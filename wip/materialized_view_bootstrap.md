1. Set `as_of`
1. Set `until`
1. Call `active_compute().create_dataflow()`
1. Call `storage.create_collections()`
1. Call `initialize_compute_read_policies()`

```rust
self.controller
    .storage
    .create_collections(vec![(entry.id(), collection_desc)])
    .await
    .unwrap_or_terminate("cannot fail to create collections");

policies_to_set
    .entry(policy.expect("materialized views have a compaction window"))
    .or_insert_with(Default::default)
    .storage_ids
    .insert(entry.id());

// Re-create the sink on the compute instance.
let internal_view_id = self.allocate_transient_id()?;
let debug_name = self
    .catalog()
    .resolve_full_name(entry.name(), entry.conn_id())
    .to_string();

let mut builder = self.dataflow_builder(mview.cluster_id);
let mut df = builder.build_materialized_view(
    entry.id(),
    internal_view_id,
    debug_name,
    &mview.optimized_expr,
    &mview.desc,
)?;

// Note: ideally, the optimized_plan should be computed and
// set when the CatalogItem is re-constructed (in
// parse_item).
//
// However, it's not clear how exactly to change
// `load_catalog_items` to accomodate for the
// `build_materialized_view` call above.
self.catalog_mut()
    .set_optimized_plan(entry.id(), df.clone());

// The 'as_of' field of the dataflow changes after restart
let as_of = self.bootstrap_materialized_view_as_of(&df, mview.cluster_id);
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

let output_ids = df.export_ids().collect();
self.initialize_compute_read_policies(
    output_ids,
    instance,
    Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS),
)
.await;
```