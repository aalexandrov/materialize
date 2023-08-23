* Set `as_of`
* Call `active_compute().create_dataflow`

```rust
let mut dataflow = self
    .dataflow_builder(idx.cluster_id)
    .build_index_dataflow(entry.id())?;
let as_of = self.bootstrap_index_as_of(
    &dataflow,
    idx.cluster_id,
    idx.is_retained_metrics_object,
);
dataflow.set_as_of(as_of);

// What follows is morally equivalent to `self.ship_dataflow(df, idx.cluster_id)`,
// but we cannot call that as it will also downgrade the read hold on the index.
policy_entry
    .compute_ids
    .entry(idx.cluster_id)
    .or_insert_with(Default::default)
    .extend(dataflow.export_ids());
let dataflow_plan = self.must_finalize_dataflow(dataflow, idx.cluster_id);
self.controller
    .active_compute()
    .create_dataflow(idx.cluster_id, dataflow_plan)
    .unwrap_or_terminate("cannot fail to create dataflows");
```