* Set `until`
* Call `active_compute().create_dataflow`
* Call `update_compute_base_read_policy` (via `set_index_options`)

```rust
let mut builder = txn.dataflow_builder(cluster_id);
let df = builder.build_index_dataflow(id)?;

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

self.set_index_options(id, options)
```