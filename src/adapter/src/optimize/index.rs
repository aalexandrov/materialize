// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `CREATE INDEX` statements.

use std::marker::PhantomData;
use std::sync::Arc;

use maplit::btreemap;
use mz_compute_types::dataflows::IndexDesc;
use mz_compute_types::plan::Plan;
use mz_compute_types::ComputeInstanceId;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::names::QualifiedItemName;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::optimizer_notices::OptimizerNotice;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use timely::progress::Antichain;

use crate::catalog::Catalog;
use crate::coord::dataflows::{
    prep_relation_expr, prep_scalar_expr, ComputeInstanceSnapshot, DataflowBuilder, ExprPrepStyle,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizerConfig, OptimizerError,
};
use crate::CollectionIdBundle;

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    _typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A durable GlobalId to be used with the exported index arrangement.
    exported_index_id: GlobalId,
    // Optimizer config.
    config: OptimizerConfig,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        exported_index_id: GlobalId,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            _typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            exported_index_id,
            config,
        }
    }

    pub fn cluster_id(&self) -> ComputeInstanceId {
        self.compute_instance.instance_id()
    }
}

/// A wrapper of index parts needed to start the optimization process.
pub struct Index {
    name: QualifiedItemName,
    on: GlobalId,
    keys: Vec<mz_expr::MirScalarExpr>,
}

impl Index {
    pub fn new(
        name: &QualifiedItemName,
        on: &GlobalId,
        keys: &Vec<mz_expr::MirScalarExpr>,
    ) -> Self {
        Self {
            name: name.clone(),
            on: on.clone(),
            keys: keys.clone(),
        }
    }
}

/// The (sealed intermediate) result after:
///
/// 1. embedding an [`Index`] into a [`MirDataflowDescription`],
/// 2. transitively inlining referenced views, and
/// 3. jointly optimizing the `MIR` plans in the [`MirDataflowDescription`].
#[derive(Clone)]
pub struct GlobalMirPlan {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl GlobalMirPlan {
    pub fn df_desc(&self) -> &MirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }
}

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
#[derive(Clone)]
pub struct GlobalLirPlan<T: Clone> {
    df_desc: LirDataflowDescription,
    df_meta: DataflowMetainfo,
    phantom: PhantomData<T>,
}

impl<T: Clone> GlobalLirPlan<T> {
    pub fn df_desc(&self) -> &LirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }

    /// Computes the [`CollectionIdBundle`] of the wrapped dataflow.
    pub fn id_bundle(&self, compute_instance_id: ComputeInstanceId) -> CollectionIdBundle {
        let storage_ids = self.df_desc.source_imports.keys().copied().collect();
        let compute_ids = self.df_desc.index_imports.keys().copied().collect();
        CollectionIdBundle {
            storage_ids,
            compute_ids: btreemap! {compute_instance_id => compute_ids},
        }
    }
}

/// Marker type for [`GlobalLirPlan`] structs representing an optimization
/// result without a resolved timestamp.
#[derive(Clone)]
pub struct Unresolved;

/// Marker type for [`GlobalLirPlan`] structs representing an optimization
/// result with a resolved timestamp.
///
/// The actual timestamp value is set in the [`LirDataflowDescription`] of the
/// surrounding [`GlobalLirPlan`] when we call `resolve()`.
#[derive(Clone)]
pub struct Resolved;

impl Optimize<Index> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, index: Index) -> Result<Self::To, OptimizerError> {
        let state = self.catalog.state();
        let on_entry = state.get_entry(&index.on);
        let full_name = state.resolve_full_name(&index.name, on_entry.conn_id());
        let on_desc = on_entry
            .desc(&full_name)
            .expect("can only create indexes on items with a valid description");

        let mut df_builder = DataflowBuilder::new(state, self.compute_instance.clone());
        let mut df_desc = MirDataflowDescription::new(full_name.to_string());

        df_builder.import_into_dataflow(&index.on, &mut df_desc)?;
        df_builder.reoptimize_imported_views(&mut df_desc, &self.config)?;

        for desc in df_desc.objects_to_build.iter_mut() {
            prep_relation_expr(&mut desc.plan, ExprPrepStyle::Index)?;
        }

        let mut index_desc = IndexDesc {
            on_id: index.on,
            key: index.keys.clone(),
        };

        for key in index_desc.key.iter_mut() {
            prep_scalar_expr(key, ExprPrepStyle::Index)?;
        }

        df_desc.export_index(self.exported_index_id, index_desc, on_desc.typ().clone());

        // Optimize the dataflow across views, and any other ways that appeal.
        let mut df_meta = mz_transform::optimize_dataflow(
            &mut df_desc,
            &df_builder,
            &mz_transform::EmptyStatisticsOracle,
        )?;

        if index.keys.is_empty() {
            df_meta.push_optimizer_notice_dedup(OptimizerNotice::IndexKeyEmpty);
        }

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan { df_desc, df_meta })
    }
}

impl Optimize<GlobalMirPlan> for Optimizer {
    type To = GlobalLirPlan<Unresolved>;

    fn optimize(&mut self, plan: GlobalMirPlan) -> Result<Self::To, OptimizerError> {
        let GlobalMirPlan {
            mut df_desc,
            df_meta,
        } = plan;

        // Ensure all expressions are normalized before finalizing.
        for build in df_desc.objects_to_build.iter_mut() {
            normalize_lets(&mut build.plan.0)?
        }

        // Finalize the dataflow. This includes:
        // - MIR ⇒ LIR lowering
        // - LIR ⇒ LIR transforms
        let df_desc = Plan::finalize_dataflow(
            df_desc,
            self.config.enable_consolidate_after_union_negate,
            self.config.enable_specialized_arrangements,
        )
        .map_err(OptimizerError::Internal)?;

        // Return the plan at the end of this `optimize` step.
        Ok(GlobalLirPlan {
            df_desc,
            df_meta,
            phantom: PhantomData::<Unresolved>,
        })
    }
}

impl GlobalLirPlan<Unresolved> {
    /// Produces the [`GlobalLirPlan`] with [`Resolved`] timestamp.
    pub fn resolve(mut self, as_of: Antichain<Timestamp>) -> GlobalLirPlan<Resolved> {
        // Set the `as_of` timestamp for the dataflow.
        self.df_desc.set_as_of(as_of);

        // The only dataflow exports are indexes, so the `df_desc.until` should
        // be the empty frontier.
        assert!(self.df_desc.sink_exports.is_empty());
        // The `until` is set to the empty frontier in the `df_desc` constructor.
        assert!(self.df_desc.until.is_empty());

        GlobalLirPlan {
            df_desc: self.df_desc,
            df_meta: self.df_meta,
            phantom: PhantomData::<Resolved>,
        }
    }
}

impl GlobalLirPlan<Resolved> {
    /// Unwraps the parts of the final result of the optimization pipeline.
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }
}
