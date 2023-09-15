// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for view expressions.

use differential_dataflow::lattice::Lattice;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_repr::{ColumnName, GlobalId, RelationDesc, Timestamp};
use mz_sql::plan::HirRelationExpr;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::Optimizer as TransformOptimizer;
use timely::progress::Antichain;

use crate::catalog::CatalogState;
use crate::coord::dataflows::{ComputeInstanceSnapshot, DataflowBuilder};
use crate::optimize::{Optimize, OptimizerError};

/// The (sealed intermediate) result after HIR ⇒ MIR lowering and decorrelation
/// and MIR optimization.
pub struct LocalLogicalPlan {
    expr: MirRelationExpr,
}

/// The (sealed intermediate) result after optimizing `DataflowDescription` with
/// `MIR` plans.
pub struct GlobalLogicalPlan {
    df_desc: DataflowDescription<OptimizedMirRelationExpr>,
    df_meta: DataflowMetainfo,
}

/// The (final) result after HIR ⇒ MIR lowering and optimizing the resulting
/// `DataflowDescription` with `HIR` plans.
pub struct GlobalPhysicalPlan {
    pub df_desc: DataflowDescription<Plan>,
    pub df_meta: DataflowMetainfo,
}

pub struct OptimizeMaterializedView<'ctx> {
    typecheck_ctx: TypecheckContext,
    catalog_state: &'ctx CatalogState,
    compute_instance: ComputeInstanceSnapshot,
    exported_sink_id: GlobalId,
    internal_view_id: GlobalId,
    column_names: Vec<ColumnName>,
    debug_name: String,
    as_of: Antichain<Timestamp>,
    // Feature flags
    enable_consolidate_after_union_negate: bool,
}

impl<'ctx> OptimizeMaterializedView<'ctx> {
    #[allow(unused)]
    pub fn new(
        catalog_state: &'ctx CatalogState,
        compute_instance: ComputeInstanceSnapshot,
        exported_sink_id: GlobalId,
        internal_view_id: GlobalId,
        column_names: Vec<ColumnName>,
        debug_name: String,
        as_of: Antichain<Timestamp>,
        enable_consolidate_after_union_negate: bool,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog_state,
            compute_instance,
            exported_sink_id,
            internal_view_id,
            column_names,
            debug_name,
            as_of,
            enable_consolidate_after_union_negate,
        }
    }
}

impl<'ctx> Optimize<'ctx, HirRelationExpr> for OptimizeMaterializedView<'ctx> {
    type To = LocalLogicalPlan;

    fn optimize<'a: 'ctx>(&'a mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        // HIR ⇒ MIR
        let config = mz_sql::plan::OptimizerConfig {};
        let expr = expr.optimize_and_lower(&config)?;

        // MIR ⇒ MIR (local)
        let logical_optimizer = TransformOptimizer::logical_optimizer(&self.typecheck_ctx);
        let expr = logical_optimizer.optimize(expr)?.into_inner();

        // Return the (sealed) plan at the end of this optimization step.
        Ok(LocalLogicalPlan { expr })
    }
}

impl<'ctx> Optimize<'ctx, LocalLogicalPlan> for OptimizeMaterializedView<'ctx> {
    type To = GlobalLogicalPlan;

    fn optimize<'a: 'ctx>(
        &'a mut self,
        plan: LocalLogicalPlan,
    ) -> Result<Self::To, OptimizerError> {
        let LocalLogicalPlan { expr } = plan;

        let relation_desc = RelationDesc::new(expr.typ(), self.column_names.clone());

        let mut df_builder =
            DataflowBuilder::new(self.catalog_state, self.compute_instance.clone());

        let (df_desc, df_meta) = df_builder.build_materialized_view(
            self.exported_sink_id,
            self.internal_view_id,
            self.debug_name.clone(),
            &OptimizedMirRelationExpr(expr),
            &relation_desc,
        )?;

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalLogicalPlan { df_desc, df_meta })
    }
}

impl<'ctx> Optimize<'ctx, GlobalLogicalPlan> for OptimizeMaterializedView<'ctx> {
    type To = GlobalPhysicalPlan;

    fn optimize<'a: 'ctx>(
        &'a mut self,
        plan: GlobalLogicalPlan,
    ) -> Result<Self::To, OptimizerError> {
        let GlobalLogicalPlan {
            mut df_desc,
            df_meta,
        } = plan;

        df_desc.set_as_of(self.as_of.clone());

        // If the only outputs of the dataflow are sinks, we might be able to
        // turn off the computation early, if they all have non-trivial
        // `up_to`s.
        //
        // TODO: This should always be the case here so we can demote
        // the outer index to a soft assert.
        if df_desc.index_exports.is_empty() {
            df_desc.until = Antichain::from_elem(Timestamp::MIN);
            for (_, sink) in &df_desc.sink_exports {
                df_desc.until.join_assign(&sink.up_to);
            }
        }

        // Ensure all expressions are normalized before finalizing.
        for build in df_desc.objects_to_build.iter_mut() {
            normalize_lets(&mut build.plan.0)?
        }

        let df_desc = Plan::finalize_dataflow(
            df_desc,
            self.enable_consolidate_after_union_negate,
            false, // we are not in a monotonic context here
        )
        .map_err(OptimizerError::Internal)?;

        // Return the plan at the end of this `optimize` step.
        Ok(GlobalPhysicalPlan { df_desc, df_meta })
    }
}
