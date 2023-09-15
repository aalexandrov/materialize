// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for view expressions.

use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_repr::{GlobalId, Timestamp};
use mz_sql::plan::HirRelationExpr;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::{EmptyStatisticsOracle, Optimizer as TransformOptimizer};

use crate::catalog::CatalogState;
use crate::coord::dataflows::{
    prep_relation_expr, prep_scalar_expr, ComputeInstanceSnapshot, DataflowBuilder, EvalTime,
    ExprPrepStyle,
};
use crate::optimize::{Optimize, OptimizerError};
use crate::session::Session;
use crate::TimestampContext;

/// The (sealed intermediate) result after HIR ⇒ MIR lowering and decorrelation
/// and MIR optimization.
pub struct LocalLogicalPlan {
    expr: MirRelationExpr,
}

/// The (sealed intermediate) result after optimizing `DataflowDescription` with
/// `MIR` plans. This can only be converted into a [`TimestampedLogicalPlan`].
pub struct GlobalLogicalPlan<'ctx> {
    df_desc: DataflowDescription<OptimizedMirRelationExpr>,
    df_meta: DataflowMetainfo,
    session: &'ctx mut Session,
}

/// The (sealed intermediate) result after optimizing `DataflowDescription` with
/// `MIR` plans and aquiring a timestamp context for query execution.
pub struct TimestampedPlan {
    df_desc: DataflowDescription<OptimizedMirRelationExpr>,
    df_meta: DataflowMetainfo,
    timestamp_ctx: TimestampContext<Timestamp>,
}

/// The (final) result after HIR ⇒ MIR lowering and optimizing the resulting
/// `DataflowDescription` with `HIR` plans.
pub struct GlobalPhysicalPlan {
    pub df_desc: DataflowDescription<Plan>,
    pub df_meta: DataflowMetainfo,
}

pub struct OptimizeSelect<'s> {
    typecheck_ctx: TypecheckContext,
    // A snapshot of the catalog state.
    catalog_state: &'s CatalogState,
    // A snapshot of the compute instance that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    // A transient GlobalId to be used when constructing the dataflow.
    select_id: GlobalId,
    // A transient GlobalId to be used when constructing a PeekPlan.
    index_id: GlobalId,
    // A session
    session: &'s mut Session,
    // Feature flags
    enable_consolidate_after_union_negate: bool,
    enable_monotonic_oneshot_selects: bool,
}

impl<'ctx> OptimizeSelect<'ctx> {
    #[allow(unused)]
    pub fn new(
        catalog_state: &'ctx CatalogState,
        compute_instance: ComputeInstanceSnapshot,
        select_id: GlobalId,
        index_id: GlobalId,
        session: &'ctx mut Session,
        enable_consolidate_after_union_negate: bool,
        enable_monotonic_oneshot_selects: bool,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog_state,
            compute_instance,
            select_id,
            index_id,
            session,
            enable_consolidate_after_union_negate,
            enable_monotonic_oneshot_selects,
        }
    }
}

impl<'ctx> Optimize<'static, HirRelationExpr> for OptimizeSelect<'ctx> {
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

impl<'ctx> Optimize<'ctx, LocalLogicalPlan> for OptimizeSelect<'ctx> {
    type To = GlobalLogicalPlan<'ctx>;

    fn optimize<'a: 'ctx>(
        &'a mut self,
        plan: LocalLogicalPlan,
    ) -> Result<Self::To, OptimizerError> {
        let LocalLogicalPlan { expr } = plan;

        let mut df_builder =
            DataflowBuilder::new(self.catalog_state, self.compute_instance.clone());

        let mut df_desc =
            DataflowDescription::<OptimizedMirRelationExpr>::new("explanation".to_string());

        df_builder.import_view_into_dataflow(
            &self.select_id,
            &OptimizedMirRelationExpr(expr),
            &mut df_desc,
        )?;

        // Resolve all unmaterializable function calls except mz_now(), because we don't yet have a
        // timestamp.
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Deferred,
            session: self.session,
        };
        df_desc.visit_children(
            |r| prep_relation_expr(&self.catalog_state, r, style),
            |s| prep_scalar_expr(&self.catalog_state, s, style),
        )?;

        // TODO: proper stats needs exact timestamp at the moment.
        // However, we don't want to get it so early.
        let stats = EmptyStatisticsOracle;
        let df_meta = mz_transform::optimize_dataflow(&mut df_desc, &df_builder, &stats)?;

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalLogicalPlan {
            df_desc,
            df_meta,
            session: self.session,
        })
    }
}

impl<'ctx> Optimize<'static, TimestampedPlan> for OptimizeSelect<'ctx> {
    type To = GlobalPhysicalPlan;

    fn optimize<'a: 'ctx>(&'a mut self, plan: TimestampedPlan) -> Result<Self::To, OptimizerError> {
        let TimestampedPlan {
            mut df_desc,
            df_meta,
            timestamp_ctx,
        } = plan;

        df_desc.set_as_of(timestamp_ctx.antichain());

        // Resolve all unmaterializable function calls including mz_now().
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Time(timestamp_ctx.timestamp_or_default()),
            session: self.session,
        };
        df_desc.visit_children(
            |r| prep_relation_expr(&self.catalog_state, r, style),
            |s| prep_scalar_expr(&self.catalog_state, s, style),
        )?;

        // Ensure all expressions are normalized before finalizing.
        for build in df_desc.objects_to_build.iter_mut() {
            normalize_lets(&mut build.plan.0)?
        }

        let df_desc = Plan::finalize_dataflow(
            df_desc,
            self.enable_consolidate_after_union_negate,
            self.enable_monotonic_oneshot_selects,
        )
        .map_err(OptimizerError::Internal)?;

        // Return the plan at the end of this `optimize` step.
        Ok(GlobalPhysicalPlan { df_desc, df_meta })
    }
}
