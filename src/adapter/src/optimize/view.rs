// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for view expressions.

use mz_expr::OptimizedMirRelationExpr;
use mz_sql::plan::HirRelationExpr;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::Optimizer as TransformOptimizer;

use crate::optimize::{Optimize, OptimizerError};

pub struct OptimizeView {
    typecheck_ctx: TypecheckContext,
}

impl OptimizeView {
    #[allow(unused)]
    pub fn new() -> Self {
        Self {
            typecheck_ctx: empty_context(),
        }
    }
}

impl Optimize<'static, HirRelationExpr> for OptimizeView {
    type To = OptimizedMirRelationExpr;

    fn optimize<'a: 'static>(
        &'a mut self,
        expr: HirRelationExpr,
    ) -> Result<Self::To, OptimizerError> {
        // HIR ⇒ MIR
        let config = mz_sql::plan::OptimizerConfig {};
        let expr = expr.optimize_and_lower(&config)?;

        // MIR ⇒ MIR (local)
        let logical_optimizer = TransformOptimizer::logical_optimizer(&self.typecheck_ctx);
        let expr = logical_optimizer.optimize(expr)?;

        // Return.
        Ok(expr)
    }
}
