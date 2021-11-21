use super::from_vec;
use crate::hir;
use crate::hir::HIR;
use crate::syntax::aggregate::hir::*;
use crate::syntax::relation::{hir::*, *};
use crate::syntax::scalar::{hir::*, *};
use sql::plan::{AggregateExpr, HirRelationExpr, HirScalarExpr};

impl From<HirRelationExpr> for hir::Relation {
    fn from(expr: HirRelationExpr) -> Self {
        use HirRelationExpr::*;
        match expr {
            Constant { rows, typ } => HIR::constant(rows, typ),
            Get { id, typ } => HIR::get(id, typ),
            Project { input, outputs } => {
                let input = (*input).into();
                HIR::project(input, outputs)
            }
            Map { input, scalars } => {
                let input = (*input).into();
                let scalars = from_vec(&scalars);
                HIR::map(input, scalars)
            }
            CallTable { func, exprs } => {
                let exprs = from_vec(&exprs);
                HIR::call_table(func, exprs)
            }
            Filter { input, predicates } => {
                let input = (*input).into();
                let predicates = from_vec(&predicates);
                HIR::filter(input, predicates)
            }
            Join {
                left,
                right,
                on,
                kind,
            } => {
                let left = (*left).into();
                let right = (*right).into();
                let on = on.into();
                let kind = kind.into();
                HIR::join(left, right, on, kind)
            }
            Reduce {
                input,
                group_key,
                aggregates,
                expected_group_size,
            } => {
                let input = (*input).into();
                let aggregates = from_vec(&aggregates);
                HIR::reduce(input, group_key, aggregates, expected_group_size)
            }
            Distinct { input } => {
                let input = (*input).into();
                HIR::distinct(input)
            }
            TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
            } => {
                let input = (*input).into();
                HIR::top_k(input, group_key, order_key, limit, offset)
            }
            Negate { input } => {
                let input = (*input).into();
                HIR::negate(input)
            }
            Threshold { input } => {
                let input = (*input).into();
                HIR::threshold(input)
            }
            Union { base, inputs } => {
                let base = (*base).into();
                let inputs = from_vec(&inputs);
                HIR::union(base, inputs)
            }
            DeclareKeys { input, keys } => {
                let input = (*input).into();
                HIR::declare_keys(input, keys)
            }
        }
    }
}

impl From<HirScalarExpr> for hir::Scalar {
    fn from(expr: HirScalarExpr) -> Self {
        use HirScalarExpr::*;
        match expr {
            Column(cref) => {
                let level = cref.level;
                let column = cref.column;
                HIR::column(level, column)
            }
            Parameter(id) => HIR::parameter(id),
            Literal(row, typ) => HIR::literal(row, typ),
            CallNullary(func) => HIR::call_nullary(func),
            CallUnary { func, expr } => {
                let expr = (*expr).into();
                HIR::call_unary(func, expr)
            }
            CallBinary { func, expr1, expr2 } => {
                let expr1 = (*expr1).into();
                let expr2 = (*expr2).into();
                HIR::call_binary(func, expr1, expr2)
            }
            CallVariadic { func, exprs } => {
                let exprs = from_vec(&exprs);
                HIR::call_variadic(func, exprs)
            }
            If { cond, then, els } => {
                let cond = (*cond).into();
                let then = (*then).into();
                let els = (*els).into();
                HIR::if_then_else(cond, then, els)
            }
            Exists(input) => {
                let input = (*input).into();
                HIR::exists(input)
            }
            Select(input) => {
                let input = (*input).into();
                HIR::select(input)
            }
            Windowing(_) => panic!("Unsupported expression variant `Window`"),
        }
    }
}

impl From<AggregateExpr> for hir::Aggregate {
    fn from(aggregate: AggregateExpr) -> Self {
        let func = aggregate.func;
        let expr = (*aggregate.expr).into();
        let distinct = aggregate.distinct;
        HIR::aggregate_spec(func, expr, distinct)
    }
}
