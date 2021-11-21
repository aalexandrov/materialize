use super::{from_vec, from_vec_vec};
use crate::mir;
use crate::mir::MIR;
use crate::syntax::aggregate::mir::*;
use crate::syntax::relation::{mir::*, *};
use crate::syntax::scalar::{mir::*, *};
use expr::{AggregateExpr, MirRelationExpr, MirScalarExpr};

impl From<MirRelationExpr> for mir::Relation {
    fn from(expr: MirRelationExpr) -> Self {
        use MirRelationExpr::*;
        match expr {
            Constant { rows, typ } => MIR::constant(rows, typ),
            Get { id, typ } => MIR::get(id, typ),
            Let { id, value, body } => {
                let value = (*value).into();
                let body = (*body).into();
                MIR::with(id, value, body)
            }
            Project { input, outputs } => {
                let input = (*input).into();
                MIR::project(input, outputs)
            }
            Map { input, scalars } => {
                let input = (*input).into();
                let scalars = from_vec(&scalars);
                MIR::map(input, scalars)
            }
            FlatMap { input, func, exprs } => {
                let input = (*input).into();
                let exprs = from_vec(&exprs);
                MIR::flat_map(input, func, exprs)
            }
            Filter { input, predicates } => {
                let input = (*input).into();
                let predicates = from_vec(&predicates);
                MIR::filter(input, predicates)
            }
            Join {
                inputs,
                equivalences,
                implementation: _,
            } => {
                let inputs = from_vec(&inputs);
                let equivalences = from_vec_vec(&equivalences);
                MIR::join(inputs, equivalences)
            }
            Reduce {
                input,
                group_key,
                aggregates,
                expected_group_size,
                monotonic: _,
            } => {
                let input = (*input).into();
                let group_key = from_vec(&group_key);
                let aggregates = from_vec(&aggregates);
                MIR::reduce(input, group_key, aggregates, expected_group_size)
            }
            TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
                monotonic: _,
            } => {
                let input = (*input).into();
                MIR::top_k(input, group_key, order_key, limit, offset)
            }
            Negate { input } => {
                let input = (*input).into();
                MIR::negate(input)
            }
            Threshold { input } => {
                let input = (*input).into();
                MIR::threshold(input)
            }
            Union { base, inputs } => {
                let base = (*base).into();
                let inputs = from_vec(&inputs);
                MIR::union(base, inputs)
            }
            DeclareKeys { input, keys } => {
                let input = (*input).into();
                MIR::declare_keys(input, keys)
            }
            ArrangeBy { input, keys } => {
                let input = (*input).into();
                let keys = from_vec_vec(&keys);
                MIR::arrange_by(input, keys)
            }
        }
    }
}

impl From<MirScalarExpr> for mir::Scalar {
    fn from(expr: MirScalarExpr) -> Self {
        use MirScalarExpr::*;
        match expr {
            Column(position) => MIR::column(position),
            Literal(row, typ) => MIR::literal(row, typ),
            CallNullary(func) => MIR::call_nullary(func),
            CallUnary { func, expr } => {
                let expr = (*expr).into();
                MIR::call_unary(func, expr)
            }
            CallBinary { func, expr1, expr2 } => {
                let expr1 = (*expr1).into();
                let expr2 = (*expr2).into();
                MIR::call_binary(func, expr1, expr2)
            }
            CallVariadic { func, exprs } => {
                let exprs = from_vec(&exprs);
                MIR::call_variadic(func, exprs)
            }
            If { cond, then, els } => {
                let cond = (*cond).into();
                let then = (*then).into();
                let els = (*els).into();
                MIR::if_then_else(cond, then, els)
            }
        }
    }
}

impl From<AggregateExpr> for mir::Aggregate {
    fn from(aggregate: AggregateExpr) -> Self {
        let func = aggregate.func;
        let expr = aggregate.expr.into();
        let distinct = aggregate.distinct;
        MIR::aggregate_spec(func, expr, distinct)
    }
}
