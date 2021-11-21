use crate::mir;
use crate::syntax::aggregate::mir::*;
use crate::syntax::relation::{mir::*, *};
use crate::syntax::scalar::{mir::*, *};
use crate::syntax::*;
use expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, ColumnOrder, EvalError, Id, JoinImplementation,
    LocalId, MirRelationExpr, MirScalarExpr, NullaryFunc, TableFunc, UnaryFunc, VariadicFunc,
};
use ir_tools::alg;
use repr::{ColumnType, Row};
use repr::{Diff, RelationType};

impl From<mir::Relation> for MirRelationExpr {
    fn from(expr: mir::Relation) -> Self {
        expr.fold::<ToMIR, ToMIR>()
    }
}
impl From<mir::Scalar> for MirScalarExpr {
    fn from(expr: mir::Scalar) -> Self {
        expr.fold::<ToMIR, ToMIR>()
    }
}
impl From<mir::Aggregate> for AggregateExpr {
    fn from(expr: mir::Aggregate) -> Self {
        expr.fold::<ToMIR, ToMIR>()
    }
}

struct ToMIR;

impl TypScalar for ToMIR {
    type Scalar = MirScalarExpr;
}
impl TypRelation for ToMIR {
    type Relation = MirRelationExpr;
}
impl TypAggregate for ToMIR {
    type Aggregate = AggregateExpr;
}

// MirRelationExpr
// ---------------

#[alg(ToMIR)]
fn get(id: Id, typ: RelationType) -> MirRelationExpr {
    MirRelationExpr::Get { id, typ }
}
#[alg(ToMIR)]
fn with(id: LocalId, value: MirRelationExpr, body: MirRelationExpr) -> MirRelationExpr {
    let value = Box::new(value);
    let body = Box::new(body);
    MirRelationExpr::Let { id, value, body }
}
#[alg(ToMIR)]
fn project(input: MirRelationExpr, outputs: Vec<usize>) -> MirRelationExpr {
    let input = Box::new(input);
    MirRelationExpr::Project { input, outputs }
}
#[alg(ToMIR)]
fn map(input: MirRelationExpr, scalars: Vec<MirScalarExpr>) -> MirRelationExpr {
    let input = Box::new(input);
    MirRelationExpr::Map { input, scalars }
}
#[alg(ToMIR)]
fn flat_map(input: MirRelationExpr, func: TableFunc, exprs: Vec<MirScalarExpr>) -> MirRelationExpr {
    let input = Box::new(input);
    MirRelationExpr::FlatMap { input, func, exprs }
}
#[alg(ToMIR)]
fn filter(input: MirRelationExpr, predicates: Vec<MirScalarExpr>) -> MirRelationExpr {
    let input = Box::new(input);
    MirRelationExpr::Filter { input, predicates }
}
#[alg(ToMIR)]
fn reduce(
    input: MirRelationExpr,
    group_key: Vec<MirScalarExpr>,
    aggregates: Vec<AggregateExpr>,
    expected_group_size: Option<usize>,
) -> MirRelationExpr {
    let input = Box::new(input);
    MirRelationExpr::Reduce {
        input,
        group_key,
        aggregates,
        expected_group_size,
        monotonic: false,
    }
}
#[alg(ToMIR)]
fn top_k(
    input: MirRelationExpr,
    group_key: Vec<usize>,
    order_key: Vec<ColumnOrder>,
    limit: Option<usize>,
    offset: usize,
) -> MirRelationExpr {
    let input = Box::new(input);
    MirRelationExpr::TopK {
        input,
        group_key,
        order_key,
        limit,
        offset,
        monotonic: false,
    }
}
#[alg(ToMIR)]
fn negate(input: MirRelationExpr) -> MirRelationExpr {
    let input = Box::new(input);
    MirRelationExpr::Negate { input }
}
#[alg(ToMIR)]
fn threshold(input: MirRelationExpr) -> MirRelationExpr {
    let input = Box::new(input);
    MirRelationExpr::Threshold { input }
}
#[alg(ToMIR)]
fn union(base: MirRelationExpr, inputs: Vec<MirRelationExpr>) -> MirRelationExpr {
    let base = Box::new(base);
    MirRelationExpr::Union { base, inputs }
}
#[alg(ToMIR)]
fn declare_keys(input: MirRelationExpr, keys: Vec<Vec<usize>>) -> MirRelationExpr {
    let input = Box::new(input);
    MirRelationExpr::DeclareKeys { input, keys }
}
#[alg(ToMIR)]
fn constant(rows: Result<Vec<(Row, Diff)>, EvalError>, typ: RelationType) -> MirRelationExpr {
    MirRelationExpr::Constant { rows, typ }
}
#[alg(ToMIR)]
fn join(inputs: Vec<MirRelationExpr>, equivalences: Vec<Vec<MirScalarExpr>>) -> MirRelationExpr {
    MirRelationExpr::Join {
        inputs,
        equivalences,
        implementation: JoinImplementation::Unimplemented,
    }
}
#[alg(ToMIR)]
fn arrange_by(input: MirRelationExpr, keys: Vec<Vec<MirScalarExpr>>) -> MirRelationExpr {
    let input = Box::new(input);
    MirRelationExpr::ArrangeBy { input, keys }
}

// MirScalarExpr
// -------------

#[alg(ToMIR)]
fn call_nullary(func: NullaryFunc) -> MirScalarExpr {
    MirScalarExpr::CallNullary(func)
}
#[alg(ToMIR)]
fn call_unary(func: UnaryFunc, expr: MirScalarExpr) -> MirScalarExpr {
    let expr = Box::new(expr);
    MirScalarExpr::CallUnary { func, expr }
}
#[alg(ToMIR)]
fn call_binary(func: BinaryFunc, expr1: MirScalarExpr, expr2: MirScalarExpr) -> MirScalarExpr {
    let expr1 = Box::new(expr1);
    let expr2 = Box::new(expr2);
    MirScalarExpr::CallBinary { func, expr1, expr2 }
}
#[alg(ToMIR)]
fn call_variadic(func: VariadicFunc, exprs: Vec<MirScalarExpr>) -> MirScalarExpr {
    MirScalarExpr::CallVariadic { func, exprs }
}
#[alg(ToMIR)]
fn if_then_else(cond: MirScalarExpr, then: MirScalarExpr, els: MirScalarExpr) -> MirScalarExpr {
    let cond = Box::new(cond);
    let then = Box::new(then);
    let els = Box::new(els);
    MirScalarExpr::If { cond, then, els }
}
#[alg(ToMIR)]
fn literal(row: Result<Row, EvalError>, typ: ColumnType) -> MirScalarExpr {
    MirScalarExpr::Literal(row, typ)
}
#[alg(ToMIR)]
fn column(position: usize) -> MirScalarExpr {
    MirScalarExpr::Column(position)
}

// AggregateExpr
// -------------

#[alg(ToMIR)]
fn aggregate_spec(func: AggregateFunc, expr: MirScalarExpr, distinct: bool) -> AggregateExpr {
    AggregateExpr {
        func,
        expr,
        distinct,
    }
}
