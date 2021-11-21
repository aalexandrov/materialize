use crate::hir;
use crate::syntax::aggregate::hir::*;
use crate::syntax::relation::{hir::*, *};
use crate::syntax::scalar::{hir::*, *};
use crate::syntax::*;
use expr::{BinaryFunc, ColumnOrder, Id, NullaryFunc, TableFunc, UnaryFunc, VariadicFunc};
use ir_tools::alg;
use repr::RelationType;
use repr::{ColumnType, Row};
use sql::plan::{
    AggregateExpr, AggregateFunc, ColumnRef, HirRelationExpr, HirScalarExpr, JoinKind,
};

impl From<hir::Relation> for HirRelationExpr {
    fn from(expr: hir::Relation) -> Self {
        expr.fold::<ToHIR, ToHIR>()
    }
}
impl From<hir::Scalar> for HirScalarExpr {
    fn from(expr: hir::Scalar) -> Self {
        expr.fold::<ToHIR, ToHIR>()
    }
}
impl From<hir::Aggregate> for AggregateExpr {
    fn from(expr: hir::Aggregate) -> Self {
        expr.fold::<ToHIR, ToHIR>()
    }
}

struct ToHIR;

impl TypScalar for ToHIR {
    type Scalar = HirScalarExpr;
}
impl TypRelation for ToHIR {
    type Relation = HirRelationExpr;
}
impl TypAggregate for ToHIR {
    type Aggregate = AggregateExpr;
}

// HirRelationExpr
// ---------------

#[alg(ToHIR)]
fn get(id: Id, typ: RelationType) -> HirRelationExpr {
    HirRelationExpr::Get { id, typ }
}
#[alg(ToHIR)]
fn project(input: HirRelationExpr, outputs: Vec<usize>) -> HirRelationExpr {
    let input = Box::new(input);
    HirRelationExpr::Project { input, outputs }
}
#[alg(ToHIR)]
fn map(input: HirRelationExpr, scalars: Vec<HirScalarExpr>) -> HirRelationExpr {
    let input = Box::new(input);
    HirRelationExpr::Map { input, scalars }
}
#[alg(ToHIR)]
fn filter(input: HirRelationExpr, predicates: Vec<HirScalarExpr>) -> HirRelationExpr {
    let input = Box::new(input);
    HirRelationExpr::Filter { input, predicates }
}
#[alg(ToHIR)]
fn call_table(func: TableFunc, exprs: Vec<HirScalarExpr>) -> HirRelationExpr {
    HirRelationExpr::CallTable { func, exprs }
}
#[alg(ToHIR)]
fn reduce(
    input: HirRelationExpr,
    group_key: Vec<usize>,
    aggregates: Vec<AggregateExpr>,
    expected_group_size: Option<usize>,
) -> HirRelationExpr {
    let input = Box::new(input);
    HirRelationExpr::Reduce {
        input,
        group_key,
        aggregates,
        expected_group_size,
    }
}
#[alg(ToHIR)]
fn distinct(input: HirRelationExpr) -> HirRelationExpr {
    let input = Box::new(input);
    HirRelationExpr::Distinct { input }
}
#[alg(ToHIR)]
fn top_k(
    input: HirRelationExpr,
    group_key: Vec<usize>,
    order_key: Vec<ColumnOrder>,
    limit: Option<usize>,
    offset: usize,
) -> HirRelationExpr {
    let input = Box::new(input);
    HirRelationExpr::TopK {
        input,
        group_key,
        order_key,
        limit,
        offset,
    }
}
#[alg(ToHIR)]
fn negate(input: HirRelationExpr) -> HirRelationExpr {
    let input = Box::new(input);
    HirRelationExpr::Negate { input }
}
#[alg(ToHIR)]
fn threshold(input: HirRelationExpr) -> HirRelationExpr {
    let input = Box::new(input);
    HirRelationExpr::Threshold { input }
}
#[alg(ToHIR)]
fn union(base: HirRelationExpr, inputs: Vec<HirRelationExpr>) -> HirRelationExpr {
    let base = Box::new(base);
    HirRelationExpr::Union { base, inputs }
}
#[alg(ToHIR)]
fn declare_keys(input: HirRelationExpr, keys: Vec<Vec<usize>>) -> HirRelationExpr {
    let input = Box::new(input);
    HirRelationExpr::DeclareKeys { input, keys }
}
#[alg(ToHIR)]
fn constant(rows: Vec<Row>, typ: RelationType) -> HirRelationExpr {
    HirRelationExpr::Constant { rows, typ }
}
#[alg(ToHIR)]
fn join(
    left: HirRelationExpr,
    right: HirRelationExpr,
    on: HirScalarExpr,
    kind: JoinKind,
) -> HirRelationExpr {
    let left = Box::new(left);
    let right = Box::new(right);
    HirRelationExpr::Join {
        left,
        right,
        on,
        kind,
    }
}

// HirScalarExpr
// -------------

#[alg(ToHIR)]
fn parameter(id: usize) -> HirScalarExpr {
    HirScalarExpr::Parameter(id)
}
#[alg(ToHIR)]
fn call_nullary(func: NullaryFunc) -> HirScalarExpr {
    HirScalarExpr::CallNullary(func)
}
#[alg(ToHIR)]
fn call_unary(func: UnaryFunc, expr: HirScalarExpr) -> HirScalarExpr {
    let expr = Box::new(expr);
    HirScalarExpr::CallUnary { func, expr }
}
#[alg(ToHIR)]
fn call_binary(func: BinaryFunc, expr1: HirScalarExpr, expr2: HirScalarExpr) -> HirScalarExpr {
    let expr1 = Box::new(expr1);
    let expr2 = Box::new(expr2);
    HirScalarExpr::CallBinary { func, expr1, expr2 }
}
#[alg(ToHIR)]
fn call_variadic(func: VariadicFunc, exprs: Vec<HirScalarExpr>) -> HirScalarExpr {
    HirScalarExpr::CallVariadic { func, exprs }
}
#[alg(ToHIR)]
fn if_then_else(cond: HirScalarExpr, then: HirScalarExpr, els: HirScalarExpr) -> HirScalarExpr {
    let cond = Box::new(cond);
    let then = Box::new(then);
    let els = Box::new(els);
    HirScalarExpr::If { cond, then, els }
}
#[alg(ToHIR)]
fn literal(row: Row, typ: ColumnType) -> HirScalarExpr {
    HirScalarExpr::Literal(row, typ)
}
#[alg(ToHIR)]
fn column(level: usize, column: usize) -> HirScalarExpr {
    HirScalarExpr::Column(ColumnRef { level, column })
}
#[alg(ToHIR)]
fn exists(input: HirRelationExpr) -> HirScalarExpr {
    let input = Box::new(input);
    HirScalarExpr::Exists(input)
}
#[alg(ToHIR)]
fn select(input: HirRelationExpr) -> HirScalarExpr {
    let input = Box::new(input);
    HirScalarExpr::Select(input)
}

// AggregateExpr
// -------------

#[alg(ToHIR)]
fn aggregate_spec(func: AggregateFunc, expr: HirScalarExpr, distinct: bool) -> AggregateExpr {
    let expr = Box::new(expr);
    AggregateExpr {
        func,
        expr,
        distinct,
    }
}
