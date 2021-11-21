use ir_tools::{adt, IR};

use crate::syntax::aggregate::hir::*;
use crate::syntax::relation::{hir::*, *};
use crate::syntax::scalar::{hir::*, *};
use crate::syntax::*;

#[adt(Relation + Scalar + Aggregate)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct HIR;

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[input_sorts(Relation, Scalar, Aggregate)]
#[fold_type(FoldHIR)]
pub enum Relation<Syn = ()> {
    Constant(Constant, Syn),
    Get(Get, Syn),
    Project(Project<HIR>, Syn),
    Map(Map<HIR>, Syn),
    CallTable(CallTable<HIR>, Syn),
    Filter(Filter<HIR>, Syn),
    Join(Join<HIR>, Syn),
    Reduce(Reduce<HIR>, Syn),
    Distinct(Distinct<HIR>, Syn),
    TopK(TopK<HIR>, Syn),
    Negate(Negate<HIR>, Syn),
    Threshold(Threshold<HIR>, Syn),
    Union(Union<HIR>, Syn),
    DeclareKeys(DeclareKeys<HIR>, Syn),
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[input_sorts(Relation, Scalar, Aggregate)]
#[fold_type(FoldHIR)]
pub enum Scalar<Syn = ()> {
    Column(Column, Syn),
    Parameter(Parameter, Syn),
    Literal(Literal, Syn),
    CallNullary(CallNullary, Syn),
    CallUnary(CallUnary<HIR>, Syn),
    CallBinary(CallBinary<HIR>, Syn),
    CallVariadic(CallVariadic<HIR>, Syn),
    IfThenElse(IfThenElse<HIR>, Syn),
    Exists(Exists<HIR>, Syn),
    Select(Select<HIR>, Syn),
    // Window(Window),
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[input_sorts(Relation, Scalar, Aggregate)]
#[fold_type(FoldHIR)]
pub enum Aggregate<Syn = ()> {
    AggregateSpec(AggregateSpec<HIR>, Syn),
}

impl Relation {
    pub fn print(&self) -> String {
        todo!("print()") // self.fold::<Print, Print>()
    }
}
impl Scalar {
    pub fn print(&self) -> String {
        todo!("print()") // self.fold::<Print, Print>()
    }
}
impl Aggregate {
    pub fn print(&self) -> String {
        todo!("print()") // self.fold::<Print, Print>()
    }
}
