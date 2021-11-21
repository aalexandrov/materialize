use ir_tools::{adt, IR};

use crate::syntax::aggregate::mir::*;
use crate::syntax::relation::{mir::*, *};
use crate::syntax::scalar::{mir::*, *};
use crate::syntax::*;

#[adt(Relation + Scalar + Aggregate)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MIR;

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[input_sorts(Relation, Scalar, Aggregate)]
#[fold_type(FoldMIR)]
pub enum Relation<Syn = ()> {
    Constant(Constant, Syn),
    Get(Get, Syn),
    With(With<MIR>, Syn),
    Project(Project<MIR>, Syn),
    Map(Map<MIR>, Syn),
    FlatMap(FlatMap<MIR>, Syn),
    Filter(Filter<MIR>, Syn),
    Join(Join<MIR>, Syn),
    Reduce(Reduce<MIR>, Syn),
    TopK(TopK<MIR>, Syn),
    Negate(Negate<MIR>, Syn),
    Threshold(Threshold<MIR>, Syn),
    Union(Union<MIR>, Syn),
    ArrangeBy(ArrangeBy<MIR>, Syn),
    DeclareKeys(DeclareKeys<MIR>, Syn),
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[input_sorts(Relation, Scalar, Aggregate)]
#[fold_type(FoldMIR)]
pub enum Scalar<Syn = ()> {
    Column(Column, Syn),
    Literal(Literal, Syn),
    CallNullary(CallNullary, Syn),
    CallUnary(CallUnary<MIR>, Syn),
    CallBinary(CallBinary<MIR>, Syn),
    CallVariadic(CallVariadic<MIR>, Syn),
    IfThenElse(IfThenElse<MIR>, Syn),
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[input_sorts(Relation, Scalar, Aggregate)]
#[fold_type(FoldMIR)]
pub enum Aggregate<Syn = ()> {
    AggregateSpec(AggregateSpec<MIR>, Syn),
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
