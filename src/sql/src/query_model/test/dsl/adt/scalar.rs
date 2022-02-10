// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr::{AggregateFunc, BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
use mz_repr::{ColumnType, Row};

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct BaseColumn {
    pub position: usize,
    pub column_type: ColumnType,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct ColumnReference {
    pub quantifier_id: String, // String instead of QuantifierId
    pub position: usize,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct Literal {
    pub row: Row,
    pub column_type: ColumnType,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct CallUnmaterializable {
    pub func: UnmaterializableFunc,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct CallUnary<T> {
    pub func: UnaryFunc,
    pub expr: T,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct CallBinary<T> {
    pub func: BinaryFunc,
    pub expr1: T,
    pub expr2: T,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct CallVariadic<T> {
    pub func: VariadicFunc,
    pub exprs: Vec<T>,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct If<T> {
    pub cond: T,
    pub then: T,
    pub els: T,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct Aggregate<T> {
    /// Names the aggregation function.
    pub func: AggregateFunc,
    /// An expression which extracts from each row the input to `func`.
    pub expr: T,
    /// Should the aggregation be applied only to distinct results in each group.
    pub distinct: bool,
}

/// Combine syntactic forms defined above in a [`ScalarExpr`] algebraic data type.
///
/// In general, the syntactic forms can be shared between ADTs.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) enum ScalarExpr {
    ColumnReference(ColumnReference),
    BaseColumn(BaseColumn),
    Literal(Literal),
    CallUnmaterializable(CallUnmaterializable),
    CallUnary(CallUnary<Box<Self>>),
    CallBinary(CallBinary<Box<Self>>),
    CallVariadic(CallVariadic<Box<Self>>),
    If(If<Box<Self>>),
    Aggregate(Aggregate<Box<Self>>),
}

impl From<ColumnReference> for ScalarExpr {
    fn from(expr: ColumnReference) -> Self {
        ScalarExpr::ColumnReference(expr)
    }
}

impl From<BaseColumn> for ScalarExpr {
    fn from(expr: BaseColumn) -> Self {
        ScalarExpr::BaseColumn(expr)
    }
}

impl From<Literal> for ScalarExpr {
    fn from(expr: Literal) -> Self {
        ScalarExpr::Literal(expr)
    }
}

impl From<CallUnmaterializable> for ScalarExpr {
    fn from(expr: CallUnmaterializable) -> Self {
        ScalarExpr::CallUnmaterializable(expr)
    }
}

impl From<CallUnary<Box<ScalarExpr>>> for ScalarExpr {
    fn from(expr: CallUnary<Box<ScalarExpr>>) -> Self {
        ScalarExpr::CallUnary(expr)
    }
}

impl From<CallBinary<Box<ScalarExpr>>> for ScalarExpr {
    fn from(expr: CallBinary<Box<ScalarExpr>>) -> Self {
        ScalarExpr::CallBinary(expr)
    }
}

impl From<CallVariadic<Box<ScalarExpr>>> for ScalarExpr {
    fn from(expr: CallVariadic<Box<ScalarExpr>>) -> Self {
        ScalarExpr::CallVariadic(expr)
    }
}

impl From<If<Box<ScalarExpr>>> for ScalarExpr {
    fn from(expr: If<Box<ScalarExpr>>) -> Self {
        ScalarExpr::If(expr)
    }
}

impl From<Aggregate<Box<ScalarExpr>>> for ScalarExpr {
    fn from(expr: Aggregate<Box<ScalarExpr>>) -> Self {
        ScalarExpr::Aggregate(expr)
    }
}
