// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt;

use ore::str::separated;
use repr::*;

use crate::plan::expr::{BinaryFunc, NullaryFunc, UnaryFunc, VariadicFunc};
use crate::query_model::{Model, QuantifierId, QuantifierSet, QuantifierType};
use expr::AggregateFunc;

/// Representation for scalar expressions within a query graph model.
///
/// Similar to HirScalarExpr but:
/// * subqueries are represented as column references to the subquery
///   quantifiers within the same box the expression belongs to,
/// * aggregate expressions are considered scalar expressions here
///   even though they are only valid in the context of a Grouping box,
/// * column references are represented by a pair (quantifier ID, column
///   position),
/// * BaseColumn is used to represent leaf columns, only allowed in
///   the projection of BaseTables and TableFunctions.
///
/// Scalar expressions only make sense within the context of a
/// [`crate::query_model::QueryBox`], and hence, their name.
#[derive(Debug, PartialEq, Clone)]
pub enum BoxScalarExpr {
    /// A reference to a column from a quantifier that either lives in
    /// the same box as the expression or is a sibling quantifier of
    /// an ascendent box of the box that contains the expression.
    ColumnReference(ColumnReference),
    /// A leaf column. Only allowed as standalone expressions in the
    /// projection of `BaseTable` and `TableFunction` boxes.
    BaseColumn(BaseColumn),
    /// A literal value.
    /// (A single datum stored as a row, because we can't own a Datum)
    Literal(Row, ColumnType),
    CallNullary(NullaryFunc),
    CallUnary {
        func: UnaryFunc,
        expr: Box<BoxScalarExpr>,
    },
    CallBinary {
        func: BinaryFunc,
        expr1: Box<BoxScalarExpr>,
        expr2: Box<BoxScalarExpr>,
    },
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<BoxScalarExpr>,
    },
    If {
        cond: Box<BoxScalarExpr>,
        then: Box<BoxScalarExpr>,
        els: Box<BoxScalarExpr>,
    },
    Aggregate {
        /// Names the aggregation function.
        func: AggregateFunc,
        /// An expression which extracts from each row the input to `func`.
        expr: Box<BoxScalarExpr>,
        /// Should the aggregation be applied only to distinct results in each group.
        distinct: bool,
    },
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ColumnReference {
    pub quantifier_id: QuantifierId,
    pub position: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct BaseColumn {
    pub position: usize,
    pub column_type: repr::ColumnType,
}

impl fmt::Display for BoxScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            BoxScalarExpr::ColumnReference(c) => {
                write!(f, "Q{}.C{}", c.quantifier_id, c.position)
            }
            BoxScalarExpr::BaseColumn(c) => {
                write!(f, "C{}", c.position)
            }
            BoxScalarExpr::Literal(row, _) => {
                write!(f, "{}", row.unpack_first())
            }
            BoxScalarExpr::CallNullary(func) => {
                write!(f, "{}()", func)
            }
            BoxScalarExpr::CallUnary { func, expr } => {
                write!(f, "{}({})", func, expr)
            }
            BoxScalarExpr::CallBinary { func, expr1, expr2 } => {
                if func.is_infix_op() {
                    write!(f, "({} {} {})", expr1, func, expr2)
                } else {
                    write!(f, "{}({}, {})", func, expr1, expr2)
                }
            }
            BoxScalarExpr::CallVariadic { func, exprs } => {
                write!(f, "{}({})", func, separated(", ", exprs.clone()))
            }
            BoxScalarExpr::If { cond, then, els } => {
                write!(f, "if {} then {{{}}} else {{{}}}", cond, then, els)
            }
            BoxScalarExpr::Aggregate {
                func,
                expr,
                distinct,
            } => {
                write!(
                    f,
                    "{}({}{})",
                    *func,
                    if *distinct { "distinct " } else { "" },
                    expr
                )
            }
        }
    }
}

impl BoxScalarExpr {
    /// Returns the type of the given scalar expression.
    pub fn typ(&self, model: &Model) -> ColumnType {
        use BoxScalarExpr::*;
        match self {
            BaseColumn(c) => c.column_type.clone(),
            ColumnReference(c) => c.typ(model),
            Literal(_, typ) => typ.clone(),
            CallNullary(func) => func.output_type(),
            CallUnary { expr, func } => func.output_type(expr.typ(model)),
            CallBinary { expr1, expr2, func } => {
                func.output_type(expr1.typ(model), expr2.typ(model))
            }
            CallVariadic { exprs, func } => {
                func.output_type(exprs.iter().map(|e| e.typ(model)).collect())
            }
            If { then, els, .. } => {
                let then_type = then.typ(model);
                let else_type = els.typ(model);
                debug_assert!(then_type.scalar_type.base_eq(&else_type.scalar_type));
                ColumnType {
                    nullable: then_type.nullable || else_type.nullable,
                    scalar_type: then_type.scalar_type,
                }
            }
            Aggregate { expr, func, .. } => func.output_type(expr.typ(model)),
        }
    }

    /// Adds any columns that *must* be non-Null for `self` to be non-Null.
    pub fn non_null_requirements(&self, columns: &mut HashSet<ColumnReference>) {
        use BoxScalarExpr::*;
        match self {
            ColumnReference(col) => {
                columns.insert(col.clone());
            }
            BaseColumn(..) | Literal(..) | CallNullary(_) => (),
            CallUnary { func, expr } => {
                if func.propagates_nulls() {
                    expr.non_null_requirements(columns);
                }
            }
            CallBinary { func, expr1, expr2 } => {
                if func.propagates_nulls() {
                    expr1.non_null_requirements(columns);
                    expr2.non_null_requirements(columns);
                }
            }
            CallVariadic { func, exprs } => {
                if func.propagates_nulls() {
                    for expr in exprs {
                        expr.non_null_requirements(columns);
                    }
                }
            }
            If { .. } => (),
            // TODO the non-null requeriments of an aggregate expression can
            // be pused down to, for example, convert an outer join into an
            // inner join
            Aggregate { .. } => (),
        }
    }

    pub fn visit1<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        use BoxScalarExpr::*;
        match self {
            ColumnReference(..) | BaseColumn(..) | Literal(..) | CallNullary(..) => (),
            CallUnary { expr, .. } => f(expr),
            CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
            Aggregate { expr, .. } => {
                f(expr);
            }
        }
    }

    /// A generalization of `visit`. The function `pre` runs on a
    /// `BoxScalarExpr` before it runs on any of the child `BoxScalarExpr`s.
    /// The function `post` runs on child `BoxScalarExpr`s first before the
    /// parent. Optionally, `pre` can return which child `BoxScalarExpr`s, if
    /// any, should be visited (default is to visit all children).
    pub fn visit_pre_post<F1, F2>(&self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self),
    {
        let to_visit = pre(self);
        if let Some(to_visit) = to_visit {
            for e in to_visit {
                e.visit_pre_post(pre, post);
            }
        } else {
            self.visit1(|e| e.visit_pre_post(pre, post));
        }
        post(self);
    }

    pub fn collect_column_references_from_context(
        &self,
        context: &QuantifierSet,
        column_refs: &mut HashSet<ColumnReference>,
    ) {
        self.visit_pre_post(&mut |_| None, &mut |expr| {
            if let BoxScalarExpr::ColumnReference(c) = expr {
                if context.contains(&c.quantifier_id) {
                    column_refs.insert(c.clone());
                }
            }
        })
    }
}

impl ColumnReference {
    /// Returns the type of the underlying expression behind the column
    /// reference.
    pub fn typ(&self, model: &Model) -> ColumnType {
        let input_box = model.get_quantifier(self.quantifier_id).input_box;
        let mut column_type = model.get_box(input_box).column_type(model, self.position);
        if !column_type.nullable {
            // Column references from a scalar subqueries are always nullable unless
            // the exactly-one-row comdition is satisfied by the input box
            let q = model.get_quantifier(self.quantifier_id);
            if q.quantifier_type == QuantifierType::Scalar {
                column_type.nullable = true;
            }
        }
        column_type
    }
}
