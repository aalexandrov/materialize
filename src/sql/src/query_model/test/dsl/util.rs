// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::query_model::test::dsl::adt::scalar::*;
use mz_expr::*;
use mz_repr::*;

// pub(crate) use crate::query_model::test::util::qgm;
pub(crate) use crate::query_model::test::util::typ;

/// This is the same as [`super::util::exp`], but produces [`adt::scalar`] expressions.
pub(crate) mod exp {
    use super::*;

    pub(crate) fn cref(quantifier_id: &str, position: usize) -> ColumnReference {
        ColumnReference {
            quantifier_id: quantifier_id.to_string(),
            position,
        }
    }

    pub(crate) fn base(position: usize, column_type: ColumnType) -> BaseColumn {
        BaseColumn {
            position,
            column_type,
        }
    }

    pub(crate) fn call_unary<T>(func: UnaryFunc, expr: T) -> CallUnary<T> {
        CallUnary { func, expr }
    }

    pub(crate) fn call_binary<T>(func: BinaryFunc, expr1: T, expr2: T) -> CallBinary<T> {
        CallBinary { func, expr1, expr2 }
    }

    pub(crate) fn call_variadic<T>(func: VariadicFunc, exprs: Vec<T>) -> CallVariadic<T> {
        CallVariadic { func, exprs }
    }

    pub(crate) mod lit {
        use super::*;

        pub(crate) fn bool(value: bool) -> Literal {
            Literal {
                row: Row::pack(&[if value { Datum::True } else { Datum::False }]),
                column_type: typ::bool(true),
            }
        }

        pub(crate) fn string(value: &str) -> Literal {
            Literal {
                row: Row::pack(&[Datum::String(value)]),
                column_type: typ::string(true),
            }
        }

        pub(crate) fn int32(value: i32) -> Literal {
            Literal {
                row: Row::pack(&[Datum::from(value)]),
                column_type: typ::int32(true),
            }
        }

        pub(crate) fn float32(value: f32) -> Literal {
            Literal {
                row: Row::pack(&[Datum::from(value)]),
                column_type: typ::float32(true),
            }
        }
    }
}
