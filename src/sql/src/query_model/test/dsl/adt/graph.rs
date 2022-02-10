// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::ColumnType;
use mz_repr::GlobalId;

use super::scalar::ScalarExpr;
use crate::query_model::model::QuantifierType;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct Model {
    pub box_defs: Vec<Def<Box>>,
    pub root_box: Select,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct Def<T> {
    pub sym: String,
    pub val: T,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) enum RefOrVal<T: Into<Self>> {
    Ref(String),
    Val(T),
}

impl From<Box> for RefOrVal<Box> {
    fn from(expr: Box) -> Self {
        RefOrVal::Val(expr)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct Get {
    pub id: GlobalId,
    pub keys: Vec<Vec<usize>>,
    pub typ: Vec<ColumnType>,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct Select {
    pub quantifiers: Vec<Def<Quantifier>>,
    pub predicates: Vec<ScalarExpr>,
    pub columns: Vec<ScalarExpr>,
    // pub order_key: Option<Vec<ScalarExpr>>,
    // pub limit: Option<ScalarExpr>,
    // pub offset: Option<ScalarExpr>,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) enum Box {
    Get(Get),
    Select(Select),
}

impl From<Get> for Box {
    fn from(expr: Get) -> Self {
        Box::Get(expr)
    }
}

impl From<Select> for Box {
    fn from(expr: Select) -> Self {
        Box::Select(expr)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct Quantifier {
    pub q_type: QuantifierType,
    pub in_box: RefOrVal<Box>,
}
