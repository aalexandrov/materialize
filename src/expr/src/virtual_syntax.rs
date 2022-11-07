// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A set of virtual nodes that are used to recover some high-level
//! concepts that are desugared to non-trival terms in some IRs.

use mz_repr::{GlobalId, RelationType, Row};

pub trait IR: Sized {
    type Relation;
    type Scalar;
}

#[allow(missing_debug_implementations)]
pub struct Except<'a, U: IR> {
    pub all: bool,
    pub lhs: &'a U::Relation,
    pub rhs: &'a U::Relation,
}

pub trait AlgExcept: IR {
    fn except(all: &bool, lhs: Self::Relation, rhs: Self::Relation) -> Self::Relation;
    fn un_except<'a>(expr: &'a Self::Relation) -> Option<Except<'a, Self>>;
}

#[allow(missing_debug_implementations)]
pub struct IndexedFilter<'a> {
    // The id of the index
    pub id: &'a GlobalId,
    // The type of the records in the index
    pub typ: &'a RelationType,
    // The values that we are looking up
    pub constants: Option<&'a Vec<Row>>,
}

pub trait AlgIndexedFilter: IR {
    fn indexed_filter(
        id: &GlobalId,
        typ: &RelationType,
        constants: Option<&Vec<Row>>,
    ) -> Self::Relation;
    fn un_indexed_filter<'a>(expr: &'a Self::Relation) -> Option<IndexedFilter<'a>>;
}
