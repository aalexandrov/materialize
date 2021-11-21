use super::*;
use expr::{ColumnOrder, EvalError, Id, LocalId, TableFunc};
use ir_tools::IR;
use repr::{Diff, RelationType, Row};
use sql::plan::JoinKind;

///! Syntactic forms that can be used represent SQL exeuction plans.

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct Get {
    pub id: Id,
    pub typ: RelationType,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct With<T: AdtRelation> {
    pub id: LocalId,
    pub value: Box<T::Relation>,
    pub body: Box<T::Relation>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct Project<T: AdtRelation> {
    pub input: Box<T::Relation>,
    pub outputs: Vec<usize>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct Map<T: AdtRelation + AdtScalar> {
    pub input: Box<T::Relation>,
    pub scalars: Vec<T::Scalar>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct FlatMap<T: AdtRelation + AdtScalar> {
    pub input: Box<T::Relation>,
    pub func: TableFunc,
    pub scalars: Vec<T::Scalar>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct Filter<T: AdtRelation + AdtScalar> {
    pub input: Box<T::Relation>,
    pub predicates: Vec<T::Scalar>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct CallTable<T: AdtRelation + AdtScalar> {
    pub func: TableFunc,
    pub scalars: Vec<T::Scalar>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct Distinct<T: AdtRelation> {
    pub input: Box<T::Relation>,
}

/// Groups and orders within each group, limiting output.
#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct TopK<T: AdtRelation> {
    /// The source collection.
    pub input: Box<T::Relation>,
    /// Column indices used to form groups.
    pub group_key: Vec<usize>,
    /// Column indices used to order rows within groups.
    pub order_key: Vec<ColumnOrder>,
    /// Number of records to retain
    pub limit: Option<usize>,
    /// Number of records to skip
    pub offset: usize,
    // @todo: input monotonic flag as a derived attribute,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct Negate<T: AdtRelation> {
    pub input: Box<T::Relation>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct Threshold<T: AdtRelation> {
    pub input: Box<T::Relation>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct Union<T: AdtRelation> {
    pub base: Box<T::Relation>,
    pub inputs: Vec<T::Relation>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct DeclareKeys<T: AdtRelation> {
    pub input: Box<T::Relation>,
    pub keys: Vec<Vec<usize>>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Relation)]
pub struct ArrangeBy<T: AdtRelation + AdtScalar> {
    pub input: Box<T::Relation>,
    pub keys: Vec<Vec<T::Scalar>>,
}

pub mod hir {
    use super::*;

    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Relation)]
    pub struct Constant {
        pub rows: Vec<Row>,
        pub typ: RelationType,
    }

    /// Returns a single row with the aggregates evaluated over empty groups
    /// when `key` is empty AND `input` is empty.
    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Relation)]
    pub struct Reduce<T: AdtRelation + AdtAggregate> {
        pub input: Box<T::Relation>,
        pub group_key: Vec<usize>,
        pub aggregates: Vec<T::Aggregate>,
        pub expected_group_size: Option<usize>,
        // @todo: input monotonic flag as a derived attribute,
    }

    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Relation)]
    pub struct Join<T: AdtRelation + AdtScalar> {
        pub left: Box<T::Relation>,
        pub right: Box<T::Relation>,
        pub on: Box<T::Scalar>,
        pub kind: JoinKind,
    }
}

pub mod mir {
    use super::*;

    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Relation)]
    pub struct Constant {
        pub rows: Result<Vec<(Row, Diff)>, EvalError>,
        pub typ: RelationType,
    }

    /// Returns zero when `key` is empty AND `input` is empty.
    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Relation)]
    pub struct Reduce<T: AdtRelation + AdtScalar + AdtAggregate> {
        pub input: Box<T::Relation>,
        pub group_key: Vec<T::Scalar>,
        pub aggregates: Vec<T::Aggregate>,
        pub expected_group_size: Option<usize>,
        // @todo: input monotonic flag as a derived attribute,
    }

    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Relation)]
    pub struct Join<T: AdtRelation + AdtScalar> {
        pub inputs: Vec<T::Relation>,
        pub equivalences: Vec<Vec<T::Scalar>>,
        // @todo: JoinImplementation as a derived attribute,
    }
}
