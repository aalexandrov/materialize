use super::*;
use expr::{BinaryFunc, EvalError, NullaryFunc, UnaryFunc, VariadicFunc};
use ir_tools::IR;
use repr::{ColumnType, Row};

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Scalar)]
pub struct Parameter {
    pub id: usize,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Scalar)]
pub struct CallNullary {
    pub func: NullaryFunc,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Scalar)]
pub struct CallUnary<T: AdtScalar> {
    pub func: UnaryFunc,
    pub expr: Box<T::Scalar>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Scalar)]
pub struct CallBinary<T: AdtScalar> {
    pub func: BinaryFunc,
    pub expr1: Box<T::Scalar>,
    pub expr2: Box<T::Scalar>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Scalar)]
pub struct CallVariadic<T: AdtScalar> {
    pub func: VariadicFunc,
    pub exprs: Vec<T::Scalar>,
}

#[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
#[output_sort(Scalar)]
pub struct IfThenElse<T: AdtScalar> {
    pub cond: Box<T::Scalar>,
    pub then: Box<T::Scalar>,
    pub els: Box<T::Scalar>,
}

/// SQL-specific extensions of the scalar expressions defined in [`crate::syntax::scalar`].
pub mod hir {
    use super::*;

    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Scalar)]
    pub struct Literal {
        pub row: Row,
        pub typ: ColumnType,
    }

    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Scalar)]
    pub struct Column {
        // scope level, where 0 is the current scope and 1+ are outer scopes.
        pub level: usize,
        // level-local column identifier used.
        pub column: usize,
    }

    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Scalar)]
    pub struct Exists<T: AdtScalar + AdtRelation> {
        pub input: Box<T::Relation>,
    }

    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Scalar)]
    pub struct Select<T: AdtScalar + AdtRelation> {
        pub input: Box<T::Relation>,
    }
}

/// Dataflow-specific extensions of the scalar expressions defined in [`crate::syntax::scalar`].
pub mod mir {

    use super::*;

    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Scalar)]
    pub struct Literal {
        pub row: Result<Row, EvalError>,
        pub typ: ColumnType,
    }

    #[derive(IR, Clone, Debug, Eq, Hash, PartialEq)]
    #[output_sort(Scalar)]
    pub struct Column {
        pub column: usize,
    }
}
