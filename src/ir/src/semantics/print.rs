use crate::syntax::relation::*;
use crate::syntax::scalar::*;
use crate::syntax::*;
use expr::{EvalError, Id};
use ir_tools::alg;
use repr::{ColumnType, RelationType, Row};

pub struct Print;

impl TypScalar for Print {
    type Scalar = String;
}
impl TypRelation for Print {
    type Relation = String;
}

#[alg(Print)]
fn get(id: Id, _typ: RelationType) -> String {
    format!("Get({})", id)
}
#[alg(Print)]
fn project(input: String, outputs: Vec<usize>) -> String {
    format!("Project({:?})({})", outputs, input)
}
#[alg(Print)]
fn map(input: String, scalars: Vec<String>) -> String {
    format!("Map({})({})", scalars.join("; "), input)
}
#[alg(Print)]
fn filter(input: String, predicates: Vec<String>) -> String {
    format!("Filter({})({})", predicates.join(" && "), input)
}

pub mod hir {
    use super::*;
    use crate::syntax::relation::hir::*;
    use crate::syntax::scalar::hir::*;

    #[alg(Print)]
    fn constant(rows: Vec<Row>, _typ: RelationType) -> String {
        format!("{:?}", rows)
    }

    #[alg(Print)]
    fn literal(row: Row, _typ: ColumnType) -> String {
        format!("{:?}", row)
    }

    #[alg(Print)]
    fn parameter(id: usize) -> String {
        format!(":{}", id)
    }

    #[alg(Print)]
    fn column(level: usize, column: usize) -> String {
        let digits = (column as f64).log10().ceil() as usize;
        format!("#{:^>1$}", column, level + 1 + digits)
    }
}

pub mod mir {
    use super::*;
    use crate::syntax::scalar::mir::*;

    #[alg(Print)]
    fn literal(row: Result<Row, EvalError>, _typ: ColumnType) -> String {
        format!("{:?}", row)
    }

    #[alg(Print)]
    fn column(column: usize) -> String {
        format!("#{}", column)
    }
}
