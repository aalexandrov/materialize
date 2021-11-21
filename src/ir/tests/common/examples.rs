// use ir::{hir, syntax::*};

// pub fn t1<IR>() -> IR::Scalar
// where
//     IR: AdtAggregate + AdtScalar + AdtRelation,
//     IR: hir::AlgAggregate<IR> + hir::AlgScalar<IR> + hir::AlgRelation<IR>,
// {
//     IR::mul(IR::add(IR::i_64(3), IR::i_64(4)), IR::i_64(3))
// }

// pub fn t2<IR>() -> IR::Relation
// where
//     IR: AdtAggregate + AdtScalar + AdtRelation,
//     IR: hir::AlgAggregate<IR> + hir::AlgScalar<IR> + hir::AlgRelation<IR>,
// {
//     IR::project(
//         IR::map(
//             IR::constant(vec![row::R(1, 2, 3)], schema::R()),
//             vec![IR::mul(IR::column(r#ref(0, 1)), IR::i_64(4))],
//         ),
//         vec![0, 1],
//     )
// }

// pub fn t3<IR>() -> IR::Relation
// where
//     IR: AdtAggregate + AdtScalar + AdtRelation,
//     IR: hir::AlgAggregate<IR> + hir::AlgScalar<IR> + hir::AlgRelation<IR>,
// {
//     IR::filter(
//         IR::map(
//             IR::constant(vec![row::R(1, 2, 3)], schema::R()),
//             vec![IR::mul(IR::column(r#ref(0, 1)), IR::i_64(4))],
//         ),
//         vec![IR::column(r#ref(0, 0)), IR::column(r#ref(0, 1))],
//     )
// }

// fn r#ref(level: usize, column: usize) -> ColumnRef {
//     ColumnRef { level, column }
// }

#[allow(non_snake_case)]
mod row {
    use repr::{Datum, Row};

    #[allow(unused)]
    pub(crate) fn R(x: i32, y: i32, z: i32) -> Row {
        Row::pack_slice(&[Datum::Int32(x), Datum::Int32(y), Datum::Int32(z)])
    }
}

#[allow(non_snake_case)]
mod schema {
    use repr::{ColumnType, RelationType, ScalarType};

    #[allow(unused)]
    pub(super) fn R() -> RelationType {
        let i32_type = ColumnType {
            scalar_type: ScalarType::Int32,
            nullable: false,
        };
        RelationType {
            column_types: vec![i32_type.clone(), i32_type.clone(), i32_type.clone()],
            keys: vec![],
        }
    }
}
