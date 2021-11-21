use super::*;
use ir_tools::IR;

pub mod hir {
    use super::*;
    use sql::plan::AggregateFunc;

    #[derive(IR, Debug, Clone, PartialEq, Eq, Hash)]
    #[output_sort(Aggregate)]
    pub struct AggregateSpec<T: AdtAggregate + AdtScalar> {
        pub func: AggregateFunc,
        pub expr: Box<T::Scalar>,
        pub distinct: bool,
    }
}

pub mod mir {
    use super::*;
    use expr::AggregateFunc;

    #[derive(IR, Debug, Clone, PartialEq, Eq, Hash)]
    #[output_sort(Aggregate)]
    pub struct AggregateSpec<T: AdtAggregate + AdtScalar> {
        pub func: AggregateFunc,
        pub expr: Box<T::Scalar>,
        pub distinct: bool,
    }
}
