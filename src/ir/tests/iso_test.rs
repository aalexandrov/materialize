mod common;

use common::catalog::TestCatalog;
use common::transform::{Into, Lower, Parser, Pipeline, Planner};
use ir::{hir, mir};

#[test]
fn run() {
    let catalog = TestCatalog::default();
    let old_mir = pipeline![Parser::default(), Planner::new(&catalog), Lower::default(),];
    let old_hir = pipeline![Parser::default(), Planner::new(&catalog)];
    let new_mir = pipeline![old_mir.clone(), Into::<_, mir::Relation>::new()];
    let new_hir = pipeline![old_hir.clone(), Into::<_, hir::Relation>::new()];
    datadriven::walk("tests/testdata/iso", |f| {
        f.run(|test| {
            let hir_result = match (old_hir.apply(&test.input), new_hir.apply(&test.input)) {
                (Err(e), _) => format!("pipeline error: {:}\n", e.msg),
                (_, Err(e)) => format!("pipeline error: {:}\n", e.msg),
                (Ok(l), Ok(r)) => match l == r.into() {
                    true => "HIR=OK\n".into(),
                    false => "HIR=NOT OK\n".into(),
                },
            };
            let mir_result = match (old_mir.apply(&test.input), new_mir.apply(&test.input)) {
                (Err(e), _) => format!("pipeline error: {:}\n", e.msg),
                (_, Err(e)) => format!("pipeline error: {:}\n", e.msg),
                (Ok(l), Ok(r)) => match l == r.into() {
                    true => "MIR=OK\n".into(),
                    false => "MIR=NOT OK\n".into(),
                },
            };
            format!("{}{}", hir_result, mir_result)
        });
    });
}
