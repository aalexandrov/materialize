mod common;

// use common::examples;
// use ir::hir::HIR;

// #[test]
// fn it_prints_hir_terms() {
//     let t1 = examples::t1::<HIR>();
//     assert_eq!(t1.print(), "((3 + 4) * 3)");
//     let t2 = examples::t2::<HIR>();
//     assert_eq!(
//         t2.print(),
//         "Project([0, 1])(Map((#1 * 4))([Row{[Int32(1), Int32(2), Int32(3)]}]))"
//     );
//     let t3 = examples::t3::<HIR>();
//     assert_eq!(
//         t3.print(),
//         "Filter(#0 && #1)(Map((#1 * 4))([Row{[Int32(1), Int32(2), Int32(3)]}]))"
//     );
// }

// #[test]
// fn it_prints_mir_terms() {
//     let t1 = examples::t1::<MIR>();
//     assert_eq!(t1.print(), "((3 + 4) * 3)");
//     let t2 = examples::t2::<MIR>();
//     assert_eq!(
//         t2.print(),
//         "Project([0, 1])(Map((#1 * 4))([Row{[Int32(1), Int32(2), Int32(3)]}]))"
//     );
//     let t3 = examples::t3::<MIR>();
//     assert_eq!(
//         t3.print(),
//         "Filter(#0 && #1)(Map((#1 * 4))([Row{[Int32(1), Int32(2), Int32(3)]}]))"
//     );
// }
