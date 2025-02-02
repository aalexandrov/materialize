// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

mod doc;
mod util;

use mz_sql_parser::ast::*;
use mz_sql_parser::parser::{parse_statements, ParserStatementError};
use pretty::RcDoc;

use crate::doc::{
    doc_create_materialized_view, doc_create_view, doc_display, doc_insert, doc_select_statement,
};

const TAB: isize = 4;

fn to_doc<T: AstInfo>(v: &Statement<T>) -> RcDoc {
    match v {
        Statement::Select(v) => doc_select_statement(v),
        Statement::Insert(v) => doc_insert(v),
        Statement::CreateView(v) => doc_create_view(v),
        Statement::CreateMaterializedView(v) => doc_create_materialized_view(v),
        _ => doc_display(v, "statement"),
    }
}

/// Pretty prints a statement at a width.
pub fn to_pretty<T: AstInfo>(stmt: &Statement<T>, width: usize) -> String {
    format!("{};", to_doc(stmt).pretty(width))
}

/// Parses `str` into SQL statements and pretty prints them.
pub fn pretty_strs(str: &str, width: usize) -> Result<Vec<String>, ParserStatementError> {
    let stmts = parse_statements(str)?;
    Ok(stmts.iter().map(|s| to_pretty(&s.ast, width)).collect())
}

/// Parses `str` into a single SQL statement and pretty prints it.
pub fn pretty_str(str: &str, width: usize) -> Result<String, ParserStatementError> {
    Ok(pretty_strs(str, width)?.join("\n\n"))
}
