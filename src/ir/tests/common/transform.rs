use std::marker::PhantomData;

use expr::MirRelationExpr;
use sql::ast::Raw;
use sql::plan::plan_root_query;
use sql::plan::{HirRelationExpr, QueryLifetime, StatementContext};
use sql_parser::ast::Statement;
use sql_parser::parser::ParserError;

use super::catalog::TestCatalog;

pub struct PipelineError {
    pub msg: String,
}
impl PipelineError {
    fn new(msg: String) -> PipelineError {
        PipelineError { msg }
    }
}
impl From<&str> for PipelineError {
    fn from(str: &str) -> PipelineError {
        PipelineError {
            msg: str.to_string(),
        }
    }
}
impl From<ParserError> for PipelineError {
    fn from(e: ParserError) -> PipelineError {
        PipelineError {
            msg: format!("unable to parse SQL: {}", e),
        }
    }
}

#[macro_export]
macro_rules! pipeline {
    ($e:expr) => { $e };
    ($e:expr, $($es:expr$(,)?)+) => {{
        let f = $e;
        let g = pipeline!($($es),*);
        crate::common::transform::Compose::new(f, g)
    }};
}

pub trait Pipeline<A, B> {
    fn apply(&self, input: A) -> Result<B, PipelineError>;
}

#[derive(Clone)]
pub struct Compose<F, G, A, B, C>
where
    F: Pipeline<A, B>,
    G: Pipeline<B, C>,
{
    f: F,
    g: G,
    a: PhantomData<A>,
    b: PhantomData<B>,
    c: PhantomData<C>,
}
impl<F, G, A, B, C> Compose<F, G, A, B, C>
where
    F: Pipeline<A, B>,
    G: Pipeline<B, C>,
{
    #[allow(dead_code)]
    pub fn new(f: F, g: G) -> Compose<F, G, A, B, C> {
        Compose {
            f,
            g,
            a: PhantomData,
            b: PhantomData,
            c: PhantomData,
        }
    }
}
impl<F, G, A, B, C> Pipeline<A, C> for Compose<F, G, A, B, C>
where
    F: Pipeline<A, B>,
    G: Pipeline<B, C>,
{
    fn apply(&self, input: A) -> Result<C, PipelineError> {
        self.g.apply(self.f.apply(input)?)
    }
}

#[derive(Clone, Default)]
pub struct Parser;
impl Pipeline<&String, Statement<Raw>> for Parser {
    fn apply(&self, string: &String) -> Result<Statement<Raw>, PipelineError> {
        let stmts = sql_parser::parser::parse_statements(string)?;
        match stmts.len() {
            1 => Ok(stmts.into_iter().next().unwrap()),
            _ => Err(PipelineError::from("expected a single statement")),
        }
    }
}

#[derive(Clone)]
pub struct Planner<'a> {
    catalog: &'a TestCatalog,
}
impl Planner<'_> {
    #[allow(dead_code)]
    pub fn new<'a>(catalog: &'a TestCatalog) -> Planner<'a> {
        Planner { catalog }
    }
}
impl<'a> Pipeline<Statement<Raw>, HirRelationExpr> for Planner<'a> {
    fn apply(&self, stmt: Statement<Raw>) -> Result<HirRelationExpr, PipelineError> {
        // assert that query is Select
        let query = match stmt {
            Statement::Select(query) => Ok(query.query),
            _ => Err(PipelineError::from("unable to plan non-Select query")),
        }?;
        // plan query to a HirRelationExpr
        let scx = &StatementContext::new(None, self.catalog);
        match plan_root_query(scx, query, QueryLifetime::Static) {
            Ok(q) => Ok(q.expr),
            Err(e) => Err(PipelineError::new(format!("unable to plan query: {}", e))),
        }
    }
}

#[derive(Clone, Default)]
pub struct Lower;
impl Pipeline<HirRelationExpr, MirRelationExpr> for Lower {
    fn apply(&self, expr: HirRelationExpr) -> Result<MirRelationExpr, PipelineError> {
        Ok(expr.lower())
    }
}

#[derive(Clone, Default)]
pub struct Into<A, B>
where
    A: std::convert::Into<B>,
{
    a: PhantomData<A>,
    b: PhantomData<B>,
}
impl<A, B> Into<A, B>
where
    A: std::convert::Into<B>,
{
    #[allow(dead_code)]
    pub fn new() -> Into<A, B> {
        Into {
            a: PhantomData,
            b: PhantomData,
        }
    }
}
impl<A, B: From<A>> Pipeline<A, B> for Into<A, B> {
    fn apply(&self, expr: A) -> Result<B, PipelineError> {
        Ok(expr.into())
    }
}
