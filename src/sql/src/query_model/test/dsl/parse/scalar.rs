// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::query_model::test::dsl::adt::scalar::*;
use crate::query_model::test::dsl::util::*;
use mz_expr::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
use mz_repr::ColumnType;
use serde_json::from_str;
use syn::parse::discouraged::Speculative;

use super::fork_and_parse;
use super::parse_usize;

/// Custom keywords required by the [`syn::parse::Parse`] implementations.
mod kw {
    syn::custom_keyword!(base);
}

/// Parse [`BaseColumn`] node.
///
/// Expected syntax: `base($i): $type`.
impl syn::parse::Parse for BaseColumn {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let content;
        input.parse::<kw::base>()?;
        syn::parenthesized!(content in input);
        let position = parse_usize(&content)?;
        input.parse::<syn::Token![:]>()?;
        let column_type = input.parse::<ColumnType>()?;

        Ok(BaseColumn {
            position,
            column_type,
        })
    }
}

/// Parse [`ColumnReference`] node.
///
/// Expected syntax: `${quantifier_id}.${position}`.
impl syn::parse::Parse for ColumnReference {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let quantifier_id = input.parse::<syn::Ident>()?.to_string();
        input.parse::<syn::Token![.]>()?;
        let position = super::parse_usize(input)?;

        Ok(ColumnReference {
            quantifier_id,
            position,
        })
    }
}

/// Parse [`Literal`] node. Currently supports boolean, string, int32, and float32 literals.
impl syn::parse::Parse for Literal {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        if let Ok((expr, fork)) = fork_and_parse::<syn::Lit>(input) {
            let expr = match expr {
                syn::Lit::Bool(expr) => Ok(exp::lit::bool(expr.value).into()),
                syn::Lit::Int(expr) => match expr.base10_parse() {
                    Ok(value) => Ok(exp::lit::int32(value).into()),
                    Err(err) => Err(input.error(format!("{}", err))),
                },
                syn::Lit::Float(expr) => match expr.base10_parse() {
                    Ok(value) => Ok(exp::lit::int32(value).into()),
                    Err(err) => Err(input.error(format!("{}", err))),
                },
                syn::Lit::Str(expr) => Ok(exp::lit::string(&expr.value()).into()),
                _ => Err(input.error(format!("unsupported literal type"))),
            };
            if expr.is_ok() {
                input.advance_to(&fork);
            }
            expr
        } else {
            Err(input.error("cannot parse"))
        }
    }
}

/// Parse [`CallUnmaterializable`] node.
///
/// Expected syntax: `$func`.
impl syn::parse::Parse for CallUnmaterializable {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let func = snake_to_camel(input.parse::<syn::Ident>()?.to_string());

        let func = if let Ok(func) = from_str::<UnmaterializableFunc>(&func) {
            Ok(func)
        } else if let Ok(func) = from_str::<UnmaterializableFunc>(&as_json_obj(func)) {
            Ok(func)
        } else {
            Err(input.error("expected nullary function name"))
        }?;

        Ok(CallUnmaterializable { func })
    }
}

/// Parse [`CallUnary`] node.
///
/// Expected syntax: `$func($expr)`.
impl<T: syn::parse::Parse> syn::parse::Parse for CallUnary<T> {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        // parse function symbol
        let func = snake_to_camel(input.parse::<syn::Ident>()?.to_string());
        let func = if let Ok(func) = from_str::<UnaryFunc>(&func) {
            Ok(func)
        } else if let Ok(func) = from_str::<UnaryFunc>(&as_json_obj(func)) {
            Ok(func)
        } else {
            Err(input.error("expected unary function name"))
        }?;

        // parse function arguments
        let content;
        syn::parenthesized!(content in input);
        let expr = content.parse::<T>()?;

        Ok(CallUnary { func, expr })
    }
}

/// Parse [`CallBinary`] node.
///
/// Expected syntax: `$func($expr1, $expr2)`.
impl<T: syn::parse::Parse> syn::parse::Parse for CallBinary<T> {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        // parse function symbol
        let func = snake_to_camel(input.parse::<syn::Ident>()?.to_string());
        let func = if let Ok(func) = from_str::<BinaryFunc>(&func) {
            Ok(func)
        } else if let Ok(func) = from_str::<BinaryFunc>(&as_json_obj(func)) {
            Ok(func)
        } else {
            Err(input.error("expected binary function name"))
        }?;

        // parse function arguments
        let content;
        syn::parenthesized!(content in input);
        let mut exprs = content
            .parse_terminated::<_, syn::Token![,]>(T::parse)?
            .into_iter();
        let expr1 = exprs.next().ok_or_else(|| {
            input.error("expected two arguments in a binary function call, found zero")
        })?;
        let expr2 = exprs.next().ok_or_else(|| {
            input.error("expected two arguments in a binary function call, found one")
        })?;

        Ok(CallBinary { func, expr1, expr2 })
    }
}

/// Parse [`CallVariadic`] node.
///
/// Expected syntax: `$func($expr1, ..., $exprN)`.
impl<T: syn::parse::Parse> syn::parse::Parse for CallVariadic<T> {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        // parse function symbol
        let func = snake_to_camel(input.parse::<syn::Ident>()?.to_string());
        let func = if let Ok(func) = from_str::<VariadicFunc>(&func) {
            Ok(func)
        } else if let Ok(func) = from_str::<VariadicFunc>(&as_json_obj(func)) {
            Ok(func)
        } else {
            Err(input.error("expected variadic function name"))
        }?;

        // parse function arguments
        let content;
        syn::parenthesized!(content in input);
        let exprs = content
            .parse_terminated::<_, syn::Token![,]>(T::parse)?
            .into_iter()
            .collect::<Vec<_>>();

        Ok(CallVariadic { func, exprs })
    }
}

/// Parse [`If`] node.
///
/// Expected syntax: `if ($cond) $then else $else)`.
impl<T: syn::parse::Parse> syn::parse::Parse for If<T> {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Err(input.error("`If` syntax is not supported"))
    }
}

/// Parse [`Aggregate`] node.
///
/// Expected syntax: `distinct? aggregate($expr) using $func`.
impl<T: syn::parse::Parse> syn::parse::Parse for Aggregate<T> {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Err(input.error("`Aggregate` syntax is not supported"))
    }
}

/// Parse [`ScalarExpr`].
impl syn::parse::Parse for ScalarExpr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        if let Ok((expr, fork)) = fork_and_parse::<ColumnReference>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else if let Ok((expr, fork)) = fork_and_parse::<BaseColumn>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else if let Ok((expr, fork)) = fork_and_parse::<Literal>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else if let Ok((expr, fork)) = fork_and_parse::<CallUnmaterializable>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else if let Ok((expr, fork)) = fork_and_parse::<CallUnary<Box<Self>>>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else if let Ok((expr, fork)) = fork_and_parse::<CallBinary<Box<Self>>>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else if let Ok((expr, fork)) = fork_and_parse::<CallVariadic<Box<Self>>>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else if let Ok((expr, fork)) = fork_and_parse::<If<Box<Self>>>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else if let Ok((expr, fork)) = fork_and_parse::<Aggregate<Box<Self>>>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else {
            Err(input.error("cannot parse"))
        }
    }
}

/// Convert a `str` from from snake_case to CamelCase
fn snake_to_camel(str: String) -> String {
    str.split('_')
        .map(|s| {
            let mut chars = s.chars();
            let result = chars
                .next()
                .map(|c| c.to_uppercase().chain(chars).collect::<String>())
                .unwrap_or_else(String::new);
            result
        })
        .collect::<Vec<_>>()
        .concat()
}

/// Map `str` to `{"$str":null}`.
fn as_json_obj(str: String) -> String {
    format!(r#"{{"{}":null}}"#, str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::func::IsNull;

    #[test]
    fn parse_base_column() {
        let res: BaseColumn = syn::parse_quote!(base(42): VARCHAR(25)?);
        let act = exp::base(42, typ::varchar(Some(25), true));
        assert_eq!(res, act);
    }

    #[test]
    fn parse_column_ref() {
        let res: ColumnReference = syn::parse_quote!(lhs.0);
        let act = exp::cref("lhs", 0);
        assert_eq!(res, act);
    }

    #[test]
    fn parse_literal_bool() {
        let res: Literal = syn::parse_quote!(true);
        let act = exp::lit::bool(true);
        assert_eq!(res, act);
    }

    #[test]
    fn parse_literal_int32() {
        let res: Literal = syn::parse_quote!(42);
        let act = exp::lit::int32(42);
        assert_eq!(res, act);
    }

    #[test]
    fn parse_literal_float32() {
        let res: Literal = syn::parse_quote!(42.0);
        let act = exp::lit::float32(42.0);
        assert_eq!(res, act);
    }

    #[test]
    fn parse_literal_string() {
        let res: Literal = syn::parse_quote!("Hello, world!");
        let act = exp::lit::string("Hello, world!");
        assert_eq!(res, act);
    }

    #[test]
    fn parse_unary_func() {
        let res: CallUnary<ColumnReference> = syn::parse_quote!(is_null(lhs.0));
        let act = exp::call_unary(UnaryFunc::IsNull(IsNull), exp::cref("lhs", 0));
        assert_eq!(res, act);
    }

    #[test]
    fn parse_binary_func() {
        let res: CallBinary<ColumnReference> = syn::parse_quote!(add_int_16(lhs.0, lhs.1));
        let act = exp::call_binary(
            BinaryFunc::AddInt16,
            exp::cref("lhs", 0),
            exp::cref("lhs", 1),
        );
        assert_eq!(res, act);
    }

    #[test]
    fn parse_variadic_func() {
        let res: CallVariadic<ColumnReference> = syn::parse_quote!(coalesce(lhs.0, lhs.1, lhs.2));
        let act = exp::call_variadic(
            VariadicFunc::Coalesce,
            vec![
                exp::cref("lhs", 0),
                exp::cref("lhs", 1),
                exp::cref("lhs", 2),
            ],
        );
        assert_eq!(res, act);
    }

    #[test]
    fn parse_scalar_expr() {
        let res: ScalarExpr = syn::parse_quote! {
            is_null(coalesce(lhs.0))
        };
        let act = exp::call_unary(
            UnaryFunc::IsNull(IsNull),
            Box::new(
                exp::call_variadic(
                    VariadicFunc::Coalesce,
                    vec![Box::new(exp::cref("lhs", 0).into())],
                )
                .into(),
            ),
        )
        .into();
        assert_eq!(res, act);
    }
}
