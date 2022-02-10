// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::query_model::model::QuantifierType;
use crate::query_model::test::dsl::adt::graph::*;
use crate::query_model::test::dsl::adt::scalar::*;
use mz_repr::ColumnType;
use mz_repr::GlobalId;
use syn::parse::discouraged::Speculative;

use super::fork_and_parse;

/// Custom keywords required by the [`syn::parse::Parse`] implementations.
mod kw {
    syn::custom_keyword!(Model);

    // boxes
    syn::custom_keyword!(Get);
    syn::custom_keyword!(Select);

    // box keywords
    syn::custom_keyword!(User);
    syn::custom_keyword!(id);
    syn::custom_keyword!(keys);
    syn::custom_keyword!(typ);

    // quantifiers
    syn::custom_keyword!(All);
    syn::custom_keyword!(Existential);
    syn::custom_keyword!(Foreach);
    syn::custom_keyword!(PreservedForeach);
    syn::custom_keyword!(Scalar);
}

/// Parse [`Model`] node.
///
/// Expected syntax: `Model { $box_defs in $box }`.
impl syn::parse::Parse for Model {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        input.parse::<kw::Model>()?;

        let inner;
        syn::braced!(inner in input);

        // parse $box_defs
        let mut box_defs = vec![inner.parse::<Def<Box>>()?];
        while inner.peek(syn::Token![let]) {
            let box_def = inner.parse::<Def<Box>>()?;
            box_defs.push(box_def);
        }

        // parse root $box
        inner.parse::<syn::Token![in]>()?;
        let root_box = inner.parse::<Select>()?;

        Ok(Model { box_defs, root_box })
    }
}

/// Parse [`CallVariadic`] node.
///
/// Expected syntax: `let $id = $value`.
impl<T: syn::parse::Parse> syn::parse::Parse for Def<T> {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        input.parse::<syn::Token![let]>()?;
        let sym = input.parse::<syn::Ident>()?.to_string();
        input.parse::<syn::Token![=]>()?;
        let val = input.parse::<T>()?;
        Ok(Def { sym, val })
    }
}

impl<T: syn::parse::Parse + Into<RefOrVal<T>>> syn::parse::Parse for RefOrVal<T> {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        // start with the value, because a reference might be a valid value prefix
        if let Ok((expr, fork)) = fork_and_parse::<T>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else if let Ok((expr, fork)) = fork_and_parse::<syn::Ident>(input) {
            input.advance_to(&fork);
            Ok(RefOrVal::Ref(expr.to_string()))
        } else {
            Err(input.error("expected reference or value"))
        }
    }
}

/// Parse [`Get`] node.
///
/// Expected syntax: `Get { $id_def, $keys_def?, $type_def }`.
impl syn::parse::Parse for Get {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        input.parse::<kw::Get>()?;
        let content;
        syn::braced!(content in input);

        // parse $id_def
        content.parse::<kw::id>()?;
        content.parse::<syn::Token![:]>()?;
        content.parse::<kw::User>()?;
        let id = {
            let inner;
            syn::parenthesized!(inner in content);
            let id = inner.parse::<syn::LitInt>()?.base10_parse::<u64>()?;
            GlobalId::User(id)
        };

        // parse optional $keys_def
        let keys = if content.peek(kw::keys) {
            content.parse::<kw::keys>()?;
            content.parse::<syn::Token![:]>()?;
            parse_keys(&content)?
                .into_iter()
                .map(|key| key.into_iter().collect())
                .collect()
        } else {
            Vec::new()
        };

        // parse $type
        let typ = parse_type(&content)?.into_iter().collect();

        Ok(Get { id, keys, typ })
    }
}

/// Parse [`Get`] node.
///
/// Expected syntax: `Select { $quantifiers, $predicates, $columns, $order_by?, $limit?, $offset? }`.
impl syn::parse::Parse for Select {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        input.parse::<kw::Select>()?;
        let content;
        syn::braced!(content in input);

        // parse $quantifiers
        let mut quantifiers = vec![];
        quantifiers.push(content.parse::<Def<Quantifier>>()?); // at least one
        while content.peek(syn::Token![let]) {
            quantifiers.push(content.parse::<Def<Quantifier>>()?);
        }

        // parse $predicates
        let mut predicates = vec![];
        while content.peek(syn::Token![if]) {
            content.parse::<syn::Token![if]>()?;
            predicates.push(content.parse::<ScalarExpr>()?);
        }

        // parse $columns
        content.parse::<syn::Token![in]>()?;
        let columns = parse_columns(&content)?.into_iter().collect();

        // parse optional $order_by

        // parse optional $limit

        // parse optional $offset

        Ok(Select {
            quantifiers,
            predicates,
            columns,
        })
    }
}

impl syn::parse::Parse for Box {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        if let Ok((expr, fork)) = fork_and_parse::<Get>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else if let Ok((expr, fork)) = fork_and_parse::<Select>(input) {
            input.advance_to(&fork);
            Ok(expr.into())
        } else {
            Err(input.error("cannot parse box"))
        }
    }
}

impl syn::parse::Parse for Quantifier {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let q_type = if input.peek(kw::All) {
            input.parse::<kw::All>()?;
            QuantifierType::All
        } else if input.peek(kw::Existential) {
            input.parse::<kw::Existential>()?;
            QuantifierType::Existential
        } else if input.peek(kw::Foreach) {
            input.parse::<kw::Foreach>()?;
            QuantifierType::Foreach
        } else if input.peek(kw::PreservedForeach) {
            input.parse::<kw::PreservedForeach>()?;
            QuantifierType::PreservedForeach
        } else if input.peek(kw::Scalar) {
            input.parse::<kw::Scalar>()?;
            QuantifierType::Scalar
        } else {
            return Err(input.error("unexpected quantifier type"));
        };

        let content;
        syn::parenthesized!(content in input);
        let in_box = content.parse::<RefOrVal<Box>>()?;

        Ok(Quantifier { q_type, in_box })
    }
}

type ParseKey = syn::punctuated::Punctuated<usize, syn::Token![,]>;
/// Helper parser for a single key in [`Get`] box. Expected syntax: `[$usize, $usize]`.
fn parse_key(input: syn::parse::ParseStream) -> syn::Result<ParseKey> {
    let content;
    syn::bracketed!(content in input);
    ParseKey::parse_separated_nonempty_with(&content, super::parse_usize)
}

type ParseKeys = syn::punctuated::Punctuated<ParseKey, syn::Token![,]>;
/// Helper parser for the `keys` in a [`Get`] box. Expected syntax: `[$keys, $keys]`.
fn parse_keys(input: syn::parse::ParseStream) -> syn::Result<ParseKeys> {
    let content;
    syn::bracketed!(content in input);
    ParseKeys::parse_terminated_with(&content, parse_key)
}

type ParseType = syn::punctuated::Punctuated<ColumnType, syn::Token![,]>;
/// Helper parser for the `typ` in a [`Get`] box. Expected syntax: `[$col_type, $col_type]`.
fn parse_type(input: syn::parse::ParseStream) -> syn::Result<ParseType> {
    let content;
    syn::bracketed!(content in input);
    ParseType::parse_terminated(&content)
}

type ParseColumns = syn::punctuated::Punctuated<ScalarExpr, syn::Token![,]>;
/// Helper parser for the `columns` in a query box. Expected syntax: `[$expr, $expr]`.
fn parse_columns(input: syn::parse::ParseStream) -> syn::Result<ParseColumns> {
    let content;
    syn::bracketed!(content in input);
    ParseColumns::parse_terminated(&content)
}

#[cfg(test)]
mod tests {
    use mz_expr::BinaryFunc;

    use super::*;
    use crate::query_model::test::dsl::util::*;

    #[test]
    fn parse_get() {
        let act: Get = syn::parse_quote! {
            Get {
                id: User(42)
                keys: [[0], [1]]
                [BOOL, INT?, REAL?]
            }
        };
        let exp: Get = examples::get(42, vec![vec![0], vec![1]]);
        assert_eq!(act, exp);
    }

    #[test]
    fn parse_select() {
        let act: Select = syn::parse_quote! {
            Select {
                let lhs = Foreach(B1)
                let rhs = Foreach(B2)
                if eq(lhs.0, rhs.0)
                in [lhs.1, rhs.1]
            }
        };
        let exp: Select = examples::select();
        assert_eq!(act, exp);
    }

    #[test]
    fn test_graph() {
        let act: Model = syn::parse_quote! {
            Model {
                let B1 = Get {
                    id: User(24)
                    keys: [[0]]
                    [BOOL, INT?, REAL?]
                }
                let B2 = Get {
                    id: User(42)
                    keys: [[0], [1]]
                    [BOOL, INT?, REAL?]
                }
                in Select {
                    let lhs = Foreach(B1)
                    let rhs = Foreach(B2)
                    if eq(lhs.0, rhs.0)
                    in [lhs.1, rhs.1]
                }
            }
        };
        let exp: Model = examples::model();
        assert_eq!(act, exp);
    }

    mod examples {
        use super::*;

        pub(crate) fn get(id: u64, keys: Vec<Vec<usize>>) -> Get {
            Get {
                id: GlobalId::User(id),
                keys,
                typ: vec![typ::bool(false), typ::int32(true), typ::float32(true)],
            }
        }

        pub(crate) fn select() -> Select {
            Select {
                quantifiers: vec![
                    Def {
                        sym: "lhs".to_string(),
                        val: Quantifier {
                            q_type: QuantifierType::Foreach,
                            in_box: RefOrVal::Ref("B1".to_string()),
                        },
                    },
                    Def {
                        sym: "rhs".to_string(),
                        val: Quantifier {
                            q_type: QuantifierType::Foreach,
                            in_box: RefOrVal::Ref("B2".to_string()),
                        },
                    },
                ],
                predicates: vec![exp::call_binary(
                    BinaryFunc::Eq,
                    std::boxed::Box::new(exp::cref("lhs", 0).into()),
                    std::boxed::Box::new(exp::cref("rhs", 0).into()),
                )
                .into()],
                columns: vec![exp::cref("lhs", 1).into(), exp::cref("rhs", 1).into()],
            }
        }

        pub(crate) fn model() -> Model {
            Model {
                box_defs: vec![
                    Def {
                        sym: "B1".to_string(),
                        val: Box::from(get(24, vec![vec![0]])),
                    },
                    Def {
                        sym: "B2".to_string(),
                        val: Box::from(get(42, vec![vec![0], vec![1]])),
                    },
                ],
                root_box: select(),
            }
        }
    }
}
