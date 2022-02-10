// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{adt::varchar::VarCharMaxLength, ScalarType};

/// Custom keywords required by the [`syn::parse::Parse`] implementations.
mod kw {
    syn::custom_keyword!(INT);
}

/// Parse a [`ScalarType`].
impl syn::parse::Parse for ScalarType {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let ident = input.parse::<syn::Ident>()?;
        match ident.to_string().to_lowercase().as_str() {
            "bigint" | "int8" => Ok(ScalarType::Int64),
            "int" | "int4" => Ok(ScalarType::Int32),
            "boolean" | "bool" => Ok(ScalarType::Bool),
            "bytea" => Ok(ScalarType::Bytes),
            "date" => Ok(ScalarType::Date),
            "float" | "float8" | "double" => Ok(ScalarType::Float64),
            "float4" | "real" => Ok(ScalarType::Float32),
            "interval" => Ok(ScalarType::Interval),
            "jsonb" | "json" => Ok(ScalarType::Jsonb),
            "varchar" => {
                let max_length = parse_max_length(input)?;
                Ok(ScalarType::VarChar { max_length })
            }
            // TODO...
            token => Err(input.error(format!("expected type, found {}", token))),
        }
    }
}

/// Parse an optional `($n)` fragment and return `Some($n)` if present.
fn parse_max_length(input: syn::parse::ParseStream) -> syn::Result<Option<VarCharMaxLength>> {
    let content;
    let length = if input.peek(syn::token::Paren) {
        syn::parenthesized!(content in input);
        let length = content.parse::<syn::LitInt>()?.base10_parse::<i64>()?;
        let length = VarCharMaxLength::try_from(length);
        let length = length.map_err(|e| input.error(format!("{}", e)))?;
        Some(length)
    } else {
        None
    };
    Ok(length)
}

#[cfg(test)]
mod tests {
    use crate::adt::varchar::VarCharMaxLength;

    use super::*;

    #[test]
    fn parse_int() {
        let act: ScalarType = syn::parse_quote!(INT);
        let exp = ScalarType::Int32;
        assert_eq!(act, exp);
    }

    #[test]
    fn parse_varchar() {
        let act: ScalarType = syn::parse_quote!(VARCHAR(25));
        let exp = {
            let max_length = Some(VarCharMaxLength::try_from(25).unwrap());
            ScalarType::VarChar { max_length }
        };
        assert_eq!(act, exp);
    }
}
