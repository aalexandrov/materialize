// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{ColumnType, ScalarType};

/// Custom keywords required by the [`syn::parse::Parse`] implementations.
mod kw {}

/// Parse a [`ColumnType`].
impl syn::parse::Parse for ColumnType {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let scalar_type = input.parse::<ScalarType>()?;
        let mut nullable = false;

        if input.peek(syn::Token![?]) {
            input.parse::<syn::Token![?]>()?;
            nullable = true;
        };

        Ok(ColumnType {
            scalar_type,
            nullable,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::adt::varchar::VarCharMaxLength;

    use super::*;

    #[test]
    fn parse_int() {
        let act: ColumnType = syn::parse_quote!(INT?);
        let exp = ColumnType {
            scalar_type: ScalarType::Int32,
            nullable: true,
        };
        assert_eq!(act, exp);
    }

    #[test]
    fn parse_varchar() {
        let act: ColumnType = syn::parse_quote!(VARCHAR(25));
        let exp = {
            let max_length = Some(VarCharMaxLength::try_from(25).unwrap());
            ColumnType {
                scalar_type: ScalarType::VarChar { max_length },
                nullable: false,
            }
        };
        assert_eq!(act, exp);
    }
}
