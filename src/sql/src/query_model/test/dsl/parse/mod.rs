// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub(crate) mod graph;
pub(crate) mod scalar;

use syn;

fn parse_usize(input: syn::parse::ParseStream) -> syn::Result<usize> {
    input.parse::<syn::LitInt>()?.base10_parse::<usize>()
}

fn fork_and_parse<T: syn::parse::Parse>(
    input: syn::parse::ParseStream,
) -> syn::Result<(T, syn::parse::ParseBuffer)> {
    let fork = input.fork();
    fork.parse::<T>().map(|expr| (expr, fork))
}
