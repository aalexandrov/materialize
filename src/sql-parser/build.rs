// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;
use std::{env, fs};

use anyhow::{Context, Result};

const AST_DEFS_MOD: &str = "src/ast/defs.rs";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").context("Cannot read OUT_DIR env var")?);

    // Generate AST visitors.
    {
        let ir = mz_walkabout::load(AST_DEFS_MOD)?;
        let fold = mz_walkabout::gen_fold(&ir);
        let visit = mz_walkabout::gen_visit(&ir);
        let visit_mut = mz_walkabout::gen_visit_mut(&ir);
        fs::write(out_dir.join("fold.rs"), fold)?;
        fs::write(out_dir.join("visit.rs"), visit)?;
        fs::write(out_dir.join("visit_mut.rs"), visit_mut)?;
    }
    // Generate derived items for AST types modelling simple options.
    {
        let defs = simple_options::load(AST_DEFS_MOD)?;
        // Generate `Parser` methods.
        let parse = simple_options::gen_parse(&defs);
        println!("DEBUG:\n{parse}");
        fs::write(out_dir.join("parse.simple_options.rs"), parse)?;
        // Generate `AstDisplay` implementations.
        let display = simple_options::gen_display(&defs);
        fs::write(out_dir.join("display.simple_options.rs"), display)?;
    }

    Ok(())
}

mod simple_options {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use syn::{Data, DataEnum, DeriveInput, Ident, Item};

    use mz_ore::codegen::CodegenBuf;

    // TODO: we identify enums using an attribute
    const SIMPLE_OPTION_ENUMS: [&'static str; 2] = ["ExplainPlanOptionName", "ClusterFeatureName"];

    /// Load enum items for which to generate code.
    pub(super) fn load<P>(path: P) -> Result<Vec<DeriveInput>>
    where
        P: AsRef<Path>,
    {
        let mut todo = vec![PathBuf::from(path.as_ref())];
        let mut done = BTreeSet::new();
        let mut result = Vec::new();

        fn is_simple_option_enum(ident: &Ident) -> bool {
            SIMPLE_OPTION_ENUMS.iter().any(|x| ident == x)
        }

        while let Some(path) = todo.pop() {
            let dir = path.parent().expect("missing parent directory");
            let stem = path
                .file_stem()
                .expect("missing file stem")
                .to_str()
                .expect("file stem is not valid UTF-8");

            let src = fs::read_to_string(&path)
                .with_context(|| format!("Failed to read {}", path.display()))?;
            let file = syn::parse_file(&src)
                .with_context(|| format!("Failed to parse {}", path.display()))?;

            for item in file.items {
                match item {
                    Item::Mod(item) if item.content.is_none() => {
                        let path = match stem {
                            "mod" | "lib" => dir.join(format!("{}.rs", item.ident)),
                            _ => dir.join(format!("{}/{}.rs", stem, item.ident)),
                        };
                        if !done.contains(&path) {
                            todo.push(path);
                        }
                    }
                    Item::Enum(item) if is_simple_option_enum(&item.ident) => {
                        result.push(DeriveInput {
                            ident: item.ident,
                            vis: item.vis,
                            attrs: item.attrs,
                            generics: item.generics,
                            data: Data::Enum(DataEnum {
                                enum_token: item.enum_token,
                                brace_token: item.brace_token,
                                variants: item.variants,
                            }),
                        });
                    }
                    _ => (),
                }
            }

            assert_eq!(done.insert(path), true);
        }

        Ok(result)
    }

    pub(super) fn gen_display(items: &[DeriveInput]) -> String {
        let mut buf = CodegenBuf::new();
        for item in items {
            if let Data::Enum(enum_item) = &item.data {
                write_ast_display(&item.ident, enum_item, &mut buf)
            }
        }
        buf.into_string()
    }

    fn write_ast_display(ident: &Ident, enum_item: &DataEnum, buf: &mut CodegenBuf) {
        let typ = ident.to_string();
        let fn_fmt = "fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>)";

        buf.write_block(format!("impl AstDisplay for {typ}"), |buf| {
            buf.write_block(fn_fmt, |buf| {
                buf.write_block("match self", |buf| {
                    for v in enum_item.variants.iter().map(|v| v.ident.to_string()) {
                        let ts = separate_tokens(&v, ' ').to_uppercase();
                        buf.writeln(format!(r#"Self::{v} => f.write_str("{ts}"),"#));
                    }
                });
            });
        });
        buf.end_line();
    }

    pub(super) fn gen_parse(items: &[DeriveInput]) -> String {
        let mut buf = CodegenBuf::new();
        buf.write_block("impl<'a> Parser<'a>", |mut buf| {
            for item in items {
                if let Data::Enum(enum_item) = &item.data {
                    write_fn_parse(&item.ident, enum_item, &mut buf)
                }
            }
        });
        buf.into_string()
    }

    fn write_fn_parse(ident: &Ident, enum_item: &DataEnum, buf: &mut CodegenBuf) {
        let typ = ident.to_string();
        let msg = separate_tokens(&typ, ' ').to_lowercase();
        let fn_name = format!("parse_{}", separate_tokens(&typ, '_').to_lowercase());
        let fn_type = format!("Result<{typ}, ParserError>");

        buf.write_block(format!("fn {fn_name}(&mut self) -> {fn_type}"), |b| {
            for v in enum_item.variants.iter().map(|v| v.ident.to_string()) {
                let kws = separate_tokens(&v, ',').to_uppercase();
                b.write_block(format!("if self.parse_keywords(&[{kws}])"), |buf| {
                    buf.writeln(format!(r#"return Ok({typ}::{v})"#));
                });
            }
            b.write_block("", |b| {
                b.writeln(format!(r#"let msg = "a valid {msg}".to_string();"#));
                b.writeln(format!(r#"Err(self.error(self.peek_pos(), msg))"#));
            });
        });
        buf.end_line();
    }

    fn separate_tokens(name: &str, sep: char) -> String {
        let mut buf = String::new();
        let mut prev = sep;
        for ch in name.chars() {
            if ch.is_uppercase() && prev != sep {
                buf.push(sep);
            }
            buf.push(ch);
            prev = ch;
        }
        buf
    }
}
