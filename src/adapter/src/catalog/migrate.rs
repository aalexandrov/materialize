// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use mz_catalog::durable::Transaction;
use mz_ore::collections::CollectionExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::Raw;
use mz_storage_types::connections::ConnectionContext;
use semver::Version;
use tracing::info;

use crate::catalog::{Catalog, CatalogState, ConnCatalog};

async fn rewrite_items<F>(
    tx: &mut Transaction<'_>,
    cat: Option<&ConnCatalog<'_>>,
    mut f: F,
) -> Result<(), anyhow::Error>
where
    F: for<'a> FnMut(
        &'a mut Transaction<'_>,
        &'a Option<&ConnCatalog<'_>>,
        &'a mut mz_sql::ast::Statement<Raw>,
    ) -> BoxFuture<'a, Result<(), anyhow::Error>>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.loaded_items();
    for mut item in items {
        let mut stmt = mz_sql::parse::parse(&item.create_sql)?.into_element().ast;

        f(tx, &cat, &mut stmt).await?;

        item.create_sql = stmt.to_ast_string_stable();

        updated_items.insert(item.id, item);
    }
    tx.update_items(updated_items)?;
    Ok(())
}

pub(crate) async fn migrate(
    state: &mut CatalogState,
    txn: &mut Transaction<'_>,
    now: NowFn,
    _connection_context: Option<ConnectionContext>,
) -> Result<(), anyhow::Error> {
    let catalog_version = txn.get_catalog_content_version();
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!("migrating from catalog version {:?}", catalog_version);

    let _now = now();
    // First, do basic AST -> AST transformations.
    // rewrite_items(&mut tx, None, |_tx, _cat, _stmt| Box::pin(async { Ok(()) })).await?;

    // Then, load up a temporary catalog with the rewritten items, and perform
    // some transformations that require introspecting the catalog. These
    // migrations are *weird*: they're rewriting the catalog while looking at
    // it. You probably should be adding a basic AST migration above, unless
    // you are really certain you want one of these crazy migrations.
    let state = Catalog::load_catalog_items(txn, state)?;
    let conn_cat = state.for_system_session();
    rewrite_items(txn, Some(&conn_cat), |_tx, cat, item| {
        Box::pin(async move {
            let _conn_cat = cat.expect("must provide access to conn catalog");
            ast_rewrite_create_connection_options_0_77_0(item)?;
            Ok(())
        })
    })
    .await?;

    info!(
        "migration from catalog version {:?} complete",
        catalog_version
    );
    Ok(())
}

// Add new migrations below their appropriate heading, and precede them with a
// short summary of the migration's purpose and optional additional commentary
// about safety or approach.
//
// The convention is to name the migration function using snake case:
// > <category>_<description>_<version>
//
// Note that:
// - The sum of all migrations must be idempotent because all migrations run
//   every time the catalog opens, unless migrations are explicitly disabled.
//   This might mean changing code outside the migration itself, or only
//   executing some migrations when encountering certain versions.
// - Migrations must preserve backwards compatibility with all past releases of
//   Materialize.
//
// Please include @benesch on any code reviews that add or edit migrations.

// ****************************************************************************
// AST migrations -- Basic AST -> AST transformations
// ****************************************************************************

/// Remove any durably recorded `WITH (VALIDATE = true|false)` from `CREATE
/// CONNECTION` statements.
///
/// This `WITH` option only has an effect when creating the connection, and the
/// fact that it was persisted in the item's definition is unclear, especially
/// in light of `ALTER CONNECTION`, which also offers a `WITH (VALIDATE =
/// true|false)` option, but cannot alter the `VALIDATE` clause on the `CREATE
/// CONNECTION` statement.
fn ast_rewrite_create_connection_options_0_77_0(
    stmt: &mut mz_sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::visit_mut::VisitMut;
    struct CreateConnectionRewriter;
    impl<'ast> VisitMut<'ast, Raw> for CreateConnectionRewriter {
        fn visit_create_connection_statement_mut(
            &mut self,
            node: &'ast mut mz_sql::ast::CreateConnectionStatement<Raw>,
        ) {
            node.with_options
                .retain(|o| o.name != mz_sql::ast::CreateConnectionOptionName::Validate);
        }
    }

    CreateConnectionRewriter.visit_statement_mut(stmt);
    Ok(())
}

// ****************************************************************************
// Semantic migrations -- Weird migrations that require access to the catalog
// ****************************************************************************

fn _add_to_audit_log(
    tx: &mut Transaction,
    event_type: mz_audit_log::EventType,
    object_type: mz_audit_log::ObjectType,
    details: mz_audit_log::EventDetails,
    occurred_at: EpochMillis,
) -> Result<(), anyhow::Error> {
    let id = tx.get_and_increment_id(mz_catalog::durable::AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
    let event =
        mz_audit_log::VersionedEvent::new(id, event_type, object_type, details, None, occurred_at);
    tx.insert_audit_log_event(event);
    Ok(())
}
