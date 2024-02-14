// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_stash::upgrade::{wire_compatible, MigrationAction, WireCompatible};
use mz_stash::{Transaction, TypedCollection};
use mz_stash_types::StashError;

use crate::durable::upgrade::{objects_v46 as v46, objects_v47 as v47};

wire_compatible!(v46::ClusterKey with v47::ClusterKey);
// wire_compatible!(v46::ClusterValue with v47::ClusterValue);

unsafe impl mz_stash::upgrade::WireCompatible<v46::ClusterValue> for v47::ClusterValue {}

// #[mz_ore::test]
// #[cfg_attr(miri, ignore)] // slow
// fn proptest_wire_compat_v46_cluster_value_to_v47_cluster_value(b: v46::ClusterValue) {
//     use ::prost::Message;
//     let b_bytes = b.encode_to_vec();
//     let a_decoded = v47::ClusterValue::decode(&b_bytes[..]);
//     proptest::prelude::prop_assert!(a_decoded.is_ok());

//     // Maybe superfluous, but this is a method called in production.
//     let a_decoded = a_decoded.expect("asserted Ok");
//     let a_converted: v47::ClusterValue = mz_stash::upgrade::WireCompatible::convert(&b);
//     assert_eq!(a_decoded, a_converted);

//     let a_bytes = a_decoded.encode_to_vec();
//     proptest::prelude::prop_assert_eq!(a_bytes, b_bytes, "a and b serialize differently");
// }

const CLUSTER_COLLECTION: TypedCollection<v46::ClusterKey, v47::ClusterValue> =
    TypedCollection::new("clusters");

/// Introduce empty `optimizer_feature_overrides` in `ManagedCluster`'s.
pub async fn upgrade(tx: &Transaction<'_>) -> Result<(), StashError> {
    CLUSTER_COLLECTION
        .migrate_to::<v47::ClusterKey, v47::ClusterValue>(tx, |entries| {
            entries
                .iter()
                .map(|(cluster_key, cluster_value)| {
                    MigrationAction::Update(
                        cluster_key.clone(),
                        (
                            WireCompatible::convert(cluster_key),
                            WireCompatible::convert(cluster_value),
                        ),
                    )
                })
                .collect()
        })
        .await?;
    Ok(())
}
