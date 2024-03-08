# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Source definitions
# ------------------

# Define t0 source
define
DefSource name=t0 keys=[[#0]]
  - c0: bigint
  - c1: bigint?
----
Source defined as t0

# Define t1 source
define
DefSource name=t1 keys=[[#0]]
  - c0: text
  - c1: bigint
  - c2: boolean
----
Source defined as t1

# Define t2 source
define
DefSource name=t2
  - c0: bigint?
  - c1: bigint?
----
Source defined as t2

# Define t3 source
define
DefSource name=t3
  - c0: bigint?
  - c1: bigint?
----
Source defined as t3


# Simplify Constant using outer equivalences.
apply pipeline=equivalence_propagation
Filter (#0 = 1) AND (#1 IS NULL)
  Constant // { types: "(bigint?, bigint?)" }
    - (0, null)
    - (1, null)
    - (1, 2)
----
Filter (#0 = 1) AND (#1) IS NULL
  Constant
    - (1, null)

# Simplify Map using derived equivalences.
# Note: no reduction / canonicalization of substituted scalar expressions.
apply pipeline=equivalence_propagation
Map (#0 + #1)
  Filter #0 = 1 AND #1 = #0
    Get t0
----
Map ((1 + 1))
  Filter (#0 = 1) AND (#1 = #0)
    Get t0

# Simplify Map using outer equivalences.
apply pipeline=equivalence_propagation
Join on=(#0 = #2)
  Filter #0 = 1
    Get t0
  Map (#0 + #1)
    Get t1
----
Join on=(#0 = #2)
  Filter (#0 = 1)
    Get t0
  Map ((#0 + #1))
    Get t1

# Simplify Map using outer equivalences.
apply pipeline=equivalence_propagation
Join on=(#0 = #2 AND #1 = #3)
  Filter #0 = 1 AND #1 = #0
    Get t0
  Map (#0 + #1)
    Get t1
----
Join on=(#0 = #2 AND #1 = #3)
  Filter (#0 = 1) AND (#1 = #0)
    Get t0
  Map ((#0 + #1))
    Get t1



# Infer and apply constant value knowledge.
# Cases: Map, FlatMap, Filter, Project, Reduce, Let/Get.
apply pipeline=equivalence_propagation
Return
  FlatMap generate_series(#1, #0 + 3, 1)
    Project (#2, #0)
      Get l0
With
  cte l0 =
    Filter (#1) IS NULL
      Reduce group_by=[#0 * #1] aggregates=[sum(#2), max(#0 + #2)]
        Filter #0 + #1 > 2 AND #2 > #1 + #1
          Map (#0 + #1)
            Filter #0 = 1 AND #1 = 2
              Get t0
----
Return
  FlatMap generate_series(2, (#0 + 3), 1)
    Project (#2, #0)
      Get l0
With
  cte l0 =
    Filter (#1) IS NULL
      Reduce group_by=[(#0 * #1)] aggregates=[sum(#2), max((#0 + #2))]
        Filter ((1 + 2) > 2) AND (3 > (2 + 2))
          Constant <empty>
