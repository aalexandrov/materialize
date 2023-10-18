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
DefSource name=t2 keys=[[#0]]
  - c0: text
  - c1: bigint
  - c2: boolean
----
Source defined as t2


# Pushing through Get, Distinct, Union, Project, Join
apply pipeline=predicate_pushdown
Return
  Filter #0 = "a"
    Get l1
With
  cte l1 =
    Distinct project=[#0, #1, #2]
      Union
        Project (#0, #1, #5)
          Join on=(#0 = #3 AND #2 = #4)
            Get l0
            Get t1
        Get t1
  cte l0 =
    Constant // { types: "(text, bigint, bigint)" }
      - ("a", 1, 2)
      - ("b", 3, 4)
----
Return
  Filter
    Get l1
With
  cte l1 =
    Distinct project=[#0..=#2]
      Union
        Project (#0, #1, #5)
          Join on=(#0 = #3 AND #2 = #4)
            Filter
              Get l0
            Filter (#0 = "a")
              Get t1
        Filter (#0 = "a")
          Get t1
  cte l0 =
    Filter (#0 = "a")
      Constant
        - ("a", 1, 2)
        - ("b", 3, 4)


## LetRec cases
## ------------

# TODO: Push a literal constraint through a loop
apply pipeline=predicate_pushdown
Return
  Filter (#0 = "foo")
    Get l0
With Mutually Recursive
  cte l0 = // { types: "(bigint, bigint)" }
    Distinct project=[#0, #1]
      Union
        Get t0
        Get l0
        Project (#0, #3)
          Join on=(#1 = #2)
            Get l0
            Get l0
----
Return
  Filter (#0 = "foo")
    Get l0
With Mutually Recursive
  cte l0 =
    Distinct project=[#0, #1]
      Union
        Get t0
        Get l0
        Project (#0, #3)
          Join on=(#1 = #2)
            Get l0
            Get l0


# Predicate pushdown for an equijoin that references
# an outer context (after decorrelation).
#
# Observe that while the outer local predicates
#
# > #0 > 5 AND #1 < 9
#
# are replicated and pushed down to all outer sides, the
# rewritten equijoin condition currently has a condition
# that effectively defines an equijoin, but has awkward
# support because we reference #1 instead of #6.
#
# > (#0 = #5 AND #1 = #6 AND (#0 + #3) = (#1 + #8))
#                             -------     -------
#                              {l1}       {l1,l2}
#
# This will result in a three-way join that only has
# cross-joins. One way to remediate the problem is to
# rewrite the predicate structure in order to maximize
# the amount of cross-input predicates:
#
# > (#0 = #5 AND #1 = #6 AND (#0 + #3) = (#6 + #8))
#                             -------     -------
#                              {l1}       {l2}
apply pipeline=predicate_pushdown
Return
  Filter (#0 + #3 = #1 + #6 AND #0 > 5 AND #1 < 9)
    Project (#0, #1, #2, #3, #4, #7, #8, #9)
      Join on=(#0 = #5 AND #1 = #6)
        Get l1
        Get l2
With
  cte l2 =
    CrossJoin
      Get t0
      Get t1
  cte l1 =
    CrossJoin
      Get t0
      Get t1
----
Return
  Project (#0..=#4, #7..=#9)
    Join on=(#0 = #5 AND #1 = #6 AND (#0 + #3) = (#1 + #8))
      Filter
        Get l1
      Filter
        Get l2
With
  cte l2 =
    CrossJoin
      Filter (#1 < 9) AND (#0 > 5)
        Get t0
      Filter (#1) IS NOT NULL
        Get t1
  cte l1 =
    CrossJoin
      Filter (#1 < 9) AND (#0 > 5)
        Get t0
      Filter (#1) IS NOT NULL
        Get t1
