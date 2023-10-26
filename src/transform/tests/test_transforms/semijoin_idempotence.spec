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
DefSource name=t0
  - c0: bigint
  - c1: bigint
  - c2: text?
----
Source defined as t0

# Define t1 source
define
DefSource name=t1 keys=[[#0]]
  - c0: bigint
  - c1: text?
----
Source defined as t1

# Define t2 source
define
DefSource name=t2
  - c0: bigint
  - c1: bigint
----
Source defined as t2

# Basic case
apply pipeline=semijoin_idempotence
Return
  Union
    Map (null::bigint, null::text)
      Union
        Project (#0, #1, #2)
          Negate
            Join on=(#0 = #3)
              Get t0
              Distinct project=[#0]
                Get l0
        Get t0
    Project (#0, #1, #2, #0, #4)
      Get l0
With
  cte l0 =
    Join on=(#0 = #3)
      Get t0
      Get t1
----
Return
  Union
    Map (null, null)
      Union
        Project (#0..=#2)
          Negate
            Join on=(#0 = #3)
              Get t0
              Project (#0)
                Get t1
        Get t0
    Project (#0..=#2, #0, #4)
      Get l0
With
  cte l0 =
    Join on=(#0 = #3)
      Get t0
      Get t1

## From https://github.com/MaterializeInc/materialize/pull/22560#issuecomment-1776717663
apply pipeline=semijoin_idempotence
Return
  Join on=(#0 = #1)
    Project (#1)
      Get t1
    Distinct project=[#0]
      Get l0
With
  cte l0 =
    Project (#0)
      Join on=(#0 = #1)
        Project (#1)
          Get t1
        Project (#1)
          Get t2
----
Return
  Join on=(#0 = #1)
    Project (#1)
      Get t1
    Distinct project=[#0]
      Get l0
With
  cte l0 =
    Project (#0)
      Join on=(#0 = #1)
        Project (#1)
          Get t1
        Project (#1)
          Get t2

## From https://github.com/MaterializeInc/materialize/pull/22560#issuecomment-1776717663
apply pipeline=semijoin_idempotence
Return
  Join on=(#0 = #2)
    Get t2
    Distinct project=[#0]
      Get l0
With
  cte l0 =
    Join on=(#0 = #2)
      Get t2
      Get t1
----
Return
  Join on=(#0 = #2)
    Get t2
    Project (#0)
      Get t1
With
  cte l0 =
    Join on=(#0 = #2)
      Get t2
      Get t1

## LetRec cases
## ------------
