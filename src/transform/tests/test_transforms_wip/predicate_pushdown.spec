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
  - c0: text
  - c1: text
  - c2: boolean
----
Source defined as t2


# Pushing CrossJoin
apply pipeline=predicate_pushdown explain_with=arity
Filter (#0 = #3 AND #0 = #4 AND #4 = "a")
  CrossJoin
    Get t1
    Get t2
----
Join on=(#0 = #3) // { arity: 6 }
  Filter (#0 = "a") // { arity: 3 }
    Get t1 // { arity: 3 }
  Filter (#1 = "a") AND ((#0 = #1) OR ((#0) IS NULL AND (#1) IS NULL)) // { arity: 3 }
    Get t2 // { arity: 3 }
