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
DefSource name=t0 keys=[[#0], [#1]]
  - c0: bigint
  - c1: bigint
  - c2: bigint
----
Source defined as t0

# Define t1 source
define
DefSource name=t1 keys=[[#0, #1, #2, #3, #4]]
  - c0: bigint
  - c1: bigint
  - c2: bigint
  - c3: bigint
  - c4: bigint
----
Source defined as t1

# Define t2 source
define
DefSource name=t2 keys=[[#1]]
  - c0: bigint
  - c1: bigint
  - c2: bigint
  - c3: bigint
  - c4: bigint
----
Source defined as t2


## Join patterns
## -------------


# Cross join between two inputs
explain with=keys
CrossJoin
  Get t0
  Get t1
  Get t2
----
CrossJoin // { keys: "([0, 3, 4, 5, 6, 7, 9], [1, 3, 4, 5, 6, 7, 9])" }
  Get t0 // { keys: "([0], [1])" }
  Get t1 // { keys: "([0, 1, 2, 3, 4])" }
  Get t2 // { keys: "([1])" }


# Cross join between two inputs
explain with=keys
Join on=(#2 = #7 AND #5 = 42 AND #7 = #9)
  Get t0
  Get t1
  Get t2
----
Join on=(#2 = #7 AND #5 = 42 AND #7 = #9) // { keys: "([0, 3, 4, 6, 7], [1, 3, 4, 6, 7])" }
  Get t0 // { keys: "([0], [1])" }
  Get t1 // { keys: "([0, 1, 2, 3, 4])" }
  Get t2 // { keys: "([1])" }
