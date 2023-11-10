# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# References:
#
# [1] Profiling Relational Data – A Survey
#     Ziawasch Abedjan, Lukasz Golab, Felix Naumann
# [2] Candidate keys for relations.
#     Claudio L. Lucchesi and Sylvia L. Osborn.

# Source definitions
# ------------------

# Define t0 source (one composite key)
define
DefSource name=t0 keys=[[#0, #1]]
  - c0: bigint
  - c1: bigint
  - c2: bigint
----
Source defined as t0

# Define t1 source (two trivial keys)
define
DefSource name=t1 keys=[[#0], [#1]]
  - c0: bigint
  - c1: bigint
  - c2: bigint
----
Source defined as t1

# Constant: detect empty key for empty.
explain with=keys
Constant <empty> // { types: "(bigint, bigint)" }
----
Constant <empty> // { keys: "([])" }

# Constant: detect empty key for singleton.
explain with=keys
Constant // { types: "(bigint, bigint, bigint?)" }
  - (1, 2, 3)
----
Constant // { keys: "([])" }
  - (1, 2, 3)

# Constant: detect all single-column keys
explain with=keys
Constant // { types: "(bigint, bigint, bigint?)" }
  - (1, 2, 3)
  - (1, 3, null)
----
Constant // { keys: "([1], [2])" }
  - (1, 2, 3)
  - (1, 3, null)

# Constant: detect multi-column keys (TODO)
#
# TODO: finding more candidate keys (see 5.1 in [1]).
# TODO(aalexandrov): quantify how important this is in practice.
explain with=keys
Constant // { types: "(bigint, bigint, bigint?, bigint?)" }
  - (1, 2, 6, 5)
  - (1, 2, 6, 5)
  - (1, 3, 4, 0)
  - (1, 3, 4, 0)
  - (2, 3, 5, 4)
  - (2, 3, 5, 4)
----
Constant // { keys: "()" }
  - (1, 2, 6, 5)
  - (1, 2, 6, 5)
  - (1, 3, 4, 0)
  - (1, 3, 4, 0)
  - (2, 3, 5, 4)
  - (2, 3, 5, 4)

# Should be:
# Constant // { keys: "([0, 1])" }
#   - (1, 2, 6, 5)
#   - (1, 2, 6, 5)
#   - (1, 3, 4, 0)
#   - (1, 3, 4, 0)
#   - (2, 3, 5, 4)
#   - (2, 3, 5, 4)


# Linear operators
# ----------------

# Constant map (1).
explain with=keys
Map ("foobar")
  Get t0
----
Map ("foobar") // { keys: "([0, 1])" }
  Get t0 // { keys: "([0, 1])" }

# Constant map (2).
explain with=keys
Map ("foobar")
  Get t1
----
Map ("foobar") // { keys: "([0], [1])" }
  Get t1 // { keys: "([0], [1])" }

# Non-constant map with an injective expression (1).
#
# TODO: this case might be better handled by FDs.
explain with=keys
Map (#0 + 5)
  Get t0
----
Map ((#0 + 5)) // { keys: "([0, 1])" }
  Get t0 // { keys: "([0, 1])" }

# Should be :
# Map ((#0 + 5)) // { keys: "([0, 1], [2, 1])" }
#   Get t0 // { keys: "([0, 1])" }


# Non-constant map with an injective expression (2).
#
# TODO: this case might be better handled by FDs.
explain with=keys
Map (#0 + 5)
  Get t1
----
Map ((#0 + 5)) // { keys: "([0], [1])" }
  Get t1 // { keys: "([0], [1])" }

# Should be:
# Map ((#0 + 5)) // { keys: "([0], [1], [2])" }
#   Get t1 // { keys: "([0], [1])" }


# Filter (1). 
explain with=keys
Filter (#0 = 1)
  Get t0
----
Filter (#0 = 1) // { keys: "([1])" }
  Get t0 // { keys: "([0, 1])" }


# Filter (1). 
#
# TODO: the empty key `[]` in the result has double 
# meaning. This is very bad! We should proabably remove
# non-empty keys that become empty after a Filter.
explain with=keys
Filter (#0 = 1)
  Get t1
----
Filter (#0 = 1) // { keys: "([], [1])" }
  Get t1 // { keys: "([0], [1])" }


# Project that duplicates the keys (1).
#
# TODO: This proabably requires that keys depends
# on another inferred parameter called equivalence classes. 
explain with=keys
Project (#0, #2, #1, #2, #1, #0)
  Get t0
----
Project (#0, #2, #1, #2, #1, #0) // { keys: "([0, 2])" }
  Get t0 // { keys: "([0, 1])" }

# Should be:
# Project (#0, #2, #1, #2, #1, #0) // { keys: "([0, 2])", eqs="([0, 5], [2, 4])" }
#   Get t0 // { keys: "([0, 1])" }


# Project that duplicates the keys (2).
#
# TODO: This proabably requires that keys depends
# on another inferred parameter called equivalence classes. 
explain with=keys
Project (#0, #2, #1, #2, #1, #0)
  Get t1
----
Project (#0, #2, #1, #2, #1, #0) // { keys: "([0], [2])" }
  Get t1 // { keys: "([0], [1])" }

# Should be:
# Project (#0, #2, #1, #2, #1, #0) // { keys: "([0], [2])", eqs="([0, 5], [2, 4])" }
#   Get t1 // { keys: "([0, 1])" }


# Reductions
# ----------

# Distinct that contains the key (1).
explain with=keys
Distinct project=[#0, #1, #2 + 5]
  Get t0
----
Distinct project=[#0, #1, (#2 + 5)] // { keys: "([0, 1])" }
  Get t0 // { keys: "([0, 1])" }


# Distinct that contains the key (2).
explain with=keys
Distinct project=[#0, #1, #2]
  Get t1
----
Distinct project=[#0..=#2] // { keys: "([0], [1])" }
  Get t1 // { keys: "([0], [1])" }


# Distinct with injective expressions (1).
#
# TODO: does not respect injective expressions.
explain with=keys
Distinct project=[#0 + 5, #1 + 6, #1 + #2]
  Get t1
----
Distinct project=[(#0 + 5), (#1 + 6), (#1 + #2)] // { keys: "([0, 1, 2])" }
  Get t1 // { keys: "([0], [1])" }

# Should be:
# Distinct project=[(#0 + 5), (#1 + 6), (#1 + #2)] // { keys: "([0, 1])" }
#   Get t1 // { keys: "([0], [1])" }


# Distinct with injective expressions (2).
#
# TODO: does not respect injective expressions.
explain with=keys
Distinct project=[#0 + 5, #1 + 6]
  Get t1
----
Distinct project=[(#0 + 5), (#1 + 6)] // { keys: "([0, 1])" }
  Get t1 // { keys: "([0], [1])" }

# Should be:
# Distinct project=[(#0 + 5), (#1 + 6)] // { keys: "([0], [1])" }
#   Get t1 // { keys: "([0], [1])" }



# Equi-joins
# ----------

# Join (1).
explain with=keys
Join on=(#0 = #3 AND #2 = #5)
  Get t1
  Get t1
----
Join on=(#0 = #3 AND #2 = #5) // { keys: "([0], [1])" }
  Get t1 // { keys: "([0], [1])" }
  Get t1 // { keys: "([0], [1])" }
