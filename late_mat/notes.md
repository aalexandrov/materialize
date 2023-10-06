
# Application scenarios

## Case 1

- A `Join`, followed by
- a plan fragment that needs an arrangement (e.g. `Reduce` or `TopK`), followed by
- a plan frament that first references a value on one of the inputs.

### Example

```text
Reduce group_by=[d1_k1, d1_p1, d1_p2, d1_p3, d1_p4] aggregates=[max(d2_p2), min(d2_p3), min(d2_p4), max(d3_p2), min(d3_p3), min(d3_p4)]
  Join on=(f1_d1 = d1_k1 AND f1_d2 = d2_k1 AND f1_d3 = d3_k1)
    Get f1
    Get d1
    Get d2
    Get d3
```

```text
Reduce group_by=[d1_k1, d1_p1, d1_p2, d1_p3, d1_p4] aggregates=[max(d2_p2), min(d2_p3), min(d2_p4), max(d3_p2), min(d3_p3), min(d3_p4)]
  Join on=(f1_d1 = d1_k1 AND f1_d2 = d2_k1 AND f1_d3 = d3_k1)
    Get f1
    Project ()
      Get d1
    Get d2
    Get d3
```

## Case 2

An stack of outer joins in a start schema, with extra columns on the dimension table.

```text
Reduce group_by=[d1_k1, d1_p1, d1_p2, d1_p3, d1_p4] aggregates=[max(d2_p2), min(d2_p3), min(d2_p4), max(d3_p2), min(d3_p3), min(d3_p4)]
  Join on=(f1_d1 = d1_k1 AND f1_d2 = d2_k1 AND f1_d3 = d3_k1)
    Get f1
    Get d1
    Get d2
    Get d3
```

```text
Reduce group_by=[d1_k1, d1_p1, d1_p2, d1_p3, d1_p4] aggregates=[max(d2_p2), min(d2_p3), min(d2_p4), max(d3_p2), min(d3_p3), min(d3_p4)]
  Join on=(f1_d1 = d1_k1 AND f1_d2 = d2_k1 AND f1_d3 = d3_k1)
    Get f1
    Project ()
      Get d1
    Get d2
    Get d3
```


---

- An `n`-way `Join` for `n > 2` (so an internal arrangement is created), followed by
- a plan fragment that ends with a `Filter`, followed by
- a plan frament that first references a value on one of the inputs.

## Appendix: Examples Schema

```text
# Define f1 source
define
DefSource name=f1 keys=[[#0, #1]]
  - f1_k1: bigint
  - f1_k2: bigint
  - f1_d1: bigint?
  - f1_d2: bigint?
  - f1_d3: bigint?
  - f1_p01: bigint?
  - f1_p02: bigint?
  - f1_p03: bigint?
  - f1_p04: bigint?
  - f1_p05: bigint?
  - f1_p06: bigint?
  - f1_p07: bigint?
  - f1_p08: bigint?
  - f1_p09: bigint?
  - f1_p10: bigint?
  - f1_p11: bigint?
  - f1_p12: bigint?
  - f1_p13: bigint?
  - f1_p14: bigint?
  - f1_p15: bigint?
  - f1_p16: bigint?
  - f1_p17: bigint?
  - f1_p18: bigint?
  - f1_p19: bigint?
  - f1_p20: bigint?
----
Source defined as f1

# Define d1 source
define
DefSource name=d1 keys=[[#0]]
  - d1_k1: bigint
  - d1_p1: bigint?
  - d1_p2: bigint?
  - d1_p3: bigint?
  - d1_p4: bigint?
----
Source defined as d1

# Define d2 source
define
DefSource name=d2 keys=[[#0]]
  - d2_k1: bigint
  - d2_p1: bigint?
  - d2_p2: bigint?
  - d2_p3: bigint?
  - d2_p4: bigint?
----
Source defined as d2

# Define d3 source
define
DefSource name=d3 keys=[[#0]]
  - d3_k1: bigint
  - d3_p1: bigint?
  - d3_p2: bigint?
  - d3_p3: bigint?
  - d3_p4: bigint?
----
Source defined as d3
```
