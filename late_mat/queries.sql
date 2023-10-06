SET schema = late_mat;

-- Queries
----------

-- Q1
EXPLAIN PHYSICAL PLAN
WITH(humanized_exprs, arity, keys, join_impls)
FOR
SELECT
  *
FROM
  f1
  JOIN d1 ON(f1_d1 = d1_k1)
  JOIN d2 ON(f1_d2 = d2_k1)
  JOIN d3 ON(f1_d3 = d3_k1);

-- Q2
EXPLAIN PHYSICAL PLAN
WITH(humanized_exprs, arity, keys, join_impls)
FOR
SELECT
  f1.*,
  d1.*,
  d2.*,
  d3.*
FROM
  f1
  LEFT JOIN d1 ON(f1_d1 = d1_k1)
  LEFT JOIN d2 ON(f1_d2 = d2_k1)
  LEFT JOIN d3 ON(f1_d3 = d3_k1);

-- Q2-lm
EXPLAIN PHYSICAL PLAN
WITH(humanized_exprs, arity, keys, join_impls)
FOR
SELECT
  f1.*,
  d1_k1,
  d1_p1,
  d1_p2,
  d1_p3,
  d1_p4,
  d2_k1,
  d2_p1,
  d2_p2,
  d2_p3,
  d2_p4,
  d3_k1,
  d3_p1,
  d3_p2,
  d3_p3,
  d3_p4
FROM
  (
    SELECT
      *
    FROM
      (SELECT f1_k1, f1_k2, f1_d1, f1_d2, f1_d3 FROM f1) AS f1
      LEFT JOIN d1 ON(f1_d1 = d1_k1)
      LEFT JOIN d2 ON(f1_d2 = d2_k1)
      LEFT JOIN d3 ON(f1_d3 = d3_k1)
  ) AS r1 JOIN f1 USING(f1_k1, f1_k2);

-- Q3
EXPLAIN PHYSICAL PLAN
WITH(humanized_exprs, arity, keys, join_impls)
FOR
SELECT
  *
FROM
  f1
  LEFT JOIN d1 ON(f1_d1 = d1_k1)
WHERE
  f1_p1 = 'foo';

