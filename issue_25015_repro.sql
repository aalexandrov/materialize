SELECT DISTINCT
  (a1.f1) AS c1,
  (a1.f2) AS c2
FROM
  t1 AS a1
  JOIN (VALUES(1, 2)) AS a2(f1, f2) ON (a2.f2 = a1.f1)
WHERE
  a1.f2 + a2.f2 > (SELECT DISTINCT 0 c2 FROM t2 AS a1) AND a2.f2 IS NULL
  OR NULLIF (a2.f2, a2.f1) = a1.f2 + a1.f2;

-- Pull out WHERE predicates as SELECT columns.

SELECT DISTINCT
  (a1.f1) AS c1,
  (a1.f2) AS c2,
  a1.f2 + a2.f2 > (SELECT DISTINCT 0 c2 FROM t2 AS a1) as p1,
  a2.f2 IS NULL as p2,
  NULLIF (a2.f2, a2.f1) = a1.f2 + a1.f2 as p3
FROM
  t1 AS a1
  JOIN (VALUES(1, 2)) AS a2(f1, f2) ON (a2.f2 = a1.f1);

-- Create a barrier between the JOIN and the WHERE contition.

DROP TABLE IF EXISTS temp;
CREATE TABLE temp(
  a1_f1 double precision, 
  a1_f2 double precision, 
  a2_f1 double precision, 
  a2_f2 double precision
);
INSERT INTO temp
SELECT DISTINCT
  a1.f1 as a1_f1,
  a1.f2 as a1_f2,
  a2.f1 as a2_f1,
  a2.f2 as a2_f2
FROM
  t1 AS a1
  JOIN (VALUES(1, 2)) AS a2(f1, f2) ON (a2.f2 = a1.f1);
SELECT * 
FROM temp
WHERE
  a1_f2 + a2_f2 > (SELECT DISTINCT 0 c2 FROM t2 AS a1) AND a2_f2 IS NULL
  OR NULLIF (a2_f2, a2_f1) = a1_f2 + a1_f2;