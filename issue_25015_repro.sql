SELECT DISTINCT
  (a1.f1) AS c1,
  (a2.f2 + a1.f2) AS c2,
  (a1.f2) AS c3
FROM
  pk2 AS a1
  JOIN (VALUES(1, 2)) AS a2(f1, f2) ON (a2.f2 = a1.f1)
WHERE
  a1.f2 + a2.f2 > (SELECT DISTINCT 0 c2 FROM pk2 AS a1)
  AND a2.f2 IS NULL
  AND a1.f2 IS NULL
  AND NULLIF (a2.f2, a1.f1) IS NULL
  OR NULLIF (a2.f2, a2.f1) = a1.f2 + a1.f2;