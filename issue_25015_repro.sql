SELECT DISTINCT
  (a1.f1) AS c1,
  (a2.f2 + a1.f2) AS c2,
  (a1.f2) AS c3
FROM
  pk2 AS a1
  JOIN (VALUES(1, 2)) AS a2(f1, f2) ON (a2.f2 = a1.f1)
WHERE
  a1.f2 + a2.f2 > (
    SELECT
      DISTINCT c2
    FROM
      (
        SELECT
          (a1.f1) AS c1,
          (a2.f1) AS c2,
          (a1.f2) AS c3,
          (MIN (a2.f2)) AS agg1,
          (COUNT (DISTINCT NULLIF (a2.f1, a1.f2))) AS agg2
        FROM
          (
            SELECT
              COUNT (DISTINCT a1.f2) AS f1,
              COUNT (a2.f2) AS f2
            FROM
              pk2 AS a1
              JOIN pk2 AS a2 ON (a1.f2 IS NOT NULL)
            WHERE
              a1.f2 + NULLIF (a1.f2, a1.f1) IS NULL
            ORDER BY
              1,
              2
          ) AS a1
          JOIN (
            SELECT
              MIN (
                a1.f2 + a2.f2 + NULLIF (a2.f1, a2.f2)
              ) AS f1,
              COUNT (
                DISTINCT a2.f2 + NULLIF (a2.f2, a1.f1)
              ) AS f2
            FROM
              pk2 AS a1
              JOIN pk2 AS a2 USING (f1)
            WHERE
              a1.f2 + a2.f2 IN (9, 2)
              AND a1.f1 + NULLIF (a1.f2, a1.f2) NOT IN (0, 7)
            ORDER BY
              1,
              2
          ) AS a2 USING (f1)
        WHERE
          a2.f1 + a1.f2 = a2.f1 + a1.f2
          AND a2.f1 + a2.f2 + a2.f2 = a2.f2 + a1.f2
          AND NOT (a1.f1 IN (2, 8))
        GROUP BY
          1,
          2,
          3
      ) AS dt
  )
  AND a2.f2 IS NULL
  AND a1.f2 IS NULL
  OR NOT (a2.f2 IS NOT NULL)
  OR a1.f1 + a2.f2 IS NULL
  AND NULLIF (a2.f2, a1.f1) IS NULL
  OR NULLIF (a2.f2, a2.f1) = a1.f2 + a1.f2;