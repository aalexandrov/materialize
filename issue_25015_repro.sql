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
              a1.f2 = ALL (
                SELECT
                  agg1
                FROM
                  (
                    SELECT
                      (a1.f1) AS c1,
                      (a2.f1) AS c2,
                      (a1.f2) AS c3,
                      (MIN (a1.f1)) AS agg1,
                      (COUNT (a2.f2 + a2.f2)) AS agg2
                    FROM
                      pk2 AS a1
                      RIGHT JOIN (
                        SELECT
                          COUNT (a2.f2) AS f1,
                          COUNT (a2.f2 + NULLIF (a2.f2, a2.f2)) AS f2
                        FROM
                          pk2 AS a1
                          RIGHT JOIN pk2 AS a2 ON (
                            NULLIF (a1.f2, a2.f1) = ALL (
                              SELECT
                                DISTINCT agg1
                              FROM
                                (
                                  SELECT
                                    (a1.f1) AS c1,
                                    (a2.f1) AS c2,
                                    (a1.f2) AS c3,
                                    (COUNT (NULLIF (a2.f1, a2.f2))) AS agg1,
                                    (AVG (a2.f1)) AS agg2
                                  FROM
                                    (
                                      SELECT
                                        a2.f2 + a2.f1 AS f1,
                                        a2.f1 + a1.f2 AS f2
                                      FROM
                                        t1 AS a1
                                        JOIN t2 AS a2 USING (f1, f2)
                                      WHERE
                                        a2.f2 IS NOT NULL
                                        AND a2.f2 + NULLIF (a2.f2, a1.f2) = NULLIF (a1.f2, a1.f2)
                                      ORDER BY
                                        1,
                                        2
                                    ) AS a1
                                    JOIN (
                                      SELECT
                                        *
                                      FROM
                                        (
                                          VALUES
                                            (1, 2)
                                        ) t2 (f1, f2)
                                    ) AS a2 USING (f1, f2)
                                  WHERE
                                    a2.f1 + a2.f2 IS NOT NULL
                                    AND NOT (a2.f2 IS NOT NULL)
                                    OR a1.f2 + a2.f2 + a1.f1 IS NULL
                                  GROUP BY
                                    1,
                                    2,
                                    3
                                  UNION ALL
                                  SELECT
                                    DISTINCT (NULLIF (a2.f1, a2.f2)) AS c1,
                                    (a1.f1) AS c2,
                                    (a1.f2) AS c3,
                                    (AVG (a1.f2 + a2.f1)) AS agg1,
                                    (MAX (a2.f1)) AS agg2
                                  FROM
                                    (
                                      SELECT
                                        a1.f2 AS f1,
                                        a2.f2 + a1.f1 AS f2
                                      FROM
                                        pk1 AS a1
                                        JOIN t1 AS a2 ON (NOT (a2.f1 + a2.f2 IS NOT NULL))
                                      WHERE
                                        a1.f1 + a1.f1 + a2.f2 + a1.f2 + a2.f2 > a2.f2
                                        AND NOT (NOT (a1.f2 + a1.f2 IS NOT NULL))
                                      ORDER BY
                                        1,
                                        2
                                    ) AS a1
                                    JOIN pk2 AS a2 ON (NOT (a2.f1 = a2.f2))
                                  WHERE
                                    a2.f2 = a1.f2
                                    OR a2.f2 + a2.f1 = a2.f2 + NULLIF (a1.f1, a2.f2)
                                  GROUP BY
                                    1,
                                    2,
                                    3
                                ) AS dt
                              ORDER BY
                                1
                              LIMIT
                                0
                            )
                          )
                        WHERE
                          a2.f1 IS NULL
                          AND a2.f2 = a1.f2 + a2.f2
                          AND a2.f2 IS NOT NULL
                        ORDER BY
                          1,
                          2
                        LIMIT
                          0 OFFSET 9
                      ) AS a2 USING (f1)
                    WHERE
                      a2.f2 = a2.f1
                      AND NULLIF (a2.f1, a1.f1) < a2.f2
                    GROUP BY
                      1,
                      2,
                      3
                  ) AS dt
                ORDER BY
                  1
              )
              AND a1.f2 + NULLIF (a1.f2, a1.f1) IS NULL
              OR a1.f2 > a2.f1
              OR a1.f2 = (
                SELECT
                  agg2
                FROM
                  (
                    SELECT
                      (NULLIF (a1.f2, a1.f2)) AS c1,
                      (a1.f2 + a1.f2) AS c2,
                      (a2.f2 + a1.f1 + a2.f2) AS c3,
                      (MAX (a2.f2)) AS agg1,
                      (MIN (a2.f2 + a2.f2)) AS agg2
                    FROM
                      (
                        SELECT
                          a2.f2 + a2.f2 AS f1,
                          NULLIF (a2.f1, a1.f2) AS f2
                        FROM
                          pk1 AS a1
                          JOIN t1 AS a2 ON (a2.f2 IN (8, 4, 6))
                        WHERE
                          NULLIF (a2.f2, a1.f1) IS NULL
                          OR a2.f1 < NULLIF (a1.f2, a1.f2)
                          AND a2.f2 + a1.f1 IN (0, 6, 5, 7)
                          OR NOT (
                            a1.f2 + NULLIF (a2.f2, a1.f1) IN (4, 7)
                          )
                          AND NOT (a1.f1 IS NULL)
                          AND NOT (a1.f2 IS NULL)
                          AND NOT (a2.f2 IN (7, 0))
                        ORDER BY
                          1,
                          2
                      ) AS a1
                      LEFT JOIN pk1 AS a2 USING (f2, f1)
                    WHERE
                      a2.f2 = a1.f1
                      OR a2.f2 = a2.f1
                      AND a2.f2 + a1.f2 IN (6, 1, 3, 6)
                    GROUP BY
                      1,
                      2,
                      3
                    EXCEPT
                      ALL
                    SELECT
                      DISTINCT (a2.f2 + a1.f1) AS c1,
                      (a2.f2) AS c2,
                      (a2.f2) AS c3,
                      (MAX (NULLIF (a1.f1, a2.f2))) AS agg1,
                      (AVG (NULLIF (a2.f2, a2.f2))) AS agg2
                    FROM
                      (
                        SELECT
                          NULLIF (a1.f2, a2.f2) AS f1,
                          a1.f1 + a1.f1 AS f2
                        FROM
                          pk2 AS a1
                          JOIN pk1 AS a2 ON (a2.f2 IS NOT NULL)
                        WHERE
                          NOT (NOT (a1.f2 + a1.f1 IN (0, 0)))
                          AND NOT (a2.f1 + a1.f1 IN (4, 4))
                          AND NOT (a2.f2 NOT IN (9, 8, 3, 5))
                        ORDER BY
                          1,
                          2
                      ) AS a1
                      LEFT JOIN pk2 AS a2 ON (NULLIF (a1.f1, a2.f2) < a2.f1)
                    WHERE
                      a2.f2 IN (5, 4)
                      AND NULLIF (a2.f2, a2.f2) = a2.f1
                      OR NOT (a1.f2 + a1.f2 + a1.f1 = a2.f2)
                      AND a2.f2 IS NOT NULL
                      OR a1.f2 IN (2, 7)
                      AND a2.f2 + a2.f2 > a1.f2 + a2.f2
                      AND NOT (a2.f2 + a1.f2 IS NULL)
                      AND NOT (
                        a1.f1 + NULLIF (a2.f2, a2.f2) = a2.f2
                      )
                    GROUP BY
                      1,
                      2,
                      3
                  ) AS dt
                ORDER BY
                  1
                LIMIT
                  1 OFFSET 5
              )
              AND a1.f2 NOT IN (8, 0, 4)
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