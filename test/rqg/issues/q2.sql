SELECT DISTINCT
  ft.k, ft.v, d1.k, d1.v, d2.k, d2.k 
FROM
  star.ft
  LEFT JOIN star.d1 AS d1 ON (ft.k = d1.k)
  LEFT JOIN star.d2 AS d2 ON (ft.v = d2.k)
ORDER BY
  1, 2, 3, 4, 5, 6;

SELECT DISTINCT
  ft.k, ft.v, d1.k, d1.v, d2.k, d2.k 
FROM
  star.ft
  LEFT JOIN star.d1 AS d1 ON (ft.k = d1.k)
  LEFT JOIN star.d2 AS d2 ON (ft.v = d2.k)
WHERE
  (d1.k = d2.k)
ORDER BY
  1, 2, 3, 4, 5, 6;
