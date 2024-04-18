SELECT
  ft.k AS ft_k,
  ft.v AS ft_v,
  d1.k AS d1_k,
  d1.v AS d1_v,
  d2.k AS d2_k,
  d2.v AS d2_v
FROM
  star.ft
  LEFT JOIN star.d1 AS d1 ON (ft.k = d1.k AND ft.v = d1.k)
  LEFT JOIN star.d2 AS d2 ON (ft.v = d2.v)
ORDER BY
  ft_k, ft_v, d1_k, d1_v, d2_k, d2_v;
