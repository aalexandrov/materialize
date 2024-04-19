-- Notes: why do we need to augment left with NULLs?

SELECT DISTINCT
  ft.k, ft.v, d1.k, d1.v, d2.k, d2.v 
FROM
  star.ft
  LEFT JOIN star.d1 AS d1 ON (ft.k = d1.k AND ft.v = d1.k)
  LEFT JOIN star.d2 AS d2 ON (ft.v = d2.v)
ORDER BY
  1, 2, 3, 4, 5, 6;

SELECT DISTINCT
  ft.k, ft.v, d1.k, d1.v, d2.k, d2.v 
FROM
  star.ft
  LEFT JOIN star.d1 AS d1 ON (ft.v = d1.k AND ft.k = d1.k)
  LEFT JOIN star.d2 AS d2 ON (ft.v = d2.v)
ORDER BY
  1, 2, 3, 4, 5, 6;

explain locally optimized plan for
select 
  ft.k as ft_k,
  ft.v as ft_v,
  d1.k as d1_k,
  d1.v as d1_v
from ft left join d1 on(ft.k = d1.k and ft.v = d1.k)
order by 1,2,3,4;

explain locally optimized plan for
with
  -- lhs augmented with an all-null row
  left_vals_ft as (
    select distinct * from
      (select k, v from ft)
      union all
      (select null::int as k, null::int as v)
  ),
  -- non-null join keys on the right side
  right_vals_d1 as (
    select k as k0, k as k1 from d1 where k is not null
  ),
  -- null-row complement for d1
  additions_d1 as (
    select null::int as v, k0 as v, null::boolean as is_real
    from (
      (select k as k0, v as k1 from left_vals_ft)
      except all
      (select k0, k1 from right_vals_d1)
    )
  ),
  -- Agument d1 with complement
  aug_value_d1 as (
    (select k, v, true as is_real from d1 where k is not null)
    union all
    (select * from additions_d1)
  )
  -- Do inner join
  (
    select
      ft.k as ft_k,
      ft.v as ft_v,
      case when d1_aug.is_real is null then null else d1_aug.k end as d1_k,
      case when d1_aug.is_real is null then null else d1_aug.v end as d1_v
    from
      ft, aug_value_d1 as d1_aug
    where
      (ft.k = d1_aug.v OR (ft.k is null AND d1_aug.v is null)) AND 
      (ft.v = d1_aug.v OR (ft.v is null AND d1_aug.v is null))
    order by
      1,2,3,4
  );