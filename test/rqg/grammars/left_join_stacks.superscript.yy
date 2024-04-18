
explain:
  EXPLAIN query
;

query:
  select
;

select:
  SELECT
    select_table_alias.col_name AS c01,
    select_table_alias.col_name AS c02,
    select_table_alias.col_name AS c03,
    select_table_alias.col_name AS c04,
    select_table_alias.col_name AS c05,
    select_table_alias.col_name AS c06,
    select_table_alias.col_name AS c07,
    select_table_alias.col_name AS c08,
    select_table_alias.col_name AS c09,
    select_table_alias.col_name AS c10
    # select_table_alias.col_name AS c11,
    # select_table_alias.col_name AS c12,
    # select_table_alias.col_name AS c13,
    # select_table_alias.col_name AS c14,
    # select_table_alias.col_name AS c15,
    # select_table_alias.col_name AS c16,
    # select_table_alias.col_name AS c17,
    # select_table_alias.col_name AS c18,
    # select_table_alias.col_name AS c19,
    # select_table_alias.col_name AS c20
  FROM
    t1 as ft
    left_join_01
    left_join_02
    left_join_03
    left_join_04
  # left_join_05
  # left_join_06
;

select_table_alias:
    ft
  | d1
  | d2
  | d3
  | d4
# | d5
# | d6
;

col_name:
    f1
  | f2
;

table_name:
    t1
  | t2
  | pk1
  | pk2
# | t3
;

left_join_01:
    LEFT JOIN table_name AS d1 ON(ft.f2 = d1.col_name)
;

left_join_02:
    LEFT JOIN table_name AS d2 ON(ft.f2 = d2.col_name)
# | LEFT JOIN table_name AS d2 ON(d1.col_name = d2.col_name)
;

left_join_03:
    LEFT JOIN table_name AS d3 ON(ft.f2 = d3.col_name)
# | LEFT JOIN table_name AS d3 ON(d1.col_name = d3.col_name)
# | LEFT JOIN table_name AS d3 ON(d2.col_name = d3.col_name)
;

left_join_04:
    LEFT JOIN table_name AS d4 ON(ft.f1 = d4.col_name)
# | LEFT JOIN table_name AS d4 ON(d1.col_name = d4.col_name)
# | LEFT JOIN table_name AS d4 ON(d2.col_name = d4.col_name)
# | LEFT JOIN table_name AS d4 ON(d3.col_name = d4.col_name)
;

left_join_05:
    LEFT JOIN table_name AS d5 ON(ft.f2 = d5.col_name)
# | LEFT JOIN table_name AS d5 ON(d1.col_name = d5.col_name)
# | LEFT JOIN table_name AS d5 ON(d2.col_name = d5.col_name)
# | LEFT JOIN table_name AS d5 ON(d3.col_name = d5.col_name)
# | LEFT JOIN table_name AS d5 ON(d4.col_name = d5.col_name)
;

left_join_06:
    LEFT JOIN table_name AS d6 ON(ft.f1 = d6.col_name)
# | LEFT JOIN table_name AS d6 ON(d1.col_name = d6.col_name)
# | LEFT JOIN table_name AS d6 ON(d2.col_name = d6.col_name)
# | LEFT JOIN table_name AS d6 ON(d3.col_name = d6.col_name)
# | LEFT JOIN table_name AS d6 ON(d4.col_name = d6.col_name)
# | LEFT JOIN table_name AS d6 ON(d5.col_name = d6.col_name)
;
