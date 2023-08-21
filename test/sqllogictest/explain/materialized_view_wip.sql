drop schema public cascade;
create schema public;

create source auction_house
  from load generator auction
  (tick interval '1s')
  for all tables
  with (size = '1');

create or replace materialized view mv as
  select * from accounts where balance = 100;

-- baseline explain (no index used)
explain materialized view mv;

create index accounts_balance_idx on accounts(balance);

-- ensure that the index is still not used
explain materialized view mv;

-- re-create the view so it can pick up the index
create or replace materialized view mv as
  select * from accounts where balance = 100;

-- ensure that the index is now used by the view
explain materialized view mv;

-- drop the index
drop index accounts_balance_idx;

-- ensure that the index is still used by the view
explain materialized view mv;