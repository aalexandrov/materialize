-- FIX optimizer trace
GRANT materialize TO anonymous_http_user;

CREATE SOURCE auction_house
   FROM LOAD GENERATOR AUCTION
   (TICK INTERVAL '1s')
   FOR ALL TABLES
   WITH (SIZE = '1');

SHOW SOURCES;

SHOW COLUMNS IN accounts;
-- id           false   bigint
-- org_id       false   bigint
-- balance      false   bigint

SHOW COLUMNS IN auctions;
-- id           false   bigint
-- seller       false   bigint
-- item         false   text
-- end_time     false   timestamp with time zone

SHOW COLUMNS IN bids;
-- id           false   bigint
-- buyer        false   bigint
-- auction_id   false   bigint
-- amount       false   integer
-- bid_time     false   timestamp with time zone

SHOW COLUMNS IN organizations;
-- id           false   bigint
-- name         false   text

SHOW COLUMNS IN users;
-- id           false   bigint
-- org_id       false   bigint
-- name         false   text

-- Q1-a
SELECT *
FROM accounts
WHERE id IN (
   SELECT buyer
   FROM bids
   GROUP BY buyer
   HAVING max(amount) > 100 AND count(id) > 10
)

-- Q1-b
SELECT *
FROM accounts a
WHERE EXISTS (
   SELECT buyer
   FROM bids WHERE a.id = bids.buyer
   GROUP BY buyer
   HAVING max(amount) > 100 AND count(id) > 10
)

-- Q2-a
SELECT *
FROM accounts
WHERE id NOT IN (
   SELECT buyer
   FROM bids
   GROUP BY buyer
   HAVING max(amount) > 100 AND count(id) > 10
)

-- Q2-b
SELECT *
FROM accounts a
WHERE NOT EXISTS (
   SELECT buyer
   FROM bids WHERE a.id = bids.buyer
   GROUP BY buyer
   HAVING max(amount) > 100 AND count(id) > 10
)

-- Q3-a
SELECT *
FROM accounts
WHERE id IN (
   SELECT buyer
   FROM bids
)

-- Q3-b
SELECT *
FROM accounts a
WHERE EXISTS (
   SELECT buyer
   FROM bids WHERE a.id = bids.buyer
)

-- Q4-a
SELECT *
FROM accounts
WHERE id NOT IN (
   SELECT buyer
   FROM bids
)

-- Q4-b
SELECT *
FROM accounts a
WHERE NOT EXISTS (
   SELECT buyer
   FROM bids WHERE a.id = bids.buyer
)

-- Q4
SELECT *
FROM accounts a
LEFT JOIN bids b ON (a.id = b.buyer)

-- Q5
SELECT SUM(amount)
FROM bids

-- Q6
SELECT buyer, max(amount) > 100 AND count(id) > 10
FROM bids
GROUP BY buyer
LIMIT 10

-- Q6
SELECT *
FROM bids b
LEFT JOIN accounts a ON (a.id = b.buyer)

-- Q7
SELECT *
FROM accounts a
LEFT JOIN bids b ON (a.id = b.buyer)
WHERE a.org_id < 100

-- Q8
SELECT *
FROM bids b
LEFT JOIN accounts a ON (a.id = b.buyer)
WHERE b.amount < 100

explain with(column_names)
(
   select buyer, sum(amount)
   from bids b
   group by b.buyer
)
union
(
   select 1, 2;
)
