-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Run this as `mz_system`

DROP SCHEMA IF EXISTS optimizer_notices CASCADE;
CREATE SCHEMA optimizer_notices;
SET SCHEMA = optimizer_notices;

SET CLUSTER = default;

CREATE TABLE t1(x int, y int);
CREATE DEFAULT INDEX ON t1;

-- index too wide in MV
-- emits an "index too wide" notice
CREATE MATERIALIZED VIEW mv1 AS
SELECT x, y FROM t1 WHERE x = 5;

-- index too wide in an index
CREATE VIEW v1 AS
SELECT x, y FROM t1 WHERE x = 7;

-- emits an "index too wide" notice and an "index key empty" notice
CREATE INDEX ON v1();

-- Select notices
-- There are exacty 3 notices (as expected)
SELECT n.notice_type, n.object_id, n.dependency_ids FROM mz_internal.mz_optimizer_notices n;

-- Select notices with a subquery that counts how many dependencies exist in mz_objects
-- There are 5 notices and the counts are off
SELECT
    n.notice_type,
    n.object_id,
    n.dependency_ids,
    list_length(n.dependency_ids) as all_dependency_ids,
    (
        SELECT COUNT(*)
        FROM unnest(n.dependency_ids) AS d(id)
        WHERE id IN (SELECT id FROM mz_catalog.mz_objects)
    ) as total_cnt
FROM
    mz_internal.mz_optimizer_notices n;

-- Drop the t1 index. This changes the derived `hint` line.
-- DROP INDEX t1_primary_idx;

-- Drop the view. This creates a retraction for a row that does not exist.
-- DROP MATERIALIZED VIEW mv1;

-- Select notices
-- SELECT n.notice_type, n.object_id, n.dependency_ids FROM mz_internal.mz_optimizer_notices n;

-- Drop the view. This creates a retraction for a row that does not exist.
-- DROP VIEW v1;

-- Select notices
-- SELECT n.notice_type, n.object_id, n.dependency_ids FROM mz_internal.mz_optimizer_notices n;

-- Repro of something wrong.

DROP SCHEMA IF EXISTS issue_xxxxx CASCADE;
CREATE SCHEMA issue_xxxxx;
SET SCHEMA = issue_xxxxx;

-- Mimick the `mz_optimizer_notices` and `mz_object` catalog tables in a separate schema.
DROP TABLE IF EXISTS optimizer_notices;
DROP TABLE IF EXISTS objects;

CREATE TABLE objects (
    id text not null
);

CREATE TABLE optimizer_notices (
    object_id text,
    dependency_ids text list not null
);

-- Mimick the above workload.
INSERT INTO objects VALUES ('u1'), ('u2'), ('u3'), ('u4'), ('u5');
INSERT INTO optimizer_notices VALUES ('u3', '{u1, u2}'), ('u5', '{u1, u2}'), ('u5', '{}');

-- Select notices 
-- There are exacty 3 notices (as expected)
SELECT
    n.notice_type,
    n.object_id,
    n.dependency_ids
FROM
    optimizer_notices n;

-- Select notices with a subquery that counts how many dependencies exist in mz_objects
-- There are 5 notices and the counts are off
SELECT
    n.notice_type,
    n.object_id,
    n.dependency_ids,
    list_length(n.dependency_ids) as all_dependency_ids,
    (
        SELECT COUNT(*)
        FROM unnest(n.dependency_ids) AS d(id)
        WHERE id IN (SELECT id FROM mz_catalog.mz_objects)
    ) as total_cnt
FROM
    mz_internal.mz_optimizer_notices n
    ;

-- EXPLAIN
SELECT
    n.object_id,
    n.dependency_ids,
    list_length(n.dependency_ids) as all_dependency_ids,
    (
        SELECT COUNT(*)
        FROM unnest(n.dependency_ids) AS d(id)
        WHERE id IN (SELECT id FROM objects)
    ) as total_cnt
FROM
    optimizer_notices n;
