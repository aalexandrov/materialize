-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Schema to manually validate that `u_dependencies.sql` works as expected.

DROP SCHEMA IF EXISTS test CASCADE;
CREATE SCHEMA test;
SET SCHEMA = test;

CREATE MATERIALIZED VIEW t1 IN CLUSTER c2 AS SELECT 1 as k, 2 as v1;
CREATE MATERIALIZED VIEW t2 IN CLUSTER c2 AS SELECT 1 as k, 2 as v2;
CREATE MATERIALIZED VIEW t3 IN CLUSTER c2 AS SELECT 1 as k, 2 as v3;

CREATE INDEX IN CLUSTER "c3 - the cluster" ON t1(k);
CREATE INDEX IN CLUSTER "c3 - the cluster" ON t2(k);
CREATE INDEX IN CLUSTER "c3 - the cluster" ON t3(k);

CREATE MATERIALIZED VIEW v1 IN CLUSTER "c3 - the cluster" AS SELECT * FROM t1 JOIN t2 USING(k);
CREATE VIEW v2 AS SELECT * FROM v1 JOIN t3 USING(k);
CREATE VIEW v3 AS SELECT * FROM v2;
CREATE VIEW v4 AS SELECT * FROM v3;
CREATE MATERIALIZED VIEW v5 IN CLUSTER "c3 - the cluster" AS SELECT * FROM v4;
CREATE VIEW v6 AS SELECT * FROM v5;
CREATE VIEW v7 AS SELECT * FROM v6;
CREATE VIEW v8 AS SELECT * FROM v7;
CREATE VIEW v9 AS SELECT * FROM v8;

CREATE INDEX IN CLUSTER "c3 - the cluster" ON v1(k);
CREATE INDEX IN CLUSTER "c3 - the cluster" ON v2(k);
CREATE INDEX IN CLUSTER "c3 - the cluster" ON v3(k);
CREATE INDEX IN CLUSTER "c3 - the cluster" ON v4(k);
CREATE INDEX IN CLUSTER "c3 - the cluster" ON v5(k);
CREATE INDEX IN CLUSTER "c3 - the cluster" ON v6(k);
CREATE INDEX IN CLUSTER "c3 - the cluster" ON v7(k);
CREATE INDEX IN CLUSTER "c3 - the cluster" ON v8(k);
CREATE INDEX IN CLUSTER "c3 - the cluster" ON v9(k);

WITH
  object_clusters(object_id, cluster_id) AS (
    SELECT id, cluster_id FROM mz_catalog.mz_indexes
    UNION
    SELECT id, cluster_id FROM mz_catalog.mz_materialized_views
  )
  SELECT
    o.id as id,
    o.oid as oid,
    o.name as name,
    s.name as schema,
    d.name as database,
    o.type as type,
    c.id as cluster_id,
    c.name as cluster_name
  FROM
    mz_objects AS o JOIN
    mz_schemas AS s ON (o.schema_id = s.id) JOIN
    mz_databases AS d ON (s.database_id = d.id) LEFT JOIN
    object_clusters oc ON (o.id = oc.object_id) LEFT JOIN
    mz_clusters c ON (oc.cluster_id = c.id)
  WHERE
    o.id like 'u%'
  ORDER BY
    d.name,
    s.name,
    o.name,
    o.type;
