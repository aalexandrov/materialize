-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

DROP SCHEMA IF EXISTS optimizer_notices_internal CASCADE;
CREATE SCHEMA optimizer_notices_internal;
SET SCHEMA = optimizer_notices_internal;

\set all_optimizer_notices optimizer_notices_internal.all_optimizer_notices
\set objects optimizer_notices_internal.objects

\set all_optimizer_notices mz_internal.mz_all_optimizer_notices
\set objects mz_catalog.mz_objects

CREATE TABLE :all_optimizer_notices (
    notice_type text not null,
    message text not null,
    hint text not null,
    action text,
    action_type text,
    object_id text,
    dependency_ids text list
);

INSERT INTO :all_optimizer_notices VALUES
   ('t1', 'm1', 'h1', null, null, 'u3', '{u1, u2}'),
   ('t1', 'm2', 'h2', null, null, 'u4', '{u1, u3}'),
   ('t2', 'm3', 'h3', null, null, 'u5', '{}');

CREATE TABLE :objects (
    id text not null
);

INSERT INTO :objects VALUES
   ('u1'),
   ('u2'),
   ('u4'),
   ('u5');


SELECT
    n.object_id,
    n.notice_type,
    n.dependency_ids
FROM
    :all_optimizer_notices n
ORDER BY
    n.object_id,
    n.notice_type;

EXPLAIN WITH(humanized_exprs, arity)
SELECT
    n.*
FROM
    :all_optimizer_notices n
WHERE
    (
        SELECT COUNT(*)
        FROM unnest(n.dependency_ids) AS d(id)
        WHERE id IN (SELECT id FROM :objects)
    ) = list_length(n.dependency_ids);
