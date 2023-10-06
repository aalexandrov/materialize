DROP SCHEMA IF EXISTS late_mat CASCADE;
CREATE SCHEMA late_mat;

SET schema = late_mat;

-- Star schema
--------------

CREATE TABLE f1_raw(
    f1_k1 int not null, -- primary key
    f1_k2 int not null, -- primary key
    f1_d1 int,  -- foreign key (optional)
    f1_d2 int,  -- foreign key (optional)
    f1_d3 int,  -- foreign key (optional)
    f1_p01 int, -- payload column
    f1_p02 int, -- payload column
    f1_p03 int, -- payload column
    f1_p04 int, -- payload column
    f1_p05 int, -- payload column
    f1_p06 int, -- payload column
    f1_p07 int, -- payload column
    f1_p08 int, -- payload column
    f1_p09 int, -- payload column
    f1_p10 int, -- payload column
    f1_p11 int, -- payload column
    f1_p12 int, -- payload column
    f1_p13 int, -- payload column
    f1_p14 int, -- payload column
    f1_p15 int, -- payload column
    f1_p16 int, -- payload column
    f1_p17 int, -- payload column
    f1_p18 int, -- payload column
    f1_p19 int, -- payload column
    f1_p20 int  -- payload column
);

CREATE MATERIALIZED VIEW f1 as
SELECT DISTINCT ON(f1_k1, f1_k2) * FROM f1_raw;

CREATE TABLE d1_raw(
    d1_k1 int not null, -- primary key
    d1_p1 int, -- payload column
    d1_p2 int, -- payload column
    d1_p3 int, -- payload column
    d1_p4 int  -- payload column
);

CREATE MATERIALIZED VIEW d1 as
SELECT DISTINCT ON(d1_k1) * FROM d1_raw;

CREATE TABLE d2_raw(
    d2_k1 int not null, -- primary key
    d2_p1 int, -- payload column
    d2_p2 int, -- payload column
    d2_p3 int, -- payload column
    d2_p4 int  -- payload column
);

CREATE MATERIALIZED VIEW d2 as
SELECT DISTINCT ON(d2_k1) * FROM d2_raw;

CREATE TABLE d3_raw(
    d3_k1 int not null, -- primary key
    d3_p1 int, -- payload column
    d3_p2 int, -- payload column
    d3_p3 int, -- payload column
    d3_p4 int  -- payload column
);

CREATE MATERIALIZED VIEW d3 as
SELECT DISTINCT ON(d3_k1) * FROM d3_raw;

-- Indexes
----------

CREATE INDEX f1_pk ON f1(f1_k1, f1_k2);
CREATE INDEX d1_pk ON d1(d1_k1);
CREATE INDEX d2_pk ON d2(d2_k1);
CREATE INDEX d3_pk ON d3(d3_k1);
