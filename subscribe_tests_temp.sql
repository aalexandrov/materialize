BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (
    SELECT
        COALESCE(f000, lag(f000, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f000_filled,
        COALESCE(f001, lag(f001, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f001_filled,
        COALESCE(f002, lag(f002, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f002_filled,
        COALESCE(f003, lag(f003, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f003_filled,
        COALESCE(f004, lag(f004, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f004_filled,
        COALESCE(f005, lag(f005, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f005_filled,
        COALESCE(f006, lag(f006, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f006_filled,
        COALESCE(f007, lag(f007, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f007_filled
    FROM 
        materialize.accounts_29.t1
);

EXPLAIN
SELECT
    COALESCE(f000, lag(f000, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f000_filled,
    COALESCE(f001, lag(f001, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f001_filled,
    COALESCE(f002, lag(f002, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f002_filled,
    COALESCE(f003, lag(f003, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f003_filled,
    COALESCE(f004, lag(f004, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f004_filled,
    COALESCE(f005, lag(f005, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f005_filled,
    COALESCE(f006, lag(f006, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f006_filled,
    COALESCE(f007, lag(f007, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f007_filled
FROM 
    materialize.accounts_29.t1;

COPY (
    SUBSCRIBE (
        SELECT
            COALESCE(f000, lag(f000, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f000_filled,
            COALESCE(f001, lag(f001, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f001_filled,
            COALESCE(f002, lag(f002, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f002_filled,
            COALESCE(f003, lag(f003, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f003_filled,
            COALESCE(f004, lag(f004, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f004_filled,
            COALESCE(f005, lag(f005, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f005_filled,
            COALESCE(f006, lag(f006, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f006_filled,
            COALESCE(f007, lag(f007, 1) IGNORE NULLS OVER (ORDER BY read_time)) AS f007_filled
        FROM 
            materialize.accounts_29.t1
    )
) TO STDOUT;