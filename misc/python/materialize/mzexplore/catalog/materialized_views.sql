-- extract CREATE MATERIALIZED VIEW definitions
SELECT
  mv.id as id,
  mv.oid as oid,
  mv.name as name,
  mv.definition as definition,
  c.name as cluster,
  d.name as database,
  s.name as schema
FROM
  mz_materialized_views AS mv JOIN 
  mz_clusters AS c ON (mv.cluster_id = c.id) JOIN
  mz_schemas AS s ON (mv.schema_id = s.id) JOIN
  mz_databases AS d ON (s.database_id = d.id)
WHERE
  mv.id like 'u%'
ORDER BY
  d.name,
  s.name,
  mv.name;
