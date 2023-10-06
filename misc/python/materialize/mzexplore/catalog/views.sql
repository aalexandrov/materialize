-- extract CREATE VIEW definitions
SELECT 
  v.id as id,
  v.oid as oid,
  v.name as name,
  v.definition as definition,
  d.name as database,
  s.name as schema
FROM
  mz_views AS v JOIN 
  mz_schemas AS s ON (v.schema_id = s.id) JOIN
  mz_databases AS d ON (s.database_id = d.id)
WHERE
  v.id like 'u%'
ORDER BY
  d.name,
  s.name,
  v.name;
