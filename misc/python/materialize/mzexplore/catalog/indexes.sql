-- extract CREATE INDEX definitions
SELECT 
  o.id as id,
  o.oid as oid,
  i.name as name,
  i.on as on_name,
  i.key as key,
  s.name as schema,
  d.name as database,
  i.cluster as cluster
FROM
  mz_internal.mz_show_indexes AS i JOIN
  mz_objects AS o ON (i.name = o.name) JOIN
  mz_schemas AS s ON (o.schema_id = s.id) JOIN
  mz_databases AS d ON (s.database_id = d.id)
WHERE
  o.id like 'u%'
ORDER BY
  d.name,
  s.name,
  i.name;
