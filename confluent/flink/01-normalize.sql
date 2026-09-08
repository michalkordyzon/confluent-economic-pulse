-- Confluent Cloud for Apache Flink
--
-- The Worker sends schemaless JSON to topic `economic.raw` through Kafka REST.
-- With no Schema Registry subjects on that raw topic, Confluent exposes it as
-- a raw inferred table containing VARBINARY key/value columns (typically `key`
-- and `val`). Confirm with:
--
--   SHOW CREATE TABLE `economic.raw`;
--
-- If your inferred value column has a different name, replace `val` below.

CREATE TABLE IF NOT EXISTS economy_dashboard (
  metric STRING NOT NULL,
  source STRING,
  label STRING,
  value DOUBLE,
  unit STRING,
  observed_at STRING,
  collected_at STRING,
  dimensions_json STRING,
  PRIMARY KEY (metric) NOT ENFORCED
)
DISTRIBUTED BY HASH(metric) INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry'
);

INSERT INTO economy_dashboard
SELECT
  JSON_VALUE(CAST(val AS STRING), '$.metric') AS metric,
  JSON_VALUE(CAST(val AS STRING), '$.source') AS source,
  JSON_VALUE(CAST(val AS STRING), '$.label') AS label,
  CAST(JSON_VALUE(CAST(val AS STRING), '$.value') AS DOUBLE) AS value,
  JSON_VALUE(CAST(val AS STRING), '$.unit') AS unit,
  JSON_VALUE(CAST(val AS STRING), '$.observed_at') AS observed_at,
  JSON_VALUE(CAST(val AS STRING), '$.collected_at') AS collected_at,
  JSON_QUERY(CAST(val AS STRING), '$.dimensions') AS dimensions_json
FROM `economic.raw`
WHERE JSON_VALUE(CAST(val AS STRING), '$.metric') IS NOT NULL;
