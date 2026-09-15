-- Confluent Cloud for Apache Flink
--
-- The Worker sends schemaless JSON to topic `economic.raw` through Kafka REST.
-- With no Schema Registry subjects on that raw topic, Confluent exposes it as
-- a raw inferred table containing VARBINARY key/value columns (typically `key`
-- and `val`). Confirm with:
--
--   SHOW CREATE TABLE `default`.`economic-pulse`.`economic.raw`;
--
-- If your inferred value column has a different name, replace `val` below.

CREATE TABLE IF NOT EXISTS economy_dashboard (
  metric STRING NOT NULL,
  source STRING,
  label STRING,
  `value` DOUBLE,
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

EXECUTE STATEMENT SET
BEGIN
INSERT INTO economy_dashboard
SELECT
  JSON_VALUE(MAKE_VALID_UTF8(val), '$.metric') AS metric,
  JSON_VALUE(MAKE_VALID_UTF8(val), '$.source') AS source,
  JSON_VALUE(MAKE_VALID_UTF8(val), '$.label') AS label,
  CAST(JSON_VALUE(MAKE_VALID_UTF8(val), '$.value') AS DOUBLE) AS `value`,
  JSON_VALUE(MAKE_VALID_UTF8(val), '$.unit') AS unit,
  JSON_VALUE(MAKE_VALID_UTF8(val), '$.observed_at') AS observed_at,
  JSON_VALUE(MAKE_VALID_UTF8(val), '$.collected_at') AS collected_at,
  JSON_QUERY(MAKE_VALID_UTF8(val), '$.dimensions') AS dimensions_json
FROM `economic.raw`
WHERE JSON_VALUE(MAKE_VALID_UTF8(val), '$.metric') IS NOT NULL;
END;
