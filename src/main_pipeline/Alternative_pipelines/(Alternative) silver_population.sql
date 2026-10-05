-- Silver layer: DataUSA population (SQL alternative to silver_population.py)
-- Deduplicated by latest source file per (nation_id, year)

CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.population
COMMENT 'Cleansed DataUSA population data, exploded from nested JSON array'
(
  CONSTRAINT nation_id_not_null EXPECT (nation_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT year_not_null EXPECT (year IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH exploded AS (
  SELECT
    source_file,
    EXPLODE(data) AS record
  FROM rearc.bronze.population
),
parsed AS (
  SELECT
    TRIM(record['Nation']) AS nation,
    TRIM(record['Nation ID']) AS nation_id,
    CAST(CAST(record['Population'] AS DOUBLE) AS BIGINT) AS population,
    CAST(record['Year'] AS INT) AS year,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM exploded
)
SELECT nation, nation_id, population, year
FROM parsed
QUALIFY ROW_NUMBER() OVER (PARTITION BY nation_id, year ORDER BY _source_ts DESC) = 1
