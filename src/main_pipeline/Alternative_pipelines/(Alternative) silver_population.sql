-- Silver layer: DataUSA population (SQL alternative to silver_population.py)

CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.population
COMMENT 'Cleansed DataUSA population data, exploded from nested JSON array'
(
  CONSTRAINT nation_id_not_null EXPECT (nation_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT year_not_null EXPECT (year IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
SELECT
  TRIM(record['Nation']) AS nation,
  TRIM(record['Nation ID']) AS nation_id,
  CAST(CAST(record['Population'] AS DOUBLE) AS BIGINT) AS population,
  CAST(record['Year'] AS INT) AS year
FROM (
  SELECT EXPLODE(data) AS record
  FROM rearc.bronze.population
)
