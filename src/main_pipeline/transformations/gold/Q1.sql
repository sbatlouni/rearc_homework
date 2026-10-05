-- Gold layer KPIs

CREATE OR REFRESH MATERIALIZED VIEW rearc.gold.population_stats_2013_2018
COMMENT 'Mean and standard deviation of annual US population, 2013-2018 inclusive'
AS
SELECT
  AVG(population) AS mean_population,
  STDDEV(population) AS stddev_population
FROM rearc.silver.population
WHERE year BETWEEN 2013 AND 2018
  AND nation_id = '01000US'
