-- Gold layer: Productivity series joined with population

CREATE OR REFRESH MATERIALIZED VIEW rearc.gold.series_population
COMMENT 'Productivity data for series PRS30006032, Q01 period, joined with US population by year'
AS
SELECT
  d.series_id,
  d.year,
  d.period,
  p.population,
  d.value
FROM rearc.silver.all_data d
LEFT JOIN rearc.silver.population p ON d.year = p.year
WHERE d.series_id = 'PRS30006032'
  AND d.period = 'Q01'
