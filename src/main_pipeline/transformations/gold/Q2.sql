-- Gold layer: Major Sector Productivity and Costs Index

CREATE OR REFRESH MATERIALIZED VIEW rearc.gold.productivity_costs_index
COMMENT 'Major Sector Productivity and Costs Index - highest annual total per series with dimension lookups'
AS
WITH yearly_totals AS (
  SELECT
    series_id,
    year,
    SUM(value) AS total
  FROM rearc.silver.all_data
  GROUP BY series_id, year
),
ranked AS (
  SELECT
    series_id,
    year,
    total,
    SUBSTRING(series_id, 3, 1) AS season_code,
    SUBSTRING(series_id, 4, 4) AS sector_code,
    SUBSTRING(series_id, 8, 1) AS class_code,
    SUBSTRING(series_id, 9, 2) AS measure_code,
    SUBSTRING(series_id, 11, 1) AS duration_code
  FROM yearly_totals
  QUALIFY row_number() OVER (PARTITION BY series_id ORDER BY total DESC) = 1
)
SELECT
  r.series_id,
  'PR' AS survey_code,
  r.season_code,
  r.sector_code,
  r.class_code,
  r.measure_code,
  r.duration_code,
  'Major Sector Productivity and Costs Index' AS survey_abbreviation,
  s.seasonal_text,
  sec.sector_name,
  c.class_text,
  m.measure_text,
  d.duration_text,
  r.year,
  r.total
FROM ranked r
LEFT JOIN rearc.silver.seasonal  s   ON r.season_code   = s.seasonal_code
LEFT JOIN rearc.silver.sector    sec ON r.sector_code   = sec.sector_code
LEFT JOIN rearc.silver.class     c   ON r.class_code    = c.class_code
LEFT JOIN rearc.silver.measure   m   ON r.measure_code  = m.measure_code
LEFT JOIN rearc.silver.duration  d   ON r.duration_code = d.duration_code
