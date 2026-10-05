-- Silver layer: BLS Productivity and Costs (SQL alternative to silver_bls.py)
-- Each MV deduplicates by keeping the row from the latest source file,
-- parsed from the filename timestamp pattern _yyyyMMdd_HHmmss

-- ── rearc.silver.series ──
CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.series
COMMENT 'Cleansed BLS PR series metadata, deduplicated by latest source file'
(
  CONSTRAINT series_id_not_null EXPECT (series_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH src AS (
  SELECT
    TRIM(series_id) AS series_id,
    TRIM(CAST(sector_code AS STRING)) AS sector_code,
    TRIM(CAST(class_code AS STRING)) AS class_code,
    TRIM(CAST(measure_code AS STRING)) AS measure_code,
    TRIM(CAST(duration_code AS STRING)) AS duration_code,
    TRIM(seasonal) AS seasonal,
    CASE WHEN TRIM(base_year) = '-' THEN NULL ELSE TRIM(base_year) END AS base_year,
    TRIM(footnote_codes) AS footnote_codes,
    CAST(begin_year AS INT) AS begin_year,
    TRIM(begin_period) AS begin_period,
    CAST(end_year AS INT) AS end_year,
    TRIM(end_period) AS end_period,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM rearc.bronze.series
)
SELECT series_id, sector_code, class_code, measure_code, duration_code, seasonal, base_year, footnote_codes, begin_year, begin_period, end_year, end_period
FROM src
QUALIFY ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY _source_ts DESC) = 1;

-- ── rearc.silver.current_data ──
CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.current_data
COMMENT 'Cleansed BLS PR current data, deduplicated by latest source file'
(
  CONSTRAINT series_id_not_null EXPECT (series_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT year_not_null EXPECT (year IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT period_not_null EXPECT (period IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH src AS (
  SELECT
    TRIM(series_id) AS series_id,
    CAST(year AS INT) AS year,
    TRIM(period) AS period,
    CAST(value AS DOUBLE) AS value,
    TRIM(footnote_codes) AS footnote_codes,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM rearc.bronze.current_data
)
SELECT series_id, year, period, value, footnote_codes
FROM src
QUALIFY ROW_NUMBER() OVER (PARTITION BY series_id, year, period ORDER BY _source_ts DESC) = 1;

-- ── rearc.silver.all_data ──
CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.all_data
COMMENT 'Cleansed BLS PR all historical data, deduplicated by latest source file'
(
  CONSTRAINT series_id_not_null EXPECT (series_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT year_not_null EXPECT (year IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT period_not_null EXPECT (period IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH src AS (
  SELECT
    TRIM(series_id) AS series_id,
    CAST(year AS INT) AS year,
    TRIM(period) AS period,
    CAST(value AS DOUBLE) AS value,
    TRIM(footnote_codes) AS footnote_codes,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM rearc.bronze.all_data
)
SELECT series_id, year, period, value, footnote_codes
FROM src
QUALIFY ROW_NUMBER() OVER (PARTITION BY series_id, year, period ORDER BY _source_ts DESC) = 1;

-- ── rearc.silver.class ──
CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.class
COMMENT 'Cleansed BLS PR class lookup, deduplicated by latest source file'
(
  CONSTRAINT class_code_not_null EXPECT (class_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH src AS (
  SELECT
    TRIM(CAST(class_code AS STRING)) AS class_code,
    TRIM(class_text) AS class_text,
    CAST(display_level AS INT) AS display_level,
    TRIM(selectable) AS selectable,
    CAST(sort_sequence AS INT) AS sort_sequence,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM rearc.bronze.class
)
SELECT class_code, class_text, display_level, selectable, sort_sequence
FROM src
QUALIFY ROW_NUMBER() OVER (PARTITION BY class_code ORDER BY _source_ts DESC) = 1;

-- ── rearc.silver.measure ──
CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.measure
COMMENT 'Cleansed BLS PR measure lookup, deduplicated by latest source file'
(
  CONSTRAINT measure_code_not_null EXPECT (measure_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH src AS (
  SELECT
    TRIM(CAST(measure_code AS STRING)) AS measure_code,
    TRIM(measure_text) AS measure_text,
    CAST(display_level AS INT) AS display_level,
    TRIM(selectable) AS selectable,
    CAST(sort_sequence AS INT) AS sort_sequence,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM rearc.bronze.measure
)
SELECT measure_code, measure_text, display_level, selectable, sort_sequence
FROM src
QUALIFY ROW_NUMBER() OVER (PARTITION BY measure_code ORDER BY _source_ts DESC) = 1;

-- ── rearc.silver.sector ──
CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.sector
COMMENT 'Cleansed BLS PR sector lookup, deduplicated by latest source file'
(
  CONSTRAINT sector_code_not_null EXPECT (sector_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH src AS (
  SELECT
    TRIM(CAST(sector_code AS STRING)) AS sector_code,
    TRIM(sector_name) AS sector_name,
    CAST(display_level AS INT) AS display_level,
    TRIM(selectable) AS selectable,
    CAST(sort_sequence AS INT) AS sort_sequence,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM rearc.bronze.sector
)
SELECT sector_code, sector_name, display_level, selectable, sort_sequence
FROM src
QUALIFY ROW_NUMBER() OVER (PARTITION BY sector_code ORDER BY _source_ts DESC) = 1;

-- ── rearc.silver.duration ──
CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.duration
COMMENT 'Cleansed BLS PR duration lookup, deduplicated by latest source file'
(
  CONSTRAINT duration_code_not_null EXPECT (duration_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH src AS (
  SELECT
    TRIM(CAST(duration_code AS STRING)) AS duration_code,
    TRIM(duration_text) AS duration_text,
    CAST(display_level AS INT) AS display_level,
    TRIM(selectable) AS selectable,
    CAST(sort_sequence AS INT) AS sort_sequence,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM rearc.bronze.duration
)
SELECT duration_code, duration_text, display_level, selectable, sort_sequence
FROM src
QUALIFY ROW_NUMBER() OVER (PARTITION BY duration_code ORDER BY _source_ts DESC) = 1;

-- ── rearc.silver.period ──
CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.period
COMMENT 'Cleansed BLS PR period lookup, deduplicated by latest source file'
(
  CONSTRAINT period_code_not_null EXPECT (period_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH src AS (
  SELECT
    TRIM(period_code) AS period_code,
    TRIM(period_abbr) AS period_abbr,
    TRIM(period_name) AS period_name,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM rearc.bronze.period
)
SELECT period_code, period_abbr, period_name
FROM src
QUALIFY ROW_NUMBER() OVER (PARTITION BY period_code ORDER BY _source_ts DESC) = 1;

-- ── rearc.silver.seasonal ──
CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.seasonal
COMMENT 'Cleansed BLS PR seasonal lookup, deduplicated by latest source file'
(
  CONSTRAINT seasonal_code_not_null EXPECT (seasonal_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH src AS (
  SELECT
    TRIM(seasonal_code) AS seasonal_code,
    TRIM(seasonal_text) AS seasonal_text,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM rearc.bronze.seasonal
)
SELECT seasonal_code, seasonal_text
FROM src
QUALIFY ROW_NUMBER() OVER (PARTITION BY seasonal_code ORDER BY _source_ts DESC) = 1;

-- ── rearc.silver.footnote ──
CREATE OR REFRESH MATERIALIZED VIEW rearc.silver.footnote
COMMENT 'Cleansed BLS PR footnote lookup, deduplicated by latest source file'
(
  CONSTRAINT footnote_code_not_null EXPECT (footnote_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS
WITH src AS (
  SELECT
    TRIM(footnote_code) AS footnote_code,
    TRIM(footnote_text) AS footnote_text,
    to_timestamp(regexp_extract(source_file, '_([0-9]{8}_[0-9]{6})$', 1), 'yyyyMMdd_HHmmss') AS _source_ts
  FROM rearc.bronze.footnote
)
SELECT footnote_code, footnote_text
FROM src
QUALIFY ROW_NUMBER() OVER (PARTITION BY footnote_code ORDER BY _source_ts DESC) = 1;
