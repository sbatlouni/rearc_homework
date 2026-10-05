-- Bronze layer: BLS Productivity and Costs (SQL alternative to bronze_bls.py)
-- Ingests tab-delimited BLS files via Auto Loader with an explicit all-STRING schema.
--
-- Differences from the PySpark version:
--   * BLS headers are padded and inconsistently cased ("series_id        ", "Seasonal_code"), so the
--     schema is applied by position (enforceSchema => true ignores the header) rather than by name.
--   * SQL has no definition-time hook to compare the file header with the expected columns, so a
--     reordered column is caught by the row-level format expectations instead (e.g. a year that is
--     not four digits), and an extra column is caught by the rescued-data expectation.
-- Typing is left to silver, so codes such as measure "01" keep their leading zeros.

-- ── rearc.bronze.series ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.series
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT series_id_format EXPECT (TRIM(series_id) RLIKE '^PR[SU][0-9]{4}[0-9][0-9]{2}[0-9]$') ON VIOLATION FAIL UPDATE,
  CONSTRAINT measure_code_format EXPECT (TRIM(measure_code) RLIKE '^[0-9]{2}$') ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw BLS PR series metadata ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.series',
  format => 'csv',
  header => true,
  enforceSchema => true,
  sep => '\t',
  schema => 'series_id STRING, sector_code STRING, class_code STRING, measure_code STRING, duration_code STRING, seasonal STRING, base_year STRING, footnote_codes STRING, begin_year STRING, begin_period STRING, end_year STRING, end_period STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.current_data ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.current_data
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT series_id_format EXPECT (TRIM(series_id) RLIKE '^PR[SU][0-9]{4}[0-9][0-9]{2}[0-9]$') ON VIOLATION FAIL UPDATE,
  CONSTRAINT year_format EXPECT (TRIM(year) RLIKE '^[0-9]{4}$') ON VIOLATION FAIL UPDATE,
  CONSTRAINT period_format EXPECT (TRIM(period) RLIKE '^Q0[1-5]$') ON VIOLATION FAIL UPDATE,
  CONSTRAINT value_numeric EXPECT (TRY_CAST(TRIM(value) AS DOUBLE) IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw BLS PR current data ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.data.0.Current',
  format => 'csv',
  header => true,
  enforceSchema => true,
  sep => '\t',
  schema => 'series_id STRING, year STRING, period STRING, value STRING, footnote_codes STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.all_data ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.all_data
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT series_id_format EXPECT (TRIM(series_id) RLIKE '^PR[SU][0-9]{4}[0-9][0-9]{2}[0-9]$') ON VIOLATION FAIL UPDATE,
  CONSTRAINT year_format EXPECT (TRIM(year) RLIKE '^[0-9]{4}$') ON VIOLATION FAIL UPDATE,
  CONSTRAINT period_format EXPECT (TRIM(period) RLIKE '^Q0[1-5]$') ON VIOLATION FAIL UPDATE,
  CONSTRAINT value_numeric EXPECT (TRY_CAST(TRIM(value) AS DOUBLE) IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw BLS PR all historical data ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.data.1.AllData',
  format => 'csv',
  header => true,
  enforceSchema => true,
  sep => '\t',
  schema => 'series_id STRING, year STRING, period STRING, value STRING, footnote_codes STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.class ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.class
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT class_code_format EXPECT (TRIM(class_code) RLIKE '^[0-9]$') ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw BLS PR class lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.class',
  format => 'csv',
  header => true,
  enforceSchema => true,
  sep => '\t',
  schema => 'class_code STRING, class_text STRING, display_level STRING, selectable STRING, sort_sequence STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.measure ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.measure
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT measure_code_format EXPECT (TRIM(measure_code) RLIKE '^[0-9]{2}$') ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw BLS PR measure lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.measure',
  format => 'csv',
  header => true,
  enforceSchema => true,
  sep => '\t',
  schema => 'measure_code STRING, measure_text STRING, display_level STRING, selectable STRING, sort_sequence STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.sector ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.sector
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT sector_code_format EXPECT (TRIM(sector_code) RLIKE '^[0-9]{4}$') ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw BLS PR sector lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.sector',
  format => 'csv',
  header => true,
  enforceSchema => true,
  sep => '\t',
  schema => 'sector_code STRING, sector_name STRING, display_level STRING, selectable STRING, sort_sequence STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.duration ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.duration
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT duration_code_format EXPECT (TRIM(duration_code) RLIKE '^[0-9]$') ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw BLS PR duration lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.duration',
  format => 'csv',
  header => true,
  enforceSchema => true,
  sep => '\t',
  schema => 'duration_code STRING, duration_text STRING, display_level STRING, selectable STRING, sort_sequence STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.period ──
-- The file's first column is named "period"; it is loaded as period_code to match silver.
CREATE OR REFRESH STREAMING TABLE rearc.bronze.period
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT period_code_format EXPECT (TRIM(period_code) RLIKE '^Q0[1-5]$') ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw BLS PR period lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.period',
  format => 'csv',
  header => true,
  enforceSchema => true,
  sep => '\t',
  schema => 'period_code STRING, period_abbr STRING, period_name STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.seasonal ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.seasonal
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT seasonal_code_format EXPECT (TRIM(seasonal_code) RLIKE '^[SU]$') ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw BLS PR seasonal lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.seasonal',
  format => 'csv',
  header => true,
  enforceSchema => true,
  sep => '\t',
  schema => 'seasonal_code STRING, seasonal_text STRING',
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.footnote ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.footnote
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT footnote_code_format EXPECT (TRIM(footnote_code) RLIKE '^[A-Z]$') ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw BLS PR footnote lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.footnote',
  format => 'csv',
  header => true,
  enforceSchema => true,
  sep => '\t',
  schema => 'footnote_code STRING, footnote_text STRING',
  rescuedDataColumn => '_rescued_data'
);
