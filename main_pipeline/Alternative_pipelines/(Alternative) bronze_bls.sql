-- Bronze layer: BLS Productivity and Costs (SQL alternative to bronze_bls.py)
-- Ingests tab-delimited BLS files via Auto Loader with column type inference

-- ── rearc.bronze.series ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.series
COMMENT 'Raw BLS PR series metadata ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.series',
  format => 'csv',
  header => true,
  sep => '\t',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.current_data ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.current_data
COMMENT 'Raw BLS PR current data ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.data.0.Current',
  format => 'csv',
  header => true,
  sep => '\t',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.all_data ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.all_data
COMMENT 'Raw BLS PR all historical data ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.data.1.AllData',
  format => 'csv',
  header => true,
  sep => '\t',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.class ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.class
COMMENT 'Raw BLS PR class lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.class',
  format => 'csv',
  header => true,
  sep => '\t',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.measure ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.measure
COMMENT 'Raw BLS PR measure lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.measure',
  format => 'csv',
  header => true,
  sep => '\t',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.sector ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.sector
COMMENT 'Raw BLS PR sector lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.sector',
  format => 'csv',
  header => true,
  sep => '\t',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.duration ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.duration
COMMENT 'Raw BLS PR duration lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.duration',
  format => 'csv',
  header => true,
  sep => '\t',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.period ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.period
COMMENT 'Raw BLS PR period lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.period',
  format => 'csv',
  header => true,
  sep => '\t',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.seasonal ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.seasonal
COMMENT 'Raw BLS PR seasonal lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.seasonal',
  format => 'csv',
  header => true,
  sep => '\t',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data'
);

-- ── rearc.bronze.footnote ──
CREATE OR REFRESH STREAMING TABLE rearc.bronze.footnote
COMMENT 'Raw BLS PR footnote lookup ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/bls/pr/pr.footnote',
  format => 'csv',
  header => true,
  sep => '\t',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data'
);
