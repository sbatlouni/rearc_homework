-- Bronze layer: DataUSA Population (SQL alternative to bronze_population.py)
-- Ingests multi-line JSON via Auto Loader with explicit schema hints

CREATE OR REFRESH STREAMING TABLE rearc.bronze.population
COMMENT 'Raw DataUSA population data ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/datausa',
  format => 'json',
  multiLine => true,
  schemaHints => 'annotations MAP<STRING, STRING>, columns ARRAY<STRING>, data ARRAY<MAP<STRING, STRING>>, page MAP<STRING, STRING>'
);
