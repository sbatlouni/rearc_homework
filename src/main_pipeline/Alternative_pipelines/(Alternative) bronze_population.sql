-- Bronze layer: DataUSA Population (SQL alternative to bronze_population.py)
-- Ingests multi-line JSON via Auto Loader with an explicit schema; fields outside it are rescued

CREATE OR REFRESH STREAMING TABLE rearc.bronze.population
(
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT has_data EXPECT (size(data) > 0) ON VIOLATION FAIL UPDATE,
  CONSTRAINT expected_columns EXPECT (array_sort(columns) = array_sort(array('Nation ID', 'Nation', 'Year', 'Population'))) ON VIOLATION FAIL UPDATE
)
COMMENT 'Raw DataUSA population data ingested via Auto Loader'
AS
SELECT *, _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/rearc/raw/volume/datausa',
  format => 'json',
  multiLine => true,
  schema => 'annotations MAP<STRING, STRING>, columns ARRAY<STRING>, data ARRAY<MAP<STRING, STRING>>, page MAP<STRING, STRING>',
  rescuedDataColumn => '_rescued_data'
);
