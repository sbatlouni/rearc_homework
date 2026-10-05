from pyspark import pipelines as dp
from pyspark.sql.functions import col

_BASE = "/Volumes/rearc/raw/volume/bls/pr"


def _ingest_bls(path, clean_names):
    """Load a BLS tab-delimited file via Auto Loader with clean column names and source file metadata."""
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("sep", "\t")
        .load(path)
    )
    cols = df.columns
    for i, name in enumerate(clean_names):
        if i < len(cols) and cols[i] != name:
            df = df.withColumnRenamed(cols[i], name)
    return df.withColumn("source_file", col("_metadata.file_name"))


@dp.table(name="rearc.bronze.series", comment="Raw BLS PR series metadata ingested via Auto Loader")
def bronze_series():
    return _ingest_bls(f"{_BASE}/pr.series", [
        "series_id", "sector_code", "class_code", "measure_code",
        "duration_code", "seasonal", "base_year", "footnote_codes",
        "begin_year", "begin_period", "end_year", "end_period",
    ])


@dp.table(name="rearc.bronze.current_data", comment="Raw BLS PR current data ingested via Auto Loader")
def bronze_current_data():
    return _ingest_bls(f"{_BASE}/pr.data.0.Current", [
        "series_id", "year", "period", "value", "footnote_codes",
    ])


@dp.table(name="rearc.bronze.all_data", comment="Raw BLS PR all historical data ingested via Auto Loader")
def bronze_all_data():
    return _ingest_bls(f"{_BASE}/pr.data.1.AllData", [
        "series_id", "year", "period", "value", "footnote_codes",
    ])


@dp.table(name="rearc.bronze.class", comment="Raw BLS PR class lookup ingested via Auto Loader")
def bronze_class():
    return _ingest_bls(f"{_BASE}/pr.class", [
        "class_code", "class_text", "display_level", "selectable", "sort_sequence",
    ])


@dp.table(name="rearc.bronze.measure", comment="Raw BLS PR measure lookup ingested via Auto Loader")
def bronze_measure():
    return _ingest_bls(f"{_BASE}/pr.measure", [
        "measure_code", "measure_text", "display_level", "selectable", "sort_sequence",
    ])


@dp.table(name="rearc.bronze.sector", comment="Raw BLS PR sector lookup ingested via Auto Loader")
def bronze_sector():
    return _ingest_bls(f"{_BASE}/pr.sector", [
        "sector_code", "sector_name", "display_level", "selectable", "sort_sequence",
    ])


@dp.table(name="rearc.bronze.duration", comment="Raw BLS PR duration lookup ingested via Auto Loader")
def bronze_duration():
    return _ingest_bls(f"{_BASE}/pr.duration", [
        "duration_code", "duration_text", "display_level", "selectable", "sort_sequence",
    ])


@dp.table(name="rearc.bronze.period", comment="Raw BLS PR period lookup ingested via Auto Loader")
def bronze_period():
    return _ingest_bls(f"{_BASE}/pr.period", [
        "period_code", "period_abbr", "period_name",
    ])


@dp.table(name="rearc.bronze.seasonal", comment="Raw BLS PR seasonal lookup ingested via Auto Loader")
def bronze_seasonal():
    return _ingest_bls(f"{_BASE}/pr.seasonal", [
        "seasonal_code", "seasonal_text",
    ])


@dp.table(name="rearc.bronze.footnote", comment="Raw BLS PR footnote lookup ingested via Auto Loader")
def bronze_footnote():
    return _ingest_bls(f"{_BASE}/pr.footnote", [
        "footnote_code", "footnote_text",
    ])
