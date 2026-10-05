from pyspark import pipelines as dp
from pyspark.sql.functions import col

_BASE = "/Volumes/rearc/raw/volume/bls/pr"

# BLS series IDs are PR + seasonal (S/U) + sector (4) + class (1) + measure (2) + duration (1); see pr.txt.
_SERIES_ID_FORMAT = r"TRIM(series_id) RLIKE '^PR[SU][0-9]{4}[0-9][0-9]{2}[0-9]$'"

# Applied to every BLS bronze table: any row with fields that don't fit the expected columns is rescued.
_NO_RESCUED_DATA = {"no_rescued_data": "_rescued_data IS NULL"}

_DATA_EXPECTATIONS = {
    **_NO_RESCUED_DATA,
    "series_id_format": _SERIES_ID_FORMAT,
    "year_format": "TRIM(year) RLIKE '^[0-9]{4}$'",
    "period_format": "TRIM(period) RLIKE '^Q0[1-5]$'",
    "value_numeric": "TRY_CAST(TRIM(value) AS DOUBLE) IS NOT NULL",
}


def _ingest_bls(path, columns):
    """Load a BLS tab-delimited file via Auto Loader, checking its header against the expected columns.

    `columns` is a list of (header in the BLS file, clean column name). Headers are compared after
    trimming and ignoring case, because BLS pads some of them (e.g. "series_id        ") and
    capitalises others (e.g. "Seasonal_code"). Every column is read as a string; silver does the typing,
    so codes like measure "01" keep their leading zeros.
    """
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("sep", "\t")
        .load(path)
    )
    file_cols = [c for c in df.columns if c != "_rescued_data"]
    expected = [header.lower() for header, _ in columns]
    actual = [c.strip().lower() for c in file_cols]
    if actual != expected:
        raise ValueError(f"Unexpected columns in {path}: expected {expected}, got {actual}")

    for file_col, (_, name) in zip(file_cols, columns):
        df = df.withColumnRenamed(file_col, name)
    return df.withColumn("source_file", col("_metadata.file_name"))


@dp.table(name="rearc.bronze.series", comment="Raw BLS PR series metadata ingested via Auto Loader")
@dp.expect_all_or_fail({
    **_NO_RESCUED_DATA,
    "series_id_format": _SERIES_ID_FORMAT,
    "measure_code_format": "TRIM(measure_code) RLIKE '^[0-9]{2}$'",
})
def bronze_series():
    return _ingest_bls(f"{_BASE}/pr.series", [
        ("series_id", "series_id"), ("sector_code", "sector_code"), ("class_code", "class_code"),
        ("measure_code", "measure_code"), ("duration_code", "duration_code"), ("seasonal", "seasonal"),
        ("base_year", "base_year"), ("footnote_codes", "footnote_codes"), ("begin_year", "begin_year"),
        ("begin_period", "begin_period"), ("end_year", "end_year"), ("end_period", "end_period"),
    ])


@dp.table(name="rearc.bronze.current_data", comment="Raw BLS PR current data ingested via Auto Loader")
@dp.expect_all_or_fail(_DATA_EXPECTATIONS)
def bronze_current_data():
    return _ingest_bls(f"{_BASE}/pr.data.0.Current", [
        ("series_id", "series_id"), ("year", "year"), ("period", "period"),
        ("value", "value"), ("footnote_codes", "footnote_codes"),
    ])


@dp.table(name="rearc.bronze.all_data", comment="Raw BLS PR all historical data ingested via Auto Loader")
@dp.expect_all_or_fail(_DATA_EXPECTATIONS)
def bronze_all_data():
    return _ingest_bls(f"{_BASE}/pr.data.1.AllData", [
        ("series_id", "series_id"), ("year", "year"), ("period", "period"),
        ("value", "value"), ("footnote_codes", "footnote_codes"),
    ])


@dp.table(name="rearc.bronze.class", comment="Raw BLS PR class lookup ingested via Auto Loader")
@dp.expect_all_or_fail({**_NO_RESCUED_DATA, "class_code_format": "TRIM(class_code) RLIKE '^[0-9]$'"})
def bronze_class():
    return _ingest_bls(f"{_BASE}/pr.class", [
        ("class_code", "class_code"), ("class_text", "class_text"), ("display_level", "display_level"),
        ("selectable", "selectable"), ("sort_sequence", "sort_sequence"),
    ])


@dp.table(name="rearc.bronze.measure", comment="Raw BLS PR measure lookup ingested via Auto Loader")
@dp.expect_all_or_fail({**_NO_RESCUED_DATA, "measure_code_format": "TRIM(measure_code) RLIKE '^[0-9]{2}$'"})
def bronze_measure():
    return _ingest_bls(f"{_BASE}/pr.measure", [
        ("measure_code", "measure_code"), ("measure_text", "measure_text"), ("display_level", "display_level"),
        ("selectable", "selectable"), ("sort_sequence", "sort_sequence"),
    ])


@dp.table(name="rearc.bronze.sector", comment="Raw BLS PR sector lookup ingested via Auto Loader")
@dp.expect_all_or_fail({**_NO_RESCUED_DATA, "sector_code_format": "TRIM(sector_code) RLIKE '^[0-9]{4}$'"})
def bronze_sector():
    return _ingest_bls(f"{_BASE}/pr.sector", [
        ("sector_code", "sector_code"), ("sector_name", "sector_name"), ("display_level", "display_level"),
        ("selectable", "selectable"), ("sort_sequence", "sort_sequence"),
    ])


@dp.table(name="rearc.bronze.duration", comment="Raw BLS PR duration lookup ingested via Auto Loader")
@dp.expect_all_or_fail({**_NO_RESCUED_DATA, "duration_code_format": "TRIM(duration_code) RLIKE '^[0-9]$'"})
def bronze_duration():
    return _ingest_bls(f"{_BASE}/pr.duration", [
        ("duration_code", "duration_code"), ("duration_text", "duration_text"), ("display_level", "display_level"),
        ("selectable", "selectable"), ("sort_sequence", "sort_sequence"),
    ])


@dp.table(name="rearc.bronze.period", comment="Raw BLS PR period lookup ingested via Auto Loader")
@dp.expect_all_or_fail({**_NO_RESCUED_DATA, "period_code_format": "TRIM(period_code) RLIKE '^Q0[1-5]$'"})
def bronze_period():
    return _ingest_bls(f"{_BASE}/pr.period", [
        ("period", "period_code"), ("period_abbr", "period_abbr"), ("period_name", "period_name"),
    ])


@dp.table(name="rearc.bronze.seasonal", comment="Raw BLS PR seasonal lookup ingested via Auto Loader")
@dp.expect_all_or_fail({**_NO_RESCUED_DATA, "seasonal_code_format": "TRIM(seasonal_code) RLIKE '^[SU]$'"})
def bronze_seasonal():
    return _ingest_bls(f"{_BASE}/pr.seasonal", [
        ("seasonal_code", "seasonal_code"), ("seasonal_text", "seasonal_text"),
    ])


@dp.table(name="rearc.bronze.footnote", comment="Raw BLS PR footnote lookup ingested via Auto Loader")
@dp.expect_all_or_fail({**_NO_RESCUED_DATA, "footnote_code_format": "TRIM(footnote_code) RLIKE '^[A-Z]$'"})
def bronze_footnote():
    return _ingest_bls(f"{_BASE}/pr.footnote", [
        ("footnote_code", "footnote_code"), ("footnote_text", "footnote_text"),
    ])
