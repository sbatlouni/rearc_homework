from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, when, lit, regexp_extract, to_timestamp, row_number
from pyspark.sql.window import Window


def _parse_file_timestamp(file_col):
    """Parse the timestamp from a BLS source filename (e.g., pr.series_20260903_083000)."""
    return to_timestamp(
        regexp_extract(file_col, r"_([0-9]{8}_[0-9]{6})$", 1),
        "yyyyMMdd_HHmmss",
    )


def _dedup_latest(df, key_cols):
    """Deduplicate rows by key columns, keeping the row from the latest source file."""
    w = Window.partitionBy(*key_cols).orderBy(col("_source_ts").desc())
    return (
        df.withColumn("_source_ts", _parse_file_timestamp(col("source_file")))
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn", "_source_ts", "source_file")
    )


@dp.materialized_view(name="rearc.silver.series", comment="Cleansed BLS PR series metadata, deduplicated by latest source file")
@dp.expect_or_fail("series_id_not_null", "series_id IS NOT NULL")
def silver_series():
    return _dedup_latest(
        spark.read.table("rearc.bronze.series")
        .select(
            trim(col("series_id")).alias("series_id"),
            trim(col("sector_code")).alias("sector_code"),
            trim(col("class_code")).alias("class_code"),
            trim(col("measure_code")).alias("measure_code"),
            trim(col("duration_code")).alias("duration_code"),
            trim(col("seasonal")).alias("seasonal"),
            when(col("base_year") == "-", lit(None)).otherwise(trim(col("base_year"))).alias("base_year"),
            trim(col("footnote_codes")).alias("footnote_codes"),
            col("begin_year").cast("int").alias("begin_year"),
            trim(col("begin_period")).alias("begin_period"),
            col("end_year").cast("int").alias("end_year"),
            trim(col("end_period")).alias("end_period"),
            col("source_file"),
        ),
        ["series_id"],
    )


@dp.materialized_view(name="rearc.silver.current_data", comment="Cleansed BLS PR current data, deduplicated by latest source file")
@dp.expect_or_fail("series_id_not_null", "series_id IS NOT NULL")
@dp.expect_or_fail("year_not_null", "year IS NOT NULL")
@dp.expect_or_fail("period_not_null", "period IS NOT NULL")
def silver_current_data():
    return _dedup_latest(
        spark.read.table("rearc.bronze.current_data")
        .select(
            trim(col("series_id")).alias("series_id"),
            col("year").cast("int").alias("year"),
            trim(col("period")).alias("period"),
            col("value").cast("double").alias("value"),
            trim(col("footnote_codes")).alias("footnote_codes"),
            col("source_file"),
        ),
        ["series_id", "year", "period"],
    )


@dp.materialized_view(name="rearc.silver.all_data", comment="Cleansed BLS PR all historical data, deduplicated by latest source file")
@dp.expect_or_fail("series_id_not_null", "series_id IS NOT NULL")
@dp.expect_or_fail("year_not_null", "year IS NOT NULL")
@dp.expect_or_fail("period_not_null", "period IS NOT NULL")
def silver_all_data():
    return _dedup_latest(
        spark.read.table("rearc.bronze.all_data")
        .select(
            trim(col("series_id")).alias("series_id"),
            col("year").cast("int").alias("year"),
            trim(col("period")).alias("period"),
            col("value").cast("double").alias("value"),
            trim(col("footnote_codes")).alias("footnote_codes"),
            col("source_file"),
        ),
        ["series_id", "year", "period"],
    )


@dp.materialized_view(name="rearc.silver.class", comment="Cleansed BLS PR class lookup, deduplicated by latest source file")
@dp.expect_or_fail("class_code_not_null", "class_code IS NOT NULL")
def silver_class():
    return _dedup_latest(
        spark.read.table("rearc.bronze.class")
        .select(
            trim(col("class_code")).alias("class_code"),
            trim(col("class_text")).alias("class_text"),
            col("display_level").cast("int").alias("display_level"),
            trim(col("selectable")).alias("selectable"),
            col("sort_sequence").cast("int").alias("sort_sequence"),
            col("source_file"),
        ),
        ["class_code"],
    )


@dp.materialized_view(name="rearc.silver.measure", comment="Cleansed BLS PR measure lookup, deduplicated by latest source file")
@dp.expect_or_fail("measure_code_not_null", "measure_code IS NOT NULL")
def silver_measure():
    return _dedup_latest(
        spark.read.table("rearc.bronze.measure")
        .select(
            trim(col("measure_code")).alias("measure_code"),
            trim(col("measure_text")).alias("measure_text"),
            col("display_level").cast("int").alias("display_level"),
            trim(col("selectable")).alias("selectable"),
            col("sort_sequence").cast("int").alias("sort_sequence"),
            col("source_file"),
        ),
        ["measure_code"],
    )


@dp.materialized_view(name="rearc.silver.sector", comment="Cleansed BLS PR sector lookup, deduplicated by latest source file")
@dp.expect_or_fail("sector_code_not_null", "sector_code IS NOT NULL")
def silver_sector():
    return _dedup_latest(
        spark.read.table("rearc.bronze.sector")
        .select(
            trim(col("sector_code")).alias("sector_code"),
            trim(col("sector_name")).alias("sector_name"),
            col("display_level").cast("int").alias("display_level"),
            trim(col("selectable")).alias("selectable"),
            col("sort_sequence").cast("int").alias("sort_sequence"),
            col("source_file"),
        ),
        ["sector_code"],
    )


@dp.materialized_view(name="rearc.silver.duration", comment="Cleansed BLS PR duration lookup, deduplicated by latest source file")
@dp.expect_or_fail("duration_code_not_null", "duration_code IS NOT NULL")
def silver_duration():
    return _dedup_latest(
        spark.read.table("rearc.bronze.duration")
        .select(
            trim(col("duration_code")).alias("duration_code"),
            trim(col("duration_text")).alias("duration_text"),
            col("display_level").cast("int").alias("display_level"),
            trim(col("selectable")).alias("selectable"),
            col("sort_sequence").cast("int").alias("sort_sequence"),
            col("source_file"),
        ),
        ["duration_code"],
    )


@dp.materialized_view(name="rearc.silver.period", comment="Cleansed BLS PR period lookup, deduplicated by latest source file")
@dp.expect_or_fail("period_code_not_null", "period_code IS NOT NULL")
def silver_period():
    return _dedup_latest(
        spark.read.table("rearc.bronze.period")
        .select(
            trim(col("period_code")).alias("period_code"),
            trim(col("period_abbr")).alias("period_abbr"),
            trim(col("period_name")).alias("period_name"),
            col("source_file"),
        ),
        ["period_code"],
    )


@dp.materialized_view(name="rearc.silver.seasonal", comment="Cleansed BLS PR seasonal lookup, deduplicated by latest source file")
@dp.expect_or_fail("seasonal_code_not_null", "seasonal_code IS NOT NULL")
def silver_seasonal():
    return _dedup_latest(
        spark.read.table("rearc.bronze.seasonal")
        .select(
            trim(col("seasonal_code")).alias("seasonal_code"),
            trim(col("seasonal_text")).alias("seasonal_text"),
            col("source_file"),
        ),
        ["seasonal_code"],
    )


@dp.materialized_view(name="rearc.silver.footnote", comment="Cleansed BLS PR footnote lookup, deduplicated by latest source file")
@dp.expect_or_fail("footnote_code_not_null", "footnote_code IS NOT NULL")
def silver_footnote():
    return _dedup_latest(
        spark.read.table("rearc.bronze.footnote")
        .select(
            trim(col("footnote_code")).alias("footnote_code"),
            trim(col("footnote_text")).alias("footnote_text"),
            col("source_file"),
        ),
        ["footnote_code"],
    )
