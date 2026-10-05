from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, explode, regexp_extract, to_timestamp, row_number
from pyspark.sql.window import Window


@dp.materialized_view(name="rearc.silver.population", comment="Cleansed DataUSA population data, exploded from nested JSON array")
@dp.expect_or_fail("nation_id_not_null", "nation_id IS NOT NULL")
@dp.expect_or_fail("year_not_null", "year IS NOT NULL")
@dp.expect_or_fail("population_positive", "population > 0")
def silver_population():
    return (
        spark.read.table("rearc.bronze.population")
        .select(col("source_file"), explode(col("data")).alias("record"))
        .select(
            trim(col("record")["Nation"]).alias("nation"),
            trim(col("record")["Nation ID"]).alias("nation_id"),
            col("record")["Population"].cast("double").cast("long").alias("population"),
            col("record")["Year"].cast("int").alias("year"),
            col("source_file"),
        )
        .withColumn(
            "_source_ts",
            to_timestamp(
                regexp_extract(col("source_file"), r"_([0-9]{8}_[0-9]{6})", 1),
                "yyyyMMdd_HHmmss",
            ),
        )
        .withColumn(
            "_rn",
            row_number().over(
                Window.partitionBy("nation_id", "year").orderBy(col("_source_ts").desc())
            ),
        )
        .filter(col("_rn") == 1)
        .drop("_rn", "_source_ts", "source_file")
    )
