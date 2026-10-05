from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, explode


@dp.materialized_view(name="rearc.silver.population", comment="Cleansed DataUSA population data, exploded from nested JSON array")
@dp.expect_or_fail("nation_id_not_null", "nation_id IS NOT NULL")
@dp.expect_or_fail("year_not_null", "year IS NOT NULL")
def silver_population():
    return (
        spark.read.table("rearc.bronze.population")
        .select(explode(col("data")).alias("record"))
        .select(
            trim(col("record")["Nation"]).alias("nation"),
            trim(col("record")["Nation ID"]).alias("nation_id"),
            col("record")["Population"].cast("double").cast("long").alias("population"),
            col("record")["Year"].cast("int").alias("year"),
        )
    )
