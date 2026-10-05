from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.materialized_view(
    name="rearc.gold.series_population",
    comment="Productivity data for series PRS30006032, Q01 period, joined with US population by year",
)
def gold_series_population():
    return (
        spark.read.table("rearc.silver.all_data").alias("d")
        .join(
            spark.read.table("rearc.silver.population").alias("p"),
            col("d.year") == col("p.year"),
            "left",
        )
        .filter((col("d.series_id") == "PRS30006032") & (col("d.period") == "Q01"))
        .select(
            col("d.series_id"),
            col("d.year"),
            col("d.period"),
            col("p.population"),
            col("d.value"),
        )
    )
