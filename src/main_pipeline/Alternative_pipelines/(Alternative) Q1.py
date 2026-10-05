from pyspark import pipelines as dp
from pyspark.sql.functions import col, avg, stddev


@dp.materialized_view(
    name="rearc.gold.population_stats_2013_2018",
    comment="Mean and standard deviation of annual US population, 2013-2018 inclusive",
)
def gold_population_stats_2013_2018():
    return (
        spark.read.table("rearc.silver.population")
        .filter((col("year").between(2013, 2018)) & (col("nation_id") == "01000US"))
        .agg(
            avg("population").alias("mean_population"),
            stddev("population").alias("stddev_population"),
        )
    )
