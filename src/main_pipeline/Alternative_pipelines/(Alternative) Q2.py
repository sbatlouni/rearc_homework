from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum as _sum, substring, row_number, lit
from pyspark.sql.window import Window


@dp.materialized_view(
    name="rearc.gold.productivity_costs_index",
    comment="Major Sector Productivity and Costs Index - highest annual total per series with dimension lookups",
)
def gold_productivity_costs_index():
    yearly_totals = (
        spark.read.table("rearc.silver.all_data")
        .groupBy("series_id", "year")
        .agg(_sum("value").alias("total"))
    )

    w = Window.partitionBy("series_id").orderBy(col("total").desc())

    ranked = (
        yearly_totals
        .withColumn("season_code", substring(col("series_id"), 3, 1))
        .withColumn("sector_code", substring(col("series_id"), 4, 4))
        .withColumn("class_code", substring(col("series_id"), 8, 1))
        .withColumn("measure_code", substring(col("series_id"), 9, 2))
        .withColumn("duration_code", substring(col("series_id"), 11, 1))
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    return (
        ranked.alias("r")
        .join(
            spark.read.table("rearc.silver.seasonal").alias("s"),
            col("r.season_code") == col("s.seasonal_code"),
            "left",
        )
        .join(
            spark.read.table("rearc.silver.sector").alias("sec"),
            col("r.sector_code") == col("sec.sector_code"),
            "left",
        )
        .join(
            spark.read.table("rearc.silver.class").alias("c"),
            col("r.class_code") == col("c.class_code"),
            "left",
        )
        .join(
            spark.read.table("rearc.silver.measure").alias("m"),
            col("r.measure_code") == col("m.measure_code"),
            "left",
        )
        .join(
            spark.read.table("rearc.silver.duration").alias("d"),
            col("r.duration_code") == col("d.duration_code"),
            "left",
        )
        .select(
            col("r.series_id"),
            lit("PR").alias("survey_code"),
            col("r.season_code"),
            col("r.sector_code"),
            col("r.class_code"),
            col("r.measure_code"),
            col("r.duration_code"),
            lit("Major Sector Productivity and Costs Index").alias("survey_abbreviation"),
            col("s.seasonal_text"),
            col("sec.sector_name"),
            col("c.class_text"),
            col("m.measure_text"),
            col("d.duration_text"),
            col("r.year"),
            col("r.total"),
        )
    )
