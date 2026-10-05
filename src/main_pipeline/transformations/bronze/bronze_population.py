from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

_BASE = "/Volumes/rearc/raw/volume/datausa"

_schema = StructType([
    StructField("annotations", MapType(StringType(), StringType())),
    StructField("columns", ArrayType(StringType())),
    StructField("data", ArrayType(MapType(StringType(), StringType()))),
    StructField("page", MapType(StringType(), StringType())),
])


@dp.table(name="rearc.bronze.population", comment="Raw DataUSA population data ingested via Auto Loader")
def bronze_population():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .schema(_schema)
        .load(_BASE)
        .withColumn("source_file", col("_metadata.file_name"))
    )
