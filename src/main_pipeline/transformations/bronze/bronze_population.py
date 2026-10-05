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
@dp.expect_all_or_fail({
    # Top-level fields not in _schema are rescued, so a change in the API's response shape stops the update.
    "no_rescued_data": "_rescued_data IS NULL",
    "has_data": "size(data) > 0",
    # Each record in `data` is a map, so check the API's own column list instead.
    "expected_columns": "array_sort(columns) = array_sort(array('Nation ID', 'Nation', 'Year', 'Population'))",
})
def bronze_population():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(_schema)
        .load(_BASE)
        .withColumn("source_file", col("_metadata.file_name"))
    )
