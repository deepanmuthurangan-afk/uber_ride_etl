from pyspark import pipelines as dp
from pyspark.sql.functions import *

SOURCE_PATH = "/Volumes/uber/default/city"

@dp.materialized_view(
    name="transportation.bronze.city",
    comment="City csv Raw Data",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():
    df_city_bronze = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("mergeSchema", "true")
        .option("columnNameOfCorruptRecord","_corrupt_record")
        .load(SOURCE_PATH)
    )

    df_result = df_city_bronze.withColumn("file_name", col("_metadata.file_path")).withColumn("ingest_datetime", current_timestamp())
    
    return df_result
