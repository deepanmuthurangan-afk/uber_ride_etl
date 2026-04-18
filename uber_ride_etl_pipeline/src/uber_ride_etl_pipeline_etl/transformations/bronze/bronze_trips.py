from pyspark import pipelines as dp
from pyspark.sql.functions import *

SOURCE_PATH = "/Volumes/uber/default/trips"

@dp.table(
    name="transportation.bronze.trips",
    comment="Streaming ingestion of raw orders data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def orders_bronze():
    df_trips_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(SOURCE_PATH)
    )

    df_column_rename = df_trips_stream.withColumnRenamed("distance_travelled(km)","distance_travelled_km")

    df_result = df_column_rename.withColumn("file_name", col("_metadata.file_path")).withColumn("ingest_datetime", current_timestamp())

    return df_result
