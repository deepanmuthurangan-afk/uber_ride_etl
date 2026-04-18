from pyspark import pipelines as dp
from pyspark.sql.functions import *

start_date = '2025-01-01'
end_date = '2025-12-31'

@dp.materialized_view(
    name="transportation.silver.calendar",
    comment="Calendar dimension",
    table_properties={
        "quality": "transportation.silver.calendar",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def calendar():
    df_calendar = spark.sql(
        f"""
        SELECT explode(sequence(to_date('{start_date}'),to_date('{end_date}'),interval 1 day)) as date
        """
        )

    df_date = df_calendar.withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))

    df_split = (
        df_date.withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("day_of_month", dayofmonth(col("date")))
        .withColumn("day_of_week", date_format(col("date"), "EEEE"))
        .withColumn("day_of_week_abbr", date_format(col("date"), "EEE"))
        .withColumn("day_of_week_num", dayofweek(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
        .withColumn("month_year",concat(date_format(col("date"), "MMMM"), lit(" "), col("year")))
        .withColumn("quarter_year",concat(lit("Q"), col("quarter"), lit(" "), col("year")))
        .withColumn("week_of_year", weekofyear(col("date")))
        .withColumn("day_of_year", dayofyear(col("date")))
        .withColumn("is_weekend",when(col("day_of_week_num").isin([1, 7]), True).otherwise(False))
        .withColumn("is_weekday",when(col("day_of_week_num").isin([1, 7]), False).otherwise(True))
    )

    df_leaves = (
        df_split.withColumn(
            "holiday_name",
            when((col("month") == 1) & (col("day_of_month") == 26), lit("Republic Day"))
            .when((col("month") == 8) & (col("day_of_month") == 15),lit("Independence Day"))
            .otherwise(None)
        )
        .withColumn("is_holiday", when(col("holiday_name").isNotNull(), True).otherwise(False))
    )

    df_timestamp = df_leaves.withColumn("silver_processed_timestamp", current_timestamp())

    df_result = df_timestamp.select(
        "date",
        "date_key",
        "year",
        "month",
        "day_of_month",
        "day_of_week",
        "day_of_week_abbr",
        "month_name",
        "month_year",
        "quarter",
        "quarter_year",
        "week_of_year",
        "day_of_year",
        "is_weekday",
        "is_weekend",
        "is_holiday",
        "holiday_name",
        "silver_processed_timestamp"
    )

    return df_result