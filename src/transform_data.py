from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_PATH = "data/raw/yellow_tripdata_2026-01.parquet"
OUTPUT_PATH = "data/processed/yellow_tripdata_2026-01_clean.parquet"

def get_spark():
    return(
        SparkSession.builder
        .appName("NYC_Taxi_Transform")
        .master("local[*]")
        .getOrCreate()
    )

def transform(spark):
    df = spark.read.parquet(RAW_PATH)
    print(f"Raw Row Count: {df.count()}")
    df = df.filter((F.col("fare_amount") >= 0) & (F.col("trip_distance") > 0) & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) & (F.col("VendorID").isin([1,2,6,7])))
    print(f"Row Count After Core Filters: {df.count()}")
    df = df.withColumn("is_incomplete_metadata",F.col("RatecodeID").isNull())
    df = df.withColumn("payment_type_clean", F.when(F.col("payment_type").isin([1,2,3,4,5,6]),F.col("payment_type")).otherwise(F.lit(0)))
    df = df.withColumn("ratecode_clean", F.when(F.col("RatecodeID").isin([1,2,3,4,5,6]), F.col("RatecodeID")).otherwise(F.lit(99)))
    df = df.filter((F.col("passenger_count").isNull()) | (F.col("passenger_count") > 0))
    df = df.withColumn("trip_duration_minutes", (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60) \
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")
    ).withColumn("pickup_date",F.to_date("tpep_pickup_datetime")) \
    .withColumn("pickup_day_of_week", F.date_format("tpep_pickup_datetime", "EEEE")) \
    .withColumn("total_revenue", F.col("fare_amount") + F.coalesce(F.col("tip_amount"), F.lit(0)) + F.coalesce(F.col("tolls_amount"), F.lit(0)))
    df = df.filter((F.col("trip_duration_minutes") > 0) & (F.col("trip_duration_minutes") < 300))
    print(f"Row Count After Duration Filter: {df.count()}")

    return df

def write_output(df):
    df.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"Written processed data to: {OUTPUT_PATH}")

spark = get_spark()
result_df = transform(spark)
result_df.show(5)
write_output(result_df)
spark.stop()