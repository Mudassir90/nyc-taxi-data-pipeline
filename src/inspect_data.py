import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

FILE_PATH = "data/raw/yellow_tripdata_2026-01.parquet"

def inspect_schema(file_path):
    print("\n" + "-" * 60)
    print("Schema (columns + data type)")
    print("\n" + "-"*60)
    parquet_file = pq.ParquetFile(file_path)
    print(parquet_file.schema)

    print("\n" + "-"*60)
    metadata = parquet_file.metadata
    print(f"Number Of Rows :  {metadata.num_rows}")
    print(f"Number Of Columns : {metadata.num_columns}")

    print("\n" + "-"*60)
    file_size_mb = Path(file_path).stat().st_size / (1024 * 1024)
    print(f"File Size: {file_size_mb:.2f} MB")

    print("\n" + "-"*60)
    df = pd.read_parquet(file_path)
    print("\nFirst 5 Rows:" )
    print(df.head())

    print("\n" + "-"*60)
    print("Null Counts Per Column ")
    print(df.isnull().sum())

    print("\n" + "-"*60)
    print("Duplicated Rows: ",df.duplicated().sum())

    print("\n" + "-"*60)
    print("Basic Stats: ")
    print(df.describe())

    print("\n" + "-"*60)
    print("Unique Values -->")
    print("Vendor IDs: ",df["VendorID"].unique())
    print("Payment Type: ",df["payment_type"].unique())
    print("Rate Code: ",df["RatecodeID"].unique())
    print("Store and Forward Flags: ",df["store_and_fwd_flag"].unique())

    print("\n" + "-"*60)
    if "fare_amount" in df.columns:
        print(f"Negative Fare Amount: {(df["fare_amount"] < 0).sum()}")
    if "trip_distance" in df.columns:
        print(f"Negative Trip Distance: {(df["trip_distance"] < 0).sum()}")
    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
        Bad_orders = (df["tpep_dropoff_datetime"] < df["tpep_pickup_datetime"]).sum()
        print(f"Dropoff Before Pickup Rows: {Bad_orders}")
inspect_schema(FILE_PATH)
