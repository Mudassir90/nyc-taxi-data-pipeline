-- USE DATABASE NYC_TAXI_DB;
-- USE SCHEMA RAW;
-- USE WAREHOUSE NYC_TAXI_WH;

-- -- 1. Create the external stage pointing to S3 (via the storage integration)
-- CREATE STAGE IF NOT EXISTS s3_processed_stage
--     URL = 's3://nyc-taxi-001/processed/'
--     STORAGE_INTEGRATION = s3_taxi_integration
--     FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE);

-- -- 2. Verify Snowflake can see the files (sanity check before loading)
-- LIST @s3_processed_stage;

-- -- 3. Create a new table for the full 6-month dataset
-- -- (separate from the January-only table, so we don't mix them up)
-- CREATE TABLE IF NOT EXISTS RAW.YELLOW_TAXI_2026_H1 (
--     VendorID                INTEGER,
--     tpep_pickup_datetime     TIMESTAMP_NTZ,
--     tpep_dropoff_datetime    TIMESTAMP_NTZ,
--     passenger_count           INTEGER,
--     trip_distance              FLOAT,
--     RatecodeID                  INTEGER,
--     store_and_fwd_flag            STRING,
--     PULocationID                   INTEGER,
--     DOLocationID                    INTEGER,
--     payment_type                     INTEGER,
--     fare_amount                       FLOAT,
--     extra                              FLOAT,
--     mta_tax                            FLOAT,
--     tip_amount                          FLOAT,
--     tolls_amount                         FLOAT,
--     improvement_surcharge                 FLOAT,
--     total_amount                           FLOAT,
--     congestion_surcharge                    FLOAT,
--     Airport_fee                              FLOAT,
--     cbd_congestion_fee                        FLOAT,
--     is_incomplete_metadata                     BOOLEAN,
--     payment_type_clean                          INTEGER,
--     ratecode_clean                               INTEGER,
--     trip_duration_minutes                         FLOAT,
--     pickup_hour                                    INTEGER,
--     pickup_date                                     DATE,
--     pickup_day_of_week                               STRING,
--     total_revenue                                     FLOAT,
--     month                                              STRING
-- );

-- -- 4. Load all 6 months in one COPY INTO
-- COPY INTO RAW.YELLOW_TAXI_2026_H1
-- FROM @s3_processed_stage
-- FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
-- Force = TRUE
-- PATTERN = '.*part-.*[.]parquet';

-- -- 5. Verify
-- SELECT month, COUNT(*) AS row_count
-- FROM RAW.YELLOW_TAXI_2026_H1
-- GROUP BY month
-- ORDER BY month;



USE DATABASE NYC_TAXI_DB;
USE SCHEMA RAW;

-- 1. Clear the duplicated/broken data
TRUNCATE TABLE RAW.YELLOW_TAXI_2026_H1;

-- 2. Reload ONCE, cleanly (no FORCE this time — table is empty, no need)
COPY INTO RAW.YELLOW_TAXI_2026_H1
FROM @s3_processed_stage
FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- 3. Fix the month column — derive it from pickup_date instead of relying
-- on Spark's folder-based partition column (which COPY INTO can't see)
UPDATE RAW.YELLOW_TAXI_2026_H1
SET month = LPAD(MONTH(pickup_date), 2, '0')
WHERE month IS NULL;

-- 4. Verify — should now show 6 months with correct row counts, total ~21.6M
SELECT month, COUNT(*) AS row_count
FROM RAW.YELLOW_TAXI_2026_H1
GROUP BY month
ORDER BY month;