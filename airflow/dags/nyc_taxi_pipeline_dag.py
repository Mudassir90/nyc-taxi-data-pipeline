"""
NYC Taxi Pipeline DAG
Orchestrates: AWS Glue transform -> Snowflake load -> dbt run/test
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "mudassir",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    description="End-to-end NYC Taxi pipeline: Glue -> Snowflake -> dbt",
    schedule=None,  # manual trigger for now; can set to a cron string later
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nyc_taxi"],
) as dag:

    # --- Task 1: Trigger the AWS Glue job ---
    trigger_glue_job = GlueJobOperator(
        task_id="trigger_glue_job",
        job_name="nyc_taxi_glue_transform",  
        aws_conn_id="aws_default",
        region_name="ap-south-1",
        wait_for_completion=False, 
    )

    # --- Task 2: Wait for the Glue job to finish ---
    wait_for_glue = GlueJobSensor(
        task_id="wait_for_glue",
        job_name="nyc_taxi_glue_transform",
        run_id="{{ ti.xcom_pull(task_ids='trigger_glue_job') }}",
        aws_conn_id="aws_default",
        poke_interval=30,   
        timeout=1800,       
    )

    # --- Task 3: Load the fresh processed data into Snowflake ---
    load_to_snowflake = SnowflakeOperator(
        task_id="load_to_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="""
            TRUNCATE TABLE RAW.YELLOW_TAXI_2026_H1;

            COPY INTO RAW.YELLOW_TAXI_2026_H1
            FROM @s3_processed_stage
            FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

            UPDATE RAW.YELLOW_TAXI_2026_H1
            SET month = LPAD(MONTH(pickup_date), 2, '0')
            WHERE month IS NULL;
        """,
    )

    # --- Task 4: Run dbt via the official dbt-labs Docker image ---
    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="ghcr.io/dbt-labs/dbt-snowflake:1.8.latest",
        command="run --project-dir /usr/app --profiles-dir /root/.dbt",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,  
                              
        mounts=[
            Mount(
                # IMPORTANT: because we're using docker.sock (docker-outside-of-docker),
                # this new container is created by the HOST's Docker engine, not by the
                # Airflow container. So this path must be the actual Windows host path,
                # not the path as seen inside the Airflow container.
                source="C:\\nyc_end_to_end_pipeline\\nyc_taxi_dbt",
                target="/usr/app",
                type="bind",
            ),
            Mount(
                # profiles.yml lives separately from the dbt project, in the user's
                # .dbt folder (this is where dbt debug showed it earlier)
                source="C:\\Users\\USER\\.dbt",
                target="/root/.dbt",
                type="bind",
            ),
        ],
        auto_remove="success",
    )

    trigger_glue_job >> wait_for_glue >> load_to_snowflake >> dbt_run
