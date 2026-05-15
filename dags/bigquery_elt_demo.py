"""Sample BigQuery ELT DAG for Cloud Composer 3.

Demonstrates a three-task ELT pipeline against the
``bigquery-public-data.austin_bikeshare.bikeshare_trips`` public dataset:

  1. ``stage_trips``           - load a 30-day window into a managed staging table.
  2. ``aggregate_daily_rides`` - aggregate the staging table into a daily mart.
  3. ``export_to_gcs``         - export the mart table to GCS as Parquet.

Configuration is read from Airflow Variables so the DAG parses out-of-the-box
but won't run against the wrong project by accident:

  - ``gcp_project_id`` (required for runs)
  - ``bq_dataset``     (defaults to ``airflow_demo``)
  - ``gcs_bucket``     (required for runs)

Set them on Composer with::

    gcloud composer environments run <env> --location <loc> \
        variables -- set gcp_project_id <project>
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)

GCP_CONN_ID = "google_cloud_default"
LOCATION = "US"

PROJECT = "{{ var.value.get('gcp_project_id', 'your-project-id') }}"
DATASET = "{{ var.value.get('bq_dataset', 'airflow_demo') }}"
BUCKET = "{{ var.value.get('gcs_bucket', 'your-bucket') }}"

STAGE_TABLE = f"{PROJECT}.{DATASET}.stg_bikeshare_trips"
MART_TABLE = f"{PROJECT}.{DATASET}.mart_daily_rides"

STAGE_SQL = f"""
DECLARE max_dt DATE DEFAULT (
  SELECT DATE(MAX(start_time))
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
);

CREATE OR REPLACE TABLE `{STAGE_TABLE}` AS
SELECT
  trip_id,
  subscriber_type,
  bike_id,
  start_time,
  duration_minutes,
  start_station_id,
  start_station_name,
  end_station_id,
  end_station_name
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
WHERE DATE(start_time) BETWEEN DATE_SUB(max_dt, INTERVAL 30 DAY) AND max_dt
"""

AGGREGATE_SQL = f"""
CREATE OR REPLACE TABLE `{MART_TABLE}` AS
SELECT
  DATE(start_time)         AS ride_date,
  COUNT(*)                 AS rides,
  AVG(duration_minutes)    AS avg_duration_minutes,
  COUNT(DISTINCT bike_id)  AS unique_bikes
FROM `{STAGE_TABLE}`
GROUP BY ride_date
ORDER BY ride_date
"""


@dag(
    dag_id="bigquery_elt_demo",
    description="Three-task BigQuery ELT against the austin_bikeshare public dataset.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "gcp_conn_id": GCP_CONN_ID,
    },
    tags=["bigquery", "composer3", "demo"],
)
def bigquery_elt_demo() -> None:
    stage_trips = BigQueryInsertJobOperator(
        task_id="stage_trips",
        configuration={
            "query": {
                "query": STAGE_SQL,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
        deferrable=True,
    )

    aggregate_daily_rides = BigQueryInsertJobOperator(
        task_id="aggregate_daily_rides",
        configuration={
            "query": {
                "query": AGGREGATE_SQL,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
        deferrable=True,
    )

    export_to_gcs = BigQueryToGCSOperator(
        task_id="export_to_gcs",
        source_project_dataset_table=MART_TABLE,
        destination_cloud_storage_uris=[
            f"gs://{BUCKET}/bikeshare-extract/{{{{ ds }}}}/part-*.parquet",
        ],
        export_format="PARQUET",
        compression="SNAPPY",
        location=LOCATION,
        deferrable=True,
    )

    stage_trips >> aggregate_daily_rides >> export_to_gcs


bigquery_elt_demo()
