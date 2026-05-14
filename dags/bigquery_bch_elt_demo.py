"""Sample BigQuery ELT DAG for Cloud Composer 3 (Bitcoin Cash transactions).

Demonstrates a three-task ELT pipeline against
``bigquery-public-data.crypto_bitcoin_cash.transactions``:

  1. ``stage_transactions``   - load a 30-day window into a managed staging table.
  2. ``aggregate_daily_tx``   - daily aggregates into a mart table.
  3. ``export_to_gcs``       - export the mart to GCS as Parquet.

Uses ``block_timestamp_month`` in predicates where possible to limit scanned
partitions (public table is large).

Configuration matches ``bigquery_elt_demo`` — set Airflow Variables:

  - ``gcp_project_id`` (required for runs)
  - ``bq_dataset``     (defaults to ``airflow_demo``)
  - ``gcs_bucket``     (required for runs)

Set them on Composer with::

    gcloud composer environments run <env> --location <loc> \\
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

STAGE_TABLE = f"{PROJECT}.{DATASET}.stg_bch_transactions"
MART_TABLE = f"{PROJECT}.{DATASET}.mart_daily_bch_transactions"

PUBLIC_TX = "`bigquery-public-data.crypto_bitcoin_cash.transactions`"

STAGE_SQL = f"""
DECLARE max_dt DATE DEFAULT (
  SELECT DATE(MAX(block_timestamp))
  FROM {PUBLIC_TX}
  WHERE block_timestamp_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 420 DAY)
);

CREATE OR REPLACE TABLE `{STAGE_TABLE}` AS
SELECT
  `hash`,
  size,
  virtual_size,
  version,
  block_number,
  block_hash,
  block_timestamp,
  input_count,
  output_count,
  input_value,
  output_value,
  is_coinbase,
  fee
FROM {PUBLIC_TX}
WHERE block_timestamp_month >= DATE_SUB(max_dt, INTERVAL 70 DAY)
  AND DATE(block_timestamp) BETWEEN DATE_SUB(max_dt, INTERVAL 30 DAY) AND max_dt
"""

AGGREGATE_SQL = f"""
CREATE OR REPLACE TABLE `{MART_TABLE}` AS
SELECT
  DATE(block_timestamp)     AS tx_date,
  COUNT(*)                  AS tx_count,
  SUM(input_value)          AS total_input_value,
  SUM(output_value)         AS total_output_value,
  AVG(output_count)         AS avg_output_count,
  COUNTIF(is_coinbase)      AS coinbase_tx_count
FROM `{STAGE_TABLE}`
GROUP BY tx_date
ORDER BY tx_date
"""


@dag(
    dag_id="bigquery_bch_elt_demo",
    description=(
        "Three-task BigQuery ELT against crypto_bitcoin_cash.transactions "
        "(public dataset)."
    ),
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "gcp_conn_id": GCP_CONN_ID,
    },
    tags=["bigquery", "composer3", "demo", "bitcoin-cash"],
)
def bigquery_bch_elt_demo() -> None:
    stage_transactions = BigQueryInsertJobOperator(
        task_id="stage_transactions",
        configuration={
            "query": {
                "query": STAGE_SQL,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
        deferrable=True,
    )

    aggregate_daily_tx = BigQueryInsertJobOperator(
        task_id="aggregate_daily_tx",
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
            f"gs://{BUCKET}/bch-transactions-extract/{{{{ ds }}}}/part-*.parquet",
        ],
        export_format="PARQUET",
        compression="SNAPPY",
        location=LOCATION,
        deferrable=True,
    )

    stage_transactions >> aggregate_daily_tx >> export_to_gcs


bigquery_bch_elt_demo()
