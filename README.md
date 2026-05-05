# rabbit-sample-dags

Sample Apache Airflow DAGs for [Cloud Composer 3](https://cloud.google.com/composer/docs/composer-3/composer-overview).

## DAGs

### `bigquery_elt_demo`

A daily ELT pipeline against the
[`bigquery-public-data.austin_bikeshare`](https://console.cloud.google.com/marketplace/product/city-of-austin/austin-bikeshare)
public dataset. Three BigQuery tasks chained in series:

| Task | Operator | What it does |
| --- | --- | --- |
| `stage_trips` | `BigQueryInsertJobOperator` | `CREATE OR REPLACE TABLE` of the last 30 days of `bikeshare_trips` into `stg_bikeshare_trips`. |
| `aggregate_daily_rides` | `BigQueryInsertJobOperator` | Aggregates the staging table into `mart_daily_rides` (rides, avg duration, unique bikes per day). |
| `export_to_gcs` | `BigQueryToGCSOperator` | Exports `mart_daily_rides` to `gs://<bucket>/exports/mart_daily_rides/<ds>/part-*.parquet`. |

All three operators run with `deferrable=True` to free worker slots while
BigQuery jobs run.

## Prerequisites

1. A **Cloud Composer 3** environment. See
   [Create environments](https://cloud.google.com/composer/docs/composer-3/create-environments).
2. A **BigQuery dataset** in the same project (e.g. `airflow_demo`):
   ```bash
   bq --location=US mk --dataset "$PROJECT_ID:airflow_demo"
   ```
3. A **GCS bucket** to receive the Parquet exports:
   ```bash
   gcloud storage buckets create "gs://$PROJECT_ID-airflow-demo-exports" \
       --location=US --uniform-bucket-level-access
   ```
4. The Composer environment's service account needs:
   - `roles/bigquery.jobUser` on the project,
   - `roles/bigquery.dataEditor` on the target dataset,
   - `roles/storage.objectAdmin` on the export bucket,
   - `roles/bigquery.dataViewer` on `bigquery-public-data` is granted by default.

   Grant the dataset and bucket roles with:
   ```bash
   SA="$(gcloud composer environments describe <env> \
       --location <loc> --format='value(config.nodeConfig.serviceAccount)')"

   bq add-iam-policy-binding \
       --member="serviceAccount:$SA" \
       --role="roles/bigquery.dataEditor" \
       "$PROJECT_ID:airflow_demo"

   gcloud projects add-iam-policy-binding "$PROJECT_ID" \
       --member="serviceAccount:$SA" \
       --role="roles/bigquery.jobUser"

   gcloud storage buckets add-iam-policy-binding \
       "gs://$PROJECT_ID-airflow-demo-exports" \
       --member="serviceAccount:$SA" \
       --role="roles/storage.objectAdmin"
   ```

## Configure Airflow Variables

The DAG reads three Airflow Variables: `gcp_project_id`, `bq_dataset`, and
`gcs_bucket`. The recommended way to set them is the GitHub Actions deploy
described below, which mirrors the matching GitHub repo Variables into the
Composer environment on every release. To set them by hand:

```bash
ENV=<your-composer-env>
LOC=<your-composer-region>

gcloud composer environments run "$ENV" --location "$LOC" \
    variables -- set gcp_project_id "$PROJECT_ID"

gcloud composer environments run "$ENV" --location "$LOC" \
    variables -- set bq_dataset airflow_demo

gcloud composer environments run "$ENV" --location "$LOC" \
    variables -- set gcs_bucket "$PROJECT_ID-airflow-demo-exports"
```

The default `google_cloud_default` connection that ships with Composer is used,
so no additional connection setup is required.

## Deploying with GitHub Actions (release-please)

[`.github/workflows/release.yml`](.github/workflows/release.yml) is the
recommended deploy path. It uses
[release-please](https://github.com/googleapis/release-please) to manage
versioning and triggers a Cloud Composer deploy on every release.

### How it works

1. Every push to `main` runs the `release-please` job, which opens or updates
   a "Release PR" based on [Conventional Commits](https://www.conventionalcommits.org/)
   (`feat:` -> minor bump, `fix:` -> patch, `feat!:` / `BREAKING CHANGE` -> major).
2. Merging the Release PR cuts a GitHub release + tag and bumps `version.txt`.
3. The release event gates the `deploy` job, which:
   - authenticates with the `GCP_SA_KEY` secret,
   - mirrors the GitHub repo Variables into Airflow Variables on the
     Composer environment (via `gcloud composer environments run ... variables -- set`),
   - imports every `dags/*.py` into the environment with
     `gcloud composer environments storage dags import`.
4. The workflow can also be triggered manually (`workflow_dispatch`) to
   redeploy `main` without cutting a release.

### Required GitHub Secrets

Settings -> Secrets and variables -> Actions -> Secrets:

| Name | Value |
| --- | --- |
| `GCP_SA_KEY` | JSON key for the deploy service account (see roles below). |

### Required GitHub Variables

Settings -> Secrets and variables -> Actions -> Variables:

| Name | Example |
| --- | --- |
| `GCP_PROJECT_ID` | `my-gcp-project` |
| `COMPOSER_ENV_NAME` | `composer-demo` |
| `COMPOSER_LOCATION` | `us-central1` |
| `BQ_DATASET` | `airflow_demo` |
| `GCS_BUCKET` | `my-gcp-project-airflow-demo-exports` |

### Deploy service account roles

The SA whose JSON you put in `GCP_SA_KEY` needs:

- `roles/composer.user` on the project (to call
  `gcloud composer environments run` and `... storage dags import`).
- `roles/iam.serviceAccountUser` on the Composer environment's runtime
  service account (required by `composer environments run`).

Grant them with:

```bash
DEPLOY_SA=<deploy-sa>@$PROJECT_ID.iam.gserviceaccount.com
RUNTIME_SA="$(gcloud composer environments describe "$ENV" \
    --location "$LOC" --format='value(config.nodeConfig.serviceAccount)')"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$DEPLOY_SA" \
    --role="roles/composer.user"

gcloud iam service-accounts add-iam-policy-binding "$RUNTIME_SA" \
    --member="serviceAccount:$DEPLOY_SA" \
    --role="roles/iam.serviceAccountUser"
```

The workflow targets a GitHub Environment named `production`, which lets you
add manual approval / branch protection. Remove the `environment: production`
line in [`.github/workflows/release.yml`](.github/workflows/release.yml) if
you don't want that gate.

## Manual deploy (ad-hoc)

For one-off deploys without going through release-please:

```bash
gcloud composer environments storage dags import \
    --environment "$ENV" --location "$LOC" \
    --source dags/bigquery_elt_demo.py
```

It will appear in the Airflow UI within ~1 minute. Trigger it manually from
the UI or via:

```bash
gcloud composer environments run "$ENV" --location "$LOC" \
    dags trigger -- bigquery_elt_demo
```

## Verify

```bash
bq query --use_legacy_sql=false \
    "SELECT * FROM \`$PROJECT_ID.airflow_demo.mart_daily_rides\` ORDER BY ride_date DESC LIMIT 10"

gcloud storage ls "gs://$PROJECT_ID-airflow-demo-exports/exports/mart_daily_rides/"
```

## Local development

Composer 3 ships Airflow 2.x with `apache-airflow-providers-google` preinstalled,
so `requirements.txt` here is **only for local IDE/lint**:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -c "from airflow.models import DagBag; \
    db = DagBag('dags', include_examples=False); \
    assert not db.import_errors, db.import_errors; print('DAGs OK')"
```
