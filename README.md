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
| `export_to_gcs` | `BigQueryToGCSOperator` | Exports `mart_daily_rides` to `gs://<bucket>/bikeshare-extract/<ds>/part-*.parquet`. |

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

The DAG reads three Airflow Variables — `gcp_project_id`, `bq_dataset`, and
`gcs_bucket`. They're environment-specific state owned by Airflow, so set them
once per Composer environment with `gcloud` or the Airflow UI (Admin →
Variables):

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

The sample DAG uses the default `google_cloud_default` connection for BigQuery.
If you enable the Rabbit BQ Optimizer plugin below, add the separate
`rabbit_api` connection there (the plugin does not reuse
`google_cloud_default`).

## Rabbit BQ Optimizer plugin (C1)

This repo vendors the [Rabbit BigQuery Job Optimizer Airflow plugin](https://github.com/followrabbit-ai/bq-job-optimizer-airflow-plugin)
so BigQuery jobs submitted through Airflow can be routed through Rabbit’s
optimizer API before `BigQueryHook.insert_job` runs. The DAG code in
`bigquery_elt_demo` does not change; the plugin monkey-patches
`BigQueryHook` at Airflow startup.

### Why both PyPI and `plugins/`?

- **`rabbit-bq-job-optimizer` (PyPI)** — Python client library (`rabbit_bq_job_optimizer`).
  Installing it on the Composer environment makes `import rabbit_bq_job_optimizer`
  succeed in the worker image.
- **`plugins/rabbit_bq_optimizer_plugin.py`** — Airflow plugin shim: subclasses
  `AirflowPlugin`, loads the `rabbit_api` connection and
  `rabbit_bq_optimizer_config` variable, and applies the hook patch. Composer
  loads plugins from the environment bucket’s `plugins/` prefix (synced by
  the deploy workflow), not from the site-packages layout of arbitrary wheels.

### Secrets (Rabbit API key)

The Rabbit API key is **not** committed to this repository, stored in GitHub
Actions variables, or read by the sample DAG. Operators create the
**`rabbit_api`** Airflow connection manually (CLI or UI) in each Composer
environment. The deploy workflow does not create or update Airflow connections.

### One-time Airflow Connection and Variable

Use the same `ENV` / `LOC` pattern as [Configure Airflow Variables](#configure-airflow-variables).
Replace the API key and reservation IDs with values from your Rabbit and GCP
setup.

**Connection `rabbit_api`** (API key in the password field; optional base URL
in extras — omit `api_base_url` to use Rabbit’s default):

```bash
gcloud composer environments run "$ENV" --location "$LOC" \
    connections -- add rabbit_api \
    --conn-type generic \
    --conn-password "$RABBIT_API_KEY" \
    --conn-extra '{"api_base_url": "https://api.followrabbit.ai/bq-job-optimizer"}'
```

**Variable `rabbit_bq_optimizer_config`** — JSON with
`default_pricing_mode` (`on_demand` or `slot_based`) and **`reservation_ids`**
as a non-empty list of BigQuery reservation IDs in the form
`project:region.reservation-name`. The upstream plugin **skips** optimization
when `reservation_ids` is empty (jobs run with the original configuration and
you will see a warning in task logs instead of
`Rabbit BQ Optimizer: Received optimization result:`).

```bash
gcloud composer environments run "$ENV" --location "$LOC" \
    variables -- set rabbit_bq_optimizer_config \
    '{"default_pricing_mode":"on_demand","reservation_ids":["YOUR_PROJECT:US.YOUR_RESERVATION"]}'
```

### Verify the plugin

1. Airflow UI → **Admin → Plugins** lists **Rabbit BQ Optimizer** (or the plugin
   name shown there).
2. Run `bigquery_elt_demo` and open a BigQuery task log. When optimization runs,
   look for `Rabbit BQ Optimizer: Received optimization result:`.
3. Confirm BigQuery jobs still succeed end-to-end.

Composer PyPI dependencies for the demo live in
[`requirements-composer.txt`](requirements-composer.txt). The release workflow
applies them with `gcloud composer environments update
--update-pypi-packages-from-file` **only when that file changes** (the update
rebuilds the environment image and typically takes 15–25 minutes).

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
   - authenticates to GCP via [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation)
     (no long-lived Service Account JSON),
   - when [`requirements-composer.txt`](requirements-composer.txt) changed
     since the previous release tag, runs
     `gcloud composer environments update ... --update-pypi-packages-from-file=requirements-composer.txt`
     to install Composer PyPI deps (otherwise skips this slow step),
   - resolves the environment’s `config.dagGcsPrefix` and uploads `dags/*` and
     `plugins/*` with `gcloud storage cp --recursive` (avoids a duplicated
     `dags/dags/` path under the bucket).

   Airflow Variables (`gcp_project_id`, `bq_dataset`, `gcs_bucket`) are owned
   by Airflow itself — set them once per environment (see
   [Configure Airflow Variables](#configure-airflow-variables)) rather than
   re-mirroring on every deploy.
4. The workflow can also be triggered manually (`workflow_dispatch`) to
   redeploy `main` without cutting a release.

### Required GitHub Secrets

None for GCP deploy: authentication uses Workload Identity Federation, so no
Service Account JSON or other long-lived GCP credential needs to live in repo
Secrets.

The Rabbit optimizer API key is also **not** a GitHub Secret for this repo; it
lives only in the Composer Airflow connection `rabbit_api` (see
[Rabbit BQ Optimizer plugin (C1)](#rabbit-bq-optimizer-plugin-c1)).

### Required GitHub Variables

Settings -> Secrets and variables -> Actions -> Variables:

| Name | Example |
| --- | --- |
| `GCP_PROJECT_ID` | `rbt-sandbox-stewart` |
| `COMPOSER_ENV_NAME` | `rabbit-airflow-demo` |
| `COMPOSER_LOCATION` | `us-central1` |
| `GCP_WIF_PROVIDER` | `projects/270391591458/locations/global/workloadIdentityPools/github-actions/providers/github` |
| `GCP_COMPOSER_SA` | `composer-sa@rbt-sandbox-stewart.iam.gserviceaccount.com` |

### Workload Identity Federation setup

There is **no per-repo WIF setup** to do. The Workload Identity Pool
(`github-actions`) and OIDC provider (`github`) are centrally managed in the
[`gcp-foundation`](https://github.com/followrabbit-ai/gcp-foundation)
infrastructure repo (under `org_core/`) and shared across all
`followrabbit-ai/*` repos. The pool's `attribute_condition` restricts token
exchange to GitHub repos owned by `followrabbit-ai`, and a per-repo
`principalSet://...attribute.repository/followrabbit-ai/rabbit-sample-dags`
binding on `composer-sa` ensures only this repo can impersonate the SA.

`composer-sa` only needs `roles/composer.user` (to discover the environment)
plus `roles/storage.objectAdmin` on the Composer DAGs bucket (to upload DAG
files) — both managed in `gcp-foundation`. Adding a new sample-DAG repo to
this pattern is just a one-line change in that infrastructure repo.

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

gcloud storage ls "gs://$GCS_BUCKET/bikeshare-extract/"
```

## Continuous validation

[`.github/workflows/validate.yml`](.github/workflows/validate.yml) runs on
every pull request against `main` (and on `workflow_dispatch`) with two jobs:

| Job | What it does |
| --- | --- |
| `ruff` | `ruff check` / `ruff format --check` on `dags/` and `plugins/` |
| `parse-dags` | Installs `requirements.txt` and parses every DAG via Airflow's `DagBag`, failing the build on any import error |

This is independent of `release.yml` — no GCP credentials needed, so it
runs on PRs from forks as well. `parse-dags` uses
[`astral-sh/setup-uv`](https://github.com/astral-sh/setup-uv) with its
built-in CI cache, so the Airflow install typically runs in seconds rather
than the ~90s a cold `pip install` takes.

## Local development

Composer 3 ships Airflow 2.x with `apache-airflow-providers-google` preinstalled,
so `requirements.txt` here is **only for local IDE/lint**:

```bash
# Install uv once: https://docs.astral.sh/uv/getting-started/installation/
uv venv --python 3.11   # use Python 3.11 to match Composer 3
source .venv/bin/activate
uv pip install -r requirements.txt
ruff check dags plugins && ruff format --check dags plugins
python -c "from airflow.models import DagBag; \
    db = DagBag('dags', include_examples=False); \
    assert not db.import_errors, db.import_errors; print('DAGs OK')"
```
