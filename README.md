# rabbit-sample-dags

Sample Apache Airflow DAGs for Google Cloud's
[Managed Service for Apache Airflow](https://docs.cloud.google.com/composer/docs/composer-3/composer-overview)
(Composer 3) — Google renamed Cloud Composer to Managed Service for Apache
Airflow, but the `gcloud composer` CLI, resource names, and "Composer 3"
version name are unchanged, and this doc uses them as-is.

## Reference implementations

Two PRs in this repo are the historical anchor for how the Rabbit BQ Optimizer
plugin integration evolved:

- **[PR #8](https://github.com/followrabbit-ai/rabbit-sample-dags/pull/8)** —
  the initial copied-plugin integration (Composer PyPI client, GCS `plugins/`
  upload, and documentation).
- **[PR #22](https://github.com/followrabbit-ai/rabbit-sample-dags/pull/22)** —
  the migration from a copied `plugins/rabbit_bq_optimizer_plugin.py` to the
  PyPI plugin package, including updated Composer dependencies, deploy
  workflow, and removal of the legacy GCS plugin file.

Both PRs show the code changes for their respective implementations, but
**current** deploy behavior lives in
[`.github/workflows/release.yml`](.github/workflows/release.yml) on `main`
and in [Part 1](#part-1--implement-the-rabbit-bq-optimizer-plugin) below —
follow those rather than replaying either PR alone.

## Part 1 — Implement the Rabbit BQ Optimizer plugin

This repo installs the [Rabbit BigQuery Job Optimizer Airflow plugin](https://github.com/followrabbit-ai/bq-job-optimizer-airflow-plugin)
via PyPI as [`rabbit-bq-optimizer-airflow-plugin`](https://pypi.org/project/rabbit-bq-optimizer-airflow-plugin/).
Airflow discovers the plugin through its entry point — no file copy into the
environment's `plugins/` folder is required. BigQuery jobs submitted through
Airflow can be routed through Rabbit's optimizer API before
`BigQueryHook.insert_job` runs. The DAG code in the sample DAGs
(`bigquery_elt_demo`, `bigquery_bch_elt_demo`) does not change; at Airflow
startup the plugin alters `BigQueryHook` so job configurations are passed
through Rabbit before they are sent to BigQuery.

### Prerequisites

- A **Managed Service for Apache Airflow (Composer 3)** environment. See
  [Create environments](https://cloud.google.com/composer/docs/composer-3/create-environments).
- A Rabbit account and API key (for the `rabbit_api` connection below).

### 1. Install the plugin

Composer installs both packages from
[`requirements-composer.txt`](requirements-composer.txt):

- **`rabbit-bq-job-optimizer`** — Python client library (`rabbit_bq_job_optimizer`).
- **`rabbit-bq-optimizer-airflow-plugin`** — Airflow plugin that subclasses
  `AirflowPlugin`, loads the `rabbit_api` connection and
  `rabbit_bq_optimizer_config` variable, and applies the hook patch. Registers
  automatically via Airflow's plugin entry point when the environment image is
  rebuilt.

If you previously deployed the copied `rabbit_bq_optimizer_plugin.py` to the
environment's GCS `plugins/` prefix, the release workflow removes that legacy
file on deploy so it does not duplicate the PyPI-registered plugin.

### 2. Configure the connection and secrets

The Rabbit API key is **not** committed to this repository, stored in GitHub
Actions variables, or read by the sample DAG. Operators create the
**`rabbit_api`** Airflow connection manually (CLI or UI) in each environment.
The deploy workflow does not create or update Airflow connections.

Use the same `ENV` / `LOC` pattern as
[Configure Airflow Variables](#configure-airflow-variables). Replace the API
key and reservation IDs with values from your Rabbit and GCP setup.

**Connection `rabbit_api`** (API key in the password field; optional base URL
in extras — omit `api_base_url` to use Rabbit's default):

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

### 3. Deploy it to your environment

For a one-off manual install:

```bash
gcloud composer environments update "$ENV" --location "$LOC" \
    --update-pypi-packages-from-file=requirements-composer.txt
```

This rebuilds the environment image and typically takes 15–25 minutes. For
an automated, repeatable path (recommended), use the GitHub Actions release
workflow — see
[Deploying with GitHub Actions](#deploying-with-github-actions-release-please)
in Part 2.

### 4. Verify the plugin

1. Airflow UI → **Admin → Plugins** lists **Rabbit BQ Optimizer** (or the plugin
   name shown there).
2. Run `bigquery_elt_demo` or `bigquery_bch_elt_demo` and open a BigQuery task log. When optimization runs,
   look for `Rabbit BQ Optimizer: Received optimization result:`.
3. Confirm BigQuery jobs still succeed end-to-end.

## Part 2 — Everything else in this repo

### Sample DAGs

#### `bigquery_elt_demo`

An ELT pipeline against the
[`bigquery-public-data.austin_bikeshare`](https://console.cloud.google.com/marketplace/product/city-of-austin/austin-bikeshare)
public dataset (**no Airflow schedule**—trigger manually or via API/CLI). Three BigQuery tasks chained in series:

| Task | Operator | What it does |
| --- | --- | --- |
| `stage_trips` | `BigQueryInsertJobOperator` | `CREATE OR REPLACE TABLE` of the last 30 days of `bikeshare_trips` into `stg_bikeshare_trips`. |
| `aggregate_daily_rides` | `BigQueryInsertJobOperator` | Aggregates the staging table into `mart_daily_rides` (rides, avg duration, unique bikes per day). |
| `export_to_gcs` | `BigQueryToGCSOperator` | Exports `mart_daily_rides` to `gs://<bucket>/bikeshare-extract/<ds>/part-*.parquet`. |

All three operators run with `deferrable=True` to free worker slots while
BigQuery jobs run.

#### `bigquery_bch_elt_demo`

An ELT pipeline against
[`bigquery-public-data.crypto_bitcoin_cash.transactions`](https://console.cloud.google.com/marketplace/product/google-cloud-public-datasets/crypto-bitcoin-cash)
(Bitcoin Cash blockchain transactions in BigQuery; **no Airflow schedule**—trigger manually or via API/CLI). Same three-task shape as
`bigquery_elt_demo`, using the same Airflow Variables (`gcp_project_id`,
`bq_dataset`, `gcs_bucket`):

| Task | Operator | What it does |
| --- | --- | --- |
| `stage_transactions` | `BigQueryInsertJobOperator` | `CREATE OR REPLACE TABLE` of the last 30 days of transactions (with `block_timestamp_month` predicates to prune partitions) into `stg_bch_transactions`. |
| `aggregate_daily_tx` | `BigQueryInsertJobOperator` | Aggregates into `mart_daily_bch_transactions` (daily tx counts, input/output value sums, coinbase counts). |
| `export_to_gcs` | `BigQueryToGCSOperator` | Exports the mart to `gs://<bucket>/bch-transactions-extract/<ds>/part-*.parquet`. |

### Demo environment prerequisites

1. A **Managed Service for Apache Airflow (Composer 3)** environment. See
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
4. The environment's service account needs:
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

### Configure Airflow Variables

The DAG reads three Airflow Variables — `gcp_project_id`, `bq_dataset`, and
`gcs_bucket`. They're environment-specific state owned by Airflow, so set them
once per environment with `gcloud` or the Airflow UI (Admin → Variables):

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
If you enable the Rabbit BQ Optimizer plugin (see
[Part 1](#part-1--implement-the-rabbit-bq-optimizer-plugin)), add the separate
`rabbit_api` connection there (the plugin does not reuse
`google_cloud_default`).

### Deploying with GitHub Actions (release-please)

[`.github/workflows/release.yml`](.github/workflows/release.yml) is the
recommended deploy path. It uses
[release-please](https://github.com/googleapis/release-please) to manage
versioning and triggers a deploy on every release.

#### How it works

1. Every push to `main` runs the `release-please` job, which opens or updates
   a "Release PR" based on [Conventional Commits](https://www.conventionalcommits.org/)
   (`feat:` -> minor bump, `fix:` -> patch, `feat!:` / `BREAKING CHANGE` -> major).
2. Merging the Release PR cuts a GitHub release + tag and bumps `version.txt`.
3. The release event gates the `deploy` job, which:
   - authenticates to GCP via [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation)
     (no long-lived Service Account JSON),
   - resolves the environment's `config.dagGcsPrefix` and uploads `dags/*` with
     `gcloud storage cp --recursive` (avoids a duplicated `dags/dags/` path
     under the bucket), and removes any legacy copied
     `rabbit_bq_optimizer_plugin.py` from the environment's GCS `plugins/`
     prefix. This step runs **before** any PyPI update so a failed
     `composer environments update` (for example missing
     `composer.environments.update` on the deploy service account) does not
     block DAG files from reaching GCS.
   - when [`requirements-composer.txt`](requirements-composer.txt) changed since
     the previous release tag, runs
     `gcloud composer environments update ... --update-pypi-packages-from-file=requirements-composer.txt`
     to install Composer PyPI deps (including the Rabbit optimizer plugin;
     otherwise skips this slow step).

   Airflow Variables (`gcp_project_id`, `bq_dataset`, `gcs_bucket`) are owned
   by Airflow itself — set them once per environment (see
   [Configure Airflow Variables](#configure-airflow-variables)) rather than
   re-mirroring on every deploy.
4. The workflow can also be triggered manually (`workflow_dispatch`) to
   redeploy `main` without cutting a release. When you run it from the Actions
   tab, enable **force_pypi_sync** if you need `gcloud composer environments
   update --update-pypi-packages-from-file` even though `requirements-composer.txt`
   did not change since the last release tag (for example to fix an
   environment image that never picked up PyPI deps). Leave it off for a
   faster run that only refreshes DAGs in GCS.

#### Required GitHub Secrets

None for GCP deploy: authentication uses Workload Identity Federation, so no
Service Account JSON or other long-lived GCP credential needs to live in repo
Secrets.

The Rabbit optimizer API key is also **not** a GitHub Secret for this repo; it
lives only in the Airflow connection `rabbit_api` (see
[Part 1](#part-1--implement-the-rabbit-bq-optimizer-plugin)).

#### Required GitHub Variables

Settings -> Secrets and variables -> Actions -> Variables:

| Name | Example (replace with your values) |
| --- | --- |
| `GCP_PROJECT_ID` | `YOUR_GCP_PROJECT_ID` |
| `COMPOSER_ENV_NAME` | `YOUR_COMPOSER_ENVIRONMENT_NAME` |
| `COMPOSER_LOCATION` | `YOUR_REGION` (e.g. `us-central1`) |
| `GCP_WIF_PROVIDER` | `projects/YOUR_WIF_HOST_PROJECT_NUMBER/locations/global/workloadIdentityPools/YOUR_POOL_ID/providers/YOUR_PROVIDER_ID` |
| `GCP_COMPOSER_SA` | `YOUR_DEPLOY_SA@YOUR_GCP_PROJECT_ID.iam.gserviceaccount.com` |

#### Workload Identity Federation setup

There is **no WIF pool creation inside this repository** — your platform team
provisions the Workload Identity Pool, OIDC provider, and IAM bindings (often
in a separate infrastructure repository). A typical pattern:

- The pool's `attribute_condition` restricts which GitHub organizations or
  repositories may exchange an OIDC token for a Google access token.
- A **per-repository** principal set (for example scoped to
  `YOUR_GITHUB_ORG/YOUR_REPO_NAME`) is bound to **`GCP_COMPOSER_SA`** so only
  this repo's GitHub Actions workflows can impersonate the deploy service
  account.

The deploy service account usually needs at least **`roles/composer.user`**
(to resolve the environment and `dagGcsPrefix`) plus
**`roles/storage.objectAdmin`** on the environment bucket (to upload
DAGs). If you use **`gcloud composer environments update`** in CI
to install PyPI packages, that account also needs permission to **update** the
environment (for example a role that includes **`composer.environments.update`**
— see [Composer access control](https://cloud.google.com/composer/docs/how-to/access-control)).

Adding another GitHub repo to the same pattern is an infrastructure change
(WIF provider / IAM bindings), not a change to this DAG repo alone.

The workflow targets a GitHub Environment named `production`, which lets you
add manual approval / branch protection. Remove the `environment: production`
line in [`.github/workflows/release.yml`](.github/workflows/release.yml) if
you don't want that gate.

### Manual deploy (ad-hoc)

For one-off deploys without going through release-please:

```bash
gcloud composer environments storage dags import \
    --environment "$ENV" --location "$LOC" \
    --source dags/bigquery_elt_demo.py
# Or import the Bitcoin Cash demo DAG:
#   --source dags/bigquery_bch_elt_demo.py
# Or upload the whole folder (matches CI): use ``gcloud storage cp`` to the
# environment's ``dags/`` prefix — see the deploy workflow in this repo.
```

It will appear in the Airflow UI within ~1 minute. Trigger it manually from
the UI or via:

```bash
gcloud composer environments run "$ENV" --location "$LOC" \
    dags trigger -- bigquery_elt_demo
```

### Verify

```bash
bq query --use_legacy_sql=false \
    "SELECT * FROM \`$PROJECT_ID.airflow_demo.mart_daily_rides\` ORDER BY ride_date DESC LIMIT 10"

gcloud storage ls "gs://$GCS_BUCKET/bikeshare-extract/"
```

### Continuous validation

[`.github/workflows/validate.yml`](.github/workflows/validate.yml) runs on
every pull request against `main` (and on `workflow_dispatch`) with two jobs:

| Job | What it does |
| --- | --- |
| `ruff` | `ruff check` / `ruff format --check` on `dags/` |
| `parse-dags` | Installs `requirements.txt` and parses every DAG via Airflow's `DagBag`, failing the build on any import error |

This is independent of `release.yml` — no GCP credentials needed, so it
runs on PRs from forks as well. `parse-dags` uses
[`astral-sh/setup-uv`](https://github.com/astral-sh/setup-uv) with its
built-in CI cache, so the Airflow install typically runs in seconds rather
than the ~90s a cold `pip install` takes.

### Local development

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
