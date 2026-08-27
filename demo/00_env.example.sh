# Copy to demo/00_env.sh, fill in real values, then before recording:
#
#   source demo/00_env.sh
#
# demo/00_env.sh is gitignored. Never commit real keys or reservation IDs.
#
# This copy covers Chapter 2 (Airflow / Composer). Chapter 1 (dbt) lives in
# the dbt-rabbit-samples repo and has its own demo/00_env.example.sh.

export DEMO_ENV_SOURCED=1

# --- GCP -----------------------------------------------------------------------
export GCP_PROJECT="your-project-id"

# BigQuery reservation, form:  project:location.reservation-name
export RESERVATION_ID="your-project:US.your-reservation"

# --- Rabbit ------------------------------------------------------------------
# Real key ONLY here (this file is gitignored). Never echo/cat it on camera.
export RABBIT_API_KEY="rabbit_xxxxxxxxxxxxxxxxxxxx"

# --- Composer 3 environment --------------------------------------------------
export ENV="your-composer-env"
export LOC="us-central1"
