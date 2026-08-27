#!/usr/bin/env bash
# Chapter 2 - Airflow / Composer.  Paced demo driver: press Enter to advance.
#
# Run from the rabbit-sample-dags repo root:
#   1. source demo/00_env.sh   (needs ENV, LOC, RESERVATION_ID, RABBIT_API_KEY)
#   2. The plugin PyPI install MUST ALREADY BE DONE - it rebuilds the Composer
#      image (15-25 min). This script only *shows* that command, never runs it.
#   3. ./demo/chapter2_airflow.sh
#
# Beats E and F happen in the Airflow UI / Rabbit dashboard, not the terminal -
# the script just cues you.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=demo/lib.sh
source demo/lib.sh

[ -n "${DEMO_ENV_SOURCED:-}" ] || { echo "Run:  source demo/00_env.sh   first"; exit 1; }
guard_env RABBIT_API_KEY RESERVATION_ID ENV LOC

clear_screen
# ==========================================================================
# BEAT A - two PyPI pins, zero DAG changes
# ==========================================================================
say "Client library + the Airflow plugin. Composer installs both."
run 'grep -vE "^\s*#|^\s*$" requirements-composer.txt'
say "And the DAG has no idea Rabbit exists."
run 'grep -nE "rabbit|Rabbit|optimizer" dags/bigquery_elt_demo.py || echo "(no Rabbit references in the DAG)"'
pause
clear_screen

# ==========================================================================
# BEAT B - the install command (SHOWN, NOT RUN - pre-baked earlier)
# ==========================================================================
say "This is the install. I ran it yesterday; it rebuilds the environment image."
note "PRE-RUN before recording - shown here, not executed (15-25 min)"
printf 'demo$ gcloud composer environments update "$ENV" --location "$LOC" \\\n'
printf '          --update-pypi-packages-from-file=requirements-composer.txt\n'
pause
say "Proof it registered: Airflow UI -> Admin -> Plugins -> 'Rabbit BQ Optimizer'."
note "switch to the Airflow UI for this beat"
pause
clear_screen

# ==========================================================================
# BEAT C - connection + config variable
# ==========================================================================
say "API key lives only in the Airflow connection - not the repo, not GitHub."
run 'gcloud composer environments run "$ENV" --location "$LOC" connections -- add rabbit_api --conn-type generic --conn-password "$RABBIT_API_KEY" --conn-extra '\''{"api_base_url":"https://api.followrabbit.ai/bq-job-optimizer"}'\'''
say "The config variable carries the reservation id list. Empty list = skip."
run 'gcloud composer environments run "$ENV" --location "$LOC" variables -- set rabbit_bq_optimizer_config "{\"default_pricing_mode\":\"on_demand\",\"reservation_ids\":[\"$RESERVATION_ID\"]}"'
pause
clear_screen

# ==========================================================================
# BEAT D - trigger a DAG
# ==========================================================================
say "Trigger the bikeshare pipeline. Then watch it in the UI."
run 'gcloud composer environments run "$ENV" --location "$LOC" dags trigger -- bigquery_elt_demo'
pause
clear_screen

# ==========================================================================
# BEAT E - proof in the task log (Airflow UI)
# ==========================================================================
say "Open the stage_trips task log. Look for: Rabbit BQ Optimizer: Received optimization result:"
note "this beat is in the Airflow UI"
pause
say "Then the Rabbit dashboard - jobs from Airflow now show up alongside the dbt ones."
note "switch to the Rabbit Dynamic Pricing dashboard"
echo
echo "== Chapter 2 script complete =="
