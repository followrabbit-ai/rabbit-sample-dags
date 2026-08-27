# Voice-over script — Rabbit Pricing Optimizer walkthrough

Read at ~15% slower than conversational. `‖` = pause / cut point. Timings are
targets for the *finished* cut (waits removed), total ~15:00.

---

## COLD OPEN — 0:00–0:55  *(record last; to camera or over the two READMEs)*

> BigQuery gives you two ways to pay. ‖ On-demand — you pay per byte scanned.
> Or slot-based — you reserve capacity up front. ‖ Which one is cheaper changes
> from query to query, and choosing per job by hand isn't realistic.
>
> The Rabbit Pricing Model Optimizer makes that choice automatically, at query
> time. ‖ In the next fifteen minutes I'll turn it on two ways — first in a dbt
> project, then in an Airflow pipeline on Cloud Composer. ‖ Same optimizer. And
> in both cases, the transformation code doesn't change at all.

**On screen:** `dbt-rabbit-samples` and `rabbit-sample-dags` READMEs side by side, or a title card. Cut to terminal.

---

## CHAPTER 1 — dbt — 0:55–7:00

### Beat A — the integration · ~1:00
> Here's the entire integration on the dbt side. ‖ In the profile, the adapter
> type goes from `bigquery` to `rabbitbigquery` — that's a drop-in replacement,
> a superset of the standard adapter. ‖ Then three new keys: the Rabbit API key,
> a default pricing mode, and reservation IDs. ‖ Same OAuth connection as before.
>
> And the models? ‖ Untouched. This is plain dbt SQL — staging model here, no
> Rabbit anything. Same for every model, every test.

### Beat B — install & connect · ~0:50
> `requirements.txt` pins the Rabbit adapter in place of `dbt-bigquery`. ‖
> Install, point dbt at the profile, and `dbt debug` — connection's good.

### Beat C — baseline, optimization OFF · ~1:10
> First, on purpose, with **no** reservation ID set. ‖ Run a build.
> *(wait — cut)* ‖ It succeeds — sixteen models and tests, all green. ‖ And in
> the log: "Rabbit optimization disabled — missing required reservation IDs." ‖
> That's the key safety property. No reservation, and it quietly falls back to
> normal BigQuery. Nothing breaks.

### Beat D — flip it ON · ~1:15
> Now I set one environment variable — the reservation ID. ‖ Exact same
> `dbt build` command. *(wait — cut)* ‖ And now the log looks different — these
> `RabbitBigQuery` lines are the adapter routing each job through Rabbit's
> pricing API before it hits BigQuery. ‖ That's it. That's the whole change:
> off, then on, with one ID.

### Beat E — the payoff · ~1:00
> Over in the Rabbit dashboard — the Dynamic Pricing view — here are the jobs
> from that build. ‖ For each one, the pricing decision Rabbit made, and the
> savings. *(there's a short lag; these are from a few minutes ago)*

### Beat F — productionizing · ~0:35
> Quickly, for production: same adapter in a container, the API key pulled from
> Secret Manager, deployed as a Cloud Run Job by release-please. ‖ Your CI/CD
> will look different — the point is the optimizer travels with the adapter.
> Nothing extra to run.

---

## CHAPTER 2 — Airflow / Composer — 7:00–13:30

### Beat A — two pins, zero DAG changes · ~1:00
> On the Airflow side it's a plugin. ‖ Two packages in the Composer requirements
> file — the client library and the plugin itself. ‖ Airflow finds the plugin
> through its entry point; at startup it wraps `BigQueryHook`, so every BigQuery
> job any DAG submits goes through Rabbit first. ‖ And the DAG — three BigQuery
> tasks, no Rabbit imports anywhere.

### Beat B — the install · ~0:55
> This is the install command. ‖ I ran it yesterday, because it rebuilds the
> environment image and takes fifteen to twenty-five minutes. ‖ Proof it
> registered: Admin, Plugins — "Rabbit BQ Optimizer", right there.

### Beat C — connection + config · ~1:10
> The API key lives only in an Airflow connection — `rabbit_api`. Not in the
> repo, not in GitHub Actions. Operators set it per environment. ‖ And a config
> variable with the reservation ID list. ‖ Same rule as dbt: empty list, and the
> plugin skips optimization — you'd see a warning in the task log instead of a
> result.

### Beat D — trigger a DAG · ~0:45
> Trigger the bikeshare pipeline from the CLI. ‖ In the UI: stage, aggregate,
> export — *(wait — cut)* — all green.

### Beat E — proof in the task log · ~1:05
> Open the `stage_trips` task log. ‖ There: "Rabbit BQ Optimizer — received
> optimization result." ‖ Same story as the dbt chapter, just in the task log
> instead of `dbt.log`.

### Beat F — payoff + productionizing · ~1:00
> Back to the same Rabbit dashboard — now with jobs that came from Airflow,
> next to the dbt ones. One optimizer, both pipelines. ‖ And for production:
> release-please cuts a version, the deploy job syncs DAGs to GCS and only
> reinstalls PyPI packages when the requirements file changed, auth via Workload
> Identity Federation — no service-account keys.

---

## CLOSE — 13:30–14:10  *(record last)*

> Two integrations, one idea. ‖ A drop-in adapter for dbt, a drop-in plugin for
> Airflow. One reservation ID to turn it on. And every BigQuery job priced the
> cheaper way, automatically. ‖ Links to both sample repos and the setup docs
> are below. ‖ If you want help wiring this into your own project, reach out to
> your Rabbit contact.

**On screen:** recap card — `dbt: profiles → type: rabbitbigquery` · `Airflow: requirements-composer.txt + rabbit_api connection` · `both: reservation_ids`.

---

### Honest-result note
With the sample data volumes, Rabbit may pick on-demand every time — that's a
legitimate outcome; say so. To show a slot-based decision on camera, use the
heavier `bitcoin_cash` pipeline (`--select bitcoin_cash` / `bigquery_bch_elt_demo`).
