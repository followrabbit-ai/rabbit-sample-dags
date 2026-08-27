# demo/ — recording aids for the Rabbit Pricing Optimizer walkthrough video

Not part of the DAG deployment. These drive the screen recording so it's
repeatable and can't leak secrets on camera.

The video is **one recording, two chapters**. This repo holds **Chapter 2
(Airflow / Composer)**; **Chapter 1 (dbt)** lives in
[`dbt-rabbit-samples`](https://github.com/followrabbit-ai/dbt-rabbit-samples)
under the same `docs/pricing-optimizer-demo-video` branch. `narration.md` and
`runbook.html` are the full-video docs and are kept identical in both repos.

| File | Purpose |
| --- | --- |
| `runbook.html` | Full production runbook — phases, shot lists for both chapters, gotchas. Open in a browser. |
| `narration.md` | Word-for-word voice-over for the whole video, with timings and cut points. |
| `lib.sh` | Shared shell helpers (`run`, `pause`, `note`, `clear_screen`, `guard_env`). |
| `00_env.example.sh` | Template for env vars. Copy to `00_env.sh` (gitignored) and fill in. |
| `chapter2_airflow.sh` | Paced demo driver for Chapter 2. Press Enter to advance each step. |

## Use

```bash
cp demo/00_env.example.sh demo/00_env.sh
# edit demo/00_env.sh with real values

source demo/00_env.sh
./demo/chapter2_airflow.sh
```

Each `run 'cmd'` prints the command, waits for you to press Enter, then executes
it. Each `clear_screen` is a natural cut point — record between them as separate
takes and stitch in the edit.

## Before recording Chapter 2

The Composer PyPI plugin install (`--update-pypi-packages-from-file=requirements-composer.txt`)
rebuilds the environment image and takes 15–25 minutes. Run it the day before —
`chapter2_airflow.sh` only *displays* that command, and Beat B cuts straight to
**Admin → Plugins** to show it registered.

## Secret safety

- Real `RABBIT_API_KEY` and `RESERVATION_ID` go **only** in `demo/00_env.sh`, which is gitignored.
- The scripts pass `$RABBIT_API_KEY` single-quoted, so the terminal shows the
  variable name, never the value. Still blur the Airflow `connections add` line
  in post as a backstop.
- `clear_screen` wipes scrollback so nothing earlier is scrollable on camera.
