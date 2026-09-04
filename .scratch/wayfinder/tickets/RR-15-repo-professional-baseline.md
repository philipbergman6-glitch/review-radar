---
id: RR-15
title: Repo professional baseline — CI, secrets, packaging
type: task
status: closed
assignee: philipbergman (claimed 2026-09-04)
blocked-by: []
blocks: [RR-14]
---

## Question

Nothing to decide — this is the execution the map's **Professional bar** note carries on
purpose (see `map.md` Notes): it must be true before the next push, and every implementation
session after this map inherits it. Four findings from the 2026-09-04 review, all
`[observed]`:

1. **No CI.** No `.github/`, no pre-commit, no Makefile. Add a GitHub Actions workflow that
   runs `ruff check` and `pytest` on every push and pull request, on Python 3.11, with uv.
   The Spark-dependent tests are none today (`tests/` is two pure-function files, 93 lines),
   so the job needs no JDK — record that assumption in the workflow comment so the first
   Spark integration test knows to add one.
2. **Secrets hardcoded.** `minioadmin`/`minioadmin` and `bigdata`/`bigdata` are literal in
   `docker-compose.yml` and are the *defaults* in `src/common/config.py`, so a missing `.env`
   silently works with known credentials. Make compose read them from `.env` (`${VAR:?}`
   syntax so a missing value fails compose loudly), and make `config.py` use `_req()` for
   `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `PG_PASSWORD`. `.env.example` keeps placeholder values.
   **Redact every secret value in any output.**
3. **`sys.path.insert` in every entrypoint.** `bronze.py:31`, `producer.py:30`,
   `healthcheck.py:16`, `prove_exactly_once.py:38`. Make the project installable
   (`[tool.uv] package = true`, or `[build-system]` + `uv sync` editable) and delete the
   hacks. `pytest` already sets `pythonpath = ["."]`.
4. **Single entrypoint for the stack.** A `Makefile` or `justfile` with `up`, `health`,
   `produce`, `bronze`, `eos`, `test`, `lint` — the README run section then points at it.

Resolution records: the workflow file path, the CI run URL that went green, the three
`_req()` keys, and the `make` targets. Nothing else changes; this ticket touches no pipeline
logic.

Blocks only the closing reconciliation, because README's run section must describe the
final shape.

## Answer

Resolved 2026-09-04. Landed in commit `Raise the repo to a professional baseline: CI,
secrets from .env, packaging, Makefile` on `main`; the first CI run went green.

- **Workflow:** `.github/workflows/ci.yml` — `ruff check .` then `pytest -q`, Python 3.11,
  `astral-sh/setup-uv@v6`, `uv sync --locked`. No JDK; the header comment records the
  assumption and tells the first Spark integration test to add `actions/setup-java@v4`
  (temurin 17) plus a service block for the compose stack. Test step injects dummy
  `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `PG_PASSWORD` because config now hard-fails without them.
- **Green run:** https://github.com/philipbergman6-glitch/review-radar/actions/runs/33883548702
  — `lint-and-test` in 1m5s `[observed]`. Only annotation: the Node 20 deprecation notice on
  `checkout@v4` / `setup-uv@v6`; harmless, bump when v5/v7 land.
- **Required keys (`_req()`):** `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `PG_PASSWORD`. The literal
  `minioadmin` / `bigdata` defaults are gone from `src/common/config.py`.
  `docker-compose.yml` reads `MINIO_ROOT_USER/PASSWORD`, `POSTGRES_USER/PASSWORD` from the
  same `.env` keys with `${VAR:?}`; verified `[observed]` that `docker compose config` with
  an empty env-file fails with `required variable S3_ACCESS_KEY is missing a value`. The
  bucket-init container and the Postgres healthcheck also stopped hardcoding them.
  `.env.example` unchanged — it already holds placeholders — and the live `bd-*` containers
  were not recreated (`docker compose up --dry-run` touched only the one-shot `minio-init`).
  `make health` after the change: `All components healthy.` `[observed]`
- **Packaging:** `[build-system]` hatchling, `[tool.hatch.build.targets.wheel] packages =
  ["src"]`, `[tool.uv] package = true`; `uv sync` installs `review-radar==0.1.0` editable.
  All 11 `sys.path.insert` sites deleted (8 entrypoints + `ROOT` + the two subprocess
  snippets in `prove_exactly_once.py`). Verified `import src.common.config` from `cwd=/`.
- **Make targets:** `up`, `down`, `health`, `produce`, `produce-sample`, `bronze`, `verify`,
  `eos`, `test`, `lint`, `check` (= lint + test, what CI runs). Bare `make` prints the list.
  README Setup, Running-the-pipeline, endpoints table and Layout now point at them; the
  underlying commands stay documented beside each target for their flags.
- **Incidental:** ruff 0.16.5 (locked) had 31 findings, all cleared, so CI was green on the
  first push rather than red. No pipeline logic changed; 17 unit tests still pass.

Unblocks `Reconcile the three docs` (`RR-14`) on this edge; it still waits on its other
blockers. Fog item *Integration test strategy* now has its CI skeleton; it still waits on
silver for something to test.
