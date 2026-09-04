---
id: RR-15
title: Repo professional baseline — CI, secrets, packaging
type: task
status: open
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
