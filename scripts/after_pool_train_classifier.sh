#!/usr/bin/env bash
# Ticket 08's three runs, chained behind ticket 07's pool-labelling run.
#
# The pool takes hours and the classifier cannot be trained until it lands, so this runs the
# labelling to completion and then runs the classifier sequence once, in order, stopping at the
# first failure. It refuses to go on after anything but a clean finish: a partial pool trained
# on silently would produce a classifier whose numbers mean nothing, and `make pool-census` is
# what says the frame is complete rather than merely large.
#
# The labelling is resumable -- every row already committed is an idempotency-key cache hit --
# so a restart after a crash costs only what it had not reached. Run 20e03c76 died at ~475/3000
# on 2026-09-11 00:28 when its local Spark executor lost the driver, which is why this script
# runs the labelling itself rather than waiting on a process.
#
# Everything after it is idempotent-or-refusing: `--train` will not overwrite a frozen spec,
# `--fit-thresholds` will not refit one, and the audit set is never touched here.
#
# Run:  nohup caffeinate -is ./scripts/after_pool_train_classifier.sh > logs/classifier-chain.log 2>&1 &
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

say() { echo "[chain $(date '+%Y-%m-%d %H:%M:%S')] $*"; }

latest_pool_status() {
  docker exec bd-postgres psql -U bigdata -d catalog -tA -c \
    "SELECT status FROM pipeline_runs WHERE job_name = 'theme_labels_llm'
       AND params->>'budget_line' = 'training_pool' ORDER BY started_at DESC LIMIT 1" \
    2>/dev/null | tr -d '[:space:]'
}

say "make label-pool   # resumes from the rows already committed"
make label-pool
status=$(latest_pool_status)
if [ "$status" != "success" ]; then
  say "the pool run ended '$status', not 'success' -- stopping. Nothing was trained."
  exit 1
fi
say "the pool run succeeded"

set -e
say "make pool-census   # refuses a partially labelled frame"
make pool-census
say "make classifier-train"
make classifier-train
say "make classifier-thresholds"
make classifier-thresholds
say "make classifier-score SAMPLE_NAME=development"
make classifier-score SAMPLE_NAME=development
say "make classifier-table SAMPLE_NAME=development"
make classifier-table SAMPLE_NAME=development
say "done: the classifier is trained, its cuts are frozen, and development is scored."
say "the audit set was not opened -- that is ticket 09."
