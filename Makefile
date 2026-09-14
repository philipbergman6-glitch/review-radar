# Single entrypoint for the stack. `make` with no target prints this list.
# Every Python target goes through ./run.sh, which pins JAVA_HOME to JDK 17.

.DEFAULT_GOAL := help
RUN := ./run.sh python
CATEGORY ?= All_Beauty
SAMPLE ?= data/sample/$(CATEGORY).sample.jsonl
TOPIC ?= reviews.raw
SAMPLE_NAME ?= development
# The frozen prompt (conf/theme-label-spec.json frozen_prompt, ticket 06). Keep these in step:
# a stale default here would label the training pool with a superseded teacher.
PROMPT ?= label_v5
# P7's frozen answering prompt (conf/rag-answer-spec.json frozen_prompt, ticket 12). Only the
# development target reads it -- the evaluation run refuses any prompt but the frozen one.
RAG_PROMPT ?= rag_v5

.PHONY: help up up-ui down health pg-migrate catalogue produce produce-sample sort-replay sort-replay-sample stream-produce stream-produce-sample bronze bronze-sample silver silver-sample gate-silver reproduce-silver gold gold-sample reproduce-gold index-reviews index-reviews-sample index-product-month index-product-month-sample kibana-import pool-search judge-search eval-search gate-search embed embed-sample index-reviews-vectors index-reviews-vectors-sample pool-embeddings export-judgements eval-embeddings ann-recall gate-embeddings theme-samples theme-samples-sample freeze-theme-terms theme-frames discover-phrases blind-export label-themes label-pool pool-census import-reference score-themes select-prompt gate-themes adjudicate-export adjudicate-import agreement-draw agreement-export agreement-labeller agreement-check agreement-import agreement-score sentiment-check star-baseline-fit star-baseline-score audit-once classifier-train classifier-thresholds classifier-score classifier-table diagnose-failures discovery-failures propose-taxonomy score-taxonomy rag-dev-questions rag-dev-answers rag-answers gate-rag eval-table gate-lineage gate-lineage-publication reconcile-run verify eos test lint check

help:  ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

up:  ## start Kafka, MinIO, Elasticsearch, Postgres (reads credentials from .env)
	docker compose up -d
	docker compose ps

up-ui:  ## also start Kibana (compose profile ui) on :5601
	docker compose --profile ui up -d

down:  ## stop the stack, keep the volumes
	docker compose down

health:  ## one line per component, non-zero exit if any is unreachable
	$(RUN) scripts/healthcheck.py

pg-migrate:  ## apply conf/postgres-migrations to the running Postgres (checksummed, idempotent)
	$(RUN) scripts/pg_migrate.py

catalogue:  ## load meta_$(CATEGORY).jsonl into Postgres products under a catalogue_load_id
	$(RUN) -m src.catalogue.load_products --category $(CATEGORY) --scope full

produce:  ## replay the full $(CATEGORY) review file into Kafka
	$(RUN) -m src.ingest.producer --category $(CATEGORY)

produce-sample:  ## replay the committed 10k-review sample into the $(TOPIC).sample topic
	$(RUN) -m src.ingest.producer --source $(SAMPLE) --topic $(TOPIC).sample

sort-replay:  ## order the raw file by event time once -> $(CATEGORY).sorted.jsonl, digest in the ledger
	$(RUN) -m src.ingest.sort_replay --category $(CATEGORY) --scope full

sort-replay-sample:  ## same, over the committed 10k-review sample
	$(RUN) -m src.ingest.sort_replay --source $(SAMPLE) --scope sample

stream-produce:  ## paced replay of the sorted file into reviews.stream (control run, no injection)
	$(RUN) -m src.ingest.stream_producer --category $(CATEGORY) --scope full

stream-produce-sample:  ## same, over the sorted sample -> reviews.stream.sample
	$(RUN) -m src.ingest.stream_producer --source $(SAMPLE:.jsonl=.sorted.jsonl) --scope sample

bronze:  ## drain the topic into the bronze Iceberg table, one trigger
	$(RUN) -m src.spark.bronze --trigger once --max-per-trigger 150000

bronze-sample:  ## same, from the $(TOPIC).sample topic into bronze.reviews_raw_sample
	$(RUN) -m src.spark.bronze --topic $(TOPIC).sample --trigger once --max-per-trigger 150000

silver:  ## bronze snapshot -> silver.reviews/rejects/review_collisions, prints SILVER_GATE
	$(RUN) -m src.spark.silver --scope full --verify-rerun

silver-sample:  ## same, over the sample topic's bronze table
	$(RUN) -m src.spark.silver --scope sample --verify-rerun

gate-silver:  ## re-derive SILVER_GATE from the ledger row and the pinned tables
	$(RUN) scripts/gate_silver.py --scope full --verify-rerun

reproduce-silver:  ## independent pandas reproduction over raw JSONL vs the pinned silver tables
	$(RUN) scripts/reproduce_silver.py --scope full

gold:  ## silver snapshot -> gold.product_month/evaluation_points/decline_episodes, prints GOLD_GATE
	$(RUN) -m src.spark.gold --scope full --verify-rerun

gold-sample:  ## same, over the sample silver tables
	$(RUN) -m src.spark.gold --scope sample --verify-rerun

reproduce-gold:  ## independent re-derivation of the three gold tables from the pinned silver snapshot
	$(RUN) scripts/reproduce_gold.py --scope full

index-reviews:  ## silver snapshot -> ES review search index generation -> alias `reviews`
	$(RUN) -m src.serving.index_reviews --scope full --prune

index-reviews-sample:  ## same, over the sample silver table -> alias `reviews_sample`
	$(RUN) -m src.serving.index_reviews --scope sample --prune

index-product-month:  ## gold snapshot -> ES product_month generation -> alias `product_month`
	$(RUN) -m src.serving.index_product_month --scope full --prune

index-product-month-sample:  ## same, over the sample gold table
	$(RUN) -m src.serving.index_product_month --scope sample --prune

kibana-import:  ## import conf/kibana/*.ndjson into Kibana (idempotent), prints KIBANA_DASHBOARD
	$(RUN) scripts/kibana_import.py

pool-search:  ## pool top-10 of every Search system for the 20 frozen queries -> eval/search/pool.jsonl
	$(RUN) scripts/judge_search.py --pool --round search

judge-search:  ## interactive judging of the unjudged pool (retriever hidden, order randomised)
	$(RUN) scripts/judge_search.py --judge --judge-name philip

eval-search:  ## P@5 / MRR@10 per system and stratum on complete judgements; freezes the analyzer default
	$(RUN) scripts/eval_search.py

gate-search:  ## re-derive every Search constituent from ES, the ledger and the judgements; prints SEARCH_GATE
	$(RUN) scripts/gate_search.py

embed:  ## silver snapshot -> MiniLM vectors for the >=20-word cohort -> gold.review_embeddings (spec conf/embedding-spec.json)
	$(RUN) -m src.ai.embed --scope full

embed-sample:  ## same, over the sample silver table
	$(RUN) -m src.ai.embed --scope sample

index-reviews-vectors:  ## rebuild the `reviews` generation with text_vector from the latest embeddings run, alias swap
	$(RUN) -m src.serving.index_reviews --scope full --prune --with-embeddings

index-reviews-vectors-sample:  ## same, for alias reviews_sample
	$(RUN) -m src.serving.index_reviews --scope sample --prune --with-embeddings

pool-embeddings:  ## pool top-10 of knn, hybrid, bm25_cohort, hybrid_cohort for the 20 frozen queries
	$(RUN) scripts/judge_search.py --pool --round embeddings

export-judgements:  ## dump the unjudged pool (retriever hidden) to eval/search/unjudged.jsonl for judging outside the terminal
	$(RUN) scripts/judge_search.py --export eval/search/unjudged.jsonl

ann-recall:  ## ANN recall@10 of knn vs exact cosine on the live alias -> eval/embeddings/ann_recall.json
	$(RUN) scripts/ann_recall.py

eval-embeddings:  ## controlled + production tables, H-E1..3 verdicts -> docs/decisions/embeddings-retrieval.md
	$(RUN) scripts/eval_embeddings.py

gate-embeddings:  ## re-derive every Embeddings constituent (spec, table, index, recall, judgements); prints EMBED_GATE
	$(RUN) scripts/gate_embeddings.py

theme-samples:  ## pre-2020 candidates + matched controls + seeded 600-review discovery draw
	$(RUN) -m src.spark.theme_samples --sample discovery --scope full

theme-samples-sample:  ## same, over the sample gold/silver tables
	$(RUN) -m src.spark.theme_samples --sample discovery --scope sample

freeze-theme-terms:  ## mine conf/theme-terms.json from the discovery quotes (RR-22, one-shot)
	$(RUN) scripts/freeze_theme_terms.py --scope full

theme-frames:  ## draw the development, audit and training-pool frames, in that order (RR-22)
	$(RUN) -m src.spark.theme_samples --sample development --scope full
	$(RUN) -m src.spark.theme_samples --sample audit --scope full
	$(RUN) -m src.spark.theme_samples --sample training_pool --scope full

discover-phrases:  ## free complaint phrases from the 600-review discovery sample (local qwen3:8b)
	$(RUN) -m src.ai.discover_phrases --scope full

blind-export:  ## write eval/themes/blind-<SAMPLE>.jsonl + .map.json for blind labelling (RR-21)
	$(RUN) scripts/blind_export.py --sample $(SAMPLE_NAME)

label-themes:  ## run the labeller over a frame (SAMPLE_NAME, PROMPT, MODEL) -> gold.review_theme_labels
	$(RUN) -m src.ai.label_themes --sample $(SAMPLE_NAME) --prompt $(PROMPT) $(if $(MODEL),--model $(MODEL),)

label-pool:  ## label the 3,000-review training pool with the frozen prompt (ticket 07); resumable
	$(RUN) -m src.ai.label_themes --sample training_pool --prompt $(PROMPT)

pool-census:  ## what the labelled pool teaches after the drop -> eval/themes/training-pool-*.json
	$(RUN) scripts/pool_census.py --scope full

import-reference:  ## import the agent's blind ground truth (label_source=agent_reference)
	$(RUN) scripts/import_reference_labels.py --sample $(SAMPLE_NAME)

score-themes:  ## per-theme table for one configuration (SAMPLE_NAME, PROMPT, MODEL)
	$(RUN) scripts/score_themes.py --sample $(SAMPLE_NAME) --prompt $(PROMPT) $(if $(MODEL),--model $(MODEL),)

select-prompt:  ## apply the committed selection rule to the development scores -> eval/themes/selection-development.json (add FREEZE=1 to record the freeze)
	$(RUN) scripts/select_prompt.py $(if $(FREEZE),--freeze,)

diagnose-failures:  ## census a configuration's parse failures by named cause -> eval/themes/parse-census-*.json
	$(RUN) scripts/diagnose_label_failures.py --sample $(SAMPLE_NAME) --prompt $(PROMPT) $(if $(MODEL),--model $(MODEL),)

audit-once:  ## open the held-out audit set ONCE, scoring all three systems together (ticket 09)
	$(RUN) scripts/open_audit.py --scope full

gate-themes:  ## re-derive every P6 constituent from the spec, the ledger and Iceberg; prints THEMES_GATE
	$(RUN) scripts/gate_themes.py --scope full

adjudicate-export:  ## write the disagreement worklist for one configuration (SAMPLE_NAME, PROMPT)
	$(RUN) scripts/adjudicate_themes.py --sample $(SAMPLE_NAME) --prompt $(PROMPT) --export

adjudicate-import:  ## validate the filled causes and write docs/theme-taxonomy/adjudication-*.csv
	$(RUN) scripts/adjudicate_themes.py --sample $(SAMPLE_NAME) --prompt $(PROMPT) --import

agreement-draw:  ## draw Philip's blind stratified 50 of the audit set; frozen on first write (RR-21)
	$(RUN) scripts/agreement_subset.py --draw --scope full

agreement-export:  ## carve the 50 blind rows out of the audit export -> eval/themes/blind-agreement-audit.jsonl
	$(RUN) scripts/agreement_subset.py --export --scope full

agreement-labeller:  ## build the offline blind labelling page for the 50 -> .scratch/agreement-labeller/index.html
	$(RUN) .scratch/agreement-labeller/build.py

agreement-check:  ## validate Philip's partial hand-label file without importing it
	$(RUN) scripts/check_reference_labels.py --sample audit --blind blind-agreement-audit.jsonl --labels human-agreement-audit.jsonl

agreement-import:  ## import Philip's hand labels with label_source=human; all 50 or none
	$(RUN) scripts/agreement_subset.py --import --scope full

agreement-score:  ## per-theme and overall agreement with Wilson intervals -> eval/themes/agreement-audit.json
	$(RUN) scripts/agreement_subset.py --score --scope full

sentiment-check:  ## overall_sentiment vs stars, 3-star excluded, Wilson intervals (ADR-0003)
	$(RUN) scripts/sentiment_check.py --sample $(SAMPLE_NAME) --prompt $(PROMPT)

star-baseline-fit:  ## fit the per-theme star thresholds on development and freeze them (RR-23)
	$(RUN) scripts/baseline_star_only.py --fit

star-baseline-score:  ## apply the frozen star thresholds to SAMPLE_NAME
	$(RUN) scripts/baseline_star_only.py --score --sample $(SAMPLE_NAME)

classifier-train:  ## train the MLlib theme baseline on the frozen-prompt training pool (ADR-0002)
	$(RUN) -m src.spark.theme_classifier --train $(if $(CONFIG_HASH),--source-config-hash $(CONFIG_HASH),)

classifier-thresholds:  ## sweep the per-theme probability cuts on development and freeze them
	$(RUN) -m src.spark.theme_classifier --fit-thresholds

classifier-score:  ## apply the frozen classifier to SAMPLE_NAME -> label_source=classifier
	$(RUN) -m src.spark.theme_classifier --score --sample $(SAMPLE_NAME)

classifier-table:  ## per-theme table for the classifier's rows on SAMPLE_NAME
	$(RUN) scripts/score_themes.py --sample $(SAMPLE_NAME) --source classifier

discovery-failures:  ## characterise the discovery parse failures by named validation cause
	$(RUN) scripts/discovery_failures.py --scope full

propose-taxonomy:  ## count the raw complaint aspects (input to the hand merge, never the merge)
	$(RUN) scripts/propose_taxonomy.py --scope full

score-taxonomy:  ## apply the ADR-0003 support rule to the hand-written merge proposal
	$(RUN) scripts/score_taxonomy.py --scope full

rag-ranking:  ## rank the decline candidates by RR-09's bootstrap lower bound, draw nothing
	$(RUN) scripts/rag_slots.py --ranking-only

rag-prefilter:  ## lexical pre-filter over every product in the ranking head (necessary, never sufficient)
	$(RUN) scripts/rag_prefilter.py

rag-slots:  ## top five eligible decline candidates + nearest distinct control each -> eval/rag/slots.json
	$(RUN) scripts/rag_slots.py --eligible-from eval/rag/prefilter.json $(if $(wildcard eval/rag/validation.json),--reject-from eval/rag/validation.json,)

rag-scan:  ## evidence scan step 1: frozen terms over every in-scope review -> census, worklist, probes
	$(RUN) scripts/rag_evidence_scan.py

rag-validate:  ## evidence scan step 2: expand the manual reading into the validation record
	$(RUN) scripts/rag_validate.py

rag-questions-draft:  ## build the thirty questions and check every invariant, writing only a draft
	$(RUN) scripts/rag_freeze_questions.py --dry-run

rag-freeze:  ## freeze conf/rag-questions.json; refuses if any answer already exists (ADR-0006)
	$(RUN) scripts/rag_freeze_questions.py --freeze

rag-dev-questions:  ## draw the ten development questions (pre-2020, non-candidate products); prompt development only
	$(RUN) scripts/rag_dev_questions.py

rag-dev-answers:  ## answer the development set, for prompt development; never scored, never in the table
	$(RUN) -m src.ai.rag_run --questions eval/rag/dev/questions.json --question-set development --prompt $(RAG_PROMPT)

rag-answers:  ## answer the thirty frozen questions ONCE under the frozen prompt; writes eval/rag/answers.json and the seal
	$(RUN) -m src.ai.rag_run --questions conf/rag-questions.json --question-set evaluation

gate-rag:  ## re-derive every P7 constituent from the manifest, the seal, the retrieval and the call ledger; prints RAG_GATE
	$(RUN) scripts/gate_rag.py --scope full

eval-table:  ## the whole evaluation table from conf/lineage_chain.toml + eval/*/gate.json; non-zero on a capability that is neither a number nor a written reason
	$(RUN) scripts/eval_table.py

gate-lineage:  ## walk the declared chain: every artefact, snapshot and ES generation back to its ledger row; prints LINEAGE_GATE and chain_links_checked
	$(RUN) scripts/gate_lineage.py --mode development --scope full

reconcile-run:  ## close a run a killed driver left `running`: RUN_ID=<id> REASON="what happened" -- records it failed, never success
	$(RUN) scripts/reconcile_run.py --run "$(RUN_ID)" --reason "$(REASON)"

gate-lineage-publication:  ## the same walk at the publication bar: every declared capability pinned, current and finished
	$(RUN) scripts/gate_lineage.py --mode publication --scope full

verify:  ## row count, snapshot history, time-travel read
	$(RUN) scripts/verify_iceberg.py

eos:  ## exactly-once gate: load, SIGKILL mid-stream, restart, assert no loss/dupes
	$(RUN) scripts/prove_exactly_once.py --records 120000 --kill-after 25

test:  ## unit tests
	./run.sh pytest -q

lint:  ## ruff
	./run.sh ruff check .

check: lint test  ## what CI runs
