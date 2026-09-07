# Single entrypoint for the stack. `make` with no target prints this list.
# Every Python target goes through ./run.sh, which pins JAVA_HOME to JDK 17.

.DEFAULT_GOAL := help
RUN := ./run.sh python
CATEGORY ?= All_Beauty
SAMPLE ?= data/sample/$(CATEGORY).sample.jsonl
TOPIC ?= reviews.raw

.PHONY: help up up-ui down health pg-migrate catalogue produce produce-sample bronze bronze-sample silver silver-sample gate-silver reproduce-silver gold gold-sample index-reviews index-reviews-sample index-product-month index-product-month-sample kibana-import pool-search judge-search eval-search gate-search verify eos test lint check

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

verify:  ## row count, snapshot history, time-travel read
	$(RUN) scripts/verify_iceberg.py

eos:  ## exactly-once gate: load, SIGKILL mid-stream, restart, assert no loss/dupes
	$(RUN) scripts/prove_exactly_once.py --records 120000 --kill-after 25

test:  ## unit tests
	./run.sh pytest -q

lint:  ## ruff
	./run.sh ruff check .

check: lint test  ## what CI runs
