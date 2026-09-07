-- Relational side of the project. This file is the fresh-install truth: a new
-- Postgres volume gets exactly this schema. Changes to an existing database go
-- through conf/postgres-migrations/NNNN_*.sql via `make pg-migrate`, and a test
-- asserts that init and init+migrations agree (ADR-0008).
--
-- Schema baseline: bump when a migration's effect is folded into this file.
-- `scripts/pg_migrate.py` records every migration <= the baseline as applied on a
-- fresh install, so it is never re-run against a database that already has it.

-- ------------------------------------------------------------ products ----
-- The product catalogue: OLTP-style, ACID, the SQL enrichment source. Reviews
-- arrive on Kafka carrying only a `parent_asin`; every silver run reads this
-- table over JDBC and left-joins it (ADR-0007). Loaded by
-- src/catalogue/load_products.py under a `catalogue_load_id` = that run's id.
CREATE TABLE IF NOT EXISTS products (
    parent_asin        TEXT PRIMARY KEY,
    title              TEXT NOT NULL,
    main_category      TEXT,
    store              TEXT,
    price              NUMERIC(10, 2),        -- NULL for ~84% of rows: docs/phase0-profile.txt
    average_rating     REAL,
    rating_number      INTEGER,
    categories         TEXT[],
    catalogue_load_id  UUID NOT NULL,         -- pipeline_runs.run_id of the load
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_products_store    ON products (store);
CREATE INDEX IF NOT EXISTS idx_products_category ON products (main_category);

-- ------------------------------------------------------- pipeline_runs ----
-- The run ledger (ADR-0008): one row per execution attempt of every job. The
-- driver inserts `running` before it starts so the id exists to stamp on every
-- output, and finalizes to success|failed afterwards. Typed core, JSONB tail;
-- the required keys per (job_name, spec_version) live in src/common/runs.py,
-- and tests/test_runs_contracts.py asserts this CHECK list equals that registry.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id                      UUID PRIMARY KEY,
    job_name                    TEXT NOT NULL CHECK (job_name IN (
                                    'produce', 'catalogue_load', 'bronze_drain', 'silver',
                                    'gold', 'search_index_reviews',
                                    'search_index_product_month', 'embeddings',
                                    'theme_samples', 'theme_labels_llm', 'theme_classifier_train',
                                    'theme_classifier_score', 'rag_answers')),
    spec_version                TEXT NOT NULL,
    status                      TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    category                    TEXT NOT NULL,
    data_scope                  TEXT NOT NULL CHECK (data_scope IN ('sample', 'full')),
    records_in                  BIGINT,
    records_out                 BIGINT,
    records_rejected            BIGINT,
    started_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at                 TIMESTAMPTZ,
    git_commit_sha              TEXT,
    worktree_dirty              BOOLEAN NOT NULL DEFAULT true,
    cleanliness_policy_version  TEXT NOT NULL,
    inputs                      JSONB NOT NULL DEFAULT '{}'::jsonb,
    outputs                     JSONB NOT NULL DEFAULT '{}'::jsonb,
    counts                      JSONB NOT NULL DEFAULT '{}'::jsonb,
    params                      JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes                       TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_job_started ON pipeline_runs (job_name, started_at DESC);

-- --------------------------------------------------- schema_migrations ----
-- Checksummed ledger of applied migrations. `baseline` records which migrations
-- this init file already contains; the migrate script fills the rest.
CREATE TABLE IF NOT EXISTS schema_migrations (
    name        TEXT PRIMARY KEY,
    checksum    TEXT,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO schema_migrations (name, checksum) VALUES ('baseline', '0002')
    ON CONFLICT (name) DO NOTHING;
