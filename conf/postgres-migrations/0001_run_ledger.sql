-- 0001: pipeline_runs becomes the run ledger (ADR-0008); products carries the
-- catalogue load id (ADR-0007). Folded into 01_schema.sql at baseline '0001'.
--
-- The old pipeline_runs (BIGSERIAL run_id, `stage` column) was never written to.
-- This migration refuses to run if that is no longer true, rather than dropping
-- rows: a ledger with history is rebuilt by hand, not by a script.

DO $$
DECLARE n BIGINT;
BEGIN
    IF to_regclass('public.pipeline_runs') IS NOT NULL THEN
        EXECUTE 'SELECT count(*) FROM pipeline_runs' INTO n;
        IF n > 0 THEN
            RAISE EXCEPTION 'pipeline_runs holds % rows; refusing to replace it', n;
        END IF;
    END IF;
END $$;

DROP TABLE IF EXISTS pipeline_runs;

CREATE TABLE pipeline_runs (
    run_id                      UUID PRIMARY KEY,
    job_name                    TEXT NOT NULL CHECK (job_name IN (
                                    'produce', 'catalogue_load', 'bronze_drain', 'silver',
                                    'gold', 'search_index_reviews',
                                    'search_index_product_month', 'embeddings',
                                    'theme_labels_llm', 'theme_classifier_train',
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

-- products was empty before any loader existed; the NOT NULL is safe to add when
-- it still is, and the migration says so if it is not.
DO $$
DECLARE n BIGINT;
BEGIN
    SELECT count(*) INTO n FROM products;
    IF n > 0 THEN
        RAISE EXCEPTION 'products holds % rows without a catalogue_load_id; reload it', n;
    END IF;
END $$;
ALTER TABLE products ADD COLUMN IF NOT EXISTS catalogue_load_id UUID NOT NULL;
