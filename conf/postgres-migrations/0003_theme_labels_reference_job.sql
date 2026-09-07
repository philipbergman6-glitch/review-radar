-- P6 Themes: `theme_labels_reference` joins the job vocabulary (ADR-0003 amendment (c), RR-21).
--
-- RR-21 made the in-session agent the author of P6's ground truth, with provenance
-- `label_source="agent_reference"` -- a value distinct from `local_llm` (the system under
-- test) and from `human` (reserved for Philip). Producing those labels is a separate job
-- from running the labeller: it reads a blind export (title and text only, keyed by an
-- opaque blind id) and merges the agent's labels into gold.review_theme_labels. It writes
-- an Iceberg table, so it needs a ledger row (ADR-0008), and the CHECK constraint that
-- guards job_name has to learn the name first.

ALTER TABLE pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_job_name_check;

ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_job_name_check CHECK (job_name IN (
    'produce', 'catalogue_load', 'bronze_drain', 'silver', 'gold',
    'search_index_reviews', 'search_index_product_month', 'embeddings',
    'theme_samples', 'theme_labels_llm', 'theme_labels_reference', 'theme_classifier_train',
    'theme_classifier_score', 'rag_answers'));
