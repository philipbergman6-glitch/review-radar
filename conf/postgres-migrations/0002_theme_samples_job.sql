-- P6 Themes: `theme_samples` joins the job vocabulary (ADR-0003, RR-19).
--
-- The job selects which reviews the complaint-theme work is allowed to see: decline
-- candidates, their matched controls, and the seeded discovery draw. It writes Iceberg
-- tables, so it needs a ledger row like every other job (ADR-0008), and the CHECK
-- constraint that guards job_name has to learn the name first.

ALTER TABLE pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_job_name_check;

ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_job_name_check CHECK (job_name IN (
    'produce', 'catalogue_load', 'bronze_drain', 'silver', 'gold',
    'search_index_reviews', 'search_index_product_month', 'embeddings',
    'theme_samples', 'theme_labels_llm', 'theme_classifier_train',
    'theme_classifier_score', 'rag_answers'));
