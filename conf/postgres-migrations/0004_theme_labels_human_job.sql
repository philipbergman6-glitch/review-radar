-- P6 close-out: `theme_labels_human` joins the job vocabulary (RR-21, ticket 10).
--
-- The last provenance the labels table reserves. `theme_labels_reference` writes the agent's
-- blind ground truth; this writes Philip's blind stratified 50 of the audit set, under
-- `label_source="human"`, and the agreement between the two is published as THEMES_AGREEMENT.
--
-- It is a separate job rather than the same one with a different parameter because the whole
-- point of the number is that the two annotators are independent: one job writing under both
-- provenances would leave nothing in the ledger that could tell them apart after the fact, and
-- an agreement number nobody can attribute to two separate runs prices nothing.

ALTER TABLE pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_job_name_check;

ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_job_name_check CHECK (job_name IN (
    'produce', 'catalogue_load', 'bronze_drain', 'silver', 'gold',
    'search_index_reviews', 'search_index_product_month', 'embeddings',
    'theme_samples', 'theme_labels_llm', 'theme_labels_reference', 'theme_labels_human',
    'theme_classifier_train', 'theme_classifier_score', 'rag_answers'));
