-- P7 judging: `rag_judgements` joins the job vocabulary (ADR-0006, ticket 13).
--
-- Philip's pass over the thirty answers is a ledger job for the same reason his 50 theme
-- labels are (`theme_labels_human`, migration 0004): the four `RAG_QUALITY` numbers are
-- computed from a file a human wrote, and the evaluation artefact has to name the run that
-- imported it -- which answers file it judged (by run id and digest), which rubric, how many
-- rows -- or the lineage walk ends at a number with no producer.
--
-- Separate from `rag_answers` because the two are made by different actors at different
-- times against the same bytes: the model answered once under the seal, the judge read those
-- answers later. One row for both would leave "were these the answers that were judged?"
-- answerable only by trusting the file on disk.

ALTER TABLE pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_job_name_check;

ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_job_name_check CHECK (job_name IN (
    'produce', 'catalogue_load', 'bronze_drain', 'silver', 'gold',
    'search_index_reviews', 'search_index_product_month', 'embeddings',
    'theme_samples', 'theme_labels_llm', 'theme_labels_reference', 'theme_labels_human',
    'theme_classifier_train', 'theme_classifier_score', 'rag_answers', 'rag_judgements',
    'sort_replay', 'stream_produce', 'stream_aggregate'));
