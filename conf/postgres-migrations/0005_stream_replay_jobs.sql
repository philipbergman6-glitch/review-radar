-- P8 replay: `sort_replay` and `stream_produce` join the job vocabulary (ADR-0010, ticket 14).
--
-- Two jobs, not one. `sort_replay` orders the raw file by event time and writes a durable
-- artefact with its own digest; `stream_produce` paces that artefact into the stream topic.
-- Splitting them is what makes event order a property of the data rather than of a run: the
-- replay's ledger row names the file it sent by sha256, and that file's ledger row names the
-- input it was sorted from. A single job would leave "which ordering was this?" answerable
-- only by re-running it.
--
-- `stream_produce` is also distinct from `produce` because they write different topics from
-- different orderings of the same file (ADR-0008: the stream never shares the batch topic).
-- One job name covering both would make a lineage walk unable to tell the bronze drain's
-- source from the watermark job's.

ALTER TABLE pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_job_name_check;

ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_job_name_check CHECK (job_name IN (
    'produce', 'catalogue_load', 'bronze_drain', 'silver', 'gold',
    'search_index_reviews', 'search_index_product_month', 'embeddings',
    'theme_samples', 'theme_labels_llm', 'theme_labels_reference', 'theme_labels_human',
    'theme_classifier_train', 'theme_classifier_score', 'rag_answers',
    'sort_replay', 'stream_produce'));
