-- P8 projection: `stream_aggregate` joins the job vocabulary (ADR-0010, ticket 15).
--
-- The third P8 job, and separate from the two replay jobs for the same reason they are
-- separate from each other: it consumes a topic rather than producing one, and its numbers
-- are an accounting of what the watermark did. Folding it into `stream_produce` would put a
-- producer's acked count and a consumer's drop count in one row, and no lineage walk could
-- then say which side of the topic a missing row went missing on.
--
-- It writes two Iceberg tables. `stream.product_month_batches` is append-only -- one partial
-- aggregate per (product, month, micro-batch) -- and `stream.product_month` is their sum,
-- replaced atomically at the end of the run. Calendar months are not expressible in Spark's
-- `window()` (it refuses any interval carrying months), so the append-mode aggregate ADR-0010
-- describes is realised as append-only contributions plus a summed projection.

ALTER TABLE pipeline_runs DROP CONSTRAINT IF EXISTS pipeline_runs_job_name_check;

ALTER TABLE pipeline_runs ADD CONSTRAINT pipeline_runs_job_name_check CHECK (job_name IN (
    'produce', 'catalogue_load', 'bronze_drain', 'silver', 'gold',
    'search_index_reviews', 'search_index_product_month', 'embeddings',
    'theme_samples', 'theme_labels_llm', 'theme_labels_reference', 'theme_labels_human',
    'theme_classifier_train', 'theme_classifier_score', 'rag_answers',
    'sort_replay', 'stream_produce', 'stream_aggregate'));
