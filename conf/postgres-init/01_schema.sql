-- Relational side of the project: the product catalogue.
--
-- Role in the architecture: this is the OLTP-style, ACID, normalised store that
-- the streaming pipeline enriches against. Reviews arrive on Kafka carrying only
-- a `parent_asin`; Spark joins them to this table over JDBC to attach the
-- product title, brand/store and price. That is the "enrich with data from a
-- SQL database" option in the brief.

CREATE TABLE IF NOT EXISTS products (
    parent_asin     TEXT PRIMARY KEY,
    title           TEXT NOT NULL,
    main_category   TEXT,
    store           TEXT,
    price           NUMERIC(10, 2),           -- NULL for ~84% of rows: see docs/phase0-profile.txt
    average_rating  REAL,
    rating_number   INTEGER,
    categories      TEXT[],
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Lookup pattern is a point-get by key (the streaming join) plus browse by brand.
CREATE INDEX IF NOT EXISTS idx_products_store    ON products (store);
CREATE INDEX IF NOT EXISTS idx_products_category ON products (main_category);

-- Audit table: every pipeline run records what it processed, so results are
-- reproducible and we can show lineage during the demo.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    stage           TEXT NOT NULL,            -- bronze | silver | gold | ai_enrich | index
    category        TEXT NOT NULL,
    records_in      BIGINT,
    records_out     BIGINT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    notes           TEXT
);
