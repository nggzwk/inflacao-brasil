BEGIN;

CREATE SCHEMA IF NOT EXISTS inflacao_brasil;

-- Raw standardized observations (one row per price observation in source files).
CREATE TABLE IF NOT EXISTS inflacao_brasil.price_observation (
    id BIGSERIAL PRIMARY KEY,
    reference_date DATE NOT NULL,
    month_ref DATE NOT NULL,
    rede TEXT NOT NULL,
    endereco TEXT,
    produto TEXT NOT NULL,
    marca TEXT,
    preco NUMERIC(12,4) NOT NULL CHECK (preco > 0),
    qtd_embalagem TEXT,
    unidade_sigla TEXT,
    categoria_score NUMERIC(10,6),
    produto_categoria INTEGER,
    produto_subcategoria INTEGER,
    source_file TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_price_observation_month_ref
        CHECK (month_ref = date_trunc('month', reference_date)::date)
);

CREATE INDEX IF NOT EXISTS ix_price_observation_month_ref
    ON inflacao_brasil.price_observation (month_ref);

CREATE INDEX IF NOT EXISTS ix_price_observation_item_key
    ON inflacao_brasil.price_observation (qtd_embalagem, unidade_sigla, produto_categoria, produto_subcategoria);

CREATE INDEX IF NOT EXISTS ix_price_observation_category_month
    ON inflacao_brasil.price_observation (produto_categoria, produto_subcategoria, month_ref);

-- Your canonical item key for inflation tracking.
CREATE TABLE IF NOT EXISTS inflacao_brasil.item_key (
    id BIGSERIAL PRIMARY KEY,
    qtd_embalagem TEXT NOT NULL,
    unidade_sigla TEXT NOT NULL,
    produto_categoria INTEGER NOT NULL,
    produto_subcategoria INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_item_key UNIQUE (
        qtd_embalagem,
        unidade_sigla,
        produto_categoria,
        produto_subcategoria
    )
);

CREATE INDEX IF NOT EXISTS ix_item_key_category
    ON inflacao_brasil.item_key (produto_categoria, produto_subcategoria);

-- Monthly aggregated price per item_key (median is robust to outliers/promotions).
CREATE TABLE IF NOT EXISTS inflacao_brasil.item_monthly_price (
    item_id BIGINT NOT NULL REFERENCES inflacao_brasil.item_key(id) ON DELETE CASCADE,
    month_ref DATE NOT NULL,
    median_price NUMERIC(12,4) NOT NULL,
    avg_price NUMERIC(12,4) NOT NULL,
    obs_count INTEGER NOT NULL,
    min_price NUMERIC(12,4) NOT NULL,
    max_price NUMERIC(12,4) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (item_id, month_ref)
);

CREATE INDEX IF NOT EXISTS ix_item_monthly_price_month
    ON inflacao_brasil.item_monthly_price (month_ref);

-- Optional basket for MVP charts (example: "cesta_basica").
CREATE TABLE IF NOT EXISTS inflacao_brasil.basket (
    id BIGSERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inflacao_brasil.basket_item (
    basket_id BIGINT NOT NULL REFERENCES inflacao_brasil.basket(id) ON DELETE CASCADE,
    item_id BIGINT NOT NULL REFERENCES inflacao_brasil.item_key(id) ON DELETE CASCADE,
    weight NUMERIC(12,6) NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (basket_id, item_id)
);

-- Refreshes item_key and item_monthly_price from raw observations.
CREATE OR REPLACE FUNCTION inflacao_brasil.refresh_item_monthly_price()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO inflacao_brasil.item_key (
        qtd_embalagem,
        unidade_sigla,
        produto_categoria,
        produto_subcategoria
    )
    SELECT DISTINCT
        trim(o.qtd_embalagem) AS qtd_embalagem,
        upper(trim(o.unidade_sigla)) AS unidade_sigla,
        o.produto_categoria,
        o.produto_subcategoria
    FROM inflacao_brasil.price_observation o
    WHERE o.produto_categoria IS NOT NULL
      AND o.produto_subcategoria IS NOT NULL
      AND NULLIF(trim(o.qtd_embalagem), '') IS NOT NULL
      AND NULLIF(trim(o.unidade_sigla), '') IS NOT NULL
    ON CONFLICT (qtd_embalagem, unidade_sigla, produto_categoria, produto_subcategoria)
    DO NOTHING;

    TRUNCATE TABLE inflacao_brasil.item_monthly_price;

    INSERT INTO inflacao_brasil.item_monthly_price (
        item_id,
        month_ref,
        median_price,
        avg_price,
        obs_count,
        min_price,
        max_price
    )
    SELECT
        ik.id AS item_id,
        o.month_ref,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY o.preco)::NUMERIC(12,4) AS median_price,
        avg(o.preco)::NUMERIC(12,4) AS avg_price,
        count(*)::INTEGER AS obs_count,
        min(o.preco)::NUMERIC(12,4) AS min_price,
        max(o.preco)::NUMERIC(12,4) AS max_price
    FROM inflacao_brasil.price_observation o
    INNER JOIN inflacao_brasil.item_key ik
        ON ik.qtd_embalagem = trim(o.qtd_embalagem)
       AND ik.unidade_sigla = upper(trim(o.unidade_sigla))
       AND ik.produto_categoria = o.produto_categoria
       AND ik.produto_subcategoria = o.produto_subcategoria
    WHERE o.produto_categoria IS NOT NULL
      AND o.produto_subcategoria IS NOT NULL
      AND NULLIF(trim(o.qtd_embalagem), '') IS NOT NULL
      AND NULLIF(trim(o.unidade_sigla), '') IS NOT NULL
    GROUP BY ik.id, o.month_ref;
END;
$$;

-- Per-item monthly inflation (month over month and base-month).
CREATE OR REPLACE VIEW inflacao_brasil.v_item_monthly_inflation AS
WITH base AS (
    SELECT
        imp.item_id,
        imp.month_ref,
        imp.median_price,
        lag(imp.median_price) OVER (
            PARTITION BY imp.item_id
            ORDER BY imp.month_ref
        ) AS prev_month_price,
        first_value(imp.median_price) OVER (
            PARTITION BY imp.item_id
            ORDER BY imp.month_ref
        ) AS base_month_price
    FROM inflacao_brasil.item_monthly_price imp
)
SELECT
    b.item_id,
    b.month_ref,
    b.median_price,
    b.prev_month_price,
    CASE
        WHEN b.prev_month_price IS NULL OR b.prev_month_price = 0 THEN NULL
        ELSE round(((b.median_price / b.prev_month_price) - 1) * 100, 4)
    END AS mom_pct,
    b.base_month_price,
    CASE
        WHEN b.base_month_price IS NULL OR b.base_month_price = 0 THEN NULL
        ELSE round(((b.median_price / b.base_month_price) - 1) * 100, 4)
    END AS since_base_pct
FROM base b;

-- Basket series for charting (weighted sum by month + percentage deltas).
CREATE OR REPLACE FUNCTION inflacao_brasil.get_basket_monthly_series(p_basket_id BIGINT)
RETURNS TABLE (
    month_ref DATE,
    basket_total NUMERIC(14,4),
    prev_basket_total NUMERIC(14,4),
    mom_pct NUMERIC(10,4),
    base_basket_total NUMERIC(14,4),
    since_base_pct NUMERIC(10,4)
)
LANGUAGE sql
AS $$
WITH weighted_monthly AS (
    SELECT
        imp.month_ref,
        sum(imp.median_price * bi.weight)::NUMERIC(14,4) AS basket_total,
        count(*) AS present_items,
        (SELECT count(*) FROM inflacao_brasil.basket_item x WHERE x.basket_id = p_basket_id) AS basket_size
    FROM inflacao_brasil.basket_item bi
    INNER JOIN inflacao_brasil.item_monthly_price imp
        ON imp.item_id = bi.item_id
    WHERE bi.basket_id = p_basket_id
    GROUP BY imp.month_ref
),
complete_months AS (
    SELECT
        month_ref,
        basket_total
    FROM weighted_monthly
    WHERE present_items = basket_size
),
deltas AS (
    SELECT
        cm.month_ref,
        cm.basket_total,
        lag(cm.basket_total) OVER (ORDER BY cm.month_ref) AS prev_basket_total,
        first_value(cm.basket_total) OVER (ORDER BY cm.month_ref) AS base_basket_total
    FROM complete_months cm
)
SELECT
    d.month_ref,
    d.basket_total,
    d.prev_basket_total,
    CASE
        WHEN d.prev_basket_total IS NULL OR d.prev_basket_total = 0 THEN NULL
        ELSE round(((d.basket_total / d.prev_basket_total) - 1) * 100, 4)
    END AS mom_pct,
    d.base_basket_total,
    CASE
        WHEN d.base_basket_total IS NULL OR d.base_basket_total = 0 THEN NULL
        ELSE round(((d.basket_total / d.base_basket_total) - 1) * 100, 4)
    END AS since_base_pct
FROM deltas d
ORDER BY d.month_ref;
$$;

COMMIT;
