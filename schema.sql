-- store: franchise locations
CREATE TABLE store (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(50) NOT NULL,
    address TEXT,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_store_name UNIQUE (name)
);

-- week_metadata: defines reporting week ranges
CREATE TABLE week_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    CONSTRAINT week_dates_valid CHECK (end_date > start_date)
);

-- category: product categories
CREATE TABLE category (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_category_name UNIQUE (name)
);

-- item: individual menu items
CREATE TABLE item (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    category_id UUID REFERENCES category(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_item_name UNIQUE (name)
);

-- sales_report: container per store/week for a submitted report
CREATE TABLE sales_report (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    store_id UUID REFERENCES store(id) ON DELETE CASCADE,
    week_id UUID REFERENCES week_metadata(id) ON DELETE RESTRICT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index to speed lookups by week and store
CREATE INDEX idx_sales_report_week_id_store_id ON sales_report(week_id, store_id);

-- sales_report_by_category: category-level rows submitted by franchises
CREATE TABLE sales_report_by_category (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sales_report_id UUID REFERENCES sales_report(id) ON DELETE CASCADE,
    category_id UUID REFERENCES category(id) ON DELETE RESTRICT NOT NULL,
    number_of_sales INT NOT NULL DEFAULT 0 CHECK (number_of_sales >= 0)
);

CREATE INDEX idx_sales_report_by_category_sales_report_id ON sales_report_by_category(sales_report_id);

-- sales_report_by_item: item-level rows submitted by franchises
CREATE TABLE sales_report_by_item (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sales_report_id UUID REFERENCES sales_report(id) ON DELETE CASCADE,
    item_id UUID REFERENCES item(id) ON DELETE RESTRICT NOT NULL,
    number_of_sales INT NOT NULL DEFAULT 0 CHECK (number_of_sales >= 0)
);

CREATE INDEX idx_sales_report_by_item_sales_report_id ON sales_report_by_item(sales_report_id);

-- index to speed item -> category joins
CREATE INDEX idx_item_category_id ON item(category_id);

-- Materialized view: sales by category per week (category-level reports)
CREATE OR REPLACE MATERIALIZED VIEW sales_by_category_per_week AS
SELECT
    src.category_id AS category_id,
    sr.week_id,
    SUM(src.number_of_sales) AS sales
FROM
    sales_report sr
JOIN
    sales_report_by_category src ON src.sales_report_id = sr.id
GROUP BY
    src.category_id, sr.week_id;

-- Materialized view: sales by item per week (item-level reports)
CREATE OR REPLACE MATERIALIZED VIEW sales_by_item_per_week AS
SELECT
    sri.item_id,
    SUM(sri.number_of_sales) AS sales,
    sr.week_id
FROM
    sales_report sr
JOIN
    sales_report_by_item sri ON sri.sales_report_id = sr.id
GROUP BY
    sri.item_id, sr.week_id;

-- Materialized view: total sales by category per week (combines both formats)
CREATE OR REPLACE MATERIALIZED VIEW total_sales_by_category_per_week AS
SELECT
    t.category_id,
    SUM(t.sales) AS total_sales,
    t.week_id
FROM (
    SELECT category_id, sales, week_id
    FROM sales_by_category_per_week
    UNION ALL
    SELECT i.category_id, SUM(siw.sales) AS sales, siw.week_id
    FROM sales_by_item_per_week siw
    JOIN item i ON siw.item_id = i.id
    GROUP BY i.category_id, siw.week_id
) t
GROUP BY
    t.category_id, t.week_id;

CREATE INDEX idx_total_sales_by_category_per_week_category_id_week_id
ON total_sales_by_category_per_week(category_id, week_id);

-- Regular view: best selling categories by week (top N)
CREATE OR REPLACE VIEW best_selling_categories_by_week AS
SELECT category_id, week_id, total_sales, rank
FROM (
    SELECT
        category_id,
        week_id,
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY week_id ORDER BY total_sales DESC) AS rank
    FROM total_sales_by_category_per_week
) t
WHERE rank <= 10;

-- Materialized view: sales by store and week (store-level totals)
CREATE OR REPLACE MATERIALIZED VIEW sales_by_store_and_week AS
SELECT
    sr.store_id AS store_id,
    SUM(srn.number_of_sales) AS sales,
    sr.week_id
FROM
    sales_report sr
JOIN (
    SELECT sales_report_id, number_of_sales FROM sales_report_by_category
    UNION ALL
    SELECT sales_report_id, number_of_sales FROM sales_report_by_item
) srn ON srn.sales_report_id = sr.id
GROUP BY
    sr.store_id, sr.week_id;

CREATE INDEX idx_sales_by_store_and_week_week_id ON sales_by_store_and_week(week_id);

-- Regular view: outlier stores by week using z-score
CREATE OR REPLACE VIEW outlier_stores_by_week AS
SELECT
    sbw.store_id,
    sbw.week_id,
    (sbw.sales - stats.avg_sales) / NULLIF(stats.stddev_sales, 0) AS z_score,
    CASE
        WHEN sbw.sales > stats.avg_sales THEN 'high'
        ELSE 'low'
    END AS outlier_type
FROM (
    SELECT week_id, AVG(sales) AS avg_sales, STDDEV(sales) AS stddev_sales
    FROM sales_by_store_and_week
    GROUP BY week_id
) stats
JOIN sales_by_store_and_week sbw ON sbw.week_id = stats.week_id
WHERE
    ABS(sbw.sales - stats.avg_sales) > 2 * stats.stddev_sales;

-- Regular view: weekly sales compared to all-time high by category
CREATE OR REPLACE VIEW weekly_sales_vs_all_time_high_by_category AS
SELECT
    tsbw.category_id,
    tsbw.week_id,
    tsbw.total_sales,
    ath.all_time_high_sales,
    CASE
        WHEN ath.all_time_high_sales = 0 THEN 0
        ELSE (tsbw.total_sales::DECIMAL / ath.all_time_high_sales) * 100
    END AS percentage_of_all_time_high
FROM
    total_sales_by_category_per_week tsbw
JOIN (
    SELECT category_id, MAX(total_sales) AS all_time_high_sales
    FROM total_sales_by_category_per_week
    GROUP BY category_id
) ath ON tsbw.category_id = ath.category_id;
