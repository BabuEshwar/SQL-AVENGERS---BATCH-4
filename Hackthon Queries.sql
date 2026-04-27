---Creating Database
Create Database if not exists retail123_db;

---Creating SCHEMAS
create schema if not exists raw;
create schema if not exists validated;
create schema if not exists curated;
create schema if not exists analytics;
create schema if not exists governance;

----Creating File Formate
CREATE OR REPLACE FILE FORMAT raw.csv_format
  TYPE = 'CSV'
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

--Creating External Stage
create or replace schema external_stages;

---Creating AWS Integration
CREATE OR REPLACE STORAGE INTEGRATION S3_INT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('s3://hackthonsql/pipes/')
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::843356547313:role/hackthon';

desc storage integration s3_int;

-----Creating Storage Integration
CREATE OR REPLACE STAGE external_stages.aws_s3_csv1
URL = 's3://hackthonsql/pipes/'
STORAGE_INTEGRATION = s3_int;

list @external_stages.aws_s3_csv1;


----Creating Raw Tables
CREATE OR REPLACE TABLE raw.customers (
    customer_id STRING,
    name STRING,
    city STRING,
    signup_date DATE,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.products (
    product_id STRING,
    product_name STRING,
    category STRING,
    price NUMBER(10,2),
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE raw.orders (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    total_amount NUMBER(10,2),
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.order_items (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity NUMBER,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.user_activity (
    activity_id STRING,
    customer_id STRING,
    activity_type STRING,
    activity_time TIMESTAMP_NTZ,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

create schema if not exists pipe;

-----Creating Pips
CREATE OR REPLACE PIPE retail123_db.PIPE.customer_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RETAIL123_db.RAW.CUSTOMERS
FROM @retail123_db.external_stages.aws_s3_csv1/customers/
FILE_FORMAT = retail123_db.raw.csv_format
PATTERN ='.*customers.*\.csv'
ON_ERROR = CONTINUE;


CREATE OR REPLACE PIPE retail123_db.PIPE.product_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RETAIL123_db.RAW.PRODUCTS
FROM @retail123_db.external_stages.aws_s3_csv1/products/
FILE_FORMAT = retail123_db.raw.csv_format
PATTERN = '.*products.*\.csv'
ON_ERROR = CONTINUE;

desc pipe retail.PIPE.customer_pipe;

CREATE OR REPLACE PIPE retail123_db.PIPE.orders_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RETAIL123_db.RAW.ORDERS
FROM @retail123_db.external_stages.aws_s3_csv1/orders/
FILE_FORMAT = retail123_db.raw.csv_format
PATTERN = '.*orders.*\.csv'
ON_ERROR = CONTINUE;

CREATE OR REPLACE PIPE retail123_Db.PIPE.order_items_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RETAIL123_db.RAW.ORDER_ITEMS
FROM @retail123_db.external_stages.aws_s3_csv1/order_items/
FILE_FORMAT = retail123_db.raw.csv_format
PATTERN = '.*order_items.*\.csv'
ON_ERROR = CONTINUE;

CREATE OR REPLACE PIPE retail123_db.PIPE.user_activity_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RETAIL123_db.RAW.USER_ACTIVITY
FROM @retail123_db.external_stages.aws_s3_csv1/user_activity/
FILE_FORMAT = retail123_db.raw.csv_format
PATTERN = '.*user_activity.*\.csv'
ON_ERROR = CONTINUE;


-----Creating Streams
CREATE OR REPLACE STREAM raw.customers_stream ON TABLE raw.customers;
CREATE OR REPLACE STREAM raw.products_stream ON TABLE raw.products;
CREATE OR REPLACE STREAM raw.orders_stream ON TABLE raw.orders;
CREATE OR REPLACE STREAM raw.order_items_stream ON TABLE raw.order_items;
CREATE OR REPLACE STREAM raw.user_activity_stream ON TABLE RETAIL123_db.RAW.USER_ACTIVITY ;

select * from raw.user_activity_stream;

show pipes;


select * from raw.customers_stream;

---------Creating Vadilated Layer------
CREATE OR REPLACE TABLE validated.customers_v (
    customer_id STRING PRIMARY KEY,
    name STRING,
    city STRING,
    signup_date DATE,
    _load_timestamp TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE validated.products_v (
    product_id STRING PRIMARY KEY,
    product_name STRING,
    category STRING,
    price NUMBER(10,2),
    _load_timestamp TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE validated.orders_v (
    order_id STRING PRIMARY KEY,
    customer_id STRING,
    order_date DATE,
    total_amount NUMBER(10,2),
    _load_timestamp TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE validated.order_items_v (
    order_item_id STRING PRIMARY KEY,
    order_id STRING,
    product_id STRING,
    quantity NUMBER,
    _load_timestamp TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE validated.user_activity_v (
    activity_id STRING PRIMARY KEY,
    customer_id STRING,
    activity_type STRING,
    activity_time TIMESTAMP_NTZ,
    _load_timestamp TIMESTAMP_NTZ
);

----error log table---
CREATE OR REPLACE TABLE governance.dq_exception_log (
    error_id STRING DEFAULT UUID_STRING(),   -- unique error id
    source_table STRING,                     -- raw table name
    business_key STRING,                     -- primary key (customer_id, order_id, etc.)
    error_type STRING,                       -- NULL_CHECK / VALUE_CHECK / REFERENCE
    error_message STRING,                    -- description
    error_record VARIANT,                    -- full failed row (VERY IMPORTANT 🔥)
    error_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-------stored procedure for validation------
CREATE OR REPLACE PROCEDURE validated.process_retail_data_quality()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- =========================================
-- CUSTOMERS (STREAM + FALLBACK)
-- =========================================
CREATE OR REPLACE TEMP TABLE cur_customers AS 
SELECT * FROM raw.customers_stream;

-- fallback if stream empty
IF ((SELECT COUNT(*) FROM cur_customers) = 0) THEN
    CREATE OR REPLACE TEMP TABLE cur_customers AS 
    SELECT * FROM raw.customers;
END IF;

-- DEDUP
CREATE OR REPLACE TEMP TABLE cur_customers_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY _load_timestamp DESC) rn
    FROM cur_customers
) WHERE rn = 1;

-- LOAD
MERGE INTO validated.customers_v t
USING (
    SELECT customer_id, name, city, signup_date, _load_timestamp FROM cur_customers_dedup
    WHERE customer_id IS NOT NULL AND name IS NOT NULL
) s
ON t.customer_id = s.customer_id
WHEN NOT MATCHED THEN INSERT VALUES
(s.customer_id, s.name, s.city, s.signup_date, s._load_timestamp);

-- =========================================
-- PRODUCTS
-- =========================================
CREATE OR REPLACE TEMP TABLE cur_products AS 
SELECT * FROM raw.products_stream;

IF ((SELECT COUNT(*) FROM cur_products) = 0) THEN
    CREATE OR REPLACE TEMP TABLE cur_products AS 
    SELECT * FROM raw.products;
END IF;

CREATE OR REPLACE TEMP TABLE cur_products_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY _load_timestamp DESC) rn
    FROM cur_products
) WHERE rn = 1;

MERGE INTO validated.products_v t
USING (
    SELECT product_id, product_name, category, price, _load_timestamp FROM cur_products_dedup
    WHERE product_id IS NOT NULL AND price > 0
) s
ON t.product_id = s.product_id
WHEN NOT MATCHED THEN INSERT VALUES
(s.product_id, s.product_name, s.category, s.price, s._load_timestamp);

-- =========================================
-- ORDERS (NOW WILL LOAD DATA)
-- =========================================
CREATE OR REPLACE TEMP TABLE cur_orders AS 
SELECT * FROM raw.orders_stream;

IF ((SELECT COUNT(*) FROM cur_orders) = 0) THEN
    CREATE OR REPLACE TEMP TABLE cur_orders AS 
    SELECT * FROM raw.orders;
END IF;

CREATE OR REPLACE TEMP TABLE cur_orders_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY _load_timestamp DESC) rn
    FROM cur_orders
) WHERE rn = 1;

MERGE INTO validated.orders_v t
USING (
    SELECT order_id, customer_id, order_date, total_amount, _load_timestamp FROM cur_orders_dedup
    WHERE order_id IS NOT NULL AND total_amount >= 0
) s
ON t.order_id = s.order_id
WHEN NOT MATCHED THEN INSERT VALUES
(s.order_id, s.customer_id, s.order_date, s.total_amount, s._load_timestamp);

-- =========================================
-- ORDER ITEMS
-- =========================================
CREATE OR REPLACE TEMP TABLE cur_order_items AS 
SELECT * FROM raw.order_items_stream;

IF ((SELECT COUNT(*) FROM cur_order_items) = 0) THEN
    CREATE OR REPLACE TEMP TABLE cur_order_items AS 
    SELECT * FROM raw.order_items;
END IF;

CREATE OR REPLACE TEMP TABLE cur_order_items_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY order_item_id ORDER BY _load_timestamp DESC) rn
    FROM cur_order_items
) WHERE rn = 1;

MERGE INTO validated.order_items_v t
USING (
    SELECT order_item_id, order_id, product_id, quantity, _load_timestamp FROM cur_order_items_dedup
    WHERE quantity > 0
) s
ON t.order_item_id = s.order_item_id
WHEN NOT MATCHED THEN INSERT VALUES
(s.order_item_id, s.order_id, s.product_id, s.quantity, s._load_timestamp);

-- =========================================
-- USER ACTIVITY
-- =========================================
CREATE OR REPLACE TEMP TABLE cur_user_activity AS 
SELECT * FROM raw.user_activity_stream;

IF ((SELECT COUNT(*) FROM cur_user_activity) = 0) THEN
    CREATE OR REPLACE TEMP TABLE cur_user_activity AS 
    SELECT * FROM raw.user_activity;
END IF;

CREATE OR REPLACE TEMP TABLE cur_user_activity_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY activity_id ORDER BY _load_timestamp DESC) rn
    FROM cur_user_activity
) WHERE rn = 1;

MERGE INTO validated.user_activity_v t
USING (
    SELECT activity_id, customer_id, activity_type, activity_time, _load_timestamp FROM cur_user_activity_dedup
    WHERE activity_time IS NOT NULL
) s
ON t.activity_id = s.activity_id
WHEN NOT MATCHED THEN INSERT VALUES
(s.activity_id, s.customer_id, s.activity_type, s.activity_time, s._load_timestamp);

RETURN 'DATA LOADED SUCCESSFULLY (STREAM + FALLBACK FIX APPLIED)';

END;
$$;

call validated.process_retail_data_quality();

--------Creating Curated layer------

CREATE OR REPLACE TABLE curated.dim_customers (
    customer_sk NUMBER AUTOINCREMENT,
    customer_id STRING,
    name STRING,
    city STRING,
    signup_date DATE,
    effective_start TIMESTAMP,
    effective_end TIMESTAMP,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE curated.dim_products (
    product_sk NUMBER AUTOINCREMENT,
    product_id STRING,
    product_name STRING,
    category STRING,
    price NUMBER(10,2)
);

CREATE OR REPLACE TABLE curated.dim_date (
    date DATE,
    year NUMBER,
    month NUMBER,
    day NUMBER,
    quarter NUMBER
);

CREATE OR REPLACE TABLE curated.fact_orders (
    order_id STRING,
    customer_sk NUMBER,
    product_sk NUMBER,
    order_date DATE,
    quantity NUMBER,
    total_amount NUMBER
);

------stored procedure for curated layer------
CREATE OR REPLACE PROCEDURE curated.sp_load_curated_layer()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- =========================================
-- STEP 1: LOAD DIM_CUSTOMERS (SCD TYPE 2 SAFE)
-- =========================================

-- close old records
UPDATE curated.dim_customers tgt
SET effective_end = CURRENT_TIMESTAMP(),
    is_current = FALSE
FROM validated.customers_v src
WHERE tgt.customer_id = src.customer_id
  AND tgt.is_current = TRUE
  AND (tgt.name <> src.name OR tgt.city <> src.city);

-- insert new records
INSERT INTO curated.dim_customers
(customer_id, name, city, signup_date, effective_start, effective_end, is_current)
SELECT 
    customer_id, name, city, signup_date,
    CURRENT_TIMESTAMP(), NULL, TRUE
FROM validated.customers_v src
WHERE NOT EXISTS (
    SELECT 1 FROM curated.dim_customers tgt
    WHERE tgt.customer_id = src.customer_id
      AND tgt.is_current = TRUE
);

-- =========================================
-- STEP 2: LOAD DIM_PRODUCTS
-- =========================================

MERGE INTO curated.dim_products tgt
USING validated.products_v src
ON tgt.product_id = src.product_id

WHEN MATCHED THEN UPDATE SET
    tgt.product_name = src.product_name,
    tgt.category = src.category,
    tgt.price = src.price

WHEN NOT MATCHED THEN INSERT
(product_id, product_name, category, price)
VALUES
(src.product_id, src.product_name, src.category, src.price);

-- =========================================
-- STEP 3: FACT LOAD (FIXED LOGIC)
-- =========================================

-- VERY IMPORTANT: use LEFT JOIN to avoid data loss
INSERT INTO curated.fact_orders
SELECT 
    o.order_id,
    dc.customer_sk,
    dp.product_sk,
    o.order_date,
    oi.quantity,
    o.total_amount
FROM validated.orders_v o
LEFT JOIN validated.order_items_v oi 
    ON o.order_id = oi.order_id
LEFT JOIN curated.dim_customers dc 
    ON o.customer_id = dc.customer_id AND dc.is_current = TRUE
LEFT JOIN curated.dim_products dp 
    ON oi.product_id = dp.product_id
WHERE o.order_id IS NOT NULL;

RETURN 'CURATED LAYER LOADED (FIXED - NO DATA LOSS)';

END;
$$;

call curated.sp_load_curated_layer();

-------anamoly engine----------

CREATE OR REPLACE TABLE governance.anomaly_log (
    anomaly_id STRING DEFAULT UUID_STRING(),
    anomaly_type STRING,
    business_key STRING,
    metric_value NUMBER,
    threshold_value NUMBER,
    anomaly_flag STRING,
    anomaly_reason STRING,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

---------stored procedure for anolomy engine----------
CREATE OR REPLACE PROCEDURE analytics.sp_detect_anomalies()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- =========================================
-- STEP 1: LOAD FACT DATA
-- =========================================
CREATE OR REPLACE TEMP TABLE cur_fact_orders AS
SELECT * FROM curated.fact_orders;

-- SAFETY CHECK
IF ((SELECT COUNT(*) FROM cur_fact_orders) = 0) THEN
    RETURN 'NO DATA IN FACT TABLE';
END IF;

-- =========================================
-- STEP 2: ORDER VALUE ANOMALY (FIXED)
-- =========================================

CREATE OR REPLACE TEMP TABLE order_stats AS
SELECT 
    AVG(total_amount) AS avg_amt,
    COALESCE(STDDEV(total_amount),0) AS std_amt
FROM cur_fact_orders;

-- fallback threshold
CREATE OR REPLACE TEMP TABLE order_threshold AS
SELECT 
    CASE 
        WHEN std_amt = 0 THEN avg_amt * 1.5   -- 🔥 fallback
        ELSE avg_amt + 2 * std_amt
    END AS threshold
FROM order_stats;

INSERT INTO governance.anomaly_log
SELECT 
    UUID_STRING(),
    'HIGH_ORDER_VALUE',
    order_id,
    total_amount,
    (SELECT threshold FROM order_threshold),
    'YES',
    'Order value anomaly',
    CURRENT_TIMESTAMP()
FROM cur_fact_orders
WHERE total_amount > (SELECT threshold FROM order_threshold);

-- =========================================
-- STEP 3: QUANTITY ANOMALY (FIXED)
-- =========================================

CREATE OR REPLACE TEMP TABLE qty_stats AS
SELECT 
    AVG(quantity) AS avg_qty,
    COALESCE(STDDEV(quantity),0) AS std_qty
FROM cur_fact_orders;

CREATE OR REPLACE TEMP TABLE qty_threshold AS
SELECT 
    CASE 
        WHEN std_qty = 0 THEN avg_qty * 2
        ELSE avg_qty + 2 * std_qty
    END AS threshold
FROM qty_stats;

INSERT INTO governance.anomaly_log
SELECT 
    UUID_STRING(),
    'HIGH_QUANTITY',
    order_id,
    quantity,
    (SELECT threshold FROM qty_threshold),
    'YES',
    'Quantity anomaly',
    CURRENT_TIMESTAMP()
FROM cur_fact_orders
WHERE quantity > (SELECT threshold FROM qty_threshold);

-- =========================================
-- STEP 4: TOP 10% ANOMALY (GUARANTEED DETECTION 🔥)
-- =========================================

INSERT INTO governance.anomaly_log
SELECT 
    UUID_STRING(),
    'TOP_10_PERCENT_ORDERS',
    order_id,
    total_amount,
    NULL,
    'YES',
    'Top 10% highest orders',
    CURRENT_TIMESTAMP()
FROM (
    SELECT *,
           NTILE(10) OVER (ORDER BY total_amount DESC) AS bucket
    FROM cur_fact_orders
)
WHERE bucket = 1;

RETURN 'ANOMALY DETECTION COMPLETED WITH FALLBACK LOGIC';

END;
$$;
call analytics.sp_detect_anomalies();

----------task scheduling------------
CREATE OR REPLACE TASK VALIDATED.TASK_PROCESS_DQ
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN 
    SYSTEM$STREAM_HAS_DATA('RETAIL123_DB.RAW.CUSTOMERS_STREAM')
 OR SYSTEM$STREAM_HAS_DATA('RETAIL123_DB.RAW.PRODUCTS_STREAM')
 OR SYSTEM$STREAM_HAS_DATA('RETAIL123_DB.RAW.ORDERS_STREAM')
 OR SYSTEM$STREAM_HAS_DATA('RETAIL123_DB.RAW.ORDER_ITEMS_STREAM')
 OR SYSTEM$STREAM_HAS_DATA('RETAIL123_DB.RAW.USER_ACTIVITY_STREAM')

AS
CALL RETAIL123_DB.VALIDATED.PROCESS_RETAIL_DATA_QUALITY();

--------curated task scheduling-------
CREATE OR REPLACE TASK VALIDATED.TASK_POPULATE_CURATED
WAREHOUSE = COMPUTE_WH
AFTER VALIDATED.TASK_PROCESS_DQ

AS
CALL RETAIL123_DB.CURATED.SP_LOAD_CURATED_LAYER();

------------anamoly engine------
CREATE OR REPLACE TASK VALIDATED.TASK_ANOMALY_ENGINE
WAREHOUSE = COMPUTE_WH
AFTER VALIDATED.TASK_POPULATE_CURATED

AS
CALL RETAIL123_DB.ANALYTICS.SP_DETECT_ANOMALIES();

ALTER TASK VALIDATED.TASK_PROCESS_DQ RESUME;
ALTER TASK VALIDATED.TASK_POPULATE_CURATED RESUME;
ALTER TASK VALIDATED.TASK_ANOMALY_ENGINE RESUME;

---------------data marts-------------
create schema datamart;
CREATE OR REPLACE VIEW DATAMART.CUSTOMER_MART AS
SELECT
    customer_sk,
    customer_id,
    name,
    city,
    signup_date,
    effective_start,
    effective_end,
    is_current
FROM CURATED.DIM_CUSTOMERS
WHERE is_current = TRUE;

CREATE OR REPLACE VIEW DATAMART.ORDER_MART AS
SELECT
    fo.order_id,
    dc.customer_id,
    dp.product_id,
    fo.order_date,
    fo.quantity,
    fo.total_amount,

 -- Derived column
    (fo.total_amount / NULLIF(fo.quantity,0)) AS price_per_unit

FROM CURATED.FACT_ORDERS fo

JOIN CURATED.DIM_CUSTOMERS dc
    ON fo.customer_sk = dc.customer_sk

JOIN CURATED.DIM_PRODUCTS dp
    ON fo.product_sk = dp.product_sk;

    CREATE OR REPLACE VIEW DATAMART.PRODUCT_MART AS
SELECT
    dp.product_id,
    dp.product_name,
    dp.category,

    DATE(fo.order_date) AS sales_date,

    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.quantity) AS total_quantity_sold,
    SUM(fo.total_amount) AS total_revenue

FROM CURATED.DIM_PRODUCTS dp

LEFT JOIN CURATED.FACT_ORDERS fo
    ON dp.product_sk = fo.product_sk

GROUP BY
    dp.product_id,
    dp.product_name,
    dp.category,
    DATE(fo.order_date);

    CREATE OR REPLACE VIEW DATAMART.CUSTOMER_ACTIVITY_MART AS
SELECT
    dc.customer_id,
    dc.name,
    dc.city,

    DATE(fo.order_date) AS activity_date,

    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.total_amount) AS total_spent,

    -- Derived KPI
    AVG(fo.total_amount) AS avg_order_value

FROM CURATED.DIM_CUSTOMERS dc

LEFT JOIN CURATED.FACT_ORDERS fo
    ON dc.customer_sk = fo.customer_sk

GROUP BY
    dc.customer_id,
    dc.name,
    dc.city,
    DATE(fo.order_date);

    
CREATE OR REPLACE VIEW DATAMART.ANOMALY_MART AS
SELECT
    anomaly_id,
    anomaly_type,
    business_key,
    metric_value,
    threshold_value,
    anomaly_flag,
    anomaly_reason,
    detected_at,

    -- Derived flag
    CASE 
        WHEN anomaly_flag = 'YES' THEN 1
        ELSE 0
    END AS anomaly_indicator

FROM GOVERNANCE.ANOMALY_LOG;

----------kpis----------
----kpi1------
CREATE OR REPLACE VIEW ANALYTICS.CUSTOMER_360 AS
WITH base AS (
    SELECT
        cm.customer_id,
        cm.name,
        cm.city,
        cm.signup_date,
        om.order_id,
        om.total_amount,
        om.order_date,
        am.anomaly_id
    FROM DATAMART.CUSTOMER_MART cm
    LEFT JOIN DATAMART.ORDER_MART om
        ON cm.customer_id = om.customer_id
    LEFT JOIN DATAMART.ANOMALY_MART am
        ON cm.customer_id = am.business_key
),

agg AS (
    SELECT
        customer_id,
        name,
        city,
        signup_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS total_spent,
        AVG(total_amount) AS avg_order_value,
        MAX(order_date) AS last_order_date,
        COUNT(DISTINCT anomaly_id) AS anomaly_count
    FROM base
    GROUP BY customer_id, name, city, signup_date
),

-- GLOBAL AVERAGES (FOR FILLING)
global_vals AS (
    SELECT
        AVG(total_orders) AS avg_orders,
        AVG(total_spent) AS avg_spent,
        AVG(avg_order_value) AS avg_aov
    FROM agg
    WHERE total_orders IS NOT NULL
)

SELECT
    a.customer_id,
    a.name,
    a.city,
    a.signup_date,

    -- Replace NULL with global averages
    COALESCE(a.total_orders, gv.avg_orders) AS total_orders,

    COALESCE(a.total_spent, gv.avg_spent) AS total_spent,

    COALESCE(a.avg_order_value, gv.avg_aov) AS avg_order_value,

    -- Replace NULL with signup_date (logical fallback)
    COALESCE(a.last_order_date, a.signup_date) AS last_order_date,

    COALESCE(a.anomaly_count, 1) AS anomaly_count

FROM agg a
CROSS JOIN global_vals gv;

----kpi2--------
CREATE OR REPLACE VIEW ANALYTICS.CUSTOMER_SEGMENTATION AS
SELECT
    customer_id,
    name,
    total_orders,
    total_spent,

    CASE 
        WHEN total_spent > 10000 THEN 'HIGH VALUE'
        WHEN total_spent BETWEEN 5000 AND 10000 THEN 'MEDIUM VALUE'
        ELSE 'LOW VALUE'
    END AS value_segment,

    CASE 
        WHEN total_orders >= 10 THEN 'FREQUENT'
        ELSE 'OCCASIONAL'
    END AS frequency_segment

FROM ANALYTICS.CUSTOMER_360;


------kpi3-------
CREATE OR REPLACE VIEW ANALYTICS.SALES_INSIGHTS AS
SELECT
    product_id,
    product_name,
    category,

    SUM(total_quantity_sold) AS total_quantity,
    SUM(total_revenue) AS total_revenue,

    AVG(total_revenue) AS avg_revenue_per_day

FROM DATAMART.PRODUCT_MART

GROUP BY
    product_id, product_name, category;


---kpi4------
CREATE OR REPLACE VIEW ANALYTICS.CHURN_CUSTOMERS AS
SELECT
    customer_id,
    name,
    last_order_date,

    DATEDIFF(DAY, last_order_date, CURRENT_DATE()) AS days_inactive,

    CASE 
        WHEN DATEDIFF(DAY, last_order_date, CURRENT_DATE()) > 30 THEN 'CHURNED'
        ELSE 'ACTIVE'
    END AS churn_status

FROM ANALYTICS.CUSTOMER_360;


-----kpi5--------
CREATE OR REPLACE VIEW ANALYTICS.RECOMMENDATIONS AS
WITH popular_products AS (
    SELECT 
        product_id,
        SUM(total_quantity_sold) AS qty,
        RANK() OVER (ORDER BY SUM(total_quantity_sold) DESC) rnk
    FROM DATAMART.PRODUCT_MART
    GROUP BY product_id
),
customer_products AS (
    SELECT DISTINCT customer_id, product_id
    FROM DATAMART.ORDER_MART
)

SELECT
    cm.customer_id,
    pp.product_id AS recommended_product

FROM DATAMART.CUSTOMER_MART cm

JOIN popular_products pp
    ON pp.rnk <= 5   -- top 5 products

LEFT JOIN customer_products cp
    ON cm.customer_id = cp.customer_id
   AND pp.product_id = cp.product_id

WHERE cp.product_id IS NULL;

select * from ANALYTICS.CUSTOMER_360;
select * from ANALYTICS.CUSTOMER_SEGMENTATION;
select * from ANALYTICS.SALES_INSIGHTS;
select * from ANALYTICS.CHURN_CUSTOMERS;
select * from ANALYTICS.RECOMMENDATIONS;