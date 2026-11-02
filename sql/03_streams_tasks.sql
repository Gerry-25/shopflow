/* =============================================================================
   ShopFlow — JOUR 3 : Pipeline automatisé — Streams & Tasks
   Livrable : sql/03_streams_tasks.sql
   -----------------------------------------------------------------------------
   Contenu
     1. Streams (CDC) sur les tables RAW
     2. Tables STAGING typées + fonction utilitaire FN_LOAD_BATCH
     3. Requêtes de transformation (MERGE consommant les streams)
     4. Graphe de Tasks (root planifiée toutes les 5 min + tasks AFTER)
     5. Démarrage, 1re exécution (historique J1), monitoring
     6. TEST DYNAMIQUE : arrivée du lot J2

   Graphe de tasks (DAG)

        TSK_LOAD_STG_PRODUCTS  (root, SCHEDULE 5 min, WHEN streams non vides)
          ├── TSK_LOAD_STG_WEB_EVENTS        (enrichi avec la catégorie produit)
          └── TSK_LOAD_STG_CUSTOMERS
                └── TSK_LOAD_STG_ORDERS      (enrichi avec la date d'inscription)
                      └── TSK_LOAD_STG_ORDER_ITEMS (enrichi produit, rattaché à la commande)

   Pourquoi PRODUCTS en root : les référentiels (produits, clients) doivent être
   à jour AVANT les faits qui s'y joignent (lignes de commande, événements).

   ⚠ Piège du dataset : le lot J2 réutilise les identifiants de J1
     (orders_j2.csv commence à O0000001, web_events_j2.json à E000000001) avec
     des contenus différents. Une clé « order_id » seule ferait écraser 5 000
     commandes J1 par le MERGE. On utilise une clé technique
     ORDER_KEY = <lot>-<order_id>, le lot étant déduit du nom de fichier.
   ============================================================================= */

USE ROLE SHOPFLOW_ENGINEER;
USE WAREHOUSE WH_TRANSFORM;
USE DATABASE SHOPFLOW_DB;


/* -----------------------------------------------------------------------------
   1. STREAMS
   SHOW_INITIAL_ROWS = TRUE : à sa première lecture, le stream renvoie aussi les
   lignes déjà présentes (lot J1). Sans cela, un stream créé après le Jour 1/2
   serait vide et STAGING ne recevrait jamais l'historique.
   Streams « standard » (pas APPEND_ONLY) : ils captent aussi les DELETE /
   UPDATE, ce qui propage l'exercice Time Travel du Jour 4 jusqu'aux marts.
   ----------------------------------------------------------------------------- */
CREATE STREAM IF NOT EXISTS RAW.STR_ORDERS
  ON TABLE RAW.ORDERS_RAW      SHOW_INITIAL_ROWS = TRUE COMMENT = 'CDC commandes';
CREATE STREAM IF NOT EXISTS RAW.STR_ORDER_ITEMS
  ON TABLE RAW.ORDER_ITEMS_RAW SHOW_INITIAL_ROWS = TRUE COMMENT = 'CDC lignes de commande';
CREATE STREAM IF NOT EXISTS RAW.STR_WEB_EVENTS
  ON TABLE RAW.WEB_EVENTS_RAW  SHOW_INITIAL_ROWS = TRUE COMMENT = 'CDC événements web';
-- Référentiels : mêmes mécanismes, pour un pipeline homogène
CREATE STREAM IF NOT EXISTS RAW.STR_PRODUCTS
  ON TABLE RAW.PRODUCTS_RAW    SHOW_INITIAL_ROWS = TRUE COMMENT = 'CDC catalogue';
CREATE STREAM IF NOT EXISTS RAW.STR_CUSTOMERS
  ON TABLE RAW.CUSTOMERS_RAW   SHOW_INITIAL_ROWS = TRUE COMMENT = 'CDC clients';

SHOW STREAMS IN SCHEMA RAW;

-- Lire un stream NE le consomme PAS (seul un DML validé avance son offset)
SELECT METADATA$ACTION, METADATA$ISUPDATE, COUNT(*) FROM RAW.STR_ORDERS GROUP BY 1, 2;


/* -----------------------------------------------------------------------------
   2. TABLES STAGING
   ----------------------------------------------------------------------------- */
-- Lot de chargement déduit du nom de fichier : orders_j2.csv(.gz) -> 'J2',
-- fichier sans suffixe (orders.csv) -> 'J1'. Indépendant de la compression.
CREATE OR REPLACE FUNCTION STAGING.FN_LOAD_BATCH(SOURCE_FILE VARCHAR)
RETURNS VARCHAR
AS
$$
  COALESCE(UPPER(REGEXP_SUBSTR(SOURCE_FILE, '_(j[0-9]+)[.]', 1, 1, 'ie', 1)), 'J1')
$$;

SELECT STAGING.FN_LOAD_BATCH('orders/orders.csv.gz')        AS j1,   -- J1
       STAGING.FN_LOAD_BATCH('orders/orders_j2.csv.gz')     AS j2,   -- J2
       STAGING.FN_LOAD_BATCH('order_items/order_items.csv') AS j1b;  -- J1

CREATE TABLE IF NOT EXISTS STAGING.STG_PRODUCTS (
  PRODUCT_ID      VARCHAR       NOT NULL,
  PRODUCT_NAME    VARCHAR,
  CATEGORY        VARCHAR,
  BRAND           VARCHAR,
  PRICE           NUMBER(10,2),
  COLOR           VARCHAR,
  WARRANTY_MONTHS INTEGER,
  WEIGHT_G        INTEGER,
  TAGS            ARRAY,
  IS_PROMO        BOOLEAN,
  IS_BESTSELLER   BOOLEAN,
  SOURCE_FILE     VARCHAR,
  LOADED_AT       TIMESTAMP_LTZ,
  UPDATED_AT      TIMESTAMP_LTZ,
  CONSTRAINT PK_STG_PRODUCTS PRIMARY KEY (PRODUCT_ID)   -- informative (non appliquée)
);

CREATE TABLE IF NOT EXISTS STAGING.STG_CUSTOMERS (
  CUSTOMER_ID  VARCHAR NOT NULL,
  EMAIL        VARCHAR,
  FIRST_NAME   VARCHAR,
  LAST_NAME    VARCHAR,
  CITY         VARCHAR,
  SIGNUP_DATE  DATE,
  SIGNUP_MONTH DATE,
  SOURCE_FILE  VARCHAR,
  LOADED_AT    TIMESTAMP_LTZ,
  UPDATED_AT   TIMESTAMP_LTZ,
  CONSTRAINT PK_STG_CUSTOMERS PRIMARY KEY (CUSTOMER_ID)
);

CREATE TABLE IF NOT EXISTS STAGING.STG_ORDERS (
  ORDER_KEY              VARCHAR NOT NULL,   -- <lot>-<order_id>, ex. J2-O0000001
  ORDER_ID               VARCHAR,
  LOAD_BATCH             VARCHAR,
  CUSTOMER_ID            VARCHAR,
  ORDER_TS               TIMESTAMP_NTZ,
  ORDER_DATE             DATE,
  STATUS                 VARCHAR,            -- completed | pending | cancelled | refunded | unknown
  TOTAL_AMOUNT           NUMBER(12,2),
  IS_REVENUE             BOOLEAN,            -- TRUE si status = completed (définition du CA)
  CUSTOMER_SIGNUP_DATE   DATE,
  DQ_UNKNOWN_CUSTOMER    BOOLEAN,
  DQ_ORDER_BEFORE_SIGNUP BOOLEAN,            -- anomalie source (~10 % des commandes J1)
  SOURCE_FILE            VARCHAR,
  LOADED_AT              TIMESTAMP_LTZ,
  UPDATED_AT             TIMESTAMP_LTZ,      -- date de traitement par le pipeline
  CONSTRAINT PK_STG_ORDERS PRIMARY KEY (ORDER_KEY)
);

CREATE TABLE IF NOT EXISTS STAGING.STG_ORDER_ITEMS (
  ORDER_ITEM_KEY VARCHAR NOT NULL,           -- <lot>-<order_id>-<n° de ligne fichier>
  ORDER_KEY      VARCHAR,
  ORDER_ID       VARCHAR,
  LOAD_BATCH     VARCHAR,
  PRODUCT_ID     VARCHAR,
  QUANTITY       INTEGER,
  UNIT_PRICE     NUMBER(10,2),
  LINE_AMOUNT    NUMBER(12,2),
  PRODUCT_NAME   VARCHAR,
  CATEGORY       VARCHAR,
  BRAND          VARCHAR,
  SOURCE_FILE    VARCHAR,
  LOADED_AT      TIMESTAMP_LTZ,
  UPDATED_AT     TIMESTAMP_LTZ,
  CONSTRAINT PK_STG_ORDER_ITEMS PRIMARY KEY (ORDER_ITEM_KEY)
);

CREATE TABLE IF NOT EXISTS STAGING.STG_WEB_EVENTS (
  EVENT_KEY        VARCHAR NOT NULL,         -- <lot>-<event_id>
  EVENT_ID         VARCHAR,
  LOAD_BATCH       VARCHAR,
  USER_ID          VARCHAR,                  -- NULL = visiteur anonyme
  IS_ANONYMOUS     BOOLEAN,
  SESSION_ID       VARCHAR,
  EVENT_TYPE       VARCHAR,
  PRODUCT_ID       VARCHAR,
  PRODUCT_CATEGORY VARCHAR,
  EVENT_TS         TIMESTAMP_NTZ,
  EVENT_DATE       DATE,
  DEVICE           VARCHAR,
  REFERRER         VARCHAR,                  -- NULL source -> 'direct'
  USER_AGENT       VARCHAR,
  IP_ADDRESS       VARCHAR,                  -- donnée personnelle (RGPD)
  SOURCE_FILE      VARCHAR,
  LOADED_AT        TIMESTAMP_LTZ,
  UPDATED_AT       TIMESTAMP_LTZ,
  CONSTRAINT PK_STG_WEB_EVENTS PRIMARY KEY (EVENT_KEY)
);


/* -----------------------------------------------------------------------------
   3 & 4. TRANSFORMATIONS ENCAPSULÉES DANS LES TASKS

   Modèle commun à chaque MERGE :
     - source = stream (+ jointures d'enrichissement sur STAGING)
     - nettoyage : TRIM, LOWER, TRY_TO_* (une valeur invalide devient NULL au
       lieu de faire échouer la task), COALESCE des NULL métier
     - lignes sans clé ou sans date valide : écartées
     - dédoublonnage dans le lot : 1 ligne par clé (QUALIFY ROW_NUMBER), en
       privilégiant INSERT sur DELETE (un UPDATE = DELETE + INSERT dans un stream)
     - DELETE dans RAW => DELETE dans STAGING ; INSERT => upsert
   ----------------------------------------------------------------------------- */

-- Les tasks d'un graphe doivent être suspendues pour être (re)créées
ALTER TASK IF EXISTS STAGING.TSK_LOAD_STG_PRODUCTS SUSPEND;

-- 4.1  ROOT : produits ---------------------------------------------------------
CREATE OR REPLACE TASK STAGING.TSK_LOAD_STG_PRODUCTS
  WAREHOUSE = WH_TRANSFORM
  SCHEDULE  = '5 MINUTE'
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
  COMMENT   = 'Root du pipeline ShopFlow : RAW.PRODUCTS_RAW -> STAGING.STG_PRODUCTS'
WHEN
     SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_PRODUCTS')
  OR SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_CUSTOMERS')
  OR SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDERS')
  OR SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDER_ITEMS')
  OR SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_WEB_EVENTS')
AS
MERGE INTO STAGING.STG_PRODUCTS t
USING (
  SELECT TRIM(DATA:product_id::VARCHAR)                         AS PRODUCT_ID,
         TRIM(DATA:name::VARCHAR)                               AS PRODUCT_NAME,
         TRIM(DATA:category::VARCHAR)                           AS CATEGORY,
         TRIM(DATA:brand::VARCHAR)                              AS BRAND,
         TRY_TO_NUMBER(DATA:price::VARCHAR, 10, 2)              AS PRICE,
         LOWER(DATA:attributes:color::VARCHAR)                  AS COLOR,
         TRY_TO_NUMBER(DATA:attributes:warranty_months::VARCHAR) AS WARRANTY_MONTHS,
         TRY_TO_NUMBER(DATA:attributes:weight_g::VARCHAR)       AS WEIGHT_G,
         COALESCE(DATA:tags::ARRAY, ARRAY_CONSTRUCT())          AS TAGS,
         ARRAY_CONTAINS('promo'::VARIANT,      COALESCE(DATA:tags::ARRAY, ARRAY_CONSTRUCT())) AS IS_PROMO,
         ARRAY_CONTAINS('bestseller'::VARIANT, COALESCE(DATA:tags::ARRAY, ARRAY_CONSTRUCT())) AS IS_BESTSELLER,
         SOURCE_FILE, LOADED_AT,
         METADATA$ACTION                                        AS CDC_ACTION
  FROM RAW.STR_PRODUCTS
  WHERE DATA:product_id::VARCHAR IS NOT NULL   -- JSON null != SQL NULL : on caste avant le test
  QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID
                             ORDER BY IFF(CDC_ACTION = 'INSERT', 0, 1), LOADED_AT DESC) = 1
) s
ON t.PRODUCT_ID = s.PRODUCT_ID
WHEN MATCHED AND s.CDC_ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  PRODUCT_NAME = s.PRODUCT_NAME, CATEGORY = s.CATEGORY, BRAND = s.BRAND, PRICE = s.PRICE,
  COLOR = s.COLOR, WARRANTY_MONTHS = s.WARRANTY_MONTHS, WEIGHT_G = s.WEIGHT_G, TAGS = s.TAGS,
  IS_PROMO = s.IS_PROMO, IS_BESTSELLER = s.IS_BESTSELLER,
  SOURCE_FILE = s.SOURCE_FILE, LOADED_AT = s.LOADED_AT, UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.CDC_ACTION = 'INSERT' THEN INSERT
  (PRODUCT_ID, PRODUCT_NAME, CATEGORY, BRAND, PRICE, COLOR, WARRANTY_MONTHS, WEIGHT_G, TAGS,
   IS_PROMO, IS_BESTSELLER, SOURCE_FILE, LOADED_AT, UPDATED_AT)
VALUES
  (s.PRODUCT_ID, s.PRODUCT_NAME, s.CATEGORY, s.BRAND, s.PRICE, s.COLOR, s.WARRANTY_MONTHS, s.WEIGHT_G, s.TAGS,
   s.IS_PROMO, s.IS_BESTSELLER, s.SOURCE_FILE, s.LOADED_AT, CURRENT_TIMESTAMP());

-- 4.2  Clients (AFTER produits) -------------------------------------------------
CREATE OR REPLACE TASK STAGING.TSK_LOAD_STG_CUSTOMERS
  WAREHOUSE = WH_TRANSFORM
  COMMENT   = 'RAW.CUSTOMERS_RAW -> STAGING.STG_CUSTOMERS'
  AFTER STAGING.TSK_LOAD_STG_PRODUCTS
AS
MERGE INTO STAGING.STG_CUSTOMERS t
USING (
  SELECT TRIM(CUSTOMER_ID)                                         AS CUSTOMER_ID,
         LOWER(TRIM(EMAIL))                                        AS EMAIL,
         TRIM(FIRST_NAME)                                          AS FIRST_NAME,
         TRIM(LAST_NAME)                                           AS LAST_NAME,
         COALESCE(TRIM(CITY), 'Inconnue')                          AS CITY,
         TRY_TO_DATE(SIGNUP_DATE, 'YYYY-MM-DD')                    AS SIGNUP_DATE,
         DATE_TRUNC('MONTH', TRY_TO_DATE(SIGNUP_DATE, 'YYYY-MM-DD')) AS SIGNUP_MONTH,
         SOURCE_FILE, LOADED_AT,
         METADATA$ACTION                                           AS CDC_ACTION
  FROM RAW.STR_CUSTOMERS
  WHERE CUSTOMER_ID IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CUSTOMER_ID)
                             ORDER BY IFF(CDC_ACTION = 'INSERT', 0, 1), LOADED_AT DESC) = 1
) s
ON t.CUSTOMER_ID = s.CUSTOMER_ID
WHEN MATCHED AND s.CDC_ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  EMAIL = s.EMAIL, FIRST_NAME = s.FIRST_NAME, LAST_NAME = s.LAST_NAME, CITY = s.CITY,
  SIGNUP_DATE = s.SIGNUP_DATE, SIGNUP_MONTH = s.SIGNUP_MONTH,
  SOURCE_FILE = s.SOURCE_FILE, LOADED_AT = s.LOADED_AT, UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.CDC_ACTION = 'INSERT' THEN INSERT
  (CUSTOMER_ID, EMAIL, FIRST_NAME, LAST_NAME, CITY, SIGNUP_DATE, SIGNUP_MONTH, SOURCE_FILE, LOADED_AT, UPDATED_AT)
VALUES
  (s.CUSTOMER_ID, s.EMAIL, s.FIRST_NAME, s.LAST_NAME, s.CITY, s.SIGNUP_DATE, s.SIGNUP_MONTH,
   s.SOURCE_FILE, s.LOADED_AT, CURRENT_TIMESTAMP());

-- 4.3  Commandes (AFTER clients) ------------------------------------------------
CREATE OR REPLACE TASK STAGING.TSK_LOAD_STG_ORDERS
  WAREHOUSE = WH_TRANSFORM
  COMMENT   = 'RAW.ORDERS_RAW -> STAGING.STG_ORDERS (typage, statut, enrichissement client)'
  AFTER STAGING.TSK_LOAD_STG_CUSTOMERS
AS
MERGE INTO STAGING.STG_ORDERS t
USING (
  SELECT STAGING.FN_LOAD_BATCH(o.SOURCE_FILE) || '-' || TRIM(o.ORDER_ID)   AS ORDER_KEY,
         TRIM(o.ORDER_ID)                                                  AS ORDER_ID,
         STAGING.FN_LOAD_BATCH(o.SOURCE_FILE)                              AS LOAD_BATCH,
         TRIM(o.CUSTOMER_ID)                                               AS CUSTOMER_ID,
         TRY_TO_TIMESTAMP_NTZ(o.ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS')       AS ORDER_TS,
         TO_DATE(TRY_TO_TIMESTAMP_NTZ(o.ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS')) AS ORDER_DATE,
         COALESCE(LOWER(TRIM(o.STATUS)), 'unknown')                        AS STATUS,
         TRY_TO_NUMBER(o.TOTAL_AMOUNT, 12, 2)                              AS TOTAL_AMOUNT,
         COALESCE(LOWER(TRIM(o.STATUS)), 'unknown') = 'completed'          AS IS_REVENUE,
         c.SIGNUP_DATE                                                     AS CUSTOMER_SIGNUP_DATE,
         c.CUSTOMER_ID IS NULL                                             AS DQ_UNKNOWN_CUSTOMER,
         COALESCE(TO_DATE(TRY_TO_TIMESTAMP_NTZ(o.ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS')) < c.SIGNUP_DATE, FALSE)
                                                                           AS DQ_ORDER_BEFORE_SIGNUP,
         o.SOURCE_FILE, o.LOADED_AT,
         o.METADATA$ACTION                                                 AS CDC_ACTION
  FROM RAW.STR_ORDERS o
  LEFT JOIN STAGING.STG_CUSTOMERS c ON c.CUSTOMER_ID = TRIM(o.CUSTOMER_ID)
  WHERE o.ORDER_ID IS NOT NULL
    AND TRY_TO_TIMESTAMP_NTZ(o.ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS') IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_KEY
                             ORDER BY IFF(CDC_ACTION = 'INSERT', 0, 1), o.LOADED_AT DESC) = 1
) s
ON t.ORDER_KEY = s.ORDER_KEY
WHEN MATCHED AND s.CDC_ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  ORDER_ID = s.ORDER_ID, LOAD_BATCH = s.LOAD_BATCH, CUSTOMER_ID = s.CUSTOMER_ID,
  ORDER_TS = s.ORDER_TS, ORDER_DATE = s.ORDER_DATE, STATUS = s.STATUS,
  TOTAL_AMOUNT = s.TOTAL_AMOUNT, IS_REVENUE = s.IS_REVENUE,
  CUSTOMER_SIGNUP_DATE = s.CUSTOMER_SIGNUP_DATE, DQ_UNKNOWN_CUSTOMER = s.DQ_UNKNOWN_CUSTOMER,
  DQ_ORDER_BEFORE_SIGNUP = s.DQ_ORDER_BEFORE_SIGNUP,
  SOURCE_FILE = s.SOURCE_FILE, LOADED_AT = s.LOADED_AT, UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.CDC_ACTION = 'INSERT' THEN INSERT
  (ORDER_KEY, ORDER_ID, LOAD_BATCH, CUSTOMER_ID, ORDER_TS, ORDER_DATE, STATUS, TOTAL_AMOUNT,
   IS_REVENUE, CUSTOMER_SIGNUP_DATE, DQ_UNKNOWN_CUSTOMER, DQ_ORDER_BEFORE_SIGNUP,
   SOURCE_FILE, LOADED_AT, UPDATED_AT)
VALUES
  (s.ORDER_KEY, s.ORDER_ID, s.LOAD_BATCH, s.CUSTOMER_ID, s.ORDER_TS, s.ORDER_DATE, s.STATUS, s.TOTAL_AMOUNT,
   s.IS_REVENUE, s.CUSTOMER_SIGNUP_DATE, s.DQ_UNKNOWN_CUSTOMER, s.DQ_ORDER_BEFORE_SIGNUP,
   s.SOURCE_FILE, s.LOADED_AT, CURRENT_TIMESTAMP());

-- 4.4  Lignes de commande (AFTER commandes) -------------------------------------
-- Pas de dédoublonnage sur (order_id, product_id) : un produit peut apparaître
-- sur plusieurs lignes d'une même commande. La ligne est identifiée par son
-- numéro de ligne dans le fichier source.
CREATE OR REPLACE TASK STAGING.TSK_LOAD_STG_ORDER_ITEMS
  WAREHOUSE = WH_TRANSFORM
  COMMENT   = 'RAW.ORDER_ITEMS_RAW -> STAGING.STG_ORDER_ITEMS (montant ligne, enrichissement produit)'
  AFTER STAGING.TSK_LOAD_STG_ORDERS
AS
MERGE INTO STAGING.STG_ORDER_ITEMS t
USING (
  SELECT STAGING.FN_LOAD_BATCH(i.SOURCE_FILE) || '-' || TRIM(i.ORDER_ID) || '-' || i.FILE_ROW_NUMBER
                                                                  AS ORDER_ITEM_KEY,
         STAGING.FN_LOAD_BATCH(i.SOURCE_FILE) || '-' || TRIM(i.ORDER_ID) AS ORDER_KEY,
         TRIM(i.ORDER_ID)                                         AS ORDER_ID,
         STAGING.FN_LOAD_BATCH(i.SOURCE_FILE)                     AS LOAD_BATCH,
         TRIM(i.PRODUCT_ID)                                       AS PRODUCT_ID,
         TRY_TO_NUMBER(i.QUANTITY)                                AS QUANTITY,
         TRY_TO_NUMBER(i.UNIT_PRICE, 10, 2)                       AS UNIT_PRICE,
         TRY_TO_NUMBER(i.QUANTITY) * TRY_TO_NUMBER(i.UNIT_PRICE, 10, 2) AS LINE_AMOUNT,
         p.PRODUCT_NAME, p.CATEGORY, p.BRAND,
         i.SOURCE_FILE, i.LOADED_AT,
         i.METADATA$ACTION                                        AS CDC_ACTION
  FROM RAW.STR_ORDER_ITEMS i
  LEFT JOIN STAGING.STG_PRODUCTS p ON p.PRODUCT_ID = TRIM(i.PRODUCT_ID)
  WHERE i.ORDER_ID IS NOT NULL
    AND i.PRODUCT_ID IS NOT NULL
    AND COALESCE(TRY_TO_NUMBER(i.QUANTITY), 0) > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ITEM_KEY
                             ORDER BY IFF(CDC_ACTION = 'INSERT', 0, 1), i.LOADED_AT DESC) = 1
) s
ON t.ORDER_ITEM_KEY = s.ORDER_ITEM_KEY
WHEN MATCHED AND s.CDC_ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  ORDER_KEY = s.ORDER_KEY, ORDER_ID = s.ORDER_ID, LOAD_BATCH = s.LOAD_BATCH, PRODUCT_ID = s.PRODUCT_ID,
  QUANTITY = s.QUANTITY, UNIT_PRICE = s.UNIT_PRICE, LINE_AMOUNT = s.LINE_AMOUNT,
  PRODUCT_NAME = s.PRODUCT_NAME, CATEGORY = s.CATEGORY, BRAND = s.BRAND,
  SOURCE_FILE = s.SOURCE_FILE, LOADED_AT = s.LOADED_AT, UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.CDC_ACTION = 'INSERT' THEN INSERT
  (ORDER_ITEM_KEY, ORDER_KEY, ORDER_ID, LOAD_BATCH, PRODUCT_ID, QUANTITY, UNIT_PRICE, LINE_AMOUNT,
   PRODUCT_NAME, CATEGORY, BRAND, SOURCE_FILE, LOADED_AT, UPDATED_AT)
VALUES
  (s.ORDER_ITEM_KEY, s.ORDER_KEY, s.ORDER_ID, s.LOAD_BATCH, s.PRODUCT_ID, s.QUANTITY, s.UNIT_PRICE, s.LINE_AMOUNT,
   s.PRODUCT_NAME, s.CATEGORY, s.BRAND, s.SOURCE_FILE, s.LOADED_AT, CURRENT_TIMESTAMP());

-- 4.5  Événements web (AFTER produits) ------------------------------------------
CREATE OR REPLACE TASK STAGING.TSK_LOAD_STG_WEB_EVENTS
  WAREHOUSE = WH_TRANSFORM
  COMMENT   = 'RAW.WEB_EVENTS_RAW -> STAGING.STG_WEB_EVENTS (parsing VARIANT, enrichissement produit)'
  AFTER STAGING.TSK_LOAD_STG_PRODUCTS
AS
MERGE INTO STAGING.STG_WEB_EVENTS t
USING (
  SELECT STAGING.FN_LOAD_BATCH(e.SOURCE_FILE) || '-' || e.DATA:event_id::VARCHAR AS EVENT_KEY,
         e.DATA:event_id::VARCHAR                                    AS EVENT_ID,
         STAGING.FN_LOAD_BATCH(e.SOURCE_FILE)                        AS LOAD_BATCH,
         e.DATA:user_id::VARCHAR                                     AS USER_ID,
         e.DATA:user_id::VARCHAR IS NULL                             AS IS_ANONYMOUS,
         e.DATA:session_id::VARCHAR                                  AS SESSION_ID,
         LOWER(e.DATA:event_type::VARCHAR)                           AS EVENT_TYPE,
         e.DATA:product_id::VARCHAR                                  AS PRODUCT_ID,
         p.CATEGORY                                                  AS PRODUCT_CATEGORY,
         TRY_TO_TIMESTAMP_NTZ(e.DATA:"timestamp"::VARCHAR)           AS EVENT_TS,
         TO_DATE(TRY_TO_TIMESTAMP_NTZ(e.DATA:"timestamp"::VARCHAR))  AS EVENT_DATE,
         COALESCE(LOWER(e.DATA:device::VARCHAR), 'unknown')          AS DEVICE,
         COALESCE(e.DATA:context:referrer::VARCHAR, 'direct')        AS REFERRER,
         e.DATA:context:user_agent::VARCHAR                          AS USER_AGENT,
         e.DATA:context:ip::VARCHAR                                  AS IP_ADDRESS,
         e.SOURCE_FILE, e.LOADED_AT,
         e.METADATA$ACTION                                           AS CDC_ACTION
  FROM RAW.STR_WEB_EVENTS e
  LEFT JOIN STAGING.STG_PRODUCTS p ON p.PRODUCT_ID = e.DATA:product_id::VARCHAR
  WHERE e.DATA:event_id::VARCHAR IS NOT NULL
    AND TRY_TO_TIMESTAMP_NTZ(e.DATA:"timestamp"::VARCHAR) IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_KEY
                             ORDER BY IFF(CDC_ACTION = 'INSERT', 0, 1), e.LOADED_AT DESC) = 1
) s
ON t.EVENT_KEY = s.EVENT_KEY
WHEN MATCHED AND s.CDC_ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  EVENT_ID = s.EVENT_ID, LOAD_BATCH = s.LOAD_BATCH, USER_ID = s.USER_ID, IS_ANONYMOUS = s.IS_ANONYMOUS,
  SESSION_ID = s.SESSION_ID, EVENT_TYPE = s.EVENT_TYPE, PRODUCT_ID = s.PRODUCT_ID,
  PRODUCT_CATEGORY = s.PRODUCT_CATEGORY, EVENT_TS = s.EVENT_TS, EVENT_DATE = s.EVENT_DATE,
  DEVICE = s.DEVICE, REFERRER = s.REFERRER, USER_AGENT = s.USER_AGENT, IP_ADDRESS = s.IP_ADDRESS,
  SOURCE_FILE = s.SOURCE_FILE, LOADED_AT = s.LOADED_AT, UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.CDC_ACTION = 'INSERT' THEN INSERT
  (EVENT_KEY, EVENT_ID, LOAD_BATCH, USER_ID, IS_ANONYMOUS, SESSION_ID, EVENT_TYPE, PRODUCT_ID,
   PRODUCT_CATEGORY, EVENT_TS, EVENT_DATE, DEVICE, REFERRER, USER_AGENT, IP_ADDRESS,
   SOURCE_FILE, LOADED_AT, UPDATED_AT)
VALUES
  (s.EVENT_KEY, s.EVENT_ID, s.LOAD_BATCH, s.USER_ID, s.IS_ANONYMOUS, s.SESSION_ID, s.EVENT_TYPE, s.PRODUCT_ID,
   s.PRODUCT_CATEGORY, s.EVENT_TS, s.EVENT_DATE, s.DEVICE, s.REFERRER, s.USER_AGENT, s.IP_ADDRESS,
   s.SOURCE_FILE, s.LOADED_AT, CURRENT_TIMESTAMP());


/* -----------------------------------------------------------------------------
   5. DÉMARRAGE & PREMIÈRE EXÉCUTION
   ----------------------------------------------------------------------------- */
-- Une task est créée SUSPENDUE. Cette fonction active la root ET tous ses
-- descendants (sinon : ALTER TASK ... RESUME des enfants d'abord, root en dernier).
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('SHOPFLOW_DB.STAGING.TSK_LOAD_STG_PRODUCTS');

SHOW TASKS IN SCHEMA STAGING;   -- state = started pour les 5 tasks

-- Pas envie d'attendre 5 minutes ? On déclenche le graphe immédiatement.
EXECUTE TASK STAGING.TSK_LOAD_STG_PRODUCTS;

-- Suivi des exécutions (relancer jusqu'à voir les 5 tasks en SUCCEEDED)
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
       SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -1, CURRENT_TIMESTAMP()),
       RESULT_LIMIT => 50))
ORDER BY SCHEDULED_TIME DESC;

-- Résultat attendu après le lot J1 :
--   STG_PRODUCTS 500 | STG_CUSTOMERS 10 000 | STG_ORDERS 50 000
--   STG_ORDER_ITEMS 127 827 | STG_WEB_EVENTS 200 000
SELECT 'STG_PRODUCTS' AS table_name, COUNT(*) AS nb FROM STAGING.STG_PRODUCTS
UNION ALL SELECT 'STG_CUSTOMERS',   COUNT(*) FROM STAGING.STG_CUSTOMERS
UNION ALL SELECT 'STG_ORDERS',      COUNT(*) FROM STAGING.STG_ORDERS
UNION ALL SELECT 'STG_ORDER_ITEMS', COUNT(*) FROM STAGING.STG_ORDER_ITEMS
UNION ALL SELECT 'STG_WEB_EVENTS',  COUNT(*) FROM STAGING.STG_WEB_EVENTS;

-- Les streams ont été consommés : FALSE partout
SELECT SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDERS')      AS orders,
       SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_ORDER_ITEMS') AS order_items,
       SYSTEM$STREAM_HAS_DATA('SHOPFLOW_DB.RAW.STR_WEB_EVENTS')  AS web_events;


/* -----------------------------------------------------------------------------
   6. TEST DYNAMIQUE — ARRIVÉE DU LOT J2
   ----------------------------------------------------------------------------- */
/* 6.1  Upload (SnowSQL, depuis la racine du dépôt)
     PUT file://data/j2/orders_j2.csv      @SHOPFLOW_DB.RAW.STAGE_LANDING/orders/      AUTO_COMPRESS = TRUE;
     PUT file://data/j2/order_items_j2.csv @SHOPFLOW_DB.RAW.STAGE_LANDING/order_items/ AUTO_COMPRESS = TRUE;
     OPTION A : aws s3 cp data/j2/web_events_j2.json s3://<VOTRE_BUCKET>/shopflow/web_events/
     OPTION B : PUT file://data/j2/web_events_j2.json @SHOPFLOW_DB.RAW.STAGE_LANDING/web_events/ AUTO_COMPRESS = TRUE;
   NB : l'énoncé ne cite que orders_j2 et web_events_j2, mais sans
   order_items_j2 les 5 000 nouvelles commandes n'auraient aucune ligne
   (top produits faux) : on charge les trois.                                   */

USE WAREHOUSE WH_INGEST;

-- 6.2  Chargement RAW : EXACTEMENT les mêmes ordres qu'aux Jours 1 et 2.
--      Les fichiers J1 déjà chargés sont ignorés (load metadata).
COPY INTO RAW.ORDERS_RAW
       (ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, SOURCE_FILE, FILE_ROW_NUMBER, LOADED_AT)
FROM (SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
      FROM @RAW.STAGE_LANDING/orders/)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
PATTERN     = '.*[.]csv([.]gz)?'
ON_ERROR    = ABORT_STATEMENT;          -- => 1 fichier chargé : orders_j2.csv, 5 000 lignes

COPY INTO RAW.ORDER_ITEMS_RAW
       (ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, SOURCE_FILE, FILE_ROW_NUMBER, LOADED_AT)
FROM (SELECT $1, $2, $3, $4, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
      FROM @RAW.STAGE_LANDING/order_items/)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
PATTERN     = '.*[.]csv([.]gz)?'
ON_ERROR    = ABORT_STATEMENT;          -- => order_items_j2.csv, 12 933 lignes


INSERT INTO RAW.WEB_EVENTS_RAW (DATA, SOURCE_FILE, FILE_ROW_NUMBER, LOADED_AT)
SELECT s.DATA, s.SOURCE_FILE, s.FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
FROM RAW.V_WEB_EVENTS_SRC s
WHERE s.SOURCE_FILE NOT IN (SELECT DISTINCT SOURCE_FILE FROM RAW.WEB_EVENTS_RAW);   -- => 20 000 lignes

-- 6.3  Les streams se remplissent (à montrer en démo)
SELECT 'STR_ORDERS' AS stream, METADATA$ACTION AS action, COUNT(*) AS nb FROM RAW.STR_ORDERS GROUP BY 2
UNION ALL SELECT 'STR_ORDER_ITEMS', METADATA$ACTION, COUNT(*) FROM RAW.STR_ORDER_ITEMS GROUP BY 2
UNION ALL SELECT 'STR_WEB_EVENTS',  METADATA$ACTION, COUNT(*) FROM RAW.STR_WEB_EVENTS  GROUP BY 2;
-- attendu : INSERT 5 000 / 12 933 / 20 000

-- 6.4  Attendre le prochain déclenchement (<= 5 min) ou forcer :
EXECUTE TASK STAGING.TSK_LOAD_STG_PRODUCTS;

SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
       SCHEDULED_TIME_RANGE_START => DATEADD('MINUTE', -15, CURRENT_TIMESTAMP())))
ORDER BY SCHEDULED_TIME DESC;

-- 6.5  Vérification STAGING : les commandes J2 S'AJOUTENT (pas d'écrasement)
--      attendu : J1 50 000 + J2 5 000 = 55 000 ; items 127 827 + 12 933 = 140 760 ;
--      events 200 000 + 20 000 = 220 000
SELECT LOAD_BATCH, COUNT(*) AS nb_orders, MIN(ORDER_DATE) AS min_date, MAX(ORDER_DATE) AS max_date
FROM STAGING.STG_ORDERS GROUP BY 1 ORDER BY 1;

SELECT LOAD_BATCH, COUNT(*) AS nb_items FROM STAGING.STG_ORDER_ITEMS GROUP BY 1 ORDER BY 1;
SELECT LOAD_BATCH, COUNT(*) AS nb_events FROM STAGING.STG_WEB_EVENTS GROUP BY 1 ORDER BY 1;

-- Même order_id, deux commandes différentes : la preuve que la clé composite est nécessaire
SELECT ORDER_KEY, ORDER_ID, CUSTOMER_ID, ORDER_TS, STATUS, TOTAL_AMOUNT
FROM STAGING.STG_ORDERS
WHERE ORDER_ID = 'O0000001'
ORDER BY ORDER_KEY;

-- Commandes traitées lors du dernier passage
SELECT ORDER_KEY, ORDER_TS, STATUS, TOTAL_AMOUNT, UPDATED_AT
FROM STAGING.STG_ORDERS
ORDER BY UPDATED_AT DESC, ORDER_TS DESC
LIMIT 20;
