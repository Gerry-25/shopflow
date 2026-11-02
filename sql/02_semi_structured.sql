/* =============================================================================
   ShopFlow — JOUR 2 : Data Lake — semi-structuré & external table
   Livrable : sql/02_semi_structured.sql
   -----------------------------------------------------------------------------
   Contenu
     1. File format FF_JSON + upload products.json / web_events.json
     2. RAW.PRODUCTS_RAW (VARIANT) + COPY INTO
     3. Requêtes sur VARIANT : notation data:champ::type, LATERAL FLATTEN
     4. External table RAW.WEB_EVENTS_EXT
          ⚠ Snowflake ne supporte les external tables QUE sur un stage EXTERNE
            (S3 / Azure / GCS) — pas sur un stage interne. Deux options :
            OPTION A (recommandée) : bucket S3 + storage integration
            OPTION B (sans compte cloud) : requête directe du stage interne
          Les deux options exposent la même vue RAW.V_WEB_EVENTS_SRC, utilisée
          par la suite du pipeline : le reste du script est identique.
     5. RAW.WEB_EVENTS_RAW + INSERT ... SELECT depuis la source externe
     6. Comparaison de performances external vs table matérialisée
   ============================================================================= */

USE ROLE SHOPFLOW_ENGINEER;
USE WAREHOUSE WH_INGEST;
USE DATABASE SHOPFLOW_DB;
USE SCHEMA RAW;


/* -----------------------------------------------------------------------------
   1. FILE FORMAT JSON + UPLOAD
   STRIP_OUTER_ARRAY = TRUE : products.json est un tableau JSON [ {...}, ... ]
   -> 1 ligne par produit (sinon 1 seule ligne de 150 Ko). Sans effet sur
   web_events.json qui est en JSON « line-delimited » (1 objet par ligne).
   ----------------------------------------------------------------------------- */
CREATE FILE FORMAT IF NOT EXISTS RAW.FF_JSON
  TYPE              = JSON
  STRIP_OUTER_ARRAY = TRUE
  COMPRESSION       = AUTO
  COMMENT = 'JSON (array ou NDJSON) - catalogue PIM et événements web';

/* Upload (SnowSQL, depuis la racine du dépôt) :
     PUT file://data/j1/products.json   @SHOPFLOW_DB.RAW.STAGE_LANDING/products/   AUTO_COMPRESS = TRUE;
     PUT file://data/j1/web_events.json @SHOPFLOW_DB.RAW.STAGE_LANDING/web_events/ AUTO_COMPRESS = TRUE;
   (ou Snowsight > Stage STAGE_LANDING > + Files, chemins products/ et web_events/)
   Pour l'OPTION A, web_events.json doit AUSSI être déposé dans le bucket S3
   (voir section 4).                                                            */

LIST @RAW.STAGE_LANDING/products/;
LIST @RAW.STAGE_LANDING/web_events/;


/* -----------------------------------------------------------------------------
   2. PRODUCTS_RAW (data VARIANT) + COPY INTO — attendu : 500 lignes
   ----------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS RAW.PRODUCTS_RAW (
  DATA        VARIANT,
  SOURCE_FILE VARCHAR,
  LOADED_AT   TIMESTAMP_LTZ
) COMMENT = 'Catalogue produits brut (PIM JSON), 1 objet par ligne';

COPY INTO RAW.PRODUCTS_RAW (DATA, SOURCE_FILE, LOADED_AT)
FROM (
  SELECT $1, METADATA$FILENAME, METADATA$START_SCAN_TIME
  FROM @RAW.STAGE_LANDING/products/
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
PATTERN     = '.*[.]json([.]gz)?'
ON_ERROR    = ABORT_STATEMENT;

SELECT COUNT(*) AS nb_products FROM RAW.PRODUCTS_RAW;          -- 500


/* -----------------------------------------------------------------------------
   3. REQUÊTES SUR LE SEMI-STRUCTURÉ
   ----------------------------------------------------------------------------- */
-- 3.a  Découverte du schéma : tous les chemins et leurs types
SELECT f.PATH, TYPEOF(f.VALUE) AS json_type, COUNT(*) AS nb
FROM RAW.PRODUCTS_RAW p,
     LATERAL FLATTEN(INPUT => p.DATA, RECURSIVE => TRUE) f
GROUP BY 1, 2
ORDER BY 1;

-- 3.b  Notation data:champ::type, y compris champs imbriqués (attributes.*)
SELECT DATA:product_id::VARCHAR                 AS product_id,
       DATA:name::VARCHAR                       AS product_name,
       DATA:category::VARCHAR                   AS category,
       DATA:brand::VARCHAR                      AS brand,
       DATA:price::NUMBER(10,2)                 AS price,
       DATA:attributes:color::VARCHAR           AS color,
       DATA:attributes:warranty_months::INTEGER AS warranty_months,
       DATA:attributes:weight_g::INTEGER        AS weight_g,
       ARRAY_SIZE(DATA:tags)                    AS nb_tags
FROM RAW.PRODUCTS_RAW
ORDER BY product_id
LIMIT 20;

-- 3.c  LATERAL FLATTEN sur le tableau tags : 1 ligne par (produit, tag)
--      OUTER => TRUE conserve les produits sans tag (tag = NULL)
SELECT p.DATA:product_id::VARCHAR AS product_id,
       p.DATA:name::VARCHAR       AS product_name,
       t.INDEX                    AS tag_position,
       t.VALUE::VARCHAR           AS tag
FROM RAW.PRODUCTS_RAW p,
     LATERAL FLATTEN(INPUT => p.DATA:tags, OUTER => TRUE) t
ORDER BY product_id, tag_position
LIMIT 50;

-- 3.d  Analyse : nombre de produits et prix moyen par tag
SELECT COALESCE(t.VALUE::VARCHAR, '(sans tag)') AS tag,
       COUNT(*)                                  AS nb_products,
       ROUND(AVG(p.DATA:price::NUMBER(10,2)), 2) AS avg_price
FROM RAW.PRODUCTS_RAW p,
     LATERAL FLATTEN(INPUT => p.DATA:tags, OUTER => TRUE) t
GROUP BY 1
ORDER BY nb_products DESC;

-- 3.e  FLATTEN d'un OBJET (attributes) : clés / valeurs génériques
SELECT a.KEY AS attribute, COUNT(DISTINCT a.VALUE) AS nb_distinct_values
FROM RAW.PRODUCTS_RAW p,
     LATERAL FLATTEN(INPUT => p.DATA:attributes) a
GROUP BY 1;

-- 3.f  Catalogue : catégories x marques (le nom commercial ne correspond pas
--      toujours à la marque — dataset synthétique, on se fie au champ brand)
SELECT DATA:category::VARCHAR AS category, DATA:brand::VARCHAR AS brand,
       COUNT(*) AS nb, ROUND(AVG(DATA:price::NUMBER(10,2)), 2) AS avg_price
FROM RAW.PRODUCTS_RAW
GROUP BY 1, 2
ORDER BY 1, 2;

-- 3.g  Lecture directe des événements sur le stage (sans chargement)
SELECT $1:event_id::VARCHAR             AS event_id,
       $1:event_type::VARCHAR           AS event_type,
       $1:"timestamp"::TIMESTAMP_NTZ    AS event_ts,
       $1:context:referrer::VARCHAR     AS referrer
FROM @RAW.STAGE_LANDING/web_events/ (FILE_FORMAT => 'RAW.FF_JSON')
LIMIT 10;


/* -----------------------------------------------------------------------------
   4. EXTERNAL TABLE RAW.WEB_EVENTS_EXT
   ----------------------------------------------------------------------------- */

/* ============================ OPTION A — S3 ==================================
   Pré-requis (côté AWS, une fois) :
     - un bucket, ex. s3://<VOTRE_BUCKET>/shopflow/web_events/
     - y déposer data/j1/web_events.json  (aws s3 cp ... ou console AWS)
     - une IAM role en lecture sur ce préfixe (s3:GetObject, s3:ListBucket)
   Côté Snowflake : une STORAGE INTEGRATION => aucune clé AWS dans le script.
   Remplacer les 2 placeholders <...> avant exécution.                          
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS SI_SHOPFLOW_S3
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = 'S3'
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = '<ARN_DE_VOTRE_ROLE_IAM>'          -- ex. arn:aws:iam::123456789012:role/shopflow-snowflake-read
  STORAGE_ALLOWED_LOCATIONS = ('s3://<VOTRE_BUCKET>/shopflow/');

-- Récupérer STORAGE_AWS_IAM_USER_ARN et STORAGE_AWS_EXTERNAL_ID, puis les
-- reporter dans la « trust policy » de la role IAM côté AWS.
DESC INTEGRATION SI_SHOPFLOW_S3;

GRANT USAGE ON INTEGRATION SI_SHOPFLOW_S3 TO ROLE SHOPFLOW_ENGINEER;

USE ROLE SHOPFLOW_ENGINEER;
USE WAREHOUSE WH_INGEST;

CREATE STAGE IF NOT EXISTS RAW.STAGE_EXT_LAKE
  URL                 = 's3://<VOTRE_BUCKET>/shopflow/'
  STORAGE_INTEGRATION = SI_SHOPFLOW_S3
  FILE_FORMAT         = (FORMAT_NAME = 'RAW.FF_JSON')
  COMMENT = 'Stage externe (data lake S3) pour les événements web';

LIST @RAW.STAGE_EXT_LAKE/web_events/;

-- Colonnes virtuelles calculées à partir de VALUE (VARIANT = 1 ligne du fichier)
CREATE OR REPLACE EXTERNAL TABLE RAW.WEB_EVENTS_EXT (
  EVENT_ID   VARCHAR       AS (VALUE:event_id::VARCHAR),
  USER_ID    VARCHAR       AS (VALUE:user_id::VARCHAR),
  SESSION_ID VARCHAR       AS (VALUE:session_id::VARCHAR),
  EVENT_TYPE VARCHAR       AS (VALUE:event_type::VARCHAR),
  PRODUCT_ID VARCHAR       AS (VALUE:product_id::VARCHAR),
  EVENT_TS   TIMESTAMP_NTZ AS (VALUE:"timestamp"::TIMESTAMP_NTZ),
  DEVICE     VARCHAR       AS (VALUE:device::VARCHAR),
  REFERRER   VARCHAR       AS (VALUE:context:referrer::VARCHAR)
)
LOCATION     = @RAW.STAGE_EXT_LAKE/web_events/
PATTERN      = '.*web_events.*[.]json([.]gz)?'
FILE_FORMAT  = (FORMAT_NAME = 'RAW.FF_JSON')
AUTO_REFRESH = FALSE     -- pas de notification S3 configurée : refresh manuel
COMMENT = 'Événements web lus directement dans S3 (aucune copie physique)';

ALTER EXTERNAL TABLE RAW.WEB_EVENTS_EXT REFRESH;   -- enregistre les fichiers présents

SELECT COUNT(*) FROM RAW.WEB_EVENTS_EXT;           -- 200 000

-- Vue d'accès uniforme utilisée par la suite du pipeline
CREATE OR REPLACE VIEW RAW.V_WEB_EVENTS_SRC AS
SELECT VALUE                     AS DATA,
       METADATA$FILENAME         AS SOURCE_FILE,
       METADATA$FILE_ROW_NUMBER  AS FILE_ROW_NUMBER
FROM RAW.WEB_EVENTS_EXT;
/* ========================== fin OPTION A ==================================== */


/* ==================== OPTION B — sans compte cloud ===========================
   Si vous n'avez pas de bucket S3/Azure/GCS : commentez l'OPTION A ci-dessus,
   décommentez ce bloc. On garde le principe « lire sans charger » en
   interrogeant directement le fichier sur le stage interne (comme au badge 2).*/

CREATE OR REPLACE VIEW RAW.WEB_EVENTS_EXT AS
SELECT $1:event_id::VARCHAR           AS EVENT_ID,
       $1:user_id::VARCHAR            AS USER_ID,
       $1:session_id::VARCHAR         AS SESSION_ID,
       $1:event_type::VARCHAR         AS EVENT_TYPE,
       $1:product_id::VARCHAR         AS PRODUCT_ID,
       $1:"timestamp"::TIMESTAMP_NTZ  AS EVENT_TS,
       $1:device::VARCHAR             AS DEVICE,
       $1:context:referrer::VARCHAR   AS REFERRER,
       $1                             AS VALUE,
       METADATA$FILENAME              AS SOURCE_FILE,
       METADATA$FILE_ROW_NUMBER       AS FILE_ROW_NUMBER
FROM @RAW.STAGE_LANDING/web_events/ (FILE_FORMAT => 'RAW.FF_JSON',
                                     PATTERN     => '.*web_events.*[.]json([.]gz)?');

CREATE OR REPLACE VIEW RAW.V_WEB_EVENTS_SRC AS
SELECT VALUE AS DATA, SOURCE_FILE, FILE_ROW_NUMBER FROM RAW.WEB_EVENTS_EXT;
/* ========================== fin OPTION B ==================================== */


/* -----------------------------------------------------------------------------
   5. RAW.WEB_EVENTS_RAW (matérialisée) + INSERT ... SELECT
   Idempotent : on n'insère que les fichiers pas encore chargés. Le même
   INSERT sera rejoué au Jour 3 pour web_events_j2.json.
   ----------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS RAW.WEB_EVENTS_RAW (
  DATA            VARIANT,
  SOURCE_FILE     VARCHAR,
  FILE_ROW_NUMBER NUMBER,
  LOADED_AT       TIMESTAMP_LTZ
) COMMENT = 'Événements web bruts matérialisés (copie de la source externe)';

INSERT INTO RAW.WEB_EVENTS_RAW (DATA, SOURCE_FILE, FILE_ROW_NUMBER, LOADED_AT)
SELECT s.DATA, s.SOURCE_FILE, s.FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
FROM RAW.V_WEB_EVENTS_SRC s
WHERE s.SOURCE_FILE NOT IN (SELECT DISTINCT SOURCE_FILE FROM RAW.WEB_EVENTS_RAW);

SELECT SOURCE_FILE, COUNT(*) AS nb FROM RAW.WEB_EVENTS_RAW GROUP BY 1;   -- 200 000

-- Profilage rapide des événements
SELECT DATA:event_type::VARCHAR AS event_type,
       COUNT(*)                                       AS nb_events,
       COUNT_IF(DATA:user_id::VARCHAR IS NULL)        AS anonymous_events,   -- ~15 %
       COUNT_IF(DATA:product_id::VARCHAR IS NULL)     AS no_product,         -- ~30 %
       COUNT_IF(DATA:context:referrer::VARCHAR IS NULL) AS direct_traffic    -- ~25 %
FROM RAW.WEB_EVENTS_RAW
GROUP BY 1
ORDER BY 1;


/* -----------------------------------------------------------------------------
   6. PERFORMANCES : external table vs table matérialisée
   On désactive le cache de résultats pour mesurer un vrai calcul, et on tague
   les requêtes pour les retrouver dans l'historique.
   ----------------------------------------------------------------------------- */
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Requête 1 : agrégation complète
ALTER SESSION SET QUERY_TAG = 'PERF_EXT_AGG';
SELECT EVENT_TYPE, DEVICE, COUNT(*) AS nb, COUNT(DISTINCT USER_ID) AS nb_users
FROM RAW.WEB_EVENTS_EXT
GROUP BY 1, 2;

ALTER SESSION SET QUERY_TAG = 'PERF_NATIVE_AGG';
SELECT DATA:event_type::VARCHAR AS event_type, DATA:device::VARCHAR AS device,
       COUNT(*) AS nb, COUNT(DISTINCT DATA:user_id::VARCHAR) AS nb_users
FROM RAW.WEB_EVENTS_RAW
GROUP BY 1, 2;

-- Requête 2 : filtre sélectif (un produit)
ALTER SESSION SET QUERY_TAG = 'PERF_EXT_FILTER';
SELECT COUNT(*) FROM RAW.WEB_EVENTS_EXT WHERE PRODUCT_ID = 'P00042';

ALTER SESSION SET QUERY_TAG = 'PERF_NATIVE_FILTER';
SELECT COUNT(*) FROM RAW.WEB_EVENTS_RAW WHERE DATA:product_id::VARCHAR = 'P00042';

ALTER SESSION UNSET QUERY_TAG;
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- Comparaison (complétez avec le Query Profile : partitions scannées, % remote I/O)
SELECT QUERY_TAG,
       TOTAL_ELAPSED_TIME / 1000 AS elapsed_s,
       COMPILATION_TIME   / 1000 AS compile_s,
       EXECUTION_TIME     / 1000 AS exec_s,
       BYTES_SCANNED,
       LEFT(QUERY_TEXT, 80)      AS query
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 100))
WHERE QUERY_TAG LIKE 'PERF_%'
ORDER BY START_TIME;

/* Lecture attendue :
   - L'external table relit et re-parse le JSON brut à chaque requête (pas de
     micro-partitions, pas de pruning, pas de statistiques) : nettement plus lente,
     surtout sur le filtre sélectif.
   - La table native stocke le VARIANT en colonnes (sous-colonnarisation) avec
     métadonnées min/max par micro-partition : scan plus court, pruning possible.
   - L'external table reste utile pour explorer un data lake sans payer le
     stockage ni l'ingestion. Pour des requêtes répétées : matérialiser.         */
