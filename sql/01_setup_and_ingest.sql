/* =============================================================================
   ShopFlow — JOUR 1 : Fondations & ingestion structurée
   Livrable : sql/01_setup_and_ingest.sql
   -----------------------------------------------------------------------------
   Contenu
     0. Rôles & privilèges (SHOPFLOW_ENGINEER, SHOPFLOW_ANALYST)
     1. Virtual warehouses (WH_INGEST, WH_TRANSFORM, WH_BI)
     2. Base SHOPFLOW_DB + schémas RAW / STAGING / MARTS
     3. Stage interne RAW.STAGE_LANDING + upload des fichiers
     4. File formats FF_CSV_ORDERS, FF_PARQUET
     5. Tables RAW (landing, colonnes VARCHAR + métadonnées de chargement)
     6. Validation à blanc (VALIDATION_MODE) puis COPY INTO
     7. Contrôles : compteurs, COPY_HISTORY, premiers contrôles qualité

   Exécution : worksheet Snowsight ou SnowSQL, avec un utilisateur disposant
   de ACCOUNTADMIN (sections 0 à 2), puis le rôle SHOPFLOW_ENGINEER.
   Script ré-exécutable : CREATE ... IF NOT EXISTS partout où un DROP casserait
   un objet dépendant (les streams du Jour 3 sont liés aux tables RAW).
   Aucun credential dans ce fichier.
   ============================================================================= */


/* -----------------------------------------------------------------------------
   0. RÔLES & PRIVILÈGES
   ----------------------------------------------------------------------------- */
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS SHOPFLOW_ENGINEER
  COMMENT = 'ShopFlow - rôle applicatif data engineering (ingestion, pipeline, marts)';
CREATE ROLE IF NOT EXISTS SHOPFLOW_ANALYST
  COMMENT = 'ShopFlow - lecture seule des marts (dashboard / BI)';

-- Hiérarchie : SYSADMIN hérite des rôles applicatifs (bonne pratique Snowflake)
GRANT ROLE SHOPFLOW_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE SHOPFLOW_ANALYST  TO ROLE SHOPFLOW_ENGINEER;

-- Attribution à l'utilisateur courant, sans écrire son nom en dur
SET my_user = CURRENT_USER();
GRANT ROLE SHOPFLOW_ENGINEER TO USER IDENTIFIER($my_user);

-- Privilèges de compte nécessaires aux Tasks (Jour 3)
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SHOPFLOW_ENGINEER;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SHOPFLOW_ENGINEER;  -- optionnel (tasks serverless)


/* -----------------------------------------------------------------------------
   1. VIRTUAL WAREHOUSES — un warehouse par workload, AUTO_SUSPEND <= 60 s
   ----------------------------------------------------------------------------- */
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS WH_INGEST
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'ShopFlow - chargements COPY INTO / INSERT depuis les stages';

CREATE WAREHOUSE IF NOT EXISTS WH_TRANSFORM
  WAREHOUSE_SIZE      = 'SMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'ShopFlow - Tasks (RAW -> STAGING) et Dynamic Tables (MARTS)';

-- Isolation du workload de restitution : le dashboard ne ralentit pas le pipeline
CREATE WAREHOUSE IF NOT EXISTS WH_BI
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'ShopFlow - requêtes du dashboard Snowsight';

-- Garde-fou si le script est rejoué sur des warehouses existants
ALTER WAREHOUSE WH_INGEST    SET AUTO_SUSPEND = 60;
ALTER WAREHOUSE WH_TRANSFORM SET AUTO_SUSPEND = 60;
ALTER WAREHOUSE WH_BI        SET AUTO_SUSPEND = 60;

GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE WH_INGEST    TO ROLE SHOPFLOW_ENGINEER;
GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE WH_TRANSFORM TO ROLE SHOPFLOW_ENGINEER;
GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE WH_BI        TO ROLE SHOPFLOW_ENGINEER;
GRANT USAGE                   ON WAREHOUSE WH_BI        TO ROLE SHOPFLOW_ANALYST;


/* -----------------------------------------------------------------------------
   2. BASE & SCHÉMAS — architecture multicouche RAW -> STAGING -> MARTS
   ----------------------------------------------------------------------------- */
CREATE DATABASE IF NOT EXISTS SHOPFLOW_DB
  COMMENT = 'ShopFlow - plateforme analytique e-commerce';

CREATE SCHEMA IF NOT EXISTS SHOPFLOW_DB.RAW
  COMMENT = 'Landing : données brutes telles que reçues (VARCHAR / VARIANT) + métadonnées de chargement';
CREATE SCHEMA IF NOT EXISTS SHOPFLOW_DB.STAGING
  COMMENT = 'Données nettoyées, typées, dédoublonnées, enrichies (alimentées par Streams + Tasks)';
CREATE SCHEMA IF NOT EXISTS SHOPFLOW_DB.MARTS
  COMMENT = 'Indicateurs métier (Dynamic Tables) consommés par le dashboard';

GRANT USAGE ON DATABASE SHOPFLOW_DB TO ROLE SHOPFLOW_ENGINEER;
GRANT USAGE ON DATABASE SHOPFLOW_DB TO ROLE SHOPFLOW_ANALYST;

-- Le rôle engineer crée (et possède) tous les objets des trois couches
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT,
      CREATE EXTERNAL TABLE, CREATE STREAM, CREATE TASK, CREATE DYNAMIC TABLE,
      CREATE FUNCTION, CREATE PROCEDURE
  ON SCHEMA SHOPFLOW_DB.RAW     TO ROLE SHOPFLOW_ENGINEER;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT,
      CREATE EXTERNAL TABLE, CREATE STREAM, CREATE TASK, CREATE DYNAMIC TABLE,
      CREATE FUNCTION, CREATE PROCEDURE
  ON SCHEMA SHOPFLOW_DB.STAGING TO ROLE SHOPFLOW_ENGINEER;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT,
      CREATE EXTERNAL TABLE, CREATE STREAM, CREATE TASK, CREATE DYNAMIC TABLE,
      CREATE FUNCTION, CREATE PROCEDURE
  ON SCHEMA SHOPFLOW_DB.MARTS   TO ROLE SHOPFLOW_ENGINEER;

-- L'analyste ne voit que la couche MARTS (moindre privilège)
USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA SHOPFLOW_DB.MARTS TO ROLE SHOPFLOW_ANALYST;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA SHOPFLOW_DB.MARTS TO ROLE SHOPFLOW_ANALYST;
GRANT SELECT ON FUTURE VIEWS          IN SCHEMA SHOPFLOW_DB.MARTS TO ROLE SHOPFLOW_ANALYST;


/* -----------------------------------------------------------------------------
   3. STAGE INTERNE — un sous-dossier par entité
   ----------------------------------------------------------------------------- */
USE ROLE SHOPFLOW_ENGINEER;
USE WAREHOUSE WH_INGEST;
USE DATABASE SHOPFLOW_DB;
USE SCHEMA RAW;

CREATE STAGE IF NOT EXISTS RAW.STAGE_LANDING
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Zone de dépôt des fichiers sources (orders/, order_items/, customers/, products/, web_events/)';

/* Upload du lot J1 — à faire AVANT la section 6.
   Option 1 : SnowSQL, depuis la racine du dépôt (compression gzip automatique)
     PUT file://data/j1/orders.csv        @SHOPFLOW_DB.RAW.STAGE_LANDING/orders/      AUTO_COMPRESS = TRUE;
     PUT file://data/j1/order_items.csv   @SHOPFLOW_DB.RAW.STAGE_LANDING/order_items/ AUTO_COMPRESS = TRUE;
     PUT file://data/j1/customers.parquet @SHOPFLOW_DB.RAW.STAGE_LANDING/customers/   AUTO_COMPRESS = FALSE;
   Option 2 : Snowsight > Data > Databases > SHOPFLOW_DB > RAW > Stages > STAGE_LANDING
              > "+ Files", en renseignant le chemin (orders/, order_items/, customers/).

   Ne PAS uploader le lot J2 maintenant : il sert au test du Jour 3.            */

LIST @RAW.STAGE_LANDING;   -- doit montrer orders/, order_items/, customers/


/* -----------------------------------------------------------------------------
   4. FILE FORMATS
   ----------------------------------------------------------------------------- */
-- CSV du back-office : header, séparateur virgule, fins de ligne CRLF (gérées
-- nativement), valeurs vides => NULL. Utilisé pour orders ET order_items.
CREATE FILE FORMAT IF NOT EXISTS RAW.FF_CSV_ORDERS
  TYPE                           = CSV
  FIELD_DELIMITER                = ','
  SKIP_HEADER                    = 1
  FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
  TRIM_SPACE                     = TRUE
  EMPTY_FIELD_AS_NULL            = TRUE
  NULL_IF                        = ('', 'NULL', 'null', 'N/A')
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  ENCODING                       = 'UTF8'
  COMPRESSION                    = AUTO
  COMMENT = 'CSV back-office ShopFlow (orders, order_items)';

-- Parquet compressé snappy (détection automatique)
CREATE FILE FORMAT IF NOT EXISTS RAW.FF_PARQUET
  TYPE        = PARQUET
  COMPRESSION = AUTO
  COMMENT = 'Référentiel clients CRM (Parquet snappy)';


/* -----------------------------------------------------------------------------
   5. TABLES RAW
   Choix : en landing, tout est VARCHAR (on n'échoue jamais sur un typage) ;
   le typage se fait en STAGING. On ajoute 3 colonnes de traçabilité :
     SOURCE_FILE     -> METADATA$FILENAME   (indispensable : le lot J2 réutilise
                                            les mêmes order_id que J1 !)
     FILE_ROW_NUMBER -> METADATA$FILE_ROW_NUMBER (identifie une ligne de commande)
     LOADED_AT       -> METADATA$START_SCAN_TIME
   ----------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS RAW.ORDERS_RAW (
  ORDER_ID        VARCHAR,
  CUSTOMER_ID     VARCHAR,
  ORDER_DATE      VARCHAR,
  STATUS          VARCHAR,
  TOTAL_AMOUNT    VARCHAR,
  SOURCE_FILE     VARCHAR,
  FILE_ROW_NUMBER NUMBER,
  LOADED_AT       TIMESTAMP_LTZ
) COMMENT = 'Commandes brutes (back-office CSV)';

CREATE TABLE IF NOT EXISTS RAW.ORDER_ITEMS_RAW (
  ORDER_ID        VARCHAR,
  PRODUCT_ID      VARCHAR,
  QUANTITY        VARCHAR,
  UNIT_PRICE      VARCHAR,
  SOURCE_FILE     VARCHAR,
  FILE_ROW_NUMBER NUMBER,
  LOADED_AT       TIMESTAMP_LTZ
) COMMENT = 'Lignes de commande brutes (back-office CSV)';

CREATE TABLE IF NOT EXISTS RAW.CUSTOMERS_RAW (
  CUSTOMER_ID     VARCHAR,
  EMAIL           VARCHAR,
  FIRST_NAME      VARCHAR,
  LAST_NAME       VARCHAR,
  CITY            VARCHAR,
  SIGNUP_DATE     VARCHAR,       -- stocké en texte dans le Parquet (cf. INFER_SCHEMA)
  SOURCE_FILE     VARCHAR,
  FILE_ROW_NUMBER NUMBER,
  LOADED_AT       TIMESTAMP_LTZ
) COMMENT = 'Référentiel clients brut (CRM Parquet)';


/* -----------------------------------------------------------------------------
   6. VALIDATION À BLANC PUIS CHARGEMENT
   VALIDATION_MODE n'est pas supporté sur un COPY avec transformation (SELECT).
   On valide donc chaque CSV dans une table temporaire « miroir » du fichier,
   sans rien charger, puis on lance le vrai COPY avec métadonnées.
   ----------------------------------------------------------------------------- */

-- 6.a  Validation à blanc — doit renvoyer 0 ligne (= 0 erreur)
CREATE OR REPLACE TEMPORARY TABLE RAW.TMP_VALIDATE_ORDERS (
  ORDER_ID VARCHAR, CUSTOMER_ID VARCHAR, ORDER_DATE VARCHAR, STATUS VARCHAR, TOTAL_AMOUNT VARCHAR);
COPY INTO RAW.TMP_VALIDATE_ORDERS
  FROM @RAW.STAGE_LANDING/orders/
  FILE_FORMAT     = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
  PATTERN         = '.*[.]csv([.]gz)?'
  VALIDATION_MODE = RETURN_ERRORS;

CREATE OR REPLACE TEMPORARY TABLE RAW.TMP_VALIDATE_ORDER_ITEMS (
  ORDER_ID VARCHAR, PRODUCT_ID VARCHAR, QUANTITY VARCHAR, UNIT_PRICE VARCHAR);
COPY INTO RAW.TMP_VALIDATE_ORDER_ITEMS
  FROM @RAW.STAGE_LANDING/order_items/
  FILE_FORMAT     = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
  PATTERN         = '.*[.]csv([.]gz)?'
  VALIDATION_MODE = RETURN_ERRORS;

-- Aperçu des 10 premières lignes telles que parsées (contrôle visuel)
COPY INTO RAW.TMP_VALIDATE_ORDERS
  FROM @RAW.STAGE_LANDING/orders/
  FILE_FORMAT     = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
  PATTERN         = '.*[.]csv([.]gz)?'
  VALIDATION_MODE = RETURN_10_ROWS;

-- Parquet : on inspecte le schéma et quelques lignes directement sur le stage
SELECT *
FROM TABLE(INFER_SCHEMA(
       LOCATION    => '@RAW.STAGE_LANDING/customers/',
       FILE_FORMAT => 'RAW.FF_PARQUET'));   -- signup_date = TEXT => cast en STAGING

SELECT $1 FROM @RAW.STAGE_LANDING/customers/ (FILE_FORMAT => 'RAW.FF_PARQUET') LIMIT 5;

-- 6.b  Chargements réels
-- Le même COPY est réutilisé au Jour 3 : grâce aux métadonnées de chargement
-- (64 jours), Snowflake ignore les fichiers déjà chargés => seul orders_j2.csv
-- sera pris. Ne jamais utiliser FORCE = TRUE ici (doublons).
COPY INTO RAW.ORDERS_RAW
       (ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT,
        SOURCE_FILE, FILE_ROW_NUMBER, LOADED_AT)
FROM (
  SELECT $1, $2, $3, $4, $5,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
  FROM @RAW.STAGE_LANDING/orders/
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
PATTERN     = '.*[.]csv([.]gz)?'
ON_ERROR    = ABORT_STATEMENT;

COPY INTO RAW.ORDER_ITEMS_RAW
       (ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE,
        SOURCE_FILE, FILE_ROW_NUMBER, LOADED_AT)
FROM (
  SELECT $1, $2, $3, $4,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
  FROM @RAW.STAGE_LANDING/order_items/
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_ORDERS')
PATTERN     = '.*[.]csv([.]gz)?'
ON_ERROR    = ABORT_STATEMENT;

-- Parquet : on adresse les colonnes par nom ($1:col), sensibles à la casse
COPY INTO RAW.CUSTOMERS_RAW
       (CUSTOMER_ID, EMAIL, FIRST_NAME, LAST_NAME, CITY, SIGNUP_DATE,
        SOURCE_FILE, FILE_ROW_NUMBER, LOADED_AT)
FROM (
  SELECT $1:customer_id::VARCHAR,
         $1:email::VARCHAR,
         $1:first_name::VARCHAR,
         $1:last_name::VARCHAR,
         $1:city::VARCHAR,
         $1:signup_date::VARCHAR,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
  FROM @RAW.STAGE_LANDING/customers/
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_PARQUET')
PATTERN     = '.*[.]parquet'
ON_ERROR    = ABORT_STATEMENT;


/* -----------------------------------------------------------------------------
   7. CONTRÔLES
   ----------------------------------------------------------------------------- */
-- 7.a  Compteurs attendus après le lot J1 : 50 000 / 127 827 / 10 000
SELECT 'ORDERS_RAW'      AS table_name, COUNT(*) AS nb_rows FROM RAW.ORDERS_RAW
UNION ALL SELECT 'ORDER_ITEMS_RAW', COUNT(*) FROM RAW.ORDER_ITEMS_RAW
UNION ALL SELECT 'CUSTOMERS_RAW',   COUNT(*) FROM RAW.CUSTOMERS_RAW;
-- NB : l'énoncé annonce ~150 000 lignes de commande ; le fichier en contient 127 827.

-- 7.b  Historique de chargement (fichiers, lignes, erreurs)
SELECT TABLE_NAME, FILE_NAME, STATUS, ROW_COUNT, ROW_PARSED, ERROR_COUNT, FIRST_ERROR_MESSAGE, LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'RAW.ORDERS_RAW',
       START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())))
UNION ALL
SELECT TABLE_NAME, FILE_NAME, STATUS, ROW_COUNT, ROW_PARSED, ERROR_COUNT, FIRST_ERROR_MESSAGE, LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'RAW.ORDER_ITEMS_RAW',
       START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())))
UNION ALL
SELECT TABLE_NAME, FILE_NAME, STATUS, ROW_COUNT, ROW_PARSED, ERROR_COUNT, FIRST_ERROR_MESSAGE, LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'RAW.CUSTOMERS_RAW',
       START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())));

-- 7.c  Premiers contrôles qualité (profilage)
-- Clés nulles / doublons de clé
SELECT COUNT(*)                                   AS nb_orders,
       COUNT(DISTINCT ORDER_ID)                   AS nb_distinct_order_id,
       COUNT_IF(ORDER_ID IS NULL)                 AS null_order_id,
       COUNT_IF(CUSTOMER_ID IS NULL)              AS null_customer_id,
       COUNT_IF(TRY_TO_TIMESTAMP_NTZ(ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS') IS NULL) AS bad_dates,
       COUNT_IF(TRY_TO_NUMBER(TOTAL_AMOUNT, 12, 2) IS NULL)                      AS bad_amounts,
       MIN(ORDER_DATE) AS min_date, MAX(ORDER_DATE) AS max_date
FROM RAW.ORDERS_RAW;

SELECT STATUS, COUNT(*) AS nb FROM RAW.ORDERS_RAW GROUP BY STATUS ORDER BY nb DESC;

-- Même produit plusieurs fois dans une commande : 266 cas (dont 62 lignes
-- strictement identiques). Ce sont des lignes LÉGITIMES : total_amount n'est
-- égal à la somme des lignes que si on les garde toutes => pas de DISTINCT !
SELECT COUNT(*) AS nb_pairs_repeated
FROM (SELECT ORDER_ID, PRODUCT_ID FROM RAW.ORDER_ITEMS_RAW
      GROUP BY ORDER_ID, PRODUCT_ID HAVING COUNT(*) > 1);

-- Réconciliation montant commande vs somme des lignes (attendu : 0 écart)
SELECT COUNT(*) AS nb_orders_mismatch
FROM RAW.ORDERS_RAW o
JOIN (SELECT ORDER_ID, SUM(QUANTITY::NUMBER * UNIT_PRICE::NUMBER(12,2)) AS items_total
      FROM RAW.ORDER_ITEMS_RAW GROUP BY ORDER_ID) i
  ON i.ORDER_ID = o.ORDER_ID
WHERE ABS(o.TOTAL_AMOUNT::NUMBER(12,2) - i.items_total) > 0.05;

-- Intégrité référentielle commandes -> clients (attendu : 0 orphelin)
SELECT COUNT(*) AS orphan_orders
FROM RAW.ORDERS_RAW o
LEFT JOIN RAW.CUSTOMERS_RAW c ON c.CUSTOMER_ID = o.CUSTOMER_ID
WHERE c.CUSTOMER_ID IS NULL;

-- Anomalie métier : commande antérieure à l'inscription du client (4 909 cas)
SELECT COUNT(*) AS orders_before_signup
FROM RAW.ORDERS_RAW o
JOIN RAW.CUSTOMERS_RAW c ON c.CUSTOMER_ID = o.CUSTOMER_ID
WHERE TO_DATE(o.ORDER_DATE::TIMESTAMP_NTZ) < c.SIGNUP_DATE::DATE;

-- Fin de journée : suspendre les warehouses (cf. sql/99_pause_and_cleanup.sql).
-- NB : ALTER WAREHOUSE ... SUSPEND échoue si le warehouse est déjà suspendu
-- (AUTO_SUSPEND = 60 s) : ce n'est pas grave.
