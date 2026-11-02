/* =============================================================================
   ShopFlow — JOUR 4 : Marts analytiques & Time Travel
   Livrable : sql/04_marts_and_timetravel.sql
   -----------------------------------------------------------------------------
   Contenu
     1. Dynamic Tables MARTS (TARGET_LAG = 5 minutes)
          DT_DAILY_REVENUE     CA et nb de commandes par jour
          DT_TOP_PRODUCTS      top 20 produits par CA sur 30 jours
          DT_CUSTOMER_COHORTS  rétention par mois d'inscription
     2. Vérification du rafraîchissement automatique
     3. Time Travel : suppression accidentelle de 1 000 commandes + restauration
     4. UNDROP d'une table supprimée

   Règles de gestion retenues (à rappeler en soutenance)
     - CA = somme de TOTAL_AMOUNT des commandes au statut 'completed'
       (pending = non encaissé, cancelled / refunded = pas de CA)
     - Commandes datées dans le futur exclues des marts : le lot J2 contient des
       commandes jusqu'au 28/12/2026. Filtre ORDER_TS <= CURRENT_TIMESTAMP().
     - « 30 derniers jours » ancrés sur la DERNIÈRE DATE DE COMMANDE disponible,
       pas sur CURRENT_DATE : avec le seul lot J1 (qui s'arrête au 30/06/2026),
       CURRENT_DATE - 30 donnerait un top produits vide. En production, on
       remplacerait l'ancre par CURRENT_DATE().
     - Cohortes : commandes antérieures à l'inscription (anomalie source,
       ~10 % des commandes J1) exclues, sinon « mois -3 » de rétention.
   ============================================================================= */

USE ROLE SHOPFLOW_ENGINEER;
USE WAREHOUSE WH_TRANSFORM;
USE DATABASE SHOPFLOW_DB;
USE SCHEMA MARTS;


/* -----------------------------------------------------------------------------
   1. DYNAMIC TABLES
   ----------------------------------------------------------------------------- */

-- 1.a  CA et nombre de commandes par jour
CREATE OR REPLACE DYNAMIC TABLE MARTS.DT_DAILY_REVENUE
  TARGET_LAG   = '5 minutes'
  WAREHOUSE    = WH_TRANSFORM
  REFRESH_MODE = AUTO
  INITIALIZE   = ON_CREATE
  COMMENT = 'CA (commandes completed) et volumes de commandes par jour'
AS
SELECT ORDER_DATE,
       COUNT(*)                                     AS NB_ORDERS,
       COUNT_IF(IS_REVENUE)                         AS NB_ORDERS_COMPLETED,
       COUNT_IF(STATUS = 'pending')                 AS NB_ORDERS_PENDING,
       COUNT_IF(STATUS = 'cancelled')               AS NB_ORDERS_CANCELLED,
       COUNT_IF(STATUS = 'refunded')                AS NB_ORDERS_REFUNDED,
       SUM(IFF(IS_REVENUE, TOTAL_AMOUNT, 0))        AS REVENUE,
       SUM(TOTAL_AMOUNT)                            AS GROSS_ORDER_VALUE,
       ROUND(SUM(IFF(IS_REVENUE, TOTAL_AMOUNT, 0))
             / NULLIF(COUNT_IF(IS_REVENUE), 0), 2)  AS AVG_BASKET,
       COUNT(DISTINCT CUSTOMER_ID)                  AS NB_CUSTOMERS
FROM STAGING.STG_ORDERS
WHERE ORDER_TS <= CURRENT_TIMESTAMP()
GROUP BY ORDER_DATE;

-- 1.b  Top 20 produits par CA sur les 30 derniers jours
--      Fenêtre glissante + classement : rafraîchissement complet (FULL),
--      volume faible (quelques milliers de lignes de commande sur 30 jours).
CREATE OR REPLACE DYNAMIC TABLE MARTS.DT_TOP_PRODUCTS
  TARGET_LAG   = '5 minutes'
  WAREHOUSE    = WH_TRANSFORM
  REFRESH_MODE = FULL
  INITIALIZE   = ON_CREATE
  COMMENT = 'Top 20 produits par CA (commandes completed) sur 30 jours glissants'
AS
WITH valid_orders AS (
  SELECT ORDER_KEY, ORDER_DATE
  FROM STAGING.STG_ORDERS
  WHERE IS_REVENUE
    AND ORDER_TS <= CURRENT_TIMESTAMP()
),
ref AS (
  SELECT MAX(ORDER_DATE) AS REF_DATE FROM valid_orders
),
sales AS (
  SELECT i.PRODUCT_ID,
         SUM(i.LINE_AMOUNT)          AS REVENUE_30D,
         SUM(i.QUANTITY)             AS UNITS_30D,
         COUNT(DISTINCT i.ORDER_KEY) AS NB_ORDERS_30D
  FROM STAGING.STG_ORDER_ITEMS i
  JOIN valid_orders o ON o.ORDER_KEY = i.ORDER_KEY
  CROSS JOIN ref
  WHERE o.ORDER_DATE >  DATEADD(DAY, -30, ref.REF_DATE)
    AND o.ORDER_DATE <= ref.REF_DATE
  GROUP BY i.PRODUCT_ID
)
SELECT ROW_NUMBER() OVER (ORDER BY s.REVENUE_30D DESC, s.PRODUCT_ID) AS PRODUCT_RANK,
       s.PRODUCT_ID,
       p.PRODUCT_NAME,
       p.CATEGORY,
       p.BRAND,
       s.REVENUE_30D,
       s.UNITS_30D,
       s.NB_ORDERS_30D,
       DATEADD(DAY, -29, r.REF_DATE) AS WINDOW_START,
       r.REF_DATE                    AS WINDOW_END
FROM sales s
CROSS JOIN ref r
LEFT JOIN STAGING.STG_PRODUCTS p ON p.PRODUCT_ID = s.PRODUCT_ID
QUALIFY PRODUCT_RANK <= 20;

-- 1.c  Rétention par mois d'inscription
--      Pour chaque cohorte (mois d'inscription) et chaque mois d'activité :
--      nb de clients ayant passé >= 1 commande completed / taille de la cohorte
CREATE OR REPLACE DYNAMIC TABLE MARTS.DT_CUSTOMER_COHORTS
  TARGET_LAG   = '5 minutes'
  WAREHOUSE    = WH_TRANSFORM
  REFRESH_MODE = AUTO
  INITIALIZE   = ON_CREATE
  COMMENT = 'Rétention mensuelle par cohorte de mois d''inscription'
AS
WITH cohort_size AS (
  SELECT SIGNUP_MONTH, COUNT(*) AS COHORT_SIZE
  FROM STAGING.STG_CUSTOMERS
  WHERE SIGNUP_MONTH IS NOT NULL
  GROUP BY SIGNUP_MONTH
),
activity AS (
  SELECT c.SIGNUP_MONTH,
         DATE_TRUNC('MONTH', o.ORDER_DATE) AS ACTIVITY_MONTH,
         COUNT(DISTINCT o.CUSTOMER_ID)     AS ACTIVE_CUSTOMERS,
         SUM(o.TOTAL_AMOUNT)               AS REVENUE
  FROM STAGING.STG_ORDERS o
  JOIN STAGING.STG_CUSTOMERS c ON c.CUSTOMER_ID = o.CUSTOMER_ID
  WHERE o.IS_REVENUE
    AND NOT o.DQ_ORDER_BEFORE_SIGNUP
    AND o.ORDER_TS <= CURRENT_TIMESTAMP()
  GROUP BY 1, 2
)
SELECT a.SIGNUP_MONTH                                           AS COHORT_MONTH,
       a.ACTIVITY_MONTH,
       DATEDIFF(MONTH, a.SIGNUP_MONTH, a.ACTIVITY_MONTH)        AS MONTH_NUMBER,
       cs.COHORT_SIZE,
       a.ACTIVE_CUSTOMERS,
       ROUND(100 * a.ACTIVE_CUSTOMERS / cs.COHORT_SIZE, 2)      AS RETENTION_PCT,
       a.REVENUE
FROM activity a
JOIN cohort_size cs ON cs.SIGNUP_MONTH = a.SIGNUP_MONTH;


/* -----------------------------------------------------------------------------
   2. VÉRIFICATIONS & RAFRAÎCHISSEMENT
   ----------------------------------------------------------------------------- */
SHOW DYNAMIC TABLES IN SCHEMA MARTS;   -- colonnes target_lag, refresh_mode, scheduling_state

SELECT * FROM MARTS.DT_DAILY_REVENUE ORDER BY ORDER_DATE DESC LIMIT 14;
SELECT * FROM MARTS.DT_TOP_PRODUCTS  ORDER BY PRODUCT_RANK;
SELECT * FROM MARTS.DT_CUSTOMER_COHORTS
WHERE COHORT_MONTH >= '2026-01-01'
ORDER BY COHORT_MONTH, MONTH_NUMBER;

-- Historique de rafraîchissement : REFRESH_ACTION = INCREMENTAL / FULL / NO_DATA
SELECT NAME, STATE, REFRESH_ACTION, REFRESH_TRIGGER, DATA_TIMESTAMP,
       REFRESH_START_TIME, REFRESH_END_TIME
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
ORDER BY REFRESH_START_TIME DESC
LIMIT 30;

-- Démo : forcer un rafraîchissement sans attendre le target lag
ALTER DYNAMIC TABLE MARTS.DT_DAILY_REVENUE    REFRESH;
ALTER DYNAMIC TABLE MARTS.DT_TOP_PRODUCTS     REFRESH;
ALTER DYNAMIC TABLE MARTS.DT_CUSTOMER_COHORTS REFRESH;

-- Graphe : Snowsight > Data > SHOPFLOW_DB > MARTS > Dynamic Tables > (DT) > Graph


/* -----------------------------------------------------------------------------
   3. TIME TRAVEL — suppression accidentelle puis restauration
   Durée de rétention : DATA_RETENTION_TIME_IN_DAYS (1 jour par défaut,
   jusqu'à 90 en Enterprise). Au-delà : Fail-safe (7 j, support Snowflake).
   ----------------------------------------------------------------------------- */
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE RAW.ORDERS_RAW;

-- 3.0  État de référence
SELECT COUNT(*) AS nb_rows FROM RAW.ORDERS_RAW;                -- 55 000 après J2
SELECT SUM(REVENUE) AS total_revenue FROM MARTS.DT_DAILY_REVENUE;

-- 3.1  L'INCIDENT : 1 000 commandes J1 supprimées par erreur
DELETE FROM RAW.ORDERS_RAW
WHERE ORDER_ID BETWEEN 'O0010001' AND 'O0011000'
  AND STAGING.FN_LOAD_BATCH(SOURCE_FILE) = 'J1';
SET delete_query_id = LAST_QUERY_ID();       -- on garde l'identifiant de la requête fautive

SELECT COUNT(*) AS nb_rows_after_delete FROM RAW.ORDERS_RAW;   -- 54 000
SELECT METADATA$ACTION, COUNT(*) FROM RAW.STR_ORDERS GROUP BY 1;  -- 1 000 DELETE captés

-- Ce qui se passerait sans réaction : la task propage la suppression en
-- STAGING puis la DT fait baisser le CA. (Facultatif en démo :)
-- EXECUTE TASK STAGING.TSK_LOAD_STG_PRODUCTS;

-- 3.2  DIAGNOSTIC : l'état de la table avant la requête fautive
SELECT COUNT(*) AS nb_rows_before_delete
FROM RAW.ORDERS_RAW BEFORE (STATEMENT => $delete_query_id);    -- 55 000

-- Variante de l'énoncé : décalage relatif en secondes. ⚠ Fragile : -60 doit
-- tomber AVANT le DELETE ; si plus d'une minute s'est écoulée, ajuster l'offset.
SELECT COUNT(*) FROM RAW.ORDERS_RAW AT (OFFSET => -60);

-- 3.3  RESTAURATION : on ne réinsère QUE les lignes disparues
--      (différence ensembliste entre l'état passé et l'état courant)
INSERT INTO RAW.ORDERS_RAW
SELECT * FROM RAW.ORDERS_RAW BEFORE (STATEMENT => $delete_query_id)
MINUS
SELECT * FROM RAW.ORDERS_RAW;

/* Variante avec OFFSET (à exécuter < 60 s après le DELETE) :
INSERT INTO RAW.ORDERS_RAW
SELECT * FROM RAW.ORDERS_RAW AT (OFFSET => -60)
WHERE ORDER_ID BETWEEN 'O0010001' AND 'O0011000'
  AND STAGING.FN_LOAD_BATCH(SOURCE_FILE) = 'J1';
*/

-- 3.4  CONTRÔLE
SELECT COUNT(*) AS nb_rows_restored FROM RAW.ORDERS_RAW;       -- 55 000
SELECT COUNT(*) AS still_missing                                 -- 0
FROM (SELECT * FROM RAW.ORDERS_RAW BEFORE (STATEMENT => $delete_query_id)
      MINUS SELECT * FROM RAW.ORDERS_RAW);

-- Le stream voit 1 000 DELETE + 1 000 INSERT : le MERGE de la task retient
-- l'INSERT pour chaque clé => STAGING et les marts restent cohérents.
EXECUTE TASK STAGING.TSK_LOAD_STG_PRODUCTS;
-- (attendre la fin du graphe, cf. TASK_HISTORY, puis)
ALTER DYNAMIC TABLE MARTS.DT_DAILY_REVENUE REFRESH;
SELECT COUNT(*) FROM STAGING.STG_ORDERS;                         -- 55 000
SELECT SUM(REVENUE) AS total_revenue FROM MARTS.DT_DAILY_REVENUE; -- identique au 3.0


/* -----------------------------------------------------------------------------
   4. UNDROP — table supprimée
   On travaille sur un clone (zero-copy) pour ne pas casser le stream STR_ORDERS
   attaché à ORDERS_RAW.
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE RAW.ORDERS_RAW_TT_DEMO CLONE RAW.ORDERS_RAW;

DROP TABLE RAW.ORDERS_RAW_TT_DEMO;                    -- « oups »
SHOW TABLES HISTORY LIKE 'ORDERS_RAW_TT_DEMO' IN SCHEMA RAW;   -- dropped_on renseigné

UNDROP TABLE RAW.ORDERS_RAW_TT_DEMO;
SELECT COUNT(*) FROM RAW.ORDERS_RAW_TT_DEMO;          -- 55 000

-- ⚠ UNDROP échoue si une table du même nom existe déjà : la renommer d'abord
--   ALTER TABLE RAW.ORDERS_RAW_TT_DEMO RENAME TO RAW.ORDERS_RAW_TT_DEMO_BAD;

DROP TABLE RAW.ORDERS_RAW_TT_DEMO;                    -- nettoyage

-- Autre filet de sécurité : cloner l'état passé d'une table
-- CREATE TABLE RAW.ORDERS_RAW_RESTORE CLONE RAW.ORDERS_RAW BEFORE (STATEMENT => $delete_query_id);
