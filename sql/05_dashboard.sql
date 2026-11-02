/* =============================================================================
   ShopFlow — JOUR 5 : Dashboard Snowsight
   Livrable : sql/05_dashboard.sql (une requête = une tuile)
   -----------------------------------------------------------------------------
   Mise en place dans Snowsight
     1. Projects > Dashboards > + Dashboard  : « ShopFlow — Pilotage »
     2. Rôle SHOPFLOW_ANALYST (ou ENGINEER), warehouse WH_BI
     3. Pour chaque tuile : + New Tile > From SQL Worksheet, coller la requête,
        Run, onglet Chart, choisir le type indiqué ci-dessous.
     4. Rafraîchissement : bouton Refresh du dashboard (les DT ont 5 min de lag)

   Toutes les tuiles lisent la couche MARTS (+ STAGING pour la tuile détail).
   Ancrage temporel : « aujourd'hui » = dernier jour de commande disponible
   (cf. 04_marts_and_timetravel.sql). Remplacer par CURRENT_DATE() en prod.
   ============================================================================= */

USE ROLE SHOPFLOW_ENGINEER;      -- ou SHOPFLOW_ANALYST pour les tuiles 1, 2, 4, 5
USE WAREHOUSE WH_BI;
USE DATABASE SHOPFLOW_DB;

-- -----------------------------------------------------------------------------
-- TUILE 1 — Ligne : CA par jour, 14 derniers jours
--   Chart : Line | X = ORDER_DATE | Y = REVENUE (option : 2e série NB_ORDERS)
-- -----------------------------------------------------------------------------
WITH ref AS (SELECT MAX(ORDER_DATE) AS REF_DATE FROM MARTS.DT_DAILY_REVENUE)
SELECT d.ORDER_DATE,
       d.REVENUE,
       d.NB_ORDERS
FROM MARTS.DT_DAILY_REVENUE d
CROSS JOIN ref
WHERE d.ORDER_DATE > DATEADD(DAY, -14, ref.REF_DATE)
ORDER BY d.ORDER_DATE;

-- -----------------------------------------------------------------------------
-- TUILE 2 — Barres : Top 10 produits (CA 30 jours)
--   Chart : Bar (horizontal) | X = PRODUCT_LABEL | Y = REVENUE_30D | tri desc
-- -----------------------------------------------------------------------------
SELECT PRODUCT_RANK,
       PRODUCT_ID || ' · ' || PRODUCT_NAME AS PRODUCT_LABEL,
       CATEGORY,
       REVENUE_30D,
       UNITS_30D
FROM MARTS.DT_TOP_PRODUCTS
WHERE PRODUCT_RANK <= 10
ORDER BY PRODUCT_RANK;

-- -----------------------------------------------------------------------------
-- TUILE 3 — Table : dernières commandes traitées par le pipeline
--   Chart : Table. UPDATED_AT = passage de la task => après l'arrivée du lot J2,
--   les commandes J2 remontent en tête (démo live).
-- -----------------------------------------------------------------------------
SELECT o.ORDER_KEY,
       o.ORDER_TS,
       o.CUSTOMER_ID,
       o.STATUS,
       o.TOTAL_AMOUNT,
       o.LOAD_BATCH,
       o.UPDATED_AT AS PROCESSED_AT
FROM STAGING.STG_ORDERS o
WHERE o.ORDER_TS <= CURRENT_TIMESTAMP()
ORDER BY o.UPDATED_AT DESC, o.ORDER_TS DESC
LIMIT 25;

-- -----------------------------------------------------------------------------
-- TUILE 4 — KPI : commandes du jour & panier moyen (avec variation J-1)
--   Chart : Scorecard (une tuile par colonne : NB_ORDERS_DAY, AVG_BASKET_DAY)
-- -----------------------------------------------------------------------------
WITH ranked AS (
  SELECT ORDER_DATE, NB_ORDERS, AVG_BASKET, REVENUE,
         LAG(NB_ORDERS)  OVER (ORDER BY ORDER_DATE) AS NB_ORDERS_PREV,
         LAG(AVG_BASKET) OVER (ORDER BY ORDER_DATE) AS AVG_BASKET_PREV
  FROM MARTS.DT_DAILY_REVENUE
)
SELECT ORDER_DATE                                                   AS KPI_DATE,
       NB_ORDERS                                                    AS NB_ORDERS_DAY,
       AVG_BASKET                                                   AS AVG_BASKET_DAY,
       REVENUE                                                      AS REVENUE_DAY,
       ROUND(100 * (NB_ORDERS  - NB_ORDERS_PREV)  / NULLIF(NB_ORDERS_PREV, 0), 1)  AS NB_ORDERS_VS_D1_PCT,
       ROUND(100 * (AVG_BASKET - AVG_BASKET_PREV) / NULLIF(AVG_BASKET_PREV, 0), 1) AS AVG_BASKET_VS_D1_PCT
FROM ranked
QUALIFY ROW_NUMBER() OVER (ORDER BY ORDER_DATE DESC) = 1;

-- Variante KPI « panier moyen 14 jours » (plus stable qu'un seul jour)
WITH ref AS (SELECT MAX(ORDER_DATE) AS REF_DATE FROM MARTS.DT_DAILY_REVENUE)
SELECT ROUND(SUM(REVENUE) / NULLIF(SUM(NB_ORDERS_COMPLETED), 0), 2) AS AVG_BASKET_14D
FROM MARTS.DT_DAILY_REVENUE, ref
WHERE ORDER_DATE > DATEADD(DAY, -14, ref.REF_DATE);

-- -----------------------------------------------------------------------------
-- TUILE 5 (bonus) — Heatmap : rétention des cohortes 2026
--   Chart : Heatgrid | lignes = COHORT_MONTH | colonnes = MONTH_NUMBER | cellule = RETENTION_PCT
-- -----------------------------------------------------------------------------
SELECT TO_CHAR(COHORT_MONTH, 'YYYY-MM') AS COHORT_MONTH,
       MONTH_NUMBER,
       RETENTION_PCT
FROM MARTS.DT_CUSTOMER_COHORTS
WHERE COHORT_MONTH >= '2025-07-01'
ORDER BY COHORT_MONTH, MONTH_NUMBER;

-- -----------------------------------------------------------------------------
-- TUILE 6 (bonus) — Barres empilées : événements web par type et device
-- -----------------------------------------------------------------------------
SELECT EVENT_TYPE, DEVICE, COUNT(*) AS NB_EVENTS
FROM STAGING.STG_WEB_EVENTS
WHERE EVENT_TS <= CURRENT_TIMESTAMP()
GROUP BY 1, 2
ORDER BY 1, 2;
