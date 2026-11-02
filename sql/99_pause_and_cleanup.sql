/* =============================================================================
   ShopFlow — Fin de journée / nettoyage
   Fichier : sql/99_pause_and_cleanup.sql
   -----------------------------------------------------------------------------
   Section 1 : à exécuter CHAQUE SOIR (contrôle des crédits).
   Section 2 : remise à zéro complète (fin de projet uniquement, irréversible
               au-delà de la période de Time Travel).
   ============================================================================= */

USE ROLE SHOPFLOW_ENGINEER;
USE DATABASE SHOPFLOW_DB;

/* -----------------------------------------------------------------------------
   1. PAUSE DU SOIR
   ----------------------------------------------------------------------------- */
-- Tasks : suspendre la root suffit à stopper tout le graphe
ALTER TASK IF EXISTS STAGING.TSK_LOAD_STG_PRODUCTS SUSPEND;

-- Dynamic Tables : plus aucun rafraîchissement planifié
ALTER DYNAMIC TABLE IF EXISTS MARTS.DT_DAILY_REVENUE    SUSPEND;
ALTER DYNAMIC TABLE IF EXISTS MARTS.DT_TOP_PRODUCTS     SUSPEND;
ALTER DYNAMIC TABLE IF EXISTS MARTS.DT_CUSTOMER_COHORTS SUSPEND;

-- Warehouses (erreur sans conséquence si déjà suspendu par AUTO_SUSPEND)
ALTER WAREHOUSE IF EXISTS WH_INGEST    SUSPEND;
ALTER WAREHOUSE IF EXISTS WH_TRANSFORM SUSPEND;
ALTER WAREHOUSE IF EXISTS WH_BI        SUSPEND;

SHOW WAREHOUSES LIKE 'WH_%';          -- state = SUSPENDED

-- Consommation de la journée (latence ACCOUNT_USAGE : jusqu'à ~3 h ; ACCOUNTADMIN)
-- SELECT WAREHOUSE_NAME, ROUND(SUM(CREDITS_USED), 3) AS credits
-- FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
-- WHERE START_TIME >= DATEADD(DAY, -1, CURRENT_TIMESTAMP()) AND WAREHOUSE_NAME LIKE 'WH_%'
-- GROUP BY 1;

/* Reprise le lendemain :
   SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('SHOPFLOW_DB.STAGING.TSK_LOAD_STG_PRODUCTS');
   ALTER DYNAMIC TABLE MARTS.DT_DAILY_REVENUE    RESUME;
   ALTER DYNAMIC TABLE MARTS.DT_TOP_PRODUCTS     RESUME;
   ALTER DYNAMIC TABLE MARTS.DT_CUSTOMER_COHORTS RESUME;
   (les warehouses redémarrent seuls : AUTO_RESUME = TRUE)                       */


/* -----------------------------------------------------------------------------
   2. REMISE À ZÉRO COMPLÈTE — décommenter volontairement
   -----------------------------------------------------------------------------
USE ROLE SYSADMIN;
DROP DATABASE IF EXISTS SHOPFLOW_DB;          -- récupérable par UNDROP DATABASE pendant la rétention
DROP WAREHOUSE IF EXISTS WH_INGEST;
DROP WAREHOUSE IF EXISTS WH_TRANSFORM;
DROP WAREHOUSE IF EXISTS WH_BI;
USE ROLE ACCOUNTADMIN;
DROP INTEGRATION IF EXISTS SI_SHOPFLOW_S3;
USE ROLE SECURITYADMIN;
DROP ROLE IF EXISTS SHOPFLOW_ANALYST;
DROP ROLE IF EXISTS SHOPFLOW_ENGINEER;
*/
