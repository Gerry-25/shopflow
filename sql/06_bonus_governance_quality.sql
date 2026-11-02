/* =============================================================================
   ShopFlow — BONUS : gouvernance (RGPD) & qualité de données
   Fichier : sql/06_bonus_governance_quality.sql
   -----------------------------------------------------------------------------
     1. Masking policy sur STG_CUSTOMERS.EMAIL (et IP des événements web)
     2. Row access policy « par région » (régions SIMULÉES, cf. note)
     3. Tests de qualité : procédure stockée + task d'alerte dans le graphe
   ⚠ Masking et row access policies nécessitent l'édition Enterprise
     (c'est le cas des comptes d'essai Snowflake).
   ============================================================================= */

USE ROLE SECURITYADMIN;
GRANT CREATE MASKING POLICY, CREATE ROW ACCESS POLICY
  ON SCHEMA SHOPFLOW_DB.STAGING TO ROLE SHOPFLOW_ENGINEER;

-- Pour tester le masquage, l'analyste doit pouvoir lire STG_CUSTOMERS
GRANT USAGE ON SCHEMA SHOPFLOW_DB.STAGING TO ROLE SHOPFLOW_ANALYST;
GRANT SELECT ON TABLE SHOPFLOW_DB.STAGING.STG_CUSTOMERS TO ROLE SHOPFLOW_ANALYST;
GRANT SELECT ON TABLE SHOPFLOW_DB.STAGING.STG_ORDERS    TO ROLE SHOPFLOW_ANALYST;

USE ROLE SHOPFLOW_ENGINEER;
USE WAREHOUSE WH_TRANSFORM;
USE DATABASE SHOPFLOW_DB;
USE SCHEMA STAGING;


/* -----------------------------------------------------------------------------
   1. MASKING POLICIES
   Seul SHOPFLOW_ENGINEER voit l'e-mail en clair ; les autres voient le domaine
   (utile pour l'analyse) : *****@example.org
   ----------------------------------------------------------------------------- */
CREATE MASKING POLICY IF NOT EXISTS STAGING.MP_EMAIL
  AS (VAL STRING) RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('SHOPFLOW_ENGINEER') THEN VAL
    WHEN VAL IS NULL THEN NULL
    ELSE '*****' || SUBSTR(VAL, POSITION('@' IN VAL))
  END
  COMMENT = 'RGPD - masque la partie locale des e-mails clients';

CREATE MASKING POLICY IF NOT EXISTS STAGING.MP_IP
  AS (VAL STRING) RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('SHOPFLOW_ENGINEER') THEN VAL
    ELSE REGEXP_REPLACE(VAL, '[0-9]+$', 'xxx')        -- 171.42.65.141 -> 171.42.65.xxx
  END
  COMMENT = 'RGPD - tronque le dernier octet des adresses IP';

ALTER TABLE STAGING.STG_CUSTOMERS  MODIFY COLUMN EMAIL      SET MASKING POLICY STAGING.MP_EMAIL;
ALTER TABLE STAGING.STG_WEB_EVENTS MODIFY COLUMN IP_ADDRESS SET MASKING POLICY STAGING.MP_IP;

-- Test
SELECT CUSTOMER_ID, EMAIL FROM STAGING.STG_CUSTOMERS LIMIT 3;     -- en clair
USE ROLE SHOPFLOW_ANALYST;
USE SECONDARY ROLES NONE;     -- sinon les rôles secondaires peuvent « démasquer »
SELECT CUSTOMER_ID, EMAIL FROM SHOPFLOW_DB.STAGING.STG_CUSTOMERS LIMIT 3;   -- *****@example.org
USE SECONDARY ROLES ALL;
USE ROLE SHOPFLOW_ENGINEER;


/* -----------------------------------------------------------------------------
   2. ROW ACCESS POLICY « PAR RÉGION »
   Note : le dataset ne contient pas de région, et les villes sont fictives
   (générées par Faker). On SIMULE donc une région par client (hash stable)
   uniquement pour démontrer le mécanisme. En réel : table de référence
   ville -> région fournie par le métier.
   ----------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS STAGING.REF_CUSTOMER_REGION AS
SELECT CUSTOMER_ID,
       DECODE(MOD(ABS(HASH(CUSTOMER_ID)), 4), 0, 'NORD', 1, 'SUD', 2, 'EST', 'OUEST') AS REGION
FROM STAGING.STG_CUSTOMERS;

-- Qui a le droit de voir quelle région
CREATE TABLE IF NOT EXISTS STAGING.REF_REGION_ACCESS (ROLE_NAME VARCHAR, REGION VARCHAR);
MERGE INTO STAGING.REF_REGION_ACCESS t
USING (SELECT 'SHOPFLOW_ANALYST' AS ROLE_NAME, 'NORD' AS REGION) s
ON t.ROLE_NAME = s.ROLE_NAME AND t.REGION = s.REGION
WHEN NOT MATCHED THEN INSERT VALUES (s.ROLE_NAME, s.REGION);

CREATE ROW ACCESS POLICY IF NOT EXISTS STAGING.RAP_ORDERS_REGION
  AS (P_CUSTOMER_ID VARCHAR) RETURNS BOOLEAN ->   -- argument préfixé : évite la confusion avec r.CUSTOMER_ID
  IS_ROLE_IN_SESSION('SHOPFLOW_ENGINEER')
  OR EXISTS (
    SELECT 1
    FROM STAGING.REF_CUSTOMER_REGION r
    JOIN STAGING.REF_REGION_ACCESS a ON a.REGION = r.REGION
    WHERE r.CUSTOMER_ID = P_CUSTOMER_ID
      AND a.ROLE_NAME = CURRENT_ROLE()
  )
  COMMENT = 'Limite les commandes visibles à la région autorisée pour le rôle';

ALTER TABLE STAGING.STG_ORDERS ADD ROW ACCESS POLICY STAGING.RAP_ORDERS_REGION ON (CUSTOMER_ID);

SELECT COUNT(*) FROM STAGING.STG_ORDERS;          -- engineer : tout
USE ROLE SHOPFLOW_ANALYST;
USE SECONDARY ROLES NONE;
SELECT COUNT(*) FROM SHOPFLOW_DB.STAGING.STG_ORDERS;   -- analyst : ~1/4 (région NORD)
USE SECONDARY ROLES ALL;
USE ROLE SHOPFLOW_ENGINEER;

/* ⚠ Les Dynamic Tables lisent STG_ORDERS avec le rôle PROPRIÉTAIRE
   (SHOPFLOW_ENGINEER) : les marts ne sont pas filtrés. Pour filtrer aussi les
   marts, poser la policy sur les DT elles-mêmes.
   Retirer la policy :
   ALTER TABLE STAGING.STG_ORDERS DROP ROW ACCESS POLICY STAGING.RAP_ORDERS_REGION; */


/* -----------------------------------------------------------------------------
   3. QUALITÉ DE DONNÉES : procédure + task d'alerte
   Chaque contrôle écrit un résultat dans DQ_RESULTS. Si un contrôle bloquant
   échoue, la procédure lève une exception => la task passe en FAILED et
   l'anomalie est visible dans TASK_HISTORY (et dans Snowsight > Task graph).
   ----------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS STAGING.DQ_RESULTS (
  RUN_AT      TIMESTAMP_LTZ,
  CHECK_NAME  VARCHAR,
  SEVERITY    VARCHAR,     -- BLOCKING | WARNING
  FAILED_ROWS NUMBER,
  THRESHOLD   NUMBER,
  STATUS      VARCHAR      -- PASS | FAIL
);

CREATE OR REPLACE PROCEDURE STAGING.SP_RUN_DQ_CHECKS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  run_ts       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP();
  nb_blocking  INTEGER DEFAULT 0;
  nb_warning   INTEGER DEFAULT 0;
  dq_failed    EXCEPTION (-20001, 'ShopFlow DQ : au moins un contrôle bloquant en échec (voir STAGING.DQ_RESULTS)');
BEGIN
  INSERT INTO STAGING.DQ_RESULTS (RUN_AT, CHECK_NAME, SEVERITY, FAILED_ROWS, THRESHOLD, STATUS)
  SELECT :run_ts, CHECK_NAME, SEVERITY, FAILED_ROWS, THRESHOLD,
         IFF(FAILED_ROWS > THRESHOLD, 'FAIL', 'PASS')
  FROM (
    -- clés techniques uniques
    SELECT 'orders_duplicate_key' AS CHECK_NAME, 'BLOCKING' AS SEVERITY,
           COUNT(*) - COUNT(DISTINCT ORDER_KEY) AS FAILED_ROWS, 0 AS THRESHOLD
    FROM STAGING.STG_ORDERS
    UNION ALL
    -- montants obligatoires
    SELECT 'orders_null_amount', 'BLOCKING', COUNT_IF(TOTAL_AMOUNT IS NULL), 0
    FROM STAGING.STG_ORDERS
    UNION ALL
    -- total commande = somme des lignes (tolérance 5 centimes)
    SELECT 'orders_total_vs_items', 'BLOCKING', COUNT(*), 0
    FROM (SELECT o.ORDER_KEY
          FROM STAGING.STG_ORDERS o
          JOIN STAGING.STG_ORDER_ITEMS i ON i.ORDER_KEY = o.ORDER_KEY
          GROUP BY o.ORDER_KEY, o.TOTAL_AMOUNT
          HAVING ABS(o.TOTAL_AMOUNT - SUM(i.LINE_AMOUNT)) > 0.05)
    UNION ALL
    -- lignes de commande orphelines
    SELECT 'order_items_orphans', 'BLOCKING', COUNT(*), 0
    FROM STAGING.STG_ORDER_ITEMS i
    LEFT JOIN STAGING.STG_ORDERS o ON o.ORDER_KEY = i.ORDER_KEY
    WHERE o.ORDER_KEY IS NULL
    UNION ALL
    -- produits inconnus du catalogue
    SELECT 'order_items_unknown_product', 'BLOCKING', COUNT_IF(CATEGORY IS NULL), 0
    FROM STAGING.STG_ORDER_ITEMS
    UNION ALL
    -- anomalie source connue : tolérée jusqu'à 12 % des commandes
    SELECT 'orders_before_signup_pct', 'WARNING',
           ROUND(100 * COUNT_IF(DQ_ORDER_BEFORE_SIGNUP) / NULLIF(COUNT(*), 0), 0), 12
    FROM STAGING.STG_ORDERS
    UNION ALL
    -- commandes datées dans le futur
    SELECT 'orders_future_dated', 'WARNING', COUNT_IF(ORDER_TS > CURRENT_TIMESTAMP()), 0
    FROM STAGING.STG_ORDERS
  );

  SELECT COUNT_IF(SEVERITY = 'BLOCKING' AND STATUS = 'FAIL'),
         COUNT_IF(SEVERITY = 'WARNING'  AND STATUS = 'FAIL')
    INTO :nb_blocking, :nb_warning
  FROM STAGING.DQ_RESULTS
  WHERE RUN_AT = :run_ts;

  IF (nb_blocking > 0) THEN
    RAISE dq_failed;
  END IF;
  RETURN 'DQ OK - ' || nb_warning || ' avertissement(s)';
END;
$$;

CALL STAGING.SP_RUN_DQ_CHECKS();
SELECT * FROM STAGING.DQ_RESULTS ORDER BY RUN_AT DESC, CHECK_NAME;

-- Brancher le contrôle en fin de graphe (la root doit être suspendue)
ALTER TASK STAGING.TSK_LOAD_STG_PRODUCTS SUSPEND;

CREATE OR REPLACE TASK STAGING.TSK_DQ_CHECKS
  WAREHOUSE = WH_TRANSFORM
  COMMENT   = 'Contrôles qualité après chargement STAGING'
  AFTER STAGING.TSK_LOAD_STG_ORDER_ITEMS, STAGING.TSK_LOAD_STG_WEB_EVENTS
AS
  CALL STAGING.SP_RUN_DQ_CHECKS();

SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('SHOPFLOW_DB.STAGING.TSK_LOAD_STG_PRODUCTS');

-- Alertes : exécutions en échec sur les dernières 24 h
SELECT NAME, STATE, SCHEDULED_TIME, ERROR_CODE, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
       SCHEDULED_TIME_RANGE_START => DATEADD('DAY', -1, CURRENT_TIMESTAMP()),
       ERROR_ONLY => TRUE))
ORDER BY SCHEDULED_TIME DESC;

/* Pour une alerte e-mail (optionnel) : créer une NOTIFICATION INTEGRATION de
   type EMAIL (ACCOUNTADMIN, adresse vérifiée dans Snowsight) puis un ALERT :
   CREATE ALERT STAGING.ALR_DQ_FAILED WAREHOUSE = WH_TRANSFORM SCHEDULE = '60 MINUTE'
     IF (EXISTS (SELECT 1 FROM STAGING.DQ_RESULTS
                 WHERE STATUS = 'FAIL' AND RUN_AT > DATEADD('HOUR', -1, CURRENT_TIMESTAMP())))
     THEN CALL SYSTEM$SEND_EMAIL('<INTEGRATION>', '<email vérifié>', 'ShopFlow DQ', 'Contrôle en échec');
*/
