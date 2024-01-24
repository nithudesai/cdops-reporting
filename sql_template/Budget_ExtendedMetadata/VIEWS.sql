--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset BUDGET_EXTENDED_VIEW:1 runOnChange:true stripComments:true
--labels: BUDGET
CREATE OR REPLACE SECURE VIEW CDOPS_STATESTORE.REPORTING.VW_DATABASE_METADATA
AS
  (SELECT c_name AS DATABASE_NAME,
          CASE
            WHEN o_owner IS NOT NULL THEN o_owner
            ELSE c_owner
          END AS OWNER,
          CASE
            WHEN o_comment IS NOT NULL THEN o_comment
            ELSE c_comment
          END AS COMMENT,
          CASE
            WHEN o_db_type IS NOT NULL THEN o_db_type
            ELSE c_db_type
          END AS DB_TYPE,
          CASE
            WHEN o_env IS NOT NULL THEN o_env
            ELSE c_env
          END AS ENV,
          CASE
            WHEN o_bu IS NOT NULL THEN o_bu
            ELSE c_bu
          END AS BU,
          CASE
            WHEN o_db_name IS NOT NULL THEN o_db_name
            ELSE c_db_name
          END AS DB_NAME
   FROM   (SELECT "name"    AS c_name,
                  "owner"   AS c_owner,
                  "comment" AS c_comment,
                  "db_type" AS c_db_type,
                  "env"     AS c_env,
                  "bu"      AS c_bu,
                  "db_name" AS c_db_name
           FROM   calculated_database_metadata) calculated
          left join (SELECT name    AS o_name,
                            owner   AS o_owner,
                            COMMENT AS o_comment,
                            db_type AS o_db_type,
                            env     AS o_env,
                            bu      AS o_bu,
                            db_name AS o_db_name
                     FROM   override_database_metadata) override
                 ON c_name = o_name
  );

CREATE OR REPLACE  VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_VW_DATABASE_METADATA
AS
  SELECT DISTINCT T.* FROM TABLE(CDOPS_STATESTORE.REPORTING.RESOLVE_MEMBER_RESOURCE_MAPPING_UDF()), CDOPS_STATESTORE.REPORTING.VW_DATABASE_METADATA T
  WHERE RLIKE(T.DATABASE_NAME,DATABASE_PATTERN) ;

CREATE OR REPLACE SECURE VIEW CDOPS_STATESTORE.REPORTING.VW_WAREHOUSE_METADATA
AS
  (SELECT c_name AS WAREHOUSE_NAME,
          CASE
            WHEN o_owner IS NOT NULL THEN o_owner
            ELSE c_owner
          END  AS OWNER,
          CASE
            WHEN o_comment IS NOT NULL THEN o_comment
            ELSE c_comment
          END AS COMMENT,
          CASE
            WHEN o_wh_type IS NOT NULL THEN o_wh_type
            ELSE c_wh_type
          END  AS WH_TYPE,
          CASE
            WHEN o_env IS NOT NULL THEN o_env
            ELSE c_env
          END  AS ENV,
          CASE
            WHEN o_bu IS NOT NULL THEN o_bu
            ELSE c_bu
          END AS BU,
          CASE
            WHEN o_db_name IS NOT NULL THEN o_db_name
            ELSE c_db_name
          END AS DB_NAME,
          CASE
            WHEN o_purpose IS NOT NULL THEN o_purpose
            ELSE c_purpose
          END  AS PURPOSE
   FROM   (SELECT "name"    AS c_name,
                  "owner"   AS c_owner,
                  "comment" AS c_comment,
                  "wh_type" AS c_wh_type,
                  "env"     AS c_env,
                  "bu"      AS c_bu,
                  "db_name" AS c_db_name,
                  "purpose" AS c_purpose
           FROM   calculated_warehouse_metadata) calculated
          left join (SELECT name    AS o_name,
                            owner   AS o_owner,
                            COMMENT AS o_comment,
                            wh_type AS o_wh_type,
                            env     AS o_env,
                            bu      AS o_bu,
                            db_name AS o_db_name,
                            purpose AS o_purpose
                     FROM   override_warehouse_metadata) override
                 ON c_name = o_name
  );

CREATE OR REPLACE  VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_VW_WAREHOUSE_METADATA
AS
  SELECT DISTINCT T.* FROM TABLE(CDOPS_STATESTORE.REPORTING.RESOLVE_MEMBER_RESOURCE_MAPPING_UDF()), CDOPS_STATESTORE.REPORTING.VW_WAREHOUSE_METADATA T
  WHERE RLIKE(T.WAREHOUSE_NAME,WAREHOUSE_PATTERN) ;

CREATE OR REPLACE SECURE VIEW CDOPS_STATESTORE.REPORTING.VW_WAREHOUSE_TAGS
AS
  (SELECT name,
         tag
  FROM raw_warehouse_tags
  );

CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING.VW_DATABASE_TAGS
AS
  (SELECT name,
         tag
  FROM raw_database_tags
  );

CREATE OR REPLACE SECURE VIEW CDOPS_STATESTORE.REPORTING.VW_WORKSPACE_MONTHLY_BUDGET
AS
WITH RECURSIVE WORKSPACE_DATES(WS_ID, ACTIVE_DATE)
AS
(
  SELECT
    WS_ID,
    "PURCHASED_DATE"
  FROM
    WORKSPACE_PURCHASES
  UNION ALL
  SELECT
    WORKSPACE_DATES.WS_ID,
    DATEADD(MONTH, 1, ACTIVE_DATE) AS NEXT_DATE
  FROM
    WORKSPACE_DATES
    JOIN
      WORKSPACE_PURCHASES
      ON (WORKSPACE_DATES.WS_ID = WORKSPACE_PURCHASES."WS_ID")
  WHERE
    ACTIVE_DATE < DATEADD(MONTH, - 1, WORKSPACE_PURCHASES."RENEWAL_DATE")
)
SELECT
  WORKSPACE_DATES.WS_ID,
  ACTIVE_DATE,
  PUR_ID,
  MONTHLY_CREDIT_BUDGET
FROM
  WORKSPACE_DATES
  JOIN
    (
      SELECT
        WORKSPACE_PURCHASES."WS_ID" AS WS_ID,
        WORKSPACE_PURCHASES."PUR_ID" AS PUR_ID,
        FLOOR("PURCHASED_CREDITS" / DATEDIFF(MONTH, "PURCHASED_DATE", DATEADD(DAYS, 1, "RENEWAL_DATE"))) AS MONTHLY_CREDIT_BUDGET
      FROM
        WORKSPACE_PURCHASES
        INNER JOIN
          WORKSPACE_TRACKER
          ON WORKSPACE_PURCHASES.WS_ID = WORKSPACE_TRACKER.WS_ID
    )
    WS_DETAIL
    ON WS_DETAIL.WS_ID = WORKSPACE_DATES.WS_ID
ORDER BY
  WS_ID,
  ACTIVE_DATE;

CREATE OR REPLACE SECURE VIEW CDOPS_STATESTORE.REPORTING.VW_BU_WAREHOUSE_SPEND_MONTHLY
AS
SELECT
  WCU.WAREHOUSE_NAME,
  WAREHOUSE_METADATA.BU ,
  ENV,
  PURPOSE,
  DB_NAME,
  TOTAL_CREDITS_USED,
  PURCHASED_RATE,
  WORKSPACE,
  WS_ID,
  PUR_ID,
  BUDGET.BUDGET_MONTH,
  MONTHLY_CREDIT_BUDGET
FROM
  VW_WAREHOUSE_METADATA AS WAREHOUSE_METADATA
  INNER JOIN
    (
      SELECT
        WAREHOUSE_NAME,
        SUM(CREDITS_USED) AS TOTAL_CREDITS_USED,
        DATE_TRUNC('month', TO_DATE(ACTIVE_DATE)) AS ACTIVE_MONTH
      FROM
        WAREHOUSE_CREDIT_USAGE
      GROUP BY
        WAREHOUSE_NAME,
        ACTIVE_MONTH
    )
    WCU
    ON WCU.WAREHOUSE_NAME = WAREHOUSE_METADATA.WAREHOUSE_NAME
  LEFT JOIN
    (
      SELECT
        WORKSPACE_MONTHLY_BUDGET.WS_ID,
        WORKSPACE_MONTHLY_BUDGET.PUR_ID,
        WORKSPACE_MONTHLY_BUDGET.ACTIVE_DATE AS BUDGET_MONTH,
        WORKSPACE_MONTHLY_BUDGET.MONTHLY_CREDIT_BUDGET,
        WORKSPACE_PURCHASES.PURCHASED_RATE AS PURCHASED_RATE,
        WORKSPACE_TRACKER.BU AS BU,
        WORKSPACE_TRACKER.WORKSPACE AS WORKSPACE
      FROM
        VW_WORKSPACE_MONTHLY_BUDGET AS WORKSPACE_MONTHLY_BUDGET
        INNER JOIN
          WORKSPACE_PURCHASES
          ON WORKSPACE_MONTHLY_BUDGET.WS_ID = WORKSPACE_PURCHASES.WS_ID
          AND WORKSPACE_MONTHLY_BUDGET.PUR_ID = WORKSPACE_PURCHASES.PUR_ID
        INNER JOIN
          WORKSPACE_TRACKER
          ON WORKSPACE_TRACKER.WS_ID = WORKSPACE_MONTHLY_BUDGET.WS_ID
    )
    BUDGET
    ON WAREHOUSE_METADATA.BU = BUDGET.BU
    AND WCU.ACTIVE_MONTH = BUDGET.BUDGET_MONTH
ORDER BY
  BUDGET.WS_ID,
  BU,
  BUDGET.BUDGET_MONTH;

create or replace view CDOPS_STATESTORE.REPORTING.VW_WAREHOUSE_DETAILS as (
  select
      wst.WS_ID
      ,wst.WORKSPACE
      ,wst.WORKSPACE_DESC
      ,wst.WORKSPACE_TYPE
      ,wst.BU
      ,wsp.Purchased_Credits
      ,wsp.Purchased_Rate
      ,wsp.Purchased_Amount
      ,wsp.Purchased_Date
      ,wsp.Renewal_Date
  from WORKSPACE_TRACKER as wst
  inner join WORKSPACE_PURCHASES as wsp on wst.WS_ID=wsp.WS_ID
 );
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_DATABASE_METADATA;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_VW_DATABASE_METADATA;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_WAREHOUSE_METADATA;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_VW_WAREHOUSE_METADATA;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_WAREHOUSE_TAGS;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_DATABASE_TAGS;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_WORKSPACE_MONTHLY_BUDGET;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_BU_WAREHOUSE_SPEND_MONTHLY;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_WAREHOUSE_DETAILS;

