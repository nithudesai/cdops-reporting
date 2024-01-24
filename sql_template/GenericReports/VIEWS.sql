--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset USAGE_MONTHLY_SUMMARY:1 runOnChange:true stripComments:true
--labels: "USAGE_MONTHLY_SUMMARY or GENERIC"
CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_CREDIT_USAGE_MONTHLY_SUMMARY
AS
select
    date_trunc(month, START_TIME) AS USAGE_MONTH
  , SUM(CREDITS_USED) as CREDITS_USED
  , SUM(CREDITS_USED_COMPUTE ) as CREDITS_USED_COMPUTE
  , SUM(CREDITS_USED_CLOUD_SERVICES ) as CREDITS_USED_CLOUD_SERVICES
from SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
GROUP BY date_trunc(month, START_TIME)
ORDER BY date_trunc(month, START_TIME) ;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_CREDIT_USAGE_MONTHLY_SUMMARY;

--changeset DIM_DATE:1 runOnChange:true stripComments:true
--labels: "DIM_DATE or GENERIC"
CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING.VW_DIM_DATE
AS
 -- GENERATES A DAY ROW for 730 days
  WITH CTE_DATE_GENERATOR AS (
    SELECT DATEADD(DAY, SEQ4() * -1, CURRENT_DATE) AS DAY_ROW
      FROM TABLE(GENERATOR(ROWCOUNT=>730))

  )
  SELECT TO_DATE(DAY_ROW) AS DATE
        ,YEAR(DAY_ROW) AS YEAR
        ,QUARTER(DAY_ROW) AS QUARTER
        ,MONTH(DAY_ROW) AS MONTH_NUM
        ,MONTHNAME(DAY_ROW) AS MONTH_NAME
        ,WEEKOFYEAR(DAY_ROW) AS WEEK_NUM
        ,DAY(DAY_ROW) AS DAY_NUM
        ,DAYNAME(DAY_ROW) AS DAY_NAME
        ,DAYOFWEEK(DAY_ROW) AS DAY_OF_WEEK
        ,DAYOFYEAR(DAY_ROW) AS DAY_OF_YEAR
    FROM CTE_DATE_GENERATOR
;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_DIM_DATE;