--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset ORG_USAGE_IN_CURRENCY_DAILY:1 runOnChange:true stripComments:true
--labels: "ORG_USAGE_IN_CURRENCY_DAILY or GENERIC"

DROP TABLE IF EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_USAGE_IN_CURRENCY_DAILY_TABLE;
DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_USAGE_IN_CURRENCY_DAILY;
DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_USAGE_IN_CURRENCY_DAILY();
DROP VIEW IF EXISTS  CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_USAGE_IN_CURRENCY_DAILY;
DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.TABLE_VW_SNOWFLAKE_USAGE_IN_CURRENCY_DAILY;

create TRANSIENT TABLE IF NOT EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY_TABLE (
	SH_KEY BINARY(20),
	ORGANIZATION_NAME VARCHAR(16777216),
	ACCOUNT_LOCATOR VARCHAR(16777216),
	REGION VARCHAR(16777216),
    USAGE_DATE DATE,
	CONTRACT_NUMBER NUMBER(38,0),
	ACCOUNT_NAME VARCHAR(16777216),
	SERVICE_LEVEL VARCHAR(16777216),
	USAGE_TYPE VARCHAR(16777216),
	CURRENCY VARCHAR(16777216),
	USAGE NUMBER(38,2),
	USAGE_IN_CURRENCY NUMBER(38,6)
);

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON 0 */3 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='TASK_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY',
--VAR_NAME='TASK_SCHEDULE';

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY SUSPEND;

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY()
  returns string not null
  language javascript
  as
  '
    const sql_begin_trans = snowflake.createStatement({ sqlText:`BEGIN TRANSACTION;`});
    const sql_commit_trans = snowflake.createStatement({ sqlText:`COMMIT;`});

    const sql_temp_table = snowflake.createStatement({ sqlText:
    `
    CREATE OR REPLACE TEMPORARY TABLE CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY_TABLE_TEMP AS
     SELECT
            sha1_binary( concat(
                        \'|\', ifnull( organization_name, \'~\' )
                        ,\'|\', ifnull( contract_number, -9 )
                        ,\'|\', ifnull( account_name, \'~\' )
                        ,\'|\', ifnull( account_locator, \'~\' )
                        ,\'|\', ifnull( region, \'~\' )
                        ,\'|\', ifnull( service_level, \'~\' )
                        ,\'|\', ifnull( usage_type, \'~\' )
                        ,\'|\', to_char( usage_date, \'yyyy-mmm-dd\'  )
                       )
                )   SH_KEY,
            ORGANIZATION_NAME,
            CONTRACT_NUMBER,
            ACCOUNT_NAME,
            ACCOUNT_LOCATOR,
            REGION,
            SERVICE_LEVEL,
            USAGE_DATE,
            USAGE_TYPE,
            CURRENCY,
            SUM(USAGE) USAGE,
            SUM(USAGE_IN_CURRENCY) USAGE_IN_CURRENCY
        FROM (
            select ORGANIZATION_NAME,CONTRACT_NUMBER,ACCOUNT_NAME,ACCOUNT_LOCATOR,REGION,SERVICE_LEVEL,USAGE_DATE,
              case
                when usage_type in (\'adj for incl cloud services\',\'overage-adj for incl cloud services\',\'overage-cloud services\','','') then \'cloud services\'
                when usage_type=\'overage-automatic clustering\' then \'automatic clustering\'
                when usage_type=\'overage-compute\' then \'compute\'
                when usage_type=\'overage-snowpipe\' then \'snowpipe\'
                when usage_type=\'overage-materialized views\' then \'materialized views\'
                when usage_type=\'overage-search optimization\' then \'search optimization\'
                when usage_type=\'overage-serverless tasks\' then \'serverless tasks\'
                when usage_type=\'overage-replication\' then \'replication\'
                when usage_type=\'overage-storage\' then \'storage\'
              else usage_type
              end   USAGE_TYPE,
                    CURRENCY,USAGE,USAGE_IN_CURRENCY
             from
                SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
              )
         where
  usage_date >= (select NVL(MAX(usage_date),(SELECT MIN(USAGE_DATE)::DATE AS USAGE_DATE FROM snowflake.organization_usage.USAGE_IN_CURRENCY_DAILY))
  from CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY_TABLE)
  GROUP BY ORGANIZATION_NAME,CONTRACT_NUMBER,ACCOUNT_NAME,ACCOUNT_LOCATOR,REGION,SERVICE_LEVEL,USAGE_DATE,USAGE_TYPE,CURRENCY;
    `
    });

    const sql_merge_table = snowflake.createStatement({ sqlText:
    `
        MERGE INTO CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY_TABLE T USING CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY_TABLE_TEMP S
        ON (T.SH_KEY = S.SH_KEY)
        WHEN NOT MATCHED THEN
        INSERT (SH_KEY,ORGANIZATION_NAME,CONTRACT_NUMBER,ACCOUNT_NAME,ACCOUNT_LOCATOR,REGION,SERVICE_LEVEL,USAGE_DATE,USAGE_TYPE,CURRENCY,USAGE,USAGE_IN_CURRENCY)
        VALUES (S.SH_KEY,S.ORGANIZATION_NAME,S.CONTRACT_NUMBER,S.ACCOUNT_NAME,S.ACCOUNT_LOCATOR,S.REGION,S.SERVICE_LEVEL,S.USAGE_DATE,S.USAGE_TYPE,S.CURRENCY,S.USAGE,S.USAGE_IN_CURRENCY);
    `
    });

    const sql_delete_table = snowflake.createStatement({ sqlText:
    `
        DELETE FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY_TABLE
        WHERE USAGE_DATE <= (select dateadd(day,-var_value,current_date) from table(get_var(\'DAYS_TO_RETAIN\',\'VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY_TABLE\',CURRENT_ACCOUNT(),CURRENT_REGION())))
        AND ACCOUNT_LOCATOR = CURRENT_ACCOUNT();
    `
    });

    try{
        sql_begin_trans.execute();
        sql_temp_table.execute();
        sql_merge_table.execute();
        sql_delete_table.execute();

    }
    catch(err){
   const error = `Failed: Code: ${err.code}\\n  State: ${err.state}\\n  Message: ${err.message}\\n Stack Trace:\\n   ${err.stackTraceTxt}`;
   throw error;
               }
    finally{
        sql_commit_trans.execute();
    }
    return "Success";
  ';

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','TASK_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY',CURRENT_ACCOUNT(),CURRENT_REGION())));

CREATE OR REPLACE task CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY
    WAREHOUSE = ${TASK_WAREHOUSE}
    SCHEDULE = $TASK_SCHEDULE
AS
    CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY();


ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY RESUME;

CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY();


CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY AS
  SELECT DISTINCT
        CCD.*
    FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY_TABLE CCD
    WHERE
        (
          select NVL(VALUE IS NOT NULL, FALSE) AS VALUE from table(flatten(input => parse_json(current_available_roles()))) WHERE VALUE='CDOPS_REPORT_SERVICE'
          );

-- rollback DROP TABLE IF EXISTS "CDOPS_STATESTORE"."REPORTING"."VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY_TABLE";
-- rollback DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY;
-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY();
-- rollback DROP VIEW IF EXISTS  CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.TABLE_VW_SNOWFLAKE_ORG_USAGE_IN_CURRENCY_DAILY;