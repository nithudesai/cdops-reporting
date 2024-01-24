--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset STORAGE_USAGE_MONTHLY_SUMMARY:1 runOnChange:true stripComments:true
--labels: "STORAGE_USAGE_MONTHLY_SUMMARY or GENERIC"

DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY;

CREATE TRANSIENT TABLE IF NOT EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY_TABLE (
	SH_KEY BINARY(20),
	ACCOUNT_LOCATOR VARCHAR(16777216),
	REGION_NAME VARCHAR(16777216),
    ORGANIZATION_NAME VARCHAR(16777216),
    USAGE_MONTH DATE,
	TOTAL_BILLABLE_STORAGE_GB FLOAT,
	STORAGE_BILLABLE_STORAGE_GB FLOAT,
	STAGE_BILLABLE_STORAGE_GB FLOAT,
	FAILSAFE_BILLABLE_STORAGE_GB FLOAT
);

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON 0 */3 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='TASK_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY',
--VAR_NAME='TASK_SCHEDULE';

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY SUSPEND;

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY()
  returns string not null
  language javascript
  as
  '
    const sql_begin_trans = snowflake.createStatement({ sqlText:`BEGIN TRANSACTION;`});
    const sql_commit_trans = snowflake.createStatement({ sqlText:`COMMIT;`});
    try{
      sql_begin_trans.execute();
      var my_sql_command_1  = "CREATE OR REPLACE TEMPORARY TABLE CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY_TABLE_TEMP AS " +
                                "SELECT " +
                                    "     sha1_binary( concat( CURRENT_REGION() " +
                                	"                             ,\'|\', T.ACCOUNT_LOCATOR " +
                                    "                             ,\'|\', to_char( USAGE_DATE, \'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM\'  )  " +
                                    "                             ) " +
                                    "                      )   SH_KEY " +
                                    ",T.ACCOUNT_LOCATOR AS ACCOUNT_LOCATOR " +
                                    ",CURRENT_REGION() AS REGION_NAME " +
                                    ",T.VAR_VALUE AS ORGANIZATION_NAME " +
                                    ",USAGE_DATE as USAGE_MONTH " +
                                    ",floor((storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 3),3) as TOTAL_BILLABLE_STORAGE_GB " +
                                    ",floor((storage_bytes ) / power(1024, 3)) as STORAGE_BILLABLE_STORAGE_GB " +
                                    ",floor((stage_bytes ) / power(1024, 3)) as STAGE_BILLABLE_STORAGE_GB " +
                                    ",floor((failsafe_bytes ) / power(1024, 3)) as FAILSAFE_BILLABLE_STORAGE_GB " +
                                  " from SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE, " +
                " TABLE(get_var(\'ORGANIZATION\',\'GLOBAL\',CURRENT_ACCOUNT(),CURRENT_REGION())) T " +
       "WHERE USAGE_MONTH >= (SELECT NVL(MAX(USAGE_MONTH),DATEADD(MONTH,-12,CURRENT_DATE)) FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY_TABLE);"

    var statement_1 = snowflake.createStatement( {sqlText: my_sql_command_1} );
    var result_set_1 = statement_1.execute();

    var my_sql_command_2 = "MERGE INTO CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY_TABLE T USING CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY_TABLE_TEMP S " +
                             "ON (T.SH_KEY = S.SH_KEY) " +
                             "WHEN NOT MATCHED THEN " +
                             "INSERT (SH_KEY,ACCOUNT_LOCATOR,REGION_NAME,ORGANIZATION_NAME,USAGE_MONTH,TOTAL_BILLABLE_STORAGE_GB,STORAGE_BILLABLE_STORAGE_GB,STAGE_BILLABLE_STORAGE_GB,FAILSAFE_BILLABLE_STORAGE_GB) " +
                             "VALUES (S.SH_KEY,S.ACCOUNT_LOCATOR,S.REGION_NAME,S.ORGANIZATION_NAME,S.USAGE_MONTH,S.TOTAL_BILLABLE_STORAGE_GB,S.STORAGE_BILLABLE_STORAGE_GB,S.STAGE_BILLABLE_STORAGE_GB,S.FAILSAFE_BILLABLE_STORAGE_GB);"

    var statement_2 = snowflake.createStatement( {sqlText: my_sql_command_2} );
    var result_set_2 = statement_2.execute();

    var my_sql_command_3 =   "DELETE FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY_TABLE " +
                             "WHERE USAGE_MONTH <= (select dateadd(day,-var_value,current_date) from table(get_var(\'DAYS_TO_RETAIN\',\'VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY_TABLE\',CURRENT_ACCOUNT(),CURRENT_REGION()))) " +
                             "AND ACCOUNT_LOCATOR = CURRENT_ACCOUNT()";

    var statement_3 = snowflake.createStatement( {sqlText: my_sql_command_3} );
    var result_set_3 = statement_3.execute();

    var my_sql_command = my_sql_command_1 + my_sql_command_2 + my_sql_command_3
    }
    catch(err){
   const error = `Failed: Code: ${err.code}\\n  State: ${err.state}\\n  Message: ${err.message}\\n Stack Trace:\\n   ${err.stackTraceTxt}`;
   throw error;
               }
    finally{
        sql_commit_trans.execute();
    }
    return "Success-"+ my_sql_command ;
  ';

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','TASK_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY',CURRENT_ACCOUNT(),CURRENT_REGION())));

CREATE OR REPLACE task CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY
    WAREHOUSE = ${TASK_WAREHOUSE}
    SCHEDULE = $TASK_SCHEDULE
AS
    CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY();

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY RESUME;

CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY();

CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY AS
  SELECT
    ST.*
        FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY_TABLE ST
  ORDER BY USAGE_MONTH DESC;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY;
-- rollback DROP TABLE "CDOPS_STATESTORE"."REPORTING"."VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY_TABLE";
-- rollback DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY;
-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_STORAGE_USAGE_MONTHLY_SUMMARY();

