--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY:1 runOnChange:true stripComments:true
--labels: "DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY or GENERIC"

DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL;

CREATE TRANSIENT TABLE IF NOT EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL_TABLE (
  	SH_KEY BINARY(20),
	ACCOUNT_LOCATOR VARCHAR(16777216),
	REGION_NAME VARCHAR(16777216),
	USAGE_MONTH DATE,
	DATABASE_NAME VARCHAR(16777216),
	DATABASE_ID NUMBER(38,0),
	AVG_DATABASE_STORAGE_INCLUDE_TIMETRAVEL_GB FLOAT,
	AVG_FAILSAFE_STORAGE_GB FLOAT,
	TOTAL_STORAGE_GB FLOAT,
    ORGANIZATION_NAME VARCHAR(16777216)
);

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON 0 */3 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='TASK_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL',
--VAR_NAME='TASK_SCHEDULE';

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL SUSPEND;

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL()
  returns string not null
  language javascript
  as
  '
    const sql_begin_trans = snowflake.createStatement({ sqlText:`BEGIN TRANSACTION;`});
    const sql_commit_trans = snowflake.createStatement({ sqlText:`COMMIT;`});
    try{
      sql_begin_trans.execute();
    var my_sql_command = ""
    var my_sql_command_1 = "CREATE OR REPLACE TEMPORARY TABLE CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL_TABLE_TEMP AS " +
                            "SELECT  " +
                            "sha1_binary( concat( CURRENT_REGION() " +
                             "         ,\'|\', T.ACCOUNT_LOCATOR " +
                             "         ,\'|\', to_char( database_name ) " +
                             "         ,\'|\', to_char( usage_date, \'yyyy-mmm-dd\'  ) " +
                             "        ) " +
                             "     ) SH_KEY " +
                             ", T.ACCOUNT_LOCATOR AS ACCOUNT_LOCATOR " +
                             ", CURRENT_REGION() AS REGION_NAME " +
                             ", USAGE_DATE as USAGE_MONTH " +
                             ", DATABASE_NAME " +
                             ", DATABASE_ID " +
                             ", ROUND((AVERAGE_DATABASE_BYTES/ power(1024, 3)),3) AVG_DATABASE_STORAGE_INCLUDE_TIMETRAVEL_GB " +
                             ", ROUND((AVERAGE_FAILSAFE_BYTES/ power(1024, 3)),3) AVG_FAILSAFE_STORAGE_GB " +
                             ", (ROUND((AVERAGE_DATABASE_BYTES/ power(1024, 3)),3) + ROUND((AVERAGE_FAILSAFE_BYTES/ power(1024, 3)),3))  AS TOTAL_STORAGE_GB " +
                             ", T.VAR_VALUE AS ORGANIZATION_NAME " +
                             " from SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY, table(get_var(\'ORGANIZATION\',\'GLOBAL\',CURRENT_ACCOUNT(),CURRENT_REGION())) T" +
                             " WHERE USAGE_MONTH >= (SELECT NVL(MAX(USAGE_MONTH),DATEADD(MONTH,-12,CURRENT_DATE)) FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL_TABLE);"

   var statement_1 = snowflake.createStatement( {sqlText: my_sql_command_1} );
   var result_set_1 = statement_1.execute();

    var my_sql_command_2 = "MERGE INTO CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL_TABLE T USING CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL_TABLE_TEMP S " +
                           "ON (T.SH_KEY = S.SH_KEY) " +
                           "WHEN NOT MATCHED THEN " +
                           "INSERT (SH_KEY,ACCOUNT_LOCATOR,REGION_NAME,USAGE_MONTH,DATABASE_NAME,DATABASE_ID,AVG_DATABASE_STORAGE_INCLUDE_TIMETRAVEL_GB,AVG_FAILSAFE_STORAGE_GB,TOTAL_STORAGE_GB,ORGANIZATION_NAME) " +
                           "VALUES (S.SH_KEY,S.ACCOUNT_LOCATOR,S.REGION_NAME,S.USAGE_MONTH,S.DATABASE_NAME,S.DATABASE_ID,S.AVG_DATABASE_STORAGE_INCLUDE_TIMETRAVEL_GB,S.AVG_FAILSAFE_STORAGE_GB,S.TOTAL_STORAGE_GB,S.ORGANIZATION_NAME); "
    var statement_2 = snowflake.createStatement( {sqlText: my_sql_command_2} );
    var result_set_2 = statement_2.execute();

    var my_sql_command_3 = "DELETE FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL_TABLE T " +
                           "WHERE USAGE_MONTH <= (select dateadd(day,-var_value,current_date) from table(get_var(\'DAYS_TO_RETAIN\',\'VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL_TABLE\',CURRENT_ACCOUNT(),CURRENT_REGION()))) " +
                           "AND ACCOUNT_LOCATOR = CURRENT_ACCOUNT();"

    var statement_3 = snowflake.createStatement( {sqlText: my_sql_command_3} );
    var result_set_3 = statement_3.execute();

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

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','TASK_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL',CURRENT_ACCOUNT(),CURRENT_REGION())));

CREATE OR REPLACE task CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL
    WAREHOUSE = ${TASK_WAREHOUSE}
    SCHEDULE = $TASK_SCHEDULE
AS
    CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL();

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL RESUME;

CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL();

CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL AS
  SELECT DISTINCT
  SH.*
        FROM TABLE(CDOPS_STATESTORE.REPORTING.RESOLVE_MEMBER_RESOURCE_MAPPING_UDF()) AS C, CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL_TABLE SH
  WHERE RLIKE(SH.DATABASE_NAME,C.DATABASE_PATTERN)
  ORDER BY USAGE_MONTH DESC;

-- rollback DROP TABLE IF EXISTS "CDOPS_STATESTORE"."REPORTING"."VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL_TABLE";
-- rollback DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL;
-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL();
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_DATABASE_STORAGE_USAGE_MONTHLY_SUMMARY_FL;
