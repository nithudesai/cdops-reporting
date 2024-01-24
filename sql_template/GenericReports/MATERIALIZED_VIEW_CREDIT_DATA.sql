--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset MATERIALIZED_VIEW_CREDIT_DATA:1 runOnChange:true stripComments:true
--labels: "MATERIALIZED_VIEW_CREDIT_DATA or GENERIC"

DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFALKE_MATERIALIZED_VIEW_CREDIT_DATA_FL;

CREATE TRANSIENT TABLE IF NOT EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL_TABLE
(
	SH_KEY BINARY(20),
	ACCOUNT_LOCATOR VARCHAR(16777216),
	REGION_NAME VARCHAR(16777216),
    ORGANIZATION_NAME VARCHAR(16777216),
	START_DATE DATE,
	OPERATION_HOURS NUMBER(9,0),
	TIME_OF_DAY TIME(9),
	START_TIME TIMESTAMP_LTZ(6),
	END_TIME TIMESTAMP_LTZ(9),
	TABLE_NAME VARCHAR(16777216),
	SCHEMA_NAME VARCHAR(16777216),
	DATABASE_NAME VARCHAR(16777216),
	CREDITS_USED NUMBER(38,9)
);

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON 0 */3 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='TASK_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL',
--VAR_NAME='TASK_SCHEDULE';

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL SUSPEND;

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL()
  returns string not null
  language javascript
  as
  '
    const sql_begin_trans = snowflake.createStatement({ sqlText:`BEGIN TRANSACTION;`});
    const sql_commit_trans = snowflake.createStatement({ sqlText:`COMMIT;`});
    try{
      sql_begin_trans.execute();
      var my_sql_command_1  = "CREATE OR REPLACE TEMPORARY TABLE CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL_TABLE_TEMP AS " +
                                "SELECT " +
                                    "     sha1_binary( concat( CURRENT_REGION() " +
                                	"                             ,\'|\', T.ACCOUNT_LOCATOR " +
                                    "                             ,\'|\', ifnull( database_name, \'~\' )  " +
                                    "                             ,\'|\', ifnull( schema_name, \'~\' )  " +
                                    "                             ,\'|\', ifnull( table_name, \'~\' )  " +
                                    "                             ,\'|\', to_char( start_time, \'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM\'  )  " +
                                    "                             ) " +
                                    "                      )   SH_KEY " +
                                    ",T.ACCOUNT_LOCATOR AS ACCOUNT_LOCATOR " +
                                    ",CURRENT_REGION() AS REGION_NAME " +
                                    ",T.VAR_VALUE AS ORGANIZATION_NAME " +
                                    ",TO_DATE(START_TIME) AS START_DATE  " +
                                    ",DATEDIFF(HOUR, START_TIME, END_TIME) AS OPERATION_HOURS  " +
                                    ",TO_TIME(START_TIME) AS TIME_OF_DAY  " +
                                    ",START_TIME  " +
                                    ",END_TIME  " +
                                    ",TABLE_NAME  " +
                                    ",SCHEMA_NAME  " +
                                    ",DATABASE_NAME  " +
                                    ",CREDITS_USED  " +
                                "FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY A , table(get_var(\'ORGANIZATION\',\'GLOBAL\',CURRENT_ACCOUNT(),CURRENT_REGION())) T " +
                                  "WHERE START_TIME >= (SELECT NVL(MAX(START_DATE),DATEADD(MONTH,-12,CURRENT_DATE)) FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL_TABLE);"


    var statement_1 = snowflake.createStatement( {sqlText: my_sql_command_1} );
    var result_set_1 = statement_1.execute();

    var my_sql_command_2 = "MERGE INTO CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL_TABLE T USING CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL_TABLE_TEMP S " +
                             "ON (T.SH_KEY = S.SH_KEY) " +
                             "WHEN NOT MATCHED THEN " +
                             "INSERT (SH_KEY,ACCOUNT_LOCATOR,REGION_NAME,ORGANIZATION_NAME,START_DATE,OPERATION_HOURS,TIME_OF_DAY,START_TIME,END_TIME,TABLE_NAME,SCHEMA_NAME,DATABASE_NAME,CREDITS_USED) " +
                             "VALUES (S.SH_KEY,S.ACCOUNT_LOCATOR,S.REGION_NAME,S.ORGANIZATION_NAME,S.START_DATE,S.OPERATION_HOURS,S.TIME_OF_DAY,S.START_TIME,S.END_TIME,S.TABLE_NAME,S.SCHEMA_NAME,S.DATABASE_NAME,S.CREDITS_USED);"

    var statement_2 = snowflake.createStatement( {sqlText: my_sql_command_2} );
    var result_set_2 = statement_2.execute();

    var my_sql_command_3 =   "DELETE FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL_TABLE " +
                             "WHERE START_TIME <= (select dateadd(day,-var_value,current_date) from table(get_var(\'DAYS_TO_RETAIN\',\'VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL_TABLE\',CURRENT_ACCOUNT(),CURRENT_REGION()))) " +
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

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','TASK_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL',CURRENT_ACCOUNT(),CURRENT_REGION())));

CREATE OR REPLACE task CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL
    WAREHOUSE = ${TASK_WAREHOUSE}
    SCHEDULE = $TASK_SCHEDULE
AS
    CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL();

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL RESUME;

CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL();

CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL AS
  SELECT DISTINCT
    MVH.*, datediff(second, MVH.START_TIME, MVH.END_TIME) AS ELAPSED_TIME_IN_SEC
  FROM TABLE(CDOPS_STATESTORE.REPORTING.RESOLVE_MEMBER_RESOURCE_MAPPING_UDF()) AS C, CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL_TABLE MVH
  WHERE
      RLIKE(MVH.DATABASE_NAME,C.DATABASE_PATTERN)
  ORDER BY START_DATE DESC;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL;
-- rollback DROP TABLE "CDOPS_STATESTORE"."REPORTING"."VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_TABLE";
-- rollback DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL;
-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_MATERIALIZED_VIEW_CREDIT_DATA_FL();
