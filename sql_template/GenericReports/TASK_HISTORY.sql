--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset TASK_HISTORY:1 runOnChange:true stripComments:true
--labels: "TASK_HISTORY or GENERIC"

DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_TASK_HISTORY;

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON 0 */3 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='TASK_VW_SNOWFLAKE_TASK_HISTORY',
--VAR_NAME='TASK_SCHEDULE';

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_TASK_HISTORY SUSPEND;

CREATE TRANSIENT TABLE IF NOT EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_TASK_HISTORY_TABLE (
	SH_KEY BINARY(20),
    ACCOUNT_LOCATOR VARCHAR(16777216),
	REGION_NAME VARCHAR(16777216),
	ORGANIZATION_NAME VARCHAR(16777216),
	QUERY_ID VARCHAR(16777216),
	TASK_NAME VARCHAR(16777216),
	DATABASE_NAME VARCHAR(16777216),
	SCHEMA_NAME VARCHAR(16777216),
	QUERY_TEXT VARCHAR(16777216),
	STATE VARCHAR(12),
	ERROR_CODE VARCHAR(16777216),
	ERROR_MESSAGE VARCHAR(16777216),
	SCHEDULED_TIME TIMESTAMP_LTZ(3),
	QUERY_START_TIME TIMESTAMP_LTZ(3),
	COMPLETED_TIME TIMESTAMP_LTZ(3),
	SCHEDULED_TIME_PBI VARCHAR(16777216),
	QUERY_START_TIME_PBI VARCHAR(16777216),
	COMPLETED_TIME_PBI VARCHAR(16777216),
	ROOT_TASK_ID VARCHAR(16777216),
	GRAPH_VERSION NUMBER(38,0),
	RUN_ID NUMBER(38,0),
	RETURN_VALUE VARCHAR(16777216),
    CONSTRAINT PKEY_1 PRIMARY KEY (ACCOUNT_LOCATOR,REGION_NAME,TASK_NAME,ROOT_TASK_ID,RUN_ID,SCHEDULED_TIME,GRAPH_VERSION)
);

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_TASK_HISTORY()
  returns string not null
  language javascript
  as
  '
    const sql_begin_trans = snowflake.createStatement({ sqlText:`BEGIN TRANSACTION;`});
    const sql_commit_trans = snowflake.createStatement({ sqlText:`COMMIT;`});
    try{
        sql_begin_trans.execute();
        var my_sql_command_1 = "CREATE OR REPLACE TEMPORARY TABLE CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_TASK_HISTORY_TABLE_TEMP AS " +
                        "select  "+
                                    "     sha1_binary( concat( CURRENT_REGION() " +
                                	"                             ,\'|\', T.ACCOUNT_LOCATOR " +
                                    "                              ,\'|\', ifnull( TH.QUERY_ID, \'~\' ) " +
                                    "                              ,\'|\', ifnull( TH.NAME, \'~\' ) " +
                                    "                              ,\'|\', ifnull( TH.DATABASE_NAME, \'~\' ) " +
                                    "                              ,\'|\', ifnull( TH.SCHEMA_NAME, \'~\' ) " +
                                    "                              ,\'|\', ifnull( TH.ROOT_TASK_ID, \'~\' ) " +
                                    "                              ,\'|\', ifnull( TH.RUN_ID, -9 ) " +
                                    "                              ,\'|\', ifnull( TH.GRAPH_VERSION, -1 ) " +
                                    "                             ,\'|\', to_char( SCHEDULED_TIME, \'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM\'  )  " +
                                    "                             ) " +
                                    "                    )   SH_KEY " +
                            ",T.ACCOUNT_LOCATOR AS ACCOUNT_LOCATOR " +
                            ",CURRENT_REGION() AS REGION_NAME " +
                            ",T.VAR_VALUE AS ORGANIZATION_NAME " +
                            ",TH.QUERY_ID " +
                            ",TH.NAME AS TASK_NAME " +
                            ",TH.DATABASE_NAME " +
                            ",TH.SCHEMA_NAME " +
                            ",TH.QUERY_TEXT " +
                            ",TH.STATE " +
                            ",TH.ERROR_CODE " +
                            ",TH.ERROR_MESSAGE " +
                            ",TH.SCHEDULED_TIME " +
                            ",TH.QUERY_START_TIME " +
                            ",TH.COMPLETED_TIME " +
                            ",TH.SCHEDULED_TIME::TIMESTAMP_NTZ::STRING SCHEDULED_TIME_PBI " +
                            ",TH.QUERY_START_TIME::TIMESTAMP_NTZ::STRING QUERY_START_TIME_PBI " +
                            ",TH.COMPLETED_TIME::TIMESTAMP_NTZ::STRING COMPLETED_TIME_PBI " +
                            ",TH.ROOT_TASK_ID " +
                            ",TH.GRAPH_VERSION " +
                            ",TH.RUN_ID " +
                            ",TH.RETURN_VALUE " +
                           "FROM " +
                            "  SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY TH, " +
                            "  table(get_var(\'ORGANIZATION\',\'GLOBAL\',CURRENT_ACCOUNT(),CURRENT_REGION())) T " +
                           "WHERE TH.SCHEDULED_TIME >= (SELECT NVL(DATEADD(hr,-36,MAX(SCHEDULED_TIME)),DATEADD(MONTH,-12,CURRENT_DATE)) FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_TASK_HISTORY_TABLE);"


        var statement_1 = snowflake.createStatement( {sqlText: my_sql_command_1} );
        var result_set_1 = statement_1.execute();

        var my_sql_command_2 = "MERGE INTO CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_TASK_HISTORY_TABLE T USING CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_TASK_HISTORY_TABLE_TEMP S " +
                             "ON (T.SH_KEY = S.SH_KEY) " +
                             "WHEN NOT MATCHED THEN " +
                             "INSERT (SH_KEY,ACCOUNT_LOCATOR,REGION_NAME,ORGANIZATION_NAME,QUERY_ID,TASK_NAME,DATABASE_NAME,SCHEMA_NAME,QUERY_TEXT,STATE,ERROR_CODE,ERROR_MESSAGE,SCHEDULED_TIME,QUERY_START_TIME,COMPLETED_TIME,SCHEDULED_TIME_PBI,QUERY_START_TIME_PBI,COMPLETED_TIME_PBI,ROOT_TASK_ID,GRAPH_VERSION,RUN_ID,RETURN_VALUE) " +
                             "VALUES (S.SH_KEY,S.ACCOUNT_LOCATOR,S.REGION_NAME,S.ORGANIZATION_NAME,S.QUERY_ID,S.TASK_NAME,S.DATABASE_NAME,S.SCHEMA_NAME,S.QUERY_TEXT,S.STATE,S.ERROR_CODE,S.ERROR_MESSAGE,S.SCHEDULED_TIME,S.QUERY_START_TIME,S.COMPLETED_TIME,S.SCHEDULED_TIME_PBI,S.QUERY_START_TIME_PBI,S.COMPLETED_TIME_PBI,S.ROOT_TASK_ID,S.GRAPH_VERSION,S.RUN_ID,S.RETURN_VALUE) "+
                             "WHEN MATCHED THEN " +
                             "UPDATE SET STATE = S.STATE,COMPLETED_TIME = S.COMPLETED_TIME,ERROR_CODE=S.ERROR_CODE,ERROR_MESSAGE=S.ERROR_MESSAGE,COMPLETED_TIME_PBI=S.COMPLETED_TIME::TIMESTAMP_NTZ::STRING,RETURN_VALUE=S.RETURN_VALUE;"

        var statement_2 = snowflake.createStatement( {sqlText: my_sql_command_2} );
        var result_set_2 = statement_2.execute();

        var my_sql_command_3 = "DELETE FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_TASK_HISTORY_TABLE " +
                                 "WHERE SCHEDULED_TIME <= (select dateadd(day,-var_value,current_date) from table(get_var(\'DAYS_TO_RETAIN\',\'VW_SNOWFLAKE_TASK_HISTORY_TABLE\',CURRENT_ACCOUNT(),CURRENT_REGION()))) " +
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
    return "Success" ;
  ';

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','TASK_VW_SNOWFLAKE_TASK_HISTORY',CURRENT_ACCOUNT(),CURRENT_REGION())));

CREATE OR REPLACE task CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_TASK_HISTORY
    WAREHOUSE = ${TASK_WAREHOUSE}
    SCHEDULE =  $TASK_SCHEDULE
AS
    CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_TASK_HISTORY();

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_TASK_HISTORY RESUME;

CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_TASK_HISTORY();

CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_TASK_HISTORY AS
  SELECT DISTINCT
TH.*, datediff(second, TH.QUERY_START_TIME, TH.COMPLETED_TIME) AS ELAPSED_TIME_IN_SEC
        FROM TABLE(CDOPS_STATESTORE.REPORTING.RESOLVE_MEMBER_RESOURCE_MAPPING_UDF()) AS C, CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_TASK_HISTORY_TABLE TH
  WHERE
      C.DATABASE_PATTERN IS NOT NULL AND RLIKE(TH.DATABASE_NAME,C.DATABASE_PATTERN)
  ;

-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_TASK_HISTORY;
-- rollback DROP TABLE "CDOPS_STATESTORE"."REPORTING"."VW_SNOWFLAKE_TASK_HISTORY_TABLE";
-- rollback DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_TASK_HISTORY;
-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_TASK_HISTORY();
