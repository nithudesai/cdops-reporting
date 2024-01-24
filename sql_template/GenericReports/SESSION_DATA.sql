--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset SESSIONS:1 runOnChange:true stripComments:true
--labels: "SESSIONS or GENERIC"

DROP VIEW IF EXISTS  CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_SESSIONS;

CREATE TRANSIENT TABLE IF NOT EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_SESSIONS_TABLE (
	SH_KEY BINARY(20),
	ACCOUNT_LOCATOR VARCHAR(16777216),
	REGION_NAME VARCHAR(16777216),
    ORGANIZATION_NAME VARCHAR(16777216),
    SESSION_ID NUMBER(38,0),
	CREATED_ON DATE,
	USER_NAME VARCHAR(16777216),
	CLIENT_APPLICATION_ID VARCHAR(16777216),
	EVENT_TYPE VARCHAR(16777216),
	IS_SUCCESS VARCHAR(3)
);

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON 0 */3 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='TASK_VW_SNOWFLAKE_SESSIONS',
--VAR_NAME='TASK_SCHEDULE';

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_SESSIONS SUSPEND;

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_SESSIONS()
  returns string not null
  language javascript
  as
  '
    const sql_begin_trans = snowflake.createStatement({ sqlText:`BEGIN TRANSACTION;`});
    const sql_commit_trans = snowflake.createStatement({ sqlText:`COMMIT;`});
    try{
      sql_begin_trans.execute();
      var my_sql_command_1  = "CREATE OR REPLACE TEMPORARY TABLE CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_SESSIONS_TABLE_TEMP AS " +
                                "SELECT " +
                                    "     sha1_binary( concat( CURRENT_REGION() " +
                                	"                             ,\'|\', T.ACCOUNT_LOCATOR " +
                                    "                             ,\'|\', ifnull( S.SESSION_ID, \'-9\' )  " +
                                    "                             ,\'|\', ifnull( S.USER_NAME, \'~\' )  " +
                                    "                             ,\'|\', ifnull( S.CLIENT_APPLICATION_ID, \'~\' )  " +
                                    "                             ,\'|\', to_char( S.CREATED_ON, \'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM\'  )  " +
                                    "                             ) " +
                                    "                      )   SH_KEY " +
                                    ",T.ACCOUNT_LOCATOR AS ACCOUNT_LOCATOR " +
                                    ",CURRENT_REGION() AS REGION_NAME " +
                                    ",T.VAR_VALUE AS ORGANIZATION_NAME " +
                                    ",S.SESSION_ID " +
                                    ",S.CREATED_ON::DATE CREATED_ON " +
                                    ",S.USER_NAME " +
                                    ",S.CLIENT_APPLICATION_ID  " +
                                    ",L.EVENT_TYPE " +
                                    ",L.IS_SUCCESS " +
                                    "FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS S , SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY L, " +
                                    "TABLE(get_var(\'ORGANIZATION\',\'GLOBAL\',CURRENT_ACCOUNT(),CURRENT_REGION())) T " +
                                    "WHERE S.LOGIN_EVENT_ID=L.EVENT_ID AND " +
                                    "CREATED_ON >= (SELECT NVL(MAX(CREATED_ON),DATEADD(MONTH,-12,CURRENT_DATE)) FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_SESSIONS_TABLE);"

    var statement_1 = snowflake.createStatement( {sqlText: my_sql_command_1} );
    var result_set_1 = statement_1.execute();

    var my_sql_command_2 = "MERGE INTO CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_SESSIONS_TABLE T USING CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_SESSIONS_TABLE_TEMP S " +
                             "ON (T.SH_KEY = S.SH_KEY) " +
                             "WHEN NOT MATCHED THEN " +
                             "INSERT (SH_KEY,ACCOUNT_LOCATOR,REGION_NAME,ORGANIZATION_NAME,SESSION_ID,CREATED_ON,USER_NAME,CLIENT_APPLICATION_ID,EVENT_TYPE,IS_SUCCESS) " +
                             "VALUES (S.SH_KEY,S.ACCOUNT_LOCATOR,S.REGION_NAME,S.ORGANIZATION_NAME,S.SESSION_ID,S.CREATED_ON,S.USER_NAME,S.CLIENT_APPLICATION_ID,S.EVENT_TYPE,S.IS_SUCCESS);"

    var statement_2 = snowflake.createStatement( {sqlText: my_sql_command_2} );
    var result_set_2 = statement_2.execute();

    var my_sql_command_3 =   "DELETE FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_SESSIONS_TABLE " +
                             "WHERE CREATED_ON <= (select dateadd(day,-var_value,current_date) from table(get_var(\'DAYS_TO_RETAIN\',\'VW_SNOWFLAKE_SESSIONS_TABLE\',CURRENT_ACCOUNT(),CURRENT_REGION()))) " +
                             "AND ACCOUNT_LOCATOR = CURRENT_ACCOUNT();"

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

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','TASK_VW_SNOWFLAKE_SESSIONS',CURRENT_ACCOUNT(),CURRENT_REGION())));

CREATE OR REPLACE task CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_SESSIONS
    WAREHOUSE = ${TASK_WAREHOUSE}
    SCHEDULE = $TASK_SCHEDULE
AS
    CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_SESSIONS();

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_SESSIONS RESUME;

CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_SESSIONS();

CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_SESSIONS AS
  SELECT
    S.*
        FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_SESSIONS_TABLE S
  ORDER BY CREATED_ON DESC;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_SESSIONS;
-- rollback DROP TABLE "CDOPS_STATESTORE"."REPORTING"."VW_SNOWFLAKE_SESSIONS_TABLE";
-- rollback DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_SESSIONS;
-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_SESSIONS();