--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset WAREHOUSE_CREDIT_DATA:1 runOnChange:true stripComments:true
--labels: "WAREHOUSE_CREDIT_DATA or GENERIC"

DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL;

CREATE TRANSIENT TABLE IF NOT EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL_TABLE (
	SH_KEY BINARY(20),
	ACCOUNT_LOCATOR VARCHAR(16777216),
	REGION_NAME VARCHAR(16777216),
    ORGANIZATION_NAME VARCHAR(16777216),
    START_TIME TIMESTAMP_LTZ(9),
	END_TIME TIMESTAMP_LTZ(9),
	WAREHOUSE_ID NUMBER(38,0),
	WAREHOUSE_NAME VARCHAR(16777216),
	SERVICE_TYPE VARCHAR(16777216),
	CREDITS_USED NUMBER(38,9),
	CREDITS_USED_COMPUTE NUMBER(38,9),
	CREDITS_USED_CLOUD_SERVICES NUMBER(38,9),
	START_DATE DATE,
	OPERATION_HOURS NUMBER(38,0),
	TIME_OF_DAY TIME(9)
);

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON 0 */3 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='TASK_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL',
--VAR_NAME='TASK_SCHEDULE';

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL SUSPEND;

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL()
  returns string not null
  language javascript
  as
  '
    const sql_begin_trans = snowflake.createStatement({ sqlText:`BEGIN TRANSACTION;`});
    const sql_commit_trans = snowflake.createStatement({ sqlText:`COMMIT;`});
    try{
      sql_begin_trans.execute();
      var my_sql_command_1  = "CREATE OR REPLACE TEMPORARY TABLE CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL_TABLE_TEMP AS " +
                                "SELECT " +
                                    "     sha1_binary( concat( CURRENT_REGION() " +
                                	"                             ,\'|\', T.ACCOUNT_LOCATOR " +
                                    "                             ,\'|\', ifnull( service_type, \'~\' )  " +
                                    "                             ,\'|\', to_char( start_time, \'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM\'  )  " +
                                    "                             ) " +
                                    "                      )   SH_KEY " +
                                    ",T.ACCOUNT_LOCATOR AS ACCOUNT_LOCATOR " +
                                    ",CURRENT_REGION() AS REGION_NAME " +
                                    ",T.VAR_VALUE AS ORGANIZATION_NAME " +
                                    ",START_TIME " +
                                    ",END_TIME " +
                                    ",ENTITY_ID AS WAREHOUSE_ID " +
                                    ",NAME AS WAREHOUSE_NAME " +
                                    ",SERVICE_TYPE " +
                                    ",CREDITS_USED " +
                                    ",CREDITS_USED_COMPUTE " +
                                    ",CREDITS_USED_CLOUD_SERVICES " +
                                    ",START_DATE " +
                                    ",OPERATION_HOURS " +
                                    ",TIME_OF_DAY " +
        "FROM TABLE(CDOPS_STATESTORE.REPORTING.UDF_METERING_HISTORY( \'.+\' , " +
                                                                           " array_construct(\'WAREHOUSE_METERING\',\'WAREHOUSE_METERING_READER\'), " +
                                                                           " TO_DATE(DATEADD(DAY,-365,CURRENT_TIMESTAMP)), " +
                                                                           " TO_DATE(CURRENT_TIMESTAMP) " +
                                                                         " )),  " +
             " TABLE(get_var(\'ORGANIZATION\',\'GLOBAL\',CURRENT_ACCOUNT(),CURRENT_REGION())) T " +
       "WHERE START_TIME >= (SELECT NVL(MAX(START_DATE),DATEADD(MONTH,-12,CURRENT_DATE)) FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL_TABLE);"

    var statement_1 = snowflake.createStatement( {sqlText: my_sql_command_1} );
    var result_set_1 = statement_1.execute();

    var my_sql_command_2 = "MERGE INTO CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL_TABLE T USING CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL_TABLE_TEMP S " +
                             "ON (T.SH_KEY = S.SH_KEY) " +
                             "WHEN NOT MATCHED THEN " +
                             "INSERT (SH_KEY,ACCOUNT_LOCATOR,REGION_NAME,ORGANIZATION_NAME,START_TIME,END_TIME,WAREHOUSE_ID,WAREHOUSE_NAME,SERVICE_TYPE,CREDITS_USED,CREDITS_USED_COMPUTE,CREDITS_USED_CLOUD_SERVICES,START_DATE,OPERATION_HOURS,TIME_OF_DAY) " +
                             "VALUES (S.SH_KEY,S.ACCOUNT_LOCATOR,S.REGION_NAME,S.ORGANIZATION_NAME,S.START_TIME,S.END_TIME,S.WAREHOUSE_ID,S.WAREHOUSE_NAME,S.SERVICE_TYPE,S.CREDITS_USED,S.CREDITS_USED_COMPUTE,S.CREDITS_USED_CLOUD_SERVICES,S.START_DATE,S.OPERATION_HOURS,S.TIME_OF_DAY);"

var statement_2 = snowflake.createStatement( {sqlText: my_sql_command_2} );
    var result_set_2 = statement_2.execute();

    var my_sql_command_3 =   "DELETE FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL_TABLE " +
                             "WHERE START_TIME <= (select dateadd(day,-var_value,current_date) from table(get_var(\'DAYS_TO_RETAIN\',\'VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL_TABLE\',CURRENT_ACCOUNT(),CURRENT_REGION()))) " +
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

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','TASK_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL',CURRENT_ACCOUNT(),CURRENT_REGION())));

CREATE OR REPLACE task CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL
    WAREHOUSE = ${TASK_WAREHOUSE}
    SCHEDULE =  $TASK_SCHEDULE
AS
    CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL();

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL RESUME;

CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL();

CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL AS
  SELECT DISTINCT
    WH.*, datediff(second, WH.START_TIME, WH.END_TIME) AS ELAPSED_TIME_IN_SEC,
    IFF(COMPUTE.COMPUTE_RATE IS NOT NULL, WH.CREDITS_USED*COMPUTE.COMPUTE_RATE, 0) CREDITS_USED_EFFECTIVE_RATE,
    IFF(COMPUTE.CLOUD_COMPUTE_RATE IS NOT NULL, WH.CREDITS_USED_CLOUD_SERVICES*COMPUTE.CLOUD_COMPUTE_RATE, 0) CREDITS_USED_CLOUD_SERVICES_EFFECTIVE_RATE,
    IFF(COMPUTE.COMPUTE_RATE IS NOT NULL, WH.CREDITS_USED_COMPUTE*COMPUTE.COMPUTE_RATE, 0) CREDITS_USED_COMPUTE_EFFECTIVE_RATE,
    COMPUTE.CURRENCY
  FROM
    TABLE(CDOPS_STATESTORE.REPORTING.RESOLVE_MEMBER_RESOURCE_MAPPING_UDF()) AS C,
      CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL_TABLE WH
      LEFT OUTER JOIN
          (
            SELECT
                COMPUTE.RATE_DATE,
                COMPUTE.CURRENCY,
                COMPUTE.EFFECTIVE_RATE COMPUTE_RATE,
                CLOUD.EFFECTIVE_RATE CLOUD_COMPUTE_RATE
            FROM
                TABLE(CDOPS_STATESTORE.REPORTING.RATE_SHEET_TIMESERIES('compute')) COMPUTE
                    JOIN
                TABLE(CDOPS_STATESTORE.REPORTING.RATE_SHEET_TIMESERIES('cloud services')) CLOUD
                    ON COMPUTE.RATE_DATE=CLOUD.RATE_DATE
          ) COMPUTE
      ON WH.START_TIME::DATE = COMPUTE.RATE_DATE
  WHERE
      C.WAREHOUSE_PATTERN IS NOT NULL AND RLIKE(WH.WAREHOUSE_NAME,C.WAREHOUSE_PATTERN)
  ORDER BY START_DATE DESC;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL;
-- rollback DROP TABLE "CDOPS_STATESTORE"."REPORTING"."VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_TABLE";
-- rollback DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL;
-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL();
