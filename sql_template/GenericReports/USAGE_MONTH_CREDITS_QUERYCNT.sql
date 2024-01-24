--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset USAGE_MONTH_CREDITS_QUERYCNT:1 runOnChange:true stripComments:true
--labels: "USAGE_MONTH_CREDITS_QUERYCNT or GENERIC"
ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_QUERY_HISTORY_FL SUSPEND;
ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_QUERY_CREDIT SUSPEND;
ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_WH_DB_SCHEMA_CREDIT_DATA_FL SUSPEND;
ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CALL_SP_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT SUSPEND;
ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.TASK_LOAD_USAGE_MONTH_CREDITS_QUERYCNT SUSPEND;

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.SP_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT (STARTING_FROM_MM double, DURATION double)
  RETURNS STRING
  LANGUAGE JAVASCRIPT
AS '
    //GETTING THE MONTH AND YEAR
    var mn_yr = [], x = "";
    for (var i = STARTING_FROM_MM; i < (STARTING_FROM_MM+DURATION); i++) {
        var monthname = "select iff( date_part(mm,add_months(current_date(),- " + i + "))<10, (\'0\'||date_part(mm,add_months(current_date(),- " + i + ")))::STRING,(date_part(mm,add_months(current_date(),- " + i + ")))::STRING)::STRING month";
        var yearnumber = "select year(DATE_TRUNC( Month,add_months(current_date(),- " + i + ")))";

        //STATEMENT EXECUTION TO RETRIEVE THE MONTH
        var monthname_stmt = snowflake.createStatement( {sqlText: monthname} );
        var resultSetMN = monthname_stmt.execute();
        resultSetMN.next();

        //STATEMENT EXECUTION TO RETRIEVE THE YEAR
        var yearname_stmt = snowflake.createStatement( {sqlText: yearnumber} );
        var resultSetYN = yearname_stmt.execute();
        resultSetYN.next();

        //STORING THE RETRIEVED MONTH AND YEAR IN MONTH_YEAR FORMAT IN ARRAY
        x = resultSetYN.getColumnValue(1)+""+resultSetMN.getColumnValue(1);
        mn_yr.push(x);
    }

    var CI = "";
    var QC = "";
    var CI_ARRAY = [];
    var QC_ARRAY = [];
    let m_y = mn_yr

    //CONSTRUCTING THE CREDITS_IN AND QUERY_COUNT COLUMN IN ARRAY
    for(let i = 0; i < mn_yr.length; i++) {
        CI = `ROUND(SUM(CASE WHEN USAGE_MONTH =DATE_TRUNC( Month ,add_months(current_date(),- ` + (STARTING_FROM_MM+i) + `)) THEN CREDIT_USED_BY_QUERY ELSE 0 END),9) AS "` + m_y[i]+`_CREDITS"`
        QC = `ROUND(SUM(CASE WHEN USAGE_MONTH =DATE_TRUNC( Month ,add_months(current_date(),- ` + (STARTING_FROM_MM+i) + `)) THEN 1 ELSE 0 END),0) AS "` + m_y[i]+`_QUERY"`
        CI_ARRAY.push(CI);
        QC_ARRAY.push(QC);
    }

    //USE THE ABOVE CREDITS_IN AND QUERY_COUNT IN THE SELECT STATEMENT
    var final_select_stmt = `SELECT
        NAME,
        USER_NAME,
        RESOURCE_MONITOR_NAME,`
        + CI_ARRAY + `,`
        + QC_ARRAY + `
        FROM
      CREDITS_PER_QUERY AS CPQ`;


    var v_SQLstmt = `WITH RA AS (SELECT
                        DATABASE AS DATABASE_PATTERN, WAREHOUSE AS WAREHOUSE_PATTERN, ACCOUNT, ROLE
                      FROM
                        CDOPS_STATESTORE.REPORTING.MEMBER_RESOURCE_MAPPING
                      WHERE
                        ACCOUNT = CURRENT_USER() OR ROLE = CURRENT_ROLE()
                      ),
                      RESOURCE_MONITOR AS (
                        SELECT DISTINCT
                          		  RM.NAME AS RESOURCE_MONITOR_NAME,
                                  WH.NAME AS WAREHOUSE_NAME,
                                  RM.CREDIT_QUOTA,
                                  RM.USED_CREDITS,
                                  RM.REMAINING_CREDITS,
                                  RM.FREQUENCY,
                                  RM.START_TIME AS RM_START_TIME,
                                  RM.END_TIME AS RM_END_TIME,
                                  RM.NOTIFY_AT AS RM_NOTIFY_AT,
                                  RM.SUSPEND_AT AS RM_SUSPEND_AT,
                                  RM.SUSPEND_IMMEDIATELY_AT AS RM_SUSPEND_SUSPEND_IMMEDIATELY_AT,
                                  RM.CREATED_ON AS RM_CREATED_ON,
                                  RM.OWNER AS RM_OWNER,
                                  RM.COMMENT AS RM_COMMENT,
                                  WH.TYPE AS WAREHOUSE_TYPE,
                                  WH.SIZE AS WAREHOUSE_SIZE,
                                  WH.MIN_CLUSTER_COUNT AS WAREHOUSE_MIN_CLUSTER_COUNT,
                                  WH.MAX_CLUSTER_COUNT AS WAREHOUSE_MAX_CLUSTER_COUNT,
                                  WH.CREATED_ON AS WAREHOUSE_CREATED_ON,
                                  WH.UPDATED_ON AS WAREHOUSE_UPDATED_ON,
                                  WH.COMMENT AS WAREHOUSE_COMMENT,
                                  WH.SCALING_POLICY AS WAREHOUSE_SCALING_POLICY
                            FROM
                                CDOPS_STATESTORE.REPORTING.RESOURCE_MONITOR RM,
                                CDOPS_STATESTORE.REPORTING.WAREHOUSE WH
                            WHERE
                                RM.NAME = WH.RESOURCE_MONITOR
                      ),
                      CTE AS (
                        SELECT
                          CASE
                            WHEN RA.ACCOUNT = CURRENT_USER() AND RA.ROLE = CURRENT_ROLE() THEN RA.WAREHOUSE_PATTERN
                            WHEN RA.ACCOUNT = CURRENT_USER() AND RA.ROLE IS NULL THEN RA.WAREHOUSE_PATTERN
                            WHEN RA.ACCOUNT IS NULL AND RA.ROLE = CURRENT_ROLE() THEN RA.WAREHOUSE_PATTERN
                            ELSE \'\'
                          END AS WAREHOUSE_PATTERN
                        FROM RA
                      ),
                      CREDITS_PER_QUERY AS
                        (
                          SELECT
                            WH.WAREHOUSE_NAME NAME,
                            VRM.RESOURCE_MONITOR_NAME AS RESOURCE_MONITOR_NAME,
                            DATE_TRUNC("Month", WH.START_TIME) AS USAGE_MONTH,
                            WH.START_TIME,
                            WH.END_TIME,
                            WH.CREDITS_USED,
                            QH.QUERY_ID,
                            QH.QUERY_TYPE,
                            QH.USER_NAME,
                            QH.ROLE_NAME,
                            QH.START_TIME AS QUERY_START_TIME,
                            QH.END_TIME AS QUERY_END_TIME,
                            QH.EXECUTION_TIME,
                            (QH.EXECUTION_TIME / SUM(IFF(QH.EXECUTION_TIME=0,1,QH.EXECUTION_TIME)) OVER (PARTITION BY WH.WAREHOUSE_NAME, WH.START_TIME, WH.END_TIME) ) * 100 AS PCT_CREDIT_USED_BY_QUERY,
                            ((PCT_CREDIT_USED_BY_QUERY / 100) * WH.CREDITS_USED ) AS CREDIT_USED_BY_QUERY
                          FROM
                            CTE, CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_WAREHOUSE_CREDIT_DATA_FL_TABLE WH
                            JOIN
                              CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_QUERY_HISTORY_FL_TABLE QH
                            ON WH.WAREHOUSE_ID = QH.WAREHOUSE_ID  AND QH.START_TIME BETWEEN WH.START_TIME AND WH.END_TIME
                            LEFT JOIN
                              RESOURCE_MONITOR AS VRM
                            ON WH.WAREHOUSE_NAME = VRM.WAREHOUSE_NAME
                          WHERE
                            WH.START_TIME >= DATE_TRUNC("Month", add_months(current_date(), - ` + DURATION + `))
                            AND (  CTE.WAREHOUSE_PATTERN IS NOT NULL AND RLIKE(QH.WAREHOUSE_NAME,CTE.WAREHOUSE_PATTERN) )

                        )
                        ` + final_select_stmt + `
                        GROUP BY
                          NAME, USER_NAME,RESOURCE_MONITOR_NAME
                        ORDER BY
                          1, 2
                          `;


    var VIEW_NAME = "CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT";
    var view_ddl = "CREATE OR REPLACE VIEW " + VIEW_NAME + " AS " + v_SQLstmt;

    // Now run the CREATE VIEW statement
    var view_stmt = snowflake.createStatement({sqlText:view_ddl});
    var view_res = view_stmt.execute();
    view_res.next();

    return "Success";

 ';

CREATE OR REPLACE TASK CDOPS_STATESTORE.REPORTING.CALL_SP_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT
    WAREHOUSE= ${TASK_WAREHOUSE}
    AFTER CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_QUERY_HISTORY_FL
AS
  CALL CDOPS_STATESTORE.REPORTING.SP_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT(0::double, 12::double);


CALL CDOPS_STATESTORE.REPORTING.SP_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT(0::double, 12::double);

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT()
  returns string not null
  language javascript
  as
  '
    var my_sql_command = ""
    var my_sql_command_1 = "CREATE OR REPLACE TRANSIENT TABLE CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT_TABLE_TMP AS SELECT * FROM CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT;"
    var statement_1 = snowflake.createStatement( {sqlText: my_sql_command_1} );
    var result_set_1 = statement_1.execute();

    var my_sql_command_a = "SELECT COLUMN_NAME FROM CDOPS_STATESTORE.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=\'VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT_TABLE_TMP\' and COLUMN_NAME ILIKE (\'%credit%\')  ;"
    var statement_a = snowflake.createStatement( {sqlText: my_sql_command_a} );
    var result_set_a = statement_a.execute();
    const column_names = [];
    while (result_set_a.next()) {
        column_names.push(\'"\' + result_set_a.getColumnValue(1) + \'"\');
    }
    snowflake.execute({
                sqlText: `CREATE OR REPLACE TRANSIENT TABLE CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT_TABLE AS SELECT NAME,USER_NAME,RESOURCE_MONITOR_NAME,ATTRIBUTE,VALUE FROM
                                                 CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT_TABLE_TMP
                                                     unpivot(Value for Attribute in (${column_names.join(\',\')})) order by NAME;`
            });

    var my_sql_command_4 = "DROP TABLE CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT_TABLE_TMP;"
    var statement_4 = snowflake.createStatement( {sqlText: my_sql_command_4} );
    var result_set_4 = statement_4.execute();
    var my_sql_command = my_sql_command_1 + my_sql_command_4;
  return my_sql_command; // Statement returned for info/debug purposes
  ';

CREATE OR REPLACE task CDOPS_STATESTORE.REPORTING.TASK_LOAD_USAGE_MONTH_CREDITS_QUERYCNT
    WAREHOUSE = ${TASK_WAREHOUSE}
    AFTER CDOPS_STATESTORE.REPORTING.CALL_SP_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT
AS
    CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT();


CALL CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT();

CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT AS
  SELECT DISTINCT
RC.*
FROM
 TABLE(CDOPS_STATESTORE.REPORTING.RESOLVE_MEMBER_RESOURCE_MAPPING_UDF()) CTE,CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT_TABLE RC
 WHERE
    CTE.WAREHOUSE_PATTERN IS NOT NULL AND RLIKE(RC.NAME,CTE.WAREHOUSE_PATTERN)
 ORDER BY ATTRIBUTE DESC;

--comment: Resume all dependent task tied to root task
SELECT system$task_dependents_enable('CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_QUERY_HISTORY_FL');

-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT;
-- rollback DROP TABLE "CDOPS_STATESTORE"."REPORTING"."VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT_TABLE";
-- rollback DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.TASK_VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT;
-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_VW_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT();
-- rollback DROP VIEW IF EXISTS  CDOPS_STATESTORE.REPORTING.VW_SNOWFALKE_USAGE_MONTH_CREDITS_QUERYCNT;
-- rollback DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.CALL_SP_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT;
-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_SNOWFLAKE_USAGE_MONTH_CREDITS_QUERYCNT (double, double);
