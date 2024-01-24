--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE:1 runOnChange:true stripComments:true
--labels: RESOURCE_MONITOR

USE ROLE SYSADMIN;

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CALL_RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE SUSPEND;

create or replace procedure CDOPS_STATESTORE.REPORTING.RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE()
RETURNS VARCHAR
LANGUAGE javascript
AS
'
    const sql_begin_trans = snowflake.createStatement({ sqlText:`BEGIN TRANSACTION;`});
    const sql_commit_trans = snowflake.createStatement({ sqlText:`COMMIT;`});
    try {
        const insert_query_list = [];
        const sql_command_show_rm = snowflake.createStatement({ sqlText:`SHOW RESOURCE MONITORS;`});
        const sql_command_insert_rm = snowflake.createStatement({ sqlText:`INSERT INTO CDOPS_STATESTORE.REPORTING.RESOURCE_MONITOR
                                                                  (
                                                                    name,credit_quota,used_credits,remaining_credits,frequency,
                                                                    level,start_time,end_time,notify_at,suspend_at,suspend_immediately_at,
                                                                    created_on,owner,comment
                                                                  )
                                                                  SELECT "name",
                                                                         "credit_quota",
                                                                         "used_credits",
                                                                         "remaining_credits",
                                                                         "frequency",
                                                                         "level",
                                                                         "start_time",
                                                                         "end_time",
                                                                         "notify_at",
                                                                         "suspend_at",
                                                                         "suspend_immediately_at",
                                                                         "created_on",
                                                                         "owner",
                                                                         "comment"
                                                                  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
                                                                  WHERE "level"=\'WAREHOUSE\';`
                                                      });
        const sql_command_show_wh = snowflake.createStatement({ sqlText:`SHOW WAREHOUSES;`});
        const sql_command_insert_wh = snowflake.createStatement({ sqlText:`INSERT INTO CDOPS_STATESTORE.REPORTING.WAREHOUSE
                                                                  (
                                                                    name,type,size,min_cluster_count,max_cluster_count,
                                                                    auto_suspend,auto_resume,created_on,updated_on,
                                                                    owner,comment,resource_monitor,scaling_policy
                                                                  )
                                                                  SELECT "name",
                                                                         "type",
                                                                         "size",
                                                                         "min_cluster_count",
                                                                         "max_cluster_count",
                                                                         "auto_suspend",
                                                                         "auto_resume",
                                                                         "created_on",
                                                                         "updated_on",
                                                                         "owner",
                                                                         "comment",
                                                                         "resource_monitor",
                                                                         "scaling_policy"
                                                                  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`
                                                              });
        const sql_command_truncate_wh = snowflake.createStatement({ sqlText:`TRUNCATE TABLE IF EXISTS CDOPS_STATESTORE.REPORTING.WAREHOUSE;`});
        const sql_command_truncate_rm = snowflake.createStatement({ sqlText:`TRUNCATE TABLE IF EXISTS CDOPS_STATESTORE.REPORTING.RESOURCE_MONITOR;`});

        sql_begin_trans.execute();
        sql_command_truncate_rm.execute();
        sql_command_show_rm.execute();
        sql_command_insert_rm.execute();

        sql_command_truncate_wh.execute();
        sql_command_show_wh.execute();
        sql_command_insert_wh.execute();

    } catch (err) {
        let result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
        result += "\\n  Message: " + err.message;
        result += "\\nStack Trace:\\n" + err.stackTraceTxt;
        return result;
    }finally{
        sql_commit_trans.execute();
    }
    return "Success";
';

CALL CDOPS_STATESTORE.REPORTING.RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE();

USE ROLE CDOPS_ADMIN;

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON 0 */3 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='CALL_RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE',
--VAR_NAME='TASK_SCHEDULE';

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','CALL_RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE',CURRENT_ACCOUNT(),CURRENT_REGION())));

CREATE OR REPLACE TASK CDOPS_STATESTORE.REPORTING.CALL_RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE
    WAREHOUSE= ${TASK_WAREHOUSE}
    SCHEDULE = $TASK_SCHEDULE
AS
  CALL CDOPS_STATESTORE.REPORTING.RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE();

ALTER TASK CDOPS_STATESTORE.REPORTING.CALL_RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE RESUME;



-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE();
-- rollback DROP TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CALL_RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE;