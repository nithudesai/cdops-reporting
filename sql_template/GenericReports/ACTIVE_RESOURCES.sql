--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset ACTIVE_RESOURCES:1 runOnChange:true stripComments:true
--labels: "ACTIVE_RESOURCES or GENERIC"

DROP TABLE IF EXISTS CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES;

CREATE TRANSIENT TABLE CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES
(
    RESOURCE_TYPE VARCHAR(20),
    RESOURCE_NAME VARCHAR(100),
    CREATED_ON TIMESTAMP
);

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON ,0 6 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='TASK_CDOPS_ACTIVE_RESOURCES',
--VAR_NAME='TASK_SCHEDULE';

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.SP_CDOPS_ACTIVE_RESOURCES()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    const sql_begin_trans = snowflake.createStatement({ sqlText:`BEGIN TRANSACTION;`});
    const sql_commit_trans = snowflake.createStatement({ sqlText:`COMMIT;`});
    try{

    const sql_command_truncate = snowflake.createStatement({ sqlText:`TRUNCATE TABLE IF EXISTS CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES;`});

    const sql_command_show_db = snowflake.createStatement({ sqlText:`SHOW DATABASES;`});
    const sql_command_insert_db = snowflake.createStatement({ sqlText:`INSERT INTO CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES
                                                                  SELECT \'DATABASE\',"name","created_on"
                                                                  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`
                                                              });

    const sql_command_show_wh = snowflake.createStatement({ sqlText:`SHOW WAREHOUSES;`});
    const sql_command_insert_wh = snowflake.createStatement({ sqlText:`INSERT INTO CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES
                                                                  SELECT \'WAREHOUSE\',"name","created_on"
                                                                  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`
                                                              });

    const sql_command_show_roles = snowflake.createStatement({ sqlText:`SHOW ROLES;`});
    const sql_command_insert_roles = snowflake.createStatement({ sqlText:`INSERT INTO CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES
                                                                  SELECT \'ROLE\',"name","created_on"
                                                                  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`
                                                              });

    const sql_command_show_users = snowflake.createStatement({ sqlText:`SHOW USERS;`});
    const sql_command_insert_users = snowflake.createStatement({ sqlText:`INSERT INTO CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES
                                                                  SELECT \'USER\',"name","created_on"
                                                                  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`
                                                               });

	const sql_command_show_rm = snowflake.createStatement({ sqlText:`SHOW RESOURCE MONITORS;`});
    const sql_command_insert_rm = snowflake.createStatement({ sqlText:`INSERT INTO CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES
                                                                  SELECT \'RM\',"name","created_on"
                                                                  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`
                                                               });

        snowflake.execute({sqlText: ` USE ROLE CDOPS_ADMIN;`});
        sql_begin_trans.execute();
        sql_command_truncate.execute();
        sql_command_show_db.execute();
        sql_command_insert_db.execute();
        sql_command_show_wh.execute();
        sql_command_insert_wh.execute();
		sql_command_show_rm.execute();
        sql_command_insert_rm.execute();
        snowflake.execute({sqlText: ` USE ROLE CDOPS_ADMIN;`});
		/* snowflake.execute({sqlText: ` USE ROLE SYSADMIN;`}); */
        snowflake.execute({sqlText: ` USE DATABASE CDOPS_STATESTORE;`}); 
		sql_command_show_roles.execute();
        sql_command_insert_roles.execute();
        /* snowflake.execute({sqlText: ` USE ROLE SECURITYADMIN;`}); */
        sql_command_show_users.execute();
        /* snowflake.execute({sqlText: ` USE ROLE CDOPS_ADMIN;`}); */
        /* snowflake.execute({sqlText: ` USE DATABASE CDOPS_STATESTORE;`});  */               
        sql_command_insert_users.execute();
        snowflake.execute({sqlText: ` USE ROLE CDOPS_ADMIN;`});

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

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','TASK_CDOPS_ACTIVE_RESOURCES',CURRENT_ACCOUNT(),CURRENT_REGION())));
SET TASK_WAREHOUSE = 'CDOPS_REPORT_SYSTEM_WH';
CREATE OR REPLACE task CDOPS_STATESTORE.REPORTING.TASK_CDOPS_ACTIVE_RESOURCES
    WAREHOUSE = ${TASK_WAREHOUSE}
    SCHEDULE = $TASK_SCHEDULE
AS
    CALL CDOPS_STATESTORE.REPORTING.SP_CDOPS_ACTIVE_RESOURCES();

ALTER TASK CDOPS_STATESTORE.REPORTING.TASK_CDOPS_ACTIVE_RESOURCES RESUME;
CALL CDOPS_STATESTORE.REPORTING.SP_CDOPS_ACTIVE_RESOURCES();

CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_CDOPS_ACTIVE_DATABASES AS
  SELECT
    S.*
        FROM CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES S
        WHERE RESOURCE_TYPE = 'DATABASE'
  ORDER BY CREATED_ON DESC;

  CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_CDOPS_ACTIVE_WAREHOUSES AS
  SELECT
    S.*
        FROM CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES S
        WHERE RESOURCE_TYPE = 'WAREHOUSE'
  ORDER BY CREATED_ON DESC;

    CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_CDOPS_ACTIVE_ROLES AS
  SELECT
    S.*
        FROM CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES S
        WHERE RESOURCE_TYPE = 'ROLE'
  ORDER BY CREATED_ON DESC;

  CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_CDOPS_ACTIVE_USERS AS
  SELECT
    S.*
        FROM CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES S
        WHERE RESOURCE_TYPE = 'USER'
  ORDER BY CREATED_ON DESC;

  CREATE OR REPLACE VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_CDOPS_ACTIVE_RM AS
  SELECT
    S.*
        FROM CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES S
        WHERE RESOURCE_TYPE = 'RM'
  ORDER BY CREATED_ON DESC;



-- rollback DROP TABLE IF EXISTS CDOPS_STATESTORE.REPORTING.CDOPS_ACTIVE_RESOURCES;
-- rollback DROP TASK IF EXISTS  CDOPS_STATESTORE.REPORTING.TASK_CDOPS_ACTIVE_RESOURCES;
-- rollback DROP PROCEDURE IF EXISTS  CDOPS_STATESTORE.REPORTING.SP_CDOPS_ACTIVE_RESOURCES();
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_CDOPS_ACTIVE_RM;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_CDOPS_ACTIVE_USERS;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_CDOPS_ACTIVE_ROLES;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_CDOPS_ACTIVE_WAREHOUSES;
-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_TABLE_VW_SNOWFLAKE_CDOPS_ACTIVE_DATABASES;