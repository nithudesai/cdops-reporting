--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset BUDGET_EXTENDED_PROCEDURE:1 runOnChange:true stripComments:true
--labels: BUDGET

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CDOPS_METADATA_GATHERING SUSPEND;
ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CDOPS_METADATA_WAREHOUSE_GATHERING SUSPEND;
ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CDOPS_WAREHOUSE_USAGE_GATHERING SUSPEND;

USE ROLE SYSADMIN;

--comment: Extract database name metadata and populate calculated_database_metadata
CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.UPDATE_DATABASE_METADATA()
RETURNS string
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
  '
  var swapResult = "Swap Succeeded";
  try {
    snowflake.execute( { sqlText:
      `show databases;`
       } );

    snowflake.execute( { sqlText:
      `create or replace table temp_calculated_database_metadata copy grants as
          select "name", "owner", "comment", "db_type",
            case
                when "db_type" = \'ES\' then \'ES\'
                when "db_type" = \'POC\' then \'POC\'
                when "db_type" = \'LAB\' then \'LAB\'
                when "db_type" = \'DP\' then TO_VARCHAR(parts[0])
                when "name" = \'SNOWFLAKE\' then \'META\'
                else NULL
            end as "env",
            case
                when "db_type" = \'ES\' then TO_VARCHAR(parts[1])
                when "db_type" = \'POC\' then NULL
                when "db_type" = \'LAB\' then TO_VARCHAR(parts[0])
                when "db_type" = \'DP\' then TO_VARCHAR(parts[1])
                when "name" = \'SNOWFLAKE\' then \'META\'
                else NULL
            end as "bu",
            case
                when "db_type" = \'ES\' then TO_VARCHAR(parts[2])
                when "db_type" = \'POC\' then replace(left("name", len("name")- 7), \'_\', \' \')
                when "db_type" = \'LAB\' then TO_VARCHAR(parts[1])
                when "db_type" = \'DP\' then IFF((array_size(parts))>3,TO_VARCHAR(parts[2]),TO_VARCHAR(parts[1]))
                when "name" = \'SNOWFLAKE\' then \'META\'
                else NULL
            end as "db_name"
          from (
            select "name", "owner", "comment", parts,
                case
                    when parts[0] = \'ES\' then \'ES\'
                    when endswith("name", \'_POC_DB\') then \'POC\'
                    when endswith("name", \'_LAB_DB\') then \'LAB\'
                    when startswith("name", \'DEV_\') then \'DP\'
                    when startswith("name", \'TEST_\') then \'DP\'
                    when startswith("name", \'STAGE_\') then \'DP\'
                    when startswith("name", \'PROD_\') then \'DP\'
                    else \'UNKNOWN\'
                end as "db_type"
            from (
              SELECT "name", "owner", "comment", split("name", \'_\') as parts
                  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
              )
          );`
       } );
       try {
          snowflake.execute( { sqlText:
            `alter table calculated_database_metadata swap with temp_calculated_database_metadata;`
             } );
        } catch(e) {
          swapResult = \' swap failed, which may be ok:\'+e;
          snowflake.execute( { sqlText:
            `alter table if exists temp_calculated_database_metadata rename to calculated_database_metadata;`
             } );
        }

    snowflake.execute( { sqlText:
      `drop table if exists temp_calculated_database_metadata;`
       } );

    snowflake.execute( { sqlText:
      `grant ownership on calculated_database_metadata to role CDOPS_ADMIN COPY CURRENT GRANTS;`
       } );
  } catch(err) {
    var result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
    result += "\\n  Message: " + err.message;
    result += "\\nStack Trace:\\n" + err.stackTraceTxt;
    result += "\\n"+swapResult;
    return result;
  }
  return \'Success\';
  ';


CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.UPDATE_WAREHOUSE_METADATA()
RETURNS string
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
  '
  var swapResult = \'Swap Succeeded\';
  try {
    snowflake.execute( { sqlText:
      `show warehouses;`
       } );

    snowflake.execute( { sqlText:
      `create or replace  table temp_calculated_warehouse_metadata copy grants as
          select "name", "owner", "comment", "wh_type",
            case
                when "wh_type" = \'ES\' then \'ES\'
                when "wh_type" = \'POC\' then \'POC\'
                when "wh_type" = \'LAB\' then \'LAB\'
                when "wh_type" = \'DP\' or "wh_type" = \'DP_EXTRA\' then TO_VARCHAR(parts[0])
                when "name" = \'SNOWFLAKE\' then \'META\'
                else NULL
            end as "env",
            case
                when "wh_type" = \'ES\' then TO_VARCHAR(parts[1])
                when "wh_type" = \'POC\' then NULL
                when "wh_type" = \'LAB\' then TO_VARCHAR(parts[0])
                when "wh_type" = \'DP\' or "wh_type" = \'DP_EXTRA\' then TO_VARCHAR(parts[1])
                when "name" = \'SNOWFLAKE\' then \'META\'
                else NULL
            end as "bu",
            case
                when "wh_type" = \'ES\' then TO_VARCHAR(parts[2])
                when "wh_type" = \'POC\' then replace(left("name", len("name")- 7), \'_\', \' \')
                when "wh_type" = \'LAB\' then TO_VARCHAR(parts[1])
                when "wh_type" = \'DP\'  then IFF(array_size(parts)>5,TO_VARCHAR(parts[2]),IFF((array_size(parts))=5,TO_VARCHAR(parts[2]),TO_VARCHAR(parts[1])))
                when "name" = \'SNOWFLAKE\' then \'META\'
                else NULL
            end as "db_name",
            case
                when "wh_type" = \'ES\' then TO_VARCHAR(parts[2])
                when "wh_type" = \'POC\' then replace(left("name", len("name")- 7), \'_\', \' \')
                when "wh_type" = \'LAB\' then TO_VARCHAR(parts[1])
                when "wh_type" = \'DP\' then TO_VARCHAR(parts[array_size(parts)-2])
                when "wh_type" = \'UTIL\' then TO_VARCHAR(parts[array_size(parts)-2])
                when "name" = \'SNOWFLAKE\' then \'META\'
                else NULL
            end as "purpose"
          from (
            select "name", "owner", "comment", parts,
                case
                    when parts[0] = \'ES\' then \'ES\'
                    when endswith("name", \'_POC_WH\') then \'POC\'
                    when endswith("name", \'_LAB_WH\') then \'LAB\'
                    when array_size(parts) = 2 then \'UTIL\'
                    when startswith("name", \'DEV_\') then \'DP\'
                    when startswith("name", \'TEST_\') then \'DP\'
                    when startswith("name", \'STAGE_\') then \'DP\'
                    when startswith("name", \'PROD_\') then \'DP\'
                    else \'UNKNOWN\'
                end as "wh_type"
            from (
              SELECT "name", "owner", "comment", split("name", \'_\') as parts
                  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
              )
          );`
       } );
       try {
          snowflake.execute( { sqlText:
            `alter table calculated_warehouse_metadata swap with temp_calculated_warehouse_metadata;`
             } );
        } catch(e) {
          swapResult = \' swap failed, which may be ok:\'+e;
          snowflake.execute( { sqlText:
            `alter table if exists temp_calculated_warehouse_metadata rename to calculated_warehouse_metadata;`
             } );
        }

    snowflake.execute( { sqlText:
      `drop table if exists temp_calculated_warehouse_metadata;`
       } );

    snowflake.execute( { sqlText:
      `grant ownership on calculated_warehouse_metadata to role CDOPS_ADMIN COPY CURRENT GRANTS;`
       } );
  } catch(err) {
    var result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
    result += "\\n  Message: " + err.message;
    result += "\\nStack Trace:\\n" + err.stackTraceTxt;
    result += "\\n"+swapResult;
    return result;
  }
  return \'Success\';
  ';

CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.UPDATE_WAREHOUSE_CREDIT_USAGE()
RETURNS string
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
  '
  var swapResult = \'Swap Succeeded\';
  try {
    snowflake.execute( { sqlText:
      `merge into warehouse_credit_usage using
        (SELECT
            WAREHOUSE_NAME AS warehouse_name,
            TO_CHAR(TO_DATE(START_TIME), \'YYYY-MM-DD\') AS active_date,
            COALESCE(SUM(warehouse_metering_history.CREDITS_USED), 0) AS credits_used
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY AS warehouse_metering_history
            GROUP BY warehouse_name, active_date
        ) as refresh_credits
        on refresh_credits.warehouse_name = warehouse_credit_usage.warehouse_name
        and refresh_credits.active_date = warehouse_credit_usage.active_date
        when matched then update set credits_used = refresh_credits.credits_used
        when not matched then insert (warehouse_name, active_date, credits_used) values (refresh_credits.warehouse_name, refresh_credits.active_date, refresh_credits.credits_used);`
      }
    );
    snowflake.execute( { sqlText:
      `create or replace table temp_warehouse_credit_usage copy grants as
          SELECT
              warehouse_name,
              active_date,
              credits_used
          FROM warehouse_credit_usage
          order by warehouse_name, active_date;`
       } );
    try {
      snowflake.execute( { sqlText:
        `alter table warehouse_credit_usage swap with temp_warehouse_credit_usage;`
         } );
    } catch(e) {
      swapResult = \' swap failed, which may be ok:\'+e;
      snowflake.execute( { sqlText:
        `alter table if exists temp_warehouse_credit_usage rename to warehouse_credit_usage;`
         } );
    }

    snowflake.execute( { sqlText:
      `drop table if exists temp_warehouse_credit_usage;`
       } );

    snowflake.execute( { sqlText:
      `grant ownership on warehouse_credit_usage to role CDOPS_ADMIN COPY CURRENT GRANTS;`
       } );
  } catch(err) {
    var result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
    result += "\\n  Message: " + err.message;
    result += "\\nStack Trace:\\n" + err.stackTraceTxt;
    result += "\\n"+swapResult;
    return result;
  }
  return \'Success\';
  ';

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON 0 */6 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='CDOPS_METADATA_GATHERING',
--VAR_NAME='TASK_SCHEDULE';

--comment: Call One Time to initialize all tables before view creation
CALL CDOPS_STATESTORE.REPORTING.UPDATE_DATABASE_METADATA();
CALL CDOPS_STATESTORE.REPORTING.UPDATE_WAREHOUSE_METADATA();
CALL CDOPS_STATESTORE.REPORTING.UPDATE_WAREHOUSE_CREDIT_USAGE();

USE ROLE CDOPS_ADMIN;

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','CDOPS_METADATA_GATHERING',CURRENT_ACCOUNT(),CURRENT_REGION())));

CREATE OR REPLACE TASK CDOPS_STATESTORE.REPORTING.CDOPS_METADATA_GATHERING
  WAREHOUSE = ${TASK_WAREHOUSE}
  SCHEDULE = $TASK_SCHEDULE
  USER_TASK_TIMEOUT_MS = 60000
  COMMENT = 'Gather Metadata from existing Database objects'
AS
  CALL UPDATE_DATABASE_METADATA();

CREATE OR REPLACE TASK CDOPS_STATESTORE.REPORTING.CDOPS_METADATA_WAREHOUSE_GATHERING
  WAREHOUSE = ${TASK_WAREHOUSE}
  USER_TASK_TIMEOUT_MS = 60000
  COMMENT = 'Gather Metadata from existing Warehouse objects'
  AFTER CDOPS_METADATA_GATHERING
AS
  CALL UPDATE_WAREHOUSE_METADATA();

CREATE OR REPLACE TASK CDOPS_STATESTORE.REPORTING.CDOPS_WAREHOUSE_USAGE_GATHERING
  WAREHOUSE = ${TASK_WAREHOUSE}
  USER_TASK_TIMEOUT_MS = 60000
  COMMENT = 'Gather Warehouse Credit Usage'
  AFTER CDOPS_METADATA_WAREHOUSE_GATHERING
AS
  CALL UPDATE_WAREHOUSE_CREDIT_USAGE();

--comment: Resume all dependent task tied to root task
SELECT system$task_dependents_enable('CDOPS_STATESTORE.REPORTING.CDOPS_METADATA_GATHERING');

--rollback: DROP PROCEDURE IF EXISTS CDOPS_STATESTORE.REPORTING.UPDATE_DATABASE_METADATA();
--rollback: DROP PROCEDURE IF EXISTS CDOPS_STATESTORE.REPORTING.UPDATE_WAREHOUSE_METADATA();
--rollback: DROP PROCEDURE IF EXISTS CDOPS_STATESTORE.REPORTING.UPDATE_WAREHOUSE_CREDIT_USAGE();
--rollback: DROP TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CDOPS_WAREHOUSE_USAGE_GATHERING;
--rollback: DROP TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CDOPS_METADATA_WAREHOUSE_GATHERING;
--rollback: DROP TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CDOPS_METADATA_GATHERING;
--rollback: ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CDOPS_METADATA_GATHERING SUSPEND;

