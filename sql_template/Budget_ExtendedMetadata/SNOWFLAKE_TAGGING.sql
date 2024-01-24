--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset SNOWFLAKE_TAGGING_PROCEDURE:1 runOnChange:true stripComments:true
--labels: BUDGET

--Override CDOPS Variables
--UPDATE CDOPS_STATESTORE.REPORTING.CDOPS_VARIABLES SET VAR_VALUE='USING CRON 0 */6 * * * UTC'
--WHERE
--ACCOUNT_LOCATOR=CURRENT_ACCOUNT() AND
--REGION_NAME=CURRENT_REGION() AND
--VAR_USAGE='CDOPS_METADATA_GATHERING',
--VAR_NAME='TASK_SCHEDULE';

--USE ROLE SYSADMIN;

USE ROLE CDOPS_ADMIN;    -- anitha added
USE SCHEMA CDOPS_STATESTORE.REPORTING;  -- anitha added

CREATE TABLE IF NOT EXISTS CDOPS_STATESTORE.REPORTING.OVERRIDE_SFT_DATABASE_METADATA (
    OBJECT_NAME         VARCHAR,
    BUSINESS_UNIT       VARCHAR,
    COST_CENTRE         VARCHAR,
    BUSINESS_DIVISION   VARCHAR,
    PRODUCT_TYPE        VARCHAR,
    PRODUCT_NAME        VARCHAR,
    PURPOSE             VARCHAR,
    ENVIRONMENT         VARCHAR
);

CREATE TABLE IF NOT EXISTS CDOPS_STATESTORE.REPORTING.OVERRIDE_SFT_WAREHOUSE_METADATA (
    OBJECT_NAME         VARCHAR,
    BUSINESS_UNIT       VARCHAR,
    COST_CENTRE         VARCHAR,
    BUSINESS_DIVISION   VARCHAR,
    PRODUCT_TYPE        VARCHAR,
    PRODUCT_NAME        VARCHAR,
    PURPOSE             VARCHAR,
    ENVIRONMENT         VARCHAR
);

ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CR_TASK_SFT_CDOPS_METADATA_GATHERING SUSPEND;

--comment: Extract database name metadata and populate calculated_database_metadata
CREATE OR REPLACE PROCEDURE CDOPS_STATESTORE.REPORTING.CR_SP_SFT_METADATA()
RETURNS string
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
    '
        snowflake.execute( { sqlText:
              `show tags in account;`
        } );

        snowflake.execute( { sqlText:
              `create or replace table CDOPS_STATESTORE.REPORTING.temp_calculated_sft_metadata copy grants as
                select OBJECT_NAME, DOMAIN AS OBJECT_TYPE,
                    iff(ARRAY_CONTAINS(\'BUSINESS_UNIT\'::VARIANT,TAG_NAME_ARR),GET(TAG_VALUE_ARR,ARRAY_POSITION(\'BUSINESS_UNIT\'::VARIANT,TAG_NAME_ARR)),NULL)::string AS BUSINESS_UNIT,
                    iff(ARRAY_CONTAINS(\'COST_CENTRE\'::VARIANT,TAG_NAME_ARR),GET(TAG_VALUE_ARR,ARRAY_POSITION(\'COST_CENTRE\'::VARIANT,TAG_NAME_ARR)),NULL)::string AS COST_CENTRE,
                    iff(ARRAY_CONTAINS(\'BUSINESS_DIVISION\'::VARIANT,TAG_NAME_ARR),GET(TAG_VALUE_ARR,ARRAY_POSITION(\'BUSINESS_DIVISION\'::VARIANT,TAG_NAME_ARR)),NULL)::string AS BUSINESS_DIVISION,
                    iff(ARRAY_CONTAINS(\'PRODUCT_TYPE\'::VARIANT,TAG_NAME_ARR),GET(TAG_VALUE_ARR,ARRAY_POSITION(\'PRODUCT_TYPE\'::VARIANT,TAG_NAME_ARR)),NULL)::string AS PRODUCT_TYPE,
                    iff(ARRAY_CONTAINS(\'PRODUCT_NAME\'::VARIANT,TAG_NAME_ARR),GET(TAG_VALUE_ARR,ARRAY_POSITION(\'PRODUCT_NAME\'::VARIANT,TAG_NAME_ARR)),NULL)::string AS PRODUCT_NAME,
                    iff(ARRAY_CONTAINS(\'PURPOSE\'::VARIANT,TAG_NAME_ARR),GET(TAG_VALUE_ARR,ARRAY_POSITION(\'PURPOSE\'::VARIANT,TAG_NAME_ARR)),NULL)::string AS PURPOSE,
                    iff(ARRAY_CONTAINS(\'ENVIRONMENT\'::VARIANT,TAG_NAME_ARR),GET(TAG_VALUE_ARR,ARRAY_POSITION(\'ENVIRONMENT\'::VARIANT,TAG_NAME_ARR)),NULL)::string AS ENVIRONMENT,
                    TRUE IS_RESOLVED
                from (
                  select DISTINCT st.OBJECT_NAME, st.DOMAIN,array_agg(st.tag_value) within group (order by st.tag_name desc) over ( partition by st.OBJECT_NAME, st.DOMAIN) TAG_VALUE_ARR,
                  array_agg(st.tag_name) within group (order by st.tag_name desc) over ( partition by st.OBJECT_NAME, st.DOMAIN) TAG_NAME_ARR
                  from
                      snowflake.account_usage.tag_references st,(select "name" tag_name,"database_name" database_name, "schema_name" schema_name from table( result_scan( last_query_id() ) ) ) t
                  where
                      object_deleted is null and domain in (\'DATABASE\',\'WAREHOUSE\') and t.database_name = st.TAG_DATABASE and t.schema_name = TAG_SCHEMA and t.tag_name = st.TAG_NAME
                );`
        });

        snowflake.execute( { sqlText:
             `show databases;`
        } );

        snowflake.execute( { sqlText:
              `INSERT INTO CDOPS_STATESTORE.REPORTING.temp_calculated_sft_metadata
                 SELECT OBJECT_NAME,\'DATABASE\',NULL,NULL,NULL,NULL,NULL,NULL,NULL,FALSE from (
                   select "name" as OBJECT_NAME from table(result_scan(last_query_id())) where "origin"=\'\'
                   minus
                   select OBJECT_NAME from CDOPS_STATESTORE.REPORTING.temp_calculated_sft_metadata where OBJECT_TYPE=\'DATABASE\'
                 )
               ;`
        });

        snowflake.execute( { sqlText:
             `show warehouses;`
        } );

        snowflake.execute( { sqlText:
              `INSERT INTO CDOPS_STATESTORE.REPORTING.temp_calculated_sft_metadata
                 SELECT OBJECT_NAME,\'WAREHOUSE\',NULL,NULL,NULL,NULL,NULL,NULL,NULL,FALSE from (
                   select "name" as OBJECT_NAME from table(result_scan(last_query_id()))
                   minus
                   select OBJECT_NAME from CDOPS_STATESTORE.REPORTING.temp_calculated_sft_metadata where OBJECT_TYPE=\'WAREHOUSE\'
                 )
               ;`
        });

        var swapResult = \'Swap Succeeded\';
        try {

           try {
              snowflake.execute( { sqlText:
                `alter table CDOPS_STATESTORE.REPORTING.calculated_sft_metadata swap with CDOPS_STATESTORE.REPORTING.temp_calculated_sft_metadata;`
                 } );
            } catch(e) {
              swapResult = "Swap failed, which may be ok:";
              snowflake.execute( { sqlText:
                `alter table if exists CDOPS_STATESTORE.REPORTING.temp_calculated_sft_metadata rename to CDOPS_STATESTORE.REPORTING.calculated_sft_metadata;`
              } );
            }

            snowflake.execute( { sqlText:
              `drop table if exists CDOPS_STATESTORE.REPORTING.temp_calculated_sft_metadata;`
               } );

            snowflake.execute( { sqlText:
              `grant ownership on CDOPS_STATESTORE.REPORTING.calculated_sft_metadata to role CDOPS_ADMIN COPY CURRENT GRANTS;`
               } );
        } catch(err) {
            var result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
            result += "\\n  Message: " + err.message;
            result += "\\nStack Trace:\\n" + err.stackTraceTxt;
            result += "\\n"+swapResult;
            throw result;
        }
        return "Success";
    ';

--comment: Call One Time to initialize all tables before view creation
CALL CDOPS_STATESTORE.REPORTING.CR_SP_SFT_METADATA();

USE ROLE CDOPS_ADMIN;

SET TASK_SCHEDULE = (SELECT VAR_VALUE FROM table(get_var('TASK_SCHEDULE','TASK_SFT_CDOPS_METADATA_GATHERING',CURRENT_ACCOUNT(),CURRENT_REGION())));

CREATE OR REPLACE TASK CDOPS_STATESTORE.REPORTING.CR_TASK_SFT_CDOPS_METADATA_GATHERING
  WAREHOUSE = ${TASK_WAREHOUSE}
  SCHEDULE = $TASK_SCHEDULE
  USER_TASK_TIMEOUT_MS = 60000
  COMMENT = 'Gather Snowflake Tag Metadata from existing objects'
AS
  CALL CR_SP_SFT_METADATA();

--comment: Resume all dependent task tied to root task
SELECT system$task_dependents_enable('CDOPS_STATESTORE.REPORTING.CR_TASK_SFT_CDOPS_METADATA_GATHERING');

CREATE OR REPLACE SECURE VIEW CDOPS_STATESTORE.REPORTING.VW_SFT_DATABASE_METADATA
AS
  (SELECT C_OBJECT_NAME AS DATABASE_NAME,
          CASE
            WHEN O_BUSINESS_UNIT IS NOT NULL THEN O_BUSINESS_UNIT
            ELSE C_BUSINESS_UNIT
          END AS BUSINESS_UNIT,
          CASE
            WHEN O_COST_CENTRE IS NOT NULL THEN O_COST_CENTRE
            ELSE C_COST_CENTRE
          END AS COST_CENTRE,
          CASE
            WHEN O_BUSINESS_DIVISION IS NOT NULL THEN O_BUSINESS_DIVISION
            ELSE C_BUSINESS_DIVISION
          END AS BUSINESS_DIVISION,
          CASE
            WHEN O_PRODUCT_TYPE IS NOT NULL THEN O_PRODUCT_TYPE
            ELSE C_PRODUCT_TYPE
          END AS PRODUCT_TYPE,
          CASE
            WHEN O_PRODUCT_NAME IS NOT NULL THEN O_PRODUCT_NAME
            ELSE C_PRODUCT_NAME
          END AS PRODUCT_NAME,
          CASE
            WHEN O_PURPOSE IS NOT NULL THEN O_PURPOSE
            ELSE C_PURPOSE
          END AS PURPOSE,
          CASE
            WHEN O_ENVIRONMENT IS NOT NULL THEN O_ENVIRONMENT
            ELSE C_ENVIRONMENT
          END AS ENVIRONMENT
   FROM   (SELECT OBJECT_NAME       AS C_OBJECT_NAME,
                  BUSINESS_UNIT     AS C_BUSINESS_UNIT,
                  COST_CENTRE       AS C_COST_CENTRE,
                  BUSINESS_DIVISION AS C_BUSINESS_DIVISION,
                  PRODUCT_TYPE      AS C_PRODUCT_TYPE,
                  PRODUCT_NAME      AS C_PRODUCT_NAME,
                  PURPOSE           AS C_PURPOSE,
                  ENVIRONMENT       AS C_ENVIRONMENT
           FROM   CDOPS_STATESTORE.REPORTING.calculated_sft_metadata where OBJECT_TYPE='DATABASE') calculated
          left join (SELECT OBJECT_NAME  AS O_OBJECT_NAME,
                          BUSINESS_UNIT     AS O_BUSINESS_UNIT,
                          COST_CENTRE       AS O_COST_CENTRE,
                          BUSINESS_DIVISION AS O_BUSINESS_DIVISION,
                          PRODUCT_TYPE      AS O_PRODUCT_TYPE,
                          PRODUCT_NAME      AS O_PRODUCT_NAME,
                          PURPOSE           AS O_PURPOSE,
                          ENVIRONMENT       AS O_ENVIRONMENT
                     FROM CDOPS_STATESTORE.REPORTING.OVERRIDE_SFT_DATABASE_METADATA) override
                 ON C_OBJECT_NAME = O_OBJECT_NAME
  );

CREATE OR REPLACE  VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_VW_SFT_DATABASE_METADATA
AS
  SELECT DISTINCT T.* FROM TABLE(CDOPS_STATESTORE.REPORTING.RESOLVE_MEMBER_RESOURCE_MAPPING_UDF()), CDOPS_STATESTORE.REPORTING.VW_SFT_DATABASE_METADATA T
  WHERE RLIKE(T.DATABASE_NAME,DATABASE_PATTERN) ;

CREATE OR REPLACE SECURE VIEW CDOPS_STATESTORE.REPORTING.VW_SFT_WAREHOUSE_METADATA
AS
  (SELECT C_OBJECT_NAME AS WAREHOUSE_NAME,
          CASE
            WHEN O_BUSINESS_UNIT IS NOT NULL THEN O_BUSINESS_UNIT
            ELSE C_BUSINESS_UNIT
          END AS BUSINESS_UNIT,
          CASE
            WHEN O_COST_CENTRE IS NOT NULL THEN O_COST_CENTRE
            ELSE C_COST_CENTRE
          END AS COST_CENTRE,
          CASE
            WHEN O_BUSINESS_DIVISION IS NOT NULL THEN O_BUSINESS_DIVISION
            ELSE C_BUSINESS_DIVISION
          END AS BUSINESS_DIVISION,
          CASE
            WHEN O_PRODUCT_TYPE IS NOT NULL THEN O_PRODUCT_TYPE
            ELSE C_PRODUCT_TYPE
          END AS PRODUCT_TYPE,
          CASE
            WHEN O_PRODUCT_NAME IS NOT NULL THEN O_PRODUCT_NAME
            ELSE C_PRODUCT_NAME
          END AS PRODUCT_NAME,
          CASE
            WHEN O_PURPOSE IS NOT NULL THEN O_PURPOSE
            ELSE C_PURPOSE
          END AS PURPOSE,
          CASE
            WHEN O_ENVIRONMENT IS NOT NULL THEN O_ENVIRONMENT
            ELSE C_ENVIRONMENT
          END AS ENVIRONMENT
   FROM   (SELECT OBJECT_NAME       AS C_OBJECT_NAME,
                  BUSINESS_UNIT     AS C_BUSINESS_UNIT,
                  COST_CENTRE       AS C_COST_CENTRE,
                  BUSINESS_DIVISION AS C_BUSINESS_DIVISION,
                  PRODUCT_TYPE      AS C_PRODUCT_TYPE,
                  PRODUCT_NAME      AS C_PRODUCT_NAME,
                  PURPOSE           AS C_PURPOSE,
                  ENVIRONMENT       AS C_ENVIRONMENT
           FROM   CDOPS_STATESTORE.REPORTING.calculated_sft_metadata where OBJECT_TYPE='WAREHOUSE') calculated
          left join (SELECT OBJECT_NAME  AS O_OBJECT_NAME,
                          BUSINESS_UNIT     AS O_BUSINESS_UNIT,
                          COST_CENTRE       AS O_COST_CENTRE,
                          BUSINESS_DIVISION AS O_BUSINESS_DIVISION,
                          PRODUCT_TYPE      AS O_PRODUCT_TYPE,
                          PRODUCT_NAME      AS O_PRODUCT_NAME,
                          PURPOSE           AS O_PURPOSE,
                          ENVIRONMENT       AS O_ENVIRONMENT
                     FROM CDOPS_STATESTORE.REPORTING.OVERRIDE_SFT_DATABASE_METADATA) override
                 ON C_OBJECT_NAME = O_OBJECT_NAME
  );

CREATE OR REPLACE  VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_VW_SFT_WAREHOUSE_METADATA
AS
  SELECT DISTINCT T.* FROM TABLE(CDOPS_STATESTORE.REPORTING.RESOLVE_MEMBER_RESOURCE_MAPPING_UDF()), CDOPS_STATESTORE.REPORTING.VW_SFT_WAREHOUSE_METADATA T
  WHERE RLIKE(T.WAREHOUSE_NAME,WAREHOUSE_PATTERN) ;

--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_SFT_DATABASE_METADATA;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_VW_SFT_DATABASE_METADATA;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_SFT_WAREHOUSE_METADATA;
--rollback: DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_VW_SFT_WAREHOUSE_METADATA;
--rollback: DROP PROCEDURE IF EXISTS CDOPS_STATESTORE.REPORTING.CR_SP_SFT_METADATA();
--rollback: DROP TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CR_TASK_SFT_CDOPS_METADATA_GATHERING;
--rollback: ALTER TASK IF EXISTS CDOPS_STATESTORE.REPORTING.CR_TASK_SFT_CDOPS_METADATA_GATHERING SUSPEND;

