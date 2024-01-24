--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset RESOURCE_MONITOR_DATA:1 runOnChange:true stripComments:true
--labels: RESOURCE_MONITOR

DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING.VW_SNOWFLAKE_RESOURCE_MONITOR_DATA_FL;

CREATE OR REPLACE  VIEW CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_VW_SNOWFLAKE_RESOURCE_MONITOR_DATA_FL AS
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
        TABLE(CDOPS_STATESTORE.REPORTING.RESOLVE_MEMBER_RESOURCE_MAPPING_UDF()) AS C,
        CDOPS_STATESTORE.REPORTING.RESOURCE_MONITOR RM,
        CDOPS_STATESTORE.REPORTING.WAREHOUSE WH
    WHERE
        RM.NAME = WH.RESOURCE_MONITOR AND
        RLIKE(WH.NAME,C.WAREHOUSE_PATTERN) ;

-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.EXTENDED_VW_SNOWFLAKE_RESOURCE_MONITOR_DATA_FL;
