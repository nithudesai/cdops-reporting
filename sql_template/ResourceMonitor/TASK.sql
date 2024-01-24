--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset CALL_RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE:1 runOnChange:true stripComments:true
--labels: RESOURCE_MONITOR
