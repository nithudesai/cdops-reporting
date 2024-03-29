--liquibase formatted sql
--preconditions onFail:HALT onError:HALT

--changeset FIVETRAN_USER:1 runOnChange:true stripComments:true
--labels: "FIVETRAN_USER or GENERIC"

CREATE OR REPLACE VIEW   CDOPS_STATESTORE.REPORTING_EXT.VW_FIVETRAN_USER as
SELECT *
  FROM FIVETRAN_TERRAFORM_LAB_DB.FIVETRAN_LOG.USER;

-- rollback DROP VIEW IF EXISTS CDOPS_STATESTORE.REPORTING_EXT.VW_FIVETRAN_USER;