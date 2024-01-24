README
====
**Table of Contents**

[TOC]

---
## What is this repository for?

CDOps Monitoring project provides set of SQL files which provisions UDF, VIEWS and ACCESS CONTROL TABLE
to access snowflake metadata from `SNOWFLAKE -> ACCOUNT_USAGE -> VIEWS` or `SNOWFLAKE -> INFORMATION_SCHEMA
-> TABLE_FUNCTIONS`

More details about the project can be found here [High Level Document](https://docs.google.com/document/d/1E5n-Vn3gE0FQf7NmlI_RMryhHoFgrMzxtdrsX_ty0ak/edit)

## How do I get set up?

### Snowflake Setup
Execute TASK/PROCEDURE/VIEWS/REPORTING_TABLE SQL file to build PROCEDURE, TASK, VIEWS, REPORTING_TABLE.

Sequence of SQL file execution has to be maintained as shown below, executing a 
VIEW before UDF would fail.

`snowsql -w <WAREHOUSE_NAME> -a <account> -u <loginid> --private-key-path=<key_path> -f REPORTING_TABLE.sql -f PROCEDURE.sql -f TASK.sql -f VIEWS.sql
`

Please refer to [PROCEDURE.sql](https://bitbucket.org/phdata/cdops-reporting/src/master/sql_template/Budget/PROCEDURE.sql) 
and [VIEW.sql](https://bitbucket.org/phdata/cdops-reporting/src/master/sql_template/Budget/VIEWS.sql) 
file to know more about the views and Procedure used.

|Command | Description|
|----------|----------|
|OVERRIDE_DATABASE_METADATA | Provide relative metadata information about database which are missing in CALCULATED_DATABASE_METADATA|
|OVERRIDE_WAREHOUSE_METADATA | Provide relative metadata information about database which are missing in CALCULATED_WAREHOUSE_METADATA|
|RAW_WAREHOUSE_TAGS | Tag a Warehouse|
|RAW_DATABASE_TAGS | Tag a Database|
|WORKSPACE_PURCHASES | Store purchase information of credits and assigned to Workspace ID|
|WORKSPACE_TRACKER | Track workspace details which will be used along with WORKSPACE_PURCHASES|
|WORKSPACE_TYPES | Types of Workspace supported ES, DP, POC, LRN|
|WAREHOUSE_CREDIT_USAGE | Managed by procedure UPDATE_WAREHOUSE_CREDIT_USAGE|
|CALCULATED_DATABASE_METADATA | Managed by procedure UPDATE_DATABASE_METADATA, stores business metadata associated with Database|
|CALCULATED_WAREHOUSE_METADATA | Managed by procedure UPDATE_WAREHOUSE_METADATA, stores business metadata associated with Database|

| Views Created | 
|----------|
|VW_DATABASE_METADATA|
|VW_WAREHOUSE_METADATA|
|VW_WAREHOUSE_TAGS|
|VW_DATABASE_TAGS|
|VW_WORKSPACE_MONTHLY_BUDGET|
|VW_BU_WAREHOUSE_SPEND_MONTHLY|
|VW_WAREHOUSE_DETAILS|

|Procedures Created |
|----------|
|UPDATE_DATABASE_METADATA|
|UPDATE_WAREHOUSE_METADATA|
|UPDATE_WAREHOUSE_CREDIT_USAGE|


