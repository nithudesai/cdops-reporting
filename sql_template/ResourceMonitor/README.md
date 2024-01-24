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

`snowsql -a <account> -u <loginid> --private-key-path=<key_path> -f REPORTING_TABLE.sql -f PROCEDURE.sql -f TASK.sql -f VIEWS.sql
`

Please refer to [PROCEDURE.sql](https://bitbucket.org/phdata/cdops-reporting/src/master/sql_template/ResourceMonitor/PROCEDURE.sql) 
and [VIEW.sql](https://bitbucket.org/phdata/cdops-reporting/src/master/sql_template/ResourceMonitor/VIEWS.sql) 
file to know more about the views and Procedure used.

View Supported |
----------|
VW_SNOWFLAKE_RESOURCE_MONITOR_DATA_FL/

Procdure Supported |
----------|
RESOURCE_MONITOR_STATUS_CAPTURE_PROCEDURE/


