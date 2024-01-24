**Table of Contents**

[TOC]
  
---
# Development Enviornment Setup and Operations

## Setup enviornment for ManageControlDataAccess Script


Prerequisites: `python, pip, virtualenv`
```  
$ pip install virtualenv 
$ virtualenv penv 
$ source penv/bin/activate 
$ pip install -r requirements.txt 
```  

**Execute Command**

The code expects a file called `cda.csv` which should be present in same location where script is present.  Example uses SSO mechnaism to authenticate with Snowflake whereas Basic and Private Key is also supported.

```
python ManageControlDataAccess.py -a snowdata -c SSO -u balbir@phdata.io
python ManageControlDataAccess.py -r -a snowdata -c SSO -u balbir@phdata.io
```

Complete Code options can be viewed using `--help` option

```
(env) balbir@Balbirs-MacBook-Pro cdops-reporting % python ManageControlDataAccess.py --help
Usage: ManageControlDataAccess.py [OPTIONS]

Options:
  -r, --read                      Read mode  [default: False]
  -rl, --role TEXT                Snowflake role to be assigned to perform
                                  this task  [default: CDOPS_ADMIN;required]
  -w, --warehouse TEXT            Snowflake Warehouse  [required]
  -f, --cdafile TEXT              Control Data Access File Location
                                  [required]
  -t, --table TEXT                Fully Qualified table name
                                  <DATABASE.SCHEMA.TABLE>  [default: CDOPS_STA
                                  TESTORE.REPORTING.MEMBER_RESOURCE_MAPPING]
  -a, --account TEXT              Snowflake account name  [required]
  -pk, --privateKey TEXT          Private Key absolute path if connTpe
                                  selected is KEY. If password proctected set
                                  enviornment variable PRIVATE_KEY_PASSPHRASE
  -c, --connType [BASIC|SSO|KEY]  Type of connection. BASIC=USERID/PASSWORD
                                  SSO=WEB_SSO_LOGIN KEY=PRIVATE KEY
                                  [required]
  -u, --userID TEXT               UserID if connType selected is
                                  BASIC/SSO/KEY. Password should be set via
                                  enviornment variable SNOWSQL_PWD if connType
                                  is BASIC.
  --help                          Show this message and exit.
```

## Steps involved to add Role Base Data Access Control record
Role Base Data Access Control records are managed in a `cda.csv` file. You can have more then one `cda` file referring to different env. Infact if you want you can name the file according to your project since this is applied using `ManageControlDataAccess.py -f` option.

Since all changes are applied as part of CI/CD process the steps involved to add, update or delete access records are as follow.

1. Create a new branch from Master/Main branch
2. Make necessary changes to cda.csv file
3. Checkin and create a PR for the change.
4. Wait for Dry Run pipeline job to complete
5. Merge the changes and wait for pipeline job to apply the changes.

In case you plan to apply changes outside of CI/CD process please follow the steps mentioned **Setup enviornment for ManageControlDataAccess Script**.

## Liquibase Setup

Provides DevOps functionality over Database. Database code is treated as App Code, workflow involves Track -> Version -> Deploy. Liquibase Opensource version supports rollback tags, ToDate and number of changeset; Pro-Version supports traget changeset rollback.

[Installation](https://www.liquibase.org/download)
[How Liquibase Works](https://www.liquibase.org/get-started/how-liquibase-works)
[Getting Started](https://www.liquibase.org/get-started)

For CDOPS-Reporting we use open source version of liquibase. There are two configuration files which controls the liquibase connection to snowflake:

- [liquibase.properties](../resource/liquibase.properties)
- [snowflake.properties](../resource/snowflake.properties)

Snowflake and liquibase driver referenced in [liquibase.properties](../resource/liquibase.properties) file is avialable under [lib](../lib) directory

### Changelog Template Layout
We have a master change log which includes reference to sub change log. Each change log file manages it's own set of Table, UDF, Procedure and View SQL file. This helps us to rollback any changes for a particular service or at any point of time, we can nuke the entrie setup by doing rollback to master change log tag name.

```
masterchangelog.xml
└───sql_template
│   │   budgetextendedchangelog.xml
│   │   genericreportchangelog.xml
│   │   resourcemonitorchangelog.xml
```
Rollback command which can be executed from command shell:

```
./liquibase --defaults-file=./resource/liquibase.properties --changeLogFile=./masterchangelog.xml --log-level=info rollbackSQL rollback_all
```
 
