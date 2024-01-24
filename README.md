
README  
==== **Table of Contents**

[TOC]  
--- # What is this repository for?

CDOps Monitoring project provides set of SQL files which provisions UDF, VIEWS and ACCESS CONTROL TABLE    
to access snowflake metadata from `SNOWFLAKE -> ACCOUNT_USAGE -> VIEWS` or `SNOWFLAKE -> INFORMATION_SCHEMA -> TABLE_FUNCTIONS`

#
# Infrastructure Design

![CDOPS Reporting High Level Resource Definition](./doc/CDOPS_REPORTING_HIGH_LEVEL.jpeg)

[CDOPS Infrastructure Tram Template  
](https://bitbucket.org/phdata/snowflake-provisioning/src/master/provision-files/groups/cdops-resources.yml)

**Steps involved:**

1. Provision CDOPS Reporting account and infrastructure using TRAM.
2. Utilize [CDOps_Monitoring](https://docs.google.com/document/d/1aUGtr-Mi_21cubhjHxm4-_b1lRg-w_M6Atjm2FwNoqE/edit) infrastructure provisioned by TRAM to provide control data access and access to SHARED SNOWFLAKE database.

   **Core Snowflake Resources:**

   -  **CDOPS Core Roles**: `CDOPS_ADMIN -> CDOPS_MONITOR -> USAGE_MONITOR -> ( IMPORTED PRIVILEGES ) SHARED SNOWFLAKE DATABASE`
   - **CDOPS Reporting Roles**: `CDOPS_REPORT_SERVICE -> CDOPS_REPORT_USER`
   - **Database**: `CDOPS_STATESTORE`
   - **SCHEMA**: `REPORTING, REPORTING_EXT`
   - **WAREHOUSE**: `CDOPS_REPORT_WH, CDOPS_REPORT_SYSTEM_WH`

     As depicted in the diagram above we have two schemas provisioned as part of Infrastructure Architecture,`REPORTING` and `REPORTING_EXT` schema.

     `REPORTING` Schema is a core repository for staging custom secure Views querying the `ACCOUNT_USAGE` Views as well maintenance Tasks, Procedures, Tables.

     We have a dedicated warehouse `CDOPS_REPORT_SYSTEM_WH` of size SMALL for every scheduled TASK operation so that user lookup is not impacted with ongoing task operation. All metadata is pre-computed as part of Task Job and stored in resource specific staging tables.

     `REPORTING_EXT` Schema hosts secure views which are client facing and all views have role based data access control logic built into it. These Views refer to staging tables hosted in `REPORTING` schema.

3. `GRANT` Privileges at different user level which looks like following:    
   We generally manage three types of users in CDOPS Reporting environment

   -   **CDOPS Reporting Account**: This is an service account which has grants over SYSADMIN and SECURITYADMIN roles. Account which is used as part of project setup.

   -   **Service User Admin**: These users are assigned with elevated access to core Views, Procedures, Functions, Tasks and Tables in REPORTING schema along with client facing Views in REPORTING_EXT schema.

   -   **User:** This is an application user who’s provided only Select access to client facing Views in REPORTING_EXT schema.

![CDOPS Reporting High Level Resource Definition](./doc/CDOPS_REPORTING_USERS.jpeg)


4. Create a table `MEMBER_RESOURCE_MAPPING` which defines controls data access rules for `current_user()`,`current_available_role()`    
   **Column Structure:**    
   **Account**: Column holds the `current_user()` id for which data sharing access rule will be applied. Data access is controlled using Warehouse and Database columns.

   **Role**: Column holds the `ROLE_NAME` for which data sharing access rule will be applied. Data access is controlled using Warehouse and Database columns.

   **Note:**    
   Account and Role go hand-in-hand. Following conditions are checked in view before selecting a record:

   `IF <ACCOUNT=CURRENT_ACCOUNT() AND ROLE=NULL>    
   ELSE IF <ROLE=CURRENT_AVAILABLE_ROLE() AND ACCOUNT=NULL>` **Warehouse**: Column takes a POSIX extended regular expression.

   To lookup all Warehouse: `(.+)`    
   To lookup a specific list of warehouse: `(DEMO)|(DEFAULT_USER_.+)`    
   **Database**: Column takes a POSIX extended regular expression.

   To lookup all Database Objects: `(.+)`    
   To lookup a specific list of database: `(USER_BALBIR)`

## Security Layer
Access to Reporting is governed by two layers of Security model defined.

1. **Access to Reporting Views:** Using Tram we define which user can access these Views. For UseCase around using PowerBI Cloud to access Views we have to make sure the Default Role of a User has access to these Views and Warehouse dedicated to process these views.
   - [cdops-reporting-user-role-grant.yml](https://bitbucket.org/phdata/snowflake-provisioning/src/master/provision-files/groups/cdops-reporting-user-role-grant.yml)
   - [cdops-reporting-service-user-role-grant.yml](https://bitbucket.org/phdata/snowflake-provisioning/src/master/provision-files/groups/cdops-reporting-service-user-role-grant.yml)
3. **Maaping Table:** Table which controls which Role or User in active session has access to Warehouse and Databases.


## CDOPS Snowflake Account
CDOPS Reporting connects to snowflake using a dedicated snowflake account provisioned using Tram.
- [cdops-reporting-account.yml](https://bitbucket.org/phdata/snowflake-provisioning/src/master/provision-files/groups/cdops-reporting-account.yml)

### Generating Key Pair
Generate a Private and Public key using following command:

openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -v1 PBE-SHA1-RC4-128 -out rsa_key.p8 openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub    
Refrence:

- https://community.snowflake.com/s/article/Private-key-provided-is-invalid-or-not-supported-rsa-key-p8--data-isn-t-an-object-ID
- https://docs.snowflake.com/en/user-guide/key-pair-auth.html#step-1-generate-the-private-key

## CDOPS Snowflake Resource Provision
CDOPS Reporting resource is provisioned using Tram.
- [cdops-resources.yml](https://bitbucket.org/phdata/snowflake-provisioning/src/master/provision-files/groups/cdops-resources.yml)

### Roles associated with account
`SYSADMIN` is assigned as default role and extra role `SECURITYADMIN` is assigned to this account to query `USER` information using `SHOW` command.

### Warehouse associated with account
It's better to have two warehouse created as part of CDOPS Reporting project provisioning. One warehouse will be used by end users to query the reporting metadata and second warehouse will be used by schedule Task to build the reporting metadata before hand. Both warehouse are sized to be small.

## CI/CD Workflow

![CDOPS Reporting CI?CD Workflow](./doc/CDOps_Reporting-CI_CD Workflow.jpeg)

### BitBucket CI/CD Setup
Reference: [Configure bitbucket-pipelines.yml](https://support.atlassian.com/bitbucket-cloud/docs/configure-bitbucket-pipelinesyml/)

Steps involved to enable BitBucket CICD Setup for CDOPS_REPORTING service.

1. BitBucket Pipeline configuration file [bitbucket-pipelines.yml](https://bitbucket.org/phdata/cdops-reporting/src/master/bitbucket-pipelines.yml)
2. Script files referenced by Pipeline can be found here [Pipeline Scripts](https://bitbucket.org/phdata/cdops-reporting/src/master/bin/)

|Script Name| Purpose  |    
|--|--|    
| [fetch.sh](./bin/fetch.sh) | Downloads Liquibase JAR from GitHub Repo. Depends on Liquibase Version defined in following file [LIQUIBASE_VERSION](https://bitbucket.org/phdata/cdops-reporting/src/master/LIQUIBASE_VERSION)  |    
| [populate-snowflake-properties](./bin/populate-snowflake-properties) | Populates [snowflake.properties](https://bitbucket.org/phdata/cdops-reporting/src/master/resource/snowflake.properties) file with `user (Not Secure), private_key_file_pwd (Secure) and private_key_file (Secure)` variables. These variables are defined under Repository `settings -> pipelines -> Respository variables`.|    
| [dryRun.sh](./bin/dryRun.sh)| Executes LiquiBase process in dry run mode which generates delta SQL changes |    
| [provision.sh](./bin/provision.sh)| Executes LiquiBase process in execute mode mode and applies delta changes to Snowflake account|

**Note:** In BitBucket all variables are stored as Key:Value pair either secure or non-secure, variable private_key_file content is stored as type secure removing following lines `-----BEGIN ENCRYPTED PRIVATE KEY-----` and `-----END ENCRYPTED PRIVATE KEY-----` from the actual content.

### GitLab CICD Setup
Reference: [CI/CD pipelines](https://docs.gitlab.com/ee/ci/pipelines/)

Steps involved to enable GitLab CICD Setup for CDOPS_REPORTING service.

1. GitLab Pipeline configuration file [gitlab-ci.yml](https://bitbucket.org/phdata/cdops-reporting/src/master/gitlab-ci.yml)
2. Script files referenced by Pipeline can be found here [Pipeline Scripts](https://bitbucket.org/phdata/cdops-reporting/src/master/bin-gitlab/)

    
| [fetch.sh](./bin-gitlab/fetch.sh) | Downloads Liquibase JAR from GitHub Repo. Depends on Liquibase Version defined in following file [LIQUIBASE_VERSION](https://bitbucket.org/phdata/cdops-reporting/src/master/LIQUIBASE_VERSION)  |    
| [populate-snowflake-properties](./bin-gitlab/populate-snowflake-properties) | Populates [snowflake.properties](https://bitbucket.org/phdata/cdops-reporting/src/master/resource/snowflake.properties) file with `user (Not Secure), private_key_file_pwd (Secure) and private_key_file (Secure)` variables. These variables are defined under Settings -> CI/CD -> Vaariables`.|    
| [dryRun.sh](./bin-gitlab/dryRun.sh)| Executes LiquiBase process in dry run mode which generates delta SQL changes |    
| [provision.sh](./bin-gitlab/provision.sh)| Executes LiquiBase process in execute mode mode and applies delta changes to Snowflake account|

# User On-Boarding

Raise a request with CDOPS Snowflake Admin team to provide access to secure views. Provide following details:

**Account specific access only:** Here individual user account will have access to specific set of Database and Warehouse as provided:
- Account ID
- List of Database/Warehouse

**Role Base Access:** Here any user belonging to the role will have access to specific set of Database and Warehouse as provided:
- Account ID
- List of Database/Warehouse or all Database/Warehouse associated with the Role.

# Developer Guide
[Dev Guide](./doc/DeveloperGuide.md)