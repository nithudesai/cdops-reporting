#!/bin/bash
set -euo pipefail

. bin/populate-snowflake-properties

JAVA_CMD=""
if [ -z ${JAVA_HOME+""} ];
then
    echo "Using JAVA_HOME: $JAVA_HOME"
    JAVA_CMD=$JAVA_HOME/bin/java
else
    echo "Using default java"
    JAVA_CMD=java
fi

./liquibase/liquibase --defaults-file=./resource/liquibase.properties --changeLogFile=./masterchangelog.xml --log-level=info updateSQL -DTASK_WAREHOUSE=${TASK_WAREHOUSE} -DACCOUNT_LOCATOR=${ACCOUNT_LOCATOR} -DORGANIZATION=${ORGANIZATION}