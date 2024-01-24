#!/bin/bash
set -euo pipefail

echo user=$CDOPS_SNOWFLAKE_USER >> ./resource/snowflake.properties
echo private_key_file_pwd=$CDOPS_PRIVATE_PASSPHRASE >> ./resource/snowflake.properties
echo private_key_file=$CDOPS_PRIVATE_KEY >> ./resource/snowflake.properties

JAVA_CMD=""
if [ -z ${JAVA_HOME+""} ];
then
    echo "Using JAVA_HOME: $JAVA_HOME"
    JAVA_CMD=$JAVA_HOME/bin/java
else
    echo "Using default java"
    JAVA_CMD=java
fi

unzip -qq -o -d liquibase liquibase.zip

./liquibase/liquibase --defaults-file=./resource/liquibase.properties --changeLogFile=./masterchangelog.xml --log-level=info updateSQL -DTASK_WAREHOUSE=$TASK_WAREHOUSE -DACCOUNT_LOCATOR=$ACCOUNT_LOCATOR -DORGANIZATION=$ORGANIZATION

if [ ${#} -eq 0 ]
then
	echo "Success: Liquibase Execution"
else
	echo "Failed: Liquibase Execution"
	echo "Release Liquibase Lock"
	./liquibase/liquibase --defaults-file=./resource/liquibase.properties releaseLocks
	exit 1 # wrong args
fi
