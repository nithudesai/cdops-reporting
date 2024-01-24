#!/bin/bash
set -eu -o pipefail

LB_VERSION=$(cat LIQUIBASE_VERSION)

echo "Using LB_VERSION: $LB_VERSION"

unzip -qq -o -d liquibase liquibase-4.4.0.zip

JAVA_CMD=""
if [ -z ${JAVA_HOME+""} ];
then
    echo "Using JAVA_HOME: $JAVA_HOME"
    JAVA_CMD=$JAVA_HOME/bin/java
else
    echo "Using default java"
    JAVA_CMD=java
fi

./liquibase/liquibase --version

echo "Fetch successful and LiquiBase setup is successful"