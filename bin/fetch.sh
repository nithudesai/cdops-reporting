#!/bin/bash
set -eu -o pipefail

LB_VERSION=$(cat LIQUIBASE_VERSION)

echo "Using LB_VERSION: $LB_VERSION"

curl -o liquibase.zip -L https://github.com/liquibase/liquibase/releases/download/v${LB_VERSION}/liquibase-${LB_VERSION}.zip --output liquibase-${LB_VERSION}.zip
unzip -qq -o -d liquibase liquibase.zip

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