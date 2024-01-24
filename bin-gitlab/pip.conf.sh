#!/bin/bash

mkdir ~/.pip

cat <<EOF > ~/.pip/pip.conf
[global]
index-url = https://case.artifacts.medtronic.com/artifactory/api/pypi/ext-python-virtual/simple
EOF
