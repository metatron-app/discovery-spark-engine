#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if [ "$1" == "-h" ]; then
  echo "Usage: `basename $0` [<application.yaml path>]"
  exit 0
fi

# Test mode
if [ "$1" == "-t" ]; then
  JAR=discovery-prep-spark-engine/target/discovery-prep-spark-engine-1.2.0.jar
  PORT=5300
  java -jar $JAR --server.port=$PORT
  exit 0
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

function parse_yaml {
   local prefix=$2
   local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
   sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p"  $1 |
   awk -F$fs '{
      indent = length($1)/2;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
         printf("%s%s%s=%s\n", "'$prefix'",vn, $2, $3);
      }
   }'
}

### Find discovery's configuration file ###
if [ ! -z "$1" ]; then
  CONF_YAML=$1
else
  CONF_YAML=$METATRON_HOME/conf/application-config.yaml
fi

echo -e "${WHITE}Finding discovery's configuration... ${WHITE}${CONF_YAML}"

if [ ! -f "$CONF_YAML" ]; then
  echo -e "${RED}Config file not found: ${CONF_YAML}"
  echo -e "${NC}"
  exit -1
fi

echo -e "${WHITE}Parsing discovery's configuration..."


### Get jar path ###
CONF_KEY_JAR="polaris.dataprep.etl.spark.jar"
JAR=`parse_yaml $CONF_YAML | grep ${CONF_KEY_JAR} | cut -d= -f2 | envsubst`
if [ -z "$JAR" ]; then
  echo -e "${GREEN}${CONF_KEY_JAR}: ${RED}Not configured"
  exit -1
fi
echo -e "${GREEN}${CONF_KEY_JAR}: ${CYAN}${JAR}"


### Get service port ###
CONF_KEY_PORT="polaris.dataprep.etl.spark.port"
PORT=`parse_yaml $CONF_YAML | grep ${CONF_KEY_PORT} | cut -d= -f2 | envsubst`
if [ -z "$PORT" ]; then
  echo -e "${GREEN}${CONF_KEY_PORT}: ${RED}Not configured"
  exit -1
fi
echo -e "${GREEN}${CONF_KEY_PORT}: ${CYAN}${PORT}"


### Launch service ###
echo -e "${WHITE}Lauching Prep Spark Engine... java -jar $JAR --server.port=$PORT${NC}"
java -jar $JAR --server.port=$PORT

#eof
