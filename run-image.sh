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

#docker run --name metatron_prep -p 7080:7080  -p 3000:3000 --hostname=spark-master metatron_prep:1.0.0

if [ -z "$1" ]
  then
      echo "Usage: $0 <container_name> [<prep_spark_engine_port>] [<prep_local_base_dir>]"
      exit -1
fi

CONTAINER_NAME=$1
PREP_SPARK_ENGINE_PORT=${2:-8080}           # to connect to
PREP_LOCAL_BASE_DIR=${3:-$HOME/dataprep}    # for shared folder


docker run -it --name $CONTAINER_NAME --memory=8192M \
         --hostname=prep-spark-engine \
         -p $PREP_SPARK_ENGINE_PORT:$PREP_SPARK_ENGINE_PORT \
         --volume $PREP_LOCAL_BASE_DIR/uploads:$PREP_LOCAL_BASE_DIR/uploads \
         --volume $PREP_LOCAL_BASE_DIR/snapshots:$PREP_LOCAL_BASE_DIR/snapshots \
         teamsprint/metatron_prep:1.2.0

#eof
