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

if [ "$1" == "-h" ]  || [ "$1" == "" ]; then
  echo "Usage: `basename $0` (start|stop|status)"
  exit 0
fi

VERSION=1.3.0
DEPLOY_NAME=discovery-spark-engine-$VERSION
MODULE_NAME=discovery-prep-spark-engine
JAR=$MODULE_NAME-$VERSION.jar

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
echo "DISCOVERY_SPARK_ENGINE_HOME=$DISCOVERY_SPARK_ENGINE_HOME"

LOG_DIR="$DISCOVERY_SPARK_ENGINE_HOME/logs"
CONF_DIR="$DISCOVERY_SPARK_ENGINE_HOME/conf"

pid="$DISCOVERY_SPARK_ENGINE_HOME/discovery-spark-engine.pid"

command=$1
case $command in

  (start)
  JAVA=java
    if [ "$JAVA_HOME" != "" ]; then
      JAVA=$JAVA_HOME/bin/java
    fi

  if [[ ! -d $LOG_DIR ]]; then
    $(mkdir -p $LOG_DIR)
  fi

    nohup $JAVA -Xmx8g -Xms8g -XX:MaxMetaspaceSize=256m -cp $CONF_DIR/ -Dlogging.config=$CONF_DIR/log4j2.xml -Dfile.encoding=UTF-8 -jar $DISCOVERY_SPARK_ENGINE_HOME/$JAR --spring.config.location=file:$CONF_DIR/application.properties >> $LOG_DIR/discovery-spark-engine.log 2>&1 &
    discovery_spark_engine_PID=$!
    echo $discovery_spark_engine_PID > $pid
    echo "Discovery Spark Engine successfully started ($discovery_spark_engine_PID)"
    ;;
  
  (status)
    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      echo "Discovery Spark Engine running ($TARGET_PID)"
    else
      echo No Discovery Spark Engine
    fi
    ;;
  (stop)
  if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo Stopping process `cat $pid`...
        kill $TARGET_PID
      else
        echo No Discovery Spark Engine to stop
      fi
      rm -f $pid
    else
      echo No Discovery Spark Engine node to stop
    fi
    ;;
esac
 
#eof

