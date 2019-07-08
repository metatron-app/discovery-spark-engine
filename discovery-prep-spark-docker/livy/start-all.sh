#export SPARK_MASTER_HOST=`hostname`
#!/bin/bash

export SPARK_HOME=/spark
#export SPARK_WORKER_WEBUI_PORT=8081
#export SPARK_MASTER_HOST=`hostname`
# turn on bash's job control
set -m

. "/spark/sbin/spark-config.sh"
. "/spark/bin/load-spark-env.sh"

#echo $SPARK_CONF_DIR &
# Start the primary process and put it in the background
cd /${SPARK_HOME}/bin && /${SPARK_HOME}/sbin/../bin/spark-class org.apache.spark.deploy.master.Master --host ${SPARK_MASTER_HOST} --port ${SPARK_MASTER_PORT} --webui-port ${SPARK_MASTER_WEBUI_PORT} &
# Start the helper process
/${SPARK_HOME}/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker --webui-port ${SPARK_WORKER_WEBUI_PORT}  spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} &

#start livy
/livy/bin/livy-server start &

# the my_helper_process might need to know how to wait on the
# primary process to start before it does its work and returns
java -jar /spring-spark-word-count-0.0.1-SNAPSHOT.jar

# now we bring the primary process back into the foreground
# and leave it there
#fg %1


