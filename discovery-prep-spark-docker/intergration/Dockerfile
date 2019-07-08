FROM docker.io/centos:7
#RUN apk --no-cache add procps && apk --update add coreutils
USER root

#RUN yum clean all \
#      yum -y update
#for yum & wget 
RUN yum -y upgrade && yum -y install wget
#for java
RUN yum install -y \
   java-1.8.0-openjdk \
   java-1.8.0-openjdk-devel

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk/

#download spark
ENV DAEMON_RUN=true
ENV SPARK_VERSION=2.3.3
ENV HADOOP_VERSION=2.7


RUN wget --no-verbose http://apache.mirror.iphh.net/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

#for master
#ENV SPARK_LOCAL_IP spark-master
ENV SPARK_MASTER_PORT 7077
#ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_WEBUI_PORT 7080
ENV SPARK_MASTER_LOG /spark/log
ENV SPARK_MASTER_HOST spark-master

#for slaves
ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs

#for spark_app
ADD spring-spark-word-count-0.0.1-SNAPSHOT.jar /
VOLUME /tmp

COPY start-all.sh /
#master port
EXPOSE 8080 7077 6066
#slaves port
EXPOSE 8081
#spark app port
EXPOSE 3000

#command
CMD ["/bin/bash", "/start-all.sh"]
