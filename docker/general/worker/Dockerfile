FROM spark-base:2.3.3

COPY start-worker.sh /

ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://spark-master:7077"

#COPY spring-spark-word-count-0.0.1-SNAPSHOT.jar /

EXPOSE 8081

CMD ["/bin/bash", "/start-worker.sh"]
#CMD ["java","-jar","/spring-spark-word-count-0.0.1-SNAPSHOT.jar"]
