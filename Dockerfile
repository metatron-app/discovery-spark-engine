FROM docker.io/openjdk:8-jdk-alpine
USER root

COPY discovery-prep-spark-engine/target/discovery-prep-spark-engine-1.2.0.jar /

EXPOSE 8080

CMD ["/bin/sh"]
