FROM docker.io/openjdk:8-jdk-alpine
USER root

COPY discovery-prep-spark-engine/target/discovery-prep-spark-engine-2.1.2.RELEASE.jar /

EXPOSE 8080

CMD ["/bin/sh"]
