#FROM java:8-jdk-alpine
#FROM docker.io/centos:7.4.1708
FROM openjdk:8-jdk-alpine
ENV APP_HOME=/myapp
ENV APP_EXE_FILE=spring-spark-word-count-0.0.1-SNAPSHOT.jar

#install MAVEN
ADD spring-spark-word-count-0.0.1-SNAPSHOT.jar /
##RUN cd ${APP_EXE_HOME}
#&& mvn clean package
VOLUME /tmp
#RUN java -jar /spring-spark-word-count-0.0.1-SNAPSHOT.jar
#java -jar target/gs-spring-boot-docker-0.1.0.jar
EXPOSE 3000 3000
CMD ["java","-jar","/spring-spark-word-count-0.0.1-SNAPSHOT.jar"]
