image 종류
1. general : multi container 모드로spark master, spark worker, spark app 을 각각 띄움
2. intergaration : one container 모드로 spark master, spark worker, spark app 을 하나의 container에 띄움
3. livy : one container 모드로 spark master, spark worker, spark app, livy server 를 하나의 container에 띄움

base image : centos 7

1. java 및 spark  설치
2. spark 구동 옵션 설정
3. spark context application 구동 설정 (외부와 rest api 통신 )
4. 필요한 포트 오픈


빌드 

1. spring-spark-word-count-0.0.1-SNAPSHOT.jar 파일을 (1) /general/spark_app or (2) /intergaration (3) livy 폴더에 카피
2. sh build-images.sh

도커 구동

1. sh run-images.sh ( /tmp 디렉토리를 로컬과 공유 하도록 설정하여 띄움)

테스트
1. spark master web ui :  “localhost:7077” url 사용하여 확인
2. spark worker web ui :  “localhost:8081” url 사용하여 확인
3. spark context application : 3000번 포트로 tomcat 서버 띄움. “http://localhost:3000/wordcount?words=Siddhant|Agnihotry|Technocrat|Siddhant|Sid” rest api(GET method) 호출하면 응답 확인 가능 
4. livy server web ui : "localhost:8998" url 사용하여 확인
5. 공유 디렉토리는 container 내부에 진입하여 /tmp 디렉토리와 로컬의 /tmp 디렉토리를 확인
