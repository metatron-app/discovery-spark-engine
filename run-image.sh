#docker run --name metatron_prep -p 7080:7080  -p 3000:3000 --hostname=spark-master metatron_prep:1.0.0

if [ -z "$2" ]
  then
      echo "Usage: $0 <localBaseDir> <container_name>"
      exit -1
fi

docker run -it --name $2 -p 7080:7080 -p 8181:8181 -p 3000:3000 -p 8080:8080 \
         --hostname=prep-spark-server --memory=8192M \
         --volume $1/uploads:$1/uploads \
         --volume $1/snapshots:$1/snapshots \
         metatron_prep:1.2.0

#eof
