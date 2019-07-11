#!/bin/bash

set -e

docker build -t spark-base:2.3.3 ./base
docker build -t spark-master:2.3.3 ./master
docker build -t spark-worker:2.3.3 ./worker
docker build -t spark-app:1.0.0 ./spark_app
