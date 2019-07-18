#!/bin/bash

ENGINE=discovery-prep-spark-engine
VER=1.2.0

DEPLOY_NAME=$ENGINE-$VER

mkdir $DEPLOY_NAME
cp $ENGINE/target/$DEPLOY_NAME.jar $DEPLOY_NAME
cp run-prep-spark-engine.sh $DEPLOY_NAME
tar zcvf $DEPLOY_NAME.tar.gz $DEPLOY_NAME

rm -rf $DEPLOY_NAME

#eof
