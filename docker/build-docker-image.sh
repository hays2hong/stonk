#!/bin/sh

cd ..
mvn clean package install -DskipTests

IMAGE_VERSION="0.1"
docker build -t registry.cn-hangzhou.aliyuncs.com/stonk/spark-k8s-driver:${IMAGE_VERSION} driver/Dockerfile
docker push registry.cn-hangzhou.aliyuncs.com/stonk/spark-k8s-driver:${IMAGE_VERSION}

docker build -t registry.cn-hangzhou.aliyuncs.com/stonk/spark-k8s-excutor:${IMAGE_VERSION} driver/Dockerfile
docker push registry.cn-hangzhou.aliyuncs.com/stonk/spark-k8s-excutor:${IMAGE_VERSION}