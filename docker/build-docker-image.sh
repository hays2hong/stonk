#!/bin/sh

rm -rf stonk
git clone https://github.com/hays2hong/stonk.git
cd stonk
mvn clean package install -DskipTests
cd ..

IMAGE_VERSION="0.1"
echo "FROM kubespark/spark-driver:v2.2.0-kubernetes-0.5.0\nADD stonk/stonk-spark/target/stonk-spark-0.1-assembly.jar /opt/spark/stonk-spark.jar" \
> Dockerfile
docker build -t registry.cn-hangzhou.aliyuncs.com/stonk/spark-k8s-driver:${IMAGE_VERSION}
docker push registry.cn-hangzhou.aliyuncs.com/stonk/spark-k8s-driver:${IMAGE_VERSION}

echo "FROM kubespark/spark-executor:v2.2.0-kubernetes-0.5.0\nADD stonk/stonk-spark/target/stonk-spark-0.1-assembly.jar /opt/spark/stonk-spark.jar" \
> Dockerfile
docker build -t registry.cn-hangzhou.aliyuncs.com/stonk/spark-k8s-executor:${IMAGE_VERSION}
docker push registry.cn-hangzhou.aliyuncs.com/stonk/spark-k8s-executor:${IMAGE_VERSION}