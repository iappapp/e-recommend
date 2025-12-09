#!/bin/zsh

CURRENT_DIR=$(cd "$(dirname $0)" && pwd)
SPARK_HOME=/Users/mac/Downloads/spark-3.5.7/bin
JAR_FILE=$CURRENT_DIR/target/OfflineRecommender-1.0-jar-with-dependencies.jar

if [ ! -f $JAR_FILE ]; then
  mvn clean install -DskipTests
fi


$SPARK_HOME/spark-submit --class com.huat.huangjiahao.OfflineRecommender \
 --master spark://tiger.local:7077 \
 --deploy-mode client $JAR_FILE

echo 'task submit finish'