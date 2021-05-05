#!/bin/bash
echo "运行[数据生产]程序"
nohup java -jar /opt/module/data/ct/ct-producer.jar  /opt/module/data/ct/contact-in.log /opt/module/data/ct/calllog-out.txt > /dev/null 2>&1 & echo $! > run.pid
echo "运行[Flume-Kafka]Agent"
nohup /opt/module/flume/bin/flume-ng agent --conf /opt/module/flume/conf/ --name a1 --conf-file /opt/module/data/ct/flume-kafka.conf > /dev/null 2>&1 & echo $! >> run.pid
echo "运行[kafka-console-consumer]消费者程序"
/opt/module/kafka/bin/kafka-console-consumer.sh --zookeeper hadoop102:2181 -topic calllog # --from-beginning

echo "正在关闭程序..."
cat run.pid | xargs kill -9

echo "" > run.pid # 清空此文件的数据
rm run.pid
echo "[已结束运行]"