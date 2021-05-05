package com.atguigu.ct.consumer.bean;

import com.atguigu.ct.common.bean.Consumer;
import com.atguigu.ct.common.constant.Names;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 *通话日志消费者对象
 */
public class CalllogConsumer implements Consumer {
    /**
     * 消费数据
     */
    public void consume() {
        try {
            // 创建配置对象
            Properties prop = new Properties();
            // 使用类加载器getContextClassLoader,加载文件流
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));

            // 获取flume采集的数据
            KafkaConsumer<String,String> consumer = new KafkaConsumer(prop);
            //关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue().toString()));
            //hbase数据访问对象
            HBaseDao dao = new HBaseDao();
            // 初始化
            dao.init();
            System.out.println("read msg from kafka...");
            while (true){
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);//100ms超时时间?
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
                    // 插入数据 到Hbase
                }
            }
        }catch (Exception e){

        }
    }

    public void close() throws IOException {

    }
}
