package com.atguigu.ct.consumer.bean;

import com.atguigu.ct.common.bean.Consumer;
import com.atguigu.ct.common.constant.Names;
import com.atguigu.ct.consumer.dao.HBaseDao;
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
                    System.out.println(".");
//                    dao.insertData(consumerRecord.value());

                    // 优化, 插入类对象数据 到Hbase
                    // 使用反射,注解, 将类对象映射到hbase表中去
                    // 主叫用户的记录
                    Calllog log = new Calllog(consumerRecord.value());
                    dao.insertData(log);
//                    // 被叫用户的记录
//                    Calllog log2 = new Calllog(consumerRecord.value());
//                    // 交换两个属性的值
//                    String call1 = log2.getCall1();
//                    String call2 = log2.getCall2();
//                    log2.setCall1(call2);
//                    log2.setCall2(call1);
//                    log2.setFlg("0"); // flg = 0
//                    dao.insertData(log2);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void close() throws IOException {

    }
}
