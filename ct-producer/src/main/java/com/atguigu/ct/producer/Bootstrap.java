package com.atguigu.ct.producer;

import com.atguigu.ct.common.bean.Producer;
import com.atguigu.ct.producer.bean.LocalFileProducer;
import com.atguigu.ct.producer.io.LocalFileDataIn;
import com.atguigu.ct.producer.io.LocalFileDataOut;

//启动程序
public class Bootstrap {
    public static void main(String[] args) throws  Exception {

        if(args.length < 2){
            System.out.println("系统参数不正确，请按照指定格式传递：java -jar Produce.jar path1 path2 ");
            System.exit(1);
        }
        //构建生产者对象
        Producer producer = new LocalFileProducer();

        //设置生产者的数据来源, 输出
//        producer.setIn(new LocalFileDataIn("f:\\contact.log"));
//        producer.setOut(new LocalFileDataOut("f:\\contact-out.log"));
        // 使用参数
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));

        //生产数据
        producer.produce();
        //关闭生产者对象
        producer.close();
    }
}
