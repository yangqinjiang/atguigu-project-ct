package com.atguigu.ct.producer.bean;

import com.atguigu.ct.common.bean.DataIn;
import com.atguigu.ct.common.bean.DataOut;
import com.atguigu.ct.common.bean.Producer;
import com.atguigu.ct.common.util.DateUtil;
import com.atguigu.ct.common.util.NumberUtil;
import org.apache.commons.lang.math.NumberUtils;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 本地数据文件生产者
 */
public class LocalFileProducer implements Producer {
    //保存一份对象引用地址
    private DataIn in;
    private DataOut out;
    private  Boolean flg = true;
    public void setIn(DataIn in) {
        this.in = in;
    }

    public void setOut(DataOut out) {
    this.out = out;
    }

    //生产数据
    public void produce() {

        try {
            //1,读取通讯录的数据
            List<Contact> contacts = in.read(Contact.class);
            if(null == contacts){
                return;
            }
            while (flg){
                //2, 从通讯录中随机查找2个电话号码(主叫, 被叫)
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index = 0;
                //查找call2的数据, 必须与call1不一致
                while (true){
                    call2Index = new Random().nextInt(contacts.size());
                    if (call1Index != call2Index){// 如果是相同的index, 则继续查找下一个
                        break;
                    }
                }
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);
//                System.out.println(call1);
//                System.out.println(call2);
//                System.out.println("=================");
                //3,生成随机的通话时间
                String startDate = "20200101000000";
                String endDate = "20210101000000";
                long startTime = DateUtil.parse(startDate,"yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate,"yyyyMMddHHmmss").getTime();
                //4,通话时间, 随机数
                long calltime = startTime + (long)((endTime - startTime) * Math.random());
                //5, 通话时间字符串
                String callTimeSting =DateUtil.format(new Date(calltime),"yyyyMMddHHmmss");
                //6, 生成随机的通话时长, 3000s以内, 保持4个字符, 例如 100,格式为0100
                String duration = NumberUtil.format(new Random().nextInt(3000),4);
                //7,生成通话记录
                Calllog log = new Calllog(call1.getTel(),call2.getTel(),callTimeSting,duration);
                //8, 将通话记录刷写到数据文件中
                out.write(log);
                //9, sleep 500ms
                Thread.sleep(500);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // 退出 时, 清理资源
    public void close() throws IOException {
        if(null != this.in){
            this.in.close();
        }
        if(null != this.out){
            this.out.close();
        }
    }
}
