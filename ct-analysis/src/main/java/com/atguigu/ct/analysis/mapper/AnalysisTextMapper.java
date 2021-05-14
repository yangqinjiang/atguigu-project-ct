package com.atguigu.ct.analysis.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 分析数据mapper
 */
// 此处的泛型<Text,Text> 是输出的key,value类型
public class AnalysisTextMapper extends TableMapper<Text,Text> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //1, 获取hbase数据表的rowkey
        // 例如: 5_13154926260_20180802160747_13574556259_0054_1
        // 说明:
        // 5 是分区号
        // 尾数  1 是指,此条数据是 主叫用户的通话记录
        // 13154926260 是主叫用户
        // 20180802160747 日期,时间
        // 13574556259 被叫用户
        // 0054 通话时长
        String rowkey = Bytes.toString(key.get());
        String[] values = rowkey.split("_");
        //防御
        if(6 != values.length){
            return;
        }

        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];


        // 分析通话日期
        if(8 != calltime.length()){
            return;
        }
        String year = calltime.substring(0, 4);
        String month = calltime.substring(0, 6);
        String date = calltime.substring(0, 8);

        //主叫用户
        //主叫用户 - 年
        context.write(new Text(call1 + "_" + year),new Text(duration));
        //主叫用户 - 月
        context.write(new Text(call1 + "_" + month),new Text(duration));
        //主叫用户 -  日
        context.write(new Text(call1 + "_" + date),new Text(duration));

       // 被叫用户
        //被叫用户 - 年
        context.write(new Text(call2 + "_" + year),new Text(duration));
        //被叫用户 -  月
        context.write(new Text(call2 + "_" + month),new Text(duration));
        //被叫用户 - 日
        context.write(new Text(call2 + "_" + date),new Text(duration));

    }
}
