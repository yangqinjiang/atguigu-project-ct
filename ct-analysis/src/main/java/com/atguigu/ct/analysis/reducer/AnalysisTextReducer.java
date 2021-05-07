package com.atguigu.ct.analysis.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 分析数据Reducer
 */
// 输入输出的key,value都是Text类型
public class AnalysisTextReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 同一个key下的所有value集合

        int sumCall = 0;
        int sumDuration = 0;
        for (Text value:values){
            int duration = Integer.parseInt(value.toString());
            sumDuration = sumDuration + duration; // 通话时长
            sumCall++; // 通话次数

        }
        System.out.println("reducer key = " + key.toString());
        context.getCounter("myReducer",key.toString()).increment(1);
        // reducer写出去, 到outputFormat
        context.write(key,new Text(sumCall+"_"+sumDuration));
    }
}
