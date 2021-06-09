package com.atguigu.ct.analysis.reducer;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * 分析数据Reducer
 */
// 输入输出的key,value都是Text类型
public class AnalysisTextReducer extends Reducer<Text,Text,Text,Text> {

    private Text v = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.v =new Text();
    }

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

        // reducer写出去, 到outputFormat
        v.set(sumCall+"_"+sumDuration);
        context.write(key,v);

        context.getCounter("myReducer","mykey").increment(1);
//        context.getCounter("myReducer",key.toString()+" duration").increment(sumDuration);
//        context.getCounter("myReducer",key.toString() + " call").increment(sumCall);
    }
}
