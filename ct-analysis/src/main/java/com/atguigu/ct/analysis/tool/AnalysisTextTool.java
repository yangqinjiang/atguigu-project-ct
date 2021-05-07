package com.atguigu.ct.analysis.tool;

import com.atguigu.ct.analysis.io.MySQLTextOutputFormat;
import com.atguigu.ct.analysis.mapper.AnalysisTextMapper;
import com.atguigu.ct.analysis.reducer.AnalysisTextReducer;
import com.atguigu.ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * 分析数据的工具类, key是文本类型的
 */
public class AnalysisTextTool implements Tool {
    //分析源码,最后会调用此函数 run
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        //jar
        job.setJarByClass(AnalysisTextTool.class);

        // hbase的scan对象
        Scan scan = new Scan();
        // 只扫描 指定列族的数据
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue().toString()));

        //设置mapper,因为从hbase读取数据, 使用特定的api,更方便一些
        // 注意导入的包, 有新版和旧版之分
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getValue().toString(),
                scan,
                AnalysisTextMapper.class, // 此类是TableMapper的子类
                Text.class,
                Text.class,
                job
        );

        // 设置reducer, 按普通的方式
        job.setReducerClass(AnalysisTextReducer.class);
        // 设置reducer输出时的key,value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 紧接着上面的reducer输出后, 调用此outputformat
        // 将输出结果保存到mysql
        job.setOutputFormatClass(MySQLTextOutputFormat.class);
        //运行,并等待运行结果
        boolean flg = job.waitForCompletion(true);
        if(flg){
            return JobStatus.State.SUCCEEDED.getValue();
        }else{
            return JobStatus.State.FAILED.getValue();
        }

    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}

