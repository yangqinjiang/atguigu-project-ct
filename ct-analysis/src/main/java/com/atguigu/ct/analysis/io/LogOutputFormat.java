package com.atguigu.ct.analysis.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 测试的outputFormat
 */
public class LogOutputFormat extends FileOutputFormat<Text, Text> {
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new LogRecordWriter();
    }
}
class LogRecordWriter extends RecordWriter<Text,Text>{
    static Logger log = Logger.getLogger(
            LogRecordWriter.class.getName());
    public LogRecordWriter(){
        log.info("create logRecordWriter.");
    }
    public void write(Text key, Text value) throws IOException, InterruptedException {
        String s = key.toString();
        //根据一行的log数据是否包含15280214634,
        if(s.contains("15280214634")){
            log.info("----------contains 15280214634----------");
        }else{
            log.info("**********not contains 15280214634**********");
        }

    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        log.info("close record writer.");
    }
}
