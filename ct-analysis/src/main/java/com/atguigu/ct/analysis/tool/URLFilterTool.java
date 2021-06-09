package com.atguigu.ct.analysis.tool;

import com.atguigu.ct.analysis.io.FilterMySQLTextOutputFormat;
import com.atguigu.ct.analysis.mapper.FilterMapper;
import com.atguigu.ct.analysis.reducer.FilterReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class URLFilterTool  implements Tool {
    public int run(String[] args) throws Exception {
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        //args = new String[] { "e:/mr/input/outputformat", "e:/mr/output/outputformat/" };

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(URLFilterTool.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 要将自定义的输出格式组件设置到job中
        job.setOutputFormatClass(FilterMySQLTextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        if(result){
            return JobStatus.State.SUCCEEDED.getValue();
        }else{
            return JobStatus.State.FAILED.getValue();
        }
    }

    private Configuration conf;
    static Logger log = Logger.getLogger(
            URLFilterTool.class.getName());
    public void setConf(Configuration configuration) {
        log.info("setConf");
        this.conf = configuration;
    }

    public Configuration getConf() {
        log.info("getConf");
        return this.conf;
    }
}
