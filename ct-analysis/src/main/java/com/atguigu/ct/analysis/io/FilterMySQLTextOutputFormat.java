package com.atguigu.ct.analysis.io;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class FilterMySQLTextOutputFormat extends OutputFormat<Text,NullWritable> {
    static Logger log = Logger.getLogger(
            MySQLTextOutputFormat.class.getName());
    public FilterMySQLTextOutputFormat(){
//        new MySQLRecordWriter();
        log.info("new FilterMySQLTextOutputFormat()");
    }

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        log.info("getRecordWriter");
        return new MySQLFilterRecordWriter(context);
    }


    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        log.info("call checkOutputSpecs...");
    }

    //------------以下的代码  从mr源代码拿到 的----------------
    private FileOutputCommitter committer = null;
    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null: new Path(name);
    }
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }
    //------------以上的代码  从mr源代码拿到 的----------------
}


//内部类
//将mr的结果写出到mysql数据表
class MySQLFilterRecordWriter extends RecordWriter<Text,NullWritable>{
    static Logger log = Logger.getLogger(
            MySQLFilterRecordWriter.class.getName());
    private Connection connection = null;
    //userMap 保存用户号码与对应id的对应关系
    Map<String,Integer> userMap = new HashMap ();
    //dateMap 保存日期(年,年月,年月日)与对应id的对应关系
    Map<String,Integer> dateMap = new HashMap ();
    // 无参的构造器
    public MySQLFilterRecordWriter(){
        log.info("call MySQLFilterRecordWriter()");
    }
    // 初始化辅助的数据
    public  MySQLFilterRecordWriter(TaskAttemptContext taskAttemptContext){

        log.info("new MySQLRecordWriter(taskAttemptContext)");
        //连接mysql
        connection = JDBCUtil.getConnection();
        if(null == connection){
            log.error("连接失败....");
            return;
        }
        PreparedStatement pstat1 = null;
        PreparedStatement pstat2 = null;
        ResultSet rs1 = null;
        ResultSet rs2 = null;
        try {
            log.info("query...");
            // 1, 用户手机号码
            String queryUserSql = "select id,tel from ct_user;";
            pstat1 = connection.prepareStatement(queryUserSql);
            rs1 = pstat1.executeQuery();
            while (rs1.next()){
                int id = rs1.getInt(1);
                String tel = rs1.getString(2);
                userMap.put(tel,id);
            }

            // 2, 日期(年,年月,年月日)
            String queryDataSql= "select id,year,month,day from ct_date;";
            pstat2 = connection.prepareStatement(queryDataSql);
            rs2 = pstat2.executeQuery();
            while (rs2.next()){
                int id = rs2.getInt(1);
                String year = rs2.getString(2);
                String month = rs2.getString(3);
                // month有一个字符,则添加0在开头
                if( 1 == month.length() ){
                    month = "0"+month;
                }
                String day = rs2.getString(4);
                // day有一个字符,则添加0在开头
                if( 1 == day.length() ){
                    day = "0"+day;
                }
                dateMap.put(year+month+day,id);
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(null != pstat1){
                try {
                    pstat1.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(null != rs1){
                try {
                    rs1.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }


            if(null != pstat2){
                try {
                    pstat2.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(null != rs2){
                try {
                    rs2.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        log.info("cache userMap size="+userMap.size()+" , dateMap Size" + dateMap.size());

    }
    // 输出数据到mysql
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        log.info("call output format write...");
        // 从key获取tel,date
        String k = key.toString() + "_2020";
        String[] ks = k.split("_");
        //防御
//            if(2 != ks.length){
//                System.err.println("ks length neq 2");
//                return;
//            }
        String tel = ks[0];
        String date = ks[1];

        //解析value
        String[] values = "1_2".split("_");//value.toString().split("_");
        //防御
//            if(2 != values.length){
//                System.err.println("values length neq 2");
//                return;
//            }
        String sumCall = values[0];
        String sumDuration = values[1];
        PreparedStatement pstat = null;

        try {

            String insertSQL = "insert into ct_call(telid,dateid,sumcall,sumduration) values(?,?,?,?)";
            pstat = connection.prepareStatement(insertSQL);

            // 从map读取数据时, 要判断 是否存在此key对应的value
            pstat.setInt(1,userMap.containsKey(tel) ? userMap.get(tel) : 0);
            pstat.setInt(2,dateMap.containsKey(date) ? dateMap.get(date) : 0);
            pstat.setInt(3,Integer.parseInt(sumCall));
            pstat.setInt(4,Integer.parseInt(sumDuration ));

            int res = pstat.executeUpdate();// 执行sql
            log.info("exec mysql res = " + res);



        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(null != pstat){
                try {
                    pstat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if(null != connection){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
class JDBCUtil {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/ct?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT&useSSL=false";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "oneinstack";

    public static Connection getConnection() {
        System.out.println("call getConnection...");
        Connection conn = null;
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (ClassNotFoundException e1) {
            System.out.println ("数据库驱动加载失败！");
        }catch(SQLException e2){
            System.out.println ("数据库连接失败！");
        }  catch ( Exception e ) {
            e.printStackTrace();
        }
        System.out.println("mysql getConnection......"+MYSQL_URL+"::"+MYSQL_USERNAME+"::"+MYSQL_PASSWORD);
        return conn;

    }
}