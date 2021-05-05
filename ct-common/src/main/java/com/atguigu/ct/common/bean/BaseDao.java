package com.atguigu.ct.common.bean;

import com.atguigu.ct.common.api.Column;
import com.atguigu.ct.common.api.Rowkey;
import com.atguigu.ct.common.api.TableRef;
import com.atguigu.ct.common.constant.Names;
import com.atguigu.ct.common.constant.ValueConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * 基础数据访问对象
 */
public abstract class BaseDao {
    // 连接对象 的线程池
    private  ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    // admin对象的线程池
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();
    // 开始, 获取连接对象, 获取admin对象
    protected void start() throws Exception{
        getConnection();
        getAdmin();
    }
    //结束, 释放资源
    protected  void end() throws Exception{
        // 首先, 释放admin
        Admin admin = getAdmin();
        if(null != admin){
            admin.close();
            adminHolder.remove();
        }
        // 再, 释放conn
        Connection conn = getConnection();
        if (null != conn){
            conn.close();
            connHolder.remove();
        }

    }

    // 获取admin对象, 使用同步方法锁
    protected synchronized Admin getAdmin() throws Exception{
        Admin admin = adminHolder.get();
        if(null == admin){
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    // 获取连接对象, 使用同步方法锁
    protected  synchronized Connection getConnection() throws IOException {
        Connection conn = connHolder.get();
        if(null == conn){
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            System.out.println("hbase conn success...");
            connHolder.set(conn);
        }
        return conn;

    }


    /**
     * 创建表,如果表已经存在, 那么删除后, 再创建新的表
     */
    protected void createTableXX(String name,String... families) throws Exception{
        createTableXX(name,null,null,families);
    }
    /**
     * 创建表,如果表已经存在, 那么删除后, 再创建新的表
     */
    protected void createTableXX(String name, String coprocessorClass, Integer regionCount, String... families)  throws Exception{
        System.out.println("createTableXX..."+name);
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        if (admin.tableExists(tableName)){
            // 表存在,则删除它
            deleteTable(name);
        }
        System.out.println("createTable...");
        //创建表
        createTable(name,coprocessorClass,regionCount,families);
    }

    /**
     * 删除表格
     * @param name
     * @throws Exception
     */
    protected void deleteTable(String name) throws Exception{
        TableName tableName = TableName.valueOf(name);
        Admin admin = getAdmin();
        admin.disableTable(tableName);//先禁用表
        admin.deleteTable(tableName);//然后删除表
    }
    /**
     * 创建表
     * @param name
     * @param coprocessorClass String   协处理器,这里应该传入字符串
     * @param regionCount Integer 分区数量 , 这里使用Integer类型,而不是int基础类型, 方便传入null值
     * @param families
     */
    private  void createTable(String name,String coprocessorClass,Integer regionCount, String... families) throws Exception{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        // 创建表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        // 列族数量
        if (null == families || 0 == families.length ){
            // 不给定列族,则给一个默认值
            families = new String[1];
            families[0] = Names.CF_INFO.getValue().toString();
        }
        // 往表里添加列族
        for (String family:families){
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }
        System.out.println("addCoprocessor...");
        // 如果给定协处理器参数 ,则使用它
        if(null != coprocessorClass && !"".equals(coprocessorClass)){
            tableDescriptor.addCoprocessor(coprocessorClass);
        }
        System.out.println("genSplitKeys...");
        // 增加预分区
        if(null == regionCount || regionCount <= 1){
            admin.createTable(tableDescriptor);//不设置分区数量
        }else{
            //有设置分区数量, 则生成分区键...
            byte[][] splitKeys = genSplitKeys(regionCount);
            System.out.println("genSplitKeys , and createTable...");
            admin.createTable(tableDescriptor,splitKeys);
            System.out.println("createTable success...");
        }
    }
    // 生成分区键
    private  byte[][] genSplitKeys(int regionCount){
        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];
        // 0|,1|,2|,3|,4|
        // (-∞, 0|), [0|,1|), [1| +∞)
        List<byte[]> bsList = new ArrayList<byte[]>();
        for (int i = 0; i < splitKeyCount; i++) {
            String splitKey =i+"|";
            bsList.add(Bytes.toBytes(splitKey));
        }
        bsList.toArray(bs); //
        return bs;
    }

    //创建命名空间, 如果命名空间已经存在,不需要创建,否则,创建新的
    protected void createNamespaceNx(String namespace) throws  Exception {
        System.out.println("createNamespaceNx..."+namespace);
        Admin admin = getAdmin();
        try {
            admin.getNamespaceDescriptor(namespace);
        }catch (NamespaceNotFoundException e){
            // 这里只捕获NamespaceNotFoundException异常, 并处理这异常时出现的情况
            // 创建命名空间
            NamespaceDescriptor nsd = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(nsd);
        }

    }

    // 计算分区号(0,1,2)
    protected int genRegionNum(String tel,String date){
        // 13301234567
        String usercode = tel.substring(tel.length() - 4); // 获取前7位?
        // 20181010120000
        String yearMonth = date.substring(0, 6);
        int userCodeHash = usercode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        // crc校验采用异或算法， hash
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        // 取模
        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;
    }
    /**
     * 增加对象：自动封装数据，将对象数据直接保存到hbase中去
     * @param obj
     * @throws Exception
     */

    protected void putData(Object obj) throws Exception {
        System.out.println(obj.toString());
        //利用反射,注解技术,自动封装数据
        Class clazz = obj.getClass();
        TableRef tableRef = (TableRef)clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();

        // 查找rowkey属性
        Field[] fs = clazz.getDeclaredFields();
        String stringRowkey = "";
        for (Field f : fs){
            Rowkey rowkey = f.getAnnotation(Rowkey.class);
            if(null != rowkey){
                f.setAccessible(true);
                stringRowkey = (String)f.get(obj);
                break;
            }
        }

        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(stringRowkey));
        // 查找列族与列名,设置相应的值
        for (Field f:fs){
            Column column = f.getAnnotation(Column.class);
            if (null != column){
                String family = column.family();
                String colName = column.column();
                //列名, 无默认值,则使用其属性名
                if ( null == colName || "".equals(colName)){
                    colName = f.getName();
                }
                f.setAccessible(true);
                // 获取相应属性的值
                String value = (String)f.get(obj);
                // 调用hbase的函数
                put.addColumn(Bytes.toBytes(family) , Bytes.toBytes(colName),Bytes.toBytes(value));
            }
        }
        //增加数据
        table.put(put);
        //关闭表
        table.close();

    }
    /**
     * 增加多条数据
     * @param name
     * @param puts
     */
    protected void putData( String name, List<Put> puts ) throws Exception {

        // 获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));

        // 增加数据
        table.put(puts);

        // 关闭表
        table.close();
    }
}
