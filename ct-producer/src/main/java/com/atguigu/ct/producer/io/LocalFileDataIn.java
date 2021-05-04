package com.atguigu.ct.producer.io;

import com.atguigu.ct.common.bean.Data;
import com.atguigu.ct.common.bean.DataIn;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
/**
 * 本地文件数据输入
 */
public class LocalFileDataIn implements DataIn {
    public LocalFileDataIn(String path){
        setPath(path);
    }

    private BufferedReader reader = null;
    public void setPath(String path) {
        //生成一个读取文本的reader
        try {
            //涉及到IO流的知识
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Object read() throws IOException {
        return null;
    }

    //读取数据,返回数据集合
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {

        //使用泛型
        List<T> ts = new ArrayList<T>();
        try {
            String line = null;
            while ( ( line = reader.readLine() ) != null ){
                // 将数据转换为指定类型的对象，封装为集合返回
                T t = clazz.newInstance();//Class类与反射
                t.setValue(line);
                ts.add(t);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return ts;
    }

    public void close() throws IOException {
        if(null != reader){
            reader.close();
        }
    }
}
