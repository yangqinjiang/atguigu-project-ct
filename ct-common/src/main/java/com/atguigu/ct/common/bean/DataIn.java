package com.atguigu.ct.common.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface DataIn extends Closeable {
    // 数据输入的路径
    public void setPath(String path);
    //读取一个完整的数据体
    public  Object read() throws IOException;

    //读取一些数据,放到列表里, 泛型T只能是Data的子类
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException;

}
