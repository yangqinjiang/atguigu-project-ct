package com.atguigu.ct.common.bean;

import java.io.Closeable;

/**
 * 生产者接口
 */
public interface Producer  extends Closeable {
    //数据输入的接口
    public void setIn(DataIn in);
    //数据输出的接口
    public void setOut(DataOut out);

    //生产数据
    public  void produce();
}
