package com.atguigu.ct.common.bean;

/**
 * 数据对象
 */
public abstract class Data implements Val{

    //数据本体
    public String content;

    public void setValue(Object val) {
        content = (String)val;
    }

    public String getValue() {
        return content;
    }

}
