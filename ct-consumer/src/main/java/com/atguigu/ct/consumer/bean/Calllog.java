package com.atguigu.ct.consumer.bean;

import com.atguigu.ct.common.api.Column;
import com.atguigu.ct.common.api.Rowkey;
import com.atguigu.ct.common.api.TableRef;

@TableRef("ct:calllog")
public class Calllog {
    @Rowkey
    private String rowKey;
    @Column(family = "caller")
    private String call1;
    @Column(family = "caller")
    private String call2;
    @Column(family = "caller")
    private String calltime;
    @Column(family = "caller")
    private String duration;
    @Column(family = "caller")
    private String flg = "1";// 0 被叫,  1主叫
    private String name;

    public Calllog() {
    }
    //接收一个字符串, 并解释它, 赋值
    public Calllog(String data) {
        String[]  values = data.split("\t");
        call1 = values[0];
        call2 = values[1];
        calltime = values[2];
        duration = values[3];
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCalltime() {
        return calltime;
    }

    public void setCalltime(String calltime) {
        this.calltime = calltime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getFlg() {
        return flg;
    }

    public void setFlg(String flg) {
        this.flg = flg;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Calllog{" +
                "rowKey='" + rowKey + '\'' +
                ", call1='" + call1 + '\'' +
                ", call2='" + call2 + '\'' +
                ", calltime='" + calltime + '\'' +
                ", duration='" + duration + '\'' +
                ", flg='" + flg + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
