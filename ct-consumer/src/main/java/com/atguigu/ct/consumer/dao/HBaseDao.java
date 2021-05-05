package com.atguigu.ct.consumer.dao;

import com.atguigu.ct.common.bean.BaseDao;
import com.atguigu.ct.common.constant.Names;
import com.atguigu.ct.common.constant.ValueConstant;
import com.atguigu.ct.consumer.bean.Calllog;

/**
 * Hbase数据访问对象
 */
public class HBaseDao extends BaseDao {
    //初始化
    public void init() throws Exception{
        start();
        createNamespaceNx(Names.NAMESPACE.getValue().toString());
        //TODO: 缺少协处理器模块
//       createTableXX(Names.TABLE.getValue().toString(),"com.atguigu.ct.consumer.coprocessor.InsertCalleeCoprocessor", ValueConstant.REGION_COUNT,Names.CF_CALLER.getValue().toString(),Names.CF_CALLEE.getValue().toString());
         createTableXX(Names.TABLE.getValue().toString(),null, ValueConstant.REGION_COUNT,Names.CF_CALLER.getValue().toString(),Names.CF_CALLEE.getValue().toString());

        end();
    }



    public void insertData( Calllog log) throws Exception{
        log.setRowKey(genRegionNum(
                log.getCall1(),log.getCalltime()) // 使用主叫号码,和日期 计算出分区号
                +"_"+log.getCall1()
                + "_" +log.getCalltime()
                + "_" + log.getCall2()
                +"_"+log.getDuration());
        putData(log);
    }
}
