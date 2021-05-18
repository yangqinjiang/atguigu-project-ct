package com.atguigu.ct.analysis;

import com.atguigu.ct.analysis.tool.AnalysisTextTool;
import org.apache.hadoop.util.ToolRunner;

public class AnalysisData {
    public static void main(String[] args) throws Exception {
        System.out.println("run AnalysisData module ......");
        //key是文本类型的
        int res = ToolRunner.run(new AnalysisTextTool(), args);
        System.out.println("res= "+ res);
    }
}
