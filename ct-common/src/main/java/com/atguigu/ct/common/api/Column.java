package com.atguigu.ct.common.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    String family() default "info";  // 列族,有默认值 info
    String column() default  ""; // 列名, 无默认值,则使用其属性名
}
