package com.atguigu.ct.common.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE}) // 定义在类的注解
@Retention(RetentionPolicy.RUNTIME) // 运行时
public @interface TableRef {
    String value();
}
