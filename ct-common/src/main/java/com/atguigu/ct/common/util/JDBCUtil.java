package com.atguigu.ct.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCUtil {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/ct?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT&useSSL=false";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "oneinstack";

    public static Connection getConnection() {
        System.out.println("call getConnection...");
        Connection conn = null;
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (ClassNotFoundException e1) {
            System.out.println ("数据库驱动加载失败！");
        }catch(SQLException e2){
            System.out.println ("数据库连接失败！");
        }  catch ( Exception e ) {
            e.printStackTrace();
        }
        System.out.println("mysql getConnection......"+MYSQL_URL+"::"+MYSQL_USERNAME+"::"+MYSQL_PASSWORD);
        return conn;

    }
}
