package com.zx.hive;

import java.sql.*;

/**
 * @program: hadoop_demo1
 * @description:
 * @author: Ling
 * @create: 2018-12-05 10:58
 **/
public class Util {

//    // 加载驱动、创建连接
//    @Before
//    public void init() throws Exception {
//        Class.forName(driverName);
//        conn = DriverManager.getConnection(url,user,password);
//        stmt = conn.createStatement();
//    }
//
//    @Test
//    public void showTables() throws Exception {
//        String sql = "show tables";
//        System.out.println("Running: " + sql);
//        rs = stmt.executeQuery(sql);
//        while (rs.next()) {
//            System.out.println(rs.getString(1));
//        }
//    }

    public static void main(String[] args) throws ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection conn = DriverManager.getConnection("jdbc:hive2://self:10000/lxtest/", "hadoop", "123123")) {
            Statement st = conn.createStatement();
            ResultSet re = st.executeQuery("select count(*) from wc_result");
            if (re.next()) {
                System.err.println("wc_result数据条数:" + re.getInt(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
