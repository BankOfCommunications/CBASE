package com.alipay.oceanbase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


public class ObDataSourceSpringExample {

    public static void main(String[] args) throws Exception {
        executeByDataSource();
        System.exit(0);
    }

    public static void executeByDataSource() throws Exception {
        ApplicationContext context = new ClassPathXmlApplicationContext(
            "classpath:applicationContext.xml");
        DataSource ds = (DataSource) context.getBean("dataSource");

        Connection conn = ds.getConnection();
        PreparedStatement pstmt = conn
            .prepareStatement("select * from __all_server where cluster_id = ?");
        pstmt.setInt(1, 1);

        ResultSet rs = pstmt.executeQuery();

        ResultSetMetaData rSetMetaData = rs.getMetaData();
        for (int i = 1; i <= rSetMetaData.getColumnCount(); i++) {
            System.out.print("|" + rSetMetaData.getColumnName(i) + "");
        }
        System.out.println();

        while (rs.next()) {
            for (int i = 1; i <= rSetMetaData.getColumnCount(); i++) {
                System.out.print("|" + rs.getString(i) + "");
            }
            System.out.println();
        }

        if (rs != null) {
            rs.close();
        }

        if (pstmt != null) {
            pstmt.close();
        }

        if (conn != null) {
            conn.close();
        }

        OceanbaseDataSourceProxy obdatasource = (OceanbaseDataSourceProxy) ds;
        obdatasource.destroy();
    }

}
