/* @(#)ObJDBCExample.java
 */
/**
 * ObDataSource JDBC example code
 *
 * @author <a href="mailto:zhuweng.yzf@alipay.com">Zhifeng YANG</a>
 */
package com.alipay.oceanbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 
 * 
 * @author liangjieli
 * @version $Id: ObJDBCExample.java, v 0.1 Mar 6, 2013 5:45:07 PM liangjieli Exp $
 */
public class ObJDBCExample {

    public static void main(String[] argv) throws ClassNotFoundException, SQLException {
        executeByStatement();

        executeByPreparedStatement();
    }

    public static void executeByStatement() throws ClassNotFoundException, SQLException {

        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://10.232.36.33:29097", "admin",
            "admin");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from __all_server");

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

        if (stmt != null) {
            stmt.close();
        }

        if (conn != null) {
            conn.close();
        }
    }

    public static void executeByPreparedStatement() throws ClassNotFoundException, SQLException {

        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager
            .getConnection(
                "jdbc:mysql://10.232.36.33:29097/oceanbase?useServerPrepStmts=true&prepStmtCacheSqlLimit=1000&useLocalSessionState=true",
                "admin", "admin");
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
    }
}
