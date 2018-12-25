package com.alipay.oceanbase;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;

/**
 * 
 * 
 * @author liangjieli
 * @version $Id: ObDatasourceExample.java, v 0.1 Mar 7, 2013 10:49:48 AM
 *          liangjieli Exp $
 */
public class ObDataSourceExample {

  public static void main(String[] args) throws Exception {
    useDataSource();
    useDataSourceTransaction();
    System.exit(0);
  }

  public static void useDataSource() throws Exception {
    OceanbaseDataSourceProxy datasource = new OceanbaseDataSourceProxy();
    datasource.setConfigURL("http://obconsole.test.alibaba-inc.com/ob-config/config.co?dataId=yongle_test");
    datasource.setUsername("admin");
    datasource.setPassword("admin");
    datasource.init();

    Connection conn = datasource.getConnection();
    PreparedStatement pstmt = conn.prepareStatement("select * from __all_server where cluster_id = ?");
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

    datasource.destroy();
  }
  
  public static void useDataSourceTransaction() throws Exception {
    OceanbaseDataSourceProxy datasource = new OceanbaseDataSourceProxy();
    datasource.setConfigURL("http://obconsole.test.alibaba-inc.com/ob-config/config.co?dataId=yongle_test");
    datasource.setUsername("admin");
    datasource.setPassword("admin");
    datasource.init();

    Connection conn = datasource.getConnection();
    conn.setAutoCommit(false);  
    
    PreparedStatement ps_insert = conn.prepareStatement("insert into example (c1, c2, c3, c4, c5) values(?, ?, ?, ?, ?)");
    ps_insert.setInt(1, 1001);
    ps_insert.setInt(2, 1);
    ps_insert.setString(3, "ab");
    ps_insert.setTimestamp(4, new Timestamp(0));
    ps_insert.setFloat(5, 1.11F);
    ps_insert.executeUpdate();    
    conn.rollback();

    PreparedStatement ps_select = conn.prepareStatement("select * from example");
    ResultSet rs = ps_select.executeQuery();

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
    
    if (ps_insert != null) {
      ps_insert.close();
    }
    
    if (ps_select != null) {
      ps_select.close();
    }

    if (conn != null) {
      conn.close();
    }

    datasource.destroy();
  }
}
