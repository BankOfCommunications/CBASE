package com.taobao.oceanbase.stress;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLClient {
	private static final String URL = "jdbc:mysql://10.232.36.29:3306/";
	private static final String USER = "root";
	private static final String PASSWORD = "123456";

	private String url;
	private String user;
	private String password;
	private Connection conn;

	SQLClient() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		url = URL;
		user = USER;
		password = PASSWORD;
		conn = null;
	}

	SQLClient(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
		conn = null;
	}

	public void connect() throws SQLException {
		conn = DriverManager.getConnection(url, user, password);
	}

	public void close() throws SQLException {
		conn.close();
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery(sql);
		return rs;
	}

	public int executeUpdate(String sql) throws SQLException {
		Statement stat = conn.createStatement();
		int affected_num = stat.executeUpdate(sql);

		if (stat != null) {
			stat.close();
		}
		return affected_num;
	}
}
