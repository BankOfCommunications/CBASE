package com.taobao.oceanbase.vo;

import java.util.HashSet;
import java.util.Set;

public class GetParam {

	private String tableName;

	private Rowkey rowkey;

	private Set<String> columns = new HashSet<String>();

	public GetParam() {
	}

	public GetParam(String tableName) {
		this.tableName = tableName;
	}

	public GetParam(String tableName, Rowkey rowkey) {
		this.tableName = tableName;
		this.rowkey = rowkey;
	}

	public GetParam(String tableName, Rowkey rowkey, Set<String> columns) {
		this.tableName = tableName;
		this.rowkey = rowkey;
		this.columns = columns;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Rowkey getRowkey() {
		return rowkey;
	}

	public void setRowkey(Rowkey rowkey) {
		this.rowkey = rowkey;
	}

	public Set<String> getColumns() {
		return columns;
	}

	public void setColumns(Set<String> columns) {
		this.columns = columns;
	}

	public void addColumn(String column) {
		columns.add(column);
	}
}
