package com.taobao.oceanbase.vo.inner;

public class ObCell {

	private String tableName;
	private String columnName;
	private ObObj value;
	private String rowKey;

	public ObCell(String tableName, String rowKey, String columnName,
			ObObj value) {
		this.tableName = tableName;
		this.rowKey = rowKey;
		this.columnName = columnName;
		this.value = value;
	}

	public String getTableName() {
		return tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	@SuppressWarnings("unchecked")
	public <V> V getValue() {
		return (V) value.getValue();
	}

	public String getRowKey() {
		return rowKey;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(columnName).append("=");
		sb.append(value == null ? null : value.getValue());
		return sb.toString();
	}

	protected ObObj getObObject() {
		return value;
	}

}