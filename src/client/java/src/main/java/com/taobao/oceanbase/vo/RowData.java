package com.taobao.oceanbase.vo;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.taobao.oceanbase.util.Const;

public class RowData {

	private Map<String, Object> data;

	public RowData() {
		data = new HashMap<String, Object>();
	}

	public RowData(String table, String rowkey, int capacity) {
		this.table = table;
		this.rowkey = rowkey;
		data = new HashMap<String, Object>(capacity);
	}

	public void addData(String name, Object value) {
		data.put(name, value);
	}

	@SuppressWarnings("unchecked")
	public <T> T get(String name) {
		return (T) data.get(name.toLowerCase());
	}

	public boolean isEmpty() {
		return data.size() == 0;
	}
	
	public Map<String,Object> getRow() {
		return data;
	}
	
	private String table;

	private String rowkey;

	public String getTable() {
		return table;
	}

	public Rowkey getRowKey() {
		return new Rowkey(){

			@Override
			public byte[] getBytes() {
				try {
					return rowkey.getBytes(Const.NO_CHARSET);
				} catch (UnsupportedEncodingException e) {
					return null;//never happen
				}
			}};
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();

		for (Map.Entry<String, Object> entry : data.entrySet()) {
			sb.append(entry.getKey());
			sb.append(" => ");
			sb.append(entry.getValue());
			sb.append(" ");
		}
		sb.append("\n");
		return sb.toString();
	}
}