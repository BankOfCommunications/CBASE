package com.taobao.oceanbase.vo.inner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("unchecked")
public class ObRow {

	private String tableName;
	private String rowKey;
	private List<ObCell> cells = new ArrayList<ObCell>();
	private boolean isFull;
	private AtomicInteger tabletIndex;
	
	public ObRow() {
		tabletIndex = new AtomicInteger((int)System.currentTimeMillis());
	}
	
	public int incrAndGetIndex() {
		return Math.abs(tabletIndex.incrementAndGet());
	}

	public void addCell(ObCell cell) {
		cells.add(cell);
	}

	public List<ObCell> getColumns() {
		return Collections.unmodifiableList(cells);
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public <V> V getValueByColumnName(String columnName) {
		for (ObCell cell : cells) {
			if (columnName.equals(cell.getColumnName()))
				return (V) cell.getValue();
		}
		return null;
	}
	
	public boolean isFull() {
		return isFull;
	}

	public void setFull(boolean isFull) {
		this.isFull = isFull;
	}

	public String toString() {
		if (cells.size() == 0)
			return "[]\n";

		StringBuffer sb = new StringBuffer();
		sb.append("{tablename=").append(getTableName()).append(",");
		sb.append("rowKey=").append(getRowKey()).append("} => ");
		sb.append("[");
		for (ObCell cell : cells) {
			sb.append(cell.toString()).append(",");
		}
		sb.append("]\n");
		return sb.toString();
	}
}