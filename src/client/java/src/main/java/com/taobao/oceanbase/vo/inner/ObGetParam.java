package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ObGetParam extends ObReadParam {

	private long borderFlag = 15;
	private final List<ObCell> cells;

	public ObGetParam(int capacity) {
		cells = new ArrayList<ObCell>(capacity);
	}

	public List<ObCell> getCells() {
		return Collections.unmodifiableList(cells);
	}

	public void addCell(ObCell cell) {
		cells.add(cell);
	}

	public long getCellsSize() {
		return cells.size();
	}

	public void setBorderFlag(long borderFlag) {
		this.borderFlag = borderFlag;
	}

	public void serialize(ByteBuffer buffer) {
		ObObj obj = new ObObj();
		
		obj.setExtend(ObActionFlag.RESERVE_PARAM_FIELD);
		obj.serialize(buffer);
		obj.setNumber(isReadConsistency());
		obj.serialize(buffer);

		obj.setExtend(ObActionFlag.BASIC_PARAM_FIELD);
		obj.serialize(buffer);
		obj.setNumber(isCached(), false);
		obj.serialize(buffer);
		obj.setNumber(borderFlag, false);
		obj.serialize(buffer);
		obj.setNumber(getBeginVersion(), false);
		obj.serialize(buffer);
		obj.setNumber(getEndVersion(), false);
		obj.serialize(buffer);

		obj.setExtend(ObActionFlag.TABLE_PARAM_FIELD);
		obj.serialize(buffer);
		String tableName = null;
		String rowkey = null;
		for (ObCell cell : cells) {
			if (cell.getTableName() == null || cell.getRowKey() == null
					|| cell.getColumnName() == null)
				continue;

			if (!cell.getTableName().equals(tableName)) {
				tableName = cell.getTableName();
				obj.setExtend(ObActionFlag.TABLE_NAME_FIELD);
				obj.serialize(buffer);
				obj.setString(tableName);
				obj.serialize(buffer);
			}
			if (!cell.getRowKey().equals(rowkey)) {
				rowkey = cell.getRowKey();
				obj.setExtend(ObActionFlag.ROW_KEY_FIELD);
				obj.serialize(buffer);
				obj.setString(rowkey);
				obj.serialize(buffer);
			}
			obj.setString(cell.getColumnName());
			obj.serialize(buffer);
		}

		obj.setExtend(ObActionFlag.END_PARAM_FIELD);
		obj.serialize(buffer);
	}

	public int getSize() {
		int length = 0;
		ObObj obj = new ObObj();
		
		obj.setExtend(ObActionFlag.RESERVE_PARAM_FIELD);
		length += obj.getSize();
		obj.setNumber(isReadConsistency());
		length += obj.getSize();

		obj.setExtend(ObActionFlag.BASIC_PARAM_FIELD);
		length += obj.getSize();
		obj.setNumber(isCached(), false);
		length += obj.getSize();
		obj.setNumber(borderFlag, false);
		length += obj.getSize();
		obj.setNumber(getBeginVersion(), false);
		length += obj.getSize();
		obj.setNumber(getEndVersion(), false);
		length += obj.getSize();

		obj.setExtend(ObActionFlag.TABLE_PARAM_FIELD);
		length += obj.getSize();
		String tableName = null;
		String rowkey = null;
		for (ObCell cell : cells) {
			if (cell.getTableName() == null || cell.getRowKey() == null
					|| cell.getColumnName() == null)
				continue;

			if (!cell.getTableName().equals(tableName)) {
				tableName = cell.getTableName();
				obj.setExtend(ObActionFlag.TABLE_NAME_FIELD);
				length += obj.getSize();
				obj.setString(tableName);
				length += obj.getSize();
			}
			if (!cell.getRowKey().equals(rowkey)) {
				rowkey = cell.getRowKey();
				obj.setExtend(ObActionFlag.ROW_KEY_FIELD);
				length += obj.getSize();
				obj.setString(rowkey);
				length += obj.getSize();
			}
			obj.setString(cell.getColumnName());
			length += obj.getSize();
		}

		obj.setExtend(ObActionFlag.END_PARAM_FIELD);
		length += obj.getSize();

		return length;
	}
}