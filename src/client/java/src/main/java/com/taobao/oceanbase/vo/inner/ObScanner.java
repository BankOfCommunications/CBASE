package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ObScanner {
	private static final Log log = LogFactory.getLog(ObScanner.class);
	private List<ObRow> rows = new ArrayList<ObRow>();
	private boolean isEnd;
	private long affectRow;
	private ObRange range;

	public ObScanner() {
	}

	public void setEnd(boolean isEnd) {
		this.isEnd = isEnd;
	}

	public boolean isEnd() {
		return isEnd;
	}

	private void addRow(ObRow row) {
		rows.add(row);
	}

	public List<ObRow> getRows() {
		return Collections.unmodifiableList(rows);
	}

	public long getAffectRow() {
		return affectRow;
	}

	public ObRange getRange() {
		return range;
	}

	public String toString() {
		if (rows.size() == 0)
			return "[]";
		StringBuffer buffer = new StringBuffer();
		for (ObRow row : rows) {
			buffer.append(row.toString());
		}
		return buffer.toString();
	}

	public void deserialize(ByteBuffer buffer) {

		ObObj obj = new ObObj();
		// get BASIC_PARAM_FIELD
		do {
			obj.deserialize(buffer);
		} while (!obj.equalExtendType(ObActionFlag.BASIC_PARAM_FIELD));

		// is end
		obj.deserialize(buffer);
		long value = getNumber(obj);
		setEnd(value == 1);
		// affect row number
		obj.deserialize(buffer);
		affectRow = getNumber(obj);

		obj.deserialize(buffer); // data version, we do not care about this

		obj.deserialize(buffer);
		if (obj.equalExtendType(ObActionFlag.TABLET_RANGE_FIELD)) {
			obj.deserialize(buffer);
			value = getNumber(obj);
			ObBorderFlag bf = new ObBorderFlag();
			bf.setData((byte) value);

			obj.deserialize(buffer);
			String sk = obj.getValue().toString();

			obj.deserialize(buffer);
			String ek = obj.getValue().toString();

			range = new ObRange(sk, ek, bf);

			obj.deserialize(buffer); // for next extend flag
		}

		if (obj.equalExtendType(ObActionFlag.TABLE_PARAM_FIELD)) {
			obj.deserialize(buffer);
		}

		String table = null;
		String rowkey = null;

		ObRow row = null;
		String columnName = null;

		while (true && !obj.equalExtendType(ObActionFlag.END_PARAM_FIELD)) {
			if (obj.equalExtendType(ObActionFlag.TABLE_NAME_FIELD)) {
				obj.deserialize(buffer);
				table = obj.getValue().toString();
				obj.deserialize(buffer);
				continue;
			}

			if (obj.equalExtendType(ObActionFlag.ROW_KEY_FIELD)) {
				obj.deserialize(buffer);
				rowkey = obj.getValue().toString();
				row = new ObRow();
				row.setRowKey(rowkey);
				row.setTableName(table);
				addRow(row);
				obj.deserialize(buffer);
				continue;
			}

			if (obj.equalExtendType(ObActionFlag.OP_ROW_DOES_NOT_EXIST)) {
				// row not exist
				log.debug("row key not exist");
				obj.deserialize(buffer);
				continue;
			}

			if (!obj.isStringType()) {
				System.err.println("invalid struct");
				break;
			}

			columnName = obj.getValue().toString();

			obj.deserialize(buffer); // column value
			ObCell cell = new ObCell(table, rowkey, columnName, obj.clone());
			row.addCell(cell);

			obj.deserialize(buffer);
		}

		while (!obj.equalExtendType(ObActionFlag.END_PARAM_FIELD))
			obj.deserialize(buffer);
	}

	private long getNumber(ObObj obj) {
		Long value = (Long) obj.getValue();
		return value.longValue();
	}
}