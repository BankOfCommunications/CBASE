package com.taobao.oceanbase.vo.inner;

import static com.taobao.oceanbase.vo.inner.ObActionFlag.END_PARAM_FIELD;
import static com.taobao.oceanbase.vo.inner.ObActionFlag.MUTATOR_PARAM_FIELD;
import static com.taobao.oceanbase.vo.inner.ObActionFlag.OBDB_SEMANTIC_FIELD;
import static com.taobao.oceanbase.vo.inner.ObActionFlag.OP_DEL_ROW;
import static com.taobao.oceanbase.vo.inner.ObActionFlag.OP_USE_OB_SEM;
import static com.taobao.oceanbase.vo.inner.ObActionFlag.ROW_KEY_FIELD;
import static com.taobao.oceanbase.vo.inner.ObActionFlag.TABLE_NAME_FIELD;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.taobao.oceanbase.network.codec.ObObjectUtil;
import com.taobao.oceanbase.util.Const;

public class ObMutator {

	private ObActionFlag semantic;
	private final List<Tuple> cells;

	public ObMutator(int capacity, ObActionFlag semantic) {
		this.cells = new ArrayList<Tuple>(capacity);
		this.semantic = semantic != null ? semantic : OP_USE_OB_SEM;
	}

	public void addCell(ObCell cell, ObActionFlag flag) {
		if (cell == null || flag == null)
			throw new IllegalArgumentException(
					"Both cell and flag must be not null");
		cells.add(new Tuple(cell, flag));
	}

	public void serialize(ByteBuffer buffer) {
		String table = "";
		String rowkey = "";
		serializeFlag(buffer, MUTATOR_PARAM_FIELD);
		serializeFlag(buffer, OBDB_SEMANTIC_FIELD);
		serializeFlag(buffer, semantic);
		for (Tuple tuple : cells) {
			ObCell cell = tuple.cell;
			ObActionFlag flag = tuple.flag;
			if (cell.getTableName() == null || cell.getRowKey() == null
					|| cell.getObObject() == null)
				throw new IllegalArgumentException(
						"cell's one field or more is null");
			if (!table.equalsIgnoreCase(cell.getTableName())) {
				table = cell.getTableName();
				serializeFlag(buffer, TABLE_NAME_FIELD);
				serialize(buffer, table);
			}
			if (!rowkey.equalsIgnoreCase(cell.getRowKey())) {
				rowkey = cell.getRowKey();
				serializeFlag(buffer, ROW_KEY_FIELD);
				serialize(buffer, rowkey);
			}
			if (flag == OP_DEL_ROW) {
				cell.getObObject().serialize(buffer);
			} else {
				serialize(buffer, cell.getColumnName());
				serializeFlag(buffer, flag);
				cell.getObObject().serialize(buffer);
			}
		}
		serializeFlag(buffer, END_PARAM_FIELD);
	}

	private void serializeFlag(ByteBuffer buffer, ObActionFlag flag) {
		ObObj flagObj = new ObObj();
		flagObj.setExtend(flag);
		flagObj.serialize(buffer);
	}

	private void serialize(ByteBuffer buffer, String str) {
		ObObj name = new ObObj();
		name.setString(str);
		name.serialize(buffer);
	}

	private int getFlagSize(ObActionFlag flag) {
		return ObObjectUtil.encodeLengthExtend(flag.action);
	}

	private int getSize(String str) {
		return ObObjectUtil
				.encodeLengthStr((String) str, Const.DEFAULT_CHARSET);
	}

	public int getSize() {
		int length = 0;
		length += getFlagSize(MUTATOR_PARAM_FIELD);
		length += getFlagSize(OBDB_SEMANTIC_FIELD);
		length += getFlagSize(semantic);
		String table = "";
		String rowkey = "";
		for (Tuple tuple : cells) {
			ObCell cell = tuple.cell;
			if (!table.equalsIgnoreCase(cell.getTableName())) {
				table = cell.getTableName();
				length += getFlagSize(TABLE_NAME_FIELD);
				length += getSize(cell.getTableName());
			}
			if (!rowkey.equals(cell.getRowKey())) {
				rowkey = cell.getRowKey();
				length += getFlagSize(ROW_KEY_FIELD);
				length += getSize(cell.getRowKey());
			}
			if (tuple.flag == OP_DEL_ROW) {
				length += getFlagSize(OP_DEL_ROW);
			} else {
				length += getSize(cell.getColumnName());
				length += getFlagSize(tuple.flag);
				length += cell.getObObject().getSize();
			}
		}
		return length += getFlagSize(END_PARAM_FIELD);
	}
	
	private static final class Tuple {
		public final ObCell cell;
		public final ObActionFlag flag;

		public Tuple(ObCell cell, ObActionFlag flag) {
			this.cell = cell;
			this.flag = flag;
		}
	}

}