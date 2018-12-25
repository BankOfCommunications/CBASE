package com.taobao.oceanbase.vo.inner.schema;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.util.Const;

public class ObColumnSchemaV2 {
	private boolean maintained;
	private long tableId = ObSchemaCons.OB_INVALID_ID;
	private long columnGroupId = ObSchemaCons.OB_INVALID_ID;
	private long columnId = ObSchemaCons.OB_INVALID_ID;

	private long size;
	private int type;
	private String columnName;
	private ObJoinInfo joinInfo;
	private ObColumnSchemaV2 nextColumnGroup;
	
	public void deserialize(ByteBuffer buffer) {
		maintained = Serialization.decodeBoolean(buffer);
		tableId = Serialization.decodeVarLong(buffer);
		columnGroupId = Serialization.decodeVarLong(buffer);
		columnId = Serialization.decodeVarLong(buffer);
		size  = Serialization.decodeVarLong(buffer);
		type = Serialization.decodeVarInt(buffer);
		columnName = Serialization.decodeString(buffer, Const.NO_CHARSET);
		joinInfo = new ObJoinInfo();
		joinInfo.deserialize(buffer);
	}

	public boolean isMaintained() {
		return maintained;
	}

	public long getTableId() {
		return tableId;
	}

	public long getColumnGroupId() {
		return columnGroupId;
	}

	public long getColumnId() {
		return columnId;
	}

	public long getSize() {
		return size;
	}

	public int getType() {
		return type;
	}

	public String getColumnName() {
		return columnName;
	}

	public ObJoinInfo getJoinInfo() {
		return joinInfo;
	}

	public ObColumnSchemaV2 getNextColumnGroup() {
		return nextColumnGroup;
	}
}
