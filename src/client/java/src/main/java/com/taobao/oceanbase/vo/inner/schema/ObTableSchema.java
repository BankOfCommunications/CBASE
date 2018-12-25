package com.taobao.oceanbase.vo.inner.schema;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.util.Const;

public class ObTableSchema {
	private long tableId = ObSchemaCons.OB_INVALID_ID;
	private long maxColumnId;
	private long rowkeySplit;
	private long rowkeyMaxLength;
	private int blockSize;
	private int tableType;
	private String name;
	private String compressFuncName;
	private boolean pureUpdateTable;
	private boolean useBloomfilter;
	private boolean rowkeyFixLen;

	public void deserialize(ByteBuffer buffer) {
		tableId = Serialization.decodeVarLong(buffer);
		maxColumnId = Serialization.decodeVarLong(buffer);
		rowkeySplit = Serialization.decodeVarLong(buffer);
		rowkeyMaxLength = Serialization.decodeVarLong(buffer);
		blockSize = Serialization.decodeVarInt(buffer);
		tableType = Serialization.decodeVarInt(buffer);
		name = Serialization.decodeString(buffer, Const.NO_CHARSET);
		compressFuncName = Serialization.decodeString(buffer, Const.NO_CHARSET);
		pureUpdateTable = Serialization.decodeBoolean(buffer);
		useBloomfilter = Serialization.decodeBoolean(buffer);
		rowkeyFixLen = Serialization.decodeBoolean(buffer);
	}

	public long getTableId() {
		return tableId;
	}

	public long getMaxColumnId() {
		return maxColumnId;
	}

	public long getRowkeySplit() {
		return rowkeySplit;
	}

	public long getRowkeyMaxLength() {
		return rowkeyMaxLength;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public int getTableType() {
		return tableType;
	}

	public String getName() {
		return name;
	}

	public String getCompressFuncName() {
		return compressFuncName;
	}

	public boolean isPureUpdateTable() {
		return pureUpdateTable;
	}

	public boolean isUseBloomfilter() {
		return useBloomfilter;
	}

	public boolean isRowkeyFixLen() {
		return rowkeyFixLen;
	}
}
