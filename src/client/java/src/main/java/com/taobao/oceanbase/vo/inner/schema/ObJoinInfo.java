package com.taobao.oceanbase.vo.inner.schema;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.network.codec.Serialization;

public class ObJoinInfo extends BaseInited {
	private long joinTable;
	private long leftColumnId;
	private long correlatedColumn;
	private int startPos;
	private int endPos;

	public void deserialize(ByteBuffer buffer) {
		joinTable = Serialization.decodeVarLong(buffer);
		leftColumnId = Serialization.decodeVarLong(buffer);
		startPos = Serialization.decodeVarInt(buffer);
		endPos = Serialization.decodeVarInt(buffer);
		correlatedColumn = Serialization.decodeVarLong(buffer);
	}

	public long getJoinTable() {
		return joinTable;
	}

	public long getLeftColumnId() {
		return leftColumnId;
	}

	public long getCorrelatedColumn() {
		return correlatedColumn;
	}

	public int getStartPos() {
		return startPos;
	}

	public int getEndPos() {
		return endPos;
	}

}
