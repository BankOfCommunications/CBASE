package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.util.Const;

public class ObRange {

	private long tableId;
	private ObBorderFlag borderFlag;
	private String startKey;
	private String endKey;

	public ObRange(String startKey, String endKey, ObBorderFlag boderFlag) {
		this.startKey = startKey;
		this.endKey = endKey;
		this.borderFlag = boderFlag;
	}

	public void serialize(ByteBuffer buffer) {
		buffer.put(Serialization.encodeVarLong(tableId));
		buffer.put(Serialization.encode(borderFlag.getData()));
		buffer.put(Serialization.encode(startKey, Const.NO_CHARSET));
		buffer.put(Serialization.encode(endKey, Const.NO_CHARSET));
	}

	public int getSize() {
		int length = 0;
		length += Serialization.getNeedBytes(tableId);
		length += 8;
		length += Serialization.getNeedBytes(startKey);
		length += Serialization.getNeedBytes(endKey);
		return length;
	}

	public ObBorderFlag getBorderFlag() {
		return borderFlag;
	}

	public void setBorderFlag(ObBorderFlag borderFlag) {
		this.borderFlag = borderFlag;
	}

	public String getStartKey() {
		return startKey;
	}

	public void setStartKey(String startKey) {
		this.startKey = startKey;
	}

	public String getEndKey() {
		return endKey;
	}

	public void setEndKey(String endKey) {
		this.endKey = endKey;
	}

}