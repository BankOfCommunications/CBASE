package com.taobao.oceanbase.network.packet;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.vo.inner.ObScanParam;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class ObScanRequest extends BasePacket {

	private ObScanParam param;

	private long timeStamp = System.currentTimeMillis();

	public ObScanRequest(int request, ObScanParam param) {
		super(PacketCode.OB_SCAN_REQUEST, request);
		this.param = param;
	}

	@Override
	public int getBodyLen() {
		return param.getSize() + Serialization.getNeedBytes(timeStamp);
	}

	@Override
	void writeBody(ByteBuffer buffer) {
		param.serialize(buffer);
		buffer.put(Serialization.encodeVarLong(timeStamp));
	}
}