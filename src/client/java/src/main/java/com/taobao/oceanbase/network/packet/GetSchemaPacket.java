package com.taobao.oceanbase.network.packet;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.vo.inner.PacketCode;

public class GetSchemaPacket extends BasePacket {

	public GetSchemaPacket(int request) {
		super(PacketCode.OB_FETCH_SCHEMA, request);
	}

	@Override
	public int getBodyLen() {
		return 0;
	}

	@Override
	void writeBody(ByteBuffer buffer) {
	}
}
