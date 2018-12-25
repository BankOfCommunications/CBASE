package com.taobao.oceanbase.network.packet;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.vo.inner.PacketCode;

public class GetUpdateServerPacket extends BasePacket {

	public GetUpdateServerPacket(int request) {
		super(PacketCode.OB_GET_UPDATE_SERVER_INFO, request);
	}

	@Override
	public int getBodyLen() {
		return 0;
	}

	@Override
	void writeBody(ByteBuffer buffer) {
	}

}