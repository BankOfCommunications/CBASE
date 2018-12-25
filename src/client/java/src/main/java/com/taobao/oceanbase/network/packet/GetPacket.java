package com.taobao.oceanbase.network.packet;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.vo.inner.ObGetParam;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class GetPacket extends BasePacket {

	private ObGetParam param;

	public GetPacket(int request, ObGetParam param) {
		super(PacketCode.OB_GET_REQUEST, request);
		this.param = param;
	}

	void writeBody(ByteBuffer buffer) {
		param.serialize(buffer);
	}

	@Override
	public int getBodyLen() {
		return param.getSize();
	}
}