package com.taobao.oceanbase.network.packet;

import com.taobao.oceanbase.vo.inner.ObResultCode;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class WriteResponse extends BaseResponsePacket {

	private ObResultCode resultCode;

	public WriteResponse(PacketCode pcode, int request, byte[] data) {
		super(pcode, request, data);
	}

	public void decodeBody() {
		this.resultCode = ObResultCode.deserialize(buffer);
	}

	public ObResultCode getResultCode() {
		return resultCode;
	}

}