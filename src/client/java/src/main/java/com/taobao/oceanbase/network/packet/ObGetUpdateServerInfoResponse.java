package com.taobao.oceanbase.network.packet;

import com.taobao.oceanbase.vo.inner.ObResultCode;
import com.taobao.oceanbase.vo.inner.ObServer;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class ObGetUpdateServerInfoResponse extends BaseResponsePacket {

	private ObServer server;
	private ObResultCode resultCode;

	public ObGetUpdateServerInfoResponse(int request, byte[] data) {
		super(PacketCode.OB_GET_UPDATE_SERVER_INFO_RES, request, data);
		System.err.println("create response for get ups");
	}

	public void decodeBody() {
		this.resultCode = ObResultCode.deserialize(buffer);
		this.server = new ObServer();
		this.server.deserialize(buffer);
	}

	public ObResultCode getResultCode() {
		return resultCode;
	}

	public ObServer getServer() {
		return server;
	}

}