package com.taobao.oceanbase.network.packet;

import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.vo.inner.ObResultCode;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class OBIConfigResponseInfo extends BaseResponsePacket {
	
	public OBIConfigResponseInfo(int request,byte [] data) {
		super(PacketCode.OB_GET_OBI_RESPONSE,request,data);
	}

	@Override
	public void decodeBody() {
		this.resultCode = ObResultCode.deserialize(buffer);
		read_percent = Serialization.decodeVarInt(buffer);
		reserved_1 = Serialization.decodeVarInt(buffer);
		reserved_2 = Serialization.decodeVarLong(buffer);
	}
	
	public ObResultCode getResultCode() {
		return resultCode;
	}
	
	public int getReadPercent() {
		return read_percent;
	}
	
	private ObResultCode resultCode;
	int read_percent;
	int reserved_1;
	long reserved_2;
}
