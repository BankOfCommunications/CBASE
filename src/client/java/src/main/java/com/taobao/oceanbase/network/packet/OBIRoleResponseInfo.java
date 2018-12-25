package com.taobao.oceanbase.network.packet;

import javax.sql.rowset.serial.SerialArray;

import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.vo.inner.ObResultCode;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class OBIRoleResponseInfo extends BaseResponsePacket {
	
	public OBIRoleResponseInfo(int request,byte [] data) {
		super(PacketCode.OB_GET_OBI_RESPONSE,request,data);
	}

	@Override
	public void decodeBody() {
		this.resultCode = ObResultCode.deserialize(buffer);
		role = Serialization.decodeVarInt(buffer);
	}
	
	public ObResultCode getResultCode() {
		return resultCode;
	}
	
	public int getRole() {
		return role;
	}
	
	private ObResultCode resultCode;
	private int role;
}
