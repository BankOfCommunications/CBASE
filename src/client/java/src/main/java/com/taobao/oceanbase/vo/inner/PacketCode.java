package com.taobao.oceanbase.vo.inner;

import com.taobao.oceanbase.network.packet.BaseResponsePacket;
import com.taobao.oceanbase.network.packet.GetSchemaResponsePacket;
import com.taobao.oceanbase.network.packet.OBIConfigResponseInfo;
import com.taobao.oceanbase.network.packet.OBIRoleResponseInfo;
import com.taobao.oceanbase.network.packet.ObGetUpdateServerInfoResponse;
import com.taobao.oceanbase.network.packet.ResponsePacket;
import com.taobao.oceanbase.network.packet.WriteResponse;

public enum PacketCode {

	OB_GET_REQUEST(101), OB_GET_RESPONSE(102) {
		protected BaseResponsePacket createPacket(int request, byte[] data) {
			return new ResponsePacket(this, request, data);
		}
	},
	OB_FETCH_SCHEMA(205),OB_FETCH_SCHEMA_RESPONSE(206) {
		protected BaseResponsePacket createPacket(int request, byte[] data) {
			return new GetSchemaResponsePacket(request, data);
		}
	},OB_GET_UPDATE_SERVER_INFO(211), OB_GET_UPDATE_SERVER_INFO_RES(212) {
		protected BaseResponsePacket createPacket(int request, byte[] data) {
			return new ObGetUpdateServerInfoResponse(request, data);
		}
	},
	OB_SCAN_REQUEST(122), OB_SCAN_REPONSE(123) {
		protected BaseResponsePacket createPacket(int request, byte[] data) {
			return new ResponsePacket(this, request, data);
		}
	},
	OB_CLEAR_ACTIVE_MEMTABLE(1239), OB_RESPONSE(1240) {
		protected BaseResponsePacket createPacket(int request, byte[] data) {
			return new WriteResponse(this, request, data);
		}
	},
	OB_WRITE(301), OB_WRITE_RES(302) {
		protected BaseResponsePacket createPacket(int request, byte[] data) {
			return new WriteResponse(this, request, data);
		}
	},
	OB_GET_OBI_ROLE(163),OB_GET_OBI_RESPONSE(164) {
			protected BaseResponsePacket createPacket(int request,byte[] data) {
				return new OBIRoleResponseInfo(request, data);
			}
	},
	
	OB_GET_OBI_CONFIG(173),OB_GET_OBI_CONFIG_RESPONSE(174) {
			protected BaseResponsePacket createPacket(int request, byte[] data) {
				return new OBIConfigResponseInfo(request, data);
			}
	};
	
	private int code;

	public int getCode() {
		return code;
	}

	PacketCode(int code) {
		this.code = code;
	}

	protected BaseResponsePacket createPacket(int request, byte[] data) {
		throw new UnsupportedOperationException("this is a request packet");
	};

	public static BaseResponsePacket createPacket(int pcode, int request, byte data[]) {
		for (PacketCode value : PacketCode.values()) {
			if (pcode == value.getCode()) {
				return value.createPacket(request, data);
			}
		}
		throw new IllegalArgumentException("pcode wrong");
	}
}