package com.taobao.oceanbase.network.packet;

import com.taobao.oceanbase.vo.inner.ObResultCode;
import com.taobao.oceanbase.vo.inner.PacketCode;
import com.taobao.oceanbase.vo.inner.schema.ObSchemaManagerV2;

public class GetSchemaResponsePacket extends BaseResponsePacket {
	private ObSchemaManagerV2 schema;
	private ObResultCode resultCode;

	public GetSchemaResponsePacket(int request, byte[] data) {
		super(PacketCode.OB_FETCH_SCHEMA_RESPONSE, request, data);
	}

	@Override
	public void decodeBody() {
		this.resultCode = ObResultCode.deserialize(buffer);
		schema = new ObSchemaManagerV2();
		schema.deserialize(buffer);
	}

	public ObSchemaManagerV2 getSchema() {
		return schema;
	}

	public ObResultCode getResultCode() {
		return resultCode;
	}
}
