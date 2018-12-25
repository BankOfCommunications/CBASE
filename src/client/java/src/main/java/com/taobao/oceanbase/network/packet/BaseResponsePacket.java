package com.taobao.oceanbase.network.packet;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.vo.inner.PacketCode;

public abstract class BaseResponsePacket extends BasePacket {
	
	private static final Log log = LogFactory.getLog(BaseResponsePacket.class);

	BaseResponsePacket(PacketCode pcode, int request, byte data[]) {
		super(pcode, request);
		buffer = ByteBuffer.wrap(data);
	}

	@Override
	public int getBodyLen() {
		throw new UnsupportedOperationException("response packet unspport");
	}

	@Override
	void writeBody(ByteBuffer buffer) {
		throw new UnsupportedOperationException("response packet unspport");
	}

	public void decode() {
		//log.info("deserialize scanner@" + System.currentTimeMillis());
		decodeHead();
		docheckSum();
		decodeBody();
		//log.info("deserialize end@" + System.currentTimeMillis());
	}

	private void docheckSum() {
		int bodySize = buffer.remaining();
		byte[] bodyBuffer = new byte[bodySize];
		buffer.get(bodyBuffer);
		buffer.position(buffer.position() - bodySize);
		boolean ret = this.obHeader.checkRecord(bodyBuffer);
		if (ret == false) {
			throw new RuntimeException("packet checksum failed");
		}
	}

	private void decodeHead() {
		buffer.getInt();
		buffer.getInt();
		buffer.getLong();
		this.obHeader.deserialize(buffer);
	}

	abstract void decodeBody();
}