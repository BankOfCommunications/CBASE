package com.taobao.oceanbase.network.packet;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.vo.inner.ObHeader;
import com.taobao.oceanbase.vo.inner.PacketCode;

public abstract class BasePacket {

	protected ByteBuffer buffer;
	protected PacketCode pcode;
	private int request;
	private int version = 1;
	private int timeout = 0;
	protected ObHeader obHeader = new ObHeader();

	protected BasePacket(PacketCode pcode, int request) {
		this.pcode = pcode;
		this.request = request;
	}

	private int[] getHead() {
		return new int[] { Const.MAGIC, request, pcode.getCode(), 0 /* lazy_caculate */};
	}

	private void encode() {
		int capacity = getBodyLen();
		if (capacity < 0)
			throw new RuntimeException("getBodyLen method error");
		buffer = ByteBuffer.allocate(capacity + 16 + obHeader.getSize() + 16);
		for (int head : getHead()) {
			buffer.putInt(head);
		}
		buffer.putInt(version);
		buffer.putInt((Const.SYSTEM_CODE & 0xff) << 16 | Const.CLIENT_CODE);
		buffer.putInt((Const.SYSTEM_CODE & 0xff) << 16);
		buffer.putInt(timeout * 1000);// client timeout, conver ms to us
		
		// get body for check
		
		ByteBuffer bodyBuffer = ByteBuffer.allocate(capacity);		
		writeBody(bodyBuffer);
		bodyBuffer.rewind();
		// do check sum
		obHeader.setRecord(bodyBuffer, capacity);
		
		obHeader.serialize(buffer);
		buffer.put(bodyBuffer);
		buffer.putInt(12, buffer.position() - 16);
	}

	abstract int getBodyLen();

	abstract void writeBody(ByteBuffer buffer);

	public ByteBuffer getBuffer() {
		if (buffer == null)
			encode();
		return this.buffer;
	}
	
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getRequest() {
		return request;
	}

}