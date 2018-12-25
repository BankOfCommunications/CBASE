package com.taobao.oceanbase.network.packet;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.vo.inner.ObMutator;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class WritePacket extends BasePacket {

	private ObMutator mutator;

	public WritePacket(int request, ObMutator mutator) {
		super(PacketCode.OB_WRITE, request);
		this.mutator = mutator;
	}

	void writeBody(ByteBuffer buffer) {
		mutator.serialize(buffer);
	}

	@Override
	public int getBodyLen() {
		return mutator.getSize();
	}
}