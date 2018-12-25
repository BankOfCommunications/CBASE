package com.taobao.oceanbase.network.packet;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.vo.inner.PacketCode;

/**
 * command packet, this packet send a command to server (base on the packet
 * code), the body is empty
 * 
 * @author ruohai
 */
public class CommandPacket extends BasePacket {

	public CommandPacket(PacketCode pcode, int request) {
		super(pcode, request);
	}

	@Override
	int getBodyLen() {
		return 0;
	}

	@Override
	void writeBody(ByteBuffer buffer) {
	}

}
