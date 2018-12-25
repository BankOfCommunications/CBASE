package com.taobao.oceanbase.network.mina;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

public class MinaEncoder extends ProtocolEncoderAdapter {

	public void encode(IoSession session, Object message,
			ProtocolEncoderOutput out) throws Exception {
		if (!(message instanceof byte[])) {
			throw new Exception("must send byte[]");
		}
		byte[] payload = (byte[]) message;
		ByteBuffer buf = ByteBuffer.wrap(payload);
		out.write(buf);
	}

}