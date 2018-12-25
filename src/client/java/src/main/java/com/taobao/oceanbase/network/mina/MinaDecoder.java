package com.taobao.oceanbase.network.mina;

import java.io.IOException;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

import com.taobao.oceanbase.network.packet.BaseResponsePacket;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class MinaDecoder extends CumulativeProtocolDecoder {

	@Override
	protected boolean doDecode(IoSession session, ByteBuffer in,
			ProtocolDecoderOutput out) throws Exception {
		final int origonPos = in.position();
		if (in.remaining() < 16) {
			in.position(origonPos);
			return false;
		}
		int magic = in.getInt();
		if (magic != Const.MAGIC)
        	throw new IOException("flag error, except: " + Const.MAGIC + ", but get " + magic);
		int request = in.getInt();
		int pcode = in.getInt();
		int len = in.getInt();
		if (in.remaining() < len) {
			in.position(origonPos);
			return false;
		}
		byte[] data = new byte[len];
		in.get(data);
		BaseResponsePacket packet = PacketCode.createPacket(pcode, request, data);
		out.write(packet);
		return true;
	}

}