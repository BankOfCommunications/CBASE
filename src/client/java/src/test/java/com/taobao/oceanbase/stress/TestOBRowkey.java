package com.taobao.oceanbase.stress;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.vo.Rowkey;

public class TestOBRowkey extends Rowkey {
	ByteBuffer buffer = ByteBuffer.allocate(17);

	public TestOBRowkey(long userId, int itemType, long itemId) {
		buffer.putLong(userId);
		byte type = (itemType == 0) ? (byte) 0 : (byte) 1;
		buffer.put(type);
		buffer.putLong(itemId);
	}

	@Override
	public byte[] getBytes() {
		return buffer.array();
	}

	public String toString() {
		String ret = null;
		try {
			ret = new String(buffer.array(), Const.NO_CHARSET);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return ret;
	}

}
