package com.taobao.oceanbase.parsermysql;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.vo.Rowkey;


public class CollectRowKey extends Rowkey {
	ByteBuffer buffer = null;

	public CollectRowKey(long uid, char itype, long tid) throws Exception {
		buffer = ByteBuffer.allocate(17);
		buffer.putLong(uid);
		if (itype == '0') {
			buffer.put((byte) 0);
		} else if (itype == '1') {
			buffer.put((byte) 1);
		} 
		buffer.putLong(tid);
	}

	public CollectRowKey(char itype, long tid) throws Exception {
		buffer = ByteBuffer.allocate(9);
		if (itype == '0') {
			buffer.put((byte) 0);
		} else if (itype == '1') {
			buffer.put((byte) 1);
		} else {
			throw new Exception("itype is invalid");
		}

		buffer.putLong(tid);
	}

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
