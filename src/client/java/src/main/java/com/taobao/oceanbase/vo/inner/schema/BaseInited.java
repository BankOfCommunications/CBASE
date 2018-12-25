package com.taobao.oceanbase.vo.inner.schema;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.network.codec.Serialization;

public class BaseInited {

	private boolean inited;
	
	public void setFlag() {
		inited = true;
	}
	
	public boolean hasInited() {
		return inited;
	}
	
	public void deserialize(ByteBuffer buffer) {
		inited = Serialization.decodeBoolean(buffer);
	}
}
