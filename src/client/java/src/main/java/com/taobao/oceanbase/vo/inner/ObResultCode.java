package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.util.Const;

public class ObResultCode {

	private int code;
	private String message;

	ObResultCode(int code, String message) {
		this.code = code;
		this.message = message;
	}

	public static ObResultCode deserialize(ByteBuffer buffer) {
		int code = Serialization.decodeVarInt(buffer);
		String message = Serialization.decodeString(buffer, Const.NO_CHARSET);
		return new ObResultCode(code, message);
	}

	public int getCode() {
		return code;
	}

	public String getMessage() {
		return message;
	}
}