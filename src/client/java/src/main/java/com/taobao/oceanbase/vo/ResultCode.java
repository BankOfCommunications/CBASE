package com.taobao.oceanbase.vo;

import com.taobao.oceanbase.vo.inner.ObResultCode;

public enum ResultCode {

	OB_ERROR(1, "unknow error"),
	OB_SUCCESS(0, "success"), 
	OB_INVALID_ARGUMENT(-2,"invalid argument"), 
	OB_NOT_INIT(-6, "server not init"),
	OB_NOT_SUPPORTED(-7,"only ob semantic is supported"), 
	OB_ERROR_FUNC_VERSION(-10, "api version not match"), 
	OB_MEM_OVERFLOW(-14,"mem overflow"), 
	OB_ERR_UNEXPECTED(-16, "get schema eorror"), 
	OB_SCHEMA_ERROR(-29, "request not conform to schema"),
	OB_DATA_NOT_SERVE(-30,"not host this data"),
	OB_NOT_MASTER(-38,"write or consistency read not on master instance"),
	OB_ERROR_OUT_OF_RANGE(-3003, "out of range"); 

	private int code;

	private String message;

	ResultCode(int code, String message) {
		this.code = code;
		this.message = message;
	}

	public String getMessage() {
		return this.message;
	}

	public static ResultCode getResultCode(ObResultCode code) {
		if (code == null)
			throw new IllegalArgumentException("ObResultCode null");
		for (ResultCode value : ResultCode.values()) {
			if (value.code == code.getCode()) {
				return value;
			}
		}
		return OB_ERROR;
	}

	@Override
	public String toString() {
		return "[" + code + ", " + message + "]";
	}
}