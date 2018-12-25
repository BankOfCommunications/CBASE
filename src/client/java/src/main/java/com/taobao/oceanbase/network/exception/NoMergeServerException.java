package com.taobao.oceanbase.network.exception;

@SuppressWarnings("serial")
public class NoMergeServerException extends RuntimeException {

	public NoMergeServerException(Throwable cause) {
		super(cause);
	}

	public NoMergeServerException(String message) {
		super(message);
	}

}
