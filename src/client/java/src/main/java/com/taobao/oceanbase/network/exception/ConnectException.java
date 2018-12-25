package com.taobao.oceanbase.network.exception;

@SuppressWarnings("serial")
public class ConnectException extends RuntimeException {

	public ConnectException(Throwable cause) {
		super(cause);
	}

	public ConnectException(String message) {
		super(message);
	}

}