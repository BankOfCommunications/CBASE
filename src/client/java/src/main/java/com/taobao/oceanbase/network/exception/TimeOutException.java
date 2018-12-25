package com.taobao.oceanbase.network.exception;

@SuppressWarnings("serial")
public class TimeOutException extends RuntimeException {
	public TimeOutException(Throwable cause){
		super(cause);
	}
	
	public TimeOutException(String message){
		super(message);
	}

}
