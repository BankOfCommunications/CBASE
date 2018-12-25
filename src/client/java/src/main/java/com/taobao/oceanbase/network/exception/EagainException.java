package com.taobao.oceanbase.network.exception;


@SuppressWarnings("serial")
public class EagainException extends RuntimeException {
	public EagainException(Throwable cause){
		super(cause);
	}
	
	public EagainException(String message){
		super(message);
	}

}