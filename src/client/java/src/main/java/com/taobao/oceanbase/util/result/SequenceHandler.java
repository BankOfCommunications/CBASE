package com.taobao.oceanbase.util.result;

/**
 * 
 * ���handler,˳���ִ������handler
 */
public class SequenceHandler<F, T, M> implements Handler<F, T> {

	private Handler<F, M> a;
	private Handler<M, T> b;

	public SequenceHandler(Handler<F, M> a, Handler<M, T> b) {
		this.a = a;
		this.b = b;
	}

	public T handle(F resultSet) {
		return b.handle(a.handle(resultSet));
	}
}