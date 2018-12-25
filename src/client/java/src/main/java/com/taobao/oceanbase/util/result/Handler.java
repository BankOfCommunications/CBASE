package com.taobao.oceanbase.util.result;

public interface Handler<F, T> {

	T handle(F f);
}