package com.taobao.oceanbase.network.handler;

import com.taobao.oceanbase.network.Session;

public interface Handler {

	void messageRecive(Session session, Object result);

	public void sessionClosed(Session session);
}