package com.taobao.oceanbase.network.mina;

import java.net.SocketAddress;

import org.apache.mina.common.IoSession;

import com.taobao.oceanbase.network.Session;
import com.taobao.oceanbase.network.SessionFactory;

public final class MinaSessionAdapter implements Session {
	private IoSession session;
	private SessionFactory factory;

	protected MinaSessionAdapter(SessionFactory factory) {
		this.factory = factory;
	}

	@SuppressWarnings("unchecked")
	public <T> T commit(byte[] message) {
		return (T) session.write(message);
	}

	public void close() {
		session.close();
		factory.removeSession(this);
	}

	protected Session wrap(IoSession session) {
		if (this.session == null)
			this.session = session;// 为了使每个session绑定不变的对象
		return this;
	}

	public SocketAddress getServer() {
		return session.getRemoteAddress();
	}

	public boolean isConnected() {
		return session.isConnected();
	}
}