package com.taobao.oceanbase.network.handler;

import com.taobao.oceanbase.network.Server;
import com.taobao.oceanbase.network.Session;
import com.taobao.oceanbase.network.packet.BaseResponsePacket;

public class HandlerImpl implements Handler {

	private Server server;

	public HandlerImpl(Server server) {
		this.server = server;
	}

	public void messageRecive(Session session, Object result) {
		BaseResponsePacket packet = (BaseResponsePacket) result;
		server.addToResults(packet.getRequest(), packet);
	}

	public void sessionClosed(Session session) {
		session.close();
	}
}