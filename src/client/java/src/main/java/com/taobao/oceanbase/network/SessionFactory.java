package com.taobao.oceanbase.network;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public abstract class SessionFactory {

	private ConcurrentMap<SocketAddress, Session> cache = new ConcurrentHashMap<SocketAddress, Session>();

	private ReentrantLock lock = new ReentrantLock();

	public Session getSession(Server server, SocketAddress address,
			int timeout, TimeUnit unit) {
		Session session = cache.get(address);
		if (session != null)
			return session;
		lock.lock();
		try {
			session = cache.get(address);
			if (session == null) {
				session = getNewSession(server, address, timeout, unit);
				cache.put(address, session);
			}
			return session;
		} finally {
			lock.unlock();
		}
	}

	abstract protected Session getNewSession(Server server,
			SocketAddress address, int timeout, TimeUnit unit);

	public boolean removeSession(Session session) {
		return cache.remove(session.getServer(), session);
	}
}