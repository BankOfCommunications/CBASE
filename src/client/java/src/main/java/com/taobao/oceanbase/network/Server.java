package com.taobao.oceanbase.network;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.taobao.oceanbase.network.exception.TimeOutException;
import com.taobao.oceanbase.network.packet.BasePacket;
import com.taobao.oceanbase.network.packet.BaseResponsePacket;

/**
 * implements an immutable server object
 */
public abstract class Server {
	private SocketAddress address;
	private SessionFactory sessionFactory;
	private int timeout;
	private TimeUnit unit;
	private ConcurrentMap<Integer, BlockingQueue<BaseResponsePacket>> results = new ConcurrentHashMap<Integer, BlockingQueue<BaseResponsePacket>>();

	public Server(SocketAddress address, SessionFactory factory, int timeout,
			TimeUnit unit) {
		this.address = address;
		this.sessionFactory = factory;
		this.timeout = timeout;
		this.unit = unit;
	}

	abstract protected void handle(Session session, Object object);

	public SocketAddress getAddress() {
		return address;
	}

	public void addToResults(Integer requestId, BaseResponsePacket response) {
		try {
			BlockingQueue<BaseResponsePacket> queue = results.get(requestId);
			if (queue != null) 
				queue.put(response);
		} catch (Exception e) {
			throw new RuntimeException("addToResults fail", e);
		}
	}

	public <T extends BaseResponsePacket> T commit(BasePacket packet) {
		try {
			BlockingQueue<BaseResponsePacket> queue = new ArrayBlockingQueue<BaseResponsePacket>(1);
			results.put(packet.getRequest(), queue);
			Session session = sessionFactory.getSession(this, address, timeout,
					unit);
			packet.setTimeout(timeout); // set timeout, should before getBuffer
			ByteBuffer bb = packet.getBuffer();
			//System.out.println("start commit@" + System.currentTimeMillis());
			this.handle(session, session.commit(bb.array()));
			//System.out.println("end commit@" + System.currentTimeMillis());
			BaseResponsePacket result = queue.poll(timeout, unit);
			//System.out.println("get response@" + System.currentTimeMillis());
			if (result == null) {
				//session.close();
				//System.out.println("get null response");
				throw new TimeOutException("timeout: " + timeout + ", server: " + toString());
			}
			result.decode();
			return (T) result;
		} catch (InterruptedException e) {
			throw new RuntimeException("error", e);
		} finally {
			results.remove(packet.getRequest());
		}
	}

	public String toString() {
		return address.toString();
	}
}