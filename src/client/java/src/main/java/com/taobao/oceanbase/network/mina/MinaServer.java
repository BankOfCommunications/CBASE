package com.taobao.oceanbase.network.mina;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.mina.common.IoFuture;
import org.apache.mina.common.IoFutureListener;
import org.apache.mina.common.WriteFuture;

import com.taobao.oceanbase.network.Server;
import com.taobao.oceanbase.network.Session;
import com.taobao.oceanbase.network.SessionFactory;

public class MinaServer extends Server {

	public MinaServer(SocketAddress address, SessionFactory factory,
			int timeout, TimeUnit unit) {
		super(address, factory, timeout, unit);
	}

	@Override
	protected void handle(final Session session, Object writeFuture) {
		((WriteFuture) writeFuture).addListener(new IoFutureListener() {

			public void operationComplete(IoFuture future) {
				WriteFuture wFuture = (WriteFuture) future;
				if (wFuture.isWritten())
					return;
				if (session.isConnected())
					session.close();
			}
		});
	}

}