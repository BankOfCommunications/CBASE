package com.taobao.oceanbase.network.mina;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.IoHandler;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.RuntimeIOException;
import org.apache.mina.common.ThreadModel;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.SocketConnector;
import org.apache.mina.transport.socket.nio.SocketConnectorConfig;

import com.taobao.oceanbase.network.Server;
import com.taobao.oceanbase.network.Session;
import com.taobao.oceanbase.network.SessionFactory;
import com.taobao.oceanbase.network.exception.ConnectException;
import com.taobao.oceanbase.network.exception.TimeOutException;
import com.taobao.oceanbase.network.handler.Handler;
import com.taobao.oceanbase.network.handler.HandlerImpl;
import com.taobao.oceanbase.util.NamedThreadFactory;

public class MinaSessionFactory extends SessionFactory {
	private static final Log log = LogFactory.getLog(MinaSessionFactory.class);
	private final SocketConnector ioConnector;

	{
		int processorCount = Runtime.getRuntime().availableProcessors() + 1;
		ioConnector = new SocketConnector(processorCount,
				Executors.newCachedThreadPool(new NamedThreadFactory(
						"OCEANBASE")));
	}

	public Session getNewSession(final Server server, SocketAddress address,
			int timeout, TimeUnit unit) {
		SocketConnectorConfig config = new SocketConnectorConfig();
		config.getSessionConfig().setTcpNoDelay(true);
		config.setThreadModel(ThreadModel.MANUAL);
		config.setConnectTimeout((int) unit.toSeconds(timeout));
		final MinaSessionAdapter minaSession = new MinaSessionAdapter(
				MinaSessionFactory.this);
		IoHandler proxyHandler = new IoHandlerAdapter() {
			private Handler handler = new HandlerImpl(server);

			public void messageReceived(IoSession session, Object message)
					throws Exception {
				handler.messageRecive(minaSession, message);
			}

			public void exceptionCaught(IoSession session, Throwable cause)
					throws Exception {
				log.error("session throws exception " + cause);
				if (!(cause instanceof IOException)) {
					minaSession.close();
				}
			}

			public void sessionClosed(IoSession session) throws Exception {
				minaSession.close();
			}
		};
		config.getFilterChain().addLast("serialize",
				new ProtocolCodecFilter(new MinaEncoder(), new MinaDecoder()));
		ConnectFuture connectFuture = ioConnector.connect(address,
				proxyHandler, config);
		boolean ret = connectFuture.join(unit.toMillis(timeout));
		if (!ret){
			throw new TimeOutException("get connect to " + address + "timeout");
		} else {
			try {
				IoSession ioSession = connectFuture.getSession();
				return minaSession.wrap(ioSession);
			} catch (RuntimeIOException e) {
				throw new ConnectException("get connection to " + address
						+ " fails");
			}
		}
	}
}