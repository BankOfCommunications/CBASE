package com.taobao.oceanbase.network;

import java.net.SocketAddress;

public interface Session {

	<T> T commit(byte[] message);

	SocketAddress getServer();

	boolean isConnected();

	void close();
}