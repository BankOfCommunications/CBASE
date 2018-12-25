package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.network.codec.Serialization;

public class ObServer {

	private int version;
	private int port;
	private int ipV4;
	private int[] ipV6;

	public ObServer() {
	}

	public int getPort() {
		return port;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getIpV4() {
		return ipV4;
	}

	public void setIpV4(int ipV4) {
		this.ipV4 = ipV4;
	}

	public int[] getIpV6() {
		return ipV6;
	}

	public void setIpV6(int[] ipV6) {
		this.ipV6 = ipV6;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void deserialize(ByteBuffer buffer) {
		int version = Serialization.decodeVarInt(buffer);
		this.version = version;
		this.port = Serialization.decodeVarInt(buffer);
		if (version == 4)
			ipV4 = Serialization.decodeVarInt(buffer);
		if (version == 6) {
			ipV6 = new int[4];
			for (int index = 0; index < ipV6.length; index++) {
				ipV6[index] = Serialization.decodeVarInt(buffer);
			}
		}
	}
}