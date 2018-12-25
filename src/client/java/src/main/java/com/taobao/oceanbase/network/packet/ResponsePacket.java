package com.taobao.oceanbase.network.packet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.vo.inner.ObResultCode;
import com.taobao.oceanbase.vo.inner.ObScanner;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class ResponsePacket extends BaseResponsePacket {
	private static final Log log = LogFactory.getLog(ResponsePacket.class);
	private ObScanner scanner;
	private ObResultCode resultCode;

	public ResponsePacket(PacketCode pcode, int request, byte[] data) {
		super(pcode, request, data);
	}

	public void decodeBody() {
		this.resultCode = ObResultCode.deserialize(buffer);
		log.debug("resultCode : " + resultCode.getCode() + " for " + this.pcode);
		scanner = new ObScanner();

		scanner.deserialize(buffer);

	}

	public ObScanner getScanner() {
		return scanner;
	}

	public ObResultCode getResultCode() {
		return resultCode;
	}
}