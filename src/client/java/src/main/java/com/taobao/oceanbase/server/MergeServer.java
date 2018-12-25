package com.taobao.oceanbase.server;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.network.Server;
import com.taobao.oceanbase.network.exception.EagainException;
import com.taobao.oceanbase.network.packet.BasePacket;
import com.taobao.oceanbase.network.packet.GetPacket;
import com.taobao.oceanbase.network.packet.ObScanRequest;
import com.taobao.oceanbase.network.packet.ResponsePacket;

import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;

import com.taobao.oceanbase.vo.inner.ObGetParam;
import com.taobao.oceanbase.vo.inner.ObReadParam;
import com.taobao.oceanbase.vo.inner.ObRow;
import com.taobao.oceanbase.vo.inner.ObScanParam;

public class MergeServer {
	
	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(MergeServer.class);

	private Server server;
	private AtomicInteger request = new AtomicInteger(0);

	public MergeServer(Server server) {
		this.server = server;
	}
	
	public Result<List<ObRow>> get(ObGetParam getParam) {
		BasePacket packet = new GetPacket(getRequest(), getParam);
		ResponsePacket response = server.commit(packet);
		List<ObRow> rows = response.getScanner().getRows();
		ResultCode code = ResultCode.getResultCode(response.getResultCode());
		if (code == ResultCode.OB_NOT_MASTER)
			throw new EagainException("consistey read not on master,clean cache and try again");
		return new Result<List<ObRow>>(code, rows);
	}

	public Result<List<ObRow>> scan(ObScanParam param) {
		BasePacket packet = new ObScanRequest(getRequest(), param);
		ResponsePacket response = server.commit(packet);
		ResultCode code = ResultCode.getResultCode(response.getResultCode());
		if (code == ResultCode.OB_NOT_MASTER)
			throw new EagainException("consistey read not on master,clean cache and try again");
		Result<List<ObRow>> result =  new Result<List<ObRow>>(code, response.getScanner().getRows());
		return result;
	}
	
	public <T extends ObReadParam>Result<List<ObRow>> query(T param) {
		if (param instanceof ObGetParam)
			return get((ObGetParam)param);
		else
			return scan((ObScanParam)param);
	}

	private int getRequest() {
		int seq = request.incrementAndGet();
		return seq != 0 ? seq : getRequest();
	}

	public int hashCode() {
		return server.getAddress().hashCode();
	}

	public boolean equals(Object server) {
		if (server != null && server instanceof Server) {
			return this.hashCode() == server.hashCode();
		}
		return false;
	}

	protected SocketAddress getAddress() {
		return server.getAddress();
	}
	
	@Override
	public String toString() {
		return server.toString();
	}
}