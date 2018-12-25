package com.taobao.oceanbase.server;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.oceanbase.network.exception.NoMergeServerException;
import com.taobao.oceanbase.network.exception.TimeOutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.network.Server;
import com.taobao.oceanbase.network.SessionFactory;
import com.taobao.oceanbase.network.exception.ConnectException;
import com.taobao.oceanbase.network.packet.BasePacket;
import com.taobao.oceanbase.network.packet.CommandPacket;
import com.taobao.oceanbase.network.packet.GetPacket;
import com.taobao.oceanbase.network.packet.GetSchemaPacket;
import com.taobao.oceanbase.network.packet.GetSchemaResponsePacket;
import com.taobao.oceanbase.network.packet.GetUpdateServerPacket;
import com.taobao.oceanbase.network.packet.OBIConfigResponseInfo;
import com.taobao.oceanbase.network.packet.OBIRoleResponseInfo;
import com.taobao.oceanbase.network.packet.ObGetUpdateServerInfoResponse;
import com.taobao.oceanbase.network.packet.ObScanRequest;
import com.taobao.oceanbase.network.packet.ResponsePacket;
import com.taobao.oceanbase.server.ObInstanceManager.ObInstance;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.util.Helper;
import com.taobao.oceanbase.util.NavigableCache;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.inner.ObBorderFlag;
import com.taobao.oceanbase.vo.inner.ObCell;
import com.taobao.oceanbase.vo.inner.ObGetParam;
import com.taobao.oceanbase.vo.inner.ObRange;
import com.taobao.oceanbase.vo.inner.ObRow;
import com.taobao.oceanbase.vo.inner.ObScanParam;
import com.taobao.oceanbase.vo.inner.ObServer;
import com.taobao.oceanbase.vo.inner.PacketCode;
import com.taobao.oceanbase.vo.inner.schema.ObSchemaManagerV2;

public class RootServer {

	private static final Log log = LogFactory.getLog(RootServer.class);
	private Server server;
	private UpdateServer updateServer;
	private Random random = new Random();
	private ReentrantLock lock = new ReentrantLock();
	private AtomicInteger request = new AtomicInteger(0);
	private ConcurrentHashMap<String,NavigableCache<String, ObRow>> tabletsCache = new ConcurrentHashMap<String,NavigableCache<String, ObRow>>();
	private ConcurrentMap<SocketAddress, MergeServer> normals = new ConcurrentHashMap<SocketAddress, MergeServer>();
	private ConcurrentMap<SocketAddress, InvalidMergeServer> invalids = new ConcurrentHashMap<SocketAddress, InvalidMergeServer>();
	
	public RootServer(Server server) {
		this.server = server;
		random.setSeed(System.currentTimeMillis());
	}

	public UpdateServer getUpdateServer(SessionFactory factory, int timeout) {
		if (updateServer != null) {
			//log.debug("use cached updateserver: " + updateServer);
			return updateServer;
		}
		lock.lock();
		try {
			if (updateServer != null)
				return updateServer;
			BasePacket packet = new GetUpdateServerPacket(request
					.incrementAndGet());
			ObGetUpdateServerInfoResponse response = null;
			try {
				response = server.commit(packet);
			} catch (ConnectException e) {
				Helper.sleep(Const.MIN_TIMEOUT);
				throw new TimeOutException("get updateserver timeout" + e);
			}
			if (!(ResultCode.OB_SUCCESS == ResultCode.getResultCode(response
					.getResultCode())))
				throw new RuntimeException("get update server error");
			ObServer server = response.getServer();
			updateServer = new UpdateServer(Helper.getServer(factory, Helper
					.toIP(server.getIpV4()), server.getPort(), timeout));
			log.info("got new updateserver [" + updateServer
					+ "] from rootserver");
			return updateServer;
		} finally {
			lock.unlock();
		}
	}
	
	public Result<ObSchemaManagerV2> getSchema(SessionFactory factory, int timeout) {
		BasePacket packet = new GetSchemaPacket(request.incrementAndGet());
		GetSchemaResponsePacket response = null;
		try {
			response = server.commit(packet);
		}catch (ConnectException e){
			Helper.sleep(Const.MIN_TIMEOUT);
			throw new TimeOutException("get schema timeout" + e);
		}
		ResultCode code = ResultCode.getResultCode(response.getResultCode());
		if(!(ResultCode.OB_SUCCESS == code)) {
			throw new RuntimeException("get schema error: " + response.getResultCode());
		}
		return new Result<ObSchemaManagerV2>(code, response.getSchema());
	}

	public MergeServer getMergeServer(SessionFactory factory, String table,
			String rowkey, int timeout) {
		NavigableCache<String, ObRow> tablets = tabletsCache.get(table);
		if (tablets == null) {
			tabletsCache.putIfAbsent(table, new NavigableCache<String, ObRow>(
					1000));
			tablets = tabletsCache.get(table);
		}
		ObRow tablet = tablets.get(rowkey);
		try {
			if (tablet == null || tablet.isFull() == false) {
				tablet = getTablet(table, rowkey);
			} else {
				log.debug("tablet cache hit");
			}
		} catch (RuntimeException e) {
			tablet = null;
		}
		MergeServer target = null;
		List<MergeServer> servers = getMergeServer(factory, tablet, timeout);
		if (servers.size() == 0) {
			tablets.removeIfExpired(rowkey);
			// throw new
			// NoMergeServerException("can not find valid mergeserver");
		} else {
			int index = tablet.incrAndGetIndex() % servers.size();
			target = servers.get(index);
			log.debug("request will route to mergeserver: " + target);
		}
		return target;
	}
	
	public Result<List<ObRow>> scanTabletList(String table, QueryInfo query) {
		boolean start = query.isInclusiveStart();
		boolean end = query.isInclusiveEnd();
		boolean min = query.isMinValue();
		boolean max = query.isMaxValue();
		ObRange range = new ObRange(query.getStartKey(), query.getEndKey(),
				new ObBorderFlag(start, end, min, max));
		ObScanParam param = new ObScanParam();
		param.setReadConsistency(false);
		param.set(table, range);
		if (query.getPageNum() == 0) {
			param.setLimit(0, query.getLimit());
		} else {
			param.setLimit(query.getPageSize() * (query.getPageNum() - 1),
					Math.min(query.getLimit(), query.getPageSize()));
		}
		if (query.getFilter() != null) {
			if (query.getFilter().isValid()) {
				param.setFilter(query.getFilter());
			} else {
				throw new IllegalArgumentException("condition is invalid");
			}
		}
		param.setScanDirection(query.getScanFlag());
//		param.setGroupByParam(query.getGroupbyParam());
		BasePacket packet = new ObScanRequest(request.incrementAndGet(), param);
		ResponsePacket response = null;
		try {
			response = server.commit(packet);
		}catch (ConnectException e){
			Helper.sleep(Const.MIN_TIMEOUT);
			throw new TimeOutException("get tablets timeout" + e);
		}
		ResultCode code = ResultCode.getResultCode(response.getResultCode());
		return new Result<List<ObRow>>(code, response.getScanner().getRows());
	}

	private List<MergeServer> getMergeServer(SessionFactory factory, ObRow tablet, int timeout) {
		List<MergeServer> servers = new ArrayList<MergeServer>();
		for (int index = 1; index < 4 && tablet != null; index++) {
			Long ip = tablet.getValueByColumnName(index + "_ipv4");
			Long port = tablet.getValueByColumnName(index + "_ms_port");
			if (ip != null && ip != 0 && port != null && port != 0) {
				log.debug("get mergeserver:" + Helper.toIP(ip.intValue()) + ":" + port.intValue());
				SocketAddress key = new InetSocketAddress(Helper.toIP(ip
						.intValue()), port.intValue());
				if (!invalids.containsKey(key)) {
					if (!normals.containsKey(key))
						normals.putIfAbsent(
								key,
								new MergeServer(Helper.getServer(factory,
										Helper.toIP(ip.intValue()),
										port.intValue(), timeout)));
				} else {
					InvalidMergeServer invalid = invalids.get(key);
					long interval = System.currentTimeMillis() - invalid.time;
					if (interval < Const.INVALID_RETRY_TIME)
						continue;
					log.debug("invalid server enabled at " + System.currentTimeMillis());
					invalids.remove(key);
					normals.putIfAbsent(key, invalid.server);
				}
				servers.add(normals.get(key));
			}
		}
		return servers;
	}

	private ObRow getTablet(String table, String rowkey) {
		List<ObRow> tablets = getTablets(table, rowkey);
		if (tablets == null || tablets.size() < 2)
			throw new RuntimeException("tablets returned by rootserver are null");
		for (int i=0; i<tablets.size(); ++i) {
			ObRow endkey = tablets.get(i);
			String key = endkey.getRowKey();
			if (i == 0) {
				// first one
				ObRow oldKey = tabletsCache.get(table).get(key);
				if (oldKey != null && oldKey.isFull()) {
					// if exist, and is full, skip
					continue;
				} else {
					tabletsCache.get(table).put(key, endkey);
				}
			} else {
				endkey.setFull(true);
				tabletsCache.get(table).put(key, endkey);
			}
		}
		return tablets.get(1);
	}

	private List<ObRow> getTablets(String table, String rowkey) {
		ObGetParam param = new ObGetParam(1);
		param.addCell(new ObCell(table, rowkey, "*", null));
		param.setReadConsistency(false);
		GetPacket packet = new GetPacket(request.incrementAndGet(), param);
		ResponsePacket response = null;
		try {
			response = server.commit(packet);
		}catch (ConnectException e){
			Helper.sleep(Const.MIN_TIMEOUT);
			throw new TimeOutException("get tablets" + e);
		}
		List<ObRow> rows = response.getScanner().getRows();
		log.debug("query tablet info from rootserver:" + rows.size());
		for (ObRow row : rows) {
			log.debug(row.toString());
		}
		return rows;
	}

	public void remove(String rowkey, MergeServer server) {
		normals.remove(server.getAddress());
		invalids.putIfAbsent(server.getAddress(),
				new InvalidMergeServer(server));
	}

	private static class InvalidMergeServer {
		long time;
		MergeServer server;

		InvalidMergeServer(MergeServer server) {
			this.server = server;
			this.time = System.currentTimeMillis();
			log.debug(server + " invalid at " + time);
		}
	}
	
	
	public boolean isMasterInstance() {
		boolean ret = false;
		CommandPacket packet = new CommandPacket(PacketCode.OB_GET_OBI_ROLE,
				request.incrementAndGet());
		
		OBIRoleResponseInfo response = null;
		try {
			response = server.commit(packet);
		} catch (ConnectException e) {
			Helper.sleep(Const.MIN_TIMEOUT);
			throw new TimeOutException("get role info timeout" + e);
		}
		if (!(ResultCode.OB_SUCCESS == ResultCode.getResultCode(response
				.getResultCode())))
			throw new RuntimeException("get role info  error");
		int role = response.getRole();

		if (role == 0) {
			log.info("is master instance ");
			ret = true;
		} else {
			log.debug("this instance is not a master instance ");
		}
		return ret;
	}
	
	public int getReadPercent() {
		CommandPacket packet = new CommandPacket(PacketCode.OB_GET_OBI_CONFIG,
				request.incrementAndGet());
		
		OBIConfigResponseInfo response = null;
		try {
			response = server.commit(packet);
		} catch (ConnectException e) {
			Helper.sleep(Const.MIN_TIMEOUT);
			throw new TimeOutException("get role info timeout" + e);
		}
		if (!(ResultCode.OB_SUCCESS == ResultCode.getResultCode(response
				.getResultCode())))
			throw new RuntimeException("get role info  error");
		return response.getReadPercent();
	}
	
	public String toString(){
		return tabletsCache.toString();
	}
	
}