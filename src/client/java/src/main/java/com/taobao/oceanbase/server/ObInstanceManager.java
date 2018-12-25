package com.taobao.oceanbase.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.network.Server;
import com.taobao.oceanbase.network.SessionFactory;
import com.taobao.oceanbase.network.exception.ConnectException;
import com.taobao.oceanbase.network.exception.TimeOutException;
import com.taobao.oceanbase.network.packet.CommandPacket;
import com.taobao.oceanbase.network.packet.OBIRoleResponseInfo;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.util.Helper;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.ResultCode;

import com.taobao.oceanbase.vo.inner.ObGetParam;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class ObInstanceManager {
	static  long EXPIRED_TIME = 5000;
	class ObInstance {
		public ObInstance(String a,int p) {
			ip = a;
			port = p;
		}
		public ObInstance(String a,int p,int v) {
			ip = a;
			port = p;
			version = v;
		}
		public void setReadPercent(int percent) {
			read_percent = percent;
		}
		public int getReadPercent() {
			return read_percent;
		}
		public int getReadLowInterval() {
			return read_low;
		}
		
		public int getReadHighInterval() {
			return read_high;
		}
		
		public void setReadInterval(int l,int r) {
			read_low = l;
			read_high = r;
		}
		public void setRootServer(RootServer rs) {
			rootserver = rs;
		}
		public RootServer getRootServer() {
			return rootserver;
		}
		public void setExpiredTime() {
			expired_time = System.currentTimeMillis() + EXPIRED_TIME;
		}
		public long getExpiredTime() {
			return expired_time;
		}
		public String ip;
		public int port;
		public int version;
		private int read_percent;
		private int read_low;
		private int read_high;
		private long expired_time;
		private RootServer rootserver;
	}
	
	private static int MASTER = 0;
	//private static int SLAVE = 1; 
	final public static int VERSION = 2;
	final public static int OLD_VERSION = 1;
	
	private static final Log log = LogFactory.getLog(ObInstanceManager.class);
	private ObInstance master_instance_;
	private List<ObInstance> instance_list_ = new ArrayList<ObInstanceManager.ObInstance>();
	private AtomicInteger request = new AtomicInteger(0);
	private ReentrantLock lock = new ReentrantLock();
	private Map<ObInstance,Server> session_list = new HashMap<ObInstance,Server>();
	public SessionFactory factory;
	private int timeout;
	
	public ObInstanceManager(SessionFactory session,int network_timeout,List<String>instance_list) {
		factory = session;
		timeout = network_timeout;
		for(String addr : instance_list)  {
			log.debug(Helper.getHost(addr) + " " + Helper.getPort(addr) + " " + Helper.getVersion(addr));
			this.instance_list_.add(new ObInstance(Helper.getHost(addr),Helper.getPort(addr),Helper.getVersion(addr)));
		}
		createClusterList();
	}
	
	public RootServer getInstance() {
		if (master_instance_ != null && master_instance_.getExpiredTime() > System.currentTimeMillis()) {
			return master_instance_.getRootServer();
		}
		
		lock.lock();
		try {
			if (master_instance_ != null && master_instance_.getExpiredTime() > System.currentTimeMillis()) {
				return master_instance_.getRootServer();
			}
			
			try {
				if (master_instance_ != null
						&& master_instance_.getRootServer() != null
						&& master_instance_.version != OLD_VERSION 
						&& master_instance_.getRootServer().isMasterInstance()) {
					master_instance_.setExpiredTime();
					return master_instance_.getRootServer();
				}
			} catch (RuntimeException e) {
				log.debug(e);
			}
			ObInstance ins = getMasterInstance(factory,instance_list_);
			if (ins == null) {
				throw new RuntimeException("cann't get master instance");
			} else {
				master_instance_ = ins;
			}
		} finally {
			lock.unlock();
		}
		return master_instance_.getRootServer();
	}
	
	public RootServer getInstance(ObGetParam param) {
		//param check:no check
		if (param.isReadConsistency() == 1) {
			log.debug("get master instance");
			return getInstance();
		} else {
			log.debug("get instance recoding to read percent");
			return getInstance(param, null);
		}
	}
	
	public RootServer getInstance(String rowkey,boolean isReadConsistency,RootServer exclude) {
		if (isReadConsistency) {
			return getInstance();
		} else if (exclude != null) {
			for(ObInstance ins : instance_list_) {
				if (ins.getRootServer() != exclude) 
					return ins.getRootServer();
			}
		} else {
			return getInstance(rowkey);
		}
		return null;
	}
	
	public RootServer getInstance(ObGetParam param,RootServer exclude) {
		if (exclude != null) {
			for(ObInstance ins : instance_list_) {
				if (ins.getRootServer() != exclude) 
					return ins.getRootServer();
			}
		}
		String rk = param.getCells().get((int)(param.getCellsSize() - 1)).getRowKey();
		return getInstance(rk);
	}
	
	public RootServer getInstance(QueryInfo param) {
		if (param.isReadConsistency()) {
			return getInstance();
		} else {
			return getInstance(param, null);
		}
	}
	
	public RootServer getInstance(QueryInfo param,RootServer exclude) {
		if (exclude != null) {
			for(ObInstance ins : instance_list_) {
				if (ins.getRootServer() != exclude) 
					return ins.getRootServer();
			}
		}
		String rk = param.getStartKey();
		return getInstance(rk);
	}
	
	private RootServer getInstance(String rk) {
		int v = rk.hashCode();
		v %= 100;
		if (v < 0) {
			v = -v;
		}
		for(ObInstance ins : instance_list_) {
			if (ins.getExpiredTime() < System.currentTimeMillis()) {
				getReadPercent();
			}
			if (ins.getReadLowInterval() <= v && ins.getReadHighInterval() >= v) {
				log.debug("will route to :" + ins.ip + ":" + ins.port + " " + ins.getReadLowInterval() + ":"  + ins.getReadHighInterval() + ":" + v);
				return ins.getRootServer();
			}
		}
		//读比例设置错误，随机挑选一个
		return instance_list_.get(v % instance_list_.size()).getRootServer();
	}
	
	public void removeCache() {
		master_instance_ = null;
	}
	
	private ObInstance getMasterInstance(SessionFactory session,List<ObInstance> instance_list) {
		ObInstance ret = null;
		ObInstance old_instance = null;
		for(ObInstance ins : instance_list) {
			try {
				if (ins.version == OLD_VERSION) {
					old_instance = ins;
				}else if (isMasterInstance(ins)){
					ret = ins;
					break;
				}
			} catch (RuntimeException e) {
				//can't decide
				old_instance = null;
				continue;
			}
		}
		
		if (ret == null) {
			ret = old_instance;
		}
		return ret;
	}
	
	private boolean isMasterInstance(ObInstance instance) {
		boolean ret = false;
		if (instance == null){
			ret = false;
		} else {
			try {
				RootServer root = instance.getRootServer();
				if (root == null) {
					root = createCluster(instance);
					instance.setRootServer(root);
					instance.setExpiredTime();
				}
				if (root != null) {
					ret = root.isMasterInstance();
				}
			} catch (RuntimeException e) {
				ret = false;
			}
		}
		return ret;
	}
	
	
	private void createClusterList() {
		for (ObInstance instance : instance_list_) {
			try {
				if (instance.getRootServer() == null)
					instance.setRootServer(createCluster(instance));
			} catch (RuntimeException e) {
				continue;
			}
		}
	}
	
	private RootServer createCluster(ObInstance instance) {
		RootServer ret = null;
		if  (instance != null) {
			Server tmp_server = session_list.get(instance);
			if (tmp_server == null) {
				tmp_server = Helper.getServer(factory, instance.ip,
						instance.port, timeout);
				session_list.put(instance, tmp_server);
			}
			ret = new RootServer(tmp_server);
		}
		return ret;
	}
	
	private void getReadPercent() {
		int tmp = 0;
		int old = 0;
		for(ObInstance ins : instance_list_) {
			if (ins.version == OLD_VERSION) {
				log.debug("have old version");
				continue;
			}
			if (ins.getRootServer() == null)
				continue;
			try {
				tmp = ins.getRootServer().getReadPercent();
				ins.setReadPercent(tmp);
				ins.setReadInterval(old,(old + tmp) > 100 ? 100 : (old + tmp));
				log.debug("read percent:" + ins.ip + ":" + ins.port + " " + old + ":" + (old + tmp));
				old = tmp;
			}catch (RuntimeException e) {
				continue;
			}
		}
		
		for(ObInstance ins : instance_list_) {
			if (ins.version == OLD_VERSION) {
				ins.setReadInterval(tmp,100);
				break;
			}
		}
	}
	private int getRequest() {
		int seq = request.incrementAndGet();
		return seq != 0 ? seq : getRequest();
	}

}
