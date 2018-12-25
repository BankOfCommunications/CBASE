package com.taobao.oceanbase.util;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.network.Server;
import com.taobao.oceanbase.network.SessionFactory;
import com.taobao.oceanbase.network.mina.MinaServer;
import com.taobao.oceanbase.server.ObInstanceManager;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.inner.ObCell;
import com.taobao.oceanbase.vo.inner.ObRow;

public class Helper {
	private static final Log log = LogFactory.getLog(Helper.class);
	public static String toIP(int value) {
		StringBuilder host = new StringBuilder(16);
		host.append((value & 0xff)).append('.');
		host.append(((value >> 8) & 0xff)).append('.');
		host.append(((value >> 16) & 0xff)).append('.');
		host.append(((value >> 24) & 0xff));
		return host.toString();
	}

	public static Server getServer(SessionFactory factory, String ip, int port, int timeout) {
		if(timeout < Const.MIN_TIMEOUT){
			timeout = Const.MIN_TIMEOUT;
			log.info("timeout is too small,change to MIN_TIMEOUT:" + Const.MIN_TIMEOUT);
		}
		return new MinaServer(new InetSocketAddress(ip, port), factory, timeout,
				TimeUnit.MILLISECONDS);
	}

	public static String toHex(byte[] raw) {
		final String HEXES = "0123456789ABCDEF";
		if (raw == null) {
			return null;
		}
		final StringBuilder hex = new StringBuilder(2 * raw.length);
		for (final byte b : raw) {
			hex.append(HEXES.charAt((b & 0xF0) >> 4))
					.append(HEXES.charAt((b & 0x0F))).append(" ");
		}
		return hex.toString();
	}

	public static RowData getRowData(ObRow row) {
		try {
			RowData data = new RowData(row.getTableName(), row.getRowKey(), row
					.getColumns().size());
			for (ObCell cell : row.getColumns()) {
				Object value = cell.getValue();
				if (value != null && value instanceof String) {
					value = new String(value.toString().getBytes(
							Const.NO_CHARSET), Const.DEFAULT_CHARSET);
				}
				data.addData(cell.getColumnName(), value);
			}
			return data;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);// never happen
		}
	}
	
   public static void sleep(int msec) {
		try {
			Thread.sleep(msec);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
   
   //随意挑选的hash算法，没有评估过
   public static int FNVHash(byte[] data)
   {
       final int p = 16777619;
       int hash = (int)2166136261L;
       for(byte b:data)
           hash = (hash ^ b) * p;
       hash += hash << 13;
       hash ^= hash >> 7;
       hash += hash << 3;
       hash ^= hash >> 17;
       hash += hash << 5;
       return hash;
   }
   
	public static String getHost(String address) {
		String host = null;
		if (address != null) {
			String[] a = address.split(":");
			if (a.length >= 2)
				host = a[0].trim();
		}
		return host;
	}

	public static int getPort(String address) {
		int port = 0;
		if (address != null) {
			String[] a = address.split(":");
			if (a.length >= 2)
				port = Integer.parseInt(a[1].trim());
		}
		return port;
	}
	
	public static int getVersion(String address) {
		int version = ObInstanceManager.VERSION;
		if (address != null) {
			String[] a = address.split(":");
			if (a.length >= 3) {
				if (a[2].trim().equalsIgnoreCase("0.1")) 
					version = ObInstanceManager.OLD_VERSION;
				else
					version = ObInstanceManager.VERSION;
			}
		}
		return version;
	}

}