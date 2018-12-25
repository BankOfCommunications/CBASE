package com.taobao.oceanbase.base;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.vo.Rowkey;

/**
 * @author baoni
 * 
 */
public class RowKey extends Rowkey {
	ByteBuffer buffer = null;
	long uid;
	byte itype;
	long tid;
	String type_arr = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	public long get_uid() {
		return uid;
	}

	public byte get_itype() {
		return itype;
	}

	public long get_tid() {
		return tid;
	}

	public static String snprintf(int size, String format, Object... args) {
		StringWriter writer = new StringWriter(size);
		PrintWriter out = new PrintWriter(writer);
		out.printf(format, args);
		out.close();
		return writer.toString();
	}

	public RowKey() {
		this.uid = 0;
		this.itype = (byte) '0';
		this.tid = 0;
		String rk = snprintf(17, "%s", "");
		buffer = ByteBuffer.wrap(rk.getBytes());
	}

	public RowKey(String rowkey) {
		if(rowkey.getBytes().length>17){
			throw new IllegalArgumentException("rowkey should be less than 17");
		}else {
			  int len_to_add = 17 - rowkey.getBytes().length;
			  if(len_to_add>=9){
				  this.uid=Long.parseLong(rowkey);
				  this.itype = (byte) '0';
				  this.tid = 0;
			  }else if(len_to_add==8){
				  this.uid=Long.parseLong(rowkey.substring(0, 8));
				  this.itype=(byte)rowkey.charAt(8);
				  this.tid = 0;
			  }else {
				  this.uid=Long.parseLong(rowkey.substring(0, 8));
				  this.itype=(byte)rowkey.charAt(8);
				  this.tid = Long.parseLong(rowkey.substring(9, rowkey.length()));
			  }
			//  String rk = snprintf(17,"%s%0"+len_to_add+"d",rowkey,0);
			  String rk = snprintf(17, "%08d%c%08d", uid, itype, tid);
		      buffer = ByteBuffer.wrap(rk.getBytes());
		}
	}

	public RowKey(long uid, char itype) {
		this.uid = uid;
		this.itype = (byte) itype;
		this.tid = 0;
		String rk = snprintf(17, "%08d%c%08d", uid, itype, tid);
		buffer = ByteBuffer.wrap(rk.getBytes());
	}

	public RowKey(long uid, char itype, long tid) {
		this.uid = uid;
		this.itype = (byte) itype;
		this.tid = tid;
		String rk = snprintf(17, "%08d%c%08d", uid, itype, tid);
		buffer = ByteBuffer.wrap(rk.getBytes());
	}

	public RowKey(char itype, long tid) {
		this.itype = (byte) itype;
		this.tid = tid;
		String rk = snprintf(9, "%c%08d", itype, tid);
		buffer = ByteBuffer.wrap(rk.getBytes());
	}

	public byte[] getBytes() {
		return buffer.array();
	}

	public String toString() {
		String ret = null;
		try {
			ret = new String(buffer.array(), Const.NO_CHARSET);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return ret;
	}
}
