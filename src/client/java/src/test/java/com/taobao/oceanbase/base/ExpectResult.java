package com.taobao.oceanbase.base;

import java.util.HashMap;
import java.util.Map;

/**
 * @author baoni
 * 
 */
public class ExpectResult {
	private static long time_start = 1289915563;

	private enum Type {
		INT, FLOAT, DOUBLE, VARCHAR, DATE_TIME, CREATE_TIME, MODIFY_TIME, PRECISE_DATETIME
	}

	private Map<String, Type> str2type;
	private Map<String, Integer> str2columnid; // store schema info

	public ExpectResult() {
		str2columnid = new HashMap<String, Integer>();
		str2columnid.put("user_nick", new Integer(2));
		str2columnid.put("is_shared", new Integer(3));
		str2columnid.put("note", new Integer(4));
		str2columnid.put("collect_time", new Integer(5));
		str2columnid.put("status", new Integer(6));
		str2columnid.put("gm_create", new Integer(7));
		str2columnid.put("gm_modified", new Integer(8));
		str2columnid.put("tag", new Integer(9));
		str2columnid.put("category", new Integer(10));
		str2columnid.put("title", new Integer(11));
		str2columnid.put("picurl", new Integer(12));
		str2columnid.put("owner_id", new Integer(13));
		str2columnid.put("owner_nick", new Integer(14));
		str2columnid.put("price", new Integer(15));
		str2columnid.put("ends", new Integer(16));
		str2columnid.put("proper_xml", new Integer(17));
		str2columnid.put("comment_count", new Integer(18));
		str2columnid.put("collector_count", new Integer(19));
		str2columnid.put("collect_count", new Integer(20));
		str2columnid.put("total_money", new Integer(21));
		str2columnid.put("collect_time_precise", new Integer(22));

		/*
		 * column_info=1,2,user_nick,varchar,32 column_info=1,3,is_shared,int
		 * column_info=1,4,note,varchar,512
		 * column_info=1,5,collect_time,datetime column_info=1,6,status,int
		 * column_info=1,7,gm_create,create_time
		 * column_info=1,8,gm_modified,modify_time
		 * column_info=1,9,tag,varchar,105 column_info=1,10,category,int
		 * column_info=1,11,title,varchar,256
		 * column_info=1,12,picurl,varchar,256 column_info=1,13,owner_id,int
		 * column_info=1,14,owner_nick,varchar,32 column_info=1,15,price,float
		 * column_info=1,16,ends,datetime
		 * column_info=1,17,proper_xml,varchar,2048
		 * column_info=1,18,comment_count,int
		 * column_info=1,19,collector_count,int
		 * column_info=1,20,collect_count,int
		 * column_info=1,21,total_money,double
		 * column_info=1,22,collect_time_precise,precise_datetime
		 */

		str2type = new HashMap<String, Type>();
		str2type.put("user_nick", Type.VARCHAR);
		str2type.put("is_shared", Type.INT);
		str2type.put("note", Type.VARCHAR);
		str2type.put("collect_time", Type.DATE_TIME);
		str2type.put("status", Type.INT);
		str2type.put("gm_create", Type.CREATE_TIME);
		str2type.put("gm_modified", Type.MODIFY_TIME);
		str2type.put("tag", Type.VARCHAR);
		str2type.put("category", Type.INT);
		str2type.put("title", Type.VARCHAR);
		str2type.put("picurl", Type.VARCHAR);
		str2type.put("owner_id", Type.INT);
		str2type.put("owner_nick", Type.VARCHAR);
		str2type.put("price", Type.FLOAT);
		str2type.put("ends", Type.DATE_TIME);
		str2type.put("proper_xml", Type.VARCHAR);
		str2type.put("comment_count", Type.INT);
		str2type.put("collector_count", Type.INT);
		str2type.put("collect_count", Type.INT);
		str2type.put("total_money", Type.DOUBLE);
		str2type.put("collect_time_precise", Type.PRECISE_DATETIME);
	}

	public Object get_value(RowKey rkey, String column_name) {
		Object result = null;
		switch (str2type.get(column_name)) {
		case INT:
			result = get_long(rkey, column_name);
			break;
		case FLOAT:
			result = get_float(rkey, column_name);
			break;
		case DOUBLE:
			result = get_double(rkey, column_name);
			break;
		case DATE_TIME:
			result = get_time(rkey, column_name);
			break;
		case CREATE_TIME:
			result = get_precise_time(rkey, column_name);
			break;
		case MODIFY_TIME:
			result = get_precise_time(rkey, column_name);
			break;
		case PRECISE_DATETIME:
			result = get_precise_time(rkey, column_name);
			break;
		case VARCHAR:
			result = get_string(rkey, column_name);
			break;
		default:
			break;
		}
		return result;
	}

	public String get_string(RowKey rkey, String column_name) {
		String result = null;
		result = rkey.toString() + "-"
				+ str2columnid.get(column_name).intValue();
		return result;
	}

	public long get_long(RowKey rkey, String column_name) {
		long result = 0;
		long uid = rkey.get_uid();
		long tid = rkey.get_tid();
		result = ((uid * 256) + tid) * 100
				+ str2columnid.get(column_name).intValue();
		return result;
	}

	public long get_time(RowKey rkey, String column_name) {
		long result = 0;
		long uid = rkey.get_uid();
		long tid = rkey.get_tid();
		result = time_start - uid * 256 - tid
				- str2columnid.get(column_name).intValue();
		return result;
	}

	public long get_precise_time(RowKey rkey, String column_name) {
		long result = 0;
		long uid = rkey.get_uid();
		long tid = rkey.get_tid();
		result = (time_start - uid * 256 - tid - str2columnid.get(column_name)
				.intValue()) * 1000000;
		return result;
	}

	public float get_float(RowKey rkey, String column_name) {
		float result = 0;
		long uid = rkey.get_uid();
		long tid = rkey.get_tid();
		result = (float) ((((uid * 256) + tid) * 100 + str2columnid.get(
				column_name).intValue()) * 1.0);
		return result;
	}

	public double get_double(RowKey rkey, String column_name) {
		double result = 0;
		long uid = rkey.get_uid();
		long tid = rkey.get_tid();
		result = (((uid * 256) + tid) * 100 + str2columnid.get(column_name)
				.intValue()) * 1.0;
		return result;
	}
}
