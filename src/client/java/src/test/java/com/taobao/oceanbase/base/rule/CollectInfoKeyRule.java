package com.taobao.oceanbase.base.rule;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.vo.Value;

public class CollectInfoKeyRule implements Rule {

	private long user_id;
	private byte item_type;
	private long item_id;

	private final static String type_arr = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	public CollectInfoKeyRule(long uid, byte itype, long iid) {
		user_id = uid;
		item_type = itype;
		item_id = iid;
	}

	// TODO
	public CollectInfoKeyRule(long uid, long iid) {
		user_id = uid;
		item_type = type_arr.getBytes()[(int) ((iid / Rule.NUM_PER_UIDTYPE) % Rule.NUM_OF_TYPE)];
		item_id = iid;
	}

	/*
	 * //TODO generate the very first rowkey under the specify user_id
	 */
	public CollectInfoKeyRule(long uid) {
		user_id = uid;
		item_type = type_arr.getBytes()[0];
		item_id = 0;
	}
	
	public long getUser_id() {
		return user_id;
	}

	public byte getItem_type() {
		return item_type;
	}

	public long getItem_id() {
		return item_id;
	}

	public ByteBuffer genRowKey() {
		// TODO Auto-generated method stub
		String rk = String.format("%08d%c%08d", user_id, item_type, item_id);
		return ByteBuffer.wrap(rk.getBytes());
	}

	public Object genColumnValue(RKey rkey, long col_id) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public RKey currRkey(){
		Rule rule = new CollectInfoKeyRule(user_id, item_type, item_id);
		return new RKey(rule);
	}
	public RKey nextRkey() {
		item_id++;
		// if item_id(records per user_id) equals Rule.NUM_OF_TYPE *
		// Rule.NUM_PER_UIDTYPE
		// Then reset item_id and item_type increase user_id
		if (Rule.NUM_OF_TYPE * Rule.NUM_PER_UIDTYPE == item_id) {
			item_type = '0';
			item_id = 0;
			user_id++;
		} else {
			item_type = type_arr.getBytes()[(int) (item_id
					/ Rule.NUM_PER_UIDTYPE % Rule.NUM_OF_TYPE)];
		}
		Rule rule = new CollectInfoKeyRule(user_id, item_type, item_id);
		return new RKey(rule);
	}

	public RKey preRkey() {
		item_id--;
		// if item_id equals -1
		// Then reset item_id and item_type decrease user_id
		if (-1 == item_id) {
			item_type = 'z';
			item_id = Rule.NUM_OF_TYPE * Rule.NUM_PER_UIDTYPE - 1;
			user_id--;
		} else {
			item_type = type_arr.getBytes()[(int) (item_id
					/ Rule.NUM_PER_UIDTYPE % Rule.NUM_OF_TYPE)];
		}
		Rule rule = new CollectInfoKeyRule(user_id, item_type, item_id);
		return new RKey(rule);
	}

	public static long countNum(RKey start_key, RKey end_key,
			boolean inclu_start, boolean inclu_end) {
		long count = 0;
		long start_uid = ((CollectInfoKeyRule) (start_key.getKeyRule()))
				.getUser_id();
		long start_iid = ((CollectInfoKeyRule) (start_key.getKeyRule()))
				.getItem_id();
		long end_uid = ((CollectInfoKeyRule) (end_key.getKeyRule()))
				.getUser_id();
		long end_iid = ((CollectInfoKeyRule) (end_key.getKeyRule()))
				.getItem_id();
		count = end_uid
				* Rule.NUM_OF_TYPE
				* Rule.NUM_PER_UIDTYPE
				+ end_iid
				- (start_uid * Rule.NUM_OF_TYPE * Rule.NUM_PER_UIDTYPE + start_iid)
				- 1;
		if (true == inclu_start)
			count++;
		if (true == inclu_end)
			count++;
		return count;
	}

	public Value genColumnUpdateResult(RKey rkey, long colid, int updateTimes,
			boolean isAdd) {
		// TODO Auto-generated method stub
		return null;
	}

}
