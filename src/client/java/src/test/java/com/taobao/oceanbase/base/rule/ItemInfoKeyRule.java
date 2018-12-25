package com.taobao.oceanbase.base.rule;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.vo.Value;

public class ItemInfoKeyRule implements Rule {

	private byte item_type;
	private long item_id;

	private final static String type_arr = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	public ItemInfoKeyRule(byte itype, long iid) {
		item_type = itype;
		item_id = iid;
	}

	// TODO
	public ItemInfoKeyRule(long iid) {
		item_type = type_arr.getBytes()[(int) ((iid / Rule.NUM_PER_UIDTYPE) % Rule.NUM_OF_TYPE)];
		item_id = iid;
	}

	public byte getItem_type() {
		return item_type;
	}

	public long getItem_id() {
		return item_id;
	}

	public ByteBuffer genRowKey() {
		// TODO Auto-generated method stub
		String rk = String.format("%c%08d", item_type, item_id);
		return ByteBuffer.wrap(rk.getBytes());
	}

	public Object genColumnValue(RKey rkey, long col_id) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public RKey currRkey(){
		Rule rule = new ItemInfoKeyRule(item_type, item_id);
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
		} else {
			item_type = type_arr.getBytes()[(int) (item_id
					/ Rule.NUM_PER_UIDTYPE % Rule.NUM_OF_TYPE)];
		}
		Rule rule = new ItemInfoKeyRule(item_type, item_id);
		return new RKey(rule);
	}

	public RKey preRkey() {
		item_id--;
		// if item_id equals -1
		// Then reset item_id and item_type decrease user_id
		if (-1 == item_id) {
			item_type = 'z';
			item_id = Rule.NUM_OF_TYPE * Rule.NUM_PER_UIDTYPE - 1;
		} else {
			item_type = type_arr.getBytes()[(int) (item_id
					/ Rule.NUM_PER_UIDTYPE % Rule.NUM_OF_TYPE)];
		}
		Rule rule = new ItemInfoKeyRule(item_type, item_id);
		return new RKey(rule);
	}

	public static long countNum(RKey start_key, RKey end_key,
			boolean inclu_start, boolean inclu_end) {
		long count = 0;
		long start_iid = ((ItemInfoKeyRule) (start_key.getKeyRule()))
				.getItem_id();
		long end_iid = ((ItemInfoKeyRule) (end_key.getKeyRule())).getItem_id();
		count = end_iid - start_iid - 1;
		if (true == inclu_start)
			count++;
		if (true == inclu_end)
			count++;
		return count;
	}

	public Value genColumnUpdateResult(RKey rkey, long colid, int updateTimes, boolean isAdd) {
		return null;
	}

}
