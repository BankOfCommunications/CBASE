package com.taobao.oceanbase.base;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.base.rule.Rule;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.vo.Rowkey;

public class RKey extends Rowkey {

	private Rule kRule;
	private ByteBuffer buffer = null;

	public RKey(Rowkey rkey) {
		long user_id = Long.parseLong(rkey.getRowkey().substring(0, 8));
		long item_id = Long.parseLong(rkey.getRowkey().substring(9, 17));
		byte item_type = rkey.getRowkey().getBytes()[8];
		kRule = new CollectInfoKeyRule(user_id, item_type, item_id);
		buffer = kRule.genRowKey();
	}

	public RKey(String rkey, boolean isCollect) {
		if (isCollect) {
			long user_id;
			long item_id;
			byte item_type;
			if (rkey.getBytes().length > 17) {
				throw new IllegalArgumentException(
						"info rowkey should be less than 17");
			} else {
				int len_to_add = 17 - rkey.getBytes().length;
				if (len_to_add >= 9) {
					user_id = Long.parseLong(rkey);
					item_type = (byte) '0';
					item_id = 0;
				} else if (len_to_add == 8) {
					user_id = Long.parseLong(rkey.substring(0, 8));
					item_type = (byte) rkey.charAt(8);
					item_id = 0;
				} else {
					user_id = Long.parseLong(rkey.substring(0, 8));
					item_type = (byte) rkey.charAt(8);
					item_id = Long.parseLong(rkey.substring(9, rkey.length()));
				}
				kRule = new CollectInfoKeyRule(user_id, item_type, item_id);
				buffer = kRule.genRowKey();
			}
		} else {
			long item_id;
			byte item_type;
			if (rkey.getBytes().length > 9) {
				throw new IllegalArgumentException(
						"item rowkey should be less than 17");
			} else {
				int len_to_add = 9 - rkey.getBytes().length;
				if (len_to_add ==0) {
					item_type = (byte) rkey.charAt(0);
					item_id =  Long.parseLong(rkey.substring(1, rkey.length()));
				}  else {
					item_type = (byte) '0';
					item_id = Long.parseLong(rkey.substring(0, rkey.length()));
				}
				kRule = new ItemInfoKeyRule(item_type, item_id);
				buffer = kRule.genRowKey();
			}
		}

	}

	public RKey(Rule key_rule) {
		if (key_rule instanceof CollectInfoKeyRule)
			kRule = (CollectInfoKeyRule) key_rule;
		if (key_rule instanceof ItemInfoKeyRule)
			kRule = (ItemInfoKeyRule) key_rule;
		buffer = kRule.genRowKey();
	}

	public Rule getKeyRule() {
		return kRule;
	}

	public RKey(byte[] bytes) {
		buffer = ByteBuffer.wrap(bytes);
	}

	@Override
	public byte[] getBytes() {
		// TODO Auto-generated method stub
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

	public boolean equals(RKey key) {
		return this.toString().equals(key.toString());
	}
}
