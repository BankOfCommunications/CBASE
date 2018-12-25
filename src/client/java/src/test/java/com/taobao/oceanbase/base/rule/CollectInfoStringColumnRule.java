package com.taobao.oceanbase.base.rule;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.vo.Value;

public class CollectInfoStringColumnRule implements Rule {

	public ByteBuffer genRowKey() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object genColumnValue(RKey rkey, long col_id) {
		// TODO Auto-generated method stub
		String result = null;
		if (rkey.getKeyRule() instanceof CollectInfoKeyRule) {
			result = rkey.toString() + "-" + col_id;
			return result;
		}
		return null;
	}

	public Value genColumnUpdateResult(RKey rkey, long colid, int times, boolean isAdd) {
		Value v=new Value();
		v.setString("UPDATE");
		return v;
	}

}
