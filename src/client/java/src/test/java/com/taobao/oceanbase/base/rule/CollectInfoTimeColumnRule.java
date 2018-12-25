package com.taobao.oceanbase.base.rule;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.vo.Value;

public class CollectInfoTimeColumnRule implements Rule {

	public ByteBuffer genRowKey() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object genColumnValue(RKey rkey, long col_id) {
		// TODO Auto-generated method stub
		long result = 0;
		if (rkey.getKeyRule() instanceof CollectInfoKeyRule) {
			long user_id = ((CollectInfoKeyRule) (rkey.getKeyRule()))
					.getUser_id();
			long type_id = ((CollectInfoKeyRule) (rkey.getKeyRule()))
					.getItem_id();
			result = Rule.TIME_START - user_id * Rule.FIR_MULTIPLIER - type_id
					- col_id;
			return result;
		} else
			return null;
	}

	public Value genColumnUpdateResult(RKey rkey, long colid, int times, boolean isAdd) {
		Value v=new Value();
		if(!isAdd)
			v.setSecond((Long)this.genColumnValue(rkey, colid)+times*1L);
		else
			v.setSecond(times*1L);
		return v;
	}
}
