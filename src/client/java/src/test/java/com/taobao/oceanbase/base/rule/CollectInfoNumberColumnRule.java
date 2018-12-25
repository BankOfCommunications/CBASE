package com.taobao.oceanbase.base.rule;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.vo.Value;

public class CollectInfoNumberColumnRule implements Rule {

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
			long item_id = ((CollectInfoKeyRule) (rkey.getKeyRule()))
					.getItem_id();
			result = ((user_id * Rule.FIR_MULTIPLIER) + item_id)
					* Rule.SEC_MULTIPLIER + col_id;
			return result;
		} else
			return null;
	}

	public Value genColumnUpdateResult(RKey rkey, long colid, int times, boolean isAdd) {
		Value v=new Value();
		if(!isAdd)
			v.setNumber((Long)this.genColumnValue(rkey, colid)+times);
		else
			v.setNumber(times*1L);
		return v;
	}
}
