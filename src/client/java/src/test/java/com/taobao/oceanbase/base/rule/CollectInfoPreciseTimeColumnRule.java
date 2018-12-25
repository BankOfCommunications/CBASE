package com.taobao.oceanbase.base.rule;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.vo.Value;

public class CollectInfoPreciseTimeColumnRule implements Rule {

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
			result = (Rule.TIME_START - user_id * FIR_MULTIPLIER - type_id - col_id)
					* Rule.MICRO_PER_SEC;
			return result;
		}
		return null;
	}

	public Value genColumnUpdateResult(RKey rkey, long colid, int times, boolean isAdd) {
		Value v=new Value();
		if(!isAdd)
			v.setMicrosecond((Long)this.genColumnValue(rkey, colid)+times*1L);
		else
			v.setMicrosecond(times*1L);
		return v;
	}

}
