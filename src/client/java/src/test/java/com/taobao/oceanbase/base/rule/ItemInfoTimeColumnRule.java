package com.taobao.oceanbase.base.rule;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.vo.Value;

public class ItemInfoTimeColumnRule implements Rule {

	public ByteBuffer genRowKey() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object genColumnValue(RKey rkey, long col_id) {
		// TODO Auto-generated method stub
		long result = 0;
		if (rkey.getKeyRule() instanceof ItemInfoKeyRule) {
			long type_id = ((ItemInfoKeyRule) (rkey.getKeyRule())).getItem_id();
			result = Rule.TIME_START - type_id - col_id;
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
