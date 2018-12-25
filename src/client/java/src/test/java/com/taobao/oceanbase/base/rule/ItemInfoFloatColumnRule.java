package com.taobao.oceanbase.base.rule;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.vo.Value;

public class ItemInfoFloatColumnRule implements Rule {

	public ByteBuffer genRowKey() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object genColumnValue(RKey rkey, long col_id) {
		// TODO Auto-generated method stub
		float result = 0;
		if (rkey.getKeyRule() instanceof ItemInfoKeyRule) {
			long item_id = ((ItemInfoKeyRule) (rkey.getKeyRule())).getItem_id();
			result = (float) ((item_id * Rule.SEC_MULTIPLIER + col_id) * 1.0F);
			return result;
		} else
			return null;
	}
	
	public Value genColumnUpdateResult(RKey rkey, long colid, int times, boolean isAdd) {
		Value v=new Value();
		if(!isAdd)
			v.setFloat((Float)this.genColumnValue(rkey, colid)+times*1.0f);
		else
			v.setFloat(times*1.0f);
		return v;
	}
	
}
