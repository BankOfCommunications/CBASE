package com.taobao.oceanbase.base.rule;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.vo.Value;

public class ItemInfoDoubleColumnRule implements Rule {

	public ByteBuffer genRowKey() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object genColumnValue(RKey rkey, long col_id) {
		// TODO Auto-generated method stub
		double result = 0;
		if (rkey.getKeyRule() instanceof ItemInfoKeyRule) {
			long item_id = ((ItemInfoKeyRule) (rkey.getKeyRule())).getItem_id();
			result = (double) ((item_id * Rule.SEC_MULTIPLIER + col_id) * 1.0F);
			return result;
		} else
			return null;
	}

	public Value genColumnUpdateResult(RKey rkey, long colid,int times, boolean isAdd) {
		Value v=new Value();
		if(!isAdd)
			v.setDouble((Double)this.genColumnValue(rkey, colid)+times*(double)1);
		else
			v.setDouble(times*(double)1);
		return v;
	}

}
