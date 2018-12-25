package com.taobao.oceanbase.base.table;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.column.Column;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.vo.Value;

public class CollectInfoTable extends Table {

	// add all columns to map according to schema file
	public void init() {

	}

	public Object getColumnValue(String col_name, RKey rkey) {
		Column col;
		boolean join = false;
		col = str2col.get(col_name);
		if (null == col) {
			col = jstr2col.get(col_name);
			if(null == col)return null;
			join = true;
		}
		if (true == join) {
			rkey = transKey(rkey);
		}
		return col.getColumnValue(rkey);
	}

	// TODO trans collect_info rkey to item_info rkey
	private RKey transKey(RKey rkey) {
		ItemInfoKeyRule rule = new ItemInfoKeyRule(
				((CollectInfoKeyRule) (rkey.getKeyRule())).getItem_type(),
				((CollectInfoKeyRule) (rkey.getKeyRule())).getItem_id());
		return new RKey(rule);
	}
	
	@Override
	public Value genColumnUpdateResult(String colName, RKey rkey, int times, boolean isAdd) {
		Column col;
		boolean join = false;
		col = str2col.get(colName);
		if (null == col) {
			col = jstr2col.get(colName);
			if (null == col)return null;
			join = true;
		}
		if( true == join )
		{
			rkey = transKey(rkey);
		}
		return col.genColumnUpdateResult(rkey, times, isAdd);
	}

}
