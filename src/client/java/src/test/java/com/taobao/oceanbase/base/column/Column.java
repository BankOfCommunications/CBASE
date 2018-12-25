package com.taobao.oceanbase.base.column;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.rule.Rule;
import com.taobao.oceanbase.vo.Value;

public class Column {
	
	/**
	 * @param name
	 * @param id
	 * @param colRule
	 */
	public Column(String name, long id, Rule colRule) {
		this.name = name;
		this.id = id;
		this.colRule = colRule;
	}


	public String getName() {
		return name;
	}


	public long getId() {
		return id;
	}
	/**
	 * @return the colRule
	 */
	public Rule getColRule() {
		return colRule;
	}
	
	
	protected String name;
	protected long id;
	protected Rule colRule;
	

	public Object getColumnValue(RKey rkey){
		return colRule.genColumnValue(rkey, this.id);
	}
	
	public Value genColumnUpdateResult(RKey rkey,int times, boolean isAdd){
		return colRule.genColumnUpdateResult(rkey, this.id, times, isAdd);
	}

}
