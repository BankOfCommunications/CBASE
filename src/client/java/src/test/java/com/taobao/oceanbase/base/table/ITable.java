package com.taobao.oceanbase.base.table;

import java.util.Set;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.column.Column;
import com.taobao.oceanbase.vo.Value;

public interface ITable {

	public abstract void addColumn(String col_name, Column col, boolean join);
	
	public abstract void removeColumn(String colName);

	public abstract Object getColumnValue(String col_name, RKey rkey);
	
	public abstract Set<String> getAllColumnNames();
	
	public abstract Set<String> getJoinColumnNames();
	
	public abstract Set<String>	getNonJoinColumnNames();
	
	public abstract String getTableName();
	
	public abstract Value genColumnUpdateResult(String colName,RKey rkey, int times, boolean isAdd);

}