package com.taobao.oceanbase.base.table;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.column.Column;
import com.taobao.oceanbase.vo.Value;

public class Table implements ITable {

	/**
	 * @param str2col the str2col to set
	 */
	public void setStr2col(Map<String, Column> str2col) {
		this.str2col = str2col;
	}

	/**
	 * @param jstr2col the jstr2col to set
	 */
	public void setJstr2col(Map<String, Column> jstr2col) {
		this.jstr2col = jstr2col;
	}

	protected Map<String, Column> str2col = new HashMap<String, Column>();
	protected Map<String, Column> jstr2col = new HashMap<String, Column>();
	protected String tableName;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/* (non-Javadoc)
	 * @see com.taobao.obtest.base.table.ITable#addColumn(java.lang.String, com.taobao.obtest.base.column.Column, boolean)
	 */

	public void addColumn(String col_name, Column col, boolean join) {
		if (false == join)
			str2col.put(col_name, col);
		else
			jstr2col.put(col_name, col);
	}

	public void removeColumn(String colName)
	{
		Column col=str2col.remove(colName);
		if(null==col)
			jstr2col.remove(colName);
	}
	@Deprecated
	public Column getColumn(String col_name) {
		Column col;
		col = str2col.get(col_name);
		if (null == col)
			col = jstr2col.get(col_name);
		return col;
	}
	/* (non-Javadoc)
	 * @see com.taobao.obtest.base.table.ITable#getColumnValue(java.lang.String, com.taobao.obtest.base.RKey)
	 */

	public Object getColumnValue(String col_name, RKey rkey) {
		return null;
	}
	

	public Set<String> getAllColumnNames(){
		Set<String> colnames=new HashSet<String>();
		for (String str : str2col.keySet()) {
			colnames.add(str);
		}
		for(String str:jstr2col.keySet()){
			colnames.add(str);
		}
		return colnames;
	}


	public Value genColumnUpdateResult(String colName, RKey rkey, int times, boolean isAdd) {
		return null;
	}


	public Set<String> getJoinColumnNames() {
		Set<String> colnames=new HashSet<String>();
		for(String str:jstr2col.keySet()){
			colnames.add(str);
		}
		return colnames;
	}


	public Set<String> getNonJoinColumnNames() {
		Set<String> colnames=new HashSet<String>();
		for (String str : str2col.keySet()) {
			colnames.add(str);
		}
		return colnames;
	}
}
