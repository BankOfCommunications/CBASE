package com.taobao.oceanbase.vo;

import java.util.List;

import com.taobao.oceanbase.vo.inner.ObActionFlag;

public abstract class ObMutatorBase {
	public  final class Tuple {
		public final String name;
		public final Value value;
		public final Boolean add;

		public Tuple(String column, Value value, Boolean add) {
			this.name = column;
			this.value = value;
			this.add = add;
		}
		
		public Tuple(String column, Value value) {
			this.name = column;
			this.value = value;
			this.add = false;
		}
	}
	
	public String getTable() {
		return table;
	}

	public String getRowkey() {
		return rowkey.getRowkey();
	}
	
	public ObActionFlag getActionFlag() {
		return flag;
	}
	
	public abstract List<Tuple> getColumns();
	
	protected String table;
	protected Rowkey rowkey;
	protected ObActionFlag flag;
	
}
