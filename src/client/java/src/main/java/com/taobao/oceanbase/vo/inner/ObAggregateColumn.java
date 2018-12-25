package com.taobao.oceanbase.vo.inner;

public class ObAggregateColumn {

	public static enum ObAggregateFuncType {
		AGG_FUNC_MIN(0), SUM(1), COUNT(2), MAX(3), MIN(4), AGG_FUNC_END(5);
		
		private int value;
		
		public int getValue() {
			return value;
		}
		
		ObAggregateFuncType(int value) {
			this.value = value;
		}
	}
	
	private String orgColumnName;
	private String asColumnName;
	private ObAggregateFuncType funcType;

	public ObAggregateColumn(String orgColumnName, String asColumnName,
			ObAggregateFuncType funcType) {
		this.orgColumnName = orgColumnName;
		this.asColumnName = asColumnName;
		this.funcType = funcType;
	}

	public String getOrgColumnName() {
		return orgColumnName;
	}

	public void setOrgColumnName(String orgColumnName) {
		this.orgColumnName = orgColumnName;
	}

	public String getAsColumnName() {
		return asColumnName;
	}

	public void setAsColumnName(String asColumnName) {
		this.asColumnName = asColumnName;
	}

	public ObAggregateFuncType getFuncType() {
		return funcType;
	}

	public void setFuncType(ObAggregateFuncType funcType) {
		this.funcType = funcType;
	}
}
