package com.taobao.oceanbase.vo;

public class Option {

	private String column;
	private String alias;
	private StatsFunc statsFunc;

	public Option(String column, String alias, StatsFunc statsFunc) {
		this.column = column;
		this.alias = alias;
		this.statsFunc = (statsFunc == null ? StatsFunc.NONE : statsFunc);
	}

	public String getColumn() {
		return column;
	}

	public String getAlias() {
		return alias;
	}

	public StatsFunc getStatsFunc() {
		return statsFunc;
	}
}