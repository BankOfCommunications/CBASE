package com.taobao.oceanbase.vo;

import java.util.List;

public enum StatsFunc {

	COUNT {
		@Override
		public Object stats(List<RowData> group, String column) {
			return group.size();
		}
	},

	NONE {
		@Override
		public Object stats(List<RowData> group, String column) {
			return group.get(0).get(column);
		}
	};

	public abstract Object stats(List<RowData> group, String column);

}