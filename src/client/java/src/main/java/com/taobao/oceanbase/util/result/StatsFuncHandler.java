package com.taobao.oceanbase.util.result;

import java.util.ArrayList;
import java.util.List;

import com.taobao.oceanbase.vo.Option;
import com.taobao.oceanbase.vo.RowData;

public class StatsFuncHandler implements
		Handler<List<List<RowData>>, List<RowData>> {

	private List<Option> columns = new ArrayList<Option>();

	public StatsFuncHandler(List<Option> columns) {
		this.columns = columns;
	}

	public List<RowData> handle(List<List<RowData>> resultSet) {
		if (columns == null)
			throw new RuntimeException("select clause have null columns");
		List<RowData> result = new ArrayList<RowData>();
		for (List<RowData> group : resultSet) {
			RowData row = new RowData();
			for (Option option : columns) {
				row.addData(option.getAlias(),
						option.getStatsFunc().stats(group, option.getColumn()));
			}
			result.add(row);
		}
		return result;
	}
}