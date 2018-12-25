package com.taobao.oceanbase.util.result;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.taobao.oceanbase.vo.RowData;

public class SelectHandler implements Handler<List<RowData>, List<RowData>> {
	private Set<String> columns;

	public SelectHandler(Set<String> columns) {
		this.columns = columns;
	}

	public List<RowData> handle(List<RowData> resultSet) {
		if (columns == null)
			throw new RuntimeException("select clause have null columns");
		List<RowData> result = new ArrayList<RowData>(resultSet.size());
		for (RowData row : resultSet) {
			RowData data = new RowData();
			for (String column : columns) {
				data.addData(column, row.get(column));
			}
			result.add(data);
		}
		return result;
	}
}