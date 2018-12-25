package com.taobao.oceanbase.util.result;

import java.util.Collections;
import java.util.List;

import com.taobao.oceanbase.vo.RowData;

public class LimitHandler implements Handler<List<RowData>, List<RowData>> {

	private int limit;

	public LimitHandler(int limit) {
		this.limit = limit;
	}

	public List<RowData> handle(List<RowData> resultSet) {
		if (resultSet == null)
			return Collections.emptyList();
		return resultSet.subList(0, resultSet.size() < limit ? resultSet.size()
				: limit);
	}
}