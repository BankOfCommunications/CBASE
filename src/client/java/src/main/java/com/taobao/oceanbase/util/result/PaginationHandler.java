package com.taobao.oceanbase.util.result;

import java.util.Collections;
import java.util.List;

import com.taobao.oceanbase.vo.RowData;

public class PaginationHandler implements Handler<List<RowData>, List<RowData>> {

	private int pageNum;
	private int pageSize;

	public PaginationHandler(int pageNum, int pageSize) {
		if (pageNum < 1 || pageSize < 1)
			throw new RuntimeException("pageNum or PageSize < 1");
		this.pageNum = pageNum;
		this.pageSize = pageSize;
	}

	public List<RowData> handle(List<RowData> resultSet) {
		int start = (pageNum - 1) * pageSize;
		int end = pageNum * pageSize;
		if (start > resultSet.size())
			return Collections.emptyList();
		return resultSet.subList(start, resultSet.size() > end ? end
				: resultSet.size());
	}
}