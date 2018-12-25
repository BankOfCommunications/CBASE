package com.taobao.oceanbase.util.result;

import java.util.List;

import com.taobao.oceanbase.vo.RowData;

public class NoOpHandler implements Handler<List<RowData>, List<RowData>> {

	public List<RowData> handle(List<RowData> f) {
		return f;
	}
}