package com.taobao.oceanbase.util.result;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.taobao.oceanbase.vo.RowData;

public class OrderByHandler implements Handler<List<RowData>, List<RowData>> {
	private Map<String, Boolean> columns;

	public OrderByHandler(LinkedHashMap<String, Boolean> columns) {
		this.columns = columns;
	}

	public List<RowData> handle(List<RowData> results) {
		if (columns == null)
			return results;
		Collections.sort(results, new ColumnsComparator());
		return results;
	}

	private class ColumnsComparator implements Comparator<RowData> {

		@SuppressWarnings("unchecked")
		public int compare(RowData row1, RowData row2) {
			for (Map.Entry<String, Boolean> column : columns.entrySet()) {
				Comparable o1 = (Comparable) (row1.get(column.getKey()));
				Comparable o2 = (Comparable) (row2.get(column.getKey()));
				int result = o1.compareTo(o2);
				if (result != 0)
					return column.getValue() ? result : -result;
			}
			return 0;
		}
	}
}