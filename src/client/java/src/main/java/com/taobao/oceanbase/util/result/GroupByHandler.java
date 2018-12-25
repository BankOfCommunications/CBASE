package com.taobao.oceanbase.util.result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.oceanbase.vo.RowData;

public class GroupByHandler implements
		Handler<List<RowData>, List<List<RowData>>> {

	private Set<String> columns;

	public GroupByHandler(Set<String> columns) {
		this.columns = columns;
	}

	public List<List<RowData>> handle(List<RowData> resultSet) {
		if (columns == null)
			throw new RuntimeException("null columns");
		Map<String, List<RowData>> map = getMapResult(resultSet);
		List<List<RowData>> result = new ArrayList<List<RowData>>();
		for (Map.Entry<String, List<RowData>> entry : map.entrySet()) {
			result.add(entry.getValue());
		}
		return result;
	}

	private Map<String, List<RowData>> getMapResult(List<RowData> resultSet) {
		Map<String, List<RowData>> map = new HashMap<String, List<RowData>>();
		for (RowData row : resultSet) {
			String key = "";
			for (String column : columns) {
				key += row.get(column);
			}
			if (map.containsKey(key)) {
				map.get(key).add(row);
			} else {
				List<RowData> group = new ArrayList<RowData>();
				group.add(row);
				map.put(key, group);
			}
		}
		return map;
	}

}