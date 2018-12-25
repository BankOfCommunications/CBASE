package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ObGroupByParam {

	private List<String> groupByColumns = new ArrayList<String>();
	private List<String> returnColumns = new ArrayList<String>();
	private List<ObAggregateColumn> aggregateColumns = new ArrayList<ObAggregateColumn>();

	public void addGroupByColumn(String column) {
		if (column != null && column.trim().length() > 0)
			groupByColumns.add(column);
	}
	
	public void addReturnColumn(String column) {
		if (column != null && column.trim().length() > 0)
			returnColumns.add(column);
	}
	
	public void addAggregateColumn(ObAggregateColumn column) {
		if (column != null) {
			aggregateColumns.add(column);
		}
	}
	
	public void serialize(ByteBuffer buffer) {
		ObObj obj = new ObObj();

		if (groupByColumns.size() > 0) {
			obj.setExtend(ObActionFlag.GROUPBY_GRO_COLUMN_FIELD);
			obj.serialize(buffer);
			for (String column : groupByColumns) {
				obj.setString(column);
				obj.serialize(buffer);
			}
		}

		if (returnColumns.size() > 0) {
			obj.setExtend(ObActionFlag.GROUPBY_RET_COLUMN_FIELD);
			obj.serialize(buffer);
			for (String column : returnColumns) {
				obj.setString(column);
				obj.serialize(buffer);
			}
		}

		if (aggregateColumns.size() > 0) {
			obj.setExtend(ObActionFlag.GROUPBY_AGG_COLUMN_FIELD);
			obj.serialize(buffer);
			for (ObAggregateColumn aggColumn : aggregateColumns) {
				obj.setNumber(aggColumn.getFuncType().getValue());
				obj.serialize(buffer);
				obj.setString(aggColumn.getAsColumnName());
				obj.serialize(buffer);
				obj.setString(aggColumn.getOrgColumnName());
				obj.serialize(buffer);
			}
		}
	}
	
	public int getSize() {
		int length = 0;
		ObObj obj = new ObObj();
		
		if (groupByColumns.size() > 0) {
			obj.setExtend(ObActionFlag.GROUPBY_GRO_COLUMN_FIELD);
			length += obj.getSize();
			for (String column : groupByColumns) {
				obj.setString(column);
				length += obj.getSize();
			}
		}

		if (returnColumns.size() > 0) {
			obj.setExtend(ObActionFlag.GROUPBY_RET_COLUMN_FIELD);
			length += obj.getSize();
			for (String column : returnColumns) {
				obj.setString(column);
				length += obj.getSize();
			}
		}

		if (aggregateColumns.size() > 0) {
			obj.setExtend(ObActionFlag.GROUPBY_AGG_COLUMN_FIELD);
			length += obj.getSize();
			for (ObAggregateColumn aggColumn : aggregateColumns) {
				obj.setNumber(aggColumn.getFuncType().getValue());
				length += obj.getSize();
				obj.setString(aggColumn.getAsColumnName());
				length += obj.getSize();
				obj.setString(aggColumn.getOrgColumnName());
				length += obj.getSize();
			}
		}
		
		return length;
	}
}
