package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ObScanParam extends ObReadParam {

	private String table;
	private ObRange range;
	private List<String> columns = new ArrayList<String>();
	private long size;
	private long limitCount;
	private long limitOffset;
	private List<String> orderByColumns = new ArrayList<String>();
	private List<Long> orders = new ArrayList<Long>();
	private long scanDirection;
	private ObBorderFlag versionBorderFlag = new ObBorderFlag(true, true, true,
			true);
	private ObSimpleFilter filter;
	private ObGroupByParam groupbyParam;
	
	public String getTable(){
		return table;
	}
	
	public ObRange getRange(){
		return range;
	}
	
	public void setFilter(ObSimpleFilter filter) {
		this.filter = filter;
	}
	
	public void setGroupByParam(ObGroupByParam groupbyParam) {
		this.groupbyParam = groupbyParam;
	}

	public void setVersionBorderFlag(byte data) {
		versionBorderFlag.setData(data);
	}

	public void setScanSize(long size) {
		this.size = size;
	}

	public void setLimit(long offset, long count) {
		this.limitOffset = offset;
		this.limitCount = count;
	}

	public void set(String table, ObRange range) {
		this.table = table;
		this.range = range;
	}

	public void addColumns(String column) {
		columns.add(column);
	}

	public void addOrderBy(String column, boolean order) {
		orderByColumns.add(column);
		orders.add(order ? 1L : 2L);
	}

	public void setScanDirection(long direction) {
		this.scanDirection = direction;
	}

	public void serialize(ByteBuffer buffer) {
		ObObj obj = new ObObj();

		// reserved_param field
		obj.setExtend(ObActionFlag.RESERVE_PARAM_FIELD);
		obj.serialize(buffer);
		obj.setNumber(isReadConsistency());
		obj.serialize(buffer);
		// basic params
		obj.setExtend(ObActionFlag.BASIC_PARAM_FIELD);
		obj.serialize(buffer);
		obj.setNumber(isCached(), false); // is cache
		obj.serialize(buffer);
		obj.setNumber(versionBorderFlag.getData(), false); // border flag
		obj.serialize(buffer);
		obj.setNumber(getBeginVersion(), false);
		obj.serialize(buffer);
		obj.setNumber(getEndVersion(), false);
		obj.serialize(buffer);

		obj.setString(table); // table name
		obj.serialize(buffer);

		obj.setNumber(range.getBorderFlag().getData(), false); // border flag
		obj.serialize(buffer);
		obj.setString(range.getStartKey()); // start key
		obj.serialize(buffer);
		obj.setString(range.getEndKey()); // end key
		obj.serialize(buffer);

		obj.setNumber(scanDirection, false);
		obj.serialize(buffer);
		obj.setNumber(size, false);
		obj.serialize(buffer);
		// column list
		obj.setExtend(ObActionFlag.COLUMN_PARAM_FIELD);
		obj.serialize(buffer);
		for (String column : columns) {
			obj.setString(column);
			obj.serialize(buffer);
		}
		// filter
		if (filter != null) {
			obj.setExtend(ObActionFlag.FILTER_PARAM_FIELD);
			obj.serialize(buffer);
			filter.serialize(buffer);
		}
		
		// group by
		if (groupbyParam != null) {
			obj.setExtend(ObActionFlag.GROUPBY_PARAM_FIELD);
			obj.serialize(buffer);
			groupbyParam.serialize(buffer);
		}
		
		// sort params
		obj.setExtend(ObActionFlag.SORT_PARAM_FIELD);
		obj.serialize(buffer);
		for (int i = 0; i < orderByColumns.size(); ++i) {
			obj.setString(orderByColumns.get(i));
			obj.serialize(buffer);
			obj.setNumber(orders.get(i), false);
			obj.serialize(buffer);
		}
		// limit params
		obj.setExtend(ObActionFlag.LIMIT_PARAM_FIELD);
		obj.serialize(buffer);
		obj.setNumber(limitOffset, false);
		obj.serialize(buffer);
		obj.setNumber(limitCount, false);
		obj.serialize(buffer);
		// FILTER_PARAM_FIELD

		obj.setExtend(ObActionFlag.END_PARAM_FIELD);
		obj.serialize(buffer);
	}

	public int getSize() {
		int length = 0;
		ObObj obj = new ObObj();
		
		obj.setExtend(ObActionFlag.RESERVE_PARAM_FIELD);
		length += obj.getSize();
		obj.setNumber(isReadConsistency());
		length += obj.getSize();

		obj.setExtend(ObActionFlag.BASIC_PARAM_FIELD); // type
		length += obj.getSize();
		obj.setNumber(isCached(), false); // is cache
		length += obj.getSize();
		obj.setNumber(range.getBorderFlag().getData(), false); // border flag
		length += obj.getSize();
		obj.setNumber(getBeginVersion(), false);
		length += obj.getSize();
		obj.setNumber(getEndVersion(), false);
		length += obj.getSize();

		obj.setString(table); // table name
		length += obj.getSize();

		obj.setNumber(range.getBorderFlag().getData(), false); // border flag
		length += obj.getSize();
		obj.setString(range.getStartKey()); // start key
		length += obj.getSize();
		obj.setString(range.getEndKey()); // end key
		length += obj.getSize();

		obj.setNumber(scanDirection, false);
		length += obj.getSize();
		obj.setNumber(size, false);
		length += obj.getSize();

		obj.setExtend(ObActionFlag.COLUMN_PARAM_FIELD);
		length += obj.getSize();
		for (String column : columns) {
			obj.setString(column);
			length += obj.getSize();
		}

		// filter
		if (filter != null) {
			obj.setExtend(ObActionFlag.FILTER_PARAM_FIELD);
			length += obj.getSize();
			length += filter.getSize();
		}
		
		// group by
		if (groupbyParam != null) {
			obj.setExtend(ObActionFlag.GROUPBY_PARAM_FIELD);
			length += obj.getSize();
			length += groupbyParam.getSize();
		}	
		
		obj.setExtend(ObActionFlag.SORT_PARAM_FIELD);
		length += obj.getSize();
		for (int i = 0; i < orderByColumns.size(); ++i) {
			obj.setString(orderByColumns.get(i));
			length += obj.getSize();
			obj.setNumber(orders.get(i), false);
			length += obj.getSize();
		}

		obj.setExtend(ObActionFlag.LIMIT_PARAM_FIELD);
		length += obj.getSize();
		obj.setNumber(limitOffset, false);
		length += obj.getSize();
		obj.setNumber(limitCount, false);
		length += obj.getSize();

		// FILTER_PARAM_FIELD
		obj.setExtend(ObActionFlag.END_PARAM_FIELD);
		length += obj.getSize();

		return length;
	}
}