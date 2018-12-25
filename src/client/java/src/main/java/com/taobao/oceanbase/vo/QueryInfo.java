package com.taobao.oceanbase.vo;

import static com.taobao.oceanbase.util.Const.INCLUSIVE_END;
import static com.taobao.oceanbase.util.Const.INCLUSIVE_START;
import static com.taobao.oceanbase.util.Const.MIN_VALUE;
import static com.taobao.oceanbase.util.Const.MAX_VALUE;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.taobao.oceanbase.util.CheckParameter;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.vo.inner.ObSimpleFilter;

public class QueryInfo {

	private Rowkey startKey;
	private Rowkey endKey;
	private int flag;
	private Map<String, Boolean> orderBy = new LinkedHashMap<String, Boolean>();
	private Set<String> columns = new HashSet<String>();
	private int pageSize;
	private int pageNum;
	private int limit = Const.MAX_ROW_NUMBER_PER_QUERY;
	private ObSimpleFilter filter;
	private boolean isReadConsistency = false;
	//private ObGroupByParam groupbyParam;
	
	// scan flag
	private int scanDirection = Const.SCAN_DIRECTION_FORWARD;
	private int scanMode = Const.SCAN_MODE_SYNCREAD;
	
	public long getScanFlag() {
		long retFlag = 0;
		retFlag |= ((scanMode & 0XF) << 4);
		retFlag |= ((scanDirection & 0XF));
		return retFlag;
	}
	
	public int getScanDirection() {
		return scanDirection;
	}

	public void setScanDirection(int scanDirection) {
		if (scanDirection != Const.SCAN_DIRECTION_BACKWARD
				&& scanDirection != Const.SCAN_DIRECTION_FORWARD) {
			throw new IllegalArgumentException("scan direction invalid: " + scanDirection);
		}
		this.scanDirection = scanDirection;
	}

	public int getScanMode() {
		return scanMode;
	}

	public void setScanMode(int scanMode) {
		if (scanMode != Const.SCAN_MODE_PREREAD
				&& scanMode != Const.SCAN_MODE_SYNCREAD) {
			throw new IllegalArgumentException("scan mode invalid: " + scanMode);
		}
		this.scanMode = scanMode;
	}
	
	public void setReadConsistency(boolean isReadConsistency) {
		this.isReadConsistency = isReadConsistency;
	}
	
	public boolean isReadConsistency() {
		return isReadConsistency;
	}

	public boolean isInclusiveStart() {
		return (flag & INCLUSIVE_START) == INCLUSIVE_START;
	}

	public QueryInfo setInclusiveStart(boolean value) {
		flag = value ? (flag | INCLUSIVE_START) : (flag & ~INCLUSIVE_START);
		return this;
	}

	public boolean isInclusiveEnd() {
		return (flag & INCLUSIVE_END) == INCLUSIVE_END;
	}

	public QueryInfo setInclusiveEnd(boolean value) {
		flag = value ? (flag | INCLUSIVE_END) : (flag & ~INCLUSIVE_END);
		return this;
	}
	
	public boolean isMinValue() {
		return (flag & MIN_VALUE) == MIN_VALUE;
	}
	
	public QueryInfo setMinValue(boolean value) {
		flag = value ? (flag | MIN_VALUE) : (flag & ~MIN_VALUE);
		return this;
	}
	
	public boolean isMaxValue() {
		return (flag & MAX_VALUE) == MAX_VALUE;
	}
	
	public QueryInfo setMaxValue(boolean value) {
		flag = value ? (flag | MAX_VALUE) : (flag & ~MAX_VALUE);
		return this;
	}

	public String getStartKey() {
		String key = null;
		if (startKey != null) {
			key = startKey.getRowkey();
		}
		return key;
	}

	public QueryInfo setStartKey(Rowkey startKey) {
		CheckParameter.checkNull("start rowkey null", startKey);
		this.startKey = startKey;
		return this;
	}

	public String getEndKey() {
		String key = null;
		if (endKey != null) {
			key = endKey.getRowkey();
		}
		return key;
	}

	public QueryInfo setEndKey(Rowkey endKey) {
		CheckParameter.checkNull("end rowkey null", endKey);
		this.endKey = endKey;
		return this;
	}

	public Map<String, Boolean> getOrderBy() {
		return orderBy;
	}

	public QueryInfo addOrderBy(String orderBy, boolean order) {
		CheckParameter.checkColumnName(orderBy);
		if (this.orderBy.containsKey(orderBy.toLowerCase())) {
			throw new RuntimeException("order by duplicate");
		}
		this.orderBy.put(orderBy.toLowerCase(), order);
		return this;
	}

	public Set<String> getColumns() {
		return Collections.unmodifiableSet(columns);
	}

	public QueryInfo addColumn(String column) {
		CheckParameter.checkColumnName(column);
		this.columns.add(column.toLowerCase());
		return this;
	}

	public int getPageSize() {
		return pageSize;
	}

	public QueryInfo setPageSize(int pageSize) {
		if (pageSize < 1)
			throw new RuntimeException("pageSize can not be less than 1");
		this.pageSize = pageSize;
		return this;
	}

	public int getPageNum() {
		return pageNum;
	}

	public QueryInfo setPageNum(int pageNum) {
		if (pageNum < 1)
			throw new RuntimeException("pageNum can not be less than 1");
		this.pageNum = pageNum;
		return this;
	}

	public int getLimit() {
		return limit;
	}

	public QueryInfo setLimit(int limit) {
		if (limit < 1 || limit > Const.MAX_ROW_NUMBER_PER_QUERY)
			throw new RuntimeException(
					"limit can not be less than 1 or more than 2000");
		this.limit = limit;
		return this;
	}
	
	public ObSimpleFilter getFilter() {
		return filter;
	}

	public void setFilter(ObSimpleFilter filter) {
		this.filter = filter;
	}

//	public ObGroupByParam getGroupbyParam() {
//		return groupbyParam;
//	}
//
//	public void setGroupbyParam(ObGroupByParam groupbyParam) {
//		this.groupbyParam = groupbyParam;
//	}

	public String toString() {
		StringBuilder content = new StringBuilder();
		content.append("select ");
		for (String column : columns) {
			content.append("[" + column + "]");
		}
		for (Map.Entry<String, Boolean> entry : orderBy.entrySet()) {
			content.append(
					"[" + entry.getKey() + " "
							+ (entry.getValue() ? "asc" : "desc")).append("]");
		}
		return content.toString();
	}

}