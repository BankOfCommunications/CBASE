package com.taobao.oceanbase.util.result;

import java.util.LinkedHashMap;
import java.util.List;

import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.RowData;

public class ResultFilter {

	public static List<RowData> filter(QueryInfo query, List<RowData> resultSet) {
		Handler<List<RowData>, List<RowData>> handler = new NoOpHandler();
//		if (!query.getStatsColumns().isEmpty() && !query.getGroupBy().isEmpty()) {
//			Handler<List<RowData>, List<List<RowData>>> groupBy = new GroupByHandler(
//					query.getGroupBy());
//			Handler<List<List<RowData>>, List<RowData>> statsFunc = new StatsFuncHandler(
//					query.getStatsColumns());
//			handler = new SequenceHandler<List<RowData>, List<RowData>, List<List<RowData>>>(
//					groupBy, statsFunc);
//		}
		Handler<List<RowData>, List<RowData>> orderBy = new OrderByHandler(
				(LinkedHashMap) query.getOrderBy());
		handler = new SequenceHandler<List<RowData>, List<RowData>, List<RowData>>(
				handler, orderBy);
		Handler<List<RowData>, List<RowData>> limit = new LimitHandler(
				query.getLimit());
		handler = new SequenceHandler<List<RowData>, List<RowData>, List<RowData>>(
				handler, limit);
		if (query.getPageNum() > 0 && query.getPageSize() > 0) {
			Handler<List<RowData>, List<RowData>> pagination = new PaginationHandler(
					query.getPageNum(), query.getPageSize());
			handler = new SequenceHandler<List<RowData>, List<RowData>, List<RowData>>(
					handler, pagination);
		}
		return handler.handle(resultSet);
	}

}