package com.taobao.oceanbase.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.taobao.oceanbase.vo.QueryInfo;

public class CheckQueryParams {

	private static interface Validation {
		void check(QueryInfo query);
	}

	private static List<Validation> validations = new ArrayList<Validation>();

	static {
		validations.add(new Validation() {
			public void check(QueryInfo query) {
				CheckParameter.checkNull("query info null", query);
			}
		});
		validations.add(new Validation() {
			public void check(QueryInfo query) {
				if (0 < query.getStartKey().compareTo(query.getEndKey()))
					throw new RuntimeException(
							"start rowkey have to be less than end rowkey");
			}
		});
		validations.add(new Validation() {
			public void check(QueryInfo query) {
				for (String e : query.getColumns()) {
					if (e == null)
						throw new IllegalArgumentException("columns are null");
				}
			}
		});
		validations.add(new Validation() {
			public void check(QueryInfo query) {
				if (Math.min(query.getPageNum(), query.getPageSize()) == 0
						&& Math.max(query.getPageNum(), query.getPageSize()) > 0)
					throw new RuntimeException(
							"both pageNum and pageSize have to be set at the same time");
			}
		});
	}

	public static void check(QueryInfo query) {
		for (Validation validation : validations) {
			validation.check(query);
		}
	}
}