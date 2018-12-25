package com.taobao.oceanbase.testcase;

import static org.junit.Assert.assertEquals;
import java.util.List;
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.oceanbase.base.BaseCase;
import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Value;
import com.taobao.oceanbase.vo.inner.ObSimpleCond;
import com.taobao.oceanbase.vo.inner.ObSimpleFilter;

public class RowKeyReverseQueryTestForInfo extends BaseCase {

	private String[] ColumnInfo = collectInfoTable.getAllColumnNames().toArray(
			new String[0]);

	
	private void addColumns(QueryInfo Info) {
		for (int i = 0; i < ColumnInfo.length; i++) {
			Info.addColumn(ColumnInfo[i]);
		}
	}

	private void query_check_JoinInfo(Result<List<RowData>> result,
			RKey start_rowkey, RKey end_rowkey, boolean isInclusiveStart,
			boolean isInclusiveEnd) {
		CollectInfoKeyRule infoRowkeys = ((CollectInfoKeyRule) start_rowkey
				.getKeyRule());
		long length = CollectInfoKeyRule.countNum(start_rowkey, end_rowkey,
				isInclusiveStart, isInclusiveEnd);
		assertEquals(result.getResult().size(), length);
		
		RKey infoRowkey;
		for (int i = 0; i < length; i++) {
			if (i == 0 && isInclusiveStart) {
				infoRowkey = infoRowkeys.currRkey();
			} else {
				infoRowkey = infoRowkeys.nextRkey();
			}

			for (String infoColumn : collectInfoTable.getAllColumnNames()) {
				assertEquals(
						collectInfoTable.getColumnValue(infoColumn, infoRowkey),
						result.getResult().get(i).get(infoColumn));
			}
		}
	}

	@Before
	public void before() {
		obClient.clearActiveMemTale();
	}

	@After
	public void after() {
		obClient.clearActiveMemTale();
	}


	@Test
	public void test_rowkeyReverseQuery01_01_Normal() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		addColumns(Info);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
				(byte) '0', 9));
		RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0',
				10));

		query_check_JoinInfo(result, expect_start_rowkey, expect_end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	@Test
	public void test_rowkeyReverseQuery01_02_Where() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 
		// prepare where condition
		RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		Value value = new Value();
		value.setNumber((Long) collectInfoTable.getColumnValue("is_shared",
				tmp_rowkey));
		ObSimpleCond obSimpleCond = new ObSimpleCond("is_shared",
				ObSimpleCond.ObLogicOperator.GT, value);
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
				(byte) '0', 9));
		RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0',
				10));

		query_check_JoinInfo(result, expect_start_rowkey, expect_end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	@Test
	public void test_rowkeyReverseQuery01_03_Where_Like_One_Result() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 
		// prepare where condition
		RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 9));
		Value value = new Value();
		value.setString((String) collectInfoTable.getColumnValue("note",
				tmp_rowkey));
		ObSimpleCond obSimpleCond = new ObSimpleCond("note",
				ObSimpleCond.ObLogicOperator.LIKE, value);
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
				(byte) '0', 9));
		RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0',
				9));

		query_check_JoinInfo(result, expect_start_rowkey, expect_end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	@Test
	public void test_rowkeyReverseQuery01_03_Where_Like_Mult_Result() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 
		// prepare where condition
		RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		Value value = new Value();
		value.setString(collectInfoTable.getColumnValue("note", tmp_rowkey)
				.toString().substring(0, 5));
		ObSimpleCond obSimpleCond = new ObSimpleCond("note",
				ObSimpleCond.ObLogicOperator.LIKE, value);
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
				(byte) '0', 9));
		RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0',
				10));

		query_check_JoinInfo(result, expect_start_rowkey, expect_end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	@Test
	public void test_rowkeyReverseQuery02_01_Limit() {
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			Info.setLimit(-1);
		} catch (RuntimeException e) {
			Assert.assertEquals(
					"limit can not be less than 1 or more than 2000",
					e.getMessage());
		}
	}

	@Test
	public void test_rowkeyReverseQuery02_02_Limit() {
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			Info.setLimit(0);
		} catch (RuntimeException e) {
			Assert.assertEquals(
					"limit can not be less than 1 or more than 2000",
					e.getMessage());
		}
	}

	@Test
	public void test_rowkeyReverseQuery02_03_Limit() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		Info.setLimit(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals(
					"limit can not be less than 1 or more than 2000",
					e.getMessage());
		}
	}

	@Test
	public void test_rowkeyReverseQuery02_04_Limit() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		Info.setLimit(999);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 9));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals(
					"limit can not be less than 1 or more than 2000",
					e.getMessage());
		}
	}

	@Test
	public void test_rowkeyReverseQuery02_05_Limit() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		Info.setLimit(2000);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 9));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals(
					"limit can not be less than 1 or more than 2000",
					e.getMessage());
		}
	}

	@Test
	public void test_rowkeyReverseQuery02_06_Limit() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);

		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			Info.setLimit(2001);
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 9));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals(
					"limit can not be less than 1 or more than 2000",
					e.getMessage());
		}
	}

	@Test
	public void test_rowkeyReverseQuery03_01_PageSize() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);

		Info.setPageNum(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			Info.setPageSize(-1);
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 9));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals("pageSize can not be less than 1",
					e.getMessage());
		}
	}
	
	@Test
	public void test_rowkeyReverseQuery03_02_PageSize() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);

		Info.setPageNum(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			Info.setPageSize(0);
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 9));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals("pageSize can not be less than 1",
					e.getMessage());
		}
	}

	@Test
	public void test_rowkeyReverseQuery03_03_PageSize() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(1);
		Info.setPageNum(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals("pageSize can not be less than 1",
					e.getMessage());
		}
	}
	
	@Test
	public void test_rowkeyReverseQuery03_04_PageSize() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 99));
		
		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setLimit(90);
		Info.setPageSize(99);
		Info.setPageNum(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 99));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals("pageSize can not be less than 1",
					e.getMessage());
		}
	}
	
	@Test
	public void test_rowkeyReverseQuery04_01_PageNum() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			Info.setPageNum(-1);
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 9));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals("pageNum can not be less than 1",
					e.getMessage());
		}
	}
	
	@Test
	public void test_rowkeyReverseQuery04_02_PageNum() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			Info.setPageNum(0);
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 9));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals("pageNum can not be less than 1",
					e.getMessage());
		}
	}
	
	@Test
	public void test_rowkeyReverseQuery04_03_PageNum() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate
		try {
			result = obClient.query(collectInfoTable.getTableName(), Info);

			RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 9));
			RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2,
					(byte) '0', 10));

			query_check_JoinInfo(result, expect_start_rowkey,
					expect_end_rowkey, Info.isInclusiveStart(),
					Info.isInclusiveEnd());

		} catch (RuntimeException e) {
			Assert.assertEquals("pageNum can not be less than 1",
					e.getMessage());
		}
	}
	
	@Test
	public void test_rowkeyReverseQuery04_04_PageNum() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(5);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate

		result = obClient.query(collectInfoTable.getTableName(), Info);

		RKey expect_start_rowkey = new RKey(new CollectInfoKeyRule(2,
				(byte) '0', 1));
		RKey expect_end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0',
				2));

		query_check_JoinInfo(result, expect_start_rowkey, expect_end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());

	}
	
	@Test
	public void test_rowkeyReverseQuery04_05_PageNum() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(6);
		addColumns(Info);
		Info.setScanDirection(Const.SCAN_DIRECTION_BACKWARD); 

		// query operate

		result = obClient.query(collectInfoTable.getTableName(), Info);
		Assert.assertEquals(0, result.getResult().size());
	}
	
	@Test
	public void test_rowkeyReverseQuery05_01_Flag() {
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 10));

		// prepare QueryInfo
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(2);
		Info.setPageNum(1);
		addColumns(Info);
//		Info.setScanDirection(2); //set flag to 2

		// query operate
		try {
			Info.setScanDirection(2); //set flag to 2
	
		} catch (Exception e) {
			Assert.assertEquals("scan direction invalid: 2",
					e.getMessage());
		}
	}
}
