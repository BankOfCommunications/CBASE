package com.taobao.oceanbase.testcase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.oceanbase.base.BaseCase;
import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.Value;

public class QueryTestCaseForItem extends BaseCase {

	private String[] ColumnInfo = itemInfoTable.getAllColumnNames().toArray(
			new String[0]);

	private static Result<List<RowData>> queryResult;

	private static void prepareInfo(QueryInfo Info, RKey start_rowkey,
			RKey end_rowkey) {
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey); 
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setLimit(2000);
		Info.setPageSize(20);
		Info.setPageNum(1);
	}

	private void addColumns(QueryInfo Info) {
		for (int i = 0; i < ColumnInfo.length; i++) {
			Info.addColumn(ColumnInfo[i]);
		}
	}

	private void query_check_single(RKey rowkey, int i) {
		RowData result_tmp = queryResult.getResult().get(i);
		for (String infoColumn : itemInfoTable.getAllColumnNames()) {
			assertEquals(itemInfoTable.getColumnValue(infoColumn, rowkey),
					result_tmp.get(infoColumn));
		}
	}

	private void query_check_Item(Result<List<RowData>> result,
			RKey start_rowkey, RKey end_rowkey, boolean isInclusiveStart,
			boolean isInclusiveEnd) {
		ItemInfoKeyRule infoRowkeys = ((ItemInfoKeyRule) start_rowkey
				.getKeyRule());
		long length = ItemInfoKeyRule.countNum(start_rowkey, end_rowkey,
				isInclusiveStart, isInclusiveEnd);
		assertEquals(result.getResult().size(), length);

		RKey itemRowkey;
		for (int i = 0; i < length; i++) {
			if (i == 0 && isInclusiveStart) {
				itemRowkey = infoRowkeys.currRkey();
			} else {
				itemRowkey = infoRowkeys.nextRkey();
			}

			for (String itemColumn : itemInfoTable.getAllColumnNames()) {
				assertEquals(
						itemInfoTable.getColumnValue(itemColumn, itemRowkey),
						result.getResult().get(i).get(itemColumn));
			}
		}
	}

	private void query_check_item_case78(Result<List<RowData>> result,
			RKey start_rowkey, RKey end_rowkey, boolean isInclusiveStart,
			boolean isInclusiveEnd) {
		ItemInfoKeyRule infoRowkeys = ((ItemInfoKeyRule) start_rowkey
				.getKeyRule());
		long length = ItemInfoKeyRule.countNum(start_rowkey, end_rowkey,
				isInclusiveStart, isInclusiveEnd);
		assertEquals(result.getResult().size(), length);

		RKey infoRowkey;
		for (int i = 0; i < length; i++) {
			if (i == 0 && isInclusiveStart) {
				infoRowkey = infoRowkeys.currRkey();
			} else {
				infoRowkey = infoRowkeys.nextRkey();
			}

			for (String infoColumn : itemInfoTable.getAllColumnNames()) {
				if (!infoColumn.equals(ColumnInfo[5])) {
					assertEquals(itemInfoTable.getColumnValue(infoColumn,
							infoRowkey),
							result.getResult().get(i).get(infoColumn));
				} else {
					assertEquals(null,
							result.getResult().get(i).get(ColumnInfo[5]));
				}
			}
		}
	}
	@Before
	public void before(){
		obClient.clearActiveMemTale();
	}
	@After
	public void after(){
		obClient.clearActiveMemTale();
	}
	@Test
	public void test_query01Normal() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 1));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 10));

		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		result = obClient.query(itemInfoTable.getTableName(), Info);

		query_check_Item(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	@Test
	public void test_query02NodataNormal() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 6300));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 6310));

		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		result = obClient.query(itemInfoTable.getTableName(), Info);

		assertEquals(true, result.getResult().isEmpty());
	}

	@Test
	public void test_query03TableNameIsNull() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 11));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 20));

		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			result = obClient.query(null, Info);// table is null
		} catch (RuntimeException e) {
			assertEquals("table name null", e.getMessage());}
			//assertEquals("can not find valid mergeserver",e.getMessage());}
	}

	@Test
	public void test_query04TableNameIsBlank() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 21));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 30));

		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			result = obClient.query("", Info);// table is null
		} catch (RuntimeException e) {
			assertEquals("table name null", e.getMessage());}
			//assertEquals("can not find valid mergeserver",e.getMessage());
	}

	@Test
	public void test_query05QueryInfoIsNull() {
		Result<List<RowData>> result;
		try {
			result = obClient.query(itemInfoTable.getTableName(), null);
		} catch (RuntimeException e) {
			assertEquals("query info null", e.getMessage());}
	}
			//assertEquals("can not find valid mergeserver",e.getMessage());

	// the Field startKey of QueryInfo is null
	@Test
	public void test_query06SKNull() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 20));
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(20);
		Info.setPageNum(1);
		addColumns(Info);

		try {
			Info.setStartKey(null);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			assertEquals("start rowkey null", e.getMessage());
		}
	}

	// TODO start_rowkey length less than 9
	@Test
	public void test_query07SKLessThan9() {
		Result<List<RowData>> result1;
		Result<List<RowData>> result2;
		QueryInfo Info1 = new QueryInfo();
		QueryInfo Info2 = new QueryInfo();

		RKey start_rowkey1 = new RKey("001", false);
		RKey start_rowkey2 = new RKey(new ItemInfoKeyRule((byte) '0', 1));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 20));

		prepareInfo(Info1, start_rowkey1, end_rowkey);
		prepareInfo(Info2, start_rowkey2, end_rowkey);

		addColumns(Info1);
		addColumns(Info2);
		result1 = obClient.query(itemInfoTable.getTableName(), Info1);
		result2 = obClient.query(itemInfoTable.getTableName(), Info2);

		Assert.assertEquals(result1.getResult().size(), result2.getResult()
				.size());

		query_check_Item(result1, start_rowkey1, end_rowkey,
				Info1.isInclusiveStart(), Info1.isInclusiveEnd());

		query_check_Item(result2, start_rowkey2, end_rowkey,
				Info2.isInclusiveStart(), Info2.isInclusiveEnd());
	}

	// start_rowkey is blank
	@Test
	public void test_query08SKBlank() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new byte[] {});
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 30));

		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);
		try {
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("rowkey bytes null", e.getMessage());
		}
	}

	// the end_rowkey is NULL
	@Test
	public void test_query09EKIsNull() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 20));
	
		Info.setStartKey(start_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(20);
		Info.setPageNum(1);
		Info.setLimit(2000);
		addColumns(Info);

		try {
			Info.setEndKey(null);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			assertEquals("end rowkey null", e.getMessage());
		}
	}

	// end_rowkey is blank
	@Test
	public void test_query11EKBlank() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 30));
		RKey end_rowkey = new RKey(new byte[] {});
	
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);
		try {
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("rowkey bytes null", e.getMessage());
		}
	}

	@Test
	public void test_query12SKNullEKBlank() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey end_rowkey = new RKey(new byte[] {});
		
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(20);
		Info.setPageNum(1);
		Info.setLimit(2000);
		addColumns(Info);

		try {
			Info.setStartKey(null);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("start rowkey null", e.getMessage());
		}
	}

	// the field startKey is blank,endKey is null
	@Test
	public void test_query13SKBlankEKNull() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new byte[] {});
		
		Info.setStartKey(start_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(20);
		Info.setPageNum(1);
		Info.setLimit(2000);
		addColumns(Info);

		try {
			Info.setEndKey(null);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("end rowkey null", e.getMessage());
		}
	}

	// the field startKey is bigger than endKey
	@Test
	public void test_query14SKBiggerEK() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 61));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 51));

		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(20);
		Info.setPageNum(1);
		Info.setLimit(2000);
		addColumns(Info);

		try {
			Info.setStartKey(start_rowkey);
			Info.setEndKey(end_rowkey);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			assertEquals("start rowkey have to be less than end rowkey",
					e.getMessage());
			//assertEquals("can not find valid mergeserver",e.getMessage());	
			}
	}

	// the field startKey equals endKey,in closed interval
	@Test
	public void test_query15SKEqualEK() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 61));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 61));

		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("start rowkey have to be less than end rowkey",
				e.getMessage());
			//assertEquals("can not find valid mergeserver",e.getMessage());
			}
	}

	// startrowkey equals endkey,
	// startrowkey is in open interval and endkey is in closed interval
	@Test
	public void test_query16SKEqualEK() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 61));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 61));

		prepareInfo(Info, start_rowkey, end_rowkey);
		Info.setInclusiveStart(false);// startrowkey is in open interval
		Info.setInclusiveEnd(true);// endkey is in closed interval
		addColumns(Info);

		try {
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("start rowkey have to be less than end rowkey",
				e.getMessage());
			//assertEquals("can not find valid mergeserver",e.getMessage());
			}
	}

	// startrowkey is in closed interval and endkey is in open interval
	@Test
	public void test_query17PreClosedLastOpen() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 61));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 70));

		prepareInfo(Info, start_rowkey, end_rowkey);
		Info.setInclusiveStart(true);// startrowkey is in closed interval
		Info.setInclusiveEnd(false);// endkey is in open interval
		addColumns(Info);

		result = obClient.query(itemInfoTable.getTableName(), Info);

		query_check_Item(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	// startrowkey is in open interval and endkey is in open interval
	@Test
	public void test_query18PreOpenLastOpen() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 71));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 80));

		prepareInfo(Info, start_rowkey, end_rowkey);
		Info.setInclusiveStart(false);// startrowkey is in open interval
		Info.setInclusiveEnd(false);// endkey is in open interval
		addColumns(Info);

		result = obClient.query(itemInfoTable.getTableName(), Info);

		query_check_Item(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	// Columns is null, there is no column here
	@Test
	public void test_query19NoColumns() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 81));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 90));

		prepareInfo(Info, start_rowkey, end_rowkey);

		try {
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("columns are null", e.getMessage());
		//	assertEquals("can not find valid mergeserver",e.getMessage());
			}
	}

	// there are some incorrect columns
	@Test
	public void test_query20IncorrectColumns() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 91));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 100));
	
		prepareInfo(Info, start_rowkey, end_rowkey);

		Info.addColumn("column1_wrong");
		Info.addColumn(ColumnInfo[0]);
		Info.addColumn(ColumnInfo[1]);
		Info.addColumn(ColumnInfo[2]);
		Info.addColumn("column5_wrong");
		Info.addColumn(ColumnInfo[3]);
		Info.addColumn(ColumnInfo[4]);
		Info.addColumn(ColumnInfo[5]);
		for (int i = 8; i < ColumnInfo.length; i++)
			Info.addColumn(ColumnInfo[i]);

		result = obClient.query(itemInfoTable.getTableName(), Info);
		Assert.assertEquals("[-29, request not conform to schema]", result
				.getCode().toString());
	}

	// there are some columns whose name are null
	@Test
	public void test_query21ColumnNull() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 101));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 110));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		try {
			Info.addColumn(null);
			Info.addColumn(ColumnInfo[0]);
			Info.addColumn(ColumnInfo[1]);
			Info.addColumn(ColumnInfo[2]);
			Info.addColumn(null);
			for (int i = 5; i < ColumnInfo.length; i++)
				Info.addColumn(ColumnInfo[i]);

			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// there are some columns whose name are blank
	@Test
	public void test_query22Blank() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 111));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 120));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		try {
			Info.addColumn("");
			Info.addColumn(ColumnInfo[0]);
			Info.addColumn(ColumnInfo[1]);
			Info.addColumn(ColumnInfo[2]);
			Info.addColumn("");
			for (int i = 5; i < ColumnInfo.length; i++)
				Info.addColumn(ColumnInfo[i]);

			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// there are some columns whose name are null,some are wrong
	@Test
	public void test_query23WrongAndNull() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 121));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 130));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		try {
			Info.addColumn("column1_wrong");
			Info.addColumn(ColumnInfo[0]);
			Info.addColumn("column3_wrong");
			Info.addColumn(ColumnInfo[1]);
			Info.addColumn(null);
			Info.addColumn(ColumnInfo[3]);
			Info.addColumn(ColumnInfo[2]);
			Info.addColumn(null);
			for (int i = 8; i < ColumnInfo.length; i++)
				Info.addColumn(ColumnInfo[i]);

			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// there are some columns whose name are null,some are blank
	@Test
	public void test_query24NullAndBlank() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 131));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 140));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		try {
			Info.addColumn("");
			Info.addColumn(ColumnInfo[0]);
			Info.addColumn(ColumnInfo[2]);
			Info.addColumn("");
			Info.addColumn(ColumnInfo[1]);
			Info.addColumn(null);
			Info.addColumn(ColumnInfo[3]);
			Info.addColumn(null);
			for (int i = 8; i < ColumnInfo.length; i++)
				Info.addColumn(ColumnInfo[i]);

			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// there are some columns whose name are wrong,some are blank
	@Test
	public void test_query25WrongAndBlank() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 141));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 150));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		try {
			Info.addColumn("column1_wrong");
			Info.addColumn(ColumnInfo[0]);
			Info.addColumn("column3_wrong");
			Info.addColumn(ColumnInfo[1]);
			Info.addColumn(ColumnInfo[2]);
			Info.addColumn("");
			Info.addColumn(ColumnInfo[3]);
			Info.addColumn("");
			for (int i = 8; i < ColumnInfo.length; i++)
				Info.addColumn(ColumnInfo[i]);

			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// some columns' name null,wrong,blank
	@Test
	public void test_query26WrongAndNullAndBlank() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 151));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 160));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		try {
			Info.addColumn("");
			Info.addColumn(ColumnInfo[0]);
			Info.addColumn("");
			Info.addColumn(ColumnInfo[1]);
			Info.addColumn(null);
			Info.addColumn(ColumnInfo[2]);
			Info.addColumn(ColumnInfo[3]);
			Info.addColumn("column8_wrong");
			for (int i = 8; i < ColumnInfo.length; i++)
				Info.addColumn(ColumnInfo[i]);

			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// check the order function,reverse order
	@Test
	public void test_query27Reverse() {
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 161));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 170));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		addColumns(Info);

		Info.addOrderBy(ColumnInfo[5], false);// take the first column，reverse
												// order
		queryResult = obClient.query(itemInfoTable.getTableName(), Info);

		long length = ItemInfoKeyRule.countNum(start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
		assertEquals(queryResult.getResult().size(), length);

		ItemInfoKeyRule rowkeys = (ItemInfoKeyRule) end_rowkey.getKeyRule();

		query_check_single(rowkeys.currRkey(), 0);
		for (int i = 1; i < length; i++) {
			query_check_single(rowkeys.preRkey(), i);
		}

		// verfify the order
		String first = ((RowData) queryResult.getResult().get(0))
				.get(ColumnInfo[5]);
		int index;
		for (index = 1; index < length; index++) {
			String next = ((RowData) queryResult.getResult().get(index))
					.get(ColumnInfo[5]);
			if (first.compareToIgnoreCase(next) >= 0) {
				first = next;
			} else {
				break;
			}
		}
		Assert.assertEquals(length, index);
	}

	// check the order function,positive order
	@Test
	public void test_query28Positive() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 171));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 180));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		addColumns(Info);

		Info.addOrderBy(ColumnInfo[5], true);// take the first column，positive
												// order
		result = obClient.query(itemInfoTable.getTableName(), Info);

		query_check_Item(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());

		// verfify the order
		String first = ((RowData) result.getResult().get(0)).get(ColumnInfo[5]);
		int length = result.getResult().size();
		int index;
		for (index = 1; index < length; index++) {
			String next = ((RowData) result.getResult().get(index))
					.get(ColumnInfo[5]);
			if (first.compareToIgnoreCase(next) <= 0) {
				first = next;
			} else {
				break;
			}
		}
		Assert.assertEquals(length, index);
	}

	// check the order function,multiColumns order,first third
	// reverse,second positive
	@Test
	public void test_query29MutiColumns() {
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 181));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 190));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		addColumns(Info);

		Info.addOrderBy(ColumnInfo[5], false);
		Info.addOrderBy(ColumnInfo[6], true);
		Info.addOrderBy(ColumnInfo[4], false);

		queryResult = obClient.query(itemInfoTable.getTableName(), Info);

		long length = ItemInfoKeyRule.countNum(start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
		assertEquals(queryResult.getResult().size(), length);

		ItemInfoKeyRule rowkeys = (ItemInfoKeyRule) end_rowkey.getKeyRule();

		query_check_single(rowkeys.currRkey(), 0);

		for (int i = 1; i < length; i++) {
			query_check_single(rowkeys.preRkey(), i);
		}

		// verfify the order
		String first = ((RowData) queryResult.getResult().get(0))
				.get(ColumnInfo[5]);
		int index;
		List<Integer> sameFirst = new ArrayList<Integer>();
		List<Integer> sameSecond = new ArrayList<Integer>();
		for (index = 1; index < length; index++) {
			String next = ((RowData) queryResult.getResult().get(index))
					.get(ColumnInfo[5]);
			if (first.compareToIgnoreCase(next) > 0) {
				first = next;
			} else if (first.compareToIgnoreCase(next) == 0) {
				sameFirst.add(index);
			} else {
				break;
			}
		}

		Assert.assertEquals(queryResult.getResult().size(), index);

		if (!sameFirst.isEmpty()) {
			String is_shared = ((RowData) queryResult.getResult().get(0))
					.get(ColumnInfo[6]);
			int length2 = sameFirst.size();
			for (index = 0; index < length2; index++) {
				String next = ((RowData) queryResult.getResult().get(
						sameFirst.get(index))).get(ColumnInfo[6]);
				if (is_shared.compareToIgnoreCase(next) < 0) {
					is_shared = next;
				} else if (is_shared.compareToIgnoreCase(next) == 0) {
					sameSecond.add(index);
				} else {
					break;
				}
			}

			Assert.assertEquals(length2, index);

			if (!sameSecond.isEmpty()) {
				String note = ((RowData) queryResult.getResult().get(0))
						.get(ColumnInfo[4]);
				int length3 = sameSecond.size();
				for (index = 0; index < length3; index++) {
					String next = ((RowData) queryResult.getResult().get(
							sameSecond.get(index))).get(ColumnInfo[4]);
					if (note.compareToIgnoreCase(next) >= 0) {
						note = next;
					} else {
						break;
					}
				}
				Assert.assertEquals(length3, index);
			}
		}
	}

	// there is an incorrect column in orderBy
	@Test
	public void test_query30OneWrong() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '1', 191));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 200));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		Info.addOrderBy("wrong_column1", false); // the reverse order
		result = obClient.query(itemInfoTable.getTableName(), Info);
		Assert.assertEquals("[-29, request not conform to schema]", result
				.getCode().toString());
	}

	@Test
	public void test_query31OneNull() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 201));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 210));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.addOrderBy(null, false);// the reverse order
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// the field orderBy is blank
	@Test
	public void test_query32OneBlank() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 211));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 220));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.addOrderBy("", false);// the reverse order
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// OrderBy field:One correct,One incorrect,the return is empty
	@Test
	public void test_query33OneToOneWrong() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 221));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 230));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		Info.addOrderBy("wrong_column1", false);
		Info.addOrderBy(ColumnInfo[2], false);

		result = obClient.query(itemInfoTable.getTableName(), Info);

		Assert.assertEquals("[-29, request not conform to schema]", result
				.getCode().toString());
	}

	// one is null,the other is correct
	@Test
	public void test_query34OneNullOneCorrect() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 231));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 240));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.addOrderBy(null, false);
			Info.addOrderBy(ColumnInfo[2], false);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// one is blank,the other is correct
	@Test
	public void test_query35OneBlankOneCorrect() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 241));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 250));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);
		try {
			Info.addOrderBy("", false);
			Info.addOrderBy(ColumnInfo[2], false);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// one wrong,the other two are correct
	@Test
	public void test_query36OneWrongTwoCorrect() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 251));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 260));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		Info.addOrderBy("wrong_column1", false);
		Info.addOrderBy(ColumnInfo[2], false);
		Info.addOrderBy(ColumnInfo[3], false);

		result = obClient.query(itemInfoTable.getTableName(), Info);
		Assert.assertEquals("[-29, request not conform to schema]", result
				.getCode().toString());
	}

	// one null,the other two are correct
	@Test
	public void test_query37OneNullTwoCorrect() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 261));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 270));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.addOrderBy(ColumnInfo[2], false);
			Info.addOrderBy(ColumnInfo[3], false);
			Info.addOrderBy(null, false);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// one Blank,the other two are correct
	@Test
	public void test_query38OneBlankTwoCorrect() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 271));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 280));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.addOrderBy(ColumnInfo[2], false);
			Info.addOrderBy(ColumnInfo[3], false);
			Info.addOrderBy("", false);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// One wrong,one is null,one is correct
	@Test
	public void test_query39WrongNullCorrect() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 281));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 290));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.addOrderBy(null, false);
			Info.addOrderBy("wrong_column1", false);
			Info.addOrderBy(ColumnInfo[3], false);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// One Null,one is blank,one is correct
	@Test
	public void test_query40NullBlankCorrect() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 291));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 300));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.addOrderBy(null, false);
			Info.addOrderBy("", false);
			Info.addOrderBy(ColumnInfo[3], false);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	@Test
	public void test_query41XRepeatTwoColumn() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '2', 291));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 300));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		Info.addColumn(ColumnInfo[5]);
		Info.addColumn(ColumnInfo[5]);

		result = obClient.query(itemInfoTable.getTableName(), Info);

		long length = ItemInfoKeyRule.countNum(start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
		ItemInfoKeyRule rowkeys = (ItemInfoKeyRule) start_rowkey.getKeyRule();

		assertEquals(result.getResult().size(), length);

		assertEquals(
				itemInfoTable.getColumnValue(ColumnInfo[5], rowkeys.currRkey()),
				result.getResult().get(0).get(ColumnInfo[5]));

		// return only one column,the other column return null
		for (int i = 1; i < length; i++) {
			assertEquals(
					itemInfoTable.getColumnValue(ColumnInfo[5],
							rowkeys.nextRkey()),
					result.getResult().get(i).get(ColumnInfo[5]));
			for (String infoColumn : itemInfoTable.getAllColumnNames()) {
				if (!(infoColumn == ColumnInfo[5])) {
					assertEquals(null, result.getResult().get(i)
							.get(infoColumn));// TODO
				}
			}
		}
	}

	// One wrong,one is blank,one is correct
	@Test
	public void test_query41WrongblankCorrect() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 301));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 310));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.addOrderBy("", false);
			Info.addOrderBy("wrong_column1", false);
			Info.addOrderBy(ColumnInfo[2], false);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("column name null", e.getMessage());
		}
	}

	// pageSize=0
	@Test
	public void test_query63PageSizeZero() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 311));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 320));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.setPageSize(0);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("pageSize can not be less than 1",
					e.getMessage());
		}
	}

	// pageSize is less than 0
	@Test
	public void test_query64PageSizeLessThanZero() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 321));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 330));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.setPageSize(-2);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("pageSize can not be less than 1",
					e.getMessage());
		}
	}

	// pageSize=limit
	@Test
	public void test_query65PageSizeEqualLimit() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 331));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 340));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		Info.setPageSize(Info.getLimit());
		result = obClient.query(itemInfoTable.getTableName(), Info);
		query_check_Item(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	// pageSize>limit
	@Test
	public void test_query66PageSizeMoreThanLimit() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 341));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 350));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		Info.setLimit(20);// for test
		Info.setPageSize(Info.getLimit() + 1);
		addColumns(Info);

		result = obClient.query(itemInfoTable.getTableName(), Info);

		query_check_Item(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	// pageSize>limit
	@Test
	public void test_query66PageSizeAndResultMoreThanLimit() {
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 341));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 370));
	
		prepareInfo(Info, start_rowkey, end_rowkey);

		Info.setLimit(20);// for test
		addColumns(Info);

		Info.setPageSize(Info.getLimit() + 1);
		queryResult = obClient.query(itemInfoTable.getTableName(), Info);

		Assert.assertEquals(Info.getLimit(), queryResult.getResult().size());

		ItemInfoKeyRule infoRowkeys = ((ItemInfoKeyRule) start_rowkey
				.getKeyRule());
		long length = ItemInfoKeyRule.countNum(start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
		boolean equals = (length == queryResult.getResult().size());

		Assert.assertFalse(equals);

		query_check_single(infoRowkeys.currRkey(), 0);
		for (int i = 1; i < Info.getLimit(); i++) {
			query_check_single(infoRowkeys.nextRkey(), i);
		}
	}

	// pageNum=0
	@Test
	public void test_query67PageNumEqualZero() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 351));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 360));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.setPageNum(0);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("pageNum can not be less than 1",
					e.getMessage());
		}
	}

	// pageNum<0
	@Test
	public void test_query68PageNumLessThanZero() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 361));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 370));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.setPageNum(-1);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals("pageNum can not be less than 1",
					e.getMessage());
		}
	}

	@Test
	public void test_query69NumMultiplySizeLessThanFindCount() {

		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 371));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 495));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		Info.setLimit(40);
		Info.setPageSize(10);
		Info.setPageNum(10);// 10*19>20

		queryResult = obClient.query(itemInfoTable.getTableName(), Info);

		ItemInfoKeyRule rowkeys = new ItemInfoKeyRule((byte) '4', 461);

		assertEquals(queryResult.getResult().size(), Info.getPageSize());

		query_check_single(rowkeys.currRkey(), 0);
		for (int i = 1; i < queryResult.getResult().size(); i++) {
			query_check_single(rowkeys.nextRkey(), i);
		}
	}

	@Test
	public void test_query69ProductMoreThanFindCount() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 371));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 380));
	
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		Info.setLimit(20);
		Info.setPageSize(20);
		Info.setPageNum(50);// 200*50>20

		result = obClient.query(itemInfoTable.getTableName(), Info);

		Assert.assertEquals(true, result.getResult().isEmpty());
	}

	// Limit=0
	@Test
	public void test_query70LimitEqualZero() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 381));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 390));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.setLimit(0);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals(
					"limit can not be less than 1 or more than 2000",
					e.getMessage());
		}
	}

	// Limit<0
	@Test
	public void test_query71LimitLessThanZero() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '3', 391));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 400));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.setLimit(-5);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals(
					"limit can not be less than 1 or more than 2000",
					e.getMessage());
		}
	}

	// limit more than 2000
	@Test
	public void test_query72LimitMoreThan2000() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 401));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 410));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		try {
			Info.setLimit(2005);
			result = obClient.query(itemInfoTable.getTableName(), Info);
		} catch (RuntimeException e) {
			Assert.assertEquals(
					"limit can not be less than 1 or more than 2000",
					e.getMessage());
		}
	}

	// TODO Abnormal data ,the first and third are null
	@Test
	public void test_query73OrderBy() {
		String table = itemInfoTable.getTableName();
		RKey rowkey1 = new RKey(new ItemInfoKeyRule((byte) '4', 403));
		RKey rowkey2 = new RKey(new ItemInfoKeyRule((byte) '4', 405));

		UpdateMutator rowMutator1 = new UpdateMutator(table, rowkey1);
		UpdateMutator rowMutator2 = new UpdateMutator(table, rowkey2);
		Value value;
		value = new Value();
		
		//the fifth column has null value
		rowMutator1.addColumn(ColumnInfo[5], value, false);
		rowMutator2.addColumn(ColumnInfo[5], value, false);
		
		Assert.assertTrue("update falied", obClient.update(rowMutator1)
				.getResult());
		assertTrue("update falied", obClient.update(rowMutator2).getResult());

		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 401));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 410));
		
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		Info.addOrderBy(ColumnInfo[5], true);// positive sequence
		result = obClient.query(itemInfoTable.getTableName(), Info);

		// priority of NULL is the first
		Assert.assertEquals(result.getResult().get(0).getRowKey().getRowkey(),
				new RKey(new ItemInfoKeyRule((byte) '4', 403)).toString());

		Assert.assertEquals(result.getResult().get(1).getRowKey().getRowkey(),
				new RKey(new ItemInfoKeyRule((byte) '4', 405)).toString());

		Assert.assertEquals(result.getResult().get(2).getRowKey().getRowkey(),
				new RKey(new ItemInfoKeyRule((byte) '4', 401)).toString());

		Assert.assertEquals(result.getResult().get(3).getRowKey().getRowkey(),
				new RKey(new ItemInfoKeyRule((byte) '4', 402)).toString());

		Assert.assertEquals(result.getResult().get(4).getRowKey().getRowkey(),
				new RKey(new ItemInfoKeyRule((byte) '4', 404)).toString());

		Assert.assertEquals(result.getResult().get(5).getRowKey().getRowkey(),
				new RKey(new ItemInfoKeyRule((byte) '4', 406)).toString());

		Assert.assertEquals(result.getResult().get(6).getRowKey().getRowkey(),
				new RKey(new ItemInfoKeyRule((byte) '4', 407)).toString());
		Assert.assertEquals(result.getResult().get(7).getRowKey().getRowkey(),
				new RKey(new ItemInfoKeyRule((byte) '4', 408)).toString());
		Assert.assertEquals(result.getResult().get(8).getRowKey().getRowkey(),
				new RKey(new ItemInfoKeyRule((byte) '4', 409)).toString());
		Assert.assertEquals(result.getResult().get(9).getRowKey().getRowkey(),
				new RKey(new ItemInfoKeyRule((byte) '4', 410)).toString());

	}

	// the last column repeat
	@Test
	public void test_query76LastColumnRepeat() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 411));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 420));

		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);
		Info.addColumn(ColumnInfo[ColumnInfo.length - 1]);

		result = obClient.query(itemInfoTable.getTableName(), Info);
		query_check_Item(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	// the mid column repeat , and surplus a column
	@Test
	public void test_query77MidColumnRepeat() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 421));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 430));
		prepareInfo(Info, start_rowkey, end_rowkey);

		for (int i = 0; i < 5; i++)
			Info.addColumn(ColumnInfo[i]);
		Info.addColumn(ColumnInfo[4]);// the mid column is picur1
		for (int i = 5; i < ColumnInfo.length; i++)
			Info.addColumn(ColumnInfo[i]);

		result = obClient.query(itemInfoTable.getTableName(), Info);

		query_check_Item(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	// the mid column repeat , and the next is missing ,
	@Test
	public void test_query78MidColumnRepeatViciLostForItem() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 431));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 440));
	
		prepareInfo(Info, start_rowkey, end_rowkey);

		for (int i = 0; i < 5; i++)
			Info.addColumn(ColumnInfo[i]);
		Info.addColumn(ColumnInfo[4]);// the mid column is picur1
		for (int i = 6; i < ColumnInfo.length; i++)
			// the next column miss
			Info.addColumn(ColumnInfo[i]);

		result = obClient.query(itemInfoTable.getTableName(), Info);

		query_check_item_case78(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	@Test
	public void test_query79ColumnsOutOFOrder() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 441));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 450));
		
		prepareInfo(Info, start_rowkey, end_rowkey);

		Info.addColumn(ColumnInfo[6]);
		Info.addColumn(ColumnInfo[2]);
		Info.addColumn(ColumnInfo[5]);
		Info.addColumn(ColumnInfo[4]);
		Info.addColumn(ColumnInfo[1]);
		Info.addColumn(ColumnInfo[9]);
		Info.addColumn(ColumnInfo[8]);
		Info.addColumn(ColumnInfo[7]);
		Info.addColumn(ColumnInfo[0]);
		Info.addColumn(ColumnInfo[3]);
		for (int i = 9; i < ColumnInfo.length; i++)
			Info.addColumn(ColumnInfo[i]);

		result = obClient.query(itemInfoTable.getTableName(), Info);

		query_check_Item(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}
	
	@Test
	public void test_query80PageNumNoSet() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 451));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 460));

		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setLimit(2000);
		Info.setPageSize(20);
		// Info.setPageNum(1);

		addColumns(Info);

		try {
			result = obClient.query(itemInfoTable.getTableName(), Info);
			query_check_Item(result, start_rowkey, end_rowkey,
					Info.isInclusiveStart(), Info.isInclusiveEnd());
		} catch (RuntimeException e) {
			assertEquals(
					"both pageNum and pageSize have to be set at the same time",
					e.getMessage());
			//assertEquals("can not find valid mergeserver",e.getMessage());
			}
	}

	@Test
	public void test_query81PageNumAndPageSizeNoSet() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 461));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 470));

		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setLimit(2000);
		// Info.setPageSize(20);
		// Info.setPageNum(1);

		addColumns(Info);

		result = obClient.query(itemInfoTable.getTableName(), Info);
		query_check_Item(result, start_rowkey, end_rowkey,
				Info.isInclusiveStart(), Info.isInclusiveEnd());
	}

	@Test
	public void test_query82PageNumAndPageSizeNoSet() {
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 471));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '4', 480));

		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setLimit(5);
		// Info.setPageSize(20);
		// Info.setPageNum(1);

		addColumns(Info);

		queryResult = obClient.query(itemInfoTable.getTableName(), Info);
		
		ItemInfoKeyRule rowkeys = (ItemInfoKeyRule) start_rowkey
				.getKeyRule();
		
		assertEquals(queryResult.getResult().size(), Info.getLimit());

		query_check_single(rowkeys.currRkey(), 0);
		for (int i = 1; i < Info.getLimit(); i++) {
			query_check_single(rowkeys.nextRkey(), i);
		}
	}
}
