package com.taobao.oceanbase.testcase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.oceanbase.base.BaseCase;
import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Value;
import com.taobao.oceanbase.vo.inner.ObSimpleCond;
import com.taobao.oceanbase.vo.inner.ObSimpleFilter;

public class QueryTestCaseForWhere_06 extends BaseCase {

	private String[] ColumnInfo = collectInfoTable.getAllColumnNames().toArray(
			new String[0]);

	private static void prepareInfo(QueryInfo Info, RKey start_rowkey,
			RKey end_rowkey) {
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(30);
		Info.setPageNum(1);
	}

	private void addColumns(QueryInfo Info) {
		for (int i = 0; i < ColumnInfo.length; i++) {
			Info.addColumn(ColumnInfo[i]);
		}
	}

	private void query_check(Result<List<RowData>> result, RKey start_rowkey,
			RKey end_rowkey, boolean isInclusiveStart, boolean isInclusiveEnd) {
		CollectInfoKeyRule infoRowkeys = ((CollectInfoKeyRule) start_rowkey
				.getKeyRule());
		long length = CollectInfoKeyRule.countNum(start_rowkey, end_rowkey,
				isInclusiveStart, isInclusiveEnd);
		assertEquals(length, result.getResult().size());

		RKey infoRowkey;
		for (int i = 0; i < length; i++) {
			if (i == 0 && isInclusiveStart) {
				infoRowkey = infoRowkeys.currRkey();
			} else {
				infoRowkey = infoRowkeys.nextRkey();
			}

			for (String infoColumn : collectInfoTable.getAllColumnNames()) {
				if(!(infoColumn.equals("gm_create")||infoColumn.equals("gm_modified"))){
				assertEquals(
						collectInfoTable.getColumnValue(infoColumn, infoRowkey),
						result.getResult().get(i).get(infoColumn));
				}
			}
		}
	}

	public Set<String> setInsertColumnsforCollect() {
	        Set<String> Columns = collectInfoTable.getAllColumnNames();
	        Columns.remove("gm_modified");
	        Columns.remove("gm_create");
	        return Columns;
	}
	
	private void prepare_date(int count) {	
		for (int tmp = 0; tmp < count; tmp++) {
			RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', tmp+1));
			InsertMutator im = new InsertMutator(collectInfoTable.getTableName(), rkey);
		
			for (String str : collectInfoTable.getNonJoinColumnNames()) {
				if (!(str.equals("gm_modified")||str.equals("gm_create"))) {
					if(str.equals("is_shared")||str.equals("status")||str.equals("category")||str.equals("owner_id")||str.equals("comment_count")||str.equals("collector_count")||str.equals("collect_count")){
						Value value = new Value();
						value.setNumber((Long) collectInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}else if (str.equals("user_nick")||str.equals("note")||str.equals("tag")||str.equals("title")||str.equals("picurl")||str.equals("owner_nick")||str.equals("proper_xml")){
						Value value = new Value();
						value.setString((String) collectInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}else if(str.equals("collect_time")||str.equals("ends")){
						Value value = new Value();
						value.setSecond((Long) collectInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}else if(str.equals("price")){
						Value value = new Value();
						value.setFloat((Float) collectInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}else if(str.equals("total_money")||str.equals("money")){
						Value value = new Value();
						value.setDouble((Double) collectInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}else if(str.equals("collect_time_precise")){
						Value value = new Value();
						value.setMicrosecond((Long) collectInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}
				}
			}
			Assert.assertTrue("Insert fail!", obClient.insert(im).getResult());
		}
		
		// prepare itemInfo table data
		for (int tmp = 0; tmp < count; tmp++) {
			RKey rkey = new RKey(new ItemInfoKeyRule((byte) '0', tmp+1));
			InsertMutator im = new InsertMutator(itemInfoTable.getTableName(), rkey);
		
			for (String str : itemInfoTable.getAllColumnNames()) {
				if (!(str.equals("gm_modified")||str.equals("gm_create"))) {
					if(str.equals("is_shared")||str.equals("status")||str.equals("category")||str.equals("owner_id")||str.equals("comment_count")||str.equals("collector_count")||str.equals("collect_count")){
						Value value = new Value();
						value.setNumber((Long) itemInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}else if (str.equals("user_nick")||str.equals("note")||str.equals("tag")||str.equals("title")||str.equals("picurl")||str.equals("owner_nick")||str.equals("proper_xml")){
						Value value = new Value();
						value.setString((String) itemInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}else if(str.equals("collect_time")||str.equals("ends")){
						Value value = new Value();
						value.setSecond((Long) itemInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}else if(str.equals("price")){
						Value value = new Value();
						value.setFloat((Float) itemInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}else if(str.equals("total_money")||str.equals("money")){
						Value value = new Value();
						value.setDouble((Double) itemInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}else if(str.equals("collect_time_precise")){
						Value value = new Value();
						value.setMicrosecond((Long) itemInfoTable.getColumnValue(str, rkey));
						im.addColumn(str, value);
					}
				}
			}
			Assert.assertTrue("Insert fail!", obClient.insert(im).getResult());
		}
	}
	
	private void verify_insert_data(int count) {
		// verify insert data collectInfo table data "user_nick", "owner_nick",
		// "category", "collect_count"
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
		for (int tmp = 0; tmp < count; tmp++) {
			Result<RowData> result;
			RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', tmp + 1));
			result = obClient.get(collectInfoTable.getTableName(), rkey,
					ColumnsforCollectInsert);
			// verify result

			for (String infoColumn : ColumnsforCollectInsert) {
				if (!(infoColumn.equals("gm_create") || infoColumn
						.equals("gm_modified")))
					assertEquals(
							collectInfoTable.getColumnValue(infoColumn, rkey),
							result.getResult().get(infoColumn));
			}
		}
	}

	private void delete_data(int count){
		for (int tmp = 0; tmp < count; tmp++) {
			RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', tmp + 1));
			assertTrue("delete failed",obClient.delete(collectInfoTable.getTableName(), rkey).getResult());
		}
		for (int tmp = 0; tmp < count; tmp++) {
			RKey rkey = new RKey(new ItemInfoKeyRule((byte) '0', tmp+1));
			assertTrue("delete failed",obClient.delete(itemInfoTable.getTableName(), rkey).getResult());
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
	public void test_QueryTestCaseForWhere06_01_EQ_and_GT_Same_Column() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey tmp01_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 4));
		RKey tmp02_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 3));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		// delete row data
		delete_data(6);
		
		// prepare query date
		int count = 6;
		prepare_date(count);
		
		// verify insert data
		verify_insert_data(count);
		
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		// prepare where condition
		Value value01 = new Value();
		value01.setNumber((Long) collectInfoTable.getColumnValue("collect_count",tmp01_rowkey));
		ObSimpleCond obSimpleCond01 = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.EQ, value01);
		Value value02 = new Value();
		value02.setNumber((Long) collectInfoTable.getColumnValue("collect_count",tmp02_rowkey));
		ObSimpleCond obSimpleCond02 = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.GT, value02);
		
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond01);
		filter.addCondition(obSimpleCond02);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);
		
		// verify result
		query_check(result, tmp01_rowkey, tmp01_rowkey, true, true);
	}

	@Test
	public void test_QueryTestCaseForWhere06_02_EQ_and_GT_Different_Column() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey tmp01_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 4));
		RKey tmp02_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 3));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		// delete row data
		delete_data(6);
		
		// prepare query date
		int count = 6;
		prepare_date(count);
		
		// verify insert data
		verify_insert_data(count);
		
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		// prepare where condition
		Value value01 = new Value();
		value01.setNumber((Long) collectInfoTable.getColumnValue("collect_count",tmp02_rowkey));
		ObSimpleCond obSimpleCond01 = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.GT, value01);
		Value value02 = new Value();
		value02.setNumber((Long) collectInfoTable.getColumnValue("owner_id",tmp01_rowkey));
		ObSimpleCond obSimpleCond02 = new ObSimpleCond("owner_id",
				ObSimpleCond.ObLogicOperator.EQ, value02);
		
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond01);
		filter.addCondition(obSimpleCond02);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);
		
		// verify result
		query_check(result, tmp01_rowkey, tmp01_rowkey, true, true);
	}
	
	@Test
	public void test_QueryTestCaseForWhere06_03_GT_and_LT_Same_Column_and_Same_Value() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 4));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		// delete row data
		delete_data(6);
		
		// prepare query date
		int count = 6;
		prepare_date(count);
		
		// verify insert data
		verify_insert_data(count);
		
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		// prepare where condition
		Value value01 = new Value();
		value01.setNumber((Long) collectInfoTable.getColumnValue("collect_count",tmp_rowkey));
		ObSimpleCond obSimpleCond01 = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.GT, value01);
		Value value02 = new Value();
		value02.setNumber((Long) collectInfoTable.getColumnValue("collect_count",tmp_rowkey));
		ObSimpleCond obSimpleCond02 = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.LT, value02);
		
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond01);
		filter.addCondition(obSimpleCond02);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);
		Assert.assertEquals(0, result.getResult().size());
	}
	
	@Test
	public void test_QueryTestCaseForWhere06_04_GT_and_LT_Same_Column_and_Different_Value() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey tmp01_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 3));
		RKey tmp02_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 5));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		// delete row data
		delete_data(6);
		
		// prepare query date
		int count = 6;
		prepare_date(count);
		
		// verify insert data
		verify_insert_data(count);
		
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		// prepare where condition
		Value value01 = new Value();
		value01.setNumber((Long) collectInfoTable.getColumnValue("collect_count",tmp01_rowkey));
		ObSimpleCond obSimpleCond01 = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.GT, value01);
		Value value02 = new Value();
		value02.setNumber((Long) collectInfoTable.getColumnValue("collect_count",tmp02_rowkey));
		ObSimpleCond obSimpleCond02 = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.LT, value02);
		
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond01);
		filter.addCondition(obSimpleCond02);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);
		
		// verify result
		RKey expect_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 4));
		query_check(result, expect_rowkey, expect_rowkey, true, true);
	}
	
	@Test
	public void test_QueryTestCaseForWhere06_05_Like_two_Value_From_Different_Column() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 3));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		// delete row data
		delete_data(6);
		
		// prepare query date
		int count = 6;
		prepare_date(count);
		
		// verify insert data
		verify_insert_data(count);
		
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		// prepare where condition
		Value value01 = new Value();
		value01.setString(collectInfoTable.getColumnValue("user_nick",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond01 = new ObSimpleCond("user_nick",
				ObSimpleCond.ObLogicOperator.LIKE, value01);
		Value value02 = new Value();
		value02.setString( collectInfoTable.getColumnValue("owner_nick",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond02 = new ObSimpleCond("owner_nick",
				ObSimpleCond.ObLogicOperator.LIKE, value02);
		
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond01);
		filter.addCondition(obSimpleCond02);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);
		
		// verify result
		query_check(result, tmp_rowkey, tmp_rowkey, true, true);
	}
	
	@Test
	public void test_QueryTestCaseForWhere06_06_Like_two_Value_From_Same_Column() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 3));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		// delete row data
		delete_data(6);
		
		// prepare query date
		int count = 6;
		prepare_date(count);
		
		// verify insert data
		verify_insert_data(count);
		
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		// prepare where condition
		Value value01 = new Value();
		value01.setString(collectInfoTable.getColumnValue("user_nick",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond01 = new ObSimpleCond("user_nick",
				ObSimpleCond.ObLogicOperator.LIKE, value01);
		Value value02 = new Value();
		value02.setString( collectInfoTable.getColumnValue("user_nick",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond02 = new ObSimpleCond("user_nick",
				ObSimpleCond.ObLogicOperator.LIKE, value02);
		
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond01);
		filter.addCondition(obSimpleCond02);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);
		
		// verify result
		query_check(result, tmp_rowkey, tmp_rowkey, true, true);
	}
	
	@Test
	public void test_QueryTestCaseForWhere06_07_Int_Column_EQ_and_GT_String_Value_Same_Column() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 3));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		// delete row data
		delete_data(6);
		
		// prepare query date
		int count = 6;
		prepare_date(count);
		
		// verify insert data
		verify_insert_data(count);
		
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		// prepare where condition
		Value value01 = new Value();
		value01.setString(collectInfoTable.getColumnValue("collect_count",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond01 = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.EQ, value01);
		Value value02 = new Value();
		value02.setString( collectInfoTable.getColumnValue("collect_count",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond02 = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.GT, value02);
		
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond01);
		filter.addCondition(obSimpleCond02);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);
		
		// verify result
		Assert.assertEquals("[-2, invalid argument]", result.getCode().toString());
	}
	
	@Test
	public void test_QueryTestCaseForWhere06_08_Int_Column_EQ_and_GT_String_Value_Different_Column() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 3));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		// delete row data
		delete_data(6);
		
		// prepare query date
		int count = 6;
		prepare_date(count);
		
		// verify insert data
		verify_insert_data(count);
		
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		// prepare where condition
		Value value01 = new Value();
		value01.setString(collectInfoTable.getColumnValue("collect_count",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond01 = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.EQ, value01);
		Value value02 = new Value();
		value02.setString( collectInfoTable.getColumnValue("is_shared",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond02 = new ObSimpleCond("is_shared",
				ObSimpleCond.ObLogicOperator.GT, value02);
		
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond01);
		filter.addCondition(obSimpleCond02);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);
		
		// verify result
		Assert.assertEquals("[-2, invalid argument]", result.getCode().toString());
	}
}
