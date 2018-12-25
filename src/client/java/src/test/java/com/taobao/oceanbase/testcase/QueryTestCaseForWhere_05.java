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

public class QueryTestCaseForWhere_05 extends BaseCase {

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
		assertEquals(result.getResult().size(), length);

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
	
	private void prepare_date(int count, int[] intArray, String[] stringArray, String columns[]) {	
		// prepare collectInfo table data "user_nick", "owner_nick", "category", "collect_count"
		for (int tmp = 0; tmp < count; tmp++) {
			RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', tmp+1));
			InsertMutator im = new InsertMutator(collectInfoTable.getTableName(), rkey);
			Value value01 = new Value();
			value01.setString(stringArray[tmp]);
			
			Value value02 = new Value();
			value02.setNumber(intArray[tmp]);
			
			im.addColumn(columns[0], value01);
			im.addColumn(columns[2], value02);
			
		
			for (String str : collectInfoTable.getNonJoinColumnNames()) {
				if (!(str.equals(columns[0])||str.equals(columns[2])||str.equals("gm_modified")||str.equals("gm_create"))) {
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
			Value value01 = new Value();
			value01.setString(stringArray[tmp]);
			
			Value value02 = new Value();
			value02.setNumber(intArray[tmp]);
			
			im.addColumn(columns[1], value01);
			im.addColumn(columns[3], value02);
			
		
			for (String str : itemInfoTable.getAllColumnNames()) {
				if (!(str.equals(columns[1])||str.equals(columns[3])||str.equals("gm_modified")||str.equals("gm_create"))) {
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
	
	private void prepare_null_date(String columns[], int RkeyNum) {
		// prepare collectInfo table data "user_nick", "owner_nick", "category",
		// "collect_count"
		RKey rkey_collect = new RKey(new CollectInfoKeyRule(2, (byte) '0', RkeyNum));
			InsertMutator im = new InsertMutator(collectInfoTable.getTableName(), rkey_collect);
			
		for (String str : collectInfoTable.getNonJoinColumnNames()) {
			if (!(str.equals(columns[0]) || str.equals(columns[1])
					|| str.equals("gm_modified") || str.equals("gm_create"))) {
				if (str.equals("is_shared") || str.equals("status")
						|| str.equals("category") || str.equals("owner_id")
						|| str.equals("comment_count")
						|| str.equals("collector_count")
						|| str.equals("collect_count")) {
					Value value = new Value();
					value.setNumber((Long) collectInfoTable.getColumnValue(str,
							rkey_collect));
					im.addColumn(str, value);
				} else if (str.equals("user_nick")||str.equals("note") || str.equals("tag")
						|| str.equals("title") || str.equals("picurl")
						|| str.equals("owner_nick") || str.equals("proper_xml")) {
					Value value = new Value();
					value.setString((String) collectInfoTable.getColumnValue(
							str, rkey_collect));
					im.addColumn(str, value);
				} else if (str.equals("collect_time") || str.equals("ends")) {
					Value value = new Value();
					value.setSecond((Long) collectInfoTable.getColumnValue(str,
							rkey_collect));
					im.addColumn(str, value);
				} else if (str.equals("price")) {
					Value value = new Value();
					value.setFloat((Float) collectInfoTable.getColumnValue(str,
							rkey_collect));
					im.addColumn(str, value);
				} else if (str.equals("total_money") || str.equals("money")) {
					Value value = new Value();
					value.setDouble((Double) collectInfoTable.getColumnValue(
							str, rkey_collect));
					im.addColumn(str, value);
				} else if (str.equals("collect_time_precise")) {
					Value value = new Value();
					value.setMicrosecond((Long) collectInfoTable
							.getColumnValue(str, rkey_collect));
					im.addColumn(str, value);
				}
			}
		}
		Assert.assertTrue("Insert fail!", obClient.insert(im).getResult());
		
		
		// prepare itemInfo table data
		RKey rkey_item = new RKey(new ItemInfoKeyRule((byte) '0', RkeyNum));
		InsertMutator im02 = new InsertMutator(itemInfoTable.getTableName(),
				rkey_item);
		
		for (String str : itemInfoTable.getAllColumnNames()) {
			if (!(str.equals(columns[0]) || str.equals(columns[1])
					|| str.equals("gm_modified") || str.equals("gm_create"))) {
				if (str.equals("is_shared") || str.equals("status")
						|| str.equals("category") || str.equals("owner_id")
						|| str.equals("comment_count")
						|| str.equals("collector_count")
						|| str.equals("collect_count")) {
					Value value = new Value();
					value.setNumber((Long) itemInfoTable.getColumnValue(str,
							rkey_item));
					im02.addColumn(str, value);
				} else if (str.equals("user_nick")||str.equals("note") || str.equals("tag")
						|| str.equals("title") || str.equals("picurl")
						|| str.equals("owner_nick") || str.equals("proper_xml")) {
					Value value = new Value();
					value.setString((String) itemInfoTable.getColumnValue(str,
							rkey_item));
					im02.addColumn(str, value);
				} else if (str.equals("collect_time") || str.equals("ends")) {
					Value value = new Value();
					value.setSecond((Long) itemInfoTable.getColumnValue(str,
							rkey_item));
					im02.addColumn(str, value);
				} else if (str.equals("price")) {
					Value value = new Value();
					value.setFloat((Float) itemInfoTable.getColumnValue(str,
							rkey_item));
					im02.addColumn(str, value);
				} else if (str.equals("total_money") || str.equals("money")) {
					Value value = new Value();
					value.setDouble((Double) itemInfoTable.getColumnValue(str,
							rkey_item));
					im02.addColumn(str, value);
				} else if (str.equals("collect_time_precise")) {
					Value value = new Value();
					value.setMicrosecond((Long) itemInfoTable.getColumnValue(
							str, rkey_item));
					im02.addColumn(str, value);
				}
			}
		}
		Assert.assertTrue("Insert fail!", obClient.insert(im02).getResult());
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
	
	private void verify_insert_data(int count, int[] intArray,String[] stringArray, String columns[]) {
		// verify insert data collectInfo table data "user_nick", "owner_nick", "category", "collect_count"
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
		for (int tmp = 0; tmp < count; tmp++) {
			Result<RowData> result;
			RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', tmp + 1));
			result = obClient.get(collectInfoTable.getTableName(), rkey, ColumnsforCollectInsert);
			// verify result

			for (String infoColumn : ColumnsforCollectInsert) {
				if (infoColumn.equals(columns[0])|| infoColumn.equals(columns[1])) {
					assertEquals(stringArray[tmp], result.getResult().get(infoColumn));
				} else if (infoColumn.equals(columns[2])|| infoColumn.equals(columns[3])) {
					assertEquals(new Long((long)intArray[tmp]), result.getResult().get(infoColumn));
				} else {
					if (!(infoColumn.equals("gm_create") || infoColumn.equals("gm_modified")))
						assertEquals(collectInfoTable.getColumnValue(infoColumn, rkey), result.getResult().get(infoColumn));
				}
			}
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

	private void verify_insert_null_data(String columns[], int RkeyNum){
		// verify insert null data: collectInfo table data "user_nick", "owner_nick", "category", "collect_count"
		Result<RowData> result;
		RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', RkeyNum));
		Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
		result = obClient.get(collectInfoTable.getTableName(), rkey, ColumnsforCollectInsert);
		for (String infoColumn : ColumnsforCollectInsert) {
			if (infoColumn.equals(columns[0])|| infoColumn.equals(columns[1])) {
				Assert.assertNull(result.getResult().get(infoColumn));
			} else {
				if (!(infoColumn.equals("gm_create") || infoColumn.equals("gm_modified")))
					assertEquals(collectInfoTable.getColumnValue(infoColumn, rkey), result.getResult().get(infoColumn));
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
	public void test_QueryTestCaseForWhere01_Happy_Path() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		// delete row data
		delete_data(7);
		// prepare query data
		int count = 6;
		int intArray[] = {1, 2, 3, 4, 5, 6};
		String stringArray[] = {"@", "$", "S", "*", "&" , ")"};
		String columns[] = {"user_nick", "owner_nick", "category", "collect_count"};  
		prepare_date(count, intArray, stringArray, columns);
		// verify insert data
		verify_insert_data(count, intArray, stringArray, columns);
		
		// prepare null data
		String null_columns[] = {"user_nick", "owner_nick"} ;
		prepare_null_date(null_columns, 7);
		
		// verify insert null data
		verify_insert_null_data(null_columns, 7);
		
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		addColumns(Info);

		// prepare where condition
		Value value = new Value();
		value.setString("$");
		ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick",
				ObSimpleCond.ObLogicOperator.LIKE, value);
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		// verify result
		RKey expect_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 2));
		assertEquals(result.getResult().size(), 1);
		for (String infoColumn : collectInfoTable.getAllColumnNames()) {
			if (infoColumn.equals(columns[0]) || infoColumn.equals(columns[1])) {
				assertEquals("$", result.getResult().get(0).get(infoColumn));
			} else if (infoColumn.equals(columns[2])
					|| infoColumn.equals(columns[3])) {
				assertEquals(2L, result.getResult().get(0).get(infoColumn));
			} else {
				if(infoColumn.equals("user_nick")||infoColumn.equals("is_shared")||infoColumn.equals("note")||infoColumn.equals("collect_time")||infoColumn.equals("status")||infoColumn.equals("tag")||infoColumn.equals("category")||infoColumn.equals("collect_time_precise"))
					assertEquals(collectInfoTable.getColumnValue(infoColumn,expect_rowkey), result.getResult().get(0).get(infoColumn));
				else if(!(infoColumn.equals("gm_create")||infoColumn.equals("gm_modified")))
					assertEquals(collectInfoTable.getColumnValue(infoColumn,expect_rowkey), result.getResult().get(0).get(infoColumn));
			}
		}
	}

	
	@Test
	public void test_QueryTestCaseForWhere01_01_LE() {
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
		Value value = new Value();
		value.setNumber((Long) collectInfoTable.getColumnValue("collect_count",tmp_rowkey));
		ObSimpleCond obSimpleCond = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.LE, value);
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		// verify result
		query_check(result, start_rowkey, tmp_rowkey, true, true);
	}

	@Test
	public void test_QueryTestCaseForWhere01_02_GE() {
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
		Value value = new Value();
		value.setNumber((Long) collectInfoTable.getColumnValue("collect_count",tmp_rowkey));
		ObSimpleCond obSimpleCond = new ObSimpleCond("collect_count",
				ObSimpleCond.ObLogicOperator.GE, value);
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		// verify result
		query_check(result, tmp_rowkey, end_rowkey, true, true);
		
	}
	
	@Test
	public void test_QueryTestCaseForWhere01_03_Like_String() {
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
		Value value = new Value();
		value.setString(collectInfoTable.getColumnValue("user_nick",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick",
				ObSimpleCond.ObLogicOperator.LIKE, value);
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		// verify result
		query_check(result, tmp_rowkey, tmp_rowkey, true, true);
	}
	
	@Test
	public void test_QueryTestCaseForWhere01_04_Int_Column_LIKE_String_value() {
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
		Value value = new Value();
		value.setString(collectInfoTable.getColumnValue("category",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond = new ObSimpleCond("category",
				ObSimpleCond.ObLogicOperator.LIKE, value);
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);
		Assert.assertEquals("[-2, invalid argument]", result.getCode()
				.toString());
	}
	
	@Test
	public void test_QueryTestCaseForWhere01_05_01_DateTime_Column_Like_String_Value() {
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
		Value value = new Value();
		value.setString(collectInfoTable.getColumnValue("collect_time",tmp_rowkey).toString());
		ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",
				ObSimpleCond.ObLogicOperator.LIKE, value);
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(obSimpleCond);
		Info.setFilter(filter);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);
		Assert.assertEquals("[-2, invalid argument]", result.getCode()
				.toString());
	}
	
}
