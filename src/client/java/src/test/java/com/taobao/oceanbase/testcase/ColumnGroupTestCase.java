package com.taobao.oceanbase.testcase;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
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
import com.taobao.oceanbase.vo.GetParam;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.UpdateMutator;


public class ColumnGroupTestCase extends BaseCase {

	private static void prepareInfo(QueryInfo Info, RKey start_rowkey,
			RKey end_rowkey) {
		Info.setStartKey(start_rowkey);
		Info.setEndKey(end_rowkey);
		Info.setInclusiveStart(true);
		Info.setInclusiveEnd(true);
		Info.setPageSize(30);
		Info.setPageNum(1);
	}

	private void addColumns(QueryInfo Info, String[] columns) {
		for (int i = 0; i < columns.length; i++) {
			Info.addColumn(columns[i]);
		}
	}

	public Set<String> setInsertColumnsforCollect() {
	        Set<String> Columns = collectInfoTable.getAllColumnNames();
	        Columns.remove("gm_modified");
	        Columns.remove("gm_create");
	        return Columns;
	}
	
	public void updateAndVerify_ItemInfo(String tabName,RKey rkey,Set<String>colNames,int times){
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, itemInfoTable.genColumnUpdateResult(str, rkey, times, false), false);
		}
		Result<Boolean> rs=obClient.update(um);
		Assert.assertTrue("update fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(itemInfoTable.genColumnUpdateResult(str, rkey, times, false).getObject(false).getValue(),
					rd.getResult().get(str));
		}
	}
	
	public void insertAndVerify_ItemInfo(String tabName,RKey rkey,Set<String>colNames){
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im.addColumn(str, itemInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		Result<Boolean>rs=obClient.insert(im);
		Assert.assertTrue("Insert fail!",rs.getResult());
		Result<RowData> rd=obClient.get(tabName, rkey, colNames);
		for(String str:colNames){
			Assert.assertEquals(str,itemInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(), rd.getResult().get(str));
		}
	}
	
	private void query_check_CollectInfo(Result<List<RowData>> result,
			RKey start_rowkey, RKey end_rowkey, boolean isInclusiveStart,
			boolean isInclusiveEnd, String[] columns) {
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

			for (String infoColumn : columns) {
				assertEquals(
						collectInfoTable.getColumnValue(infoColumn, infoRowkey),
						result.getResult().get(i).get(infoColumn));
			}
		}
	}
	
	private void get_check_CollectInfo(Result<RowData> result, RKey rowkey,
			Set<String> columns) {
		for (String infoColumn : columns) {
			assertEquals(collectInfoTable.getColumnValue(infoColumn, rowkey),
					result.getResult().get(infoColumn));
		}
	}
	
	private void get_check_ItemInfo(Result<RowData> result, RKey rowkey,
			Set<String> columns) {
		for (String infoColumn : columns) {
			assertEquals(itemInfoTable.getColumnValue(infoColumn, rowkey),
					result.getResult().get(infoColumn));
		}
	}
	
	private void mget_check_CollectInfo(Result<List<RowData>> result,
			RKey start_rowkey, RKey end_rowkey, boolean isInclusiveStart,
			boolean isInclusiveEnd, Set<String> columns) {
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

			for (String infoColumn : columns) {
				assertEquals(
						collectInfoTable.getColumnValue(infoColumn, infoRowkey),
						result.getResult().get(i).get(infoColumn));
			}
		}
	}
	
	private void mget_check_ItemInfo(Result<List<RowData>> result,
			RKey start_rowkey, RKey end_rowkey, boolean isInclusiveStart,
			boolean isInclusiveEnd, Set<String> columns) {
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

			for (String infoColumn : columns) {
				assertEquals(
						itemInfoTable.getColumnValue(infoColumn, infoRowkey),
						result.getResult().get(i).get(infoColumn));
			}
		}
	}
	
	private void query_check_ItemInfo(Result<List<RowData>> result,
			RKey start_rowkey, RKey end_rowkey, boolean isInclusiveStart,
			boolean isInclusiveEnd, String[] columns) {
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

			for (String infoColumn : columns) {
				assertEquals(
						itemInfoTable.getColumnValue(infoColumn, infoRowkey),
						result.getResult().get(i).get(infoColumn));
			}
		}
	}
	
	private void Prepare_GetParams_CollectInfo(List<GetParam> getParams, Set<String>colNames, int count){
		
		for(int tmp = 0; tmp < count; tmp++){
			RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', tmp+1));

			GetParam getParam = new GetParam();
			getParam.setColumns(colNames);
			getParam.setRowkey(rkey);
			getParam.setTableName(collectInfoTable.getTableName());
			
			getParams.add(getParam);
		}
	}
	
private void Prepare_GetParams_ItemInfo(List<GetParam> getParams, Set<String>colNames, int count){
		
		for(int tmp = 0; tmp < count; tmp++){
			RKey rkey = new RKey(new ItemInfoKeyRule((byte) '0', tmp+1));

			GetParam getParam = new GetParam();
			getParam.setColumns(colNames);
			getParam.setRowkey(rkey);
			getParam.setTableName(itemInfoTable.getTableName());
			
			getParams.add(getParam);
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
	public void test_ColumnGroup_Happy_Path() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		String columns[] = {"user_nick", "is_shared", "note", "collect_time", "status"}; 
		addColumns(Info,columns);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		// verify result
		query_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}

	@Test
	public void test_ColumnGroup01_01_Query_SingleGroupData_NoJoin_CollectInfoTable() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		String columns[] = {"user_nick", "is_shared", "note", "collect_time", "status"}; 
		addColumns(Info,columns);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		// verify result
		query_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup01_02_Query_SingleGroupData_MixJoin_CollectInfoTable() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		String columns[] = {"tag", "category", "title", "owner_id", "price", "collect_time_precise"}; 
		addColumns(Info,columns);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		// verify result
		query_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}

	@Test
	public void test_ColumnGroup01_03_Query_SingleGroupData_Join_CollectInfoTable() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		String columns[] = {"picurl", "owner_nick", "comment_count", "collector_count"}; 
		addColumns(Info,columns);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		// verify result
		query_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup01_04_Query_SingleGroupData_NoJoin_ItemInfoTable() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 1));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		String columns[] = {"title", "picurl", "owner_id", "owner_nick", "price", "ends"}; 
		addColumns(Info,columns);

		// query operate
		result = obClient.query(itemInfoTable.getTableName(), Info);

		// verify result
		query_check_ItemInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup02_01_QueryInOrder_MultiGroupData_CollectInfoTable() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		String columns[] = {"collector_count", "comment_count", "owner_nick", "picurl", "tag", "status"}; 
		addColumns(Info,columns);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		// verify result
		query_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup02_02_QueryInOrder_MultiGroupData_ItemInfoTable() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 1));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		String columns[] = {"money", "collect_count", "collector_count", "comment_count", "proper_xml", "ends", "title"}; 
		addColumns(Info,columns);

		// query operate
		result = obClient.query(itemInfoTable.getTableName(), Info);

		// verify result
		query_check_ItemInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup03_01_QueryIntercross_MultiGroupData_CollectInfoTable() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		String columns[] = {"user_nick", "tag", "picurl", "gm_create", "is_shared", "category", "owner_nick", "gm_modified", "status", "collect_time_precise", "collector_count", "total_money"}; 
		addColumns(Info,columns);

		// query operate
		result = obClient.query(collectInfoTable.getTableName(), Info);

		// verify result
		query_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup03_02_QueryIntercross_MultiGroupData_ItemInfoTable() {
		Result<List<RowData>> result;
		QueryInfo Info = new QueryInfo();
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 1));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare QueryInfo
		prepareInfo(Info, start_rowkey, end_rowkey);
		String columns[] = {"title", "proper_xml", "picurl", "comment_count", "proper_xml", "ends", "money"}; 
		addColumns(Info,columns);

		// query operate
		result = obClient.query(itemInfoTable.getTableName(), Info);

		// verify result
		query_check_ItemInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup04_01_Get_SingleGroupData_NoJoin_CollectInfoTable() {
		Result<RowData> result;
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("user_nick");
		columns.add("is_shared");
		columns.add("note");
		columns.add("collect_time");
		columns.add("status");		

		// query operate
		result = obClient.get(collectInfoTable.getTableName(), rowkey, columns);

		// verify result
		get_check_CollectInfo(result, rowkey, columns);
	}
	
	@Test
	public void test_ColumnGroup04_02_Get_SingleGroupData_MixJoin_CollectInfoTable() {
		Result<RowData> result;
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("tag");
		columns.add("category");
		columns.add("title");
		columns.add("owner_id");
		columns.add("price");
		columns.add("collect_time_precise");

		// query operate
		result = obClient.get(collectInfoTable.getTableName(), rowkey, columns);

		// verify result
		get_check_CollectInfo(result, rowkey, columns);
	}
	
	@Test
	public void test_ColumnGroup04_03_Get_SingleGroupData_Join_CollectInfoTable() {
		Result<RowData> result;
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("picurl");
		columns.add("owner_nick");
		columns.add("comment_count");
		columns.add("collector_count");

		// query operate
		result = obClient.get(collectInfoTable.getTableName(), rowkey, columns);

		// verify result
		get_check_CollectInfo(result, rowkey, columns);
	}
	
	@Test
	public void test_ColumnGroup04_04_Get_SingleGroupData_NoJoin_ItemInfoTable() {
		Result<RowData> result;
		RKey rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("title");
		columns.add("picurl");
		columns.add("owner_id");
		columns.add("owner_nick");
		columns.add("price");
		columns.add("ends");

		// query operate
		result = obClient.get(itemInfoTable.getTableName(), rowkey, columns);

		// verify result
		get_check_ItemInfo(result, rowkey, columns);
	}

	@Test
	public void test_ColumnGroup05_01_GetInOrder_MultiGroupData_CollectInfoTable() {
		Result<RowData> result;
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("collector_count");
		columns.add("comment_count");
		columns.add("owner_nick");
		columns.add("picurl");
		columns.add("tag");
		columns.add("status");

		// query operate
		result = obClient.get(collectInfoTable.getTableName(), rowkey, columns);

		// verify result
		get_check_CollectInfo(result, rowkey, columns);
	}

	@Test
	public void test_ColumnGroup05_02_GetInOrder_MultiGroupData_ItemInfoTable() {
		Result<RowData> result;
		RKey rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare get columns 
		Set<String> columns = new HashSet<String>();
		columns.add("money");
		columns.add("collect_count");
		columns.add("collector_count");
		columns.add("comment_count");
		columns.add("proper_xml");
		columns.add("ends");
		columns.add("title");

		// query operate
		result = obClient.get(itemInfoTable.getTableName(), rowkey, columns);

		// verify result
		get_check_ItemInfo(result, rowkey, columns);
	}
	
	@Test
	public void test_ColumnGroup06_01_GetIntercross_MultiGroupData_CollectInfoTable() {
		Result<RowData> result;
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("user_nick");
		columns.add("tag");
		columns.add("picurl");
		columns.add("gm_create");
		columns.add("is_shared");
		columns.add("category");
		columns.add("owner_nick");
		columns.add("gm_modified");
		columns.add("status");
		columns.add("collect_time_precise");
		columns.add("collector_count");
		columns.add("total_money");		

		// query operate
		result = obClient.get(collectInfoTable.getTableName(), rowkey, columns);

		// verify result
		get_check_CollectInfo(result, rowkey, columns);
	}
	
	@Test
	public void test_ColumnGroup06_02_GetIntercross_MultiGroupData_ItemInfoTable() {
		Result<RowData> result;
		RKey rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare get columns 		
		Set<String> columns = new HashSet<String>();
		columns.add("title");
		columns.add("proper_xml");
		columns.add("picurl");
		columns.add("comment_count");
		columns.add("proper_xml");
		columns.add("ends");
		columns.add("money");

		// query operate
		result = obClient.get(itemInfoTable.getTableName(), rowkey, columns);

		// verify result
		get_check_ItemInfo(result, rowkey, columns);
	}
	
	@Test
	public void test_ColumnGroup07_01_MGet_SingleGroupData_NoJoin_CollectInfoTable() {
		Result<List<RowData>> result;
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		List<GetParam> getParams = new ArrayList<GetParam>();
		
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("user_nick");
		columns.add("is_shared");
		columns.add("note");
		columns.add("collect_time");
		columns.add("status");		
		
		// prepare List<GetParam> getParams
		Prepare_GetParams_CollectInfo(getParams, columns, 6);

		// query operate
		result = obClient.get(getParams);

		// verify result
		mget_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup07_02_MGet_SingleGroupData_MixJoin_CollectInfoTable() {
		Result<List<RowData>> result;
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		List<GetParam> getParams = new ArrayList<GetParam>();
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("tag");
		columns.add("category");
		columns.add("title");
		columns.add("owner_id");
		columns.add("price");
		columns.add("collect_time_precise");

		// prepare List<GetParam> getParams
		Prepare_GetParams_CollectInfo(getParams, columns, 6);
		
		// query operate
		result = obClient.get(getParams);

		// verify result
		mget_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup07_03_MGet_SingleGroupData_Join_CollectInfoTable() {
		Result<List<RowData>> result;
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		List<GetParam> getParams = new ArrayList<GetParam>();
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("picurl");
		columns.add("owner_nick");
		columns.add("comment_count");
		columns.add("collector_count");

		// prepare List<GetParam> getParams
		Prepare_GetParams_CollectInfo(getParams, columns, 6);
		
		// query operate
		result = obClient.get(getParams);

		// verify result
		mget_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup07_04_MGet_SingleGroupData_NoJoin_ItemInfoTable() {
		Result<List<RowData>> result;
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 1));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		
		List<GetParam> getParams = new ArrayList<GetParam>();
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("title");
		columns.add("picurl");
		columns.add("owner_id");
		columns.add("owner_nick");
		columns.add("price");
		columns.add("ends");

		// prepare List<GetParam> getParams
		Prepare_GetParams_ItemInfo(getParams, columns, 6);
		
		// query operate
		result = obClient.get(getParams);

		// verify result
		mget_check_ItemInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}

	@Test
	public void test_ColumnGroup08_01_MGetInOrder_MultiGroupData_CollectInfoTable() {
		Result<List<RowData>> result;
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		List<GetParam> getParams = new ArrayList<GetParam>();
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("collector_count");
		columns.add("comment_count");
		columns.add("owner_nick");
		columns.add("picurl");
		columns.add("tag");
		columns.add("status");

		// prepare List<GetParam> getParams
		Prepare_GetParams_CollectInfo(getParams, columns, 6);
		
		// query operate
		result = obClient.get(getParams);

		// verify result
		mget_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}

	@Test
	public void test_ColumnGroup08_02_MGetInOrder_MultiGroupData_ItemInfoTable() {
		Result<List<RowData>> result;
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 1));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		
		List<GetParam> getParams = new ArrayList<GetParam>();
		 	
		// prepare get columns 
		Set<String> columns = new HashSet<String>();
		columns.add("money");
		columns.add("collect_count");
		columns.add("collector_count");
		columns.add("comment_count");
		columns.add("proper_xml");
		columns.add("ends");
		columns.add("title");

		// prepare List<GetParam> getParams
		Prepare_GetParams_ItemInfo(getParams, columns, 6);
		
		// query operate
		result = obClient.get(getParams);

		// verify result
		mget_check_ItemInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}
	
	@Test
	public void test_ColumnGroup09_01_MGetIntercross_MultiGroupData_CollectInfoTable() {
		Result<List<RowData>> result;
		RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		
		List<GetParam> getParams = new ArrayList<GetParam>();
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("user_nick");
		columns.add("tag");
		columns.add("picurl");
		columns.add("gm_create");
		columns.add("is_shared");
		columns.add("category");
		columns.add("owner_nick");
		columns.add("gm_modified");
		columns.add("status");
		columns.add("collect_time_precise");
		columns.add("collector_count");
		columns.add("total_money");		

		// prepare List<GetParam> getParams
		Prepare_GetParams_CollectInfo(getParams, columns, 6);
		
		// query operate
		result = obClient.get(getParams);

		// verify result
		mget_check_CollectInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}

	@Test
	public void test_ColumnGroup09_02_MGetIntercross_MultiGroupData_ItemInfoTable() {
		Result<List<RowData>> result;
		RKey start_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 1));
		RKey end_rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		
		List<GetParam> getParams = new ArrayList<GetParam>();
		 	
		// prepare get columns 		
		Set<String> columns = new HashSet<String>();
		columns.add("title");
		columns.add("proper_xml");
		columns.add("picurl");
		columns.add("comment_count");
		columns.add("proper_xml");
		columns.add("ends");
		columns.add("money");

		// prepare List<GetParam> getParams
		Prepare_GetParams_ItemInfo(getParams, columns, 6);
		
		// query operate
		result = obClient.get(getParams);

		// verify result
		mget_check_ItemInfo(result, start_rowkey, end_rowkey, true, true, columns);
	}

	@Test
	public void test_ColumnGroup10_01_Update_SingleGroupData_NoJoin_CollectInfoTable() {
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("user_nick");
		columns.add("is_shared");
		columns.add("note");
		columns.add("collect_time");
		columns.add("status");

		updateAndVerify(collectInfoTable.getTableName(), rowkey, columns, 0);
	}
	
	@Test
	public void test_ColumnGroup10_02_Update_SingleGroupData_MixJoinGroupNoJoinColumn_CollectInfoTable() {
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("tag");
		columns.add("category");
		columns.add("collect_time_precise");

		updateAndVerify(collectInfoTable.getTableName(), rowkey, columns, 0);
	}
	
	@Test
	public void test_ColumnGroup10_03_Update_SingleGroupData_NoJoin_ItemInfoTable() {
		RKey rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("title");
		columns.add("picurl");
		columns.add("owner_id");
		columns.add("owner_nick");
		columns.add("price");
		columns.add("ends");

		updateAndVerify_ItemInfo(itemInfoTable.getTableName(), rowkey, columns, 0);
	}
	
	@Test
	public void test_ColumnGroup11_01_UpdateInOrder_MultiGroupData_NoJoin_CollectInfoTable() {
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("collect_time_precise");
		columns.add("category");
		columns.add("tag");
		columns.add("status");
		columns.add("collect_time");
		columns.add("note");
		columns.add("is_shared");
		columns.add("user_nick");

		updateAndVerify(collectInfoTable.getTableName(), rowkey, columns, 0);
	}

	@Test
	public void test_ColumnGroup11_02_UpdateInOrder_MultiGroupData_NoJoin_ItemInfoTable() {
		RKey rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("money");
		columns.add("collector_count");
		columns.add("proper_xml");
		columns.add("ends");
		columns.add("price");
		columns.add("owner_nick");
		columns.add("owner_id");
		columns.add("picurl");
		columns.add("title");

		updateAndVerify_ItemInfo(itemInfoTable.getTableName(), rowkey, columns, 0);
	}
	
	@Test
	public void test_ColumnGroup12_01_UpdateIntercross_MultiGroupData_NoJoin_CollectInfoTable() {
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("user_nick");
		columns.add("tag");
		columns.add("is_shared");
		columns.add("category");
		columns.add("note");
		columns.add("collect_time_precise");
		columns.add("collect_time");
		columns.add("status");
		
		updateAndVerify(collectInfoTable.getTableName(), rowkey, columns, 0);
	}

	@Test
	public void test_ColumnGroup12_02_UpdateIntercross_MultiGroupData_NoJoin_ItemInfoTable() {
		RKey rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("title");
		columns.add("proper_xml");
		columns.add("picurl");
		columns.add("collector_count");
		columns.add("owner_id");
		columns.add("money");
		columns.add("owner_nick");
		columns.add("ends");
		columns.add("price");

		updateAndVerify_ItemInfo(itemInfoTable.getTableName(), rowkey, columns, 0);
	}
	
	public void test_ColumnGroup13_01_Insert_SingleGroupData_NoJoin_CollectInfoTable() {
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("user_nick");
		columns.add("is_shared");
		columns.add("note");
		columns.add("collect_time");
		columns.add("status");

		insertAndVerify(collectInfoTable.getTableName(), rowkey, columns);
	}
	
	@Test
	public void test_ColumnGroup13_02_Insert_SingleGroupData_MixJoinGroupNoJoinColumn_CollectInfoTable() {
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("tag");
		columns.add("category");
		columns.add("collect_time_precise");

		insertAndVerify(collectInfoTable.getTableName(), rowkey, columns);
	}
	
	@Test
	public void test_ColumnGroup13_03_Update_SingleGroupData_NoJoin_ItemInfoTable() {
		RKey rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("title");
		columns.add("picurl");
		columns.add("owner_id");
		columns.add("owner_nick");
		columns.add("price");
		columns.add("ends");

		insertAndVerify_ItemInfo(itemInfoTable.getTableName(), rowkey, columns);
	}
	
	@Test
	public void test_ColumnGroup14_01_InsertInOrder_MultiGroupData_NoJoin_CollectInfoTable() {
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("collect_time_precise");
		columns.add("category");
		columns.add("tag");
		columns.add("status");
		columns.add("collect_time");
		columns.add("note");
		columns.add("is_shared");
		columns.add("user_nick");

		insertAndVerify(collectInfoTable.getTableName(), rowkey, columns);
	}

	@Test
	public void test_ColumnGroup14_02_InsertInOrder_MultiGroupData_NoJoin_ItemInfoTable() {
		RKey rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("money");
		columns.add("collector_count");
		columns.add("proper_xml");
		columns.add("ends");
		columns.add("price");
		columns.add("owner_nick");
		columns.add("owner_id");
		columns.add("picurl");
		columns.add("title");

		insertAndVerify_ItemInfo(itemInfoTable.getTableName(), rowkey, columns);
	}
	
	@Test
	public void test_ColumnGroup15_01_InsertIntercross_MultiGroupData_NoJoin_CollectInfoTable() {
		RKey rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("user_nick");
		columns.add("tag");
		columns.add("is_shared");
		columns.add("category");
		columns.add("note");
		columns.add("collect_time_precise");
		columns.add("collect_time");
		columns.add("status");
		
		insertAndVerify(collectInfoTable.getTableName(), rowkey, columns);
	}

	@Test
	public void test_ColumnGroup15_02_InsertIntercross_MultiGroupData_NoJoin_ItemInfoTable() {
		RKey rowkey = new RKey(new ItemInfoKeyRule((byte) '0', 6));
		 	
		// prepare get columns
		Set<String> columns = new HashSet<String>();
		columns.add("title");
		columns.add("proper_xml");
		columns.add("picurl");
		columns.add("collector_count");
		columns.add("owner_id");
		columns.add("money");
		columns.add("owner_nick");
		columns.add("ends");
		columns.add("price");

		insertAndVerify_ItemInfo(itemInfoTable.getTableName(), rowkey, columns);
	}
}