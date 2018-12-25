package com.taobao.oceanbase.testcase;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.lang.Math;
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.oceanbase.base.BaseCase;
import com.taobao.oceanbase.base.CollectColumnEnum;
import com.taobao.oceanbase.base.ExpectResult;
import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.RowKey;
import com.taobao.oceanbase.base.RowKeyFactory;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.CollectInfoTimeColumnRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.base.table.ITable;
import com.taobao.oceanbase.base.table.Table;
import com.taobao.oceanbase.cli.OBClient;
import com.taobao.oceanbase.vo.DeleteMutator;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.ObMutatorBase;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.Value;

public class UpdateTestCaseExecuteForJoin extends BaseCase{

	private static String exceptionMessage = new String();
	static Result<RowData> result = null;
	String collectInfoTableName = collectInfoTable.getTableName();
	String itemInfoTableName = itemInfoTable.getTableName();
	// trans collect_info rkey to item_info rkey
	private RKey transKey(RKey rkey) {
		ItemInfoKeyRule rule = new ItemInfoKeyRule(
				((CollectInfoKeyRule) (rkey.getKeyRule())).getItem_type(),
				((CollectInfoKeyRule) (rkey.getKeyRule())).getItem_id());
		return new RKey(rule);
	}
	public void updateCollectInforJionAndVerify(String tabName, RKey rkey,
			Set<String> colNames, int times,boolean isAdd) {
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, times, isAdd), isAdd);
		}
		Result<Boolean> rs=obClient.update(um);
		Assert.assertFalse("update successed!",rs.getResult());
	}
	public void updateItemInfoAndVerify(String tabName, RKey rkey,
			Set<String> colNames, int times,boolean isAdd) {
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, itemInfoTable.genColumnUpdateResult(str, rkey, times, isAdd), isAdd);
		}
		Result<Boolean> rs=obClient.update(um);
		Assert.assertTrue("update fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(itemInfoTable.genColumnUpdateResult(str, rkey, times, isAdd).getObject(isAdd).getValue(),
					rd.getResult().get(str));
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
	public  void test_update_01_HappyPath(){
		/* CollectInfo */
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 1));
		Set<String> nonJoin=new HashSet<String>();
		nonJoin=collectInfoTable.getNonJoinColumnNames();
		nonJoin.remove("gm_modified");
		nonJoin.remove("gm_create");
		updateAndVerify(collectInfoTableName, rowkey, nonJoin, 1);
		
		/*verify collectinfo table join columns can not update*/
		Set<String> join=new HashSet<String>();
		join = collectInfoTable.getJoinColumnNames();
		updateCollectInforJionAndVerify(collectInfoTableName, rowkey, join, 1,false);
		
		/* ItemInfo*/
		updateItemInfoAndVerify(itemInfoTable.getTableName(), transKey(rowkey), itemInfoTable.getNonJoinColumnNames(), 1,false);
		Result<RowData> rc=obClient.get(collectInfoTableName, rowkey, collectInfoTable.getJoinColumnNames());
		/* after iteminfo table update ,check collectinfo table join columns.*/
		for(String str:collectInfoTable.getJoinColumnNames()){
			assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkey, 1, false).getObject(false).getValue(), rc.getResult().get(str));
		}
	} 
	@Test
	public void  test_update_02_NumberAdd(){
		/*collectInfo*/
		//obClient.clearActiveMemTale();
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 2));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		CollectColumnEnum temp;
		Set<String> columns=new HashSet<String>();
		columns=collectInfoTable.getNonJoinColumnNames();
		columns.remove("gm_modified");
		columns.remove("gm_create");
		updateCollectAndVerify_Case2(rowkey, rowMutator, columns);
		/*itemInfo */
		rowMutator = new UpdateMutator(itemInfoTableName, transKey(rowkey));
		columns=itemInfoTable.getAllColumnNames();
		updateItemAndVerify_Case2(transKey(rowkey), rowMutator, columns);
		/*After itemInfo update verify collectInfo */
		columns = collectInfoTable.getJoinColumnNames();
		Result<RowData> rc=obClient.get(collectInfoTableName, rowkey, columns);
		for(String str:collectInfoTable.getJoinColumnNames()){
			temp = CollectColumnEnum.valueOf(str.toUpperCase());
			if(!temp.getType().equals("string")){
				assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkey, 1, false).getObject(true).getValue(), rc.getResult().get(str));
			}
		}
	}
	private void updateCollectAndVerify_Case2(RKey rowkey, UpdateMutator rowMutator,
			Set<String> columns) {
		CollectColumnEnum temp;
		for(String str : columns){
			temp = CollectColumnEnum.valueOf(str.toUpperCase());
			if(!temp.getType().equals("string")){
				rowMutator.addColumn(str,  collectInfoTable.genColumnUpdateResult(str, rowkey, 1, true), true);
			}
		}
		Assert.assertTrue("update fail!",obClient.update(rowMutator).getResult());
		result = obClient.get(collectInfoTableName, rowkey, collectInfoTable.getNonJoinColumnNames());
		for (String str : columns) {
			temp = CollectColumnEnum.valueOf(str.toUpperCase());
			if(!temp.getType().equals("string")){
				Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rowkey, 1, false).getObject(true).getValue(),
						result.getResult().get(str));
			}
		}
	}
	private void updateItemAndVerify_Case2(RKey rowkey, UpdateMutator rowMutator,
			Set<String> columns) {
		CollectColumnEnum temp;
		for(String str : columns){
			temp = CollectColumnEnum.valueOf(str.toUpperCase());
			if(!temp.getType().equals("string")){
				rowMutator.addColumn(str,  itemInfoTable.genColumnUpdateResult(str, rowkey, 1, true), true);
			}
		}
		Assert.assertTrue("update fail!",obClient.update(rowMutator).getResult());
		result = obClient.get(itemInfoTableName, rowkey, itemInfoTable.getNonJoinColumnNames());
		for (String str : columns) {
			temp = CollectColumnEnum.valueOf(str.toUpperCase());
			if(!temp.getType().equals("string")){
				Assert.assertEquals(itemInfoTable.genColumnUpdateResult(str, rowkey, 1, false).getObject(true).getValue(),
						result.getResult().get(str));
			}
		}
	}
	@Test
	public  void test_update_03_StringAdd(){
		//collect info
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 3));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Value value=new Value();
		value.setString("string");
		try {
			rowMutator.addColumn("user_nick", value, true);
		}catch (Exception e) {
			exceptionMessage = e.getMessage();
		} finally {
			assertEquals("can not add null or string", exceptionMessage);
			exceptionMessage = null;
		}
		// item info
		rowMutator = new UpdateMutator(itemInfoTableName,transKey(rowkey));
		value=new Value();
		value.setString("1");
		try {
			rowMutator.addColumn("proper_xml", value, true);
		}catch (Exception e) {
			exceptionMessage = e.getMessage();
		} finally {
			assertEquals("can not add null or string", exceptionMessage);
			exceptionMessage = null;
		}
	}
	@Test
	public  void test_update_04_AddException(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 5));
		/*collectInfo*/
		Set<String> NonJoin=new HashSet<String>();
		NonJoin=collectInfoTable.getNonJoinColumnNames();
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Result<RowData> result=obClient.get(collectInfoTableName, rowkey, NonJoin);
		//System.out.println("before result success "+ result.getCode().getMessage());
		long beforedata=result.getResult().<Long>get("is_shared");
		//System.out.println("beforedata "+ beforedata);
		Value value=new Value();value.setDouble(26.0);
		rowMutator.addColumn("is_shared", value, true);
		Assert.assertFalse("update successed",obClient.update(rowMutator).getResult());
		result=obClient.get(collectInfoTableName, rowkey, NonJoin);
		assertTrue(result.getCode().getMessage().equals("success"));
		//System.out.println("result.getResult().get " +result.getResult().get("is_shared"));
		Assert.assertEquals(beforedata, result.getResult().get("is_shared"));
		/*itemInfo*/
		Set<String> itemJoin=new HashSet<String>();
		itemJoin=itemInfoTable.getNonJoinColumnNames();
		rowMutator=new UpdateMutator(itemInfoTableName, rowkey);
		result=obClient.get(itemInfoTableName, transKey(rowkey), itemJoin);
		beforedata=result.getResult().<Long>get("comment_count");
		value=new Value();value.setDouble(26.0);
		rowMutator.addColumn("comment_count", value, true);
		Assert.assertFalse("update successed",obClient.update(rowMutator).getResult());
		result=obClient.get(itemInfoTableName, transKey(rowkey), itemJoin);
		Assert.assertEquals(beforedata, result.getResult().get("comment_count"));
		
	}
	@Test
	public  void test_update_05_UpdateNormal(){
		//Reduplicated with case 1
	}
	@Test
	public  void test_update_06_UpdateInNormal(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 6));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Value value=new Value();
		value.setString("is_shared");
		rowMutator.addColumn("is_shared", value, false);
		Assert.assertFalse("update successed",obClient.update(rowMutator).getResult());
		result=obClient.get(collectInfoTableName, rowkey, collectInfoTable.getNonJoinColumnNames());
		Assert.assertEquals(collectInfoTable.getColumnValue("is_shared", rowkey), result.getResult().get("is_shared"));
		
		rowMutator=new UpdateMutator(itemInfoTableName, rowkey);
		value=new Value();value.setDouble(26.0);
		rowMutator.addColumn("comment_count", value, false);
		Assert.assertFalse("update successed",obClient.update(rowMutator).getResult());
		
		
	}
	@Test
	public  void test_update_07_tableIsNull(){
		String table=null;
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 7));
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		try{
		UpdateMutator rowMutator=new UpdateMutator(table, rowkey);
		Value value=new Value();
		value.setNumber(1007);
		rowMutator.addColumn("is_shared", value, false);
		Assert.assertFalse("update sucessed",obClient.update(rowMutator).getResult());
		result=obClient.get(table, rowkey, columns);
		Assert.assertEquals(null, result.getResult());
		}
		catch(IllegalArgumentException e){
			exceptionMessage=e.getMessage();
		}finally{
			assertEquals("table name null", exceptionMessage);
			exceptionMessage=null;
		}
	}
	
	@Test
	public  void test_update_08_tableIsBlank(){
		String table="";
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 8));
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		try{
		UpdateMutator rowMutator=new UpdateMutator(table, rowkey);
		Value value=new Value();
		value.setNumber(1008);
		rowMutator.addColumn("is_shared", value, false);
		Assert.assertFalse("update sucessed",obClient.update(rowMutator).getResult());
		result=obClient.get(table, rowkey, columns);
		Assert.assertEquals(null, result.getResult());
		}
		catch(IllegalArgumentException e){
			exceptionMessage=e.getMessage();
		}finally{
			assertEquals("table name null", exceptionMessage);
			exceptionMessage=null;
		}
	}
	
	@Test
	public  void test_update_09_rowkeyIsNull(){
		RowKey rowkey=null;
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		try{
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Value value=new Value();
		value.setNumber(1007);
		rowMutator.addColumn("is_shared", value, false);
		Assert.assertFalse("update sucessed",obClient.update(rowMutator).getResult());
		result=obClient.get(collectInfoTableName, rowkey, columns);
		Assert.assertEquals(null, result.getResult());
		}catch(IllegalArgumentException e){
			exceptionMessage=e.getMessage();
		}finally{
			assertEquals("rowkey is null",exceptionMessage);
			exceptionMessage=null;
		}
		try{
			UpdateMutator rowMutator=new UpdateMutator(itemInfoTableName, rowkey);
		}catch(IllegalArgumentException e){
			exceptionMessage=e.getMessage();
		}finally{
			assertEquals("rowkey is null",exceptionMessage);
			exceptionMessage=null;
		}
	}
	
	@Test
	public  void test_update_10_rowkeyIsBlank(){
		RKey rowkey = new RKey(new byte[] {});
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Value value=new Value();
		value.setNumber(10101);
		rowMutator.addColumn("is_shared", value, false);
		try{
		obClient.update(rowMutator);
		}catch(IllegalArgumentException e){
			exceptionMessage=e.getMessage();
		}finally{
			Assert.assertEquals(exceptionMessage, "rowkey bytes null");
		}
		
		rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		value=new Value();
		value.setNumber(10101);
		rowMutator.addColumn("comment_count", value, false);
		try{
		obClient.update(rowMutator);
		}catch(IllegalArgumentException e){
			exceptionMessage=e.getMessage();
		}finally{
			Assert.assertEquals(exceptionMessage, "rowkey bytes null");
			exceptionMessage=null;
		}
	}
	
	@Test
	public  void test_update_11_tableIsNullRowkeyIsBlank(){
		String table=null;
		RKey rowkey = new RKey(new byte[] {});
		try{
		UpdateMutator rowMutator=new UpdateMutator(table, rowkey);
		}catch(IllegalArgumentException e){
			exceptionMessage=e.getMessage();
		}finally{
			assertEquals(exceptionMessage,"table name null");
			exceptionMessage=null;
		}
	}
	@Test
	public  void test_update_12_tableIsBlankRowkeyIsNull(){
		String table="";
		RowKey rowkey=null;
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		try{
		UpdateMutator rowMutator=new UpdateMutator(table, rowkey);
		Value value=new Value();
		value.setNumber(1012);
		rowMutator.addColumn("is_shared", value, false);
		Assert.assertFalse("update sucessed",obClient.update(rowMutator).getResult());
		result=obClient.get(table, rowkey, columns);
		Assert.assertEquals(null, result.getResult());
		}catch(IllegalArgumentException e){
			exceptionMessage= e.getMessage();
		}finally{
			assertEquals("table name null",exceptionMessage);
			exceptionMessage=null;
		}
	}
	
	@Test
	public  void test_update_13_RowMutatorIsNull(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 13));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName,rowkey);
		try{
			assertTrue("updata sucessed ",obClient.update(rowMutator).getResult());
		}catch(IllegalArgumentException e){
			exceptionMessage=e.getMessage();
		}finally{
			assertEquals("columns null", exceptionMessage);
			exceptionMessage=null;
		}
	}
	
	@Test
	public  void test_update_14_OneColumnNameIsNull(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 14));
		Set<String>columns = collectInfoTable.getNonJoinColumnNames();
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Value value=new Value();
		value.setString("value0003");
		for(String str: columns){
			rowMutator.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rowkey, 1, true), false);
		}
		try{
			rowMutator.addColumn(null, value, false);
		}catch(IllegalArgumentException e){
			exceptionMessage= e.getMessage();
		}finally{
			assertEquals("column name null",exceptionMessage);
			exceptionMessage=null;
		}
		Assert.assertFalse("update sucessed", obClient.update(rowMutator).getResult());
	}
	
	@Test
	public  void test_update_15_MultiColumnsNameIsNull(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 15));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		Value value; 
		for(String str: columns){
			rowMutator.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rowkey, 1, true), false);
		}
		value=new Value();
		value.setString("value0015");
		try{
		rowMutator.addColumn(null, value, false);
		}catch(IllegalArgumentException e){
			exceptionMessage=e.getMessage();
		}finally{
			assertEquals("column name null", exceptionMessage);
			exceptionMessage=null;
		}
		value=new Value();value.setNumber(1015);
		try{
		rowMutator.addColumn(null, value, false);
		}catch(IllegalArgumentException e){
			exceptionMessage= e.getMessage();
		}finally{
			assertEquals("column name null", exceptionMessage);
			exceptionMessage=null;
		}
		Assert.assertFalse("update sucessed",obClient.update(rowMutator).getResult());
	}
	
	@Test
	public  void test_update_16_OneColumnIsBlank(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 16));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		for(String str: columns){
			rowMutator.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rowkey, 1, true), false);
		}
		Value value; 
		value = new Value();
		value.setString("value0016");
		try{
		rowMutator.addColumn("", value, false);
		}catch(IllegalArgumentException e){
			exceptionMessage= e.getMessage();
		}finally{
			assertEquals("column name null", exceptionMessage);
			exceptionMessage=null;
		}
		Assert.assertFalse("update sucessed",obClient.update(rowMutator).getResult());
		
	}
	
	@Test
	public  void test_update_17_MultiColumnsIsBlank(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 17));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		for(String str: columns){
			rowMutator.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rowkey, 1, true), false);
		}
		Value value; 
		value = new Value();
		value.setString("value0017");
		try{
		rowMutator.addColumn("", value, false);
		}catch(IllegalArgumentException e){
			exceptionMessage= e.getMessage();
		}finally{
			assertEquals("column name null", exceptionMessage);
			exceptionMessage=null;
		}
		value=new Value();value.setNumber(1017);
		try{
		rowMutator.addColumn("", value, false);
		}catch(IllegalArgumentException e){
			exceptionMessage= e.getMessage();
		}finally{
			assertEquals("column name null", exceptionMessage);
			exceptionMessage=null;
		}
		Assert.assertFalse("update sucessed",obClient.update(rowMutator).getResult());
		result=obClient.get(collectInfoTableName, rowkey, columns);
		Assert.assertEquals(null, result.getResult().get(""));
	}
	
	@Test
	public  void test_update_18_ColumnsNullOrBlank(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 18));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		for(String str: columns){
			rowMutator.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rowkey, 1, true), false);
		}
		Value value; 
		value = new Value();
		value.setString("value0018");
		try{
		rowMutator.addColumn(null, value, false);
		}catch(IllegalArgumentException e){
			exceptionMessage= e.getMessage();
		}finally{
			assertEquals("column name null", exceptionMessage);
			exceptionMessage=null;
		}
		value=new Value();value.setNumber(1018);
		try{
		rowMutator.addColumn("", value, false);
		}catch(IllegalArgumentException e){
			exceptionMessage= e.getMessage();
		}finally{
			assertEquals("column name null", exceptionMessage);
			exceptionMessage=null;
		}
		Assert.assertFalse("update sucessed",obClient.update(rowMutator).getResult());
		result=obClient.get(collectInfoTableName, rowkey, columns);
		Assert.assertEquals(null, result.getResult().get(""));
	}
	@Test
	public  void test_update_19_ForbidNullIsNull(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 20));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Value value;
		value=new Value();
		rowMutator.addColumn("gm_modified", value, false);
		value = new Value();
		rowMutator.addColumn("gm_create", value, false);
		assertFalse("update failed",obClient.update(rowMutator).getResult());
	}
	
	@Test
	public  void test_update_20_canNullisNull(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 20));
		/*collectInfo table*/
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		columns.remove("gm_modified");
		columns.remove("gm_create");
		updateCollectAndVerify_Case20(rowkey,rowMutator,columns);
		
		/*itemInfo table*/
		rowMutator = new UpdateMutator(itemInfoTableName, transKey(rowkey));
		columns = itemInfoTable.getAllColumnNames();
		updateItemAndVerify_Case20(transKey(rowkey),rowMutator,columns);
	}
	private void updateCollectAndVerify_Case20(RKey rowkey, UpdateMutator rowMutator,
			Set<String> columns) {
		for(String str : columns){
			Value value = new Value(); 
			rowMutator.addColumn(str,  value, false);
		}
		Assert.assertTrue("update fail!",obClient.update(rowMutator).getResult());
		result = obClient.get(collectInfoTableName, rowkey, collectInfoTable.getNonJoinColumnNames());
		for (String str : columns) {
			Assert.assertEquals(null,result.getResult().get(str));
		}
	}
	private void updateItemAndVerify_Case20(RKey rowkey, UpdateMutator rowMutator,
			Set<String> columns) {
		for(String str : columns){
			Value value = new Value(); 
			rowMutator.addColumn(str,  value, false);
		}
		Assert.assertTrue("update fail!",obClient.update(rowMutator).getResult());
		result = obClient.get(itemInfoTableName, rowkey, itemInfoTable.getNonJoinColumnNames());
		for (String str : columns) {
			Assert.assertEquals(null,result.getResult().get(str));
		}
	}
	@Test
	public  void test_update_21_AddSmallBoundryValue(){
		/*collectInfoTable*/
		//obClient.clearActiveMemTale();
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 21));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		columns.remove("gm_modified");
		columns.remove("gm_create");
		updateAndVerify_case21(rowkey, rowMutator, columns, collectInfoTableName, collectInfoTable);
		/*itemInfoTable*/
		rowMutator = new UpdateMutator(itemInfoTableName, transKey(rowkey));
		columns = itemInfoTable.getAllColumnNames();
		updateAndVerify_case21(transKey(rowkey), rowMutator, columns, itemInfoTableName, itemInfoTable);
		
		/*After itemInfoTable update ,verify collectInfo Table*/
		columns = collectInfoTable.getJoinColumnNames();
		afterItemInfoUpdateVerify_case21(rowkey, columns, collectInfoTable);


	}
	private void afterItemInfoUpdateVerify_case21(RKey rowkey, Set<String> columns, ITable table) {
		Result<RowData> result=obClient.get(collectInfoTableName, rowkey, columns);
		CollectColumnEnum temp;
		for(String s:columns){
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				assertEquals((Double)table.getColumnValue(s, rowkey)+Double.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("float")){
				assertEquals((Float)table.getColumnValue(s, rowkey)+Float.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("long")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("time")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("precisetime")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MIN_VALUE, result.getResult().get(s));
			}
		}
	}
	private void updateAndVerify_case21(RKey rowkey, UpdateMutator rowMutator,
			Set<String> columns, String tableName, ITable table) {
		CollectColumnEnum temp;
		Value value;
		for(String s:columns){
			value = new Value();
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				value.setDouble(Double.MIN_VALUE);
				rowMutator.addColumn(s, value,true);
			}
			if(temp.getType().equals("float")){
				value.setFloat(Float.MIN_VALUE);
				rowMutator.addColumn(s, value,true);
			}
			if(temp.getType().equals("long")){
				value.setNumber(Long.MIN_VALUE);
				rowMutator.addColumn(s, value,true);
			}
			if(temp.getType().equals("time")){
				value.setSecond(Long.MIN_VALUE);
				rowMutator.addColumn(s, value,true);
			}
			if(temp.getType().equals("precisetime")){
				value.setMicrosecond(Long.MIN_VALUE);
				rowMutator.addColumn(s, value,true);
			}
		}
		Assert.assertTrue("update failed",obClient.update(rowMutator).getResult());
		result = obClient.get(tableName, rowkey, columns);
		for(String s:columns){
			value = new Value();
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				assertEquals((Double)table.getColumnValue(s, rowkey)+Double.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("float")){
				assertEquals((Float)table.getColumnValue(s, rowkey)+Float.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("long")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("time")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("precisetime")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MIN_VALUE, result.getResult().get(s));
			}
		}
	}
	
	@Test
	public  void test_update_22_addBigBoundryValue(){
		/*collectInfoTable*/
		//obClient.clearActiveMemTale();
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 22));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		columns.remove("gm_modified");
		columns.remove("gm_create");
		updateAndVerify_case22(rowkey, rowMutator, columns, collectInfoTableName, collectInfoTable);
		/*itemInfoTable*/
		rowMutator = new UpdateMutator(itemInfoTableName, transKey(rowkey));
		columns = itemInfoTable.getAllColumnNames();
		updateAndVerify_case22(transKey(rowkey), rowMutator, columns, itemInfoTableName, itemInfoTable);
		
		/*After itemInfoTable update ,verify collectInfo Table*/
		columns = collectInfoTable.getJoinColumnNames();
		afterItemInfoUpdateVerify_case22(rowkey, columns, collectInfoTable);
		
	}
	private void afterItemInfoUpdateVerify_case22(RKey rowkey, Set<String> columns, ITable table) {
		Result<RowData> result=obClient.get(collectInfoTableName, rowkey, columns);
		CollectColumnEnum temp;
		for(String s:columns){
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				assertEquals((Double)table.getColumnValue(s, rowkey)+Double.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("float")){
				assertEquals((Float)table.getColumnValue(s, rowkey)+Float.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("long")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("time")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("precisetime")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MAX_VALUE, result.getResult().get(s));
			}
		}
	}
	private void updateAndVerify_case22(RKey rowkey, UpdateMutator rowMutator,
			Set<String> columns, String tableName, ITable table) {
		CollectColumnEnum temp;
		Value value;
		for(String s:columns){
			value = new Value();
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				value.setDouble(Double.MAX_VALUE);
				rowMutator.addColumn(s, value,true);
			}
			if(temp.getType().equals("float")){
				value.setFloat(Float.MAX_VALUE);
				rowMutator.addColumn(s, value,true);
			}
			if(temp.getType().equals("long")){
				value.setNumber(Long.MAX_VALUE);
				rowMutator.addColumn(s, value,true);
			}
			if(temp.getType().equals("time")){
				value.setSecond(Long.MAX_VALUE);
				rowMutator.addColumn(s, value,true);
			}
			if(temp.getType().equals("precisetime")){
				value.setMicrosecond(Long.MAX_VALUE);
				rowMutator.addColumn(s, value,true);
			}
		}
		Assert.assertTrue("update failed",obClient.update(rowMutator).getResult());
		result = obClient.get(tableName, rowkey, columns);
		for(String s:columns){
			value = new Value();
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				assertEquals((Double)table.getColumnValue(s, rowkey)+Double.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("float")){
				assertEquals((Float)table.getColumnValue(s, rowkey)+Float.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("long")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("time")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("precisetime")){
				assertEquals((Long)table.getColumnValue(s, rowkey)+Long.MAX_VALUE, result.getResult().get(s));
			}
		}
	}
	@Test
	public  void test_update_25_AddSmallBoundryValue(){
		/*collectInfoTable*/
		//obClient.clearActiveMemTale();
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 25));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		columns.remove("gm_modified");
		columns.remove("gm_create");
		updateAndVerify_case25(rowkey, rowMutator, columns, collectInfoTableName, collectInfoTable);
		/*itemInfoTable*/
		rowMutator = new UpdateMutator(itemInfoTableName, transKey(rowkey));
		columns = itemInfoTable.getAllColumnNames();
		updateAndVerify_case25(transKey(rowkey), rowMutator, columns, itemInfoTableName, itemInfoTable);
		
		/*After itemInfoTable update ,verify collectInfo Table*/
		columns = collectInfoTable.getJoinColumnNames();
		afterItemInfoUpdateVerify_case25(rowkey, columns, collectInfoTable);
	}
	private void afterItemInfoUpdateVerify_case25(RKey rowkey, Set<String> columns, ITable table) {
		Result<RowData> result=obClient.get(collectInfoTableName, rowkey, columns);
		CollectColumnEnum temp;
		for(String s:columns){
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				assertEquals(Double.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("float")){
				assertEquals(Float.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("long")){
				assertEquals(Long.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("time")){
				assertEquals(Long.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("precisetime")){
				assertEquals(Long.MIN_VALUE, result.getResult().get(s));
			}
		}
	}
	private void updateAndVerify_case25(RKey rowkey, UpdateMutator rowMutator,
			Set<String> columns, String tableName, ITable table) {
		CollectColumnEnum temp;
		Value value;
		for(String s:columns){
			value = new Value();
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				value.setDouble(Double.MIN_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("float")){
				value.setFloat(Float.MIN_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("long")){
				value.setNumber(Long.MIN_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("time")){
				value.setSecond(Long.MIN_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("precisetime")){
				value.setMicrosecond(Long.MIN_VALUE);
				rowMutator.addColumn(s, value,false);
			}
		}
		Assert.assertTrue("update failed",obClient.update(rowMutator).getResult());
		result = obClient.get(tableName, rowkey, columns);
		for(String s:columns){
			value = new Value();
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				assertEquals(Double.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("float")){
				assertEquals(Float.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("long")){
				assertEquals(Long.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("time")){
				assertEquals(Long.MIN_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("precisetime")){
				assertEquals(Long.MIN_VALUE, result.getResult().get(s));
			}
		}
	}
	
	@Test
	public  void test_update_26_addBigBoundryValue(){
		/*collectInfoTable*/
		//obClient.clearActiveMemTale();
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 26));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		columns.remove("gm_modified");
		columns.remove("gm_create");
		updateAndVerify_case26(rowkey, rowMutator, columns, collectInfoTableName, collectInfoTable);
		/*itemInfoTable*/
		rowMutator = new UpdateMutator(itemInfoTableName, transKey(rowkey));
		columns = itemInfoTable.getAllColumnNames();
		updateAndVerify_case26(transKey(rowkey), rowMutator, columns, itemInfoTableName, itemInfoTable);
		
		/*After itemInfoTable update ,verify collectInfo Table*/
		columns = collectInfoTable.getJoinColumnNames();
		afterItemInfoUpdateVerify_case26(rowkey, columns, collectInfoTable);
	}
	private void afterItemInfoUpdateVerify_case26(RKey rowkey, Set<String> columns, ITable table) {
		Result<RowData> result=obClient.get(collectInfoTableName, rowkey, columns);
		CollectColumnEnum temp;
		for(String s:columns){
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				assertEquals(Double.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("float")){
				assertEquals(Float.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("long")){
				assertEquals(Long.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("time")){
				assertEquals(Long.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("precisetime")){
				assertEquals(Long.MAX_VALUE, result.getResult().get(s));
			}
		}
	}
	private void updateAndVerify_case26(RKey rowkey, UpdateMutator rowMutator,
			Set<String> columns, String tableName, ITable table) {
		CollectColumnEnum temp;
		Value value;
		for(String s:columns){
			value = new Value();
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				value.setDouble(Double.MAX_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("float")){
				value.setFloat(Float.MAX_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("long")){
				value.setNumber(Long.MAX_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("time")){
				value.setSecond(Long.MAX_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("precisetime")){
				value.setMicrosecond(Long.MAX_VALUE);
				rowMutator.addColumn(s, value,false);
			}
		}
		Assert.assertTrue("update failed",obClient.update(rowMutator).getResult());
		result = obClient.get(tableName, rowkey, columns);
		for(String s:columns){
			value = new Value();
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				assertEquals(Double.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("float")){
				assertEquals(Float.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("long")){
				assertEquals(Long.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("time")){
				assertEquals(Long.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("precisetime")){
				assertEquals(Long.MAX_VALUE, result.getResult().get(s));
			}
		}
	}
	
	@Test
	public  void test_update_29_Too_Long_String_update(){
		String table="collect_info";
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 29));
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		
		UpdateMutator rowMutator=new UpdateMutator(table, rowkey);
		result=obClient.get(table, rowkey, columns);
		String beforeupdateuser_nick=result.getResult().get("user_nick");
		Value value=new Value();
		String s="012345678901234567890123456789012";  //length should less or equal than 32
		value.setString(s);
		rowMutator.addColumn("user_nick", value, false);
		StringBuffer sb = new StringBuffer();
		sb.setLength(513);
		System.out.println("sb.toString()" +sb.toString());
		value = new Value();
		value.setString(sb.toString());
		rowMutator.addColumn("note", value, false);
		Assert.assertFalse("update successed",obClient.update(rowMutator).getResult());
		result=obClient.get(table, rowkey, columns);
		//assertEquals(beforeupdateuser_nick, result.getResult().get("user_nick"));
	}
	@Test
	public  void test_update_31_NameAndValueAllNullNoType(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 31));
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Value value=new Value();
		try{
		rowMutator.addColumn(null, value, false);
		}catch(IllegalArgumentException e){
			exceptionMessage= e.getMessage();
		}finally{
			assertEquals("column name null", exceptionMessage);
			exceptionMessage=null;
		}
		try{
		obClient.update(rowMutator);
		}catch(IllegalArgumentException e){
			exceptionMessage=e.getMessage();
		}finally{
			assertEquals("columns null", exceptionMessage);
			exceptionMessage=null;
		}
		result=obClient.get(collectInfoTableName, rowkey, columns);
		assertEquals(null, result.getResult().get(""));
	}
	
	@Test
	public  void test_update_32_AddBaseNull(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 32));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Value value = new Value();
		rowMutator.addColumn("user_nick", value, false);
		rowMutator.addColumn("is_shared", value, false);
		Assert.assertTrue("update failed",obClient.update(rowMutator).getResult());
		value = new Value();
		value.setString("user_nick");
		rowMutator.addColumn("user_nick", value, false);
		value = new Value(); value.setNumber(12);
		rowMutator.addColumn("is_shared", value, false);
		Assert.assertTrue("update failed",obClient.update(rowMutator).getResult());
		result=obClient.get(collectInfoTableName, rowkey, collectInfoTable.getNonJoinColumnNames());
		assertEquals("user_nick", result.getResult().get("user_nick"));
		assertEquals(new Long(12), result.getResult().get("is_shared"));
	}
	
	@Test
	public  void test_update_33_OOOEColumns(){
		//same as case 1 
		/* CollectInfo */
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 1));
		Set<String> nonJoin=new HashSet<String>();
		nonJoin=collectInfoTable.getNonJoinColumnNames();
		nonJoin.remove("gm_modified");
		nonJoin.remove("gm_create");
		updateAndVerify(collectInfoTableName, rowkey, nonJoin, 1);
		
		/*verify collectinfo table join columns can not update*/
		Set<String> join=new HashSet<String>();
		join = collectInfoTable.getJoinColumnNames();
		updateCollectInforJionAndVerify(collectInfoTableName, rowkey, join, 1,false);
		
		/* ItemInfo*/
		updateItemInfoAndVerify(itemInfoTable.getTableName(), transKey(rowkey), itemInfoTable.getNonJoinColumnNames(), 1,false);
		Result<RowData> rc=obClient.get(collectInfoTableName, rowkey, collectInfoTable.getJoinColumnNames());
		/* after iteminfo table update ,check collectinfo table join columns.*/
		for(String str:collectInfoTable.getJoinColumnNames()){
			assertEquals(str, collectInfoTable.genColumnUpdateResult(str, rowkey, 1, false).getObject(false).getValue(), rc.getResult().get(str));
		}
	}
	
	@Test
	public  void test_update_34_AddNegative(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 34));
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		result = obClient.get(collectInfoTableName, rowkey, columns);
		long is_shared,collect_time,status,category,collect_time_precise;
		long ladd=-26;
		is_shared=result.getResult().<Long>get("is_shared");
		collect_time=result.getResult().<Long>get("collect_time");
		status=result.getResult().<Long>get("status");
		category=result.getResult().<Long>get("category");
		collect_time_precise=result.getResult().<Long>get("collect_time_precise");
				
		Value value=new Value();value.setNumber(ladd);
		rowMutator.addColumn("is_shared", value, true);
		value=new Value();value.setSecond(ladd);
		rowMutator.addColumn("collect_time", value, true);
		value=new Value();value.setNumber(ladd);
		rowMutator.addColumn("status", value, true);
		rowMutator.addColumn("category", value, true);
		value=new Value();value.setMicrosecond(ladd);
		rowMutator.addColumn("collect_time_precise", value, true);
		obClient.update(rowMutator);
		result = obClient.get(collectInfoTableName, rowkey, columns);
		Assert.assertEquals(is_shared+ladd, result.getResult().get("is_shared"));
		Assert.assertEquals(collect_time+ladd, result.getResult().get("collect_time"));
		Assert.assertEquals(status+ladd, result.getResult().get("status"));
		Assert.assertEquals(category+ladd, result.getResult().get("category"));
		Assert.assertEquals(collect_time_precise+ladd, result.getResult().get("collect_time_precise"));
	}
	
	@Test
	public  void test_update_36_midColumnRepeat(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 34));
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Value value;
		value=new Value();
		value.setNumber(+14);
		rowMutator.addColumn("is_shared", value,false);
		value=new Value();
		value.setString("user_nick");
		rowMutator.addColumn("user_nick", value,false);
		value=new Value();
		value.setNumber(26);
		rowMutator.addColumn("is_shared", value,false);
		
		Assert.assertTrue("updata failed",obClient.update(rowMutator).getResult());
		result = obClient.get(collectInfoTableName, rowkey, columns);
		Assert.assertEquals("user_nick", result.getResult().get("user_nick"));
		Assert.assertEquals((long)26,result.getResult().get("is_shared"));
	}
	
	/*add a case to test update time type column to 2^56*/
	@Test
	public  void test_update_37_addBigBoundryValue(){
		/*collectInfoTable*/
		//obClient.clearActiveMemTale();
		RKey rowkey = new RKey(new CollectInfoKeyRule(4, (byte)'0', 26));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		columns.remove("gm_modified");
		columns.remove("gm_create");
		updateAndVerify_case37(rowkey, rowMutator, columns, collectInfoTableName, collectInfoTable);
		/*itemInfoTable*/
		rowMutator = new UpdateMutator(itemInfoTableName, transKey(rowkey));
		columns = itemInfoTable.getAllColumnNames();
		updateAndVerify_case37(transKey(rowkey), rowMutator, columns, itemInfoTableName, itemInfoTable);
		
		/*After itemInfoTable update ,verify collectInfo Table*/
		columns = collectInfoTable.getJoinColumnNames();
		afterItemInfoUpdateVerify_case37(rowkey, columns, collectInfoTable);
	}
	private void afterItemInfoUpdateVerify_case37(RKey rowkey, Set<String> columns, ITable table) {
		Result<RowData> result=obClient.get(collectInfoTableName, rowkey, columns);
		CollectColumnEnum temp;
		for(String s:columns){
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				assertEquals(Double.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("float")){
				assertEquals(Float.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("long")){
				assertEquals(Long.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("time")){
				assertEquals((long)Math.pow(2, 56), result.getResult().get(s));
			}
			if(temp.getType().equals("precisetime")){
				assertEquals((long)Math.pow(2, 56), result.getResult().get(s));
			}
		}
	}
	private void updateAndVerify_case37(RKey rowkey, UpdateMutator rowMutator,
			Set<String> columns, String tableName, ITable table) {
		CollectColumnEnum temp;
		Value value;
		for(String s:columns){
			value = new Value();
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				value.setDouble(Double.MAX_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("float")){
				value.setFloat(Float.MAX_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("long")){
				value.setNumber(Long.MAX_VALUE);
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("time")){
				value.setSecond((long)Math.pow(2, 56));
				rowMutator.addColumn(s, value,false);
			}
			if(temp.getType().equals("precisetime")){
				value.setMicrosecond((long)Math.pow(2, 56));
				rowMutator.addColumn(s, value,false);
			}
		}
		Assert.assertTrue("update failed",obClient.update(rowMutator).getResult());
		result = obClient.get(tableName, rowkey, columns);
		for(String s:columns){
			value = new Value();
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("double")){
				assertEquals(Double.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("float")){
				assertEquals(Float.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("long")){
				assertEquals(Long.MAX_VALUE, result.getResult().get(s));
			}
			if(temp.getType().equals("time")){
				assertEquals((long)Math.pow(2, 56), result.getResult().get(s));
			}
			if(temp.getType().equals("precisetime")){
				assertEquals((long)Math.pow(2, 56), result.getResult().get(s));
			}
		}
	}
	
	@Test
	public void transaction_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		int times=0;
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm);
		listmutator.add(im);listmutator.add(um);
		Result<Boolean> rs=obClient.update(listmutator);
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 1, false).getObject(false).getValue(),
					rd.getResult().get(str));
		}
	}
	
}
		

