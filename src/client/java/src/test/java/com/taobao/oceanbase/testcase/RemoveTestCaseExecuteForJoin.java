package com.taobao.oceanbase.testcase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.oceanbase.base.BaseCase;
import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.RowKey;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.Value;
public class RemoveTestCaseExecuteForJoin extends BaseCase{

	/**
	 * @param args
	 */
	private static String exceptionMessage=new String();
	private String collectInfoTableName = collectInfoTable.getTableName();
	private String itemInfoTableName = itemInfoTable.getTableName();
	static Result<RowData> result=null;

	// trans collect_info rkey to item_info rkey
	private RKey transKey(RKey rkey) {
		ItemInfoKeyRule rule = new ItemInfoKeyRule(
				((CollectInfoKeyRule) (rkey.getKeyRule())).getItem_type(),
				((CollectInfoKeyRule) (rkey.getKeyRule())).getItem_id());
		return new RKey(rule);
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
	public  void test_remove_01_HappyPath() {
		/* step1 delete collect table than check the iteminfo table*/
		//obClient.clearActiveMemTale();
		RKey rowkey = new RKey(new CollectInfoKeyRule(3, (byte)'0', 1));//change the value of rowkey
		result = obClient.get(collectInfoTableName, rowkey,  collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			Assert.assertEquals(str, collectInfoTable.getColumnValue(str, rowkey), result.getResult().get(str));
		}
		assertTrue("delete failed",obClient.delete(collectInfoTableName, rowkey).getResult());
		result = obClient.get(collectInfoTableName, rowkey, collectInfoTable.getNonJoinColumnNames());
		assertEquals(ResultCode.OB_SUCCESS,result.getCode());
		assertTrue("No records find!", result.getResult().isEmpty());
		
		result = obClient.get(itemInfoTableName, transKey(rowkey),  itemInfoTable.getNonJoinColumnNames());
		for(String str:itemInfoTable.getNonJoinColumnNames()){
			Assert.assertEquals(str, itemInfoTable.getColumnValue(str, transKey(rowkey)), result.getResult().get(str));
		}
		
		//step2 delete iteminfo table than check the collectinfo table
		rowkey = new RKey(new CollectInfoKeyRule(3, (byte)'0', 50));
		assertTrue("delete failed",obClient.delete(itemInfoTableName, transKey(rowkey)).getResult());
		result = obClient.get(itemInfoTableName, rowkey, itemInfoTable.getNonJoinColumnNames());
		assertEquals(ResultCode.OB_SUCCESS,result.getCode());
		assertTrue("No records find!", result.getResult().isEmpty());
		result = obClient.get(collectInfoTableName, rowkey,  collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getNonJoinColumnNames()){
			Assert.assertEquals(str, collectInfoTable.getColumnValue(str, rowkey), result.getResult().get(str));
		}
		for(String str:collectInfoTable.getJoinColumnNames()){
			Assert.assertEquals(str, null, result.getResult().get(str));
		}
	}
	
	@Test
	public  void test_remove_02_DataNotExist(){
		RKey rowkey = new RKey(new CollectInfoKeyRule(3, (byte)'1', 1));//change the value of rowkey
		result=obClient.get( collectInfoTableName, rowkey, collectInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS,result.getCode());
		assertTrue("No records find!", result.getResult().isEmpty());
		assertTrue("delete failed",obClient.delete(collectInfoTableName, rowkey).getResult());
		result=obClient.get( collectInfoTableName, rowkey, collectInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS,result.getCode());
		assertTrue("No records find!", result.getResult().isEmpty());
	}
	@Test
	public  void test_remove_03_tableIsNull(){
		String table=null;
		RKey rowkey = new RKey(new CollectInfoKeyRule(3, (byte)'0', 3));//change the value of rowkey
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		try{
		result=obClient.get(table, rowkey, columns);
		}catch(IllegalArgumentException e){
		 exceptionMessage=e.getMessage();
		}finally{
			assertEquals(exceptionMessage, "table name null");
			exceptionMessage=null;
		}
		
		try {
			assertTrue("delete failed",obClient.delete(table, rowkey).getResult());
		} catch (IllegalArgumentException e) {
			exceptionMessage = e.getMessage();
		} finally {
			assertEquals(exceptionMessage, "table name null");
			exceptionMessage=null;
		}
	}
	@Test
	public  void test_remove_04_tableIsBlank(){
		String table="";
		RKey rowkey = new RKey(new CollectInfoKeyRule(3, (byte)'0', 4));//change the value of rowkey
		Set<String> columns = collectInfoTable.getNonJoinColumnNames();
		try{
		result=obClient.get(table, rowkey, columns);
		}catch(IllegalArgumentException e){
		 exceptionMessage=e.getMessage();
		}finally{
			assertEquals(exceptionMessage, "table name null");
			exceptionMessage=null;
		}
		
		try {
		assertTrue("delete failed",obClient.delete(table, rowkey).getResult());
		} catch (IllegalArgumentException e) {
			exceptionMessage = e.getMessage();
		} finally {
			assertEquals(exceptionMessage, "table name null");
			exceptionMessage=null;
		}
	}
	@Test
	public  void test_remove_05_rowkeyIsNull(){
		RowKey rowkey=null;
		try {
			assertTrue("delete failed",obClient.delete(collectInfoTableName, rowkey).getResult());
		} catch (IllegalArgumentException e) {
			exceptionMessage = e.getMessage();
		} finally {
			assertEquals(exceptionMessage, "rowkey is null");
			exceptionMessage=null;
		}
	}

	@Test
	public  void test_remove_06_rowkeyIsBlank(){
		RKey rowkey = new RKey(new byte[] {});
		try {
			result=obClient.get(collectInfoTableName, rowkey, collectInfoTable.getAllColumnNames());
		} catch (IllegalArgumentException e) {
			exceptionMessage = e.getMessage();
		} finally {
			assertEquals(exceptionMessage, "rowkey bytes null");
		}
		try {
			assertTrue("delete failed",obClient.delete(collectInfoTableName, rowkey).getResult());
		} catch (IllegalArgumentException e) {
			exceptionMessage = e.getMessage();
		} finally {
			assertEquals(exceptionMessage, "rowkey bytes null");
		}
	}
	@Test
	public  void test_remove_09_InnormalDataHappyPath(){
	
		RKey rowkey = new RKey(new CollectInfoKeyRule(3, (byte)'0', 9));
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTableName, rowkey);
		Value value;
		value=new Value();
		rowMutator.addColumn("user_nick", value,false);
		rowMutator.addColumn("is_shared", value,false);
		rowMutator.addColumn("note", value,false);
		rowMutator.addColumn("collect_time", value,false);
		rowMutator.addColumn("status", value,false);
		rowMutator.addColumn("tag", value,false);
		rowMutator.addColumn("category", value,false);
		rowMutator.addColumn("collect_time_precise", value,false);
		Assert.assertTrue("update failed",obClient.update(rowMutator).getResult());
		result = obClient.get(collectInfoTableName, rowkey, collectInfoTable.getAllColumnNames());
		Assert.assertEquals(null, result.getResult().get("user_nick"));
		Assert.assertEquals(null,result.getResult().get("is_shared"));
		Assert.assertEquals(null, result.getResult().get("note"));
		Assert.assertEquals(null, result.getResult().get("collect_time"));
		Assert.assertEquals(null,result.getResult().get("status"));
		Assert.assertEquals(null, result.getResult().get("tag"));
		Assert.assertEquals(null, result.getResult().get("category"));
		Assert.assertEquals(null, result.getResult().get("collect_time_precise"));
		
		assertTrue("update failed ",obClient.update(rowMutator).getResult());
		
		assertTrue("delete failed",obClient.delete(collectInfoTableName, rowkey).getResult());
		result = obClient.get(collectInfoTableName, rowkey, collectInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS,result.getCode());
		assertTrue("No records find!", result.getResult().isEmpty());
		
	}
}
