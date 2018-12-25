package com.taobao.oceanbase.testcase;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;
import java.util.Date;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.taobao.oceanbase.base.BaseCase;
import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.RowData;

public class GetTestCaseExecuteForJoin extends BaseCase{
	/*column_info=1,2,user_nick,varchar,32
column_info=1,3,is_shared,int
column_info=1,4,note,varchar,512
column_info=1,5,collect_time,datetime
column_info=1,6,status,int
column_info=1,7,gm_create,create_time
column_info=1,8,gm_modified,modify_time
column_info=1,9,tag,varchar,105
column_info=1,10,category,int
column_info=1,11,title,varchar,256
column_info=1,12,picurl,varchar,256
column_info=1,13,owner_id,int
column_info=1,14,owner_nick,varchar,32
column_info=1,15,price,float
column_info=1,16,ends,datetime
column_info=1,17,proper_xml,varchar,2048
column_info=1,18,comment_count,int
column_info=1,19,collector_count,int
column_info=1,20,collect_count,int
column_info=1,21,total_money,double
column_info=1,22,collect_time_precise,precise_datetime

rowkey_max_length=17
rowkey_split=8

	 * 
	 */

	public Set<String> setOutofOrderColumnsforCollect(){
		Set<String> Columns=new HashSet<String>();
		Columns.add(CollectColumns[0]);
		Columns.add(CollectColumns[3]);
		Columns.add(CollectColumns[1]);
		Columns.add(CollectColumns[2]);
		Columns.add(CollectColumns[4]);
		return Columns;
	}
	
	public Set<String> setOutofOrderColumnsforItem(){
		Set<String> Columns=new HashSet<String>();
		Columns.add(ItemColumns[0]);
		Columns.add(ItemColumns[3]);
		Columns.add(ItemColumns[1]);
		Columns.add(ItemColumns[2]);
		Columns.add(ItemColumns[4]);
		return Columns;
	}
	
	public Set<String> setPartColumnsforCollect(){
		Set<String> Columns=new HashSet<String>();
		Columns.add(CollectColumns[1]);
		Columns.add(CollectColumns[7]);	
		Columns.add(CollectColumns[18]);
		return Columns;
	}
	
	public Set<String> setPartColumnsforItem(){
		Set<String> Columns=new HashSet<String>();
		Columns.add(ItemColumns[1]);
		Columns.add(ItemColumns[3]);
		Columns.add(ItemColumns[4]);
		return Columns;
	}
	
	private String []CollectColumns = collectInfoTable.getAllColumnNames().toArray(new String[0]);
	private String []ItemColumns = itemInfoTable.getAllColumnNames().toArray(new String[0]);
	
	@Before
	public void setUp() throws Exception {
		obClient.clearActiveMemTale();
	}	

	@After
	public void tearDown() throws Exception {
		obClient.clearActiveMemTale();
	}
	
	@Test
	public void test_get01_HappyPath(){		
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 1));
//		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		Result<RowData> resultForCollect = obClient.get("collect_info", rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 1));
//		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		Result<RowData> resultForItem = obClient.get("item_info", rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
	}
	
	@Test
	public void test_get02_Nodata(){ 
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'1', 2));
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS, resultForCollect.getCode());
		assertTrue("No records find!", resultForCollect.getResult().isEmpty());
				
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'1', 2));
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		assertEquals(ResultCode.OB_SUCCESS, resultForItem.getCode());
		assertTrue("No records find!", resultForItem.getResult().isEmpty()); 
	}
	
	@Test
	public void test_get03_PartOfAllColumns(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 3));
		Set<String> Columns1 = setPartColumnsforCollect();
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		for(String str:Columns1){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 3));
		Set<String> Columns2 = setPartColumnsforItem();
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		for(String str:Columns2){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
	}
	
	@Test
	public void test_get04_TableNameIncorrect(){
		String table="wrong_tableName";
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 4));
		Result<RowData> resultForCollect;
		try {
			resultForCollect = obClient.get(table, rowkeyForCollect, collectInfoTable.getAllColumnNames());
		}
		catch(RuntimeException e) {
			assertEquals("tablets returned by rootserver are null", e.getMessage());
		}		
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 4));
		Result<RowData> resultForItem;
		try {
			resultForItem = obClient.get(table, rowkeyForItem, itemInfoTable.getAllColumnNames());
		}
		catch(RuntimeException e) {
			assertEquals("tablets returned by rootserver are null", e.getMessage());
		}
	}
	
	@Test
	public void test_get05_tableIsNull(){
		String table = null;
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 5));
		Result<RowData> resultForCollect;
		try {
			resultForCollect = obClient.get(table, rowkeyForCollect, collectInfoTable.getAllColumnNames());
		}
		catch(IllegalArgumentException e) {
			assertEquals("table name null", e.getMessage());
		}		
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 5));
		Result<RowData> resultForItem;
		try {
			resultForItem = obClient.get(table, rowkeyForItem, itemInfoTable.getAllColumnNames());
		}
		catch(IllegalArgumentException e) {
			assertEquals("table name null", e.getMessage());
		}		
	}
	
	@Test
	public void test_get06_tableIsBlank(){
		String table="";
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 6));
		Result<RowData> resultForCollect;
		try {
			resultForCollect = obClient.get(table, rowkeyForCollect, collectInfoTable.getAllColumnNames());
		}
		catch(IllegalArgumentException e) {
			assertEquals("table name null", e.getMessage());
		}		
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 6));
		Result<RowData> resultForItem;
		try {
			resultForItem = obClient.get(table, rowkeyForItem, itemInfoTable.getAllColumnNames());
		}
		catch(IllegalArgumentException e) {
			assertEquals("table name null", e.getMessage());
		}		
	}
	
	@Test
	public void test_get07_rowkeyIsNull(){
		/* CollectInfo */
		RKey rowkeyForCollect = null;
		Result<RowData> resultForCollect;
		try {
			resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		}
		catch(IllegalArgumentException e) {
			assertEquals("rowkey is null", e.getMessage());
		}		
		
		/* ItemInfo */
		RKey rowkeyForItem = null;
		Result<RowData> resultForItem;
		try {
			resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		}
		catch(IllegalArgumentException e) {
			assertEquals("rowkey is null", e.getMessage());
		}		
	}
	
	@Test
	public void test_get08_rowkeyIsBlank(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new byte[] {});
		Result<RowData> resultForCollect;
		try {
			resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		}
		catch(IllegalArgumentException e) {
			assertEquals("rowkey bytes null", e.getMessage());
		}		
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new byte[] {});
		Result<RowData> resultForItem;
		try {
			resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		}
		catch(IllegalArgumentException e) {
			assertEquals("rowkey bytes null", e.getMessage());
		}		
	}
	
	@Test
	public void test_get09_ColumnsIsNull(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 9));
		Set<String> Columns1 = null;
		Result<RowData> resultForCollect;
		try {
			resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		}
		catch(IllegalArgumentException e) {
			assertEquals("columns are null", e.getMessage());
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 9));
		Set<String> Columns2 = null;
		Result<RowData> resultForItem; 
		try {
			resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		}
		catch(IllegalArgumentException e) {
			assertEquals("columns are null", e.getMessage());
		}
	}
	
	@Test
	public void test_get11_OneValueOfColumnNull(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 11));
		Set<String> Columns1 = new HashSet<String>();
		Columns1.add(null);
		for(int i = 1; i < CollectColumns.length; i++)
			Columns1.add(CollectColumns[i]);
		Result<RowData> resultForCollect;
		try {
			resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		}
		catch(IllegalArgumentException e) {
			assertEquals("columns are null", e.getMessage());
		} 
				
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 11));
		Set<String> Columns2 = new HashSet<String>();
		Columns2.add(null);
		for(int i = 1; i < ItemColumns.length; i++)
			Columns2.add(ItemColumns[i]);
		Result<RowData> resultForItem;
		try {
			resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		}
		catch(IllegalArgumentException e) {
			assertEquals("columns are null", e.getMessage());
		} 
	}	
	
	@Test
	public void test_get12_ThreeValueOfColumnsNull(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 12));
		Set<String> Columns1 = new HashSet<String>();
		Columns1.add(null);Columns1.add(null);Columns1.add(null);
		for(int i = 3; i < CollectColumns.length; i++)
			Columns1.add(CollectColumns[i]);
		Result<RowData> resultForCollect;
		try {
			resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		}
		catch(IllegalArgumentException e) {
			assertEquals("columns are null", e.getMessage());
		} 
				
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 12));
		Set<String> Columns2 = new HashSet<String>();
		Columns2.add(null);Columns2.add(null);Columns2.add(null);
		for(int i = 3; i < ItemColumns.length; i++)
			Columns2.add(ItemColumns[i]);
		Result<RowData> resultForItem;
		try {
			resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		}
		catch(IllegalArgumentException e) {
			assertEquals("columns are null", e.getMessage());
		} 
	}
	
	@Test
	public void test_get13_ThreeValueOfColumnsBlank(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 13));
		Set<String> Columns1 = new HashSet<String>();
		Columns1.add("");Columns1.add("");Columns1.add("");
		for(int i = 3; i < CollectColumns.length; i++)
			Columns1.add(CollectColumns[i]);
		Result<RowData> resultForCollect;
		try {
			resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		}
		catch(IllegalArgumentException e) {
			assertEquals("columns have null column", e.getMessage());
		} 
				
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 13));
		Set<String> Columns2 = new HashSet<String>();
		Columns2.add("");Columns2.add("");Columns2.add("");
		for(int i = 3; i < ItemColumns.length; i++)
			Columns2.add(ItemColumns[i]);
		Result<RowData> resultForItem;
		try {
			resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		}
		catch(IllegalArgumentException e) {
			assertEquals("columns have null column", e.getMessage());
		} 
	}
	
	@Test
	public void test_get14_FirstColumnWrong(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 14));
		Set<String> Columns1 = new HashSet<String>();
		Columns1.add("wrong");
		for(int i = 1; i < CollectColumns.length; i++)
			Columns1.add(CollectColumns[i]);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		assertEquals(null, resultForCollect.getResult());
		assertEquals("[-29, request not conform to schema]", resultForCollect.getCode().toString());
					
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 14));
		Set<String> Columns2 = new HashSet<String>();
		Columns2.add("wrong");
		for(int i = 1; i < ItemColumns.length; i++)
			Columns2.add(ItemColumns[i]);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		assertEquals(null, resultForItem.getResult());
		assertEquals("[-29, request not conform to schema]", resultForItem.getCode().toString());
	}
	
	@Test
	public void test_get15_SomeColumnsWrong(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 15));
		Set<String> Columns1 = new HashSet<String>();
		Columns1.add("wrong");
		for(int i = 1;i < 4;i++)
			Columns1.add(CollectColumns[i]);
		Columns1.add("wrong");
		for(int i = 5;i < 17;i++)
			Columns1.add(CollectColumns[i]);
		Columns1.add("wrong");
		for(int i = 18;i < CollectColumns.length;i++)
			Columns1.add(CollectColumns[i]);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		assertEquals(null, resultForCollect.getResult());
		assertEquals("[-29, request not conform to schema]", resultForCollect.getCode().toString());
					
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 15));
		Set<String> Columns2 = new HashSet<String>();
		Columns2.add("wrong");
		for(int i = 1;i < 3;i++)
			Columns2.add(ItemColumns[i]);
		Columns2.add("wrong");
		for(int i = 4;i < 7;i++)
			Columns2.add(ItemColumns[i]);
		Columns2.add("wrong");
		for(int i = 8;i < ItemColumns.length;i++)
			Columns2.add(ItemColumns[i]);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		assertEquals(null, resultForItem.getResult());
		assertEquals("[-29, request not conform to schema]", resultForItem.getCode().toString());
	}
	
	@Test
	public void test_get16_AmoreColumnsLast(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 16));
		Set<String> Columns1 = new HashSet<String>();
		for(int i = 0; i < CollectColumns.length; i++)
			Columns1.add(CollectColumns[i]);
		Columns1.add("wrong_column");
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		assertEquals(null, resultForCollect.getResult());
		assertEquals("[-29, request not conform to schema]", resultForCollect.getCode().toString());
					
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 16));
		Set<String> Columns2 = new HashSet<String>();
		for(int i = 0; i < ItemColumns.length; i++)
			Columns2.add(ItemColumns[i]);
		Columns2.add("wrong_column");
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		assertEquals(null, resultForItem.getResult());
		assertEquals("[-29, request not conform to schema]", resultForItem.getCode().toString());
	}
	
	@Test
	public void test_get17_AmoreColumnsMid(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 17));
		Set<String> Columns1 = new HashSet<String>();
		for(int i = 0; i < 6;i++)
			Columns1.add(CollectColumns[i]);
		Columns1.add("wrong_column");
		for(int i = 7; i < CollectColumns.length;i++)
			Columns1.add(CollectColumns[i]);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		assertEquals(null, resultForCollect.getResult());
		assertEquals("[-29, request not conform to schema]", resultForCollect.getCode().toString());
					
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 17));
		Set<String> Columns2 = new HashSet<String>();
		for(int i = 0; i < 6;i++)
			Columns2.add(ItemColumns[i]);
		Columns2.add("wrong_column");
		for(int i = 7; i < ItemColumns.length;i++)
			Columns2.add(ItemColumns[i]);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		assertEquals(null, resultForItem.getResult());
		assertEquals("[-29, request not conform to schema]", resultForItem.getCode().toString());
	}
	
	@Test
	public void test_get18_OneColumnRepeat(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 18));
		Set<String> Columns1 = new HashSet<String>();
		for(int i = 0; i < 5;i++)
			Columns1.add(CollectColumns[i]);
		Columns1.add(CollectColumns[4]);
		for(int i = 6; i < CollectColumns.length;i++)
			Columns1.add(CollectColumns[i]);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		for(String str:Columns1){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
					
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 18));
		Set<String> Columns2 = new HashSet<String>();
		for(int i = 0; i < 5;i++)
			Columns2.add(ItemColumns[i]);
		Columns2.add(ItemColumns[4]);
		for(int i = 6; i < ItemColumns.length;i++)
			Columns2.add(ItemColumns[i]);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);;
		for(String str:Columns2){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
	}
	
	@Test
	public void test_get19_ColumnMixWrong(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 19));
		Set<String> Columns1 = new HashSet<String>();
		Columns1.add(null);
		Columns1.add("");
		Columns1.add("wrong");
		for(int i = 3;i < CollectColumns.length;i++)
			Columns1.add(CollectColumns[i]);
		Result<RowData> resultForCollect;
		try {
			resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		}
		catch(IllegalArgumentException e) {
			assertEquals("columns are null", e.getMessage());
		}
					
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 19));
		Set<String> Columns2 = new HashSet<String>();
		Columns2.add(null);
		Columns2.add("");
		Columns2.add("wrong");
		for(int i = 3;i < ItemColumns.length;i++)
			Columns2.add(ItemColumns[i]);
		Result<RowData> resultForItem;
		try {
			resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		}
		catch(IllegalArgumentException e) {
			assertEquals("columns are null", e.getMessage());
		}
	}
	
	@Test
	public void test_get20_InnormalDataAllColumns(){
		// prepare data
		InsertTestCaseExecuteForJoin prepare = new InsertTestCaseExecuteForJoin();
		// CollectInfo
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 20));
		InsertMutator imforCollect = prepare.getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect, 14);
		Result<Boolean> rsforCollect = obClient.insert(imforCollect);
		Date time = new Date();
		long modify_time = time.getTime();
		assertTrue("Insert fail!", rsforCollect.getResult());
		// ItemInfo
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 20));
		InsertMutator imforItem = prepare.getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 14);
		Result<Boolean> rsforItem = obClient.insert(imforItem);
		assertTrue("Insert fail!", rsforItem.getResult());
		
		/* CollectInfo */
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			if (str.equals("gm_create"))
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
			else if (str.equals("gm_modified"))  {
				long diff = Long.parseLong(resultForCollect.getResult().get(str).toString()) - modify_time*1000; 
				assertEquals(str, diff/1000000, 0);
			}
			else 
				assertEquals(str, null, resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, null, resultForItem.getResult().get(str));
		}
	}
	
	@Test
	public void test_get21_InnormalDataPartColumns(){
		// prepare data
		InsertTestCaseExecuteForJoin prepare = new InsertTestCaseExecuteForJoin();
		// CollectInfo
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 21));
		InsertMutator imforCollect = prepare.getAbnormalIMforCollectNonJoin(collectInfoTable.getTableName(), rowkeyForCollect, 14);
		Result<Boolean> rsforCollect = obClient.insert(imforCollect);
		Date time = new Date();
		long modify_time = time.getTime();
		assertTrue("Insert fail!", rsforCollect.getResult());
		// ItemInfo
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 21));
		InsertMutator imforItem = prepare.getAbnormalIMforItem(itemInfoTable.getTableName(), rowkeyForItem, 14);
		Result<Boolean> rsforItem = obClient.insert(imforItem);
		assertTrue("Insert fail!", rsforItem.getResult());
		
		/* CollectInfo */
		Set<String> Columns1 = setPartColumnsforCollect();
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		for(String str:Columns1){
			if (str.equals("gm_create"))
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
			else if (str.equals("gm_modified"))  {
				long diff = Long.parseLong(resultForCollect.getResult().get(str).toString()) - modify_time*1000; 
				assertEquals(str, diff/1000000, 0);
			}
			else 
				assertEquals(str, null, resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		Set<String> Columns2 = setPartColumnsforItem();
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		for(String str:Columns2){
			assertEquals(str, null, resultForItem.getResult().get(str));
		}
	}
	
	@Test
	public void test_get22_ColumnsIsOutofOrder(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(1, (byte)'0', 22));
		Set<String> Columns1 = setOutofOrderColumnsforCollect();
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, Columns1);
		for(String str:Columns1){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 22));
		Set<String> Columns2 = setOutofOrderColumnsforItem();
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, Columns2);
		for(String str:Columns2){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
	}
	
	@Test
	public void test_get23_get_serval_times(){		
		/* CollectInfo */
		for (int iLoop = 0; iLoop < 10; iLoop ++)
		{
			RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(2, (byte)'0', 1));
			Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getAllColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
			}
		}
		
		/* ItemInfo */
		for (int iLoop = 0; iLoop < 10; iLoop ++)
		{
			RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 1));
			Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			for(String str:itemInfoTable.getAllColumnNames()){
				assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
			}
		}
	}
}

