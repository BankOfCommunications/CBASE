package com.taobao.oceanbase.testcase;

import static org.junit.Assert.*;

import java.util.Set;
//import java.util.HashSet;
//import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
//import org.junit.Ignore;
import com.taobao.oceanbase.base.CollectColumnEnum;
import com.taobao.oceanbase.base.BaseCase;
import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Value;

public class GetFunctionTestCaseForJoin extends BaseCase{
	
//	private String []CollectColumns = collectInfoTable.getAllColumnNames().toArray(new String[0]);
	private String []CollectJoinColumns = collectInfoTable.getJoinColumnNames().toArray(new String[0]);
//	private String []CollectNonJoinColumns = collectInfoTable.getNonJoinColumnNames().toArray(new String[0]);
	private String []CollectInsertColumns = setInsertColumnsforCollect().toArray(new String[0]);
	private String []ItemColumns = itemInfoTable.getAllColumnNames().toArray(new String[0]);
	
	public Set<String> setInsertColumnsforCollect(){
		Set<String> Columns = collectInfoTable.getNonJoinColumnNames(); 
		Columns.remove("gm_modified");
		Columns.remove("gm_create");
		return Columns; 			
	}
	
	public InsertMutator getNormalIMforCollect(RKey rowkey, int times){
		InsertMutator mutator = new InsertMutator(collectInfoTable.getTableName(), rowkey);	
		CollectColumnEnum temp;
		for(int i = 0; i < CollectInsertColumns.length; i++){
			Value value = new Value();
			String s = CollectInsertColumns[i];
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string")){
				value.setString("cinsert"+String.valueOf(times));
				mutator.addColumn(s, value);
			}
			else 
				mutator.addColumn(s, collectInfoTable.genColumnUpdateResult(s, rowkey, times, false));
		}
					
		return mutator;
	}
	
	public InsertMutator getNormalIMforItem(RKey rowkey, int times){
		InsertMutator mutator = new InsertMutator(itemInfoTable.getTableName(), rowkey);	
		CollectColumnEnum temp;
		for(int i = 0; i < ItemColumns.length; i++){
			Value value = new Value();
			String s = ItemColumns[i];
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string")){
				value.setString("iinsert"+String.valueOf(times));
				mutator.addColumn(s, value);
			}
			else 
				mutator.addColumn(s, itemInfoTable.genColumnUpdateResult(s, rowkey, times, false));
		}
					
		return mutator;
	}
	
	private void insertCheckforCollect(Result<RowData> result, RKey rowkey, int times) {
		CollectColumnEnum temp;
		for(int i = 0; i < CollectInsertColumns.length; i++) {
			String s = CollectInsertColumns[i];
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string")){
				assertEquals(s, "cinsert"+String.valueOf(times), result.getResult().get(s));
			}
			else 
				assertEquals(s, collectInfoTable.genColumnUpdateResult(s, rowkey, times, false).getObject(false).getValue(), result.getResult().get(s));
		}
	}
	
	private void insertCheckforItem(Result<RowData> result, RKey rowkey, int times, boolean flag) {
		CollectColumnEnum temp;
		for(int i = 0; i < ItemColumns.length; i++) {
			String s = null;
			if (flag)
				s = ItemColumns[i];
			else 
				s = CollectJoinColumns[i];
			temp=CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string")){
				assertEquals(s, "iinsert"+String.valueOf(times), result.getResult().get(s));
			}
			else { 
				if (flag)
					assertEquals(s, itemInfoTable.genColumnUpdateResult(s, rowkey, times, false).getObject(false).getValue(), result.getResult().get(s));
				else
					assertEquals(s, collectInfoTable.genColumnUpdateResult(s, rowkey, times, false).getObject(false).getValue(), result.getResult().get(s));	
			}
		}
	}
	
	
	public UpdateMutator getNormalUMforCollect(RKey rowkey, int times){
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTable.getTableName(), rowkey);	
		CollectColumnEnum temp;
		for(int i = 0; i < CollectInsertColumns.length; i++){
			Value value = new Value();
			String s = CollectInsertColumns[i];
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string")){
				value.setString("cupdate"+String.valueOf(times));
				rowMutator.addColumn(s, value, false);
			}
			else 
				rowMutator.addColumn(s, collectInfoTable.genColumnUpdateResult(s, rowkey, times, false), false);
		}
				
		return rowMutator;
	}
	
	public UpdateMutator getNormalUMforItem(RKey rowkey, int times){
		UpdateMutator rowMutator=new UpdateMutator(itemInfoTable.getTableName(), rowkey);	
		CollectColumnEnum temp;
		for(int i = 0; i < ItemColumns.length; i++){
			Value value = new Value();
			String s = ItemColumns[i];
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string")){
				value.setString("iupdate"+String.valueOf(times));
				rowMutator.addColumn(s, value, false);
			}
			else 
				rowMutator.addColumn(s, itemInfoTable.genColumnUpdateResult(s, rowkey, times, false), false);
		}
					
		return rowMutator;
	}
	
	private void updateCheckforCollect(Result<RowData> result, RKey rowkey, int times) {
		CollectColumnEnum temp;
		for(int i = 0; i < CollectInsertColumns.length; i++) {
			String s = CollectInsertColumns[i];
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string")){
				assertEquals(s, "cupdate"+String.valueOf(times), result.getResult().get(s));
			}
			else 
				assertEquals(s, collectInfoTable.genColumnUpdateResult(s, rowkey, times, false).getObject(false).getValue(), result.getResult().get(s));
		}
	}
	
	private void updateCheckforItem(Result<RowData> result, RKey rowkey, int times, boolean flag) {
		CollectColumnEnum temp;
		for(int i = 0; i < ItemColumns.length; i++) {
			String s = null;
			if (flag)
				s = ItemColumns[i];
			else 
				s = CollectJoinColumns[i];
			temp=CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string")){
				assertEquals(s, "iupdate"+String.valueOf(times), result.getResult().get(s));
			}
			else { 
				if (flag)
					assertEquals(s, itemInfoTable.genColumnUpdateResult(s, rowkey, times, false).getObject(false).getValue(), result.getResult().get(s));
				else
					assertEquals(s, collectInfoTable.genColumnUpdateResult(s, rowkey, times, false).getObject(false).getValue(), result.getResult().get(s));	
			}
		}
	}
	
	public UpdateMutator getASUMforCollect(RKey rowkey, int num){
		UpdateMutator rowMutator=new UpdateMutator(collectInfoTable.getTableName(), rowkey);
		CollectColumnEnum temp;
		for(int i = 0; i < CollectInsertColumns.length; i++){
			String s = CollectInsertColumns[i];
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string"))
				;
			else 
				rowMutator.addColumn(s, collectInfoTable.genColumnUpdateResult(s, rowkey, num, true), true);
		}		
		return rowMutator;
	}
	
	public UpdateMutator getASUMforItem(RKey rowkey, int num){
		UpdateMutator rowMutator=new UpdateMutator(itemInfoTable.getTableName(), rowkey);
		CollectColumnEnum temp;
		for(int i = 0; i < ItemColumns.length; i++){
			String s = ItemColumns[i];
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string"))
				;
			else 
				rowMutator.addColumn(s, itemInfoTable.genColumnUpdateResult(s, rowkey, num, true), true);
		}		
		return rowMutator;
	}
	
	private void addsubCheckforCollect(Result<RowData> resultbefore, Result<RowData> result, int num) {
		CollectColumnEnum temp;
		for(int i = 0; i < CollectInsertColumns.length; i++) {
			String s = CollectInsertColumns[i];
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null)
					assertEquals(s, null, result.getResult().get(s));
				else
					assertEquals(s, resultbefore.getResult().get(s), result.getResult().get(s));			
			}
			else if(temp.getType().equals("double")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null) {
					double d = num;
					assertEquals(s, d, result.getResult().get(s));
				}
				else {
					double d = resultbefore.getResult().<Double>get(s);
					d = d + num;
					assertEquals(s, d, result.getResult().get(s));
				}
			}
			else if(temp.getType().equals("float")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null) {
					float f = num;
					assertEquals(s, f, result.getResult().get(s));
				}
				else {
					float f = resultbefore.getResult().<Float>get(s);
					f = f + num;
					assertEquals(s, f, result.getResult().get(s));
				}
			}
			else if(temp.getType().equals("long")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null) {
					long l = num;
					assertEquals(s, l, result.getResult().get(s));
				}
				else {
					long l = resultbefore.getResult().<Long>get(s);
					l = l + num;
					assertEquals(s, l, result.getResult().get(s));
				}
			}
			else if(temp.getType().equals("time")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null) {
					long t = num;
					assertEquals(s, t, result.getResult().get(s));
				}
				else {
					long t = resultbefore.getResult().<Long>get(s);
					t = t + num;
					assertEquals(s, t, result.getResult().get(s));
				}
			}
			else if(temp.getType().equals("precisetime")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null) {
					long pt = num;
					assertEquals(s, pt, result.getResult().get(s));
				}
				else {
					long pt = resultbefore.getResult().<Long>get(s);
					pt = pt + num;
					assertEquals(s, pt, result.getResult().get(s));
				}
			}
		}
	}
	
	private void addsubCheckforItem(Result<RowData> resultbefore, Result<RowData> result, int num, boolean flag) {
		CollectColumnEnum temp;
		for(int i = 0; i < ItemColumns.length; i++) {
			String s = null;
			if (flag)
				s = ItemColumns[i];
			else 
				s = CollectJoinColumns[i];
			temp = CollectColumnEnum.valueOf(s.toUpperCase());
			if(temp.getType().equals("string")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null)
					assertEquals(s, null, result.getResult().get(s));
				else
					assertEquals(s, resultbefore.getResult().get(s), result.getResult().get(s));			
			}
			else if(temp.getType().equals("double")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null) {
					double d = num;
					assertEquals(s, d, result.getResult().get(s));
				}
				else {
					double d = resultbefore.getResult().<Double>get(s);
					d = d + num;
					assertEquals(s, d, result.getResult().get(s));
				}
			}
			else if(temp.getType().equals("float")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null) {
					float f = num;
					assertEquals(s, f, result.getResult().get(s));
				}
				else {
					float f = resultbefore.getResult().<Float>get(s);
					f = f + num;
					assertEquals(s, f, result.getResult().get(s));
				}
			}
			else if(temp.getType().equals("long")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null) {
					long l = num;
					assertEquals(s, l, result.getResult().get(s));
				}
				else {
					long l = resultbefore.getResult().<Long>get(s);
					l = l + num;
					assertEquals(s, l, result.getResult().get(s));
				}
			}
			else if(temp.getType().equals("time")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null) {
					long t = num;
					assertEquals(s, t, result.getResult().get(s));
				}
				else {
					long t = resultbefore.getResult().<Long>get(s);
					t = t + num;
					assertEquals(s, t, result.getResult().get(s));
				}
			}
			else if(temp.getType().equals("precisetime")){
				if(resultbefore.getResult() == null || resultbefore.getResult().get(s) == null) {
					long pt = num;
					assertEquals(s, pt, result.getResult().get(s));
				}
				else {
					long pt = resultbefore.getResult().<Long>get(s);
					pt = pt + num;
					assertEquals(s, pt, result.getResult().get(s));
				}
			}
		}
	}

	@Before
	public void setUp() throws Exception {
		obClient.clearActiveMemTale();
	}	

	@After
	public void tearDown() throws Exception {
		obClient.clearActiveMemTale();
	}
	
	
	@Test
	public void test_get_function19_DataExistUpdateMerge(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 19));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		String sforCollect = "";
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 19));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		String sforItem = "";
		
		for (int i = 0; i < 130; i++) {
			/* CollectInfo */
			sforCollect += "c";
			Value value = new Value();
			value.setString(sforCollect);
			umforCollect.addColumn(CollectInsertColumns[6], value, false);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getNonJoinColumnNames());
			assertEquals(CollectInsertColumns[6], sforCollect, rdforCollect1.getResult().get(CollectInsertColumns[6]));
			
			/* ItemInfo */
			sforItem += "i";
			value = new Value();
			value.setString(sforItem);
			umforItem.addColumn(ItemColumns[1], value, false);
			Result<Boolean> rsforItem = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem.getResult());
			Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ItemColumns[1], sforItem, rdforItem.getResult().get(ItemColumns[1]));
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[1], sforItem, rdforCollect2.getResult().get(CollectJoinColumns[1]));
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 19));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[1], sforItem, rdforCollect3.getResult().get(CollectJoinColumns[1]));
		}	
	}
	
	@Test
	public void test_get_function20_DataNotExistUpdateMerge(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'1', 20));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		String sforCollect = "";
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'1', 20));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		String sforItem = "";
		
		for (int i = 0; i < 130; i++) {
			/* CollectInfo */
			sforCollect += "c";
			Value value = new Value();
			value.setString(sforCollect);
			umforCollect.addColumn(CollectInsertColumns[6], value, false);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getNonJoinColumnNames());
			assertEquals(CollectInsertColumns[6], sforCollect, rdforCollect1.getResult().get(CollectInsertColumns[6]));
			
			/* ItemInfo */
			sforItem += "i";
			value = new Value();
			value.setString(sforItem);
			umforItem.addColumn(ItemColumns[4], value, false);
			Result<Boolean> rsforItem = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem.getResult());
			Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ItemColumns[4], sforItem, rdforItem.getResult().get(ItemColumns[4]));
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[4], sforItem, rdforCollect2.getResult().get(CollectJoinColumns[4]));
		}	
	}
	
	@Test
	public void test_get_function21_DataNotExistInsertMerge(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'1', 21));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		String sforCollect = "";
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'1', 21));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		String sforItem = "";
		
		for (int i = 0; i < 130; i++) {
			/* CollectInfo */
			sforCollect += "c";
			Value value = new Value();
			value.setString(sforCollect);
			imforCollect.addColumn(CollectInsertColumns[6], value);
			Result<Boolean> rsforCollect1 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getNonJoinColumnNames());
			assertEquals(CollectInsertColumns[6], sforCollect, rdforCollect1.getResult().get(CollectInsertColumns[6]));
			
			/* ItemInfo */
			sforItem += "i";
			value = new Value();
			value.setString(sforItem);
			imforItem.addColumn(ItemColumns[8], value);
			Result<Boolean> rsforItem = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem.getResult());
			Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ItemColumns[8], sforItem, rdforItem.getResult().get(ItemColumns[8]));
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[7], sforItem, rdforCollect2.getResult().get(CollectJoinColumns[7]));
		}	
	}
	
	@Test
	public void test_get_function22_DataExistInsertMerge(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 22));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		String sforCollect = "";
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 22));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		String sforItem = "";
		
		for (int i = 0; i < 130; i++) {
			/* CollectInfo */
			sforCollect += "c";
			Value value = new Value();
			value.setString(sforCollect);
			imforCollect.addColumn(CollectInsertColumns[6], value);
			Result<Boolean> rsforCollect1 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getNonJoinColumnNames());
			assertEquals(CollectInsertColumns[6], sforCollect, rdforCollect1.getResult().get(CollectInsertColumns[6]));
			
			/* ItemInfo */
			sforItem += "i";
			value = new Value();
			value.setString(sforItem);
			imforItem.addColumn(ItemColumns[1], value);
			Result<Boolean> rsforItem = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem.getResult());
			Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ItemColumns[1], sforItem, rdforItem.getResult().get(ItemColumns[1]));
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[1], sforItem, rdforCollect2.getResult().get(CollectJoinColumns[1]));
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 22));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[1], sforItem, rdforCollect3.getResult().get(CollectJoinColumns[1]));
		}	
	}
	
	@Test
	public void test_get_function23_DataNotExistDeleteMerge(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'1', 23));
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'1', 23));

		for (int i = 0; i < 130; i++) {
			/* CollectInfo */
			Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
			assertTrue("No records find!", rdforCollect1.getResult().isEmpty());

			
			/* ItemInfo */
			Result<Boolean> rsforItem = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem.getResult());
			Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rsforItem.getCode());
			assertTrue("No records find!", rdforItem.getResult().isEmpty());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect2.getCode());
			assertTrue("No records find!", rdforCollect2.getResult().isEmpty());
		}
	}
	
	@Test
	public void test_get_function24_DataExistDeleteMerge(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 24));
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 24));

		for (int i = 0; i < 130; i++) {
			/* CollectInfo */
			Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
			assertTrue("No records find!", rdforCollect1.getResult().isEmpty());

			
			/* ItemInfo */
			Result<Boolean> rsforItem = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem.getResult());
			Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem.getCode());
			assertTrue("No records find!", rdforItem.getResult().isEmpty());

			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect2.getCode());
			assertTrue("No records find!", rdforCollect2.getResult().isEmpty());

			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 24));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect3.getResult().get(str));
			}
		}
	}
	
	@Test
	public void test_get_function25_DataExistAddOneMerge(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 25));
		Result<RowData> rs1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getNonJoinColumnNames());
		long formerforCollect = rs1.getResult().<Long>get(CollectInsertColumns[0]);
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 25));
		Result<RowData> rs2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		long formerforItem = rs2.getResult().<Long>get(ItemColumns[0]);
		
		Value value=new Value();
		value.setNumber(1);
		for (int i = 0; i < 130; i++) {
			/* CollectInfo */
			UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
			umforCollect.addColumn(CollectInsertColumns[0], value, true);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getNonJoinColumnNames());
			formerforCollect = formerforCollect + 1;
			assertEquals(CollectInsertColumns[0], formerforCollect, rdforCollect1.getResult().get(CollectInsertColumns[0]));
			
			/* ItemInfo */
			UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
			umforItem.addColumn(ItemColumns[0], value, true);
			Result<Boolean> rsforItem = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem.getResult());
			Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			formerforItem = formerforItem + 1;
			assertEquals(ItemColumns[0], formerforItem, rdforItem.getResult().get(ItemColumns[0]));
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[0], formerforItem, rdforCollect2.getResult().get(CollectJoinColumns[0]));
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 25));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[0], formerforItem, rdforCollect3.getResult().get(CollectJoinColumns[0]));
		}
	}
	
	@Test
	public void test_get_function26_DataNotExistAddOneMerge() {
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'1', 26));
		long formerforCollect = 0;
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'1', 26));
		long formerforItem = 0;

		Value value=new Value();
		value.setSecond(1);
		for (int i = 0; i < 130; i++) {
			/* CollectInfo */
			UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
			umforCollect.addColumn(CollectInsertColumns[7], value, true);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getNonJoinColumnNames());
			formerforCollect = formerforCollect + 1;
			assertEquals(CollectInsertColumns[7], formerforCollect, rdforCollect1.getResult().get(CollectInsertColumns[7]));
			
			/* ItemInfo */
			UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
			umforItem.addColumn(ItemColumns[2], value, true);
			Result<Boolean> rsforItem = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem.getResult());
			Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			formerforItem = formerforItem + 1;
			assertEquals(ItemColumns[2], formerforItem, rdforItem.getResult().get(ItemColumns[2]));
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[2], formerforItem, rdforCollect2.getResult().get(CollectJoinColumns[2]));
		}
	}
	
	@Test
	public void test_get_function27_DataExistSubOneMerge(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 27));
		Result<RowData> rs1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getNonJoinColumnNames());
		long formerforCollect = rs1.getResult().<Long>get(CollectInsertColumns[2]);
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 27));
		Result<RowData> rs2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		long formerforItem = rs2.getResult().<Long>get(ItemColumns[7]);
		
		Value value=new Value();
		value.setNumber(-1);
		for (int i = 0; i < 130; i++) {
			/* CollectInfo */
			UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
			umforCollect.addColumn(CollectInsertColumns[2], value, true);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getNonJoinColumnNames());
			formerforCollect = formerforCollect - 1;
			assertEquals(CollectInsertColumns[2], formerforCollect, rdforCollect1.getResult().get(CollectInsertColumns[2]));
			
			/* ItemInfo */
			UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
			umforItem.addColumn(ItemColumns[7], value, true);
			Result<Boolean> rsforItem = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem.getResult());
			Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			formerforItem = formerforItem - 1;
			assertEquals(ItemColumns[7], formerforItem, rdforItem.getResult().get(ItemColumns[7]));
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[6], formerforItem, rdforCollect2.getResult().get(CollectJoinColumns[6]));
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 27));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[6], formerforItem, rdforCollect3.getResult().get(CollectJoinColumns[6]));
		}
	}
	
	@Test
	public void test_get_function28_DataNotExistSubOneMerge(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'1', 28));
		long formerforCollect = 0;
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'1', 28));
		long formerforItem = 0;
		
		Value value=new Value();
		value.setNumber(-1);
		for (int i = 0; i < 130; i++) {
			/* CollectInfo */
			UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
			umforCollect.addColumn(CollectInsertColumns[5], value, true);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getNonJoinColumnNames());
			formerforCollect = formerforCollect - 1;
			assertEquals(CollectInsertColumns[5], formerforCollect, rdforCollect1.getResult().get(CollectInsertColumns[5]));
			
			/* ItemInfo */
			UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
			umforItem.addColumn(ItemColumns[9], value, true);
			Result<Boolean> rsforItem = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem.getResult());
			Result<RowData> rdforItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			formerforItem = formerforItem - 1;
			assertEquals(ItemColumns[9], formerforItem, rdforItem.getResult().get(ItemColumns[9]));
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(CollectJoinColumns[8], formerforItem, rdforCollect2.getResult().get(CollectJoinColumns[8]));
		}
	}
	
	@Test
	public void test_get_function29_DataExistDeleteInsert(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 29));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 29));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {					
			/* CollectInfo */
			Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
			assertTrue("No records find!", rdforCollect1.getResult().isEmpty());

			
			imforCollect = getNormalIMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect2 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect2, rowkeyForCollect, i);
			
			/* ItemInfo */
			Result<Boolean> rsforItem1 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem1.getCode());
			assertTrue("No records find!", rdforItem1.getResult().isEmpty());

			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 29));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect3.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect3.getResult().get(str));
			}
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect5, rowkeyForCollect, i);
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect5.getResult().get(str));
			}
			
			imforItem = getNormalIMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem2 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem2, rowkeyForItem, i, true);
			RKey rowkeyForCollect3 = new RKey(new CollectInfoKeyRule(8, (byte)'0', 29));
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect6, rowkeyForCollect3, i, false);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect7, rowkeyForCollect2, i, false);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect4, rowkeyForCollect, i, false);
		}
	}
	
	@Test
	public void test_get_function30_DataExistDeleteUpdate(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 30));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 30));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {
			/* CollectInfo */
			Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
			assertTrue("No records find!", rdforCollect1.getResult().isEmpty());
			
			umforCollect = getNormalUMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect2, rowkeyForCollect, i);
			
			/* ItemInfo */
			Result<Boolean> rsforItem1 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem1.getCode());
			assertTrue("No records find!", rdforItem1.getResult().isEmpty());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 30));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect3.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect3.getResult().get(str));
			}
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect5, rowkeyForCollect, i);
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect5.getResult().get(str));
			}
			
			umforItem = getNormalUMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem2, rowkeyForItem, i, true);
			RKey rowkeyForCollect3 = new RKey(new CollectInfoKeyRule(8, (byte)'0', 30));
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect6, rowkeyForCollect3, i, false);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect7, rowkeyForCollect2, i, false);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect4, rowkeyForCollect, i, false);
		}
	}
	
	@Test
	public void test_get_function31_DataExistDeleteAddOne(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 31));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 31));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {
			/* CollectInfo */
			Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
			assertTrue("No records find!", rdforCollect1.getResult().isEmpty());
			
			umforCollect = getASUMforCollect(rowkeyForCollect, 1);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, 1);
			
			/* ItemInfo */
			Result<Boolean> rsforItem1 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem1.getCode());
			assertTrue("No records find!", rdforItem1.getResult().isEmpty());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 31));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect3.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect3.getResult().get(str));
			}
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect5, 1);
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect5.getResult().get(str));
			}
			
			umforItem = getASUMforItem(rowkeyForItem, 1);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, 1, true);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect7, 1, false);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect4, 1, false);
		}
	}
	
	@Test
	public void test_get_function32_DataExistDeleteSubOne(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 32));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 32));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {
			/* CollectInfo */
			Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
			assertTrue("No records find!", rdforCollect1.getResult().isEmpty());
			
			umforCollect = getASUMforCollect(rowkeyForCollect, -1);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, -1);
			
			/* ItemInfo */
			Result<Boolean> rsforItem1 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem1.getCode());
			assertTrue("No records find!", rdforItem1.getResult().isEmpty());
			
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 32));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect3.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect3.getResult().get(str));
			}
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect5, -1);
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect5.getResult().get(str));
			}
			
			umforItem = getASUMforItem(rowkeyForItem, -1);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, -1, true);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect7, -1, false);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect4, -1, false);
		}
	}
	
	@Test
	public void test_get_function33_DataExistDID(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 33));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 33));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 44; i++) {							
			/* CollectInfo */
			Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
			assertTrue("No records find!", rdforCollect1.getResult().isEmpty());
			
			imforCollect = getNormalIMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect2 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect2, rowkeyForCollect, i);

			Result<Boolean> rsforCollect3 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect3.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect3.getCode());
			assertTrue("No records find!", rdforCollect3.getResult().isEmpty());
			
			/* ItemInfo */
			Result<Boolean> rsforItem1 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem1.getCode());
			assertTrue("No records find!", rdforItem1.getResult().isEmpty());
			
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 33));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect4.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect4.getResult().get(str));
			}
			
			imforItem = getNormalIMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem2 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem2, rowkeyForItem, i, true);
			RKey rowkeyForCollect3 = new RKey(new CollectInfoKeyRule(8, (byte)'0', 33));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect5, rowkeyForCollect3, i, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect6, rowkeyForCollect2, i, false);

			Result<Boolean> rsforItem3 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem3.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem3.getCode());
			assertTrue("No records find!", rdforItem3.getResult().isEmpty());
			
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect7.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect7.getResult().get(str));
			}
		}

	}
	
	@Test
	public void test_get_function34_DataExistDUD(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 34));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 34));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 44; i++) {
			Result<Boolean> rsforCollect1 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect1.getCode());
			assertTrue("No records find!", rdforCollect1.getResult().isEmpty());
			
			umforCollect = getNormalUMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect2, rowkeyForCollect, i);

			Result<Boolean> rsforCollect3 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect3.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect3.getCode());
			assertTrue("No records find!", rdforCollect3.getResult().isEmpty());
			
			
			/* ItemInfo */
			Result<Boolean> rsforItem1 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem1.getCode());
			assertTrue("No records find!", rdforItem1.getResult().isEmpty());
			
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 34));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect4.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect4.getResult().get(str));
			}
			
			umforItem = getNormalUMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem2, rowkeyForItem, i, true);
			RKey rowkeyForCollect3 = new RKey(new CollectInfoKeyRule(8, (byte)'0', 34));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect5, rowkeyForCollect3, i, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect6, rowkeyForCollect2, i, false);

			Result<Boolean> rsforItem3 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem3.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem3.getCode());
			assertTrue("No records find!", rdforItem3.getResult().isEmpty());
			
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect7.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect7.getResult().get(str));
			}
		}
	}
	
	@Test
	public void test_get_function35_DataExistMultiInsert(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 35));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		int timesforCollect = 0;
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 35));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		int timesforItem = 0;
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			imforCollect = getNormalIMforCollect(rowkeyForCollect, timesforCollect);
			Result<Boolean> rsforCollect1 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect1, rowkeyForCollect, timesforCollect);
			timesforCollect++;
			
			imforCollect = getNormalIMforCollect(rowkeyForCollect, timesforCollect);
			Result<Boolean> rsforCollect2 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect2, rowkeyForCollect, timesforCollect);
			timesforCollect++;
			
			/* ItemInfo */			
			imforItem = getNormalIMforItem(rowkeyForItem, timesforItem);
			Result<Boolean> rsforItem1 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem1, rowkeyForItem, timesforItem, true);
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect3, rowkeyForCollect, timesforItem, false);
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 35));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect4, rowkeyForCollect2, timesforItem, false);
			timesforItem++;

			imforItem = getNormalIMforItem(rowkeyForItem, timesforItem);
			Result<Boolean> rsforItem2 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem2, rowkeyForItem, timesforItem, true);
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect5, rowkeyForCollect, timesforItem, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect6, rowkeyForCollect2, timesforItem, false);
			timesforItem++;
		}
	}
	
	@Test
	public void test_get_function36_DataExistInsertUpdate(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 36));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 36));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			imforCollect = getNormalIMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect1 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect1, rowkeyForCollect, i);
			
			umforCollect = getNormalUMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect2, rowkeyForCollect, i);
			
			/* ItemInfo */			
			imforItem = getNormalIMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem1 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem1, rowkeyForItem, i, true);
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect3, rowkeyForCollect, i, false);
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 36));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect4, rowkeyForCollect2, i, false);

			umforItem = getNormalUMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem2, rowkeyForItem, i, true);
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect5, rowkeyForCollect, i, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect6, rowkeyForCollect2, i, false);
		}
	}
	
	@Test
	public void test_get_function37_DataExistInsertDelete(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 37));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 37));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {
			/* CollectInfo */
			imforCollect = getNormalIMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect1 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect1, rowkeyForCollect, i);
			
			Result<Boolean> rsforCollect2 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect2.getCode());
			assertTrue("No records find!", rdforCollect2.getResult().isEmpty());			
			
			/* ItemInfo */
			imforItem = getNormalIMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem1 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem1, rowkeyForItem, i, true);
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 37));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect3, rowkeyForCollect2, i, false);
			RKey rowkeyForCollect3 = new RKey(new CollectInfoKeyRule(8, (byte)'0', 37));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect4, rowkeyForCollect3, i, false);
			// can remove
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect8.getCode());
			assertTrue("No records find!", rdforCollect8.getResult().isEmpty());
						
			Result<Boolean> rsforItem2 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem2.getCode());
			assertTrue("No records find!", rdforCollect8.getResult().isEmpty());
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect5.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect5.getResult().get(str));
			}
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect3), rdforCollect6.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect6.getResult().get(str));
			}
			// can remove
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect7.getCode());
			assertTrue("No records find!", rdforCollect7.getResult().isEmpty());
		}
	}
	
	@Test
	public void test_get_function38_DataExistInsertAddOne(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 38));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 38));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			imforCollect = getNormalIMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect1 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect1, rowkeyForCollect, i);
			
			umforCollect = getASUMforCollect(rowkeyForCollect, 1);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, 1);
			
			/* ItemInfo */			
			imforItem = getNormalIMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem1 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem1, rowkeyForItem, i, true);
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect3, rowkeyForCollect, i, false);
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 38));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect4, rowkeyForCollect2, i, false);
			
			umforItem = getASUMforItem(rowkeyForItem, 1);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, 1, true);
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect5, 1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect4, rdforCollect6, 1, false);			
		}
	}
	
	@Test
	public void test_get_function39_DataExistInsertSubOne(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 39));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 39));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			imforCollect = getNormalIMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect1 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect1, rowkeyForCollect, i);
			
			umforCollect = getASUMforCollect(rowkeyForCollect, -1);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, -1);
			
			/* ItemInfo */			
			imforItem = getNormalIMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem1 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem1, rowkeyForItem, i, true);
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect3, rowkeyForCollect, i, false);
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 39));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect4, rowkeyForCollect2, i, false);
			
			umforItem = getASUMforItem(rowkeyForItem, -1);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, -1, true);
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect5, -1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect4, rdforCollect6, -1, false);			
		}
	}
	
	@Test
	public void test_get_function40_DataExistMultiUpdate(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 40));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		int timesforCollect = 0;
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 40));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		int timesforItem = 0;
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */			
			umforCollect = getNormalUMforCollect(rowkeyForCollect, timesforCollect);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect1, rowkeyForCollect, timesforCollect);
			timesforCollect++;
			umforCollect = getNormalUMforCollect(rowkeyForCollect, timesforCollect);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect2, rowkeyForCollect, timesforCollect);
			timesforCollect++;
			
			/* ItemInfo */			
			umforItem = getNormalUMforItem(rowkeyForItem, timesforItem);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem1, rowkeyForItem, timesforItem, true);
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect3, rowkeyForCollect, timesforItem, false);
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 40));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect4, rowkeyForCollect2, timesforItem, false);
			timesforItem++;
			umforItem = getNormalUMforItem(rowkeyForItem, timesforItem);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem2, rowkeyForItem, timesforItem, true);
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect5, rowkeyForCollect, timesforItem, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect6, rowkeyForCollect2, timesforItem, false);
			timesforItem++;
		}
	}
	
	@Test
	public void test_get_function41_DataExistUpdateInsert(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 41));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 41));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			umforCollect = getNormalUMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect1, rowkeyForCollect, i);
			
			imforCollect = getNormalIMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect2 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect2, rowkeyForCollect, i);			
			
			/* ItemInfo */			
			umforItem = getNormalUMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem1, rowkeyForItem, i, true);
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect3, rowkeyForCollect, i, false);
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 41));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect4, rowkeyForCollect2, i, false);
			
			imforItem = getNormalIMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem2 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem2, rowkeyForItem, i, true);
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect5, rowkeyForCollect, i, false);			
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect6, rowkeyForCollect2, i, false);			
		}
	}
	
	@Test
	public void test_get_function42_DataExistUpdateDelete(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 42));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 42));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {
			/* CollectInfo */
			umforCollect = getNormalUMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect1, rowkeyForCollect, i);
			
			Result<Boolean> rsforCollect2 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect2.getCode());
			assertTrue("No records find!", rdforCollect2.getResult().isEmpty());			
			
			/* ItemInfo */
			umforItem = getNormalUMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem1, rowkeyForItem, i, true);
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 42));
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect3, rowkeyForCollect2, i, false);
			RKey rowkeyForCollect3 = new RKey(new CollectInfoKeyRule(8, (byte)'0', 42));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect4, rowkeyForCollect3, i, false);
			// can remove
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect8.getCode());
			assertTrue("No records find!", rdforCollect8.getResult().isEmpty());
			
			Result<Boolean> rsforItem2 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem2.getCode());
			assertTrue("No records find!", rdforItem2.getResult().isEmpty());
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect5.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect5.getResult().get(str));
			}
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect3), rdforCollect6.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect6.getResult().get(str));
			}
			// can remove
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect7.getCode());
			assertTrue("No records find!", rdforCollect7.getResult().isEmpty());		
		}
	}
	
	@Test
	public void test_get_function43_DataExistUpdateAddOne(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 43));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 43));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			umforCollect = getNormalUMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect1, rowkeyForCollect, i);
			
			umforCollect = getASUMforCollect(rowkeyForCollect, 1);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, 1);
			
			/* ItemInfo */			
			umforItem = getNormalUMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem1, rowkeyForItem, i, true);
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect3, rowkeyForCollect, i, false);
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 43));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect4, rowkeyForCollect2, i, false);
			
			umforItem = getASUMforItem(rowkeyForItem, 1);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, 1, true);
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect5, 1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect4, rdforCollect6, 1, false);			
		}
	}
	
	@Test
	public void test_get_function44_DataExistUpdateSubOne(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 44));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 44));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			umforCollect = getNormalUMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect1, rowkeyForCollect, i);
			
			umforCollect = getASUMforCollect(rowkeyForCollect, -1);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, -1);
			
			/* ItemInfo */			
			umforItem = getNormalUMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem1, rowkeyForItem, i, true);
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect3, rowkeyForCollect, i, false);
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 44));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect4, rowkeyForCollect2, i, false);
			
			umforItem = getASUMforItem(rowkeyForItem, -1);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, -1, true);
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect5, -1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect4, rdforCollect6, -1, false);			
		}
	}
	
	@Test
	public void test_get_function45_DataExistMultiAddOne(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 45));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 45));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			umforCollect = getASUMforCollect(rowkeyForCollect, 1);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, 1);
			
			umforCollect = getASUMforCollect(rowkeyForCollect, 1);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect2, rdforCollect3, 1);
			
			/* ItemInfo */			
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 45));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			umforItem = getASUMforItem(rowkeyForItem, 1);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, 1, true);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect4, 1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect6, 1, false);			
			
			umforItem = getASUMforItem(rowkeyForItem, 1);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem2, rdforItem3, 1, true);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect4, rdforCollect7, 1, false);
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect6, rdforCollect8, 1, false);			
		}
	}
	
	@Test
	public void test_get_function46_DataExistAddOneUpdate(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 46));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 46));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			umforCollect = getASUMforCollect(rowkeyForCollect, 1);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, 1);
			
			umforCollect = getNormalUMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect3, rowkeyForCollect, i);		
			
			/* ItemInfo */			
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 46));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			umforItem = getASUMforItem(rowkeyForItem, 1);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, 1, true);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect4, 1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect6, 1, false);		
			
			umforItem = getNormalUMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem3, rowkeyForItem, i, true);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect7, rowkeyForCollect, i, false);
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect8, rowkeyForCollect2, i, false);				
		}
	}
	
	
	@Test
	public void test_get_function47_DataExistAddOneInsert(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 47));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 47));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			umforCollect = getASUMforCollect(rowkeyForCollect, 1);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, 1);
			
			imforCollect = getNormalIMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect2 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect3, rowkeyForCollect, i);			
			
			/* ItemInfo */			
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 47));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			umforItem = getASUMforItem(rowkeyForItem, 1);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, 1, true);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect4, 1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect6, 1, false);			
			
			imforItem = getNormalIMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem2 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem2.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem3, rowkeyForItem, i, true);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect7, rowkeyForCollect, i, false);
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect8, rowkeyForCollect2, i, false);			
		}
	}
	
	@Test
	public void test_get_function49_DataExistAddOneDelete(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 49));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 49));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {
			/* CollectInfo */
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			umforCollect = getASUMforCollect(rowkeyForCollect, 1);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, 1);
			
			Result<Boolean> rsforCollect2 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect3.getCode());
			assertTrue("No records find!", rdforCollect3.getResult().isEmpty());
			
			/* ItemInfo */
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 49));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			RKey rowkeyForCollect3 = new RKey(new CollectInfoKeyRule(8, (byte)'0', 49));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getJoinColumnNames());
			umforItem = getASUMforItem(rowkeyForItem, 1);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, 1, true);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect4, rdforCollect6, 1, false);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect7, 1, false);
			// can remove
			Result<RowData> rdforCollect10 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect10.getCode());
			assertTrue("No records find!", rdforCollect10.getResult().isEmpty());
						
			Result<Boolean> rsforItem2 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem2.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem3.getCode());
			assertTrue("No records find!", rdforItem3.getResult().isEmpty());
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect8.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect8.getResult().get(str));
			}
			Result<RowData> rdforCollect9 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect3), rdforCollect9.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect9.getResult().get(str));
			}
			// can remove
			Result<RowData> rdforCollect11 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect11.getCode());
			assertTrue("No records find!", rdforCollect11.getResult().isEmpty());		
		}
	}
	
	@Test
	public void test_get_function50_DataExistAddSubOne(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 50));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 50));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			umforCollect = getASUMforCollect(rowkeyForCollect, 1);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, 1);
			
			umforCollect = getASUMforCollect(rowkeyForCollect, -1);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect2, rdforCollect3, -1);
			
			/* ItemInfo */			
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 50));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			umforItem = getASUMforItem(rowkeyForItem, 1);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, 1, true);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect4, 1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect6, 1, false);			
			
			umforItem = getASUMforItem(rowkeyForItem, -1);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem2, rdforItem3, -1, true);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect4, rdforCollect7, -1, false);
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect6, rdforCollect8, -1, false);			
		}
	}
	
	@Test
	public void test_get_function51_DataExistMultiSubOne(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 51));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 51));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			umforCollect = getASUMforCollect(rowkeyForCollect, -1);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, -1);
			
			umforCollect = getASUMforCollect(rowkeyForCollect, -1);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect2, rdforCollect3, -1);
			
			/* ItemInfo */			
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 51));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			umforItem = getASUMforItem(rowkeyForItem, -1);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, -1, true);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect4, -1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect6, -1, false);			
			
			umforItem = getASUMforItem(rowkeyForItem, -1);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem2, rdforItem3, -1, true);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect4, rdforCollect7, -1, false);
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect6, rdforCollect8, -1, false);			
		}
	}
	
	@Test
	public void test_get_function52_DataExistSubOneInsert(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 52));
		InsertMutator imforCollect = new InsertMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 52));
		InsertMutator imforItem = new InsertMutator(itemInfoTable.getTableName(), rowkeyForItem);
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			umforCollect = getASUMforCollect(rowkeyForCollect, -1);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, -1);
			
			imforCollect = getNormalIMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect2 = obClient.insert(imforCollect);
			assertTrue("Insert fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			insertCheckforCollect(rdforCollect3, rowkeyForCollect, i);			
			
			/* ItemInfo */			
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 52));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			umforItem = getASUMforItem(rowkeyForItem, -1);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, -1, true);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect4, -1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect6, -1, false);			
			
			imforItem = getNormalIMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem2 = obClient.insert(imforItem);
			assertTrue("Insert fail!", rsforItem2.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			insertCheckforItem(rdforItem3, rowkeyForItem, i, true);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect7, rowkeyForCollect, i, false);
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			insertCheckforItem(rdforCollect8, rowkeyForCollect2, i, false);			
		}
	}
	
	@Test
	public void test_get_function53_DataExistSubOneUpdate(){
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 53));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 53));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			umforCollect = getASUMforCollect(rowkeyForCollect, -1);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, -1);
			
			umforCollect = getNormalUMforCollect(rowkeyForCollect, i);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			updateCheckforCollect(rdforCollect3, rowkeyForCollect, i);		
			
			/* ItemInfo */			
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 53));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			umforItem = getASUMforItem(rowkeyForItem, -1);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, -1, true);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect4, -1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect6, -1, false);		
			
			umforItem = getNormalUMforItem(rowkeyForItem, i);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			updateCheckforItem(rdforItem3, rowkeyForItem, i, true);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect7, rowkeyForCollect, i, false);
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			updateCheckforItem(rdforCollect8, rowkeyForCollect2, i, false);				
		}
	}
	
	@Test
	public void test_get_function54_DataExistSubOneDelete(){
		
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 54));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 54));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {
			/* CollectInfo */
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			umforCollect = getASUMforCollect(rowkeyForCollect, -1);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, -1);
			
			Result<Boolean> rsforCollect2 = obClient.delete(collectInfoTable.getTableName(), rowkeyForCollect);
			assertTrue("Delete fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect3.getCode());
			assertTrue("No records find!", rdforCollect3.getResult().isEmpty());			
			/* ItemInfo */
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 54));
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			RKey rowkeyForCollect3 = new RKey(new CollectInfoKeyRule(8, (byte)'0', 54));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getJoinColumnNames());
			umforItem = getASUMforItem(rowkeyForItem, -1);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, -1, true);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect4, rdforCollect6, -1, false);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect7, -1, false);
			// can remove
			Result<RowData> rdforCollect10 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect10.getCode());
			assertTrue("No records find!", rdforCollect10.getResult().isEmpty());
						
			Result<Boolean> rsforItem2 = obClient.delete(itemInfoTable.getTableName(), rowkeyForItem);
			assertTrue("Delete fail!", rsforItem2.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforItem3.getCode());
			assertTrue("No records find!", rdforItem3.getResult().isEmpty());
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect2), rdforCollect8.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect8.getResult().get(str));
			}
			Result<RowData> rdforCollect9 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect3, collectInfoTable.getAllColumnNames());
			for(String str:collectInfoTable.getNonJoinColumnNames()){
				assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect3), rdforCollect9.getResult().get(str));
			}
			for(String str:collectInfoTable.getJoinColumnNames()){
				assertEquals(str, null, rdforCollect9.getResult().get(str));
			}
			// can remove
			Result<RowData> rdforCollect11 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			assertEquals(ResultCode.OB_SUCCESS, rdforCollect11.getCode());
			assertTrue("No records find!", rdforCollect11.getResult().isEmpty());	
		}
	}
	
	@Test
	public void test_get_function55_DataExistSubAddOne(){
		
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(6, (byte)'0', 55));
		UpdateMutator umforCollect = new UpdateMutator(collectInfoTable.getTableName(), rowkeyForCollect);
		Result<RowData> resultForCollect = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
		for(String str:collectInfoTable.getAllColumnNames()){
			assertEquals(str, collectInfoTable.getColumnValue(str, rowkeyForCollect), resultForCollect.getResult().get(str));
		}
		
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 55));
		UpdateMutator umforItem = new UpdateMutator(itemInfoTable.getTableName(), rowkeyForItem);
		Result<RowData> resultForItem = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
		for(String str:itemInfoTable.getAllColumnNames()){
			assertEquals(str, itemInfoTable.getColumnValue(str, rowkeyForItem), resultForItem.getResult().get(str));
		}
		
		for (int i = 0; i < 65; i++) {							
			/* CollectInfo */
			Result<RowData> rdforCollect1 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			umforCollect = getASUMforCollect(rowkeyForCollect, -1);
			Result<Boolean> rsforCollect1 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect1.getResult());
			Result<RowData> rdforCollect2 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect1, rdforCollect2, -1);
			
			umforCollect = getASUMforCollect(rowkeyForCollect, 1);
			Result<Boolean> rsforCollect2 = obClient.update(umforCollect);
			assertTrue("Update fail!", rsforCollect2.getResult());
			Result<RowData> rdforCollect3 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getAllColumnNames());
			addsubCheckforCollect(rdforCollect2, rdforCollect3, 1);
			
			/* ItemInfo */			
			Result<RowData> rdforItem1 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(7, (byte)'0', 55));
			Result<RowData> rdforCollect5 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			umforItem = getASUMforItem(rowkeyForItem, -1);
			Result<Boolean> rsforItem1 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem1.getResult());
			Result<RowData> rdforItem2 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem1, rdforItem2, -1, true);
			Result<RowData> rdforCollect4 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect3, rdforCollect4, -1, false);
			Result<RowData> rdforCollect6 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect5, rdforCollect6, -1, false);			
			
			umforItem = getASUMforItem(rowkeyForItem, 1);
			Result<Boolean> rsforItem2 = obClient.update(umforItem);
			assertTrue("Update fail!", rsforItem2.getResult());
			Result<RowData> rdforItem3 = obClient.get(itemInfoTable.getTableName(), rowkeyForItem, itemInfoTable.getAllColumnNames());
			addsubCheckforItem(rdforItem2, rdforItem3, 1, true);
			Result<RowData> rdforCollect7 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect4, rdforCollect7, 1, false);
			Result<RowData> rdforCollect8 = obClient.get(collectInfoTable.getTableName(), rowkeyForCollect2, collectInfoTable.getJoinColumnNames());
			addsubCheckforItem(rdforCollect6, rdforCollect8, 1, false);			
		}		
	}
	
	
}
