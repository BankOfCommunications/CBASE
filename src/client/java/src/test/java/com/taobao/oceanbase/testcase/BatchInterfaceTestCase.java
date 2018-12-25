package com.taobao.oceanbase.testcase;
//import static org.junit.Assert.*;

import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
import java.util.List;
//import java.util.Map;
import java.util.Set;
//import java.lang.Math;
import junit.framework.Assert;

//import org.apache.log.output.db.ColumnInfo;
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
//import com.taobao.oceanbase.base.rule.CollectInfoTimeColumnRule;
//import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
//import com.taobao.oceanbase.base.table.ITable;
//import com.taobao.oceanbase.base.table.Table;
//import com.taobao.oceanbase.cli.OBClient;
import com.taobao.oceanbase.vo.DeleteMutator;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.ObMutatorBase;
//import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.UpdateMutator;
//import com.taobao.oceanbase.vo.Value;
public class BatchInterfaceTestCase extends BaseCase{
	
	@Before
	public void before(){
		obClient.clearActiveMemTale();
		
	}
	@After
	public void after(){
		obClient.clearActiveMemTale();}
	@Test
	public void transaction_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
	
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
		listmutator.add(im);
		listmutator.add(um);
		Result<Boolean> rs=obClient.update(listmutator);
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 1, false).getObject(false).getValue(),
					rd.getResult().get(str));		
			}
	}
	@Test
	public void transaction_1_01_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames)
		{
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(im);
		Result<Boolean> rs=obClient.update(listmutator);
		Assert.assertTrue("transaction success!",rs.getResult());
	}
	@Test
	public void transaction_1_02_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		InsertMutator im1=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		InsertMutator im2=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im2.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		List<InsertMutator>listmutator=new ArrayList<InsertMutator>();
		
		listmutator.add(im1);
		listmutator.add(im2);
		Result<Boolean> rs=obClient.insert(listmutator);
		Assert.assertTrue("transaction fail!",rs.getResult());
//		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
//		for (String str : colNames) {
//			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(),
//					rd.getResult().get(str));
//		}
	}
	@Test
	public void transaction_1_03_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
	
		InsertMutator im1=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		for (String str : colNames) {
			im1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(im1);
		Result<Boolean> rs=obClient.update(listmutator);//去做 删/插入/更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
//		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
//		for (String str : colNames) {
//			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(),
//					rd.getResult().get(str));//验证
//		}
	}
	@Test
	public void transaction_1_04_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		InsertMutator im1=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		InsertMutator im2=new InsertMutator(tabName, rkey);for (String str : colNames) {
			im2.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		
		listmutator.add(im1);
		listmutator.add(im2);
		
		Result<Boolean> rs=obClient.update(listmutator);//去做 删/插入/更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
//		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
//		for (String str : colNames) {
//			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(),
//					rd.getResult().get(str));//验证
//		}
	}
	@Test
	public void transaction_1_05_01_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm);
		Result<Boolean> rs=obClient.update(listmutator);//去做 删/插入/更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
//		System.out.println("rs.getResult() is " + rs.getResult());
	//	Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		
		//System.out.println("rd.getResult() is " + rd.getResult());
/*
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(),
					rd.getResult().get(str));
		}
		*/
	}
	@Test
	public void transaction_1_05_02_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		DeleteMutator dm1=new DeleteMutator(tabName, rkey);
		DeleteMutator dm2=new DeleteMutator(tabName, rkey);
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm1);
		listmutator.add(dm2);
		Result<Boolean> rs=obClient.update(listmutator);//去做 删/插入/更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		//Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		//System.out.println("rd.getResult() is " + rd.getResult());
//		for (String str : colNames) {
//			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(),
//					rd.getResult().get(str));//验证
//		}
	}
	@Test
	public void transaction_1_06_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		DeleteMutator dm1=new DeleteMutator(tabName, rkey);
		DeleteMutator dm2=new DeleteMutator(tabName, rkey);
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm1);
		listmutator.add(dm2);
		Result<Boolean> rs=obClient.update(listmutator);//去做 删/插入/更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		//Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		//System.out.println("rd.getResult() is " + rd.getResult());
//		for (String str : colNames) {
//			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 1, false).getObject(false).getValue(),
//					rd.getResult().get(str));//验证
//		}
	}
	@Test
	public void transaction_1_07_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		DeleteMutator dm1=new DeleteMutator(tabName, rkey);
		DeleteMutator dm2=new DeleteMutator(tabName, rkey);
		DeleteMutator dm3=new DeleteMutator(tabName, rkey);
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm1);
		listmutator.add(dm2);
		listmutator.add(dm3);
		Result<Boolean> rs=obClient.update(listmutator);//去做 删/插入/更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		//Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		//System.out.println("rd.getResult() is " + rd.getResult());
//		for (String str : colNames) {
//			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(),
//					rd.getResult().get(str));//验证
//		}
	}
	@Test             //???????
	public void transaction_1_08_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(um);

		Result<Boolean> rs=obClient.update(listmutator);//去做 删/插入/更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str1 : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str1, rkey, 1, false).getObject(false).getValue(),
					rd.getResult().get(str1));//验证
		}	}
	@Test
	public void transaction_1_9_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		UpdateMutator um1=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		for (String str : colNames) {
			um1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(um1);
		Result<Boolean> rs=obClient.update(listmutator);//去做 删/插入/更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str1 : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str1, rkey, 1, false).getObject(false).getValue(),
					rd.getResult().get(str1));//验证
		}	
	}
	@Test
	public void transaction_1_10_test(){
		RKey rkey1 = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		UpdateMutator um1=new UpdateMutator(tabName,rkey1);
		for (String str : colNames) {
			um1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey1, 1, false), false);
		}
		RKey rkey2 = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
	   UpdateMutator um2=new UpdateMutator(tabName,rkey2);
		for (String str : colNames) {
			um2.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey2, 1, false), false);
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(um1);
		listmutator.add(um2);
		
		Result<Boolean> rs=obClient.update(listmutator);//去做 删/插入/更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd1=obClient.get(tabName, rkey1, colNames);
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey1, 1, false).getObject(false).getValue(),
					rd1.getResult().get(str));//验证
		}
		Result<RowData>rd2=obClient.get(tabName, rkey2, colNames);
		System.out.println("rd.getResult() is " + rd2.getResult());
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey2, 1, false).getObject(false).getValue(),
					rd2.getResult().get(str));//验证
		}
	}
	@Test
	public void transaction_2_01_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		for (String str : colNames) {
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm);
		listmutator.add(im);
		
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(),
					rd.getResult().get(str));//验证
		}
	}
	@Test
	public void transaction_2_02_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm);
		listmutator.add(im);
		
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		}
	@Test
	public void transaction_2_03_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		InsertMutator im1=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
		InsertMutator im2=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im2.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		UpdateMutator um1=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		UpdateMutator um2=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um2.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(im1);
		listmutator.add(im2);
		listmutator.add(um1);
		listmutator.add(um2);
		
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 1, false).getObject(false).getValue(),
					rd.getResult().get(str));//验证
		}
	}
	@Test
	public void transaction_2_04_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		InsertMutator im1=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		for (String str : colNames) {
			im1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
		InsertMutator im2=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im2.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		UpdateMutator um1=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		UpdateMutator um2=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um2.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(im1);
		listmutator.add(im2);
		listmutator.add(um1);
		listmutator.add(um2);
		
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 1, false).getObject(false).getValue(),
					rd.getResult().get(str));//验证
		}
	}	
	@Test
	public void transaction_2_05_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		for (String str : colNames) {
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(im);
		listmutator.add(dm);
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
}
	@Test
	public void transaction_2_06_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm);
		listmutator.add(im);
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
	}
	@Test
	public void transaction_2_07_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm);
		listmutator.add(um);
	
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 1, false).getObject(false).getValue(),
					rd.getResult().get(str));//验证
		}
	}
	@Test
	public void transaction_2_08_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(um);
		listmutator.add(dm);
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());}
	@Test
	public void transaction_2_09_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(um);
		listmutator.add(im);
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
//		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
//		for (String str : colNames) 
//		{
//			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(),
//					rd.getResult().get(str));//验证
//		}
	}
	@Test
	public void transaction_3_01_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
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
		listmutator.add(im);
		listmutator.add(um);
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 1, false).getObject(false).getValue(),
					rd.getResult().get(str));//验证
		}
	}
	@Test
	public void transaction_3_02_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) 
		{
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm);
		listmutator.add(um);
		listmutator.add(im);
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
//		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
//		for (String str : colNames) {
//			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(),
//					rd.getResult().get(str));//验证
	}
	@Test
	public void transaction_3_03_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) 
		{
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(im);
		listmutator.add(dm);
		listmutator.add(um);
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 1, false).getObject(false).getValue(),
					rd.getResult().get(str));//验证
		}}
@Test
	public void transaction_3_04_test(){
		RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
		String tabName=collectInfoTable.getTableName();
		Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
		colNames.remove("gm_create");
		colNames.remove("gm_modified");
		DeleteMutator dm=new DeleteMutator(tabName, rkey);
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
		}
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) 
		{
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
		listmutator.add(dm);
		listmutator.add(um);
		listmutator.add(im);
		Result<Boolean> rs=obClient.update(listmutator);//更新操作
		Assert.assertTrue("transaction fail!",rs.getResult());
//		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
//		for (String str : colNames) {
//			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(),
//					rd.getResult().get(str));//验证
		}
@Test
public void transaction_3_05_test(){
	RKey rkey = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
	String tabName=collectInfoTable.getTableName();
	Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
	colNames.remove("gm_create");
	colNames.remove("gm_modified");
	InsertMutator im=new InsertMutator(tabName, rkey);
	for (String str : colNames) 
	{
		im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
	}
	DeleteMutator dm=new DeleteMutator(tabName, rkey);
	UpdateMutator um=new UpdateMutator(tabName,rkey);
	for (String str : colNames) {
		um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
	}
	for (String str : colNames) {
		um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 1, false), false);
	}
	List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
	listmutator.add(im);
	listmutator.add(dm);
	listmutator.add(um);
	Result<Boolean> rs=obClient.update(listmutator);//更新操作
	Assert.assertTrue("transaction fail!",rs.getResult());
	Result<RowData>rd=obClient.get(tabName, rkey, colNames);
	for (String str : colNames) {
		Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, 1, false).getObject(false).getValue(),
				rd.getResult().get(str));//验证
	}}
@Test
public void transaction_3_06_test(){
	RKey rkey1 = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
	String tabName=collectInfoTable.getTableName();
	Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
	colNames.remove("gm_create");
	colNames.remove("gm_modified");
	DeleteMutator dm=new DeleteMutator(tabName, rkey1);
	RKey rkey2 = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
	InsertMutator im=new InsertMutator(tabName, rkey2);
	for (String str : colNames) 
	{
		im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey2, 0, false));
	}
	tabName=collectInfoTable.getTableName();
	UpdateMutator um1=new UpdateMutator(tabName,rkey1);
	for (String str : colNames) {
		um1.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey1, 1, false), false);
	}
	UpdateMutator um2=new UpdateMutator(tabName,rkey2);
	for (String str : colNames) {
		um2.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey2, 1, false), false);
	}
	List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
	listmutator.add(im);
	listmutator.add(dm);
	listmutator.add(um1);
	listmutator.add(um2);
	Result<Boolean> rs=obClient.update(listmutator);//更新操作
	Assert.assertTrue("transaction fail!",rs.getResult());
	Result<RowData>rd1=obClient.get(tabName, rkey1, colNames);
	for (String str : colNames) {
		Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey1, 1, false).getObject(false).getValue(),
				rd1.getResult().get(str));//验证
	}
	Result<RowData>rd2=obClient.get(tabName, rkey2, colNames);
	for (String str : colNames) {
		Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey2, 1, false).getObject(false).getValue(),
				rd2.getResult().get(str));//验证
	}
	}
@Test
public void transaction_3_07_test(){
	RKey rkey1 = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
	String tabName=collectInfoTable.getTableName();
	Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
	colNames.remove("gm_create");
	colNames.remove("gm_modified");
	DeleteMutator dm=new DeleteMutator(tabName, rkey1);
	RKey rkey2 = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
	InsertMutator im=new InsertMutator(tabName, rkey2);
	for (String str : colNames) 
	{
		im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey2, 0, false));
	}
	RKey rkey3 = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
	UpdateMutator um=new UpdateMutator(tabName,rkey3);
	for (String str : colNames) {
		um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey3, 1, false), false);
	}
	List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
	listmutator.add(dm);
	listmutator.add(im);
	listmutator.add(um);
	Result<Boolean> rs=obClient.update(listmutator);//更新操作
	Assert.assertTrue("transaction fail!",rs.getResult());
	Result<RowData>rd=obClient.get(tabName, rkey3, colNames);
	for (String str : colNames) {
		Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey3, 1, false).getObject(false).getValue(),
				rd.getResult().get(str));//验证
	}}
@Test
public void transaction_3_08_test(){
	RKey rkey1 = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
	String tabName=collectInfoTable.getTableName();
	Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
	colNames.remove("gm_create");
	colNames.remove("gm_modified");
	InsertMutator im=new InsertMutator(tabName, rkey1);
	for (String str : colNames) 
	{
		im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey1, 0, false));
	}
	RKey rkey2 = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
	DeleteMutator dm=new DeleteMutator(tabName, rkey2);
	RKey rkey3 = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
	UpdateMutator um=new UpdateMutator(tabName,rkey3);
	for (String str : colNames) {
		um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey3, 1, false), false);
	}
	List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
	listmutator.add(dm);
	listmutator.add(im);
	listmutator.add(um);
	Result<Boolean> rs=obClient.update(listmutator);//更新操作
	Assert.assertTrue("transaction fail!",rs.getResult());
	Result<RowData>rd=obClient.get(tabName, rkey3, colNames);
	for (String str : colNames) {
		Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey3, 1, false).getObject(false).getValue(),
				rd.getResult().get(str));//验证
	}}
@Test
public void transaction_3_09_test(){
	RKey rkey1 = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
	String tabName=collectInfoTable.getTableName();
	Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
	colNames.remove("gm_create");
	colNames.remove("gm_modified");
	UpdateMutator um=new UpdateMutator(tabName,rkey1);
	for (String str : colNames) {
		um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey1, 1, false), false);
	}
	RKey rkey2 = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
	InsertMutator im=new InsertMutator(tabName, rkey2);
	for (String str : colNames) 
	{
		im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey2, 0, false));
	}
	RKey rkey3 = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
	DeleteMutator dm=new DeleteMutator(tabName, rkey3);
	List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
	listmutator.add(um);
	listmutator.add(im);
	listmutator.add(dm);
	Result<Boolean> rs=obClient.update(listmutator);//更新操作
	Assert.assertTrue("transaction fail!",rs.getResult());
	Result<RowData>rd=obClient.get(tabName, rkey1, colNames);
	for (String str : colNames) {
		Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey1, 1, false).getObject(false).getValue(),
				rd.getResult().get(str));//验证
	}}
@Test
public void transaction_3_10_test(){
	RKey rkey1 = new RKey(new CollectInfoKeyRule(0,(byte)'0',1));
	String tabName=collectInfoTable.getTableName();
	Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
	colNames.remove("gm_create");
	colNames.remove("gm_modified");
	UpdateMutator um=new UpdateMutator(tabName,rkey1);
	for (String str : colNames) {
		um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey1, 1, false), false);
	}
	RKey rkey2 = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
	DeleteMutator dm=new DeleteMutator(tabName, rkey2);
	RKey rkey3 = new RKey(new CollectInfoKeyRule(0,(byte)'0',2));
	InsertMutator im=new InsertMutator(tabName, rkey3);
	for (String str : colNames) 
	{
		im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey2, 0, false));
	}
	
	List<ObMutatorBase> listmutator=new ArrayList<ObMutatorBase>(); 
	listmutator.add(um);
	listmutator.add(dm);
	listmutator.add(im);
	
	Result<Boolean> rs=obClient.update(listmutator);//更新操作
	Assert.assertTrue("transaction fail!",rs.getResult());
	Result<RowData>rd=obClient.get(tabName, rkey1, colNames);
	for (String str : colNames) {
		Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey1, 1, false).getObject(false).getValue(),
				rd.getResult().get(str));//验证
	}}
}	

