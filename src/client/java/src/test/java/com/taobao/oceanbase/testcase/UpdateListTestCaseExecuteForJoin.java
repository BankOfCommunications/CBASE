package com.taobao.oceanbase.testcase;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.oceanbase.base.BaseCase;
import com.taobao.oceanbase.base.CollectColumnEnum;
import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.vo.ObMutatorBase;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.UpdateMutator;

public class UpdateListTestCaseExecuteForJoin extends BaseCase{

	static Result<RowData> result = null;
	String collectInfoTableName = collectInfoTable.getTableName();
	String itemInfoTableName = itemInfoTable.getTableName();
	// trans collect_info rkey to item_info rkey
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
	
	public Set<String> getCollectAddColNameSet()
	{
		Set<String> set = new HashSet<String>();
		Set<String> setDel = new HashSet<String>();
		set = collectInfoTable.getNonJoinColumnNames();
		setDel.add("gm_modified");
		setDel.add("gm_create");
		CollectColumnEnum temp;
		for(String str : set)
		{
			temp = CollectColumnEnum.valueOf(str.toUpperCase());
			if(temp.getType().equals("string")){
				setDel.add(str);
			}
		}
		for(String str : setDel)
		{
			set.remove(str);
		}
		
		return set;
	}
	
	public Set<String> getItemAddColNameSet()
	{
		Set<String> set = new HashSet<String>();
		Set<String> setDel = new HashSet<String>();
		set = itemInfoTable.getAllColumnNames();
		CollectColumnEnum temp;
		for(String str : set)
		{
			temp = CollectColumnEnum.valueOf(str.toUpperCase());
			if(temp.getType().equals("string")){
				setDel.add(str);
			}
		}
		for(String str : setDel)
		{
			set.remove(str);
		}
		
		return set;
	}
	
	public Set<String> getCollectNonJoinColNameSet()
	{
		Set<String> set = new HashSet<String>();
		set = collectInfoTable.getNonJoinColumnNames();
		set.remove("gm_modified");
		set.remove("gm_create");
		return set;
	}
	
	public UpdateMutator getCollectUpdateMutator(RKey rkey, Set<String> colSet, boolean isAdd, int iTimes){
		UpdateMutator um=new UpdateMutator(collectInfoTable.getTableName(),rkey);
		for (String str : colSet) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, iTimes, isAdd), isAdd);
		}
		return um;
	}
	
	public UpdateMutator getItemUpdateMutator(RKey rkey, Set<String> colSet, boolean isAdd, int iTimes){
		UpdateMutator um=new UpdateMutator(itemInfoTable.getTableName(),rkey);
		for (String str : colSet) {
			um.addColumn(str, itemInfoTable.genColumnUpdateResult(str, rkey, iTimes, isAdd), isAdd);
		}
		return um;
	}
	
	public boolean getVerify(List<String>listTableName, List<RKey>listRowkey, List<Set<String>>listColSet, List<Integer> listTimes, List<Boolean> listIsAdd)
    {
    	boolean bRet = false;
    	Result<RowData> rs = null;
    	boolean iCollectFlag = false;
    	boolean iItemFlag = false;
    	
    	if (listTableName.size() != listRowkey.size() || listRowkey.size() != listColSet.size())
    	{
    		log.error("List size error!!!");
    		return bRet;
    	}
    	
    	for (int iLoop = 0; iLoop < listTableName.size(); iLoop ++)
    	{
    		/* Reset */
    		rs = null;
    		
    		rs = obClient.get(listTableName.get(iLoop), listRowkey.get(iLoop),listColSet.get(iLoop));
    		if (rs.getCode()!= ResultCode.OB_SUCCESS)
    		{
    			log.error("Rowkey(" + listRowkey.get(iLoop).toString() + ") is failed to get in table(" + listTableName.get(iLoop) + ")!!!");
    			return bRet;
    		}	
    		
            for (String str : listColSet.get(iLoop)) {
            	if (listTableName.get(iLoop).equals("collect_info"))
            	{
            		if (iCollectFlag)
            		{
            			if (rs.getResult() == null)
            			{
	            			log.error("Rs which is get from ob is null on table(" + listTableName.get(iLoop) + ") at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            			return bRet;
            			} else {
    	            		if (!collectInfoTable.getColumnValue(str, listRowkey.get(iLoop)).equals(rs.getResult().get(str)))
    	            		{
    	            			log.error("Col(" + str + ") is failed to verify at rowkey(" + listRowkey.get(iLoop) + ")!!!");
    	            			return bRet;
    	            		} else {
    	            			log.info("Col(" + str + ") is verified successful at rowkey(" + listRowkey.get(iLoop) + ")!!!");
    	            		}
            			}
            		} else {
	            		if (rs.getResult() == null)
	            		{
	            			log.error("Rs which is get from ob is null on table(" + listTableName.get(iLoop) + ") at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            			return bRet;
	            		}
	            		if (!collectInfoTable.genColumnUpdateResult(str, listRowkey.get(iLoop), listTimes.get(iLoop), false).getObject(listIsAdd.get(iLoop)).getValue().equals(rs.getResult().get(str)))
	            		{
	            			log.error("Col(" + str + ") is failed to verify at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            			return bRet;
	            		} else {
	            			log.info("Col(" + str + ") is verified successful at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            		}
            		}
            	} else {
            		if (iItemFlag)
            		{
                		if (rs.getResult() == null)
                		{
    	            		if (rs.getResult() == null)
    	            		{
    	            			log.error("Rs which is get from ob is null on table(" + listTableName.get(iLoop) + ") at rowkey(" + listRowkey.get(iLoop) + ")!!!");
    	            			return bRet;
    	            		}
                		} else {
    	            		if (!itemInfoTable.getColumnValue(str, listRowkey.get(iLoop)).equals(rs.getResult().get(str)))
    	            		{
    	            			log.error("Col(" + str + ") is failed to verify at rowkey(" + listRowkey.get(iLoop) + ")!!!");
    	            			return bRet;
    	            		} else {
    	            			log.info("Col(" + str + ") is verified successful at rowkey(" + listRowkey.get(iLoop) + ")!!!");
    	            		}
                		}
            		} else {
            			
	            		if (rs.getResult() == null)
	            		{
	            			log.error("Rs which is get from ob is null on table(" + listTableName.get(iLoop) + ") at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            			return bRet;
	            		}
	            		
	            		if (!itemInfoTable.genColumnUpdateResult(str, listRowkey.get(iLoop), listTimes.get(iLoop), false).getObject(listIsAdd.get(iLoop)).getValue().equals(rs.getResult().get(str)))
	            		{
	            			log.error("Col(" + str + ") is failed to verify at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            			return bRet;
	            		} else {
	            			log.info("Col(" + str + ") is verified successful at rowkey(" + listRowkey.get(iLoop) + ")!!!");
	            		}
            		}
            	}
            }
            
            if (listTableName.get(iLoop).equals("collect_info"))
            {
            	iCollectFlag = false;
            } else {
            	iItemFlag = false;
            }
            
    	}	/* for */
    	
    	return true;
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
	public  void test_update_list_01_HappyPath(){
		
		Set<String> collectColSet = new HashSet<String>();
		Set<String> itemColSet = new HashSet<String>();
		
    	/* List */
    	List<ObMutatorBase> listMutator = new ArrayList<ObMutatorBase>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	List<Integer> listTimes = new ArrayList<Integer>();
    	List<Boolean> listIsAdd = new ArrayList<Boolean>(); 
    	
    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(4, (byte)'0', 41));
		RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(4, (byte)'0', 42));
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 41));
		RKey rowkeyForItem2 = new RKey(new ItemInfoKeyRule((byte)'0', 42));
		RKey rowkeyForItem3 = new RKey(new ItemInfoKeyRule((byte)'0', 43));
		
		/* Col set */
		/* CollectInfo */
		collectColSet = getCollectNonJoinColNameSet();
		itemColSet = itemInfoTable.getAllColumnNames();

		/* Mutator */
		/* CollectInfo */
		UpdateMutator umForCollect = getCollectUpdateMutator(rowkeyForCollect, collectColSet, false, 1);
		UpdateMutator umForCollect2 = getCollectUpdateMutator(rowkeyForCollect2, collectColSet, false, 1);
		/* ItemInfo */
		UpdateMutator umForItem = getItemUpdateMutator(rowkeyForItem, itemColSet, false, 1);
		UpdateMutator umForItem2 = getItemUpdateMutator(rowkeyForItem2, itemColSet, false, 1);
		UpdateMutator umForItem3 = getItemUpdateMutator(rowkeyForItem3, itemColSet, false, 1);
		
		/* List */
		listMutator.add(umForCollect);
		listMutator.add(umForCollect2);
		listMutator.add(umForItem);
		listMutator.add(umForItem2);
		listMutator.add(umForItem3);
		
		/* Table name */
		listTableName.add(collectInfoTable.getTableName());
		listTableName.add(collectInfoTable.getTableName());
		listTableName.add(itemInfoTable.getTableName());
		listTableName.add(itemInfoTable.getTableName());
		listTableName.add(itemInfoTable.getTableName());
		
		/* ColSet */
		listColSet.add(collectColSet);
		listColSet.add(collectColSet);
		listColSet.add(itemColSet);
		listColSet.add(itemColSet);
		listColSet.add(itemColSet);
		
		/* Rowkey */
		listRowkey.add(rowkeyForCollect);
		listRowkey.add(rowkeyForCollect2);
		listRowkey.add(rowkeyForItem);
		listRowkey.add(rowkeyForItem2);
		listRowkey.add(rowkeyForItem3);
		
		/* Times */
		listTimes.add(1);
		listTimes.add(1);
		listTimes.add(1);
		listTimes.add(1);
		listTimes.add(1);
		
		/* Is add */
		listIsAdd.add(false);
		listIsAdd.add(false);
		listIsAdd.add(false);
		listIsAdd.add(false);
		listIsAdd.add(false);
		
		Result<Boolean> rs=obClient.update(listMutator);
		Assert.assertTrue("update successed!",rs.getResult());
		
		Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet, listTimes, listIsAdd));
		
	} 
	
	@Test
	public  void test_update_list_02_onerow(){
		
		Set<String> collectColSet = new HashSet<String>();
		Set<String> itemColSet = new HashSet<String>();
		
    	/* List */
    	List<ObMutatorBase> listMutator = new ArrayList<ObMutatorBase>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	List<Integer> listTimes = new ArrayList<Integer>();
    	List<Boolean> listIsAdd = new ArrayList<Boolean>(); 
    	
    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(4, (byte)'0', 41));

		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 41));

		
		/* Col set */
		/* CollectInfo */
		collectColSet = getCollectNonJoinColNameSet();
		itemColSet = itemInfoTable.getAllColumnNames();

		/* Mutator */
		/* CollectInfo */
		UpdateMutator umForCollect = getCollectUpdateMutator(rowkeyForCollect, collectColSet, false, 1);

		/* ItemInfo */
		UpdateMutator umForItem = getItemUpdateMutator(rowkeyForItem, itemColSet, false, 1);
		
		/* List */
		listMutator.add(umForCollect);
		listMutator.add(umForItem);
		
		/* Table name */
		listTableName.add(collectInfoTable.getTableName());
		listTableName.add(itemInfoTable.getTableName());
		
		/* ColSet */
		listColSet.add(collectColSet);
		listColSet.add(itemColSet);
		
		/* Rowkey */
		listRowkey.add(rowkeyForCollect);
		listRowkey.add(rowkeyForItem);
		
		/* Times */
		listTimes.add(1);
		listTimes.add(1);
		
		/* Is add */
		listIsAdd.add(false);
		listIsAdd.add(false);
		
		Result<Boolean> rs=obClient.update(listMutator);
		Assert.assertTrue("update successed!",rs.getResult());
		
		Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet, listTimes, listIsAdd));
		
	} 

	@Test
	public  void test_update_list_03_one_collect_table_onerow(){
		
		Set<String> collectColSet = new HashSet<String>();
		Set<String> itemColSet = new HashSet<String>();
		
    	/* List */
    	List<ObMutatorBase> listMutator = new ArrayList<ObMutatorBase>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	List<Integer> listTimes = new ArrayList<Integer>();
    	List<Boolean> listIsAdd = new ArrayList<Boolean>(); 
    	
    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(4, (byte)'0', 41));

		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 41));

		
		/* Col set */
		/* CollectInfo */
		collectColSet = getCollectNonJoinColNameSet();
		itemColSet = itemInfoTable.getAllColumnNames();

		/* Mutator */
		/* CollectInfo */
		UpdateMutator umForCollect = getCollectUpdateMutator(rowkeyForCollect, collectColSet, false, 1);

		/* ItemInfo */
		UpdateMutator umForItem = getItemUpdateMutator(rowkeyForItem, itemColSet, false, 1);
		
		/* List */
		listMutator.add(umForCollect);
		
		/* Table name */
		listTableName.add(collectInfoTable.getTableName());
		
		/* ColSet */
		listColSet.add(collectColSet);
		
		/* Rowkey */
		listRowkey.add(rowkeyForCollect);
		
		/* Times */
		listTimes.add(1);
		
		/* Is add */
		listIsAdd.add(false);
		
		Result<Boolean> rs=obClient.update(listMutator);
		Assert.assertTrue("update successed!",rs.getResult());
		
		Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet, listTimes, listIsAdd));
		
	} 
	
	@Test
	public  void test_update_list_04_one_table_item_onerow(){
		
		Set<String> collectColSet = new HashSet<String>();
		Set<String> itemColSet = new HashSet<String>();
		
    	/* List */
    	List<ObMutatorBase> listMutator = new ArrayList<ObMutatorBase>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	List<Integer> listTimes = new ArrayList<Integer>();
    	List<Boolean> listIsAdd = new ArrayList<Boolean>(); 
    	
    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(4, (byte)'0', 41));

		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 41));

		
		/* Col set */
		/* CollectInfo */
		collectColSet = getCollectNonJoinColNameSet();
		itemColSet = itemInfoTable.getAllColumnNames();

		/* Mutator */
		/* CollectInfo */
		UpdateMutator umForCollect = getCollectUpdateMutator(rowkeyForCollect, collectColSet, false, 1);

		/* ItemInfo */
		UpdateMutator umForItem = getItemUpdateMutator(rowkeyForItem, itemColSet, false, 1);
		
		/* List */
		listMutator.add(umForItem);
		
		/* Table name */
		listTableName.add(itemInfoTable.getTableName());
		
		/* ColSet */
		listColSet.add(itemColSet);
		
		/* Rowkey */
		listRowkey.add(rowkeyForItem);
		
		/* Times */
		listTimes.add(1);
		
		/* Is add */
		listIsAdd.add(false);
		
		Result<Boolean> rs=obClient.update(listMutator);
		Assert.assertTrue("update successed!",rs.getResult());
		
		Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet, listTimes, listIsAdd));
		
	}
	
	@Test
	public  void test_update_list_05_null(){
		
		List<ObMutatorBase> listMutator = null;
		Result<Boolean> rs = null;
		try{
			rs=obClient.update(listMutator);
			Assert.fail("update a null");
		} catch(IllegalArgumentException e){
			Assert.assertTrue(e.getMessage(),rs==null);
		}
	}
	
	@Test
	public  void test_update_list_06_one_null(){
		
		Set<String> collectColSet = new HashSet<String>();
		Set<String> itemColSet = new HashSet<String>();
		
    	/* List */
    	List<ObMutatorBase> listMutator = new ArrayList<ObMutatorBase>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	List<Integer> listTimes = new ArrayList<Integer>();
    	List<Boolean> listIsAdd = new ArrayList<Boolean>(); 
    	
    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(4, (byte)'0', 41));
		RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(4, (byte)'0', 42));
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 41));
		RKey rowkeyForItem2 = new RKey(new ItemInfoKeyRule((byte)'0', 42));
		RKey rowkeyForItem3 = new RKey(new ItemInfoKeyRule((byte)'0', 43));
		
		/* Col set */
		/* CollectInfo */
		collectColSet = getCollectNonJoinColNameSet();
		itemColSet = itemInfoTable.getAllColumnNames();

		/* Mutator */
		/* CollectInfo */
		UpdateMutator umForCollect = getCollectUpdateMutator(rowkeyForCollect, collectColSet, false, 1);
		UpdateMutator umForCollect2 = getCollectUpdateMutator(rowkeyForCollect2, collectColSet, false, 1);
		/* ItemInfo */
		UpdateMutator umForItem = getItemUpdateMutator(rowkeyForItem, itemColSet, false, 1);
		UpdateMutator umForItem2 = getItemUpdateMutator(rowkeyForItem2, itemColSet, false, 1);
		UpdateMutator umForItem3 = getItemUpdateMutator(rowkeyForItem3, itemColSet, false, 1);
		
		/* List */
		listMutator.add(umForCollect);
		listMutator.add(umForCollect2);
		listMutator.add(umForItem);
		listMutator.add(umForItem2);
		listMutator.add(null);
		
		/* Table name */
		listTableName.add(collectInfoTable.getTableName());
		listTableName.add(collectInfoTable.getTableName());
		listTableName.add(itemInfoTable.getTableName());
		listTableName.add(itemInfoTable.getTableName());
		listTableName.add(itemInfoTable.getTableName());
		
		/* ColSet */
		listColSet.add(collectColSet);
		listColSet.add(collectColSet);
		listColSet.add(itemColSet);
		listColSet.add(itemColSet);
		listColSet.add(itemColSet);
		
		/* Rowkey */
		listRowkey.add(rowkeyForCollect);
		listRowkey.add(rowkeyForCollect2);
		listRowkey.add(rowkeyForItem);
		listRowkey.add(rowkeyForItem2);
		listRowkey.add(rowkeyForItem3);
		
		/* Times */
		listTimes.add(1);
		listTimes.add(1);
		listTimes.add(1);
		listTimes.add(1);
		listTimes.add(1);
		
		/* Is add */
		listIsAdd.add(false);
		listIsAdd.add(false);
		listIsAdd.add(false);
		listIsAdd.add(false);
		listIsAdd.add(false);
		
		Result<Boolean> rs = null;
		
		try{
			rs=obClient.update(listMutator);
			Assert.fail("update a null");
		} catch(IllegalArgumentException e){
			Assert.assertTrue(e.getMessage(),rs==null);
			Assert.assertFalse(getVerify(listTableName, listRowkey, listColSet, listTimes, listIsAdd));
		}
		
		
	} 
	
	@Test
	public  void test_update_list_07_empty(){
		
		List<ObMutatorBase> listMutator = new ArrayList<ObMutatorBase>();
		Result<Boolean> rs = null;
		try{
			rs=obClient.update(listMutator);
			Assert.fail("update null");
		} catch(IllegalArgumentException e){
			Assert.assertTrue(e.getMessage(), rs==null);
		}
	}
	
	@Test
	public  void test_update_list_08_one_collect_table_duplicate_row(){
		
		Set<String> collectColSet = new HashSet<String>();
		Set<String> itemColSet = new HashSet<String>();
		
    	/* List */
    	List<ObMutatorBase> listMutator = new ArrayList<ObMutatorBase>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	List<Integer> listTimes = new ArrayList<Integer>();
    	List<Boolean> listIsAdd = new ArrayList<Boolean>(); 
    	
    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(4, (byte)'0', 41));

		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 41));

		
		/* Col set */
		/* CollectInfo */
		collectColSet = getCollectNonJoinColNameSet();
		itemColSet = itemInfoTable.getAllColumnNames();

		/* Mutator */
		/* CollectInfo */
		UpdateMutator umForCollect = getCollectUpdateMutator(rowkeyForCollect, collectColSet, false, 1);
		UpdateMutator umForCollect2 = getCollectUpdateMutator(rowkeyForCollect, collectColSet, false, 1);

		/* ItemInfo */
		UpdateMutator umForItem = getItemUpdateMutator(rowkeyForItem, itemColSet, false, 1);
		
		/* List */
		listMutator.add(umForCollect);
		
		/* Table name */
		listTableName.add(collectInfoTable.getTableName());
		listTableName.add(collectInfoTable.getTableName());
		
		/* ColSet */
		listColSet.add(collectColSet);
		listColSet.add(collectColSet);
		
		/* Rowkey */
		listRowkey.add(rowkeyForCollect);
		listRowkey.add(rowkeyForCollect);
		
		/* Times */
		listTimes.add(1);
		listTimes.add(1);
		
		/* Is add */
		listIsAdd.add(false);
		listIsAdd.add(false);
		
		Result<Boolean> rs=obClient.update(listMutator);
		Assert.assertTrue("update successed!",rs.getResult());
		
		Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet, listTimes, listIsAdd));
		
	} 
	
	@Test
	public  void test_update_list_09_add(){
		
		Set<String> collectColSet = new HashSet<String>();
		Set<String> itemColSet = new HashSet<String>();
		
    	/* List */
    	List<ObMutatorBase> listMutator = new ArrayList<ObMutatorBase>();
    	List<String> listTableName = new ArrayList<String>();
    	List<RKey> listRowkey = new ArrayList<RKey>();
    	List<Set<String>> listColSet = new ArrayList<Set<String>>();
    	List<Integer> listTimes = new ArrayList<Integer>();
    	List<Boolean> listIsAdd = new ArrayList<Boolean>(); 
    	
    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(4, (byte)'0', 41));
		RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(4, (byte)'0', 42));
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 41));
		RKey rowkeyForItem2 = new RKey(new ItemInfoKeyRule((byte)'0', 42));
		RKey rowkeyForItem3 = new RKey(new ItemInfoKeyRule((byte)'0', 43));
		
		/* Col set */
		/* CollectInfo */
		collectColSet = getCollectAddColNameSet();
		itemColSet = getItemAddColNameSet();

		/* Mutator */
		/* CollectInfo */
		UpdateMutator umForCollect = getCollectUpdateMutator(rowkeyForCollect, collectColSet, true, 1);
		UpdateMutator umForCollect2 = getCollectUpdateMutator(rowkeyForCollect2, collectColSet, true, 2);
		/* ItemInfo */
		UpdateMutator umForItem = getItemUpdateMutator(rowkeyForItem, itemColSet, true, 3);
		UpdateMutator umForItem2 = getItemUpdateMutator(rowkeyForItem2, itemColSet, true, 4);
		UpdateMutator umForItem3 = getItemUpdateMutator(rowkeyForItem3, itemColSet, true, 5);
		
		/* List */
		listMutator.add(umForCollect);
		listMutator.add(umForCollect2);
		listMutator.add(umForItem);
		listMutator.add(umForItem2);
		listMutator.add(umForItem3);
		
		/* Table name */
		listTableName.add(collectInfoTable.getTableName());
		listTableName.add(collectInfoTable.getTableName());
		listTableName.add(itemInfoTable.getTableName());
		listTableName.add(itemInfoTable.getTableName());
		listTableName.add(itemInfoTable.getTableName());
		
		/* ColSet */
		listColSet.add(collectColSet);
		listColSet.add(collectColSet);
		listColSet.add(itemColSet);
		listColSet.add(itemColSet);
		listColSet.add(itemColSet);
		
		/* Rowkey */
		listRowkey.add(rowkeyForCollect);
		listRowkey.add(rowkeyForCollect2);
		listRowkey.add(rowkeyForItem);
		listRowkey.add(rowkeyForItem2);
		listRowkey.add(rowkeyForItem3);
		
		/* Times */
		listTimes.add(1);
		listTimes.add(2);
		listTimes.add(3);
		listTimes.add(4);
		listTimes.add(5);
		
		/* Is add */
		listIsAdd.add(true);
		listIsAdd.add(true);
		listIsAdd.add(true);
		listIsAdd.add(true);
		listIsAdd.add(true);
		
		Result<Boolean> rs=obClient.update(listMutator);
		Assert.assertTrue("update successed!",rs.getResult());
		
		Assert.assertTrue(getVerify(listTableName, listRowkey, listColSet, listTimes, listIsAdd));
		
	} 
	
}
		


