package com.taobao.oceanbase.testcase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import junit.framework.Assert;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.oceanbase.base.BaseCase;
import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Rowkey;
public class RemoveListTestCaseExecuteForJoin extends BaseCase{

	/**
	 * @param args
	 */
//	private static String exceptionMessage=new String();
//	private String collectInfoTableName = collectInfoTable.getTableName();
//	private String itemInfoTableName = itemInfoTable.getTableName();
	static Result<RowData> result=null;

	// trans collect_info rkey to item_info rkey
//	private RKey transKey(RKey rkey) {
//		ItemInfoKeyRule rule = new ItemInfoKeyRule(
//				((CollectInfoKeyRule) (rkey.getKeyRule())).getItem_type(),
//				((CollectInfoKeyRule) (rkey.getKeyRule())).getItem_id());
//		return new RKey(rule);
//	}
	
	public boolean getVerify(Map<String, Set<Rowkey>> map)
	{		
		Iterator<Map.Entry<String,Set<Rowkey>>> it = map.entrySet().iterator() ; 
		while (it.hasNext()) 
		{
			Map.Entry<String, Set<Rowkey>> entry = it.next(); 
			Set<Rowkey> set = new HashSet<Rowkey>();
			set = (Set<Rowkey>) entry.getValue();
			if (entry.getKey().equals("collect_info"))
			{
				Iterator<Rowkey> itSet = set.iterator(); 
				while (itSet.hasNext())
				{
					result = obClient.get((String)entry.getKey(), (Rowkey)itSet.next(), collectInfoTable.getNonJoinColumnNames());
					assertEquals(ResultCode.OB_SUCCESS, result.getCode());
					if (result.getResult().isEmpty() == false )
					{
						log.error("Collect_InfoThe data is still exist after delete at rowkey(" + entry.getValue().toString() + ")!!!");
						return false;
					}
				}
			} else {
				Iterator<Rowkey> itSet = set.iterator(); 
				while (itSet.hasNext())
				{
					result = obClient.get((String)entry.getKey(), (Rowkey)itSet.next(), itemInfoTable.getAllColumnNames());
					assertEquals(ResultCode.OB_SUCCESS, result.getCode());
					if (result.getResult().isEmpty() == false )
					{
						log.error("The data is still exist after delete at rowkey(" + entry.getValue().toString() + ")!!!");
						return false;
					}
				}
			}
			
		}
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
	public  void test_remove_01_HappyPath() {

    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte)'0', 51));
		RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(3, (byte)'0', 52));
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 51));
		RKey rowkeyForItem2 = new RKey(new ItemInfoKeyRule((byte)'0', 52));
		RKey rowkeyForItem3 = new RKey(new ItemInfoKeyRule((byte)'0', 53));
		
		Map<String,Set<Rowkey>> map = new HashMap<String, Set<Rowkey>>(); 
		Set<Rowkey> setRowkey = new HashSet<Rowkey>();
		Set<Rowkey> setRowkeyI = new HashSet<Rowkey>();
//		Set<Rowkey> setRowkeyII = new HashSet<Rowkey>();
//		Set<Rowkey> setRowkeyIII = new HashSet<Rowkey>();
//		Set<Rowkey> setRowkeyIV = new HashSet<Rowkey>();
		
		setRowkey.add(rowkeyForCollect);
		setRowkey.add(rowkeyForCollect2);
		setRowkeyI.add(rowkeyForItem);
		setRowkeyI.add(rowkeyForItem2);
		setRowkeyI.add(rowkeyForItem3);
		
		map.put(collectInfoTable.getTableName(),setRowkey);
		map.put(itemInfoTable.getTableName(), setRowkeyI);
		//map.put(itemInfoTable.getTableName(), setRowkeyII);
		//map.put(itemInfoTable.getTableName(), setRowkeyIII);
		//map.put(itemInfoTable.getTableName(), setRowkeyIV);
		
		assertTrue("delete failed",obClient.delete(map).getResult());
		
		Assert.assertTrue(getVerify(map));
	}
	
	@Test
	public  void test_remove_02_onerow() {

    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte)'0', 51));

		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 51));

		
		Map<String,Set<Rowkey>> map = new HashMap<String, Set<Rowkey>>();
		
		Set<Rowkey> setRowkey = new HashSet<Rowkey>();
		Set<Rowkey> setRowkeyI = new HashSet<Rowkey>();
		
		setRowkey.add(rowkeyForCollect);
		setRowkeyI.add(rowkeyForItem);
		
		map.put(collectInfoTable.getTableName(),setRowkey);
		map.put(itemInfoTable.getTableName(), setRowkeyI);

		
		assertTrue("delete failed",obClient.delete((Map<String,Set<Rowkey>>)map).getResult());
		
		Assert.assertTrue(getVerify(map));
	}
	
	@Test
	public  void test_remove_03_one_collect_onerow() {

    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte)'0', 51));

		/* ItemInfo */

		
		Map<String,Set<Rowkey>> map = new HashMap<String, Set<Rowkey>>(); 
		
		Set<Rowkey> setRowkey = new HashSet<Rowkey>();
		
		setRowkey.add(rowkeyForCollect);
		
		map.put(collectInfoTable.getTableName(),setRowkey);

		
		assertTrue("delete failed",obClient.delete((Map<String,Set<Rowkey>>)map).getResult());
		
		Assert.assertTrue(getVerify(map));
	}
	
	@Test
	public  void test_remove_04_one_item_onerow() {

    	/* Rowkey */
		/* CollectInfo */

		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 51));

		
		Map<String,Set<Rowkey>> map = new HashMap<String, Set<Rowkey>>(); 
		
		Set<Rowkey> setRowkey = new HashSet<Rowkey>();
		
		setRowkey.add(rowkeyForItem);
		
		map.put(itemInfoTable.getTableName(), setRowkey);

		
		assertTrue("delete failed",obClient.delete((Map<String,Set<Rowkey>>)map).getResult());
		
		Assert.assertTrue(getVerify(map));
	}
	
	@Test
	public  void test_remove_05_null() {
		
		Map<String,Set<Rowkey>> map = null; 
	
		Result<Boolean> rs = null;
		try{
			rs = obClient.delete((Map<String,Set<Rowkey>>)map);
			Assert.fail("delete a null");
		} catch(IllegalArgumentException e){
			Assert.assertTrue(e.getMessage(),rs == null);
		}
	}
	
	@Test
	public  void test_remove_06_one_null() {

    	/* Rowkey */
		/* CollectInfo */
		RKey rowkeyForCollect = new RKey(new CollectInfoKeyRule(3, (byte)'0', 51));
		RKey rowkeyForCollect2 = new RKey(new CollectInfoKeyRule(3, (byte)'0', 52));
		/* ItemInfo */
		RKey rowkeyForItem = new RKey(new ItemInfoKeyRule((byte)'0', 51));
		RKey rowkeyForItem2 = new RKey(new ItemInfoKeyRule((byte)'0', 52));
		//RKey rowkeyForItem3 = new RKey(new ItemInfoKeyRule((byte)'0', 53));
		
		Map<String,Set<Rowkey>> map = new HashMap<String, Set<Rowkey>>(); 
		
		Set<Rowkey> setRowkey = new HashSet<Rowkey>();
		Set<Rowkey> setRowkeyI = new HashSet<Rowkey>();
		Set<Rowkey> setRowkeyII = new HashSet<Rowkey>();
		Set<Rowkey> setRowkeyIII = new HashSet<Rowkey>();

		setRowkey.add(rowkeyForCollect);
		setRowkeyI.add(rowkeyForCollect2);
		setRowkeyII.add(rowkeyForItem);
		setRowkeyIII.add(rowkeyForItem2);
		
		map.put(collectInfoTable.getTableName(),setRowkey);
		map.put(collectInfoTable.getTableName(), setRowkeyI);
		map.put(itemInfoTable.getTableName(), setRowkeyII);
		map.put(itemInfoTable.getTableName(), setRowkeyIII);
		
		map.put(null, null);
		
		Result<Boolean> rs = null;
		try{
			rs = obClient.delete((Map<String,Set<Rowkey>>)map);
			Assert.fail("delete a null");
		} catch(IllegalArgumentException e){
			Assert.assertTrue(e.getMessage(),rs == null);
			
			map.clear();
			map.put(collectInfoTable.getTableName(),setRowkey);
			
			Assert.assertFalse(getVerify(map));
		}
	}
	
	
	@Test
	public  void test_remove_07_empty() {
		
		Map<String,Set<Rowkey>> map = new HashMap<String,Set<Rowkey>>(); 
	
		Result<Boolean> rs = null;
		try{
			rs = obClient.delete((Map<String,Set<Rowkey>>)map);
			Assert.fail("remove null!");
		} catch(IllegalArgumentException e){
			Assert.assertTrue(e.getMessage(), rs==null);
		}
	}
	
}
