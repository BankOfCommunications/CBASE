    package com.taobao.oceanbase.testcase;
	import static org.junit.Assert.assertEquals;
	
	
	//import java.text.ParseException;
	import java.text.SimpleDateFormat;
	import java.util.Date;
	import java.util.HashSet;
	import java.util.List;
	import java.util.Set;
	
	import junit.framework.Assert;
	
	import org.junit.After;
	import org.junit.Before;
	import org.junit.Test;
	//import org.springframework.dao.DataAccessException;

	import com.taobao.oceanbase.base.BaseCase;
	import com.taobao.oceanbase.base.RKey;
	import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
	import com.taobao.oceanbase.base.rule.ItemInfoKeyRule;
	//import com.taobao.oceanbase.base.table.CollectInfoTable;
	//import com.taobao.oceanbase.util.Const;
	import com.taobao.oceanbase.vo.InsertMutator;
	import com.taobao.oceanbase.vo.QueryInfo;
	import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
	import com.taobao.oceanbase.vo.RowData;
	import com.taobao.oceanbase.vo.Value;
	import com.taobao.oceanbase.vo.inner.ObSimpleCond;
   // import com.taobao.oceanbase.vo.inner.ObSimpleCond.ObLogicOperator;
import com.taobao.oceanbase.vo.inner.ObSimpleFilter;
	    public class QueryTestCaseForWhere_04 extends BaseCase{
	    private  String[] ColumnInfo = collectInfoTable.getAllColumnNames().toArray(new String[0]);
	
	    private int count = 7;
		private String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
		private String sUserNick[]={"lexin","baoni","dongpo","gongyuan","zongluo","yinlong", null};
		private long lOwnerId[]={1, 1, 2, 2, 3, 5, 6};
		private String sDate[]={"2001-12-01 12:00:00", "2002-12-01 12:00:00", "2003-12-01 12:00:00",
					            "2004-12-01 12:00:00", "2005-12-01 12:00:00", "2006-12-01 12:00:00", 
					            "2007-12-01 12:00:00"};
		private Date[] dtCollectTime = new Date[7];
		
	    private long lCollecTimePrecise[]={10001,20001,30001,40001,50001,10001,10002};


	//	private Object infoColumn;
	
	   	private static void prepareInfo(QueryInfo queryInfo, RKey start_rowkey, RKey end_rowkey) 
	   	{
	    	queryInfo.setStartKey(start_rowkey);
	    	queryInfo.setEndKey(end_rowkey);
	    	queryInfo.setInclusiveStart(true);
	    	queryInfo.setInclusiveEnd(true);
	    	queryInfo.setLimit(2000);
	    	queryInfo.setPageSize(30);
	    	queryInfo.setPageNum(1);
	   	}
	
	   	private void addColumns(QueryInfo info) {
			for (int i = 0; i < ColumnInfo.length; i++) {
				info.addColumn(ColumnInfo[i]);
			}
		}
	   	//rData是查询出来的行
	   	//nInsertNumber是准备数据中原表行的序号,
	   	//验证查找的数据是不是我们想要的数据。
	   	private void check_query_result(RowData rData, int nInsertNumber)
	    {
	   	    for (String infoColumn: collectInfoTable.getAllColumnNames()) 
	   	    {
	   	        if (infoColumn.equals(sColumns[0])) 
	   	        {
	   	            assertEquals(sUserNick[nInsertNumber], rData.get(infoColumn));
	   	        } 
	   	        else if(infoColumn.equals(sColumns[2])) 
	   	        {
	   	           assertEquals(dtCollectTime[nInsertNumber].getTime(),rData.get(infoColumn));
	   	        }
	   	        else if(infoColumn.equals(sColumns[3])) 
	   	        {
	   	           assertEquals(lCollecTimePrecise[nInsertNumber], rData.get(infoColumn));
	   	        }
	   	     }
	   	    for (String infoColumn: itemInfoTable.getAllColumnNames()) 
	   	    {
	   	        if (infoColumn.equals(sColumns[1])) 
	   	        {
	   	        	//System.out.println(infoColumn + " value is: " + rData.get(infoColumn));
	                assertEquals(lOwnerId[nInsertNumber], rData.get(infoColumn));
	            } 
	        }
	    }
	   	
//   private void query_check_JoinInfo(Result<List<RowData>> result,RKey start_rowkey, RKey end_rowkey,
//			boolean isInclusiveStart,
//			boolean isInclusiveEnd) {
//	
//		CollectInfoKeyRule infoRowkeys = ((CollectInfoKeyRule) start_rowkey
//				.getKeyRule());
//		long length = CollectInfoKeyRule.countNum(start_rowkey, end_rowkey,
//				isInclusiveStart, isInclusiveEnd);
//		assertEquals(result.getResult().size(), length);
//	
//		RKey infoRowkey;
//		for (int i = 0; i < length; i++) {
//			if (i == 0 && isInclusiveStart) {
//				infoRowkey = infoRowkeys.currRkey();
//			} else {
//				infoRowkey = infoRowkeys.nextRkey();
//			}
//	
//			for (String infoColumn : collectInfoTable.getAllColumnNames()) 
//			{
//				assertEquals(
//						collectInfoTable.getColumnValue(infoColumn, infoRowkey),
//						result.getResult().get(i).get(infoColumn));
//			}
//		}
//		}
		public Set<String> setInsertColumnsforCollect() {
	        Set<String> Columns = collectInfoTable.getAllColumnNames();
	        return Columns;
	    }
	
		private void delete_data(int nStartColumn, int nColumnCount)
		{
			int i;
			for (i = nStartColumn; i < nColumnCount; i++) {
				RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', i));
				Assert.assertTrue("delete failed",obClient.delete(collectInfoTable.getTableName(), rkey).getResult());
			}
			for (i = nStartColumn; i < nColumnCount; i++) {
				RKey rkey = new RKey(new ItemInfoKeyRule((byte) '0', i));
				Assert.assertTrue("delete failed",obClient.delete(itemInfoTable.getTableName(), rkey).getResult());
			}
		}
	
        private void prepare_ob_data() 
		{
			// prepare collectInfo table data
			// string			long	    date		  precise time
			//"user_nick", "owner_id", "collect_time", "collect_time_precise"
			//prepare Null
			int i;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-DD hh:mm:ss");
			delete_data(1, 7);
            for(i = 0; i < count; i++)
			{
				try{
					dtCollectTime[i] = sdf.parse(sDate[i]);
				}catch (Exception e ) {
					e.printStackTrace();
				}
			}

			//prepare collect info data, non join field, str2col
			for (i = 0; i < count; i++) 
			{
				RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', i+1));
				InsertMutator im = new InsertMutator(collectInfoTable.getTableName(), rkey);
	
				Value value00 = new Value();
				if( i != 6 )
    			value00.setString(sUserNick[i]);
	
				Value value02 = new Value();
				value02.setSecond(dtCollectTime[i].getTime());

				Value value03= new Value();
				value03.setMicrosecond(lCollecTimePrecise[i]);
	
				im.addColumn(sColumns[0], value00);
				im.addColumn(sColumns[2], value02);
				im.addColumn(sColumns[3], value03);
	
				Assert.assertTrue("Insert fail!", obClient.insert(im).getResult());
			}
			//prepare item info date, jstr2col
			for (i = 0; i < count; i++) 
			{
				RKey rkey = new RKey(new ItemInfoKeyRule((byte) '0', i+1));
				InsertMutator im = new InsertMutator(itemInfoTable.getTableName(), rkey);
	
				Value value01 = new Value();
				value01.setNumber(lOwnerId[i]);
	
				im.addColumn(sColumns[1], value01);
	
				Assert.assertTrue("Insert fail!", obClient.insert(im).getResult());
			}
			verify_insert_data();
		}
        
		private void prepare_null_data() 
		{
		    int RkeyNum = 7;

		    RKey rkey_collect = new RKey(new CollectInfoKeyRule(2, (byte) '0', RkeyNum));
			InsertMutator im = new InsertMutator(collectInfoTable.getTableName(), rkey_collect);

			delete_data(7,1);

            Value val = new Value();
            im.addColumn(sColumns[0], val);
            im.addColumn(sColumns[2], val);
            im.addColumn(sColumns[3], val);
            Set<String> colNames=collectInfoTable.getNonJoinColumnNames();
            colNames.remove(sColumns[0]);
            colNames.remove(sColumns[2]);
            colNames.remove(sColumns[3]);
            colNames.remove("gm_modified");
            colNames.remove("gm_create");
            for(String str:colNames)
            {
            	im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey_collect, 0, false));
            }
			Assert.assertTrue("Insert fail!", obClient.insert(im).getResult());
			
			
			// prepare itemInfo table data
			RKey rkey_item = new RKey(new ItemInfoKeyRule((byte) '0', RkeyNum));
			InsertMutator im02 = new InsertMutator(itemInfoTable.getTableName(), rkey_item);
			
			im.addColumn(sColumns[1], val);

            Set<String> colNames1=itemInfoTable.getAllColumnNames();
            colNames1.remove(sColumns[1]);
            for(String str:colNames1)
            {
            	im02.addColumn(str, itemInfoTable.genColumnUpdateResult(str, rkey_item, 0, false));
            }
			Assert.assertTrue("Insert fail!", obClient.insert(im02).getResult()); 
		}	
		private void verify_insert_data()  
		{
			// verify collectInfo table data
			// string			long	    date		  precise time
			//"user_nick", "owner_id", "collect_time", "collect_time_precise"

			Set<String> ColumnsforCollectInsert = setInsertColumnsforCollect();
			for (int i = 0; i < count; i++) 
			{
				Result<RowData> result;
				RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', i + 1));
				result = obClient.get(collectInfoTable.getTableName(), rkey, ColumnsforCollectInsert);
				//print_columns(i);
				for (String infoColumn : ColumnsforCollectInsert) 
				{
					if (infoColumn.equals(sColumns[0]))
					{
						assertEquals(sUserNick[i], result.getResult().get(infoColumn));
					}
					else if (infoColumn.equals(sColumns[1])) 
					{
						assertEquals(lOwnerId[i], result.getResult().get(infoColumn));
					}
					else if (infoColumn.equals(sColumns[2]))
					{
						assertEquals(dtCollectTime[i].getTime(), result.getResult().get(infoColumn));
					}
					else if (infoColumn.equals(sColumns[3]))
					{
						assertEquals(lCollecTimePrecise[i], result.getResult().get(infoColumn));
					} 
				}
			}
		}
//	    private void (int keyNum)
//	    {
//			RKey rKey = new RKey(new CollectInfoKeyRule(2, (byte) '0', keyNum));
//			for (String infoColumn : collectInfoTable.getNonJoinColumnNames()) 
//			{
//	            if( infoColumn.equals(sColumns[0]) || infoColumn.equals(sColumns[1])
//                  || infoColumn.equals(sColumns[2]) || infoColumn.equals(sColumns[3])
//                  || infoColumn.equals("gm_create") || infoColumn.equals("gm_modified") ) 
//                {
//			        System.out.println(infoColumn + " is :" + collectInfoTable.getColumnValue(infoColumn, rKey));
//                }
//			}
//        }	
		@Before
		public void before(){
			obClient.clearActiveMemTale();
			prepare_ob_data();
		}
		@After
		public void after(){
			obClient.clearActiveMemTale();
		}
		@Test
		public void test_Query_04_01_ColumnValueEqualCreateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		    
		    prepare_ob_data();

		   // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		   	addColumns(queryInfo);

		   	//RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond( gm_createValue );

		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.EQ, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(1, result.getResult().size());
		    check_query_result(result.getResult().get(0), 0);
		}
		@Test
		public void test_query_04_02_where_ColumnValue_equal_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();
		    verify_insert_data();
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setString("abcd");
		    //val.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.EQ, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());  
		    Assert.assertEquals(0,result.getResult().size()); 

		}
		@Test
		public void test_query_04_03_where_ColumnValue_equal_PreciseTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value obj = new Value();
		    obj.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise", ObSimpleCond.ObLogicOperator.EQ, obj);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);

		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(0, result.getResult().size());
		}
		@Test
		public void test_query_04_04_where_ColumnValue_equal_DateTimeType()  
		{
		    /*
		    **  QueryInfo
		    **  Filter
		    **  Column      Operator        Operand
		    **  Column is OB column
		    **  Operator is > < <> >= <= = like
		    **  Operand is Value
		    **  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
		    */
		    Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val=new Value();
			val.setSecond(dtCollectTime[0].getTime());//2001-12-01 
		
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.EQ, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);

		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(result.getResult().size(), 1);//查询出匹配的行数
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals("success", result.getCode().getMessage());
		    // verify data 插入的数据和查询的数据是否一致
		    check_query_result(result.getResult().get(0), 0);
		}
		@Test
		public void test_query_04_05_01where_ColumnValue_equal_IntType()  
		{
		    /*
		    **  QueryInfo
		    **  Filter
		    **  Column      Operator        Operand
		    **  Column is OB column
		    **  Operator is > < <> >= <= = like
		    **  Operand is Value
		    **  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
		    */
		    Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		    
		    prepare_ob_data();
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.EQ, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(result.getResult().size(),2);//查询出匹配的行数
		
		    // verify data 插入的数据和查询的数据是否一致
		    for( int i = 0; i < 2; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);
		    }
		}
		@Test
		public void test_query_04_05_02where_ColumnValue_equal_NULL()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));

		    prepare_ob_data();
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise", ObSimpleCond.ObLogicOperator.EQ, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);

		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		   Assert.assertEquals("success", result.getCode().getMessage());
		    Assert.assertEquals(0, result.getResult().size());
		}
		@Test
		public void test_query_04_06_where_ColumnValue_Equal_ObPreciseDateTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
		    **  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    
		    
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise", ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(2, result.getResult().size());
		
		    // verify data 插入的数据和查询的数据是否一致 1, 6
		    check_query_result(result.getResult().get(0), 0);
		    check_query_result(result.getResult().get(1), 5);
		    }
		@Test
		public void test_query_04_07_where_ColumnValue_equal_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Fitler
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_createValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.EQ, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			
			Assert.assertEquals(1, result.getResult().size());
		    check_query_result(result.getResult().get(0), 0);
		}	
		@Test
		public void test_query_04_08_where_ColumnValue_NotEqual_DateTimeType()
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data(); 
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    
		    
		    Value val = new Value();
		    val.setSecond(dtCollectTime[0].getTime());
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.NE, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
	
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(6, result.getResult().size());
		
			// verify data 插入的数据和查询的数据是否一致
		    for( int i = 0; i < 6; i++)
		    {
		    	check_query_result(result.getResult().get(i), i+1);
			}
		}
		@Test
		public void test_query_04_09_where_ColumnValue_equal_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data(); 
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.EQ, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(0, result.getResult().size());
		}
		@Test
		public void test_query_04_10_where_ColumnValue_equal_ObPreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create",ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
	
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(0, result.getResult().size());
		}	
		@Test
		public void test_Query_04_11_where_ColumnValueEqualObPreciseDateTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
		    **  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]);//查询的是第0个数据10001
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(0, result.getResult().size());
			}
		@Test
		public void test_Query_04_12_Where_ColumnValueNotEqualIntType() 
		{
		    Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.NE, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(result.getResult().size(),5);//查询出匹配的行数
		
		    // verify data 插入的数据和查询的数据是否一致
		    for( int i = 0; i < 5; i++)
		    {
		    	check_query_result(result.getResult().get(i), i+2);  
		    }
		}
		@Test
		public void test_Query_04_13_where_ColumnValueNotEqualObPreciseDateTimeType() 
		{
			/*
			**  QueryInfo
			**   Fitler
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
		    **  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]);//查询的是第1个数据20001
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(7, result.getResult().size());
		 // verify data 插入的数据和查询的数据是否一致
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  
			}
		}
		@Test
	    public void test_Query_04_14_where_ColumnValueNotEqualObPreciseDateTimeType() 
		{
			/*
			**  QueryInfo
			**   Fitler
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
		    **  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.NE, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);;
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(7, result.getResult().size());
		    // verify data 插入的数据和查询的数据是否一致
		    for( int i = 0; i < 7; i++)
		    {
		       	check_query_result(result.getResult().get(i), i);  
		       	
		   	}
		}
		@Test
		public void test_Query_04_15_where_ColumnNotEqualValueNull() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
		    **  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    
		    Value val = new Value();
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.NE, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);;

		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(7, result.getResult().size());
		    // verify data 插入的数据和查询的数据是否一致
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }
		}
		@Test	
		public void test_Query_04_16_where_ColumnLessThanValueNull() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
		    **  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			**此次测试查不到数据
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    
		    Value val = new Value();
		    // "owner_id"(long) < null
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.LT, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals("invalid argument", result.getCode().getMessage());  
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
		@Test // 
		public void test_query_04_17_where_ColumnValue_NotEqual_VcharType()
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data(); 
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    
		    
		    Value val = new Value();
		    val.setString(sUserNick[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick",ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);

		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(6, result.getResult().size());
		    for( int i = 0; i < 6; i++)
		    {
		    	check_query_result(result.getResult().get(i), i+1);  	
		    }
	     }
		@Test
		public void test_query_04_18_where_ColumnValue_NotEqual_DateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();			
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setMicrosecond(dtCollectTime[0].getTime()); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise",ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);

		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(7, result.getResult().size());
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
			//verify data 插入的数据和查询的数据是否一致
			for( int i = 0; i < 6; i++)
			{
				check_query_result(result.getResult().get(i), i);  	
			}
		}
		@Test
		public void test_query_04_19_where_ColumnValue_NotEqual_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value obj = new Value();
		    obj.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.NE, obj);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size());
		    
		   //verify data 插入的数据和查询的数据是否一致
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }
		}
		@Test
		public void test_query_04_20_where_Column_LessThan_ValueDateTimeType()  
		{
		   
		    Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();

			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val=new Value();
			val.setSecond(dtCollectTime[6].getTime());
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.LT, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		
            result = obClient.query(collectInfoTable.getTableName(), queryInfo);
            Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(result.getResult().size(),6);
		    //查询出匹配的行数,  查出来的是Null
		   
		    for( int i = 0; i < 6; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }
		}
		@Test
		public void test_Query_04_21_Where_ColumnValueLessThanIntType() 
		{
	    /*
	    **  QueryInfo
	    **  Fitler
	    **  Column      Operator        Operand
	    **  Column is OB column
	    **  Operator is > < <> >= <= = like
	    **  Operand is Value
	    **  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
	    */
	    Result<List<RowData>> result;
	    QueryInfo queryInfo = new QueryInfo();
	    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
	    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
	
	    prepare_ob_data(); 			
	    // prepare QueryInfo
	   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
	    addColumns(queryInfo);
	
	    Value val=new Value();
		val.setNumber(lOwnerId[2]);
	
	    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.LT, val);
	    ObSimpleFilter filter = new ObSimpleFilter();
	    filter.addCondition(obSimpleCond);
	    queryInfo.setFilter(filter);
	
	    // query operate
	    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
	    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
	    Assert.assertEquals(result.getResult().size(),2);
	    for( int i = 0; i < 2; i++)
	    {
	    	check_query_result(result.getResult().get(i), i);  	
	    }
	    }
	    @Test
		public void test_query_04_22_where_ColumnValue_LessThan_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data(); 			
		    
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    //RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(0, result.getResult().size());
		}
		@Test
		public void test_query_04_23_where_ColumnValue_LessThan_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data(); 			
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    //RKey tmp_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		    Value val = new Value();
		   val.setMicrosecond(lCollecTimePrecise[1]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise",ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(3, result.getResult().size());
		    check_query_result(result.getResult().get(0), 0);
		    check_query_result(result.getResult().get(1), 5);
		    check_query_result(result.getResult().get(2), 6);
		}
		@Test 
		public void test_query_04_25_where_ColumnValue_LessThan_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value obj = new Value();
		    obj.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.LT, obj);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		   
		    Assert.assertEquals(7, result.getResult().size());
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }
		}
		@Test 
		public void test_query_04_26_where_ColumnValue_GreatThan_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data(); 
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create",ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size());
		
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }
		}
		@Test
		public void test_query_04_27_where_ColumnValue_LessThan_DateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
	
		    prepare_ob_data(); 

		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    
		    Value val = new Value();
		    val.setMicrosecond(dtCollectTime[0].getTime()); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise",ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);

		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size());
	
			//verify data 插入的数据和查询的数据是否一致
			for( int i = 0; i < 7; i++)
			{
				check_query_result(result.getResult().get(i), i);  	
			}
		}
		@Test 
		public void test_query_04_28_where_ColumnValue_LessThan_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();

		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create",ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size());
		
		    for( int i = 0; i < 6; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }
		}
	    @Test
		public void test_query_04_29_where_ColumnValue_NotEqual_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data(); 

		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size());
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }
		}
	    @Test
		public void test_query_04_30_where_ColumnValue_GreatEqual_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data(); 

		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size());
		    for( int i = 0; i < 7; i++)
		    {
		    check_query_result(result.getResult().get(i), i); //验证查询到的记录是不是我们插入的记录
	        }
		 }
	    @Test
		public void test_query_04_31_where_ColumnValue_NotEqual_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data(); 

		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[1]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size());
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i); //验证查询到的记录是不是我们插入的记录
	        }
		 }
	    @Test 
		public void test_query_04_32_where_ColumnValue_LessEqual_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();

		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified",ObSimpleCond.ObLogicOperator.LE, val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);

		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		   
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(0, result.getResult().size());
		 }   
	    @Test
		public void test_query_04_33_where_ColumnValue_LessThan_DateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setMicrosecond(dtCollectTime[0].getTime()); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create",ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(0, result.getResult().size());
		 }   
	    @Test
		public void test_query_04_34_where_ColumnValue_GreatEqual_DateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    
		    Value val = new Value();
		    val.setMicrosecond(dtCollectTime[0].getTime()); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise",ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(0, result.getResult().size());
//		    for( int i = 0; i < 7; i++)
//		    {
//		    	check_query_result(result.getResult().get(i), i); //验证查询到的记录是不是我们插入的记录
//	        }
		}
	    @Test 
		public void test_query_04_35_where_ColumnValue_LessEqual_VarcharType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);

		    Value val = new Value();
		    val.setString(sUserNick[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick",ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(4, result.getResult().size());
		    for( int i = 0; i < 3; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    } //验证查询到的记录是不是我们插入的记录
	    }
	    @Test
		public void test_query_04_36_where_ColumnValue_GreatThan_IntType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		    val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id",ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(5, result.getResult().size());
		    for( int i = 0; i < 5; i++)
		    {
		    	check_query_result(result.getResult().get(i), i+2); //验证查询到的记录是不是我们插入的记录
	        }
	    }
	    @Test
		public void test_query_04_37_where_ColumnValue_GreatThan_DateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		    val.setSecond(dtCollectTime[0].getTime()); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		  
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(6, result.getResult().size());
		    for( int i = 0; i < 6; i++)
		    {
		    	check_query_result(result.getResult().get(i), i+1); //验证查询到的记录是不是我们插入的记录
	        }
		}
	    @Test 
		public void test_query_04_38_where_ColumnValue_GreatThan_VarcharType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   val.setString(sUserNick[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick",ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(2, result.getResult().size());
		    check_query_result(result.getResult().get(0), 4);
		    check_query_result(result.getResult().get(1), 5); //验证查询到的记录是不是我们插入的记录
	        }
	    @Test
		public void test_query_04_39_where_ColumnValue_LessThan_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(0, result.getResult().size());
		 }   
	    @Test
		public void test_query_04_40_where_ColumnValue_GreatThan_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified",ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(7, result.getResult().size());
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i); //验证查询到的记录是不是我们插入的记录
	        }
		}
	    @Test
		public void test_query_04_41_where_ColumnValue_GreatEqual_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified",ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(7, result.getResult().size());
		    for( int i = 0; i < 7; i++)
		    {
		    check_query_result(result.getResult().get(i), i); //验证查询到的记录是不是我们插入的记录
	        }}
	    @Test
		public void test_query_04_42_where_ColumnValue_LessEqual_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create",ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(0, result.getResult().size());}
	    @Test
		public void test_query_04_43_where_ColumnValue_LessEqual_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified",ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(0, result.getResult().size());
		}
	    @Test
		public void test_query_04_44_where_ColumnValue_GreatEqual_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create",ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(7, result.getResult().size());
		    for( int i = 0; i < 7; i++)
		    {
		    check_query_result(result.getResult().get(i), i); //验证查询到的记录是不是我们插入的记录
	        }
		}
	    @Test
		public void test_query_04_45_where_ColumnValue_Equal_VarcharType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   val.setString(sUserNick[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick",ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(1, result.getResult().size());
	
		    check_query_result(result.getResult().get(0), 0); //验证查询到的记录是不是我们插入的记录
	        }
	    @Test
		public void test_query_04_46_where_NotEqual_ColumnValue_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(7, result.getResult().size());
		    
		   //verify data 插入的数据和查询的数据是否一致
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }
		}
	    @Test
		public void test_query_04_47_where_LessEqual_ColumnValue_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(0, result.getResult().size());
		}
	    @Test
		public void test_query_04_48_where_GreatThan_ColumnValue_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(7, result.getResult().size());
		    //verify data 插入的数据和查询的数据是否一致
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }}
	    @Test
		public void test_query_04_49_01_where_ColumnValue_GreatEqual_CreateTimeType() 
		{
			/*
			**  QueryInfo
			**  Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond( gm_createValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.GE, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			
			Assert.assertEquals(7, result.getResult().size());
		    for(int i=0;i<7;i++)
			{check_query_result(result.getResult().get(i), i);}
		}	
	    @Test
		public void test_query_04_49_02_where_ColumnValue_NotEqual_CreateTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setSecond( gm_createValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.NE, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			
			Assert.assertEquals(7, result.getResult().size());
		    for(int i=0;i<7;i++)
			{check_query_result(result.getResult().get(i), i);}
		}	
	    @Test
		public void test_query_04_49_03_where_ColumnValue_LessThan_CreateTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond( gm_createValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.LT, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
			Assert.assertEquals(0, result.getResult().size());	}
	    @Test
		public void test_query_04_49_04_where_ColumnValue_GreatThan_CreateTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setSecond( gm_createValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.GT, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode()); } 
//			Assert.assertEquals(7, result.getResult().size());
//			 for(int i=0;i<7;i++)
//				{check_query_result(result.getResult().get(i), i);}
//			}	
		
	    @Test
		public void test_query_04_49_05_where_ColumnValue_GreatThan_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.GT, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
//			Assert.assertEquals(6, result.getResult().size());
//			 for(int i=0;i<6;i++)
//				{check_query_result(result.getResult().get(i), i+1);}
		}	
	    @Test
		public void test_query_04_49_06_where_ColumnValue_LessThan_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.LT, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
			Assert.assertEquals(0, result.getResult().size());
		}
	    @Test
		public void test_query_04_49_07_where_ColumnValue_GreatEqual_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.GE, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
			Assert.assertEquals(7, result.getResult().size());
			 for(int i=0;i<7;i++)
				{check_query_result(result.getResult().get(i), i);}
		}
	    @Test
		public void test_query_04_49_08_where_ColumnValue_NotEqual_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.NE, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
			Assert.assertEquals(6, result.getResult().size());
			for(int i=0;i<6;i++){
			check_query_result(result.getResult().get(i), i+1);}
		}	
	    @Test
		public void test_query_04_49_09_where_ColumnValue_LessEqual_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.LE, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
			Assert.assertEquals(1, result.getResult().size());
			check_query_result(result.getResult().get(0), 0);
		}	
	    @Test
		public void test_query_04_49_10_where_ColumnValue_Equal_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.EQ, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
			Assert.assertEquals(1, result.getResult().size());
			check_query_result(result.getResult().get(0), 0);
		}
	    @Test
		public void test_query_04_49_11_where_ColumnValue_Equal_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setSecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.EQ, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
			Assert.assertEquals(0, result.getResult().size());
		}
	    @Test
		public void test_query_04_49_12_where_ColumnValue_GreatEqual_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setSecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.GE, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
//			Assert.assertEquals(7, result.getResult().size());
//			for(int i=0;i<7;i++){
//				check_query_result(result.getResult().get(i), i);}
			}	
	    @Test
		public void test_query_04_49_13_where_ColumnValue_LessEqual_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setSecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.LE, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		//	Assert.assertEquals(0, result.getResult().size());
		}
	    @Test
		public void test_query_04_49_14_where_ColumnValue_NotEqual_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setSecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.NE, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
			Assert.assertEquals(7, result.getResult().size());
			for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }
		}
	    @Test
		public void test_query_04_49_15_where_ColumnValue_GreatThan_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.GT, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  }
//			Assert.assertEquals(5, result.getResult().size());
//			for( int i = 0; i < 5; i++)
//		    {
//		    	check_query_result(result.getResult().get(i), i+1);  	
//		    }
			
	    @Test
		public void test_query_04_49_16_where_ColumnValue_LessThan_ModifyTimeType() 
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			**  String sColumns[] = {"user_nick", "owner_id", "collect_time", "collect_time_precise"};
			*/
			Result<List<RowData>> result;
			QueryInfo queryInfo = new QueryInfo();
			RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
			RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
			
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
			addColumns(queryInfo);
			
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.LE, val);
			ObSimpleFilter filter = new ObSimpleFilter();
			filter.addCondition(obSimpleCond);
			queryInfo.setFilter(filter);
			
			// query operate
			result = obClient.query(collectInfoTable.getTableName(), queryInfo);
			Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
//			Assert.assertEquals(2, result.getResult().size());
//			
//		    	check_query_result(result.getResult().get(0), 0);
//		    	check_query_result(result.getResult().get(1), 6);
		   
			}
	    @Test
		public void test_query_04_50_where_NotEqual_ColumnValue_PreciseDateTimeType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());  
		    Assert.assertEquals(7, result.getResult().size());
		    //verify data 插入的数据和查询的数据是否一致
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }
		    }				
	    @Test
		public void test_query_04_51_where_GreatThan_ColumnValue_NULL() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   //val.setMicrosecond(lCollecTimePrecise[0]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time",ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());  
		    Assert.assertEquals(0, result.getResult().size());
		 }
	    @Test
		public void test_query_04_54_where_ColumnValue_GreatEqual_VarcharType() 
		{
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		
		    prepare_ob_data();
		    // prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		    Value val = new Value();
		   val.setString(sUserNick[3]); 
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_nick",ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		    
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(0, result.getResult().size());
	
		    //check_query_result(result.getResult().get(0), 0); //验证查询到的记录是不是我们插入的记录
	        }
	    @Test
		public void test_query_04_55_where_GreatThan_ColumnValue_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setMicrosecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(0, result.getResult().size());  
		   }
	    @Test
		public void test_query_04_56_where_GreatThan_ColumnValue_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		  
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setMicrosecond(dtCollectTime[1].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size());  }
	    @Test
		public void test_query_04_57_where_GreatThan_ColumnValue_PreciseDateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); //2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(5, result.getResult().size()); 
		    for( int i = 0; i < 4; i++)
		    {
		    	check_query_result(result.getResult().get(i), i+1);  	
		    }}
	    @Test
		public void test_query_04_58_where_GreatThan_ColumnValue_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(0, result.getResult().size());  }
	    @Test
		public void test_query_04_59_where_GreatThan_ColumnValue_PrecisedateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size());
		    for( int i = 0; i < 7; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }  }
	    @Test
		public void test_query_04_60_where_LessEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(2, result.getResult().size());
		    for( int i = 0; i < 2; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }  }
	    @Test
		public void test_query_04_61_where_LessEqual_ColumnValue_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setSecond(dtCollectTime[3].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(4, result.getResult().size()); 
		    for( int i = 0; i < 4; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    } 
	     		
	     }
	    @Test
		public void test_query_04_62_where_LessEqual_ColumnValue_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size()); 
	      for( int i = 0; i < 7; i++)
	    	check_query_result(result.getResult().get(i), i); 
	    		
	     }
	    @Test
		public void test_query_04_63_where_LessEqual_ColumnValue_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	        
		    Value val = new Value();
		    val.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size()); 
	      for( int i = 0; i < 7; i++)
	    	check_query_result(result.getResult().get(i), i); 
	    		
	     }
	    @Test
		public void test_query_04_65_where_LessEqual_ColumnValue_dateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size()); 
	      for( int i = 0; i < 6; i++)
	    	check_query_result(result.getResult().get(i), i); 
	    }
	    @Test
		public void test_query_04_66_where_GreatEqual_ColumnValue_PreciseDateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); //2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size()); 
		    for( int i = 0; i < 6; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }}
	    @Test
		public void test_query_04_67_where_GreatEqual_ColumnValue_PreciseDateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); //2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size()); 
		    for( int i = 0; i < 6; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }}	
	    @Test
		public void test_query_04_68_where_LessEqual_ColumnValue_PreciseDateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); //2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(2, result.getResult().size()); 
		    for( int i = 0; i <1; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }}//why return only one result
	    @Test
		public void test_query_04_70_where_LessEqual_ColumnValue_PreciseDateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]); //2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 
		   }
	    @Test
		public void test_query_04_71_where_GreatEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size());
		    for( int i = 0; i < 6; i++)
		    {
		    	check_query_result(result.getResult().get(i), i);  	
		    }  }
	    @Test
		public void test_query_04_72_where_GreatEqual_ColumnValue_DateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(7, result.getResult().size()); 
	      for( int i = 0; i < 6; i++)
	    	{check_query_result(result.getResult().get(i), i); 
	    	}	
	     }
	    @Test
		public void test_query_04_73_where_GreatEqual_ColumnValue_DateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	     }
	    @Test
		public void test_query_04_74_where_GreatEqual_ColumnValue_DateTimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val = new Value();
		    val.setSecond(dtCollectTime[0].getTime());	//2001-12-01 12:00:00
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_SUCCESS, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_75_where_GreatEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_76_where_LessEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_77_where_LessThan_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_78_where_greatThan_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_79_where_Notequal_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_80_where_Equal_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_modified", ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    } 
	    @Test
		public void test_query_04_81_where_LessEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }   
	    @Test
		public void test_query_04_82_where_GreatThan_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }   
	    @Test
		public void test_query_04_83_where_LessThan_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }   
	    @Test
		public void test_query_04_84_where_Equal_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }   
	    @Test
		public void test_query_04_85_where_NotEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("gm_create", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }   
	    @Test
		public void test_query_04_86_where_GreatEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    @Test
		public void test_query_04_87_where_LessEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    @Test
		public void test_query_04_88_where_LessThan_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    @Test
		public void test_query_04_89_where_GreatThan_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    @Test
		public void test_query_04_90_where_NotEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    @Test
		public void test_query_04_91_where_Equal_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    @Test
		public void test_query_04_92_where_GreatEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    } 
	    @Test
		public void test_query_04_93_where_LessEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    } 
	    @Test
		public void test_query_04_94_where_LessThan_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    } 
	    @Test
		public void test_query_04_95_where_GreatThan_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    } 
	    @Test
		public void test_query_04_96_where_Equal_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }      
	    @Test
		public void test_query_04_97_where_NotEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_98_where_NotEqual_ColumnValue_IntType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
			val.setNumber(lOwnerId[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time_precise", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_99_where_NotEqual_ColumnValue_vcharType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
		    val.setString(sUserNick[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_100_where_Equal_ColumnValue_vcharType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
		    val.setString(sUserNick[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_101_where_GreatEqual_ColumnValue_vcharType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
		    val.setString(sUserNick[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_102_where_LessEqual_ColumnValue_vcharType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
		    val.setString(sUserNick[0]);
		    ObSimpleCond obSimpleCond = new ObSimpleCond("collect_time", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_103_where_Equal_ColumnValue_precisedatetimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]);//查询的是第1个数据20001
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_104_where_NOTEqual_ColumnValue_precisedatetimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]);//查询的是第1个数据20001
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    } 
	    @Test
		public void test_query_04_105_where_GreatEqual_ColumnValue_precisedatetimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]);//查询的是第1个数据20001
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    } 
	    @Test
		public void test_query_04_106_where_LESSEqual_ColumnValue_precisedatetimeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
	
		    Value val=new Value();
		    val.setMicrosecond(lCollecTimePrecise[0]);//查询的是第1个数据20001
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    } 
	    @Test
		public void test_query_04_107_where_LESSEqual_ColumnValue_modify_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }  
	    @Test
		public void test_query_04_108_where_NotEqual_ColumnValue_modify_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }  
	    @Test
		public void test_query_04_109_where_GreatThan_ColumnValue_modify_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }   
	    @Test
		public void test_query_04_110_where_LessThan_ColumnValue_modify_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    @Test
		public void test_query_04_111_where_GreatThan_ColumnValue_modify_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    @Test
		public void test_query_04_112_where_GreatEqual_ColumnValue_modify_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    public void test_query_04_113_where_LessThan_ColumnValue_gm_modifyType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_114_where_LessEqual_ColumnValue_modify_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_modified");
		    Long gm_modifiedValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_modified");
		    val.setMicrosecond( gm_modifiedValue );
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }
	    @Test
		public void test_query_04_115_where_LessEqual_ColumnValue_create_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond(gm_createValue);
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.LE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    } 
	    @Test
		public void test_query_04_116_where_LessThan_ColumnValue_create_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond(gm_createValue);
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    } 
	    @Test
		public void test_query_04_117_where_GreatThan_ColumnValue_create_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond(gm_createValue);
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }  
	    @Test
		public void test_query_04_118_where_GreatEqual_ColumnValue_create_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond(gm_createValue);
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("owner_id", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }  
	    @Test
		public void test_query_04_119_where_GreatEqual_ColumnValue_create_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond(gm_createValue);
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.GE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }   
	    @Test
		public void test_query_04_120_where_GreatThan_ColumnValue_create_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond(gm_createValue);
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.GT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    @Test
		public void test_query_04_121_where_LessThan_ColumnValue_create_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond(gm_createValue);
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.LT,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }     
	    @Test
		public void test_query_04_122_where_Equal_ColumnValue_create_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond(gm_createValue);
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.EQ,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }    
	    @Test
		public void test_query_04_123_where_NotEqual_ColumnValue_create_timeType()
		{
			/*
			**  QueryInfo
			**   Filter
			**  Column      Operator        Operand
			**  Column is OB column
			**  Operator is > < <> >= <= = like
			**  Operand is Value
			*/
			Result<List<RowData>> result;
		    QueryInfo queryInfo = new QueryInfo();
		    RKey start_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		    RKey end_rowkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 7));
		   
		    prepare_ob_data();//prepare date
		    
			// prepare QueryInfo
		   	prepareInfo(queryInfo, start_rowkey, end_rowkey);
		    addColumns(queryInfo);
		
		    Value val = new Value();
		    RKey rkey = new RKey(new CollectInfoKeyRule(2, (byte) '0', 1));
		   
		    Set<String> columns = new HashSet<String>();
		    columns.add("gm_create");
		    Long gm_createValue = obClient.get(collectInfoTable.getTableName(), rkey, columns).getResult().get("gm_create");
		    val.setMicrosecond(gm_createValue);
		    
		    ObSimpleCond obSimpleCond = new ObSimpleCond("user_nick", ObSimpleCond.ObLogicOperator.NE,val);
		    ObSimpleFilter filter = new ObSimpleFilter();
		    filter.addCondition(obSimpleCond);
		    queryInfo.setFilter(filter);
		   
		    // query operate
		    result = obClient.query(collectInfoTable.getTableName(), queryInfo);
		    Assert.assertEquals(ResultCode.OB_INVALID_ARGUMENT, result.getCode());
		    Assert.assertEquals(0, result.getResult().size()); 	
	    }     
	    }
