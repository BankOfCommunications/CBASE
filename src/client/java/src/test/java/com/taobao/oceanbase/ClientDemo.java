package com.taobao.oceanbase;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.management.Query;

import junit.framework.Assert;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.rule.CollectInfoKeyRule;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.util.Helper;
import com.taobao.oceanbase.vo.GetParam;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.ObMutatorBase;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Rowkey;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.Value;
import com.taobao.oceanbase.vo.inner.ObSimpleCond;
import com.taobao.oceanbase.vo.inner.ObSimpleCond.ObLogicOperator;
import com.taobao.oceanbase.vo.inner.ObSimpleFilter;

class InfoRowkey extends Rowkey {
	public InfoRowkey(long userid,byte item_type,long item_id) {
	    byteBuffer.putLong(userid);
	    byteBuffer.put(item_type);
	    byteBuffer.putLong(item_id);
	    
	}
	
    final ByteBuffer byteBuffer = ByteBuffer.allocate(17);
    
    public byte[] getBytes() {
    	return byteBuffer.array(); 
    }
};

class ItemRowkey extends Rowkey {
	public ItemRowkey(byte item_type,long item_id) {
	    byteBuffer.put(item_type);
	    byteBuffer.putLong(item_id);
	}
	
    final ByteBuffer byteBuffer = ByteBuffer.allocate(9);
    
    public byte[] getBytes() {
    	return byteBuffer.array(); 
    }
}

class DcRowkey extends Rowkey {
	public DcRowkey(long userid,long item_id) {
	    byteBuffer.putLong(userid);
	    byteBuffer.putLong(item_id);
	}
	
    final ByteBuffer byteBuffer = ByteBuffer.allocate(16);
    
    public byte[] getBytes() {
    	return byteBuffer.array(); 
    }
}

class footRowkey extends Rowkey {
	public footRowkey(String key) {
		rowkey=key;
	}
	public byte[] getBytes() {
		try {
			return rowkey.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private String rowkey;
}


public class ClientDemo {
	
	private ClientImpl client;
	
	void setUp(List<String> instance_list) {
		client = new ClientImpl();
		client.setInstanceList(instance_list);
		client.setTimeout(2000);
		client.setReadConsistency(false);
		client.init();
	}
	void setUp(String ip,int port){
		client = new ClientImpl();
		client.setIp(ip);
		client.setPort(port);
		client.setTimeout(2000);
		client.init();
	}
	
	final private static String INFO_TABLE_NAME="collect_info";
	final private static String USER_NICK="info_user_nick";
	final private static String INFO_STATUS="info_status";
	final private static String ITEM_OWNER_NICK="item_owner_nick";
	
	/**
	 * for OB semantics , update & insert is identical
	 * DB semantics is not support yet.
	 */
	void UpdateDemo() {
		List<ObMutatorBase> mutatorList = new ArrayList<ObMutatorBase>();
        for (Long i = 0L; i < 100; i++) {
            UpdateMutator mutator = new UpdateMutator(INFO_TABLE_NAME, new InfoRowkey(i, (byte)0, i));
            mutator.addColumn(USER_NICK, new  Value() {{ setString("YOUR_VALUE"); }},false);
            mutator.addColumn(INFO_STATUS, new Value() {{setNumber(1L);}},false);
            mutatorList.add(mutator);
        }
        
        Result<Boolean> ret = client.update(mutatorList);
        if (ret == null || !ret.getResult()){
        	System.err.println("update failed");
        }
	}
	
	void InsertDemo() {
		
        for (Long i = 0L; i < 20000; i++) {
            InsertMutator insertMutator = new InsertMutator(INFO_TABLE_NAME, new InfoRowkey(i, (byte)0, i));
            insertMutator.addColumn(USER_NICK, new  Value() {{ setString("YOUR_VALUE"); }});
            insertMutator.addColumn(INFO_STATUS, new Value() {{setNumber(1L);}});

            Result<Boolean> ret = client.insert(insertMutator);
            if (ret == null || !ret.getResult()){
            	System.err.println("update failed " + ret.getCode());
            }
        }

	}
	
	void queryDemo() {
		QueryInfo query_info = new QueryInfo();
		query_info.setStartKey(new InfoRowkey(0L, (byte)0,0L));
		query_info.setEndKey(new InfoRowkey(100L, (byte)(0), 100L));
		query_info.addColumn(USER_NICK);
		query_info.addColumn(INFO_STATUS);
		//Result<List<RowData>> result = client.query(INFO_TABLE_NAME, query_info);
		Result<List<RowData>> result = client.query("", query_info);
		System.out.println("get " + result.getResult().size() + "items");
	}
	
	void queryDemoWhere() {
		QueryInfo query_info = new QueryInfo();
		query_info.setStartKey(new InfoRowkey(0L, (byte)0,0L));
		query_info.setEndKey(new InfoRowkey(100L, (byte)(0), 100L));
		query_info.addColumn(USER_NICK);
		query_info.addColumn(INFO_STATUS);
		
		ObSimpleFilter filter = new ObSimpleFilter();
		filter.addCondition(new ObSimpleCond(INFO_STATUS,ObSimpleCond.ObLogicOperator.EQ,new Value() {{setNumber(0L);}}));
		query_info.setFilter(filter);
		Result<List<RowData>> result = client.query(INFO_TABLE_NAME, query_info);
		System.out.println("get" + result.getResult().size() + "items");
	}
	
	void queryDemoOrderby() {
		QueryInfo query_info = new QueryInfo();
		query_info.setStartKey(new InfoRowkey(0L, (byte)0,0L));
		query_info.setEndKey(new InfoRowkey(100L, (byte)(0), 100L));
		//query_info.addColumn(USER_NICK);
		//query_info.addColumn(INFO_STATUS);
		query_info.addColumn(Const.ASTERISK);
		
		query_info.addOrderBy(INFO_STATUS, true); //order: true -ASC false - DESC
		Result<List<RowData>> result = client.query(INFO_TABLE_NAME, query_info);
		System.out.println("get" + result.getResult().size() + "items");
		
	}
	
	
	void getDemo() {
		Set<String> columns = new HashSet<String>();
		//columns.add(USER_NICK);
		//columns.add(INFO_STATUS);
		//columns.add(Const.ASTERISK);
		for(int i=0; i<1; ++i){
			Result<RowData> ret = client.get(INFO_TABLE_NAME,new InfoRowkey(34457L, (byte)0, 45379752L),columns);
			if (ret == null ){
				System.err.println("get failed ");
			}else {
				System.err.println("get " + ret.getResult().get(USER_NICK));
				ByteBuffer byteBuffer = ByteBuffer.wrap(ret.getResult().getRowKey().getBytes());
				System.err.println(byteBuffer.getLong());
				System.err.println(byteBuffer.get());
				System.err.println(byteBuffer.getLong());
			}
		}
	}
	
	void countDemo() {
		QueryInfo query_info = new QueryInfo();
		query_info.setStartKey(new InfoRowkey(0L, (byte)0,0L));
		query_info.setEndKey(new InfoRowkey(100L, (byte)(0), 100L));
		query_info.addColumn(INFO_STATUS);
		Result<Long> ret = client.count(INFO_TABLE_NAME,query_info);
		
		System.err.println(ret.getResult());
	}
	
	void mgetDemo() {
		List<GetParam> get_list = new ArrayList<GetParam>();
		//long [] id_list = {62369091L,62027487L,35275985L,62765988L,60688834L,63638663L,34739895L,58498157L,12464856403L,59794410L};
		long [] id_list = {1324361235L,13243617L,13243618L,13243619L,13243620L,1324323621L,13243622L,13243616L};
		for (long id : id_list) {
			GetParam param = new GetParam();
			param.setRowkey(new ItemRowkey((byte) 0, id));
			param.addColumn("item_title");
			param.setTableName("collect_item");
			get_list.add(param);
		}
		
		Result<List<RowData>> ret = client.get(get_list);
		if (ret.getCode() != ResultCode.OB_SUCCESS) {
			System.err.println("get failed");
		}else {
			for (RowData row : ret.getResult()) {
				System.err.println(row);
			}
		}
	}
	

	
	void collect_demo() {
	
	      QueryInfo query = new QueryInfo();
	      query.setInclusiveStart(true);
	      query.setInclusiveEnd(false);
	      query.setPageSize(2);
	      query.setPageNum(2);
	      query.setStartKey(new footRowkey("10_c"));
	      query.setEndKey(new footRowkey("10_z"));
	      
	      ObSimpleFilter STATUS_FILTER = new ObSimpleFilter();

	      STATUS_FILTER.addCondition(new ObSimpleCond("status",
	    		  ObLogicOperator.EQ, new Value() {
	    		  {
	    		  setNumber(0);
	    		  }
	    		  }));

	      query.addColumn("pid").addColumn("gmt_modified")
	            .addColumn("status").setFilter(STATUS_FILTER);
	      
	      query.addOrderBy("gmt_modified", false);
	      
//	      Result<Long> ret = client.count("footprint", query);
//	      System.err.println(ret.getResult());
//	      
	      Result<List<RowData>> r = client.query("footprint", query);
	      if (r.getCode() == ResultCode.OB_SUCCESS) {
	    	  System.err.println(r.getResult().size());
	    	  for(RowData data : r.getResult()) {
	    		  System.err.println(data);
	    	  }
	      }

//		query_info.setStartKey(new DcRowkey(0L, 0L));
//		query_info.setEndKey(new DcRowkey(100L, 100L));
//		query_info.addColumn("shop_id");
//		query_info.addColumn("site_id");
//		Set<String> columns = new HashSet<String>();
//		Result<RowData> ret = client.get("dc_user_sites",new DcRowkey(182589295L,78590L),columns);
	//	Result<List<RowData>> result = client.query("dc_user_sites", query_info);
		//Result<List<RowData>> result = client.query("", query_info);
		//System.out.println("get " + result.getResult().size() + "items");

	}
	
	public static void main(String args []) {
		List<String> instance_list = new ArrayList<String>();
		//instance_list.add("10.232.36.33:20100");
		//instance_list.add("10.232.36.34:20110");
		//instance_list.add("10.232.36.36:11010");
		//instance_list.add("10.232.36.37:11010");

		ClientDemo demo = new ClientDemo();
		
		//demo.setUp("10.232.36.43",20240);
		//demo.setUp("10.232.23.12",2600);
		demo.setUp("10.232.36.192",2500);
		//demo.setUp("10.232.19.96",2500);
		//demo.setUp(instance_list);
		//demo.UpdateDemo();
		//demo.InsertDemo();
		//demo.getDemo();
		//demo.queryDemo();
		//demo.queryDemoWhere();
		//demo.queryDemoOrderby();
		//demo.mgetDemo();
		demo.collect_demo();
		System.exit(0);
	}

}
