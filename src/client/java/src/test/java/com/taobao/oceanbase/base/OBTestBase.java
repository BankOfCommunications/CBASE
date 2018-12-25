package com.taobao.oceanbase.base;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.Assert;
import com.taobao.oceanbase.ClientImpl;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Rowkey;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.Value;

import junit.framework.TestCase;

/**
 * @author baoni
 * 
 */
public class OBTestBase extends TestCase {
//	final ApplicationContext beanFactory = new ClassPathXmlApplicationContext("server.xml");
//	public final AppGrid gid = (AppGrid) beanFactory.getBean("obGrid");
	public static ClientImpl client;// a new client
	ExpectResult er = new ExpectResult();;
	protected static Logger log = Logger.getLogger("OBTest");
	private static long DUP_ROW_NUM=6199; 
//	public final AppServer addServer1 = (AppServer) beanFactory.getBean("addServer1");
//	public final AppServer addServer2 = (AppServer) beanFactory.getBean("addServer2");
	public static String rsip = "10.232.36.35";
	static{
		client = new ClientImpl();
		client.setIp(rsip);
		client.setPort(2600);
		client.setTimeout(100);
		client.init();
		
	}
	public boolean update(String table, RowKey rowkey, Set<CollectColumnEnum> modifyColumns) {

		UpdateMutator rowMutator = new UpdateMutator(table, rowkey);
		Iterator<CollectColumnEnum> mColumns = modifyColumns.iterator();
		while (mColumns.hasNext()) {
			CollectColumnEnum column = mColumns.next();
			Value value = new Value();
			boolean add = true;
			if (column.getType().equals(ColumnTypeEnum.STRING.getType())) {
				value.setString(ModifyTypeEnum.UPDATE.getValue());
				add = false;
			} else if (column.getType().equals(ColumnTypeEnum.DOUBLE.getType()))
				value.setDouble(1.0);
			else if (column.getType().equals(ColumnTypeEnum.FLOAT.getType()))
				value.setFloat(1);
			else if (column.getType().equals(ColumnTypeEnum.LONG.getType()))
				value.setNumber(1);
			else if (column.getType().equals(ColumnTypeEnum.PRECISETIME.getType()))
				value.setMicrosecond(1);
			else if (column.getType().equals(ColumnTypeEnum.TIME.getType()))
				value.setSecond(1);
			rowMutator.addColumn(column.getValue(), value, add);
		}
		Result<Boolean> result = client.update(rowMutator);
		if (!result.getResult()) {
			return false;
		}

		return true;
	}
	public boolean dupUpdate(String table, RowKey start, RowKey end, Set<CollectColumnEnum> modifyColumns) {

		for (long i=start.get_uid(); i<end.get_uid(); i++) {
			for (long j = start.get_tid(); j <= DUP_ROW_NUM; j++) {
				RowKey rowkey = RowKeyFactory.getRowkey(i, j);
				boolean ret = update(table, rowkey, modifyColumns);
				if(!ret)
					return false;
			}
		}
		return true;
	}

	public boolean insert(String table, RowKey rowkey, Set<CollectColumnEnum> modifyColumns) {
		InsertMutator rowMutator = new InsertMutator(table, rowkey);
		Iterator<CollectColumnEnum> mColumns = modifyColumns.iterator();
		while (mColumns.hasNext()) {
			CollectColumnEnum column = mColumns.next();
			Value value = new Value();
			if (column.getType().equals(ColumnTypeEnum.STRING.getType()))
				value.setString(ModifyTypeEnum.INSERT.getValue());
			else if (column.getType().equals(ColumnTypeEnum.DOUBLE.getType()))
				value.setDouble(1.0);
			else if (column.getType().equals(ColumnTypeEnum.FLOAT.getType()))
				value.setFloat(1);
			else if (column.getType().equals(ColumnTypeEnum.LONG.getType()))
				value.setNumber(1);
			else if (column.getType().equals(ColumnTypeEnum.PRECISETIME.getType()))
				value.setMicrosecond(1);
			else if (column.getType().equals(ColumnTypeEnum.TIME.getType()))
				value.setSecond(1);
			rowMutator.addColumn(column.getValue(), value);
		}
		Result<Boolean> result = client.insert(rowMutator);
		if (!result.getResult()) {
			return false;

		}
		return true;

	}
	public boolean dupInsert(String table, RowKey start, RowKey end, Set<CollectColumnEnum> modifyColumns) {
		for (long i = start.get_uid(); i < end.get_uid(); i++) {
			for (long j = start.get_tid(); j <= DUP_ROW_NUM; j++) {
				RowKey rowkey = RowKeyFactory.getRowkey(i, j);
				boolean ret = insert(table, rowkey, modifyColumns);
				if(!ret)
					return false;
			}
		}
		return true;

	}

	public void delete(String table, RowKey rowkey) {
		client.delete(table, rowkey);
		Set<String> columns = getAllColumns();
		Result<RowData> getResult = client.get(table, rowkey, columns);
		getVerify(rowkey, ModifyTypeEnum.DELETE, columns, columns, getResult);

	}
	public void dupDelete(String table, RowKey start, RowKey end) {
		for (long i = start.get_uid(); i < end.get_uid(); i++) {
			for (long j = start.get_tid(); j <= DUP_ROW_NUM; j++) {
				RowKey rowkey = RowKeyFactory.getRowkey(i, j);
				delete(table,rowkey);
			}
		}

	}

	public void getVerify(RowKey rowkey, ModifyTypeEnum modify, Set<String> modifyColumns, Set<String> Columns, Result<RowData> getResult) {
		boolean ret = false;
			if (modify == ModifyTypeEnum.DELETE) {
				Assert.assertEquals(null, getResult.getResult());
				ret = true;
			} else {
				Assert.assertFalse(null == getResult.getResult());
				verify(rowkey, modify, modifyColumns, Columns, getResult.getResult());
				ret = true;
			}
		
	}
	public void verify(RowKey rowkey,ModifyTypeEnum modify, Set<String> modifyColumns, Set<String> Columns, RowData rowData){
		Iterator<String> collumns = Columns.iterator();
		while (collumns.hasNext()) {
			String column_name = collumns.next();
			Object result = rowData.get(column_name);
			if (modify == ModifyTypeEnum.NULL)
				Assert.assertEquals(er.get_value(rowkey, column_name), result);
			else {
				if (result instanceof String)
					Assert.assertEquals(modify.getValue(), result);
				else if (result instanceof Double)
					if (modifyColumns.contains(column_name) && modify.equals(ModifyTypeEnum.UPDATE))
						Assert.assertEquals((Double) er.get_value(rowkey, column_name) + 1, result);
					else if (modifyColumns.contains(column_name) && modify.equals(ModifyTypeEnum.INSERT))
						Assert.assertEquals(1.0, result);
					else if (result instanceof Float)
						if (modifyColumns.contains(column_name) && modify.equals(ModifyTypeEnum.UPDATE))
							Assert.assertEquals((Float) er.get_value(rowkey, column_name) + 1, result);
						else if (modifyColumns.contains(column_name) && modify.equals(ModifyTypeEnum.INSERT))
							Assert.assertEquals((float) 1.0, result);
						else if (result instanceof Long)
							if (modifyColumns.contains(column_name) && modify.equals(ModifyTypeEnum.UPDATE))
								Assert.assertEquals((Long) er.get_value(rowkey, column_name) + 1, ((Long) result).longValue());
							else if (modifyColumns.contains(column_name) && modify.equals(ModifyTypeEnum.INSERT))
								Assert.assertEquals(1, ((Long) result).longValue());
							else
								Assert.fail("the data is error");
			}

		}
	}
	public void dupGetVerfify(RowKey start, RowKey end, ModifyTypeEnum modify, Set<String> modifyColumns) {
		for (long i = start.get_uid(); i < end.get_uid(); i++) {
			for (long j = start.get_tid(); j <= DUP_ROW_NUM; j++) {
				RowKey rowkey = RowKeyFactory.getRowkey(i, j);
				String table = "collect_info";
				Set<String> columns = getAllColumns();
				Result<RowData> getResult = client.get(table, rowkey, columns);
				getVerify(rowkey, modify, modifyColumns, columns, getResult);
			}
		}
	}
	public void queryVerify(RowKey start, RowKey end, ModifyTypeEnum modify, Set<String> modifyColumns, Set<String> Columns, Result<List<RowData>> queryResult, String groupBy) {
		if (modify == ModifyTypeEnum.DELETE) {
			Assert.assertEquals(0, queryResult.getResult().size());
		} else {
			List<RowData> rowDatas = queryResult.getResult();
			Assert.assertFalse(0 == queryResult.getResult().size());
			for (int i = 0; i < rowDatas.size(); i++) {
				RowData rowData = rowDatas.get(i);
				Rowkey key = rowData.getRowKey();
				RowKey rkey = new RowKey(key.getRowkey());
				Assert.assertTrue(Integer.parseInt(rkey.toString().substring(1, 8)) - Integer.parseInt(end.toString().substring(1, 8)) <= 0 && Integer.parseInt(rkey.toString().substring(1, 8)) - Integer.parseInt(start.toString().substring(1, 8)) >= 0);
				verify(rkey, modify, modifyColumns, Columns, rowData);
			}
		}
	}

	public Set<String> getAllColumns() {
		Set<String> columns = new HashSet<String>();
		columns.add(CollectColumnEnum.USER_NICK.getValue());
		columns.add(CollectColumnEnum.IS_SHARED.getValue());
		columns.add(CollectColumnEnum.NOTE.getValue());
		columns.add(CollectColumnEnum.COLLECT_TIME.getValue());
		columns.add(CollectColumnEnum.STATUS.getValue());
		columns.add(CollectColumnEnum.GM_CREATE.getValue());
		columns.add(CollectColumnEnum.GM_MODIFIED.getValue());
		columns.add(CollectColumnEnum.TAG.getValue());
		columns.add(CollectColumnEnum.CATEGORY.getValue());
		columns.add(CollectColumnEnum.COLLECT_TIME_PRECISE.getValue());
		columns.add(CollectColumnEnum.TITLE.getValue());
		columns.add(CollectColumnEnum.PICURL.getValue());
		columns.add(CollectColumnEnum.OWNER_ID.getValue());
		columns.add(CollectColumnEnum.OWNER_NICK.getValue());
		columns.add(CollectColumnEnum.PRICE.getValue());
		columns.add(CollectColumnEnum.ENDS.getValue());
		columns.add(CollectColumnEnum.PROPER_XML.getValue());
		columns.add(CollectColumnEnum.COMMENT_COUNT.getValue());
		columns.add(CollectColumnEnum.COLLECTOR_COUNT.getValue());
		columns.add(CollectColumnEnum.COLLECT_COUNT.getValue());
		columns.add(CollectColumnEnum.TOTAL_MONEY.getValue());
		return columns;
	}
	public Set<String> getCollectModColumns() {
		Set<String> columns = new HashSet<String>();
		columns.add(CollectColumnEnum.USER_NICK.getValue());
		columns.add(CollectColumnEnum.IS_SHARED.getValue());
		columns.add(CollectColumnEnum.NOTE.getValue());
		columns.add(CollectColumnEnum.COLLECT_TIME.getValue());
		columns.add(CollectColumnEnum.STATUS.getValue());
		columns.add(CollectColumnEnum.GM_CREATE.getValue());
		columns.add(CollectColumnEnum.GM_MODIFIED.getValue());
		columns.add(CollectColumnEnum.TAG.getValue());
		columns.add(CollectColumnEnum.CATEGORY.getValue());
		columns.add(CollectColumnEnum.COLLECT_TIME_PRECISE.getValue());
		
		return columns;
	}
	public Set<CollectColumnEnum> getCollectModColumnsWithType() {
		Set<CollectColumnEnum> columns = new HashSet<CollectColumnEnum>();
		columns.add(CollectColumnEnum.USER_NICK);
		columns.add(CollectColumnEnum.IS_SHARED);
		columns.add(CollectColumnEnum.NOTE);
		columns.add(CollectColumnEnum.COLLECT_TIME);
		columns.add(CollectColumnEnum.STATUS);
		columns.add(CollectColumnEnum.GM_CREATE);
		columns.add(CollectColumnEnum.GM_MODIFIED);
		columns.add(CollectColumnEnum.TAG);
		columns.add(CollectColumnEnum.CATEGORY);
		columns.add(CollectColumnEnum.COLLECT_TIME_PRECISE);
		
		return columns;
	}
	public Set<String> getItemModColumns() {
		Set<String> columns = new HashSet<String>();
		columns.add(CollectColumnEnum.TITLE.getValue());
		columns.add(CollectColumnEnum.PICURL.getValue());
		columns.add(CollectColumnEnum.OWNER_ID.getValue());
		columns.add(CollectColumnEnum.OWNER_NICK.getValue());
		columns.add(CollectColumnEnum.PRICE.getValue());
		columns.add(CollectColumnEnum.ENDS.getValue());
		columns.add(CollectColumnEnum.PROPER_XML.getValue());
		columns.add(CollectColumnEnum.COMMENT_COUNT.getValue());
		columns.add(CollectColumnEnum.COLLECTOR_COUNT.getValue());
		columns.add(CollectColumnEnum.COLLECT_COUNT.getValue());
		columns.add(CollectColumnEnum.MONEY.getValue());

		return columns;
	}
	
	public Set<CollectColumnEnum> getItemModColumnsWithType() {
		Set<CollectColumnEnum> columns = new HashSet<CollectColumnEnum>();
		columns.add(CollectColumnEnum.TITLE);
		//columns.add(CollectColumnEnum.PICRUL);
		columns.add(CollectColumnEnum.OWNER_ID);
		columns.add(CollectColumnEnum.OWNER_NICK);
		columns.add(CollectColumnEnum.PRICE);
		columns.add(CollectColumnEnum.ENDS);
		columns.add(CollectColumnEnum.PROPER_XML);
		columns.add(CollectColumnEnum.COMMENT_COUNT);
		columns.add(CollectColumnEnum.COLLECTOR_COUNT);
		columns.add(CollectColumnEnum.COLLECT_COUNT);
		columns.add(CollectColumnEnum.MONEY);

		return columns;
	}
	public Set<CollectColumnEnum> getAllColumnsWithType() {
		Set<CollectColumnEnum> columns = new HashSet<CollectColumnEnum>();
		columns.add(CollectColumnEnum.CATEGORY);
		columns.add(CollectColumnEnum.COLLECT_COUNT);
		columns.add(CollectColumnEnum.COLLECT_TIME);
		columns.add(CollectColumnEnum.COLLECT_TIME_PRECISE);
		columns.add(CollectColumnEnum.COLLECTOR_COUNT);
		columns.add(CollectColumnEnum.COMMENT_COUNT);
		columns.add(CollectColumnEnum.ENDS);
		columns.add(CollectColumnEnum.GM_CREATE);
		columns.add(CollectColumnEnum.GM_MODIFIED);
		columns.add(CollectColumnEnum.IS_SHARED);
		columns.add(CollectColumnEnum.NOTE);
		columns.add(CollectColumnEnum.OWNER_ID);
		columns.add(CollectColumnEnum.OWNER_NICK);
		columns.add(CollectColumnEnum.PICURL);
		columns.add(CollectColumnEnum.PRICE);
		columns.add(CollectColumnEnum.PROPER_XML);
		columns.add(CollectColumnEnum.STATUS);
		columns.add(CollectColumnEnum.TAG);
		columns.add(CollectColumnEnum.TITLE);
		columns.add(CollectColumnEnum.TOTAL_MONEY);
		columns.add(CollectColumnEnum.USER_NICK);
		return columns;
	}

	public void queryAllColumns(QueryInfo query) {
		query.addColumn(CollectColumnEnum.CATEGORY.getValue());
		query.addColumn(CollectColumnEnum.COLLECT_COUNT.getValue());
		query.addColumn(CollectColumnEnum.COLLECT_TIME.getValue());
		query.addColumn(CollectColumnEnum.COLLECT_TIME_PRECISE.getValue());
		query.addColumn(CollectColumnEnum.COLLECTOR_COUNT.getValue());
		query.addColumn(CollectColumnEnum.COMMENT_COUNT.getValue());
		query.addColumn(CollectColumnEnum.ENDS.getValue());
		query.addColumn(CollectColumnEnum.GM_CREATE.getValue());
		query.addColumn(CollectColumnEnum.GM_MODIFIED.getValue());
		query.addColumn(CollectColumnEnum.IS_SHARED.getValue());
		query.addColumn(CollectColumnEnum.NOTE.getValue());
		query.addColumn(CollectColumnEnum.OWNER_ID.getValue());
		query.addColumn(CollectColumnEnum.OWNER_NICK.getValue());
		query.addColumn(CollectColumnEnum.PICURL.getValue());
		query.addColumn(CollectColumnEnum.PRICE.getValue());
		query.addColumn(CollectColumnEnum.PROPER_XML.getValue());
		query.addColumn(CollectColumnEnum.STATUS.getValue());
		query.addColumn(CollectColumnEnum.TAG.getValue());
		query.addColumn(CollectColumnEnum.TITLE.getValue());
		query.addColumn(CollectColumnEnum.TOTAL_MONEY.getValue());
		query.addColumn(CollectColumnEnum.USER_NICK.getValue());
	}

//	public boolean cleandata() {
//		HelpProc help = new HelpProc();
//		boolean ret = help.proStartBase("10.232.36.29", "./ob_deploy.sh -g c2 dstable", "/home/admin/oceanbase/servers");
//		return ret;
//	}
//	public boolean preparedata() {
//		HelpProc help = new HelpProc();
//		boolean ret = help.proStartBase("10.232.36.29", "./ob_deploy.sh -g c2 pdata", "/home/admin/oceanbase/servers");
//		return ret;
//	}
//
//	public void waitto(int sec) {
//		log.debug("wait for " + sec + "s");
//		try {
//			Thread.sleep(sec * 1000);
//		} catch (Exception e) {
//		}
//	}
//	public void migrate(String rsIp,String csIp){
//		HelpConf helpconf = new HelpConf();
//		int disk = helpconf.check_disk(csIp);
//		helpconf.confReplaceSingle(rsIp, "/home/admin/oceanbase/servers/root/rootserver.conf", "disk_trigger_level", disk/2+"");
//		HelpProc helpProc = new HelpProc();
//		AppServer rs = gid.getCluster("rootServerCluster").getServer(0);
//		helpProc.proStartBase(rsIp, "./root_admin "+rs.getPid()+" reload_config","/home/admin/oceanbase/servers/root/");
//		waitto(120);
//	}
//	public void chMigrate(String rsIp){
//		HelpConf helpconf = new HelpConf();
//		helpconf.confReplaceSingle(rsIp, "/home/admin/oceanbase/servers/root/rootserver.conf", "disk_trigger_level", 75+"");
//		
//	}
//	public boolean checkDone(String rsIp,LogEnum log,int exceptNums,int waittime){
//		int i =0;
//		boolean ret =false;
//		while(i<waittime/10){
//			int num = getLogNum(rsIp,log);
//			if(exceptNums==num){
//				ret = true;
//				break;
//			}else{
//			waitto(10);
//			}
//		}
//		return ret;
//	}
//	public int getLogNum(String rsIp,LogEnum log){
//		HelpConf helpconf = new HelpConf();
//		int num = 0;
//		num =6;//todo get the key num
//		return num;
//	}
}
