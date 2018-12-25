/**
 * @author dongpo
 */
package com.taobao.oceanbase.base;

import java.util.ArrayList;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.base.table.ITable;
import com.taobao.oceanbase.Client;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.UpdateMutator;

public class BaseCase {
	//spring container
	protected final static ApplicationContext beanFactory = new ClassPathXmlApplicationContext(new String[]{"javaclient/tables.xml","javaclient/others.xml"});
	
	//java client
	protected static Client		obClient=(Client)beanFactory.getBean("obClient");
	
	//tables
	protected ITable		collectInfoTable=(ITable)beanFactory.getBean("collectInfoTable");
	protected ITable		itemInfoTable=(ITable)beanFactory.getBean("itemInfoTable");
	
	//logger
	protected static Logger log=Logger.getLogger("basecase");
	
	public void getAndVerify(String tabName,RKey rkey,Set<String> colNames){
		Result<RowData> rs=obClient.get(tabName, rkey, colNames);
		for(String str:colNames){
			Assert.assertEquals(str,collectInfoTable.getColumnValue(str, rkey), rs.getResult().get(str));
		}
	}
	
	public void insertAndVerify(String tabName,RKey rkey,Set<String>colNames){
		InsertMutator im=new InsertMutator(tabName, rkey);
		for (String str : colNames) {
			im.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, 0, false));
		}
		Result<Boolean>rs=obClient.insert(im);
		Assert.assertTrue("Insert fail!",rs.getResult());
		Result<RowData> rd=obClient.get(tabName, rkey, colNames);
		for(String str:colNames){
			Assert.assertEquals(str,collectInfoTable.genColumnUpdateResult(str, rkey, 0, false).getObject(false).getValue(), rd.getResult().get(str));
		}
	}
	
	public void updateAndVerify(String tabName,RKey rkey,Set<String>colNames,int times){
		UpdateMutator um=new UpdateMutator(tabName,rkey);
		for (String str : colNames) {
			um.addColumn(str, collectInfoTable.genColumnUpdateResult(str, rkey, times, false), false);
		}
		Result<Boolean> rs=obClient.update(um);
		Assert.assertTrue("update fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, colNames);
		for (String str : colNames) {
			Assert.assertEquals(collectInfoTable.genColumnUpdateResult(str, rkey, times, false).getObject(false).getValue(),
					rd.getResult().get(str));
		}
	}
	
	public void deleteAndVerify(String tabName,RKey rkey){
		Result<Boolean>rs=obClient.delete(tabName, rkey);
		Assert.assertTrue("delete fail!",rs.getResult());
		Result<RowData>rd=obClient.get(tabName, rkey, collectInfoTable.getAllColumnNames());
		Assert.assertNull(rd.getResult());
		Assert.assertEquals(ResultCode.OB_SUCCESS, rd.getCode());
	}

}
