package com.taobao.oceanbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import com.taobao.oceanbase.util.CheckQueryParams;
import com.taobao.oceanbase.util.result.GroupByHandler;
import com.taobao.oceanbase.util.result.Handler;
import com.taobao.oceanbase.util.result.SequenceHandler;
import com.taobao.oceanbase.util.result.StatsFuncHandler;
import com.taobao.oceanbase.vo.Option;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.StatsFunc;

public class AppTest extends TestCase {

	private final List<RowData> data = new ArrayList<RowData>();

	protected void setUp() throws Exception {
		data.add(new RowData() {
			{
				addData("name", "tony");
				addData("level", "P");
				addData("price", 5000);
			}
		});
		data.add(new RowData() {
			{
				addData("name", "swaddy");
				addData("level", "M");
				addData("price", 5000);
			}
		});
		data.add(new RowData() {
			{
				addData("name", "wells");
				addData("level", "P");
				addData("price", 500);
			}
		});
		data.add(new RowData() {
			{
				addData("name", "sam");
				addData("level", "M");
				addData("price", 10000);
			}
		});
	}

	public void testApp() {
		Set<String> columns = new HashSet<String>();
		columns.add("level");
		Handler<List<RowData>, List<List<RowData>>> a = new GroupByHandler(
				columns);
		Option option1 = new Option("level", "num", StatsFunc.COUNT);
		Option option2 = new Option("level", "level", StatsFunc.NONE);
		Handler<List<List<RowData>>, List<RowData>> b = new StatsFuncHandler(
				Arrays.asList(option1, option2));
		Handler<List<RowData>, List<RowData>> seq = new SequenceHandler<List<RowData>, List<RowData>, List<List<RowData>>>(
				a, b);
		for (RowData row : seq.handle(data)) {
			System.out.println(row);
		}
	}

	public void testQueryParams() {
		QueryInfo query = new QueryInfo();
		CheckQueryParams.check(query);
	}
}