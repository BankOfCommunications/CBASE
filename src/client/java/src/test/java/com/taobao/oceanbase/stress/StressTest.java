package com.taobao.oceanbase.stress;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import com.taobao.oceanbase.Client;
import com.taobao.oceanbase.ClientImpl;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Rowkey;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.Value;

class ColumnInfo {
	private String column_name;
	private int column_type;

	public ColumnInfo(String column_name, int column_type) {
		this.column_name = column_name;
		this.column_type = column_type;
	}

	public String get_name() {
		return column_name;
	}

	public int get_type() {
		return column_type;
	}
}

public class StressTest extends TestCase {
	// private static final String COLLECT_INFO_TABLE = "collect_info";
	private static final String COLLECT_INFO_TABLE = "test_db.test_against_test";
	private static final String COLLECT_ITEM_TABLE = "collect_item";
	private static final String OB_COLLECT_INFO = "collect_info";

	private static final String USER_ID = "user_id";
	private static final String ITEM_ID = "item_id";
	private static final String ITEM_TYPE = "item_type";
	private static final String USER_NICK = "user_nick";
	private static final String IS_SHARED = "isshared";
	private static final String OB_IS_SHARED = "is_shared";
	private static final String NOTE = "note";
	private static final String COLLECT_TIME = "collect_time";
	private static final String STATUS = "status";
	private static final String GMT_CREATE = "gmt_create";
	private static final String GMT_MODIFIED = "gmt_modified";
	private static final String TAG = "tag";
	private static final String CATEGORY = "category";
	private static final String TITLE = "title";
	private static final String PICTURL = "picturl";
	private static final String OWNER_ID = "owner_id";
	private static final String OWNER_NICK = "owner_nick";
	private static final String PRICE = "price";
	private static final String ENDS = "ends";
	private static final String PROPER_XML = "proper_xml";
	private static final String COLLECTOR_COUNT = "collector_count";
	private static final String COLLECT_COUNT = "collect_count";

	private static int IntType = 1;
	private static int FloatType = 2;
	private static int DoubleType = 3;
	private static int VarcharType = 4;
	private static int DataTimeType = 5;

	private static ArrayList<ColumnInfo> TABLE_COLUMNS = new ArrayList<ColumnInfo>();
	static {
		TABLE_COLUMNS.add(new ColumnInfo(USER_NICK, VarcharType));
		TABLE_COLUMNS.add(new ColumnInfo(IS_SHARED, IntType));
		TABLE_COLUMNS.add(new ColumnInfo(NOTE, VarcharType));
		TABLE_COLUMNS.add(new ColumnInfo(COLLECT_TIME, DataTimeType));
		TABLE_COLUMNS.add(new ColumnInfo(STATUS, IntType));
		TABLE_COLUMNS.add(new ColumnInfo(GMT_CREATE, DataTimeType));
		TABLE_COLUMNS.add(new ColumnInfo(GMT_MODIFIED, DataTimeType));
		TABLE_COLUMNS.add(new ColumnInfo(TAG, VarcharType));
		TABLE_COLUMNS.add(new ColumnInfo(CATEGORY, IntType));
		TABLE_COLUMNS.add(new ColumnInfo(TITLE, VarcharType));
		TABLE_COLUMNS.add(new ColumnInfo(PICTURL, VarcharType));
		TABLE_COLUMNS.add(new ColumnInfo(OWNER_ID, IntType));
		TABLE_COLUMNS.add(new ColumnInfo(OWNER_NICK, VarcharType));
		TABLE_COLUMNS.add(new ColumnInfo(PRICE, FloatType));
		TABLE_COLUMNS.add(new ColumnInfo(ENDS, DataTimeType));
		TABLE_COLUMNS.add(new ColumnInfo(PROPER_XML, VarcharType));
		TABLE_COLUMNS.add(new ColumnInfo(COLLECTOR_COUNT, IntType));
		TABLE_COLUMNS.add(new ColumnInfo(COLLECT_COUNT, IntType));
	}

	// private static final String root_ip = "10.232.36.29";
	// private static int root_port = 2600;
	private static final String root_ip = "10.232.36.32";
	private static int root_port = 11000;
	Random random;
	SQLClient sql_client;
	Client ob_client;

	protected void setUp() throws Exception {
		random = new Random();

		sql_client = new SQLClient();
		ob_client = new ClientImpl() {
			{
				setIp(root_ip);
				setPort(root_port);
				init();
			}
		};

		sql_client.connect();
		System.out.println("test start");
	}

	protected void tearDown() throws Exception {
		sql_client.close();
		System.out.println("test end");
	}

	public void updateAndCmp(SQLClient sql_client, Client ob_client, int rand_row_id) throws SQLException {

		String sql = "select * from " + COLLECT_INFO_TABLE + " where rowkey=" + rand_row_id;

		ResultSet rs = sql_client.executeQuery(sql);
		rs.next();

		int is_shared = rs.getInt(IS_SHARED);
		long user_id = rs.getLong(USER_ID);
		int item_type = rs.getInt(ITEM_TYPE);
		long item_id = rs.getLong(ITEM_ID);

		System.out.println("user_id=" + user_id + ",item_type=" + item_type + ",item_id=" + item_id);
		System.out.println("is_shared=" + is_shared);

		rs.getStatement().close();
		rs.close();

		// update Mysql
		int new_is_shared = 88;
		String update_sql = "update " + COLLECT_INFO_TABLE + " set " + IS_SHARED + "=" + new_is_shared
				+ " where rowkey=" + rand_row_id;

		int affected_num = sql_client.executeUpdate(update_sql);
		System.out.println("affected_num=" + affected_num);

		// query updated Mysql
		ResultSet new_rs = sql_client.executeQuery(sql);
		new_rs.next();

		int mysql_is_shared = new_rs.getInt(IS_SHARED);

		System.out.println("new_mysql_is_shared=" + mysql_is_shared);
		new_rs.getStatement().close();
		new_rs.close();

		Rowkey row_key = new TestOBRowkey(user_id, item_type, item_id);

		Set<String> columns = new HashSet<String>();
		columns.add(OB_IS_SHARED);
		// query original Oceanbase
		Result<RowData> ori_res = ob_client.get(OB_COLLECT_INFO, row_key, columns);
		System.out.println("ori_oceanbase_val=" + ori_res.getResult().toString());

		// update Oceanbase
		UpdateMutator mutator = new UpdateMutator(OB_COLLECT_INFO, row_key);
		Value add_value = new Value();
		add_value.setNumber(new_is_shared);
		mutator.addColumn(OB_IS_SHARED, add_value, false);
		Result<Boolean> update_res = ob_client.update(mutator);
		System.out.println("update_op_res=" + update_res.getResult());

		// query updated Oceanbase
		Result<RowData> new_res = ob_client.get(OB_COLLECT_INFO, row_key, columns);

		// compare result
		System.out.println("new_val: mysql_val=" + mysql_is_shared + ", ob_val=" + new_res.getResult());
		long ob_result = new_res.getResult().get(OB_IS_SHARED);
		
		/*
		if (ob_result != mysql_is_shared) {
			throw new NullPointerException("ob_result=" + ob_result + ",mysql_result=" + mysql_is_shared);
		}
		*/
		assertEquals("ob_result is not equal with mysql_result", ob_result, mysql_is_shared);
	}

	public int getCount(SQLClient sql_client) throws SQLException {
		int count = 0;
		String sql = "select count(*) from " + COLLECT_INFO_TABLE;
		ResultSet rs = sql_client.executeQuery(sql);
		boolean ret = rs.next();
		count = rs.getInt(1);

		rs.getStatement().close();
		rs.close();
		return count;
	}

	public void testUpdate() throws SQLException {
		int row_num = getCount(sql_client);
		System.out.println("testUpdate: row_num = " + row_num);
		int count = 10;
		while (--count >= 0) {
			int rand_row_id = random.nextInt(row_num) + 1;
			System.out.println("rand_row_id = " + rand_row_id);
			try {
				updateAndCmp(sql_client, ob_client, rand_row_id);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("End Compare");
	}
}
