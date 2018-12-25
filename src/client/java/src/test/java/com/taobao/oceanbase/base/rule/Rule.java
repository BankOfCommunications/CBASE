package com.taobao.oceanbase.base.rule;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.base.RKey;
import com.taobao.oceanbase.vo.Value;

public interface Rule {
	
	/*These constants are magic number for calculating expect result
	 * Value = (user_id * FIR_MULTIPLIER + item_id)*SEC_MULTIPLIER + column_id
	 * Time 	Type   column_value = TIME_START - user_id*FIR_MULTIPLIER - item_id - column_id
	 * Num  	Type   column_value = Value
	 * Varchar	Type   column_value = rowkey.tostring+"-$column_id"
	 */
	public final static long TIME_START = 1289915563;
	public final static int FIR_MULTIPLIER = 256;
	public final static int SEC_MULTIPLIER = 100;
	public final static int NUM_OF_TYPE = 62;
	public final static int NUM_PER_UIDTYPE = 100;
	public final static long MICRO_PER_SEC = 1000000;
	
	public ByteBuffer genRowKey();
	
	public Object genColumnValue(RKey rkey, long col_id);
	
	public Value genColumnUpdateResult(RKey rkey, long colid,int times, boolean isAdd);
	
}
