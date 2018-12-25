package com.taobao.oceanbase.base;

public class RowKeyHelper {
	private// String itype_str =
	// "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	int num_per_utype;
	RowKey curr_key;

	RowKeyHelper(int num, RowKey skey) {
		num_per_utype = num;
		curr_key = skey;
	}

	public RowKeyHelper(RowKey skey) {
		num_per_utype = 100;
		curr_key = skey;
	}

	RowKeyHelper(int num) {
		num_per_utype = num;
	}

	// TODO need to handle border flag
	public long count_rowkey_num(RowKey start_rowkey, RowKey end_rowkey,
			Boolean inclu_start, Boolean inclu_end) {
		long count = 0;
		long start_uid = start_rowkey.get_uid();
		long start_tid = start_rowkey.get_tid();
		long end_uid = end_rowkey.get_uid();
		long end_tid = end_rowkey.get_tid();
		count = end_uid * 6200 + end_tid - (start_uid * 6200 + start_tid) - 1;
		if (true == inclu_start)
			count++;
		if (true == inclu_end)
			count++;
		return count;
	}

	public RowKey get_next_rkey() {
		long uid = curr_key.get_uid();
		char itype = (char) curr_key.get_itype();
		long tid = curr_key.get_tid();
		tid++;
		if (6200 == tid) {
			itype = '0';
			tid = 0;
			uid++;
		} else {
			itype = RowKeyFactory.itype_array[(int) (tid / 100 % 62)];
		}
		curr_key = RowKeyFactory.getRowkey(uid, itype, tid);
		return curr_key;
	}

	public RowKey get_pre_rkey() {
		long uid = curr_key.get_uid();
		char itype = (char) curr_key.get_itype();
		long tid = curr_key.get_tid();
		tid--;
		if (-1 == tid) {
			itype = 'z';
			tid = 6199;
			uid--;
		} else {
			itype = RowKeyFactory.itype_array[(int) (tid / 100 % 62)];
		}
		curr_key = RowKeyFactory.getRowkey(uid, itype, tid);
		return curr_key;
	}
	
	public RowKey get_cur_rkey() {
		return curr_key;
	}

}
