package com.taobao.oceanbase.base;

/**
 * @author baoni
 * 
 */
public class RowKeyFactory {
	private final static int num_per_utype = 100;
	
	public static char[] itype_array = { '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
			'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
			'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
			'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
			'x', 'y', 'z' };

	public static RowKey getRowkey(long uid, char itype, long tid) {
		return new RowKey(uid, itype, tid);
	}

	public static RowKey getBlankRowkey() {
		return new RowKey();
	}

	public static RowKey getRowKey(long uid, char itype) {
		return new RowKey(uid, itype);
	}

	public static RowKey getRowkey(long uid, long tid) {
		char itype = itype_array[(int) (tid / num_per_utype) % 62];
		return new RowKey(uid, itype, tid);
	}

	public static RowKey getItemRowkey(long tid) {
		char itype = itype_array[(int) (tid / num_per_utype) % 62];
		return new RowKey(itype, tid);
	}
}
