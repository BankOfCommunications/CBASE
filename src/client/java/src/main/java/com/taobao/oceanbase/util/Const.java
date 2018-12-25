package com.taobao.oceanbase.util;

public final class Const {
	public static final int MIN_TIMEOUT = 1000;
	public static final String DEFAULT_CHARSET = "UTF-8";
	public static final String NO_CHARSET = "ISO-8859-1";
	public static final int MAGIC = 0x416e4574;
	public static final short SYSTEM_CODE = 1;
	public static final short CLIENT_CODE = 1;
	public static final short ROOTSERVER_CODE = 2;
	public static final short MergeSERVER_CODE = 3;
	public static final short UPDATESERVER_CODE = 4;
	public static final int MAX_ROW_NUMBER_PER_QUERY = 2000;
	public static final int MAX_PACKET_LENGTH = 1 << 20;
	public static final int MAX_COLUMN_NUMBER = 50;
	public static final int MAX_COLUMN_NAME_LENGTH = 128;
	public static final int MAX_TABLE_NAME_LENGTH = 256;
	public static final byte INCLUSIVE_START = 0x1;
	public static final byte INCLUSIVE_END = 0x2;
	public static final byte MIN_VALUE = 0x4;
	public static final byte MAX_VALUE = 0x8;
	public static final int INVALID_RETRY_TIME = 5000; // 5s
	public static final int MAX_STRING_SIZE = 1 << 20; //512KB
	public static final int SCAN_DIRECTION_FORWARD = 0;
	public static final int SCAN_DIRECTION_BACKWARD = 1;
	public static final int SCAN_MODE_SYNCREAD = 0;
	public static final int SCAN_MODE_PREREAD = 1;
	public static final String ASTERISK = "*";
	public static final String COUNT_AS_NAME = "count";
}