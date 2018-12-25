package com.taobao.oceanbase.network.codec;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class ObObjectUtil {

	private static final long OB_MAX_INT_1B = 23;
	private static final long OB_MAX_INT_2B = (1l << 8) - 1;
	private static final long OB_MAX_INT_3B = (1l << 16) - 1;
	private static final long OB_MAX_INT_4B = (1l << 24) - 1;
	private static final long OB_MAX_INT_5B = (1l << 32) - 1;
	private static final long OB_MAX_INT_7B = (1l << 48) - 1;

	private static final long OB_MAX_4B = (1l << 32) - 1;
	private static final long OB_MAX_6B = (1l << 48) - 1;

	private static final long[][] MAX = { { OB_MAX_INT_1B, 1 },
			{ OB_MAX_INT_2B, 2 }, { OB_MAX_INT_3B, 3 }, { OB_MAX_INT_4B, 4 },
			{ OB_MAX_INT_5B, 5 }, { OB_MAX_INT_7B, 7 } };

	private static final long OB_MAX_1B_STR_LEN = 55;
	private static final long OB_MAX_2B_STR_LEN = OB_MAX_INT_2B;
	private static final long OB_MAX_3B_STR_LEN = OB_MAX_INT_3B;
	private static final long OB_MAX_4B_STR_LEN = OB_MAX_INT_4B;
	private static final long OB_MAX_5B_STR_LEN = OB_MAX_INT_5B;

	private static final byte OB_INT_SIGN_BIT_POS = 5;
	private static final byte OB_INT_OPERATION_BIT_POS = 6;
	private static final byte OB_DATETIME_OPERATION_BIT = 3;
	private static final byte OB_DATETIME_SIGN_BIT = 2;
	private static final byte OB_FLOAT_OPERATION_BIT_POS = 0;

	private static final byte OB_INT_VALUE_MASK = 0x1f;
	private static final byte OB_VARCHAR_LEN_MASK = 0x3f;
	private static final byte OB_DATETIME_LEN_MASK = 0x03;

	public static final byte OB_VARCHAR_TYPE = (byte) (0x01 << 7);
	public static final byte OB_SEQ_TYPE = (byte) 0xc0;
	public static final byte OB_DATETIME_TYPE = (byte) 0xd0;
	public static final byte OB_PRECISE_DATETIME_TYPE = (byte) 0xe0;
	public static final byte OB_MODIFYTIME_TYPE = (byte) 0xf0;
	public static final byte OB_CREATETIME_TYPE = (byte) 0xf4;
	public static final byte OB_FLOAT_TYPE = (byte) 0xf8;
	public static final byte OB_DOUBLE_TYPE = (byte) 0xfa;
	public static final byte OB_NULL_TYPE = (byte) 0xfc;
	public static final byte OB_EXTEND_TYPE = (byte) 0xfe;

	public static byte encodeNull() {
		return Serialization.encode(OB_NULL_TYPE);
	}

	public static long encodeNumberLength(long value) {
		if(value == Long.MIN_VALUE)
			return 9;
		long abs = Math.abs(value);
		for (int index = 0; index < 6; index++) {
			if (abs <= MAX[index][0]) {
				return MAX[index][1];
			}
		}
		return 9;
	}

	private static byte setBit(byte target, byte pos) {
		return target |= (1 << pos);
	}

	public static byte[] encodeNumber(long value, boolean isAdd) {
		long length = encodeNumberLength(value);
		byte firstByte = 0;
		if (value < 0)
			firstByte = setBit(firstByte, OB_INT_SIGN_BIT_POS);
		if (isAdd)
			firstByte = setBit(firstByte, OB_INT_OPERATION_BIT_POS);
		firstByte |= (Math.abs(value) <= OB_MAX_INT_1B && Math.abs(value) >= 0 ? Math.abs(value)
				: ((length == 7 || length == 9 ? length - 1 : length) - 1 + OB_MAX_INT_1B));
		byte[] ret = new byte[(int) length];
		ret[0] = Serialization.encode(firstByte);
		for (int index = 0; index < (length - 1); index++) {
			ret[index + 1] = Serialization
					.encode((byte) ((Math.abs(value) >>> (index << 3)) & 0xff));
		}
		return ret;
	}

	private static boolean testBit(byte target, byte pos) {
		return 0 != (target & (1 << pos));
	}

	public static long decodeNumber(byte firstByte, ByteBuffer buffer) {
		boolean isNeg = testBit(firstByte, OB_INT_SIGN_BIT_POS);
		byte lenOrValue = (byte) (firstByte & OB_INT_VALUE_MASK);
		if (lenOrValue <= OB_MAX_INT_1B) {
			return isNeg ? -lenOrValue : lenOrValue;
		} else {
			long value = 0;
			long len = lenOrValue - OB_MAX_INT_1B;
			if (len == 5 || len == 7) {
				len++;
			}
			for (int index = 0; index < len; index++) {
				value |= ((buffer.get() & 0xffl) << (index << 3));
			}
			return isNeg ? -value : value;
		}
	}

	private static final long[] MAX_STR_LEN = { OB_MAX_1B_STR_LEN,
			OB_MAX_2B_STR_LEN, OB_MAX_3B_STR_LEN, OB_MAX_4B_STR_LEN,
			OB_MAX_5B_STR_LEN };

	private static final int encodeLengthStrLen(long len) {
		int value = 0;
		for (long max : MAX_STR_LEN) {
			value++;
			if (len <= max)
				return value;
		}
		return -1;
	}

	public static final int encodeLengthStr(String str, String charset) {
		try {
			int len = getStrLen(str, charset);
			return encodeLengthStrLen(len) + len;
		} catch (UnsupportedEncodingException e) {
			return 0;// never happen
		}
	}

	public static final byte[] encode(String str, String charset) {
		try {
			int len = getStrLen(str, charset);
			int lenSize = encodeLengthStrLen(len);
			byte firstByte = OB_VARCHAR_TYPE;
			firstByte |= (lenSize == 1 ? (len & 0xff)
					: (lenSize - 1 + OB_MAX_1B_STR_LEN));
			byte[] ret = new byte[encodeLengthStr(str, charset)];
			ret[0] = Serialization.encode(firstByte);
			if (lenSize > 1) {
				for (int n = 0; n < lenSize - 1; n++) {
					ret[n + 1] = Serialization
							.encode((byte) (len >>> (n << 3)));
				}
			}
			System.arraycopy(str.getBytes(charset), 0, ret, lenSize, len);
			return ret;
		} catch (UnsupportedEncodingException e) {
			return null;// never happen
		}
	}

	private static int getStrLen(String str, String charset)
			throws UnsupportedEncodingException {
		return str.getBytes(charset).length;
	}

	public static final String decode(byte firstByte, ByteBuffer buffer,
			String charset) {
		int lenOrValue = firstByte & OB_VARCHAR_LEN_MASK;
		int strLen = 0;
		if (lenOrValue <= OB_MAX_1B_STR_LEN) {
			strLen = lenOrValue;
		} else {
			for (int n = 0; n < lenOrValue - OB_MAX_1B_STR_LEN; n++) {
				strLen |= ((buffer.get() & 0xffl) << (n << 3));
			}
		}
		byte[] ret = new byte[strLen];
		buffer.get(ret);
		try {
			return new String(ret, charset);
		} catch (UnsupportedEncodingException e) {
			return "";// never happen
		}
	}

	public static byte[] encode(float f, boolean isAdd) {
		byte[] ret = new byte[5];
		ret[0] = Serialization.encode(!isAdd ? OB_FLOAT_TYPE : setBit(
				OB_FLOAT_TYPE, OB_FLOAT_OPERATION_BIT_POS));
		System.arraycopy(Serialization.encode(Float.floatToRawIntBits(f)), 0,
				ret, 1, 4);
		return ret;
	}

	public static float decodeFloat(ByteBuffer buffer) {
		byte[] bytes = new byte[4];
		buffer.get(bytes);
		return Float.intBitsToFloat(Serialization.decodeInt(bytes));
	}

	public static double decodeDouble(ByteBuffer buffer) {
		byte[] bytes = new byte[8];
		buffer.get(bytes);
		return Double.longBitsToDouble(Serialization.decodeLong(bytes));
	}

	public static byte[] encode(double d, boolean isAdd) {
		byte[] ret = new byte[9];
		ret[0] = Serialization.encode(!isAdd ? OB_DOUBLE_TYPE : setBit(
				OB_DOUBLE_TYPE, OB_FLOAT_OPERATION_BIT_POS));
		System.arraycopy(Serialization.encode(Double.doubleToRawLongBits(d)),
				0, ret, 1, 8);
		return ret;
	}

	public static int encodedLengthForTime(long value) {
		if (value == Long.MIN_VALUE) {
			return 9;
		}
		long v = Math.abs(value);
		return v <= OB_MAX_4B ? 5 : v <= OB_MAX_6B ? 7 : 9;
	}

	private static byte[] encodeTime(long value, byte type, boolean isAdd) {
		int len = encodedLengthForTime(value);
		byte[] ret = new byte[len];
		byte firstByte = type;
		firstByte = value < 0 ? setBit(firstByte, OB_DATETIME_SIGN_BIT)
				: firstByte;
		firstByte = isAdd ? setBit(firstByte, OB_DATETIME_OPERATION_BIT)
				: firstByte;
		if (len == 7) {
			firstByte |= 1;
		} else if (len == 9) {
			firstByte |= 2;
		}
		ret[0] = Serialization.encode(firstByte);
		for (int n = 0; n < len - 1; n++) {
			ret[n + 1] = Serialization
					.encode((byte) ((Math.abs(value) >>> (n << 3)) & 0xff));
		}
		return ret;
	}

	public static byte[] encodeDateTime(long value, boolean isAdd) {
		return encodeTime(value, OB_DATETIME_TYPE, isAdd);
	}

	public static byte[] encodePreciseDatetime(long value, boolean isAdd) {
		return encodeTime(value, OB_PRECISE_DATETIME_TYPE, isAdd);
	}

	public static long decodeTime(byte firstByte, ByteBuffer buffer) {
		boolean isNeg = testBit(firstByte, OB_DATETIME_SIGN_BIT);
		long value = decodeTimeWithoutSign(firstByte, buffer);
		return isNeg ? -value : value;
	}

	public static int encodeLengthExtend(long value) {
		return 1 + Serialization.getNeedBytes(value);
	}

	public static byte[] encodeExtendType(long value) {
		byte[] ret = new byte[encodeLengthExtend(value)];
		ret[0] = OB_EXTEND_TYPE;
		System.arraycopy(Serialization.encodeVarLong(value), 0, ret, 1,
				ret.length - 1);
		return ret;
	}

	public static long decodeTimeWithoutSign(byte firstByte, ByteBuffer buffer) {
		int lenMark = firstByte & OB_DATETIME_LEN_MASK;
		int len = 0;
		if (lenMark == 0) {
			len = 4;
		} else if (lenMark == 1) {
			len = 6;
		} else if (lenMark == 2) {
			len = 8;
		}
		long value = 0;
		for (int n = 0; n < len; n++) {
			value |= ((buffer.get() & 0xffl) << (n << 3));
		}
		return value;
	}
	
}