package com.taobao.oceanbase.network.codec;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.taobao.oceanbase.util.Const;

public final class Serialization {

	public static byte[] encode(float f) {
		return encodeVarInt(Float.floatToRawIntBits(f));
	}

	public static byte[] encode(double d) {
		return encodeVarLong(Double.doubleToRawLongBits(d));
	}

	public static byte encode(byte b) {
		return b;
	}

	public static byte[] encode(short s) {
		byte[] ret = new byte[2];
		ret[1] = (byte) s;
		ret[0] = (byte) (s >>> 8);
		return ret;
	}

	public static byte[] encode(int i) {
		byte[] ret = new byte[4];
		ret[3] = (byte) i;
		ret[2] = (byte) (i >>> 8);
		ret[1] = (byte) (i >>> 16);
		ret[0] = (byte) (i >>> 24);
		return ret;
	}

	public static byte[] encode(long l) {
		byte[] ret = new byte[8];
		ret[7] = (byte) l;
		ret[6] = (byte) (l >>> 8);
		ret[5] = (byte) (l >>> 16);
		ret[4] = (byte) (l >>> 24);
		ret[3] = (byte) (l >>> 32);
		ret[2] = (byte) (l >>> 40);
		ret[1] = (byte) (l >>> 48);
		ret[0] = (byte) (l >>> 56);
		return ret;
	}

	public static byte encode(boolean b) {
		return b ? encode((byte) 1) : encode((byte) 0);
	}

	public static byte[] encode(char c) {
		return encode((short) c);
	}

	public static byte decodeByte(byte[] value) {
		assertTrue(value.length == 1);
		return value[0];
	}

	public static short decodeShort(byte[] value) {
		assertTrue(value.length == 2);
		return (short) ((value[0] << 8) | (value[1] & 0xff));
	}

	public static int decodeInt(byte[] value) {
		assertTrue(value.length == 4);
		return (value[0] << 24) | ((value[1] & 0xff) << 16)
				| ((value[2] & 0xff) << 8) | (value[3] & 0xff);
	}

	public static long decodeLong(byte[] value) {
		assertTrue(value.length == 8);
		return ((value[0] & 0xffl) << 56) | ((value[1] & 0xffl) << 48)
				| ((value[2] & 0xffl) << 40) | ((value[3] & 0xffl) << 32)
				| ((value[4] & 0xffl) << 24) | ((value[5] & 0xffl) << 16)
				| ((value[6] & 0xffl) << 8) | ((value[7] & 0xffl) << 0);
	}

	public static boolean decodeBoolean(byte[] value) {
		return decodeByte(value) == 0 ? false : true;
	}

	public static float decodeFloat(byte[] value) {
		return Float.intBitsToFloat(decodeVarInt(value));
	}

	public static float decodeFloat(ByteBuffer buffer) {
		return Float.intBitsToFloat(decodeVarInt(buffer));
	}

	public static double decodeDouble(byte[] value) {
		return Double.longBitsToDouble(decodeVarLong(value));
	}

	public static double decodeDouble(ByteBuffer buffer) {
		return Double.longBitsToDouble(decodeVarLong(buffer));
	}

	private static void assertTrue(boolean require) {
		if (!require)
			throw new RuntimeException("byte[] size wrong");
	}

	private static final long OB_MAX_V1B = (1l << 7) - 1;
	private static final long OB_MAX_V2B = (1l << 14) - 1;
	private static final long OB_MAX_V3B = (1l << 21) - 1;
	private static final long OB_MAX_V4B = (1l << 28) - 1;
	private static final long OB_MAX_V5B = (1l << 35) - 1;
	private static final long OB_MAX_V6B = (1l << 42) - 1;
	private static final long OB_MAX_V7B = (1l << 49) - 1;
	private static final long OB_MAX_V8B = (1l << 56) - 1;
	private static final long OB_MAX_V9B = (1l << 63) - 1;

	private static final long[] OB_MAX = { OB_MAX_V1B, OB_MAX_V2B, OB_MAX_V3B,
			OB_MAX_V4B, OB_MAX_V5B, OB_MAX_V6B, OB_MAX_V7B, OB_MAX_V8B,
			OB_MAX_V9B };

	public static int getNeedBytes(long l) {
		if (l < 0)
			return 10;
		int needBytes = 0;
		for (long max : OB_MAX) {
			needBytes++;
			if (l <= max)
				break;
		}
		return needBytes;
	}

	public static int getNeedBytes(int l) {
		if (l < 0)
			return 5;
		int needBytes = 0;
		for (long max : OB_MAX) {
			needBytes++;
			if (l <= max)
				break;
		}
		return needBytes;
	}

	public static byte[] encodeVarInt(int i) {
		byte[] ret = new byte[getNeedBytes(i)];
		int index = 0;
		while (i < 0 || i > OB_MAX_V1B) {
			ret[index++] = (byte) (i | 0x80);
			i >>>= 7;
		}
		ret[index] = (byte) (i & 0x7f);
		return ret;
	}

	public static int decodeVarInt(byte[] value) {
		int ret = 0;
		int shift = 0;
		for (byte b : value) {
			ret |= (b & 0x7f) << shift;
			shift += 7;
			if ((b & 0x80) == 0) {
				break;
			}
		}
		return ret;
	}

	public static byte[] encodeVarLong(long l) {
		byte[] ret = new byte[getNeedBytes(l)];
		int index = 0;
		while (l < 0 || l > OB_MAX_V1B) {
			ret[index++] = (byte) (l | 0x80);
			l >>>= 7;
		}
		ret[index] = (byte) (l & 0x7f);
		return ret;
	}

	public static long decodeVarLong(ByteBuffer buffer) {
		long ret = 0;
		int shift = 0;
		while (true) {
			byte b = buffer.get();
			ret |= (b & 0x7fl) << shift;
			shift += 7;
			if ((b & 0x80) == 0) {
				break;
			}
		}
		return ret;
	}

	public static long decodeVarLong(byte[] value) {
		long ret = 0;
		int shift = 0;
		for (byte b : value) {
			ret |= (b & 0x7fl) << shift;
			shift += 7;
			if ((b & 0x80) == 0) {
				break;
			}
		}
		return ret;
	}

	public static byte[] encode(String str, String charset) {
		if (str == null)
			str = "";
		try {
			byte[] data = str.getBytes(charset);
			int dataLen = data.length;
			int strLen = getNeedBytes(dataLen);
			byte[] ret = new byte[strLen + dataLen + 1];
			int index = 0;
			for (byte b : encodeVarInt(dataLen)) {
				ret[index++] = b;
			}
			for (byte b : data) {
				ret[index++] = b;
			}
			ret[index] = 0;
			return ret;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static int getNeedBytes(String str) {
		if (str == null)
			str = "";
		try {
			return getNeedBytes(str.getBytes(Const.NO_CHARSET).length)
					+ str.getBytes(Const.NO_CHARSET).length + 1;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static String decodeString(ByteBuffer buffer, String charset) {
		int dataLen = decodeVarInt(buffer);
		byte[] content = new byte[dataLen];
		buffer.get(content);
		buffer.get();// skip the end byte '0'
		try {
			return new String(content, charset);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static int decodeVarInt(ByteBuffer buffer) {
		int ret = 0;
		int shift = 0;
		while (true) {
			byte b = buffer.get();
			ret |= (b & 0x7f) << shift;
			shift += 7;
			if ((b & 0x80) == 0) {
				break;
			}
		}
		return ret;
	}

	public static boolean decodeBoolean(ByteBuffer buffer) {
		byte[] buf = new byte[1];
		buffer.get(buf);
		return decodeBoolean(buf);
	}
	public static int decodeInt(ByteBuffer buffer) {
		byte[] value = new byte[4];
		buffer.get(value);
		return decodeInt(value);
	}

	public static long decodeLong(ByteBuffer buffer) {
		byte[] value = new byte[8];
		buffer.get(value);
		return decodeLong(value);
	}
}