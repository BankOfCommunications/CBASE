package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.network.codec.ObObjectUtil;
import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.util.Const;

public class ObObj implements Cloneable {

	public static enum Type {
		ObNullType, ObIntType, ObFloatType, ObDoubleType, ObDateTimeType, ObPreciseDateTimeType, ObVarcharType, ObSeqType, ObCreateTimeType, ObModifyTimeType, ObExtendType;
	}

	private static final byte INVALID_OP_FLAG = 0x0;
	private static final byte ADD = 0x1;

	private Meta meta = new Meta();

	private class Meta {
		private Type type = Type.ObNullType;
		private byte OpFlag = INVALID_OP_FLAG;
	}

	private Object value;

	public ObObj clone() {
		try {
			ObObj ret = (ObObj) super.clone();
			ret.meta = new Meta();
			ret.meta.type = this.meta.type;
			ret.meta.OpFlag = this.meta.OpFlag;
			return ret;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Object getValue() {
		return value;
	}

	public boolean isStringType() {
		return meta.type == Type.ObVarcharType;
	}

	public boolean equalExtendType(ObActionFlag type) {
		return type != null && meta.type == Type.ObExtendType
				&& (Long) value == type.action;
	}

	public void setNull() {
		this.value = null;
		meta.type = Type.ObNullType;
		meta.OpFlag = INVALID_OP_FLAG;
	}

	public void setNumber(long number) {
		setNumber(number, false);
	}
	
	public void setNumber(long number, boolean isAdd) {
		meta.type = Type.ObIntType;
		this.value = number;
		meta.OpFlag = isAdd ? ADD : INVALID_OP_FLAG;
	}

	public void setFloat(float f, boolean isAdd) {
		meta.type = Type.ObFloatType;
		this.value = f;
		meta.OpFlag = isAdd ? ADD : INVALID_OP_FLAG;
	}

	public void setDouble(double d, boolean isAdd) {
		meta.type = Type.ObDoubleType;
		this.value = d;
		meta.OpFlag = isAdd ? ADD : INVALID_OP_FLAG;
	}

	public void setExtend(ObActionFlag type) {
		meta.type = Type.ObExtendType;
		this.value = type.action;
	}

	public void setDatetime(long second, boolean isAdd) {
		meta.type = Type.ObDateTimeType;
		this.value = second;
		meta.OpFlag = isAdd ? ADD : INVALID_OP_FLAG;
	}

	public void setPreciseDatetime(long microsecond, boolean isAdd) {
		meta.type = Type.ObPreciseDateTimeType;
		this.value = microsecond;
		meta.OpFlag = isAdd ? ADD : INVALID_OP_FLAG;
	}

	public void setString(String str) {
		meta.type = Type.ObVarcharType;
		this.value = str;
		meta.OpFlag = INVALID_OP_FLAG;
	}

	public void setSequence() {
		this.value = null;
		meta.type = Type.ObSeqType;
		meta.OpFlag = INVALID_OP_FLAG;
	}

	public void serialize(ByteBuffer buffer) {
		boolean isAdd = (meta.OpFlag == ADD);
		switch (meta.type) {
		case ObNullType: {
			buffer.put(ObObjectUtil.encodeNull());
			break;
		}
		case ObIntType: {
			buffer.put(ObObjectUtil.encodeNumber((Long) value, isAdd));
			break;
		}
		case ObFloatType: {
			buffer.put(ObObjectUtil.encode((Float) value, isAdd));
			break;
		}
		case ObDoubleType: {
			buffer.put(ObObjectUtil.encode((Double) value, isAdd));
			break;
		}
		case ObDateTimeType: {
			buffer.put(ObObjectUtil.encodeDateTime((Long) value, isAdd));
			break;
		}
		case ObPreciseDateTimeType: {
			buffer.put(ObObjectUtil.encodePreciseDatetime((Long) value, isAdd));
			break;
		}
		case ObVarcharType: {
			buffer.put(ObObjectUtil.encode((String) value, Const.NO_CHARSET));
			break;
		}
		case ObSeqType:
			break;
		case ObExtendType: {
			buffer.put(ObObjectUtil.encodeExtendType((Long) value));
			break;
		}
		default:
			throw new IllegalArgumentException("type error:" + meta.type);
		}
	}

	public int getSize() {
		switch (meta.type) {
		case ObNullType:
			return 1;
		case ObIntType:
			return (int) ObObjectUtil.encodeNumberLength((Long) value);
		case ObFloatType:
			return 5;
		case ObDoubleType:
			return 9;
		case ObDateTimeType:
			return ObObjectUtil.encodedLengthForTime((Long) value);
		case ObPreciseDateTimeType:
			return ObObjectUtil.encodedLengthForTime((Long) value);
		case ObVarcharType:
			return ObObjectUtil.encodeLengthStr((String) value,
					Const.NO_CHARSET);
		case ObExtendType:
			return ObObjectUtil.encodeLengthExtend((Long) value);
		case ObSeqType:
			return 0;
		default:
			throw new IllegalArgumentException("type error : " + meta.type);
		}
	}

	public void deserialize(ByteBuffer buffer) {
		byte firstByte = buffer.get();
		if (firstByte == ObObjectUtil.OB_EXTEND_TYPE) {
			meta.type = Type.ObExtendType;
			value = Serialization.decodeVarLong(buffer);
			return;
		}
		switch ((firstByte & 0xc0) >>> 6) {
		case 0:
		case 1:
			meta.type = Type.ObIntType;
			this.value = ObObjectUtil.decodeNumber(firstByte, buffer);
			break;
		case 2:
			meta.type = Type.ObVarcharType;
			this.value = ObObjectUtil.decode(firstByte, buffer,
					Const.NO_CHARSET);
			break;
		case 3:
			int subType = (firstByte & 0x30) >>> 4;
			switch (subType) {
			case 0:
				break;
			case 1: {
				meta.type = Type.ObDateTimeType;
				value = ObObjectUtil.decodeTime(firstByte, buffer);
				break;
			}
			case 2: {
				meta.type = Type.ObPreciseDateTimeType;
				value = ObObjectUtil.decodeTime(firstByte, buffer);
				break;
			}
			case 3:
				int subSubType = (firstByte & 0x0c) >>> 2;
				switch (subSubType) {
				case 0: {
					meta.type = Type.ObModifyTimeType;
					value = ObObjectUtil.decodeTimeWithoutSign(firstByte, buffer);
					break;
				}
				case 1: {
					meta.type = Type.ObCreateTimeType;
					value = ObObjectUtil.decodeTimeWithoutSign(firstByte, buffer);
					break;
				}
				case 2: {
					if ((firstByte & 0x02) == 0x02) {
						meta.type = Type.ObDoubleType;
						value = ObObjectUtil.decodeDouble(buffer);
					} else {
						meta.type = Type.ObFloatType;
						value = ObObjectUtil.decodeFloat(buffer);
					}
					break;
				}
				case 3: {
					meta.type = Type.ObNullType;
					value = null;
					break;
				}
				}
			}
		}
	}

	public String toString() {
		switch (meta.type) {
		case ObNullType:
			return "<NULL>";
		case ObIntType:
			return "[int:" + value.toString() + "]";
		case ObFloatType:
			return "[float:" + value.toString() + "]";
		case ObDoubleType:
			return "[double:" + value.toString() + "]";
		case ObDateTimeType:
			return "[date:" + value.toString() + "]";
		case ObPreciseDateTimeType:
			return "[pdate:" + value.toString() + "]";
		case ObVarcharType:
			return "[string:" + value.toString() + "]";
		case ObExtendType:
			return "[action flag:" + value.toString() + "]";
		case ObSeqType:
			return "<SEQUENCE>";
		default:
			return "unknow type: " + meta.type;
		}
	}
}