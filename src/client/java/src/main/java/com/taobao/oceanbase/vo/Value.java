package com.taobao.oceanbase.vo;

import java.io.UnsupportedEncodingException;

import com.taobao.oceanbase.util.CheckParameter;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.vo.inner.ObActionFlag;
import com.taobao.oceanbase.vo.inner.ObObj;

public class Value {

	private Object value;
	private Type type = Type.NULL;

	public void setNumber(long number) {
		this.value = number;
		this.type = Type.INT;
	}

	public void setFloat(float f) {
		this.value = f;
		this.type = Type.FLOAT;
	}

	public void setDouble(double d) {
		this.value = d;
		this.type = Type.DOUBLE;
	}

	public void setSecond(long second) {
		this.value = second;
		this.type = Type.SECOND;
	}

	public void setMicrosecond(long microsecond) {
		this.value = microsecond;
		this.type = Type.MICROSECOND;
	}

	public void setString(String str) {
		try {
			if(str == null) throw new IllegalArgumentException("string value can not be null, if update null for string filed, pass a value object without any set-method invoked");
			this.value = new String(str.getBytes(Const.DEFAULT_CHARSET),Const.NO_CHARSET);
			CheckParameter.checkStringLimitation(str);
		} catch (UnsupportedEncodingException e) {
		}
		this.type = Type.STRING;
	}
	
	public void setExtend(ObActionFlag flag){
		this.value = flag;
		this.type = Type.EXT_TYPE;
	}

	private static enum Type {
		NULL, INT, FLOAT, DOUBLE, SECOND, MICROSECOND, STRING, SEQ,CREATE_TIME,MODIFY_TIME,EXT_TYPE;
	}

	public ObObj getObject(boolean isAdd) {
		ObObj obj = new ObObj();
		switch (type) {
		case NULL:
			obj.setNull();
			break;
		case INT:
			obj.setNumber((Long) value, isAdd);
			break;
		case FLOAT:
			obj.setFloat((Float) value, isAdd);
			break;
		case DOUBLE:
			obj.setDouble((Double) value, isAdd);
			break;
		case SECOND:
			obj.setDatetime((Long) value, isAdd);
			break;
		case MICROSECOND:
			obj.setPreciseDatetime((Long) value, isAdd);
			break;
		case STRING:
			obj.setString((String) value);
			break;
		case EXT_TYPE:
			obj.setExtend((ObActionFlag)value);
			break;
		}
		return obj;
	}
	
	public boolean allowAdd(){
		return this.type != Type.NULL && this.type != Type.STRING;
	}
	
}