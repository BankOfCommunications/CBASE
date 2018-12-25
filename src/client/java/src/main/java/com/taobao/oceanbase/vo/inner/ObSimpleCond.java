package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.vo.Value;

public class ObSimpleCond {
	private static final Log log = LogFactory.getLog(ObSimpleCond.class);
			
	public static enum ObLogicOperator {
		NIL(0), LT(1), LE(2), EQ(3), GT(4), GE(5), NE(6), LIKE(7);
		
		private int value;
		
		ObLogicOperator(int value) {
			this.value = value;
		}
		
		int getValue() {
			return value;
		}
	};

	String columnName;
	ObLogicOperator operator;
	Value operand;

	public ObSimpleCond() {
	}

	public ObSimpleCond(String columnName, ObLogicOperator operator, Value operand) {
		this.columnName = columnName;
		this.operator = operator;
		this.operand = operand;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public ObLogicOperator getOperator() {
		return operator;
	}

	public void setOperator(ObLogicOperator operator) {
		this.operator = operator;
	}

	public Value getOperand() {
		return operand;
	}

	public void setOperand(Value operand) {
		this.operand = operand;
	}
	
	public ObObj getOperandAsObObj() {
		ObObj obj = null;
		if (operand != null) {
			obj = operand.getObject(false);
		}
		return obj;
	}
	
	public boolean isValid() {
		boolean res = true;
		if (columnName == null || columnName.length() == 0) {
			log.error("columnName cannot be null or empty: " + this);
			res = false;
		}

		if (res && operator == null) {
			log.error("operator cannot be null: " + this);
			res = false;
		}

		if (res && operand == null) {
			res = false;
			log.error("operand cannot be null: " + this);
		}
		if (res && operator == ObLogicOperator.LIKE) {
			ObObj obj = operand.getObject(false);
			if (!obj.isStringType() || obj.toString().length() == 0) {
				log.error("like operator's operand must be string and not empty: " + this);
				res = false;
			}
		}
		return res;
	}

	public void serialize(ByteBuffer buffer) {
		if (columnName == null || operand == null) {
			throw new IllegalArgumentException("columnName and operand can not be null");
		}
		
		ObObj obj = new ObObj();
		
		obj.setString(columnName);
		obj.serialize(buffer);
		
		obj.setNumber(operator.value);
		obj.serialize(buffer);

		operand.getObject(false).serialize(buffer);
	}
	
	public int getSize() {
		int length = 0;
		
		if (columnName != null && operand != null) {
			ObObj obj = new ObObj();
			obj.setString(columnName);
			length += obj.getSize();
			
			obj.setNumber(operator.value);
			length += obj.getSize();
			
			length += operand.getObject(false).getSize();
		}
		
		return length;
	}
	
	public String toString() {
		return "[" + columnName + ", " + operator + ", " + operand + "]";
	}
}
