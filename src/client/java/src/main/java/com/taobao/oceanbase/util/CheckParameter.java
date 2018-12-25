package com.taobao.oceanbase.util;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.taobao.oceanbase.vo.Value;

public class CheckParameter {
	
	public static void checkNull(String message,Object param){
		if(param == null)
			throw new IllegalArgumentException(message);
	}
	
	public static void checkNull(String message,Object... params){
		for(Object param : params)
			checkNull(message,param);
	}
	
	public static void checkStringNull(String message,String param){
		if(param == null || param.trim().length() == 0)
			throw new IllegalArgumentException(message);
	}
	
	public static void checkStringNull(String message,String... params){
		for(String param : params)
			checkStringNull(message,param);
	}
	
	public static <E> void checkCollection(String message,Collection<E> param){
		if(param == null || param.size()==0)
			throw new IllegalArgumentException(message);
		for( E entry : param){
			if (entry == null)
				throw new IllegalArgumentException(message);
		}
	}
	
	public static <E> void checkCollection(String message,Collection<E>... params){
		for(Collection<E> param : params)
			checkCollection(message, param);
	}

	public static void checkAddOperation(boolean add, Value value){
		if(add && !value.allowAdd())
			throw new IllegalArgumentException("can not add null or string");
	}
	
	private static void checkString(String message, String name, int limit){
		if(name.length() > limit)
			throw new IllegalArgumentException(message);
	}
	
	public static void checkTableName(String table){
		checkStringNull("table name null", table);
		checkString("table name length limition is [1-" + Const.MAX_TABLE_NAME_LENGTH + "]", table, Const.MAX_TABLE_NAME_LENGTH);
	}
	
	public static void checkColumnName(String name){
		checkStringNull("column name null", name);
		checkString("column name length limition is [1-" + Const.MAX_COLUMN_NAME_LENGTH + "]", name, Const.MAX_COLUMN_NAME_LENGTH);
	}
	
	public static void checkNullSetElement(String message,Set<String> param){
		for (String e : param) {
			if (e == null || e.trim().length() == 0)
				throw new IllegalArgumentException(message);
		}
	}
	
	public static <E,F> void checkMap(String message, Map<E,F> param) {
		if (param == null || param.size() <= 0)
			throw new IllegalArgumentException(message);
		for (Map.Entry<E, F> entry : param.entrySet()){
			if (entry.getKey() == null || entry.getValue() == null){
				throw new IllegalArgumentException(message);
			}
		}
	}
	
	public static void checkStringLimitation(String str){
		try {
			if(str.getBytes(Const.NO_CHARSET).length > Const.MAX_STRING_SIZE)
				throw new IllegalArgumentException("string size must be less than " + Const.MAX_STRING_SIZE + " bytes");
		} catch (UnsupportedEncodingException e) {
		}
	}

}
