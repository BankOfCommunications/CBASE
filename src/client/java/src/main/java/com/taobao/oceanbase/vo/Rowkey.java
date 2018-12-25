package com.taobao.oceanbase.vo;

import java.io.UnsupportedEncodingException;

import com.taobao.oceanbase.util.Const;

public abstract class Rowkey {

	public abstract byte[] getBytes();
	
	public final String getRowkey(){
		byte[] stream = getBytes();
		if(stream == null || stream.length == 0)
			throw new IllegalArgumentException("rowkey bytes null");
		try {
			return new String(stream,Const.NO_CHARSET);
		} catch (UnsupportedEncodingException e) {
			return null;// never happen
		}
	}

}
