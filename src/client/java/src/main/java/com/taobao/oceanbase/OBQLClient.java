package com.taobao.oceanbase;

import java.util.List;

import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;

public interface OBQLClient {

	public Result<List<RowData>> query(String OBQL) throws Exception;

	public Result<Boolean> insert(String OBQL) throws Exception;

	public Result<Boolean> delete(String OBQL) throws Exception;

	public Result<Boolean> update(String OBQL) throws Exception;
}