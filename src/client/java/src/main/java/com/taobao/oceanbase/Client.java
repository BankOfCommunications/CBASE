package com.taobao.oceanbase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.oceanbase.vo.GetParam;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.ObMutatorBase;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Rowkey;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.inner.ObActionFlag;
import com.taobao.oceanbase.vo.inner.schema.ObSchemaManagerV2;

public interface Client {

	public Result<RowData> get(String table, Rowkey rowkey, Set<String> columns);
	
	public Result<List<RowData>> get(List<GetParam> getParams);

	public Result<List<RowData>> query(String table, QueryInfo query);
	
	public Result<Long> count(String table, QueryInfo query);

	public Result<Boolean> insert(InsertMutator mutator);
	/** multi mutator support, use OP_USE_OB_SEM */
	public Result<Boolean> insert(List<InsertMutator> mutators);
	/** multi mutator support
	 * this mutator will be in on transaction
	 * @param mutators the mutators
	 * @param actionFlag action flag (OP_USE_OB_SEM | OP_USE_DB_SEM)
	 * @return
	 */
	public Result<Boolean> insert(List<InsertMutator> mutators, ObActionFlag actionFlag);

	public Result<Boolean> update(UpdateMutator mutator);
	/** multi mutator support, use OP_USE_OB_SEM */
	public Result<Boolean> update(List<ObMutatorBase> mutators);
	/** multi mutator support
	 * this mutator will be in on transaction
	 * @param mutators the mutators
	 * @param actionFlag action flag (OP_USE_OB_SEM | OP_USE_DB_SEM)
	 * @return
	 */
	public Result<Boolean> update(List<ObMutatorBase> mutators, ObActionFlag actionFlag);

	public Result<Boolean> delete(String table, Rowkey rowkey);
	/** multi delete support*/
	public Result<Boolean> delete(Map<String, Set<Rowkey>> keys);
	/** multi delete support
	 * this delete operations will be in on transaction
	 * @param mutators the mutators
	 * @param actionFlag action flag (OP_USE_OB_SEM | OP_USE_DB_SEM)
	 * @return
	 */
	public Result<Boolean> delete(Map<String, Set<Rowkey>> keys, ObActionFlag actionFlag);
	
	public Result<ObSchemaManagerV2> fetchSchema();
	
	// for test only
	
	/**
	 * clear the active memory table on updateserver
	 */
	public Result<Boolean> clearActiveMemTale();
}