package com.taobao.oceanbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.network.SessionFactory;
import com.taobao.oceanbase.network.exception.ConnectException;
import com.taobao.oceanbase.network.exception.EagainException;
import com.taobao.oceanbase.network.exception.NoMergeServerException;
import com.taobao.oceanbase.network.exception.TimeOutException;
import com.taobao.oceanbase.network.mina.MinaSessionFactory;
import com.taobao.oceanbase.server.MergeServer;
import com.taobao.oceanbase.server.ObInstanceManager;
import com.taobao.oceanbase.server.RootServer;
import com.taobao.oceanbase.server.UpdateServer;
import com.taobao.oceanbase.util.CheckParameter;
import com.taobao.oceanbase.util.CheckQueryParams;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.util.Helper;
import com.taobao.oceanbase.vo.GetParam;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.ObMutatorBase;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Rowkey;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.inner.ObActionFlag;
import com.taobao.oceanbase.vo.inner.ObAggregateColumn;
import com.taobao.oceanbase.vo.inner.ObBorderFlag;
import com.taobao.oceanbase.vo.inner.ObCell;
import com.taobao.oceanbase.vo.inner.ObGetParam;
import com.taobao.oceanbase.vo.inner.ObGroupByParam;
import com.taobao.oceanbase.vo.inner.ObMutator;
import com.taobao.oceanbase.vo.inner.ObObj;
import com.taobao.oceanbase.vo.inner.ObRange;
import com.taobao.oceanbase.vo.inner.ObReadParam;
import com.taobao.oceanbase.vo.inner.ObRow;
import com.taobao.oceanbase.vo.inner.ObScanParam;
import com.taobao.oceanbase.vo.inner.ObAggregateColumn.ObAggregateFuncType;
import com.taobao.oceanbase.vo.inner.schema.ObSchemaManagerV2;

public class ClientImpl implements Client {

	private static final Log log = LogFactory.getLog(ClientImpl.class);

	private int port;
	private String ip;
	public RootServer root;
	private int timeout = Const.MIN_TIMEOUT;
	private List<String> instance_list = null;
	public SessionFactory factory = new MinaSessionFactory(); 
	private ObInstanceManager instanceManager;
	
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	public void setInstanceList(List<String> instance_list) {
		this.instance_list = instance_list;
	}

	public void init() {
		if (ip != null) {
			root = new RootServer(Helper.getServer(factory, ip, port,timeout));
			log.info("OceanBase client inited, rootserver address: " + ip + ":" + port);	
		} else {
			CheckParameter.checkCollection("instance list is null", instance_list);
			if (instance_list.size() > 2) {
				log.warn("For now,client just support master/slave arch");
			}
			instanceManager = new ObInstanceManager(factory,timeout,instance_list);
			if (instanceManager == null) 
				throw new RuntimeException("cann't get master instance");
			root = instanceManager.getInstance();
		}
	}

	public Result<Boolean> update(UpdateMutator mutator) {
		CheckParameter.checkNull("UpdateMutator is null", mutator);
		ObMutator obMutator = new ObMutator(mutator.getColumns().size(),
				ObActionFlag.OP_USE_OB_SEM);
		for (UpdateMutator.Tuple column : mutator.getColumns()) {
			ObCell cell = new ObCell(mutator.getTable(), mutator.getRowkey(),
					column.name, column.value.getObject(column.add));
			obMutator.addCell(cell, ObActionFlag.OP_UPDATE);
		}
		return apply(obMutator);
	}
	
	public Result<Boolean> update(List<ObMutatorBase> mutators){
		return update(mutators,ObActionFlag.OP_USE_OB_SEM);
	}
	
	public Result<Boolean> update(List<ObMutatorBase> mutators,ObActionFlag actionFlag){
		CheckParameter.checkCollection("mutators is null", mutators);
		int cap = 0;
		for (ObMutatorBase im : mutators){
			cap += im.getColumns().size();
			CheckParameter.checkNull("mutator is null", im);
		}
		
		ObMutator mutator = new ObMutator(cap, actionFlag);
		for (ObMutatorBase im : mutators) {
			for (ObMutatorBase.Tuple column : im.getColumns()) {
				ObCell cell = new ObCell(im.getTable(), im.getRowkey(),
						column.name, column.value.getObject(column.add));
				mutator.addCell(cell, im.getActionFlag());
			}
		}
		return apply(mutator);
	}
	
	public Result<Boolean> delete(String table, Rowkey rowkey) {
		CheckParameter.checkTableName(table);
		CheckParameter.checkNull("rowkey is null", rowkey);
		ObMutator mutator = new ObMutator(1, ObActionFlag.OP_USE_OB_SEM);
		ObObj op = new ObObj();
		op.setExtend(ObActionFlag.OP_DEL_ROW);
		mutator.addCell(new ObCell(table, rowkey.getRowkey(), null, op),ObActionFlag.OP_DEL_ROW);
		return apply(mutator);
	}
	
	public Result<Boolean> delete(Map<String, Set<Rowkey>> keys) {
		return delete(keys, ObActionFlag.OP_USE_OB_SEM);
	}
	
	public Result<Boolean> delete(Map<String, Set<Rowkey>> keys, ObActionFlag actionFlag) {
		CheckParameter.checkMap("rowkey is null", keys);
		
		int count = keys.size();
		ObMutator mutator = new ObMutator(count, actionFlag);
		for (String table : keys.keySet()) {
			for (Rowkey key : keys.get(table)) {
				ObObj op = new ObObj();
				op.setExtend(ObActionFlag.OP_DEL_ROW);
				ObCell cell = new ObCell(table, key.getRowkey(), null, op);
				mutator.addCell(cell, ObActionFlag.OP_DEL_ROW);
			}
		}
	
		return apply(mutator);
	}
	
	public Result<Boolean> insert(InsertMutator mutator) {
		CheckParameter.checkNull("InsertMutator is null", mutator);
		
		ObMutator obMutator = new ObMutator(mutator.getColumns().size(),
				ObActionFlag.OP_USE_OB_SEM);
		for (InsertMutator.Tuple column : mutator.getColumns()) {
			ObCell cell = new ObCell(mutator.getTable(), mutator.getRowkey(),
					column.name, column.value.getObject(false));
			obMutator.addCell(cell, ObActionFlag.OP_INSERT);
		}
		return apply(obMutator);
	}
	
	public Result<Boolean> insert(List<InsertMutator> mutators) {
		return insert(mutators, ObActionFlag.OP_USE_OB_SEM);
	}
	public Result<Boolean> insert(List<InsertMutator> mutators, ObActionFlag actionFlag) {
		CheckParameter.checkCollection("mutators is null",mutators);
		int cap = 0;
		for (InsertMutator im : mutators) {
			CheckParameter.checkNull("mutator is null", im);
			cap += im.getColumns().size();
		}
		
		ObMutator mutator = new ObMutator(cap, actionFlag);
		for (InsertMutator im : mutators) {
			for (InsertMutator.Tuple column : im.getColumns()) {
				ObCell cell = new ObCell(im.getTable(), im.getRowkey(),
						column.name, column.value.getObject(false));
				mutator.addCell(cell, ObActionFlag.OP_INSERT);
			}
		}
		return apply(mutator);
	}
	
	public Result<RowData> get(String table, Rowkey rowkey, Set<String> columns) {
		return get(table,rowkey,columns,false);
	}

	public Result<RowData> get(String table, Rowkey rowkey, Set<String> columns,boolean isReadConsistency) {
		CheckParameter.checkTableName(table);
		CheckParameter.checkNull("rowkey is null", rowkey);
		CheckParameter.checkCollection("columns are null", columns);
		CheckParameter.checkNullSetElement("columns have null column", columns);
		ObGetParam param = new ObGetParam(columns.size());
		param.setReadConsistency(isReadConsistency);
		for (String column : columns) {
			param.addCell(new ObCell(table, rowkey.getRowkey(), column, null));
		}
		Result<List<RowData>> ret = query(table,rowkey.getRowkey(),param);
		return new Result<RowData>(ret.getCode(),(ret.getResult() == null ||
												  ret.getResult().size() == 0 ||
												  ret.getResult().get(0).getRow().size() <= 0) ? null : ret.getResult().get(0));
	}
	
	public Result<List<RowData>> get(List<GetParam> getParams) {
		return get(getParams,false);
	}
	
	public Result<List<RowData>> get(List<GetParam> getParams,boolean isReadConsistency) {
		int count = 0;
		String table = null;
		String rowkey = null;
		for (GetParam getParam : getParams) {
			CheckParameter.checkTableName(getParam.getTableName());
			CheckParameter.checkNull("rowkey is null", getParam.getRowkey());			
			CheckParameter.checkCollection("columns are null", getParam.getColumns());
			CheckParameter.checkNullSetElement("columns have null column", getParam.getColumns());			
			count += getParam.getColumns().size();
			table = getParam.getTableName();
			rowkey = getParam.getRowkey().getRowkey();
		}
		
		ObGetParam getParam = new ObGetParam(count);
		for (GetParam gp : getParams) {
			for (String column : gp.getColumns()) {
				getParam.addCell(new ObCell(gp.getTableName(), gp.getRowkey().getRowkey(), column, null));
			}
		}
		getParam.setReadConsistency(isReadConsistency);
		return query(table,rowkey,getParam);
	}
	
	public Result<Long> count(String table, QueryInfo query) {
		if (query.getColumns().size() == 0) {
			query.addColumn(Const.ASTERISK);
		}
		CheckParameter.checkTableName(table);
		CheckQueryParams.check(query);
		ObScanParam param = query2ScanParam(table, query, true);
		return query(table,query.getStartKey(),param).getResult().get(0).get(Const.COUNT_AS_NAME);
	}
	
	public Result<List<RowData>> query(String table, QueryInfo query) {
		CheckParameter.checkTableName(table);
		CheckQueryParams.check(query);
		ObScanParam param = query2ScanParam(table, query, false);
		return query(table,query.getStartKey(),param);
	}

	private <T extends ObReadParam> Result<List<RowData>> query(String table,String rowkey,T param) {
		RootServer rs = null;
		if (instanceManager != null) {
			rs = instanceManager.getInstance(rowkey,param.isReadConsistency() != 0,null);
			if (rs == null ) {
				log.warn("cann't get rs");
			}
		}else
			rs = root;
		
		while (true) {
			if (rs == null)
				throw new RuntimeException("no mergeserver");

			MergeServer merge = rs.getMergeServer(factory, table,rowkey, timeout);
			if (merge == null) {
				rs = instanceManager.getInstance(rowkey, param.isReadConsistency() != 0,rs);
				continue;
			}
			try {
				Result<List<ObRow>> tmpResult = merge.query(param);
				List<RowData> rows = new ArrayList<RowData>(tmpResult
						.getResult().size());
				for (ObRow row : tmpResult.getResult()) {
					rows.add(Helper.getRowData(row));
				}
				Result<List<RowData>> result = new Result<List<RowData>>(
						tmpResult.getCode(), rows);
				return result;
			} catch (ConnectException e) {
				log.warn("query this mergeserver failed,try another");
				rs.remove(rowkey, merge);
			} catch (EagainException e) {
				rs = instanceManager.getInstance(rowkey, param.isReadConsistency() != 0,rs);
			} catch (TimeOutException e) {
				rs.remove(rowkey, merge);
			} catch (NoMergeServerException e) {
				if (instanceManager != null) {
					rs = instanceManager.getInstance(rowkey, param.isReadConsistency() != 0,rs);
				} else {
					throw new RuntimeException("no mergeserver");
				}
			}
		}
	}
	
	public Result<List<RowData>> scanTabletList(String table, QueryInfo query) {
		CheckParameter.checkTableName(table);
		CheckQueryParams.check(query);
		try {
			Result<List<ObRow>> tmpResult = root.scanTabletList(table, query);
			List<RowData> rows = new ArrayList<RowData>(tmpResult.getResult()
					.size());
			for (ObRow row : tmpResult.getResult()) {
				rows.add(Helper.getRowData(row));
			}
			return new Result<List<RowData>>(tmpResult.getCode(), rows);
		} catch (ConnectException e) {
			// for rootserver's scan tablet list, if connection error, just quit
			throw new RuntimeException(e);
		}
	}
	
	public Result<ObSchemaManagerV2> fetchSchema() {
		return root.getSchema(factory, timeout);
	}
	
	public Result<Boolean> clearActiveMemTale() {
		UpdateServer ups = root.getUpdateServer(factory, timeout);
		return ups.cleanActiveMemTable();		
	}

	public String toString(){
		return root.toString();
	}
	
	private ObScanParam query2ScanParam (String table,QueryInfo query,boolean isCount) {
		boolean start = query.isInclusiveStart();
		boolean end = query.isInclusiveEnd();
		boolean min = query.isMinValue();
		boolean max = query.isMaxValue();
		ObRange range = new ObRange(query.getStartKey(), query.getEndKey(),
				new ObBorderFlag(start, end, min, max));
		ObScanParam param = new ObScanParam();
		param.set(table, range);
		param.setReadConsistency(query.isReadConsistency());
		
		if (query.getPageNum() == 0) {
			param.setLimit(0, query.getLimit());
		} else {
			param.setLimit(query.getPageSize() * (query.getPageNum() - 1),
					Math.min(query.getLimit(), query.getPageSize()));
		}
		for (String column : query.getColumns()) {
			param.addColumns(column);
		}
		if (isCount) {
			ObGroupByParam gb = new ObGroupByParam();
			gb.addAggregateColumn(new ObAggregateColumn(Const.ASTERISK,
					Const.COUNT_AS_NAME, ObAggregateFuncType.COUNT));
			param.setGroupByParam(gb);
		}
		for (Map.Entry<String, Boolean> entry : query.getOrderBy().entrySet()) {
			param.addOrderBy(entry.getKey(), entry.getValue());
		}
		if (query.getFilter() != null) {
			if (query.getFilter().isValid()) {
				param.setFilter(query.getFilter());
			} else {
				throw new IllegalArgumentException("condition is invalid");
			}
		}
		param.setScanDirection(query.getScanFlag());
		return param;
	}
	

	private Result<Boolean> apply(ObMutator mutator) {
		UpdateServer update = null;
		Result<Boolean> ret = null;
		int retry_times = 0;
		while (true) {
			if (retry_times > 3)
				break;
			try {
				update = root.getUpdateServer(factory, timeout);
				ret = update.apply(mutator);
				break;
			} catch (EagainException e) {
				log.warn("try again" + e);
				if (instanceManager != null) {
					instanceManager.removeCache();
					root = instanceManager.getInstance();
				}else {
					break;
				}
			} catch (ConnectException e) {
				log.warn("cannot connect update server,try again");
				if (instanceManager != null) {
					instanceManager.removeCache();
					root = instanceManager.getInstance();
				} else {
					break;
				}
			} catch (TimeOutException e) {
				log.warn("timeout,try again");
				if (instanceManager != null) {
					instanceManager.removeCache();
					root = instanceManager.getInstance();
				}else {
					break;
				}
			}
			++retry_times;
		}
		return ret;
	}
	
}