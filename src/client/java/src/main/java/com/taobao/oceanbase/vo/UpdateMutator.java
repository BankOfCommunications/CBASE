package com.taobao.oceanbase.vo;

import java.util.ArrayList;
import java.util.List;

import com.taobao.oceanbase.util.CheckParameter;
import com.taobao.oceanbase.vo.inner.ObActionFlag;

public class UpdateMutator extends ObMutatorBase {

	private List<Tuple> columns = new ArrayList<Tuple>();

	public UpdateMutator(String table, Rowkey rowkey) {
		CheckParameter.checkTableName(table);
		CheckParameter.checkNull("rowkey is null", rowkey);
		this.table = table;
		this.rowkey = rowkey;
		this.flag = ObActionFlag.OP_UPDATE;
	}

	public void addColumn(String name, Value value, boolean add) {
		CheckParameter.checkColumnName(name);
		CheckParameter.checkNull("value null", value);
		CheckParameter.checkAddOperation(add, value);
		columns.add(new Tuple(name.toLowerCase(), value, add));
	}

	public List<Tuple> getColumns() {
		CheckParameter.checkCollection("columns null", columns);
		return columns;
	}

}