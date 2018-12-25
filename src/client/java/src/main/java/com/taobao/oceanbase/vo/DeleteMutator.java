package com.taobao.oceanbase.vo;

import java.util.ArrayList;
import java.util.List;

import com.taobao.oceanbase.util.CheckParameter;
import com.taobao.oceanbase.vo.inner.ObActionFlag;

public class DeleteMutator extends ObMutatorBase {

	private List<Tuple> columns = new ArrayList<Tuple>();

	public DeleteMutator(String table, Rowkey rowkey) {
		CheckParameter.checkTableName(table);
		CheckParameter.checkNull("rowkey is null", rowkey);
		this.table = table;
		this.rowkey = rowkey;
		this.flag = ObActionFlag.OP_DEL_ROW;
		Value value = new Value() {{setExtend(ObActionFlag.OP_DEL_ROW);}};
		columns.add(new Tuple(null, value));
	}

	public List<Tuple> getColumns() {
		return columns;
	}

}
