package com.taobao.oceanbase.vo;

import java.util.Collection;

public class Result<T> {

	private ResultCode code;

	private T result;

	public Result(ResultCode code, T result) {
		this.code = code;
		this.result = result;
	}

	public ResultCode getCode() {
		return code;
	}

	public T getResult() {
		return result;
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder("ResultCode ");
		res.append(code);

		if (result != null) {
			res.append("\n");
		}

		if (result instanceof Collection) {
			Collection<RowData> rows = (Collection<RowData>) result;
			for (RowData r : rows) {
				res.append(r);
			}
		} else {
			res.append(result);
		}
		return res.toString();

	}

}