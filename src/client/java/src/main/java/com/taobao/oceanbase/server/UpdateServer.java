package com.taobao.oceanbase.server;

import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.oceanbase.network.Server;
import com.taobao.oceanbase.network.exception.ConnectException;
import com.taobao.oceanbase.network.exception.EagainException;
import com.taobao.oceanbase.network.exception.TimeOutException;
import com.taobao.oceanbase.network.packet.BasePacket;
import com.taobao.oceanbase.network.packet.CommandPacket;
import com.taobao.oceanbase.network.packet.WritePacket;
import com.taobao.oceanbase.network.packet.WriteResponse;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.util.Helper;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.inner.ObActionFlag;
import com.taobao.oceanbase.vo.inner.ObCell;
import com.taobao.oceanbase.vo.inner.ObMutator;
import com.taobao.oceanbase.vo.inner.ObObj;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class UpdateServer {

	private Server server;
	private AtomicInteger request = new AtomicInteger(0);

	public UpdateServer(Server server) {
		this.server = server;
	}

	private int getRequest() {
		int seq = request.incrementAndGet();
		return seq != 0 ? seq : getRequest();
	}

//	public Result<Boolean> insert(InsertMutator mutator) {
//		ObMutator obMutator = new ObMutator(mutator.getColumns().size(),
//				ObActionFlag.OP_USE_OB_SEM);
//		for (InsertMutator.Tuple column : mutator.getColumns()) {
//			ObCell cell = new ObCell(mutator.getTable(), mutator.getRowkey(),
//					column.name, column.value.getObject(false));
//			obMutator.addCell(cell, ObActionFlag.OP_INSERT);
//		}
//		return apply(obMutator);
//	};
//
//	public Result<Boolean> update(UpdateMutator mutator) {
//		ObMutator obMutator = new ObMutator(mutator.getColumns().size(),
//				ObActionFlag.OP_USE_OB_SEM);
//		for (UpdateMutator.Tuple column : mutator.getColumns()) {
//			ObCell cell = new ObCell(mutator.getTable(), mutator.getRowkey(),
//					column.name, column.value.getObject(column.add));
//			obMutator.addCell(cell, ObActionFlag.OP_UPDATE);
//		}
//		return apply(obMutator);
//	};
//
//	public Result<Boolean> delete(String table, String rowkey) {
//		ObMutator mutator = new ObMutator(1, ObActionFlag.OP_USE_OB_SEM);
//		ObObj op = new ObObj();
//		op.setExtend(ObActionFlag.OP_DEL_ROW);
//		mutator.addCell(new ObCell(table, rowkey, null, op),
//				ObActionFlag.OP_DEL_ROW);
//		return apply(mutator);
//	};

	public Result<Boolean> apply(ObMutator mutator) {
		BasePacket packet = new WritePacket(getRequest(), mutator);
		WriteResponse response = null;
		try {
			response = server.commit(packet);
		} catch (ConnectException e) {
			Helper.sleep(Const.MIN_TIMEOUT);
			throw new TimeOutException("apply timeout" + e);
		}
		ResultCode code = ResultCode.getResultCode(response.getResultCode());
		
		if (code == ResultCode.OB_NOT_MASTER)
			throw new EagainException("write not on master,clean cache and try it again");
		return new Result<Boolean>(code, code == ResultCode.OB_SUCCESS);
	}
	
	public Result<Boolean> cleanActiveMemTable() {
		BasePacket packet = new CommandPacket(
				PacketCode.OB_CLEAR_ACTIVE_MEMTABLE, getRequest());
		WriteResponse res = server.commit(packet);
		ResultCode rc = ResultCode.getResultCode(res.getResultCode());
		return new Result<Boolean>(rc, rc == ResultCode.OB_SUCCESS);
	}

	@Override
	public String toString() {
		return server.toString();
	}
}