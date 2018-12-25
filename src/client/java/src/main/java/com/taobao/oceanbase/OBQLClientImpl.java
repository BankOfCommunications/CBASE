package com.taobao.oceanbase;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;

import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.parser.AntlrStringStream;
import com.taobao.oceanbase.parser.OBQLLexer;
import com.taobao.oceanbase.parser.OBQLParser;
import com.taobao.oceanbase.parser.OBQLWalker;
import com.taobao.oceanbase.parser.OBQLWalker.delete_command_return;
import com.taobao.oceanbase.parser.OBQLWalker.insert_command_return;
import com.taobao.oceanbase.parser.OBQLWalker.select_command_return;
import com.taobao.oceanbase.parser.OBQLWalker.update_command_return;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Rowkey;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.Value;

public class OBQLClientImpl extends ClientImpl implements OBQLClient {

	public Result<List<RowData>> query(String OBQL) throws Exception {
		AntlrStringStream input = new AntlrStringStream(OBQL);
		OBQLLexer lexer = new OBQLLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		OBQLParser parser1 = new OBQLParser(tokens);
		CommonTree t = (CommonTree) parser1.select_command().getTree();
		CommonTreeNodeStream nodes = new CommonTreeNodeStream(t);
		OBQLWalker parser = new OBQLWalker(nodes);
		select_command_return cmd = parser.select_command();
		long user = cmd.user;
		String table = cmd.table;
		List<String> columnList = cmd.names;

		QueryInfo query = new QueryInfo();
		query.setInclusiveStart(true);
		query.setInclusiveEnd(true);
		if (columnList != null) {
			for (String column : columnList)
				query.addColumn(column);
		}
		query.setStartKey(getKey(user, true)).setEndKey(getKey(user, false));
		return this.query(table, query);
	}

	private static Rowkey getKey(final long user, final boolean begin)
			throws UnsupportedEncodingException {
		return new Rowkey(){
			public byte[] getBytes() {
				ByteBuffer buffer = ByteBuffer.allocate(17);
				buffer.put(Serialization.encode(user));
				for (int index = 0; index < 9; index++) {
					buffer = begin ? buffer.put((byte) 0) : buffer.put((byte) -1);
				}
				return buffer.array();
			}
		};
	}

	public Result<Boolean> insert(String OBQL) throws Exception {
		AntlrStringStream input = new AntlrStringStream(OBQL);
		OBQLLexer lexer = new OBQLLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		OBQLParser parser1 = new OBQLParser(tokens);
		CommonTree t = (CommonTree) parser1.insert_command().getTree();
		CommonTreeNodeStream nodes = new CommonTreeNodeStream(t);
		OBQLWalker parser = new OBQLWalker(nodes);
		insert_command_return cmd = parser.insert_command();
		String table = cmd.table;
		long user = cmd.user;
		List<String> names = cmd.names;
		List<String> values = cmd.values;
		if (names.size() != values.size())
			throw new RuntimeException("列名/值个数不匹配");
		InsertMutator mutator = new InsertMutator(table, getKey(user, true));
		for (int n = 0; n < names.size(); n++) {
			Value value = new Value();
			value.setString(values.get(n));
			mutator.addColumn(names.get(n), value);
		}
		return this.insert(mutator);
	}

	public Result<Boolean> update(String OBQL) throws Exception {
		AntlrStringStream input = new AntlrStringStream(OBQL);
		OBQLLexer lexer = new OBQLLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		OBQLParser parser1 = new OBQLParser(tokens);
		CommonTree t = (CommonTree) parser1.update_command().getTree();
		CommonTreeNodeStream nodes = new CommonTreeNodeStream(t);
		OBQLWalker parser = new OBQLWalker(nodes);
		update_command_return cmd = parser.update_command();
		String table = cmd.table;
		long user = cmd.user;
		UpdateMutator mutator = new UpdateMutator(table, getKey(user, true));
		Map<String, String> pairs = cmd.kvs;
		for (Map.Entry<String, String> pair : pairs.entrySet()) {
			Value value = new Value();
			value.setString(pair.getValue());
			mutator.addColumn(pair.getKey(), value, false);
		}
		return this.update(mutator);
	}

	public Result<Boolean> delete(String OBQL) throws Exception {
		AntlrStringStream input = new AntlrStringStream(OBQL);
		OBQLLexer lexer = new OBQLLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		OBQLParser parser1 = new OBQLParser(tokens);
		CommonTree t = (CommonTree) parser1.delete_command().getTree();
		CommonTreeNodeStream nodes = new CommonTreeNodeStream(t);
		OBQLWalker parser = new OBQLWalker(nodes);
		delete_command_return cmd = parser.delete_command();
		String table = cmd.table;
		long user = cmd.user;
		return this.delete(table, getKey(user, true));
	}
}