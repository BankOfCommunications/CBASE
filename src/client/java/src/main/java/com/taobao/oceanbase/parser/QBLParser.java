package com.taobao.oceanbase.parser;

import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;

import com.taobao.oceanbase.parser.OBQLWalker.update_command_return;

public class QBLParser {

	public static void main(String[] args) {
		String sql = "SELECT use_nick FROM collect_info WHERE ROWKEY = 12345";
		sql = "update collect_info set user_nick=hello where rowkey = 1234";
		AntlrStringStream input = new AntlrStringStream(sql);
		OBQLLexer lexer = new OBQLLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		OBQLParser parser = new OBQLParser(tokens);

		try {
			CommonTree t = (CommonTree) parser.update_command().getTree();
			CommonTreeNodeStream nodes = new CommonTreeNodeStream(t);
			OBQLWalker walker = new OBQLWalker(nodes);
			update_command_return cmd = walker.update_command();
			System.out.println(cmd);
		} catch (RecognitionException e) {
			System.err.println(e);
		}
	}
}