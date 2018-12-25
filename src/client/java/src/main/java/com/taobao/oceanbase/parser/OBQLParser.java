// $ANTLR 3.2 Sep 23, 2009 12:02:23 D:\\gfile\\OBQL.g 2010-10-29 21:05:26

package com.taobao.oceanbase.parser;

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

import org.antlr.runtime.tree.*;

public class OBQLParser extends Parser {
	public static final String[] tokenNames = new String[] { "<invalid>",
			"<EOR>", "<DOWN>", "<UP>", "TABLE", "ORDERBY", "GROUPBY", "LIMIT",
			"COLUMNLIST", "ORDERLIST", "ASC", "DESC", "ROWKEY", "COMMA", "EQ",
			"LT", "GT", "LE", "GE", "IDENTIFIER", "INT", "WS", "'SELECT'",
			"'FROM'", "'WHERE'", "'ORDER'", "'BY'", "'LIMIT'", "'AND'",
			"'UPDATE'", "'SET'", "'INSERT'", "'INTO'", "'('", "')'",
			"'VALUES'", "'DELETE'" };
	public static final int GE = 18;
	public static final int LT = 15;
	public static final int T__29 = 29;
	public static final int T__28 = 28;
	public static final int T__27 = 27;
	public static final int ASC = 10;
	public static final int T__26 = 26;
	public static final int T__25 = 25;
	public static final int T__24 = 24;
	public static final int LIMIT = 7;
	public static final int T__23 = 23;
	public static final int T__22 = 22;
	public static final int TABLE = 4;
	public static final int INT = 20;
	public static final int ORDERBY = 5;
	public static final int ROWKEY = 12;
	public static final int EOF = -1;
	public static final int T__30 = 30;
	public static final int T__31 = 31;
	public static final int T__32 = 32;
	public static final int WS = 21;
	public static final int T__33 = 33;
	public static final int T__34 = 34;
	public static final int T__35 = 35;
	public static final int T__36 = 36;
	public static final int COMMA = 13;
	public static final int IDENTIFIER = 19;
	public static final int COLUMNLIST = 8;
	public static final int GT = 16;
	public static final int EQ = 14;
	public static final int DESC = 11;
	public static final int LE = 17;
	public static final int GROUPBY = 6;
	public static final int ORDERLIST = 9;

	// delegates
	// delegators

	public OBQLParser(TokenStream input) {
		this(input, new RecognizerSharedState());
	}

	public OBQLParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);

	}

	protected TreeAdaptor adaptor = new CommonTreeAdaptor();

	public void setTreeAdaptor(TreeAdaptor adaptor) {
		this.adaptor = adaptor;
	}

	public TreeAdaptor getTreeAdaptor() {
		return adaptor;
	}

	public String[] getTokenNames() {
		return OBQLParser.tokenNames;
	}

	public String getGrammarFileName() {
		return "D:\\gfile\\OBQL.g";
	}

	public static class select_command_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "select_command"
	// D:\\gfile\\OBQL.g:43:1: select_command : select_clause from_clause
	// where_clause ( orderby_clause )? ( limit_clause )? ;
	public final OBQLParser.select_command_return select_command()
			throws RecognitionException {
		OBQLParser.select_command_return retval = new OBQLParser.select_command_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		OBQLParser.select_clause_return select_clause1 = null;

		OBQLParser.from_clause_return from_clause2 = null;

		OBQLParser.where_clause_return where_clause3 = null;

		OBQLParser.orderby_clause_return orderby_clause4 = null;

		OBQLParser.limit_clause_return limit_clause5 = null;

		try {
			// D:\\gfile\\OBQL.g:44:2: ( select_clause from_clause where_clause
			// ( orderby_clause )? ( limit_clause )? )
			// D:\\gfile\\OBQL.g:44:4: select_clause from_clause where_clause (
			// orderby_clause )? ( limit_clause )?
			{
				root_0 = (CommonTree) adaptor.nil();

				pushFollow(FOLLOW_select_clause_in_select_command234);
				select_clause1 = select_clause();

				state._fsp--;

				adaptor.addChild(root_0, select_clause1.getTree());
				pushFollow(FOLLOW_from_clause_in_select_command236);
				from_clause2 = from_clause();

				state._fsp--;

				adaptor.addChild(root_0, from_clause2.getTree());
				pushFollow(FOLLOW_where_clause_in_select_command238);
				where_clause3 = where_clause();

				state._fsp--;

				adaptor.addChild(root_0, where_clause3.getTree());
				// D:\\gfile\\OBQL.g:44:43: ( orderby_clause )?
				int alt1 = 2;
				int LA1_0 = input.LA(1);

				if ((LA1_0 == 25)) {
					alt1 = 1;
				}
				switch (alt1) {
				case 1:
					// D:\\gfile\\OBQL.g:44:44: orderby_clause
				{
					pushFollow(FOLLOW_orderby_clause_in_select_command241);
					orderby_clause4 = orderby_clause();

					state._fsp--;

					adaptor.addChild(root_0, orderby_clause4.getTree());

				}
					break;

				}

				// D:\\gfile\\OBQL.g:44:61: ( limit_clause )?
				int alt2 = 2;
				int LA2_0 = input.LA(1);

				if ((LA2_0 == 27)) {
					alt2 = 1;
				}
				switch (alt2) {
				case 1:
					// D:\\gfile\\OBQL.g:44:62: limit_clause
				{
					pushFollow(FOLLOW_limit_clause_in_select_command246);
					limit_clause5 = limit_clause();

					state._fsp--;

					adaptor.addChild(root_0, limit_clause5.getTree());

				}
					break;

				}

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "select_command"

	public static class select_clause_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "select_clause"
	// D:\\gfile\\OBQL.g:47:1: select_clause : 'SELECT' column_list -> ^(
	// 'SELECT' column_list ) ;
	public final OBQLParser.select_clause_return select_clause()
			throws RecognitionException {
		OBQLParser.select_clause_return retval = new OBQLParser.select_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token string_literal6 = null;
		OBQLParser.column_list_return column_list7 = null;

		CommonTree string_literal6_tree = null;
		RewriteRuleTokenStream stream_22 = new RewriteRuleTokenStream(adaptor,
				"token 22");
		RewriteRuleSubtreeStream stream_column_list = new RewriteRuleSubtreeStream(
				adaptor, "rule column_list");
		try {
			// D:\\gfile\\OBQL.g:48:2: ( 'SELECT' column_list -> ^( 'SELECT'
			// column_list ) )
			// D:\\gfile\\OBQL.g:48:3: 'SELECT' column_list
			{
				string_literal6 = (Token) match(input, 22,
						FOLLOW_22_in_select_clause261);
				stream_22.add(string_literal6);

				pushFollow(FOLLOW_column_list_in_select_clause263);
				column_list7 = column_list();

				state._fsp--;

				stream_column_list.add(column_list7.getTree());

				// AST REWRITE
				// elements: 22, column_list
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 48:24: -> ^( 'SELECT' column_list )
				{
					// D:\\gfile\\OBQL.g:48:27: ^( 'SELECT' column_list )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_22.nextNode(), root_1);

						adaptor.addChild(root_1, stream_column_list.nextTree());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "select_clause"

	public static class from_clause_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "from_clause"
	// D:\\gfile\\OBQL.g:51:1: from_clause : 'FROM' table_name ;
	public final OBQLParser.from_clause_return from_clause()
			throws RecognitionException {
		OBQLParser.from_clause_return retval = new OBQLParser.from_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token string_literal8 = null;
		OBQLParser.table_name_return table_name9 = null;

		CommonTree string_literal8_tree = null;

		try {
			// D:\\gfile\\OBQL.g:52:2: ( 'FROM' table_name )
			// D:\\gfile\\OBQL.g:52:3: 'FROM' table_name
			{
				root_0 = (CommonTree) adaptor.nil();

				string_literal8 = (Token) match(input, 23,
						FOLLOW_23_in_from_clause282);
				pushFollow(FOLLOW_table_name_in_from_clause285);
				table_name9 = table_name();

				state._fsp--;

				adaptor.addChild(root_0, table_name9.getTree());

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "from_clause"

	public static class where_clause_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "where_clause"
	// D:\\gfile\\OBQL.g:55:1: where_clause : 'WHERE' query_conditions -> ^(
	// 'WHERE' query_conditions ) ;
	public final OBQLParser.where_clause_return where_clause()
			throws RecognitionException {
		OBQLParser.where_clause_return retval = new OBQLParser.where_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token string_literal10 = null;
		OBQLParser.query_conditions_return query_conditions11 = null;

		CommonTree string_literal10_tree = null;
		RewriteRuleTokenStream stream_24 = new RewriteRuleTokenStream(adaptor,
				"token 24");
		RewriteRuleSubtreeStream stream_query_conditions = new RewriteRuleSubtreeStream(
				adaptor, "rule query_conditions");
		try {
			// D:\\gfile\\OBQL.g:56:2: ( 'WHERE' query_conditions -> ^( 'WHERE'
			// query_conditions ) )
			// D:\\gfile\\OBQL.g:56:3: 'WHERE' query_conditions
			{
				string_literal10 = (Token) match(input, 24,
						FOLLOW_24_in_where_clause296);
				stream_24.add(string_literal10);

				pushFollow(FOLLOW_query_conditions_in_where_clause298);
				query_conditions11 = query_conditions();

				state._fsp--;

				stream_query_conditions.add(query_conditions11.getTree());

				// AST REWRITE
				// elements: 24, query_conditions
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 56:28: -> ^( 'WHERE' query_conditions )
				{
					// D:\\gfile\\OBQL.g:56:31: ^( 'WHERE' query_conditions )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_24.nextNode(), root_1);

						adaptor.addChild(root_1,
								stream_query_conditions.nextTree());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "where_clause"

	public static class orderby_clause_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "orderby_clause"
	// D:\\gfile\\OBQL.g:59:1: orderby_clause : 'ORDER' 'BY' order_column_list
	// -> ^( ORDERBY order_column_list ) ;
	public final OBQLParser.orderby_clause_return orderby_clause()
			throws RecognitionException {
		OBQLParser.orderby_clause_return retval = new OBQLParser.orderby_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token string_literal12 = null;
		Token string_literal13 = null;
		OBQLParser.order_column_list_return order_column_list14 = null;

		CommonTree string_literal12_tree = null;
		CommonTree string_literal13_tree = null;
		RewriteRuleTokenStream stream_25 = new RewriteRuleTokenStream(adaptor,
				"token 25");
		RewriteRuleTokenStream stream_26 = new RewriteRuleTokenStream(adaptor,
				"token 26");
		RewriteRuleSubtreeStream stream_order_column_list = new RewriteRuleSubtreeStream(
				adaptor, "rule order_column_list");
		try {
			// D:\\gfile\\OBQL.g:60:2: ( 'ORDER' 'BY' order_column_list -> ^(
			// ORDERBY order_column_list ) )
			// D:\\gfile\\OBQL.g:60:3: 'ORDER' 'BY' order_column_list
			{
				string_literal12 = (Token) match(input, 25,
						FOLLOW_25_in_orderby_clause316);
				stream_25.add(string_literal12);

				string_literal13 = (Token) match(input, 26,
						FOLLOW_26_in_orderby_clause318);
				stream_26.add(string_literal13);

				pushFollow(FOLLOW_order_column_list_in_orderby_clause320);
				order_column_list14 = order_column_list();

				state._fsp--;

				stream_order_column_list.add(order_column_list14.getTree());

				// AST REWRITE
				// elements: order_column_list
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 60:34: -> ^( ORDERBY order_column_list )
				{
					// D:\\gfile\\OBQL.g:60:37: ^( ORDERBY order_column_list )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor
								.becomeRoot((CommonTree) adaptor.create(
										ORDERBY, "ORDERBY"), root_1);

						adaptor.addChild(root_1,
								stream_order_column_list.nextTree());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "orderby_clause"

	public static class limit_clause_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "limit_clause"
	// D:\\gfile\\OBQL.g:63:1: limit_clause : 'LIMIT' ( offset COMMA )? count ->
	// ^( LIMIT ( offset )? count ) ;
	public final OBQLParser.limit_clause_return limit_clause()
			throws RecognitionException {
		OBQLParser.limit_clause_return retval = new OBQLParser.limit_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token string_literal15 = null;
		Token COMMA17 = null;
		OBQLParser.offset_return offset16 = null;

		OBQLParser.count_return count18 = null;

		CommonTree string_literal15_tree = null;
		CommonTree COMMA17_tree = null;
		RewriteRuleTokenStream stream_COMMA = new RewriteRuleTokenStream(
				adaptor, "token COMMA");
		RewriteRuleTokenStream stream_27 = new RewriteRuleTokenStream(adaptor,
				"token 27");
		RewriteRuleSubtreeStream stream_count = new RewriteRuleSubtreeStream(
				adaptor, "rule count");
		RewriteRuleSubtreeStream stream_offset = new RewriteRuleSubtreeStream(
				adaptor, "rule offset");
		try {
			// D:\\gfile\\OBQL.g:64:2: ( 'LIMIT' ( offset COMMA )? count -> ^(
			// LIMIT ( offset )? count ) )
			// D:\\gfile\\OBQL.g:64:4: 'LIMIT' ( offset COMMA )? count
			{
				string_literal15 = (Token) match(input, 27,
						FOLLOW_27_in_limit_clause341);
				stream_27.add(string_literal15);

				// D:\\gfile\\OBQL.g:64:12: ( offset COMMA )?
				int alt3 = 2;
				int LA3_0 = input.LA(1);

				if ((LA3_0 == INT)) {
					int LA3_1 = input.LA(2);

					if ((LA3_1 == COMMA)) {
						alt3 = 1;
					}
				}
				switch (alt3) {
				case 1:
					// D:\\gfile\\OBQL.g:64:13: offset COMMA
				{
					pushFollow(FOLLOW_offset_in_limit_clause344);
					offset16 = offset();

					state._fsp--;

					stream_offset.add(offset16.getTree());
					COMMA17 = (Token) match(input, COMMA,
							FOLLOW_COMMA_in_limit_clause346);
					stream_COMMA.add(COMMA17);

				}
					break;

				}

				pushFollow(FOLLOW_count_in_limit_clause350);
				count18 = count();

				state._fsp--;

				stream_count.add(count18.getTree());

				// AST REWRITE
				// elements: offset, count
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 64:34: -> ^( LIMIT ( offset )? count )
				{
					// D:\\gfile\\OBQL.g:64:37: ^( LIMIT ( offset )? count )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								(CommonTree) adaptor.create(LIMIT, "LIMIT"),
								root_1);

						// D:\\gfile\\OBQL.g:64:45: ( offset )?
						if (stream_offset.hasNext()) {
							adaptor.addChild(root_1, stream_offset.nextTree());

						}
						stream_offset.reset();
						adaptor.addChild(root_1, stream_count.nextTree());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "limit_clause"

	public static class query_conditions_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "query_conditions"
	// D:\\gfile\\OBQL.g:67:1: query_conditions : query_condition ( 'AND'
	// query_condition )? ;
	public final OBQLParser.query_conditions_return query_conditions()
			throws RecognitionException {
		OBQLParser.query_conditions_return retval = new OBQLParser.query_conditions_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token string_literal20 = null;
		OBQLParser.query_condition_return query_condition19 = null;

		OBQLParser.query_condition_return query_condition21 = null;

		CommonTree string_literal20_tree = null;

		try {
			// D:\\gfile\\OBQL.g:68:2: ( query_condition ( 'AND' query_condition
			// )? )
			// D:\\gfile\\OBQL.g:68:4: query_condition ( 'AND' query_condition
			// )?
			{
				root_0 = (CommonTree) adaptor.nil();

				pushFollow(FOLLOW_query_condition_in_query_conditions372);
				query_condition19 = query_condition();

				state._fsp--;

				adaptor.addChild(root_0, query_condition19.getTree());
				// D:\\gfile\\OBQL.g:68:20: ( 'AND' query_condition )?
				int alt4 = 2;
				int LA4_0 = input.LA(1);

				if ((LA4_0 == 28)) {
					alt4 = 1;
				}
				switch (alt4) {
				case 1:
					// D:\\gfile\\OBQL.g:68:21: 'AND' query_condition
				{
					string_literal20 = (Token) match(input, 28,
							FOLLOW_28_in_query_conditions375);
					pushFollow(FOLLOW_query_condition_in_query_conditions378);
					query_condition21 = query_condition();

					state._fsp--;

					adaptor.addChild(root_0, query_condition21.getTree());

				}
					break;

				}

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "query_conditions"

	public static class query_condition_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "query_condition"
	// D:\\gfile\\OBQL.g:71:1: query_condition : ROWKEY op_type INT -> ^(
	// op_type INT ) ;
	public final OBQLParser.query_condition_return query_condition()
			throws RecognitionException {
		OBQLParser.query_condition_return retval = new OBQLParser.query_condition_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token ROWKEY22 = null;
		Token INT24 = null;
		OBQLParser.op_type_return op_type23 = null;

		CommonTree ROWKEY22_tree = null;
		CommonTree INT24_tree = null;
		RewriteRuleTokenStream stream_INT = new RewriteRuleTokenStream(adaptor,
				"token INT");
		RewriteRuleTokenStream stream_ROWKEY = new RewriteRuleTokenStream(
				adaptor, "token ROWKEY");
		RewriteRuleSubtreeStream stream_op_type = new RewriteRuleSubtreeStream(
				adaptor, "rule op_type");
		try {
			// D:\\gfile\\OBQL.g:72:2: ( ROWKEY op_type INT -> ^( op_type INT )
			// )
			// D:\\gfile\\OBQL.g:72:4: ROWKEY op_type INT
			{
				ROWKEY22 = (Token) match(input, ROWKEY,
						FOLLOW_ROWKEY_in_query_condition392);
				stream_ROWKEY.add(ROWKEY22);

				pushFollow(FOLLOW_op_type_in_query_condition394);
				op_type23 = op_type();

				state._fsp--;

				stream_op_type.add(op_type23.getTree());
				INT24 = (Token) match(input, INT,
						FOLLOW_INT_in_query_condition396);
				stream_INT.add(INT24);

				// AST REWRITE
				// elements: INT, op_type
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 72:23: -> ^( op_type INT )
				{
					// D:\\gfile\\OBQL.g:72:26: ^( op_type INT )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_op_type.nextNode(), root_1);

						adaptor.addChild(root_1, stream_INT.nextNode());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "query_condition"

	public static class column_name_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "column_name"
	// D:\\gfile\\OBQL.g:75:1: column_name : IDENTIFIER ;
	public final OBQLParser.column_name_return column_name()
			throws RecognitionException {
		OBQLParser.column_name_return retval = new OBQLParser.column_name_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token IDENTIFIER25 = null;

		CommonTree IDENTIFIER25_tree = null;

		try {
			// D:\\gfile\\OBQL.g:76:2: ( IDENTIFIER )
			// D:\\gfile\\OBQL.g:76:4: IDENTIFIER
			{
				root_0 = (CommonTree) adaptor.nil();

				IDENTIFIER25 = (Token) match(input, IDENTIFIER,
						FOLLOW_IDENTIFIER_in_column_name416);
				IDENTIFIER25_tree = (CommonTree) adaptor.create(IDENTIFIER25);
				adaptor.addChild(root_0, IDENTIFIER25_tree);

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "column_name"

	public static class table_name_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "table_name"
	// D:\\gfile\\OBQL.g:79:1: table_name : IDENTIFIER -> ^( TABLE IDENTIFIER )
	// ;
	public final OBQLParser.table_name_return table_name()
			throws RecognitionException {
		OBQLParser.table_name_return retval = new OBQLParser.table_name_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token IDENTIFIER26 = null;

		CommonTree IDENTIFIER26_tree = null;
		RewriteRuleTokenStream stream_IDENTIFIER = new RewriteRuleTokenStream(
				adaptor, "token IDENTIFIER");

		try {
			// D:\\gfile\\OBQL.g:80:2: ( IDENTIFIER -> ^( TABLE IDENTIFIER ) )
			// D:\\gfile\\OBQL.g:80:4: IDENTIFIER
			{
				IDENTIFIER26 = (Token) match(input, IDENTIFIER,
						FOLLOW_IDENTIFIER_in_table_name428);
				stream_IDENTIFIER.add(IDENTIFIER26);

				// AST REWRITE
				// elements: IDENTIFIER
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 80:15: -> ^( TABLE IDENTIFIER )
				{
					// D:\\gfile\\OBQL.g:80:18: ^( TABLE IDENTIFIER )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								(CommonTree) adaptor.create(TABLE, "TABLE"),
								root_1);

						adaptor.addChild(root_1, stream_IDENTIFIER.nextNode());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "table_name"

	public static class column_list_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "column_list"
	// D:\\gfile\\OBQL.g:83:1: column_list : column_name ( COMMA column_name )*
	// -> ^( COLUMNLIST ( column_name )+ ) ;
	public final OBQLParser.column_list_return column_list()
			throws RecognitionException {
		OBQLParser.column_list_return retval = new OBQLParser.column_list_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token COMMA28 = null;
		OBQLParser.column_name_return column_name27 = null;

		OBQLParser.column_name_return column_name29 = null;

		CommonTree COMMA28_tree = null;
		RewriteRuleTokenStream stream_COMMA = new RewriteRuleTokenStream(
				adaptor, "token COMMA");
		RewriteRuleSubtreeStream stream_column_name = new RewriteRuleSubtreeStream(
				adaptor, "rule column_name");
		try {
			// D:\\gfile\\OBQL.g:84:2: ( column_name ( COMMA column_name )* ->
			// ^( COLUMNLIST ( column_name )+ ) )
			// D:\\gfile\\OBQL.g:84:4: column_name ( COMMA column_name )*
			{
				pushFollow(FOLLOW_column_name_in_column_list448);
				column_name27 = column_name();

				state._fsp--;

				stream_column_name.add(column_name27.getTree());
				// D:\\gfile\\OBQL.g:84:16: ( COMMA column_name )*
				loop5: do {
					int alt5 = 2;
					int LA5_0 = input.LA(1);

					if ((LA5_0 == COMMA)) {
						alt5 = 1;
					}

					switch (alt5) {
					case 1:
						// D:\\gfile\\OBQL.g:84:17: COMMA column_name
					{
						COMMA28 = (Token) match(input, COMMA,
								FOLLOW_COMMA_in_column_list451);
						stream_COMMA.add(COMMA28);

						pushFollow(FOLLOW_column_name_in_column_list453);
						column_name29 = column_name();

						state._fsp--;

						stream_column_name.add(column_name29.getTree());

					}
						break;

					default:
						break loop5;
					}
				} while (true);

				// AST REWRITE
				// elements: column_name
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 84:37: -> ^( COLUMNLIST ( column_name )+ )
				{
					// D:\\gfile\\OBQL.g:84:40: ^( COLUMNLIST ( column_name )+ )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								(CommonTree) adaptor.create(COLUMNLIST,
										"COLUMNLIST"), root_1);

						if (!(stream_column_name.hasNext())) {
							throw new RewriteEarlyExitException();
						}
						while (stream_column_name.hasNext()) {
							adaptor.addChild(root_1,
									stream_column_name.nextTree());

						}
						stream_column_name.reset();

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "column_list"

	public static class order_column_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "order_column"
	// D:\\gfile\\OBQL.g:87:1: order_column : ( column_name -> ^( ASC
	// column_name ) | column_name ASC -> ^( ASC column_name ) | column_name
	// DESC -> ^( DESC column_name ) );
	public final OBQLParser.order_column_return order_column()
			throws RecognitionException {
		OBQLParser.order_column_return retval = new OBQLParser.order_column_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token ASC32 = null;
		Token DESC34 = null;
		OBQLParser.column_name_return column_name30 = null;

		OBQLParser.column_name_return column_name31 = null;

		OBQLParser.column_name_return column_name33 = null;

		CommonTree ASC32_tree = null;
		CommonTree DESC34_tree = null;
		RewriteRuleTokenStream stream_DESC = new RewriteRuleTokenStream(
				adaptor, "token DESC");
		RewriteRuleTokenStream stream_ASC = new RewriteRuleTokenStream(adaptor,
				"token ASC");
		RewriteRuleSubtreeStream stream_column_name = new RewriteRuleSubtreeStream(
				adaptor, "rule column_name");
		try {
			// D:\\gfile\\OBQL.g:88:2: ( column_name -> ^( ASC column_name ) |
			// column_name ASC -> ^( ASC column_name ) | column_name DESC -> ^(
			// DESC column_name ) )
			int alt6 = 3;
			int LA6_0 = input.LA(1);

			if ((LA6_0 == IDENTIFIER)) {
				switch (input.LA(2)) {
				case DESC: {
					alt6 = 3;
				}
					break;
				case ASC: {
					alt6 = 2;
				}
					break;
				case EOF:
				case COMMA:
				case 27: {
					alt6 = 1;
				}
					break;
				default:
					NoViableAltException nvae = new NoViableAltException("", 6,
							1, input);

					throw nvae;
				}

			} else {
				NoViableAltException nvae = new NoViableAltException("", 6, 0,
						input);

				throw nvae;
			}
			switch (alt6) {
			case 1:
				// D:\\gfile\\OBQL.g:88:4: column_name
			{
				pushFollow(FOLLOW_column_name_in_order_column476);
				column_name30 = column_name();

				state._fsp--;

				stream_column_name.add(column_name30.getTree());

				// AST REWRITE
				// elements: column_name
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 88:16: -> ^( ASC column_name )
				{
					// D:\\gfile\\OBQL.g:88:19: ^( ASC column_name )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor
								.becomeRoot(
										(CommonTree) adaptor.create(ASC, "ASC"),
										root_1);

						adaptor.addChild(root_1, stream_column_name.nextTree());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}
				break;
			case 2:
				// D:\\gfile\\OBQL.g:89:4: column_name ASC
			{
				pushFollow(FOLLOW_column_name_in_order_column489);
				column_name31 = column_name();

				state._fsp--;

				stream_column_name.add(column_name31.getTree());
				ASC32 = (Token) match(input, ASC, FOLLOW_ASC_in_order_column491);
				stream_ASC.add(ASC32);

				// AST REWRITE
				// elements: column_name, ASC
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 89:20: -> ^( ASC column_name )
				{
					// D:\\gfile\\OBQL.g:89:23: ^( ASC column_name )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_ASC.nextNode(), root_1);

						adaptor.addChild(root_1, stream_column_name.nextTree());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}
				break;
			case 3:
				// D:\\gfile\\OBQL.g:90:4: column_name DESC
			{
				pushFollow(FOLLOW_column_name_in_order_column504);
				column_name33 = column_name();

				state._fsp--;

				stream_column_name.add(column_name33.getTree());
				DESC34 = (Token) match(input, DESC,
						FOLLOW_DESC_in_order_column506);
				stream_DESC.add(DESC34);

				// AST REWRITE
				// elements: DESC, column_name
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 90:21: -> ^( DESC column_name )
				{
					// D:\\gfile\\OBQL.g:90:24: ^( DESC column_name )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_DESC.nextNode(), root_1);

						adaptor.addChild(root_1, stream_column_name.nextTree());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}
				break;

			}
			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "order_column"

	public static class order_column_list_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "order_column_list"
	// D:\\gfile\\OBQL.g:93:1: order_column_list : order_column ( COMMA
	// order_column )* -> ^( ORDERLIST ( order_column )+ ) ;
	public final OBQLParser.order_column_list_return order_column_list()
			throws RecognitionException {
		OBQLParser.order_column_list_return retval = new OBQLParser.order_column_list_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token COMMA36 = null;
		OBQLParser.order_column_return order_column35 = null;

		OBQLParser.order_column_return order_column37 = null;

		CommonTree COMMA36_tree = null;
		RewriteRuleTokenStream stream_COMMA = new RewriteRuleTokenStream(
				adaptor, "token COMMA");
		RewriteRuleSubtreeStream stream_order_column = new RewriteRuleSubtreeStream(
				adaptor, "rule order_column");
		try {
			// D:\\gfile\\OBQL.g:94:2: ( order_column ( COMMA order_column )* ->
			// ^( ORDERLIST ( order_column )+ ) )
			// D:\\gfile\\OBQL.g:94:4: order_column ( COMMA order_column )*
			{
				pushFollow(FOLLOW_order_column_in_order_column_list525);
				order_column35 = order_column();

				state._fsp--;

				stream_order_column.add(order_column35.getTree());
				// D:\\gfile\\OBQL.g:94:17: ( COMMA order_column )*
				loop7: do {
					int alt7 = 2;
					int LA7_0 = input.LA(1);

					if ((LA7_0 == COMMA)) {
						alt7 = 1;
					}

					switch (alt7) {
					case 1:
						// D:\\gfile\\OBQL.g:94:18: COMMA order_column
					{
						COMMA36 = (Token) match(input, COMMA,
								FOLLOW_COMMA_in_order_column_list528);
						stream_COMMA.add(COMMA36);

						pushFollow(FOLLOW_order_column_in_order_column_list530);
						order_column37 = order_column();

						state._fsp--;

						stream_order_column.add(order_column37.getTree());

					}
						break;

					default:
						break loop7;
					}
				} while (true);

				// AST REWRITE
				// elements: order_column
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 94:39: -> ^( ORDERLIST ( order_column )+ )
				{
					// D:\\gfile\\OBQL.g:94:42: ^( ORDERLIST ( order_column )+ )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								(CommonTree) adaptor.create(ORDERLIST,
										"ORDERLIST"), root_1);

						if (!(stream_order_column.hasNext())) {
							throw new RewriteEarlyExitException();
						}
						while (stream_order_column.hasNext()) {
							adaptor.addChild(root_1,
									stream_order_column.nextTree());

						}
						stream_order_column.reset();

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "order_column_list"

	public static class offset_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "offset"
	// D:\\gfile\\OBQL.g:97:1: offset : INT ;
	public final OBQLParser.offset_return offset() throws RecognitionException {
		OBQLParser.offset_return retval = new OBQLParser.offset_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token INT38 = null;

		CommonTree INT38_tree = null;

		try {
			// D:\\gfile\\OBQL.g:97:8: ( INT )
			// D:\\gfile\\OBQL.g:97:9: INT
			{
				root_0 = (CommonTree) adaptor.nil();

				INT38 = (Token) match(input, INT, FOLLOW_INT_in_offset550);
				INT38_tree = (CommonTree) adaptor.create(INT38);
				adaptor.addChild(root_0, INT38_tree);

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "offset"

	public static class count_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "count"
	// D:\\gfile\\OBQL.g:99:1: count : INT ;
	public final OBQLParser.count_return count() throws RecognitionException {
		OBQLParser.count_return retval = new OBQLParser.count_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token INT39 = null;

		CommonTree INT39_tree = null;

		try {
			// D:\\gfile\\OBQL.g:99:7: ( INT )
			// D:\\gfile\\OBQL.g:99:8: INT
			{
				root_0 = (CommonTree) adaptor.nil();

				INT39 = (Token) match(input, INT, FOLLOW_INT_in_count557);
				INT39_tree = (CommonTree) adaptor.create(INT39);
				adaptor.addChild(root_0, INT39_tree);

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "count"

	public static class op_type_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "op_type"
	// D:\\gfile\\OBQL.g:101:1: op_type : ( EQ | LT | GT | LE | GE );
	public final OBQLParser.op_type_return op_type()
			throws RecognitionException {
		OBQLParser.op_type_return retval = new OBQLParser.op_type_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token set40 = null;

		CommonTree set40_tree = null;

		try {
			// D:\\gfile\\OBQL.g:101:9: ( EQ | LT | GT | LE | GE )
			// D:\\gfile\\OBQL.g:
			{
				root_0 = (CommonTree) adaptor.nil();

				set40 = (Token) input.LT(1);
				if ((input.LA(1) >= EQ && input.LA(1) <= GE)) {
					input.consume();
					adaptor.addChild(root_0, (CommonTree) adaptor.create(set40));
					state.errorRecovery = false;
				} else {
					MismatchedSetException mse = new MismatchedSetException(
							null, input);
					throw mse;
				}

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "op_type"

	public static class update_command_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "update_command"
	// D:\\gfile\\OBQL.g:103:1: update_command : 'UPDATE' table_name set_clause
	// where_clause -> ^( 'UPDATE' table_name set_clause where_clause ) ;
	public final OBQLParser.update_command_return update_command()
			throws RecognitionException {
		OBQLParser.update_command_return retval = new OBQLParser.update_command_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token string_literal41 = null;
		OBQLParser.table_name_return table_name42 = null;

		OBQLParser.set_clause_return set_clause43 = null;

		OBQLParser.where_clause_return where_clause44 = null;

		CommonTree string_literal41_tree = null;
		RewriteRuleTokenStream stream_29 = new RewriteRuleTokenStream(adaptor,
				"token 29");
		RewriteRuleSubtreeStream stream_set_clause = new RewriteRuleSubtreeStream(
				adaptor, "rule set_clause");
		RewriteRuleSubtreeStream stream_where_clause = new RewriteRuleSubtreeStream(
				adaptor, "rule where_clause");
		RewriteRuleSubtreeStream stream_table_name = new RewriteRuleSubtreeStream(
				adaptor, "rule table_name");
		try {
			// D:\\gfile\\OBQL.g:104:2: ( 'UPDATE' table_name set_clause
			// where_clause -> ^( 'UPDATE' table_name set_clause where_clause )
			// )
			// D:\\gfile\\OBQL.g:104:4: 'UPDATE' table_name set_clause
			// where_clause
			{
				string_literal41 = (Token) match(input, 29,
						FOLLOW_29_in_update_command592);
				stream_29.add(string_literal41);

				pushFollow(FOLLOW_table_name_in_update_command594);
				table_name42 = table_name();

				state._fsp--;

				stream_table_name.add(table_name42.getTree());
				pushFollow(FOLLOW_set_clause_in_update_command596);
				set_clause43 = set_clause();

				state._fsp--;

				stream_set_clause.add(set_clause43.getTree());
				pushFollow(FOLLOW_where_clause_in_update_command598);
				where_clause44 = where_clause();

				state._fsp--;

				stream_where_clause.add(where_clause44.getTree());

				// AST REWRITE
				// elements: set_clause, 29, where_clause, table_name
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 104:48: -> ^( 'UPDATE' table_name set_clause where_clause )
				{
					// D:\\gfile\\OBQL.g:104:51: ^( 'UPDATE' table_name
					// set_clause where_clause )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_29.nextNode(), root_1);

						adaptor.addChild(root_1, stream_table_name.nextTree());
						adaptor.addChild(root_1, stream_set_clause.nextTree());
						adaptor.addChild(root_1, stream_where_clause.nextTree());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "update_command"

	public static class set_clause_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "set_clause"
	// D:\\gfile\\OBQL.g:107:1: set_clause : 'SET' pair ( COMMA pair )* -> ^(
	// 'SET' ( pair )+ ) ;
	public final OBQLParser.set_clause_return set_clause()
			throws RecognitionException {
		OBQLParser.set_clause_return retval = new OBQLParser.set_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token string_literal45 = null;
		Token COMMA47 = null;
		OBQLParser.pair_return pair46 = null;

		OBQLParser.pair_return pair48 = null;

		CommonTree string_literal45_tree = null;
		CommonTree COMMA47_tree = null;
		RewriteRuleTokenStream stream_30 = new RewriteRuleTokenStream(adaptor,
				"token 30");
		RewriteRuleTokenStream stream_COMMA = new RewriteRuleTokenStream(
				adaptor, "token COMMA");
		RewriteRuleSubtreeStream stream_pair = new RewriteRuleSubtreeStream(
				adaptor, "rule pair");
		try {
			// D:\\gfile\\OBQL.g:108:2: ( 'SET' pair ( COMMA pair )* -> ^( 'SET'
			// ( pair )+ ) )
			// D:\\gfile\\OBQL.g:108:4: 'SET' pair ( COMMA pair )*
			{
				string_literal45 = (Token) match(input, 30,
						FOLLOW_30_in_set_clause621);
				stream_30.add(string_literal45);

				pushFollow(FOLLOW_pair_in_set_clause623);
				pair46 = pair();

				state._fsp--;

				stream_pair.add(pair46.getTree());
				// D:\\gfile\\OBQL.g:108:15: ( COMMA pair )*
				loop8: do {
					int alt8 = 2;
					int LA8_0 = input.LA(1);

					if ((LA8_0 == COMMA)) {
						alt8 = 1;
					}

					switch (alt8) {
					case 1:
						// D:\\gfile\\OBQL.g:108:16: COMMA pair
					{
						COMMA47 = (Token) match(input, COMMA,
								FOLLOW_COMMA_in_set_clause626);
						stream_COMMA.add(COMMA47);

						pushFollow(FOLLOW_pair_in_set_clause628);
						pair48 = pair();

						state._fsp--;

						stream_pair.add(pair48.getTree());

					}
						break;

					default:
						break loop8;
					}
				} while (true);

				// AST REWRITE
				// elements: 30, pair
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 108:30: -> ^( 'SET' ( pair )+ )
				{
					// D:\\gfile\\OBQL.g:108:33: ^( 'SET' ( pair )+ )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_30.nextNode(), root_1);

						if (!(stream_pair.hasNext())) {
							throw new RewriteEarlyExitException();
						}
						while (stream_pair.hasNext()) {
							adaptor.addChild(root_1, stream_pair.nextTree());

						}
						stream_pair.reset();

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "set_clause"

	public static class pair_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "pair"
	// D:\\gfile\\OBQL.g:111:1: pair : column_name '=' IDENTIFIER -> ^(
	// column_name IDENTIFIER ) ;
	public final OBQLParser.pair_return pair() throws RecognitionException {
		OBQLParser.pair_return retval = new OBQLParser.pair_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token char_literal50 = null;
		Token IDENTIFIER51 = null;
		OBQLParser.column_name_return column_name49 = null;

		CommonTree char_literal50_tree = null;
		CommonTree IDENTIFIER51_tree = null;
		RewriteRuleTokenStream stream_EQ = new RewriteRuleTokenStream(adaptor,
				"token EQ");
		RewriteRuleTokenStream stream_IDENTIFIER = new RewriteRuleTokenStream(
				adaptor, "token IDENTIFIER");
		RewriteRuleSubtreeStream stream_column_name = new RewriteRuleSubtreeStream(
				adaptor, "rule column_name");
		try {
			// D:\\gfile\\OBQL.g:111:7: ( column_name '=' IDENTIFIER -> ^(
			// column_name IDENTIFIER ) )
			// D:\\gfile\\OBQL.g:111:9: column_name '=' IDENTIFIER
			{
				pushFollow(FOLLOW_column_name_in_pair651);
				column_name49 = column_name();

				state._fsp--;

				stream_column_name.add(column_name49.getTree());
				char_literal50 = (Token) match(input, EQ, FOLLOW_EQ_in_pair653);
				stream_EQ.add(char_literal50);

				IDENTIFIER51 = (Token) match(input, IDENTIFIER,
						FOLLOW_IDENTIFIER_in_pair655);
				stream_IDENTIFIER.add(IDENTIFIER51);

				// AST REWRITE
				// elements: IDENTIFIER, column_name
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 111:36: -> ^( column_name IDENTIFIER )
				{
					// D:\\gfile\\OBQL.g:111:39: ^( column_name IDENTIFIER )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_column_name.nextNode(), root_1);

						adaptor.addChild(root_1, stream_IDENTIFIER.nextNode());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "pair"

	public static class insert_command_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "insert_command"
	// D:\\gfile\\OBQL.g:113:1: insert_command : 'INSERT' 'INTO' table_name '('
	// column_list ')' 'VALUES' '(' IDENTIFIER ( COMMA IDENTIFIER )* ')'
	// where_clause -> ^( 'INSERT' table_name column_list ( IDENTIFIER )+
	// where_clause ) ;
	public final OBQLParser.insert_command_return insert_command()
			throws RecognitionException {
		OBQLParser.insert_command_return retval = new OBQLParser.insert_command_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token string_literal52 = null;
		Token string_literal53 = null;
		Token char_literal55 = null;
		Token char_literal57 = null;
		Token string_literal58 = null;
		Token char_literal59 = null;
		Token IDENTIFIER60 = null;
		Token COMMA61 = null;
		Token IDENTIFIER62 = null;
		Token char_literal63 = null;
		OBQLParser.table_name_return table_name54 = null;

		OBQLParser.column_list_return column_list56 = null;

		OBQLParser.where_clause_return where_clause64 = null;

		CommonTree string_literal52_tree = null;
		CommonTree string_literal53_tree = null;
		CommonTree char_literal55_tree = null;
		CommonTree char_literal57_tree = null;
		CommonTree string_literal58_tree = null;
		CommonTree char_literal59_tree = null;
		CommonTree IDENTIFIER60_tree = null;
		CommonTree COMMA61_tree = null;
		CommonTree IDENTIFIER62_tree = null;
		CommonTree char_literal63_tree = null;
		RewriteRuleTokenStream stream_32 = new RewriteRuleTokenStream(adaptor,
				"token 32");
		RewriteRuleTokenStream stream_31 = new RewriteRuleTokenStream(adaptor,
				"token 31");
		RewriteRuleTokenStream stream_35 = new RewriteRuleTokenStream(adaptor,
				"token 35");
		RewriteRuleTokenStream stream_COMMA = new RewriteRuleTokenStream(
				adaptor, "token COMMA");
		RewriteRuleTokenStream stream_33 = new RewriteRuleTokenStream(adaptor,
				"token 33");
		RewriteRuleTokenStream stream_34 = new RewriteRuleTokenStream(adaptor,
				"token 34");
		RewriteRuleTokenStream stream_IDENTIFIER = new RewriteRuleTokenStream(
				adaptor, "token IDENTIFIER");
		RewriteRuleSubtreeStream stream_column_list = new RewriteRuleSubtreeStream(
				adaptor, "rule column_list");
		RewriteRuleSubtreeStream stream_where_clause = new RewriteRuleSubtreeStream(
				adaptor, "rule where_clause");
		RewriteRuleSubtreeStream stream_table_name = new RewriteRuleSubtreeStream(
				adaptor, "rule table_name");
		try {
			// D:\\gfile\\OBQL.g:114:2: ( 'INSERT' 'INTO' table_name '('
			// column_list ')' 'VALUES' '(' IDENTIFIER ( COMMA IDENTIFIER )* ')'
			// where_clause -> ^( 'INSERT' table_name column_list ( IDENTIFIER
			// )+ where_clause ) )
			// D:\\gfile\\OBQL.g:114:4: 'INSERT' 'INTO' table_name '('
			// column_list ')' 'VALUES' '(' IDENTIFIER ( COMMA IDENTIFIER )* ')'
			// where_clause
			{
				string_literal52 = (Token) match(input, 31,
						FOLLOW_31_in_insert_command672);
				stream_31.add(string_literal52);

				string_literal53 = (Token) match(input, 32,
						FOLLOW_32_in_insert_command674);
				stream_32.add(string_literal53);

				pushFollow(FOLLOW_table_name_in_insert_command676);
				table_name54 = table_name();

				state._fsp--;

				stream_table_name.add(table_name54.getTree());
				char_literal55 = (Token) match(input, 33,
						FOLLOW_33_in_insert_command678);
				stream_33.add(char_literal55);

				pushFollow(FOLLOW_column_list_in_insert_command680);
				column_list56 = column_list();

				state._fsp--;

				stream_column_list.add(column_list56.getTree());
				char_literal57 = (Token) match(input, 34,
						FOLLOW_34_in_insert_command682);
				stream_34.add(char_literal57);

				string_literal58 = (Token) match(input, 35,
						FOLLOW_35_in_insert_command684);
				stream_35.add(string_literal58);

				char_literal59 = (Token) match(input, 33,
						FOLLOW_33_in_insert_command686);
				stream_33.add(char_literal59);

				IDENTIFIER60 = (Token) match(input, IDENTIFIER,
						FOLLOW_IDENTIFIER_in_insert_command689);
				stream_IDENTIFIER.add(IDENTIFIER60);

				// D:\\gfile\\OBQL.g:114:76: ( COMMA IDENTIFIER )*
				loop9: do {
					int alt9 = 2;
					int LA9_0 = input.LA(1);

					if ((LA9_0 == COMMA)) {
						alt9 = 1;
					}

					switch (alt9) {
					case 1:
						// D:\\gfile\\OBQL.g:114:77: COMMA IDENTIFIER
					{
						COMMA61 = (Token) match(input, COMMA,
								FOLLOW_COMMA_in_insert_command692);
						stream_COMMA.add(COMMA61);

						IDENTIFIER62 = (Token) match(input, IDENTIFIER,
								FOLLOW_IDENTIFIER_in_insert_command694);
						stream_IDENTIFIER.add(IDENTIFIER62);

					}
						break;

					default:
						break loop9;
					}
				} while (true);

				char_literal63 = (Token) match(input, 34,
						FOLLOW_34_in_insert_command698);
				stream_34.add(char_literal63);

				pushFollow(FOLLOW_where_clause_in_insert_command700);
				where_clause64 = where_clause();

				state._fsp--;

				stream_where_clause.add(where_clause64.getTree());

				// AST REWRITE
				// elements: IDENTIFIER, 31, column_list, where_clause,
				// table_name
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 115:3: -> ^( 'INSERT' table_name column_list ( IDENTIFIER )+
				// where_clause )
				{
					// D:\\gfile\\OBQL.g:115:6: ^( 'INSERT' table_name
					// column_list ( IDENTIFIER )+ where_clause )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_31.nextNode(), root_1);

						adaptor.addChild(root_1, stream_table_name.nextTree());
						adaptor.addChild(root_1, stream_column_list.nextTree());
						if (!(stream_IDENTIFIER.hasNext())) {
							throw new RewriteEarlyExitException();
						}
						while (stream_IDENTIFIER.hasNext()) {
							adaptor.addChild(root_1,
									stream_IDENTIFIER.nextNode());

						}
						stream_IDENTIFIER.reset();
						adaptor.addChild(root_1, stream_where_clause.nextTree());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "insert_command"

	public static class delete_command_return extends ParserRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "delete_command"
	// D:\\gfile\\OBQL.g:118:1: delete_command : 'DELETE' from_clause
	// where_clause -> ^( 'DELETE' from_clause where_clause ) ;
	public final OBQLParser.delete_command_return delete_command()
			throws RecognitionException {
		OBQLParser.delete_command_return retval = new OBQLParser.delete_command_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token string_literal65 = null;
		OBQLParser.from_clause_return from_clause66 = null;

		OBQLParser.where_clause_return where_clause67 = null;

		CommonTree string_literal65_tree = null;
		RewriteRuleTokenStream stream_36 = new RewriteRuleTokenStream(adaptor,
				"token 36");
		RewriteRuleSubtreeStream stream_from_clause = new RewriteRuleSubtreeStream(
				adaptor, "rule from_clause");
		RewriteRuleSubtreeStream stream_where_clause = new RewriteRuleSubtreeStream(
				adaptor, "rule where_clause");
		try {
			// D:\\gfile\\OBQL.g:119:2: ( 'DELETE' from_clause where_clause ->
			// ^( 'DELETE' from_clause where_clause ) )
			// D:\\gfile\\OBQL.g:119:3: 'DELETE' from_clause where_clause
			{
				string_literal65 = (Token) match(input, 36,
						FOLLOW_36_in_delete_command728);
				stream_36.add(string_literal65);

				pushFollow(FOLLOW_from_clause_in_delete_command730);
				from_clause66 = from_clause();

				state._fsp--;

				stream_from_clause.add(from_clause66.getTree());
				pushFollow(FOLLOW_where_clause_in_delete_command732);
				where_clause67 = where_clause();

				state._fsp--;

				stream_where_clause.add(where_clause67.getTree());

				// AST REWRITE
				// elements: 36, from_clause, where_clause
				// token labels:
				// rule labels: retval
				// token list labels:
				// rule list labels:
				// wildcard labels:
				retval.tree = root_0;
				RewriteRuleSubtreeStream stream_retval = new RewriteRuleSubtreeStream(
						adaptor, "rule retval", retval != null ? retval.tree
								: null);

				root_0 = (CommonTree) adaptor.nil();
				// 119:37: -> ^( 'DELETE' from_clause where_clause )
				{
					// D:\\gfile\\OBQL.g:119:40: ^( 'DELETE' from_clause
					// where_clause )
					{
						CommonTree root_1 = (CommonTree) adaptor.nil();
						root_1 = (CommonTree) adaptor.becomeRoot(
								stream_36.nextNode(), root_1);

						adaptor.addChild(root_1, stream_from_clause.nextTree());
						adaptor.addChild(root_1, stream_where_clause.nextTree());

						adaptor.addChild(root_0, root_1);
					}

				}

				retval.tree = root_0;
			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

		catch (RecognitionException e) {
			throw e;
		} finally {
		}
		return retval;
	}

	// $ANTLR end "delete_command"

	// Delegated rules

	public static final BitSet FOLLOW_select_clause_in_select_command234 = new BitSet(
			new long[] { 0x0000000000800000L });
	public static final BitSet FOLLOW_from_clause_in_select_command236 = new BitSet(
			new long[] { 0x0000000001000000L });
	public static final BitSet FOLLOW_where_clause_in_select_command238 = new BitSet(
			new long[] { 0x000000000A000002L });
	public static final BitSet FOLLOW_orderby_clause_in_select_command241 = new BitSet(
			new long[] { 0x0000000008000002L });
	public static final BitSet FOLLOW_limit_clause_in_select_command246 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_22_in_select_clause261 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_column_list_in_select_clause263 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_23_in_from_clause282 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_table_name_in_from_clause285 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_24_in_where_clause296 = new BitSet(
			new long[] { 0x0000000000001000L });
	public static final BitSet FOLLOW_query_conditions_in_where_clause298 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_25_in_orderby_clause316 = new BitSet(
			new long[] { 0x0000000004000000L });
	public static final BitSet FOLLOW_26_in_orderby_clause318 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_order_column_list_in_orderby_clause320 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_27_in_limit_clause341 = new BitSet(
			new long[] { 0x0000000000100000L });
	public static final BitSet FOLLOW_offset_in_limit_clause344 = new BitSet(
			new long[] { 0x0000000000002000L });
	public static final BitSet FOLLOW_COMMA_in_limit_clause346 = new BitSet(
			new long[] { 0x0000000000100000L });
	public static final BitSet FOLLOW_count_in_limit_clause350 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_query_condition_in_query_conditions372 = new BitSet(
			new long[] { 0x0000000010000002L });
	public static final BitSet FOLLOW_28_in_query_conditions375 = new BitSet(
			new long[] { 0x0000000000001000L });
	public static final BitSet FOLLOW_query_condition_in_query_conditions378 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_ROWKEY_in_query_condition392 = new BitSet(
			new long[] { 0x000000000007C000L });
	public static final BitSet FOLLOW_op_type_in_query_condition394 = new BitSet(
			new long[] { 0x0000000000100000L });
	public static final BitSet FOLLOW_INT_in_query_condition396 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_IDENTIFIER_in_column_name416 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_IDENTIFIER_in_table_name428 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_column_name_in_column_list448 = new BitSet(
			new long[] { 0x0000000000002002L });
	public static final BitSet FOLLOW_COMMA_in_column_list451 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_column_name_in_column_list453 = new BitSet(
			new long[] { 0x0000000000002002L });
	public static final BitSet FOLLOW_column_name_in_order_column476 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_column_name_in_order_column489 = new BitSet(
			new long[] { 0x0000000000000400L });
	public static final BitSet FOLLOW_ASC_in_order_column491 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_column_name_in_order_column504 = new BitSet(
			new long[] { 0x0000000000000800L });
	public static final BitSet FOLLOW_DESC_in_order_column506 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_order_column_in_order_column_list525 = new BitSet(
			new long[] { 0x0000000000002002L });
	public static final BitSet FOLLOW_COMMA_in_order_column_list528 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_order_column_in_order_column_list530 = new BitSet(
			new long[] { 0x0000000000002002L });
	public static final BitSet FOLLOW_INT_in_offset550 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_INT_in_count557 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_set_in_op_type0 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_29_in_update_command592 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_table_name_in_update_command594 = new BitSet(
			new long[] { 0x0000000040000000L });
	public static final BitSet FOLLOW_set_clause_in_update_command596 = new BitSet(
			new long[] { 0x0000000001000000L });
	public static final BitSet FOLLOW_where_clause_in_update_command598 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_30_in_set_clause621 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_pair_in_set_clause623 = new BitSet(
			new long[] { 0x0000000000002002L });
	public static final BitSet FOLLOW_COMMA_in_set_clause626 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_pair_in_set_clause628 = new BitSet(
			new long[] { 0x0000000000002002L });
	public static final BitSet FOLLOW_column_name_in_pair651 = new BitSet(
			new long[] { 0x0000000000004000L });
	public static final BitSet FOLLOW_EQ_in_pair653 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_IDENTIFIER_in_pair655 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_31_in_insert_command672 = new BitSet(
			new long[] { 0x0000000100000000L });
	public static final BitSet FOLLOW_32_in_insert_command674 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_table_name_in_insert_command676 = new BitSet(
			new long[] { 0x0000000200000000L });
	public static final BitSet FOLLOW_33_in_insert_command678 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_column_list_in_insert_command680 = new BitSet(
			new long[] { 0x0000000400000000L });
	public static final BitSet FOLLOW_34_in_insert_command682 = new BitSet(
			new long[] { 0x0000000800000000L });
	public static final BitSet FOLLOW_35_in_insert_command684 = new BitSet(
			new long[] { 0x0000000200000000L });
	public static final BitSet FOLLOW_33_in_insert_command686 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_IDENTIFIER_in_insert_command689 = new BitSet(
			new long[] { 0x0000000400002000L });
	public static final BitSet FOLLOW_COMMA_in_insert_command692 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_IDENTIFIER_in_insert_command694 = new BitSet(
			new long[] { 0x0000000400002000L });
	public static final BitSet FOLLOW_34_in_insert_command698 = new BitSet(
			new long[] { 0x0000000001000000L });
	public static final BitSet FOLLOW_where_clause_in_insert_command700 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_36_in_delete_command728 = new BitSet(
			new long[] { 0x0000000000800000L });
	public static final BitSet FOLLOW_from_clause_in_delete_command730 = new BitSet(
			new long[] { 0x0000000001000000L });
	public static final BitSet FOLLOW_where_clause_in_delete_command732 = new BitSet(
			new long[] { 0x0000000000000002L });

}