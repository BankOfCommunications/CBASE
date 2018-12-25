// $ANTLR 3.2 Sep 23, 2009 12:02:23 D:\\gfile\\OBQLWalker.g 2010-10-29 21:05:20

package com.taobao.oceanbase.parser;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class OBQLWalker extends TreeParser {
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
	public static final int T__33 = 33;
	public static final int WS = 21;
	public static final int T__34 = 34;
	public static final int T__35 = 35;
	public static final int T__36 = 36;
	public static final int COMMA = 13;
	public static final int IDENTIFIER = 19;
	public static final int COLUMNLIST = 8;
	public static final int GT = 16;
	public static final int DESC = 11;
	public static final int EQ = 14;
	public static final int GROUPBY = 6;
	public static final int LE = 17;
	public static final int ORDERLIST = 9;

	// delegates
	// delegators

	public OBQLWalker(TreeNodeStream input) {
		this(input, new RecognizerSharedState());
	}

	public OBQLWalker(TreeNodeStream input, RecognizerSharedState state) {
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
		return OBQLWalker.tokenNames;
	}

	public String getGrammarFileName() {
		return "D:\\gfile\\OBQLWalker.g";
	}

	String table;
	long user;

	public static class select_command_return extends TreeRuleReturnScope {
		public List<String> names;
		public String table;
		public long user;
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "select_command"
	// D:\\gfile\\OBQLWalker.g:24:1: select_command returns [List<String>
	// names,String table,long user] : select_clause from_clause where_clause (
	// orderby_clause )? ;
	public final OBQLWalker.select_command_return select_command()
			throws RecognitionException {
		OBQLWalker.select_command_return retval = new OBQLWalker.select_command_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		OBQLWalker.select_clause_return select_clause1 = null;

		OBQLWalker.from_clause_return from_clause2 = null;

		OBQLWalker.where_clause_return where_clause3 = null;

		OBQLWalker.orderby_clause_return orderby_clause4 = null;

		try {
			// D:\\gfile\\OBQLWalker.g:25:2: ( select_clause from_clause
			// where_clause ( orderby_clause )? )
			// D:\\gfile\\OBQLWalker.g:25:4: select_clause from_clause
			// where_clause ( orderby_clause )?
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				pushFollow(FOLLOW_select_clause_in_select_command58);
				select_clause1 = select_clause();

				state._fsp--;

				adaptor.addChild(root_0, select_clause1.getTree());
				_last = (CommonTree) input.LT(1);
				pushFollow(FOLLOW_from_clause_in_select_command60);
				from_clause2 = from_clause();

				state._fsp--;

				adaptor.addChild(root_0, from_clause2.getTree());
				_last = (CommonTree) input.LT(1);
				pushFollow(FOLLOW_where_clause_in_select_command62);
				where_clause3 = where_clause();

				state._fsp--;

				adaptor.addChild(root_0, where_clause3.getTree());
				// D:\\gfile\\OBQLWalker.g:25:43: ( orderby_clause )?
				int alt1 = 2;
				int LA1_0 = input.LA(1);

				if ((LA1_0 == ORDERBY)) {
					alt1 = 1;
				}
				switch (alt1) {
				case 1:
					// D:\\gfile\\OBQLWalker.g:25:44: orderby_clause
				{
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_orderby_clause_in_select_command65);
					orderby_clause4 = orderby_clause();

					state._fsp--;

					adaptor.addChild(root_0, orderby_clause4.getTree());

				}
					break;

				}

				retval.names = (select_clause1 != null ? select_clause1.names
						: null);
				retval.table = table;
				retval.user = user;

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "select_command"

	public static class select_clause_return extends TreeRuleReturnScope {
		public List names;
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "select_clause"
	// D:\\gfile\\OBQLWalker.g:28:1: select_clause returns [List names] : ^(
	// 'SELECT' column_list ) ;
	public final OBQLWalker.select_clause_return select_clause()
			throws RecognitionException {
		OBQLWalker.select_clause_return retval = new OBQLWalker.select_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree string_literal5 = null;
		OBQLWalker.column_list_return column_list6 = null;

		CommonTree string_literal5_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:29:1: ( ^( 'SELECT' column_list ) )
			// D:\\gfile\\OBQLWalker.g:29:2: ^( 'SELECT' column_list )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					string_literal5 = (CommonTree) match(input, 22,
							FOLLOW_22_in_select_clause83);
					string_literal5_tree = (CommonTree) adaptor
							.dupNode(string_literal5);

					root_1 = (CommonTree) adaptor.becomeRoot(
							string_literal5_tree, root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_column_list_in_select_clause85);
					column_list6 = column_list();

					state._fsp--;

					adaptor.addChild(root_1, column_list6.getTree());

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

				retval.names = (column_list6 != null ? column_list6.names
						: null);

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "select_clause"

	protected static class column_list_scope {
		private List<String> _names;
	}

	protected Stack column_list_stack = new Stack();

	public static class column_list_return extends TreeRuleReturnScope {
		public List<String> names;
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "column_list"
	// D:\\gfile\\OBQLWalker.g:32:1: column_list returns [List<String> names] :
	// ^( COLUMNLIST ( column_name )+ ) ;
	public final OBQLWalker.column_list_return column_list()
			throws RecognitionException {
		column_list_stack.push(new column_list_scope());
		OBQLWalker.column_list_return retval = new OBQLWalker.column_list_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree COLUMNLIST7 = null;
		OBQLWalker.column_name_return column_name8 = null;

		CommonTree COLUMNLIST7_tree = null;

		((column_list_scope) column_list_stack.peek())._names = new ArrayList<String>();

		try {
			// D:\\gfile\\OBQLWalker.g:42:1: ( ^( COLUMNLIST ( column_name )+ )
			// )
			// D:\\gfile\\OBQLWalker.g:42:3: ^( COLUMNLIST ( column_name )+ )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					COLUMNLIST7 = (CommonTree) match(input, COLUMNLIST,
							FOLLOW_COLUMNLIST_in_column_list118);
					COLUMNLIST7_tree = (CommonTree) adaptor
							.dupNode(COLUMNLIST7);

					root_1 = (CommonTree) adaptor.becomeRoot(COLUMNLIST7_tree,
							root_1);

					match(input, Token.DOWN, null);
					// D:\\gfile\\OBQLWalker.g:42:16: ( column_name )+
					int cnt2 = 0;
					loop2: do {
						int alt2 = 2;
						int LA2_0 = input.LA(1);

						if ((LA2_0 == IDENTIFIER)) {
							alt2 = 1;
						}

						switch (alt2) {
						case 1:
							// D:\\gfile\\OBQLWalker.g:42:17: column_name
						{
							_last = (CommonTree) input.LT(1);
							pushFollow(FOLLOW_column_name_in_column_list121);
							column_name8 = column_name();

							state._fsp--;

							adaptor.addChild(root_1, column_name8.getTree());
							((column_list_scope) column_list_stack.peek())._names
									.add((column_name8 != null ? column_name8.name
											: null));

						}
							break;

						default:
							if (cnt2 >= 1)
								break loop2;
							EarlyExitException eee = new EarlyExitException(2,
									input);
							throw eee;
						}
						cnt2++;
					} while (true);

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

			retval.names = ((column_list_scope) column_list_stack.peek())._names;

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
			column_list_stack.pop();
		}
		return retval;
	}

	// $ANTLR end "column_list"

	public static class column_name_return extends TreeRuleReturnScope {
		public String name;
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "column_name"
	// D:\\gfile\\OBQLWalker.g:45:1: column_name returns [String name] :
	// IDENTIFIER ;
	public final OBQLWalker.column_name_return column_name()
			throws RecognitionException {
		OBQLWalker.column_name_return retval = new OBQLWalker.column_name_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree IDENTIFIER9 = null;

		CommonTree IDENTIFIER9_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:46:2: ( IDENTIFIER )
			// D:\\gfile\\OBQLWalker.g:46:4: IDENTIFIER
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				IDENTIFIER9 = (CommonTree) match(input, IDENTIFIER,
						FOLLOW_IDENTIFIER_in_column_name140);
				IDENTIFIER9_tree = (CommonTree) adaptor.dupNode(IDENTIFIER9);

				adaptor.addChild(root_0, IDENTIFIER9_tree);

				retval.name = (IDENTIFIER9 != null ? IDENTIFIER9.getText()
						: null);

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "column_name"

	public static class from_clause_return extends TreeRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "from_clause"
	// D:\\gfile\\OBQLWalker.g:49:1: from_clause : ^( TABLE IDENTIFIER ) ;
	public final OBQLWalker.from_clause_return from_clause()
			throws RecognitionException {
		OBQLWalker.from_clause_return retval = new OBQLWalker.from_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree TABLE10 = null;
		CommonTree IDENTIFIER11 = null;

		CommonTree TABLE10_tree = null;
		CommonTree IDENTIFIER11_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:50:2: ( ^( TABLE IDENTIFIER ) )
			// D:\\gfile\\OBQLWalker.g:50:4: ^( TABLE IDENTIFIER )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					TABLE10 = (CommonTree) match(input, TABLE,
							FOLLOW_TABLE_in_from_clause154);
					TABLE10_tree = (CommonTree) adaptor.dupNode(TABLE10);

					root_1 = (CommonTree) adaptor.becomeRoot(TABLE10_tree,
							root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					IDENTIFIER11 = (CommonTree) match(input, IDENTIFIER,
							FOLLOW_IDENTIFIER_in_from_clause156);
					IDENTIFIER11_tree = (CommonTree) adaptor
							.dupNode(IDENTIFIER11);

					adaptor.addChild(root_1, IDENTIFIER11_tree);

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

				table = (IDENTIFIER11 != null ? IDENTIFIER11.getText() : null);

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "from_clause"

	public static class where_clause_return extends TreeRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "where_clause"
	// D:\\gfile\\OBQLWalker.g:53:1: where_clause : ^( 'WHERE' query_conditions
	// ) ;
	public final OBQLWalker.where_clause_return where_clause()
			throws RecognitionException {
		OBQLWalker.where_clause_return retval = new OBQLWalker.where_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree string_literal12 = null;
		OBQLWalker.query_conditions_return query_conditions13 = null;

		CommonTree string_literal12_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:54:2: ( ^( 'WHERE' query_conditions ) )
			// D:\\gfile\\OBQLWalker.g:54:4: ^( 'WHERE' query_conditions )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					string_literal12 = (CommonTree) match(input, 24,
							FOLLOW_24_in_where_clause171);
					string_literal12_tree = (CommonTree) adaptor
							.dupNode(string_literal12);

					root_1 = (CommonTree) adaptor.becomeRoot(
							string_literal12_tree, root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_query_conditions_in_where_clause173);
					query_conditions13 = query_conditions();

					state._fsp--;

					adaptor.addChild(root_1, query_conditions13.getTree());

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "where_clause"

	public static class query_conditions_return extends TreeRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "query_conditions"
	// D:\\gfile\\OBQLWalker.g:57:1: query_conditions : query_condition ( 'AND'
	// query_condition )? ;
	public final OBQLWalker.query_conditions_return query_conditions()
			throws RecognitionException {
		OBQLWalker.query_conditions_return retval = new OBQLWalker.query_conditions_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree string_literal15 = null;
		OBQLWalker.query_condition_return query_condition14 = null;

		OBQLWalker.query_condition_return query_condition16 = null;

		CommonTree string_literal15_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:58:2: ( query_condition ( 'AND'
			// query_condition )? )
			// D:\\gfile\\OBQLWalker.g:58:4: query_condition ( 'AND'
			// query_condition )?
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				pushFollow(FOLLOW_query_condition_in_query_conditions185);
				query_condition14 = query_condition();

				state._fsp--;

				adaptor.addChild(root_0, query_condition14.getTree());
				// D:\\gfile\\OBQLWalker.g:58:20: ( 'AND' query_condition )?
				int alt3 = 2;
				int LA3_0 = input.LA(1);

				if ((LA3_0 == 28)) {
					alt3 = 1;
				}
				switch (alt3) {
				case 1:
					// D:\\gfile\\OBQLWalker.g:58:21: 'AND' query_condition
				{
					_last = (CommonTree) input.LT(1);
					string_literal15 = (CommonTree) match(input, 28,
							FOLLOW_28_in_query_conditions188);
					string_literal15_tree = (CommonTree) adaptor
							.dupNode(string_literal15);

					adaptor.addChild(root_0, string_literal15_tree);

					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_query_condition_in_query_conditions190);
					query_condition16 = query_condition();

					state._fsp--;

					adaptor.addChild(root_0, query_condition16.getTree());

				}
					break;

				}

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "query_conditions"

	public static class query_condition_return extends TreeRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "query_condition"
	// D:\\gfile\\OBQLWalker.g:61:1: query_condition : ^( op_type INT ) ;
	public final OBQLWalker.query_condition_return query_condition()
			throws RecognitionException {
		OBQLWalker.query_condition_return retval = new OBQLWalker.query_condition_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree INT18 = null;
		OBQLWalker.op_type_return op_type17 = null;

		CommonTree INT18_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:62:2: ( ^( op_type INT ) )
			// D:\\gfile\\OBQLWalker.g:62:3: ^( op_type INT )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_op_type_in_query_condition203);
					op_type17 = op_type();

					state._fsp--;

					root_1 = (CommonTree) adaptor.becomeRoot(
							op_type17.getTree(), root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					INT18 = (CommonTree) match(input, INT,
							FOLLOW_INT_in_query_condition205);
					INT18_tree = (CommonTree) adaptor.dupNode(INT18);

					adaptor.addChild(root_1, INT18_tree);

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

				user = Long.parseLong((INT18 != null ? INT18.getText() : null));

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "query_condition"

	public static class op_type_return extends TreeRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "op_type"
	// D:\\gfile\\OBQLWalker.g:65:1: op_type : EQ ;
	public final OBQLWalker.op_type_return op_type()
			throws RecognitionException {
		OBQLWalker.op_type_return retval = new OBQLWalker.op_type_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree EQ19 = null;

		CommonTree EQ19_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:66:2: ( EQ )
			// D:\\gfile\\OBQLWalker.g:67:2: EQ
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				EQ19 = (CommonTree) match(input, EQ, FOLLOW_EQ_in_op_type220);
				EQ19_tree = (CommonTree) adaptor.dupNode(EQ19);

				adaptor.addChild(root_0, EQ19_tree);

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "op_type"

	public static class orderby_clause_return extends TreeRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "orderby_clause"
	// D:\\gfile\\OBQLWalker.g:74:1: orderby_clause : ^( ORDERBY
	// order_column_list ) ;
	public final OBQLWalker.orderby_clause_return orderby_clause()
			throws RecognitionException {
		OBQLWalker.orderby_clause_return retval = new OBQLWalker.orderby_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree ORDERBY20 = null;
		OBQLWalker.order_column_list_return order_column_list21 = null;

		CommonTree ORDERBY20_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:75:2: ( ^( ORDERBY order_column_list ) )
			// D:\\gfile\\OBQLWalker.g:75:4: ^( ORDERBY order_column_list )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					ORDERBY20 = (CommonTree) match(input, ORDERBY,
							FOLLOW_ORDERBY_in_orderby_clause241);
					ORDERBY20_tree = (CommonTree) adaptor.dupNode(ORDERBY20);

					root_1 = (CommonTree) adaptor.becomeRoot(ORDERBY20_tree,
							root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_order_column_list_in_orderby_clause243);
					order_column_list21 = order_column_list();

					state._fsp--;

					adaptor.addChild(root_1, order_column_list21.getTree());

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "orderby_clause"

	public static class order_column_list_return extends TreeRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "order_column_list"
	// D:\\gfile\\OBQLWalker.g:78:1: order_column_list : ^( ORDERLIST (
	// order_column )+ ) ;
	public final OBQLWalker.order_column_list_return order_column_list()
			throws RecognitionException {
		OBQLWalker.order_column_list_return retval = new OBQLWalker.order_column_list_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree ORDERLIST22 = null;
		OBQLWalker.order_column_return order_column23 = null;

		CommonTree ORDERLIST22_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:79:2: ( ^( ORDERLIST ( order_column )+ )
			// )
			// D:\\gfile\\OBQLWalker.g:79:5: ^( ORDERLIST ( order_column )+ )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					ORDERLIST22 = (CommonTree) match(input, ORDERLIST,
							FOLLOW_ORDERLIST_in_order_column_list257);
					ORDERLIST22_tree = (CommonTree) adaptor
							.dupNode(ORDERLIST22);

					root_1 = (CommonTree) adaptor.becomeRoot(ORDERLIST22_tree,
							root_1);

					match(input, Token.DOWN, null);
					// D:\\gfile\\OBQLWalker.g:79:17: ( order_column )+
					int cnt4 = 0;
					loop4: do {
						int alt4 = 2;
						int LA4_0 = input.LA(1);

						if (((LA4_0 >= ASC && LA4_0 <= DESC))) {
							alt4 = 1;
						}

						switch (alt4) {
						case 1:
							// D:\\gfile\\OBQLWalker.g:79:17: order_column
						{
							_last = (CommonTree) input.LT(1);
							pushFollow(FOLLOW_order_column_in_order_column_list259);
							order_column23 = order_column();

							state._fsp--;

							adaptor.addChild(root_1, order_column23.getTree());

						}
							break;

						default:
							if (cnt4 >= 1)
								break loop4;
							EarlyExitException eee = new EarlyExitException(4,
									input);
							throw eee;
						}
						cnt4++;
					} while (true);

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "order_column_list"

	public static class order_column_return extends TreeRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "order_column"
	// D:\\gfile\\OBQLWalker.g:82:1: order_column : ( ^( ASC column_name ) | ^(
	// DESC column_name ) );
	public final OBQLWalker.order_column_return order_column()
			throws RecognitionException {
		OBQLWalker.order_column_return retval = new OBQLWalker.order_column_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree ASC24 = null;
		CommonTree DESC26 = null;
		OBQLWalker.column_name_return column_name25 = null;

		OBQLWalker.column_name_return column_name27 = null;

		CommonTree ASC24_tree = null;
		CommonTree DESC26_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:83:2: ( ^( ASC column_name ) | ^( DESC
			// column_name ) )
			int alt5 = 2;
			int LA5_0 = input.LA(1);

			if ((LA5_0 == ASC)) {
				alt5 = 1;
			} else if ((LA5_0 == DESC)) {
				alt5 = 2;
			} else {
				NoViableAltException nvae = new NoViableAltException("", 5, 0,
						input);

				throw nvae;
			}
			switch (alt5) {
			case 1:
				// D:\\gfile\\OBQLWalker.g:83:4: ^( ASC column_name )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					ASC24 = (CommonTree) match(input, ASC,
							FOLLOW_ASC_in_order_column273);
					ASC24_tree = (CommonTree) adaptor.dupNode(ASC24);

					root_1 = (CommonTree) adaptor
							.becomeRoot(ASC24_tree, root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_column_name_in_order_column275);
					column_name25 = column_name();

					state._fsp--;

					adaptor.addChild(root_1, column_name25.getTree());

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

			}
				break;
			case 2:
				// D:\\gfile\\OBQLWalker.g:84:4: ^( DESC column_name )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					DESC26 = (CommonTree) match(input, DESC,
							FOLLOW_DESC_in_order_column283);
					DESC26_tree = (CommonTree) adaptor.dupNode(DESC26);

					root_1 = (CommonTree) adaptor.becomeRoot(DESC26_tree,
							root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_column_name_in_order_column285);
					column_name27 = column_name();

					state._fsp--;

					adaptor.addChild(root_1, column_name27.getTree());

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

			}
				break;

			}
			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "order_column"

	protected static class update_command_scope {
		private String key;
		private Map<String, String> bound;
	}

	protected Stack update_command_stack = new Stack();

	public static class update_command_return extends TreeRuleReturnScope {
		public String table;
		public long user;
		public Map<String, String> kvs;
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "update_command"
	// D:\\gfile\\OBQLWalker.g:87:1: update_command returns [String table,long
	// user,Map<String,String> kvs] : ^( 'UPDATE' ^( TABLE IDENTIFIER )
	// set_clause where_clause ) ;
	public final OBQLWalker.update_command_return update_command()
			throws RecognitionException {
		update_command_stack.push(new update_command_scope());
		OBQLWalker.update_command_return retval = new OBQLWalker.update_command_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree string_literal28 = null;
		CommonTree TABLE29 = null;
		CommonTree IDENTIFIER30 = null;
		OBQLWalker.set_clause_return set_clause31 = null;

		OBQLWalker.where_clause_return where_clause32 = null;

		CommonTree string_literal28_tree = null;
		CommonTree TABLE29_tree = null;
		CommonTree IDENTIFIER30_tree = null;

		((update_command_scope) update_command_stack.peek()).bound = new HashMap<String, String>();

		try {
			// D:\\gfile\\OBQLWalker.g:98:2: ( ^( 'UPDATE' ^( TABLE IDENTIFIER )
			// set_clause where_clause ) )
			// D:\\gfile\\OBQLWalker.g:98:4: ^( 'UPDATE' ^( TABLE IDENTIFIER )
			// set_clause where_clause )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					string_literal28 = (CommonTree) match(input, 29,
							FOLLOW_29_in_update_command317);
					string_literal28_tree = (CommonTree) adaptor
							.dupNode(string_literal28);

					root_1 = (CommonTree) adaptor.becomeRoot(
							string_literal28_tree, root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					{
						CommonTree _save_last_2 = _last;
						CommonTree _first_2 = null;
						CommonTree root_2 = (CommonTree) adaptor.nil();
						_last = (CommonTree) input.LT(1);
						TABLE29 = (CommonTree) match(input, TABLE,
								FOLLOW_TABLE_in_update_command320);
						TABLE29_tree = (CommonTree) adaptor.dupNode(TABLE29);

						root_2 = (CommonTree) adaptor.becomeRoot(TABLE29_tree,
								root_2);

						match(input, Token.DOWN, null);
						_last = (CommonTree) input.LT(1);
						IDENTIFIER30 = (CommonTree) match(input, IDENTIFIER,
								FOLLOW_IDENTIFIER_in_update_command322);
						IDENTIFIER30_tree = (CommonTree) adaptor
								.dupNode(IDENTIFIER30);

						adaptor.addChild(root_2, IDENTIFIER30_tree);

						match(input, Token.UP, null);
						adaptor.addChild(root_1, root_2);
						_last = _save_last_2;
					}

					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_set_clause_in_update_command325);
					set_clause31 = set_clause();

					state._fsp--;

					adaptor.addChild(root_1, set_clause31.getTree());
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_where_clause_in_update_command327);
					where_clause32 = where_clause();

					state._fsp--;

					adaptor.addChild(root_1, where_clause32.getTree());

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

				retval.table = (IDENTIFIER30 != null ? IDENTIFIER30.getText()
						: null);
				retval.user = user;

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

			retval.kvs = ((update_command_scope) update_command_stack.peek()).bound;

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
			update_command_stack.pop();
		}
		return retval;
	}

	// $ANTLR end "update_command"

	public static class set_clause_return extends TreeRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "set_clause"
	// D:\\gfile\\OBQLWalker.g:105:1: set_clause : ^( 'SET' ( pair )+ ) ;
	public final OBQLWalker.set_clause_return set_clause()
			throws RecognitionException {
		OBQLWalker.set_clause_return retval = new OBQLWalker.set_clause_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree string_literal33 = null;
		OBQLWalker.pair_return pair34 = null;

		CommonTree string_literal33_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:106:2: ( ^( 'SET' ( pair )+ ) )
			// D:\\gfile\\OBQLWalker.g:106:4: ^( 'SET' ( pair )+ )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					string_literal33 = (CommonTree) match(input, 30,
							FOLLOW_30_in_set_clause343);
					string_literal33_tree = (CommonTree) adaptor
							.dupNode(string_literal33);

					root_1 = (CommonTree) adaptor.becomeRoot(
							string_literal33_tree, root_1);

					match(input, Token.DOWN, null);
					// D:\\gfile\\OBQLWalker.g:106:12: ( pair )+
					int cnt6 = 0;
					loop6: do {
						int alt6 = 2;
						int LA6_0 = input.LA(1);

						if ((LA6_0 == IDENTIFIER)) {
							alt6 = 1;
						}

						switch (alt6) {
						case 1:
							// D:\\gfile\\OBQLWalker.g:106:12: pair
						{
							_last = (CommonTree) input.LT(1);
							pushFollow(FOLLOW_pair_in_set_clause345);
							pair34 = pair();

							state._fsp--;

							adaptor.addChild(root_1, pair34.getTree());

						}
							break;

						default:
							if (cnt6 >= 1)
								break loop6;
							EarlyExitException eee = new EarlyExitException(6,
									input);
							throw eee;
						}
						cnt6++;
					} while (true);

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "set_clause"

	public static class pair_return extends TreeRuleReturnScope {
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "pair"
	// D:\\gfile\\OBQLWalker.g:109:1: pair : ^( column_name IDENTIFIER ) ;
	public final OBQLWalker.pair_return pair() throws RecognitionException {
		OBQLWalker.pair_return retval = new OBQLWalker.pair_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree IDENTIFIER36 = null;
		OBQLWalker.column_name_return column_name35 = null;

		CommonTree IDENTIFIER36_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:109:7: ( ^( column_name IDENTIFIER ) )
			// D:\\gfile\\OBQLWalker.g:109:9: ^( column_name IDENTIFIER )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_column_name_in_pair359);
					column_name35 = column_name();

					state._fsp--;

					root_1 = (CommonTree) adaptor.becomeRoot(
							column_name35.getTree(), root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					IDENTIFIER36 = (CommonTree) match(input, IDENTIFIER,
							FOLLOW_IDENTIFIER_in_pair361);
					IDENTIFIER36_tree = (CommonTree) adaptor
							.dupNode(IDENTIFIER36);

					adaptor.addChild(root_1, IDENTIFIER36_tree);

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

				((update_command_scope) update_command_stack.peek()).bound.put(
						(column_name35 != null ? column_name35.name : null),
						(IDENTIFIER36 != null ? IDENTIFIER36.getText() : null));

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "pair"

	protected static class insert_command_scope {
		private List _values;
	}

	protected Stack insert_command_stack = new Stack();

	public static class insert_command_return extends TreeRuleReturnScope {
		public String table;
		public long user;
		public List<String> names;
		public List<String> values;
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "insert_command"
	// D:\\gfile\\OBQLWalker.g:111:1: insert_command returns [String table,long
	// user,List<String> names,List<String> values] : ^( 'INSERT' ^( TABLE t=
	// IDENTIFIER ) column_list (c= IDENTIFIER )+ where_clause ) ;
	public final OBQLWalker.insert_command_return insert_command()
			throws RecognitionException {
		insert_command_stack.push(new insert_command_scope());
		OBQLWalker.insert_command_return retval = new OBQLWalker.insert_command_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree t = null;
		CommonTree c = null;
		CommonTree string_literal37 = null;
		CommonTree TABLE38 = null;
		OBQLWalker.column_list_return column_list39 = null;

		OBQLWalker.where_clause_return where_clause40 = null;

		CommonTree t_tree = null;
		CommonTree c_tree = null;
		CommonTree string_literal37_tree = null;
		CommonTree TABLE38_tree = null;

		((insert_command_scope) insert_command_stack.peek())._values = new ArrayList<String>();

		try {
			// D:\\gfile\\OBQLWalker.g:121:2: ( ^( 'INSERT' ^( TABLE t=
			// IDENTIFIER ) column_list (c= IDENTIFIER )+ where_clause ) )
			// D:\\gfile\\OBQLWalker.g:121:4: ^( 'INSERT' ^( TABLE t= IDENTIFIER
			// ) column_list (c= IDENTIFIER )+ where_clause )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					string_literal37 = (CommonTree) match(input, 31,
							FOLLOW_31_in_insert_command391);
					string_literal37_tree = (CommonTree) adaptor
							.dupNode(string_literal37);

					root_1 = (CommonTree) adaptor.becomeRoot(
							string_literal37_tree, root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					{
						CommonTree _save_last_2 = _last;
						CommonTree _first_2 = null;
						CommonTree root_2 = (CommonTree) adaptor.nil();
						_last = (CommonTree) input.LT(1);
						TABLE38 = (CommonTree) match(input, TABLE,
								FOLLOW_TABLE_in_insert_command394);
						TABLE38_tree = (CommonTree) adaptor.dupNode(TABLE38);

						root_2 = (CommonTree) adaptor.becomeRoot(TABLE38_tree,
								root_2);

						match(input, Token.DOWN, null);
						_last = (CommonTree) input.LT(1);
						t = (CommonTree) match(input, IDENTIFIER,
								FOLLOW_IDENTIFIER_in_insert_command398);
						t_tree = (CommonTree) adaptor.dupNode(t);

						adaptor.addChild(root_2, t_tree);

						match(input, Token.UP, null);
						adaptor.addChild(root_1, root_2);
						_last = _save_last_2;
					}

					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_column_list_in_insert_command401);
					column_list39 = column_list();

					state._fsp--;

					adaptor.addChild(root_1, column_list39.getTree());
					// D:\\gfile\\OBQLWalker.g:121:49: (c= IDENTIFIER )+
					int cnt7 = 0;
					loop7: do {
						int alt7 = 2;
						int LA7_0 = input.LA(1);

						if ((LA7_0 == IDENTIFIER)) {
							alt7 = 1;
						}

						switch (alt7) {
						case 1:
							// D:\\gfile\\OBQLWalker.g:121:50: c= IDENTIFIER
						{
							_last = (CommonTree) input.LT(1);
							c = (CommonTree) match(input, IDENTIFIER,
									FOLLOW_IDENTIFIER_in_insert_command406);
							c_tree = (CommonTree) adaptor.dupNode(c);

							adaptor.addChild(root_1, c_tree);

							((insert_command_scope) insert_command_stack.peek())._values
									.add((c != null ? c.getText() : null));

						}
							break;

						default:
							if (cnt7 >= 1)
								break loop7;
							EarlyExitException eee = new EarlyExitException(7,
									input);
							throw eee;
						}
						cnt7++;
					} while (true);

					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_where_clause_in_insert_command411);
					where_clause40 = where_clause();

					state._fsp--;

					adaptor.addChild(root_1, where_clause40.getTree());

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

				retval.table = (t != null ? t.getText() : null);
				retval.user = user;
				retval.names = (column_list39 != null ? column_list39.names
						: null);

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

			retval.values = ((insert_command_scope) insert_command_stack.peek())._values;

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
			insert_command_stack.pop();
		}
		return retval;
	}

	// $ANTLR end "insert_command"

	public static class delete_command_return extends TreeRuleReturnScope {
		public String table;
		public long user;
		CommonTree tree;

		public Object getTree() {
			return tree;
		}
	};

	// $ANTLR start "delete_command"
	// D:\\gfile\\OBQLWalker.g:129:1: delete_command returns [String table,long
	// user] : ^( 'DELETE' from_clause where_clause ) ;
	public final OBQLWalker.delete_command_return delete_command()
			throws RecognitionException {
		OBQLWalker.delete_command_return retval = new OBQLWalker.delete_command_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;

		CommonTree string_literal41 = null;
		OBQLWalker.from_clause_return from_clause42 = null;

		OBQLWalker.where_clause_return where_clause43 = null;

		CommonTree string_literal41_tree = null;

		try {
			// D:\\gfile\\OBQLWalker.g:130:2: ( ^( 'DELETE' from_clause
			// where_clause ) )
			// D:\\gfile\\OBQLWalker.g:130:4: ^( 'DELETE' from_clause
			// where_clause )
			{
				root_0 = (CommonTree) adaptor.nil();

				_last = (CommonTree) input.LT(1);
				{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree) adaptor.nil();
					_last = (CommonTree) input.LT(1);
					string_literal41 = (CommonTree) match(input, 36,
							FOLLOW_36_in_delete_command433);
					string_literal41_tree = (CommonTree) adaptor
							.dupNode(string_literal41);

					root_1 = (CommonTree) adaptor.becomeRoot(
							string_literal41_tree, root_1);

					match(input, Token.DOWN, null);
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_from_clause_in_delete_command435);
					from_clause42 = from_clause();

					state._fsp--;

					adaptor.addChild(root_1, from_clause42.getTree());
					_last = (CommonTree) input.LT(1);
					pushFollow(FOLLOW_where_clause_in_delete_command437);
					where_clause43 = where_clause();

					state._fsp--;

					adaptor.addChild(root_1, where_clause43.getTree());

					match(input, Token.UP, null);
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
				}

				retval.table = table;
				retval.user = user;

			}

			retval.tree = (CommonTree) adaptor.rulePostProcessing(root_0);

		} catch (RecognitionException re) {
			reportError(re);
			recover(input, re);
		} finally {
		}
		return retval;
	}

	// $ANTLR end "delete_command"

	// Delegated rules

	public static final BitSet FOLLOW_select_clause_in_select_command58 = new BitSet(
			new long[] { 0x0000000000000010L });
	public static final BitSet FOLLOW_from_clause_in_select_command60 = new BitSet(
			new long[] { 0x0000000001000000L });
	public static final BitSet FOLLOW_where_clause_in_select_command62 = new BitSet(
			new long[] { 0x0000000000000022L });
	public static final BitSet FOLLOW_orderby_clause_in_select_command65 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_22_in_select_clause83 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_column_list_in_select_clause85 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_COLUMNLIST_in_column_list118 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_column_name_in_column_list121 = new BitSet(
			new long[] { 0x0000000000080008L });
	public static final BitSet FOLLOW_IDENTIFIER_in_column_name140 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_TABLE_in_from_clause154 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_IDENTIFIER_in_from_clause156 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_24_in_where_clause171 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_query_conditions_in_where_clause173 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_query_condition_in_query_conditions185 = new BitSet(
			new long[] { 0x0000000010000002L });
	public static final BitSet FOLLOW_28_in_query_conditions188 = new BitSet(
			new long[] { 0x0000000000004000L });
	public static final BitSet FOLLOW_query_condition_in_query_conditions190 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_op_type_in_query_condition203 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_INT_in_query_condition205 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_EQ_in_op_type220 = new BitSet(
			new long[] { 0x0000000000000002L });
	public static final BitSet FOLLOW_ORDERBY_in_orderby_clause241 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_order_column_list_in_orderby_clause243 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_ORDERLIST_in_order_column_list257 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_order_column_in_order_column_list259 = new BitSet(
			new long[] { 0x0000000000000C08L });
	public static final BitSet FOLLOW_ASC_in_order_column273 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_column_name_in_order_column275 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_DESC_in_order_column283 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_column_name_in_order_column285 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_29_in_update_command317 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_TABLE_in_update_command320 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_IDENTIFIER_in_update_command322 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_set_clause_in_update_command325 = new BitSet(
			new long[] { 0x0000000001000000L });
	public static final BitSet FOLLOW_where_clause_in_update_command327 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_30_in_set_clause343 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_pair_in_set_clause345 = new BitSet(
			new long[] { 0x0000000000080008L });
	public static final BitSet FOLLOW_column_name_in_pair359 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_IDENTIFIER_in_pair361 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_31_in_insert_command391 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_TABLE_in_insert_command394 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_IDENTIFIER_in_insert_command398 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_column_list_in_insert_command401 = new BitSet(
			new long[] { 0x0000000000080000L });
	public static final BitSet FOLLOW_IDENTIFIER_in_insert_command406 = new BitSet(
			new long[] { 0x0000000001080000L });
	public static final BitSet FOLLOW_where_clause_in_insert_command411 = new BitSet(
			new long[] { 0x0000000000000008L });
	public static final BitSet FOLLOW_36_in_delete_command433 = new BitSet(
			new long[] { 0x0000000000000004L });
	public static final BitSet FOLLOW_from_clause_in_delete_command435 = new BitSet(
			new long[] { 0x0000000001000000L });
	public static final BitSet FOLLOW_where_clause_in_delete_command437 = new BitSet(
			new long[] { 0x0000000000000008L });

}