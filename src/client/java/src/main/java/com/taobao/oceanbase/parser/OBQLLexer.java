// $ANTLR 3.2 Sep 23, 2009 12:02:23 D:\\gfile\\OBQL.g 2010-10-29 21:05:26
package com.taobao.oceanbase.parser;

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class OBQLLexer extends Lexer {
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

	public OBQLLexer() {
		;
	}

	public OBQLLexer(CharStream input) {
		this(input, new RecognizerSharedState());
	}

	public OBQLLexer(CharStream input, RecognizerSharedState state) {
		super(input, state);

	}

	public String getGrammarFileName() {
		return "D:\\gfile\\OBQL.g";
	}

	// $ANTLR start "ASC"
	public final void mASC() throws RecognitionException {
		try {
			int _type = ASC;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:9:5: ( 'ASC' )
			// D:\\gfile\\OBQL.g:9:7: 'ASC'
			{
				match("ASC");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "ASC"

	// $ANTLR start "DESC"
	public final void mDESC() throws RecognitionException {
		try {
			int _type = DESC;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:10:6: ( 'DESC' )
			// D:\\gfile\\OBQL.g:10:8: 'DESC'
			{
				match("DESC");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "DESC"

	// $ANTLR start "ROWKEY"
	public final void mROWKEY() throws RecognitionException {
		try {
			int _type = ROWKEY;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:11:8: ( 'ROWKEY' )
			// D:\\gfile\\OBQL.g:11:10: 'ROWKEY'
			{
				match("ROWKEY");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "ROWKEY"

	// $ANTLR start "COMMA"
	public final void mCOMMA() throws RecognitionException {
		try {
			int _type = COMMA;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:12:7: ( ',' )
			// D:\\gfile\\OBQL.g:12:9: ','
			{
				match(',');

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "COMMA"

	// $ANTLR start "EQ"
	public final void mEQ() throws RecognitionException {
		try {
			int _type = EQ;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:13:4: ( '=' )
			// D:\\gfile\\OBQL.g:13:6: '='
			{
				match('=');

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "EQ"

	// $ANTLR start "LT"
	public final void mLT() throws RecognitionException {
		try {
			int _type = LT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:14:4: ( '<' )
			// D:\\gfile\\OBQL.g:14:6: '<'
			{
				match('<');

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "LT"

	// $ANTLR start "GT"
	public final void mGT() throws RecognitionException {
		try {
			int _type = GT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:15:4: ( '>' )
			// D:\\gfile\\OBQL.g:15:6: '>'
			{
				match('>');

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "GT"

	// $ANTLR start "LE"
	public final void mLE() throws RecognitionException {
		try {
			int _type = LE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:16:4: ( '<=' )
			// D:\\gfile\\OBQL.g:16:6: '<='
			{
				match("<=");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "LE"

	// $ANTLR start "GE"
	public final void mGE() throws RecognitionException {
		try {
			int _type = GE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:17:4: ( '>=' )
			// D:\\gfile\\OBQL.g:17:6: '>='
			{
				match(">=");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "GE"

	// $ANTLR start "T__22"
	public final void mT__22() throws RecognitionException {
		try {
			int _type = T__22;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:18:7: ( 'SELECT' )
			// D:\\gfile\\OBQL.g:18:9: 'SELECT'
			{
				match("SELECT");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__22"

	// $ANTLR start "T__23"
	public final void mT__23() throws RecognitionException {
		try {
			int _type = T__23;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:19:7: ( 'FROM' )
			// D:\\gfile\\OBQL.g:19:9: 'FROM'
			{
				match("FROM");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__23"

	// $ANTLR start "T__24"
	public final void mT__24() throws RecognitionException {
		try {
			int _type = T__24;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:20:7: ( 'WHERE' )
			// D:\\gfile\\OBQL.g:20:9: 'WHERE'
			{
				match("WHERE");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__24"

	// $ANTLR start "T__25"
	public final void mT__25() throws RecognitionException {
		try {
			int _type = T__25;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:21:7: ( 'ORDER' )
			// D:\\gfile\\OBQL.g:21:9: 'ORDER'
			{
				match("ORDER");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__25"

	// $ANTLR start "T__26"
	public final void mT__26() throws RecognitionException {
		try {
			int _type = T__26;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:22:7: ( 'BY' )
			// D:\\gfile\\OBQL.g:22:9: 'BY'
			{
				match("BY");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__26"

	// $ANTLR start "T__27"
	public final void mT__27() throws RecognitionException {
		try {
			int _type = T__27;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:23:7: ( 'LIMIT' )
			// D:\\gfile\\OBQL.g:23:9: 'LIMIT'
			{
				match("LIMIT");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__27"

	// $ANTLR start "T__28"
	public final void mT__28() throws RecognitionException {
		try {
			int _type = T__28;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:24:7: ( 'AND' )
			// D:\\gfile\\OBQL.g:24:9: 'AND'
			{
				match("AND");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__28"

	// $ANTLR start "T__29"
	public final void mT__29() throws RecognitionException {
		try {
			int _type = T__29;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:25:7: ( 'UPDATE' )
			// D:\\gfile\\OBQL.g:25:9: 'UPDATE'
			{
				match("UPDATE");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__29"

	// $ANTLR start "T__30"
	public final void mT__30() throws RecognitionException {
		try {
			int _type = T__30;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:26:7: ( 'SET' )
			// D:\\gfile\\OBQL.g:26:9: 'SET'
			{
				match("SET");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__30"

	// $ANTLR start "T__31"
	public final void mT__31() throws RecognitionException {
		try {
			int _type = T__31;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:27:7: ( 'INSERT' )
			// D:\\gfile\\OBQL.g:27:9: 'INSERT'
			{
				match("INSERT");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__31"

	// $ANTLR start "T__32"
	public final void mT__32() throws RecognitionException {
		try {
			int _type = T__32;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:28:7: ( 'INTO' )
			// D:\\gfile\\OBQL.g:28:9: 'INTO'
			{
				match("INTO");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__32"

	// $ANTLR start "T__33"
	public final void mT__33() throws RecognitionException {
		try {
			int _type = T__33;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:29:7: ( '(' )
			// D:\\gfile\\OBQL.g:29:9: '('
			{
				match('(');

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__33"

	// $ANTLR start "T__34"
	public final void mT__34() throws RecognitionException {
		try {
			int _type = T__34;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:30:7: ( ')' )
			// D:\\gfile\\OBQL.g:30:9: ')'
			{
				match(')');

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__34"

	// $ANTLR start "T__35"
	public final void mT__35() throws RecognitionException {
		try {
			int _type = T__35;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:31:7: ( 'VALUES' )
			// D:\\gfile\\OBQL.g:31:9: 'VALUES'
			{
				match("VALUES");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__35"

	// $ANTLR start "T__36"
	public final void mT__36() throws RecognitionException {
		try {
			int _type = T__36;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:32:7: ( 'DELETE' )
			// D:\\gfile\\OBQL.g:32:9: 'DELETE'
			{
				match("DELETE");

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "T__36"

	// $ANTLR start "IDENTIFIER"
	public final void mIDENTIFIER() throws RecognitionException {
		try {
			int _type = IDENTIFIER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:39:13: ( ( 'A' .. 'Z' | 'a' .. 'z' ) ( 'A' ..
			// 'Z' | 'a' .. 'z' | '0' .. '9' | '_' )* )
			// D:\\gfile\\OBQL.g:39:15: ( 'A' .. 'Z' | 'a' .. 'z' ) ( 'A' .. 'Z'
			// | 'a' .. 'z' | '0' .. '9' | '_' )*
			{
				if ((input.LA(1) >= 'A' && input.LA(1) <= 'Z')
						|| (input.LA(1) >= 'a' && input.LA(1) <= 'z')) {
					input.consume();

				} else {
					MismatchedSetException mse = new MismatchedSetException(
							null, input);
					recover(mse);
					throw mse;
				}

				// D:\\gfile\\OBQL.g:39:34: ( 'A' .. 'Z' | 'a' .. 'z' | '0' ..
				// '9' | '_' )*
				loop1: do {
					int alt1 = 2;
					int LA1_0 = input.LA(1);

					if (((LA1_0 >= '0' && LA1_0 <= '9')
							|| (LA1_0 >= 'A' && LA1_0 <= 'Z') || LA1_0 == '_' || (LA1_0 >= 'a' && LA1_0 <= 'z'))) {
						alt1 = 1;
					}

					switch (alt1) {
					case 1:
						// D:\\gfile\\OBQL.g:
					{
						if ((input.LA(1) >= '0' && input.LA(1) <= '9')
								|| (input.LA(1) >= 'A' && input.LA(1) <= 'Z')
								|| input.LA(1) == '_'
								|| (input.LA(1) >= 'a' && input.LA(1) <= 'z')) {
							input.consume();

						} else {
							MismatchedSetException mse = new MismatchedSetException(
									null, input);
							recover(mse);
							throw mse;
						}

					}
						break;

					default:
						break loop1;
					}
				} while (true);

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "IDENTIFIER"

	// $ANTLR start "INT"
	public final void mINT() throws RecognitionException {
		try {
			int _type = INT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:40:6: ( ( '0' .. '9' ) ( '0' .. '9' )* )
			// D:\\gfile\\OBQL.g:40:9: ( '0' .. '9' ) ( '0' .. '9' )*
			{
				// D:\\gfile\\OBQL.g:40:9: ( '0' .. '9' )
				// D:\\gfile\\OBQL.g:40:10: '0' .. '9'
				{
					matchRange('0', '9');

				}

				// D:\\gfile\\OBQL.g:40:19: ( '0' .. '9' )*
				loop2: do {
					int alt2 = 2;
					int LA2_0 = input.LA(1);

					if (((LA2_0 >= '0' && LA2_0 <= '9'))) {
						alt2 = 1;
					}

					switch (alt2) {
					case 1:
						// D:\\gfile\\OBQL.g:40:20: '0' .. '9'
					{
						matchRange('0', '9');

					}
						break;

					default:
						break loop2;
					}
				} while (true);

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "INT"

	// $ANTLR start "WS"
	public final void mWS() throws RecognitionException {
		try {
			int _type = WS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\gfile\\OBQL.g:41:5: ( ( ' ' | '\\t' | '\\r' '\\n' | '\\n' |
			// '\\r' ) )
			// D:\\gfile\\OBQL.g:41:9: ( ' ' | '\\t' | '\\r' '\\n' | '\\n' |
			// '\\r' )
			{
				// D:\\gfile\\OBQL.g:41:9: ( ' ' | '\\t' | '\\r' '\\n' | '\\n' |
				// '\\r' )
				int alt3 = 5;
				switch (input.LA(1)) {
				case ' ': {
					alt3 = 1;
				}
					break;
				case '\t': {
					alt3 = 2;
				}
					break;
				case '\r': {
					int LA3_3 = input.LA(2);

					if ((LA3_3 == '\n')) {
						alt3 = 3;
					} else {
						alt3 = 5;
					}
				}
					break;
				case '\n': {
					alt3 = 4;
				}
					break;
				default:
					NoViableAltException nvae = new NoViableAltException("", 3,
							0, input);

					throw nvae;
				}

				switch (alt3) {
				case 1:
					// D:\\gfile\\OBQL.g:41:10: ' '
				{
					match(' ');

				}
					break;
				case 2:
					// D:\\gfile\\OBQL.g:41:14: '\\t'
				{
					match('\t');

				}
					break;
				case 3:
					// D:\\gfile\\OBQL.g:41:19: '\\r' '\\n'
				{
					match('\r');
					match('\n');

				}
					break;
				case 4:
					// D:\\gfile\\OBQL.g:41:30: '\\n'
				{
					match('\n');

				}
					break;
				case 5:
					// D:\\gfile\\OBQL.g:41:35: '\\r'
				{
					match('\r');

				}
					break;

				}

				skip();

			}

			state.type = _type;
			state.channel = _channel;
		} finally {
		}
	}

	// $ANTLR end "WS"

	public void mTokens() throws RecognitionException {
		// D:\\gfile\\OBQL.g:1:8: ( ASC | DESC | ROWKEY | COMMA | EQ | LT | GT |
		// LE | GE | T__22 | T__23 | T__24 | T__25 | T__26 | T__27 | T__28 |
		// T__29 | T__30 | T__31 | T__32 | T__33 | T__34 | T__35 | T__36 |
		// IDENTIFIER | INT | WS )
		int alt4 = 27;
		alt4 = dfa4.predict(input);
		switch (alt4) {
		case 1:
			// D:\\gfile\\OBQL.g:1:10: ASC
		{
			mASC();

		}
			break;
		case 2:
			// D:\\gfile\\OBQL.g:1:14: DESC
		{
			mDESC();

		}
			break;
		case 3:
			// D:\\gfile\\OBQL.g:1:19: ROWKEY
		{
			mROWKEY();

		}
			break;
		case 4:
			// D:\\gfile\\OBQL.g:1:26: COMMA
		{
			mCOMMA();

		}
			break;
		case 5:
			// D:\\gfile\\OBQL.g:1:32: EQ
		{
			mEQ();

		}
			break;
		case 6:
			// D:\\gfile\\OBQL.g:1:35: LT
		{
			mLT();

		}
			break;
		case 7:
			// D:\\gfile\\OBQL.g:1:38: GT
		{
			mGT();

		}
			break;
		case 8:
			// D:\\gfile\\OBQL.g:1:41: LE
		{
			mLE();

		}
			break;
		case 9:
			// D:\\gfile\\OBQL.g:1:44: GE
		{
			mGE();

		}
			break;
		case 10:
			// D:\\gfile\\OBQL.g:1:47: T__22
		{
			mT__22();

		}
			break;
		case 11:
			// D:\\gfile\\OBQL.g:1:53: T__23
		{
			mT__23();

		}
			break;
		case 12:
			// D:\\gfile\\OBQL.g:1:59: T__24
		{
			mT__24();

		}
			break;
		case 13:
			// D:\\gfile\\OBQL.g:1:65: T__25
		{
			mT__25();

		}
			break;
		case 14:
			// D:\\gfile\\OBQL.g:1:71: T__26
		{
			mT__26();

		}
			break;
		case 15:
			// D:\\gfile\\OBQL.g:1:77: T__27
		{
			mT__27();

		}
			break;
		case 16:
			// D:\\gfile\\OBQL.g:1:83: T__28
		{
			mT__28();

		}
			break;
		case 17:
			// D:\\gfile\\OBQL.g:1:89: T__29
		{
			mT__29();

		}
			break;
		case 18:
			// D:\\gfile\\OBQL.g:1:95: T__30
		{
			mT__30();

		}
			break;
		case 19:
			// D:\\gfile\\OBQL.g:1:101: T__31
		{
			mT__31();

		}
			break;
		case 20:
			// D:\\gfile\\OBQL.g:1:107: T__32
		{
			mT__32();

		}
			break;
		case 21:
			// D:\\gfile\\OBQL.g:1:113: T__33
		{
			mT__33();

		}
			break;
		case 22:
			// D:\\gfile\\OBQL.g:1:119: T__34
		{
			mT__34();

		}
			break;
		case 23:
			// D:\\gfile\\OBQL.g:1:125: T__35
		{
			mT__35();

		}
			break;
		case 24:
			// D:\\gfile\\OBQL.g:1:131: T__36
		{
			mT__36();

		}
			break;
		case 25:
			// D:\\gfile\\OBQL.g:1:137: IDENTIFIER
		{
			mIDENTIFIER();

		}
			break;
		case 26:
			// D:\\gfile\\OBQL.g:1:148: INT
		{
			mINT();

		}
			break;
		case 27:
			// D:\\gfile\\OBQL.g:1:152: WS
		{
			mWS();

		}
			break;

		}

	}

	protected DFA4 dfa4 = new DFA4(this);
	static final String DFA4_eotS = "\1\uffff\3\23\2\uffff\1\33\1\35\10\23\2\uffff\1\23\3\uffff\4\23"
			+ "\4\uffff\4\23\1\61\4\23\1\67\1\70\4\23\1\75\3\23\1\uffff\5\23\2"
			+ "\uffff\1\106\3\23\1\uffff\1\112\5\23\1\120\1\23\1\uffff\3\23\1\uffff"
			+ "\1\125\1\126\1\127\2\23\1\uffff\1\23\1\133\1\134\1\135\3\uffff\1"
			+ "\136\1\137\1\140\6\uffff";
	static final String DFA4_eofS = "\141\uffff";
	static final String DFA4_minS = "\1\11\1\116\1\105\1\117\2\uffff\2\75\1\105\1\122\1\110\1\122\1"
			+ "\131\1\111\1\120\1\116\2\uffff\1\101\3\uffff\1\103\1\104\1\114\1"
			+ "\127\4\uffff\1\114\1\117\1\105\1\104\1\60\1\115\1\104\1\123\1\114"
			+ "\2\60\1\103\1\105\1\113\1\105\1\60\1\115\1\122\1\105\1\uffff\1\111"
			+ "\1\101\1\105\1\117\1\125\2\uffff\1\60\1\124\1\105\1\103\1\uffff"
			+ "\1\60\1\105\1\122\2\124\1\122\1\60\1\105\1\uffff\1\105\1\131\1\124"
			+ "\1\uffff\3\60\1\105\1\124\1\uffff\1\123\3\60\3\uffff\3\60\6\uffff";
	static final String DFA4_maxS = "\1\172\1\123\1\105\1\117\2\uffff\2\75\1\105\1\122\1\110\1\122\1"
			+ "\131\1\111\1\120\1\116\2\uffff\1\101\3\uffff\1\103\1\104\1\123\1"
			+ "\127\4\uffff\1\124\1\117\1\105\1\104\1\172\1\115\1\104\1\124\1\114"
			+ "\2\172\1\103\1\105\1\113\1\105\1\172\1\115\1\122\1\105\1\uffff\1"
			+ "\111\1\101\1\105\1\117\1\125\2\uffff\1\172\1\124\1\105\1\103\1\uffff"
			+ "\1\172\1\105\1\122\2\124\1\122\1\172\1\105\1\uffff\1\105\1\131\1"
			+ "\124\1\uffff\3\172\1\105\1\124\1\uffff\1\123\3\172\3\uffff\3\172"
			+ "\6\uffff";
	static final String DFA4_acceptS = "\4\uffff\1\4\1\5\12\uffff\1\25\1\26\1\uffff\1\31\1\32\1\33\4\uffff"
			+ "\1\10\1\6\1\11\1\7\23\uffff\1\16\5\uffff\1\1\1\20\4\uffff\1\22\10"
			+ "\uffff\1\2\3\uffff\1\13\5\uffff\1\24\4\uffff\1\14\1\15\1\17\3\uffff"
			+ "\1\30\1\3\1\12\1\21\1\23\1\27";
	static final String DFA4_specialS = "\141\uffff}>";
	static final String[] DFA4_transitionS = {
			"\2\25\2\uffff\1\25\22\uffff\1\25\7\uffff\1\20\1\21\2\uffff"
					+ "\1\4\3\uffff\12\24\2\uffff\1\6\1\5\1\7\2\uffff\1\1\1\14\1\23"
					+ "\1\2\1\23\1\11\2\23\1\17\2\23\1\15\2\23\1\13\2\23\1\3\1\10\1"
					+ "\23\1\16\1\22\1\12\3\23\6\uffff\32\23",
			"\1\27\4\uffff\1\26", "\1\30", "\1\31", "", "", "\1\32", "\1\34",
			"\1\36", "\1\37", "\1\40", "\1\41", "\1\42", "\1\43", "\1\44",
			"\1\45", "", "", "\1\46", "", "", "", "\1\47", "\1\50",
			"\1\52\6\uffff\1\51", "\1\53", "", "", "", "",
			"\1\54\7\uffff\1\55", "\1\56", "\1\57", "\1\60",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23", "\1\62",
			"\1\63", "\1\64\1\65", "\1\66",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23", "\1\71",
			"\1\72", "\1\73", "\1\74",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23", "\1\76",
			"\1\77", "\1\100", "", "\1\101", "\1\102", "\1\103", "\1\104",
			"\1\105", "", "",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23", "\1\107",
			"\1\110", "\1\111", "",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23", "\1\113",
			"\1\114", "\1\115", "\1\116", "\1\117",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23", "\1\121", "",
			"\1\122", "\1\123", "\1\124", "",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23", "\1\130",
			"\1\131", "", "\1\132",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23", "", "", "",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23",
			"\12\23\7\uffff\32\23\4\uffff\1\23\1\uffff\32\23", "", "", "", "",
			"", "" };

	static final short[] DFA4_eot = DFA.unpackEncodedString(DFA4_eotS);
	static final short[] DFA4_eof = DFA.unpackEncodedString(DFA4_eofS);
	static final char[] DFA4_min = DFA
			.unpackEncodedStringToUnsignedChars(DFA4_minS);
	static final char[] DFA4_max = DFA
			.unpackEncodedStringToUnsignedChars(DFA4_maxS);
	static final short[] DFA4_accept = DFA.unpackEncodedString(DFA4_acceptS);
	static final short[] DFA4_special = DFA.unpackEncodedString(DFA4_specialS);
	static final short[][] DFA4_transition;

	static {
		int numStates = DFA4_transitionS.length;
		DFA4_transition = new short[numStates][];
		for (int i = 0; i < numStates; i++) {
			DFA4_transition[i] = DFA.unpackEncodedString(DFA4_transitionS[i]);
		}
	}

	class DFA4 extends DFA {

		public DFA4(BaseRecognizer recognizer) {
			this.recognizer = recognizer;
			this.decisionNumber = 4;
			this.eot = DFA4_eot;
			this.eof = DFA4_eof;
			this.min = DFA4_min;
			this.max = DFA4_max;
			this.accept = DFA4_accept;
			this.special = DFA4_special;
			this.transition = DFA4_transition;
		}

		public String getDescription() {
			return "1:1: Tokens : ( ASC | DESC | ROWKEY | COMMA | EQ | LT | GT | LE | GE | T__22 | T__23 | T__24 | T__25 | T__26 | T__27 | T__28 | T__29 | T__30 | T__31 | T__32 | T__33 | T__34 | T__35 | T__36 | IDENTIFIER | INT | WS );";
		}
	}

}