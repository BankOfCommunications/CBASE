grammar OBQL;

options {
	output=AST;
	language=Java;
	ASTLabelType=CommonTree;
}

tokens {
	TABLE;
	ORDERBY;
	GROUPBY;
	LIMIT;
	COLUMNLIST;
	ORDERLIST;
	ASC = 'ASC';
	DESC = 'DESC';
	ROWKEY = 'ROWKEY';
	COMMA = ',';
	EQ = '=';
	LT = '<';
	GT = '>';
	LE = '<=';
	GE = '>=';
}

@lexer::header{ package  com.taobao.oceanbase.parser; } 
@header{
package com.taobao.oceanbase.parser;

}

@rulecatch {
catch (RecognitionException e) {
throw e;
}
}

IDENTIFIER 	:	('A'..'Z'|'a'..'z')('A'..'Z'|'a'..'z'|'0'..'9'|'_')*;
INT 	:	 ('0'..'9')('0'..'9')*;
WS 	:	  (' '|'\t'|'\r' '\n' |'\n'|'\r') {skip();};  //ignore this token;
	
select_command
	: select_clause from_clause where_clause (orderby_clause)? (limit_clause)?
	;	
		
select_clause
	:'SELECT' column_list -> ^('SELECT' column_list)
	;
	
from_clause
	:'FROM'! table_name
	;
	
where_clause
	:'WHERE' query_conditions -> ^('WHERE' query_conditions)
	;

orderby_clause
	:'ORDER' 'BY' order_column_list -> ^(ORDERBY order_column_list)		
	;

limit_clause
	: 'LIMIT' (offset COMMA)? count	-> ^(LIMIT offset? count)
	;

query_conditions
	: query_condition ('AND'! query_condition)?
	;
	
query_condition
	:	ROWKEY op_type INT -> ^(op_type INT)
	;
	
column_name
	:	IDENTIFIER
	;
	
table_name
	:	IDENTIFIER -> ^(TABLE IDENTIFIER)
	;
	
column_list
	:	column_name (COMMA column_name)* -> ^(COLUMNLIST column_name+)
	;
	
order_column
	:	column_name -> ^(ASC column_name)
	|	column_name ASC -> ^(ASC column_name)
	|	column_name DESC -> ^(DESC column_name)
	;

order_column_list
	:	order_column (COMMA order_column)* -> ^(ORDERLIST order_column+)
	;

offset	:INT;

count	:INT;
	
op_type	:EQ | LT | GT | LE | GE;
	
update_command 
	:	'UPDATE' table_name set_clause where_clause -> ^('UPDATE' table_name set_clause where_clause)
	;

set_clause
	:	'SET' pair (COMMA pair)*  -> ^('SET' pair+)
	;

pair 	:	column_name '=' IDENTIFIER -> ^(column_name IDENTIFIER);

insert_command
	:	'INSERT' 'INTO' table_name '(' column_list ')' 'VALUES' '('  IDENTIFIER (COMMA IDENTIFIER)* ')' where_clause
		-> ^('INSERT' table_name column_list IDENTIFIER+ where_clause)
	;
	
delete_command
	:'DELETE' from_clause where_clause -> ^('DELETE' from_clause where_clause)
	;