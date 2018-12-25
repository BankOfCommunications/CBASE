tree grammar OBQLWalker;

options {
	tokenVocab=OBQL;
	ASTLabelType=CommonTree;
	language=Java;
	output=AST;
}

@header {
	package com.taobao.oceanbase.parser;
	
	import java.util.Map;
	import java.util.HashMap;
	import java.util.List;
	import java.util.ArrayList;
}

@members {
	String table;
	long user;
}

select_command returns [List<String> names,String table,long user]
	: select_clause from_clause where_clause (orderby_clause)? {$names = $select_clause.names;$table=table;$user=user;}
	;

select_clause returns [List names]
:^('SELECT' column_list) {$names = $column_list.names;}
	;
	
column_list returns [List<String> names]
	scope{
		private List<String> _names;
	}
	@init{
		$column_list::_names = new ArrayList<String>();
	}
	@after{
		$names = $column_list::_names;
	}
: ^(COLUMNLIST (column_name{$column_list::_names.add($column_name.name);})+)
	;

column_name returns [String name]
	:	IDENTIFIER {$name = $IDENTIFIER.text;}
	;

from_clause
	: ^(TABLE IDENTIFIER) {table = $IDENTIFIER.text;}
	;

where_clause
	: ^('WHERE' query_conditions)
	;

query_conditions
	: query_condition ('AND' query_condition)?
	;

query_condition
	:^(op_type INT) {user = Long.parseLong($INT.text);}
	;

op_type
	:
	EQ 
	//| LT {qinfo.setInclusiveEnd(false);}
	//| GT {qinfo.setInclusiveStart(false);}
	//| LE {qinfo.setInclusiveEnd(true);}
	//| GE {qinfo.setInclusiveStart(true);}
	;

orderby_clause
	:	^(ORDERBY order_column_list)
	;

order_column_list
	: 	^(ORDERLIST order_column+)
	;

order_column
	:	^(ASC column_name) //{ qinfo.addOrderBy($column_name.name, true); }
	|	^(DESC column_name) //{ qinfo.addOrderBy($column_name.name, false); }
	;
	
update_command returns[String table,long user,Map<String,String> kvs]
	scope{
		private String key;
		private Map<String,String> bound;
	}
	@init{
		$update_command::bound = new HashMap<String,String>();
	}
	@after{
		$kvs = $update_command::bound;
	}
	:	^('UPDATE' ^(TABLE IDENTIFIER) set_clause where_clause)
	{
		$table = $IDENTIFIER.text;
		$user = user;
	}
	;

set_clause
	:	^('SET' pair+)
	;

pair 	:	^(column_name IDENTIFIER){$update_command::bound.put($column_name.name,$IDENTIFIER.text);};

insert_command returns [String table,long user,List<String> names,List<String> values]
	scope{
		private List _values;
	}
	@init{
		$insert_command::_values = new ArrayList<String>();
	}
	@after{
		$values = $insert_command::_values;
	}
	:	^('INSERT' ^(TABLE t=IDENTIFIER) column_list (c=IDENTIFIER{$insert_command::_values.add($c.text);})+ where_clause)
		{
			$table = $t.text;
			$user = user;
			$names = $column_list.names;
		}
	;
	
delete_command returns [String table,long user]
	:	^('DELETE' from_clause where_clause){$table = table; $user = user;}
	;
	