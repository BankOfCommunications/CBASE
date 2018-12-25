#!/bin/bash
#
# AUTHOR: Zhifeng YANG
# DATE: 2012-10-24
# DESCRIPTION:
#

#sql_parser.tab.c sql_parser.tab.h type_name.c: sql_parser.y
#        tools/bison -d $<
#        tools/gen_type_name.sh ob_item_type.h > type_name.c

#sql_parser.lex.c sql_parser.lex.h : sql_parser.l sql_parser.tab.h
#        tools/flex -osql_parser.lex.c $<
set +x
export PATH=/usr/local/bin:$PATH
bison -d sql_parser.y
flex -o sql_parser.lex.c sql_parser.l sql_parser.tab.h
./gen_type_name.sh ob_item_type.h >type_name.c
