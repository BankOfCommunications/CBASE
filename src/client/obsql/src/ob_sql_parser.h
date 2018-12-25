/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_parser.h is for what ...
 *
 * Version: ***: ob_sql_parser.h  Fri Dec 28 10:36:02 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_PARSER_H_
#define OB_SQL_PARSER_H_

#include "ob_sql_define.h"
OB_SQL_CPP_START
#include "ob_sql_struct.h"
ObSQLType get_sql_type(const char* q, unsigned long length);
OB_SQL_CPP_END
#endif
