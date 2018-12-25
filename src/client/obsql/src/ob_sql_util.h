/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_util.h is for what ...
 *
 * Version: ***: ob_sql_util.h  Mon Nov 19 16:35:34 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_UTIL_H_
#define OB_SQL_UTIL_H_
#include "ob_sql_define.h"

OB_SQL_CPP_START
#include <stdlib.h>
#include "ob_sql_struct.h"
void dump_config(ObSQLGlobalConfig *config, const char *format_str);
void dump_table();
int get_server_ip(ObServerInfo *server, char *buffer, int32_t size);
//TODO xml配置文件修改
uint32_t trans_ip_to_int(const char* ip);
void trans_int_to_ip(ObServerInfo *server, char *buffer, int32_t size);
const char* get_server_str(ObServerInfo *server);
const char* get_ip(ObServerInfo *server);
void dump_delete_ms_conn();

int set_mysql_options(MYSQL *mysql);

OB_SQL_CPP_END
#endif
