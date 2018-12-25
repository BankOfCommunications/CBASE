/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_curl.h is for what ...
 *
 * Version: ***: ob_sql_curl.h  Tue Nov 20 15:41:35 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_CURL_H_
#define OB_SQL_CURL_H_
#include "ob_sql_define.h"
#include "libobsql.h"

OB_SQL_CPP_START
#include "curl/curl.h"

//get rootserver ip port from diamond server
int get_rs_list(const char* url);
OB_SQL_CPP_END
#endif
