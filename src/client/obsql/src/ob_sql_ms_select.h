/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_ms_select.h is for what ...
 *
 * Version: ***: ob_sql_ms_select.h  Tue Jan  8 16:17:47 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_MS_SELECT_H_
#define OB_SQL_MS_SELECT_H_

#include "ob_sql_define.h"

OB_SQL_CPP_START

#include "ob_sql_global.h"
#include "ob_sql_struct.h"

#ifdef ENABLE_SELECT_MS_TABLE
//int update_ms_table();
//int sort_ms_table();
int update_ms_select_table();
void destroy_ms_select_table(ObSQLGlobalConfig *cconfig);

ObDataSource * find_ds_by_val(ObSQLHashItem *first, int64_t num, const uint32_t hashval);
#endif

OB_SQL_CPP_END
#endif
