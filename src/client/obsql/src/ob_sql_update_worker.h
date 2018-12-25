/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_update_worker.h is for what ...
 *
 * Version: ***: ob_sql_update_worker.h  Wed Jan  9 15:41:32 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_UPDATE_WORKER_H_
#define OB_SQL_UPDATE_WORKER_H_

#include "ob_sql_define.h"

OB_SQL_CPP_START

#include "ob_sql_struct.h"
#include "ob_sql_curl.h"
#include "ob_sql_cluster_config.h"

int start_update_worker();
void stop_update_worker();
void wakeup_update_worker();
void clear_update_flag();

OB_SQL_CPP_END
#endif
