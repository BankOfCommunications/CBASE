/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_server_info.h is for what ...
 *
 * Version: ***: ob_sql_server_info.h  Wed Jan  9 19:26:47 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_SERVER_INFO_H_
#define OB_SQL_SERVER_INFO_H_

#include "ob_sql_define.h"

OB_SQL_CPP_START
#include <stdint.h>
#include <mysql/mysql.h>

typedef struct ob_sql_server
{
  uint32_t ip_;
  uint32_t port_;
  uint32_t version_;
  uint32_t percent_;
  uint32_t master_;
} ObServerInfo;

OB_SQL_CPP_END
#endif
