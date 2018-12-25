/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_query_service.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_SQL_QUERY_SERVICE_H
#define _OB_SQL_QUERY_SERVICE_H 1

#include "sql/ob_sql_scan_param.h"
#include "common/ob_new_scanner.h"

namespace oceanbase
{
  using namespace common;
  using namespace sql;

  namespace chunkserver
  {
    class ObSqlQueryService 
    {
      public:
        virtual ~ObSqlQueryService() {};

        virtual int open(const sql::ObSqlReadParam &sql_read_param) = 0;
        virtual int fill_scan_data(ObNewScanner &new_scanner) = 0;
        virtual int close() = 0;
        virtual void reset() = 0;
        virtual void set_timeout_us(int64_t timeout_us) = 0;
    };
  }
}

#endif /* _OB_SQL_QUERY_SERVICE_H */

