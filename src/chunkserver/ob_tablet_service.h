/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_service.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_TABLET_SERVICE_H
#define _OB_TABLET_SERVICE_H 1

#include "ob_sql_query_service.h"
#include "sql/ob_tablet_scan.h"
#include "sql/ob_tablet_get.h"
#include "sql/ob_sstable_scan.h"
#include "sql/ob_ups_scan.h"
#include "sql/ob_ups_multi_get.h"
#include "common/ob_ups_rpc_proxy.h"
#include "ob_chunk_server.h"

namespace oceanbase
{
  using namespace common;
  using namespace sql;

  namespace chunkserver
  {
    class ObTabletService : public ObSqlQueryService
    {
      public:
        ObTabletService(ObChunkServer &chunkserver);
        virtual ~ObTabletService();

        int open(const sql::ObSqlReadParam &sql_read_param);
        int fill_scan_data(ObNewScanner &new_scanner);
        int close();
        void reset();
        void set_timeout_us(int64_t timeout_us);

      private:
        int64_t timeout_us_;
        const ObRowkey *cur_rowkey_;
        const ObRow *cur_row_;
        ObTabletRead *tablet_read_;
        ObRowkey last_rowkey_;
        ObTabletScan tablet_scan_;
        ObTabletGet tablet_get_;
        ObChunkServer &chunk_server_;
        CharArena rowkey_allocator_;
    };
  }
}

#endif /* _OB_TABLET_SERVICE_H */

