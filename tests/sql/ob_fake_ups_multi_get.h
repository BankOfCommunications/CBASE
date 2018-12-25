/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_multi_get.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_FAKE_UPS_MULTI_GET_H
#define _OB_FAKE_UPS_MULTI_GET_H 1

#include "sql/ob_ups_multi_get.h"
#include "common/ob_row_desc.h"
#include "common/ob_ups_row.h"
#include "ob_fake_ups_scan.h"
#include <map>

using namespace std;

namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
      class ObFakeUpsMultiGet : public ObUpsMultiGet
      {
        public:
          ObFakeUpsMultiGet(const char *file_name);

          int open();
          int get_next_row(const ObRow *&row);
          int get_next_row(const ObRowkey *&rowkey, const ObRow *&row);
          int close();
          virtual int get_row_desc(const common::ObRowDesc *&row_desc) const {row_desc=NULL;return OB_NOT_IMPLEMENT;}
          void set_row_desc(const ObRowDesc &row_desc)
          {
            curr_row_.set_row_desc(row_desc);
            uint64_t table_id = OB_INVALID_ID;
            uint64_t column_id = OB_INVALID_ID;

            for(int64_t i =0; i<row_desc.get_column_num(); i++)
            {
              row_desc.get_tid_cid(i, table_id, column_id);
              ups_scan_.range_.table_id_ = table_id;
              ups_scan_.add_column(column_id);
            }
          }

          void reset();

        private:
          int convert(const ObUpsRow &from, ObUpsRow &to);

        private:
          ObRowDesc curr_row_desc_;
          ObUpsRow curr_row_;
          int32_t curr_idx_;
          ObFakeUpsScan ups_scan_;
          ObRowStore row_store_;
          map<ObRowkey, const ObRowStore::StoredRow *> row_map_;
          CharArena rowkey_arena_;
      };
    }
  }
}

#endif /* _OB_FAKE_UPS_MULTI_GET_H */
