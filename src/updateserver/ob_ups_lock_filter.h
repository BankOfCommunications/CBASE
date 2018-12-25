/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_UPDATESERVER_OB_UPS_LOCK_FILTER_H__
#define __OB_UPDATESERVER_OB_UPS_LOCK_FILTER_H__
#include "ob_session_mgr.h"
#include "sql/ob_lock_filter.h"
#include "ob_memtable.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObUpsLockFilter: public sql::ObLockFilter
    {
      public:
        ObUpsLockFilter(RWSessionCtx& session): session_(session)
        {}
        ~ObUpsLockFilter()
        {}
      public:
        int open();
        int close();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
      protected:
        virtual int lock_row(const uint64_t table_id, const common::ObRowkey& rowkey);
        int64_t to_string(char* buf, const int64_t buf_len) const;
      private:
        RWSessionCtx& session_;
    };

  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_UPS_LOCK_FILTER_H__ */
