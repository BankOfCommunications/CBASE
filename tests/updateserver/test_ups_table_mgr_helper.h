/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_ups_table_mgr_helper.h,v 0.1 2010/09/25 09:23:10 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_TEST_UPS_TABLE_MGR_HELPER_H__
#define __OCEANBASE_CHUNKSERVER_TEST_UPS_TABLE_MGR_HELPER_H__

#include "updateserver/ob_ups_table_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace updateserver
  {
    class TestUpsTableMgrHelper
    {
      public:
        TestUpsTableMgrHelper(ObUpsTableMgr& mgr)
          : mgr_(mgr)
        {
        }

        ~TestUpsTableMgrHelper()
        {
        }

      public:
        TableMgr& get_table_mgr()
        {
          return mgr_.table_mgr_;
        }
        int acquire_read_memtable(const int64_t read_version, const bool read_frozen,
            MemTable*& p_active_memtable, MemTable*& p_frozen_memtable)
        {
          UNUSED(read_version);
          UNUSED(read_frozen);
          UNUSED(p_active_memtable);
          UNUSED(p_frozen_memtable);
          /*
          ObVersionRange version_range;
          version_range.start_version_ = read_version;
          version_range.end_version_ = read_version;
          if (!read_frozen)
          {
            ++version_range.end_version_;
          }
          version_range.border_flag_.set_inclusive_start();
          version_range.border_flag_.set_inclusive_end();
          int64_t max_valid_version = 0;
          return mgr_.acquire_read_memtable_(version_range, max_valid_version, p_active_memtable,
              p_frozen_memtable);
              */
          return 0;
        }

        int release_read_memtable(MemTable* p_active_memtable, MemTable* p_frozen_memtable)
        {
          UNUSED(p_active_memtable);
          UNUSED(p_frozen_memtable);
          /*
          return mgr_.release_read_memtable_(p_active_memtable, p_frozen_memtable);
          */
          return 0;
        }

      private:
        ObUpsTableMgr& mgr_;
    };
  }
}

#endif //__TEST_UPS_TABLE_MGR_HELPER_H__

