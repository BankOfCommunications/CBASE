/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_migrate_info.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *   yongle.xh@alipay.com
 *
 */
#ifndef _OB_MIGRATE_INFO_H
#define _OB_MIGRATE_INFO_H 1
#include "common/ob_range2.h"
#include "common/page_arena.h"
#include "common/ob_data_source_desc.h"
#include "common/ob_server.h"

namespace oceanbase
{
  namespace rootserver
  {
    struct ObMigrateInfo
    {
      // types
      enum Status{
        STAT_FREE = 0,// not used
        STAT_USED= 1,// info is sent out
      };

      // members
      Status stat_;
      common::ObDataSourceDesc data_source_desc_;
      common::ObServer src_server_;
      common::ObServer dest_server_;
      int64_t start_time_;

      // methods
      ObMigrateInfo();
      ~ObMigrateInfo();
      void reuse();
      const char* get_status() const;
      common::CharArena allocator_;
    };

    class ObMigrateInfos
    {
      static const int64_t MAX_MIGRATE_INFO_NUM = 1024; //TODO: use list later
      public:
        ObMigrateInfos();
        ~ObMigrateInfos();
        void set_size(int64_t size);
        void reset();
        bool is_full() const;
        int64_t get_used_count() const;
        int64_t get_size() const { return size_; }
        int add_migrate_info(const common::ObServer& src_server, const common::ObServer& dest_server,
            const common::ObDataSourceDesc& data_source_desc);
        int free_migrate_info(const common::ObNewRange& range, const common::ObServer& src_server,
            const common::ObServer& dest_server);
        int get_migrate_info(const common::ObNewRange& range, const common::ObServer& src_server,
            const common::ObServer& dest_server, ObMigrateInfo*& info);
        bool check_migrate_info_timeout(int64_t timeout, common::ObServer& src_server,
            common::ObServer& dest_server, common::ObDataSourceDesc::ObDataSourceType& type);
        void print_migrate_info();
      private:
        ObMigrateInfo infos_[MAX_MIGRATE_INFO_NUM];
        int64_t size_;
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_MIGRATE_INFO_H */

