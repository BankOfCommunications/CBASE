/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_cache_join.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_TABLET_CACHE_JOIN_H
#define _OB_TABLET_CACHE_JOIN_H 1

#include "ob_tablet_join.h"
#include "ob_tablet_join_cache.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTabletCacheJoin : public ObTabletJoin
    {
      public:
        ObTabletCacheJoin();
        virtual ~ObTabletCacheJoin();

        virtual int open();
        virtual int close();
        virtual void reset();

      public:
        friend class test::ObTabletJoinTest_fetch_ups_row_Test;

        void set_table_id(uint64_t table_id);
        void set_cache(ObTabletJoinCache &ups_row_cache);

      private:
        int fetch_ups_row(const ObGetParam &get_param);
        int compose_next_get_param(ObGetParam &next_get_param, int64_t fused_row_idx);
        int fetch_next_ups_row(const ObGetParam &get_param, int64_t fused_row_idx);
        int gen_get_param(ObGetParam &get_param, const ObRow &fused_row);
        int revert_cache_handle();
        int fetch_fused_row_prepare();
        int get_ups_row(const ObRowkey &rowkey, ObUpsRow &ups_row, const ObGetParam &get_param);

      private:
        ObTabletJoinCache *ups_row_cache_;
        CacheHandle *cache_handle_;
        uint64_t table_id_;
        int64_t handle_count_;
    };
  }
}

#endif /* _OB_TABLET_CACHE_JOIN_H */

