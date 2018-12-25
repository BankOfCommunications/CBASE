/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_tablet_location.h,v 0.1 2010/09/26 14:01:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_COMMON_TABLET_LOCATION_CACHE_H_
#define OB_COMMON_TABLET_LOCATION_CACHE_H_


#include "ob_ms_cache_table.h"
#include "common/ob_string.h"
#include "common/ob_range2.h"
#include "common/ob_rowkey.h"
#include "ob_tablet_location_list.h"

namespace oceanbase
{
  namespace common
  {
    // cache
    class ObTabletLocationCache
    {
    public:
      /// construction
      ObTabletLocationCache();
      /// deconstruction
      virtual ~ObTabletLocationCache();

    public:
      /// cache item timeout when error occured
      static const int64_t CACHE_ERROR_TIMEOUT = 1000 * 1000 * 2L; // 2s

      // cache item alive timeout interval
      static const int64_t DEFAULT_ALIVE_TIMEOUT = 1000 * 1000 * 60 * 10L; // 10minutes

      /// init the virtual or user table tablet location cache item timeout
      int init(const uint64_t mem_size, const uint64_t count, const int64_t timeout, const int64_t special_timeout);

      /// set cache max memory size
      int set_mem_size(const int64_t mem_size);

      /// get cache item timeout for washout
      int64_t get_timeout(const uint64_t table_id) const;

      /// set user table location cache item timeout for washout
      void set_timeout(const int64_t timeout, const bool special_table = false);

      /// get table_id.rowkey location server list
      int get(const uint64_t table_id, const common::ObRowkey & rowkey,
          ObTabletLocationList & location);

      /// set the new range location, if range not valid, then del the old info
      int set(const common::ObNewRange & range, const ObTabletLocationList & location);

      /// update the exist range location, if not exist return error
      int update(const uint64_t table_id, const common::ObRowkey & rowkey,
          const ObTabletLocationList & location);

      /// delete the cache item according to table_id.rowkey
      int del(const uint64_t table_id, const common::ObRowkey & rowkey);

      /// cache item count
      uint64_t size(void);

      /// clear all items for sys admin
      int clear(void);

      /// dump debug info
      void dump(void);

    private:
      // init cache
      bool init_;
      // no special table cache alive timeout
      int64_t cache_timeout_;
      // special table cache timeout
      int64_t special_cache_timeout_;
      // tablet location cache
      common::ObVarCache<int, int, ObCBtreeTable> tablet_cache_;
    };

    inline int ObTabletLocationCache::set_mem_size(const int64_t mem_size)
    {
      return tablet_cache_.set_max_mem_size(mem_size);
    }

    inline int64_t ObTabletLocationCache::get_timeout(const uint64_t table_id) const
    {
      return IS_VIRTUAL_TABLE(table_id) ? special_cache_timeout_ : cache_timeout_;
    }

    inline void ObTabletLocationCache::set_timeout(const int64_t timeout, const bool special_table)
    {
      if (timeout <= 0)
      {
        TBSYS_LOG(ERROR, "set location cache timeout failed:timeout[%ld]", timeout);
      }
      else if (false == special_table)
      {
        cache_timeout_ = timeout;
      }
      else
      {
        special_cache_timeout_ = timeout;
      }
    }
  }
}


#endif // OB_COMMON_TABLET_LOCATION_CACHE_H_


