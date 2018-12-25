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

#ifndef __OB_UPDATESERVER_OB_ON_DISK_LOG_LOCATOR_H__
#define __OB_UPDATESERVER_OB_ON_DISK_LOG_LOCATOR_H__
#include "ob_log_locator.h"
#include "ob_recent_cache.h"

namespace oceanbase
{
  namespace updateserver
  {
    typedef ObRecentCache<int64_t, int64_t> ObFirstLogIdCache;
    int get_first_log_id_func(const char* log_dir, const int64_t file_id, int64_t& log_id, ObFirstLogIdCache* log_id_cache = NULL);
    // 通过扫描磁盘文件定位日志
    class ObOnDiskLogLocator : public IObLogLocator
    {
      const static int64_t DEFAULT_FIRST_LOG_ID_CACHE_CAPACITY = 1<<16;
      public:
        ObOnDiskLogLocator();
        virtual ~ObOnDiskLogLocator();
        int init(const char* log_dir);
        virtual int get_location(const int64_t log_id, ObLogLocation& location);
      protected:
        bool is_inited() const;
      private:
        bool enable_log_id_cache_;
        ObFirstLogIdCache first_log_id_cache_;
        const char* log_dir_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase
#endif /* __OB_UPDATESERVER_OB_ON_DISK_LOG_LOCATOR_H__ */

