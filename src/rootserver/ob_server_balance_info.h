/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-08
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_ROOT_SERVER_BALANCE_INFO_H
#define OCEANBASE_ROOTSERVER_ROOT_SERVER_BALANCE_INFO_H
#include "common/ob_vector.h"
#include "common/page_arena.h"
#include "common/ob_range.h"
#include "common/ob_server.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObServerDiskInfo
    {
      public:
        ObServerDiskInfo();
        void set_capacity(const int64_t capacity);
        void set_used(const int64_t used);
        void add_used(const int64_t add_value);
        int64_t get_capacity() const;
        int64_t get_used() const;
        int32_t get_percent() const;
        double get_utilization_ratio() const;
        void dump() const;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        int64_t capacity_;
        int64_t used_;
    };
  }
}
#endif
