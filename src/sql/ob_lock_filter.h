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
#ifndef _OB_LOCK_FILTER_H
#define _OB_LOCK_FILTER_H 1
#include "ob_husk_filter.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    class ObLockFilter: public ObHuskFilter<PHY_LOCK_FILTER>
    {
      public:
        ObLockFilter(): lock_flag_(LF_NONE) {}
        virtual ~ObLockFilter() {}
      public:
        void set_write_lock_flag() { lock_flag_ = (ObLockFlag)(lock_flag_ | LF_WRITE); }
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
        int64_t get_serialize_size(void) const;
      protected:
        ObLockFlag lock_flag_;
    };
  }; // end namespace sql
}; // end namespace oceanbase

#endif /* _OB_LOCK_FILTER_H */
