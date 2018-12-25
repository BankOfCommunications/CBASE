/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * The role of an oceanbase instance.
 *
 * Version: $Id$
 *
 * Authors:
 *   rongxuan<rongxuan.lc@taobao.com>
 *     - init
 */

#ifndef OB_SLAVE_SYNC_TYPE_H_
#define OB_SLAVE_SYNC_TYPE_H_
#include "tblog.h"
#include "common/ob_atomic.h"
#include "common/ob_define.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    class ObSlaveSyncType
    {
      public:
        enum ObSlaveType
        {
          REAL_TIME_SLAVE = 1,
          NON_REAL_TIME_SLAVE = 2,
        };
        ObSlaveSyncType();
        virtual ~ObSlaveSyncType();
        void set_sync_type(const ObSlaveType &type);
        ObSlaveType get_sync_type() const;
        bool operator== (const ObSlaveSyncType &slave_type) const;
        const char* get_type_str() const;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        ObSlaveType slave_type_;
    };

    class ObConsistencyType
    {
      public:
        enum Type
        {
          STRONG_CONSISTENCY = 1,
          NORMAL_CONSISTENCY = 2,
          WEAK_CONSISTENCY = 3,
        };
        ObConsistencyType();
        ~ObConsistencyType();
        void set_consistency_type(const Type type);
        Type get_consistency_type() const;
        const char* get_type_str() const;
      private:
        Type consistency_type_;
    };

    inline ObSlaveSyncType::ObSlaveType ObSlaveSyncType::get_sync_type() const
    {
      return slave_type_;
    }

    inline void ObSlaveSyncType::set_sync_type(const ObSlaveSyncType::ObSlaveType &type)
    {
      atomic_exchange(reinterpret_cast<uint32_t*>(&slave_type_), type);
    }

    inline bool ObSlaveSyncType::operator== (const ObSlaveSyncType &slave_type) const
    {
      return this->slave_type_ == slave_type.slave_type_;
    }

    inline const char* ObSlaveSyncType::get_type_str() const
    {
      switch(slave_type_)
      {
        case REAL_TIME_SLAVE : return "REAL_TIME_SLAVE";
        case NON_REAL_TIME_SLAVE : return "NON_REAL_TIME_SLAVE";
        default : return "UNKNOWN";
      }
    }
  }
}
#endif


