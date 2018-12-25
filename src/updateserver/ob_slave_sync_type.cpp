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

#include "ob_slave_sync_type.h"
#include "common/serialization.h"
#include "common/ob_define.h"
#include "tblog.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    ObSlaveSyncType::ObSlaveSyncType() : slave_type_(REAL_TIME_SLAVE)
    {
    }

    ObSlaveSyncType::~ObSlaveSyncType()
    {
    }

    DEFINE_SERIALIZE(ObSlaveSyncType)
    {
      int ret = serialization::encode_vi32(buf, buf_len, pos, static_cast<int32_t>(slave_type_));
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to serialize slave_type. err=%d", ret);
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObSlaveSyncType)
    {
      int32_t type;
      int ret = serialization::decode_vi32(buf, data_len, pos, &type);
      if (OB_SUCCESS == ret)
      {
        slave_type_ = static_cast<ObSlaveType>(type);
      }
      else
      {
        TBSYS_LOG(WARN, "fail to deserialize slave_type, err=%d", ret);
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSlaveSyncType)
    {
      return serialization::encoded_length_vi32(static_cast<int32_t>(slave_type_));
    }

    ObConsistencyType::ObConsistencyType()
    : consistency_type_(WEAK_CONSISTENCY)
    {
    }

    ObConsistencyType::~ObConsistencyType()
    {
    }

    void ObConsistencyType::set_consistency_type(const Type type)
    {
       atomic_exchange(reinterpret_cast<uint32_t*>(&consistency_type_), type);
    }

    ObConsistencyType::Type ObConsistencyType::get_consistency_type() const
    {
      return consistency_type_;
    }

    const char* ObConsistencyType::get_type_str() const
    {
      switch (consistency_type_)
      {
        case STRONG_CONSISTENCY: return "STRONG_CONSISTENCY";
        case NORMAL_CONSISTENCY: return "NORMAL_CONSISTENCY";
        case WEAK_CONSISTENCY: return "WEAK_CONSISTENCY";
        default: return "UNKNOWN";
      }
    }
  }
}

