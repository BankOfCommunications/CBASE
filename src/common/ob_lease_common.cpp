/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_lease_common.cpp,v 0.1 2010/12/01 09:30:33 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_lease_common.h"
#include "tbsys.h"

namespace oceanbase
{
  namespace common
  {
    bool ObLease::is_lease_valid(int64_t redun_time)
    {
      bool ret = true;

      timeval time_val;
      gettimeofday(&time_val, NULL);
      int64_t cur_time_us = time_val.tv_sec * 1000 * 1000 + time_val.tv_usec;
      if (lease_time + lease_interval + redun_time < cur_time_us)
      {
        TBSYS_LOG(INFO, "Lease expired, lease_time=%ld, lease_interval=%ld, cur_time_us=%ld",
            lease_time, lease_interval, cur_time_us);
        ret = false;
      }

      return ret;
    }

    DEFINE_SERIALIZE(ObLease)
    {
      int err = OB_SUCCESS;
      if (NULL == buf || buf_len <= 0 || pos < 0)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else if (pos + (int64_t) sizeof(*this) > buf_len)
      {
        TBSYS_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld", pos, buf_len);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        *((ObLease*) (buf + pos)) = *this;
        pos += sizeof(*this);
      }

      return err;
    }

    DEFINE_DESERIALIZE(ObLease)
    {
      int err = OB_SUCCESS;
      if (NULL == buf || data_len <= 0 || pos < 0)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld", buf, data_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else if (pos + (int64_t) sizeof(*this) > data_len)
      {
        TBSYS_LOG(WARN, "data_len is not enough, pos=%ld, data_len=%ld", pos, data_len);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        *this = *((ObLease*) (buf + pos));
        pos += sizeof(*this);
      }

      return err;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObLease)
    {
      return sizeof(*this);
    }
  }
}
