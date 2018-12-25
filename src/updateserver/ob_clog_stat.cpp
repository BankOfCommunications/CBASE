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
#include "common/serialization.h"
#include "ob_clog_stat.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    void ObBatchEventStat::clear_mvalue()
    {
      mvalue_ = 0;
      mstart_ = 0;
      mend_ = 0;
    }

    void ObBatchEventStat::add(const int64_t start_id, const int64_t end_id, const int64_t value)
    {
      batch_count_++;
      cur_end_ = end_id;
      if (value > mvalue_)
      {
        mvalue_ = value;
        mstart_ = start_id;
        mend_ = end_id;
      }
    }

    int ObBatchEventStat::serialize(char* buf, int64_t len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (NULL == buf || 0 >= len || pos <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != serialization::encode_i64(buf, len, new_pos, batch_count_)
               || OB_SUCCESS != serialization::encode_i64(buf, len, new_pos, cur_end_)
               || OB_SUCCESS != serialization::encode_i64(buf, len, new_pos, mvalue_)
               || OB_SUCCESS != serialization::encode_i64(buf, len, new_pos, mstart_)
               || OB_SUCCESS != serialization::encode_i64(buf, len, new_pos, mend_))
      {
        err = OB_SERIALIZE_ERROR;
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObBatchEventStat::deserialize(const char* buf, int64_t len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (NULL == buf || 0 >= len || pos <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != serialization::decode_i64(buf, len, new_pos, (int64_t*)&batch_count_)
               || OB_SUCCESS != serialization::decode_i64(buf, len, new_pos, (int64_t*)&cur_end_)
               || OB_SUCCESS != serialization::decode_i64(buf, len, new_pos, (int64_t*)&mvalue_)
               || OB_SUCCESS != serialization::decode_i64(buf, len, new_pos, (int64_t*)&mstart_)
               || OB_SUCCESS != serialization::decode_i64(buf, len, new_pos, (int64_t*)&mend_))
      {
        err = OB_DESERIALIZE_ERROR;
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    void ObClogStat::add_disk_us(int64_t start_id, int64_t end_id, int64_t time)
    {
      disk_stat_.add(start_id, end_id, time);
      batch_stat_.add(start_id, end_id, end_id-start_id);
      //if (time > disk_warn_us_)
      //{
      //  TBSYS_LOG(WARN, "flush_log: disk_delay=%ld, warn_us=%ld", time, disk_warn_us_);
      //}
      if (disk_warn_stat_.warn(time))
      {
        TBSYS_LOG(WARN, "flush_log: disk_delay=%ld %s", time, to_cstring(disk_warn_stat_));
      }
    }

    void ObClogStat::add_net_us(int64_t start_id, int64_t end_id, int64_t time)
    {
      net_stat_.add(start_id, end_id, time); 
      //if (time > net_warn_us_)
      //{
      //  TBSYS_LOG(WARN, "flush_log: net_delay=%ld, warn_us=%ld", time, net_warn_us_);
      //}
      if (net_warn_stat_.warn(time))
      {
        TBSYS_LOG(WARN, "flush_log: net_delay=%ld %s", time, to_cstring(net_warn_stat_));
      }
    }

    void ObClogStat::clear()
    {
      disk_stat_.clear_mvalue();
      net_stat_.clear_mvalue();
      batch_stat_.clear_mvalue();
    }

    int ObClogStat::serialize(char* buf, int64_t len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (NULL == buf || 0 >= len || pos <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != serialization::encode_i64(buf, len, new_pos, STRUCT_VERSION))
      {
        err = OB_SERIALIZE_ERROR;
      }   
      else if (OB_SUCCESS != disk_stat_.serialize(buf, len, new_pos)
               || OB_SUCCESS != net_stat_.serialize(buf, len, new_pos)
               || OB_SUCCESS != batch_stat_.serialize(buf, len, new_pos))
      {
        err = OB_SERIALIZE_ERROR;
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObClogStat::deserialize(const char* buf, int64_t len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      int64_t version = 0;
      if (NULL == buf || 0 >= len || pos <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != serialization::decode_i64(buf, len, new_pos, &version))
      {
        err = OB_DESERIALIZE_ERROR;
      }
      else if (version != STRUCT_VERSION)
      {
        err = OB_VERSION_NOT_MATCH;
        TBSYS_LOG(ERROR, "version[%ld] != VERSION[%ld]", version, STRUCT_VERSION);
      }
      else if (OB_SUCCESS != disk_stat_.deserialize(buf, len, new_pos)
               || OB_SUCCESS != net_stat_.deserialize(buf, len, new_pos)
               || OB_SUCCESS != batch_stat_.deserialize(buf, len, new_pos))
      {
        err = OB_DESERIALIZE_ERROR;
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObClogStat::to_string(char* buf, const int64_t len) const
    {
      int count = 0;
      count = snprintf(buf, len, "batch[log_count=%ld,batch_count=%ld,mvalue=%ld<%ld:%ld>], "
                       "disk[mvalue=%ld<%ld:%ld>], net[mvalue=%ld<%ld:%ld>]",
                       batch_stat_.cur_end_, batch_stat_.batch_count_,
                       batch_stat_.mvalue_, batch_stat_.mstart_, batch_stat_.mend_,
                       disk_stat_.mvalue_, disk_stat_.mstart_, disk_stat_.mend_,
                       net_stat_.mvalue_, net_stat_.mstart_, net_stat_.mend_);
      return count >= len? 0: count;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
