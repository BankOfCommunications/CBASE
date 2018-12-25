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
#include "ob_transaction.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    // Transaction Isolcation Levels: READ-UNCOMMITTED READ-COMMITTED REPEATABLE-READ SERIALIZABLE
    int ObTransReq::set_isolation_by_name(const ObString &isolation)
    {
      int ret = OB_SUCCESS;
      if (OB_LIKELY(isolation == ObString::make_string("READ-COMMITTED")))
      {
        isolation_ = READ_COMMITED;
      }
      else if (isolation == ObString::make_string("READ-UNCOMMITTED"))
      {
        isolation_ = NO_LOCK;
      }
      else if (isolation == ObString::make_string("REPEATABLE-READ"))
      {
        isolation_ = REPEATABLE_READ;
      }
      else if (isolation == ObString::make_string("SERIALIZABLE"))
      {
        isolation_ = SERIALIZABLE;
      }
      else
      {
        TBSYS_LOG(WARN, "Unknown isolation level `%.*s'", isolation.length(), isolation.ptr());
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }

    int64_t ObTransReq::to_string(char* buf, int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos,
                      "TransReq(type=%d,isolation=%x,start_time=%ld,timeout=%ld,idletime=%ld)",
                      type_, isolation_, start_time_, timeout_, idle_time_);
      return pos;
    }

    int ObTransReq::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, type_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, isolation_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, start_time_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, timeout_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, idle_time_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObTransReq::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, &type_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, &isolation_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &start_time_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &timeout_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &idle_time_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObTransReq::get_serialize_size(void) const
    {
      return serialization::encoded_length_i32(type_)
              + serialization::encoded_length_i32(isolation_)
              + serialization::encoded_length_i64(start_time_)
              + serialization::encoded_length_i64(timeout_)
              + serialization::encoded_length_i64(idle_time_);
    }

    void ObTransID::reset()
    {
      descriptor_ = INVALID_SESSION_ID;
      ups_.reset();
      start_time_us_ = 0;
      // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
      read_times_ = NO_BATCH_UD;
      // add by maosy 20161210
    }

    bool ObTransID::is_valid() const
    {
      return INVALID_SESSION_ID != descriptor_;
    }


    //add shili [LONG_TRANSACTION_LOG]  20160926:b
    bool ObTransID::equal(const ObTransID other) const
    {
      bool res = true;
      if (descriptor_ != other.descriptor_)
      {
        res = false;
      }
      else if (!ups_.is_same_ip(other.ups_))
      {
        res = false;
      }
      else if(start_time_us_ != other.start_time_us_)
      {
        res = false;
      }
//add by maosy [Delete_Update_Function_for_snpshot] 20161210
      else if(read_times_ != other.read_times_)
      {
          res =false;
      }
      // add by maosy 20161210

      return res;
    }


    /*@berif 只序列化descriptor_  start_time_us_  ups_，其他成员结果不序列化
    * @note  不要修改 该函数，为了保持对日志兼容性
    */
    int ObTransID::serialize_4_biglog(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, descriptor_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, start_time_us_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.serialize(buf, buf_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "ups.serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    /*@berif 只反序列化descriptor_  start_time_us_  ups_，其他成员反不序列化
    * @note  不要修改 该函数，为了保持对日志兼容性
    */
    int ObTransID::deserialize_4_biglog(const char *buf, const int64_t data_len, int64_t &pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&descriptor_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &start_time_us_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.deserialize(buf, data_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    /*@berif 只计算descriptor_  start_time_us_  ups_，其他成员不计入大小
    * @note  不要修改 该函数，为了保持对日志兼容性
    */
    int64_t ObTransID::get_serialize_size_4_biglog(void) const
    {
      return serialization::encoded_length_i32(descriptor_)
             + serialization::encoded_length_i64(start_time_us_)
             + ups_.get_serialize_size();
    }
    //add e

    int64_t ObTransID::to_string(char* buf, int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "TransID(sd=%u,ups=%s,start=%ld)",
                      descriptor_, ups_.to_cstring(), start_time_us_);
      return pos;
    }

    int ObTransID::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, descriptor_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, start_time_us_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.serialize(buf, buf_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "ups.serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
//add by maosy [Delete_Update_Function_for_snpshot] 20161210
      else if (OB_SUCCESS !=(err = serialization::encode_i64 (buf, buf_len, new_pos,read_times_)))
      {
          TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
//add by maosy 20161210
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObTransID::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&descriptor_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &start_time_us_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.deserialize(buf, data_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
      else if(OB_SUCCESS != (err = serialization::decode_i64 (buf, data_len, new_pos,&read_times_)))
      {
          TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
//add by maosy  20161210
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObTransID::get_serialize_size(void) const
    {
      return serialization::encoded_length_i32(descriptor_)
              + serialization::encoded_length_i64(start_time_us_)
              + ups_.get_serialize_size()
//add by maosy [Delete_Update_Function_for_snpshot] 20161210
                 +serialization::encoded_length_i64(read_times_);
// mod e
    }

    int64_t ObEndTransReq::to_string(char* buf, int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "EndTransReq(%s,rollback=%s)",
                      to_cstring(trans_id_), STR_BOOL(rollback_));
      return pos;
    }

    int ObEndTransReq::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = trans_id_.serialize(buf, buf_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_bool(buf, buf_len, new_pos, rollback_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObEndTransReq::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = trans_id_.deserialize(buf, data_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_bool(buf, data_len, new_pos, &rollback_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObEndTransReq::get_serialize_size(void) const
    {
      return trans_id_.get_serialize_size() + serialization::encoded_length_bool(rollback_);
    }
  }; // end namespace common
}; // end namespace oceanbase
