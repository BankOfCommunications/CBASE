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

#include "common/ob_define.h"
#include "common/utility.h"
#include "ob_fetched_log.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    int ObSessionBuffer:: reset()
    {
      int err = OB_SUCCESS;
      data_len_ = 0;
      return err;
    }

    int ObSessionBuffer:: serialize(char* buf, int64_t len, int64_t& pos) const
    {
      return mem_chunk_serialize(buf, len, pos, data_, data_len_);
    }

    int ObSessionBuffer:: deserialize(const char* buf, int64_t len, int64_t& pos)
    {
      return mem_chunk_deserialize(buf, len, pos, data_, sizeof(data_), data_len_);
    }

    int ObFetchLogReq::serialize(char* buf, int64_t len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || 0 > len || 0 > pos || pos > len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = session_.serialize(buf, len, tmp_pos)))
      {
        TBSYS_LOG(ERROR, "session_.serialize(buf=%p[%ld], pos=%ld)=>%d", buf, len, tmp_pos, err);
      }
      else if(OB_SUCCESS != (err = serialization::encode_i64(buf, len, tmp_pos, start_id_)))
      {
        TBSYS_LOG(ERROR, "encode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, start_id_, err);
      }
      else if(OB_SUCCESS != (err = serialization::encode_i64(buf, len, tmp_pos, max_data_len_)))
      {
        TBSYS_LOG(ERROR, "encode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, max_data_len_, err);
      }
      else
      {
        pos = tmp_pos;
      }
      return err;
    }

    int ObFetchLogReq::deserialize(const char* buf, int64_t len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || 0 > len || 0 > pos || pos > len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = session_.deserialize(buf, len, tmp_pos)))
      {
        TBSYS_LOG(ERROR, "session_.deserialize(buf=%p[%ld], pos=%ld)=>%d", buf, len, tmp_pos, err);
      }
      else if(OB_SUCCESS != (err = serialization::decode_i64(buf, len, tmp_pos, &start_id_)))
      {
        TBSYS_LOG(ERROR, "encode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, start_id_, err);
      }
      else if(OB_SUCCESS != (err = serialization::decode_i64(buf, len, tmp_pos, &max_data_len_)))
      {
        TBSYS_LOG(ERROR, "decode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, max_data_len_, err);
      }
      else
      {
        pos = tmp_pos;
      }
      return err;
    }

    int64_t ObFetchLogReq::to_string(char* buf, const int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "FetchLogReq(start_id=%ld, max_data_len=%ld)", start_id_, max_data_len_);
      return pos;
    }

    int ObFetchedLog::set_buf(char* buf, int64_t len)
    {
      int err = OB_SUCCESS;
      if (NULL == buf || 0 > len)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "set_buf(buf=%p[%ld]): INVALID ARGUMENT", buf, len);
      }
      else
      {
        log_data_ = buf;
        max_data_len_ = len;
      }
      return err;
    }

    int ObFetchedLog::serialize(char* buf, int64_t len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == log_data_)
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == buf || 0 > len || 0 > pos || pos > len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if(OB_SUCCESS != (err = next_req_.serialize(buf, len, tmp_pos)))
      {
        TBSYS_LOG(ERROR, "next_req.serialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, tmp_pos, err);
      }
      else if(OB_SUCCESS != (err = serialization::encode_i64(buf, len, tmp_pos, start_id_)))
      {
        TBSYS_LOG(ERROR, "encode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, start_id_, err);
      }
      else if(OB_SUCCESS != (err = serialization::encode_i64(buf, len, tmp_pos, end_id_)))
      {
        TBSYS_LOG(ERROR, "encode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, end_id_, err);
      }
      else if(OB_SUCCESS != (err = mem_chunk_serialize(buf, len, tmp_pos, log_data_, data_len_)))
      {
        TBSYS_LOG(ERROR, "mem_chunk_serialize(buf=%p[%ld], pos=%ld, data_len=%ld)=>%d", buf, len, tmp_pos, data_len_, err);
      }
      else
      {
        pos = tmp_pos;
      }
      return err;
    }

    int ObFetchedLog::deserialize(const char* buf, int64_t len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == log_data_)
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == buf || 0 > len || 0 > pos || pos > len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if(OB_SUCCESS != (err = next_req_.deserialize(buf, len, tmp_pos)))
      {
        TBSYS_LOG(ERROR, "next_req.deserialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, tmp_pos, err);
      }
      else if(OB_SUCCESS != (err = serialization::decode_i64(buf, len, tmp_pos, &start_id_)))
      {
        TBSYS_LOG(ERROR, "encode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, start_id_, err);
      }
      else if(OB_SUCCESS != (err = serialization::decode_i64(buf, len, tmp_pos, &end_id_)))
      {
        TBSYS_LOG(ERROR, "encode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, end_id_, err);
      }
      else if (OB_SUCCESS != (err = mem_chunk_deserialize(buf, len, tmp_pos, log_data_, max_data_len_, data_len_)))
      {
        TBSYS_LOG(ERROR, "mem_chunk_deserialize(buf=%p[%ld], pos=%ld, data_len=%ld, max_data_len=%ld)=>%d",
                  buf, len, tmp_pos, data_len_, max_data_len_, err);
      }
      else
      {
        pos = tmp_pos;
      }
      return err;
    }

    int64_t ObFetchedLog::to_string(char* buf, const int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "FetchedLog(id=[%ld,%ld], max_data_len=%ld, ret_data=%p[%ld], next_req=%s)",
                      start_id_, end_id_, max_data_len_, log_data_, data_len_, to_cstring(next_req_));
      return pos;
    }

  }; // end namespace updateserver
}; // end namespace oceanbase
