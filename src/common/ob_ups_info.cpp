/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rs_ms_message.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_ups_info.h"
#include "utility.h"
#include <tbsys.h>
using namespace oceanbase::common;

int ObUpsInfo::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, inner_port_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, (int32_t)stat_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i8(buf, buf_len, pos, ms_read_percentage_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i8(buf, buf_len, pos, cs_read_percentage_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i16(buf, buf_len, pos, reserve2_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, reserve3_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  return ret;
}

int ObUpsInfo::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_INVALID_ARGUMENT;
  int32_t stat;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, buf_len, pos, &inner_port_)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, buf_len, pos, &stat)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i8(buf, buf_len, pos, &ms_read_percentage_)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i8(buf, buf_len, pos, &cs_read_percentage_)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i16(buf, buf_len, pos, &reserve2_)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, buf_len, pos, &reserve3_)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
  }
  else if (UPS_SLAVE != stat && UPS_MASTER != stat)
  {
    TBSYS_LOG(ERROR, "invalid ups stat, stat=%d", stat);
  }
  else
  {
    stat_ = (ObUpsStat)stat;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObUpsList::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, ups_count_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else
  {
    for (int32_t i = 0; i < ups_count_; ++i)
    {
      if (OB_SUCCESS != (ret = ups_array_[i].serialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
        break;
      }
    }
  }
  return ret;
}

int ObUpsList::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, buf_len, pos, &ups_count_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    // reset
    sum_cs_percentage_ = sum_ms_percentage_ = 0;
    for (int32_t i = 0; i < ups_count_ && i < MAX_UPS_COUNT; ++i)
    {
      if (OB_SUCCESS != (ret = ups_array_[i].deserialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
        ret = OB_INVALID_ARGUMENT;
        break;
      }
      else
      {
        sum_ms_percentage_ += ups_array_[i].ms_read_percentage_;
        sum_cs_percentage_ += ups_array_[i].cs_read_percentage_;
      }
    }
  }
  return ret;
}


void ObUpsList::print() const
{
  for (int32_t i = 0; i < ups_count_; ++i)
  {
    TBSYS_LOG(INFO, "ups_list, idx=%d addr=%s inner_port=%d ms_read_percentage=%hhd cs_read_percentage=%hhd",
              i, to_cstring(ups_array_[i].addr_),
              ups_array_[i].inner_port_,
              ups_array_[i].ms_read_percentage_,
              ups_array_[i].cs_read_percentage_);
  }
}

void ObUpsList::print(char* buf, const int64_t buf_len, int64_t &pos) const
{
  for (int32_t i = 0; i < ups_count_; ++i)
  {
    databuff_printf(buf, buf_len, pos, "%s(%d %hhd %hhd) ",
                    to_cstring(ups_array_[i].addr_),
                    ups_array_[i].inner_port_,
                    ups_array_[i].ms_read_percentage_,
                    ups_array_[i].cs_read_percentage_);
  }
}
