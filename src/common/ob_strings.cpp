/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_strings.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_strings.h"
#include "common/utility.h"
using namespace oceanbase::common;
ObStrings::ObStrings()
{
}

ObStrings::~ObStrings()
{
}

int ObStrings::add_string(const ObString &str, int64_t *idx/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObString stored_str;
  if (OB_SUCCESS != (ret = buf_.write_string(str, &stored_str)))
  {
    TBSYS_LOG(WARN, "failed to write string, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = strs_.push_back(stored_str)))
  {
    TBSYS_LOG(WARN, "failed to push into array, err=%d", ret);
  }
  else
  {
    if (NULL != idx)
    {
      *idx = strs_.count() - 1;
    }
  }
  return ret;
}

int ObStrings::get_string(int64_t idx, ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = strs_.at(idx, str)))
  {
    TBSYS_LOG(WARN, "failed to get string at=%ld, err=%d", idx, ret);
  }
  return ret;
}

int64_t ObStrings::count() const
{
  return strs_.count();
}

void ObStrings::clear()
{
  buf_.reset();
  strs_.clear();
}

DEFINE_SERIALIZE(ObStrings)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, strs_.count())))
  {
  }
  else
  {
    for (int64_t i = 0; i < strs_.count(); ++i)
    {
      if (OB_SUCCESS != (ret = strs_.at(i).serialize(buf, buf_len, pos)))
      {
        break;
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObStrings)
{
  int ret = OB_SUCCESS;
  this->clear();
  int64_t count = 0;
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &count)))
  {
  }
  else
  {
    ObString str;
    for (int64_t i = 0; i < count; ++i)
    {
      if (OB_SUCCESS != (ret = str.deserialize(buf, data_len, pos)))
      {
        break;
      }
      else
      {
        if (OB_SUCCESS != (ret = this->add_string(str)))
        {
          break;
        }
      }
    }
  }
  return ret;
}

int64_t ObStrings::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  for (int64_t i = 0; i < strs_.count(); ++i)
  {
    const ObString &str = strs_.at(i);
    if (0 != i)
    {
      databuff_printf(buf, buf_len, pos, ", ");
    }
    databuff_printf(buf, buf_len, pos, "%.*s", str.length(), str.ptr());
  }
  return pos;
}
