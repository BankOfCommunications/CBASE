/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */

#include "ob_obi_role.h"
#include "common/serialization.h"
using namespace oceanbase::common;

ObiRole::ObiRole()
  :role_(INIT)
{
}

ObiRole::~ObiRole()
{
}

DEFINE_SERIALIZE(ObiRole)
{
  int ret = serialization::encode_vi32(buf, buf_len, pos, static_cast<int32_t>(role_));
  return ret;
}

DEFINE_DESERIALIZE(ObiRole)
{
  int32_t val = -1;
  int ret = serialization::decode_vi32(buf, data_len, pos, &val);
  if (OB_SUCCESS == ret)
  {
    atomic_exchange(reinterpret_cast<uint32_t*>(&role_), val);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObiRole)
{
  return serialization::encoded_length_vi32(static_cast<int32_t>(role_));
}
