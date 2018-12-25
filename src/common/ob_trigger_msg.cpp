/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_trigger_msg.cpp,  12/14/2012 11:16:33 AM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *
 *
 */
#include "tbsys.h"
#include "ob_define.h"
#include "serialization.h"
#include "ob_trigger_msg.h"

using namespace oceanbase::common;

DEFINE_SERIALIZE(ObTriggerMsg)
{
  int ret = OB_SUCCESS;
  int64_t save_pos = pos;
  if (OB_SUCCESS != (ret = src.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to encode src:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, type)))
  {
    TBSYS_LOG(WARN, "fail to encode type:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, param)))
  {
    TBSYS_LOG(WARN, "fail to encode param:ret[%d]", ret);
  }
  if (OB_SUCCESS != ret)
  {
    pos = save_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTriggerMsg)
{
  int ret = OB_SUCCESS;
  int64_t save_pos = pos;
  if (OB_SUCCESS != (ret = src.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to decode src:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &type)))
  {
    TBSYS_LOG(WARN, "fail to decode type:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &param)))
  {
    TBSYS_LOG(WARN, "fail to decode param:ret[%d]", ret);
  }
  if (OB_SUCCESS != ret)
  {
    pos = save_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTriggerMsg)
{
  return 0;
}

//add zhaoqiong [Schema Manager] 20150327:b
DEFINE_SERIALIZE(ObDdlTriggerMsg)
{
  int ret = OB_SUCCESS;
  int64_t save_pos = pos;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, type)))
  {
    TBSYS_LOG(WARN, "fail to encode type:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, param)))
  {
    TBSYS_LOG(WARN, "fail to encode param:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, version)))
  {
    TBSYS_LOG(WARN, "fail to encode version:ret[%d]", ret);
  }
  if (OB_SUCCESS != ret)
  {
    pos = save_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObDdlTriggerMsg)
{
  int ret = OB_SUCCESS;
  int64_t save_pos = pos;
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &type)))
  {
    TBSYS_LOG(WARN, "fail to decode type:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &param)))
  {
    TBSYS_LOG(WARN, "fail to decode param:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &version)))
  {
    TBSYS_LOG(WARN, "fail to decode version:ret[%d]", ret);
  }
  if (OB_SUCCESS != ret)
  {
    pos = save_pos;
  }
  return ret;
}
 //add:e
