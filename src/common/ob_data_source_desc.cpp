/*
 * (C) 2007-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Authors:
 *  yongle.xh@alipay.com
 *
 */

#include "ob_data_source_desc.h"
using namespace oceanbase;
using namespace common;

ObDataSourceDesc::ObDataSourceDesc()
{
  reset();
}

void ObDataSourceDesc::reset()
{
  type_ = UNKNOWN;
  src_server_.reset();
  sstable_version_ = 0;
  tablet_version_ = 0;
  keep_source_ = true;
  uri_.reset();
  range_.reset();
  range_.start_key_.assign(start_key_objs_, OB_MAX_ROWKEY_COLUMN_NUMBER);
  range_.end_key_.assign(end_key_objs_, OB_MAX_ROWKEY_COLUMN_NUMBER);
}


DEFINE_SERIALIZE(ObDataSourceDesc)
{
  int ret = OB_SUCCESS;
  int32_t type = static_cast<int32_t>(type_);

  if (NULL == buf || get_serialize_size() + pos > buf_len)
  {
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (OB_SUCCESS == ret
      && OB_SUCCESS == (ret = serialization::encode_i32(buf, buf_len, pos, type))
      && OB_SUCCESS == (ret = range_.serialize(buf, buf_len, pos))
      && OB_SUCCESS == (ret = src_server_.serialize(buf, buf_len, pos))
      && OB_SUCCESS == (ret = dst_server_.serialize(buf, buf_len, pos))
      && OB_SUCCESS == (ret = serialization::encode_i64(buf, buf_len, pos, sstable_version_))
      && OB_SUCCESS == (ret = serialization::encode_i64(buf, buf_len, pos, tablet_version_))
      && OB_SUCCESS == (ret = serialization::encode_bool(buf, buf_len, pos, keep_source_))
      && OB_SUCCESS == (ret = uri_.serialize(buf, buf_len, pos)))
  {
    // success
  }
  else
  {
    TBSYS_LOG(WARN, "failed to serialize data source desc, %s: ret=%d", to_cstring(*this), ret);
  }

  return ret;
}

DEFINE_DESERIALIZE(ObDataSourceDesc)
{
  int ret = OB_SUCCESS;
  int32_t type = -1;

  if (NULL == buf)
  {
    ret = OB_DESERIALIZE_ERROR;
  }

  if (OB_SUCCESS == ret
      && OB_SUCCESS == (ret = serialization::decode_i32(buf, data_len, pos, &type))
      && OB_SUCCESS == (ret = range_.deserialize(buf, data_len, pos))
      && OB_SUCCESS == (ret = src_server_.deserialize(buf, data_len, pos))
      && OB_SUCCESS == (ret = dst_server_.deserialize(buf, data_len, pos))
      && OB_SUCCESS == (ret = serialization::decode_i64(buf, data_len, pos, &sstable_version_))
      && OB_SUCCESS == (ret = serialization::decode_i64(buf, data_len, pos, &tablet_version_))
      && OB_SUCCESS == (ret = serialization::decode_bool(buf, data_len, pos, &keep_source_))
      && OB_SUCCESS == (ret = uri_.deserialize(buf, data_len, pos)))
  {
    // success
    type_ = static_cast<ObDataSourceType>(type);
  }
  else
  {
    TBSYS_LOG(WARN, "failed to deserialize data source desc, ret=%d", ret);
  }

  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(ObDataSourceDesc)
{
  int64_t size = 0;

  size += serialization::encoded_length_i32(static_cast<int32_t>(type_));
  size += range_.get_serialize_size();
  size += src_server_.get_serialize_size();
  size += dst_server_.get_serialize_size();
  size += serialization::encoded_length_i64(sstable_version_);
  size += serialization::encoded_length_i64(tablet_version_);
  size += serialization::encoded_length_bool(keep_source_);
  size += uri_.get_serialize_size();

  return size;
}

int64_t ObDataSourceDesc::to_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;
  databuff_printf(buffer, length, pos, "type=%s,", get_type());
  databuff_printf(buffer, length, pos, "range=");
  pos += range_.to_string(buffer+pos, length - pos);
  databuff_printf(buffer, length, pos, ",src_server=");
  pos += src_server_.to_string(buffer+pos, length - pos);
  databuff_printf(buffer, length, pos, ",dst_server=");
  pos += dst_server_.to_string(buffer+pos, length - pos);
  databuff_printf(buffer, length, pos,
      ",sstable_version=%ld,tablet_version=%ld,keep_source=%c,uri=",
      sstable_version_, tablet_version_, keep_source_?'Y':'N');
  pos += uri_.to_string(buffer+pos, length - pos);
  return pos;
}

const char*  ObDataSourceDesc::get_type() const
{
  const char* ret = "ERROR";
  switch(type_)
  {
    case OCEANBASE_INTERNAL:
      ret = "OCEANBASE_INTERNAL";
      break;
    case OCEANBASE_OUT:
      ret = "OCEANBASE_OUT";
      break;
    case DATA_SOURCE_PROXY:
      ret = "DATA_SOURCE_PROXY";
      break;
    case UNKNOWN:
      ret = "UNKNOWN";
      break;
    default:
      ret = "ERROR";
  }
  return ret;
}
