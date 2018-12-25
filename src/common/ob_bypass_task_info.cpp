/**
 * (C) 2007-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#include "ob_bypass_task_info.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    ObBypassTaskInfo::ObBypassTaskInfo()
    {
      operation_type_ = IMPORT_TABLE;
    }
    ObBypassTaskInfo::~ObBypassTaskInfo()
    {
    }
    void ObBypassTaskInfo::clear()
    {
      name_id_array_.clear();
      buff_.clear();
    }
    int ObBypassTaskInfo::push_back(std::pair<common::ObString, uint64_t> info)
    {
      int ret = OB_SUCCESS;
      ObString store_str;
      ret = buff_.write_string(info.first, &store_str);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to write string to buf. err=%d", ret);
      }
      else
      {
        std::pair<common::ObString, uint64_t> new_item = std::make_pair(store_str, info.second);
        ret = name_id_array_.push_back(new_item);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to push new item. ret=%d", ret);
        }
      }
      return ret;
    }
    uint64_t ObBypassTaskInfo::get_table_id(const char* table_name) const
    {
      uint64_t table_id = UINT64_MAX;
      if (NULL == table_name)
      {
        TBSYS_LOG(WARN, "invalid table_name. table_name=%p", table_name);
      }
      else
      {
        for (int64_t i = 0; i < count(); i++)
        {
          std::pair<common::ObString, uint64_t> pair = name_id_array_.at(i);
          if (strlen(table_name) == (uint64_t)pair.first.length()
              && 0 == strncmp(table_name, pair.first.ptr(), strlen(table_name)))
          {
            table_id = pair.second;
          }
        }
        if (UINT64_MAX == table_id)
        {
          TBSYS_LOG(WARN, "not find table id. table_name=%s", table_name);
        }
      }
      return table_id;
    }
    int64_t ObBypassTaskInfo::count()const
    {
      return name_id_array_.count();
    }
    std::pair<common::ObString, uint64_t>& ObBypassTaskInfo::at(const int64_t index)
    {
      return name_id_array_.at(index);
    }
    void ObBypassTaskInfo::set_operation_type(OperationType operation_type)
    {
      operation_type_ = operation_type;
    }
    const OperationType ObBypassTaskInfo::get_operation_type() const
    {
      return operation_type_;
    }
    DEFINE_SERIALIZE(ObBypassTaskInfo)
    {
      int ret = OB_SUCCESS;
      int64_t num = count();
      ret = serialization::encode_vi32(buf, buf_len, pos, operation_type_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to encode operation_type. err=%d", ret);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, pos, num);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to encode count, pos=%ld", pos);
        }
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < num; i++)
        {
          std::pair<common::ObString, uint64_t> name_id = name_id_array_.at(i);
          ObString table_name = name_id.first;
          ret = table_name.serialize(buf, buf_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to serialize table_name. pos=%ld, table_name=%s", pos, to_cstring(table_name));
            break;
          }
          ret = serialization::encode_vi64(buf, buf_len, pos, name_id.second);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to encode table_id. table_id=%lu, pos=%ld", name_id.second, pos);
            break;
          }
        }
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObBypassTaskInfo)
    {
      int ret = OB_SUCCESS;
      int64_t count = 0;
      if (NULL == buf)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "buf is null.");
      }
      if (OB_SUCCESS == ret)
      {
        clear();
        ret = serialization::decode_vi32(buf, data_len, pos, reinterpret_cast<int32_t*>(&operation_type_));
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to decode operation_type, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, pos, &count);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to decode count. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < count; i++)
        {
          char table_name_buf[OB_MAX_TABLE_NAME_LENGTH];
          ObString table_name(OB_MAX_TABLE_NAME_LENGTH, 0, table_name_buf);
          ret = table_name.deserialize(buf, data_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to deserilize table_name. ret=%d", ret);
            break;
          }
          int64_t table_id = 0;
          ret = serialization::decode_vi64(buf, data_len, pos, &table_id);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to decode table_id. ret=%ld", pos);
            break;
          }
          ret = push_back(std::make_pair(table_name, table_id));
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to push back item. table_name=%s, table_id=%lu", table_name.ptr(), table_id);
            break;
          }
        }
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObBypassTaskInfo)
    {
      int64_t size = 0;
      size += serialization::encoded_length_vi32(operation_type_);
      size += serialization::encoded_length_vi64(count());
      for (int64_t i = 0; i < count(); i++)
      {
        size += name_id_array_.at(i).first.get_serialize_size();
        size += serialization::encoded_length_vi64(name_id_array_.at(i).second);
      }
      return size;
    }
  }
}

