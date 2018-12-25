/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_get_param.cpp for define get param
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/utility.h"
#include "common/ob_action_flag.h"
#include "common/ob_malloc.h"
#include "ob_sql_get_param.h"
#include "common/ob_rowkey_helper.h"
#include "common/ob_schema.h"
#include "common/ob_string_buf.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace sql
  {
    ObSqlGetParam::ObSqlGetParam() :
      max_row_capacity_(MAX_ROW_CAPACITY),
      rowkey_list_(),
      buffer_pool_(ObModIds::OB_SQL_GET_PARAM, DEFAULT_ROW_BUF_SIZE)
    {
    }

    ObSqlGetParam::~ObSqlGetParam()
    {
    }

    int ObSqlGetParam::init()
    {
      int ret = OB_SUCCESS;
      reset();
      return ret;
    }

    void ObSqlGetParam::reset()
    {
      ObSqlReadParam::reset(); //call parent reset()
      reset_local();
    }

    void ObSqlGetParam::reset_local()
    {
      rowkey_list_.clear();
      // if memory inflates too large, free.
      if (buffer_pool_.total() > DEFAULT_ROW_BUF_SIZE)
      {
        buffer_pool_.clear();
      }
      else
      {
        buffer_pool_.reuse();
      }
    }

    int ObSqlGetParam::destroy()
    {
      rowkey_list_.clear();
      ObSqlReadParam::reset();
      buffer_pool_.clear();
      return OB_SUCCESS;
    }

    int64_t ObSqlGetParam::to_string(char *buf, int64_t buf_size) const
    {
      int64_t pos = 0;
      if (NULL != buf && buf_size > 0)
      {
        pos += ObSqlReadParam::to_string(buf, buf_size);
        databuff_printf(buf, buf_size, pos, "[Get RowNum:%ld]:\n", rowkey_list_.count());
        databuff_print_obj(buf, buf_size, pos, rowkey_list_);
      }
      return pos;
    }

    void ObSqlGetParam::print_memory_usage(const char* msg) const
    {
      TBSYS_LOG(INFO, "ObSqlGetParam use memory:%s, allocator:used:%ld,total:%ld,",
          msg,  buffer_pool_.used(), buffer_pool_.total());
    }

    int ObSqlGetParam::add_rowkey(const ObRowkey& rowkey, bool deep_copy_args)
    {
      int ret = OB_SUCCESS;
      ObRowkey stored_rowkey = rowkey;
      if (deep_copy_args)
      {
        ret = copy_rowkey(rowkey, stored_rowkey);
      }
      if (OB_SUCCESS == ret)
      {
        //cell info must include legal row key
        if (stored_rowkey.length() <= 0 || NULL == stored_rowkey.ptr())
        {
          TBSYS_LOG(WARN, "invalid row key, key_len=%ld, key_ptr=%p",
            stored_rowkey.length(), stored_rowkey.ptr());
          ret = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCCESS == ret)
      {
        //ensure there are enough space to store this cell
        if (rowkey_list_.count() < max_row_capacity_)
        {
            //store rowkey
            if (OB_SUCCESS != (ret = rowkey_list_.push_back(stored_rowkey)))
            {
              TBSYS_LOG(WARN, "fail to push rowkey to list. ret=%d", ret);
            }
        }
        else
        {
          TBSYS_LOG(INFO, "get param is full, can't add rowkey anymore, row_size=%ld, max=%d",
            rowkey_list_.count(), max_row_capacity_);
          ret = OB_SIZE_OVERFLOW;
        }
      }
      return ret;
    }

    int ObSqlGetParam::copy_rowkey(const ObRowkey &rowkey, ObRowkey &stored_rowkey)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = rowkey.deep_copy(stored_rowkey, buffer_pool_)))
      {
        TBSYS_LOG(WARN,"fail to copy rowkey to local buffer [err:%d].[rowkey=%s]", err, to_cstring(rowkey));
      }
      return err;
    }

    /////////////////////////////////////////////////////////////////////////////
    //////////////////// SERIALIZE & DESERIALIZE ////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////

    int ObSqlGetParam::serialize_flag(char* buf, const int64_t buf_len, int64_t& pos,
        const int64_t flag) const
    {
      int ret = OB_SUCCESS;
      ObObj obj;

      obj.set_ext(flag);
      ret = obj.serialize(buf, buf_len, pos);

      return ret;
    }

    int ObSqlGetParam::serialize_int(char* buf, const int64_t buf_len, int64_t& pos,
        const int64_t value) const
    {
      int ret = OB_SUCCESS;
      ObObj obj;

      obj.set_int(value);
      ret = obj.serialize(buf, buf_len, pos);

      return ret;
    }

    int ObSqlGetParam::deserialize_int(const char* buf, const int64_t data_len, int64_t& pos,
        int64_t& value) const
    {
      int ret         = OB_SUCCESS;
      ObObjType type  = ObNullType;
      ObObj obj;

      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        type = obj.get_type();
        if (ObIntType == type)
        {
          obj.get_int(value);
        }
        else
        {
          TBSYS_LOG(WARN, "expected deserialize int type, but get type=%d", type);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int64_t ObSqlGetParam::get_obj_serialize_size(const int64_t value, bool is_ext) const
    {
      ObObj obj;

      if (is_ext)
      {
        obj.set_ext(value);
      }
      else
      {
        obj.set_int(value);
      }

      return obj.get_serialize_size();
    }

    int ObSqlGetParam::serialize_basic_field(char* buf,
      const int64_t buf_len,
      int64_t& pos) const
    {
      int ret                       = OB_SUCCESS;
      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld",
          buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      //serialize basic field flag
      if (OB_SUCCESS == ret)
      {
        ret = serialize_flag(buf, buf_len, pos, ObActionFlag::BASIC_PARAM_FIELD);
      }
      /// READ_PARAM
      if (OB_SUCCESS == ret)
      {
        ret = ObSqlReadParam::serialize(buf, buf_len, pos);
      }
      //add zhujun [transaction read uncommit]2016/3/25
      if(OB_SUCCESS == ret)
      {
          //TBSYS_LOG(INFO, "trans_id before serialize %s",to_cstring(trans_id_));
          ret = trans_id_.serialize(buf,buf_len,pos);
      }
      //add:e
      return ret;
    }

    int ObSqlGetParam::deserialize_basic_field(const char* buf,
      const int64_t data_len, int64_t& pos)
    {
      int ret                 = OB_SUCCESS;
      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
          buf, data_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
          ret = ObSqlReadParam::deserialize(buf, data_len, pos);

          //add zhujun [transaction read uncommit]2016/3/25
          if(ret == OB_SUCCESS) {
              ret = trans_id_.deserialize(buf,data_len,pos);
              //add huangcc [fix transaction read uncommit bug]2016/11/21
              if (trans_id_.is_valid())
              {
                 is_read_master_ = true;
              }
              //add end
              //TBSYS_LOG(INFO, "trans_id_.deserialize %s",to_cstring(trans_id_));
          }
          //add:e
      }
      return ret;
    }

    int64_t ObSqlGetParam::get_basic_field_serialize_size(void) const
    {
      int64_t total_size = 0;
      total_size += get_obj_serialize_size(ObActionFlag::BASIC_PARAM_FIELD, true);
      total_size += ObSqlReadParam::get_serialize_size();
      //add zhujun [transaction read uncommit]2016/3/25
      total_size += trans_id_.get_serialize_size();
      //add:e
      return total_size;
    }

    int ObSqlGetParam::serialize_rowkeys(char* buf, const int64_t buf_len,
      int64_t& pos, const RowkeyListType & rowkey_list) const
    {
      int ret = OB_SUCCESS;
      int64_t i = 0;
      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld",
          buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        //serialize row keys
        if (OB_SUCCESS != (ret = serialize_flag(buf, buf_len, pos, ObActionFlag::FORMED_ROW_KEY_FIELD)))
        {
          TBSYS_LOG(WARN, "fail to serialize flag. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = serialize_int(buf, buf_len, pos, rowkey_list.count())))
        {
          TBSYS_LOG(WARN, "fail to serialize rowkey size. ret=%d", ret);
        }
        else
        {
          if (OB_SUCCESS == ret)
          {
            for (i = 0; OB_SUCCESS == ret && i < rowkey_list.count(); i++)
            {
              ret = rowkey_list.at(i).serialize(buf, buf_len, pos);
              // TBSYS_LOG(DEBUG, "serialize rowkey[%ld]: %s. ret=%d", i, to_cstring(rowkey_list.at(i)), ret);
            }
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to serialize rowkey list. buf=%p, buf_len=%ld, pos=%ld, rowkey list size=%ld",
            buf, buf_len, pos, rowkey_list.count());
      }
      return ret;
    }

    int ObSqlGetParam::deserialize_rowkeys(const char* buf, const int64_t data_len,
      int64_t& pos)
    {
      int ret         = OB_SUCCESS;
      int64_t size    = 0;
      int64_t i       = 0;
      ObRowkey rowkey;
      ObObj tmp_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (ret = deserialize_int(buf, data_len, pos, size)))
        {
          TBSYS_LOG(WARN, "fail to deserialize_int(rowkey size). buf=%p, data_len=%ld, pos=%ld, ret=%d",
              buf, data_len, pos, ret);
        }
        else
        {
          rowkey.assign(tmp_objs, OB_MAX_ROWKEY_COLUMN_NUMBER);
          for (i = 0; i < size; i++)
          {
            //deserialize rowkey
            if (OB_SUCCESS != (ret = rowkey.deserialize(buf, data_len, pos)))
            {
              TBSYS_LOG(WARN, "fail to deserialize rowkey. buf=%p, data_len=%ld, pos=%ld, i=%ld, size=%ld",
                  buf, data_len, pos, i, size);
            }
            else if (OB_SUCCESS != (ret = add_rowkey(rowkey, true)))
            {
              TBSYS_LOG(WARN, "fail to add deserialized rowkey to get param. ret=%d, i=%ld, size=%ld", ret, i, size);
            }
          }
        }
      }
      return ret;
    }

    int64_t ObSqlGetParam::get_serialize_rowkeys_size(const RowkeyListType & rowkey_list) const
    {
      int64_t total_size = 0;
      int64_t i = 0;
      total_size += get_obj_serialize_size(rowkey_list.count());
      total_size += get_obj_serialize_size(ObActionFlag::FORMED_ROW_KEY_FIELD, true);
      for (i = 0; i < rowkey_list.count(); i++)
      {
        total_size += rowkey_list.at(i).get_serialize_size();
      }
      return total_size;
    }

    DEFINE_SERIALIZE(ObSqlGetParam)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld",
          buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialize_basic_field(buf, buf_len, pos);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialize_rowkeys(buf, buf_len, pos, rowkey_list_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialize_flag(buf, buf_len, pos, ObActionFlag::END_PARAM_FIELD);
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObSqlGetParam)
    {
      int ret                 = OB_SUCCESS;
      bool end_flag           = false;
      int64_t ext_val         = -1;
      ObObjType type          = ObNullType;
      ObObj obj;

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
          buf, data_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      //better reset
      if (OB_SUCCESS == ret)
      {
        reset();
      }

      //deserialize obj one by one
      while (OB_SUCCESS == ret && pos < data_len && !end_flag)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          //check type
          type = obj.get_type();
          if (ObExtendType == type)
          {
            //check extend type
            ext_val = obj.get_ext();
            switch (ext_val)
            {
            case ObActionFlag::BASIC_PARAM_FIELD:
              ret = deserialize_basic_field(buf, data_len, pos);
              break;
            case ObActionFlag::FORMED_ROW_KEY_FIELD:
              ret = deserialize_rowkeys(buf, data_len, pos);
              break;
            case ObActionFlag::END_PARAM_FIELD:
              end_flag = true;
              break;
            default:
              TBSYS_LOG(WARN, "unkonwn extend field, extend=%ld", ext_val);
              break;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "wrong object type=%d", type);
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSqlGetParam)
    {
      int64_t total_size = 0;
      total_size += get_basic_field_serialize_size();
      total_size += get_serialize_rowkeys_size(rowkey_list_);
      total_size += get_obj_serialize_size(ObActionFlag::END_PARAM_FIELD, true);

      return total_size;
    }

    int ObSqlGetParam::assign(const ObSqlReadParam* other)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObSqlGetParam);
      reset();
      if ((ret = ObSqlReadParam::assign(other)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign ObSqlReadParam failed, ret=%d", ret);
      }
      else
      {
        max_row_capacity_ = o_ptr->max_row_capacity_;
        for (int64_t i = 0; ret == OB_SUCCESS && i < o_ptr->rowkey_list_.count(); i++)
        {
          if ((ret = add_rowkey(o_ptr->rowkey_list_.at(i), true)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Add ObRowkey failed, ret=%d", ret);
            break;
          }
        }
      }
      return ret;
    }
  } /* common */
} /* oceanbase */
