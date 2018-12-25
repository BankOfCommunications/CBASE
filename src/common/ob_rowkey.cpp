/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *         ob_rowkey.cpp is for what ...
 *
 *  Version: $Id: ob_rowkey.cpp 11/28/2011 04:27:31 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "ob_rowkey.h"
#include "ob_object.h"
#include "ob_malloc.h"
#include "ob_crc64.h"
#include "serialization.h"
#include "ob_schema.h"
#include "ob_obj_cast.h"
namespace oceanbase
{
  namespace common
  {

    ObObj ObRowkey::MIN_OBJECT(ObExtendType, 0, 0, ObObj::MIN_OBJECT_VALUE);
    ObObj ObRowkey::MAX_OBJECT(ObExtendType, 0, 0, ObObj::MAX_OBJECT_VALUE);
    ObRowkey ObRowkey::MIN_ROWKEY(&ObRowkey::MIN_OBJECT, 1);
    ObRowkey ObRowkey::MAX_ROWKEY(&ObRowkey::MAX_OBJECT, 1);

    int32_t ObRowkey::compare(const ObRowkey& rhs) const
    {
      return compare(rhs, NULL);
    }

    int32_t ObRowkey::compare(const ObRowkey& rhs, const ObRowkeyInfo * rowkey_info) const
    {
      int32_t cmp = 0;
      if (obj_cnt_ > 0 && rhs.obj_cnt_ > 0)
      {
        if (obj_ptr_[0].is_min_value()
            && rhs.obj_ptr_[0].is_min_value())
        {
          return cmp;           // <min,min,min> == <min>
        }
        else if (obj_ptr_[0].is_max_value()
            && rhs.obj_ptr_[0].is_max_value())
        {
          return cmp;           // <max,max,max> == <max>
        }
      }

      int64_t i = 0;
      for (; i < obj_cnt_ && 0 == cmp; ++i)
      {
        if (i < rhs.obj_cnt_)
        {
          cmp = obj_ptr_[i].compare(rhs.obj_ptr_[i]) *
            ((NULL == rowkey_info) ? 1 : rowkey_info->get_column(i)->order_);
        }
        else
        {
          cmp = 1;
          break;
        }
      }

      // rhs.cnt > this.cnt
      if (0 == cmp && i < rhs.obj_cnt_)
      {
        cmp = -1;
      }

      return cmp;
    }

    void ObRowkey::dump(const int32_t log_level) const
    {
      for (int64_t i = 0; i < obj_cnt_; ++i)
      {
        obj_ptr_[i].dump(log_level);
      }
    }

    int ObRowkey::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = serialization::encode_vi64(buf, buf_len, pos, obj_cnt_);
      if (OB_SUCCESS == ret)
      {
        ret = serialize_objs(buf, buf_len, pos);
      }
      return ret;
    }

    int ObRowkey::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
    {
      int64_t obj_cnt = 0;
      int ret = serialization::decode_vi64(buf, buf_len, pos, &obj_cnt);
      if (obj_cnt_ < obj_cnt)
      {
        ret = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "obj number greater than expected, expect_max_cnt=%ld real_cnt=%ld",
                  obj_cnt_, obj_cnt);
      }
      else
      {
        obj_cnt_ = obj_cnt;
        ret = deserialize_objs(buf, buf_len, pos);
      }
      return ret;
    }

    int64_t ObRowkey::get_serialize_size(void) const
    {
      int64_t size = serialization::encoded_length_vi64(obj_cnt_);
      size += get_serialize_objs_size();
      return size;
    }

    int ObRowkey::serialize_objs(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      for (int64_t i = 0; i < obj_cnt_ && OB_SUCCESS == ret; ++i)
      {
        ret = obj_ptr_[i].serialize(buf, buf_len, pos);
      }

      return ret;
    }

    int64_t ObRowkey::get_serialize_objs_size(void) const
    {
      int64_t total_size = 0;
      for (int64_t i = 0; i < obj_cnt_ ; ++i)
      {
        total_size += obj_ptr_[i].get_serialize_size();
      }
      return total_size;
    }

    int ObRowkey::deserialize_objs(const char* buf, const int64_t buf_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      if (NULL == obj_ptr_)
      {
        TBSYS_LOG(ERROR, "obj array is NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        for (int64_t i = 0; i < obj_cnt_ && OB_SUCCESS == ret; ++i)
        {
          ret = obj_ptr_[i].deserialize(buf, buf_len, pos);
        }
      }
      return ret;
    }

    int64_t ObRowkey::get_binary_key_length() const
    {
      int64_t len = 0;
      for (int64_t i = 0; i < obj_cnt_ ; ++i)
      {
        len += obj_ptr_[i].get_val_len();
      }
      return len;
    }

    int ObRowkey::deserialize_from_stream(const char* buf, const int64_t buf_len)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t i = 0;

      for (; i < obj_cnt_ && pos < buf_len && OB_SUCCESS == ret; ++i)
      {
        ret = obj_ptr_[i].deserialize(buf, buf_len, pos);
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "deserialize objs error, ret=%d", ret);
      }
      else if (pos != buf_len)
      {
        TBSYS_LOG(ERROR, "obj count not match, pos=%ld, buf_len=%ld, i=%ld, obj_count = %ld",
            pos, buf_len, i, obj_cnt_);
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        obj_cnt_ = i;
      }
      return ret;
    }

    int64_t ObRowkey::to_string(char* buffer, const int64_t length) const
    {
      int64_t pos = 0;

      if (is_min_row())
      {
        if (pos < length)
        {
          databuff_printf(buffer, length, pos, "MIN");
        }
      }
      else if (is_max_row())
      {
        if (pos < length)
        {
          databuff_printf(buffer, length, pos, "MAX");
        }
      }
      else
      {
        for (int i = 0; i < obj_cnt_; ++i)
        {
          if (pos < length)
          {
            databuff_printf(buffer, length, pos, "%s", to_cstring(obj_ptr_[i]));
            if (i < obj_cnt_ - 1)
            {
                databuff_printf(buffer, length, pos, ",");
            }
          }
          else
          {
            break;
          }
        }
      }

      return pos;
    }

    int64_t ObRowkey::checksum(const int64_t current) const
    {
      int64_t ret = current;
      if (0 < obj_cnt_
          && NULL != obj_ptr_)
      {
        for (int64_t i = 0; i < obj_cnt_; i++)
        {
          ret = obj_ptr_[i].checksum(ret);
        }
      }
      return ret;
    }

    void ObRowkey::checksum(ObBatchChecksum& bc) const
    {
      if (0 < obj_cnt_
          && NULL != obj_ptr_)
      {
        for (int64_t i = 0; i < obj_cnt_; i++)
        {
          obj_ptr_[i].checksum(bc);
        }
      }
    }

    uint32_t ObRowkey::murmurhash2(const uint32_t hash) const
    {
      uint32_t ret = hash;
      if (0 < obj_cnt_
          && NULL != obj_ptr_)
      {
        for (int64_t i = 0; i < obj_cnt_; i++)
        {
          ret = obj_ptr_[i].murmurhash2(ret);
        }
      }
      return ret;
    }

    uint64_t ObRowkey::murmurhash64A(const uint64_t hash) const
    {
      uint64_t ret = hash;
      if (0 < obj_cnt_ && NULL != obj_ptr_)
      {
        for (int64_t i = 0; i < obj_cnt_; i ++)
        {
          ret = obj_ptr_[i].murmurhash64A(ret);
        }
      }
      return ret;
    }

    //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
    int64_t ObRowkey::get_varchar_length() const
    {
      int64_t total_len = 0;
      for (int64_t i = 0; i < obj_cnt_ ; ++i)
      {
        if (obj_ptr_[i].get_type() == ObVarcharType||obj_ptr_[i].get_type() == ObDecimalType)
        {
          total_len += obj_ptr_[i].get_val_len();
        }
      }
      return total_len;
    }
    //add:e

    int64_t ObRowkey::get_deep_copy_size() const
    {
      int64_t obj_arr_len = obj_cnt_ * sizeof(ObObj);
      int64_t total_len = obj_arr_len;

      for (int64_t i = 0; i < obj_cnt_ ; ++i)
      {
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        if (obj_ptr_[i].get_type() == ObVarcharType||obj_ptr_[i].get_type() == ObDecimalType)
        //modify:e
        {
          total_len += obj_ptr_[i].get_val_len();
        }
      }
      return total_len;
    }

    RowkeyInfoHolder::RowkeyInfoHolder(const ObRowkeyInfo* ri)
      : rowkey_info_(ri)
    {
    }

    RowkeyInfoHolder::~RowkeyInfoHolder()
    {
    }

    RowkeyInfoHolder::RowkeyInfoHolder(const ObSchemaManagerV2* schema, const uint64_t table_id)
    {
      set_rowkey_info_by_schema(schema, table_id);
    }

    void RowkeyInfoHolder::set_rowkey_info_by_schema(
        const ObSchemaManagerV2* schema, const uint64_t table_id)
    {
      if (NULL == schema)
      {
        rowkey_info_ = NULL ;
      }
      else
      {
        rowkey_info_ = &(schema->get_table_schema(table_id)->get_rowkey_info());
      }
    }

    void RowkeyInfoHolder::set_rowkey_info(const ObRowkeyInfo* ri)
    {
      rowkey_info_ = ri;
    }

    bool ObRowkeyLess::operator()(const ObRowkey& lhs, const ObRowkey &rhs) const
    {
      return lhs.compare(rhs, rowkey_info_) < 0;
    }

    int ObRowkeyLess::compare(const ObRowkey& lhs, const ObRowkey &rhs) const
    {
      return lhs.compare(rhs, rowkey_info_);
    }


    int ob_cast_rowkey(const ObRowkeyInfo &rowkey_info, ObRowkey &rowkey,
                       char* buf, int64_t buf_size, int64_t &used_buf_len)
    {
      int ret = OB_SUCCESS;
      used_buf_len = 0;
      if (OB_UNLIKELY(rowkey_info.get_size() != rowkey.get_obj_cnt()))
      {
        TBSYS_LOG(ERROR, "wrong rowkey length, rowkey_info_len=%ld rowkey_len=%ld",
                  rowkey_info.get_size(), rowkey.get_obj_cnt());
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        int64_t used_len = 0;
        for (int64_t i = 0; i < rowkey.get_obj_cnt(); ++i)
        {
          ObObj &cell = const_cast<ObObj&>(rowkey.get_obj_ptr()[i]);
          ObObjType to_type = rowkey_info.get_column(i)->type_;
          if (OB_SUCCESS != (ret = obj_cast(cell, to_type, buf+used_buf_len, buf_size-used_buf_len, used_len)))
          {
            TBSYS_LOG(WARN, "failed to cast obj, i=%ld", i);
            break;
          }
          else
          {
            used_buf_len += used_len;
          }
        } // end for
      }
      return ret;
    }

    int ob_cast_rowkey_need_buf(const ObRowkeyInfo &rowkey_info, ObRowkey &rowkey, bool &need)
    {
      int ret = OB_SUCCESS;
      need = false;
      if (OB_UNLIKELY(rowkey_info.get_size() != rowkey.get_obj_cnt()))
      {
        TBSYS_LOG(ERROR, "wrong rowkey length, rowkey_info_len=%ld rowkey_len=%ld",
                  rowkey_info.get_size(), rowkey.get_obj_cnt());
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        for (int64_t i = 0; i < rowkey.get_obj_cnt(); ++i)
        {
          const ObObj &cell = rowkey.get_obj_ptr()[i];
          ObObjType to_type = rowkey_info.get_column(i)->type_;
          /* if (cell.get_type() != to_type
              && to_type == ObVarcharType)*/
              //old code

        //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
        if ((cell.get_type() != to_type
              && to_type == ObVarcharType)||(cell.get_type() != to_type
              && to_type == ObDecimalType))
        //add e
          {
            need = true;
            break;
          }
        }
      }
      return ret;
    }

    //add peiouya [IN_TYPEBUG_FIX] 20151225:b
    int ob_cast_rowkey(const ObArray<common::ObObjType>* expect_rowkey_type_desc, ObRowkey &rowkey_src,
                       ObObj *casted_objs, ObObj *rowkery_dst)
    {
      int ret = OB_SUCCESS;
      ObObj dst_type;
      dst_type.set_null ();
      if (expect_rowkey_type_desc->count () != rowkey_src.get_obj_cnt ())
      {
        TBSYS_LOG(ERROR, "wrong rowkey length, expect_rowkey_type_desc_len=%ld rowkey_len=%ld",
                  expect_rowkey_type_desc->count (), rowkey_src.get_obj_cnt ());
        ret = OB_ERR_COLUMN_SIZE;
      }
      for (int64_t idx = 0; OB_SUCCESS == ret && idx < rowkey_src.get_obj_cnt (); idx++)
      {
        const ObObj &cell = rowkey_src.get_obj_ptr()[idx];
        const ObObj *res_cell = NULL;
        dst_type.set_type (expect_rowkey_type_desc->at (idx));
        if (ObNullType == cell.get_type ())
        {
          (rowkery_dst+ idx)->set_null ();
        }
        else  if (OB_SUCCESS != (ret = obj_cast_for_rowkey(cell, dst_type, *(casted_objs + idx), res_cell)))
        {
          break;
        }
        else
        {
          *(rowkery_dst + idx) = *res_cell;
        }
      }
      return ret;
    }
    //add 20151225:e
    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
    bool is_arr_contain_null_value(ObObj* obj_ptr, int64_t obj_cnt)
    {
      OB_ASSERT(NULL != obj_ptr);
      bool is_contain_null_value = false;
      for(int64_t idx = 0; idx < obj_cnt; ++idx)
      {
        if(common::ObNullType == obj_ptr[idx].get_type())
        {
          is_contain_null_value = true;
          break;
        }
      }
      return is_contain_null_value;
    }
     //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
  }
}
