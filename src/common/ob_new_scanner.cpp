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
 *   yubai <yubai.lk@taobao.com>
 *     - some work details if you want
 *   yanran <yanran.hfs@taobao.com> 2010-10-27
 *     - new serialization format(ObObj composed, extented version)
 *   xiaochu.yh <xiaochu.yh@taobao.com> 2012-6-14
 *     - derived from ob_scanner.cpp and make it row interface only
 */

#include "ob_new_scanner.h"
#include "ob_new_scanner_helper.h"

#include <new>

#include "tblog.h"
#include "ob_malloc.h"
#include "ob_define.h"
#include "serialization.h"
#include "ob_action_flag.h"
#include "utility.h"
#include "ob_row.h"
#include "ob_schema.h"

using namespace oceanbase::common;

///////////////////////////////////////////////////////////////////////////////////////////////////

ObNewScanner::ObNewScanner() :
row_store_(ObModIds::OB_NEW_SCANNER), cur_size_counter_(0), mem_size_limit_(DEFAULT_MAX_SERIALIZE_SIZE),
mod_(ObModIds::OB_NEW_SCANNER), rowkey_allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_),
default_row_desc_(NULL)
{
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_row_num_ = 0;
  cur_row_num_ = 0;
}

ObNewScanner::~ObNewScanner()
{
  // memory auto released in row_store_;
  clear();
}

void ObNewScanner::reuse()
{
  row_store_.reuse();
  cur_size_counter_ = 0;

  range_.reset();
  rowkey_allocator_.reuse();
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_row_num_ = 0;
  last_row_key_.assign(NULL, 0);

  cur_row_num_ = 0;
  default_row_desc_ = NULL;
}

void ObNewScanner::clear()
{
  row_store_.clear();
  cur_size_counter_ = 0;

  range_.reset();
  rowkey_allocator_.free();
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_row_num_ = 0;
  last_row_key_.assign(NULL, 0);

  cur_row_num_ = 0;
  default_row_desc_ = NULL;
}

int64_t ObNewScanner::set_mem_size_limit(const int64_t limit)
{
  if (0 > limit
      || DEFAULT_MAX_SERIALIZE_SIZE < limit)
  {
    TBSYS_LOG(WARN, "invlaid limit_size=%ld cur_limit_size=%ld",
              limit, mem_size_limit_);
  }
  else if (0 == limit)
  {
    // 0 means using the default value
  }
  else
  {
    mem_size_limit_ = limit;
  }
  return mem_size_limit_;
}


int ObNewScanner::add_row(const ObRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = row_store_.add_row(row, cur_size_counter_)))
  {
    TBSYS_LOG(WARN, "fail to add_row to row store. ret=%d", ret);
  }
  if (cur_size_counter_ > mem_size_limit_)
  {
    //TBSYS_LOG(WARN, "scanner memory exceeds the limit."
    //     "cur_size_counter_=%ld, mem_size_limit_=%ld", cur_size_counter_, mem_size_limit_);
    // rollback last added row
    if (OB_SUCCESS != row_store_.rollback_last_row()) // ret code ignored
    {
      TBSYS_LOG(WARN, "fail to rollback last row");
    }
    ret = OB_SIZE_OVERFLOW;
  }
  else
  {
    cur_row_num_++;
  }
  return ret;
}


int ObNewScanner::serialize_meta_param(char * buf, const int64_t buf_len, int64_t & pos) const
{
  ObObj obj;
  obj.set_ext(ObActionFlag::META_PARAM_FIELD);
  int ret = obj.serialize(buf, buf_len, pos);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "serialize meta param error, ret=%d buf=%p data_len=%ld pos=%ld",
        ret, buf, buf_len, pos);
  }
  else
  {
    // row count
    obj.set_int(cur_row_num_);
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
          ret, buf, buf_len, pos);
    }
  }

  // last rowkey
  if (OB_SUCCESS == ret)
  {
      ret = set_rowkey_obj_array(buf, buf_len, pos,
          last_row_key_.get_obj_ptr(), last_row_key_.get_obj_cnt());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "last_row_key_ serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
            ret, buf, buf_len, pos);
      }
  }
  return ret;
}

// WARN: can not add cell after deserialize scanner if do that rollback will be failed for get last row key
int ObNewScanner::deserialize_meta_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int64_t rowkey_col_num = OB_MAX_ROWKEY_COLUMN_NUMBER;
  ObObj* rowkey = reinterpret_cast<ObObj*>(rowkey_allocator_.alloc(sizeof(ObObj) * rowkey_col_num));
  int ret = OB_SUCCESS;

  if (NULL == rowkey)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "fail to alloc memory for rowkey. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = deserialize_int_(buf, data_len, pos, cur_row_num_, last_obj)))
  {
    TBSYS_LOG(WARN, "deserialize cur_row_num_ error, ret=%d buf=%p data_len=%ld pos=%ld",
        ret, buf, data_len, pos);
  }
  // last row key
  else if (OB_SUCCESS != (ret = get_rowkey_obj_array(buf, data_len, pos, rowkey, rowkey_col_num)))
  {
    TBSYS_LOG(WARN, "deserialize last rowkey error, ret=%d buf=%p data_len=%ld pos=%ld",
        ret, buf, data_len, pos);
  }
  else
  {
    last_row_key_.assign(rowkey ,rowkey_col_num);
    if (OB_SUCCESS != (ret = last_row_key_.deep_copy(last_row_key_, rowkey_allocator_)))
    {
      TBSYS_LOG(WARN, "fail to set deserialize last row key. ret=%d last_row_key=%s", ret, to_cstring(last_row_key_));
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
    }
  }

  return ret;
}


//int ObNewScanner::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
DEFINE_SERIALIZE(ObNewScanner)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  int64_t next_pos = pos;

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_int(is_request_fullfilled_ ? 1 : 0);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_int(fullfilled_row_num_);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_int(data_version_);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (has_range_)
  {
    if (OB_SUCCESS == ret)
    {
      obj.set_ext(ObActionFlag::TABLET_RANGE_FIELD);
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
    // range - border flag
    if (OB_SUCCESS == ret)
    {
      obj.set_int(range_.border_flag_.get_data());
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
    // range - start key
    if (OB_SUCCESS == ret)
    {
      ret = range_.start_key_.serialize(buf, buf_len,  next_pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObRowkey serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
    // range - end key
    if (OB_SUCCESS == ret)
    {
      ret = range_.end_key_.serialize(buf, buf_len,  next_pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialize_meta_param(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to serialize meta param. ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_ext(ObActionFlag::TABLE_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
          ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = row_store_.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObRowStore serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
          ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    //TBSYS_LOG(DEBUG, "set  END_PARAM_FIELD to scanner");
    obj.set_ext(ObActionFlag::END_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    pos = next_pos;
  }
  return ret;
}


int64_t ObNewScanner::get_meta_param_serialize_size() const
{
  int64_t ret = 0;
  ObObj obj;
  obj.set_ext(ObActionFlag::META_PARAM_FIELD);
  ret += obj.get_serialize_size();
  obj.set_int(cur_row_num_);
  ret += obj.get_serialize_size();
  ret += get_rowkey_obj_array_size(last_row_key_.ptr(), last_row_key_.get_obj_cnt());
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObNewScanner)
//int64_t ObNewScanner::get_serialize_size(void) const
{
  int64_t ret = 0;
  ObObj obj;
  obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
  ret += obj.get_serialize_size();
  obj.set_int(is_request_fullfilled_ ? 1 : 0);
  ret += obj.get_serialize_size();
  obj.set_int(fullfilled_row_num_);
  ret += obj.get_serialize_size();
  obj.set_int(data_version_);
  ret += obj.get_serialize_size();
  if (has_range_)
  {
    obj.set_ext(ObActionFlag::TABLET_RANGE_FIELD);
    ret += obj.get_serialize_size();
    obj.set_int(range_.border_flag_.get_data());
    ret += obj.get_serialize_size();
    ret += range_.start_key_.get_serialize_size();
    ret += range_.end_key_.get_serialize_size();
  }
  ret += get_meta_param_serialize_size();
  obj.set_ext(ObActionFlag::TABLE_PARAM_FIELD);
  ret += obj.get_serialize_size();
  ret += row_store_.get_serialize_size();
  obj.set_ext(ObActionFlag::END_PARAM_FIELD);
  ret += obj.get_serialize_size();
  return ret;
}

DEFINE_DESERIALIZE(ObNewScanner)
//int ObNewScanner::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;

  if (NULL == buf || 0 >= data_len - pos)
  {
    TBSYS_LOG(WARN, "invalid param buf=%p data_len=%ld pos=%ld", buf, data_len, pos);
    ret = OB_ERROR;
  }
  else
  {
    ObObj param_id;
    ObObjType obj_type;
    int64_t param_id_type;
    bool is_end = false;
    int64_t new_pos = pos;

    clear();

    ret = param_id.deserialize(buf, data_len, new_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
    }

    while (OB_SUCCESS == ret && new_pos <= data_len && !is_end)
    {
      obj_type = param_id.get_type();
      // read until reaching a ObExtendType ObObj
      while (OB_SUCCESS == ret && ObExtendType != obj_type)
      {
        ret = param_id.deserialize(buf, data_len, new_pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
        }
        else
        {
          obj_type = param_id.get_type();
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = param_id.get_ext(param_id_type);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObObj type is not ObExtendType, invalid status, ret=%d", ret);
        }
        else
        {
          switch (param_id_type)
          {
          case ObActionFlag::BASIC_PARAM_FIELD:
            ret = deserialize_basic_(buf, data_len, new_pos, param_id);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "deserialize_basic_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                        ret, buf, data_len, new_pos);
            }
            break;
          case ObActionFlag::META_PARAM_FIELD:
             ret = deserialize_meta_param(buf, data_len, new_pos, param_id);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "deserialize_meta_data error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                        ret, buf, data_len, new_pos);
            }
            break;
          case ObActionFlag::TABLE_PARAM_FIELD:
            ret = deserialize_table_(buf, data_len, new_pos, param_id);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "deserialize_table_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                        ret, buf, data_len, new_pos);
            }
            break;
          case ObActionFlag::END_PARAM_FIELD:
            is_end = true;
            break;
          default:
            ret = param_id.deserialize(buf, data_len, new_pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
            }
            break;
          }
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      pos = new_pos;
    }
  }
  return ret;
}


int ObNewScanner::deserialize_basic_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  int64_t value = 0;

  // deserialize isfullfilled
  ret = deserialize_int_(buf, data_len, pos, value, last_obj);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    is_request_fullfilled_ = (value == 0 ? false : true);
  }
  // deserialize expect-to-be-filled row number
  if (OB_SUCCESS == ret)
  {
    ret = deserialize_int_(buf, data_len, pos, value, last_obj);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    }
    else
    {
      fullfilled_row_num_ = value;
    }
  }
  // deserialize data_version
  if (OB_SUCCESS == ret)
  {
    ret = deserialize_int_(buf, data_len, pos, value, last_obj);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    }
    else
    {
      data_version_ = value;
    }
  }
  // deserialize range
  if (OB_SUCCESS == ret)
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    }
    else
    {
      if (ObExtendType != last_obj.get_type())
      {
        // maybe new added field
      }
      else
      {
        int64_t type = last_obj.get_ext();
        ObNewRange range;
        ObObj start_rowkey_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];
        ObObj end_rowkey_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];
        range.start_key_.assign(start_rowkey_buf, OB_MAX_ROWKEY_COLUMN_NUMBER);
        range.end_key_.assign(end_rowkey_buf, OB_MAX_ROWKEY_COLUMN_NUMBER);
        if (ObActionFlag::TABLET_RANGE_FIELD == type)
        {
          int64_t border_flag = 0;
          if (OB_SUCCESS != (ret = deserialize_int_(buf, data_len, pos, border_flag, last_obj)))
          {
            TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, pos);
          }
          else if (OB_SUCCESS != (ret = range.start_key_.deserialize(buf, data_len, pos)))
          {
            TBSYS_LOG(WARN, "deserialize range start key error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
          }
          else if (OB_SUCCESS != (ret = range.end_key_.deserialize(buf, data_len, pos)))
          {
            TBSYS_LOG(WARN, "deserialize range end key error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
          }
          else
          {
            range.table_id_ = OB_INVALID_ID; // ignore
            range.border_flag_.set_data(static_cast<int8_t>(border_flag));
            if (OB_SUCCESS != (ret = deep_copy_range(rowkey_allocator_, range, range_)))
            {
              TBSYS_LOG(WARN, "fail to deep copy range %s", to_cstring(range_));
            }
            has_range_ = true;
          }
        }
        else
        {
          // maybe another param field
        }
      }
    }
  }

  // after reading all neccessary fields,
  // filter unknown ObObj
  if (OB_SUCCESS == ret && ObExtendType != last_obj.get_type())
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    while (OB_SUCCESS == ret && ObExtendType != last_obj.get_type())
    {
      ret = last_obj.deserialize(buf, data_len, pos);
    }
  }

  return ret;
}

int ObNewScanner::deserialize_table_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int ret = OB_SUCCESS;
  ret = row_store_.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "deserialize_table_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
        ret, buf, data_len, pos);
  }
  if (OB_SUCCESS == ret)
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
    }
  }
  return ret;
}

int ObNewScanner::deserialize_int_(const char* buf, const int64_t data_len, int64_t& pos,
    int64_t &value, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    if (ObIntType != last_obj.get_type())
    {
      TBSYS_LOG(WARN, "ObObj type is not Int, Type=%d", last_obj.get_type());
    }
    else
    {
      ret = last_obj.get_int(value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj get_int error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObNewScanner::deserialize_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                    ObString &value, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    if (ObVarcharType != last_obj.get_type())
    {
      TBSYS_LOG(WARN, "ObObj type is not Varchar, Type=%d", last_obj.get_type());
    }
    else
    {
      ret = last_obj.get_varchar(value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObNewScanner::deserialize_int_or_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                           int64_t& int_value, ObString &varchar_value, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    if (ObIntType == last_obj.get_type())
    {
      ret = last_obj.get_int(int_value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj get_int error, ret=%d", ret);
      }
    }
    else if (ObVarcharType == last_obj.get_type())
    {
      ret = last_obj.get_varchar(varchar_value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "ObObj type is not int or varchar, Type=%d", last_obj.get_type());
      ret = OB_ERROR;
    }
  }

  return ret;
}


int ObNewScanner::add_row(const ObRowkey &rowkey, const ObRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = row_store_.add_row(rowkey, row, cur_size_counter_)))
  {
    TBSYS_LOG(WARN, "fail to add_row to row store. ret=%d", ret);
  }
  if (cur_size_counter_ > mem_size_limit_)
  {
    //TBSYS_LOG(WARN, "scanner memory exceeds the limit."
    //     "cur_size_counter_=%ld, mem_size_limit_=%ld", cur_size_counter_, mem_size_limit_);
    // rollback last added row
    if (OB_SUCCESS != row_store_.rollback_last_row()) // ret code ignored
    {
      TBSYS_LOG(WARN, "fail to rollback last row");
    }
    ret = OB_SIZE_OVERFLOW;
  }
  else
  {
    cur_row_num_++;
  }
  return ret;
}

int ObNewScanner::get_next_row(const ObRowkey *&rowkey, ObRow &row)
{
  int ret = OB_SUCCESS;
  ret = row_store_.get_next_row(rowkey, row);
  return ret;
}

int ObNewScanner::get_next_row(ObRow &row)
{
  int ret = OB_SUCCESS;
  if (NULL == row.get_row_desc() && NULL != default_row_desc_)
  {
    row.set_row_desc(*default_row_desc_);
  }
  ret = row_store_.get_next_row(row);
  return ret;
}

bool ObNewScanner::is_empty() const
{
  return row_store_.is_empty();
}

int ObNewScanner::set_last_row_key(ObRowkey &row_key)
{
  return row_key.deep_copy(last_row_key_, rowkey_allocator_);
}

int ObNewScanner::get_last_row_key(ObRowkey &row_key) const
{
  int ret = OB_SUCCESS;

  if (last_row_key_.length() == 0)
  {
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    row_key = last_row_key_;
  }

  return ret;
}

int ObNewScanner::set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_row_num)
{
  int err = OB_SUCCESS;
  if (0 > fullfilled_row_num)
  {
    TBSYS_LOG(WARN,"param error [fullfilled_row_num:%ld]", fullfilled_row_num);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    fullfilled_row_num_ = fullfilled_row_num;
    is_request_fullfilled_ = is_fullfilled;
  }
  return err;
}

int ObNewScanner::get_is_req_fullfilled(bool &is_fullfilled, int64_t &fullfilled_row_num) const
{
  int err = OB_SUCCESS;
  is_fullfilled = is_request_fullfilled_;
  fullfilled_row_num = fullfilled_row_num_;
  return err;
}

int ObNewScanner::set_range(const ObNewRange &range)
{
  int ret = OB_SUCCESS;

  if (has_range_)
  {
    TBSYS_LOG(WARN, "range has been setted before");
    ret = OB_ERROR;
  }
  else
  {
    ret = deep_copy_range(rowkey_allocator_, range, range_);
    has_range_ = true;
  }

  return ret;
}

int ObNewScanner::set_range_shallow_copy(const ObNewRange &range)
{
  int ret = OB_SUCCESS;

  if (has_range_)
  {
    TBSYS_LOG(WARN, "range has been setted before");
    ret = OB_ERROR;
  }
  else
  {
    range_ = range;
    has_range_ = true;
  }

  return ret;
}

int ObNewScanner::get_range(ObNewRange &range) const
{
  int ret = OB_SUCCESS;

  if (!has_range_)
  {
    TBSYS_LOG(WARN, "range has not been setted");
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    range = range_;
  }

  return ret;
}

int64_t ObNewScanner::to_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;
  databuff_printf(buffer, length, pos, "ObNewScanner(range:");
  if (has_range_)
  {
    pos = range_.to_string(buffer, length);
  }
  else
  {
    databuff_printf(buffer, length, pos, "no range");
  }
  databuff_printf(buffer, length, pos, ",fullfill=%d,fullfill_row_num=%ld,cur_row_num=%ld,cur_size_count=%ld,data_version=%ld",
      is_request_fullfilled_, fullfilled_row_num_, cur_row_num_, cur_size_counter_, data_version_);
  return pos;
}

void ObNewScanner::dump(void) const
{
  TBSYS_LOG(INFO, "[dump] scanner range info:");
  if (has_range_)
  {
    range_.hex_dump();
  }
  else
  {
    TBSYS_LOG(INFO, "    no scanner range found!");
  }

  TBSYS_LOG(INFO, "[dump] is_request_fullfilled_=%d", is_request_fullfilled_);
  TBSYS_LOG(INFO, "[dump] fullfilled_row_num_=%ld", fullfilled_row_num_);
  TBSYS_LOG(INFO, "[dump] cur_row_num_=%ld", cur_row_num_);
  TBSYS_LOG(INFO, "[dump] cur_size_counter_=%ld", cur_size_counter_);
  TBSYS_LOG(INFO, "[dump] data_version_=%ld", data_version_);
}

void ObNewScanner::dump_all(int log_level) const
{
  int err = OB_SUCCESS;
  /// dump all result that will be send to ob client
  if (OB_SUCCESS == err && log_level >= TBSYS_LOG_LEVEL_DEBUG)
  {
    this->dump();
    // TODO: write new Scanner iterator
#if 0
    ObCellInfo *cell = NULL;
    bool row_change = false;
    ObNewScannerIterator iter = this->begin();
    while((iter != this->end()) &&
        (OB_SUCCESS == (err = iter.get_cell(&cell, &row_change))))
    {
      if (NULL == cell)
      {
        err = OB_ERROR;
        break;
      }
      if (row_change)
      {
        TBSYS_LOG(DEBUG, "dump cellinfo rowkey with hex format, length(%d)", cell->row_key_.length());
        hex_dump(cell->row_key_.ptr(), cell->row_key_.length(), true, TBSYS_LOG_LEVEL_DEBUG);
      }
      cell->value_.dump(TBSYS_LOG_LEVEL_DEBUG);
      ++iter;
    }
#endif

    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to get scanner cell as debug output. ");
    }
  }
}

int ObCellNewScanner::set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_row_num){
  if (!is_fullfilled)
  {
    is_size_overflow_ = true;
  }
  return ObNewScanner::set_is_req_fullfilled(is_fullfilled, fullfilled_row_num);
}

int ObCellNewScanner::add_cell(const ObCellInfo &cell_info,
    const bool is_compare_rowkey, const bool is_rowkey_change)
{
  int ret = OB_SUCCESS;
  bool row_changed = false;
  if (last_table_id_ == OB_INVALID_ID)
  {
    // first cell;
    row_changed = true;
  }
  else if (last_table_id_ != cell_info.table_id_)
  {
    TBSYS_LOG(WARN, "cannot add another table :%lu, current table:%ld",
        cell_info.table_id_, last_table_id_);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (!is_compare_rowkey && is_rowkey_change)
  {
    row_changed = true;
  }
  else if (cur_rowkey_ != cell_info.row_key_)
  {
    row_changed = true;
  }

  if (OB_SUCCESS == ret)
  {
    if (row_changed && last_table_id_ != OB_INVALID_ID)
    {
      // okay, got a row in compactor;
      ret = add_current_row();
      if(OB_SIZE_OVERFLOW == ret)
      {
        is_size_overflow_ = true;
      }
      else if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add current row fail:ret[%d]", ret);
      }
    }
  }

  if (OB_SUCCESS == ret )
  {
    if(!ups_row_valid_)
    {
      ups_row_.reuse();
      str_buf_.reset();
    }
    // add current cell to compactor;
    if (OB_SUCCESS != (ret = ObNewScannerHelper::add_ups_cell(ups_row_, cell_info, &str_buf_)))
    {
      TBSYS_LOG(WARN, "cannot add current cell to compactor, "
          "cur_size_counter_=%ld, last_table_id_=%ld, cur table=%ld, rowkey=%s",
          cur_size_counter_, last_table_id_, cell_info.table_id_, to_cstring(cell_info.row_key_));
    }
    else
    {
      ups_row_valid_ = true;
    }
  }

  if(OB_SUCCESS == ret)
  {
    if (row_changed)
    {
      last_table_id_ = cell_info.table_id_;
      cell_info.row_key_.deep_copy(cur_rowkey_, rowkey_allocator_);
    }
  }

  return ret;
}

int ObCellNewScanner::finish()
{
  int ret = OB_SUCCESS;
  if(ups_row_valid_ && !is_size_overflow_)
  {
    ret = add_current_row();
    if(OB_SIZE_OVERFLOW == ret)
    {
      is_size_overflow_ = true;
      ret = OB_SUCCESS;
    }
    else if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add current row fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = set_is_req_fullfilled(!is_size_overflow_, get_row_num())))
    {
      TBSYS_LOG(WARN, "set is req fullfilled fail:ret[%d]", ret);
    }
  }
  return ret;
}

void ObCellNewScanner::set_row_desc(const ObRowDesc &row_desc)
{
  row_desc_ = &row_desc;
  ups_row_.set_row_desc(*row_desc_);
}

int ObCellNewScanner::add_current_row()
{
  int ret = OB_SUCCESS;
  if(!ups_row_valid_)
  {
    TBSYS_LOG(WARN, "ups row is invalid");
    ret = OB_ERR_UNEXPECTED;
  }
  else if (NULL == row_desc_)
  {
    TBSYS_LOG(WARN, "set row desc first.");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (ups_row_.get_column_num() <= 0)
  {
    TBSYS_LOG(WARN, "current row has no cell");
  }

  if(OB_SUCCESS == ret)
  {
    ret = add_row(cur_rowkey_, ups_row_);
    if (OB_SUCCESS != ret && OB_SIZE_OVERFLOW != ret)
    {
      TBSYS_LOG(WARN, "add row to row_store error, ret=%d, last_row_key_=%s, cell num=%ld",
          ret, to_cstring(last_row_key_), ups_row_.get_column_num());
    }
  }

  if(OB_SUCCESS == ret)
  {
    last_row_key_ = cur_rowkey_;
    ups_row_valid_ = false;
  }
  return ret;
}

void ObCellNewScanner::clear()
{
  ObNewScanner::clear();
  row_desc_ = NULL;
  last_table_id_ = OB_INVALID_ID;
  ups_row_valid_ = false;
  is_size_overflow_ = false;
}

void ObCellNewScanner::reuse()
{
  ObNewScanner::reuse();
  str_buf_.reset();
  row_desc_ = NULL;
  last_table_id_ = OB_INVALID_ID;
  ups_row_valid_ = false;
  is_size_overflow_ = false;
}
