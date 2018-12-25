/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_row.cpp for persistent ssatable single row. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_sstable_row.h"
#include "common/ob_rowkey.h"

namespace oceanbase 
{ 
  namespace sstable 
  {
    using namespace common;
    using namespace common::serialization;

    ObSSTableRow::ObSSTableRow()
    : table_id_(OB_INVALID_ID), 
      column_group_id_(OB_INVALID_ID), 
      rowkey_obj_count_(0), 
      row_key_buf_(NULL),
      row_key_buf_size_(DEFAULT_KEY_BUF_SIZE),
      /*add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b*/
      row_key_string_buf_(NULL),
      row_key_string_buf_length_(0),
      row_key_string_buf_size_(DEFAULT_KEY_BUF_SIZE),
      row_key_string_buf_used_(0),/*add:e*/
      obj_count_(0),
      string_buf_(ObModIds::OB_SSTABLE_WRITER),
      binary_rowkey_info_(NULL)
    {
    }

    ObSSTableRow::ObSSTableRow(const ObRowkeyInfo* rowkey_info)
    : table_id_(OB_INVALID_ID), 
      column_group_id_(OB_INVALID_ID), 
      rowkey_obj_count_(0), 
      row_key_buf_(NULL),
      row_key_buf_size_(DEFAULT_KEY_BUF_SIZE),
      /*add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b*/
      row_key_string_buf_(NULL),
      row_key_string_buf_length_(0),
      row_key_string_buf_size_(DEFAULT_KEY_BUF_SIZE),
      row_key_string_buf_used_(0),/*add:e*/
      obj_count_(0),
      string_buf_(ObModIds::OB_SSTABLE_WRITER),
      binary_rowkey_info_(rowkey_info)
    {
    }


    ObSSTableRow::~ObSSTableRow()
    {
      if (NULL != row_key_buf_)
      {
        ob_free(row_key_buf_);
        row_key_buf_ = NULL;
      }
      //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
      if (NULL != row_key_string_buf_)
      {
        ob_free(row_key_string_buf_);
        row_key_string_buf_ = NULL;
      }
      //add:e
      string_buf_.clear();
    }

    int ObSSTableRow::set_obj_count(const int64_t obj_count,
                                    const bool sparse_format)
    {
      int ret = OB_SUCCESS;

      if (obj_count <= 0 || (!sparse_format && obj_count > OB_MAX_COLUMN_NUMBER))
      {
        TBSYS_LOG(WARN, "invalid param, obj_count=%ld, sparse_format=%d",
                  obj_count, sparse_format);
        ret = OB_ERROR;
      }
      else
      {
        obj_count_ = static_cast<int32_t>(obj_count);
      }

      return ret;
    }

    const ObRowkeyInfo* ObSSTableRow::get_rowkey_info() const
    {
      return binary_rowkey_info_;
    }

    int ObSSTableRow::set_rowkey_info(const ObRowkeyInfo* rowkey_info)
    {
      int ret = OB_SUCCESS;
      if (NULL == rowkey_info)
      {
        TBSYS_LOG(WARN, "invalide argument rowkey_info=%p", rowkey_info);
        ret = OB_ERROR;
      }
      else
      {
        binary_rowkey_info_ = rowkey_info;
      }
      return ret;
    }

    int ObSSTableRow::set_table_id(const uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      if (0 == table_id || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "invalid argument table_id=%lu", table_id);
        ret = OB_ERROR;
      }
      else
      {
        table_id_ = table_id;
      }
      return ret;
    }

    int ObSSTableRow::set_column_group_id(const uint64_t column_group_id)
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == column_group_id)
      {
        TBSYS_LOG(WARN, "invalid argument column_group_id=%lu", column_group_id);
        ret = OB_ERROR;
      }
      else
      {
        column_group_id_ = column_group_id;
      }

      return ret;
    }

    const ObObj* ObSSTableRow::get_obj(const int32_t index) const
    {
      const ObObj* obj = NULL;

      if (index >= 0 && index < obj_count_)
      {
        obj = &objs_[index];
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, index=%d", index);
      }

      return obj;
    }
 //add zhuyanchao [secondary index static data build]20150321
    ObObj* ObSSTableRow::get_obj_v1(const int32_t index)
    {
      ObObj* obj = NULL;

      if (index >= 0 && index < obj_count_)
      {
        obj = &objs_[index];
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, index=%d", index);
      }

      return obj;
    }
    ObObj ObSSTableRow::get_row_obj(const int64_t index)
    {
        //modify liuxiao [secondary index static_index_build.bug_fix.merge_error]20150604
        //return objs_[index];
        ObObj obj;
        if (index >= 0 && index < obj_count_)
        {
          obj = objs_[index];
        }
        else
        {
          TBSYS_LOG(WARN, "invalid param, index=%ld", index);
        }
        return obj;
        //modify e
    }
//add e
    const ObObj* ObSSTableRow::get_row_objs(int64_t& obj_count)
    {
      obj_count = obj_count_;
      return objs_;
    }

    int ObSSTableRow::clear()
    {
      int ret = OB_SUCCESS;

      rowkey_obj_count_ = 0;
      obj_count_ = 0;
      row_key_string_buf_used_ = 0; //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612
      ret = string_buf_.reset();

      return ret;
    }

    int ObSSTableRow::ensure_key_buf_space(const int64_t size)
    {
      int ret         = OB_SUCCESS;
      char* new_buf   = NULL;
      int64_t key_len = size > row_key_buf_size_
        ? size : row_key_buf_size_;

      if (size <= 0)
      {
        TBSYS_LOG(WARN, "invalid key length, size=%ld", size);
        ret = OB_ERROR;
      }
      else if (NULL == row_key_buf_
               || (NULL != row_key_buf_ && size > row_key_buf_size_))
      {
        new_buf = static_cast<char*>(ob_malloc(key_len, ObModIds::OB_SSTABLE_WRITER));
        if (NULL == new_buf)
        {
          TBSYS_LOG(WARN, "Problem allocating memory for row key buffer");
          ret = OB_ERROR;
        }
        else
        {
          if (NULL != row_key_buf_)
          {
            ob_free(row_key_buf_);
            row_key_buf_ = NULL;
          }
          row_key_buf_size_ =  static_cast<int32_t>(key_len);
          row_key_buf_ = new_buf;
          memset(row_key_buf_, 0, row_key_buf_size_);
        }
      }

      return ret;
    }


    //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
    int ObSSTableRow::ensure_key_string_buf_space(const int64_t size)
    {
      int ret         = OB_SUCCESS;
      char* new_buf   = NULL;
      int64_t key_len = size > row_key_string_buf_size_
        ? size : row_key_string_buf_size_;

      if (size < 0)
      {
        TBSYS_LOG(WARN, "invalid key length, size=%ld", size);
        ret = OB_ERROR;
      }
      else if (NULL == row_key_string_buf_
               || (NULL != row_key_string_buf_ && size > row_key_string_buf_size_))
      {
        new_buf = static_cast<char*>(ob_malloc(key_len, ObModIds::OB_SSTABLE_WRITER));
        if (NULL == new_buf)
        {
          TBSYS_LOG(WARN, "Problem allocating memory for row key buffer");
          ret = OB_ERROR;
        }
        else
        {
          if (NULL != row_key_string_buf_)
          {
            ob_free(row_key_string_buf_);
            row_key_string_buf_ = NULL;
          }
          row_key_string_buf_size_ =  static_cast<int32_t>(key_len);
          row_key_string_buf_ = new_buf;
          memset(row_key_string_buf_, 0, row_key_string_buf_size_);
        }
      }

      return ret;
    }
    //add:e

    int ObSSTableRow::set_rowkey(const ObRowkey& rowkey)
    {
      int ret = OB_SUCCESS;
      int64_t length = rowkey.get_obj_cnt();
      const ObObj* ptr = rowkey.get_obj_ptr();

      if (length <= 0 || NULL == ptr)
      {
        TBSYS_LOG(WARN, "invalid row key, len=%ld ptr=%p", length, ptr);
        ret = OB_ERROR;
      }
      //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
      else if (OB_SUCCESS != (ret = const_cast<ObSSTableRow*>(this)->ensure_key_string_buf_space(rowkey.get_varchar_length())))
      {
        TBSYS_LOG(WARN, "cannot alloc key length=%ld ", rowkey.get_varchar_length());
      }
      //add:e
      else
      {
        for (int i=0; common::OB_SUCCESS == ret && i< length; i++)
        {
          //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
          common::ObObj obj = ptr[i];
          if (obj.get_type() == common::ObVarcharType)
          {
            if (obj.need_trim() && OB_SUCCESS != (ret = obj_varchar_trim(obj)))
            {
              TBSYS_LOG(WARN, "failed to trim rowkey[%d] in varchar type", i);
            }
          }
          //add:e
          if (OB_SUCCESS == ret)
          {
            ret = add_obj(obj);
          }
        }
        if (OB_SUCCESS == ret)
        {
          rowkey_obj_count_ = length;
        }
      }

      return ret;
    }

    //add wenghaixing [secondary index static_index_build]20150318
    void ObSSTableRow::set_rowkey_obj_count(const int64_t count)
    {
      rowkey_obj_count_ = count;
    }
    //add e

    int ObSSTableRow::set_binary_rowkey(const common::ObString& binary_rowkey)
    {
      // translate binary rowkey to object array;
      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
      int64_t array_size = NULL == binary_rowkey_info_ ? OB_MAX_ROWKEY_COLUMN_NUMBER : binary_rowkey_info_->get_size();
      int ret = OB_SUCCESS;

      if (binary_rowkey.length() <= 0 || NULL == binary_rowkey.ptr())
      {
        TBSYS_LOG(WARN, "invalid row key, ken_len=%d ptr=%p",
            binary_rowkey.length(), binary_rowkey.ptr());
        ret = OB_ERROR;
      }
      else if (NULL == binary_rowkey_info_)
      {
        TBSYS_LOG(WARN, "must set rowkey info when you use old binary rowkey format.");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (obj_count_ > 0)
      {
        TBSYS_LOG(WARN, "set binary_rowkey must before add obj.");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = ObRowkeyHelper::binary_rowkey_to_obj_array(
              *binary_rowkey_info_, binary_rowkey, obj_array, array_size)))
      {
        TBSYS_LOG(WARN, "translate binary rowkey to object array error.");
      }
      else
      {
        for (int64_t i = 0; OB_SUCCESS == ret && i < array_size; ++i)
        {
          ret = add_obj(obj_array[i]);
        }
        if (OB_SUCCESS == ret)
        {
          rowkey_obj_count_ = array_size;
        }
      }

      return ret;
    }

    int ObSSTableRow::get_binary_rowkey(common::ObString& binary_rowkey) const
    {
      int ret = OB_SUCCESS;
      if (NULL == binary_rowkey_info_)
      {
        TBSYS_LOG(WARN, "must set rowkey info when you use old binary rowkey format.");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (obj_count_ <  binary_rowkey_info_->get_size())
      {
        TBSYS_LOG(WARN, "obj_count_=%d < rowkey size=%ld", obj_count_, binary_rowkey_info_->get_size());
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = const_cast<ObSSTableRow*>(this)->ensure_key_buf_space(
              binary_rowkey_info_->get_binary_rowkey_length())))
      {
        TBSYS_LOG(WARN, "cannot alloc key length=%ld ", binary_rowkey_info_->get_binary_rowkey_length());
      }
      else 
      {
        binary_rowkey.assign(row_key_buf_, row_key_buf_size_);
        ret = ObRowkeyHelper::obj_array_to_binary_rowkey(
            *binary_rowkey_info_, binary_rowkey, objs_, binary_rowkey_info_->get_size());
      }
      return ret;
    }




    //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
    //only used by updateserver to fill rowkey obj into
    //sstable row with shallow copy
    int ObSSTableRow::set_internal_rowkey(const uint64_t table_id,
      const common::ObRowkey& rowkey)
    {
      int ret = common::OB_SUCCESS;
      int64_t length = rowkey.get_obj_cnt();
      const common::ObObj* ptr = rowkey.get_obj_ptr();

      if (common::OB_INVALID_ID == table_id
          || length <= 0 || NULL == ptr
          || 0 != obj_count_
          || obj_count_ >= DEFAULT_MAX_COLUMN_NUMBER - 1)
      {
        TBSYS_LOG(WARN, "invalid argument, table_id=%lu, "
                        "rowkey_length=%ld, rowkey_ptr=%p, obj_count=%d",
                  table_id, length, ptr, obj_count_);
        ret = common::OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = const_cast<ObSSTableRow*>(this)->ensure_key_string_buf_space(rowkey.get_varchar_length())))
      {
        TBSYS_LOG(WARN, "cannot alloc key length=%ld ", rowkey.get_varchar_length());
      }
      else
      {
        table_id_ = table_id;
        column_group_id_ = common::OB_DEFAULT_COLUMN_GROUP_ID;
        for (int i=0; common::OB_SUCCESS == ret && i< length; i++)
        {
          common::ObObj obj = ptr[i];
          if (obj.get_type() == common::ObVarcharType)
          {
            if (obj.need_trim() && OB_SUCCESS != (ret = obj_varchar_trim(obj)))
            {
              TBSYS_LOG(WARN, "failed to trim rowkey[%d] in varchar type", i);
            }
          }
          memcpy(&objs_[obj_count_++], &obj, sizeof(common::ObObj));
        }
        rowkey_obj_count_ = length;
      }
      return ret;
    }
    //add:e



    //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
    int ObSSTableRow::obj_varchar_trim(common::ObObj & obj)
    {
      int ret = OB_SUCCESS;
      if (NULL == row_key_string_buf_)
      {
        TBSYS_LOG(WARN, "row_key_string_buf_ is NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (obj.get_type() != common::ObVarcharType)
      {
        TBSYS_LOG(WARN, "obj must be ObVarcharType");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (row_key_string_buf_used_ + obj.get_varchar_trim_length() > row_key_string_buf_size_)
      {
        TBSYS_LOG(WARN, "row_key_string_buf_used_=%d + VARCHAR TRIM LEN[%d] > row_key_string_buf_size_ =%d", row_key_string_buf_used_, obj.get_val_len()
                  ,row_key_string_buf_size_);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int actual_len = obj.get_varchar_trim_length();

        ObString dst_value;
        if (OB_SUCCESS != (ret = obj.get_varchar(dst_value)))
        {
          TBSYS_LOG(WARN, "obj must be ObVarcharType");
        }
        else
        {
          memcpy(row_key_string_buf_+row_key_string_buf_used_, dst_value.ptr(), actual_len);
          common::ObString trim_varchar(0,actual_len, row_key_string_buf_+row_key_string_buf_used_);
          obj.set_varchar(trim_varchar);
          row_key_string_buf_used_ += actual_len;
        }
      }
      return ret;
    }
    //add:e

    int ObSSTableRow::check_schema(const ObSSTableSchema& schema) const
    {
      int ret              = OB_SUCCESS;
      int64_t column_count = 0;
      int64_t rowkey_column_count = OB_MAX_ROWKEY_COLUMN_NUMBER;
      const ObSSTableSchemaColumnDef* rowkey_column_defs[rowkey_column_count];

      const ObSSTableSchemaColumnDef* column_def = schema.get_group_schema(table_id_, column_group_id_,
                                                                           column_count);
      ObObjType obj_type   = ObNullType;

      if (column_count <= 0 || obj_count_ <= 0 || NULL == column_def)
      {
        TBSYS_LOG(WARN, "invalid column count in scheam or obj count in row,"
                  "column_count=%ld, obj_count=%d, column_def=%p",
                  column_count, obj_count_, column_def);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = schema.get_rowkey_column_count(table_id_, rowkey_column_count)))
      {
        TBSYS_LOG(WARN, "get rowkey column count of table=%ld error.", table_id_);
      }
      else if (OB_SUCCESS != (ret = schema.get_rowkey_columns(table_id_, rowkey_column_defs, rowkey_column_count)))
      {
        TBSYS_LOG(WARN, "get rowkey column defs of table=%ld error.", table_id_);
      }
      else
      {
        // compare rowkey objs;
        int64_t def_index = 0; 
        int64_t obj_index = 0;

        for (; def_index < rowkey_column_count; ++def_index)
        {
          int16_t seq = rowkey_column_defs[def_index]->rowkey_seq_;
          if (seq <= 0 || seq > rowkey_column_count)
          {
            TBSYS_LOG(ERROR, "seq=%d not legal, rowkey column count=%ld", 
                seq, rowkey_column_count);
            ret = OB_SUCCESS;
            break;
          }
          else if (rowkey_column_defs[def_index]->column_value_type_ !=  objs_[seq - 1].get_type() 
              && ObNullType != objs_[seq - 1].get_type()) // allow NULL rowkey column
          {
              TBSYS_LOG(WARN, "rowkey obj[i=%ld,seq=%d,id=%d] type is inconsistent "
                  "with column type, obj_type=%d, column_type=%d", def_index, seq, 
                  rowkey_column_defs[def_index]->column_name_id_,
                  objs_[seq - 1].get_type(), rowkey_column_defs[def_index]->column_value_type_);
              ret = OB_ERROR;
              break;
          }
        }

        // compare columns;
        obj_index = rowkey_column_count;
        // skip rowkey columns;
        if (is_binary_rowkey() && obj_index == 0)
        {
          obj_index = binary_rowkey_info_->get_size();
        }
        def_index = 0;
        while (obj_index < obj_count_ && def_index < column_count)
        {
          obj_type = objs_[obj_index].get_type();
          if (NULL == column_def + def_index)
          {
            TBSYS_LOG(WARN, "problem get column def, column_def=%p", column_def);
            ret = OB_ERROR;
            break;
          }
          else if (column_def[def_index].rowkey_seq_ != 0)
          {
            ++def_index;
            continue;
          }
          else if (ObNullType != obj_type
                   && obj_type != column_def[def_index].column_value_type_)
          {
            TBSYS_LOG(WARN, "value obj[i=%ld,id=%d] type is inconsistent with column type,"
                "obj_type=%d, column_type=%d", def_index, 
                column_def[def_index].column_name_id_, obj_type,
                column_def[def_index].column_value_type_);
            ret = OB_ERROR;
            break;
          }
          else
          {
            ++def_index;
            ++obj_index;
          }
        }

        if (OB_SUCCESS == ret && (obj_index != obj_count_ || def_index != column_count))
        {
          TBSYS_LOG(WARN, "column count not match, obj_index=%ld, obj_count_=%d,"
              "def_index=%ld, column_count=%ld", obj_index, obj_count_, def_index, column_count);
          ret = OB_SIZE_OVERFLOW;
        }
      }

      return ret;
    }

    int ObSSTableRow::serialize(char* buf, const int64_t buf_len, int64_t& pos, 
        const int64_t row_serialize_size) const
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = row_serialize_size > 0 ? row_serialize_size : get_serialize_size();
      int64_t start_obj_index = 0;

      if((NULL == buf) || (serialize_size + pos > buf_len))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld",
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }
      else
      {
        if (is_binary_rowkey())
        {
          // serialize use old binary rowkey format;
          ObString binary_rowkey;
          ret = get_binary_rowkey(binary_rowkey);
          if (OB_SUCCESS == ret)
          {
            ret = encode_i16(buf, buf_len, pos, static_cast<int16_t>(binary_rowkey.length()));
          }
          if (OB_SUCCESS == ret)
          {
            memcpy(buf + pos, binary_rowkey.ptr(), binary_rowkey.length());
            pos += binary_rowkey.length();

            start_obj_index += binary_rowkey_info_->get_size();
          }
        }

        for (int64_t i = start_obj_index; i < obj_count_; ++i)
        {
          ret = objs_[i].serialize(buf, buf_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to serialzie obj, current obj index=%ld,"
                "obj_count=%d", i, obj_count_);
            break;
          }
        }
      }
      return ret;
    }

    DEFINE_SERIALIZE(ObSSTableRow)
    {
      return serialize(buf, buf_len, pos, 0);
    }

    DEFINE_DESERIALIZE(ObSSTableRow)
    {
      int ret                 = OB_SUCCESS;
      int64_t i               = 0;

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_ERROR;
      }
  
      if (OB_SUCCESS == ret && obj_count_ > 0)
      {
        //end of deserialze sstable rowkey in obobj way
        for (i = 0; OB_SUCCESS == ret && i < obj_count_; ++i)
        {
          if (obj_count_ < DEFAULT_MAX_COLUMN_NUMBER)
          {
            ret = objs_[i].deserialize(buf, data_len, pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to deserialzie obj, current obj index=%ld,"
                              "obj_count=%d", i, obj_count_);
            }
          }
        }

        if (i != obj_count_)
        {
          TBSYS_LOG(WARN, "expect deserialize obj_count=%d, but only "
                          "get obj_count=%ld", obj_count_, i);
          ret = OB_ERROR;
        }
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "deserialize sstable row failed, objcount = %d, binary_rowkey_info_ = %p",
                  obj_count_, binary_rowkey_info_);
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableRow)
    {
      int64_t total_size = 0;
      int64_t start_obj_index = 0;
      if (is_binary_rowkey())
      {
        //key length occupies 2 bytes
        total_size += sizeof(int16_t) + binary_rowkey_info_->get_binary_rowkey_length(); 
        start_obj_index += binary_rowkey_info_->get_size();
      }

      for (int64_t i = start_obj_index; i < obj_count_; ++i)
      {
        total_size += objs_[i].get_serialize_size();
      }
      return total_size;
    }
  } // end namespace sstable
} // end namespace oceanbase
