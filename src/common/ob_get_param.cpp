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
#include "utility.h"
#include "ob_action_flag.h"
#include "ob_malloc.h"
#include "ob_get_param.h"
#include "ob_rowkey_helper.h"
#include "ob_schema.h"

namespace oceanbase
{
  namespace common
  {
    const char * ObGetParam::OB_GET_ALL_COLUMN_NAME_CSTR = "*";
    const ObString ObGetParam::OB_GET_ALL_COLUMN_NAME(static_cast<int32_t>(strlen(OB_GET_ALL_COLUMN_NAME_CSTR)),
      static_cast<int32_t>(strlen(OB_GET_ALL_COLUMN_NAME_CSTR)),
      const_cast<char*>(OB_GET_ALL_COLUMN_NAME_CSTR));
    ObGetParam::ObGetParam(const bool deep_copy_args)
      : deep_copy_args_(deep_copy_args), single_cell_(false),
      cell_list_(NULL), cell_size_(0), cell_capacity_(DEFAULT_CELL_CAPACITY),
      prev_table_id_(OB_INVALID_ID), row_index_(NULL),
      row_size_(0), id_only_(true), is_binary_rowkey_format_(false), schema_manager_(NULL),
      mod_(ObModIds::OB_GET_PARAM), allocator_(DEFAULT_CELLS_BUF_SIZE , mod_)
      //add lbzhong [Update rowkey] 20160509:b
      , has_new_row_(false)
      //add:e
    {
      prev_rowkey_.assign(NULL, 0);
      prev_table_name_.assign(NULL, 0);
    }

    ObGetParam::ObGetParam(const ObCellInfo& cell_info, const bool deep_copy_args)
    : deep_copy_args_(deep_copy_args)
    {
      add_only_one_cell(cell_info);
    }

    ObGetParam::~ObGetParam()
    {
      destroy();
    }

    void ObGetParam::set_deep_copy_args(const bool deep_copy_args)
    {
      deep_copy_args_ = deep_copy_args;
    }

    int ObGetParam::init()
    {
      int ret = OB_SUCCESS;


      // allocate 128 ObCellInfo for first use.
      cell_capacity_ = DEFAULT_CELL_CAPACITY;
      int64_t cells_buf_size = cell_capacity_ * sizeof(ObCellInfo);

      if (NULL == cell_list_)
      {
        cell_list_ = reinterpret_cast<ObCellInfo*>(allocator_.alloc(cells_buf_size));
        if (NULL == cell_list_)
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for cell buffer of get param");
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret && NULL == row_index_ && cell_capacity_ > 0)
      {
        row_index_ = reinterpret_cast<ObRowIndex*>
          (allocator_.alloc(cell_capacity_ * sizeof(ObRowIndex)));
        if (NULL == row_index_)
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for row index of get param");
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObGetParam::expand()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = double_expand_storage(
              row_index_, cell_size_, MAX_CELL_CAPACITY, cell_capacity_, allocator_)))
      {
        TBSYS_LOG(WARN, "expand rowindex storage error, cell_size_=%ld, "
            "cell_capacity_=%ld, max=%ld", cell_size_, cell_capacity_, MAX_CELL_CAPACITY);
      }
      else if (OB_SUCCESS != (ret = double_expand_storage(
              cell_list_, cell_size_, MAX_CELL_CAPACITY, cell_capacity_, allocator_)))
      {
        TBSYS_LOG(WARN, "expand cell storage error, cell_size_=%ld, "
            "cell_capacity_=%ld, max=%ld", cell_size_, cell_capacity_, MAX_CELL_CAPACITY);
      }
      return ret;
    }


    int64_t ObGetParam::to_string(char *buf, int64_t buf_size) const
    {
      int64_t pos = 0;
      int64_t used_len = 0;
      if (NULL != buf && buf_size > 0)
      {
        if ((used_len =  snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, "[Get CellNum:%ld]:\n", cell_size_)) > 0)
        {
          pos += used_len;
        }
        databuff_printf(buf, buf_size, pos, "\t[VersionRange:%s]\n", to_cstring(version_range_));
        /*
        for (int64_t i = 0; i < cell_size_ && pos < buf_size; ++i)
        {
          used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0,
              "\tcell:%ld,(%s)\n", i, print_cellinfo(&cell_list_[i]));
          pos += used_len;
        }
        */
        int32_t offset = 0;
        int32_t size = 0;
        for (int64_t i = 0; i < row_size_ && pos < buf_size; ++i)
        {
          offset = row_index_[i].offset_;
          size   = row_index_[i].size_;
          used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0,
              "\trow:%ld,(%s)::", i, to_cstring(cell_list_[offset].row_key_));
          pos += used_len;
          if (pos + 1 > buf_size) break;
          for (int32_t j = 0; j < size; ++j)
          {
            used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0,
                "column:[%ld][%.*s]:value[%s]%s",
                cell_list_[j + offset].column_id_,
                cell_list_[j + offset].column_name_.length(),
                cell_list_[j + offset].column_name_.ptr(),
                to_cstring(cell_list_[j + offset].value_), j < size - 1 ? "|" : "\n");
            pos += used_len;
            if (pos + 1 > buf_size) break;
          }
        }
      }
      return pos;
    }

    void ObGetParam::print_memory_usage(const char* msg) const
    {
      TBSYS_LOG(INFO, "ObGetParam use memory:%s, allocator:pages:%ld,used:%ld,total:%ld,",
          msg, allocator_.pages(), allocator_.used(), allocator_.total());
    }

    int ObGetParam::check_name_and_id(const ObCellInfo& cell_info, bool is_first)
    {
      int ret = OB_SUCCESS;

      if (is_first)
      {
        //check name(table name and column name) first
        if (cell_info.table_name_.length() > 0
          && NULL != cell_info.table_name_.ptr()
          && cell_info.column_name_.length() > 0
          && NULL != cell_info.column_name_.ptr()
          && (OB_INVALID_ID == cell_info.column_id_ || 0 == cell_info.column_id_)
          && (0 == cell_info.table_id_ || OB_INVALID_ID == cell_info.table_id_))
        {
          /**
           * if the it's the first cell in cell list, and the table name
           * and column name is legal, set id_only_ false
           */
          id_only_ = false;
        }
        else if (cell_info.column_id_ != OB_INVALID_ID
          && cell_info.table_id_ != 0
          && cell_info.table_id_ != OB_INVALID_ID
          && 0 == cell_info.table_name_.length()
          && NULL == cell_info.table_name_.ptr()
          && 0 == cell_info.column_name_.length()
          && NULL == cell_info.column_name_.ptr())
        {
          /**
           * if the it's the first cell in cell list, and the table id
           * and column id is legal, set id_only_ true
           */
          id_only_ = true;
        }
        else
        {
          TBSYS_LOG(WARN, "invalid table name and table id "
            "or column name and column id, or specify "
            "both name and id, table name=%p, "
            "table name length=%d, table_id=%lu column name=%p"
            "column name length=%d, column_id=%lu",
            cell_info.table_name_.ptr(), cell_info.table_name_.length(),
            cell_info.table_id_, cell_info.column_name_.ptr(),
            cell_info.column_name_.length(), cell_info.column_id_);
          ret = OB_ERROR;
        }
      }
      else
      {
        /**
         * if only accept id(table id and column id), but id is illegal
         * or specify name(table name or column name), it's illegal
         */
        if (id_only_ && (cell_info.column_id_ == OB_INVALID_ID
          || cell_info.table_id_ == 0
          || cell_info.table_id_ == OB_INVALID_ID
          || cell_info.table_name_.length() > 0
          || NULL != cell_info.table_name_.ptr()
          || cell_info.column_name_.length() > 0
          || NULL != cell_info.column_name_.ptr()))
        {
          TBSYS_LOG(WARN, "not specify table id or cloumn id or specify table "
            "name or column name, non-consistient with cell before, "
            "id_only=%d, table name=%p, table name length=%d, "
            "table_id=%lu, column name=%p, column name length=%d, "
            "column_id=%lu",
            id_only_, cell_info.table_name_.ptr(),
            cell_info.table_name_.length(),
            cell_info.table_id_, cell_info.column_name_.ptr(),
            cell_info.column_name_.length(), cell_info.column_id_);
          ret = OB_ERROR;
        }
        else if (!id_only_ && (cell_info.column_name_.length() <= 0
          || NULL == cell_info.column_name_.ptr()
          || cell_info.table_name_.length() <= 0
          || NULL == cell_info.table_name_.ptr()
          || cell_info.column_id_ != OB_INVALID_ID
          || (cell_info.table_id_ != 0
          && cell_info.table_id_ != OB_INVALID_ID)))
        {
          /**
           * if only accept name(table name and column name), but name is
           * illegal or specify id(table id or column id), it's
           * illegal
           */
          TBSYS_LOG(WARN, "not specify table name or cloumn name, "
            "non-consistient with cell before, "
            "id_only=%d, table name=%p, table name length=%d, "
            "table_id=%lu, column name=%p, column name length=%d, "
            "column_id=%lu",
            id_only_, cell_info.table_name_.ptr(),
            cell_info.table_name_.length(),
            cell_info.table_id_, cell_info.column_name_.ptr(),
            cell_info.column_name_.length(), cell_info.column_id_);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObGetParam::copy(const ObGetParam& other)
    {
      int err = OB_SUCCESS;
      ObCellInfo* cell = NULL;
      reset();
      for(int64_t i = 0; OB_SUCCESS == err && i < other.get_cell_size(); i++)
      {
        if (NULL == (cell = other[i]))
        {
          err = OB_INVALID_ARGUMENT;
          TBSYS_LOG(ERROR, "other[%ld] = NULL", i);
        }
        else if (OB_SUCCESS != (err = add_cell(*cell)))
        {
          TBSYS_LOG(ERROR, "add_cell()=>%d", err);
        }
      }
      return err;
    }

    int ObGetParam::add_cell(const ObCellInfo& cell_info)
    {
      int ret = OB_SUCCESS;
      //add lbzhong [Update rowkey] 20160509:b
      if(!has_new_row_ && cell_info.is_new_row_)
      {
        has_new_row_ = true;
      }
      //add:e
      ObCellInfo stored_cell = cell_info;
      if (deep_copy_args_)
      {
        ret = copy_cell_info(cell_info, stored_cell);
      }

      if (OB_SUCCESS == ret)
      {
        //cell info must include legal row key
        if (stored_cell.row_key_.length() <= 0 || NULL == stored_cell.row_key_.ptr())
        {
          TBSYS_LOG(WARN, "invalid row key of cell info, key_len=%ld, key_ptr=%p",
            stored_cell.row_key_.length(), stored_cell.row_key_.ptr());
          ret = OB_INVALID_ARGUMENT;
        }
      }

      //allocate memory for cell list and row index is necessary
      if (OB_SUCCESS == ret && (NULL == cell_list_ || NULL == row_index_))
      {
        ret = init();
      }

      if (OB_SUCCESS == ret)
      {
        if (cell_size_ >= cell_capacity_)
        {
          ret = expand();
        }
      }

      if (OB_SUCCESS == ret)
      {
        //ensure there are enough space to store this cell
        if (cell_size_ < cell_capacity_ && row_size_ < cell_capacity_)
        {
          ret = check_name_and_id(stored_cell, (0 == cell_size_));
          if (OB_SUCCESS == ret)
          {
            if (0 == cell_size_)
            {
              //the first cell
              prev_table_name_ = stored_cell.table_name_;
              prev_table_id_ = stored_cell.table_id_;
              prev_rowkey_ = stored_cell.row_key_;
              row_index_[row_size_].offset_ = static_cast<int32_t>(cell_size_);
              row_index_[row_size_].size_ = 1;
              ++row_size_;
            }
            else
            {
              if (((id_only_ && prev_table_id_ == stored_cell.table_id_)
                || (!id_only_ && prev_table_name_ == stored_cell.table_name_))
                && prev_rowkey_ == stored_cell.row_key_ && row_size_ > 0
                && row_index_[row_size_ -1].size_ < MAX_CELLS_PER_ROW)
              {
                //the same row, increase the counter
                row_index_[row_size_ - 1].size_++;
              }
              else
              {
                //different row, switch row
                prev_table_name_ = stored_cell.table_name_;
                prev_table_id_ = stored_cell.table_id_;
                prev_rowkey_ = stored_cell.row_key_;
                row_index_[row_size_].offset_ = static_cast<int32_t>(cell_size_);
                row_index_[row_size_].size_ = 1;
                ++row_size_;
              }
            }

            //store cell info and increase cell counter
            cell_list_[cell_size_] = stored_cell;
            ++cell_size_;
          }
        }
        else
        {
          TBSYS_LOG(INFO, "get param is full, can't add cell anymore, "
            "cell_size=%ld row_size=%d, capacity=%ld, max=%ld",
            cell_size_, row_size_, cell_capacity_, MAX_CELL_CAPACITY);
          ret = OB_SIZE_OVERFLOW;
        }
      }

      return ret;
    }

    int ObGetParam::copy_cell_info(const ObCellInfo & cell_info, ObCellInfo & stored_cell)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = deep_copy_ob_string(allocator_, cell_info.table_name_, stored_cell.table_name_)))
      {
        TBSYS_LOG(WARN,"fail to copy table name to local buffer [err:%d]", err);
      }
      else if (OB_SUCCESS != (err = deep_copy_ob_string(allocator_, cell_info.column_name_, stored_cell.column_name_)))
      {
        TBSYS_LOG(WARN,"fail to copy column name to local buffer [err:%d]", err);
      }
      else if (OB_SUCCESS != (err = cell_info.row_key_.deep_copy(stored_cell.row_key_, allocator_)))
      {
        TBSYS_LOG(WARN,"fail to copy rowkey to local buffer [err:%d]", err);
      }
      else if (OB_SUCCESS != (err = deep_copy_ob_obj(allocator_, cell_info.value_, stored_cell.value_)))
      {
        TBSYS_LOG(WARN,"fail to copy value to local buffer [err:%d]", err);
      }
      return err;
    }

    int ObGetParam::add_only_one_cell(const ObCellInfo& cell_info)
    {
      int ret = OB_SUCCESS;
      ObCellInfo stored_cell = cell_info;
      if (deep_copy_args_)
      {
        ret = copy_cell_info(cell_info, stored_cell);
      }

      if (OB_SUCCESS == ret)
      {
        //cell info must include legal row key
        if (stored_cell.row_key_.length() <= 0 || NULL == stored_cell.row_key_.ptr())
        {
          TBSYS_LOG(WARN, "invalid row key of cell info, key_len=%ld, key_ptr=%p",
            stored_cell.row_key_.length(), stored_cell.row_key_.ptr());
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          single_cell_ = true;
          fixed_cell_ = stored_cell;
          fixed_row_index_.offset_ = 0;
          fixed_row_index_.size_ = 1;
          cell_list_ = &fixed_cell_;
          cell_size_ = 1;
          cell_capacity_ = 1;
          prev_table_id_ = OB_INVALID_ID;
          row_index_ = &fixed_row_index_;
          row_size_ = 1;
          id_only_ = true;
          prev_rowkey_.assign(NULL, 0);
          prev_table_name_.assign(NULL, 0);
        }
      }

      return ret;
    }

    int64_t ObGetParam::find_row_index(const int64_t cell_offset) const
    {
      int64_t ret     = -1;
      int64_t left    = 0;
      int64_t middle  = 0;
      int64_t right   = row_size_ - 1;

      //binary search
      if (cell_offset >= 0 && cell_offset < cell_size_
        && NULL != row_index_ && row_size_ > 0)
      {
        while (left <= right)
        {
          middle = (left + right) / 2;
          if (row_index_[middle].offset_ > cell_offset)
          {
            right = middle - 1;
          }
          else if (row_index_[middle].offset_ < cell_offset)
          {
            left = middle + 1;
          }
          else
          {
            ret = middle;
            break;
          }
        }
      }

      return ret;
    }

    int ObGetParam::rollback()
    {
      int ret = OB_SUCCESS;

      if (cell_size_ <= 0 || row_size_ <= 1
        || NULL == cell_list_ || NULL == row_index_)
      {
        TBSYS_LOG(WARN, "there is no cells or only one row in get param, "
          "can't rollback, cell_size=%ld row_size=%d "
          "cell_list=%p row_index=%p",
          cell_size_, row_size_, cell_list_, row_index_);
        ret = OB_ERROR;
      }
      else
      {
        //just decreate the row count and cell count to rollback
        --row_size_;
        cell_size_ -= row_index_[row_size_].size_;
      }

      return ret;
    }

    int ObGetParam::rollback(const int64_t count)
    {
      int ret       = OB_SUCCESS;
      int64_t index = -1;

      if (count <= 0 || cell_size_ <= count || row_size_ <= 1
        || NULL == cell_list_ || NULL == row_index_)
      {
        TBSYS_LOG(WARN, "there is no cells or only one row in get param, "
          "can't rollback, rollback count=%ld cell_size=%ld "
          "row_size=%d cell_list=%p row_index=%p",
          count, cell_size_, row_size_, cell_list_, row_index_);
        ret = OB_ERROR;
      }
      else
      {
        index = find_row_index(cell_size_ - count);
        if (index == -1)
        {
          TBSYS_LOG(WARN, "specify wrong rollback count, the count isn't in"
            "row edge, rollback count=%ld cell_size=%ld row_size=%d",
            count, cell_size_, row_size_);
          ret = OB_ERROR;
        }
        else if (index > 0)
        {
          /**
           * we need ensure after rollback there is one row at least, just
           * decreate the row count and cell count to rollback
           */
          cell_size_ -= count;
          row_size_ = static_cast<int32_t>(index);
        }
        else //index == 0
        {
          TBSYS_LOG(WARN, "after rollback there is no row"
            "rollback count=%ld cell_size=%ld row_size=%d",
            count, cell_size_, row_size_);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    void ObGetParam::reset(bool deep_copy_args)
    {
      cell_size_ = 0;
      row_size_ = 0;
      prev_rowkey_.assign(NULL, 0);
      id_only_ = true;
      row_index_ = NULL;
      cell_list_ = NULL;
      ObReadParam::reset(); //call parent reset()
      // if memory inflates too large, free.
      if (allocator_.total() > DEFAULT_CELLS_BUF_SIZE)
      {
        allocator_.free();
      }
      else
      {
        allocator_.reuse();
      }
      deep_copy_args_ = deep_copy_args;
      //add lbzhong [Update rowkey] 20160509:b
      has_new_row_ = false;
      //add:e
    }

    int ObGetParam::serialize_flag(char* buf, const int64_t buf_len, int64_t& pos,
      const int64_t flag) const
    {
      int ret = OB_SUCCESS;
      ObObj obj;

      obj.set_ext(flag);
      ret = obj.serialize(buf, buf_len, pos);

      return ret;
    }

    int ObGetParam::serialize_int(char* buf, const int64_t buf_len, int64_t& pos,
      const int64_t value) const
    {
      int ret = OB_SUCCESS;
      ObObj obj;

      obj.set_int(value);
      ret = obj.serialize(buf, buf_len, pos);

      return ret;
    }

    int ObGetParam::deserialize_int(const char* buf, const int64_t data_len, int64_t& pos,
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

    int64_t ObGetParam::get_obj_serialize_size(const int64_t value, bool is_ext) const
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

    int64_t ObGetParam::get_obj_serialize_size(const ObString& str) const
    {
      ObObj obj;

      obj.set_varchar(str);

      return obj.get_serialize_size();
    }

    int ObGetParam::serialize_basic_field(char* buf,
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
        ret = ObReadParam::serialize(buf, buf_len, pos);

        //add zhujun [transaction read uncommit]2016/3/28
        //trans_id
        if (OB_SUCCESS == ret)
        {
            //TBSYS_LOG(INFO,"ObGetParam::serialize transaction id:%s",to_cstring(trans_id_));
            ret = trans_id_.serialize(buf,buf_len,pos);
        }
        //add:e
      }
      return ret;

    }

    int ObGetParam::deserialize_basic_field(const char* buf,
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
        ret = ObReadParam::deserialize(buf, data_len, pos);
        //add zhujun [transaction read uncommit]//2016/3/28
        // trans_id
        if (OB_SUCCESS == ret)
        {
          ObTransID temp;
          ret = temp.deserialize(buf, data_len, pos);
          if (OB_SUCCESS == ret)
          {
              trans_id_ = temp;
              //add huangcc [fix transaction read uncommit bug]2016/11/21
              if (trans_id_.is_valid())
              {
                    is_read_master_ = true;
              }
              //add end
          }
         // TBSYS_LOG(INFO,"ObGetParam:deserialize transaction id:%s",to_cstring(temp));
        }
        //add:e
      }
      return ret;
    }

    int64_t ObGetParam::get_basic_field_serialize_size(void) const
    {
      int64_t total_size = 0;
      total_size += get_obj_serialize_size(ObActionFlag::BASIC_PARAM_FIELD, true);
      total_size += ObReadParam::get_serialize_size();
      //add zhujun [transaction read uncommit]//2016/3/28
      total_size += trans_id_.get_serialize_size();
      //add:e
      return total_size;
    }

    int ObGetParam::serialize_name_or_id(char* buf, const int64_t buf_len,
      int64_t& pos, const ObString name,
      const uint64_t id, bool is_field)  const
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
        //serialize table id or table name
        obj.reset();
        //check name first
        if (name.length() > 0 && NULL != name.ptr())
        {
          obj.set_varchar(name);
        }
        //table id can't be 0, but column can be 0
        else if ((is_field && id != 0 && id != OB_INVALID_ID)
          || (!is_field && id != OB_INVALID_ID))
        {
          obj.set_int(id);
        }
        else
        {
          TBSYS_LOG(WARN, "name and id is invalid, id=%lu name length=%d",
            id, name.length());
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = obj.serialize(buf, buf_len, pos);
      }

      return ret;
    }

    int ObGetParam::deserialize_name_or_id(const char* buf, const int64_t data_len,
      int64_t& pos, ObString& name,
      uint64_t& id)
    {
      int ret         = OB_SUCCESS;
      ObObjType type  = ObNullType;
      ObObj obj;

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
          buf, data_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        //deserialize table id or table name
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          type = obj.get_type();
          if (ObVarcharType == type)
          {
            obj.get_varchar(name);
          }
          else if (ObIntType == type)
          {
            obj.get_int(reinterpret_cast<int64_t&>(id));
          }
          else
          {
            TBSYS_LOG(WARN, "invalid object type for deserialize name or id,"
              "expect ObVarcharType or ObIntType, but get type=%d",
              type);
            ret = OB_ERROR;
          }
        }
      }

      return ret;
    }

    int64_t ObGetParam::get_serialize_name_or_id_size(const ObString name,
      const uint64_t id,
      bool is_field) const
    {
      int64_t total_size = 0;

      if (is_field)
      {
        total_size += get_obj_serialize_size(ObActionFlag::TABLE_NAME_FIELD, true);
      }

      //check name first
      if (name.length() > 0 && NULL != name.ptr())
      {
        total_size += get_obj_serialize_size(name);
      }
      //table id can't be 0, but column can be 0
      else if ((is_field && id != 0 && id != OB_INVALID_ID)
        || (!is_field && id != OB_INVALID_ID))
      {
        total_size += get_obj_serialize_size(id, false);
      }
      else
      {
        TBSYS_LOG(WARN, "name and id is invalid, id=%lu name length=%d",
          id, name.length());
      }

      return total_size;
    }

    int ObGetParam::serialize_table_info(char* buf, const int64_t buf_len,
      int64_t& pos, const ObString table_name,
      const uint64_t table_id) const
    {
      int ret = OB_SUCCESS;
      ObObj obj;

      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld",
          buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      //serialize table name field
      if (OB_SUCCESS == ret)
      {
        ret = serialize_flag(buf, buf_len, pos, ObActionFlag::TABLE_NAME_FIELD);
      }

      //serialize table name or table id
      if (OB_SUCCESS == ret)
      {
        ret = serialize_name_or_id(buf, buf_len, pos, table_name, table_id, true);
      }

      return ret;
    }

    int ObGetParam::deserialize_table_info(const char* buf, const int64_t data_len,
      int64_t& pos, ObString& table_name,
      uint64_t& table_id)
    {
      return deserialize_name_or_id(buf, data_len, pos, table_name, table_id);
    }

    int64_t ObGetParam::get_serialize_table_info_size(const ObString table_name,
      const uint64_t table_id) const
    {
      return get_serialize_name_or_id_size(table_name, table_id, true);
    }

    int ObGetParam::serialize_column_info(char* buf, const int64_t buf_len,
      int64_t& pos, const ObString column_name,
      const uint64_t column_id) const
    {
      return serialize_name_or_id(buf, buf_len, pos, column_name, column_id, false);
    }

    int64_t ObGetParam::get_serialize_column_info_size(const ObString column_name,
      const uint64_t column_id) const
    {
      return get_serialize_name_or_id_size(column_name, column_id, false);
    }

    int ObGetParam::serialize_rowkey(char* buf, const int64_t buf_len,
      int64_t& pos, const ObRowkey& row_key) const
    {
      int ret = OB_SUCCESS;

      if (NULL == buf || buf_len <= 0 || pos > buf_len
        || NULL == row_key.ptr() || row_key.length() <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld"
          "rowkey ptr=%p rowkey length=%ld",
          buf, buf_len, pos, row_key.ptr(), row_key.length());
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        //serialize row key
        ret = serialize_flag(buf, buf_len, pos, ObActionFlag::FORMED_ROW_KEY_FIELD);
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_rowkey_obj_array(buf, buf_len, pos,
            row_key.get_obj_ptr(), row_key.get_obj_cnt());
      }

      return ret;
    }

    int ObGetParam::deserialize_rowkey(const char* buf, const int64_t data_len,
      int64_t& pos, ObRowkey& row_key)
    {
      int ret         = OB_SUCCESS;
      int64_t size    = OB_MAX_ROWKEY_COLUMN_NUMBER;

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        //deserialize rowkey
        ObObj* array = NULL;
        char *obj_buf = allocator_.alloc(sizeof(ObObj) * size);
        if (NULL == obj_buf)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "allocate mem for obj array fail");
        }
        else
        {
          array = new(obj_buf)ObObj[size];
        }

        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = get_rowkey_obj_array(buf, data_len, pos, array, size)))
          {
            TBSYS_LOG(WARN, "get rowkey obj array fail:ret[%d]", ret);
          }
          else
          {
            row_key.assign(array, size);
          }
        }
      }

      return ret;
    }

    int ObGetParam::deserialize_binary_rowkey(const char* buf, const int64_t data_len,
        int64_t& pos, const ObRowkeyInfo& rowkey_info, ObRowkey& row_key)
    {
      int ret = OB_SUCCESS;
      int64_t size = rowkey_info.get_size();
      ObObj *array = NULL;
      bool is_binary_rowkey = false;

      if ( NULL == (array = reinterpret_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * size))) )
      {
        TBSYS_LOG(WARN, "allocate memory for rowkey's object array failed, size=%ld", size);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = get_rowkey_compatible(buf, data_len, pos,
              rowkey_info, array, size, is_binary_rowkey)))
      {
        TBSYS_LOG(WARN, "get binary rowkey and translate to ObRowkey failed=%d", ret);
      }
      else if (!is_binary_rowkey)
      {
        TBSYS_LOG(WARN, "unexpect rowkey format, not binary rowkey?");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        row_key.assign(array, size);
      }
      return ret;
    }

    int64_t ObGetParam::get_serialize_rowkey_size(const ObRowkey& row_key) const
    {
      int64_t total_size = 0;

      if (row_key.length() > 0 && NULL != row_key.ptr())
      {
        total_size += get_obj_serialize_size(ObActionFlag::ROW_KEY_FIELD, true);
        total_size += get_rowkey_obj_array_size(row_key.get_obj_ptr(), row_key.get_obj_cnt());
      }
      else
      {
        TBSYS_LOG(WARN, "row key and id is invalid, row key length=%ld",
          row_key.length());
      }

      return total_size;
    }

    bool ObGetParam::is_table_change(ObCellInfo& cell, ObString& table_name,
      uint64_t& table_id) const
    {
      bool table_change = false;

      /**
       * WARNING:this function assume that one of table name or table
       * id in cell is legal at least, so we don't check them here,
       * generally add_cell can ensure the table name and table id is
       * legal
       */

      //check table name first
      if (cell.table_name_.length() > 0 && NULL != cell.table_name_.ptr()
        && cell.table_name_ != table_name)
      {
        table_name = cell.table_name_;
        table_change = true;
      }
      else
      {
        //check table id
        if (cell.table_id_ != 0 && cell.table_id_ != OB_INVALID_ID
          && cell.table_id_ != table_id)
        {
          table_id = cell.table_id_;
          table_change = true;
        }
        else
        {
          table_change = false;
        }
      }

      return table_change;
    }

    bool ObGetParam::is_rowkey_change(ObCellInfo& cell, ObRowkey& row_key) const
    {
      bool row_change = false;

      /**
       * we assume row key is legal, add_cell can ensure this
       * assumption
       */
      if (cell.row_key_ != row_key)
      {
        row_key = cell.row_key_;
        row_change = true;
      }
      else
      {
        row_change = false;
      }

      return row_change;
    }

    int ObGetParam::serialize_cells(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret                 = OB_SUCCESS;
      ObCellInfo* cell        = NULL;
      uint64_t last_table_id  = OB_INVALID_ID;
      bool table_change       = false;
      ObString last_table_name;
      ObRowkey last_row_key;
      last_table_name.assign(NULL, 0);
      last_row_key.assign(NULL, 0);

      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld",
          buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      for (int i = 0; i < cell_size_ && OB_SUCCESS == ret; i++)
      {
        cell = &cell_list_[i];

        //serialize table info if necessary
        if (OB_SUCCESS == ret
          && is_table_change(*cell, last_table_name, last_table_id))
        {
          ret = serialize_table_info(buf, buf_len, pos,
            last_table_name, last_table_id);
          table_change = true;
        }

        //serialize row key if necessary
        if (OB_SUCCESS == ret
          && (is_rowkey_change(*cell, last_row_key) || table_change))
        {
          if (table_change)
          {
            table_change = false;
          }
          ret = serialize_rowkey(buf, buf_len, pos, last_row_key);
        }

        //serialzie column info
        if (OB_SUCCESS == ret)
        {
          ret = serialize_column_info(buf, buf_len, pos,
            cell->column_name_, cell->column_id_);
        }
        //add lbzhong [Update rowkey] 20160420:b
        if(OB_SUCCESS == ret && has_new_row_)
        {
          ObObj obj;
          obj.set_bool(cell->is_new_row_);
          ret = obj.serialize(buf, buf_len, pos);
        }
        //add:e
      }

      return ret;
    }

    int64_t ObGetParam::get_cells_serialize_size(void) const
    {
      int64_t total_size      = 0;
      ObCellInfo* cell        = NULL;
      uint64_t last_table_id  = OB_INVALID_ID;
      bool table_change       = false;
      ObString last_table_name;
      ObRowkey last_row_key;
      last_table_name.assign(NULL, 0);
      last_row_key.assign(NULL, 0);

      for (int i = 0; i < cell_size_; i++)
      {
        cell = &cell_list_[i];

        //add table info serialize size if necessary
        if (is_table_change(*cell, last_table_name, last_table_id))
        {
          total_size += get_serialize_table_info_size(last_table_name, last_table_id);
          table_change = true;
        }

        //add rowkey serialize size if necessary
        if (is_rowkey_change(*cell, last_row_key) || table_change)
        {
          if (table_change)
          {
            table_change = false;
          }
          total_size += get_serialize_rowkey_size(last_row_key);
        }

        total_size += get_serialize_column_info_size(cell->column_name_,
          cell->column_id_);

        //add lbzhong [Update rowkey] 20160420:b
        if(has_new_row_)
        {
          ObObj obj;
          obj.set_bool(cell->is_new_row_);
          total_size += obj.get_serialize_size();
        }
        //add:e
      }

      return total_size;
    }

    DEFINE_SERIALIZE(ObGetParam)
    {
      int ret = OB_SUCCESS;

      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld",
          buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      //serialize reserve parameter field
      if (OB_SUCCESS == ret)
      {
        ret = ObReadParam::serialize_reserve_param(buf, buf_len, pos);
      }

      //serialize basic parameter field
      if (OB_SUCCESS == ret)
      {
        ret = serialize_basic_field(buf, buf_len, pos);
      }

      //serialize table param field
      if (OB_SUCCESS == ret)
      {
        ret = serialize_flag(buf, buf_len, pos, ObActionFlag::TABLE_PARAM_FIELD);
      }

      //add lbzhong [Update rowkey] 20160509:b
      if (OB_SUCCESS == ret && has_new_row_)
      {
        ret = serialize_flag(buf, buf_len, pos, ObActionFlag::HAS_NEW_ROW_FIELD);
      }
      //add:e

      //serialize cells info
      if (OB_SUCCESS == ret)
      {
        ret = serialize_cells(buf, buf_len, pos);
      }

      //serialize end parameter field
      if (OB_SUCCESS == ret)
      {
        ret = serialize_flag(buf, buf_len, pos, ObActionFlag::END_PARAM_FIELD);
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObGetParam)
    {
      int ret                 = OB_SUCCESS;
      uint64_t last_table_id  = OB_INVALID_ID;
      bool read_table_param   = false;
      bool read_column        = false;
      bool end_flag           = false;
      int64_t ext_val         = -1;
      ObObjType type          = ObNullType;
      ObCellInfo cell;
      ObObj obj;
      ObString last_table_name;
      ObString last_binary_rowkey;
      ObRowkeyInfo rowkey_info;
      ObRowkey last_row_key;
      last_table_name.assign(NULL, 0);
      last_row_key.assign(NULL, 0);

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
            case ObActionFlag::RESERVE_PARAM_FIELD:
              /**
               * NOTE : this function will deserialize the next one obj(1),
               * and these obj is read master the data is in the parent class for
               * protocol compatibility
               */
              ret = ObReadParam::deserialize_reserve_param(buf, data_len, pos);
              break;
            case ObActionFlag::BASIC_PARAM_FIELD:
              /**
               * NOTE: this funtion will deserialize the next several objs(4),
               * and these objs must be existent, otherwise error happen
               */
              ret = deserialize_basic_field(buf, data_len, pos);
              break;
            case ObActionFlag::TABLE_PARAM_FIELD:
              read_table_param = true;
              break;
            //add lbzhong [Update rowkey] 20160509:b
            case ObActionFlag::HAS_NEW_ROW_FIELD:
              has_new_row_ = true;
              break;
            //add:e
            case ObActionFlag::TABLE_NAME_FIELD:
              if (read_table_param)
              {
                /**
                 * NOTE: this function will read the next one obj, and the obj
                 * must be existent and it's table name or table id, if the
                 * following obj is row key or column name, we can't know this
                 * division
                 */
                ret = deserialize_table_info(buf, data_len, pos,
                  last_table_name, last_table_id);
                if (OB_SUCCESS == ret)
                {
                  //the next obj is must be row key, so can't set read colum flag
                  read_column = false;
                }
              }
              break;
            case ObActionFlag::ROW_KEY_FIELD:
              if (read_table_param)
              {
                get_rowkey_info_from_sm(schema_manager_, last_table_id, last_table_name, rowkey_info);
                /**
                 * NOTE: this function will read the next one obj, and the obj
                 * must be existent and it's row key, if the following obj is
                 * column name not row key, we can't know this division
                 */
                ret = deserialize_binary_rowkey(buf, data_len, pos, rowkey_info, last_row_key);
                if (OB_SUCCESS == ret)
                {
                  //only after rowkey can read column info
                  read_column = true;
                  is_binary_rowkey_format_ = true;
                }
              }
              break;
            case ObActionFlag::FORMED_ROW_KEY_FIELD:
              if (read_table_param)
              {
                /**
                 * NOTE: this function will read the next one obj, and the obj
                 * must be existent and it's row key, if the following obj is
                 * column name not row key, we can't know this division
                 */
                ret = deserialize_rowkey(buf, data_len, pos, last_row_key);
                if (OB_SUCCESS == ret)
                {
                  //only after rowkey can read column info
                  read_column = true;
                  is_binary_rowkey_format_ = false;
                }
              }
              break;
            case ObActionFlag::END_PARAM_FIELD:
              end_flag = true;
              read_table_param = false;
              break;
            default:
              ret = OB_ERROR;//add liumz, [set ret value, otherwise coredump may happens]20161219
              TBSYS_LOG(WARN, "unkonwn extend field, extend=%ld", ext_val);
              break;
            }
          }
          else if (obj.is_valid_type())
          {
            if (read_column && (ObVarcharType == type || ObIntType == type
                                || (has_new_row_ && ObBoolType == type) //add lbzhong [Update rowkey] 20160420:b:e
                                ))
            {
              //read column
              //add lbzhong [Update rowkey] 20160420:b
              if(!has_new_row_)
              {
              //add:e
                cell.reset();
              } //add lbzhong [Update rowkey] 20160509:b:e //end if
              if (ObVarcharType == type)
              {
                obj.get_varchar(cell.column_name_);
              }
              else if (ObIntType == type)
              {
                obj.get_int(reinterpret_cast<int64_t&>(cell.column_id_));
              }
              //add lbzhong [Update rowkey] 20160420:b
              else if(has_new_row_ && ObBoolType == type)
              {
                obj.get_bool(reinterpret_cast<bool&>(cell.is_new_row_));
              }
              //add:e
              else
              {
                TBSYS_LOG(WARN, "invalid object type for deserialize column "
                  "name or column id, expect ObVarcharType or "
                  "ObIntType, but get type=%d", type);
                ret = OB_ERROR;
              }

              if (OB_SUCCESS == ret
                  && (!has_new_row_ || (has_new_row_ && ObBoolType == type)) //add lbzhong [Update rowkey] 20160420:b:e
                  )
              {
                cell.table_name_ = last_table_name;
                cell.table_id_ = last_table_id;
                cell.row_key_ = last_row_key;
                ret = add_cell(cell);
                //add lbzhong [Update rowkey] 20160420:b
                if(has_new_row_)
                {
                  cell.reset();
                }
                //add:e
              }
            }
            else
            {
              ret = OB_ERROR;//add liumz, [set ret value, otherwise coredump may happens]20161219
              TBSYS_LOG(INFO, "unkonwn object, type=%d", type);
            }
          }
          else
          {
            TBSYS_LOG(WARN, "wrong object type=%d", type);
            ret = OB_ERROR;
          }
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObGetParam)
    {
      int64_t total_size = 0;

      total_size += get_basic_field_serialize_size();
      total_size += ObReadParam::get_reserve_param_serialize_size();
      total_size += get_obj_serialize_size(ObActionFlag::TABLE_PARAM_FIELD, true);
      //add lbzhong [Update rowkey] 20160509:b
      if(has_new_row_)
      {
        total_size += get_obj_serialize_size(ObActionFlag::HAS_NEW_ROW_FIELD, true);
      }
      //add:e
      total_size += get_cells_serialize_size();
      total_size += get_obj_serialize_size(ObActionFlag::END_PARAM_FIELD, true);

      return total_size;
    }
  } /* common */
} /* oceanbase */
