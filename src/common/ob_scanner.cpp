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
 */

#include "ob_scanner.h"

#include <new>

#include "tblog.h"
#include "ob_malloc.h"
#include "ob_define.h"
#include "serialization.h"
#include "ob_action_flag.h"
#include "ob_schema.h"
#include "ob_rowkey_helper.h"
#include "utility.h"

using namespace oceanbase::common;

ObScanner::TableReader::TableReader()
{
  cur_table_id_ = OB_INVALID_ID;
  cur_table_name_.assign(NULL, 0);
  id_name_type_ = UNKNOWN;
  is_row_changed_ = false;
}

ObScanner::TableReader::~TableReader()
{
}

int ObScanner::TableReader::read_cell(const ObSchemaManagerV2* schema_manager, const char *buf, 
    const int64_t data_len, int64_t &pos, ObCellInfo &cell_info, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  bool get_column = false;
  int64_t next_pos = pos;

  is_row_changed_ = false;

  if (NULL == buf || 0 >= data_len)
  {
    TBSYS_LOG(WARN, "invalid arguments, buf=%p data_len=%ld pos=%ld", buf, data_len, pos);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (pos >= data_len)
  {
    ret = OB_ITER_END;
  }
  else
  {
    // deserialize table name or table id if existed
    ret = last_obj.deserialize(buf, data_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "deserialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, data_len, next_pos);
    }
    else
    {
      if (ObExtendType == last_obj.get_type() && ObActionFlag::TABLE_NAME_FIELD == last_obj.get_ext())
      {
        ret = ObScanner::deserialize_int_or_varchar_(buf, data_len, next_pos,
                                                     reinterpret_cast<int64_t&>(cur_table_id_), cur_table_name_, last_obj);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize_int_or_varchar_ error, ret=%d", ret);
        }
        else
        {
          if (cur_table_name_.length() > 0 && NULL != cur_table_name_.ptr())
          {
            if (ID == id_name_type_)
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=NAME before=ID");
            }
            else
            {
              id_name_type_ = NAME;
            }
          }
          else if (OB_INVALID_ID != cur_table_id_)
          {
            if (NAME == id_name_type_)
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=ID before=NAME");
            }
            else
            {
              id_name_type_ = ID;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "Table Name and Table ID are both invalid when deserializing");
          }
        }

        if (OB_SUCCESS == ret)
        {
          is_row_changed_ = true;
          ret = last_obj.deserialize(buf, data_len, next_pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "deserialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                      ret, buf, data_len, next_pos);
          }
        }
      }
    }

    // deserialize row key if existed
    if (OB_SUCCESS == ret)
    {
      if (ObExtendType == last_obj.get_type() 
          && (ObActionFlag::ROW_KEY_FIELD == last_obj.get_ext() 
            || ObActionFlag::FORMED_ROW_KEY_FIELD == last_obj.get_ext()))
      {
        int64_t size = OB_MAX_ROWKEY_COLUMN_NUMBER;
        if (ObActionFlag::ROW_KEY_FIELD == last_obj.get_ext())
        {
          ObString binary_rowkey;
          ObRowkeyInfo rowkey_info ;
          bool is_binary_rowkey = false;
          if (OB_SUCCESS != (ret = get_rowkey_info_from_sm(schema_manager, 
                  cur_table_id_, cur_table_name_, rowkey_info)))
          {
            TBSYS_LOG(WARN, "deserialize binray rowkey, but not set compatibility schema,"
                "cur_table_name_=%.*s, cur_table_id_=%ld, schema_manager=%p",
                cur_table_name_.length(), cur_table_name_.ptr(), cur_table_id_, schema_manager);
          }
          else if (OB_SUCCESS != (ret = get_rowkey_compatible(
                  buf, data_len, next_pos, rowkey_info, 
                  cur_rowkey_obj_array_, size, is_binary_rowkey)))
          {
            TBSYS_LOG(WARN, "cannot translate binary rowkey to ObRowkey, rowkey info size=%ld", 
                rowkey_info.get_size());
          }
          else
          {
            size = rowkey_info.get_size();
          }
        }
        else if (ObActionFlag::FORMED_ROW_KEY_FIELD == last_obj.get_ext())
        {
          ret = get_rowkey_obj_array(buf, data_len, next_pos, cur_rowkey_obj_array_, size);
        }

        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize rowkey, ret=%d, last_obj type=%ld", ret, last_obj.get_ext());
        }
        else
        {
          if (size == 0)
          {
            cur_row_key_.assign(NULL, 0);
          }
          else
          {
            cur_row_key_.assign(cur_rowkey_obj_array_, size);
          }
          is_row_changed_ = true;

          ret = last_obj.deserialize(buf, data_len, next_pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "deserialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                      ret, buf, data_len, next_pos);
          }
        }
      }
    }

    //deserialize column
    if (OB_SUCCESS == ret)
    {
      if (ObExtendType == last_obj.get_type()
          && (ObActionFlag::OP_ROW_DOES_NOT_EXIST == last_obj.get_ext()
              || ObActionFlag::OP_NOP == last_obj.get_ext()
              || ObActionFlag::OP_DEL_ROW == last_obj.get_ext()
              || ObActionFlag::OP_DEL_TABLE == last_obj.get_ext()))
      {
        cell_info.value_.set_ext(last_obj.get_ext());
        get_column = true;
      }
      else if (ObIntType == last_obj.get_type() || ObVarcharType == last_obj.get_type())
      {
        if (ObIntType == last_obj.get_type())
        {
          ret = last_obj.get_int(reinterpret_cast<int64_t&>(cell_info.column_id_));
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "deserialize column id error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                      ret, buf, data_len, next_pos);
          }
          /*
          else if (OB_INVALID_ID == cell_info.column_id_)
          {
            TBSYS_LOG(WARN, "deserialized column id is invalid");
            ret = OB_ERROR;
          }
          */
          else
          {
            if (NAME == id_name_type_)
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=ID before=NAME");
            }
            else
            {
              id_name_type_ = ID;
            }
          }
        }
        else
        {
          ret = last_obj.get_varchar(cell_info.column_name_);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "deserialize column name error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                      ret, buf, data_len, next_pos);
          }
          else if (NULL == cell_info.column_name_.ptr() || 0 == cell_info.column_name_.length())
          {
            TBSYS_LOG(WARN, "deserialized column name is invalid");
          }
          else
          {
            if (ID == id_name_type_)
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=NAME before=ID");
            }
            else
            {
              id_name_type_ = NAME;
            }
          }
        }
        if (OB_SUCCESS == ret)
        {
          ret = last_obj.deserialize(buf, data_len, next_pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "deserialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                      ret, buf, data_len, next_pos);
          }
          else
          {
            cell_info.value_ = last_obj;
            get_column = true;
          }
        }
      }
    }

    if (OB_SUCCESS == ret && get_column)
    {
      cell_info.table_name_ = cur_table_name_;
      cell_info.table_id_ = cur_table_id_;
      cell_info.row_key_ = cur_row_key_;
    }

    if (OB_SUCCESS == ret)
    {
      pos = next_pos;
    }

    if (OB_SUCCESS == ret && !get_column)
    {
      // the current obobj can not be recognized
      ret = OB_UNKNOWN_OBJ;
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

ObScanner::Iterator::Iterator() : scanner_(NULL), cur_mem_block_(NULL),
cur_pos_(0),next_pos_(0), iter_size_counter_(0),
cur_cell_info_(), new_row_cell_(false), is_end_(false)
{
}

ObScanner::Iterator::~Iterator()
{
  scanner_ = NULL;
  cur_mem_block_ = NULL;
  cur_pos_ = 0;
  next_pos_ = 0;
  iter_size_counter_ = 0;
}

ObScanner::Iterator::Iterator(const ObScanner::Iterator &other)
{
  *this = other;
}

ObScanner::Iterator::Iterator(const ObScanner *scanner,
                              const ObScanner::MemBlock *mem_block,
                              int64_t size_counter, int64_t cur_pos) : scanner_(scanner), cur_mem_block_(mem_block),
cur_pos_(cur_pos),next_pos_(cur_pos), iter_size_counter_(size_counter),
cur_cell_info_(), new_row_cell_(false), is_end_(false)
{
  if (!is_iterator_end_())
  {
    if (OB_SUCCESS != get_cell_(cur_cell_info_, next_pos_))
    {
      TBSYS_LOG(ERROR, "get cell fail next_pos=%ld cur_pos=%ld", next_pos_, cur_pos_);
    }
  }
}

void ObScanner::Iterator::update_cur_mem_block_()
{
  if (iter_size_counter_ < scanner_->cur_size_counter_)
  {
    iter_size_counter_ += (next_pos_ - cur_pos_);
  }
  if (NULL != cur_mem_block_
      && next_pos_ >= cur_mem_block_->cur_pos
      && NULL != scanner_
      && iter_size_counter_ <= scanner_->cur_size_counter_
      && NULL != cur_mem_block_->next)
  {
    cur_mem_block_ = cur_mem_block_->next;
    next_pos_ = 0;
  }
}

ObScanner::Iterator &ObScanner::Iterator::operator ++ ()
{
  update_cur_mem_block_();
  int64_t next_pos = next_pos_;
  if (!is_end_ && iter_size_counter_ < scanner_->cur_size_counter_ 
      && OB_SUCCESS != get_cell_(cur_cell_info_, next_pos_))
  {
    TBSYS_LOG(ERROR, "get cell fail next_pos=%ld cur_pos=%ld", next_pos, cur_pos_);
  }
  else
  {
    cur_pos_ = next_pos;
  }
  return *this;
}

ObScanner::Iterator ObScanner::Iterator::operator ++ (int)
{
  ObScanner::Iterator ret = *this;
  ++ *this;
  return ret;
}

bool ObScanner::Iterator::operator == (const ObScanner::Iterator &other) const
{
  bool bret = false;
  if (other.scanner_ == scanner_
      && other.cur_mem_block_ == cur_mem_block_
      && other.cur_pos_ == cur_pos_
      && other.iter_size_counter_ == iter_size_counter_)
  {
    bret = true;
  }
  return bret;
}

bool ObScanner::Iterator::operator != (const ObScanner::Iterator &other) const
{
  return !(*this == other);
}

ObScanner::Iterator &ObScanner::Iterator::operator = (const ObScanner::Iterator &other)
{
  scanner_ = other.scanner_;
  cur_mem_block_ = other.cur_mem_block_;
  cur_pos_ = other.cur_pos_;
  iter_size_counter_ = other.iter_size_counter_;
  reader_ = other.reader_;
  next_pos_ = other.next_pos_;
  cur_cell_info_ = other.cur_cell_info_;
  cur_cell_info_.row_key_ = reader_.get_cur_row_key();
  new_row_cell_ = other.new_row_cell_;
  is_end_ = other.is_end_;
  return *this;
}

int ObScanner::Iterator::get_cell(ObCellInfo &cell_info)
{
  ///int64_t next_pos = cur_pos_;
  ///return get_cell_(cell_info, next_pos);
  ObCellInfo *cell_out = NULL;
  int err = get_cell(&cell_out, NULL);
  if (NULL != cell_out)
  {
    cell_info = *cell_out;
  }
  return err;
}

int ObScanner::Iterator::get_cell(ObCellInfo **cell_info, bool *is_row_changed/* = NULL*/)
{
  int err = OB_SUCCESS;
  if (NULL == cell_info)
  {
    err  = OB_INVALID_ARGUMENT;
  }
  else if (is_iterator_end_())
  {
    err = OB_ITER_END;
  }
  else
  {
    *cell_info = &cur_cell_info_;
    if (NULL != is_row_changed)
    {
      *is_row_changed = new_row_cell_;
    }
  }
  return err;
}

int ObScanner::Iterator::get_cell_(ObCellInfo &cell_info, int64_t &next_pos)
{
  int ret = OB_SUCCESS;
  if (NULL == scanner_
      || NULL == cur_mem_block_
      || iter_size_counter_ >= scanner_->cur_size_counter_
      || is_end_)
  {
    ret = OB_ITER_END;
  }
  else
  {
    ret = get_cell_(cur_mem_block_->memory, cur_mem_block_->cur_pos, next_pos, cell_info, next_pos);
  }
  return ret;
}

int ObScanner::Iterator::get_cell_(const char *data, const int64_t data_len, const int64_t cur_pos,
                                   ObCellInfo &cell_info, int64_t &next_pos)
{
  int ret = OB_SUCCESS;
  next_pos = cur_pos;
  if (NULL == data || 0 >= data_len)
  {
    ret = OB_ERROR;
  }
  else
  {
    ObObj tmp_obj;
    ret = reader_.read_cell(scanner_->schema_manager_, data, data_len, next_pos, cell_info, tmp_obj);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "read cell failed:ret[%d], len[%ld], pos[%ld]", ret, data_len, next_pos);
      is_end_ = true;
    }
    else
    {
      new_row_cell_ = reader_.is_row_changed();
    }
  }
  return ret;
}

ObScanner::RowIterator::RowIterator() : ObScanner::Iterator()
{
  row_cell_num_ = 0;
  row_start_pos_ = 0;
  row_iter_size_counter_ = 0;
}

ObScanner::RowIterator::~RowIterator()
{
}

ObScanner::RowIterator::RowIterator(const RowIterator &other)
  : ObScanner::Iterator(other)
{
  for (int64_t i = 0; i < OB_MAX_COLUMN_NUMBER; i++)
  {
    cur_row_[i] = other.cur_row_[i];
  }
  row_cell_num_ = other.row_cell_num_;
  row_start_pos_ = other.row_start_pos_;
  row_iter_size_counter_ = other.row_iter_size_counter_;
}

ObScanner::RowIterator::RowIterator(const ObScanner *scanner,
    const ObScanner::MemBlock *mem_block, int64_t size_counter/* = 0*/,
    int64_t cur_pos/* = 0*/)
  : ObScanner::Iterator(scanner, mem_block, size_counter, cur_pos)
{
  row_cell_num_ = 0;
  row_start_pos_ = cur_pos;
  row_iter_size_counter_ = size_counter;
  if (!is_iterator_end_())
  {
    if (OB_SUCCESS != get_row_())
    {
      TBSYS_LOG(ERROR, "get cell fail next_pos=%ld cur_pos=%ld", next_pos_, cur_pos_);
    }
  }
}

ObScanner::RowIterator &ObScanner::RowIterator::operator ++ ()
{
  row_iter_size_counter_ += cur_pos_ - row_start_pos_;
  row_start_pos_ = cur_pos_;
  int err = get_row_();
  if (OB_SUCCESS != err && OB_ITER_END != err)
  {
    TBSYS_LOG(ERROR, "get_row_ returned error, err=%d", err);
  }
  return *this;
}

ObScanner::RowIterator ObScanner::RowIterator::operator ++ (int)
{
  ObScanner::RowIterator ret = *this;
  ++ *this;
  return ret;
}

bool ObScanner::RowIterator::operator == (const ObScanner::RowIterator &other) const
{
  bool bret = false;
  if (other.scanner_ == scanner_
      && other.cur_mem_block_ == cur_mem_block_
      && other.row_start_pos_ == row_start_pos_
      && other.row_iter_size_counter_ == row_iter_size_counter_)
  {
    bret = true;
  }
  return bret;
}

bool ObScanner::RowIterator::operator != (const ObScanner::RowIterator &other) const
{
  return !(*this == other);
}

ObScanner::RowIterator &ObScanner::RowIterator::operator = (const ObScanner::RowIterator &other)
{
  ObScanner::Iterator::operator=(other);
  for (int64_t i = 0; i < OB_MAX_COLUMN_NUMBER; i++)
  {
    cur_row_[i] = other.cur_row_[i];
  }
  row_cell_num_ = other.row_cell_num_;
  row_start_pos_ = other.row_start_pos_;
  row_iter_size_counter_ = other.row_iter_size_counter_;
  return *this;
}

int ObScanner::RowIterator::get_row(ObCellInfo **cell_info, int64_t *num)
{
  int err = OB_SUCCESS;
  if (is_iterator_end_())
  {
    err = OB_ITER_END;
  }
  if (OB_SUCCESS == err && NULL == cell_info)
  {
    err  = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    *cell_info = cur_row_;
    *num = row_cell_num_;
  }
  return err;
}

bool ObScanner::RowIterator::is_iterator_end_()
{
  return(NULL == scanner_
         || 0 == scanner_->cur_size_counter_
         || row_iter_size_counter_ >= scanner_->cur_size_counter_);
}

int ObScanner::RowIterator::get_row_()
{
  int err = OB_SUCCESS;
  row_cell_num_ = 0;
  bool row_changed = false;
  ObCellInfo *cell;
  err = get_cell(&cell, &row_changed);
  if (OB_SUCCESS != err && OB_ITER_END != err)
  {
    TBSYS_LOG(ERROR, "ObScanner::Iterator get_cell returned error, "
        "err=%d cell=%p row_changed=%s",
        err, cell, row_changed?"true":"false");
  }
  else if (OB_ITER_END == err)
  {
    TBSYS_LOG(DEBUG, "ObScanner::Iterator get_cell returned iteration end");
    err = OB_SUCCESS;
  }
  else
  {
    cur_row_[0] = *cell;
    row_cell_num_ = 1;
    // copy rowkey (not deep copy)
    int64_t rowkey_obj_cnt = cell->row_key_.get_obj_cnt();
    for (int64_t i = 0; i < rowkey_obj_cnt; ++i)
    {
      cur_rowkey_objs_[i] = cell->row_key_.get_obj_ptr()[i];
    }
    cur_row_[0].row_key_.assign(cur_rowkey_objs_, rowkey_obj_cnt);
    ++(*((ObScanner::Iterator*)this));
    row_changed = false;
    while (!row_changed)
    {
      err = get_cell(&cell, &row_changed);
      if (OB_SUCCESS != err && OB_ITER_END != err)
      {
        TBSYS_LOG(ERROR, "ObScanner::Iterator get_cell returned error, "
            "err=%d cell=%p row_changed=%s",
            err, cell, row_changed?"true":"false");
      }
      else if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
        break;
      }
      else if (!row_changed)
      {
        cur_row_[row_cell_num_] = *cell;
        cur_row_[row_cell_num_].row_key_.assign(cur_rowkey_objs_, 
                                                cur_row_[row_cell_num_].row_key_.get_obj_cnt());
        ++row_cell_num_;
        ++(*((ObScanner::Iterator*)this));
        //row_start_pos_ = next_pos_;
        //err = get_cell_(cur_cell_info_, next_pos_);
        //if (OB_SUCCESS != err)
        //{
        //  TBSYS_LOG(ERROR, "get_cell_ error, err=%d next_pos_=%ld",
        //      err, next_pos_);
        //}
        //else
        //{
        //}
      }
    }
  }
  return err;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObScanner::ObScanner() : head_memblock_(NULL), cur_memblock_(NULL), rollback_memblock_(NULL),
cur_size_counter_(0), rollback_size_counter_(0), cur_table_name_(), cur_table_id_(OB_INVALID_ID), cur_row_key_(),
tmp_table_name_(), tmp_table_id_(OB_INVALID_ID), tmp_row_key_(),
mem_size_limit_(DEFAULT_MAX_SERIALIZE_SIZE), iter_(), row_iter_(),
first_next_(false), first_next_row_(false), schema_manager_(NULL) ,is_binary_rowkey_format_(false),
mod_(ObModIds::OB_SCANNER), rowkey_allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_)
{
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_item_num_ = 0;
  id_name_type_ = UNKNOWN;
  has_row_key_flag_ = false;
  cur_row_num_ = 0;
  cur_cell_num_ = 0;
  last_row_key_.assign(NULL, 0);
  rollback_row_num_ = 0;
  rollback_cell_num_ = 0;
  rollback_row_key_.assign(NULL, 0);
  whole_result_row_num_ = -1;
}

ObScanner::~ObScanner()
{
  MemBlock *iter = head_memblock_;
  while (NULL != iter)
  {
    MemBlock *tmp = iter->next;
    ob_free(iter);
    iter = tmp;
  }
}

void ObScanner::reset()
{
  if (NULL != head_memblock_)
  {
    MemBlock *iter = head_memblock_->next;
    while (NULL != iter)
    {
      MemBlock *tmp = iter->next;
      ob_free(iter);
      iter = tmp;
    }
    head_memblock_->next = NULL;
    head_memblock_->reset();
  }
  cur_memblock_ = head_memblock_;
  rollback_memblock_ = cur_memblock_;
  cur_size_counter_ = 0;
  rollback_size_counter_ = 0;
  cur_table_name_.assign(NULL, 0);
  cur_table_id_ = OB_INVALID_ID;
  cur_row_key_.assign(NULL, 0);
  tmp_table_name_.assign(NULL, 0);
  tmp_table_id_ = OB_INVALID_ID;
  tmp_row_key_.assign(NULL, 0);
  iter_ = begin();
  row_iter_ = row_begin();
  first_next_ = false;
  first_next_row_ = false;
  rowkey_allocator_.free();
  cur_row_num_ = 0;
  cur_cell_num_ = 0;
  last_row_key_.assign(NULL, 0);
  rollback_row_num_ = 0;
  rollback_cell_num_ = 0;
  rollback_row_key_.assign(NULL, 0);
  range_.reset();
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_item_num_ = 0;
  id_name_type_ = UNKNOWN;
  has_row_key_flag_ = false;
  schema_manager_ = NULL;
  is_binary_rowkey_format_ = false;
  whole_result_row_num_ = -1;
}

void ObScanner::clear()
{
  if (NULL != head_memblock_)
  {
    MemBlock *iter = head_memblock_;
    while (NULL != iter)
    {
      MemBlock *tmp = iter->next;
      ob_free(iter);
      iter = tmp;
    }
    head_memblock_ = NULL;
  }
  cur_memblock_ = head_memblock_;
  rollback_memblock_ = cur_memblock_;
  cur_size_counter_ = 0;
  rollback_size_counter_ = 0;
  cur_table_name_.assign(NULL, 0);
  cur_table_id_ = OB_INVALID_ID;
  cur_row_key_.assign(NULL, 0);
  tmp_table_name_.assign(NULL, 0);
  tmp_table_id_ = OB_INVALID_ID;
  tmp_row_key_.assign(NULL, 0);
  iter_ = begin();
  row_iter_ = row_begin();
  first_next_ = false;
  first_next_row_ = false;
  rowkey_allocator_.free();

  range_.reset();
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_item_num_ = 0;
  id_name_type_ = UNKNOWN;
  last_row_key_.assign(NULL, 0);
  id_name_type_ = UNKNOWN;
  has_row_key_flag_ = false;

  cur_row_num_ = 0;
  cur_cell_num_ = 0;
  last_row_key_.assign(NULL, 0);
  rollback_row_num_ = 0;
  rollback_cell_num_ = 0;
  rollback_row_key_.assign(NULL, 0);
  whole_result_row_num_ = -1;
}

int64_t ObScanner::set_mem_size_limit(const int64_t limit)
{
  if (0 > limit
      || OB_MAX_PACKET_LENGTH < limit)
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

int ObScanner::add_cell(const ObCellInfo &cell_info, const bool is_compare_rowkey,
    const bool is_row_changed)
{
  int ret = OB_SUCCESS;

  bool has_add = false;
  if (NULL == cur_memblock_)
  {
    ret = get_memblock_(DEFAULT_MAX_SERIALIZE_SIZE);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get_memblock_ error, ret=%d", ret);
    }
  }
  else
  {
    ret = append_serialize_(cell_info, is_compare_rowkey, is_row_changed);
    if (OB_SUCCESS != ret && OB_SIZE_OVERFLOW != ret)
    {
      ret = get_memblock_(DEFAULT_MAX_SERIALIZE_SIZE);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get_memblock_ error, ret=%d", ret);
      }
    }
    else
    {
      has_add = true;
    }
  }

  if (OB_SUCCESS == ret && !has_add)
  {
    ret = append_serialize_(cell_info, is_compare_rowkey, is_row_changed);
    if (OB_SIZE_OVERFLOW == ret)
    {
      TBSYS_LOG(DEBUG, "exceed the memory limit");
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "append_serialize_ error, cell_info=%s, ret=%d", print_cellinfo(&cell_info), ret);
    }
  }

  return ret;
}

int ObScanner::rollback()
{
  int ret = OB_SUCCESS;
  if (NULL == rollback_memblock_)
  {
    ret = OB_SUCCESS;
  }
  else
  {
    rollback_memblock_->cur_pos = rollback_memblock_->rollback_pos;
    cur_memblock_ = rollback_memblock_;
    cur_size_counter_ = rollback_size_counter_;
    cur_row_num_ = rollback_row_num_;
    cur_cell_num_ = rollback_cell_num_;
    last_row_key_ = rollback_row_key_;
  }
  return ret;
}

int ObScanner::serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const
{
  ObObj obj;
  obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
  int ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
        ret, buf, buf_len, pos);
  }

  if (OB_SUCCESS == ret)
  {
    obj.set_int(is_request_fullfilled_ ? 1 : 0);
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
          ret, buf, buf_len, pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    obj.set_int(fullfilled_item_num_);
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
          ret, buf, buf_len, pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    obj.set_int(data_version_);
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
          ret, buf, buf_len, pos);
    }
  }

  if (has_range_ && (OB_SUCCESS == ret))
  {
    obj.set_ext(ObActionFlag::TABLET_RANGE_FIELD);
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
          ret, buf, buf_len, pos);
    }

    if (OB_SUCCESS == ret)
    {
      int8_t border_flag = range_.border_flag_.get_data();
      // compatible: old client may send request with min/max borderflag info.
      if (range_.start_key_.is_min_row())
      {
        border_flag = ObBorderFlag::MIN_VALUE | border_flag;
      }
      if (range_.end_key_.is_max_row())
      {
        border_flag = ObBorderFlag::MAX_VALUE& border_flag;
      }
      obj.set_int(border_flag);
      ret = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
            ret, buf, buf_len, pos);
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (!is_binary_rowkey_format_)
      {
        ret = set_rowkey_obj_array(buf, buf_len, pos, 
            range_.start_key_.get_obj_ptr(), range_.start_key_.get_obj_cnt());
      }
      else
      {
        ret = serialize_rowkey_in_binary_format(buf, buf_len, pos, 
            range_.table_id_, ObString(), range_.start_key_);
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (!is_binary_rowkey_format_)
      {
        ret = set_rowkey_obj_array(buf, buf_len, pos, 
            range_.end_key_.get_obj_ptr(), range_.end_key_.get_obj_cnt());
      }
      else
      {
        ret = serialize_rowkey_in_binary_format(buf, buf_len, pos, 
            range_.table_id_, ObString(), range_.end_key_);
      }
    }

  }
  
  if (OB_SUCCESS == ret)
  {
    obj.set_int(whole_result_row_num_);
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
          ret, buf, buf_len, pos);
    }
  }
  return ret;
}

int ObScanner::serialize_meta_param(char * buf, const int64_t buf_len, int64_t & pos) const
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

  // cell count
  if (OB_SUCCESS == ret)
  {
    obj.set_int(cur_cell_num_);
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
    if (!is_binary_rowkey_format_)
    {
      ret = set_rowkey_obj_array(buf, buf_len, pos, 
          last_row_key_.get_obj_ptr(), last_row_key_.get_obj_cnt());
    }
    else
    {
      if (OB_INVALID_ID == cur_table_id_ && cur_table_name_.ptr() == NULL)
      {
        // no any row in scanner
        ObString null_row_key;
        null_row_key.assign(NULL, 0);
        ObObj null_row_obj;
        null_row_obj.set_varchar(null_row_key);
        ret = null_row_obj.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to serialize null_row_obj, buf=%p, buf_len=%ld, pos=%ld, ret=%d",
              buf, buf_len, pos, ret);
        }
      }
      else
      {
        ret = serialize_rowkey_in_binary_format(buf, buf_len, pos, 
            cur_table_id_, cur_table_name_, last_row_key_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to serialize rowkey, table_id=%lu, table_name=%s, last_row_key=%s, ret=%d",
              cur_table_id_, to_cstring(cur_table_name_), to_cstring(last_row_key_), ret);
        }
      }
    }
  }
  return ret;
}

int ObScanner::serialize_table_param(char * buf, const int64_t buf_len, int64_t & pos) const
{
  int ret = OB_SUCCESS;
  if (cur_size_counter_ > 0)
  {
    ObObj obj;
    obj.set_ext(ObActionFlag::TABLE_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
          ret, buf, buf_len, pos);
    }

    if (OB_SUCCESS == ret)
    {
      MemBlock *iter = head_memblock_;
      while (NULL != iter && (pos + iter->cur_pos) < buf_len)
      {
        memcpy(buf + pos, iter->memory, iter->cur_pos);
        pos += iter->cur_pos;
        iter = iter->next;
      }
      if (NULL != iter)
      {
        TBSYS_LOG(WARN, "ObScanner buffer to serialize is not enough, buf_len=%ld", buf_len);
        ret = OB_BUF_NOT_ENOUGH;
      }
    }
  }
  return ret;
}

int ObScanner::serialize_end_param(char * buf, const int64_t buf_len, int64_t & pos) const
{
  ObObj obj;
  obj.set_ext(ObActionFlag::END_PARAM_FIELD);
  int ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
        ret, buf, buf_len, pos);
  }
  return ret;
}

int ObScanner::serialize_rowkey_in_binary_format(char* buf, const int64_t buf_len, int64_t &pos, 
    const uint64_t table_id, const ObString & table_name, const ObRowkey& rowkey) const
{
  int ret = OB_SUCCESS;
  //const ObRowkeyInfo* rowkey_info = &schema_manager_->get_table_schema(table_id)->get_rowkey_info();
  ObRowkeyInfo rowkey_info;
  ObString binary_rowkey;
  ObObj obj;
  int64_t binary_rowkey_size = 0;
  char* buffer = NULL;

  ret = get_rowkey_info_from_sm(schema_manager_, table_id, table_name, rowkey_info);
  if (OB_SUCCESS == ret)
  {
    binary_rowkey_size = rowkey_info.get_binary_rowkey_length();
    buffer = rowkey_allocator_.alloc(binary_rowkey_size);
  }
  else
  {
    TBSYS_LOG(WARN, "cannot get rowkey of binary rowkey table_id=%lu, table_name=%.*s", 
        table_id, table_name.length(), table_name.ptr());
  }

  if (OB_SUCCESS == ret && NULL != buffer)
  {
    binary_rowkey.assign_ptr(buffer, static_cast<int32_t>(binary_rowkey_size));
    if (rowkey.is_empty_row())
    {
      // empty row means rowkey can be any value
      memset(buffer, 0x00, binary_rowkey_size);
    }
    else
    {
      ret = ObRowkeyHelper::obj_array_to_binary_rowkey(rowkey_info, binary_rowkey, rowkey.get_obj_ptr(), rowkey.get_obj_cnt());
      if (0 != ret)
      {
        TBSYS_LOG(WARN, "failed to translate obj array to binary rowkey, rowkey=%s, ret=%d", to_cstring(rowkey), ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    obj.set_varchar(binary_rowkey);
    ret = obj.serialize(buf, buf_len, pos);
    //ret = binary_rowkey.serialize(buf, buf_len, pos);
  }

  return ret;
}

DEFINE_SERIALIZE(ObScanner)
//int ObScanner::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int64_t new_pos = pos;
  int ret = serialize_basic_param(buf, buf_len, new_pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "serialize basic param error, err=%d", ret);
  }
  else
  {
    ret = serialize_meta_param(buf, buf_len, new_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize meta param error, err=%d", ret);
    }
  }
  
  if (OB_SUCCESS == ret)
  {
    ret = serialize_table_param(buf, buf_len, new_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize table param error, err=%d", ret);
    }
  }
  
  if (OB_SUCCESS == ret)
  {
    ret = serialize_end_param(buf, buf_len, new_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize end param error, err=%d", ret);
    }
    else
    {
      pos = new_pos;
    }
  }
  return ret;
}

int64_t ObScanner::get_basic_param_serialize_size(void) const
{
  ObObj obj;
  obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
  int64_t ret = obj.get_serialize_size();

  obj.set_int(is_request_fullfilled_ ? 1 : 0);
  ret += obj.get_serialize_size();

  obj.set_int(fullfilled_item_num_);
  ret += obj.get_serialize_size();

  obj.set_int(data_version_);
  ret += obj.get_serialize_size();

  if (has_range_)
  {
    obj.set_ext(ObActionFlag::TABLET_RANGE_FIELD);
    ret += obj.get_serialize_size();
    {
      int8_t border_flag = range_.border_flag_.get_data();
      // compatible: old client may send request with min/max borderflag info.
      if (range_.start_key_.is_min_row())
      {
        border_flag = ObBorderFlag::MIN_VALUE | border_flag;
      }
      if (range_.end_key_.is_max_row())
      {
        border_flag = ObBorderFlag::MAX_VALUE& border_flag;
      }
      obj.set_int(border_flag);
    }
    ret += obj.get_serialize_size();

    int64_t binary_rowkey_length = 0;
    if (is_binary_rowkey_format_)
    {
      binary_rowkey_length = schema_manager_->get_table_schema(
          range_.table_id_)->get_rowkey_info().get_binary_rowkey_length();
    }

    // start_key_
    if (is_binary_rowkey_format_)
    {
      ret += binary_rowkey_length;
    }
    else
    {
      ret += get_rowkey_obj_array_size(
          range_.start_key_.get_obj_ptr(), range_.start_key_.get_obj_cnt());
    }

    // end_key_
    if (is_binary_rowkey_format_)
    {
      ret += binary_rowkey_length;
    }
    else
    {
      ret += get_rowkey_obj_array_size(
          range_.end_key_.get_obj_ptr(), range_.end_key_.get_obj_cnt());
    }
  }

  obj.set_int(whole_result_row_num_);
  ret += obj.get_serialize_size();

  return ret;
}

int64_t ObScanner::get_meta_param_serialize_size(void) const
{
  ObObj obj;
  obj.set_ext(ObActionFlag::META_PARAM_FIELD);
  int64_t ret = obj.get_serialize_size();

  obj.set_int(cur_row_num_);
  ret += obj.get_serialize_size();

  obj.set_int(cur_cell_num_);
  ret += obj.get_serialize_size();

  if (is_binary_rowkey_format_)
  {
    ret += schema_manager_->get_table_schema(
        range_.table_id_)->get_rowkey_info().get_binary_rowkey_length();
  }
  else
  {
    ret += get_rowkey_obj_array_size(
        last_row_key_.get_obj_ptr(), last_row_key_.get_obj_cnt());
  }

  return ret;
}

int64_t ObScanner::get_table_param_serialize_size(void) const
{
  int64_t ret = 0;
  if (cur_size_counter_ > 0)
  {
    ObObj obj;
    obj.set_ext(ObActionFlag::TABLE_PARAM_FIELD);
    ret += obj.get_serialize_size();
    ret += cur_size_counter_;
  }
  return ret; 
}
 
int64_t ObScanner::get_end_param_serialize_size(void) const
{
  ObObj obj;
  obj.set_ext(ObActionFlag::END_PARAM_FIELD);
  return obj.get_serialize_size();
}

DEFINE_GET_SERIALIZE_SIZE(ObScanner)
//int64_t ObScanner::get_serialize_size(void) const
{
  int64_t ret = get_basic_param_serialize_size();
  ret += get_meta_param_serialize_size();
  ret += get_table_param_serialize_size();
  ret += get_end_param_serialize_size();
  return ret;
}

DEFINE_DESERIALIZE(ObScanner)
//int ObScanner::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
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
          TBSYS_LOG(WARN, "ObObj deserialize error, len=%ld, pos=%ld, ret=%d", data_len, new_pos, ret);
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
            ret = deserialize_basic_param(buf, data_len, new_pos, param_id);
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
              TBSYS_LOG(WARN, "deserialize meta info error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                        ret, buf, data_len, new_pos);
            }
            break;
          case ObActionFlag::TABLE_PARAM_FIELD:
            ret = deserialize_table_param(buf, data_len, new_pos, param_id);
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

int ObScanner::append_serialize_(const ObCellInfo &cell_info, const bool is_compare_rowkey, 
    const bool is_row_changed)
{
  int ret = OB_SUCCESS;

  char* buf = cur_memblock_->memory;
  const int64_t buf_len = cur_memblock_->memory_size;
  int64_t pos = cur_memblock_->cur_pos;
  bool is_serialize_table = false;
  bool is_serialize_row = false;
  ObObj obj;

  tmp_table_name_ = cur_table_name_;
  tmp_table_id_ = cur_table_id_;
  tmp_row_key_ = cur_row_key_;

  // serialize table_name or table_id
  if (OB_SUCCESS == ret)
  {
    if (cell_info.table_name_ != cur_table_name_
        || cell_info.table_id_ != cur_table_id_)
    {
      is_serialize_table = true;
      cur_memblock_->rollback_pos = cur_memblock_->cur_pos;
      rollback_memblock_ = cur_memblock_;
      rollback_size_counter_ = cur_size_counter_;
      rollback_row_num_ = cur_row_num_;
      rollback_cell_num_ = cur_cell_num_;
      rollback_row_key_ = cur_row_key_;
      ret = serialize_table_name_or_id_(buf, buf_len, pos, cell_info);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "serialize_table_name_or_id_ error, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
    }
  }

  // serialize row_key
  if (OB_SUCCESS == ret)
  {
    if (!has_row_key_flag_ || (!is_compare_rowkey && is_row_changed) 
        || (is_compare_rowkey && cell_info.row_key_ != cur_row_key_))
    {
      is_serialize_row = true;
      if (!is_serialize_table)
      {
        cur_memblock_->rollback_pos = cur_memblock_->cur_pos;
        rollback_memblock_ = cur_memblock_;
        rollback_size_counter_ = cur_size_counter_;
        rollback_row_num_ = cur_row_num_;
        rollback_cell_num_ = cur_cell_num_;
        rollback_row_key_ = cur_row_key_;
      }

      ret = serialize_row_key_(buf, buf_len, pos, cell_info);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "serialize_row_key_ error, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
    }
  }

  // serialize column
  if (OB_SUCCESS == ret)
  {
    if (ObExtendType == cell_info.value_.get_type()
        && (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_info.value_.get_ext()
          || ObActionFlag::OP_NOP == cell_info.value_.get_ext()
          || ObActionFlag::OP_DEL_ROW == cell_info.value_.get_ext()
          || ObActionFlag::OP_DEL_TABLE == cell_info.value_.get_ext()))
    {
      ///obj.reset();
      obj.set_ext(cell_info.value_.get_ext());
      ret = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
    }
    else if (ObExtendType != cell_info.value_.get_type()
        || (ObExtendType == cell_info.value_.get_type() 
          && (ObObj::MIN_OBJECT_VALUE == cell_info.value_.get_ext()
            || ObObj::MAX_OBJECT_VALUE == cell_info.value_.get_ext())))
    {
      ret = serialize_column_(buf, buf_len, pos, cell_info);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "serialize_column_ error, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "ObCellInfo value extend type is invalid, Type=%d",
                cell_info.value_.get_type());
    }
  }

  if (OB_SUCCESS == ret)
  {
    if ((cur_size_counter_ + (pos - cur_memblock_->cur_pos)) > mem_size_limit_)
    {
      ret = OB_SIZE_OVERFLOW;
    }
    else
    {
      cur_size_counter_ += pos - cur_memblock_->cur_pos;
      cur_memblock_->cur_pos = pos;

      cur_table_name_ = tmp_table_name_;
      cur_table_id_ = tmp_table_id_;
      cur_row_key_ = tmp_row_key_;

      last_row_key_ = cur_row_key_;
      if (is_serialize_row)
      {
        has_row_key_flag_ = true;
        ++cur_row_num_;
      }
      ++cur_cell_num_;
    }
  }

  return ret;
}

int ObScanner::serialize_table_name_or_id_(char* buf, const int64_t buf_len, int64_t &pos,
                                           const ObCellInfo &cell_info)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_ext(ObActionFlag::TABLE_NAME_FIELD);
  ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
              ret, buf, buf_len, pos);
  }
  else
  {
    if (cell_info.table_name_.length() > 0
        && OB_INVALID_ID != cell_info.table_id_)
    {
      TBSYS_LOG(WARN, "table_name and table_id both exist, table_name=%.*s table_id=%lu",
                cell_info.table_name_.length(), cell_info.table_name_.ptr(), cell_info.table_id_);
      ret = OB_ERROR;
    }
    else
    {
      ///obj.reset();
      if (cell_info.table_name_.length() > 0 && NULL != cell_info.table_name_.ptr())
      {
        obj.set_varchar(cell_info.table_name_);
        if (ID == id_name_type_)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=NAME before=ID");
        }
        else
        {
          id_name_type_ = NAME;
        }
      }
      else
      {
        tmp_table_id_ = cell_info.table_id_;
        obj.set_int(cell_info.table_id_);
        if (NAME == id_name_type_)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=ID before=NAME");
        }
        else
        {
          id_name_type_ = ID;
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t cur_pos = pos;

        ret = obj.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, pos);
        }

        if (cell_info.table_name_.length() > 0)
        {
          ObObj tmp_obj;
          ret = tmp_obj.deserialize(buf, pos, cur_pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p pos=%ld cur_pos=%ld",
                      ret, buf, pos, cur_pos);
          }
          else
          {
            ret = tmp_obj.get_varchar(tmp_table_name_);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObScanner::serialize_row_key_(char* buf, const int64_t buf_len, int64_t &pos,
                                  const ObCellInfo &cell_info)
{
  int ret = OB_SUCCESS;

  ObObj obj;

  if (is_binary_rowkey_format_)
  {
    obj.set_ext(ObActionFlag::ROW_KEY_FIELD);
  }
  else
  {
    obj.set_ext(ObActionFlag::FORMED_ROW_KEY_FIELD);
  }

  ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
              ret, buf, buf_len, pos);
  }
  else
  {
    if (is_binary_rowkey_format_)
    {
      ret = serialize_rowkey_in_binary_format(buf, buf_len, pos, 
          cell_info.table_id_, cell_info.table_name_, cell_info.row_key_);
    }
    else
    {
      ret = set_rowkey_obj_array(buf, buf_len, pos, 
          cell_info.row_key_.get_obj_ptr(), cell_info.row_key_.get_obj_cnt());
    }
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
                ret, buf, buf_len, pos);
    }
    else
    {
      cell_info.row_key_.deep_copy(tmp_row_key_, rowkey_allocator_);
    }
  }

  return ret;
}

int ObScanner::serialize_column_(char* buf, const int64_t buf_len, int64_t &pos,
                                 const ObCellInfo &cell_info)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  ///obj.reset();
  if (cell_info.column_name_.length() > 0 && NULL != cell_info.column_name_.ptr())
  {
    obj.set_varchar(cell_info.column_name_);
    if (ID == id_name_type_)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=NAME before=ID");
    }
    else
    {
      id_name_type_ = NAME;
    }
  }
  else
  {
    obj.set_int(cell_info.column_id_);
    if (NAME == id_name_type_)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "Inconsistent ID/NAME type, current=ID before=NAME");
    }
    else
    {
      id_name_type_ = ID;
    }
  }

    if (OB_SUCCESS == ret)
    {
      ret = obj.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
                  ret, buf, buf_len, pos);
      }
      else
      {
        ret = cell_info.value_.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p buf_len=%ld pos=%ld",
                    ret, buf, buf_len, pos);
        }
      }
    }

  return ret;
}

int64_t ObScanner::get_valid_data_size_(const char* data, const int64_t data_len, int64_t &pos, ObObj &last_obj)
{
  // dependent on serialization protocol
  last_obj.set_int(1);
  UNUSED(data);
  ObObj obj;
  obj.set_ext(ObActionFlag::END_PARAM_FIELD);
  int64_t serial_size = obj.get_serialize_size();
  int64_t size = data_len - pos - serial_size;
  pos += size;
  return size;
}

int ObScanner::deserialize_table_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  int64_t cur_pos = pos;
  int64_t valid_data_size = get_valid_data_size_(buf, data_len, pos, last_obj);

  if (valid_data_size < 0)
  {
    TBSYS_LOG(WARN, "get valid data size fail buf=%p data_len=%ld pos=%ld", buf, data_len, pos);
    ret = OB_ERROR;
  }
  else
  {
    ret = get_memblock_(valid_data_size);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get_memblock_ error, ret=%d", ret);
    }
    else
    {
      memcpy(cur_memblock_->memory, buf + cur_pos, valid_data_size);
      cur_memblock_->cur_pos = valid_data_size;
      cur_size_counter_ = valid_data_size;
    }
  }

  return ret;
}

int ObScanner::get_memblock_(const int64_t size)
{
  int ret = OB_SUCCESS;

  int64_t memory_size = DEFAULT_MEMBLOCK_SIZE;
  if (size > DEFAULT_MEMBLOCK_SIZE)
  {
    memory_size = size;
  }

  if (NULL == head_memblock_)
  {
    head_memblock_ = new(ob_malloc(memory_size + sizeof(MemBlock), ObModIds::OB_SCANNER)) MemBlock(memory_size);
    if (NULL == head_memblock_)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      cur_memblock_ = head_memblock_;
      rollback_memblock_ = head_memblock_;
    }
  }
  else if (size + cur_memblock_->cur_pos > cur_memblock_->memory_size)
  {
    // 可能出现rollback的情况,之后的memblock不需要重新申请,reset即可
    if (NULL == cur_memblock_->next)
    {
      cur_memblock_->next = new(ob_malloc(memory_size + sizeof(MemBlock), ObModIds::OB_SCANNER)) MemBlock(memory_size);
    }
    if (NULL == cur_memblock_->next)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      cur_memblock_->next->reset();
      cur_memblock_ = cur_memblock_->next;
    }
  }
  return ret;
}

// WARN: can not add cell after deserialize scanner if do that rollback will be failed for get last row key
int ObScanner::deserialize_meta_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int64_t value = 0;
  // row count
  int ret = deserialize_int_(buf, data_len, pos, value, last_obj);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
        ret, buf, data_len, pos);
  }
  else
  {
    rollback_row_num_ = cur_row_num_ = value;
  }
  // cell num
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
      rollback_cell_num_ = cur_cell_num_ = value;
    }
  }
  // last row key
  if (OB_SUCCESS == ret)
  {
    ObRowkeyInfo info;
    ret = get_rowkey_compatible(buf, data_len, pos, info, rowkey_allocator_, last_row_key_, is_binary_rowkey_format_);
  }
  assert(cur_cell_num_ >= 0);
  assert(cur_row_num_ >= 0);
  return ret;
}

int ObScanner::deserialize_end_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  UNUSED(last_obj);
  return OB_SUCCESS;
}

int ObScanner::deserialize_basic_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int64_t value = 0;
  // deserialize isfullfilled
  int ret = deserialize_int_(buf, data_len, pos, value, last_obj);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    is_request_fullfilled_ = (value == 0 ? false : true);
  }

  // deserialize filled item number
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
      fullfilled_item_num_ = value;
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
        if (ObActionFlag::TABLET_RANGE_FIELD == type)
        {
          int64_t border_flag = 0;

          ret = deserialize_int_(buf, data_len, pos, border_flag, last_obj);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, pos);
          }

          ObRowkeyInfo info;
          if (OB_SUCCESS == ret)
          {
            ret = get_rowkey_compatible(buf, data_len, pos, info, rowkey_allocator_, 
                range_.start_key_, is_binary_rowkey_format_);
          }

          if (OB_SUCCESS == ret)
          {
            ret = get_rowkey_compatible(buf, data_len, pos, info, rowkey_allocator_, 
                range_.end_key_, is_binary_rowkey_format_);
          }

          if (OB_SUCCESS == ret)
          {
            range_.table_id_ = 0;
            range_.border_flag_.set_data(static_cast<int8_t>(border_flag));
            has_range_ = true;
          }
          // after reading all neccessary fields,
          // filter unknown ObObj
          if (OB_SUCCESS == ret)
          {
            ret = last_obj.deserialize(buf, data_len, pos);
            while (OB_SUCCESS == ret && ObExtendType != last_obj.get_type())
            {
              ret = last_obj.deserialize(buf, data_len, pos);
            }
          }
        }
        else
        {
          // maybe another param field
        }
      }
    }
  }
  // deserialize whole_result_row_num
  if (OB_SUCCESS == ret)
  {
    if (ObIntType == last_obj.get_type())
    {
      ret = last_obj.get_int(whole_result_row_num_);
      if (OB_SUCCESS != ret)
      {
        whole_result_row_num_ = -1;
        TBSYS_LOG(WARN, "ObObj get_int error, ret=%d", ret);
      }
    }
    else if (ObExtendType != last_obj.get_type())
    {
      ret = deserialize_int_(buf, data_len, pos, value, last_obj);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                  ret, buf, data_len, pos);
      }
      else
      {
        whole_result_row_num_ = value;
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

int ObScanner::deserialize_int_(const char* buf, const int64_t data_len, int64_t& pos,
                                int64_t &value, ObObj &last_obj)
{
  int ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    ret = last_obj.get_int(value);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj get_int error, ret=%d", ret);
    }
  }
  return ret;
}

int ObScanner::deserialize_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                    ObString &value, ObObj &last_obj)
{
  int ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    ret = last_obj.get_varchar(value);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
    }
  }
  return ret;
}

int ObScanner::deserialize_int_or_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
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

ObScanner::Iterator ObScanner::begin() const
{
  return ObScanner::Iterator(this, head_memblock_);
}

ObScanner::RowIterator ObScanner::row_begin() const
{
  return ObScanner::RowIterator(this, head_memblock_);
}

ObScanner::Iterator ObScanner::end() const
{
  return ObScanner::Iterator(this, cur_memblock_, cur_size_counter_,
                             (NULL == cur_memblock_) ? 0 : cur_memblock_->cur_pos);
}

ObScanner::RowIterator ObScanner::row_end() const
{
  return ObScanner::RowIterator(this, cur_memblock_, cur_size_counter_,
                             (NULL == cur_memblock_) ? 0 : cur_memblock_->cur_pos);
}

int ObScanner::reset_iter()
{
  int ret = OB_SUCCESS;
  first_next_ = false;
  iter_ = begin();
  return ret;
}

int ObScanner::reset_row_iter()
{
  int ret = OB_SUCCESS;
  first_next_row_ = false;
  row_iter_ = row_begin();
  return ret;
}

int ObScanner::next_cell()
{
  int ret = OB_SUCCESS;
  if (!first_next_)
  {
    iter_ = begin();
    if (iter_.is_iterator_end_())
    {
      ret = OB_ITER_END;
    }
    else
    {
      first_next_ = true;
    }
  }
  else if ((++iter_).is_iterator_end_())
  {
    ret = OB_ITER_END;
  }
  else
  {
    // do nothing
  }
  return ret;
}

int ObScanner::get_cell(ObCellInfo** cell)
{
  return iter_.get_cell(cell);
}

int ObScanner::get_cell(ObCellInfo** cell, bool* is_row_changed)
{
  return iter_.get_cell(cell, is_row_changed);
}

int ObScanner::get_cell(ObInnerCellInfo** cell)
{
  return get_cell(cell, NULL);
}

int ObScanner::get_cell(ObInnerCellInfo** cell, bool* is_row_changed)
{
  ObCellInfo * temp_cell = NULL;
  int ret = iter_.get_cell(&temp_cell, is_row_changed);
  if (OB_SUCCESS == ret)
  {
    ugly_inner_cell_.table_id_ = temp_cell->table_id_;
    ugly_inner_cell_.column_id_ = temp_cell->column_id_;
    ugly_inner_cell_.row_key_ = temp_cell->row_key_;
    ugly_inner_cell_.value_ = temp_cell->value_;
    *cell = &ugly_inner_cell_;
  }
  return ret;
}

int ObScanner::next_row()
{
  int ret = OB_SUCCESS;
  if (!first_next_row_)
  {
    row_iter_ = row_begin();
    if (row_iter_.is_iterator_end_())
    {
      ret = OB_ITER_END;
    }
    else
    {
      first_next_row_ = true;
    }
  }
  else if ((++row_iter_).is_iterator_end_())
  {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObScanner::get_row(ObCellInfo **cell_info, int64_t *num)
{
  int ret = OB_SUCCESS;
  if (!first_next_row_)
  {
    ret = next_row();
  }
  if (OB_SUCCESS == ret)
  {
    ret = row_iter_.get_row(cell_info, num);
  }
  return ret;
}

bool ObScanner::is_empty() const
{
  return(0 == cur_size_counter_);
}

int ObScanner::get_last_row_key(ObRowkey &row_key) const
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

int ObScanner::set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_item_num)
{
  int err = OB_SUCCESS;
  if (0 > fullfilled_item_num)
  {
    TBSYS_LOG(WARN,"param error [fullfilled_item_num:%ld]", fullfilled_item_num);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    fullfilled_item_num_ = fullfilled_item_num;
    is_request_fullfilled_ = is_fullfilled;
  }
  return err;
}

int ObScanner::get_is_req_fullfilled(bool &is_fullfilled, int64_t &fullfilled_item_num) const
{
  int err = OB_SUCCESS;
  is_fullfilled = is_request_fullfilled_;
  fullfilled_item_num = fullfilled_item_num_;
  return err;
}

int ObScanner::set_range(const ObNewRange &range)
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

int ObScanner::set_range_shallow_copy(const ObNewRange& range)
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

int ObScanner::get_range(ObNewRange& range) const
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

void ObScanner::dump(void) const
{
  TBSYS_LOG(INFO, "[dump] scanner range info:");
  if (has_range_)
  {
    range_.hex_dump();
  }
  else
  {
    TBSYS_LOG(INFO, "no scanner range found!");
  }
  if (has_row_key_flag_)
  {
    TBSYS_LOG(INFO, "[dump] last_row_key_=%s", to_cstring(last_row_key_));
  }
  else
  {
    TBSYS_LOG(INFO, "[dump] no last_row_key_ found!");
  }
 
  TBSYS_LOG(INFO, "[dump] is_request_fullfilled_=%d", is_request_fullfilled_);
  TBSYS_LOG(INFO, "[dump] fullfilled_item_num_=%ld", fullfilled_item_num_);
  TBSYS_LOG(INFO, "[dump] cell_num_=%ld", cur_cell_num_);
  TBSYS_LOG(INFO, "[dump] row_num_=%ld", cur_row_num_);
  TBSYS_LOG(INFO, "[dump] whole_result_row_num_=%ld", whole_result_row_num_);
  TBSYS_LOG(INFO, "[dump] cur_size_counter_=%ld", cur_size_counter_);
  TBSYS_LOG(INFO, "[dump] data_version_=%ld", data_version_);
}

void ObScanner::dump_all(int log_level) const
{
  int err = OB_SUCCESS;
  ObCellInfo *cell = NULL;
  bool row_change = false;
  /// dump all result that will be send to ob client
  if (OB_SUCCESS == err && log_level >= TBSYS_LOG_LEVEL_DEBUG)
  {
    this->dump();
    ObScannerIterator iter = this->begin(); 
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
        TBSYS_LOG(INFO, "dump cellinfo rowkey :%s", to_cstring(cell->row_key_));//test::lmz DEBUG -> INFO
      }
      TBSYS_LOG(INFO, "cell->value:%s", to_cstring(cell->value_));//test::lmz DEBUG -> INFO
      ++iter;
    }
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to get scanner cell as debug output. ");
    }
  }
}
