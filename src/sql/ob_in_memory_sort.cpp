/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_in_memory_sort.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_in_memory_sort.h"
#include "common/ob_row_util.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObInMemorySort::ObInMemorySort()
  :sort_columns_(NULL), sort_array_get_pos_(0), row_desc_(NULL)
{
}

ObInMemorySort::~ObInMemorySort()
{
}

void ObInMemorySort::reset()
{
  row_store_.clear();
  sort_array_.clear();
  sort_array_get_pos_ = 0;
  row_desc_ = NULL;
}

void ObInMemorySort::reuse()
{
  row_store_.clear();
  sort_array_.clear();
  sort_array_get_pos_ = 0;
  row_desc_ = NULL;
}



int ObInMemorySort::set_sort_columns(const common::ObArray<ObSortColumn> &sort_columns)
{
  int ret = OB_SUCCESS;
  sort_columns_ = &sort_columns;
  for (int32_t i = 0; i < sort_columns.count(); ++i)
  {
    const ObSortColumn &sort_column = sort_columns.at(i);
    if (OB_SUCCESS != (ret = row_store_.add_reserved_column(sort_column.table_id_, sort_column.column_id_)))
    {
      TBSYS_LOG(WARN, "failed to add reserved column, err=%d", ret);
      break;
    }
  }
  return ret;
}

int ObInMemorySort::add_row(const common::ObRow &row)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(sort_columns_);
  const common::ObRowStore::StoredRow* stored_row = NULL;
  if (OB_SUCCESS != (ret = row_store_.add_row(row, stored_row)))
  {
    TBSYS_LOG(WARN, "failed to add row into row_store, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = sort_array_.push_back(stored_row)))
  {
    TBSYS_LOG(WARN, "failed to push back to array, err=%d", ret);
  }
  else if (NULL == row_desc_)
  {
    row_desc_ = row.get_row_desc();
    TBSYS_LOG(DEBUG, "set row desc=%p col_num=%ld", row_desc_, row_desc_->get_column_num());
  }
  return ret;
}

struct ObInMemorySort::Comparer
{
  Comparer(const common::ObArray<ObSortColumn> &sort_columns)
    :sort_columns_(sort_columns)
  {
  }
  bool operator()(const common::ObRowStore::StoredRow *r1, const common::ObRowStore::StoredRow *r2) const
  {
    bool ret = false;
    OB_ASSERT(r1);
    OB_ASSERT(r2);
    for (int32_t i = 0; i < sort_columns_.count(); ++i)
    {
      if (r1->reserved_cells_[i] < r2->reserved_cells_[i])
      {
        ret = sort_columns_.at(i).is_ascending_;
        break;
      }
      else if (r1->reserved_cells_[i] > r2->reserved_cells_[i])
      {
        ret = !sort_columns_.at(i).is_ascending_;
        break;
      }
    } // end for
    return ret;
  }
  private:
    const common::ObArray<ObSortColumn> &sort_columns_;
};

int ObInMemorySort::sort_rows()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(sort_columns_);
  if (0 < sort_array_.count())
  {
    TBSYS_LOG(DEBUG, "sort rows, count=%ld", sort_array_.count());
    const common::ObRowStore::StoredRow **first_row = &sort_array_.at(0);
    std::sort(first_row, first_row+sort_array_.count(), Comparer(*sort_columns_));
  }
  return ret;
}

int ObInMemorySort::get_next_row(common::ObRow &row)
{
  int ret = OB_SUCCESS;
  if (sort_array_get_pos_ >= sort_array_.count())
  {
    ret = OB_ITER_END;
    TBSYS_LOG(DEBUG, "end of the in-memory run");
  }
  else
  {
    OB_ASSERT(row_desc_);
    row.set_row_desc(*row_desc_);
    if (OB_SUCCESS != (ret = common::ObRowUtil::convert(sort_array_.at(
              static_cast<int32_t>(sort_array_get_pos_))->get_compact_row(), row)))
    {
      TBSYS_LOG(WARN, "failed to convert row, err=%d", ret);
    }
    else
    {
      ++sort_array_get_pos_;
    }
  }
  return ret;
}

int ObInMemorySort::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = get_next_row(curr_row_)))
  {
    row = &curr_row_;
  }
  return ret;
}

int ObInMemorySort::get_next_compact_row(ObString &compact_row)
{
  int ret = OB_SUCCESS;
  if (sort_array_get_pos_ >= sort_array_.count())
  {
    ret = OB_ITER_END;
    TBSYS_LOG(INFO, "end of the in-memory run");
  }
  else
  {
    compact_row = sort_array_.at(static_cast<int32_t>(sort_array_get_pos_))->get_compact_row();
    ++sort_array_get_pos_;
  }
  return ret;
}

int64_t ObInMemorySort::get_row_count() const
{
  return sort_array_.count();
}

int64_t ObInMemorySort::get_used_mem_size() const
{
  return row_store_.get_used_mem_size() + sort_array_.count()*sizeof(void*);
}

DEFINE_SERIALIZE(ObSortColumn)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(table_id_))))
  {
    TBSYS_LOG(WARN, "fail to encode table_id_:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(column_id_))))
  {
    TBSYS_LOG(WARN, "fail to encode column_id_:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, is_ascending_)))
  {
    TBSYS_LOG(WARN, "fail to encode is_ascending_:ret[%d]", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSortColumn)
{
  int ret = OB_SUCCESS;
  int64_t table_id = 0;
  int64_t column_id = 0;

  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &table_id)))
  {
    TBSYS_LOG(WARN, "fail to decode table_id_:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &column_id)))
  {
    TBSYS_LOG(WARN, "fail to decode column_id_:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &is_ascending_)))
  {
    TBSYS_LOG(WARN, "fail to decode is_ascending_:ret[%d]", ret);
  }
  else
  {
    table_id_ = static_cast<uint64_t>(table_id);
    column_id_ = static_cast<uint64_t>(column_id);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSortColumn)
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(table_id_);
  size += serialization::encoded_length_vi64(column_id_);
  size += serialization::encoded_length_bool(is_ascending_);
  return size;
}
