/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_desc.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_row_desc.h"
#include "common/murmur_hash.h"
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "common/serialization.h"
#include "common/ob_common_stat.h"
#include <tbsys.h>

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

uint64_t ObRowDesc::HASH_COLLISIONS_COUNT = 0;

ObRowDesc::ObRowDesc()
  :cells_desc_count_(0), rowkey_cell_count_(0)
{
}

ObRowDesc::~ObRowDesc()
{
}

int64_t ObRowDesc::get_idx(const uint64_t table_id, const uint64_t column_id) const
{
  int64_t ret = OB_INVALID_INDEX;
  if (0 != table_id && 0 != column_id)
  {
    int64_t index = 0;
    if (OB_SUCCESS == hash_find(table_id, column_id, index))
    {
      if (index < cells_desc_count_)
      {
        ret = index;
      }
    }
  }
  return ret;
}

int ObRowDesc::add_column_desc(const uint64_t table_id, const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (cells_desc_count_ >= MAX_COLUMNS_COUNT)
  {
    TBSYS_LOG(ERROR, "too many column for a row, tid=%lu cid=%lu",
              table_id, column_id);
    ret = OB_BUF_NOT_ENOUGH;
  }
  else
  {
    cells_desc_[cells_desc_count_].table_id_ = table_id;
    cells_desc_[cells_desc_count_].column_id_ = column_id;
    if (OB_SUCCESS != (ret = hash_insert(table_id, column_id, cells_desc_count_)))
    {
      TBSYS_LOG(WARN, "failed to insert column desc, err=%d", ret);
    }
    else
    {
      ++cells_desc_count_;
    }
  }
  return ret;
}

void ObRowDesc::reset()
{
  cells_desc_count_ = 0;
  rowkey_cell_count_ = 0;
  hash_map_.clear();
}

int64_t ObRowDesc::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "rowkey_cell_count[%ld],", rowkey_cell_count_);
  for (int64_t i = 0; i < cells_desc_count_; ++i)
  {
    if (OB_INVALID_ID == cells_desc_[i].table_id_)
    {
      databuff_printf(buf, buf_len, pos, "<NULL,%lu>,",
                      cells_desc_[i].column_id_);
    }
    else
    {
      databuff_printf(buf, buf_len, pos, "<%lu,%lu>,",
                      cells_desc_[i].table_id_, cells_desc_[i].column_id_);
    }
  }
  return pos;
}

inline bool ObRowDesc::Desc::is_invalid() const
{
  return (0 == table_id_ || 0 == column_id_);
}

inline bool ObRowDesc::Desc::operator== (const Desc &other) const
{
  return table_id_ == other.table_id_ && column_id_ == other.column_id_;
}

int ObRowDesc::hash_find(const uint64_t table_id, const uint64_t column_id, int64_t & index) const
{
  int ret = OB_SUCCESS;
  Desc desc;
  desc.table_id_ = table_id;
  desc.column_id_ = column_id;
  ret = hash_map_.find(desc, index);
  return ret;
}

int ObRowDesc::hash_insert(const uint64_t table_id, const uint64_t column_id, const int64_t index)
{
  int ret = OB_SUCCESS;
  Desc desc;
  desc.table_id_ = table_id;
  desc.column_id_ = column_id;
  ret = hash_map_.insert(desc, index);
  if (OB_DUPLICATE_COLUMN == ret)
  {
    TBSYS_LOG(WARN, "duplicated cell desc, tid=%lu cid=%lu new_idx=%ld",
              table_id, column_id, index);
  }
  return ret;
}


ObRowDesc & ObRowDesc::operator = (const ObRowDesc & r)
{
  int err = 0;
  hash_map_.clear();
  cells_desc_count_ = 0;
  rowkey_cell_count_ = r.rowkey_cell_count_;
  for (int64_t i = 0; i < r.cells_desc_count_; i++)
  {
    err = add_column_desc(r.cells_desc_[i].table_id_, r.cells_desc_[i].column_id_);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "add_column_desc error, err: %d r: %s", err, to_cstring(r));
      break;
    }
  }
  return *this;
}

int ObRowDesc::assign(const ObRowDesc& other)
{
  int ret = OB_SUCCESS;
  reset();
  for (int64_t i = 0; i < other.cells_desc_count_; i++)
  {
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    if ((ret = other.get_tid_cid(i, table_id, column_id)) != OB_SUCCESS
      || (ret = add_column_desc(table_id, column_id)) != OB_SUCCESS)
    {
      break;
    }
  }
  rowkey_cell_count_ = other.rowkey_cell_count_;
  return ret;
}

/* int ObRowDesc::serialize(char *buf, const int64_t buf_len, int64_t &pos) const */
DEFINE_SERIALIZE(ObRowDesc)
{
  int ret = OB_SUCCESS;
  const int64_t column_cnt = get_column_num();
  const int64_t pos_bk = pos;
  uint64_t tid = 0;
  uint64_t cid = 0;

  ret = encode_vi64(buf, buf_len, pos, column_cnt);
  for (int64_t i = 0; i < column_cnt && OB_SUCCESS == ret; i++)
  {
    if (OB_SUCCESS != (ret = get_tid_cid(i, tid, cid)))
    {
      TBSYS_LOG(ERROR, "get tid cid error, ret: %d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, static_cast<int64_t>(tid))))
    {
      TBSYS_LOG(ERROR, "encode tid error, ret: %d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, static_cast<int64_t>(cid))))
    {
      TBSYS_LOG(ERROR, "encode cid error, ret: %d", ret);
      break;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, rowkey_cell_count_)))
    {
      TBSYS_LOG(WARN, "fail to encode rowkey cell count[%ld] :ret[%d]", rowkey_cell_count_, ret);
    }
  }

  if (OB_SUCCESS != ret)
  {
    pos = pos_bk;
  }
  return ret;
}

/* int ObRowDesc::deserialize(const char *buf, const int64_t data_len, int64_t &pos) */
DEFINE_DESERIALIZE(ObRowDesc)
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = 0;
  int64_t tid = 0;
  int64_t cid = 0;
  int64_t pos_bk = pos;

  ret = decode_vi64(buf, data_len, pos, &column_cnt);
  while (column_cnt-- && OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &tid)))
    {
      TBSYS_LOG(ERROR, "decode tid error, ret: %d", ret);
      break;
    }
    if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &cid)))
    {
      TBSYS_LOG(ERROR, "decode cid error, ret: %d", ret);
      break;
    }
    if (OB_SUCCESS != (ret = add_column_desc(tid, cid)))
    {
      TBSYS_LOG(ERROR, "add column desc error, ret: %d", ret);
      break;
    }
    TBSYS_LOG(DEBUG, "tid %lu, cid %lu", tid, cid);
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &rowkey_cell_count_)))
    {
      TBSYS_LOG(WARN, "fail to decode rowkey_cell_count_:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS != ret)
  {
    pos = pos_bk;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRowDesc)
{
  int err = OB_SUCCESS;
  int64_t size = 0;
  const int64_t column_cnt = get_column_num();
  uint64_t tid = 0;
  uint64_t cid = 0;

  size += encoded_length_vi64(column_cnt);
  for (int64_t i = 0; i < column_cnt; i++)
  {
    if (OB_SUCCESS != (err = get_tid_cid(i, tid, cid)))
    {
      TBSYS_LOG(ERROR, "get tid cid error, err: %d", err);
      break;
    }
    size += encoded_length_vi64(static_cast<int64_t>(tid));
    size += encoded_length_vi64(static_cast<int64_t>(cid));
  }
  size += encoded_length_vi64(rowkey_cell_count_);
  return size;
}
