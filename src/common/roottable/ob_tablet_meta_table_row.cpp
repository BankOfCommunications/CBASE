/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_tablet_meta_table_row.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_tablet_meta_table_row.h"
#include "common/ob_tablet_info.h"
#include "common/ob_common_param.h"
#include "common/ob_scanner.h"
#include "common/ob_action_flag.h"
#include "ob_old_root_table_schema.h"
using namespace oceanbase::common;

ObTabletReplica::ObTabletReplica()
  :version_(OB_INVALID_VERSION),
   row_count_(0),
   occupy_size_(0),
   checksum_(0)
{
}

ObTabletReplica::ObTabletReplica(const common::ObTabletReportInfo& tablet)
{
  version_ = tablet.tablet_location_.tablet_version_;
  cs_ = tablet.tablet_location_.chunkserver_;
  row_count_ = tablet.tablet_info_.row_count_;
  occupy_size_ = tablet.tablet_info_.occupy_size_;
  checksum_ = tablet.tablet_info_.crc_sum_;
}

ObTabletReplica::~ObTabletReplica()
{
}

DEFINE_SERIALIZE(ObTabletReplica)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, version_)))
  {
  }
  else if (OB_SUCCESS != (ret = cs_.serialize(buf, buf_len, pos)))
  {
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, row_count_)))
  {
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, occupy_size_)))
  {
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, 
                                                           static_cast<int64_t>(checksum_))))
  {
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTabletReplica)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &version_)))
  {
  }
  else if (OB_SUCCESS != (ret = cs_.deserialize(buf, data_len, pos)))
  {
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &row_count_)))
  {
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &occupy_size_)))
  {
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, 
                                                           reinterpret_cast<int64_t*>(&checksum_))))
  {
  }  
  return ret;
}
////////////////////////////////////////////////////////////////
DEFINE_SERIALIZE(ObTabletMetaTableRow)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = end_key_.serialize(buf, buf_len, pos)))
  {
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(tid_))))
  {
  }
  else if (OB_SUCCESS != (ret = table_name_.serialize(buf, buf_len, pos)))
  {
  }
  else if (OB_SUCCESS != (ret = start_key_.serialize(buf, buf_len, pos)))
  {
  }
  else
  {
    for (int i = 0; i < get_max_replica_count(); ++i)
    {
      if (OB_SUCCESS != (ret = replicas_[i].serialize(buf, buf_len, pos)))
      {
        break;
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTabletMetaTableRow)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = end_key_.deserialize(buf, data_len, pos)))
  {
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, 
                                                           reinterpret_cast<int64_t*>(&tid_))))
  {
  }
  else if (OB_SUCCESS != (ret = table_name_.deserialize(buf, data_len, pos)))
  {
  }
  else if (OB_SUCCESS != (ret = start_key_.deserialize(buf, data_len, pos)))
  {
  }
  else
  {
    for (int i = 0; i < get_max_replica_count(); ++i)
    {
      if (OB_SUCCESS != (ret = replicas_[i].deserialize(buf, data_len, pos)))
      {
        break;
      }
    }
  }
  return ret;
}

const uint64_t ObTabletMetaTableRow::MODIFIED_MASK_REPLICA[ObTabletMetaTableRow::MAX_REPLICA_COUNT] = {1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7, 1 << 8};

ObTabletMetaTableRow::ObTabletMetaTableRow()
  :modified_mask_(0), tid_(OB_INVALID_ID)
{
}

ObTabletMetaTableRow::~ObTabletMetaTableRow()
{
}

bool ObTabletMetaTableRow::has_changed() const
{
  return 0 != modified_mask_;
}

bool ObTabletMetaTableRow::has_changed_tid() const
{
  return 0 != (modified_mask_ & MODIFIED_MASK_TID);
}

bool ObTabletMetaTableRow::has_changed_start_key() const
{
  return 0 != (modified_mask_ & MODIFIED_MASK_STARTKEY);
}

bool ObTabletMetaTableRow::has_changed_end_key() const
{
  return 0 != (modified_mask_ & MODIFIED_MASK_ENDKEY);
}

bool ObTabletMetaTableRow::has_changed_replica(const int32_t i) const
{
  return 0 != (modified_mask_ & MODIFIED_MASK_REPLICA[i]);
}

void ObTabletMetaTableRow::clear_changed()
{
  modified_mask_ = 0;
}

int ObTabletMetaTableRow::add_replica(const ObTabletReplica &replica)
{
  int ret = OB_SIZE_OVERFLOW;
  for (int32_t i = 0; i < get_max_replica_count(); ++i)
  {
    if (replicas_[i].version_ == OB_INVALID_VERSION)
    {
      replicas_[i] = replica;
      modified_mask_ &= MODIFIED_MASK_REPLICA[i];
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

void ObTabletMetaTableRow::update(const ObTabletMetaTableRow &other)
{
  if (other.has_changed_tid())
  {
    this->set_tid(other.get_tid());
  }
  if (other.has_changed_start_key())
  {
    this->set_start_key(other.get_start_key());
  }
  if (other.has_changed_end_key())
  {
    this->set_end_key(other.get_end_key());
  }
  for (int i = 0; i < get_max_replica_count(); ++i)
  {
    if (other.has_changed_replica(i))
    {
      this->set_replica(i, other.get_replica(i));
    }
  }
}

int ObTabletMetaTableRow::add_into_scanner_as_old_roottable(ObScanner &scanner) const
{
  int ret = OB_SUCCESS;
  ObCellInfo cell_info;
  cell_info.table_name_ = table_name_;
  cell_info.row_key_ = end_key_;
  if (start_key_ == ObRowkey::MIN_ROWKEY)
  {
    cell_info.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST); // the special first row
    if (OB_SUCCESS != (ret = scanner.add_cell(cell_info)))
    {
      TBSYS_LOG(WARN, "failed to add cell info into scanner, err=%d", ret);
    }
  }
  // convert row to cells
  if (OB_SUCCESS == ret)
  {
    ObString column_name;
    int added_replica_count = 0;
    for (int i = 0; i < MAX_REPLICA_COUNT; ++i)
    {
      if (replicas_[i].version_ != OB_INVALID_VERSION)
      {
        if (0 == added_replica_count)
        {
          cell_info.column_name_ = old_root_table_columns::COL_OCCUPY_SIZE;
          cell_info.value_.set_int(replicas_[i].occupy_size_);
          if (OB_SUCCESS != (ret = scanner.add_cell(cell_info)))
          {
            TBSYS_LOG(WARN, "failed to add cell info into scanner, err=%d", ret);
            break;
          }
          cell_info.column_name_ = old_root_table_columns::COL_RECORD_COUNT;
          cell_info.value_.set_int(replicas_[i].row_count_);
          if (OB_SUCCESS != (ret = scanner.add_cell(cell_info)))
          {
            TBSYS_LOG(WARN, "failed to add cell info into scanner, err=%d", ret);
            break;
          }
        }
        cell_info.column_name_ = old_root_table_columns::COL_IPV4[added_replica_count];
        cell_info.value_.set_int(replicas_[i].cs_.get_ipv4());
        if (OB_SUCCESS != (ret = scanner.add_cell(cell_info)))
        {
          TBSYS_LOG(WARN, "failed to add cell info into scanner, err=%d", ret);
          break;
        }
        cell_info.column_name_ = old_root_table_columns::COL_PORT[added_replica_count];
        cell_info.value_.set_int(replicas_[i].cs_.get_port());
        if (OB_SUCCESS != (ret = scanner.add_cell(cell_info)))
        {
          TBSYS_LOG(WARN, "failed to add cell info into scanner, err=%d", ret);
          break;
        }
        cell_info.column_name_ = old_root_table_columns::COL_TABLET_VERSION[added_replica_count];
        cell_info.value_.set_int(replicas_[i].version_);
        if (OB_SUCCESS != (ret = scanner.add_cell(cell_info)))
        {
          TBSYS_LOG(WARN, "failed to add cell info into scanner, err=%d", ret);
          break;
        }

        ++added_replica_count;
        if (old_root_table_columns::MAX_REPLICA_COUNT <= added_replica_count)
        {
          break;
        }
      }
    }
  }
  return ret;
}

