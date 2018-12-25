/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_first_tablet_entry_schema.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_first_tablet_entry_schema.h"
using namespace oceanbase::common;

namespace first_tablet_entry_schema_internal
{
  using namespace oceanbase::common::first_tablet_entry_cid;
  
  static const uint64_t COLUMNS_ID[] = {
    TNAME,                        // 0
    TID,
    DBNAME,//add dolphin [database manager]@20150609
    TABLE_TYPE,
    META_TNAME,
    META_TID,
    REPLICA1_IPV4,
    REPLICA1_IPV6_HIGH,
    REPLICA1_IPV6_LOW,
    REPLICA1_IP_PORT,
    REPLICA1_VERSION,
    REPLICA1_ROW_COUNT,
    REPLICA1_SIZE,
    REPLICA1_CHECKSUM,
    REPLICA2_IPV4,
    REPLICA2_IPV6_HIGH,
    REPLICA2_IPV6_LOW,
    REPLICA2_IP_PORT,
    REPLICA2_VERSION,
    REPLICA2_ROW_COUNT,
    REPLICA2_SIZE,
    REPLICA2_CHECKSUM,
    REPLICA3_IPV4,
    REPLICA3_IPV6_HIGH,
    REPLICA3_IPV6_LOW,
    REPLICA3_IP_PORT,
    REPLICA3_VERSION,
    REPLICA3_ROW_COUNT,
    REPLICA3_SIZE,
    REPLICA3_CHECKSUM,
    REPLICA4_IPV4,
    REPLICA4_IPV6_HIGH,
    REPLICA4_IPV6_LOW,
    REPLICA4_IP_PORT,
    REPLICA4_VERSION,
    REPLICA4_ROW_COUNT,
    REPLICA4_SIZE,
    REPLICA4_CHECKSUM,
    REPLICA5_IPV4,
    REPLICA5_IPV6_HIGH,
    REPLICA5_IPV6_LOW,
    REPLICA5_IP_PORT,
    REPLICA5_VERSION,
    REPLICA5_ROW_COUNT,
    REPLICA5_SIZE,
    REPLICA5_CHECKSUM,
    REPLICA6_IPV4,
    REPLICA6_IPV6_HIGH,
    REPLICA6_IPV6_LOW,
    REPLICA6_IP_PORT,
    REPLICA6_VERSION,
    REPLICA6_ROW_COUNT,
    REPLICA6_SIZE,
    REPLICA6_CHECKSUM
  };

  enum COLUMNS_IDX
  {
    CIDX_TNAME = 0,               // 0
   // CIDX_TID = 0,
    CIDX_TID,
    CIDX_TABLE_TYPE,
    CIDX_META_TNAME,
    CIDX_META_TID,
    CIDX_REPLICA1_IPV4,
    CIDX_REPLICA1_IPV6_HIGH,
    CIDX_REPLICA1_IPV6_LOW,
    CIDX_REPLICA1_IP_PORT,
    CIDX_REPLICA1_VERSION,
    CIDX_REPLICA1_ROW_COUNT,
    CIDX_REPLICA1_SIZE,
    CIDX_REPLICA1_CHECKSUM,
  };

  struct CfAndIdx
  {
    ObTabletMetaTableColumnSchema::CF cf;
    int32_t idx;
  };

  static const CfAndIdx CID_CF_IDX_MAP[ARRAYSIZEOF(COLUMNS_ID)] = 
  {
    //{ObTabletMetaTableColumnSchema::CF_TNAME, 0}, // 0
    {ObTabletMetaTableColumnSchema::CF_TID, 0},
    {ObTabletMetaTableColumnSchema::CF_TABLE_TYPE, 0},
    {ObTabletMetaTableColumnSchema::CF_META_TNAME, 0},
    {ObTabletMetaTableColumnSchema::CF_META_TID, 0},

    {ObTabletMetaTableColumnSchema::CF_IPV4, 0},
    {ObTabletMetaTableColumnSchema::CF_IPV6_HIGH, 0},
    {ObTabletMetaTableColumnSchema::CF_IPV6_LOW, 0},
    {ObTabletMetaTableColumnSchema::CF_PORT, 0},
    {ObTabletMetaTableColumnSchema::CF_VERSION, 0},
    {ObTabletMetaTableColumnSchema::CF_ROW_COUNT, 0},
    {ObTabletMetaTableColumnSchema::CF_SIZE, 0},
    {ObTabletMetaTableColumnSchema::CF_CHECKSUM, 0},

    {ObTabletMetaTableColumnSchema::CF_IPV4, 1},
    {ObTabletMetaTableColumnSchema::CF_IPV6_HIGH, 1},
    {ObTabletMetaTableColumnSchema::CF_IPV6_LOW, 1},
    {ObTabletMetaTableColumnSchema::CF_PORT, 1},
    {ObTabletMetaTableColumnSchema::CF_VERSION, 1},
    {ObTabletMetaTableColumnSchema::CF_ROW_COUNT, 1},
    {ObTabletMetaTableColumnSchema::CF_SIZE, 1},
    {ObTabletMetaTableColumnSchema::CF_CHECKSUM, 1},

    {ObTabletMetaTableColumnSchema::CF_IPV4, 2},
    {ObTabletMetaTableColumnSchema::CF_IPV6_HIGH, 2},
    {ObTabletMetaTableColumnSchema::CF_IPV6_LOW, 2},
    {ObTabletMetaTableColumnSchema::CF_PORT, 2},
    {ObTabletMetaTableColumnSchema::CF_VERSION, 2},
    {ObTabletMetaTableColumnSchema::CF_ROW_COUNT, 2},
    {ObTabletMetaTableColumnSchema::CF_SIZE, 2},
    {ObTabletMetaTableColumnSchema::CF_CHECKSUM, 2},

    {ObTabletMetaTableColumnSchema::CF_IPV4, 3},
    {ObTabletMetaTableColumnSchema::CF_IPV6_HIGH, 3},
    {ObTabletMetaTableColumnSchema::CF_IPV6_LOW, 3},
    {ObTabletMetaTableColumnSchema::CF_PORT, 3},
    {ObTabletMetaTableColumnSchema::CF_VERSION, 3},
    {ObTabletMetaTableColumnSchema::CF_ROW_COUNT, 3},
    {ObTabletMetaTableColumnSchema::CF_SIZE, 3},
    {ObTabletMetaTableColumnSchema::CF_CHECKSUM, 3},

    {ObTabletMetaTableColumnSchema::CF_IPV4, 4},
    {ObTabletMetaTableColumnSchema::CF_IPV6_HIGH, 4},
    {ObTabletMetaTableColumnSchema::CF_IPV6_LOW, 4},
    {ObTabletMetaTableColumnSchema::CF_PORT, 4},
    {ObTabletMetaTableColumnSchema::CF_VERSION, 4},
    {ObTabletMetaTableColumnSchema::CF_ROW_COUNT, 4},
    {ObTabletMetaTableColumnSchema::CF_SIZE, 4},
    {ObTabletMetaTableColumnSchema::CF_CHECKSUM, 4},

    {ObTabletMetaTableColumnSchema::CF_IPV4, 5},
    {ObTabletMetaTableColumnSchema::CF_IPV6_HIGH, 5},
    {ObTabletMetaTableColumnSchema::CF_IPV6_LOW, 5},
    {ObTabletMetaTableColumnSchema::CF_PORT, 5},
    {ObTabletMetaTableColumnSchema::CF_VERSION, 5},
    {ObTabletMetaTableColumnSchema::CF_ROW_COUNT, 5},
    {ObTabletMetaTableColumnSchema::CF_SIZE, 5},
    {ObTabletMetaTableColumnSchema::CF_CHECKSUM, 5}
  };
  
  static const int64_t COLUMNS_PER_REPLICA = CIDX_REPLICA1_CHECKSUM - CIDX_REPLICA1_IPV4 + 1;
} // namespace internal
using namespace first_tablet_entry_schema_internal;

ObFirstTabletEntryColumnSchema::ObFirstTabletEntryColumnSchema()
{
}

ObFirstTabletEntryColumnSchema::~ObFirstTabletEntryColumnSchema()
{
}

uint64_t ObFirstTabletEntryColumnSchema::get_cid(const CF cf, const int32_t i) const
{
  uint64_t ret = OB_INVALID_ID;
  if (i >= ObTabletMetaTableRow::MAX_REPLICA_COUNT || 0 > i)
  {
    TBSYS_LOG(ERROR, "BUG invalid index, i=%d", i);
  }
  else
  {
    switch(cf)
    {
      case CF_TID:
        ret = COLUMNS_ID[CIDX_TID];
        break;
      case CF_VERSION:
        ret = COLUMNS_ID[CIDX_REPLICA1_VERSION + i * COLUMNS_PER_REPLICA];
        break;
      case CF_CHECKSUM:
        ret = COLUMNS_ID[CIDX_REPLICA1_CHECKSUM + i * COLUMNS_PER_REPLICA];
        break;
      case CF_ROW_COUNT:
        ret = COLUMNS_ID[CIDX_REPLICA1_ROW_COUNT + i * COLUMNS_PER_REPLICA];
        break;
      case CF_SIZE:
        ret = COLUMNS_ID[CIDX_REPLICA1_SIZE + i * COLUMNS_PER_REPLICA];
        break;
      case CF_IPV4:
        ret = COLUMNS_ID[CIDX_REPLICA1_IPV4 + i * COLUMNS_PER_REPLICA];
        break;
      case CF_IPV6_HIGH:
        ret = COLUMNS_ID[CIDX_REPLICA1_IPV6_HIGH + i * COLUMNS_PER_REPLICA];
        break;
      case CF_IPV6_LOW:
        ret = COLUMNS_ID[CIDX_REPLICA1_IPV6_LOW + i * COLUMNS_PER_REPLICA];
        break;
      case CF_PORT:
        ret = COLUMNS_ID[CIDX_REPLICA1_IP_PORT + i * COLUMNS_PER_REPLICA];
        break;
      case CF_META_TID:
        ret = COLUMNS_ID[CIDX_META_TID];
        break;
      case CF_META_TNAME:
        ret = COLUMNS_ID[CIDX_META_TNAME];
        break;
      case CF_TABLE_TYPE:
        ret = COLUMNS_ID[CIDX_TABLE_TYPE];
        break;
      default:
        TBSYS_LOG(ERROR, "invalid column symbol, sym=%d idx=%d", cf, i);
        break;
    } // end switch
  }
  return ret;
}

int64_t ObFirstTabletEntryColumnSchema::get_cidx(const CF cf, const int32_t i) const
{
  int64_t ret = OB_INVALID_INDEX;
  if (i >= ObTabletMetaTableRow::MAX_REPLICA_COUNT || 0 > i)
  {
    TBSYS_LOG(ERROR, "BUG invalid index, i=%d", i);
  }
  else
  {
    switch(cf)
    {
      case CF_TID:
        ret = CIDX_TID;
        break;
      case CF_VERSION:
        ret = CIDX_REPLICA1_VERSION + i * COLUMNS_PER_REPLICA;
        break;
      case CF_CHECKSUM:
        ret = CIDX_REPLICA1_CHECKSUM + i * COLUMNS_PER_REPLICA;
        break;
      case CF_ROW_COUNT:
        ret = CIDX_REPLICA1_ROW_COUNT + i * COLUMNS_PER_REPLICA;
        break;
      case CF_SIZE:
        ret = CIDX_REPLICA1_SIZE + i * COLUMNS_PER_REPLICA;
        break;
      case CF_IPV4:
        ret = CIDX_REPLICA1_IPV4 + i * COLUMNS_PER_REPLICA;
        break;
      case CF_IPV6_HIGH:
        ret = CIDX_REPLICA1_IPV6_HIGH + i * COLUMNS_PER_REPLICA;
        break;
      case CF_IPV6_LOW:
        ret = CIDX_REPLICA1_IPV6_LOW + i * COLUMNS_PER_REPLICA;
        break;
      case CF_PORT:
        ret = CIDX_REPLICA1_IP_PORT + i * COLUMNS_PER_REPLICA;
        break;
      case CF_TABLE_TYPE:
        ret = CIDX_TABLE_TYPE;
        break;
      case CF_META_TID:
        ret = CIDX_META_TID;
        break;
      case CF_META_TNAME:
        ret = CIDX_META_TNAME;
        break;
      default:
        TBSYS_LOG(ERROR, "invalid column symbol, sym=%d idx=%d", cf, i);
        break;
    } // end switch
  }
  return ret;
}

int64_t ObFirstTabletEntryColumnSchema::get_columns_num() const
{
  return ARRAYSIZEOF(COLUMNS_ID);
}

uint64_t ObFirstTabletEntryColumnSchema::get_cid_by_idx(const int64_t i) const
{
  uint64_t ret = OB_INVALID_ID;
  if (i >= static_cast<int64_t>(ARRAYSIZEOF(COLUMNS_ID)) || 0 > i)
  {
    TBSYS_LOG(ERROR, "BUG invalid column index, i=%ld", i);
  }
  else
  {
    ret = COLUMNS_ID[i];
  }
  return ret;
}


int ObFirstTabletEntryColumnSchema::get_cf(const uint64_t cid, CF &cf, int32_t &idx) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (uint32_t i = 0; i < ARRAYSIZEOF(COLUMNS_ID); ++i)
  {
    if (cid == COLUMNS_ID[i])
    {
      cf = CID_CF_IDX_MAP[i].cf;
      idx = CID_CF_IDX_MAP[i].idx;
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}
