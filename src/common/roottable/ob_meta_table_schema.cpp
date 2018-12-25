/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_meta_table_schema.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_meta_table_schema.h"
#include "common/ob_schema_service.h"
using namespace oceanbase::common;

static const char* SCOL_TID = "table_id";
static const char* SCOL_TABLET_VERSION[OB_TABLET_MAX_REPLICA_COUNT] = 
{
  "replica1_version", "replica2_version", "replica3_version", "replica4_version", "replica5_version", "replica6_version"
};
static const char* SCOL_OCCUPY_SIZE[OB_TABLET_MAX_REPLICA_COUNT] = 
{
  "replica1_size", "replica2_size", "replica3_size", "replica4_size", "replica5_size", "replica6_size"
};
static const char* SCOL_ROW_COUNT[OB_TABLET_MAX_REPLICA_COUNT] = 
{
  "replica1_row_count", "replica2_row_count", "replica3_row_count", "replica4_row_count", "replica5_row_count", "replica6_row_count"
};
static const char* SCOL_PORT[OB_TABLET_MAX_REPLICA_COUNT] = 
{
  "replica1_port", "replica2_port", "replica3_port", "replica4_port", "replica5_port", "replica6_port"
};
static const char* SCOL_IPV4[OB_TABLET_MAX_REPLICA_COUNT] = 
{
  "replica1_ipv4", "replica2_ipv4", "replica3_ipv4", "replica4_ipv4", "replica5_ipv4", "replica6_ipv4"
};
static const char* SCOL_IPV6_HIGH[OB_TABLET_MAX_REPLICA_COUNT] = 
{
  "replica1_ipv6_high", "replica2_ipv6_high", "replica3_ipv6_high", "replica4_ipv6_high", "replica5_ipv6_high", "replica6_ipv6_high"
};
static const char* SCOL_IPV6_LOW[OB_TABLET_MAX_REPLICA_COUNT] = 
{
  "replica1_ipv6_low", "replica2_ipv6_low", "replica3_ipv6_low", "replica4_ipv6_low", "replica5_ipv6_low", "replica6_ipv6_low"
};
static const char* SCOL_CHECKSUM[OB_TABLET_MAX_REPLICA_COUNT] = 
{
  "replica1_checksum", "replica2_checksum", "replica3_checksum", "replica4_checksum", "replica5_checksum", "replica6_checksum"
};
static const char* SCOL_STARTKEY_OBJ_PREFIX = "start_rowkey_obj";
static const char* SCOL_ENDKEY_OBJ_PREFIX = "end_rowkey_obj";
static const char* SCOL_STARTKEY_OBJ_PREFIX_FORMAT = "start_rowkey_obj%d";

namespace oceanbase
{
  namespace common
  {
namespace meta_table_cname
{
  const ObString TID(0, static_cast<int32_t>(strlen(SCOL_TID)), SCOL_TID);
  const ObString TABLET_VERSION[OB_TABLET_MAX_REPLICA_COUNT] = 
  {
    ObString(0, static_cast<int32_t>(strlen(SCOL_TABLET_VERSION[0])), SCOL_TABLET_VERSION[0]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_TABLET_VERSION[1])), SCOL_TABLET_VERSION[1]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_TABLET_VERSION[2])), SCOL_TABLET_VERSION[2]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_TABLET_VERSION[3])), SCOL_TABLET_VERSION[3]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_TABLET_VERSION[4])), SCOL_TABLET_VERSION[4]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_TABLET_VERSION[5])), SCOL_TABLET_VERSION[5]),
  };
  const ObString OCCUPY_SIZE[OB_TABLET_MAX_REPLICA_COUNT] =
  {
    ObString(0, static_cast<int32_t>(strlen(SCOL_OCCUPY_SIZE[0])), SCOL_OCCUPY_SIZE[0]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_OCCUPY_SIZE[1])), SCOL_OCCUPY_SIZE[1]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_OCCUPY_SIZE[2])), SCOL_OCCUPY_SIZE[2]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_OCCUPY_SIZE[3])), SCOL_OCCUPY_SIZE[3]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_OCCUPY_SIZE[4])), SCOL_OCCUPY_SIZE[4]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_OCCUPY_SIZE[5])), SCOL_OCCUPY_SIZE[5]),
  };
  const ObString ROW_COUNT[OB_TABLET_MAX_REPLICA_COUNT] = 
  {
    ObString(0, static_cast<int32_t>(strlen(SCOL_ROW_COUNT[0])), SCOL_ROW_COUNT[0]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_ROW_COUNT[1])), SCOL_ROW_COUNT[1]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_ROW_COUNT[2])), SCOL_ROW_COUNT[2]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_ROW_COUNT[3])), SCOL_ROW_COUNT[3]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_ROW_COUNT[4])), SCOL_ROW_COUNT[4]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_ROW_COUNT[5])), SCOL_ROW_COUNT[5]),
  };
  const ObString PORT[OB_TABLET_MAX_REPLICA_COUNT] = 
  {
    ObString(0, static_cast<int32_t>(strlen(SCOL_PORT[0])), SCOL_PORT[0]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_PORT[1])), SCOL_PORT[1]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_PORT[2])), SCOL_PORT[2]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_PORT[3])), SCOL_PORT[3]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_PORT[4])), SCOL_PORT[4]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_PORT[5])), SCOL_PORT[5]),
  };
  const ObString IPV4[OB_TABLET_MAX_REPLICA_COUNT] = 
  {
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV4[0])), SCOL_IPV4[0]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV4[1])), SCOL_IPV4[1]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV4[2])), SCOL_IPV4[2]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV4[3])), SCOL_IPV4[3]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV4[4])), SCOL_IPV4[4]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV4[5])), SCOL_IPV4[5]),
  };
  const ObString IPV6_HIGH[OB_TABLET_MAX_REPLICA_COUNT] = 
  {
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_HIGH[0])), SCOL_IPV6_HIGH[0]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_HIGH[1])), SCOL_IPV6_HIGH[1]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_HIGH[2])), SCOL_IPV6_HIGH[2]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_HIGH[3])), SCOL_IPV6_HIGH[3]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_HIGH[4])), SCOL_IPV6_HIGH[4]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_HIGH[5])), SCOL_IPV6_HIGH[5]),
  };
  const ObString IPV6_LOW[OB_TABLET_MAX_REPLICA_COUNT] = 
  {
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_LOW[0])), SCOL_IPV6_LOW[0]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_LOW[1])), SCOL_IPV6_LOW[1]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_LOW[2])), SCOL_IPV6_LOW[2]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_LOW[3])), SCOL_IPV6_LOW[3]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_LOW[4])), SCOL_IPV6_LOW[4]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_IPV6_LOW[5])), SCOL_IPV6_LOW[5]),
  };
  const ObString CHECKSUM[OB_TABLET_MAX_REPLICA_COUNT] = 
  {
    ObString(0, static_cast<int32_t>(strlen(SCOL_CHECKSUM[0])), SCOL_CHECKSUM[0]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_CHECKSUM[1])), SCOL_CHECKSUM[1]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_CHECKSUM[2])), SCOL_CHECKSUM[2]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_CHECKSUM[3])), SCOL_CHECKSUM[3]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_CHECKSUM[4])), SCOL_CHECKSUM[4]),
    ObString(0, static_cast<int32_t>(strlen(SCOL_CHECKSUM[5])), SCOL_CHECKSUM[5]),
  };
  const ObString STARTKEY_OBJ_PREFIX(0, static_cast<int32_t>(strlen(SCOL_STARTKEY_OBJ_PREFIX)), SCOL_STARTKEY_OBJ_PREFIX);
  const ObString ENDKEY_OBJ_PREFIX(0, static_cast<int32_t>(strlen(SCOL_ENDKEY_OBJ_PREFIX)), SCOL_ENDKEY_OBJ_PREFIX);
} // end namespace meta_table_columns_name
  } // end namespace common
} // end namespace oceanbase

////////////////////////////////////////////////////////////////
namespace meta_table_column_schema_internal
{
  using namespace ::oceanbase::common::meta_table_cname;
  static const ObString COLUMNS_NAME[] = 
  {
    TID,        // 0

    IPV4[0],    // 1
    IPV6_HIGH[0],
    IPV6_LOW[0],
    PORT[0],
    TABLET_VERSION[0],
    ROW_COUNT[0],
    OCCUPY_SIZE[0],
    CHECKSUM[0],

    IPV4[1],
    IPV6_HIGH[1],
    IPV6_LOW[1],
    PORT[1],
    TABLET_VERSION[1],
    ROW_COUNT[1],
    OCCUPY_SIZE[1],
    CHECKSUM[1],

    IPV4[2],
    IPV6_HIGH[2],
    IPV6_LOW[2],
    PORT[2],
    TABLET_VERSION[2],
    ROW_COUNT[2],
    OCCUPY_SIZE[2],
    CHECKSUM[2],

    IPV4[3],
    IPV6_HIGH[3],
    IPV6_LOW[3],
    PORT[3],
    TABLET_VERSION[3],
    ROW_COUNT[3],
    OCCUPY_SIZE[3],
    CHECKSUM[3],

    IPV4[4],
    IPV6_HIGH[4],
    IPV6_LOW[4],
    PORT[4],
    TABLET_VERSION[4],
    ROW_COUNT[4],
    OCCUPY_SIZE[4],
    CHECKSUM[4],

    IPV4[5],
    IPV6_HIGH[5],
    IPV6_LOW[5],
    PORT[5],
    TABLET_VERSION[5],
    ROW_COUNT[5],
    OCCUPY_SIZE[5],
    CHECKSUM[5]
  };

  enum COLUMNS_IDX
  {
    CIDX_TID,                     // 0
    CIDX_REPLICA1_IPV4,           // 1
    CIDX_REPLICA1_IPV6_HIGH,
    CIDX_REPLICA1_IPV6_LOW,
    CIDX_REPLICA1_IP_PORT,
    CIDX_REPLICA1_VERSION,
    CIDX_REPLICA1_ROW_COUNT,
    CIDX_REPLICA1_SIZE,
    CIDX_REPLICA1_CHECKSUM,       // 8
  };

  struct CfAndIdx
  {
    ObTabletMetaTableColumnSchema::CF cf;
    int32_t idx;
  };

  static const CfAndIdx CID_CF_IDX_MAP[ARRAYSIZEOF(COLUMNS_NAME)] = 
  {
    {ObTabletMetaTableColumnSchema::CF_TID, 0},

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
using namespace meta_table_column_schema_internal;

ObMetaTableColumnSchema::ObMetaTableColumnSchema(const TableSchema &table_schema)
  :table_schema_(table_schema)
{
}

ObMetaTableColumnSchema::~ObMetaTableColumnSchema()
{
}

int ObMetaTableColumnSchema::get_cid_by_name(const ObString &cname, uint64_t &cid) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  cid = OB_INVALID_ID;
  ObString schema_cname;
  for (int i = 0; i < table_schema_.columns_.count(); ++i)
  {
    schema_cname.assign_ptr(const_cast<char*>(table_schema_.columns_.at(i).column_name_), 
                            static_cast<int32_t>(strlen(table_schema_.columns_.at(i).column_name_)));
    if (schema_cname == cname)
    {
      ret = OB_SUCCESS;
      cid = table_schema_.columns_.at(i).column_id_;
      break;
    }
  }  
  return ret;
}

uint64_t ObMetaTableColumnSchema::get_cid(const CF cf, const int32_t i) const
{
  uint64_t ret = OB_INVALID_ID;
  if (i >= ObTabletMetaTableRow::MAX_REPLICA_COUNT || 0 > i)
  {
    TBSYS_LOG(ERROR, "BUG invalid index, i=%d", i);
  }
  else
  {
    ObString cname;
    const int buf_len = meta_table_cname::STARTKEY_OBJ_PREFIX.length() + 8;
    char cname_buf[buf_len];
    cname_buf[0] = '\0';
    int print_len = 0;
    switch(cf)
    {
      case CF_TID:
        cname = COLUMNS_NAME[CIDX_TID];
        break;
      case CF_VERSION:
        cname = COLUMNS_NAME[CIDX_REPLICA1_VERSION + i * COLUMNS_PER_REPLICA];
        break;
      case CF_CHECKSUM:
        cname = COLUMNS_NAME[CIDX_REPLICA1_CHECKSUM + i * COLUMNS_PER_REPLICA];
        break;
      case CF_ROW_COUNT:
        cname = COLUMNS_NAME[CIDX_REPLICA1_ROW_COUNT + i * COLUMNS_PER_REPLICA];
        break;
      case CF_SIZE:
        cname = COLUMNS_NAME[CIDX_REPLICA1_SIZE + i * COLUMNS_PER_REPLICA];
        break;
      case CF_IPV4:
        cname = COLUMNS_NAME[CIDX_REPLICA1_IPV4 + i * COLUMNS_PER_REPLICA];
        break;
      case CF_IPV6_HIGH:
        cname = COLUMNS_NAME[CIDX_REPLICA1_IPV6_HIGH + i * COLUMNS_PER_REPLICA];
        break;
      case CF_IPV6_LOW:
        cname = COLUMNS_NAME[CIDX_REPLICA1_IPV6_LOW + i * COLUMNS_PER_REPLICA];
        break;
      case CF_PORT:
        cname = COLUMNS_NAME[CIDX_REPLICA1_IP_PORT + i * COLUMNS_PER_REPLICA];
        break;
      case CF_STARTKEY_COL:
        print_len = snprintf(cname_buf, buf_len, "%.*s%d", meta_table_cname::STARTKEY_OBJ_PREFIX.length(),
                           meta_table_cname::STARTKEY_OBJ_PREFIX.ptr(), i + 1);
        cname.assign_ptr(cname_buf, print_len);
        break;
      default:
        TBSYS_LOG(ERROR, "BUG invalid meta table column, cf=%d", cf);
        break;
    } // end switch

    if (OB_SUCCESS != get_cid_by_name(cname, ret))
    {
      TBSYS_LOG(ERROR, "meta table column not found, cname=%.*s tid=%lu",
                cname.length(), cname.ptr(), table_schema_.table_id_);
    }
  }
  return ret;
}

int64_t ObMetaTableColumnSchema::get_cidx(const CF cf, const int32_t i) const
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
      case CF_STARTKEY_COL:
        ret = ARRAYSIZEOF(COLUMNS_NAME) + i;
        break;
      default:
        TBSYS_LOG(ERROR, "BUG invalid meta table column, cf=%d", cf);
        break;
    } // end switch
  }
  return ret;
}

int64_t ObMetaTableColumnSchema::get_columns_num() const
{
  return ARRAYSIZEOF(COLUMNS_NAME) + table_schema_.rowkey_column_num_;
}

uint64_t ObMetaTableColumnSchema::get_cid_by_idx(const int64_t i) const
{
  uint64_t ret = OB_INVALID_ID;
  if (i >= get_columns_num() || 0 > i)
  {
    TBSYS_LOG(ERROR, "invalid column index, i=%ld tid=%lu", i, table_schema_.table_id_);
  }
  else
  {
    ObString cname;
    const int buf_len = meta_table_cname::STARTKEY_OBJ_PREFIX.length() + 8;
    char cname_buf[buf_len];
    cname_buf[0] = '\0';
    if (i < static_cast<int64_t>(ARRAYSIZEOF(COLUMNS_NAME)))
    {
      cname = COLUMNS_NAME[i];
    }
    else
    {
      // start_rowkey_obj1, start_rowkey_obj2, ...
      int len = snprintf(cname_buf, buf_len, "%.*s%ld", meta_table_cname::STARTKEY_OBJ_PREFIX.length(),
                         meta_table_cname::STARTKEY_OBJ_PREFIX.ptr(), i - ARRAYSIZEOF(COLUMNS_NAME) + 1);
      cname.assign_ptr(cname_buf, len);      
    }
    if (OB_SUCCESS != get_cid_by_name(cname, ret))
    {
      TBSYS_LOG(ERROR, "meta table column not found, cname=%.*s tid=%lu",
                cname.length(), cname.ptr(), table_schema_.table_id_);
    }
  }
  return ret;
}

int ObMetaTableColumnSchema::get_name_by_cid(const uint64_t cid, const char* &cname) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int i = 0; i < table_schema_.columns_.count(); ++i)
  {
    if (cid == table_schema_.columns_.at(i).column_id_)
    {
      ret = OB_SUCCESS;
      cname = table_schema_.columns_.at(i).column_name_;
      break;
    }
  }  
  return ret;
}

int ObMetaTableColumnSchema::get_cf(const uint64_t cid, CF &cf, int32_t &idx) const
{
  int ret = OB_SUCCESS;
  const char* cname_cstr = NULL;
  ObString cname;
  if (OB_SUCCESS != (ret = get_name_by_cid(cid, cname_cstr)))
  {
    TBSYS_LOG(WARN, "no column found by id, cid=%lu", cid);
  }
  else
  {
    cname.assign_ptr(const_cast<char*>(cname_cstr), static_cast<int32_t>(strlen(cname_cstr)));
    ret = OB_ENTRY_NOT_EXIST;
    for (uint64_t i = 0; i < ARRAYSIZEOF(COLUMNS_NAME); ++i)
    {
      if (COLUMNS_NAME[i] == cname)
      {
        ret = OB_SUCCESS;
        cf = CID_CF_IDX_MAP[i].cf;
        idx = CID_CF_IDX_MAP[i].idx;
        break;
      }
    }
    if (OB_SUCCESS != ret)
    {
      int val = 0;
      int len = sscanf(cname_cstr, SCOL_STARTKEY_OBJ_PREFIX_FORMAT, &val);
      if (1 == len
          && 1 <= val
          && val <= table_schema_.rowkey_column_num_)
      {
        ret = OB_SUCCESS;
        cf = ObTabletMetaTableColumnSchema::CF_STARTKEY_COL;
        idx = val - 1;          // index start from 0
      }
    }
  }
  return ret;
}

int64_t ObMetaTableColumnSchema::get_fixed_columns_num()
{
  return ARRAYSIZEOF(COLUMNS_NAME);
}

const ObString& ObMetaTableColumnSchema::get_fixed_cname_by_idx(const int64_t i)
{
  return COLUMNS_NAME[i];
}
