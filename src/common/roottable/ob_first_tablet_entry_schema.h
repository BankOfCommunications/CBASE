/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_first_tablet_entry_schema.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_FIRST_TABLET_ENTRY_SCHEMA_H
#define _OB_FIRST_TABLET_ENTRY_SCHEMA_H 1
#include "ob_tablet_meta_table_row.h"
namespace oceanbase
{
  namespace common
  {
    struct ObFirstTabletEntryColumnSchema: public ObTabletMetaTableColumnSchema
    {
      public:
        ObFirstTabletEntryColumnSchema();
        virtual ~ObFirstTabletEntryColumnSchema();

        virtual uint64_t get_cid(const CF cf, const int32_t i) const;
        virtual int64_t get_cidx(const CF cf, const int32_t i) const;

        virtual int64_t get_columns_num() const;
        virtual uint64_t get_cid_by_idx(const int64_t i) const;

        virtual int get_cf(const uint64_t cid, CF &cf, int32_t &i) const;
    };

    namespace first_tablet_entry_cid
    {
      static const uint64_t TNAME = 16;
      static const uint64_t CREATE_TIME_COLUMN_ID = 17;
      static const uint64_t MODIFY_TIME_COLUMN_ID = 18;
      static const uint64_t TID = 19;
      static const uint64_t TABLE_TYPE = 20;
      static const uint64_t META_TNAME = 21;
      static const uint64_t META_TID = 22;
      static const uint64_t LOAD_TYPE = 23;
      static const uint64_t TABLE_DEF_TYPE = 24;
      static const uint64_t ROWKEY_COLUMN_NUM = 25;
      static const uint64_t COLUMN_NUM = 26;
      static const uint64_t MAX_USED_COLUMN_ID = 27;
      static const uint64_t REPLICA_NUM = 28;
      static const uint64_t CREATE_MEM_VERSION = 29;
      static const uint64_t TABLET_MAX_SIZE = 30;
      static const uint64_t MAX_ROWKEY_LENGTH_ID = 31;
      static const uint64_t COMPRESS_FUNC_NAME_ID = 32;
      static const uint64_t EXPIRE_INFO_COLUMN_ID = 33;
      static const uint64_t EXPIRE_INFO_DURATION_ID = 34;
      static const uint64_t USE_BLOOMFILTER_ID = 35;
      static const uint64_t MERGE_WRITE_SSTABLE_VERSION = 36;
      static const uint64_t PURE_UPDATE_TABLE_ID = 37;
      static const uint64_t ROWKEY_SPLIT_ID = 38;
      static const uint64_t EXPIRE_CONDITION_ID = 39;
      static const uint64_t SSTABLE_BLOCK_SIZE_ID = 40;
      //static const uint64_t CONSISTENCY_LEVEL_ID = 41;
      static const uint64_t READ_STATIC_ID = 41;
      static const uint64_t COMMENT_STR_ID = 42;
      static const uint64_t SCHEMA_VERSION_ID = 43;
      //add wenghaixing [secondary  index] 20141105
      static const uint64_t TABLE_ID_COLUMN_ID=44;
      static const uint64_t INDEX_STATUS_ID=45;
      static const uint64_t DBNAME = 46; //add dolphin [database manager]@20150609
      //add e
      /// UNUSED ////
      static const uint64_t REPLICA1_IPV4 = 51;
      static const uint64_t REPLICA1_IPV6_HIGH = 52;
      static const uint64_t REPLICA1_IPV6_LOW = 53;
      static const uint64_t REPLICA1_IP_PORT = 54;
      static const uint64_t REPLICA1_VERSION = 55;
      static const uint64_t REPLICA1_ROW_COUNT = 56;
      static const uint64_t REPLICA1_SIZE = 57;
      static const uint64_t REPLICA1_CHECKSUM = 58;

      static const uint64_t REPLICA2_IPV4 = REPLICA1_IPV4 + 10;
      static const uint64_t REPLICA2_IPV6_HIGH = REPLICA1_IPV6_HIGH + 10;
      static const uint64_t REPLICA2_IPV6_LOW = REPLICA1_IPV6_LOW + 10;
      static const uint64_t REPLICA2_IP_PORT = REPLICA1_IP_PORT + 10;
      static const uint64_t REPLICA2_VERSION = REPLICA1_VERSION + 10;
      static const uint64_t REPLICA2_ROW_COUNT = REPLICA1_ROW_COUNT + 10;
      static const uint64_t REPLICA2_SIZE = REPLICA1_SIZE + 10;
      static const uint64_t REPLICA2_CHECKSUM = REPLICA1_CHECKSUM + 10;

      static const uint64_t REPLICA3_IPV4 = REPLICA1_IPV4 + 20;
      static const uint64_t REPLICA3_IPV6_HIGH = REPLICA1_IPV6_HIGH + 20;
      static const uint64_t REPLICA3_IPV6_LOW = REPLICA1_IPV6_LOW + 20;
      static const uint64_t REPLICA3_IP_PORT = REPLICA1_IP_PORT + 20;
      static const uint64_t REPLICA3_VERSION = REPLICA1_VERSION + 20;
      static const uint64_t REPLICA3_ROW_COUNT = REPLICA1_ROW_COUNT + 20;
      static const uint64_t REPLICA3_SIZE = REPLICA1_SIZE + 20;
      static const uint64_t REPLICA3_CHECKSUM = REPLICA1_CHECKSUM + 20;

      static const uint64_t REPLICA4_IPV4 = REPLICA1_IPV4 + 30;
      static const uint64_t REPLICA4_IPV6_HIGH = REPLICA1_IPV6_HIGH + 30;
      static const uint64_t REPLICA4_IPV6_LOW = REPLICA1_IPV6_LOW + 30;
      static const uint64_t REPLICA4_IP_PORT = REPLICA1_IP_PORT + 30;
      static const uint64_t REPLICA4_VERSION = REPLICA1_VERSION + 30;
      static const uint64_t REPLICA4_ROW_COUNT = REPLICA1_ROW_COUNT + 30;
      static const uint64_t REPLICA4_SIZE = REPLICA1_SIZE + 30;
      static const uint64_t REPLICA4_CHECKSUM = REPLICA1_CHECKSUM + 30;

      static const uint64_t REPLICA5_IPV4 = REPLICA1_IPV4 + 40;
      static const uint64_t REPLICA5_IPV6_HIGH = REPLICA1_IPV6_HIGH + 40;
      static const uint64_t REPLICA5_IPV6_LOW = REPLICA1_IPV6_LOW + 40;
      static const uint64_t REPLICA5_IP_PORT = REPLICA1_IP_PORT + 40;
      static const uint64_t REPLICA5_VERSION = REPLICA1_VERSION + 40;
      static const uint64_t REPLICA5_ROW_COUNT = REPLICA1_ROW_COUNT + 40;
      static const uint64_t REPLICA5_SIZE = REPLICA1_SIZE + 40;
      static const uint64_t REPLICA5_CHECKSUM = REPLICA1_CHECKSUM + 40;

      static const uint64_t REPLICA6_IPV4 = REPLICA1_IPV4 + 50;
      static const uint64_t REPLICA6_IPV6_HIGH = REPLICA1_IPV6_HIGH + 50;
      static const uint64_t REPLICA6_IPV6_LOW = REPLICA1_IPV6_LOW + 50;
      static const uint64_t REPLICA6_IP_PORT = REPLICA1_IP_PORT + 50;
      static const uint64_t REPLICA6_VERSION = REPLICA1_VERSION + 50;
      static const uint64_t REPLICA6_ROW_COUNT = REPLICA1_ROW_COUNT + 50;
      static const uint64_t REPLICA6_SIZE = REPLICA1_SIZE + 50;
      static const uint64_t REPLICA6_CHECKSUM = REPLICA1_CHECKSUM + 50;
    } // end namespace first_tablet_entry_schema
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_FIRST_TABLET_ENTRY_SCHEMA_H */
