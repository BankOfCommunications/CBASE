/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_meta_table_schema.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_META_TABLE_SCHEMA_H
#define _OB_META_TABLE_SCHEMA_H 1
#include "common/ob_string.h"
#include "ob_tablet_meta_table_row.h"
namespace oceanbase
{
  namespace common
  {
    struct TableSchema;
    struct ObMetaTableColumnSchema: public ObTabletMetaTableColumnSchema
    {
      public:
        ObMetaTableColumnSchema(const TableSchema &table_schema);
        ~ObMetaTableColumnSchema();

        virtual uint64_t get_cid(const CF cf, const int32_t i) const;
        virtual int64_t get_cidx(const CF cf, const int32_t i) const;

        virtual int64_t get_columns_num() const;
        virtual uint64_t get_cid_by_idx(const int64_t i) const;

        virtual int get_cf(const uint64_t cid, CF &cf, int32_t &i) const; // for testing

        static int64_t get_fixed_columns_num();
        static const ObString& get_fixed_cname_by_idx(const int64_t i);
      private:
        int get_cid_by_name(const ObString &cname, uint64_t &cid) const;
        int get_name_by_cid(const uint64_t cid, const char* &cname) const;
      private:
        const TableSchema &table_schema_;
    };

    namespace meta_table_cname
    {
      extern const ObString TID;
      extern const ObString IPV4[OB_TABLET_MAX_REPLICA_COUNT];
      extern const ObString IPV6_HIGH[OB_TABLET_MAX_REPLICA_COUNT];
      extern const ObString IPV6_LOW[OB_TABLET_MAX_REPLICA_COUNT];
      extern const ObString PORT[OB_TABLET_MAX_REPLICA_COUNT];
      extern const ObString TABLET_VERSION[OB_TABLET_MAX_REPLICA_COUNT];
      extern const ObString ROW_COUNT[OB_TABLET_MAX_REPLICA_COUNT];
      extern const ObString OCCUPY_SIZE[OB_TABLET_MAX_REPLICA_COUNT];
      extern const ObString CHECKSUM[OB_TABLET_MAX_REPLICA_COUNT];
      extern const ObString STARTKEY_OBJ_PREFIX;
      extern const ObString ENDKEY_OBJ_PREFIX;
    } // end namespace meta_table_columns_name
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_META_TABLE_SCHEMA_H */

