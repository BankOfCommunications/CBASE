/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_tablet_meta_table_row.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_META_TABLE_ROW_H
#define _OB_TABLET_META_TABLE_ROW_H 1
#include "common/ob_define.h"
#include "common/ob_rowkey.h"
#include "common/ob_server.h"
namespace oceanbase
{
  namespace common
  {
    class ObTabletReportInfo;
    class ObScanner;
    
    struct ObTabletReplica
    {
      public:
        // data members
        int64_t version_;
        common::ObServer cs_;
        int64_t row_count_;
        int64_t occupy_size_;
        uint64_t checksum_;        
      public:
        ObTabletReplica();
        ObTabletReplica(const common::ObTabletReportInfo& tablet);
        ~ObTabletReplica();
        // serialization
        NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTabletMetaTableKey
    {
      uint64_t table_id_;
      common::ObRowkey rowkey_;

      ObTabletMetaTableKey()
        :table_id_(OB_INVALID_ID)
      {
      }
    };
    
    struct ObTabletMetaTableRow
    {
      public:
        ObTabletMetaTableRow();
        virtual ~ObTabletMetaTableRow();
        // getter and setters        
        const uint64_t& get_tid() const { return this->tid_; }
        void set_tid(const uint64_t &tid) { this->tid_ = tid; modified_mask_ |= MODIFIED_MASK_TID;}
        const ObString& get_table_name() const { return this->table_name_; }
        void set_table_name(const ObString &table_name) { this->table_name_ = table_name; } // @note no modified mask on purpose
        const common::ObRowkey& get_end_key() const { return this->end_key_; }
        void set_end_key(const common::ObRowkey &end_key) { this->end_key_ = end_key;}
        const common::ObRowkey& get_start_key() const { return this->start_key_; }
        void set_start_key(const common::ObRowkey &start_key) { this->start_key_ = start_key; modified_mask_ |= MODIFIED_MASK_STARTKEY;}
        const ObTabletReplica& get_replica(const int32_t i) const { return this->replicas_[i]; }
        void set_replica(const int32_t i, const ObTabletReplica &replica) { this->replicas_[i] = replica; modified_mask_ |= MODIFIED_MASK_REPLICA[i];}
        // whether some field has been changed
        bool has_changed() const;
        bool has_changed_tid() const;
        bool has_changed_start_key() const;
        bool has_changed_end_key() const;
        bool has_changed_replica(const int32_t i) const;        
        void clear_changed();
        /// update this row with the other one
        /// @note only the changed fields of `other' will be updated
        void update(const ObTabletMetaTableRow &other);

        int add_replica(const ObTabletReplica &replica);
        int add_into_scanner_as_old_roottable(ObScanner &scanner) const;
        // serialization
        NEED_SERIALIZE_AND_DESERIALIZE;
        // 
        static int32_t get_max_replica_count() { return MAX_REPLICA_COUNT; }
        static const int32_t MAX_REPLICA_COUNT = OB_TABLET_MAX_REPLICA_COUNT;
      protected:
        static const uint64_t MODIFIED_MASK_ENDKEY = 1;
        static const uint64_t MODIFIED_MASK_TID = 1 << 1;
        static const uint64_t MODIFIED_MASK_STARTKEY = 1 << 2;
        static const uint64_t MODIFIED_MASK_REPLICA[MAX_REPLICA_COUNT]; // ={1<<3, ..., 1<<8}
      public:
        // data members
        uint64_t modified_mask_;
        common::ObRowkey end_key_; // key
        uint64_t tid_;             // key
        ObString table_name_;
        common::ObRowkey start_key_;
        ObTabletReplica replicas_[MAX_REPLICA_COUNT];
    };

    struct ObTabletMetaTableColumnSchema
    {
      public:
        enum CF
        {
          CF_IPV4 = 0,
          CF_IPV6_HIGH,
          CF_IPV6_LOW,
          CF_PORT,
          CF_VERSION,
          CF_ROW_COUNT,
          CF_SIZE,
          CF_CHECKSUM,          // 7
          CF_TID,
          CF_STARTKEY_COL,
          CF_TABLE_TYPE,
          CF_META_TID,
          CF_META_TNAME,
        };
        virtual uint64_t get_cid(const CF cf, const int32_t i) const = 0;
        virtual int64_t get_cidx(const CF cf, const int32_t i) const = 0;

        virtual int64_t get_columns_num() const = 0;
        virtual uint64_t get_cid_by_idx(const int64_t i) const = 0;

        virtual int get_cf(const uint64_t cid, CF &cf, int32_t &i) const = 0; // for testing
      public:
        ObTabletMetaTableColumnSchema(){}
        virtual ~ObTabletMetaTableColumnSchema(){}
    };
  } // end namespace common
} // end namespace oceanbase
#endif /* _OB_TABLET_META_TABLE_ROW_H */

