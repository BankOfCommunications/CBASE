/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_first_tablet_entry.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_FIRST_TABLET_ENTRY_H
#define _OB_FIRST_TABLET_ENTRY_H 1
#include <stdint.h>
#include "common/ob_get_param.h"
#include "common/ob_scanner.h"
#include "common/ob_mutator.h"
#include "common/ob_client_helper.h"
#include "common/ob_schema.h"
#include "common/ob_array.h"
#include "common/ob_cached_allocator.h"
#include "common/ob_schema_service.h"
#include "ob_tablet_meta_table.h"
#include "ob_scan_helper.h"
#include "ob_first_tablet_entry_schema.h"
#include "ob_mutate_helper.h"

class ObFirstTabletEntryTest_test_insert_Test;
class ObMetaTableTest;
class ObRootTableTest;
namespace oceanbase
{
  namespace common
  {
    class ObTabletInfo;
    // @note not thread-safe
    // 本类对象的生命周期要小于包含本对象的ObRootTable3对象
    // 处理中产生的其他临时对象在本对象析构时统一释放
    // @note FirstTabletEntryTable中不包含指向自己本身的表项
    class ObFirstTabletEntryTable: public ObTabletMetaTable
    {
      public:
        /// @note injected objects in the constructor should be thread-safe or thread-local
        ObFirstTabletEntryTable(ObSchemaService &schema_service, ObTabletMetaTable &my_meta, ObMutator &mutator, 
                                ObScanHelper &scan_helper, ObCachedAllocator<ObScanner> &scanner_allocator,
                                ObCachedAllocator<ObScanParam> &scan_param_allocator);
        virtual ~ObFirstTabletEntryTable();
        /** 
         * get table name and meta table name/id by table id
         * 
         * @param tid [in] table id
         * @param tname [out] table name
         * @param meta_tid [out] meta table id
         * @param meta_tname [out meta table name
         * 
         * @return OB_SUCCESS on success
         */        
        int get_meta_tid(const uint64_t tid, ObString &tname, uint64_t &meta_tid, ObString &meta_tname);

        virtual int search(const KeyRange& key_range, ConstIterator *&first);

        virtual int insert(const Value &v);
        virtual int erase(const Key &k);
        virtual int update(const Value &v);
        virtual int commit();

      public:
        struct MyRow;
      private:        
        struct MyIterator;
      private:
        void destroy();
        int cons_scan_param(const ObNewRange &range, ObScanParam &scan_param);
        int cons_scan_param(const ObRowkey &start_key, ObScanParam &scan_param);
        int aquire_scanner(ObScanner *&scanner);
        int aquire_iterator(ObScanner &scanner, MyIterator *&it);
        int get_location(ObScanner &scanner);
        int table_id_to_name(const uint64_t tid, ObString &name);
        int table_id_to_name_in_cache(const uint64_t tid, ObString &name);
        int table_id_to_name_by_scan(const uint64_t tid, ObString &name);
        int table_id_to_name_in_scanner(ObScanner& scanner, const uint64_t tid, ObString &name, bool &found);
        int check_state() const;
        int scan_row(const ObString &table_name, ObScanner *&out);
        int scan(const ObScanParam& scan_param, ObScanner &out) const;
        int get(ObGetParam& get_param, const ObString &table_name) const;
        int create_table(ObMutator& mutator, const ObTableSchema& table_schema,
                         const int32_t replicas_num, const ObServer* servers) const;
        int row_to_mutator(const Value &v, ObMutator &mutator);
        int replica_to_mutator(const ObTabletReplica &r, int64_t i);
        int cons_meta_table_schema(const uint64_t tid, const TableSchema &oschema, TableSchema &tschema) const;
        int create_meta_table(const ObString &indexed_tname, ObString &meta_tname, uint64_t &meta_tid);
        void fill_int_column(ColumnSchema &col_schema) const;
        int mutator_init_row(const ObString &tname, const uint64_t tid, ObMutator &mutator);
        int mutator_set_meta_tid_name(const ObString &tname, const uint64_t meta_tid,
                                      const ObString &meta_tname, ObMutator &mutator);
        int mutator_write_meta_table(const ObString &meta_tname, const uint64_t indexed_tid, ObMutator &mutator);
        // disallow copy
        ObFirstTabletEntryTable(const ObFirstTabletEntryTable &other);
        ObFirstTabletEntryTable& operator=(const ObFirstTabletEntryTable &other);
        // friends
        friend class ::ObFirstTabletEntryTest_test_insert_Test;
        friend class ::ObMetaTableTest;
        friend class ::ObRootTableTest;
      private:
        static ObFirstTabletEntryColumnSchema SCHEMA; // my schema
        // data members
        ObSchemaService &schema_service_; // injected
        ObTabletMetaTable &my_meta_; // injected, singleton
        ObMutator &mutator_; // injected, belongs to ObRootTable3
        ObScanHelper &scan_helper_;  // injected
        ObCachedAllocator<ObScanner> &scanner_allocator_;
        ObCachedAllocator<ObScanParam> &scan_param_allocator_;
        
        ObArray<ObScanner*> scanners_;
        ObArray<MyIterator*> iterators_;
        typedef std::pair<uint64_t, ObString> IdToName;
        ObArray<IdToName> id_name_map_;
        ObStringBuf string_pool_;
    };

    struct ObFirstTabletEntryTable::MyRow: public ObTabletMetaTableRow
    {
      public:
        MyRow():meta_tid_(OB_INVALID_ID){}
        ~MyRow(){}
        // getter & setters    
        const ObString& get_meta_table_name() const { return this->meta_table_name_; }
        void set_meta_table_name(const ObString &meta_table_name) { this->meta_table_name_ = meta_table_name; }
        const uint64_t& get_meta_tid() const { return this->meta_tid_; }
        void set_meta_tid(const uint64_t &meta_tid) { this->meta_tid_ = meta_tid; }
        const TableSchema::TableType& get_table_type() const { return this->table_type_; }
        void set_table_type(const TableSchema::TableType &table_type) { this->table_type_ = table_type; }
        
        int assign_cell(const int64_t column_id, const ObObj& value);
        int assign(const int64_t n, const ObCellInfo* row);
        int mutate(ObMutator& mutator) const;
        int create_table(const ObTableSchema& table_schema,
                         const int32_t replica_num, const ObServer* servers);
      private:
        TableSchema::TableType table_type_;
        ObString meta_table_name_;
        uint64_t meta_tid_;
    };


  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_FIRST_TABLET_ENTRY_H */

