/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_meta_table3.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_META_TABLE3_H
#define _OB_META_TABLE3_H 1
#include "common/ob_string_buf.h"
#include "common/ob_array.h"
#include "common/ob_schema_service.h"
#include "common/ob_cached_allocator.h"
#include "ob_tablet_meta_table.h"
#include "ob_scan_helper.h"
namespace oceanbase
{
  namespace common
  {
    class ObScanner;
    class ObMutator;
    class ObScanParam;
    // @note not thread-safe
    // 本类对象的生命周期要小于包含本对象的ObRootTable3对象
    // 处理中产生的其他临时对象如Scanner,Iterator等在本对象析构时统一释放
    class ObMetaTable3: public common::ObTabletMetaTable
    {
      public:
        /// @note injected objects in the constructor should be thread-safe or thread-local
        ObMetaTable3(ObSchemaService &schema_service, ObMutator &mutator,
                     ObScanHelper &scan_helper, ObCachedAllocator<ObScanner> &scanner_allocator,
                     ObCachedAllocator<ObScanParam> &scan_param_allocator);
        virtual ~ObMetaTable3();

        /// set extra arguments for the following call
        /// called before calling search/insert/erase/update
        void reset(ConstIterator *meta_it, const uint64_t tid, const ObString &tname, 
                   const uint64_t indexed_tid, const ObString &indexed_tname);

        /// @pre call reset() first to set other parameters
        virtual int search(const KeyRange& key_range, ConstIterator *&first);

        virtual int insert(const Value &v);
        virtual int erase(const Key &k);
        virtual int update(const Value &v);
        virtual int commit();
      private:
        struct MyIterator;        
      private:
        int check_tid(const uint64_t op_tid);
        int aquire_scanner(ObScanner *&scanner);
        int aquire_iterator(ConstIterator &meta_it, const KeyRange &search_range, 
                            ObScanner& scanner, MyIterator *&iterator);
        int scan(const ObScanParam& scan_param, ObScanner &out) const;
        int get_table_schema(const ObString &tname, TableSchema &schema);
        void destroy();
        int row_to_mutator(const TableSchema &table_schema, const Value &v, const bool is_insert, ObMutator &mutator);
        // disallow copy
        ObMetaTable3(const ObMetaTable3 &other);
        ObMetaTable3& operator=(const ObMetaTable3 &other);
      private:
        static const int64_t SCAN_LIMIT_COUNT = 128;
        // data members
        ObSchemaService &schema_service_; // injected
        ObMutator& mutator_;    // injected, belongs to ObRootTable3
        ObScanHelper &scan_helper_;  // injected
        ObCachedAllocator<ObScanner> &scanner_allocator_; // injected
        ObCachedAllocator<ObScanParam> &scan_param_allocator_; // injected
        
        ConstIterator *my_meta_;
        uint64_t my_tid_;
        ObString my_tname_;
        uint64_t indexed_tid_;
        ObString indexed_tname_;
        ObArray<ObScanner*> scanners_;
        ObArray<MyIterator*> iterators_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_META_TABLE3_H */

