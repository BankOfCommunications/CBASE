/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_table3.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_TABLE3_H
#define _OB_ROOT_TABLE3_H 1
#include "ob_tablet_meta_table.h"
#include "ob_first_tablet_entry.h"
#include "ob_first_tablet_entry_meta.h"
#include "ob_meta_table3.h"
#include "ob_scan_helper.h"
class ObRootTableTest;

namespace oceanbase
{
  namespace common
  {
    // @note not thread-safe
    // 生命周期：每个事务开始时构造，结束时析构
    class ObRootTable3: public ObTabletMetaTable
    {
      public:
        /// @note injected objects in the constructor should be thread-safe or thread-local
        ObRootTable3(ObFirstTabletEntryMeta& the_meta, ObSchemaService &schema_service, 
                     ObScanHelper &scan_helper, ObCachedAllocator<ObScanner> &scanner_allocator,
                     ObCachedAllocator<ObScanParam> &scan_param_allocator);
        virtual ~ObRootTable3();
        
        virtual int search(const KeyRange& key_range, ConstIterator *&first);

        virtual int insert(const Value &v);
        virtual int erase(const Key &k);
        virtual int update(const Value &v);
        virtual int commit();
      private:
        int search(const KeyRange& key_range, ConstIterator *&first, int32_t recursive_level);
        int aquire_meta_table_for_mutation(const uint64_t tid, ObTabletMetaTable* &meta_table);
        // disallow copy
        ObRootTable3(const ObRootTable3 &other);
        ObRootTable3& operator=(const ObRootTable3 &other);
        // friends
        friend class ::ObRootTableTest;
      private:
        static const int32_t MAX_RECURSIVE_LEVEL = 2;
        // data members
        ObFirstTabletEntryMeta& the_meta_; // injected, the only meta known when start-up
        ObScanHelper &scan_helper_; // injected        
        ObMutator mutator_;
        ObFirstTabletEntryTable first_table_;
        ObMetaTable3 meta_table_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ROOT_TABLE3_H */

