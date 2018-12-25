/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_first_tablet_entry_meta.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_FIRST_TABLET_ENTRY_META_H
#define _OB_FIRST_TABLET_ENTRY_META_H 1

#include "ob_tablet_meta_table.h"
#include <tbsys.h>
namespace oceanbase
{
  namespace common
  {
    // @note used as a singleton
    // multi-readers & one writer using COW
    class ObFirstTabletEntryMeta : public ObTabletMetaTable
    {
      public:
        ObFirstTabletEntryMeta(const int32_t max_thread_count, const char* filename);
        virtual ~ObFirstTabletEntryMeta();
        int init();
        int init(const Value &v);

        virtual int search(const KeyRange& key_range, ConstIterator *&first);

        virtual int insert(const Value &v);
        virtual int erase(const Key &k);
        virtual int update(const Value &v);
        virtual int commit();
        
      private:
        struct MyIterator;
        struct RefCountRow;
      private:
        int check_integrity();
        int aquire_iterator(MyIterator *&it);
        int aquire_write_handle();
        void commit_write_handle();
        int check_first_meta_row(const Value &v);
        int set_filename(const char* filename);
        int load(const char* filename, ObTabletMetaTableRow &row);
        int store(const ObTabletMetaTableRow &row, const char* filename);
        int backup(const char* filename);
        // disallow copy
        ObFirstTabletEntryMeta(const ObFirstTabletEntryMeta &other);
        ObFirstTabletEntryMeta& operator=(const ObFirstTabletEntryMeta &other);
      private:
        static const uint32_t INVALID_HANDLE = UINT32_MAX;
        // data members
        char filename_[OB_MAX_FILE_NAME_LENGTH];
        uint32_t meta_array_size_;
        volatile uint32_t read_handle_;
        uint32_t write_handle_;
        RefCountRow *first_meta_; // the only one row
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_FIRST_TABLET_ENTRY_META_H */

