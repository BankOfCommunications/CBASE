/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_tablet_meta_table.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_META_TABLE_H
#define _OB_TABLET_META_TABLE_H 1
#include <stdint.h>
#include "common/ob_range2.h"
#include "ob_tablet_meta_table_row.h"
namespace oceanbase
{
  namespace common
  {
    class ObTabletMetaTable;
    class ObTabletMetaTableRowIterator
    {
      public:
        ObTabletMetaTableRowIterator() {}
        virtual ~ObTabletMetaTableRowIterator() {}
        /// get the next element
        /// @return OB_SUCCESS when get next row successfully
        /// - OB_ITER_END when no next row
        /// - or error code
        virtual int next(const ObTabletMetaTableRow *&row) = 0;
        /// set the interator to the beginning        
        virtual int rewind() = 0;
      private:
        // disallow copy
        ObTabletMetaTableRowIterator(const ObTabletMetaTableRowIterator &other);
        ObTabletMetaTableRowIterator& operator=(const ObTabletMetaTableRowIterator &other);
    };

    class ObTabletMetaTable
    {
      public:
        typedef ObTabletMetaTableKey Key;
        typedef common::ObNewRange KeyRange;
        typedef ObTabletMetaTableRow Value;
        typedef ObTabletMetaTableRowIterator ConstIterator;
      public:
        ObTabletMetaTable(){}
        virtual ~ObTabletMetaTable(){}
        
        /// search for the tablet range which contains key_range
        /// @return OB_SUCCESS even when the result is empty, test empty using first->has_next()
        virtual int search(const KeyRange& key_range, ConstIterator *&first) = 0;

        virtual int insert(const Value &v) = 0;
        virtual int erase(const Key &k) = 0;
        virtual int update(const Value &v) = 0;
        /// commit all the mutations since the last commitment or cancel
        virtual int commit() = 0;
      private:
        // disallow copy
        ObTabletMetaTable(const ObTabletMetaTable &other);
        ObTabletMetaTable& operator=(const ObTabletMetaTable &other);
      protected:
        int check_range(const KeyRange& key_range) const;
    };

    inline int ObTabletMetaTable::check_range(const KeyRange& key_range) const
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == key_range.table_id_
          || OB_FIRST_META_VIRTUAL_TID == key_range.table_id_
          || OB_NOT_EXIST_TABLE_TID == key_range.table_id_)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = OB_INVALID_ARGUMENT;
        if (key_range.start_key_ < key_range.end_key_)
        {
          ret = OB_SUCCESS;
        }
        else if (key_range.start_key_ == key_range.end_key_)
        {
          if (key_range.border_flag_.inclusive_start() 
              && key_range.border_flag_.inclusive_end())
          {
            ret = OB_SUCCESS;
          }
        }
      }
      return ret;
    }

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_TABLET_META_TABLE_H */

