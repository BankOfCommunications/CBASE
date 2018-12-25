/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         ob_multi_cg_scanner.h is for what ...
 *
 *  Version: $Id: ob_multi_cg_scanner.h 09/13/2012 02:27:28 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#ifndef __OCEANBASE_SQL_OB_MULTI_CG_SCANNER_H_
#define __OCEANBASE_SQL_OB_MULTI_CG_SCANNER_H_

#include "common/ob_define.h"
#include "common/ob_row.h"

namespace oceanbase
{
  namespace common
  {
    class ObRowkey;
  }

  namespace sql
  {
    // interface of iterator
    class ObRowIterator
    {
      public:
        ObRowIterator()
        {
          // empty
        }
        virtual ~ObRowIterator()
        {
          // empty
        }
      public:
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const = 0;
        /**
         * get current row and move to next.
         * @return OB_SUCCESS if has more rows or OB_ITER_END if has no data.
         */
        virtual int get_next_row(const common::ObRow *&row) = 0;
    };

    class ObRowkeyIterator : public ObRowIterator
    {
      public:
        ObRowkeyIterator()
        {
        }
        virtual ~ObRowkeyIterator()
        {
        }
      public:
        virtual int get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row) = 0;
        virtual int get_next_row(const common::ObRow *&row)
        {
          row = NULL;
          return common::OB_NOT_SUPPORTED;
        }
    };


    class ObMultiColumnGroupScanner : public ObRowkeyIterator
    {
      public:
        ObMultiColumnGroupScanner();
        ~ObMultiColumnGroupScanner();
        void reset();
      public:
        virtual int get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row) ;
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const ;
      public:
        int add_row_iterator(ObRowkeyIterator* iterator);
        int cons_row_desc(ObRowkeyIterator* iterator);
        int cons_row(int64_t & cur_cell_num, const common::ObRow& row);
      private:
        static const int64_t MAX_ITER_NUM = common::OB_MAX_COLUMN_GROUP_NUMBER;
        ObRowkeyIterator* iters_[MAX_ITER_NUM];
        int64_t iter_num_;
        common::ObRowDesc row_desc_;
        common::ObRow current_row_;
    };
  }
}

#endif //__OB_ROW_ITERATOR_H__

