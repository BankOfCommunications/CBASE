/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_desc_ext.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROW_DESC_EXT_H
#define _OB_ROW_DESC_EXT_H 1
#include "ob_row_desc.h"
#include "ob_raw_row.h"

namespace oceanbase
{
  namespace common
  {
    //行描述扩展 ObRowDesc with column data type information
    class ObRowDescExt
    {
      public:
        ObRowDescExt();
        ~ObRowDescExt();

        ObRowDescExt(const ObRowDescExt &other);
        ObRowDescExt& operator=(const ObRowDescExt &other);
        void reset();
        int get_by_id(const uint64_t table_id, const uint64_t column_id, int64_t &idx, ObObj &data_type) const;
        int get_by_idx(const int64_t idx, uint64_t &table_id, uint64_t &column_id, ObObj &data_type) const;
        int64_t get_column_num() const;
        
        int add_column_desc(const uint64_t table_id, const uint64_t column_id, const ObObj &data_type);
      private:
        // data members
        ObRowDesc row_desc_;
        ObRawRow data_type_;
    };

    inline int64_t ObRowDescExt::get_column_num() const
    {
      return row_desc_.get_column_num();
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ROW_DESC_EXT_H */
