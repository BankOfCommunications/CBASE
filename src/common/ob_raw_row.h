/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_raw_row.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RAW_ROW_H
#define _OB_RAW_ROW_H 1
#include "common/ob_define.h"
#include "common/ob_object.h"
//add lbzhong[Update rowkey] 20160112:b
#include "page_arena.h"
#include "utility.h"
//add:e

namespace oceanbase
{
  namespace common
  {
    class ObRow;

    class ObRawRow
    {
      friend class ObRow;

      public:
        ObRawRow();
        ~ObRawRow();

        void assign(const ObRawRow &other);
        void clear();
        int add_cell(const common::ObObj &cell);

        int64_t get_cells_count() const;
        int get_cell(const int64_t i, const common::ObObj *&cell) const;
        int get_cell(const int64_t i, common::ObObj *&cell);
        int set_cell(const int64_t i, const common::ObObj &cell);

        //add lbzhong [Update rowkey] 20160112:b
        int deep_copy(const ObRawRow& src, ModuleArena &row_alloc);
        //add:e

      private:
        // types and constants
        static const int64_t MAX_COLUMNS_COUNT = common::OB_ROW_MAX_COLUMNS_COUNT;
      private:
        const ObObj* get_obj_array(int64_t& array_size) const;
        // disallow copy
        ObRawRow(const ObRawRow &other);
        ObRawRow& operator=(const ObRawRow &other);
        // function members
      private:
        // data members
        char cells_buffer_[MAX_COLUMNS_COUNT * sizeof(ObObj)];
        common::ObObj *cells_;
        int16_t cells_count_;
        int16_t reserved1_;
        int32_t reserved2_;
    };

    inline int ObRawRow::get_cell(const int64_t i, const common::ObObj *&cell) const
    {
      int ret = common::OB_SUCCESS;
      if (0 > i || i >= MAX_COLUMNS_COUNT)
      {
        TBSYS_LOG(WARN, "invalid index, count=%hd i=%ld", cells_count_, i);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        cell = &cells_[i];
      }
      return ret;
    }

    inline int ObRawRow::get_cell(const int64_t i, common::ObObj *&cell)
    {
      int ret = common::OB_SUCCESS;
      if (0 > i || i >= MAX_COLUMNS_COUNT)
      {
        TBSYS_LOG(WARN, "invalid index, count=%hd i=%ld", cells_count_, i);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        cell = &cells_[i];
      }
      return ret;
    }

    inline int ObRawRow::set_cell(const int64_t i, const common::ObObj &cell)
    {
      int ret = common::OB_SUCCESS;
      if (0 > i || i >= MAX_COLUMNS_COUNT)
      {
        TBSYS_LOG(WARN, "invalid index, count=%ld i=%ld", MAX_COLUMNS_COUNT, i);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        cells_[i] = cell;
        if (i >= cells_count_)
        {
          cells_count_ = static_cast<int16_t>(1+i);
        }
      }
      return ret;
    }

    inline int64_t ObRawRow::get_cells_count() const
    {
      return cells_count_;
    }

    inline void ObRawRow::clear()
    {
      cells_count_ = 0;
    }

    inline const ObObj* ObRawRow::get_obj_array(int64_t& array_size) const
    {
      array_size = cells_count_;
      return cells_;
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_RAW_ROW_H */

