/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROW_H
#define _OB_ROW_H 1
#include "common/ob_array.h"
#include "ob_raw_row.h"
#include "ob_row_desc.h"
#include "ob_rowkey.h"

namespace oceanbase
{
  namespace common
  {
    /// 行
    class ObRow
    {
      public:
        enum DefaultValue
        {
          DEFAULT_NULL,
          DEFAULT_NOP
        };

      public:
        ObRow();
        virtual ~ObRow();

        void clear()
        {
          raw_row_.clear();
          row_desc_ = NULL;
          //add lbzhong [Update rowkey] 20160530:b
          new_rowkey_ = NULL;
          is_new_row_ = false;
          //add:e
        }

        /// 赋值。浅拷贝，特别的，对于varchar类型不拷贝串内容
        void assign(const ObRow &other);
        ObRow(const ObRow &other);
        ObRow &operator= (const ObRow &other);
        /**
         * 设置行描述
         * 由物理操作符在构造一个新行对象时初始化。ObRow的功能要使用到ObRowDesc的功能，多个ObRow对象可以共享一个ObRowDesc对象。
         * @param row_desc
         */
        virtual void set_row_desc(const ObRowDesc &row_desc);
        /**
         * 获取行描述
         */
        const ObRowDesc* get_row_desc() const;
        /**
         * 根据表ID和列ID获得cell
         *
         * @param table_id
         * @param column_id
         * @param cell [out]
         *
         * @return
         */
        int get_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj *&cell) const;
        int get_cell(const uint64_t table_id, const uint64_t column_id, common::ObObj *&cell);
        /**
         * 设置指定列的值
         */
        int set_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj &cell);
        /**
         * 组成本行的列元素数
         */

        int64_t get_column_num() const;


        /**
         * 获得第cell_idx个cell
         */
        int raw_get_cell(const int64_t cell_idx, const common::ObObj *&cell,
            uint64_t &table_id, uint64_t &column_id) const;
        inline int raw_get_cell(const int64_t cell_idx, const common::ObObj* &cell) const;
        inline int raw_get_cell_for_update(const int64_t cell_idx, common::ObObj *&cell);

        /// 设置第cell_idx个cell
        int raw_set_cell(const int64_t cell_idx, const common::ObObj &cell);

        int get_rowkey(const ObRowkey *&rowkey) const;

        /**
         * 比较两个row的大小
         * @param row
         * @return
         * (1) if (this == row) return 0;
         * (2) if (this >  row) return POSITIVE_VALUE;
         * (3) if (this <  row) return NEGATIVE_VALUE;
         */
        int compare(const ObRow &row) const;

        /* dump row data */
        void dump() const;

        /* if skip_rowkey == true DO NOT reset rowkey */
        virtual int reset(bool skip_rowkey, enum DefaultValue default_value);

        /*
         * 通过action_flag_column判断这一行是否为空
         * 注意：没有action_flag_column的行都会返回false
         */
        int get_is_row_empty(bool &is_row_empty) const;
        // add by maosy // add by maosy [Delete_Update_Function_isolation_RC] 20161228
		/*拿到这一行的action_flag，如果没有action_flag这一列则返回错误，正常则返回ob_success和action_flag*/
        int get_action_flag(int64_t &action_flag) const;
        // add e
        int64_t to_string(char* buf, const int64_t buf_len) const;
        friend class ObCompactCellWriter;
        friend class ObRowFuse;
        //add lbzhong [Update rowkey] 20151221:b
        int set_new_rowkey(ObRawRow*& new_rowkey_row_);
        int get_new_rowkey(const common::ObObj *&cell) const;
        inline bool is_rowkey_updated() const
        {
          return (NULL != new_rowkey_);
        }
        void reset_new_rowkey() { new_rowkey_ = NULL; }
        ObRow(const ObRow &other, ModuleArena &row_alloc);
        void set_is_new_row(const bool is_new_row)
        {
          is_new_row_ = is_new_row;
        }
        bool is_new_row() const
        {
          return is_new_row_;
        }
        int64_t rowkey_to_string(char* buf, const int64_t buf_len) const;
        //add:e
        //add huangcc [Update rowkey] bug#1216 20161019:b
        void set_is_update_rowkey(bool is_update_rowkey)
        {
            is_update_rowkey_=is_update_rowkey;
        }
        bool is_update_rowkey() const
        {
            return is_update_rowkey_;
        }
        //add:e
      private:
        const ObObj* get_obj_array(int64_t& array_size) const;

      protected:
        ObRowkey rowkey_;
        ObRawRow raw_row_;
        const ObRowDesc *row_desc_;
        //add lbzhong [Update rowkey] 20151221:b
        const ObObj *new_rowkey_;
        bool is_new_row_; // new row
        //add:e
      //add huangcc [Update rowkey] bug#1216 20161019:b
      private:
        bool is_update_rowkey_;
      //add:e
    };

    inline const ObRowDesc* ObRow::get_row_desc() const
    {
      return row_desc_;
    }

    inline int64_t ObRow::get_column_num() const
    {
      int64_t ret = -1;
      if (NULL == row_desc_)
      {
        TBSYS_LOG(ERROR, "row_desc_ is NULL");
      }
      else
      {
        ret = row_desc_->get_column_num();
      }
      return ret;
    }

    inline void ObRow::set_row_desc(const ObRowDesc &row_desc)
    {
      if (row_desc.get_rowkey_cell_count() > 0)
      {
        rowkey_.assign(raw_row_.cells_, row_desc.get_rowkey_cell_count());
      }
      row_desc_ = &row_desc;
    }

    inline int ObRow::raw_get_cell_for_update(const int64_t cell_idx, common::ObObj *&cell)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(cell_idx >= get_column_num()))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid cell_idx=%ld cells_count=%ld", cell_idx, get_column_num());
      }
      else
      {
        ret = raw_row_.get_cell(cell_idx, cell);
      }
      return ret;
    }

    inline int ObRow::raw_get_cell(const int64_t cell_idx, const common::ObObj *&cell) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(cell_idx >= get_column_num()))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid cell_idx=%ld cells_count=%ld", cell_idx, get_column_num());
      }
      else
      {
        ret = raw_row_.get_cell(cell_idx, cell);
      }
      return ret;
    }

    inline const ObObj* ObRow::get_obj_array(int64_t& array_size) const
    {
      return raw_row_.get_obj_array(array_size);
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ROW_H */
