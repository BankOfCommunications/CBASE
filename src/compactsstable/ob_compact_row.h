/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_compact_row.h is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#ifndef OB_COMPACT_ROW_H
#define OB_COMPACT_ROW_H

#include "common/ob_define.h"
#include "common/ob_object.h"
#include "common/ob_common_param.h"
#include "ob_compact_obj.h"

namespace oceanbase
{
  namespace compactsstable
  {
    class ObCompactRow
    {
    public:
      static const uint64_t MAX_COL_ID           =  common::OB_ALL_MAX_COLUMN_ID;
      static const int64_t  COL_ID_SIZE          =  2; //column id use uint16_t
      static const int64_t  MAX_COL_NUM          =  1024L;
      static const int64_t  MAX_BUF_SIZE         =  2 * 1024 * 1024; //2M
      static const uint16_t ROWKEY_COLUMN_ID     =  1;
      static const int32_t  ROWKEY_COLUMN_NUM    =  1;

    public:
      ObCompactRow();
      ~ObCompactRow();
    public:
      int  add_col(const uint64_t col_id,const common::ObObj& value);
      int  set_end();
      void reset();

    public:
      int set_row(const char* row,int64_t row_len);
      //before rowkey split
      int get_row_key(common::ObRowkey& rowkey);
        
    public:
      void reset_iter();
      int  next_cell();
      int  get_cell(common::ObCellInfo** cell);
      int  get_cell(common::ObCellInfo** cell,bool* is_row_changed);

    public:
      int  copy(const ObCompactRow& row);      
      int  copy_key(char* buf,const int64_t buf_len,int64_t& pos,int32_t key_col_nums);
      int  copy_value(char* buf,const int64_t buf_len,int64_t& pos,int32_t key_col_nums);
      
    public:
      void    set_key_col_num(int32_t key_col_num);
      int32_t get_col_num() const {return col_num_;}
      int64_t get_row_size() const {return buf_pos_;}

      int64_t get_key_columns_len(const int32_t key_col_nums) const;
      int64_t get_value_columns_len(const int32_t key_col_nums) const;

      const char* get_key_buffer() const;
      const char* get_value_buffer(int32_t key_col_nums) const;

    public:
      static int compare(ObCompactRow& left,ObCompactRow& right,int32_t key_col_num = 1);

    private:
      int32_t calc_col_num();

    private:
      DISALLOW_COPY_AND_ASSIGN(ObCompactRow);

    private:
      int32_t       buf_pos_;
      int32_t       iter_pos_;
      int32_t       iter_start_;
      int32_t       iter_end_;

      bool          self_alloc_;
      bool          is_end_;

      int32_t       col_num_;
      int32_t       key_col_num_;
      int32_t       key_col_len_;
      char*         buf_;
      common::ObCellInfo    cur_cell_;
    };
  }
}

#endif
