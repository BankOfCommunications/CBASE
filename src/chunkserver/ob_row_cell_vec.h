/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_row_cell_vec.h for manage row cells. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_ROW_CELL_VEC_H_ 
#define OCEANBASE_CHUNKSERVER_ROW_CELL_VEC_H_

#include "common/ob_common_param.h"
#include "common/ob_iterator.h"
#include "common/ob_rowkey.h"
#include "common/ob_schema.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObRowCellVec : public common::ObIterator
    {
    public:
      ObRowCellVec();
      virtual ~ObRowCellVec();

      int init(); 

      bool is_empty();

      int add_cell(const common::ObCellInfo &cell);

      inline const oceanbase::common::ObRowkey& get_row_key()const
      {
        return row_key_;
      } 

      uint64_t get_table_id()const
      {
        return NULL == row_ ? 0 : row_->table_id_;
      }

      inline int64_t get_cell_num() const
      {
        return NULL == row_ ? 0: row_->cell_num_;
      }

      void reset_iterator();
      virtual int next_cell();
      virtual int get_cell(common::ObCellInfo** cell);
      virtual int get_cell(common::ObCellInfo** cell, bool* is_row_changed);

      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; 
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos); 
      int64_t get_serialize_size(void) const; 

    private:
      struct ObMsCellInfo
      {
        uint64_t column_id_;
        common::ObObj value_;
      };
      struct ObRow
      {
        int32_t cell_num_;
        int32_t row_key_len_;
        uint64_t table_id_;
        common::ObObj row_key_obj_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
        ObMsCellInfo cells_[0];
      };
      char    *row_buffer_;
      int32_t row_buffer_size_;
      int32_t cur_buffer_offset_; 
      ObRow   *row_;
      oceanbase::common::ObRowkey row_key_;
      int32_t   consumed_cell_num_;
      bool      is_empty_;
      common::ObCellInfo cur_cell_;
    };
  }
}

#endif //OCEANBASE_CHUNKSERVER_ROW_CELL_VEC_H_
