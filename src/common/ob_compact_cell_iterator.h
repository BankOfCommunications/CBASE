/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_compact_cell_iterator.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef OCEANBASE_COMMON_COMPACT_CELL_ITERATOR_H_
#define OCEANBASE_COMMON_COMPACT_CELL_ITERATOR_H_

#include "ob_buffer_helper.h"
#include "ob_cell_meta.h"
#include "ob_object.h"
#include "ob_compact_store_type.h"

namespace oceanbase
{
  namespace common
  {
    /*
     * 紧凑格式读取类
     */
    class ObCompactCellIterator
    {
      public:
        ObCompactCellIterator();
        virtual ~ObCompactCellIterator() { }

      public:
        int init(const char *buf, enum ObCompactStoreType store_type = SPARSE);
        int init(const ObString &buf, enum ObCompactStoreType store_type = SPARSE);

        int get_next_cell(ObObj& value, uint64_t& column_id,
            bool& is_row_finished);

        int get_next_cell(ObObj& value, bool& is_row_finished);

        /*
         * 移动到下一个cell
         */
        int next_cell();

        /*
         * 取出cell 
         * @param column_id 列id，稠密格式下返回OB_INVALID_ID
         * @param value     cell
         * @param is_row_finished 是否到了一行的结束。到一行结束返回一个特殊的ObObj - OP_END_ROW
         * @param row             行结束时返回这一行的紧凑格式的内存区域
         */
        int get_cell(uint64_t &column_id, const ObObj *&value,
            bool *is_row_finished = NULL, ObString *row = NULL);

        /*
         * 用于稠密格式读取cell
         * 在带有column_id的稀疏格式读取数据使用此函数会出错
         */
        int get_cell(const ObObj *&value, bool *is_row_finished = NULL,
            ObString *row = NULL);

        /*
         * 因为效率问题，此函数过期
         */
        int get_cell(uint64_t &column_id, ObObj &value,
            bool *is_row_finished = NULL, ObString *row = NULL);

        void reset_iter();
        inline int64_t parsed_size();
        inline bool is_row_finished();
        
      private:
        virtual int parse_varchar(ObBufferReader &buf_reader, ObObj &value) const;
        int parse(ObBufferReader &buf_reader, ObObj &value, uint64_t *column_id = NULL) const;
        int parse_decimal(ObBufferReader &buf_reader, ObObj &value) const;

      private:
        ObBufferReader buf_reader_;
        uint64_t column_id_;
        ObObj value_;
	      uint64_t row_start_;
        enum ObCompactStoreType store_type_;
        int32_t step_;
        bool is_row_finished_;
        bool inited_;
    };

    bool ObCompactCellIterator::is_row_finished()
    {
      return is_row_finished_;
    }

    int64_t ObCompactCellIterator::parsed_size()
    {
      return buf_reader_.pos();
    }
  }
}

#endif /* OCEANBASE_COMMON_COMPACT_CELL_ITERATOR_H_ */

