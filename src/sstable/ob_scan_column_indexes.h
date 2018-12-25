/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_scan_column_indexs.h is for what ...
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_SCAN_COLUMN_INDEXES_H_
#define OCEANBASE_SSTABLE_SCAN_COLUMN_INDEXES_H_

#include <tblog.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_mod_define.h"
namespace oceanbase
{
  namespace sstable
  {
    class ObScanColumnIndexes
    {
      public:
        enum ColumnType
        {
          Normal = 0,
          NotExist = 1,
          Rowkey = 2,
        };
      public:
        struct Column
        {
          int32_t type_;
          int32_t index_;
          uint64_t id_;
          Column() : type_(Normal), index_(0), id_(0) {}
          Column(const ColumnType type, const int32_t index, const uint64_t id)
            : type_(type), index_(index), id_(id) {}
        };
      public:
        ObScanColumnIndexes(const int64_t column_size = common::OB_MAX_COLUMN_NUMBER)
        : column_cnt_(0), column_info_size_(column_size), column_info_(NULL), index_map_(NULL)
        {

        }

        ~ObScanColumnIndexes()
        {
          if (NULL != column_info_)
          {
            common::ob_free(column_info_);
            column_info_ = NULL;
          }
          if (NULL != index_map_)
          {
            common::ob_free(index_map_);
            index_map_ = NULL;
          }
        }

      private:
        int allocate_store()
        {
          int ret = common::OB_SUCCESS;
          if (NULL == column_info_)
          {
            if (NULL == column_info_ && NULL == (column_info_= reinterpret_cast<Column*>
                                                 (common::ob_malloc(sizeof(Column) * column_info_size_, common::ObModIds::OB_SSTABLE_INDEX))))
            {
              TBSYS_LOG(WARN, "failed to allocate memory for column index array");
              ret = common::OB_ALLOCATE_MEMORY_FAILED;
            }
          }
          if (NULL == index_map_)
          {
            if (NULL == (index_map_ = reinterpret_cast<int16_t*>
                         (common::ob_malloc(sizeof(int16_t) * common::OB_MAX_COLUMN_NUMBER, common::ObModIds::OB_SSTABLE_INDEX))))
            {
              TBSYS_LOG(WARN, "failed to allocate memory for column index array");
              ret = common::OB_ALLOCATE_MEMORY_FAILED;
            }
            else
            {
              memset(index_map_, -1, sizeof(int16_t) * common::OB_MAX_COLUMN_NUMBER);
            }
          }
          return ret;
        }

      public:
        inline void reset()
        {
          if (NULL != index_map_)
          {
            memset(index_map_, -1, sizeof(int16_t) * column_cnt_);
          }
          column_cnt_ = 0;
        }

        inline int add_column_id(const ColumnType type, const int32_t index, const uint64_t column_id)
        {
          int ret = common::OB_SUCCESS;

          if (0 == column_id || common::OB_INVALID_ID == column_id)
          {
            ret = common::OB_INVALID_ARGUMENT;
          }
          else if (column_cnt_ >= column_info_size_)
          {
            ret = common::OB_SIZE_OVERFLOW;
          }
          else if (common::OB_SUCCESS != (ret = allocate_store()))
          {
            TBSYS_LOG(WARN, "allocate_store error,ret=%d", ret);
          }
          else
          {
            column_info_[column_cnt_].type_ = type;
            column_info_[column_cnt_].id_ = column_id;
            column_info_[column_cnt_].index_ = index;
            if (type != NotExist && index >= 0 && index < common::OB_MAX_COLUMN_NUMBER)
            {
              index_map_[index] = static_cast<int16_t>(column_cnt_);
            }
            ++column_cnt_;
          }

          if (common::OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "add column error, ret = %d, index=%d, column_id=%lu,"
                "column_size=%ld, max_column_size=%ld",
              ret , index, column_id, column_cnt_, column_info_size_);
          }

          return ret;
        }

        inline int get_column(const int64_t index, Column& column) const
        {
          int ret = common::OB_SUCCESS;

          if (index >= 0 && index < column_cnt_)
          {
            column = column_info_[index];
          }
          else
          {
            column.id_ = common::OB_INVALID_ID;
            column.index_ = common::OB_INVALID_INDEX;
            column.type_ = NotExist;
            ret = common::OB_INVALID_ARGUMENT;
          }

          return ret;
        }

        inline int64_t get_column_count() const
        {
          return column_cnt_;
        }

        inline const Column* get_column_info() const
        {
          return column_info_;
        }

        inline int64_t find_by_id(const uint64_t id) const
        {
          int64_t ret = -1; // -1 means not found
          for (int64_t i = 0; i < column_cnt_; ++i)
          {
            if (column_info_[i].id_ == id)
            {
              ret = i;
              break;
            }
          }
          return ret;
        }

        inline int64_t find_by_index(const int64_t index) const
        {
          return index < common::OB_MAX_COLUMN_NUMBER ? index_map_[index] : -1;
        }

      private:
        int64_t column_cnt_;
        int64_t column_info_size_;
        Column* column_info_;
        int16_t *index_map_;
    };

    // ObSimpleColumnIndexes store a mapping of
    // [offset in dense sstable row] with [offset in input scan params]
    // [sstable row]           11 12 13 14 15 16 17 18 19
    // -------------------------------------------------------
    // [input ids]             19 5  13 22 17
    // -------------------------------------------------------
    // [input offset]          0  1  2  3  4
    // -------------------------------------------------------
    // [offset in sstable]     8  -1 2  -1 6
    class ObSimpleColumnIndexes
    {
      public:
        ObSimpleColumnIndexes (const int64_t column_size = common::OB_MAX_COLUMN_NUMBER)
        : table_id_(0), column_cnt_(0), not_null_column_cnt_(0),
        column_info_size_(column_size), column_info_(NULL)
        {
        }

        ~ObSimpleColumnIndexes()
        {
          if (NULL != column_info_)
          {
            common::ob_free(column_info_);
            column_info_ = NULL;
          }
        }

      private:
        int allocate_store()
        {
          int ret = common::OB_SUCCESS;
          if (NULL == column_info_)
          {
            if (NULL == (column_info_= reinterpret_cast<int64_t*>
                         (common::ob_malloc(sizeof(int64_t) * column_info_size_, common::ObModIds::OB_SSTABLE_INDEX))))
            {
              TBSYS_LOG(WARN, "failed to allocate memory for column index array");
              ret = common::OB_ALLOCATE_MEMORY_FAILED;
            }
            else
            {
              memset(column_info_, -1, sizeof(int64_t) * column_info_size_);
            }
          }
          return ret;
        }

      public:
        inline void reset()
        {
          table_id_ = 0;
          column_cnt_ = 0;
          not_null_column_cnt_ = 0;
          if (NULL != column_info_)
          {
            memset(column_info_, -1, sizeof(int64_t) * column_info_size_);
          }
        }

        inline int add_column(int64_t offset)
        {
          int ret = common::OB_SUCCESS;

          if (column_cnt_ >= column_info_size_)
          {
            TBSYS_LOG(WARN, "add too many columm cnt=%ld,size=%ld",
                column_cnt_, column_info_size_);
            ret = common::OB_SIZE_OVERFLOW;
          }
          else if (common::OB_SUCCESS != (ret = allocate_store()))
          {
            TBSYS_LOG(WARN, "allocate_store error,ret=%d", ret);
          }
          else
          {
            if (offset >= 0)
            {
              // offset == -1 means NotExist column;
              // do not add in columns array.
              // but MUST put placeholder
              column_info_[offset] = column_cnt_;
              ++not_null_column_cnt_;
            }
            ++column_cnt_;
          }

          return ret;
        }

        inline int64_t find_by_offset(const int64_t offset) const
        {
          return offset < common::OB_MAX_COLUMN_NUMBER ? column_info_[offset] : -1;
        }

        int64_t operator[](const int64_t offset) const
        {
          return find_by_offset(offset);
        }

        inline int64_t get_column_count() const
        {
          return column_cnt_;
        }

        inline int64_t get_not_null_column_count() const
        {
          return not_null_column_cnt_;
        }

        inline const int64_t* get_column_info() const
        {
          return column_info_;
        }

        inline void set_table_id(const uint64_t id)
        {
          table_id_ = id;
        }

        inline uint64_t get_table_id() const
        {
          return table_id_;
        }

      private:
        uint64_t table_id_;
        int64_t column_cnt_;
        int64_t not_null_column_cnt_;
        int64_t column_info_size_;
        int64_t *column_info_;
    };

  } // end namespace sstable
} // end namespace oceanbase

#endif //OCEANBASE_SSTABLE_SCAN_COLUMN_INDEXES_H_
