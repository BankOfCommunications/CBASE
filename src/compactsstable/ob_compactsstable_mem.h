/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_compactsstable_mem.h is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#ifndef OB_COMPACTSSTABLE_MEM_H
#define OB_COMPACTSSTABLE_MEM_H
#include "common/ob_define.h"
#include "common/ob_iterator.h"
#include "common/ob_range.h"
#include "common/ob_string.h"
#include "common/ob_column_filter.h"
#include "common/ob_action_flag.h"
#include "common/ob_rowkey.h"
#include "ob_compact_row.h"
#include "ob_block_membuf.h"

namespace oceanbase
{
  using namespace common;
  namespace compactsstable
  {
    struct ObFrozenVersionRange
    {
      ObFrozenVersionRange():major_version_(0),
                             minor_version_start_(0),
                             minor_version_end_(0),
                             is_final_minor_version_(0),
                             reserved_(0)
      {}
      int64_t major_version_;
      int32_t minor_version_start_;
      int32_t minor_version_end_;
      int32_t is_final_minor_version_;
      int32_t reserved_;
    };

    class ObCompactSSTableMem
    {
      public:
      static const int64_t OB_COMPACTSSTABLE_BLOCK_SIZE = 2 * 1024 * 1024L; //2M
      friend class TestCompactSSTableMem_write_mix_Test;
    public:
      ObCompactSSTableMem();
      ~ObCompactSSTableMem();
      
      int init(const ObFrozenVersionRange& version_range,
               const int64_t frozen_time);

      int append_row(uint64_t table_id,ObCompactRow& row,int64_t& approx_space_usage);

      void clear();

    public:
      const ObFrozenVersionRange& get_version_range() const { return version_range_;}
      int64_t get_data_version() const;
      uint64_t get_table_id() const { return table_id_;  }
      int64_t get_row_count() const { return row_count_; }
      int64_t get_frozen_time() const { return frozen_time_; }

    public:
      int locate_start_pos(const common::ObNewRange& range,const char**& start_iterator);
      int locate_end_pos(const common::ObNewRange& range,const char**& end_iterator);
      int find(const common::ObRowkey& rowkey,const char**& start_iterator);
      bool is_row_exist(const common::ObRowkey& rowkey);

    private:
      class Compare
      {
      public:
        Compare(ObCompactSSTableMem& mem);
        bool operator()(const char* index,const common::ObRowkey& rowkey);
      private:
        ObCompactSSTableMem& mem_;
      };
      const char** lower_bound(const char** start,const char** end,const common::ObRowkey& rowkey);

    private:
      static const int64_t INDEX_BLOCK_SIZE = 65536L; // 64K
                                                      // 64K block can hold 8192 rows
                                                      // should enough for most tablets in cs
      static const int64_t INDEX_ENTRY_SIZE = sizeof(char*);
      
      int add_index(const char* pos);

      struct RowIndex
      {
        RowIndex() : offset_(0),max_index_entry_num_(0),row_index_(NULL) {}
        int64_t offset_;
        int64_t max_index_entry_num_;
        const char**  row_index_;
        const char** start() { return row_index_; }
        const char** end()   { return row_index_ + offset_; }
      };

      int get_last_row(const char*& pos,int64_t& size) const;

    private:
      uint64_t               table_id_;
      ObFrozenVersionRange   version_range_;
      int64_t                row_count_;
      int64_t                frozen_time_;
      ObBlockMembuf          rows_;
      RowIndex               row_index_;
      bool                   inited_;
    };

    class ObCompactSSTableMemIterator : public common::ObIterator
    {
    public:
      ObCompactSSTableMemIterator();
      ~ObCompactSSTableMemIterator();
      
      int init(ObCompactSSTableMem* mem);

      void reset();

      int set_scan_param(const common::ObNewRange& range, const common::ColumnFilter* cf,const bool is_reverse_scan);
      int set_get_param(const common::ObRowkey& rowkey,const common::ColumnFilter* cf);

      int next_cell();

      inline int get_cell(oceanbase::common::ObCellInfo** cell)
      {
        return get_cell(cell, NULL);
      }

      inline int get_cell(oceanbase::common::ObCellInfo** cell, bool* is_row_changed)
      {
        int ret = common::OB_SUCCESS;
        if (NULL == cell)
        {
          TBSYS_LOG(ERROR, "invalid argument, cell=%p", cell);
          ret = common::OB_INVALID_ARGUMENT;
        }
        else if (common::OB_SUCCESS != iterator_status_)
        {
          TBSYS_LOG(ERROR, "initialize status error=%d", iterator_status_);
          ret = iterator_status_;
        }
        else
        {
          if (fill_nop_)
          {
            *cell = &nop_cell_;
          }
          else
          {
            ret = current_row_.get_cell(cell);
          }
          
          if (common::OB_SUCCESS == ret)
          {
            (*cell)->table_id_ = mem_->get_table_id();
            (*cell)->row_key_  = row_key_;
            if (is_row_changed != NULL)
            {
              *is_row_changed = is_row_changed_;
            }

            if (first_cell_got_)
            {
              is_row_changed_ = false;
            }
          }
        }
        return ret;
      }

    private:
      int next_cell_();
      int next_row();
      int load_row(const char* row);

    private:
      ObCompactSSTableMem*   mem_;
      ObCompactRow           current_row_;
      const common::ColumnFilter*  column_filter_;
      const char**           start_iterator_;
      const char**           end_iterator_;
      const char**           row_cursor_;
      common::ObRowkey       row_key_;
      ObCellInfo             nop_cell_;
      int                    iterator_status_;
      bool                   is_reverse_scan_;
      bool                   is_row_changed_;
      bool                   first_cell_got_;
      bool                   fill_nop_;
      bool                   add_delete_row_nop_;
      bool                   has_delete_row_;
      bool                   inited_;
    };

    struct ObCompactSSTableMemNode
    {
      ObCompactSSTableMemNode(): next_(NULL) {}
      ObCompactSSTableMem          mem_;
      ObCompactSSTableMemNode*     next_;
    };

    struct ObCompactMemIteratorArray
    {
      ObCompactSSTableMemIterator iters_[common::OB_MAX_COMPACTSSTABLE_NUM];
    };
    bool is_invalid_version_range(const ObFrozenVersionRange& range);
  }
}

#endif
