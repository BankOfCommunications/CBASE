/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_table.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_FAKE_TABLE_H
#define _OB_FAKE_TABLE_H 1
#include "sql/ob_phy_operator.h"
#include "common/ob_array.h"
namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
      // fake table to feed input for testing
      class ObFakeTable: public ObPhyOperator
      {
        public:
          static const uint64_t TABLE_ID = 1010;
          ObFakeTable();
          virtual ~ObFakeTable();
          virtual ObPhyOperatorType get_type() const { return PHY_INVALID; }
          void set_row_count(const int64_t count);
          void set_table_id(const uint64_t tid);
          const common::ObRowDesc &get_row_desc() const;
          void reset() {}
          void reuse() {}

          virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
          virtual int open();
          virtual int close();
          virtual int get_next_row(const common::ObRow *&row);
          virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
          virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        private:
          // types and constants
          static const int VARCHAR_CELL_BUFF_SIZE = 64;
          static const int COLUMN_COUNT = 16;
        private:
          // disallow copy
          ObFakeTable(const ObFakeTable &other);
          ObFakeTable& operator=(const ObFakeTable &other);
          // function members
          int cons_curr_row(const int64_t row_idx);
          int cons_varchar_cell(common::ObObj &cell);
          int cons_random_int_cell(common::ObObj &cell);
          int rand_int(int max);
        protected:
          uint64_t table_id_;
          int64_t row_count_;
          common::ObRowDesc row_desc_;
          common::ObRow curr_row_;

        private:
          // data members
          int64_t get_count_;
          char buff_[VARCHAR_CELL_BUFF_SIZE];
      };

      inline const common::ObRowDesc &ObFakeTable::get_row_desc() const
      {
        return row_desc_;
      }

      inline void ObFakeTable::set_table_id(const uint64_t tid)
      {
        table_id_ = tid;
      }
    } // end namespace test
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_FAKE_TABLE_H */
