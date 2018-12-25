/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_distinct.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_MERGE_DISTINCT_H
#define _OB_MERGE_DISTINCT_H 1
#include "ob_single_child_phy_operator.h"
#include "common/ob_string.h"
#include "common/ob_define.h"
#include "common/ob_row.h"

namespace oceanbase
{
  namespace sql
  {
    struct ObDistinctColumn
    {
      uint64_t table_id_;
      uint64_t column_id_;

      ObDistinctColumn()
        :table_id_(common::OB_INVALID_ID), column_id_(common::OB_INVALID_ID)
      {
      }
    };


    class ObMergeDistinct: public ObSingleChildPhyOperator
    {
      public:
        ObMergeDistinct();
        virtual ~ObMergeDistinct();
        virtual void reset();
        virtual void reuse();
        // not always whole row is compared
        // example: SELECT distinct c1 FROM t order by c2;
        int add_distinct_column(const uint64_t tid, const uint64_t cid);
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_MERGE_DISTINCT; }
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // disallow copy
        ObMergeDistinct(const ObMergeDistinct &other);
        ObMergeDistinct& operator=(const ObMergeDistinct &other);

        // member function
        int compare_equal(const common::ObRow *this_row, const common::ObRow *last_row, bool &result) const;
      private:
        // data members
        common::ObArray<ObDistinctColumn> distinct_columns_;
        bool got_first_row_;
        common::ObRow last_row_;
        common::ObRow curr_row_;
        static const uint64_t OB_ROW_BUF_SIZE = common::OB_MAX_ROW_LENGTH;
        char *last_row_buf_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_MERGE_DISTINCT_H */
