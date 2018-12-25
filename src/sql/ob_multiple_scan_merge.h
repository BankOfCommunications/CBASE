/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multiple_scan_merge.h 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_MULTIPLE_SCAN_MERGE_H
#define _OB_MULTIPLE_SCAN_MERGE_H 1

#include "ob_multiple_merge.h"

namespace oceanbase
{
  namespace sql
  {
    class ObMultipleScanMerge : public ObMultipleMerge
    {
      struct ChildContext
      {
        ObPhyOperator *child_;
        const ObRow *row_;
        int32_t seq_;

        ChildContext() : child_(NULL), row_(NULL), seq_(0)
        {
        }
      };

      class CmpFunc
      {
        public:
          bool operator() (ChildContext a, ChildContext b) const;
      };

      public:
        ObMultipleScanMerge();
        virtual ~ObMultipleScanMerge() {}
        virtual void reset();
        virtual void reuse();
        int get_next_row(const ObRow *&row);
        int open();
        int close();
        enum ObPhyOperatorType get_type() const{return PHY_MULTIPLE_SCAN_MERGE;};
        int64_t to_string(char *buf, int64_t buf_len) const;

        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        int write_row(ObRow &row);

      private:
        ChildContext child_context_array_[MAX_CHILD_OPERATOR_NUM];
        ObRow cur_row_;
        ChildContext last_child_context_;
        int32_t child_context_num_;
        bool is_cur_row_valid_;
    };
  }
}

#endif /* _OB_MULTIPLE_SCAN_MERGE_H */
  

