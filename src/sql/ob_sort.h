/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sort.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SORT_H
#define _OB_SORT_H 1
#include "ob_single_child_phy_operator.h"
#include "common/ob_string.h"
#include "ob_in_memory_sort.h"
#include "ob_merge_sort.h"

namespace oceanbase
{
  namespace sql
  {
    class ObSort: public ObSingleChildPhyOperator
    {
      public:
        ObSort();
        virtual ~ObSort();
        virtual void reset();
        virtual void reuse();
        int add_sort_column(const uint64_t tid, const uint64_t cid, bool is_ascending_order);
        //add dolphin [ROW_NUMBER]@20150830:b
        int replace_sort_column_at(const uint32_t idx,const uint64_t tid, const uint64_t cid,bool is_ascending);
        int exist_sort_column(const uint64_t tid, const uint64_t cid, bool &is_exist);
        //add:e
        int64_t get_sort_column_size() const;
        void set_mem_size_limit(const int64_t limit);
        int set_run_filename(const common::ObString &filename);

        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const;

        int64_t get_mem_size_limit() const;
        const common::ObArray<ObSortColumn>& get_sort_columns() const;
        //add wanglei:b
        int open_without_sort();
        //add:e
        //add qianzm [set_operation] 20151222 :b
        void set_is_set_op_flag(){is_set_op_flag_ = 1;}
        //add e
        //add qianzm [set_operation] 20160107:b
        common::ObArray<common::ObObjType>& get_result_type_array()
        {
          return result_type_array_for_setop_;
        }
        //add 20160107:e
		//add qianzm [set_operation] 20160115:b
        int add_result_type_array_for_setop(common::ObArray<common::ObObjType>& result_columns_type);
        //add 20160115:e
        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // disallow copy
        ObSort(const ObSort &other);
        ObSort& operator=(const ObSort &other);
        // function members
        bool need_dump() const;
        int do_sort();
      private:
        // data members
        common::ObArray<ObSortColumn> sort_columns_;
        int64_t mem_size_limit_;
        ObInMemorySort in_mem_sort_;
        ObMergeSort merge_sort_;
        ObSortHelper *sort_reader_;
        int64_t is_set_op_flag_;//add qianzm [set_operation] 20151222
        //add qianzm [set_operation] 20160107:b
        common::ObArray<common::ObObjType> result_type_array_for_setop_;
        //add 20160107:e
    };

    inline int64_t ObSort::get_mem_size_limit() const
    {
      return mem_size_limit_;
    }
    inline int64_t ObSort::get_sort_column_size() const
    {
      return sort_columns_.count();
    }
    inline const common::ObArray<ObSortColumn>& ObSort::get_sort_columns() const
    {
      return sort_columns_;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SORT_H */
