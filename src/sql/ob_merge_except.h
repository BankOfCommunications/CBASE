/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_except.h
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_MERGE_EXCEPT_H_
#define OCEANBASE_SQL_OB_MERGE_EXCEPT_H_

#include "ob_set_operator.h"
#include "common/ob_row.h"
#include "common/ob_string_buf.h"//add qianzm [set_operation] 20151222

namespace oceanbase
{
  namespace sql
  {
    class ObMergeExcept: public ObSetOperator
    {
      public:
        ObMergeExcept();
        virtual ~ObMergeExcept();
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        virtual ObPhyOperatorType get_type() const { return PHY_MERGE_EXCEPT; }
        /**
         * 获得下一行的引用
         * @note 在下次调用get_next或者close前，返回的row有效
         * @pre 调用open()
         * @param row [out]
         *
         * @return OB_SUCCESS或OB_ITER_END或错误码
         */
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual int set_distinct(bool is_distinct);
        //add qianzm [set_operation] 20160107:b
        common::ObArray<common::ObObjType>& get_result_type_array()
        {
          return result_type_array_;
        }
        //add 20160107:e
		//add qianzm [set_operation] 20160115:b
        int add_result_type_array(common::ObArray<common::ObObjType>& result_columns_type);
        //add 20160115:e
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        int cons_row_desc();
        DISALLOW_COPY_AND_ASSIGN(ObMergeExcept);
      private:
        typedef int (ObMergeExcept::*get_next_row_func_type)(const common::ObRow *&row);
        get_next_row_func_type get_next_row_func_;
        int distinct_get_next_row(const common::ObRow *&row);
        int all_get_next_row(const common::ObRow *&row);
        int compare(const common::ObRow *this_row, const common::ObRow *last_row, int &cmp) const;
        int do_distinct(ObPhyOperator *op, const common::ObRow *other, const common::ObRow *&row);
        int cast_cells_to_res_type(common::ObRow *&tmp_row);//add qianzm [set_operation] 20151222
      private:
        common::ObRow left_last_row_;
        common::ObRow right_last_row_;
        const common::ObRow *cur_first_query_row_;
        const common::ObRow *cur_second_query_row_;
        int left_ret_;
        int right_ret_;
        int last_cmp_;
        bool got_first_row_;
        char *left_last_row_buf_;
        char *right_last_row_buf_;
        static const uint64_t OB_ROW_BUF_SIZE = common::OB_MAX_ROW_LENGTH;
        //add qianzm [set_operation] 20151222 :b
        common::ObArray<common::ObObjType> result_type_array_;
        static const int64_t DEF_MEM_BLOCK_SIZE_ = 64 * 1024L;
        bool first_cur_row_has_cast;
        bool second_cur_row_has_cast;
        common::ObStringBuf tmp_string_buf_;
        //add e
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_MERGE_EXCEPT_H_ */
