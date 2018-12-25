/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_project.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_PROJECT_H
#define _OB_PROJECT_H 1
#include "ob_single_child_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/page_arena.h"
#include "common/ob_se_array.h"
//add wenghaixing [secondary index upd.2] 20141127
#include "ob_multiple_get_merge.h"
//add e
//add liumengzhan_delete_index
#include "ob_multiple_get_merge.h"
#include "ob_filter.h"
//add:e
#include "common/hash/ob_hashset.h" //add lbzhong[Update rowkey] 20151221:b:e

namespace oceanbase
{
  namespace sql
  {
    class ObProject: public ObSingleChildPhyOperator
    {
      public:
        ObProject();
        virtual ~ObProject();
        virtual void reset();
        virtual void reuse();
    //add liumengzhan_delete_index
        void reset_iterator();
        bool is_duplicated_output_column(uint64_t &col_id);
    //add:e
        int add_output_column(const ObSqlExpression& expr);
        inline int64_t get_output_column_size() const;
        inline int64_t get_rowkey_cell_count() const;
        inline void set_rowkey_cell_count(const int64_t count);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        void assign(const ObProject &other);
		void reset_stuff();//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911 /*Exp:the signal to reset project operator when the stm is insert...select... */
        const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> >  &get_output_columns() const;
        virtual ObPhyOperatorType get_type() const;
        //add wenghaixing [secondary index upd] 20150429
        inline virtual int64_t get_index_num()const{return index_num_;}
        inline void set_index_num(int64_t index_num){index_num_ = index_num;}
        //add e
        inline void set_row_store(ObRowStore* store_pre, ObRowStore* store_post){row_store_pre_ = store_pre;row_store_post_ = store_post;}
        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;

        //add liuzy [sequence select] 20150612:b
        void set_sequence_select_op();
        //20150612:e
        //add lijianqiang [sequence] 20150910:b
        int add_nextval_for_update(common::ObString &nextval);
        int add_idx_of_next_value(int64_t);
        int add_expr_idx_of_out_put_columns(int64_t idx);
        int check_the_sequence_validity();
        int64_t get_sequence_info_serialize_size() const;
        void set_sequence_num_in_one_row();
        void reset_row_num_idx();
        //this function is used to fill the sequence info to current row,
        //before use this fuction ,please reset_row_num_idx() out of the get_next_row()
        int fill_sequence_info(common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > &out_put_columns);
//        void set_row_num(int64_t row_num);
        //add 20150910:e
      //add peiouya [IN_TYPEBUG_FIX] 20151225:b
      public:
        int get_output_columns_dsttype(common::ObArray<common::ObObjType> &output_columns_dsttype) ;
        int add_dsttype_for_output_columns(common::ObArray<common::ObObjType>& columns_dsttype);
      //add 20151225:e
        //add lbzhong [Update rowkey] 20160111:b
        int add_new_rowkey(const common::ObRow *&input_row, uint32_t i, uint32_t &rowkey_index,
                           const int64_t table_id,
                           const int64_t column_id, const ObObj *&result, bool &is_new_rowkey,
                           bool& is_rowkey_change);
        int set_new_rowkey_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj &cell);
        int check_rowkey_change(const common::ObRow *&input_row, const uint64_t table_id,
                                const uint64_t column_id, const common::ObObj &value, bool& is_rowkey_change);
        int fill_sequence_info();
        inline common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator,
              ObArrayExpressionCallBack<ObSqlExpression> > & get_output_columns_for_update()
        {
          return columns_;
        }
        inline int add_rowkey_index()
        {
          return idx_of_update_rowkey_expr_.push_back(static_cast<uint32_t>(columns_.count() - 1));
        }
        inline bool is_update_rowkey_column(uint32_t column_index, uint32_t &rowkey_index)
        {
          if(rowkey_index < idx_of_update_rowkey_expr_.count() &&
             column_index == idx_of_update_rowkey_expr_.at(rowkey_index))
          {
            rowkey_index++;
            return true;
          }
          return false;
        }
        int serialize_idx_of_update_rowkey_expr(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize_idx_of_update_rowkey_expr(const char* buf, const int64_t data_len, int64_t& pos);
        int64_t get_idx_of_update_rowkey_expr_serialize_size(void) const;
        void destroy_new_rowkey_row()
        {
          if(NULL != new_rowkey_row_)
          {
            new_rowkey_row_->~ObRawRow();
            ob_free(new_rowkey_row_);
            new_rowkey_row_ = NULL;
          }
        }
        //add:e

      private:
        int cons_row_desc();
        // disallow copy
        ObProject(const ObProject &other);
        ObProject& operator=(const ObProject &other);
      protected:
        // data members
        common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > columns_;
        common::ObRowDesc row_desc_;
        common::ObRow row_;
        int64_t rowkey_cell_count_;
        //add wenghaixing [secondary index upd]20150429
        int64_t index_num_ ;
        ObRowStore *row_store_pre_; //for index
        ObRowStore *row_store_post_; //for data
        //add e
        //add liuzy [sequence select] 20150612:b
        bool is_set_sequence_select_op_;
        //20150612:e
        //add lijianqiang [sequence] 20150910:b
        const static int64_t BASIC_SYMBOL_COUNT = 64;
        //the follow three array have the same size,we can fill the "columns_" with the right sequence value by them
        //下面三个数组是一一对应的，对应关系为 值<-->值所在表达式的下标<-->值所在的表达式在所有输出列的下标
        ObSEArray<ObObj, BASIC_SYMBOL_COUNT> sequence_values_;// to store the sequence nextval for update set clause
        ObArray<int64_t> idx_of_expr_with_sequence_;//store the nextval idx in expr
        ObArray<int64_t> idx_of_out_put_columns_with_sequence_;//store the expr idx in out put columns
        int64_t sequence_count_in_one_row_;
        int64_t row_num_idx_;//do not need serialization and deserialization

        //add 20150910:e
        //add lbzhong[Update rowkey] 20151221:b
        bool is_update_rowkey_;
        ObRawRow* new_rowkey_row_;
        // if idx_of_update_rowkey_expr_.count > 0, then is_update_rowkey = true;
        ObArray<uint32_t> idx_of_update_rowkey_expr_;
        //add:e
        //add peiouya [IN_TYPEBUG_FIX] 20151225:b
        common::ObArray<common::ObObjType> output_columns_dsttype_;
        //add 20151225:e
    };

    inline int64_t ObProject::get_output_column_size() const
    {
      return columns_.count();
    }

    inline const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> >  & ObProject::get_output_columns() const
    {
      return columns_;
    }

    inline int64_t ObProject::get_rowkey_cell_count() const
    {
      return rowkey_cell_count_;
    }

    inline void ObProject::set_rowkey_cell_count(const int64_t count)
    {
      rowkey_cell_count_ = count;
    }
    //add liuzy [sequence select] 20150612:b
    inline void ObProject::set_sequence_select_op()
    {
      is_set_sequence_select_op_ = true;
    }
    //20150612:e


  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_PROJECT_H */
