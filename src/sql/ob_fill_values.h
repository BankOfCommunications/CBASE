/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fill_values.h
 *
 * Authors:
 *   gaojt
 * function:
 *   Delete_Update_Function
 *
 */
#ifndef _OB_FILL_VALUES_H
#define _OB_FILL_VALUES_H 1
#include "ob_multi_children_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/ob_schema.h"
#include "ob_table_rpc_scan.h"
#include "ob_expr_values.h"
#include "common/ob_row.h"
#include "common/ob_row_desc.h"
#include "obmysql/ob_mysql_server.h"
#include "ob_sequence_update.h"
#include <set> //add lbzhong[Update rowkey] 20151221:b:e
namespace oceanbase
{
  namespace sql
  {
    class ObFillValues: public ObMultiChildrenPhyOperator
    {
      public:
        ObFillValues();
        virtual ~ObFillValues();

        int set_row_desc(const common::ObRowDesc &row_desc);
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;


//        int construct_in_left_part(const ObRowDesc& row_desc,
//                                   const uint64_t table_id,
//                                   const ObRowkeyInfo &rowkey_info,
//                                   ObSqlExpression *&rows_filter,
//                                   ExprItem& expr_item);
//        int construct_in_right_part(const ObRowkeyInfo &rowkey_info,
//                                    const ObRow *row,
//                                    ObSqlExpression *&rows_filter,
//                                    ExprItem &expr_item,
//                                    int64_t& total_row_size);
//        int construct_in_end_part(ObTableRpcScan *&table_scan,
//                                  ObSqlExpression *&rows_filter,
//                                  ObRowkeyInfo rowkey_info,
//                                  int64_t row_num,
//                                  ExprItem &expr_item);
        int fill_values(ObValues*& tmp_value,
                             const ObRowkeyInfo &rowkey_info,
                             ObExprValues*& get_param_values,
                             const ObRowDesc &row_desc,
                             const uint64_t table_id, bool& is_close_sub_query);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;


        void set_rowkey_info(common::ObRowkeyInfo rowkey_info){rowkey_info_ = rowkey_info;};
        void set_table_id(uint64_t table_id){table_id_ = table_id;};
        void set_is_delete_update(bool is_delete_update){is_delete_update_ = is_delete_update;};
        void set_is_column_hint_index(bool is_column_hint_index){is_column_hint_index_ = is_column_hint_index;};
        const ColumnItem* get_column_item(int32_t index) const;
        int add_column_item(const ColumnItem& column_item);
        void set_generated_column_id(uint64_t generated_column_id){generated_column_id_ = generated_column_id;};
        void set_is_multi_batch(bool is_multi_batch){is_multi_batch_ = is_multi_batch;};
        void set_sql_context(ObSqlContext sql_context){sql_context_ = sql_context;};
        bool is_ud_non_row(){return is_row_num_null_;};
        bool is_delete_update(){return is_delete_update_;};
        void set_prepare_name(std::string prepare_name){prepare_name_ = prepare_name;};
        std::string get_prepare_name(){return prepare_name_;};
        enum ObPhyOperatorType get_type() const {return PHY_FILL_VALUES;}

        void set_sequence_update(ObPhyOperator *sequence_op) {sequence_update_ = sequence_op;}
        //add lbzhong [Update rowkey] 20151221:b
        inline void set_table_item(const TableItem &table_item) { table_item_ = table_item; }
        inline void set_hint_expr(const ObRpcScanHint &hint_expr)
        {
          hint_expr_ = hint_expr;
          hint_expr_.read_method_ = ObSqlReadStrategy::USE_SCAN;
        }
        inline void set_is_update_rowkey(const bool is_update_rowkey) { is_update_rowkey_ = is_update_rowkey; }
        int set_column_items(const ObStmt *stmt);
        inline bool exist_column(const uint64_t column_id) const;
        int check_rowkey_duplication(ObTableRpcScan*& table_rpc_scan_for_update, const ObRow*& row,
                                     std::set<int64_t> &rowkey_set);
        int check_sequence_update_rowkey_duplication(ObTableRpcScan*& table_rpc_scan_for_update_rowkey, const ObArray<ObRow*> &row_array,
                    std::set<int64_t> &rowkey_set, ObProject* project);
        inline void set_table_rpc_scan_for_update_rowkey(ObTableRpcScan* table_rpc_scan_for_update_rowkey)
        {
          table_rpc_scan_for_update_rowkey_ = table_rpc_scan_for_update_rowkey;
        }
        int set_exprs(const ObLogicalPlan* logical_plan, const ObStmt* stmt);
        ObSqlExpression* get_expr(const uint64_t column_id) const;
        void clear_exprs();
        int set_update_columns_flag(oceanbase::common::ObArray<uint64_t>& update_columns);
        // add by maosy [Delete_Update_Function]
        int64_t get_affect_row(){    return affect_row_;};
        bool is_multi_batch_over(){ return is_multi_batch_over_;};
        void set_max_row_value_size(int64_t max_row_value_size)
        {
            max_row_value_size_ = max_row_value_size;
        }
        void set_need_to_tage_empty_row(bool need_clear)
        {
            need_to_tage_empty_row_ = need_clear ;
        }
        // add e
        //add:e
        void set_is_in_prepare(bool is_in_prepare){is_in_prepare_ = is_in_prepare;};
        int64_t get_questionmark_num_in_assginlist_of_update(){return questionmark_num_in_assginlist_of_update_;};
        void set_questionmark_num_in_assginlist_of_update(int64_t questionmark_num_in_assginlist_of_update)
        {questionmark_num_in_assginlist_of_update_ = questionmark_num_in_assginlist_of_update;};
        int set_rowkey_ids(uint64_t rowkey_id);
        void reset_for_prepare_multi();
        //add gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20160616:b
//        int execute_stmt_no_return_rows(const ObString &stmt);
//        int start_transaction();
//        int commit();
//        int rollback();
        void get_threshhold(int64_t& threshhold){threshhold = max_rowsize_capacity_;};
        int clear_prepare_select_result();
        bool is_already_clear(){return is_already_clear_;};
        //add gaojt 20160616:e
        int add_row_to_obvalues(ObValues *&tmp_value, common::ObRow* row);
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // disallow copy
        ObFillValues(const ObFillValues &other);
        ObFillValues& operator=(const ObFillValues &other);
      private:
        static const int64_t MAX_UDPATE_DELETE_VALUE_SIZE = static_cast<int64_t>(0.8*2*1024L*1024L);
        // delete by maosy [Delete_Update_Function]
        //static const int64_t MAX_UDPATE_DELETE_ROW_NUM = 20*256;
        // del e
      private:
        // add by maosy [Delete_Update_Function]
        int64_t max_rowsize_capacity_ ;
        int64_t max_row_value_size_;
        // add e
        common::ObRowkeyInfo rowkey_info_;
        uint64_t table_id_;
        common::ObRowDesc row_desc_;
        common::ObVector<ColumnItem>   column_items_;
        bool is_row_num_null_;
        bool is_delete_update_;
        bool is_column_hint_index_;
        ObPhyOperator * sequence_update_;//add lijianqiang [sequence update] 20160319
        uint64_t generated_column_id_;
        int is_reset_;/*Exp: whether reset the environment*/
        bool is_multi_batch_;
        bool is_close_sub_query_;
        ObSqlContext sql_context_;
        ObRpcScanHint hint_;
    int64_t questionmark_num_in_assginlist_of_update_;
        //add lbzhong [Update rowkey] 20151221:b
        ObRpcScanHint hint_expr_;
        TableItem table_item_;
        bool is_update_rowkey_;
        ObTableRpcScan* table_rpc_scan_for_update_rowkey_;
        oceanbase::common::ObArray<uint64_t> update_rowkey_columns_;
        oceanbase::common::ObArray<ObSqlExpression*> update_rowkey_exprs_;
        oceanbase::common::ObArray<int32_t> update_columns_flag_; // if rowkey put 1 else 0
        //add:e
        ObResultSet* select_result_set_;
        bool is_in_prepare_;
        std::string prepare_name_;
        // add by maosy [Delete_Update_Function]
        int64_t affect_row_;
        bool is_multi_batch_over_;
        // add e
        oceanbase::common::ObArray<uint64_t> rowkey_ids_;
        bool is_already_clear_;
        bool need_to_tage_empty_row_;//add by maosy for 如果没有开启事务，则最后一个为0的批次不需要去清理时间戳
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_FILL_VALUES_H */
