/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_scan.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_TABLE_MEM_SCAN_H
#define _OB_TABLE_MEM_SCAN_H 1
#include "ob_table_scan.h"
#include "ob_sql_expression.h"
#include "ob_rename.h"
#include "ob_project.h"
#include "ob_filter.h"
#include "ob_limit.h"
#include "common/ob_row.h"
namespace oceanbase
{
  namespace sql
  {
    class ObTableMemScan: public ObTableScan
    {
      public:
        ObTableMemScan();
        virtual ~ObTableMemScan();
        virtual void reset();
        virtual void reuse();
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const;

        /**
         * 添加一个需输出的column
         *
         * @note 只有通过复合列结算新生成的列才需要new_column_id
         * @param expr [in] 需输出的列（这个列可能是个复合列的结果）
         *
         * @return OB_SUCCESS或错误码
         */
        int add_output_column(const ObSqlExpression& expr, bool change_tid_for_storing = false);//mod liumz, [optimize group_order by index]20170419
        //add fanqiushi_index
        int add_main_output_column(const ObSqlExpression& expr);
        int cons_second_row_desc(ObRowDesc &row_desc);
         int set_second_row_desc(ObRowDesc *row_desc);
        int add_main_filter(ObSqlExpression* expr);
        int add_index_filter(ObSqlExpression* expr);
        //add:e
        //add wanglei [semi join] 20160108:b
        int add_index_filter_ll(ObSqlExpression* expr) ;
        int add_index_output_column_ll(ObSqlExpression& expr) ;
        //add:e
        /**
         * 设置table_id
         * @note 只有基本表被重命名的情况才会使两个不相同id，其实两者相同时base_table_id可以给个默认值。
         * @param table_id [in] 输出的table_id
         * @param base_table_id [in] 被访问表的id
         *
         * @return OB_SUCCESS或错误码
         */
        int set_table(const uint64_t table_id, const uint64_t base_table_id);

        //add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
        bool is_base_table_id_valid();
        //add:e

        /**
         * 添加一个filter
         *
         * @param expr [in] 过滤表达式
         *
         * @return OB_SUCCESS或错误码
         */
        int add_filter(ObSqlExpression *expr);

        /**
         * 指定limit/offset
         *
         * @param limit [in]
         * @param offset [in]
         *
         * @return OB_SUCCESS或错误码
         */
        int set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset);
        void set_phy_plan(ObPhysicalPlan *the_plan);

        //add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
        int get_sub_query_index(int32_t idx,int32_t & index);

        int get_sub_query_expr(int32_t idx,ObSqlExpression* &expr,int &query_index){return filter_.get_sub_query_expr(idx,expr,query_index);};

        int remove_or_sub_query_expr();

        void update_sub_query_num();

        int copy_filter(ObFilter &object);
        //add:e

        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // disallow copy
        ObTableMemScan(const ObTableMemScan &other);
        ObTableMemScan& operator=(const ObTableMemScan &other);
      private:
        // data members
        ObRename rename_;
        ObProject project_;
        ObFilter filter_;
        ObLimit limit_;
        bool has_rename_;
        bool has_project_;
        bool has_filter_;
        bool has_limit_;
        bool plan_generated_;
    };
    inline void ObTableMemScan::set_phy_plan(ObPhysicalPlan *the_plan)
    {
      ObPhyOperator::set_phy_plan(the_plan);
      rename_.set_phy_plan(the_plan);
      project_.set_phy_plan(the_plan);
      filter_.set_phy_plan(the_plan);
      limit_.set_phy_plan(the_plan);
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLE_MEM_SCAN_H */
