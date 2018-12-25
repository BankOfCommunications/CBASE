/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_update.h
 *
 * Authors:
 *   Huang Yu <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_UPDATE_H
#define _OB_UPDATE_H 1
#include "sql/ob_single_child_phy_operator.h"
#include "sql/ob_sql_expression.h"
#include "common/ob_mutator.h"
#include "common/ob_schema.h"
#include "common/ob_row_desc_ext.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRpcProxy;
  } // end namespace mergeserver

  namespace sql
  {
    class ObUpdate: public ObSingleChildPhyOperator
    {
      public:
        ObUpdate();
        virtual ~ObUpdate();
        virtual void reset();
        virtual void reuse();
        void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc);
        void set_rowkey_info(const common::ObRowkeyInfo &rowkey_info);

        void set_table_id(const uint64_t tid);
        // set desc of rowkey columns, used for type cast
        int set_columns_desc(const common::ObRowDescExt &cols_desc);
        // add columns and updated values
        int add_update_expr(const uint64_t column_id, const ObSqlExpression &expr);
        /// execute the update statement
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_UPDATE; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      private:
        // types and constants
      private:
        // disallow copy
        ObUpdate(const ObUpdate &other);
        ObUpdate& operator=(const ObUpdate &other);
        // function members
        int update_by_mutator();
        /**
         * build rowkey taking ObRow as data source
         * @param table_id[in]      table id
         * @param row[in]           data source, in ObRow type
         * @param rowkey_info[in]   rowkey infomation, provide rowkey column ids
         * @param obj[in]           copy data from row to this obj array
         * @param obj_size[in]      obj array size
         * @param rowkey[out]       result rowkey
         */
        int get_row_key(const uint64_t table_id,
            const ObRow &row,
            const ObRowkeyInfo &rowkey_info,
            ObObj *rowkey_objs,
            const int64_t obj_size,
            ObRowkey &rowkey);
      private:
        // data members
        mergeserver::ObMergerRpcProxy* rpc_;
        uint64_t table_id_;
        common::ObRowkeyInfo rowkey_info_;
        common::ObRowDescExt cols_desc_; // set desc of rowkey columns, used for type cast
        common::ObMutator mutator_;
        common::ObArray<uint64_t> update_column_ids_;
        ObExpressionArray update_column_exprs_;
    };

    inline void ObUpdate::set_table_id(const uint64_t tid)
    {
      table_id_ = tid;
    }

    inline void ObUpdate::set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc)
    {
      rpc_ = rpc;
    }

    inline void ObUpdate::set_rowkey_info(const common::ObRowkeyInfo &rowkey_info)
    {
      rowkey_info_ = rowkey_info;
    }

    inline int ObUpdate::set_columns_desc(const common::ObRowDescExt &cols_desc)
    {
      cols_desc_ = cols_desc;
      return common::OB_SUCCESS;
    }

    inline int ObUpdate::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObUpdate::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPDATE_H */
