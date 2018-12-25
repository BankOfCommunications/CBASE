/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_delete.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *   Huang Yu <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_DELETE_H
#define _OB_DELETE_H 1
#include "sql/ob_single_child_phy_operator.h"
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
    class ObDelete: public ObSingleChildPhyOperator
    {
      public:
        ObDelete();
        virtual ~ObDelete();
        virtual void reset();
        virtual void reuse();
        void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc);
        void set_rowkey_info(const common::ObRowkeyInfo &rowkey_info);

        void set_table_id(const uint64_t tid);
        // set desc of rowkey columns, used for type cast
        int set_columns_desc(const common::ObRowDescExt &cols_desc);

        /// execute the delete statement
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_DELETE; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      private:
        // types and constants
      private:
        // disallow copy
        ObDelete(const ObDelete &other);
        ObDelete& operator=(const ObDelete &other);
        // function members
        int delete_by_mutator();
      private:
        // data members
        mergeserver::ObMergerRpcProxy* rpc_;
        uint64_t table_id_;
        common::ObRowkeyInfo rowkey_info_;
        common::ObRowDescExt cols_desc_; // set desc of rowkey columns, used for type cast
        common::ObMutator mutator_;
    };

    inline void ObDelete::set_table_id(const uint64_t tid)
    {
      table_id_ = tid;
    }

    inline void ObDelete::set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc)
    {
      rpc_ = rpc;
    }

    inline void ObDelete::set_rowkey_info(const common::ObRowkeyInfo &rowkey_info)
    {
      rowkey_info_ = rowkey_info;
    }

    inline int ObDelete::set_columns_desc(const common::ObRowDescExt &cols_desc)
    {
      cols_desc_ = cols_desc;
      return common::OB_SUCCESS;
    }

    inline int ObDelete::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObDelete::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_DELETE_H */
