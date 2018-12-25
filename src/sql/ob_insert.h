/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_insert.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_INSERT_H
#define _OB_INSERT_H 1
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
    class ObInsert: public ObSingleChildPhyOperator
    {
      public:
        ObInsert();
        virtual ~ObInsert();
        virtual void reset();
        virtual void reuse();
        void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc);
        void set_rowkey_info(const common::ObRowkeyInfo &rowkey_info);

        void set_table_id(const uint64_t tid);
        void set_replace(bool is_replace);
        // set insert columns
        int set_columns_desc(const common::ObRowDescExt &cols_desc);

        /// execute the insert statement
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_INSERT; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      private:
        // types and constants
      private:
        // disallow copy
        ObInsert(const ObInsert &other);
        ObInsert& operator=(const ObInsert &other);
        // function members
        int insert_by_mutator();
        bool is_rowkey_column(uint64_t cid);
      private:
        // data members
        mergeserver::ObMergerRpcProxy* rpc_;
        uint64_t table_id_;
        bool is_replace_; // default true
        common::ObRowkeyInfo rowkey_info_;
        common::ObRowDescExt cols_desc_; // descriptor of columns to be insert
        common::ObMutator mutator_;
        common::ObRow table_schema_;
    };

    inline void ObInsert::set_table_id(const uint64_t tid)
    {
      table_id_ = tid;
    }

    inline void ObInsert::set_replace(bool is_replace)
    {
      is_replace_ = is_replace;
    }

    inline void ObInsert::set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc)
    {
      rpc_ = rpc;
    }

    inline void ObInsert::set_rowkey_info(const common::ObRowkeyInfo &rowkey_info)
    {
      rowkey_info_ = rowkey_info;
    }

    inline int ObInsert::set_columns_desc(const common::ObRowDescExt &cols_desc)
    {
      cols_desc_ = cols_desc;
      return common::OB_SUCCESS;
    }

    inline int ObInsert::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObInsert::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_INSERT_H */
