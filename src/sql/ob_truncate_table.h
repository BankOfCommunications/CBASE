/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_truncate_table.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TRUNCATE_TABLE_H
#define _OB_TRUNCATE_TABLE_H 1
#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_strings.h"
#include "sql/ob_sql_context.h"//add liumz, [drop table -> clean table priv]20150902
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRootRpcProxy;
  } // end namespace mergeserver

  namespace sql
  {
    class ObTruncateTable: public ObNoChildrenPhyOperator
    {
      public:
        ObTruncateTable();
        virtual ~ObTruncateTable();
        virtual void reset();
        virtual void reuse();
        // init
        void set_rpc_stub(mergeserver::ObMergerRootRpcProxy* rpc);
        void set_sql_context(const ObSqlContext &context);
        void set_if_exists(bool if_exists);
        void set_comment(const common::ObString & comment);
        int add_table_name(const common::ObString &tname);
        int add_table_id(const uint64_t &tid);

        /// execute the insert statement
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_TRUNCATE_TABLE; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      private:
        // types and constants
      private:
        // disallow copy
        ObTruncateTable(const ObTruncateTable &other);
        ObTruncateTable& operator=(const ObTruncateTable &other);
        // function members
      private:
        // data members
        bool if_exists_;
        common::ObString comment_;
        common::ObStrings tables_;
        mergeserver::ObMergerRootRpcProxy* rpc_;
        common::ObArray<uint64_t> table_ids_;
        ObSqlContext local_context_;
    };

    inline int ObTruncateTable::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObTruncateTable::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }

    inline void ObTruncateTable::set_rpc_stub(mergeserver::ObMergerRootRpcProxy* rpc)
    {
      rpc_ = rpc;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TRUNCATE_TABLE_H */
