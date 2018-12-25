/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_create_table.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_CREATE_TABLE_H_
#define OCEANBASE_SQL_OB_CREATE_TABLE_H_

#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_schema_service.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRootRpcProxy;
  } // end namespace mergeserver

  namespace sql
  {
    class ObCreateTable: public ObNoChildrenPhyOperator
    {
      public:
        ObCreateTable();
        virtual ~ObCreateTable();
        // init
        void set_sql_context(const ObSqlContext &context);
        common::TableSchema& get_table_schema();
        void set_if_not_exists(bool if_not_exists);

        /// execute the create table statement
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        virtual ObPhyOperatorType get_type() const { return PHY_CREATE_TABLE; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      private:
        void grant_owner_privilege();
        // disallow copy
        ObCreateTable(const ObCreateTable &other);
        ObCreateTable& operator=(const ObCreateTable &other);
      private:
        // data members
        bool                  if_not_exists_;
        common::TableSchema   table_schema_;
        ObSqlContext local_context_;
    };

    inline common::TableSchema& ObCreateTable::get_table_schema()
    {
      return table_schema_;
    }

    inline void ObCreateTable::set_if_not_exists(bool if_not_exists)
    {
      if_not_exists_ = if_not_exists;
    }

    inline int ObCreateTable::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObCreateTable::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_CREATE_TABLE_H_ */
