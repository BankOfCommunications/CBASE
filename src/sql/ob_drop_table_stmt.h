/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_drop_table_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_DROP_TABLE_STMT_H_
#define OCEANBASE_SQL_OB_DROP_TABLE_STMT_H_

#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "sql/ob_basic_stmt.h"
#include "parse_node.h"

namespace oceanbase
{
  namespace sql
  {
    class ObDropTableStmt : public ObBasicStmt
    {
    public:
      explicit ObDropTableStmt(common::ObStringBuf* name_pool);
      virtual ~ObDropTableStmt();

      void set_if_exists(bool if_not_exists);
      int add_table_name_id(ResultPlan& result_plan, const common::ObString& table_name);

      bool get_if_exists() const;
      int64_t get_table_size() const;
      const common::ObString& get_table_name(int64_t index) const;
      void print(FILE* fp, int32_t level, int32_t index = 0);
      int64_t get_table_count() const;
      uint64_t get_table_id(int64_t index) const;
    //protected:
    //  void print_indentation(FILE* fp, int32_t level);

    protected:
      common::ObStringBuf*        name_pool_;

    private:
      bool  if_exists_;
      common::ObArray<common::ObString>   tables_;
      common::ObArray<uint64_t> table_ids_;
    };

    inline int64_t ObDropTableStmt::get_table_count() const
    {
      return table_ids_.count();
    }
    inline uint64_t ObDropTableStmt::get_table_id(int64_t index) const
    {
      OB_ASSERT( 0 <= index && index < table_ids_.count());
      return table_ids_.at(index);
    }
    inline void ObDropTableStmt::set_if_exists(bool if_exists)
    {
      if_exists_ = if_exists;
    }

    inline bool ObDropTableStmt::get_if_exists() const
    {
      return if_exists_;
    }

    inline int64_t ObDropTableStmt::get_table_size() const
    {
      return tables_.count();
    }
    
    inline const common::ObString& ObDropTableStmt::get_table_name(int64_t index) const
    {
      OB_ASSERT(0 <= index && index < tables_.count()); 
      return tables_.at(index);
    }
  }
}

#endif //OCEANBASE_SQL_OB_DROP_TABLE_STMT_H_


