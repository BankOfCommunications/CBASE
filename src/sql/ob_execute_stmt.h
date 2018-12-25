/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_execute_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_EXECUTE_STMT_H_
#define OCEANBASE_SQL_OB_EXECUTE_STMT_H_
#include "ob_basic_stmt.h"
#include "common/ob_string.h"
#include "common/ob_array.h"
 
namespace oceanbase
{
  namespace sql
  {
    class ObExecuteStmt : public ObBasicStmt
    {
    public:
      ObExecuteStmt() 
        : ObBasicStmt(T_EXECUTE)
      {
      }
      virtual ~ObExecuteStmt() {}

      void set_stmt_name(const common::ObString& name);
      int add_variable_name(const common::ObString& name);
      const common::ObString& get_stmt_name() const;
      const common::ObString& get_variable_name(int64_t index) const;
      int64_t get_variable_size() const;

      virtual void print(FILE* fp, int32_t level, int32_t index);

    private:
      common::ObString  stmt_name_;
      common::ObArray<common::ObString> variable_names_;
    };

    inline void ObExecuteStmt::set_stmt_name(const common::ObString& name)
    {
      stmt_name_ = name;
    }
    inline const common::ObString& ObExecuteStmt::get_stmt_name() const
    {
      return stmt_name_;
    }
    inline int ObExecuteStmt::add_variable_name(const common::ObString& name)
    { 
      return variable_names_.push_back(name); 
    }
    inline int64_t ObExecuteStmt::get_variable_size() const
    {
      return variable_names_.count();
    }
    inline const common::ObString& ObExecuteStmt::get_variable_name(int64_t index) const
    {
      OB_ASSERT(0 <= index && index < variable_names_.count()); 
      return variable_names_.at(index);
    }
  }
}

#endif //OCEANBASE_SQL_OB_EXECUTE_STMT_H_






