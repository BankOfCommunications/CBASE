/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_variable_set_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_VARIABLE_STMT_H_
#define OCEANBASE_SQL_OB_VARIABLE_STMT_H_
#include "ob_basic_stmt.h"
#include "common/ob_string.h"
#include "common/ob_array.h"
 
namespace oceanbase
{
  namespace sql
  {
    class ObVariableSetStmt : public ObBasicStmt
    {
    public:
      enum ScopeType
    	{
    	  NONE_SCOPE = 0,
    	  GLOBAL,
        SESSION,
        LOCAL,
    	};

      struct VariableSetNode
      {
        VariableSetNode()
        {
          is_system_variable_ = false;
          scope_type_ = NONE_SCOPE;
          value_expr_id_ = common::OB_INVALID_ID;
        }
        common::ObString variable_name_;
        bool is_system_variable_;
        ScopeType scope_type_;
        uint64_t value_expr_id_;
      };
      
      ObVariableSetStmt() : ObBasicStmt(T_VARIABLE_SET) {}
      virtual ~ObVariableSetStmt() {}

      int add_variable_node(const VariableSetNode& node);
      int64_t get_variables_size() const;
      const VariableSetNode& get_variable_node(int32_t index) const;

      virtual void print(FILE* fp, int32_t level, int32_t index);

    private:
      common::ObArray<VariableSetNode> variable_nodes_;
    };

    inline int ObVariableSetStmt::add_variable_node(const VariableSetNode& node)
    {
      return variable_nodes_.push_back(node);
    }
    inline const ObVariableSetStmt::VariableSetNode& ObVariableSetStmt::get_variable_node(int32_t index) const
    {
      OB_ASSERT(0 <= index && index < variable_nodes_.count());
      return variable_nodes_.at(index);
    }
    inline int64_t ObVariableSetStmt::get_variables_size() const
    {
      return variable_nodes_.count();
    }
  }
}

#endif //OCEANBASE_SQL_OB_VARIABLE_STMT_H_


