/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_deallocate_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_DEALLOCATE_STMT_H_
#define OCEANBASE_SQL_OB_DEALLOCATE_STMT_H_
#include "ob_basic_stmt.h"
#include "common/ob_string.h"
 
namespace oceanbase
{
  namespace sql
  {
    class ObDeallocateStmt : public ObBasicStmt
    {
    public:
      ObDeallocateStmt() 
        : ObBasicStmt(T_DEALLOCATE)
      {
      }
      virtual ~ObDeallocateStmt() {}

      void set_stmt_name(const common::ObString& name);
      const common::ObString& get_stmt_name() const;

      virtual void print(FILE* fp, int32_t level, int32_t index);

    private:
      common::ObString  stmt_name_;
    };

    inline void ObDeallocateStmt::set_stmt_name(const common::ObString& name)
    {
      stmt_name_ = name;
    }

    inline const common::ObString& ObDeallocateStmt::get_stmt_name() const
    {
      return stmt_name_;
    }
  }
}

#endif //OCEANBASE_SQL_OB_DEALLOCATE_STMT_H_






