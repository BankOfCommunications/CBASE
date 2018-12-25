/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_prepare_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_PREPARE_STMT_H_
#define OCEANBASE_SQL_OB_PREPARE_STMT_H_
#include "ob_basic_stmt.h"
#include "common/ob_string.h"
 
namespace oceanbase
{
  namespace sql
  {
    class ObPrepareStmt : public ObBasicStmt
    {
    public:
      ObPrepareStmt() 
        : ObBasicStmt(T_PREPARE)
      {
        prepare_query_id_ = common::OB_INVALID_ID;
      }
      virtual ~ObPrepareStmt() {}

      void set_stmt_name(const common::ObString& name);
      void set_prepare_query_id(const uint64_t query_id);
      const common::ObString& get_stmt_name() const;
      uint64_t get_prepare_query_id() const;

      virtual void print(FILE* fp, int32_t level, int32_t index);

    private:
      common::ObString  stmt_name_;
      common::ObString  prepare_sql_;
      uint64_t  prepare_query_id_;
    };
    inline void ObPrepareStmt::set_stmt_name(const common::ObString& name)
    {
      stmt_name_ = name;
    }
    inline const common::ObString& ObPrepareStmt::get_stmt_name() const
    {
      return stmt_name_;
    }
    inline uint64_t ObPrepareStmt::get_prepare_query_id() const
    { 
      return prepare_query_id_; 
    }
    inline void ObPrepareStmt::set_prepare_query_id(const uint64_t query_id)
    {
      prepare_query_id_ = query_id;
    }
  }
}

#endif //OCEANBASE_SQL_OB_PREPARE_STMT_H_





