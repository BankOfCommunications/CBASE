/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_explain_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_EXPLAIN_STMT_H_
#define OCEANBASE_SQL_OB_EXPLAIN_STMT_H_
#include "ob_basic_stmt.h"
#include "common/ob_define.h"
 
namespace oceanbase
{
  namespace sql
  {
    class ObExplainStmt : public ObBasicStmt
    {
    public:
      ObExplainStmt() 
        : ObBasicStmt(T_EXPLAIN)
      {
        verbose_ = false;
        explain_query_id_ = common::OB_INVALID_ID;
      }
      virtual ~ObExplainStmt() {}

      void set_verbose(bool verbose);
      void set_explain_query_id(const uint64_t query_id);
      bool is_verbose() const;
      uint64_t get_explain_query_id() const;

      virtual void print(FILE* fp, int32_t level, int32_t index);

    private:
      bool  verbose_;
      uint64_t  explain_query_id_;
    };

    inline void ObExplainStmt::set_verbose(bool verbose)
    {
      verbose_ = verbose;
    }

    inline bool ObExplainStmt::is_verbose() const
    {
      return verbose_;
    }
    
    inline uint64_t ObExplainStmt::get_explain_query_id() const
    { 
      return explain_query_id_; 
    }
    
    inline void ObExplainStmt::set_explain_query_id(const uint64_t query_id)
    {
      explain_query_id_ = query_id;
    }
  }
}

#endif //OCEANBASE_SQL_OB_EXPLAIN_STMT_H_




