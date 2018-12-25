/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_end_trans_stmt.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_END_TRANS_STMT_H
#define _OB_END_TRANS_STMT_H 1
#include "ob_basic_stmt.h"
namespace oceanbase
{
  namespace sql
  {
    class ObEndTransStmt: public ObBasicStmt
    {
      public:
        ObEndTransStmt(): ObBasicStmt(T_END_TRANS), is_rollback_(false) {}
        virtual ~ObEndTransStmt(){}
        virtual void print(FILE* fp, int32_t level, int32_t index);

        void set_is_rollback(bool val) {is_rollback_ = val;}
        bool get_is_rollback() const {return is_rollback_;}
      private:
        // types and constants
      private:
        // disallow copy
        ObEndTransStmt(const ObEndTransStmt &other);
        ObEndTransStmt& operator=(const ObEndTransStmt &other);
        // function members
      private:
        // data members
        bool is_rollback_;
    };
    inline void ObEndTransStmt::print(FILE* fp, int32_t level, int32_t index)
    {
      print_indentation(fp, level);
      fprintf(fp, "<ObEndTransStmt id=%d>\n", index);
      print_indentation(fp, level + 1);
      fprintf(fp, "IsRollback := %c\n", is_rollback_?'Y':'N');
      print_indentation(fp, level);
      fprintf(fp, "</ObEndTransStmt>\n");
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_END_TRANS_STMT_H */
