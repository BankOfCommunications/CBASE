/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_start_trans_stmt.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_START_TRANS_STMT_H
#define _OB_START_TRANS_STMT_H 1
#include "ob_basic_stmt.h"
namespace oceanbase
{
  namespace sql
  {
    class ObStartTransStmt: public ObBasicStmt
    {
      public:
        ObStartTransStmt();
        virtual ~ObStartTransStmt();
        virtual void print(FILE* fp, int32_t level, int32_t index);

        void set_with_consistent_snapshot(bool val);
        bool get_with_consistent_snapshot() const;
      private:
        // types and constants
      private:
        // disallow copy
        ObStartTransStmt(const ObStartTransStmt &other);
        ObStartTransStmt& operator=(const ObStartTransStmt &other);
        // function members
      private:
        // data members
        bool with_consistent_snapshot_;
    };

    inline ObStartTransStmt::ObStartTransStmt()
      :ObBasicStmt(T_START_TRANS), with_consistent_snapshot_(false)
    {
    }
    inline ObStartTransStmt::~ObStartTransStmt()
    {
    }
    inline void ObStartTransStmt::set_with_consistent_snapshot(bool val)
    {
      with_consistent_snapshot_ = val;
    }
    inline bool ObStartTransStmt::get_with_consistent_snapshot() const
    {
      return with_consistent_snapshot_;
    }
    inline void ObStartTransStmt::print(FILE* fp, int32_t level, int32_t index)
    {
      print_indentation(fp, level);
      fprintf(fp, "<ObStartTransStmt id=%d>\n", index);
      print_indentation(fp, level + 1);
      fprintf(fp, "WithConsistentSnapshot := %c\n", with_consistent_snapshot_?'Y':'N');
      print_indentation(fp, level);
      fprintf(fp, "</ObStartTransStmt>\n");
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_START_TRANS_STMT_H */
