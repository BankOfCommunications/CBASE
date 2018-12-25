/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_kill_stmt.h is for what ...
 *
 * Version: ***: ob_kill_stmt.h  Fri Apr 26 16:50:14 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_KILL_STMT_H_
#define OB_KILL_STMT_H_

#include "sql/ob_basic_stmt.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
namespace oceanbase
{
  namespace sql
  {
    class ObKillStmt : public ObBasicStmt
    {
    public:
      explicit ObKillStmt(common::ObStringBuf* name_pool);
      virtual ~ObKillStmt();

      void set_thread_id(int64_t thread_id);
      void set_is_query(bool is_query);
      void set_is_global(bool is_global);

      int64_t get_thread_id() const;
      bool is_query() const;
      bool is_global() const;
      virtual void print(FILE*, int32_t level, int32_t index=0);
    private:
      ObKillStmt(const ObKillStmt &other);
      ObKillStmt& operator=(const ObKillStmt &other);

    private:
      common::ObStringBuf* name_pool_;
      int64_t thread_id_;
      bool is_query_;
      bool is_global_;
    };

    inline void ObKillStmt::set_thread_id(int64_t thread_id)
    {
      thread_id_ = thread_id;
    }

    inline void ObKillStmt::set_is_query(bool is_query)
    {
      is_query_ = is_query;
    }

    inline void ObKillStmt::set_is_global(bool is_global)
    {
      is_global_ = is_global;
    }

    inline int64_t ObKillStmt::get_thread_id() const
    {
      return thread_id_;
    }

    inline bool ObKillStmt::is_query() const
    {
      return is_query_;
    }

    inline bool ObKillStmt::is_global() const
    {
      return is_global_;
    }
  }
}
#endif
