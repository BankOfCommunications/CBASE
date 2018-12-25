/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_show_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_SHOW_STMT_H_
#define OCEANBASE_SQL_OB_SHOW_STMT_H_
#include "ob_stmt.h"
#include "common/ob_string.h"

namespace oceanbase
{
  namespace sql
  {
    class ObShowStmt : public ObStmt
    {
    public:
      ObShowStmt(common::ObStringBuf* name_pool, StmtType stmt_type);
      virtual ~ObShowStmt();

      void set_global_scope(bool global_scope);
      void set_sys_table(uint64_t table_id);
      void set_show_table(uint64_t table_id);
      void set_warnings_limit(int64_t offset = 0, int64_t count = -1);
      void set_count_warnings(bool count_warnings);
      void set_full_process(bool full_process);
      void set_user_name(common::ObString name);
      void set_db_name(common::ObString name);//add liumz, [multi_database.sql]:20150613
      uint64_t get_sys_table_id() const;
      uint64_t get_show_table_id() const;
      int64_t get_warnings_offset() const;
      int64_t get_warnings_count() const;
      bool is_global_scope() const;
      bool is_count_warnings() const;
      bool is_full_process() const;
      const common::ObString& get_user_name() const;
      const common::ObString& get_db_name() const;//add liumz, [multi_database.sql]:20150613
      const common::ObString& get_like_pattern() const;
      int set_like_pattern(const common::ObString like_pattern);
      void print(FILE* fp, int32_t level, int32_t index = 0);
    private:
      // disallow copy
      ObShowStmt(const ObShowStmt &other);
      ObShowStmt& operator=(const ObShowStmt &other);
    private:
      uint64_t          sys_table_id_;
      // table_id_ is for  T_SHOW_COLUMNS, T_SHOW_CREATE_TABLE only
      uint64_t          show_table_id_;
      // like_pattern_  and where_expr_ids_ are for
      // T_SHOW_TABLES, T_SHOW_VARIABLES, T_SHOW_COLUMNS
      // T_SHOW_TABLE_STATUS, T_SHOW_SERVER_STATUS only
      common::ObString  like_pattern_;
      // global_scope_ is for T_SHOW_VARIABLES only
      bool              global_scope_;
      // following three items are for T_SHOW_WARNINGS
      int64_t           offset_;
      int64_t           row_count_; // -1 means all
      bool              count_warnings_;
      // for T_SHOW_PROCESSLIST
      bool  full_process_;
      // for T_SHOW_GRANTS
      common::ObString  user_name_;
      common::ObString  db_name_;

    };

    inline void ObShowStmt::set_global_scope(bool global_scope)
    {
      global_scope_ = global_scope;
    }
    inline void ObShowStmt::set_show_table(uint64_t table_id)
    {
      show_table_id_ = table_id;
    }
    inline void ObShowStmt::set_sys_table(uint64_t table_id)
    {
      sys_table_id_ = table_id;
    }
    inline void ObShowStmt::set_warnings_limit(int64_t offset, int64_t count)
    {
      row_count_ = count;
      offset_ = offset;
    }
    inline void ObShowStmt::set_count_warnings(bool count_warnings)
    {
      count_warnings_ = count_warnings;
    }
    inline void ObShowStmt::set_full_process(bool full_process)
    {
      full_process_ = full_process;
    }
    inline void ObShowStmt::set_user_name(common::ObString name)
    {
      user_name_ = name;
    }
    //add liumz, [multi_database.sql]:20150613
    inline void ObShowStmt::set_db_name(common::ObString name)
    {
      db_name_ = name;
    }
    //add:e
    inline uint64_t ObShowStmt::get_sys_table_id() const
    {
      return sys_table_id_;
    }
    inline uint64_t ObShowStmt::get_show_table_id() const
    {
      return show_table_id_;
    }
    inline const common::ObString& ObShowStmt::get_like_pattern() const
    {
      return like_pattern_;
    }
    inline int64_t ObShowStmt::get_warnings_offset() const
    {
      return offset_;
    }
    inline int64_t ObShowStmt::get_warnings_count() const
    {
      return row_count_;
    }
    inline bool ObShowStmt::is_global_scope() const
    {
      return global_scope_;
    }
    inline bool ObShowStmt::is_count_warnings() const
    {
      return count_warnings_;
    }
    inline bool ObShowStmt::is_full_process() const
    {
      return full_process_;
    }
    inline const common::ObString& ObShowStmt::get_user_name() const
    {
      return user_name_;
    }
    //add liumz, [multi_database.sql]:20150613
    inline const common::ObString& ObShowStmt::get_db_name() const
    {
      return db_name_;
    }
    //add:e
  }
}

#endif //OCEANBASE_SQL_OB_SHOW_STMT_H_
