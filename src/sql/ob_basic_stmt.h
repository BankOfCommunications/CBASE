/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_basic_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OCEANBASE_SQL_OB_BASIC_STMT_H_
#define OCEANBASE_SQL_OB_BASIC_STMT_H_

#include "common/ob_define.h"

namespace oceanbase
{
  namespace sql
  {
    class ObBasicStmt
    {
    public:
      enum StmtType
      {
        T_NONE,
        T_SELECT,
        T_INSERT,
        T_REPLACE,
        T_DELETE,
        T_UPDATE,
        T_EXPLAIN,
        T_CREATE_TABLE,
        T_DROP_TABLE,
        T_ALTER_TABLE,
        T_SEQUENCE,//add liuzy [sequence] [JHOBv0.1] 20150327

        // show statements
        T_SHOW_TABLES,
        T_SHOW_SYSTEM_TABLES, // add by zhangcd [multi_database.show_tables] 20150616
        // add dolphin [show database]@20150604
        T_SHOW_DATABASES,
        T_SHOW_CURRENT_DATABASE, // add by zhangcd [multi_database.show_databases] 20150617
        //add liumengzhan_show_index [20141208]
        T_SHOW_INDEX,
        //add:e
        T_SHOW_COLUMNS,
        T_SHOW_VARIABLES,
        T_SHOW_TABLE_STATUS,
        T_SHOW_SCHEMA,
        T_SHOW_CREATE_TABLE,
        T_SHOW_PARAMETERS,
        T_SHOW_SERVER_STATUS,
        T_SHOW_WARNINGS,
        T_SHOW_GRANTS,
        T_SHOW_PROCESSLIST,

        // privileges related
        T_CREATE_USER,
        T_DROP_USER,
        T_SET_PASSWORD,
        T_LOCK_USER,
        T_RENAME_USER,
        T_GRANT,
        T_REVOKE,
        /*===========================================
         * add wenghaixing [database manage]20150608
         * this block is for coding these sql
         * CREATE DATABSE,USE DATABASE and so on
         * ==========================================*/
        T_CREATE_DATABASE,
        T_DROP_DATABASE,
        T_USE_DATABASE,
        //add e
        T_PREPARE,
        T_VARIABLE_SET,
        T_EXECUTE,
        T_DEALLOCATE,

        T_START_TRANS,
        T_END_TRANS,

        T_KILL,
        T_ALTER_SYSTEM,
        T_CHANGE_OBI,
        //add wenghaixing [secondary index] 20141024
        T_CREATE_INDEX,
        //add e
        //add wenghaixing [secondary index drop index]20141222
        T_DROP_INDEX,
        //add e

        //add zhaoqiong [Truncate Table]: 20151204:b
        T_TRUNCATE_TABLE,
        //add:e
      };

      ObBasicStmt()
        : stmt_type_(T_NONE)
      {
      }
      explicit ObBasicStmt(const StmtType stmt_type)
        : stmt_type_(stmt_type)
      {
      }
      explicit ObBasicStmt(const StmtType stmt_type, uint64_t query_id)
        : stmt_type_(stmt_type), query_id_(query_id)
      {
      }
      virtual ~ObBasicStmt() {}

      void set_stmt_type(const StmtType stmt_type);
      void set_query_id(const uint64_t query_id);
      StmtType get_stmt_type() const;
      uint64_t get_query_id() const;
      bool is_show_stmt() const;

      virtual void print(FILE* fp, int32_t level, int32_t index) = 0;
    protected:
      void print_indentation(FILE* fp, int32_t level) const;

    private:
      StmtType  stmt_type_;
      uint64_t  query_id_;
    };

    inline void ObBasicStmt::set_stmt_type(StmtType stmt_type)
    {
      stmt_type_ = stmt_type;
    }

    inline ObBasicStmt::StmtType ObBasicStmt::get_stmt_type() const
    {
      return stmt_type_;
    }

    inline uint64_t ObBasicStmt::get_query_id() const
    {
      return query_id_;
    }

    inline void ObBasicStmt::set_query_id(const uint64_t query_id)
    {
      query_id_ = query_id;
    }

    inline void ObBasicStmt::print_indentation(FILE* fp, int32_t level) const
    {
      for(int i = 0; i < level; ++i)
        fprintf(fp, "    ");
    }

    inline bool ObBasicStmt::is_show_stmt() const
    {
      return (stmt_type_ >= T_SHOW_TABLES and stmt_type_ <= T_SHOW_SERVER_STATUS);
    }
  }
}

#endif //OCEANBASE_SQL_OB_BASIC_STMT_H_
