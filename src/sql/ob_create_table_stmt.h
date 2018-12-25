/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_create_table_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_CREATE_TABLE_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_TABLE_STMT_H_

#include "common/ob_array.h"
#include "common/ob_object.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_strings.h"
#include "common/ob_hint.h"
#include "sql/ob_basic_stmt.h"
#include "sql/ob_column_def.h"
#include "parse_node.h"

namespace oceanbase
{
  namespace sql
  {
    class ObCreateTableStmt : public ObBasicStmt
    {
    public:
      explicit ObCreateTableStmt(common::ObStringBuf* name_pool);
      virtual ~ObCreateTableStmt();

      uint64_t gen_column_id();
      void set_tablet_max_size(int64_t size);
      void set_tablet_block_size(int64_t size);
      void set_replica_num(int32_t num);
      void set_use_bloom_filter(bool use_bloom_filter);
      void set_consistency_level(int64_t consistency_level);
      void set_if_not_exists(bool if_not_exists);
      int set_table_name(ResultPlan& result_plan, const common::ObString& table_name);
      int set_table_id(ResultPlan& result_plan, const uint64_t table_id);
      int set_join_info(const common::ObString& join_info);
      int set_expire_info(const common::ObString& expire_info);
      int set_compress_method(const common::ObString& compress_method);
      int set_comment_str(const common::ObString& comment);
      int add_primary_key_part(uint64_t primary_key_part);
      int add_primary_key_part(ResultPlan& result_plan, const common::ObString& column_name);
      int add_column_def(ResultPlan& result_plan, const ObColumnDef& column);

      int64_t get_tablet_max_size() const;
      int64_t get_tablet_block_size() const;
      uint64_t get_table_id() const;
      int32_t get_replica_num() const;
      int64_t get_primary_key_size() const;
      int64_t get_column_size() const;
      bool use_bloom_filter() const;
      bool read_static() const;
      bool get_if_not_exists() const;
      const common::ObString& get_table_name() const;
      //add dolphin [database manager]@20150609:b
      const common::ObString& get_db_name() const;
      int set_db_name(ResultPlan& result_plan,const common::ObString& dbname);
      //add:e
      const common::ObString& get_join_info() const;
      const common::ObString& get_expire_info() const;
      const common::ObString& get_compress_method() const;
      const common::ObString& get_comment_str() const;
      const common::ObArray<uint64_t>& get_primary_key() const;
      const ObColumnDef& get_column_def(int64_t index) const;
      common::ObConsistencyLevel get_consistency_level();

      virtual void print(FILE* fp, int32_t level, int32_t index = 0);

    //protected:
    //  void print_indentation(FILE* fp, int32_t level);

    protected:
      common::ObStringBuf*        name_pool_;

    private:
      uint64_t                    next_column_id_;
      common::ObString            table_name_;
      common::ObString            dbname_;//add dolphin [database manager]@20150609
      common::ObArray<uint64_t>   primay_keys_;
      common::ObArray<ObColumnDef> columns_;

      int64_t                     tablet_max_size_;
      int64_t                     tablet_block_size_;
      uint64_t                    table_id_;
      int32_t                     replica_num_;
      bool                        use_bloom_filter_;
      common::ObConsistencyLevel            consistency_level_;
      bool                        if_not_exists_;
      common::ObString            join_info_;
      common::ObString            expire_info_;
      common::ObString            compress_method_;
      common::ObString            comment_str_;
      // for future use: create table xxx as select ......
      //ObSelectStmt                *select_clause;
      // create table xxx as already_exist_table, pay attention to whether data are need
    };

    inline uint64_t ObCreateTableStmt::gen_column_id()
    {
      return next_column_id_++;
    }
    inline void ObCreateTableStmt::set_tablet_max_size(int64_t size)
    {
      tablet_max_size_ = size;
    }
    inline void ObCreateTableStmt::set_tablet_block_size(int64_t size)
    {
      tablet_block_size_ = size;
    }
    inline void ObCreateTableStmt::set_replica_num(int32_t num)
    {
      replica_num_ = num;
    }
    inline void ObCreateTableStmt::set_use_bloom_filter(bool use_bloom_filter)
    {
      use_bloom_filter_ = use_bloom_filter;
    }
    inline void ObCreateTableStmt::set_consistency_level(int64_t consistency_level)
    {
      consistency_level_ = (common::ObConsistencyLevel) consistency_level;
    }
    inline common::ObConsistencyLevel ObCreateTableStmt::get_consistency_level()
    {
      return consistency_level_;
    }
    inline void ObCreateTableStmt::set_if_not_exists(bool if_not_exists)
    {
      if_not_exists_ = if_not_exists;
    }
    inline  int ObCreateTableStmt::add_primary_key_part(uint64_t primary_key_part)
    {
      return primay_keys_.push_back(primary_key_part);
    }
    inline int64_t ObCreateTableStmt::get_tablet_max_size() const
    {
      return tablet_max_size_;
    }
    inline int64_t ObCreateTableStmt::get_tablet_block_size() const
    {
      return tablet_block_size_;
    }
    inline uint64_t ObCreateTableStmt::get_table_id() const
    {
      return table_id_;
    }
    inline int64_t ObCreateTableStmt::get_primary_key_size() const
    {
      return primay_keys_.count();
    }
    inline int64_t ObCreateTableStmt::get_column_size() const
    {
      return columns_.count();
    }
    inline int32_t ObCreateTableStmt::get_replica_num() const
    {
      return replica_num_;
    }
    inline bool ObCreateTableStmt::use_bloom_filter() const
    {
      return use_bloom_filter_;
    }
    inline bool ObCreateTableStmt::read_static() const
    {
      return consistency_level_ == common::STATIC ? true : false;
    }
    inline bool ObCreateTableStmt::get_if_not_exists() const
    {
      return if_not_exists_;
    }
    inline const common::ObString& ObCreateTableStmt::get_table_name() const
    {
      return table_name_;
    }
    inline const common::ObString& ObCreateTableStmt::get_join_info() const
    {
      return join_info_;
    }
    inline const common::ObString& ObCreateTableStmt::get_expire_info() const
    {
      return expire_info_;
    }
    inline const common::ObString& ObCreateTableStmt::get_compress_method() const
    {
      return compress_method_;
    }
    inline const common::ObString& ObCreateTableStmt::get_comment_str() const
    {
      return comment_str_;
    }
    inline const common::ObArray<uint64_t>& ObCreateTableStmt::get_primary_key() const
    {
      return primay_keys_;
    }
    inline const common::ObString& ObCreateTableStmt::get_db_name() const
      {
        return dbname_;
      }
  }
}

#endif //OCEANBASE_SQL_OB_CREATE_TABLE_STMT_H_

