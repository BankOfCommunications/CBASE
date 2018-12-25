/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * priv_build_plan.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _PRIV_BUILD_PLAN_H
#define _PRIV_BUILD_PLAN_H 1
#include "parse_node.h"
#include "parse_malloc.h"
#include "sql/ob_logical_plan.h"
#include "common/ob_string_buf.h"
#include <stdint.h>

// mod zhangcd 20150724:b
//#define PARSER_LOG(...) snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, __VA_ARGS__)
#define PARSER_LOG(...)                                                      \
  do{                                                                        \
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, __VA_ARGS__);   \
    TBSYS_LOG(WARN, __VA_ARGS__);                                            \
  } while(0)
// mod:e

inline oceanbase::sql::ObLogicalPlan* get_logical_plan(ResultPlan* result_plan)
{
  oceanbase::sql::ObLogicalPlan* ret = NULL;
  if (result_plan->plan_tree_ == NULL)
  {
    void *ptr = parse_malloc(sizeof(oceanbase::sql::ObLogicalPlan), result_plan->name_pool_);
    if (NULL != ptr)
    {
      oceanbase::common::ObStringBuf* name_pool = static_cast<oceanbase::common::ObStringBuf*>(result_plan->name_pool_);
      ret = new(ptr) oceanbase::sql::ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = ret;
    }
  }
  else
  {
    ret = static_cast<oceanbase::sql::ObLogicalPlan*>(result_plan->plan_tree_);
  }
  return ret;
}

template <class T>
T* create_stmt(ResultPlan* result_plan)
{
  T* stmt = NULL;
  void *ptr = NULL;
  if (NULL == (ptr = parse_malloc(sizeof(T), result_plan->name_pool_)))
  {
    PARSER_LOG("Can not allocate memory for stmt");
  }
  else
  {
    stmt = new(ptr) T();
  }
  return stmt;
}

template <class T>
int prepare_resolve_stmt(ResultPlan* result_plan,
                         uint64_t& query_id,
                         T* &stmt)
{
  OB_ASSERT(result_plan);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  query_id = OB_INVALID_ID;
  oceanbase::sql::ObLogicalPlan *logical_plan = NULL;
  stmt = NULL;
  if (NULL == (logical_plan = get_logical_plan(result_plan)))
  {
    ret = OB_ERR_PARSER_MALLOC_FAILED;
    PARSER_LOG("Failed to allocate memory");
  }
  else if (NULL == (stmt = create_stmt<T>(result_plan)))
  {
    ret = OB_ERR_PARSER_MALLOC_FAILED;
    PARSER_LOG("Failed to allocate memory");
  }
  else
  {
    query_id = logical_plan->generate_query_id();
    stmt->set_query_id(query_id);
    if (OB_SUCCESS != (ret = logical_plan->add_query(stmt)))
    {
      PARSER_LOG("Can not add stmt to logical plan");
      stmt->~T();
      stmt = NULL;
    }
  }
  return ret;
}

//add liumz, [multi_database.sql]:20150613
int construct_db_table_name(ResultPlan *&result_plan, const ObString &db_name, ObString &table_name);
int write_db_table_name(const ObString &db_name, const ObString &table_name, ObString &db_table_name);
// privilege related statement

int resolve_create_user_stmt(ResultPlan* result_plan,
                             ParseNode* node,
                             uint64_t& query_id);
int resolve_drop_user_stmt(ResultPlan* result_plan,
                           ParseNode* node,
                           uint64_t& query_id);
int resolve_set_password_stmt(ResultPlan* result_plan,
                              ParseNode* node,
                              uint64_t& query_id);
int resolve_rename_user_stmt(ResultPlan* result_plan,
                             ParseNode* node,
                             uint64_t& query_id);
int resolve_lock_user_stmt(ResultPlan* result_plan,
                           ParseNode* node,
                           uint64_t& query_id);
int resolve_grant_stmt(ResultPlan* result_plan,
                       ParseNode* node,
                       uint64_t& query_id);
int resolve_revoke_stmt(ResultPlan* result_plan,
                        ParseNode* node,
                        uint64_t& query_id);
//add wenghaixing [database manage]20150608
int resolve_create_db_stmt(ResultPlan* result_plan,
                           ParseNode* node,
                           uint64_t& query_id
                           );
int resolve_use_db_stmt(ResultPlan* result_plan,
                        ParseNode* node,
                        uint64_t& query_id
                        );
int resolve_drop_db_stmt(ResultPlan* result_plan,
                         ParseNode* node,
                         uint64_t& query_id
                        );
//add e
#endif /* _PRIV_BUILD_PLAN_H */
