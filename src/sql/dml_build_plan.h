/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * dml_build_plan.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef DML_BUILD_PLAN_H_
#define DML_BUILD_PLAN_H_

#include "parse_node.h"
#include "parse_malloc.h"
#include "sql/ob_logical_plan.h"
#include "common/ob_vector.h"
#include "sql/ob_stmt.h"
#include <stdint.h>
//add wenghaixing [secondary index create fix]20141226
#include "common/ob_strings.h"
//add e
/*
  * Expressions from different scope have different limitations,
  * we need a flage to distinguish where they are from.
  */
#define T_NONE_LIMIT      0
#define T_WHERE_LIMIT     1
#define T_GROUP_LIMIT     2
#define T_HAVING_LIMIT    3
#define T_INSERT_LIMIT    4
#define T_UPDATE_LIMIT    5
#define T_AGG_LIMIT       6
#define T_VARIABLE_VALUE_LIMIT 7
#define T_WHEN_LIMIT      8

static struct oceanbase::sql::ObColumnInfo default_;

extern int resolve_select_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
extern int resolve_delete_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
extern int resolve_insert_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
extern int resolve_update_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
extern int resolve_table(
    ResultPlan * result_plan,
    oceanbase::sql::ObStmt* stmt,
    ParseNode* node,
    uint64_t& table_id);
extern int resolve_and_exprs(
    ResultPlan * result_plan,
    oceanbase::sql::ObStmt* stmt,
    ParseNode* node,
    oceanbase::common::ObVector<uint64_t>& and_exprs,
    int32_t expr_scope_type = T_NONE_LIMIT);
//mod dragon [varchar limit] 2016-8-10 10:54:12
/*--new: 增加一个参数，用于判读表达式长度--*/
extern int resolve_independ_expr(ResultPlan * result_plan,
    oceanbase::sql::ObStmt* stmt,
    ParseNode* node,
    uint64_t& expr_id,
    int32_t expr_scope_type = T_NONE_LIMIT,
    oceanbase::sql::ObColumnInfo col_info = default_
    );
/*----old----
extern int resolve_independ_expr(
    ResultPlan * result_plan,
    oceanbase::sql::ObStmt* stmt,
    ParseNode* node,
    uint64_t& expr_id,
    int32_t expr_scope_type = T_NONE_LIMIT);
*/
//mod 2016-8-10e
extern int resolve_table_columns(
    ResultPlan * result_plan,
    oceanbase::sql::ObStmt* stmt,
    oceanbase::sql::TableItem& table_item,
    int64_t num_columns = -1);
// add by zcd:b
extern int generate_index_table_name(
    const char* index_name,
    uint64_t src_tid,
    char *out_buff,
    size_t buf_size,
    int64_t& str_len);
// add :e
// add by whx:b
extern int generate_index_table_name(ObString index_name,
    uint64_t src_tid,
    char *out_buff,
    int64_t& str_len);
// add :e
//add wenghaixing [secondary index create fix]20141226
extern int generate_expire_col_list(ObString input, oceanbase::common::ObStrings &out);
//add e

//add liuzy [sequence] [JHOBv0.1] 20150327:b
/*Exp: 解析create sequence语句*/
extern int resolve_sequence_create_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
extern int resolve_sequence_drop_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
//20150327:e
#endif //DML_BUILD_PLAN_H_

