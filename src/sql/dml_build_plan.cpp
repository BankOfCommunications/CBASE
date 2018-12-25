/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * dml_build_plan.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "dml_build_plan.h"
#include "ob_raw_expr.h"
#include "common/ob_bit_set.h"
#include "ob_select_stmt.h"
#include "ob_multi_logic_plan.h"
#include "ob_insert_stmt.h"
#include "ob_delete_stmt.h"
#include "ob_update_stmt.h"
#include "ob_schema_checker.h"
#include "ob_type_convertor.h"
#include "ob_sql_session_info.h"
#include "parse_malloc.h"
#include "common/ob_define.h"
#include "common/ob_array.h"
#include "common/ob_string_buf.h"
#include "common/utility.h"
#include "common/ob_hint.h"
#include <stdint.h>
//add liumz, [multi_database.sql]:20150615
#include "priv_build_plan.h"

//add wenghaixing [secondary index create fix]20141226
#include "common/ob_postfix_expression.h"
#include "common/ob_strings.h"
//add e
#include "ob_sequence_stmt.h"   //add liuzy [sequence] [JHOBv0.1] 20150327

#include "common/ob_expr_obj.h" //add qianzm [set_operation] 20151222
using namespace oceanbase::common;
using namespace oceanbase::sql;

extern const char* get_type_name(int type);
static bool is_order_for_listagg_exist = false;//add gaojt [ListAgg][JHOBv0.1]20151026
//mod dragon [varchar limit] 2016-8-10 10:59:06
/*---new:增加一个参数col_id---*/
int resolve_expr(ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    ObSqlRawExpr *sql_expr,
    ObRawExpr*& expr,
    int32_t expr_scope_type = T_NONE_LIMIT,
    bool sub_query_results_scalar = true,
    ObColumnInfo col_info = default_
    );
/*-----old-----
int resolve_expr(ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    ObSqlRawExpr *sql_expr,
    ObRawExpr*& expr,
    int32_t expr_scope_type = T_NONE_LIMIT,
    bool sub_query_results_scalar = true
    );
-----old---*/
int resolve_agg_func(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node,
    ObSqlRawExpr*& ret_sql_expr);
//add liumz, [ROW_NUMBER]20150825:b
int resolve_anal_func(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node,
    ObSqlRawExpr*& ret_sql_expr);
//add:e
int resolve_joined_table(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node,
    JoinedTable& joined_table);
int resolve_from_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_star(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_select_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_where_clause(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node);
int resolve_group_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
//add liumz, [ROW_NUMBER]20150825:b
int resolve_partition_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_order_clause_for_rownum(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
//add:e
int resolve_having_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_order_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
//add gaojt [ListAgg][JHOBv0.1]20140104:b
int resolve_order_clause_for_listagg(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
//add 20140104:e
int resolve_limit_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_for_update_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_insert_columns(
    ResultPlan * result_plan,
    ObInsertStmt* insert_stmt,
    ParseNode* node);
int resolve_insert_values(
    ResultPlan * result_plan,
    ObInsertStmt* insert_stmt,
    ParseNode* node);
int resolve_hints(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node);
// add by zcd 20141217 :b
int generate_index_hint(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* hint_node);
// add:e
int resolve_when_clause(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node);
int resolve_when_func(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    ObSqlRawExpr*& ret_sql_expr);

//add liuzy [sequence] [JHOBv0.1] 20150327:b
int resolve_sequence_label_list(
    ResultPlan * result_plan,
    ObSequenceStmt* sequence_stmt,
    ObInsertStmt* insert_stmt,
    ParseNode* node);
//20150327:e
ObSqlRawExpr* create_middle_sql_raw_expr(
    ResultPlan& result_plan,
    ParseNode& node,
    uint64_t& expr_id);
//add tianz [EXPORT_TOOL] 20141120:b
int resolve_range_values(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
//add 20141120:e


//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140409:b
int optimize_joins_filter(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt);
//add 20140409:e

//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140416:b
int optimize_join_where_expr(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    JoinedTable* joined_table);
//add 20140416:e


//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140416:b
int optimize_meet_condition(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    const ObSqlRawExpr* where_expr,
    const ObSqlRawExpr* join_expr,
    const ObVector<uint64_t>& old_where_exprid_store,
    bool& is_meet_opti_cnd,
    ObBinaryRefRawExpr*& ref_expr_from_join);
//add 20140416:e

//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140416:b
int optimize_gen_new_where_expr(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ObSqlRawExpr* where_expr,
    ObBinaryRefRawExpr* ref_expr_of_new_expr,
    ObVector<uint64_t>& new_expr_id_store,
    ObVector<ObSqlRawExpr*>& new_expr_store);
//add 20140416:e

//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140416:b
int optimize_gen_triple_expr(
    ResultPlan * result_plan,
    const ObTripleOpRawExpr* old_main_triple_expr,
    const ObBinaryRefRawExpr* old_sub_first_ref_expr,
    const ObConstRawExpr* old_sub_second_const_expr,
    const ObConstRawExpr* old_sub_third_const_expr,
    ObRawExpr*& new_expr);
//add 20140416:e

//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140416:b
int optimize_gen_binary_expr(
    ResultPlan * result_plan,
    const ObBinaryOpRawExpr* old_main_binary_op_expr,
    const ObBinaryRefRawExpr* old_sub_ref_expr,
    const ObConstRawExpr* old_sub_const_expr,
    const bool& sub_ref_expr_is_frist,
    ObRawExpr*& new_expr);
//add 20140416:e


//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140512:b
int optimize_split_expr(
    ResultPlan * result_plan,
    const ObSqlRawExpr* source_expr,
    ObVector<ObBinaryRefRawExpr*>& sub_ref_exprs_store,
    ObVector<ObConstRawExpr*>& sub_const_exprs_store,
    bool& is_triple_expr,
    bool& sub_ref_expr_is_frist
    );
//add 20140512:e


//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140512:b
bool optimize_ref_expr_equal(
    ObBinaryRefRawExpr* ref_expr_1,
    ObBinaryRefRawExpr* ref_expr_2
    );
//add 20140512:e

//add hushuang [bloomfilter_join] 20150420:b
int generate_join_hint(
    ResultPlan *result_plan,
    ObStmt *stmt,
    ParseNode *hint_node);
//add:e
//add lijianqiang [sequence update] 20150912:b
int add_all_row_key_expr_ids(
    ResultPlan* result_plan,
    uint64_t table_id,
    ObUpdateStmt *ObUpdateStmt);
//add 20150912:e

static int add_all_rowkey_columns_to_stmt(ResultPlan* result_plan, uint64_t table_id, ObStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* rowkey_column_schema = NULL;
  ObRowkeyInfo rowkey_info;
  int64_t rowkey_idx = 0;
  uint64_t rowkey_column_id = 0;


  if (NULL == stmt || NULL == result_plan)
  {
    TBSYS_LOG(WARN, "invalid argument. stmt=%p, result_plan=%p", stmt, result_plan);
    ret = OB_INVALID_ARGUMENT;
  }

  if (ret == OB_SUCCESS)
  {
    if (NULL == (schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_)))
    {
      ret = OB_ERR_SCHEMA_UNSET;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Schema(s) are not set");
    }
    else if (NULL == (table_schema = schema_checker->get_table_schema(table_id)))
    {
      ret = OB_ERR_TABLE_UNKNOWN;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Table schema not found");
    }
    else
    {
      // fill rowkey columns to statement, which must be returned in order to delete row
      rowkey_info = table_schema->get_rowkey_info();
      for (rowkey_idx = 0; rowkey_idx < rowkey_info.get_size(); rowkey_idx++)
      {
        if (OB_SUCCESS != (ret = rowkey_info.get_column_id(rowkey_idx, rowkey_column_id)))
        {
          TBSYS_LOG(WARN, "fail to get table %lu column %ld. ret=%d", table_id, rowkey_idx, ret);
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "BUG: Unexpected primary columns.");
          break;
        }
        else if (NULL == (rowkey_column_schema = schema_checker->get_column_schema(table_id, rowkey_column_id)))
        {
          ret = OB_ENTRY_NOT_EXIST;
          TBSYS_LOG(WARN, "fail to get table %lu column %lu", table_id, rowkey_column_id);
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "BUG: Primary key column schema not found");
          break;
        }
        else
        {
          ObString column_name;
          column_name.assign(
                const_cast<char *>(rowkey_column_schema->get_name()),
                static_cast<ObString::obstr_size_t>(strlen(rowkey_column_schema->get_name())));
          ret = stmt->add_column_item(*result_plan, column_name);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fail to add column item '%s' to table %lu", rowkey_column_schema->get_name(), table_id);
            break;
          }
        }
      }/* end for */
    }
  }
  return ret;
}
int resolve_independ_expr(ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    uint64_t& expr_id,
    int32_t expr_scope_type,
    ObColumnInfo col_info)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    ObRawExpr* expr = NULL;
    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    ObSqlRawExpr* sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr), result_plan->name_pool_);
    if (sql_expr == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc space for ObSqlRawExpr");
    }
    if (ret == OB_SUCCESS)
    {
      sql_expr = new(sql_expr) ObSqlRawExpr();
      ret = logical_plan->add_expr(sql_expr);
      if (ret != OB_SUCCESS)
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Add ObSqlRawExpr error");
    }
    if (ret == OB_SUCCESS)
    {
      expr_id = logical_plan->generate_expr_id();
      sql_expr->set_expr_id(expr_id);
      //mod dragon [varchar limit] 2016-8-10 10:56:58
      /*----new:增加一个参数col_info,用于判断表达式的长度---*/
      ret = resolve_expr(result_plan, stmt, node, sql_expr, expr, expr_scope_type, true, col_info);
      /*----old---
       ret = resolve_expr(result_plan, stmt, node, sql_expr, expr, expr_scope_type);
       ----old---*/
      //mod e
    }
    if (ret == OB_SUCCESS)
    {
      if (expr->get_expr_type() == T_REF_COLUMN)
      {
        //comment liumz, column with tid,cid assigned already
        ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(expr);
        sql_expr->set_table_id(col_expr->get_first_ref_id());
        sql_expr->set_column_id(col_expr->get_second_ref_id());
      }
      else
      {
        //comment liumz, column without tid,cid assigned; for example: c1+c2 return T_OP_ADD, generate tid,cid for it
        sql_expr->set_table_id(OB_INVALID_ID);
        sql_expr->set_column_id(logical_plan->generate_column_id());
      }
      sql_expr->set_expr(expr);
    }
  }
  return ret;
}

int resolve_and_exprs(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    ObVector<uint64_t>& and_exprs,
    int32_t expr_scope_type)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    if (node->type_ != T_OP_AND)
    {
      uint64_t expr_id = OB_INVALID_ID;
      ret = resolve_independ_expr(result_plan, stmt, node, expr_id, expr_scope_type);
      if (ret == OB_SUCCESS)
      {
        //add lijianqiang [sequence delete]20150515:b
        if (expr_scope_type == T_WHERE_LIMIT && stmt->get_stmt_type() == ObStmt::T_DELETE)
        {
          ObDeleteStmt* delete_stmt = dynamic_cast<ObDeleteStmt*>(stmt);
          if (delete_stmt->current_expr_has_sequence())
          {
            //            delete_stmt->set_has_sequence();
            delete_stmt->add_sequence_expr_id(expr_id);
            delete_stmt->set_current_expr_has_sequence(false);
          }
        }
        //add lijianqiang [sequence update] 20150521:b
        else if (expr_scope_type == T_WHERE_LIMIT && stmt->get_stmt_type() == ObStmt::T_UPDATE)
        {
          ObUpdateStmt* update_stmt = dynamic_cast<ObUpdateStmt*>(stmt);

          if (update_stmt->current_expr_has_sequence())
          {
            update_stmt->add_sequence_expr_id(expr_id);
            update_stmt->add_sequence_expr_names_and_types();
            update_stmt->set_current_expr_has_sequence(false);
            update_stmt->reset_expr_types_and_names();
          }
          else
          {
            update_stmt->add_sequence_expr_id(0);
          }
        }
        //add liuzy [sequence select] 20150615:b
        else if (expr_scope_type == T_WHERE_LIMIT && stmt->get_stmt_type() == ObStmt::T_SELECT)
        {
          ObSelectStmt* select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
          if (select_stmt->current_expr_has_sequence())
          {
            if(OB_SUCCESS != (ret = select_stmt->add_sequence_where_clause_expr(expr_id)))
            {
              TBSYS_LOG(ERROR, "add sequence expr_id failed");
            }
            else if(OB_SUCCESS != (ret = select_stmt->add_sequence_single_name_type_pairs()))
            {
              TBSYS_LOG(ERROR, "add sequence pair failed");
            }
            else
            {
              select_stmt->set_current_expr_has_sequence(false);
              select_stmt->reset_sequence_single_name_type_pair();
            }
          }
          else
          {
            select_stmt->add_sequence_where_clause_expr(0);
          }
        }
        //add 20150615:e
        //add 20150521:e
        //add 20150515:e
        ret = and_exprs.push_back(expr_id);
        if (ret != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Add 'AND' expression error");
      }
    }
    else
    {
      ret = resolve_and_exprs(result_plan, stmt, node->children_[0], and_exprs, expr_scope_type);
      if (ret == OB_SUCCESS)
        ret = resolve_and_exprs(result_plan, stmt, node->children_[1], and_exprs, expr_scope_type);
    }
  }
  return ret;
}

#define CREATE_RAW_EXPR(expr, type_name, result_plan)    \
  ({    \
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_); \
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);  \
  expr = (type_name*)parse_malloc(sizeof(type_name), name_pool);   \
  if (expr != NULL) \
{ \
  expr = new(expr) type_name();   \
  if (OB_SUCCESS != logical_plan->add_raw_expr(expr))    \
{ \
  expr = NULL;  /* no memory leak, bulk dealloc */ \
  } \
  } \
  if (expr == NULL)  \
{ \
  result_plan->err_stat_.err_code_ = OB_ERR_PARSER_MALLOC_FAILED; \
  TBSYS_LOG(WARN, "out of memory"); \
  snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,  \
  "Fail to malloc new raw expression"); \
  } \
  expr; \
  })

int resolve_expr(ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    ObSqlRawExpr *sql_expr,
    ObRawExpr*& expr,
    int32_t expr_scope_type,
    bool sub_query_results_scalar,
    ObColumnInfo col_info)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  expr = NULL;
  if (node == NULL)
    return ret;

  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObSQLSessionInfo* session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);

  switch (node->type_)
  {
    case T_BINARY:
    {
      ObString str;
      ObString str_val;
      str_val.assign_ptr(const_cast<char*>(node->str_value_), static_cast<int32_t>(node->value_));
      if (OB_SUCCESS != (ret = ob_write_string(*name_pool, str_val, str)))
      {
        TBSYS_LOG(WARN, "out of memory");
        break;
      }
      ObObj val;
      val.set_varchar(str);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(node->type_);
      c_expr->set_result_type(ObVarcharType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    //mod dragon [varchar limit] 2016-8-9 14:37:07
    //将STRING单独拿出来处理
    //new
    //varchar字段的node的type为string
    case T_STRING:
    {
      ObString str_val;
      // add by maosy for fix insert varchar space  20161024
      if(col_info.table_id_ != OB_INVALID_ID)
      {
          char* test = const_cast<char*>(node->str_value_);
          int len =  static_cast<int32_t>(node->value_);
          int i =0 ;
          for ( i = len -1 ; i >=0 ;i--)
          {
              if(test[i]!=' ')
                  break;
          }
          str_val.assign_ptr (test,i+1);
      }
      else
      {
          // add by maosy 20161024 e
          str_val.assign_ptr(const_cast<char*>(node->str_value_), static_cast<int32_t>(node->value_));
      } // add by maosy for fix insert varchar space  20161024
      ObString str;
      if (OB_SUCCESS != (ret = ob_write_string(*name_pool, str_val, str)))
      {
        TBSYS_LOG(WARN, "out of memory");
        break;
      }
      //设置val之前，先检查varchar长度是否满足schema中的要求
      ObInsertStmt *ins_stmt = static_cast<ObInsertStmt*>(stmt);
      if(OB_SUCCESS != (ret = ins_stmt->do_var_len_check(result_plan, col_info, (int64_t)str.length())))
      {
        TBSYS_LOG(WARN, "string is too long, exceed varchar limit");
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Varchar is too long");
        break;
      }
      ObObj val;
      val.set_varchar(str);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(node->type_);
      c_expr->set_result_type(ObVarcharType);
      c_expr->set_value(val);
      expr = c_expr;
      if (node->type_ == T_TEMP_VARIABLE)
      {
        TBSYS_LOG(INFO, "resolve tmp variable, name=%.*s", str.length(), str.ptr());
      }
      break;
    }
    case T_SYSTEM_VARIABLE:
    case T_TEMP_VARIABLE:
    {
      ObString str_val;
      str_val.assign_ptr(const_cast<char*>(node->str_value_), static_cast<int32_t>(node->value_));
      ObString str;
      if (OB_SUCCESS != (ret = ob_write_string(*name_pool, str_val, str)))
      {
        TBSYS_LOG(WARN, "out of memory");
        break;
      }
      ObObj val;
      val.set_varchar(str);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(node->type_);
      c_expr->set_result_type(ObVarcharType);
      c_expr->set_value(val);
      expr = c_expr;
      if (node->type_ == T_TEMP_VARIABLE)
      {
        TBSYS_LOG(INFO, "resolve tmp variable, name=%.*s", str.length(), str.ptr());
      }
      break;
    }
    /* old
    case T_STRING:
    case T_SYSTEM_VARIABLE:
    case T_TEMP_VARIABLE:
    {
      ObString str_val;
      str_val.assign_ptr(const_cast<char*>(node->str_value_), static_cast<int32_t>(node->value_));
      ObString str;
      if (OB_SUCCESS != (ret = ob_write_string(*name_pool, str_val, str)))
      {
        TBSYS_LOG(WARN, "out of memory");
        break;
      }
      ObObj val;
      val.set_varchar(str);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(node->type_);
      c_expr->set_result_type(ObVarcharType);
      c_expr->set_value(val);
      expr = c_expr;
      if (node->type_ == T_TEMP_VARIABLE)
      {
        TBSYS_LOG(INFO, "resolve tmp variable, name=%.*s", str.length(), str.ptr());
      }
      break;
    }
    */
    //mod 2016-8-9e
    case T_FLOAT:
    {
      ObObj val;
      val.set_float(static_cast<float>(atof(node->str_value_)));
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_FLOAT);
      c_expr->set_result_type(ObFloatType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_DOUBLE:
    {
      ObObj val;
      val.set_double(atof(node->str_value_));
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_DOUBLE);
      c_expr->set_result_type(ObDoubleType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_DECIMAL: // set as string
    {
      ObString str;
      if (OB_SUCCESS != (ret = ob_write_string(*name_pool, ObString::make_string(node->str_value_), str)))
      {
        TBSYS_LOG(WARN, "out of memory");
        break;
      }
      ObObj val;
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      //val.set_varchar(str);  old code
      val.set_decimal(str);
      //modify:e
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_DECIMAL);
      c_expr->set_result_type(ObDecimalType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_INT:
    {
      ObObj val;
      val.set_int(node->value_);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_INT);
      c_expr->set_result_type(ObIntType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_BOOL:
    {
      ObObj val;
      val.set_bool(node->value_ == 1 ? true : false);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_BOOL);
      c_expr->set_result_type(ObBoolType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_DATE:
    {
      ObObj val;
      val.set_precise_datetime(node->value_);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_DATE);
      c_expr->set_result_type(ObPreciseDateTimeType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
      //add peiouya [DATE_TIME] 20150912:b
    case T_DATE_NEW:
    {
      ObObj val;
      val.set_date (node->value_);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_DATE_NEW);
      c_expr->set_result_type(ObDateType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_TIME:
    {
      ObObj val;
      val.set_time(node->value_);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_TIME);
      c_expr->set_result_type(ObTimeType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
      //add 20150912:e
    case T_NULL:
    {
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_NULL);
      c_expr->set_result_type(ObNullType);
      expr = c_expr;
      break;
    }
    case T_QUESTIONMARK:
    {
      ObObj val;
      val.set_int(logical_plan->inc_question_mark());
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_QUESTIONMARK);
      c_expr->set_result_type(ObIntType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_CUR_TIME_UPS:
      logical_plan->set_cur_time_fun_ups(); // same as T_CUR_TIME, except run cur time on ups
    case T_CUR_TIME:
    {
      logical_plan->set_cur_time_fun(); // do nothing if called after set_cur_time_fun_ups()
      ObCurTimeExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObCurTimeExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_CUR_TIME);
      c_expr->set_result_type(ObPreciseDateTimeType);
      expr = c_expr;
      break;
    }
      //add liuzy [datetime func] 20150902:b
      /*Exp: only get time from current timestamp*/
    case T_CUR_TIME_HMS:
    {
      logical_plan->set_cur_time_hms_func();
      ObCurTimeExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObCurTimeExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_CUR_TIME_HMS);//mod liuzy [datetime func] 20150910
      c_expr->set_result_type(ObTimeType);
      expr = c_expr;
      break;
    }
      //add 20150901:e
      //add wuna [datetime func] 20150902:b
    case T_CUR_DATE:
    {
      logical_plan->set_cur_date_fun(); // do nothing if called after set_cur_time_fun_ups()
      ObCurTimeExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObCurTimeExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_CUR_DATE);
      c_expr->set_result_type(ObDateType);//to change
      expr = c_expr;
      break;
    }
      //add 20150902:e
    case T_OP_NAME_FIELD:
    {
      //OB_ASSERT(node->children_[0]->type_ == T_IDENT);//del liumz
      //add liumz, [multi_database.sql]:20150615
      OB_ASSERT(node->children_[0]->type_ == T_RELATION);
      ParseNode *table_node = node->children_[0];
      if (table_node->children_[1]->type_ != T_IDENT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "table is illeagal");
        break;
      }
      //add:e
      // star has been expand before
      // T_IDENT.* can't has alias name here, which is illeagal
      if (node->children_[1]->type_ != T_IDENT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "%s.* is illeagal", node->children_[0]->str_value_);
        break;
      }

      const char* table_str = table_node->children_[1]->str_value_;//add liumz
      //const char* table_str = node->children_[0]->str_value_;//del liumz
      const char* column_str = node->children_[1]->str_value_;
      if (expr_scope_type == T_INSERT_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Illegal usage %s.%s", table_str, column_str);
        break;
      }
      else if (expr_scope_type == T_WHEN_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Column name %s.%s cannot be used in WHEN clause", table_str, column_str);
        break;
      }

      //add liumz, [multi_database.sql]:20150615
      ObString db_name;
      if (NULL == table_node->children_[0])
      {
        if (session_info == NULL)
        {
          ret = OB_ERR_SESSION_UNSET;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Session not set");
          break;
        }
        else
        {
          db_name = session_info->get_db_name();
        }
      }
      else
      {
        OB_ASSERT(table_node->children_[0]->type_ == T_IDENT);
        db_name.assign_ptr(
              (char*)(table_node->children_[0]->str_value_),
              static_cast<int32_t>(strlen(table_node->children_[0]->str_value_)));
      }
      //add:e

      ObString table_name;
      ObString column_name;
      table_name.assign_ptr((char*)table_str, static_cast<int32_t>(strlen(table_str)));
      column_name.assign_ptr((char*)column_str, static_cast<int32_t>(strlen(column_str)));

      //add liumz, [multi_database.sql]:20150615
      ObString db_table_name;
      int64_t buf_len = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1;
      char name_buf[buf_len];
      db_table_name.assign_buffer(name_buf, static_cast<ObString::obstr_size_t>(buf_len));
      if(OB_SUCCESS != (ret = write_db_table_name(db_name, table_name, db_table_name)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Table name too long");
        break;
      }
      // modify zhangcd [muti_database.bugfix] 20150731:b
      //if (stmt->get_stmt_type() == ObStmt::T_SELECT)
      //{
      //ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
      //if (OB_INVALID_ID == select_stmt->get_ref_table_name(db_table_name, table_name))
      if (OB_INVALID_ID == stmt->get_ref_table_name(db_table_name, table_name))
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown table %s", table_str);
        break;
      }
      //}
      // mod:e
      //add:e

      // Column name with table name, it can't be alias name, so we don't need to check select item list
      if (expr_scope_type == T_HAVING_LIMIT)
      {
        OB_ASSERT(stmt->get_stmt_type() == ObStmt::T_SELECT);
        ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
        TableItem* table_item;
        if ((select_stmt->get_table_item(table_name, &table_item)) == OB_INVALID_ID)
        {
          ret = OB_ERR_TABLE_UNKNOWN;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown table %s in having clause", table_str);
          break;
        }
        ret = select_stmt->check_having_ident(*result_plan, column_name, table_item, expr);
        // table_set is of no use in having clause, because all tables have been joined to one table
        // when having condition is calculated
        //sql_expr->get_tables_set().add_member(select_stmt->get_table_bit_index(table_item->table_id_));
      }
      else
      {
        ColumnItem *column_item = stmt->get_column_item(&table_name, column_name);
        if (!column_item)
        {
          ret = stmt->add_column_item(*result_plan, column_name, &table_name, &column_item);
          if (ret != OB_SUCCESS)
          {
            break;
          }
        }
        ObBinaryRefRawExpr *b_expr = NULL;
        if (CREATE_RAW_EXPR(b_expr, ObBinaryRefRawExpr, result_plan) == NULL)
          break;
        b_expr->set_expr_type(T_REF_COLUMN);
        b_expr->set_result_type(column_item->data_type_);
        b_expr->set_first_ref_id(column_item->table_id_);
        b_expr->set_second_ref_id(column_item->column_id_);
        expr = b_expr;
        sql_expr->get_tables_set().add_member(stmt->get_table_bit_index(column_item->table_id_));
        if (stmt->get_table_bit_index(column_item->table_id_)< 0)
        {
          TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,column_item->table_id_);
        }
      }
      break;
    }
    case T_IDENT:
    {
      if (expr_scope_type == T_INSERT_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown value %s", node->str_value_);
        break;
      }
      else if (expr_scope_type == T_VARIABLE_VALUE_LIMIT)
      {
        /* TBD */
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown value %s", node->str_value_);
        break;
      }
      else if (expr_scope_type == T_WHEN_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Column name %s cannot be used in WHEN clause", node->str_value_);
        break;
      }

      ObString column_name;
      column_name.assign_ptr(
            (char*)(node->str_value_),
            static_cast<int32_t>(strlen(node->str_value_))
            );

      if (expr_scope_type == T_HAVING_LIMIT)
      {
        OB_ASSERT(stmt->get_stmt_type() == ObStmt::T_SELECT);
        ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
        ret = select_stmt->check_having_ident(*result_plan, column_name, NULL, expr);
        // table_set is of no use in having clause, because all tables have been joined to one table
        // when having condition is calculated
        // sql_expr->get_tables_set().add_member(select_stmt->get_table_bit_index(table_item->table_id_));
      }
      else
      {
        // the checking rule is follow mysql, although not reasonable
        // 1. select user_id user_id, item_id user_id from order_list where user_id>0;
        //     syntax correct, you can try
        // 2. select item_id as user_id, user_id from order_list  where user_id>0;
        //     real order_list.user_id is used, so real column first.
        // 3. select item_id as user_id from order_list  where user_id>0;
        //     real order_list.user_id is used, so real column first.
        if (expr == NULL)
        {
          ColumnItem *column_item = stmt->get_column_item(NULL, column_name);
          if (column_item)
          {
            ObBinaryRefRawExpr *b_expr = NULL;
            if (CREATE_RAW_EXPR(b_expr, ObBinaryRefRawExpr, result_plan) == NULL)
              break;
            b_expr->set_expr_type(T_REF_COLUMN);
            b_expr->set_result_type(column_item->data_type_);
            b_expr->set_first_ref_id(column_item->table_id_);
            b_expr->set_second_ref_id(column_item->column_id_);
            expr = b_expr;
            sql_expr->get_tables_set().add_member(stmt->get_table_bit_index(column_item->table_id_));
            if (stmt->get_table_bit_index(column_item->table_id_)< 0)
            {
              TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,column_item->table_id_);
            }
          }
        }
        if (expr == NULL)
        {
          ColumnItem *column_item = NULL;
          ret = stmt->add_column_item(*result_plan, column_name, NULL, &column_item);
          if (ret == OB_SUCCESS)
          {
            ObBinaryRefRawExpr *b_expr = NULL;
            if (CREATE_RAW_EXPR(b_expr, ObBinaryRefRawExpr, result_plan) == NULL)
              break;
            b_expr->set_expr_type(T_REF_COLUMN);
            b_expr->set_result_type(column_item->data_type_);
            b_expr->set_first_ref_id(column_item->table_id_);
            b_expr->set_second_ref_id(column_item->column_id_);
            expr = b_expr;
            sql_expr->get_tables_set().add_member(stmt->get_table_bit_index(column_item->table_id_));
            if (stmt->get_table_bit_index(column_item->table_id_)< 0)
            {
              TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,column_item->table_id_);
            }
          }
          else if (ret == OB_ERR_COLUMN_UNKNOWN)
          {
            ret = OB_SUCCESS;
          }
          else
          {
            break;
          }
        }
        if (!expr && stmt->get_stmt_type() == ObStmt::T_SELECT)
        {
          ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
          uint64_t expr_id = select_stmt->get_alias_expr_id(column_name);
          if (expr_id != OB_INVALID_ID)
          {
            ObSqlRawExpr* alias_expr = logical_plan->get_expr(expr_id);
            if (alias_expr == NULL)
            {
              ret = OB_ERR_ILLEGAL_ID;
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Wrong expr_id %lu", expr_id);
              break;
            }
            if (alias_expr->is_contain_aggr()
                && (expr_scope_type == T_INSERT_LIMIT
                    || expr_scope_type == T_UPDATE_LIMIT
                    || expr_scope_type == T_AGG_LIMIT
                    || expr_scope_type == T_WHERE_LIMIT
                    || expr_scope_type == T_GROUP_LIMIT))
            {
              ret = OB_ERR_PARSER_SYNTAX;
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Invalid use of alias which contains group function");
              break;
            }
            else
            {
              ObBinaryRefRawExpr *b_expr = NULL;
              if (CREATE_RAW_EXPR(b_expr, ObBinaryRefRawExpr, result_plan) == NULL)
                break;
              b_expr->set_expr_type(T_REF_COLUMN);
              b_expr->set_result_type(alias_expr->get_result_type());
              b_expr->set_first_ref_id(alias_expr->get_table_id());
              b_expr->set_second_ref_id(alias_expr->get_column_id());
              expr = b_expr;
              sql_expr->get_tables_set().add_members(alias_expr->get_tables_set());
              sql_expr->set_contain_alias(true);
            }
          }
        }
        if (expr == NULL)
        {
          ret = OB_ERR_COLUMN_UNKNOWN;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unkown column name %.*s", column_name.length(), column_name.ptr());
        }
      }
      break;
    }
    case T_OP_EXISTS:
      if (expr_scope_type == T_INSERT_LIMIT || expr_scope_type == T_UPDATE)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "EXISTS expression can not appear in INSERT/UPDATE statement");
        break;
      }
    case T_OP_POS:
    case T_OP_NEG:
    case T_OP_NOT:
    {
      ObRawExpr* sub_expr = NULL;
      ret = resolve_expr(result_plan, stmt, node->children_[0], sql_expr, sub_expr, expr_scope_type, true);
      if (ret != OB_SUCCESS)
        break;
      if (node->type_ == T_OP_POS)
      {
        expr = sub_expr;
      }
      // T_OP_EXISTS can not has child of type T_OP_EXISTS/T_OP_POS/T_OP_NEG/T_OP_NOT,
      // so we can do this way
      else if (node->type_ == sub_expr->get_expr_type())
      {
        expr = (dynamic_cast<ObUnaryOpRawExpr*>(sub_expr))->get_op_expr();
      }
      // only INT/FLOAT/DOUBLE are in consideration
      else if (node->type_ == T_OP_NEG && sub_expr->is_const()
               && (sub_expr->get_expr_type() == T_INT
                   || sub_expr->get_expr_type() == T_FLOAT
                   || sub_expr->get_expr_type() == T_DOUBLE
                   //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
                   /*
                          *对decimal值为负的情况进行处理
                          */
                   || sub_expr->get_expr_type() == T_DECIMAL
                   //add:e
                   ))
      {
        ObConstRawExpr *const_expr = dynamic_cast<ObConstRawExpr*>(sub_expr);
        if (const_expr == NULL)
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Wrong internal status of const expression");
          break;
        }
        switch (sub_expr->get_expr_type())
        {
          case T_INT:
          {
            int64_t val = OB_INVALID_ID;
            if ((ret = const_expr->get_value().get_int(val)) == OB_SUCCESS)
            {
              ObObj new_val;
              new_val.set_int(-val);
              const_expr->set_value(new_val);
            }
            break;
          }
          case T_FLOAT:
          {
            float val = static_cast<float>(OB_INVALID_ID);
            if ((ret = const_expr->get_value().get_float(val)) == OB_SUCCESS)
            {
              ObObj new_val;
              new_val.set_float(-val);
              const_expr->set_value(new_val);
            }
            break;
          }
          case T_DOUBLE:
          {
            double val = static_cast<double>(OB_INVALID_ID);
            if ((ret = const_expr->get_value().get_double(val)) == OB_SUCCESS)
            {
              ObObj new_val;
              new_val.set_double(-val);
              const_expr->set_value(new_val);
            }
            break;
          }
            //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
            /*
               *由于在这里decimal的值是按照varcahr存的，所以这里的处理就是在varcahr的最前面加上一个负号
               */
          case T_DECIMAL:
          {
            ObString ori_str;
            ObString res_str;
            char neg_char[1] = { '-' };
            char end_char[1] = { '\0' };
            if ((ret = const_expr->get_value().get_decimal(ori_str))== OB_SUCCESS)
            {
              char buffer[ori_str.length() + 2];
              ObObj new_val;
              memcpy(buffer, neg_char, 1);
              memcpy(buffer + 1, ori_str.ptr(), ori_str.length());
              memcpy(buffer + 1 + ori_str.length(), end_char, 1);
              const char * middle_val = buffer;
              if (OB_SUCCESS
                  != (ret = ob_write_string(*name_pool,
                                            ObString::make_string(middle_val), res_str))) {
                TBSYS_LOG(WARN, "out of memory");
                break;
              }
              new_val.set_decimal(res_str);
              const_expr->set_value(new_val);
            }
            break;
          }
            //add:e
          default:
          {
            /* won't be here */
            ret = OB_ERR_PARSER_SYNTAX;
          }
        }
        if (ret == OB_SUCCESS)
        {
          expr = sub_expr;
        }
        else
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Wrong internal status of const expression");
          break;
        }
      }
      else
      {
        ObUnaryOpRawExpr *u_expr = NULL;
        if (CREATE_RAW_EXPR(u_expr, ObUnaryOpRawExpr, result_plan) == NULL)
          break;
        u_expr->set_expr_type(node->type_);
        if (node->type_ == T_OP_POS)
        {
          u_expr->set_result_type(sub_expr->get_result_type());
        }
        else if (node->type_ == T_OP_NEG)
        {
          ObObj in_type;
          in_type.set_type(sub_expr->get_result_type());
          u_expr->set_result_type(ObExprObj::type_negate(in_type).get_type());
        }
        else if (node->type_ == T_OP_EXISTS || node->type_ == T_OP_NOT)
        {
          u_expr->set_result_type(ObBoolType);
        }
        else
        {
          /* won't be here */
          u_expr->set_result_type(ObMinType);
        }
        u_expr->set_op_expr(sub_expr);
        expr = u_expr;
      }
      break;
    }
    case T_OP_ADD:
    case T_OP_MINUS:
    case T_OP_MUL:
    case T_OP_DIV:
    case T_OP_REM:
    case T_OP_POW:
    case T_OP_MOD:
    case T_OP_LE:
    case T_OP_LT:
    case T_OP_EQ:
    case T_OP_GE:
    case T_OP_GT:
    case T_OP_NE:
    case T_OP_LIKE:
    case T_OP_NOT_LIKE:
    case T_OP_AND:
    case T_OP_OR:
    case T_OP_IS:
    case T_OP_IS_NOT:
    case T_OP_CNN:
    {
      ObRawExpr* sub_expr1 = NULL;
      ret = resolve_expr(result_plan, stmt, node->children_[0], sql_expr, sub_expr1, expr_scope_type, true);
      if (ret != OB_SUCCESS)
        break;
      ObRawExpr* sub_expr2 = NULL;
      ret = resolve_expr(result_plan, stmt, node->children_[1], sql_expr, sub_expr2, expr_scope_type, true);
      if (ret != OB_SUCCESS)
        break;

      //add duyr [join_without_pushdown_is_null] 20151214:b
      if (T_OP_IS == node->type_
          && (T_NULL == sub_expr2->get_expr_type()
              || T_QUESTIONMARK == sub_expr2->get_expr_type()))
      {//FIXME:select * from t1 left join t2 on t1.c1=t2.c1 where t2.c2 is null(or is ?);
        //we don't push down is null or is ? for make sure join get the correct result
        sql_expr->set_push_down_with_outerjoin(false);
      }
      //add duyr 20151214:e

      ObBinaryOpRawExpr *b_expr = NULL;
      if (CREATE_RAW_EXPR(b_expr, ObBinaryOpRawExpr, result_plan) == NULL)
        break;
      b_expr->set_expr_type(node->type_);
      ObObj in_type1;
      in_type1.set_type(sub_expr1->get_result_type());
      ObObj in_type2;
      in_type2.set_type(sub_expr2->get_result_type());
      if (node->type_ == T_OP_ADD)
      {
        //mod peiouya [DATE_TIME] 20150906:b
        ////modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
        ////b_expr->set_result_type(ObExprObj::type_add(in_type1, in_type2).get_type()); old code
        //b_expr->set_result_type(ObExprObj::type_add_v2(in_type1, in_type2).get_type());
        ObObjType obj_type = ObExprObj::type_add_v2(in_type1, in_type2).get_type();
        if (ObMinType < obj_type && ObMaxType > obj_type)
        {
          //add peiouya [DATE_TIME] 20150909:b
          if ( ObIntervalType == obj_type)
          {
            obj_type = (in_type1.get_type() == ObIntervalType ? in_type2.get_type() : in_type1.get_type());
          }
          //add 20150909:e
          b_expr->set_result_type(obj_type);
        }
        else
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Illegal type");
          ret = OB_ERR_ILLEGAL_TYPE;
        }
        //mod 20150906:e
      }
      else if (node->type_ == T_OP_MINUS)
      {
        //mod peiouya [DATE_TIME] 20150906:b
        ////b_expr->set_result_type(ObExprObj::type_sub(in_type1, in_type2).get_type());
        //b_expr->set_result_type(ObExprObj::type_sub_v2(in_type1, in_type2).get_type());
        ObObjType obj_type = ObExprObj::type_sub_v2(in_type1, in_type2).get_type();
        if (ObMinType < obj_type && ObMaxType > obj_type)
        {
          //add peiouya [DATE_TIME] 20150909:b
          if ( ObIntervalType == obj_type)
          {
            obj_type = (in_type1.get_type() == ObIntervalType ? in_type2.get_type() : in_type1.get_type());
          }
          //add 20150909:e
          b_expr->set_result_type(obj_type);
        }
        else
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Illegal type");
          ret = OB_ERR_ILLEGAL_TYPE;
        }
        //mod 20150906:e
      }
      else if (node->type_ == T_OP_MUL)
      {
        //mod peiouya [DATE_TIME] 20150906:b
        //// b_expr->set_result_type(ObExprObj::type_mul(in_type1, in_type2).get_type());
        //b_expr->set_result_type(ObExprObj::type_mul_v2(in_type1, in_type2).get_type());
        ObObjType obj_type = ObExprObj::type_mul_v2(in_type1, in_type2).get_type();
        if (ObMinType < obj_type && ObMaxType > obj_type)
        {
          //add peiouya [DATE_TIME] 20150909:b
          if ( ObIntervalType == obj_type)
          {
            obj_type = (in_type1.get_type() == ObIntervalType ? in_type2.get_type() : in_type1.get_type());
          }
          //add 20150909:e
          b_expr->set_result_type(obj_type);
        }
        else
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Illegal type");
          ret = OB_ERR_ILLEGAL_TYPE;
        }
        //mod 20150906:e
      }
      else if (node->type_ == T_OP_DIV)
      {
        //mod peiouya [DATE_TIME] 20150906:b
        /*
        if (in_type1.get_type() == ObDoubleType || in_type2.get_type() == ObDoubleType)
          //b_expr->set_result_type(ObExprObj::type_div(in_type1, in_type2, true).get_type());
          b_expr->set_result_type(ObExprObj::type_div_v2(in_type1, in_type2, true).get_type());
        else
          //b_expr->set_result_type(ObExprObj::type_div(in_type1, in_type2, false).get_type());
          b_expr->set_result_type(ObExprObj::type_div_v2(in_type1, in_type2, false).get_type());
        */
        //simplify this branch, because type_div_v2's third parameter is unused parameter.
        ObObjType obj_type = ObExprObj::type_div_v2(in_type1, in_type2, false).get_type();
        if (ObMinType < obj_type && ObMaxType > obj_type)
        {
          //add peiouya [DATE_TIME] 20150909:b
          if ( ObIntervalType == obj_type)
          {
            obj_type = (in_type1.get_type() == ObIntervalType ? in_type2.get_type() : in_type1.get_type());
          }
          //add 20150909:e
          b_expr->set_result_type(obj_type);
        }
        else
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Illegal type");
          ret = OB_ERR_ILLEGAL_TYPE;
        }
        //mod 20150906:e
      }
      //modify e
      else if (node->type_ == T_OP_REM || node->type_ == T_OP_MOD)
      {
        b_expr->set_result_type(ObExprObj::type_mod(in_type1, in_type2).get_type());
      }
      else if (node->type_ == T_OP_POW)
      {
        b_expr->set_result_type(sub_expr1->get_result_type());
      }
      else if (node->type_ == T_OP_LE || node->type_ == T_OP_LT || node->type_ == T_OP_EQ
               || node->type_ == T_OP_GE || node->type_ == T_OP_GT || node->type_ == T_OP_NE
               || node->type_ == T_OP_LIKE || node->type_ == T_OP_NOT_LIKE || node->type_ == T_OP_AND
               || node->type_ == T_OP_OR || node->type_ == T_OP_IS || node->type_ == T_OP_IS_NOT)
      {
        b_expr->set_result_type(ObBoolType);
      }
      else if (node->type_ == T_OP_CNN)
      {
        b_expr->set_result_type(ObVarcharType);
      }
      else
      {
        /* won't be here */
        b_expr->set_result_type(ObMinType);
      }
      b_expr->set_op_exprs(sub_expr1, sub_expr2);

      //delete by xionghui [subquery_final] 20160216
      /*
      //add zhujun [fix equal-subquery bug] 20151013:b
      // mod by xionghui 20151105
      if((node->type_ == T_OP_EQ ||node->type_ == T_OP_GE||node->type_ == T_OP_GT||
              node->type_ == T_OP_LE ||node->type_ == T_OP_LT||node->type_ == T_OP_NE)
              && sub_expr2->get_expr_type()==T_REF_QUERY)
      {
          //mod e:20151105
        if(stmt->get_stmt_type()==ObBasicStmt::T_SELECT)
        {
          ObSelectStmt *select = dynamic_cast<ObSelectStmt *>(stmt);
          select->set_is_equal_subquery(true);
          //b_expr->set_expr_type(T_OP_IN);
          TBSYS_LOG(INFO, "is equal sub-query");
        }
      }
      //add zhujun 20151013:e
      //add xionghui [fix like-subquery bug]20151015:b
      if((node->type_ == T_OP_LIKE ||node->type_ == T_OP_NOT_LIKE )&& sub_expr2->get_expr_type() == T_REF_QUERY)
      {
        if(stmt->get_stmt_type()==ObBasicStmt::T_SELECT)
        {
          ObSelectStmt *select = dynamic_cast<ObSelectStmt *>(stmt);
          select->set_is_like_subquery(true);
          //b_expr->set_expr_type(T_OP_IN);
          TBSYS_LOG(INFO, "is like-subquery");
        }
      }
      //add e:
      */
      //delete e:
      expr = b_expr;
      break;
    }
    case T_OP_BTW:
      /* pass through */
    case T_OP_NOT_BTW:
    {
      ObRawExpr* sub_expr1 = NULL;
      ObRawExpr* sub_expr2 = NULL;
      ObRawExpr* sub_expr3 = NULL;
      ret = resolve_expr(result_plan, stmt, node->children_[0], sql_expr, sub_expr1, expr_scope_type);
      if (ret != OB_SUCCESS)
        break;
      ret = resolve_expr(result_plan, stmt, node->children_[1], sql_expr, sub_expr2, expr_scope_type);
      if (ret != OB_SUCCESS)
        break;
      ret = resolve_expr(result_plan, stmt, node->children_[2], sql_expr, sub_expr3, expr_scope_type);
      if (ret != OB_SUCCESS)
        break;

      ObTripleOpRawExpr *t_expr = NULL;
      if (CREATE_RAW_EXPR(t_expr, ObTripleOpRawExpr, result_plan) == NULL)
        break;
      t_expr->set_expr_type(node->type_);
      t_expr->set_result_type(ObBoolType);
      t_expr->set_op_exprs(sub_expr1, sub_expr2, sub_expr3);
      expr = t_expr;
      break;
    }
    case T_OP_IN:
      // get through
    case T_OP_NOT_IN:
    {
      ObRawExpr* sub_expr1 = NULL;
      if (node->children_[0]->type_ == T_SELECT)
        ret = resolve_expr(
              result_plan,
              stmt,
              node->children_[0],
              sql_expr, sub_expr1,
              expr_scope_type,
              false
              );
      else
        ret = resolve_expr(
              result_plan,
              stmt,
              node->children_[0],
              sql_expr,
              sub_expr1,
              expr_scope_type,
              true);
      if (ret != OB_SUCCESS)
        break;
      ObRawExpr* sub_expr2 = NULL;
      ret = resolve_expr(
            result_plan,
            stmt,
            node->children_[1],
            sql_expr,
            sub_expr2,
            expr_scope_type,
            false
            );
      if (ret != OB_SUCCESS)
        break;
      ObBinaryOpRawExpr *in_expr = NULL;
      if (CREATE_RAW_EXPR(in_expr, ObBinaryOpRawExpr, result_plan) == NULL)
        break;
      in_expr->set_expr_type(node->type_ == T_OP_IN ? T_OP_IN : T_OP_NOT_IN);
      in_expr->set_result_type(ObBoolType);
      in_expr->set_op_exprs(sub_expr1, sub_expr2);

      /* 1. get the the column num of left operand */
      int32_t num_left_param = 1;
      switch (in_expr->get_first_op_expr()->get_expr_type())
      {
        case T_OP_ROW :
        {
          ObMultiOpRawExpr *left_expr = dynamic_cast<ObMultiOpRawExpr *>(in_expr->get_first_op_expr());
          num_left_param = left_expr->get_expr_size();
          break;
        }
        case T_REF_QUERY :
        {
          ObUnaryRefRawExpr *left_expr = dynamic_cast<ObUnaryRefRawExpr *>(in_expr->get_first_op_expr());
          ObSelectStmt *sub_select = dynamic_cast<ObSelectStmt *>(logical_plan->get_query(left_expr->get_ref_id()));
          if (!sub_select)
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Sub-query of In operator is not select statment");
            break;
          }
          num_left_param = sub_select->get_select_item_size();
          break;
        }
        default:
          num_left_param = 1;
          break;
      }

      /* 2. get the the column num of right operand(s) */
      int32_t num_right_param = 0;
      switch (in_expr->get_second_op_expr()->get_expr_type())
      {
        case T_OP_ROW:
        {
          ObMultiOpRawExpr *row_expr = dynamic_cast<ObMultiOpRawExpr *>(in_expr->get_second_op_expr());
          int32_t num = row_expr->get_expr_size();
          ObRawExpr *sub_expr = NULL;
          for (int32_t i = 0; i < num; i++)
          {
            sub_expr = row_expr->get_op_expr(i);
            switch (sub_expr->get_expr_type())
            {
              case T_OP_ROW:
              {
                num_right_param = (dynamic_cast<ObMultiOpRawExpr *>(sub_expr))->get_expr_size();
                break;
              }
              case T_REF_QUERY:
              {
                uint64_t query_id = (dynamic_cast<ObUnaryRefRawExpr *>(sub_expr))->get_ref_id();
                ObSelectStmt *sub_query = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(query_id));
                if (sub_query)
                  num_right_param = sub_query->get_select_item_size();
                else
                  num_right_param = 0;
                break;
              }
              default:
                num_right_param = 1;
                break;
            }
            if (num_left_param != num_right_param)
            {
              break;
            }
          }
          break;
        }
        case T_REF_QUERY:
        {
          uint64_t query_id = (dynamic_cast<ObUnaryRefRawExpr *>(in_expr->get_second_op_expr()))->get_ref_id();
          ObSelectStmt *sub_query = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(query_id));
          if (sub_query)
            num_right_param = sub_query->get_select_item_size();
          else
            num_right_param = 0;
          break;
        }
        default:
          /* won't be here */
          OB_ASSERT(0);
          break;
      }

      /* 3. to check if the nums of two sides are equal */
      if (num_left_param != num_right_param)
      {
        ret = OB_ERR_COLUMN_SIZE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "In operands contain different column(s)");
        break;
      }

      expr = in_expr;
      break;
    }
    case T_CASE:
    {
      ObCaseOpRawExpr *case_expr = NULL;
      //mod dolphin[case when return type]@20151231:b
      //ObObjType tmp_type = ObMinType;
      ObObjType tmp_type = ObNullType;
    //add qianzm [case_type_compatible bug_1139] 20160421:b
      ObObjType tmp_then_type = ObNullType;
      ObObjType tmp_else_type = ObNullType;
      ObObjType tmp_res_type = ObNullType;
      ObExprObj tmp_expr_for_type_promotion;
    //add 20160421:e
      if (CREATE_RAW_EXPR(case_expr, ObCaseOpRawExpr, result_plan) == NULL)
        break;
      if (node->children_[0])
      {
        ObRawExpr *arg_expr = NULL;
        ret = resolve_expr(result_plan, stmt, node->children_[0], sql_expr, arg_expr, expr_scope_type);
        if(ret != OB_SUCCESS)
        {
          break;
        }
        case_expr->set_arg_op_expr(arg_expr);
        case_expr->set_expr_type(T_OP_ARG_CASE);
      }
      else
      {
        case_expr->set_expr_type(T_OP_CASE);
      }

      OB_ASSERT(node->children_[1]->type_ == T_WHEN_LIST);
      //add dolphin
      if(node->num_child_ > 2)
      {
          ObRawExpr *else_expr = NULL;
          ret = resolve_expr(result_plan, stmt, node->children_[2], sql_expr, else_expr, expr_scope_type);
          if(ret != OB_SUCCESS)
          {
              break;
          }
        if(else_expr)
        {
            if(OB_SUCCESS != (ret = ObExprObj::coalesce_type_compatible(tmp_type,else_expr->get_result_type())))
            {
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "case when function parameters type invalid");
                break;
            }
            //add qianzm [case_type_compatible bug_1139] 20160421:b
            tmp_else_type = else_expr->get_result_type();
            //add 20160421:e
        }

      }
      //add:e
      ParseNode *when_node;
      ObRawExpr   *when_expr = NULL;
      ObRawExpr   *then_expr = NULL;
      for (int32_t i = 0; ret == OB_SUCCESS && i < node->children_[1]->num_child_; i++)
      {
        when_node = node->children_[1]->children_[i];
        ret = resolve_expr(result_plan, stmt, when_node->children_[0], sql_expr, when_expr, expr_scope_type);
        if(ret != OB_SUCCESS)
        {
          break;
        }
        ret = resolve_expr(result_plan, stmt, when_node->children_[1], sql_expr, then_expr, expr_scope_type);
        if(ret != OB_SUCCESS)
        {
          break;
        }
        ret = case_expr->add_when_op_expr(when_expr);
        if (ret != OB_SUCCESS)
        {
          break;
        }
        ret = case_expr->add_then_op_expr(then_expr);
        if (ret != OB_SUCCESS)
        {
          break;
        }
        const ObObjType then_type = then_expr->get_result_type();
        //add dolphin[case when return type]@20151231:b
        if(OB_SUCCESS != (ret = ObExprObj::coalesce_type_compatible(tmp_type,then_type)))
        {
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "case when function parameters type invalid");
            break;
        }
        //TBSYS_LOG(ERROR,"then_type is %d,when type is %d,tmp_type is %d",then_type,when_expr->get_result_type(),tmp_type);
        //del dolphin[case when return type]@20151231:b
        /*if (then_type == ObNullType)
        {
          continue;
        }
        else if (then_type > ObMinType && then_type < ObMaxType
                 && (then_type == tmp_type || tmp_type == ObMinType))
        {
          tmp_type = then_type;
        }
        else
        {
          ret = OB_ERR_ILLEGAL_TYPE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Return types of then clause are not compatible");
          break;
        }*/
        //del:e
      }
      if (ret != OB_SUCCESS)
      {
        break;
      }
      // @bug FIXME
      //add qianzm [case_type_compatible bug_1139] 20160421:b
      tmp_then_type = then_expr->get_result_type();
      if (tmp_else_type != ObNullType && tmp_then_type != ObNullType)
      {
          ret = tmp_expr_for_type_promotion.do_type_promotion(tmp_then_type, tmp_else_type, tmp_res_type);
          if (tmp_res_type == ObMaxType)
          {
              ret = OB_CONSISTENT_MATRIX_DATATYPE_INVALID;
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "case when function parameters type invalid");
              break;
          }
          else
              case_expr->set_result_type(tmp_res_type);
      }
      else
      //add 20160421:e
      {
          if (tmp_type == ObMinType) tmp_type = ObVarcharType;
          case_expr->set_result_type(tmp_type);
      }
      //TBSYS_LOG(ERROR,"case result_type is %d",tmp_type);
      if (case_expr->get_when_expr_size() != case_expr->get_then_expr_size())
      {
        ret = OB_ERR_COLUMN_SIZE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Error size of when expressions");
        break;
      }
      if (node->children_[2])
      {
        ObRawExpr *default_expr = NULL;
        ret = resolve_expr(result_plan, stmt, node->children_[2], sql_expr, default_expr, expr_scope_type);
        if (ret != OB_SUCCESS)
        {
          break;
        }
        case_expr->set_default_op_expr(default_expr);
      }
      expr = case_expr;
      break;
    }
    case T_EXPR_LIST:
    {
      ObMultiOpRawExpr *multi_expr = NULL;
      if (CREATE_RAW_EXPR(multi_expr, ObMultiOpRawExpr, result_plan) == NULL)
        break;
      multi_expr->set_expr_type(T_OP_ROW);
      // not mathematic expression, result type is of no use.
      // should be ObRowType
      multi_expr->set_result_type(ObMinType);

      ObRawExpr *sub_query = NULL;
      uint64_t num = node->num_child_;
      for (uint64_t i = 0; ret == OB_SUCCESS && i < num; i++)
      {
        if (node->children_[i]->type_ == T_SELECT && !sub_query_results_scalar)
          ret = resolve_expr(
                result_plan,
                stmt,
                node->children_[i],
                sql_expr,
                sub_query,
                expr_scope_type,
                false);
        else
          ret = resolve_expr(
                result_plan,
                stmt,
                node->children_[i],
                sql_expr,
                sub_query,
                expr_scope_type,
                true);
        if (ret != OB_SUCCESS)
        {
          break;
        }
        ret = multi_expr->add_op_expr(sub_query);
      }
      if (ret == OB_SUCCESS)
        expr = multi_expr;
      break;
    }
    case T_SELECT:
    {
      if (expr_scope_type == T_INSERT_LIMIT
          || expr_scope_type == T_UPDATE_LIMIT
          || expr_scope_type == T_AGG_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Sub-query is illeagal in INSERT/UPDATE statement or AGGREGATION function");
        break;
      }

      uint64_t query_id = OB_INVALID_ID;
      if ((ret = resolve_select_stmt(result_plan, node, query_id)) != OB_SUCCESS)
        break;
      if (sub_query_results_scalar)
      {
        ObBasicStmt *sub_stmt = logical_plan->get_query(query_id);
        ObSelectStmt *sub_select = dynamic_cast<ObSelectStmt*>(sub_stmt);
        if (sub_select->get_select_item_size() != 1)
        {
          ret = OB_ERR_COLUMN_SIZE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Operand should contain 1 column(s)");
          break;
        }
      }
      ObUnaryRefRawExpr *sub_query_expr = NULL;
      if (CREATE_RAW_EXPR(sub_query_expr, ObUnaryRefRawExpr, result_plan) == NULL)
        break;
      sub_query_expr->set_expr_type(T_REF_QUERY);
      // not mathematic expression, result type is of no use.
      // should be ObRowType
      sub_query_expr->set_result_type(ObMinType);
      sub_query_expr->set_ref_id(query_id);
      expr = sub_query_expr;
      break;
    }
    case T_INSERT:
    case T_DELETE:
    case T_UPDATE:
    {
      uint64_t query_id = OB_INVALID_ID;
      ObUnaryRefRawExpr *sub_query_expr = NULL;
      if (expr_scope_type != T_WHEN_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "INSERT/DELTE/UPDATE statement is illeagal when out of when function");
        break;
      }
      switch (node->type_)
      {
        case T_INSERT:
          ret = resolve_insert_stmt(result_plan, node, query_id);
          break;
        case T_DELETE:
          ret = resolve_delete_stmt(result_plan, node, query_id);
          break;
        case T_UPDATE:
          ret = resolve_update_stmt(result_plan, node, query_id);
          break;
        default:
          ret = OB_ERR_ILLEGAL_TYPE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown statement type in when function");
          break;
      }
      if (ret != OB_SUCCESS)
      {
        break;
      }
      else if (CREATE_RAW_EXPR(sub_query_expr, ObUnaryRefRawExpr, result_plan) == NULL)
      {
        break;
      }
      else
      {
        sub_query_expr->set_expr_type(T_REF_QUERY);
        // not mathematic expression, result type is of no use.
        // should be ObRowType
        sub_query_expr->set_result_type(ObMinType);
        sub_query_expr->set_ref_id(query_id);
        expr = sub_query_expr;
      }
      break;
    }
      //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
      /*
      *  for cast函数
      */
    case T_TYPE_DECIMAL:
    {
      ObObj val;
      val.set_int(T_TYPE_DECIMAL);
      if(node->num_child_==2)
      {
        val.set_precision(static_cast<uint32_t>(node->children_[0]->value_));
        val.set_scale(static_cast<uint32_t>(node->children_[1]->value_));
      }
      else
      {
        val.set_precision(38);
        val.set_scale(0);
      }
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_INT);
      c_expr->set_result_type(ObIntType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
      //add:e
    case T_FUN_COUNT:
    case T_FUN_MAX:
    case T_FUN_MIN:
    case T_FUN_SUM:
    case T_FUN_AVG:
    case T_FUN_LISTAGG://add gaojt [ListAgg][JHOBv0.1]20150104:b
    case T_FUN_ROW_NUMBER://add liumz, [ROW_NUMBER]20150825
    {
      if (expr_scope_type == T_INSERT_LIMIT
          || expr_scope_type == T_UPDATE_LIMIT
          || expr_scope_type == T_AGG_LIMIT
          || expr_scope_type == T_WHERE_LIMIT
          || expr_scope_type == T_GROUP_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Invalid use of group function");
        break;
      }
      ObSelectStmt* select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
      ObSqlRawExpr *ret_sql_expr = NULL;
      //add liumz, [ROW_NUMBER]20150825:b
      if (OB_UNLIKELY(T_FUN_ROW_NUMBER == node->type_))
      {
        if ((ret = resolve_anal_func(result_plan, select_stmt, node, ret_sql_expr)) != OB_SUCCESS)
          break;
      }
      //add:e
      else if ((ret = resolve_agg_func(result_plan, select_stmt, node, ret_sql_expr)) != OB_SUCCESS)
        break;
      ObBinaryRefRawExpr *col_expr = NULL;
      if (CREATE_RAW_EXPR(col_expr, ObBinaryRefRawExpr, result_plan) == NULL)
        break;
      col_expr->set_expr_type(T_REF_COLUMN);
      col_expr->set_result_type(ret_sql_expr->get_result_type());
      col_expr->set_first_ref_id(ret_sql_expr->get_table_id());
      col_expr->set_second_ref_id(ret_sql_expr->get_column_id());
      // add invalid table bit index, avoid aggregate function expressions are used as filter
      sql_expr->get_tables_set().add_member(0);
      sql_expr->set_contain_aggr(true);
      expr = col_expr;
      break;
    }
    case T_FUN_SYS:
    {
      ObSysFunRawExpr *func_expr = NULL;
      if (CREATE_RAW_EXPR(func_expr, ObSysFunRawExpr, result_plan) == NULL)
        break;
      func_expr->set_expr_type(T_FUN_SYS);
      ObString func_name;
      ret = ob_write_string(*logical_plan->get_name_pool(), ObString::make_string(node->children_[0]->str_value_), func_name);
      if (ret != OB_SUCCESS)
      {
        ret = OB_ERR_PARSER_MALLOC_FAILED;
        TBSYS_LOG(WARN, "out of memory");
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Malloc function name failed");
        break;
      }
      func_expr->set_func_name(func_name);
      if (node->num_child_ > 1)
      {
        /*add wuna [datetime func] 20150909:b*/
        if(node->children_[0]->type_ == T_DATE_ADD || node->children_[0]->type_ == T_DATE_SUB)
        {
          ObRawExpr *para_datetime_expr = NULL;
          ret = resolve_expr(
                result_plan,
                stmt,
                node->children_[1],
                sql_expr,
                para_datetime_expr);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"resolve expr error.ret=%d",ret);
          }
          else if (OB_SUCCESS != (ret = func_expr->add_param_expr(para_datetime_expr)))
          {
            TBSYS_LOG(ERROR,"add param expr error.");
          }
          else
          {
            ObRawExpr *para_int_expr = NULL;
            ret = resolve_expr(
                  result_plan,
                  stmt,
                  node->children_[2],
                  sql_expr,
                  para_int_expr);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR,"resolve expr error.ret=%d",ret);
            }
            else if (OB_SUCCESS != (ret = func_expr->add_param_expr(para_int_expr)))
            {
              TBSYS_LOG(ERROR,"add param expr error.");
            }
            else
            {
            }
          }
        }
        else
        {
        /*add wuna [datetime] 20150909:e*/
          OB_ASSERT(node->children_[1]->type_ == T_EXPR_LIST);
          ObRawExpr *para_expr = NULL;
          int32_t num = node->children_[1]->num_child_;
          for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
          {
            ret = resolve_expr(
                  result_plan,
                  stmt,
                  node->children_[1]->children_[i],
                  sql_expr,
                  para_expr);
            if (ret != OB_SUCCESS)
              break;
            if (OB_SUCCESS != (ret = func_expr->add_param_expr(para_expr)))
              break;
            //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
            /*
          *      for cast函数
          */

            if(node->children_[1]->children_[i]->type_==T_TYPE_DECIMAL)
            {
              int32_t num_child=node->children_[1]->children_[i]->num_child_;
              for (int32_t j=0; ret==OB_SUCCESS&&j<num_child ;j++)
              {
                ret = resolve_expr(
                      result_plan,
                      stmt,
                      node->children_[1]->children_[i]->children_[j],
                      sql_expr,
                      para_expr);
                if (ret != OB_SUCCESS)
                  break;
                if (OB_SUCCESS != (ret = func_expr->add_param_expr(para_expr)))
                  break;
              }
              if(num_child==2)
              {
                int64_t pre=node->children_[1]->children_[i]->children_[0]->value_;
                int64_t scale=node->children_[1]->children_[i]->children_[1]->value_;
                if(pre>MAX_DECIMAL_DIGIT||scale>MAX_DECIMAL_SCALE||pre<=scale)
                {
                  ret = OB_INVALID_ARGUMENT;
                  TBSYS_LOG(WARN, "invalid argument of decimal. precision = %ld,  scale = %ld. ret=%d", pre, scale,ret);
                }
              }
            }
            //add:e
          }
        }//add wuna 20150909 for datetimefunc
      }
      if (ret == OB_SUCCESS)
      {
        int32_t param_num = 0;
        if ((ret = oceanbase::sql::ObPostfixExpression::get_sys_func_param_num(func_name, param_num)) != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown function '%.*s', ret=%d", func_name.length(), func_name.ptr(), ret);
        }
        else
        {
          switch(param_num)
          {
            case TWO_OR_THREE:
            {
              if (func_expr->get_param_size() < 2 || func_expr->get_param_size() > 3)
              {
                ret = OB_ERR_PARAM_SIZE;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "Param num of function '%.*s' can not be less than 2 or more than 3, ret=%d",
                         func_name.length(), func_name.ptr(), ret);
              }
              break;
            }
            case OCCUR_AS_PAIR:
            {
              /* Won't be here */
              /* No function of this type now */
              ret = OB_ERR_PARAM_SIZE;
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Wrong num of function param(s), function='%.*s', num=%d, ret=%d",
                       func_name.length(), func_name.ptr(), OCCUR_AS_PAIR, ret);
              break;
            }
            case MORE_THAN_ZERO:
            {
              if (func_expr->get_param_size() <= 0)
              {
                ret = OB_ERR_PARAM_SIZE;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "Param num of function '%.*s' can not be empty, ret=%d", func_name.length(), func_name.ptr(), ret);
              }
              break;
            }
              //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
              /*
              *  for cast函数
              */
            case TWO_OR_FOUR:
            {
              if (func_expr->get_param_size() != 2&&func_expr->get_param_size() != 4)
              {
                ret = OB_ERR_PARAM_SIZE;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "Param num of function '%.*s' can not be empty, ret=%d", func_name.length(), func_name.ptr(), ret);
              }
              break;
            }
              //add:e
            default:
            {
              if (func_expr->get_param_size() != param_num)
              {
                ret = OB_ERR_PARAM_SIZE;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "Param num of function '%.*s' must be %d, ret=%d", func_name.length(), func_name.ptr(), param_num, ret);
              }
              break;
            }
          }
        }
      }
      if (ret == OB_SUCCESS)
      {
        if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_LENGTH), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObIntType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_SUBSTR), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_CAST), func_name.ptr(), func_name.length()))
        {
          int32_t num = node->children_[1]->num_child_;
          if (num == 2)
          {
            ObObj obj;
            int64_t item_type;
            ObConstRawExpr *param_expr = dynamic_cast<ObConstRawExpr *>(func_expr->get_param_expr(1));
            if (NULL != param_expr)
            {
              obj = param_expr->get_value();
              if (OB_SUCCESS == (ret = obj.get_int(item_type)))
              {
                ObObjType dest_type = convert_item_type_to_obj_type(static_cast<ObItemType>(item_type));
                func_expr->set_result_type(dest_type);
              }
              else
              {
                TBSYS_LOG(WARN, "fail to get int val. obj.get_type()=%d", obj.get_type());
                break;
              }
            }
            else
            {
              TBSYS_LOG(WARN, "fail to get param expression");
              break;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "CAST function must only take 2 params");
          }
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_CUR_USER), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_TRIM), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_LOWER), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_UPPER), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_COALESCE), func_name.ptr(), func_name.length()))
        {
          // always cast to varchar as it is an all-mighty type
          //modify by dolphin [coalesce return value type]@20151118
            //func_expr->set_result_type(ObVarcharType);
            ObObjType type = ObNullType;
            for(int i = 0; i < func_expr->get_param_size();++i)
            {
                if(OB_SUCCESS != (ret = ObExprObj::coalesce_type_compatible(type,func_expr->get_param_expr(i)->get_result_type())))
                {
                    break;
                }
            }
           if(OB_SUCCESS == ret)
           {
               func_expr->set_result_type(type);
           }
           else
           {
               snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                        "coalesce function parameters type invalid");
               break;
           }

        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_GREATEST), func_name.ptr(), func_name.length()))
        {
          // always cast to varchar as it is an all-mighty type
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_LEAST), func_name.ptr(), func_name.length()))
        {
          // always cast to varchar as it is an all-mighty type
          func_expr->set_result_type(ObVarcharType);
        }
        //add wanglei [last_day] 20160311:b
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_LAST_DAY), func_name.ptr(), func_name.length()))
        {
            func_expr->set_result_type(ObDateType);
        }
        //add wanglei:e
        //add wanglei [to_char] 20160311:b
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_TO_CHAR), func_name.ptr(), func_name.length()))
        {
            func_expr->set_result_type(ObVarcharType);
        }
        //add wanglei:e
        //add wanglei [to_date] 20160311:b
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_TO_DATE), func_name.ptr(), func_name.length()))
        {
            func_expr->set_result_type(ObDateType);
        }
        //add wanglei:e
        //add wanglei [to_timestamp] 20160311:b
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_TO_TIMESTAMP), func_name.ptr(), func_name.length()))
        {
            func_expr->set_result_type(ObPreciseDateTimeType);
        }
        //add wanglei:e
        //add qianzhaoming [decode] 20151029:b
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DECODE), func_name.ptr(), func_name.length()))
        {
          // always cast to varchar as it is an all-mighty type
            //modify by dolphin [coalesce return value type]@20151118
          //func_expr->set_result_type(ObVarcharType);
            ObObjType type = ObNullType;
            for(int i = 0; i < func_expr->get_param_size();++i)
            {
                if(OB_SUCCESS != (ret = ObExprObj::coalesce_type_compatible(type,func_expr->get_param_expr(i)->get_result_type())))
                {
                    break;
                }

            }
            if(OB_SUCCESS == ret)
            {
                func_expr->set_result_type(type);
            }
            else
            {
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "decode function parameters type invalid");
                break;
            }
        }
        //add:e
        //add xionghui [floor and ceil] 20151214:b
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_FLOOR), func_name.ptr(), func_name.length()))
        {
            func_expr->set_result_type(ObDecimalType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_CEIL), func_name.ptr(), func_name.length()))
        {
            func_expr->set_result_type(ObDecimalType);
        }
        //add 20151214:e
        //add xionghui [round] 20150126:b
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_ROUND), func_name.ptr(), func_name.length()))
        {
            func_expr->set_result_type(ObDecimalType);
        }
        //add e:
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_HEX), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_UNHEX), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_IP_TO_INT), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObIntType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_INT_TO_IP), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_GREATEST), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_LEAST), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        //add wuna [datetime func] 20150828:b
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_YEAR), func_name.ptr(), func_name.length()))
        {
          //mod hongchen [int64->int32] 20161118:b
          //func_expr->set_result_type(ObIntType);
          func_expr->set_result_type(ObInt32Type);
          //mod hongchen [int64->int32] 20161118:e
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_MONTH), func_name.ptr(), func_name.length()))
        {
          //mod hongchen [int64->int32] 20161118:b
          //func_expr->set_result_type(ObIntType);
          func_expr->set_result_type(ObInt32Type);
          //mod hongchen [int64->int32] 20161118:e
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DAY), func_name.ptr(), func_name.length()))
        {
          //mod hongchen [int64->int32] 20161118:b
          //func_expr->set_result_type(ObIntType);
          func_expr->set_result_type(ObInt32Type);
          //mod hongchen [int64->int32] 20161118:e
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DAYS), func_name.ptr(), func_name.length()))
        {
          //mod hongchen [int64->int32] 20161118:b
          //func_expr->set_result_type(ObIntType);
          func_expr->set_result_type(ObInt32Type);
          //mod hongchen [int64->int32] 20161118:e
        }
        else if ( 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_ADD_YEAR), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_SUB_YEAR), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_ADD_MONTH), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_SUB_MONTH), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_ADD_DAY), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_SUB_DAY), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_ADD_HOUR), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_SUB_HOUR), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_ADD_MINUTE), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_SUB_MINUTE), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_ADD_SECOND), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_SUB_SECOND), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_ADD_MICROSECOND), func_name.ptr(), func_name.length())
                  || 0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_DATE_SUB_MICROSECOND), func_name.ptr(), func_name.length())
                  )

        {
          ObObjType result_type = ObMinType;
          //mod peiouya [BUG_FIX_INVALID_TYPE] 20160401:b
          //clean code and fix bug
//          if (T_IDENT == node->children_[1]->type_)
//          {
//            ObString column_name;
//            ObString *table_name = NULL;
//            OB_ASSERT(0 <= node->children_[1]->num_child_);
//            if (0 < node->children_[1]->num_child_)
//            {
//              column_name.assign_ptr(
//                    (char*)(node->children_[1]->children_[1]->str_value_),
//                    static_cast<int32_t>(strlen(node->children_[1]->children_[1]->str_value_))
//                    );
//              table_name->assign_ptr(
//                    (char*)(node->children_[1]->children_[0]->children_[1]->str_value_),
//                    static_cast<int32_t>(strlen(node->children_[1]->children_[0]->children_[1]->str_value_))
//                    );
//            }
//            else
//            {
//              column_name.assign_ptr(
//                    (char*)(node->children_[1]->str_value_),
//                    static_cast<int32_t>(strlen(node->children_[1]->str_value_))
//                    );
//            }
//            ColumnItem* column_item = NULL;
//            column_item = stmt->get_column_item(table_name, column_name);
//            if (NULL == column_item)
//            {
//              ret = OB_ERR_PARSER_SYNTAX;
//              TBSYS_LOG(WARN,"Fail to get column item!");
//            }
//            else
//            {
//              result_type = column_item->data_type_;
//              if(result_type != ObNullType
//                 && result_type != ObPreciseDateTimeType
//                 && result_type != ObDateType
//                 && result_type != ObTimeType)
//              {
//                ret = OB_ERR_PARSER_SYNTAX;
//              }
//            }
//          }
//          else
//          {
//            // src_type = node->children_[1]->type_;
//            switch(node->children_[1]->type_)
//            {
//              case T_NULL:
//                result_type = ObNullType;
//                break;
//              case T_DATE:
//                result_type = ObPreciseDateTimeType;
//                break;
//              case T_DATE_NEW:
//                result_type = ObDateType;
//                break;
//              case T_TIME:
//                result_type = ObTimeType;
//                break;
//              default:
//                ret = OB_ERR_PARSER_SYNTAX;
//            }
//          }
          OB_ASSERT(2 == func_expr->get_param_size ());
          switch(func_expr->get_param_expr (0)->get_result_type ())
          {
            //mod peiouya [BUF_FIX] 20160427:b
            //change invalid type to valid type
            //case T_NULL:
            case ObNullType:
                 result_type = ObNullType;
                 break;
            //case T_DATE:
            case ObPreciseDateTimeType:
                 result_type = ObPreciseDateTimeType;
                 break;
            //case T_DATE_NEW:
            case ObDateType:
                 result_type = ObDateType;
                 break;
            //case T_TIME:
            case ObTimeType:
                 result_type = ObTimeType;
                 break;
            //mod 20160427:e
            default:
                 ret = OB_ERR_PARSER_SYNTAX;
          }
          //mod peiouya [BUG_FIX_INVALID_TYPE] 20160401:e
          if(OB_SUCCESS == ret)
          {
            func_expr->set_result_type(result_type);
          }
          else
          {
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Invalid type.");
          }
        }
        //add 20150828:e
        //add liuzy [datetime func] 20150901:b
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_HOUR), func_name.ptr(), func_name.length()))
        {
          //mod hongchen [int64->int32] 20161118:b
          //func_expr->set_result_type(ObIntType);
          func_expr->set_result_type(ObInt32Type);
          //mod hongchen [int64->int32] 20161118:e
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_MINUTE), func_name.ptr(), func_name.length()))
        {
          //mod hongchen [int64->int32] 20161118:b
          //func_expr->set_result_type(ObIntType);
          func_expr->set_result_type(ObInt32Type);
          //mod hongchen [int64->int32] 20161118:e
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_SECOND), func_name.ptr(), func_name.length()))
        {
          //mod hongchen [int64->int32] 20161118:b
          //func_expr->set_result_type(ObIntType);
          func_expr->set_result_type(ObInt32Type);
          //mod hongchen [int64->int32] 20161118:e
        }
        //add 20150901:e
        //add lijianqiang [replace_fuction] 20151102:b
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_REPLACE), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);//the result type should be the data_type of source_str(the first param(expr)),we will confirm later(actually is "varchar" type)
        }
        //add 20151102:e
        else
        {
          ret = OB_ERR_UNKNOWN_SYS_FUNC;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "system function `%.*s' not supported", func_name.length(), func_name.ptr());
        }
      }
      if (ret == OB_SUCCESS)
      {
        expr = func_expr;
      }
      break;
    }
    case T_ROW_COUNT:
    {
      if (expr_scope_type != T_WHEN_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Invalid use of row_count function, it can be in WHEN clause only");
        break;
      }
      ObSqlRawExpr *ret_sql_expr = NULL;
      ObBinaryRefRawExpr *col_expr = NULL;
      if ((ret = resolve_when_func(result_plan, stmt, node, ret_sql_expr)) != OB_SUCCESS)
      {
        break;
      }
      else if (CREATE_RAW_EXPR(col_expr, ObBinaryRefRawExpr, result_plan) == NULL)
      {
        break;
      }
      col_expr->set_expr_type(T_REF_COLUMN);
      col_expr->set_result_type(ObIntType);
      col_expr->set_first_ref_id(ret_sql_expr->get_table_id());
      col_expr->set_second_ref_id(ret_sql_expr->get_column_id());
      expr = col_expr;
      break;
    }
    //add lijianqiang [sequence] 20150331:b
    /*Exp:add case T_SEQUENCE_EXPR for sequence usage*/
    case T_SEQUENCE_EXPR:
    {
      ObString str;
      if (expr_scope_type == T_INSERT_LIMIT && stmt->get_stmt_type() == ObStmt::T_INSERT)//insert
      {
        if (node->num_child_ != 1)
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Invalid use of sequence expr in insert values, the child num muse be one!");
          break;
        }
        else//normal
        {
          bool is_next_type = true;//current nextval "true",prevval,"false",default "true"
          ObString sequence_name;
          ObInsertStmt* insert_stmt = dynamic_cast<ObInsertStmt*>(stmt);
          sequence_name.assign_ptr((char*)(node->children_[0]->str_value_),
                                   static_cast<int32_t>(strlen(node->children_[0]->str_value_))
                                   );

          if (OB_SUCCESS != (ret = ob_write_string(*name_pool, sequence_name, str)))
          {
            TBSYS_LOG(WARN, "out of memory");
            break;
          }
          //          TBSYS_LOG(ERROR, "the value is ::[%ld]",node->children_[0]->value_);
          if (oceanbase::common::PREV_TYPE == node->children_[0]->value_)//prvval set false
          {
            is_next_type = false;
            //            TBSYS_LOG(ERROR,"current is preval!");
          }
          insert_stmt->set_is_next_type(is_next_type);
          insert_stmt->set_current_expr_has_sequence(true);
          insert_stmt->resolve_sequence_id_vectors();
          insert_stmt->add_sequence_name_no_dup(sequence_name);
        }
      }
      else if(expr_scope_type == T_WHERE_LIMIT && stmt->get_stmt_type() == ObStmt::T_DELETE)//delete
      {
        ObDeleteStmt* delete_stmt = dynamic_cast<ObDeleteStmt*>(stmt);
        ObString sequence_name;
        sequence_name.assign_ptr((char*)(node->children_[0]->str_value_),
                                 static_cast<int32_t>(strlen(node->children_[0]->str_value_))
                                 );
        if (OB_SUCCESS != (ret = ob_write_string(*name_pool, sequence_name, str)))
        {
          TBSYS_LOG(WARN, "out of memory");
          break;
        }
        if (oceanbase::common::NEXT_TYPE == node->children_[0]->value_)//can't be nextval type
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "NEXTVAL FOR SEQUENCE [%.*s] cannot be specified in this context",sequence_name.length(),sequence_name.ptr());
          break;
        }
        else
        {
          delete_stmt->set_current_expr_has_sequence(true);
          delete_stmt->add_sequence_name(sequence_name);
        }
      }
      else if ((expr_scope_type == T_WHERE_LIMIT || expr_scope_type == T_UPDATE_LIMIT) && stmt->get_stmt_type() == ObStmt::T_UPDATE)//update
      {
        ObUpdateStmt* update_stmt = dynamic_cast<ObUpdateStmt*>(stmt);
        ObString sequence_name;
        sequence_name.assign_ptr((char*)(node->children_[0]->str_value_),
                                 static_cast<int32_t>(strlen(node->children_[0]->str_value_))
                                 );
        if (OB_SUCCESS != (ret = ob_write_string(*name_pool, sequence_name, str)))
        {
          TBSYS_LOG(WARN, "out of memory");
          break;
        }
        if ((oceanbase::common::NEXT_TYPE == node->children_[0]->value_) && (expr_scope_type == T_WHERE_LIMIT))//nextval type
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "NEXTVAL FOR SEQUENCE [%.*s] cannot be specified in this context",sequence_name.length(),sequence_name.ptr());
          break;
        }
        if (OB_SUCCESS == ret)
        {
          bool is_next_type = true;
          if (oceanbase::common::PREV_TYPE == node->children_[0]->value_)//prvval set false
          {
            is_next_type = false;
            //            TBSYS_LOG(ERROR,"current is preval!");
          }
          update_stmt->set_is_next_type(is_next_type);//must be excute first,we need to get the sequence type fist
          update_stmt->set_current_expr_has_sequence(true);
          update_stmt->add_sequence_name_and_type(str);
        }
      }
      //add liuzy [sequence select] 20150525:b
      else if (stmt->get_stmt_type() == ObStmt::T_SELECT)//select
      {
        ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
        ObString sequence_name;
        sequence_name.assign_ptr((char*)(node->children_[0]->str_value_),
                                 static_cast<int32_t>(strlen(node->children_[0]->str_value_)));
        if ((oceanbase::common::NEXT_TYPE == node->children_[0]->value_) && (expr_scope_type == T_WHERE_LIMIT))//nextval type
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "NEXTVAL FOR SEQUENCE [%.*s] cannot be specified in this context",sequence_name.length(),sequence_name.ptr());
          break;
        }
        else
        {
          if (OB_SUCCESS != (ret = ob_write_string(*name_pool, sequence_name, str)))
          {
            TBSYS_LOG(WARN, "out of memory");
            break;
          }
          if (OB_SUCCESS == ret)
          {
            bool is_next_type = true;//current nextval "true",prevval,"false",default "true"
            if (oceanbase::common::PREV_TYPE == node->children_[0]->value_)//prvval set false
            {
              is_next_type = false;
            }
            select_stmt->set_is_next_type(is_next_type);
            select_stmt->set_current_expr_has_sequence(true);
            if (OB_SUCCESS != (ret = select_stmt->cons_sequence_single_name_type_pair(str)))
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "construction sequence single pair(name, type) failed.");
            }
          }
        }
      }
      //add 20150525:e
      if (OB_SUCCESS == ret)
      {
        ObObj val;
        val.set_varchar(str);
        ObConstRawExpr *c_expr = NULL;
        if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
          break;
        c_expr->set_expr_type(T_STRING);
        c_expr->set_result_type(ObVarcharType);
        c_expr->set_value(val);
        expr = c_expr;
      }
      break;
    }
      //add 20150331:e

    default:
      ret = OB_ERR_PARSER_SYNTAX;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Wrong type in expression");
      break;
  }

  return ret;
}

ObSqlRawExpr* create_middle_sql_raw_expr(
    ResultPlan& result_plan,
    ParseNode& node,
    uint64_t& expr_id)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSqlRawExpr* sql_expr = NULL;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan.plan_tree_);

  if (logical_plan == NULL)
  {
    TBSYS_LOG(WARN, "Logical Plan is empty");
  }
  else if ((sql_expr = (ObSqlRawExpr*)parse_malloc(
              sizeof(ObSqlRawExpr),
              result_plan.name_pool_)) == NULL)
  {
    ret = OB_ERR_PARSER_MALLOC_FAILED;
    TBSYS_LOG(WARN, "out of memory");
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "Can not malloc space for ObSqlRawExpr");
  }
  else
  {
    sql_expr = new(sql_expr) ObSqlRawExpr();
    if ((ret = logical_plan->add_expr(sql_expr)) != OB_SUCCESS)
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
               "Add ObSqlRawExpr error");
    }
  }
  if (ret == OB_SUCCESS)
  {
    expr_id = logical_plan->generate_expr_id();
    sql_expr->set_expr_id(expr_id);
    sql_expr->set_table_id(OB_INVALID_ID);
    switch(node.type_)
    {
      case T_FUN_COUNT:
      case T_FUN_MAX:
      case T_FUN_MIN:
      case T_FUN_SUM:
      case T_FUN_AVG:
      case T_FUN_LISTAGG://add gaojt [ListAgg][JHOBv0.1]20150104:b
      case T_FUN_ROW_NUMBER://add liumz, [ROW_NUMBER]20150825
        sql_expr->set_column_id(logical_plan->generate_range_column_id());
        break;
      case T_ROW_COUNT:
        sql_expr->set_column_id(logical_plan->generate_column_id());
        break;
      default:
        ret = OB_ERR_ILLEGAL_TYPE;
        snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown function type");
        break;
    }
  }
  if (ret != OB_SUCCESS)
  {
    sql_expr = NULL;
  }
  return sql_expr;
}

int resolve_when_func(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    ObSqlRawExpr*& ret_sql_expr)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t expr_id = OB_INVALID_ID;
  ObSqlRawExpr* sql_expr = NULL;
  if (node != NULL)
  {
    if ((sql_expr = create_middle_sql_raw_expr(
           *result_plan,
           *node,
           expr_id)) == NULL)
    {
      TBSYS_LOG(WARN, "Create middle sql raw expr failed");
    }
    else if ((ret = stmt->add_when_func(expr_id)) != OB_SUCCESS)
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Add when function error");
    }
    else
    {
      ObRawExpr *sub_expr = NULL;
      ObUnaryOpRawExpr *row_count_expr = NULL;
      if ((ret = resolve_expr(
             result_plan,
             stmt,
             node->children_[0],
             sql_expr,
             sub_expr,
             T_WHEN_LIMIT,
             false)) != OB_SUCCESS)
      {
      }
      else if (CREATE_RAW_EXPR(row_count_expr, ObUnaryOpRawExpr, result_plan) == NULL)
      {
        ret = OB_ERR_PARSER_MALLOC_FAILED;
        TBSYS_LOG(WARN, "Create row_count expression failed");
      }
      else
      {
        row_count_expr->set_expr_type(node->type_);
        row_count_expr->set_result_type(ObIntType);
        row_count_expr->set_op_expr(sub_expr);
        sql_expr->set_expr(row_count_expr);
        ret_sql_expr = sql_expr;
      }
    }
  }
  else
  {
    ret = OB_ERR_PARSER_SYNTAX;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "Wrong usage of When function");
  }
  return ret;
}

int resolve_agg_func(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node,
    ObSqlRawExpr*& ret_sql_expr)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t expr_id = OB_INVALID_ID;
  ObSqlRawExpr* sql_expr = NULL;
  if (node != NULL)
  {
    if ((sql_expr = create_middle_sql_raw_expr(
           *result_plan,
           *node,
           expr_id)) == NULL)
    {
      TBSYS_LOG(WARN, "Create middle sql raw expr failed");
    }
    else if ((ret = select_stmt->add_agg_func(expr_id)) != OB_SUCCESS)
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Add aggregate function error");
    }

    // When '*', do not set parameter
    ObRawExpr* sub_expr = NULL;
    ObRawExpr* sub_expr_delimeter = NULL;//add gaojt [ListAgg][JHOBv0.1]20150104 /*Exp:store delimeter of listagg */
    bool is_delimeter_exist = false;//add gaojt [ListAgg][JHOBv0.1]20150104
    if (ret == OB_SUCCESS)
    {
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      /*Exp: used for generate the logical plan of listagg*/
      if(node->type_ == T_FUN_LISTAGG)
      {
        if(node->num_child_>=2)
        {
          if(1 == node->children_[1]->num_child_)
          {
            if(OB_SUCCESS != (ret = resolve_expr(result_plan, select_stmt, node->children_[1]->children_[0], sql_expr, sub_expr, T_AGG_LIMIT)))
            {
              TBSYS_LOG(ERROR, "resolve expr_list failed");
            }
          }
          else if(2 == node->children_[1]->num_child_)
          {
            if(T_STRING == node->children_[1]->children_[1]->type_)
            {
              if(OB_SUCCESS != (ret = resolve_expr(result_plan, select_stmt, node->children_[1]->children_[0], sql_expr, sub_expr, T_AGG_LIMIT)))
              {
                TBSYS_LOG(ERROR, "resolve first parameter failed,ret=%d",ret);
              }
              else if(OB_SUCCESS != (ret = resolve_expr(result_plan, select_stmt, node->children_[1]->children_[1], sql_expr, sub_expr_delimeter, T_AGG_LIMIT)))
              {
                TBSYS_LOG(ERROR, "resolve expr_list failed,ret=%d",ret);
              }
              if(OB_SUCCESS == ret)
              {
                sub_expr_delimeter->set_is_listagg(true);
                is_delimeter_exist = true;
              }
            }
            else
            {
              ret = OB_OBJ_TYPE_ERROR;
              TBSYS_LOG(ERROR, "the delimeter's type must T_STRING',ret=%d",ret);
            }
          }
          else
          {
            ret = OB_ERR_PARAM_SIZE;
            TBSYS_LOG(ERROR, "wrong param number for listagg");
          }
          if(OB_SUCCESS == ret&& node->num_child_==3)
          {
            if(OB_SUCCESS != (ret = resolve_order_clause_for_listagg(result_plan, select_stmt, node->children_[2])))
            {
              TBSYS_LOG(ERROR, "resolve order clause for listagg failed");
            }
          }
        }
        else
        {
          ret = OB_ERR_PARAM_SIZE;
          TBSYS_LOG(ERROR, "wrong param number for listagg");
        }
      }
      else /*add 20150104:e*/if (node->type_ != T_FUN_COUNT || node->num_child_ > 1)
        ret = resolve_expr(result_plan, select_stmt, node->children_[1], sql_expr, sub_expr, T_AGG_LIMIT);
    }

    ObAggFunRawExpr *agg_expr = NULL;
    if (ret == OB_SUCCESS && CREATE_RAW_EXPR(agg_expr, ObAggFunRawExpr, result_plan) != NULL)
    {
      agg_expr->set_param_expr(sub_expr);
      if (node->children_[0] && node->children_[0]->type_ == T_DISTINCT)
        agg_expr->set_param_distinct();
      agg_expr->set_expr_type(node->type_);
      if (node->type_ == T_FUN_COUNT)
        agg_expr->set_expr_type(T_FUN_COUNT);
      else if (node->type_ == T_FUN_MAX)
        agg_expr->set_expr_type(T_FUN_MAX);
      else if (node->type_ == T_FUN_MIN)
        agg_expr->set_expr_type(T_FUN_MIN);
      else if (node->type_ == T_FUN_SUM)
        agg_expr->set_expr_type(T_FUN_SUM);
      else if (node->type_ == T_FUN_AVG)
        agg_expr->set_expr_type(T_FUN_AVG);
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      /*Exp: store the delimeter of listagg*/
      else if (node->type_ == T_FUN_LISTAGG)
      {
        if(true == is_delimeter_exist)
        {
          agg_expr->set_listagg_param_delimeter(sub_expr_delimeter);
        }
        agg_expr->set_expr_type(T_FUN_LISTAGG);
      }
      /* add 20150104:e*/
      else
      {
        /* Won't be here */

      }
      if (node->type_ == T_FUN_COUNT)
      {
        agg_expr->set_result_type(ObIntType);
      }
      else if (node->type_ == T_FUN_MAX || node->type_ == T_FUN_MIN || node->type_ == T_FUN_SUM)
      {
        agg_expr->set_result_type(sub_expr->get_result_type());
      }
      else if (node->type_ == T_FUN_AVG)
      {
        ObObj in_type1;
        ObObj in_type2;
        in_type1.set_type(sub_expr->get_result_type());
        in_type2.set_type(ObIntType);
        if (in_type1.get_type() == ObDoubleType)
          agg_expr->set_result_type(ObExprObj::type_div(in_type1, in_type2, true).get_type());
        else
          agg_expr->set_result_type(ObExprObj::type_div(in_type1, in_type2, false).get_type());
      }
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      else if(node->type_ == T_FUN_LISTAGG)
      {
        agg_expr->set_result_type(ObVarcharType);
      }
      //add 20150104:e
      else
      {
        /* won't be here */
        agg_expr->set_result_type(ObMinType);
        OB_ASSERT(false);
      }

      sql_expr->set_expr(agg_expr);
      sql_expr->set_contain_aggr(true);
      // add invalid table bit index, avoid aggregate function expressions are used as filters
      sql_expr->get_tables_set().add_member(0);
    }
  }
  else
  {
    ret = OB_ERR_PARSER_SYNTAX;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "Wrong usage of aggregate function");
  }

  if (ret == OB_SUCCESS)
    ret_sql_expr = sql_expr;
  return ret;
}

//add liumz, [ROW_NUMBER]20150825:b
int resolve_anal_func(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node,
    ObSqlRawExpr*& ret_sql_expr)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t expr_id = OB_INVALID_ID;
  ObSqlRawExpr* sql_expr = NULL;
  if (node != NULL)
  {
    if ((sql_expr = create_middle_sql_raw_expr(
           *result_plan,
           *node,
           expr_id)) == NULL)
    {
      TBSYS_LOG(WARN, "Create middle sql raw expr failed");
    }
    else if ((ret = select_stmt->add_anal_func(expr_id)) != OB_SUCCESS)
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Add analytic function error");
    }

    if (OB_SUCCESS == ret)
    {
      if (node->type_ == T_FUN_ROW_NUMBER)
      {
        OB_ASSERT(node->num_child_ == 2);
        ParseNode* partition_node = node->children_[0];
        ParseNode* order_node = node->children_[1];
        if (NULL != partition_node)
        {
          //parse partition by clause in ROW_NUMBER() over ([partition by clause] [order by clause])
          ret = resolve_partition_clause(result_plan, select_stmt, partition_node);
        }
        if (OB_SUCCESS == ret && NULL != order_node)
        {
          //parse order by clause in ROW_NUMBER() over ([partition by clause] [order by clause])
          ret = resolve_order_clause_for_rownum(result_plan, select_stmt, order_node);
        }
      }
      else
      {
        //should not be here!
      }
    }

    ObAggFunRawExpr *agg_expr = NULL;//analytic func also use ObAggFunRawExpr
    if (ret == OB_SUCCESS && CREATE_RAW_EXPR(agg_expr, ObAggFunRawExpr, result_plan) != NULL)
    {
      agg_expr->set_expr_type(node->type_);
      if (node->type_ == T_FUN_ROW_NUMBER)
      {
        agg_expr->set_expr_type(T_FUN_ROW_NUMBER);
        agg_expr->set_result_type(ObIntType);
      }
      else
      {
        //should not be here!
      }
      sql_expr->set_expr(agg_expr);
      sql_expr->set_contain_aggr(true);
      // add invalid table bit index, avoid aggregate function expressions are used as filters
      sql_expr->get_tables_set().add_member(0);
    }
  }
  else
  {
    ret = OB_ERR_PARSER_SYNTAX;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "Wrong usage of analytic function");
  }

  if (ret == OB_SUCCESS)
    ret_sql_expr = sql_expr;
  return ret;
}
//add:e

int resolve_joined_table(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node,
    JoinedTable& joined_table)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node->type_ == T_JOINED_TABLE);

  uint64_t tid = OB_INVALID_ID;
  ParseNode* table_node = NULL;

  /* resolve table */
  for (uint64_t i = 1; ret == OB_SUCCESS && i < 3; i++)
  {
    table_node = node->children_[i];
    switch (table_node->type_)
    {
      case T_IDENT:
      case T_RELATION://add liumz
      case T_SELECT:
      case T_ALIAS:
        ret = resolve_table(result_plan, select_stmt, table_node, tid);
        if (ret == OB_SUCCESS && (ret = joined_table.add_table_id(tid)) != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Add table_id to joined table failed");
        //"Add table_id to outer joined table failed");//mod liumz
        break;
      case T_JOINED_TABLE:
        ret = resolve_joined_table(result_plan, select_stmt, table_node, joined_table);
        break;
      default:
        /* won't be here */
        ret = OB_ERR_PARSER_MALLOC_FAILED;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown table type in join");
        //"Unknown table type in outer join");//mod liumz
        break;
    }
  }

  /* resolve join type */
  if (ret == OB_SUCCESS)
  {
    switch (node->children_[0]->type_)
    {
      case T_JOIN_FULL:
        ret = joined_table.add_join_type(JoinedTable::T_FULL);
        break;
      case T_JOIN_LEFT:
        ret = joined_table.add_join_type(JoinedTable::T_LEFT);
        break;
      case T_JOIN_RIGHT:
        ret = joined_table.add_join_type(JoinedTable::T_RIGHT);
        break;
      case T_JOIN_INNER:
        ret = joined_table.add_join_type(JoinedTable::T_INNER);
        break;
      /*add by wanglei [semi join] 20151106*/
      case T_JOIN_SEMI:
        ret = joined_table.add_join_type(JoinedTable::T_SEMI);
        break;
      case T_JOIN_SEMI_LEFT:
        ret = joined_table.add_join_type(JoinedTable::T_SEMI_LEFT);
        break;
      case T_JOIN_SEMI_RIGHT:
        ret = joined_table.add_join_type(JoinedTable::T_SEMI_RIGHT);
        break;
      default:
        /* won't be here */
        ret = OB_ERR_PARSER_MALLOC_FAILED;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown outer join type");
        break;
    }
  }

  /* resolve expression */
  if (ret == OB_SUCCESS)
  {
    ObVector<uint64_t> join_exprs;
    ret = resolve_and_exprs(result_plan, select_stmt, node->children_[3], join_exprs, T_WHERE_LIMIT);
    if (ret == OB_SUCCESS)
    {
      if ((ret = joined_table.add_join_exprs(join_exprs)) != OB_SUCCESS)
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Add outer join condition error");
    }
  }

  return ret;
}

int resolve_table(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    uint64_t& table_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObSQLSessionInfo* session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);
  if (node)
  {
    table_id = OB_INVALID_ID;
    ParseNode* table_node = node;
    ParseNode* alias_node = NULL;
    if (node->type_ == T_ALIAS)
    {
      OB_ASSERT(node->num_child_ == 2);
      OB_ASSERT(node->children_[0]);
      OB_ASSERT(node->children_[1]);

      table_node = node->children_[0];
      alias_node = node->children_[1];
    }

    switch (table_node->type_)
    {
      //T_IDENT, keep for show stmt
      case T_IDENT:
      {
        ObString db_name;//not use
        ObString table_name;
        ObString alias_name;
        table_name.assign_ptr(
              (char*)(table_node->str_value_),
              static_cast<int32_t>(strlen(table_node->str_value_))
              );
        if (alias_node)
        {
          alias_name.assign_ptr(
                (char*)(alias_node->str_value_),
                static_cast<int32_t>(strlen(alias_node->str_value_))
                );
          ret = stmt->add_table_item(*result_plan, db_name, table_name, alias_name, table_id, TableItem::ALIAS_TABLE);
        }
        else
          ret = stmt->add_table_item(*result_plan, db_name, table_name, alias_name, table_id, TableItem::BASE_TABLE);
        break;
      }
        //add liumz, [multi_database.sql]:20150613
      case T_RELATION:
      {
        OB_ASSERT(table_node->num_child_ == 2);
        OB_ASSERT(table_node->children_[1]);
        ParseNode* db_node = table_node->children_[0];
        table_node = table_node->children_[1];
        ObString db_name;
        ObString table_name;
        ObString alias_name;
        if (NULL != db_node)
        {
          db_name.assign_ptr(
                (char*)(db_node->str_value_),
                static_cast<int32_t>(strlen(db_node->str_value_)));
        }
        else
        {
          if (session_info == NULL)
          {
            ret = OB_ERR_SESSION_UNSET;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Session not set");
            break;
          }
          else
          {
            db_name = session_info->get_db_name();
          }
        }
        table_name.assign_ptr(
              (char*)(table_node->str_value_),
              static_cast<int32_t>(strlen(table_node->str_value_))
              );
        if (alias_node)
        {
          alias_name.assign_ptr(
                (char*)(alias_node->str_value_),
                static_cast<int32_t>(strlen(alias_node->str_value_))
                );
          ret = stmt->add_table_item(*result_plan, db_name, table_name, alias_name, table_id, TableItem::ALIAS_TABLE);
        }
        else
          ret = stmt->add_table_item(*result_plan, db_name, table_name, alias_name, table_id, TableItem::BASE_TABLE);
        break;
      }
        //add:e
      case T_SELECT:
      {
        /* It must be select statement.
              * For other statements, if the target is a view, it need to be expanded before this step
              */
        OB_ASSERT(stmt->get_stmt_type() == ObStmt::T_SELECT);
        ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
        if (alias_node == NULL)
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "generated table must have alias name");
          break;
        }

        uint64_t query_id = OB_INVALID_ID;
        ret = resolve_select_stmt(result_plan, table_node, query_id);
        if (ret == OB_SUCCESS)
        {
          ObString db_name;//no use, like alias_name
          ObString table_name;
          ObString alias_name;
          table_name.assign_ptr(
                (char*)(alias_node->str_value_),
                static_cast<int32_t>(strlen(alias_node->str_value_))
                );
          ret = select_stmt->add_table_item(
                *result_plan,
                db_name,
                table_name,
                alias_name,
                table_id,
                TableItem::GENERATED_TABLE,
                query_id
                );
        }
        break;
      }
      case T_JOINED_TABLE:
      {
        /* only select statement has this type */
        OB_ASSERT(stmt->get_stmt_type() == ObStmt::T_SELECT);
        ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
        table_id = select_stmt->generate_joined_tid();
        JoinedTable* joined_table = (JoinedTable*)parse_malloc(sizeof(JoinedTable), result_plan->name_pool_);
        if (joined_table == NULL)
        {
          ret = OB_ERR_PARSER_MALLOC_FAILED;
          TBSYS_LOG(WARN, "out of memory");
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not malloc space for JoinedTable");
          break;
        }
        joined_table = new(joined_table) JoinedTable;
        joined_table->set_joined_tid(table_id);
        ret = resolve_joined_table(result_plan, select_stmt, table_node, *joined_table);
        if (ret != OB_SUCCESS)
          break;
        ret = select_stmt->add_joined_table(joined_table);
        if (ret != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not add JoinedTable");
        break;
      }
      default:
        /* won't be here */
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown table type");
        break;
    }
  }
  else
  {
    ret = OB_ERR_PARSER_SYNTAX;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "No table in from clause");
  }

  return ret;
}

int resolve_from_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_FROM_LIST);
    OB_ASSERT(node->num_child_ >= 1);

    uint64_t tid = OB_INVALID_ID;
    for(int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      ParseNode* child_node = node->children_[i];
      ret = resolve_table(result_plan, select_stmt, child_node, tid);
      if (ret != OB_SUCCESS)
        break;

      if (child_node->type_ == T_JOINED_TABLE)
        ret = select_stmt->add_from_item(tid, true);
      else
        ret = select_stmt->add_from_item(tid);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Add from table failed");
        break;
      }
    }
  }
  return ret;
}

int resolve_table_columns(
    ResultPlan * result_plan,
    ObStmt* stmt,
    TableItem& table_item,
    int64_t num_columns)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ColumnItem *column_item = NULL;
  ColumnItem new_column_item;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  if (logical_plan == NULL)
  {
    ret = OB_ERR_LOGICAL_PLAN_FAILD;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "Wrong invocation of ObStmt::add_table_item, logical_plan must exist!!!");
  }

  ObSchemaChecker* schema_checker = NULL;
  if (ret == OB_SUCCESS)
  {
    schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
    if (schema_checker == NULL)
    {
      ret = OB_ERR_SCHEMA_UNSET;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Schema(s) are not set");
    }
  }

  if (ret == OB_SUCCESS)
  {
    if (table_item.type_ == TableItem::GENERATED_TABLE)
    {
      ObSelectStmt* sub_select = static_cast<ObSelectStmt*>(logical_plan->get_query(table_item.ref_id_));
      if (sub_select == NULL)
      {
        ret = OB_ERR_ILLEGAL_ID;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not get sub-query whose id = %lu", table_item.ref_id_);
      }
      else
      {
        int32_t num = sub_select->get_select_item_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num && (num_columns <= 0 || i < num_columns); i++)
        {
          const SelectItem& select_item = sub_select->get_select_item(i);
          column_item = stmt->get_column_item_by_id(table_item.table_id_, OB_APP_MIN_COLUMN_ID + i);
          if (column_item == NULL)
          {
            new_column_item.column_id_ = OB_APP_MIN_COLUMN_ID + i;
            if ((ret = ob_write_string(*stmt->get_name_pool(),
                                       select_item.alias_name_,
                                       new_column_item.column_name_)) != OB_SUCCESS)
            {
              ret = OB_ERR_PARSER_MALLOC_FAILED;
              TBSYS_LOG(WARN, "out of memory");
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Can not malloc space for column name");
              break;
            }
            new_column_item.table_id_ = table_item.table_id_;
            new_column_item.query_id_ = 0; // no use now, because we don't support correlated subquery
            new_column_item.is_name_unique_ = false;
            new_column_item.is_group_based_ = false;
            new_column_item.data_type_ = select_item.type_;
            ret = stmt->add_column_item(new_column_item);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Add column error");
              break;
            }
            column_item = &new_column_item;
          }

          if (stmt->get_stmt_type() == ObStmt::T_SELECT && num_columns <= 0)
          {
            ObBinaryRefRawExpr* expr = NULL;
            if (CREATE_RAW_EXPR(expr, ObBinaryRefRawExpr, result_plan) == NULL)
              break;
            expr->set_expr_type(T_REF_COLUMN);
            expr->set_first_ref_id(column_item->table_id_);
            expr->set_second_ref_id(column_item->column_id_);
            expr->set_result_type(column_item->data_type_);
            ObSqlRawExpr* sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr), result_plan->name_pool_);
            if (sql_expr == NULL)
            {
              ret = OB_ERR_PARSER_MALLOC_FAILED;
              TBSYS_LOG(WARN, "out of memory");
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Can not malloc space for ObSqlRawExpr");
              break;
            }
            sql_expr = new(sql_expr) ObSqlRawExpr();
            sql_expr->set_expr_id(logical_plan->generate_expr_id());
            sql_expr->set_table_id(OB_INVALID_ID);
            sql_expr->set_column_id(logical_plan->generate_column_id());
            sql_expr->set_expr(expr);
            ObBitSet<> tables_set;
            tables_set.add_member(stmt->get_table_bit_index(table_item.table_id_));
            if (stmt->get_table_bit_index(table_item.table_id_)< 0)
            {
              TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,table_item.table_id_);
            }
            sql_expr->set_tables_set(tables_set);
            ret = logical_plan->add_expr(sql_expr);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Can not add ObSqlRawExpr to logical plan");
              break;
            }

            ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
            ret = select_stmt->add_select_item(
                  sql_expr->get_expr_id(),
                  false,
                  column_item->column_name_,
                  select_item.expr_name_,
                  select_item.type_);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Can not add select item");
              break;
            }
          }
        }
      }
    }
    else
    {
      const ObColumnSchemaV2* column = NULL;
      int32_t column_size = 0;
      column = schema_checker->get_table_columns(table_item.ref_id_, column_size);
      if (NULL != column && column_size > 0)
      {
        if (table_item.ref_id_ == OB_TABLES_SHOW_TID) // @FIXME !!!
        {
          column_size = 1;
        }
        for (int32_t i = 0; ret == OB_SUCCESS && i < column_size && (num_columns <= 0 || i < num_columns); i++)
        {
          new_column_item.column_id_ = column[i].get_id();
          column_item = stmt->get_column_item_by_id(table_item.table_id_, new_column_item.column_id_);
          if (column_item == NULL)
          {
            ret = ob_write_string(*stmt->get_name_pool(),
                                  ObString::make_string(column[i].get_name()),
                                  new_column_item.column_name_);
            if (ret != OB_SUCCESS)
            {
              ret = OB_ERR_PARSER_MALLOC_FAILED;
              TBSYS_LOG(WARN, "out of memory");
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Can not malloc space for column name");
              break;
            }
            new_column_item.table_id_ = table_item.table_id_;
            new_column_item.query_id_ = 0; // no use now, because we don't support correlated subquery
            new_column_item.is_name_unique_ = false;
            new_column_item.is_group_based_ = false;
            new_column_item.data_type_ = column[i].get_type();
            ret = stmt->add_column_item(new_column_item);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Add column error");
              break;
            }
            column_item = &new_column_item;
          }

          if (stmt->get_stmt_type() == ObStmt::T_SELECT && num_columns <= 0)
          {
            ObBinaryRefRawExpr* expr = NULL;
            if (CREATE_RAW_EXPR(expr, ObBinaryRefRawExpr, result_plan) == NULL)
              break;
            expr->set_expr_type(T_REF_COLUMN);
            expr->set_first_ref_id(column_item->table_id_);
            expr->set_second_ref_id(column_item->column_id_);
            expr->set_result_type(column_item->data_type_);
            ObSqlRawExpr* sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr), result_plan->name_pool_);
            if (sql_expr == NULL)
            {
              ret = OB_ERR_PARSER_MALLOC_FAILED;
              TBSYS_LOG(WARN, "out of memory");
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Can not malloc space for ObSqlRawExpr");
              break;
            }
            sql_expr = new(sql_expr) ObSqlRawExpr();
            sql_expr->set_expr_id(logical_plan->generate_expr_id());
            sql_expr->set_table_id(OB_INVALID_ID);
            sql_expr->set_column_id(logical_plan->generate_column_id());
            sql_expr->set_expr(expr);
            ObBitSet<> tables_set;
            tables_set.add_member(stmt->get_table_bit_index(table_item.table_id_));
            if (stmt->get_table_bit_index(table_item.table_id_)< 0)
            {
              TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,table_item.table_id_);
            }
            sql_expr->set_tables_set(tables_set);
            ret = logical_plan->add_expr(sql_expr);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Can not add ObSqlRawExpr to logical plan");
              break;
            }

            ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
            ret = select_stmt->add_select_item(
                  sql_expr->get_expr_id(),
                  false,
                  column_item->column_name_,
                  column_item->column_name_,
                  column_item->data_type_);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Can not add select item");
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int resolve_star(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(select_stmt);
  OB_ASSERT(node);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObSQLSessionInfo* session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);

  //add liumz, [multi_database.sql]:20150615
  if (session_info == NULL)
  {
    ret = OB_ERR_SESSION_UNSET;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Session not set");
  }
  //add:e
  else if (node->type_ == T_STAR)
  {
    int32_t num = select_stmt->get_table_size();
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
      TableItem& table_item = select_stmt->get_table_item(i);
      ret = resolve_table_columns(result_plan, select_stmt, table_item);
    }
  }
  else if (node->type_ == T_OP_NAME_FIELD)
  {
    //OB_ASSERT(node->children_[0]->type_ == T_IDENT);//del liumz
    OB_ASSERT(node->children_[0]->type_ == T_RELATION);//add liumz
    OB_ASSERT(node->children_[1]->type_ == T_STAR);

    TableItem* table_item;
    ParseNode* table_node = node->children_[0];
    //add liumz, [multi_database.sql]:20150615
    ObString db_name;
    if (NULL == table_node->children_[0])
    {
      db_name = session_info->get_db_name();
    }
    else
    {
      OB_ASSERT(table_node->children_[0]->type_ == T_IDENT);
      db_name.assign_ptr(
            (char*)(table_node->children_[0]->str_value_),
            static_cast<int32_t>(strlen(table_node->children_[0]->str_value_)));
    }
    //add:e
    ObString table_name;
    table_name.assign_ptr(
          (char*)(table_node->children_[1]->str_value_),
          static_cast<int32_t>(strlen(table_node->children_[1]->str_value_))
          );
    //add liumz, [multi_database.sql]:20150615:b
    ObString db_table_name;
    int64_t buf_len = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1;
    char name_buf[buf_len];
    db_table_name.assign_buffer(name_buf, static_cast<ObString::obstr_size_t>(buf_len));
    if(OB_SUCCESS != (ret = write_db_table_name(db_name, table_name, db_table_name)))
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Table name too long");
    }
    //add:e
    if (ret == OB_SUCCESS)
    {
      //add liumz, [multi_database.sql]:20150615
      if (OB_INVALID_ID == select_stmt->get_ref_table_name(db_table_name, table_name))
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown table %s", table_node->children_[1]->str_value_);
      }
      //add:e
      else if ((select_stmt->get_table_item(table_name, &table_item)) == OB_INVALID_ID)
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown table %s", table_node->children_[1]->str_value_);
      }
    }
    if (ret == OB_SUCCESS)
      ret = resolve_table_columns(result_plan, select_stmt, *table_item);
  }
  else
  {
    /* won't be here */
  }

  return ret;
}

int resolve_select_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node->type_ == T_PROJECT_LIST);
  OB_ASSERT(node->num_child_ >= 1);

  ParseNode* project_node = NULL;
  ParseNode* alias_node = NULL;
  ObString   alias_name;
  ObString   expr_name;
  bool       is_bald_star = false;
  bool       is_real_alias;
  for (int32_t i = 0; ret == OB_SUCCESS &&i < node->num_child_; i++)
  {
    is_real_alias = false;
    expr_name.assign_ptr(
          (char*)(node->children_[i]->str_value_),
          static_cast<int32_t>(strlen(node->children_[i]->str_value_))
          );
    project_node = node->children_[i]->children_[0];
    if (project_node->type_ == T_STAR
        || (project_node->type_ == T_OP_NAME_FIELD
            && project_node->children_[1]->type_ == T_STAR))
    {
      if (project_node->type_ == T_STAR)
      {
        if (is_bald_star)
        {
          ret = OB_ERR_STAR_DUPLICATE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Wrong usage of '*'");
          break;
        }
        else
          is_bald_star = true;
      }

      ret = resolve_star(result_plan, select_stmt, project_node);
      //add lijianqiang [sequence_select_fix] 20151030:b
      /*Exp:if U use sequence with "*" together,we need to align this sequence_array  with out_put_columns */
      int64_t current_select_item_size = select_stmt->get_select_item_size();
      int64_t current_sequence_column_size =select_stmt->get_sequence_select_clause_has_sequence().count();
      for (int64_t i = 0; i < current_select_item_size - current_sequence_column_size; i++)
      {
        if ((ret = select_stmt->set_sequence_select_clause_has_sequence(false)) != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not add (false) to sequence select clause array");
          break;
        }
      }
      //add 20151030:e
      continue;
    }

    if (project_node->type_ == T_ALIAS)
    {
      OB_ASSERT(project_node->num_child_ == 2);
      expr_name.assign_ptr(
            const_cast<char*>(project_node->str_value_),
            static_cast<int32_t>(strlen(project_node->str_value_))
            );
      alias_node = project_node->children_[1];
      project_node = project_node->children_[0];
      is_real_alias = true;

      /* check if the alias name is legal */
      OB_ASSERT(alias_node->type_ == T_IDENT);
      alias_name.assign_ptr(
            (char*)(alias_node->str_value_),
            static_cast<int32_t>(strlen(alias_node->str_value_))
            );
      // Same as mysql, we do not check alias name
      // if (!(select_stmt->check_alias_name(logical_plan, sAlias)))
      // {
      //   TBSYS_LOG(ERROR, "alias name %.s is ambiguous", alias_node->str_value_);
      //   return false;
      // }
    }
    /* it is not real alias name, we just record them for convenience */
    else
    {
      if (project_node->type_ == T_IDENT)
        alias_node = project_node;
      else if (project_node->type_ == T_OP_NAME_FIELD)
      {
        expr_name.assign_ptr(
              const_cast<char*>(project_node->str_value_),
              static_cast<int32_t>(strlen(project_node->str_value_))
              );
        alias_node = project_node->children_[1];
        OB_ASSERT(alias_node->type_ == T_IDENT);
      }

      /* original column name of based table, it has been checked in expression resolve */
      if (alias_node)
        alias_name.assign_ptr(
              (char*)(alias_node->str_value_),
              static_cast<int32_t>(strlen(alias_node->str_value_))
              );
    }

    if (project_node->type_ == T_EXPR_LIST && project_node->num_child_ != 1)
    {
      ret = OB_ERR_RESOLVE_SQL;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Operand should contain 1 column(s)");
      break;
    }
    //add liuzy [sequence select] [JHOBv0.1] 20150525:b
    select_stmt->set_current_expr_has_sequence(false);
    //add 20150525:e
    uint64_t expr_id = OB_INVALID_ID;
    if ((ret = resolve_independ_expr(result_plan, select_stmt, project_node, expr_id)) != OB_SUCCESS)
      break;

    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    ObSqlRawExpr *select_expr = NULL;
    if ((select_expr = logical_plan->get_expr(expr_id)) == NULL)
    {
      ret = OB_ERR_ILLEGAL_ID;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Wrong expr_id");
      break;
    }
    //add liuzy [sequence select] [JHOBv0.1] 20150525:b
    if (OB_SUCCESS == ret && select_stmt->current_expr_has_sequence())//current expr sequence, add single_name_type_pair_ to sequence_name_type_pairs_
    {
      if ((ret = select_stmt->add_sequence_single_name_type_pairs()) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add sequence pair(name, type) to sequence_name_type_pair_");
        break;
      }
      else if ((ret = select_stmt->set_sequence_select_clause_has_sequence(true)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add (true) to sequence select clause array");
        break;
      }
      else
      {
        select_stmt->set_select_clause_has_sequence(true);
        select_stmt->generate_column_has_sequene_count();
        select_stmt->reset_sequence_single_name_type_pair();
        select_stmt->set_current_expr_has_sequence(false);
      }
    }
    else if (ret == OB_SUCCESS && !select_stmt->current_expr_has_sequence())
    {
      if ((ret = select_stmt->set_sequence_select_clause_has_sequence(false)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add (false) to sequence select clause array");
        break;
      }
    }
    //add 20150525:e

    /* if IDENT, we need to assign new id for it to avoid same (table_id, column_id) in ObRowDesc */
    /* 1. select price + off new_price, new_price from tbl; */
    /* 2. select price, price from tbl; */
    /* new_price from 1 and two price from 2 will have new column id after top project operator,
     * so, before this project any operator use none-aliased base column must get its real table id
     * and column id not the ids of the expression
     */
    if (project_node->type_ == T_IDENT || project_node->type_ == T_OP_NAME_FIELD)
    {
      select_expr->set_table_id(OB_INVALID_ID);
      select_expr->set_column_id(logical_plan->generate_column_id());
    }

    /* get table name and column name here*/
    const ObObjType type = select_expr->get_result_type();
    ret = select_stmt->add_select_item(expr_id, is_real_alias, alias_name, expr_name, type);
    if (ret != OB_SUCCESS)
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Add select item error");
      break;
    }

    alias_node = NULL;
    alias_name.assign_ptr(NULL, 0);
  }

  return ret;
}

int resolve_where_clause(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    ret = resolve_and_exprs(
          result_plan,
          stmt,
          node,
          stmt->get_where_exprs(),
          T_WHERE_LIMIT
          );
  }
  return ret;
}

int resolve_group_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;

  /*****************************************************************************
   * The non-aggregate expression of select clause must be expression of group items,
   * but we don't check it here, which is in accordance with mysql.
   * Although there are different values of one group, but the executor only pick the first one
   * E.g.
   * select c1, c2, sum(c3)
   * from tbl
   * group by c1;
   * c2 in select clause is leagal, which is not in standard.
   *****************************************************************************/

  if (ret == OB_SUCCESS && node != NULL)
  {
    OB_ASSERT(node->type_ == T_EXPR_LIST);
    OB_ASSERT(node->num_child_ >= 1);
    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    uint64_t expr_id;
    ParseNode* group_node;
    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      group_node = node->children_[i];
      if (group_node->type_ == T_INT && group_node->value_ >= 0)
      {
        int32_t pos = static_cast<int32_t>(group_node->value_);
        if (pos <= 0 || pos > select_stmt->get_select_item_size())
        {
          ret = OB_ERR_WRONG_POS;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown column '%d' in 'group clause'", pos);
          break;
        }
        expr_id = select_stmt->get_select_item(pos - 1).expr_id_;
        ObSqlRawExpr *sql_expr = logical_plan->get_expr(expr_id);
        if (!sql_expr)
        {
          ret = OB_ERR_ILLEGAL_ID;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not find expression, expr_id = %lu", expr_id);
          break;
        }
        if (sql_expr->is_contain_aggr())
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Invalid use of expression which contains group function");
          break;
        }
      }
      else
      {
        ret = resolve_independ_expr(
              result_plan,
              select_stmt,
              group_node,
              expr_id,
              T_GROUP_LIMIT
              );
      }
      if (ret == OB_SUCCESS)
      {
        if ((ret = select_stmt->add_group_expr(expr_id)) != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Add group expression error");
      }
    }
  }
  return ret;
}

//add liumz, [ROW_NUMBER]20150825:b
int resolve_partition_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;

  if (ret == OB_SUCCESS && node != NULL)
  {
    OB_ASSERT(node->type_ == T_EXPR_LIST);
    OB_ASSERT(node->num_child_ >= 1);
    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    uint64_t expr_id;
    ParseNode* partition_node;
    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      partition_node = node->children_[i];
      if (partition_node->type_ == T_INT && partition_node->value_ >= 0)
      {
        int32_t pos = static_cast<int32_t>(partition_node->value_);
        if (pos <= 0 || pos > select_stmt->get_select_item_size())
        {
          ret = OB_ERR_WRONG_POS;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown column '%d' in 'partition clause'", pos);
          break;
        }
        expr_id = select_stmt->get_select_item(pos - 1).expr_id_;
        ObSqlRawExpr *sql_expr = logical_plan->get_expr(expr_id);
        if (!sql_expr)
        {
          ret = OB_ERR_ILLEGAL_ID;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not find expression, expr_id = %lu", expr_id);
          break;
        }
        if (sql_expr->is_contain_aggr())
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Invalid use of expression which contains partition function");
          break;
        }
      }
      else
      {
        ret = resolve_independ_expr(
              result_plan,
              select_stmt,
              partition_node,
              expr_id,
              T_GROUP_LIMIT
              );
      }
      if (ret == OB_SUCCESS)
      {
        if ((ret = select_stmt->add_partition_expr(expr_id)) != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Add partition expression error");
      }
    }
  }
  return ret;
}

int resolve_order_clause_for_rownum(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_SORT_LIST);

    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      ParseNode* sort_node = node->children_[i];
      OB_ASSERT(sort_node->type_ == T_SORT_KEY);

      OrderItem order_item;
      order_item.order_type_ = OrderItem::ASC;
      if (sort_node->children_[1]->type_ == T_SORT_ASC)
        order_item.order_type_ = OrderItem::ASC;
      else if (sort_node->children_[1]->type_ == T_SORT_DESC)
        order_item.order_type_ = OrderItem::DESC;
      else
      {
        OB_ASSERT(false); /* Won't be here */
      }

      if (sort_node->children_[0]->type_ == T_INT && sort_node->children_[0]->value_ >= 0)
      {
        int32_t pos = static_cast<int32_t>(sort_node->children_[0]->value_);
        if (pos <= 0 || pos > select_stmt->get_select_item_size())
        {
          ret = OB_ERR_WRONG_POS;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown column '%d' in 'group clause'", pos);
          break;
        }
        order_item.expr_id_ = select_stmt->get_select_item(pos - 1).expr_id_;
      }
      else
      {
        ret = resolve_independ_expr(result_plan, select_stmt, sort_node->children_[0], order_item.expr_id_);
      }
      if (ret == OB_SUCCESS)
      {
        if ((ret = select_stmt->add_order_item_for_rownum(order_item)) != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Add order expression for rownum error");
      }
    }
  }
  return ret;
}
//add:e

int resolve_having_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {

    ret = resolve_and_exprs(
          result_plan,
          select_stmt,
          node,
          select_stmt->get_having_exprs(),
          T_HAVING_LIMIT
          );
  }
  return ret;
}

int resolve_order_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_SORT_LIST);

    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      ParseNode* sort_node = node->children_[i];
      OB_ASSERT(sort_node->type_ == T_SORT_KEY);

      OrderItem order_item;
      order_item.order_type_ = OrderItem::ASC;
      if (sort_node->children_[1]->type_ == T_SORT_ASC)
        order_item.order_type_ = OrderItem::ASC;
      else if (sort_node->children_[1]->type_ == T_SORT_DESC)
        order_item.order_type_ = OrderItem::DESC;
      else
      {
        OB_ASSERT(false); /* Won't be here */
      }

      if (sort_node->children_[0]->type_ == T_INT && sort_node->children_[0]->value_ >= 0)
      {
        int32_t pos = static_cast<int32_t>(sort_node->children_[0]->value_);
        if (pos <= 0 || pos > select_stmt->get_select_item_size())
        {
          ret = OB_ERR_WRONG_POS;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown column '%d' in 'group clause'", pos);
          break;
        }
        order_item.expr_id_ = select_stmt->get_select_item(pos - 1).expr_id_;
      }
      else
      {
        ret = resolve_independ_expr(result_plan, select_stmt, sort_node->children_[0], order_item.expr_id_);
      }
      if (ret == OB_SUCCESS)
      {
        if ((ret = select_stmt->add_order_item(order_item)) != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Add order expression error");
      }
    }
  }
  return ret;
}
//add gaojt [ListAgg][JHOBv0.1]20150104:b
/*Exp: the specific function for listagg. Node is the order-by-clause of listagg*/
int resolve_order_clause_for_listagg(ResultPlan * result_plan,
                                     ObSelectStmt* select_stmt,
                                     ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_SORT_LIST);

    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      ParseNode* sort_node = node->children_[i];
      OB_ASSERT(sort_node->type_ == T_SORT_KEY);

      OrderItem order_item;
      order_item.order_type_ = OrderItem::ASC;
      if (sort_node->children_[1]->type_ == T_SORT_ASC)
        order_item.order_type_ = OrderItem::ASC;
      else if (sort_node->children_[1]->type_ == T_SORT_DESC)
        order_item.order_type_ = OrderItem::DESC;
      else
      {
        OB_ASSERT(false); /* Won't be here */
      }
      if (sort_node->children_[0]->type_ == T_INT && sort_node->children_[0]->value_ >= 0)
      {
        int32_t pos = static_cast<int32_t>(sort_node->children_[0]->value_);
        if (pos <= 0 || pos > select_stmt->get_order_item_size())
        {
          ret = OB_ERR_WRONG_POS;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown column '%d' in 'group clause'", pos);
          break;
        }
        order_item.expr_id_ = select_stmt->get_select_item(pos - 1).expr_id_;
      }
      else
      {
        ret = resolve_independ_expr(result_plan, select_stmt, sort_node->children_[0], order_item.expr_id_);
      }
      if (ret == OB_SUCCESS)
      {
        if(false == is_order_for_listagg_exist)
        {
          if ((ret = select_stmt->add_order_item_for_listagg(order_item)) != OB_SUCCESS)
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Add order expression for listagg error");
        }
      }
    }
  }
  is_order_for_listagg_exist = true;
  return ret;
}
//add 20150104:e
int resolve_limit_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{

  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(result_plan != NULL);
    OB_ASSERT(node->type_ == T_LIMIT_CLAUSE);

    ParseNode* limit_node = node->children_[0];
    ParseNode* offset_node = node->children_[1];
    OB_ASSERT(limit_node != NULL || offset_node != NULL);
    uint64_t limit_count = OB_INVALID_ID;
    uint64_t limit_offset = OB_INVALID_ID;

    // resolve the question mark with less value first
    if (limit_node != NULL && limit_node->type_ == T_QUESTIONMARK
        && offset_node != NULL && offset_node->type_ == T_QUESTIONMARK
        && limit_node->value_ > offset_node->value_)
    {
      if ((ret = resolve_independ_expr(result_plan, select_stmt, offset_node, limit_offset)) != OB_SUCCESS
          || (ret = resolve_independ_expr(result_plan, select_stmt, limit_node, limit_count)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Resolve limit/offset error, ret=%d", ret);
      }
    }
    else
    {
      if (ret == OB_SUCCESS && limit_node != NULL)
      {
        if (limit_node->type_ != T_INT && limit_node->type_ != T_QUESTIONMARK)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Wrong type of limit value");
        }
        else if ((ret = resolve_independ_expr(result_plan, select_stmt, limit_node, limit_count)) != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Resolve limit error, ret=%d", ret);
        }
      }
      if (ret == OB_SUCCESS && offset_node != NULL)
      {
        if (offset_node->type_ != T_INT && offset_node->type_ != T_QUESTIONMARK)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Wrong type of limit value");
        }
        else if ((ret = resolve_independ_expr(result_plan, select_stmt, offset_node, limit_offset)) != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Resolve offset error, ret=%d", ret);
        }
      }
    }
    if (ret == OB_SUCCESS)
    {
      select_stmt->set_limit_offset(limit_count, limit_offset);
    }
  }
  return ret;
}

int resolve_for_update_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_BOOL);
    if (node->value_ == 1)
      select_stmt->set_for_update(true);
  }
  return ret;
}

int resolve_select_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  //mod tianz [EXPORT_TOOL] 20141120:b
  //OB_ASSERT(node && node->num_child_ >= 15);
  OB_ASSERT(node && node->num_child_ >= 16);
  //mod 20141120:e
  query_id = OB_INVALID_ID;

  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObLogicalPlan* logical_plan = NULL;
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  ObSelectStmt* select_stmt = NULL;
  if (ret == OB_SUCCESS)
  {
    select_stmt = (ObSelectStmt*)parse_malloc(sizeof(ObSelectStmt), result_plan->name_pool_);
    if (select_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObSelectStmt");
    }
  }

  if (ret == OB_SUCCESS)
  {
    select_stmt = new(select_stmt) ObSelectStmt(name_pool);
    query_id = logical_plan->generate_query_id();
    select_stmt->set_query_id(query_id);
    ret = logical_plan->add_query(select_stmt);
    if (ret != OB_SUCCESS)
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not add ObSelectStmt to logical plan");
    }
  }

  /* -----------------------------------------------------------------
     * The later resolve may need some infomation resolved by the former one,
     * so please follow the resolving orders:
     *
     * 1. set clause
     * 2. from clause
     * 3. select clause
     * 4. where clause
     * 5. group by clause
     * 6. having clause
     * 7. order by clause
     * 8. limit clause
     * -----------------------------------------------------------------
     */

  /* resolve set clause */
  if (node->children_[6] != NULL)
  {
    OB_ASSERT(node->children_[8] != NULL);
    OB_ASSERT(node->children_[9] != NULL);

    if (node->children_[12] && node->children_[12]->value_ == 1)
    {
      ret = OB_ERR_ILLEGAL_ID;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Select for update statement can not be processed in set query");
    }

    // assign set type
    if (ret == OB_SUCCESS)
    {
      switch (node->children_[6]->type_)
      {
        case T_SET_UNION:
          select_stmt->assign_set_op(ObSelectStmt::UNION);
          break;
        case T_SET_INTERSECT:
          select_stmt->assign_set_op(ObSelectStmt::INTERSECT);
          break;
        case T_SET_EXCEPT:
          select_stmt->assign_set_op(ObSelectStmt::EXCEPT);
          break;
        default:
          ret = OB_ERR_OPERATOR_UNKNOWN;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "unknown set operator of set clause");
          break;
      }
    }

    // assign first query
    uint64_t sub_query_id = OB_INVALID_ID;
    if (ret == OB_SUCCESS)
    {
      if (node->children_[7] == NULL || node->children_[7]->type_ == T_DISTINCT)
      {
        select_stmt->assign_set_distinct();
      }
      else
      {
        select_stmt->assign_set_all();
      }
      ret = resolve_select_stmt(result_plan, node->children_[8], sub_query_id);
      if (ret == OB_SUCCESS)
        select_stmt->assign_left_query_id(sub_query_id);
    }
    // assign second query
    if (ret == OB_SUCCESS)
    {
      ret = resolve_select_stmt(result_plan, node->children_[9], sub_query_id);
      if (ret == OB_SUCCESS)
        select_stmt->assign_right_query_id(sub_query_id);
    }

    // check if columns number ars match
    if (ret == OB_SUCCESS)
    {
      ObSelectStmt* left_select = logical_plan->get_select_query(select_stmt->get_left_query_id());
      ObSelectStmt* right_select = logical_plan->get_select_query(select_stmt->get_right_query_id());
      if (!left_select || !right_select)
      {
        ret = OB_ERR_ILLEGAL_ID;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "resolve set clause error");
      }
      else if(left_select->get_select_item_size() != right_select->get_select_item_size())
      {
        ret = OB_ERR_COLUMN_SIZE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "The used SELECT statements have a different number of columns");
      }
      else
      {
        //add qianzm [set_operation] 20151222 :b
        ObExprObj tmp_expr;
        oceanbase::common::ObArray<oceanbase::common::ObObjType> tmp_result_columns_type;//add qianzm [set_operation] 20160115
        for (int32_t i = 0; i < left_select->get_select_item_size(); i ++)
        {
          ObObjType left_type, right_type, res_type;
          left_type = left_select->get_select_item(i).type_;
          right_type = right_select->get_select_item(i).type_;
          tmp_expr.do_type_promotion(left_type, right_type, res_type);
          if ((ObVarcharType == left_type && right_type == ObIntType)
              || (left_type == ObIntType && right_type == ObVarcharType))
          {
            res_type = ObDecimalType;
          }
          else if ((left_type == ObVarcharType && right_type == ObInt32Type)
                   || (left_type == ObInt32Type && right_type == ObVarcharType))
          {
            res_type = ObDecimalType;
          }
          else if ((left_type == ObVarcharType && right_type == ObFloatType)
                   || (left_type == ObFloatType && right_type == ObVarcharType))
          {
            res_type = ObDecimalType;
          }
          else if ((left_type == ObVarcharType && right_type == ObDoubleType)
                   || (left_type == ObDoubleType && right_type == ObVarcharType))
          {
            res_type = ObDecimalType;
          }
          else if ((left_type == ObVarcharType && right_type == ObDateType)
                   || (left_type == ObDateType && right_type == ObVarcharType))
          {
            res_type = ObDateType;
          }
          //add qianzm [set_operation] 20160107:b
          else if (left_type == ObNullType)
          {
              res_type = right_type;
          }
          else if (right_type == ObNullType)
          {
              res_type = left_type;
          }
          //add 20160107:e
          if (ObMaxType == res_type || ObMinType == res_type  )
          {
            TBSYS_LOG(ERROR,"columns[%d] type are not compatible", i + 1);
            ret = OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "The data types of corresponding columns are not compatible");
            break;
          }
          ObSqlRawExpr* sql_expr = NULL;
          sql_expr = logical_plan->get_expr(left_select->get_select_item(i).expr_id_);
          sql_expr->set_result_type(res_type);
          //add :e
          //del qianzm [set_operation] 20160115:b
          //select_stmt->get_result_type_array().push_back(res_type);
          //del 20160115:e
          tmp_result_columns_type.push_back(res_type);//add qianzm [set_operation] 20160115
        }
        if (OB_SUCCESS == ret)
        {
          select_stmt->add_result_type_array_for_setop(tmp_result_columns_type);//add qianzm [set_operation] 20160115
          //ret = select_stmt->copy_select_items(left_select);
          ret = select_stmt->copy_select_items_v2(left_select, logical_plan);//mod qianzm [set_operation] 20151222
        }
        //add peiouya [IN_TYPEBUG_FIX] 20151225:b
        if (OB_SUCCESS == ret)
        {
          oceanbase::common::ObArray<oceanbase::common::ObObjType> last_output_columns_dsttype;  //save promote type ,accoring to right_data_type and left_data_type
          ObObjType left_type = ObMaxType;
          ObObjType right_type = ObMaxType;
          ObObjType res_type = ObMaxType;
          for (int32_t idx = 0; OB_SUCCESS == ret && idx < left_select->get_select_item_size(); idx ++)
          {
            left_type = logical_plan->get_expr (left_select->get_select_item (idx).expr_id_)->get_result_type ();
            right_type = logical_plan->get_expr (right_select->get_select_item (idx).expr_id_)->get_result_type ();
            res_type = ObExprObj::type_promotion_for_in_op (left_type, right_type);
            if (ObMaxType == res_type)
            {
              ret = OB_NOT_SUPPORTED;
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "%s can not compare with %s ",
                        oceanbase::common::ObObj::get_sql_type(left_type),
                        oceanbase::common::ObObj::get_sql_type(right_type));
              break;
            }
            else
            {
              last_output_columns_dsttype.push_back (res_type);
            }
          }
          if (OB_SUCCESS == ret)
          {
            ret = select_stmt->add_dsttype_for_output_columns(last_output_columns_dsttype);
          }
        }
        //add 20151225:e
      }
    }
  }
  else
  {
    /* normal select */
    select_stmt->assign_set_op(ObSelectStmt::NONE);

    if (node->children_[0] == NULL || node->children_[0]->type_ == T_ALL)
    {
      ret = OB_ERR_ILLEGAL_ID;
      select_stmt->assign_all();
    }
    else
    {
      select_stmt->assign_distinct();
    }

    /* resolve from clause */
    if ((ret = resolve_from_clause(result_plan, select_stmt, node->children_[2])) == OB_SUCCESS)
    {
    }
    if (ret == OB_SUCCESS && node->children_[12] && node->children_[12]->value_ == 1)
    {
      if (select_stmt->get_table_size() > 1)
      {
        ret = OB_ERR_ILLEGAL_ID;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Select for update statement can not process more than one table");
      }
      else if (select_stmt->get_table_size() > 0)
      {
        TableItem& table_item = select_stmt->get_table_item(0);
        uint64_t table_id = table_item.table_id_;
        ret = add_all_rowkey_columns_to_stmt(result_plan, table_id, select_stmt);
      }
    }
    /* resolve select clause */
    /* resolve where clause */
    /* resolve group by clause */
    /* resolve having clause */
    if (ret == OB_SUCCESS
        && (ret = resolve_select_clause(result_plan, select_stmt, node->children_[1]))
        == OB_SUCCESS
        && (ret = resolve_where_clause(result_plan, select_stmt, node->children_[3]))
        == OB_SUCCESS
        && (ret = resolve_group_clause(result_plan, select_stmt, node->children_[4]))
        == OB_SUCCESS
        && (ret = resolve_having_clause(result_plan, select_stmt, node->children_[5]))
        == OB_SUCCESS
        )
    {
      ;
    }

    //add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140409:b
    if (ret == OB_SUCCESS && select_stmt->get_joined_table_size()>0)
    {
      ret = optimize_joins_filter(result_plan, select_stmt);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR,"join filter ERROR!");
      }
    }
    //add 20140409:e
  }

  /* resolve order by clause */
  /* resolve limit clause */
  if (ret == OB_SUCCESS
      && (ret = resolve_order_clause(result_plan, select_stmt, node->children_[10]))
      == OB_SUCCESS
      && (ret = resolve_limit_clause(result_plan, select_stmt, node->children_[11]))
      == OB_SUCCESS
      && (ret = resolve_for_update_clause(result_plan, select_stmt, node->children_[12]))
      == OB_SUCCESS
      )
  {
    ;
  }

  // In some cases, some table(s) may have none column mentioned,
  // considerating the optimization, not all columns needed, we only need to scan out one of its columns
  // Example:
  // 1. select count(*) from t1;
  // 2. select t1.c1 from t1, t2;
  if (ret == OB_SUCCESS)
  {
    if (select_stmt->get_select_item_size() <= 0 && select_stmt->get_table_size() <= 0)
    {
      ret = OB_ERR_RESOLVE_SQL;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "No tables used");
    }
    for (int32_t i = 0; ret == OB_SUCCESS && i < select_stmt->get_table_size(); i++)
    {
      TableItem& table_item = select_stmt->get_table_item(i);
      if (!table_item.has_scan_columns_)
        ret = resolve_table_columns(result_plan, select_stmt, table_item, 1);
    }
  }

  if (ret == OB_SUCCESS && node->children_[13])
  {
    ret = resolve_hints(result_plan, select_stmt, node->children_[13]);
  }
  if (ret == OB_SUCCESS && node->children_[14])
  {
    ret = resolve_when_clause(result_plan, select_stmt, node->children_[14]);
  }
  //add tianz [EXPORT_TOOL] 20141120:b
  if (ret == OB_SUCCESS && node->children_[15])
  {
    select_stmt->set_has_range(true);
    ret = resolve_range_values(result_plan,select_stmt,node->children_[15]);
  }
  //add 20141120:e

  is_order_for_listagg_exist = false;//add gaojt [ListAgg][JHOBv0.1]20150126
  return ret;
}

int resolve_hints(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    ObQueryHint& query_hint = stmt->get_query_hint();
    OB_ASSERT(node->type_ == T_HINT_OPTION_LIST);
    for (int32_t i = 0; i < node->num_child_; i++)
    {
      ParseNode* hint_node = node->children_[i];
      if (!hint_node)
        continue;
      switch (hint_node->type_)
      {
        case T_READ_STATIC:
          query_hint.read_consistency_ = STATIC;
          break;
          //add zhuyanchao secondary index 20150708
        case T_NOT_USE_INDEX:
          query_hint.not_use_index_ = true;
          break;
        case T_HOTSPOT:
          query_hint.hotspot_= true;
          break;
          // add by hushuang [bloomfilter_join] 20150413:b
        case T_JOIN_LIST:
          ret = generate_join_hint (result_plan, stmt, hint_node);
          break;
          //add 20150413:e
        case T_READ_CONSISTENCY:
          if (hint_node->value_ == 1)
          {
            query_hint.read_consistency_ = STATIC;
          }
          else if (hint_node->value_ == 2)
          {
            query_hint.read_consistency_ = FROZEN;
          }
          else if (hint_node->value_ == 3)
          {
            query_hint.read_consistency_ = WEAK;
          }
          else if (hint_node->value_ == 4)
          {
            query_hint.read_consistency_ = STRONG;
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "unknown hint value, ret=%d", ret);
          }
          break;
          // add by zcd 20141216:b
        case T_USE_INDEX:
          ret = generate_index_hint(result_plan, stmt, hint_node);
          break;
          // add 20141216:e
          // add by zcd 20150601:b
        //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20151213:b
        case T_I_MUTLI_BATCH:
          query_hint.is_insert_multi_batch_ = true;
          break;
        //add gaojt 20151213:e
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302:b
        case T_UD_MUTLI_BATCH:
          query_hint.is_delete_update_multi_batch_ = true;
          break;
        //add gaojt 20160302:e
          //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418:b
          /*expr: used to indicate that the procedure of delete/update stmt is the original one */
        case T_UD_ALL_ROWKEY:
          query_hint.is_all_rowkey_ = true;
          break;
          //add gaojt 20160418:e
        case T_UD_NOT_PARALLAL:
          query_hint.is_parallal_ = false;
          break;
          //add maosy [Delete_Update_Function] [JHOBv0.1] 20151103:b
          case T_CHANGE_VALUE_SIZE:
              query_hint.change_value_size_= true;
          break;
              // add e
        case T_UNKOWN_HINT:
          break;
          // add by zcd 20150601:e
        default:
          ret = OB_ERR_HINT_UNKNOWN;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown hint '%s'", get_type_name(hint_node->type_));
          break;
      }
    }
  }
  return ret;
}

// add by zcd 20141222:b
// 根据索引表id和索引表名称生成对应的表名
// index_name为索引的名称
// src_tid为原表的tableid
// out_buff为结果名称的保存位置
// buf_size为out_buff的最大长度
// str_len为结果名称的长度
// 拼接规则：__tid__idx__indexname
int generate_index_table_name(const char* index_name, uint64_t src_tid, char *out_buff, size_t buf_size, int64_t& str_len)
{
  int ret = OB_SUCCESS;
  int wlen = snprintf(out_buff, buf_size, "__%ld__idx__%s", src_tid, index_name);
  if((size_t)wlen > buf_size)
  {
    ret = OB_ERROR;
  }
  str_len = wlen;
  return ret;
}
// add :e

// add by zcd 20141217:b
int generate_index_hint(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* hint_node)
{

  int ret = OB_SUCCESS;
  /* 解析在hint中强制使用index的hint，根据表名来获取表的ID */
  ObIndexTableNamePair pair;
  ObQueryHint& query_hint = stmt->get_query_hint();
  //add zhuyanchao secondary index hint bug20150713
  ObString db_name;
  char t_tablename[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 2] = {0};
  char t_index_name[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 2] = {0};
  int64_t pos = 0;
  int64_t index_pos = 0;
  //add e
  // 单表查询的情况下不应该有多个index的hint
  //remove zhuyanchao multi hint
  /*if(query_hint.use_index_array_.size() >= 1)
  {
    ret = OB_ERR_UNEXPECTED;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "too much index hint");
    TBSYS_LOG(ERROR, "too much index hint, ret=[%d]", ret);
    return ret;
  }
 */
  OB_ASSERT(2 == hint_node->num_child_);

  ObSchemaChecker* schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
  if (schema_checker == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Schema(s) are not set");
    return ret;
  }
  ObSQLSessionInfo *session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);

  //add zhuyanchao secondary index
  if(hint_node->children_[0]->children_[0] != NULL)
    db_name.assign_ptr((char*)(hint_node->children_[0]->children_[0]->str_value_),
                       static_cast<int32_t>(strlen(hint_node->children_[0]->children_[0]->str_value_)));

  if(db_name.length() <= 0)
  {
    if(session_info == NULL)
    {
      ret = OB_ERROR;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Session_info(s) are not set");
      return ret;
    }
    else
    {
      db_name = static_cast<ObSQLSessionInfo*>(result_plan->session_info_)->get_db_name();
    }
  }
  if(db_name.length() <= 0 || db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)//mod liumz [name_length]
  {
    ret = OB_INVALID_ARGUMENT;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "database name is not valid,you must specify the correct database name when create table.");
  }
  //add e
  const ObTableSchema *src_table_schema = NULL;
  const ObTableSchema *index_table_schema = NULL;
  //add zhuyanchao
  databuff_printf(t_tablename,OB_MAX_TABLE_NAME_LENGTH,pos,"%.*s.%s",db_name.length(),db_name.ptr(),hint_node->children_[0]->children_[1]->str_value_);
  // 查找原表的table_id
  //add e
  if(NULL != (src_table_schema = schema_checker->get_table_schema(t_tablename)))
  {
    pair.src_table_name_.write(t_tablename,
                               (ObString::obstr_size_t)strlen(t_tablename));
    pair.src_table_id_ = src_table_schema->get_table_id();
  }
  else
  {
    ret = OB_ERR_UNEXPECTED;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "unknown table name '%s'", hint_node->children_[0]->children_[1]->str_value_);
    TBSYS_LOG(ERROR, "unknown table name '%s', ret=[%d]", hint_node->children_[0]->children_[1]->str_value_, ret);
    return ret;
  }
  // 查找索引表的table_id，判断其是否是索引表，如果是则判断此索引表的原表是否与上面的原表是一致的
  char tablename[OB_MAX_TABLE_NAME_LENGTH];
  int64_t len = 0;
  generate_index_table_name(hint_node->children_[1]->children_[1]->str_value_,
                            src_table_schema->get_table_id(),
                            tablename, OB_MAX_TABLE_NAME_LENGTH, len);
  //add zhuyanchao secondary index
  databuff_printf(t_index_name,OB_MAX_TABLE_NAME_LENGTH,index_pos,"%.*s.%s",db_name.length(),db_name.ptr(),tablename);
  //add e
  if(NULL != (index_table_schema = schema_checker->get_table_schema(t_index_name)))
  {
    pair.index_table_name_.write(t_index_name, (ObString::obstr_size_t)strlen(t_index_name));
    pair.index_table_id_ = index_table_schema->get_table_id();

    uint64_t index_src_tid = index_table_schema->get_index_helper().tbl_tid;
    if(OB_INVALID_ID == index_src_tid || pair.src_table_id_ != index_src_tid)
    {
      ret = OB_ERR_UNEXPECTED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "unknown index name '%s' of '%s'",
               hint_node->children_[1]->children_[1]->str_value_, hint_node->children_[0]->children_[1]->str_value_);
      TBSYS_LOG(ERROR, "unknown index name '%s' of '%s', index table name '%s', ret=[%d]",
                hint_node->children_[1]->children_[1]->str_value_, hint_node->children_[0]->children_[1]->str_value_, tablename, ret);
      return ret;
    }
  }
  else
  {
    ret = OB_ERR_UNEXPECTED;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "unknown index name '%s'",
             hint_node->children_[1]->children_[1]->str_value_);
    TBSYS_LOG(ERROR, "unknown index name '%s', index table name '%s', ret=[%d]",
              hint_node->children_[1]->children_[1]->str_value_, tablename, ret);
    return ret;
  }

  // 将index的hint加入到数组中
  query_hint.use_index_array_.push_back(pair);
  return ret;
}
// add:e
int generate_join_hint(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* hint_node)
{
  int ret = OB_SUCCESS;
  ObJoinTypeArray join_node;
  ObQueryHint& query_hint = stmt->get_query_hint();

  if(query_hint.join_array_.size() >= 1)
  {
    ret = OB_ERR_UNEXPECTED;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "too much join hint");
    TBSYS_LOG(ERROR, "too much join hint, ret=[%d]", ret);

  }

  for( int32_t i = 0; OB_SUCCESS == ret && i < hint_node->num_child_; i++ )
  {
    join_node.join_type_ = hint_node->children_[i]->type_ ;
    join_node.index_ = i;
    query_hint.join_array_.push_back(join_node);
  }

  return ret;
}
//add:e

//add lijianqiang [sequence update] 20150912:b
int add_all_row_key_expr_ids(
    ResultPlan* result_plan,
    uint64_t table_id,
    ObUpdateStmt *update_stmt)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  const ObTableSchema* table_schema = NULL;
  ObRowkeyInfo rowkey_info;
  int64_t rowkey_idx = 0;
  if (NULL == update_stmt || NULL == result_plan)
  {
    TBSYS_LOG(WARN, "invalid argument. update_stmt=%p, result_plan=%p", update_stmt, result_plan);
    ret = OB_INVALID_ARGUMENT;
  }

  if (ret == OB_SUCCESS)
  {
    if (NULL == (schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_)))
    {
      ret = OB_ERR_SCHEMA_UNSET;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Schema(s) are not set");
    }
    else if (NULL == (table_schema = schema_checker->get_table_schema(table_id)))
    {
      ret = OB_ERR_TABLE_UNKNOWN;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Table schema not found");
    }
    else
    {
      rowkey_info = table_schema->get_rowkey_info();
      for (rowkey_idx = 0; rowkey_idx < rowkey_info.get_size(); rowkey_idx++)
      {
        update_stmt->add_sequence_expr_id(0);
      }
    }
  }
  return ret;
}
//add 20150912:e
//mod tianz [EXPORT_TOOL] 20141120:b
int resolve_range_values(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_RANGE);
    OB_ASSERT(node->num_child_ == 2);

    select_stmt->set_values_size(node->num_child_);

    ObArray<uint64_t> value_row;
    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      ParseNode* vector_node = node->children_[i];
      if (NULL == vector_node)
      {
        if (i == 0)
        {
          select_stmt->set_start_is_min();
        }
        else
        {
          select_stmt->set_end_is_max();
        }
        continue;
      }
      uint64_t expr_id;
      for (int32_t j = 0; ret == OB_SUCCESS && j < vector_node->num_child_; j++)
      {
        ret = resolve_independ_expr(result_plan, select_stmt, vector_node->children_[j],
                                    expr_id, T_INSERT_LIMIT);
        if (ret == OB_SUCCESS && (ret = value_row.push_back(expr_id)) != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not add expr_id to ObArray");
        }
      }

      if (ret == OB_SUCCESS)
      {
        if ((ret = select_stmt->add_value_row(value_row)) != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Add value-row to ObInsertStmt error");
      }
      value_row.clear();
    }
  }

  if (select_stmt->start_is_min() && select_stmt->end_is_max())
  {
    select_stmt->set_has_range(false);
  }
  return ret;
}
//add 20141120:e
int resolve_delete_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  OB_ASSERT(node && node->type_ == T_DELETE && node->num_child_ >= 3);
  query_id = OB_INVALID_ID;

  ObLogicalPlan* logical_plan = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    ObDeleteStmt* delete_stmt = (ObDeleteStmt*)parse_malloc(sizeof(ObDeleteStmt), result_plan->name_pool_);
    if (delete_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObDeleteStmt");
    }
    else
    {
      delete_stmt = new(delete_stmt) ObDeleteStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      delete_stmt->set_query_id(query_id);
      ret = logical_plan->add_query(delete_stmt);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add ObDeleteStmt to logical plan");
      }
      else
      {
        ParseNode* table_node = node->children_[0];
        //if (table_node->type_ != T_IDENT)
        if (table_node->type_ != T_RELATION)//mod liumz
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Only single base table is supported for delete");
        }
        if (ret == OB_SUCCESS)
        {
          ret = resolve_table(result_plan, delete_stmt, table_node, table_id);
        }
        if (ret == OB_SUCCESS)
        {
          ret = add_all_rowkey_columns_to_stmt(result_plan, table_id, delete_stmt);
        }
        if (ret == OB_SUCCESS)
        {
          delete_stmt->set_delete_table(table_id);
          ret = resolve_where_clause(result_plan, delete_stmt, node->children_[1]);
        }
        if (ret == OB_SUCCESS && node->children_[2])
        {
          ret = resolve_when_clause(result_plan, delete_stmt, node->children_[2]);
        }
    //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302:b
        if (ret == OB_SUCCESS && node->children_[3])
        {
          ret = resolve_hints(result_plan, delete_stmt, node->children_[3]);
        }
        if(OB_SUCCESS == ret)
        {
            if(true == delete_stmt->get_query_hint().is_delete_update_multi_batch_)
            {
                delete_stmt->set_is_delete_multi_batch(true);
            }
            if(true == delete_stmt->get_query_hint().is_all_rowkey_)
            {
                delete_stmt->set_is_delete_all_rowkey(true);
            }
            delete_stmt->set_ud_where_parse_tree(node->children_[1]);
            node->children_[1] = NULL;
        }
        //add gaojt 20160302:e
      }
    }
  }
  return ret;
}

int resolve_insert_columns(
    ResultPlan * result_plan,
    ObInsertStmt* insert_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_COLUMN_LIST);
    ColumnItem* column_item = NULL;
    ParseNode* column_node = NULL;
    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      column_node = node->children_[i];
      OB_ASSERT(column_node->type_ == T_IDENT);

      ObString column_name;
      column_name.assign_ptr(
            (char*)(column_node->str_value_),
            static_cast<int32_t>(strlen(column_node->str_value_))
            );
      column_item = insert_stmt->get_column_item(NULL, column_name);
      if (column_item == NULL)
      {
        if ((ret = insert_stmt->add_column_item(*result_plan, column_name)) != OB_SUCCESS)
          break;
      }
      else
      {
        ret = OB_ERR_COLUMN_DUPLICATE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Column %s are duplicate", column_node->str_value_);
        break;
      }
    }
  }
  else
  {
    if (insert_stmt->get_table_size() != 1)
    {
      ret = OB_ERR_PARSER_SYNTAX;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Insert statement only support one table");
    }
    if (ret == OB_SUCCESS)
    {
      TableItem& table_item = insert_stmt->get_table_item(0);
      if (table_item.type_ != TableItem::BASE_TABLE)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Only base table can be inserted");
      }
      else
        ret = resolve_table_columns(result_plan, insert_stmt, table_item);
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int32_t i=0;OB_SUCCESS == ret && i<insert_stmt->get_column_size();i++)
    {
      const ColumnItem* column_item = insert_stmt->get_column_item(i);
      if (NULL != column_item && column_item->table_id_ != OB_INVALID_ID)
      {
        ObSchemaChecker* schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
        if (schema_checker == NULL)
        {
          ret = OB_ERR_SCHEMA_UNSET;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Schema(s) are not set");
          break;
        }

        if (schema_checker->is_join_column(column_item->table_id_, column_item->column_id_))
        {
          ret = OB_ERR_INSERT_INNER_JOIN_COLUMN;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Cannot insert inner join column: %.*s", column_item->column_name_.length(), column_item->column_name_.ptr());
          break;
        }
      }
    }
  }
  return ret;
}

int resolve_insert_values(
    ResultPlan * result_plan,
    ObInsertStmt* insert_stmt,
    ParseNode* node)
{
  OB_ASSERT(node->type_ == T_VALUE_LIST);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;

  insert_stmt->set_values_size(node->num_child_);
  ObArray<uint64_t> value_row;

  //add dragon [varchar limit] 2016-8-10 10:14:45
  //获取表名后面跟随着的列信息
  ObArray<uint64_t> col_id; //记录列的id
  uint64_t table_id = insert_stmt->get_table_id();
  const ColumnItem* col_item;
  for (int32_t i = 0; ret == OB_SUCCESS && i < insert_stmt->get_column_size(); i++)
  {
    if(NULL == (col_item = insert_stmt->get_column_item(i)))
    {
      TBSYS_LOG(WARN, "column item not found!index[%d]", i);
      ret = OB_ERR_STMT_COL_NOT_FOUND;
    }
    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = col_id.push_back(col_item->column_id_)))
      {
        TBSYS_LOG(WARN, "array push back failed! column_id_ = %ld", col_item->column_id_);
      }
    }
  }
  //add e

  //node->num_child_: values后面()的对数
  for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
  {
    ParseNode* vector_node = node->children_[i];
    uint64_t expr_id;
    //vector_node->num_child_: values后面()中变量的个数
    for (int32_t j = 0; ret == OB_SUCCESS && j < vector_node->num_child_; j++)
    {
      //add lijianqiang [sequence] 20150402:b
      insert_stmt->set_column_num_sum();
      insert_stmt->set_current_expr_has_sequence(false);
      //add 20150402:e

      //mod dragon [varchar limit] 2016-8-10 10:51:48
      //new: 将cid作为解析表达式的参数
      int64_t cid = col_id.at (j);
      ObColumnInfo col_info;
      col_info.table_id_ = table_id;
      col_info.column_id_ = cid;
      ret = resolve_independ_expr(result_plan, insert_stmt, vector_node->children_[j],
                                  expr_id, T_INSERT_LIMIT, col_info);
      /*----- old -----
      ret = resolve_independ_expr(result_plan, insert_stmt, vector_node->children_[j],
                                  expr_id, T_INSERT_LIMIT);
      ------old--------*/
      //mod 2016-8-10e

      if (ret == OB_SUCCESS && (ret = value_row.push_back(expr_id)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add expr_id to ObArray");
      }
      //add lijianqiang [sequence] 20150402 :b
      if (insert_stmt->current_expr_has_sequence())//有sequence，将构造好的信息存储起来
      {
        if (ret == OB_SUCCESS && (ret = insert_stmt->add_next_prev_ids()))
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not add next_pre_ids to ObArray");
        }
        insert_stmt->set_current_expr_has_sequence(false);//重置，下次使用
        insert_stmt->reset_column_sequence_types();
      }
      //add 20150402:e
    }
    if (ret == OB_SUCCESS &&
        insert_stmt->get_column_size() != value_row.count())
    {
      ret = OB_ERR_COLUMN_SIZE;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Column count doesn't match value count");
    }
    if (ret == OB_SUCCESS)
    {
      if ((ret = insert_stmt->add_value_row(value_row)) != OB_SUCCESS)
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Add value-row to ObInsertStmt error");
    }
    value_row.clear();
  }
  //add lijianqiang [sequence] 20150402:b
  insert_stmt->complete_the_sequence_id_vectors();
  //  insert_stmt->print_info();//for test
  //add 20150402:e
  return ret;
}

int resolve_insert_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  OB_ASSERT(node && node->type_ == T_INSERT && node->num_child_ >= 6);
  query_id = OB_INVALID_ID;

  ObLogicalPlan* logical_plan = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {

    ObInsertStmt* insert_stmt = (ObInsertStmt*)parse_malloc(sizeof(ObInsertStmt), result_plan->name_pool_);
    if (insert_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObInsertStmt");
    }
    else
    {
      insert_stmt = new(insert_stmt) ObInsertStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      insert_stmt->set_query_id(query_id);
      ret = logical_plan->add_query(insert_stmt);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add ObInsertStmt to logical plan");
      }
      else
      {
        ParseNode* table_node = node->children_[0];
        //if (table_node->type_ != T_IDENT)
        if (table_node->type_ != T_RELATION)//mod liumz
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Only single base table is supported for insert");
        }
        if (ret == OB_SUCCESS)
          ret = resolve_table(result_plan, insert_stmt, table_node, table_id);
        if (ret == OB_SUCCESS)
        {
          insert_stmt->set_insert_table(table_id);
          ret = resolve_insert_columns(result_plan, insert_stmt, node->children_[1]);
        }
        if (ret == OB_SUCCESS)
        {
          // value list
          if (node->children_[2])
          {
            ret = resolve_insert_values(result_plan, insert_stmt, node->children_[2]);
          }
          else
          {
            // value from sub-query(insert into table select ..)
            OB_ASSERT(node->children_[3] && node->children_[3]->type_ == T_SELECT);
            uint64_t ref_id = OB_INVALID_ID;
            ret = resolve_select_stmt(result_plan, node->children_[3], ref_id);
            if (ret == OB_SUCCESS)
            {
              insert_stmt->set_insert_query(ref_id);
              ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
              if (select_stmt == NULL)
              {
                ret = OB_ERR_ILLEGAL_ID;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "Invalid query id of sub-query");
              }
              if (ret == OB_SUCCESS &&
                  insert_stmt->get_column_size() != select_stmt->get_select_item_size())
              {
                ret = OB_ERR_COLUMN_SIZE;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "select values are not match insert columns");
              }
              //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507:b
              if(OB_SUCCESS == ret)
              {
                if(true == select_stmt->get_query_hint().is_insert_multi_batch_)
                {
                  insert_stmt->set_is_insert_multi_batch(true);
                }
              }
              //add gaojt 20150507:e
            }
          }
        }
        if (ret == OB_SUCCESS)
        {
          OB_ASSERT(node->children_[4] && node->children_[4]->type_ == T_BOOL);
          if (node->children_[4]->value_ == 1)
            insert_stmt->set_replace(true);
          else
            insert_stmt->set_replace(false);
        }
        if (ret == OB_SUCCESS && node->children_[5])
        {
          ret = resolve_when_clause(result_plan, insert_stmt, node->children_[5]);
        }
      }
    }
  }
  return ret;
}


//add liuzy [sequence] [JHOBv0.1] 20150327:b
int resolve_sequence_create_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(NULL != node && T_SEQUENCE_CREATE == node->type_ && 3 == node->num_child_);
  query_id = OB_INVALID_ID;
  uint64_t expr_id;
  ObArray<uint64_t> value_row;
  uint64_t table_id = OB_INVALID_ID;
  ObLogicalPlan* logical_plan = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (NULL == result_plan->plan_tree_)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (NULL == logical_plan)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (OB_SUCCESS == ret)
  {
    ObInsertStmt* insert_stmt = (ObInsertStmt*)parse_malloc(sizeof(ObInsertStmt), result_plan->name_pool_);
    ObSequenceStmt* seq_create_stmt = (ObSequenceStmt*)parse_malloc(sizeof(ObSequenceStmt), result_plan->name_pool_);
    if (NULL == insert_stmt)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(ERROR, "out of memory for insert_stmt");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObInsertStmt");
    }
    else if (NULL == seq_create_stmt)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(ERROR, "out of memory for seq_create_stmt");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObSequenceStmt");
    }
    else
    {
      insert_stmt = new(insert_stmt) ObInsertStmt(name_pool);
      seq_create_stmt = new(seq_create_stmt) ObSequenceStmt();
      query_id = logical_plan->generate_query_id();
      insert_stmt->set_query_id(query_id);
      if (OB_SUCCESS != (ret = logical_plan->add_query(insert_stmt)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add ObInsertStmt to logical plan");
      }
    }
    if (OB_SUCCESS == ret && node->children_[0])
    {
      OB_ASSERT(T_SEQUENCE_REPLACE == node->children_[0]->type_);
      insert_stmt->set_replace(true);
    }
    if (OB_SUCCESS == ret)
    {
      //construction table_node
      ParseNode table_node;
      table_node.str_value_ = OB_ALL_SEQUENCE_TABLE_NAME;
      table_node.type_ = T_IDENT;
      ret = resolve_table(result_plan, insert_stmt, &table_node, table_id);
      if (OB_SUCCESS == ret && OB_ALL_SEQUENCE_TID == table_id)
      {
        insert_stmt->set_insert_table(table_id);
        ParseNode* column_node = NULL;
        ret = resolve_insert_columns(result_plan, insert_stmt, column_node);
      }
      //resolve sequence_name
      if (OB_SUCCESS == ret)
      {
        node->children_[1]->type_ = T_STRING;
        ret = resolve_independ_expr(result_plan, insert_stmt, node->children_[1],
                                    expr_id, T_INSERT_LIMIT);
        if (OB_SUCCESS == ret
            && OB_SUCCESS != (ret = value_row.push_back(expr_id)))
        {
          TBSYS_LOG(ERROR, "Can not add expr_id to ObArray");
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Create sequence failed.");
        }
      }
      if (OB_SUCCESS == ret &&
          OB_SUCCESS != (ret = resolve_sequence_label_list(result_plan, seq_create_stmt, insert_stmt, node->children_[2])))
      {
        TBSYS_LOG(ERROR, "Create sequence failed.");
      }
      if (OB_SUCCESS == ret
          && OB_SUCCESS == (ret = seq_create_stmt->sequence_option_validity(T_SEQUENCE_CREATE)))
      {
        for (int8_t i = 0; i < ObSequenceStmt::OptionFlag && OB_SUCCESS == ret; ++i)
        {
          if (OB_SUCCESS != (ret = value_row.push_back(seq_create_stmt->get_sequence_expr(i))))
          {
            TBSYS_LOG(ERROR, "Can not add expr_id to ObArray");
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Create sequence failed.");
          }
        }
      }
      if (OB_SUCCESS == ret &&
          insert_stmt->get_column_size() != value_row.count())
      {
        ret = OB_ERR_COLUMN_SIZE;
        TBSYS_LOG(ERROR, "Column count doesn't match value count.");
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Create sequence failed.");
      }
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = insert_stmt->add_value_row(value_row)))
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Create sequence failed.");
      }
      value_row.clear();
    }
  }
  return ret;
}
//20150327:e

//add liuzy [sequence] [JHOBv0.1] 20150327:b
int resolve_sequence_label_list(
    ResultPlan * result_plan,
    ObSequenceStmt* sequence_stmt,
    ObInsertStmt* insert_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObSequenceStmt::SequenceColumn column;
  uint64_t expr_id;
  if (OB_SUCCESS != (ret = sequence_stmt->init_sequence_option()))
  {
    TBSYS_LOG(ERROR, "Initialize default value of sequence failed.");
  }
  if (OB_SUCCESS == ret && node)
  {
    OB_ASSERT(T_SEQUENCE_LABEL_LIST == node->type_);
    for (int8_t i = 0; OB_SUCCESS == ret && i < node->num_child_; ++i)
    {
      ParseNode* opt_node = node->children_[i];
      switch (opt_node->type_)
      {
        case T_TYPE_INTEGER:
        case T_TYPE_BIG_INTEGER:
          column = ObSequenceStmt::DataType;
          if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(column))
          {
            uint8_t seq_type = OB_SEQUENCE_DEFAULT_DATA_TYPE;
            if (T_TYPE_BIG_INTEGER == opt_node->type_)
            {
              seq_type = OB_SEQUENCE_DEFAULT_DATA_TYPE_FOR_INT64;
            }
            ParseNode insert_node;
            insert_node.type_ = T_INT;
            insert_node.value_ = seq_type;
            if (OB_SUCCESS == (ret = resolve_independ_expr(result_plan, insert_stmt, &insert_node, expr_id, T_INSERT_LIMIT)))
            {
              sequence_stmt->set_sequence_expr(column, expr_id);
              sequence_stmt->set_sequence_datatype(seq_type);
            }
            else
            {
              TBSYS_LOG(ERROR, "Resolve datatype expr failed.");
            }
          }
          else
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "AS DATA_TYPE is repeated");
          }
          break;
        case T_TYPE_DECIMAL:
          column = ObSequenceStmt::DataType;
          if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(column))
          {
            /*input: "create sequence test as decimal(p,s);", p: between 1 and 31, s: must be zore.*/
            if(2 == opt_node->num_child_ && 0 != opt_node->children_[1]->value_)
            {
              ret = OB_ERR_PARSER_SYNTAX;
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "The form of datatype decimal is wrong. The scale of decimal must be zero.");
              break;
            }
            else if (1 <= opt_node->num_child_ && NULL != opt_node->children_[0])
            {
              if (31 >= opt_node->children_[0]->value_ && 1 <= opt_node->children_[0]->value_)
              {
                ParseNode insert_node;
                insert_node.type_ = T_INT;
                insert_node.value_ = opt_node->children_[0]->value_;
                if (OB_SUCCESS == (ret = resolve_independ_expr(result_plan, insert_stmt, &insert_node, expr_id, T_INSERT_LIMIT)))
                {
                  sequence_stmt->set_sequence_expr(column, expr_id);
                  sequence_stmt->set_sequence_datatype((uint8_t)opt_node->children_[0]->value_);
                }
                else
                {
                  TBSYS_LOG(ERROR, "Resolve datatype expr failed.");
                }
              }
              else
              {
                ret = OB_ERR_PARSER_SYNTAX;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Precision of decimal must be between 1 and 31.");
              }
            }
            else
            {
              /*input: "create sequence test as decimal;", default value of datatype is 5.*/
              ParseNode insert_node;
              insert_node.type_ = T_INT;
              insert_node.value_ = 5;
              if (OB_SUCCESS == (ret = resolve_independ_expr(result_plan, insert_stmt, &insert_node, expr_id, T_INSERT_LIMIT)))
              {
                sequence_stmt->set_sequence_datatype(5);
                sequence_stmt->set_sequence_expr(column, expr_id);
              }
              else
              {
                TBSYS_LOG(ERROR, "Resolve datatype expr failed.");
              }
            }
          }
          else
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "AS DATA_TYPE is repeated");
          }
          break;
        default:
          continue;
      }
    }
    for (int8_t i = 0; OB_SUCCESS == ret && i < node->num_child_; i++)
    {
      ParseNode* opt_node = node->children_[i];
      ObDecimal temp;
      switch (opt_node->type_)
      {
        case T_TYPE_INTEGER:
        case T_TYPE_BIG_INTEGER:
        case T_TYPE_DECIMAL:
          break;
        case T_SEQUENCE_START_WITH:
          column = ObSequenceStmt::CurrentValue;
          if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(column))
          {
            if (OB_SUCCESS != (ret = sequence_stmt->sequence_cons_decimal_value(temp, opt_node, OB_SEQUENCE_START_WITH)))
            {
              TBSYS_LOG(ERROR, "Construction decimal value with StartWith failed.");
              break;
            }
            else if (OB_SUCCESS == (ret = resolve_independ_expr(result_plan, insert_stmt, opt_node->children_[0], expr_id, T_INSERT_LIMIT)))
            {
              sequence_stmt->set_sequence_expr(column, expr_id);
              sequence_stmt->set_sequence_startwith(temp);
            }
            else
            {
              TBSYS_LOG(ERROR, "Resolve START WITH failed, ret = %d", ret);
            }
          }
          else
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "START WITH is repeated");
          }
          break;
        case T_SEQUENCE_INCREMENT_BY:
          column = ObSequenceStmt::IncrementBy;
          if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(column))
          {
            if (OB_SUCCESS != (ret = sequence_stmt->sequence_cons_decimal_value(temp, opt_node, OB_SEQUENCE_INCREMENT_BY)))
            {
              TBSYS_LOG(ERROR, "Construction decimal value with IncrementBy failed.");
              break;
            }
            else if (OB_SUCCESS == (ret = resolve_independ_expr(result_plan, insert_stmt, opt_node->children_[0], expr_id, T_INSERT_LIMIT)))
            {
              sequence_stmt->set_sequence_expr(column, expr_id);
              sequence_stmt->set_sequence_incrementby(temp);
            }
            else
            {
              TBSYS_LOG(ERROR, "Resolve INCREMENT BY failed, ret = %d", ret);
            }
          }
          else
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "INCREMENT BY is repeated");
          }
          break;
        case T_SEQUENCE_MINVALUE:
          column = sequence_stmt->MinValue;
          if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(column))
          {
            if (1 == opt_node->num_child_ && NULL != opt_node->children_[0])
            {
              if (OB_SUCCESS != (ret = sequence_stmt->sequence_cons_decimal_value(temp, opt_node, OB_SEQUENCE_MIN_VALUE)))
              {
                TBSYS_LOG(ERROR, "Construction decimal value with Minvalue failed.");
                break;
              }
              else if (OB_SUCCESS == (ret = resolve_independ_expr(result_plan, insert_stmt, opt_node->children_[0], expr_id, T_INSERT_LIMIT)))
              {
                sequence_stmt->set_sequence_expr(column, expr_id);
                sequence_stmt->set_sequence_minvalue(temp);
              }
              else
              {
                TBSYS_LOG(ERROR, "Resolve MINVALUE failed, ret = %d", ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "MINVALUE is repeated");
          }
          break;
        case T_SEQUENCE_MAXVALUE:
          column = ObSequenceStmt::MaxValue;
          if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(column))
          {
            if (1 == opt_node->num_child_ && NULL != opt_node->children_[0])
            {
              if (OB_SUCCESS != (ret = sequence_stmt->sequence_cons_decimal_value(temp, opt_node, OB_SEQUENCE_MAX_VALUE)))
              {
                TBSYS_LOG(ERROR, "Construction decimal value with Maxvalue failed.");
                break;
              }
              else if (OB_SUCCESS == (ret = resolve_independ_expr(result_plan, insert_stmt, opt_node->children_[0], expr_id, T_INSERT_LIMIT)))
              {
                sequence_stmt->set_sequence_expr(column, expr_id);
                sequence_stmt->set_sequence_maxvalue(temp);
              }
              else
              {
                TBSYS_LOG(ERROR, "Resolve MAXVALUE failed, ret = %d", ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "MAXVALUE is repeated");
          }
          break;
        case T_SEQUENCE_CYCLE:
          column = ObSequenceStmt::Cycle;
          if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(column))
          {
            ParseNode insert_node;
            insert_node.type_ = T_INT;
            insert_node.value_ = opt_node->value_;
            if ((ret = resolve_independ_expr(result_plan, insert_stmt, &insert_node, expr_id, T_INSERT_LIMIT)) == OB_SUCCESS)
            {
              sequence_stmt->set_sequence_expr(column, expr_id);
            }
            else
            {
              TBSYS_LOG(ERROR, "Resolve CYCLE failed, ret = %d", ret);
            }
          }
          else
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "CYCLE is repeated");
          }
          break;
        case T_SEQUENCE_CACHE:
          column = ObSequenceStmt::Cache;
          if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(column))
          {
            if (1 == opt_node->num_child_ && NULL != opt_node->children_[0])
            {
              if (OB_SUCCESS != (ret = sequence_stmt->sequence_cons_decimal_value(temp, opt_node, OB_SEQUENCE_CACHE_NUM)))
              {
                TBSYS_LOG(ERROR, "Construction decimal value with Cache failed.");
              }
              else if (opt_node->children_[0]->type_ == T_OP_NEG)
              {
                ret = OB_VALUE_OUT_OF_RANGE;
                TBSYS_LOG(USER_ERROR, "The value of Cache is out of range.");
              }
              else if (opt_node->children_[0]->type_ == T_OP_POS)
              {
                if (opt_node->children_[0]->num_child_ > 0 && opt_node->children_[0]->children_[0] != NULL)
                {
                  ParseNode* middle_node = opt_node->children_[0]->children_[0];
                  if (middle_node->value_ < 2 || middle_node->value_ > 2147483647)
                  {
                    ret = OB_VALUE_OUT_OF_RANGE;
                    TBSYS_LOG(USER_ERROR, "The value of Cache is out of range.");
                  }
                }
              }
              else
              {
                if (opt_node->children_[0]->value_ < 2 || opt_node->children_[0]->value_ > 2147483647)
                {
                  ret = OB_VALUE_OUT_OF_RANGE;
                  TBSYS_LOG(USER_ERROR, "The value of Cache is out of range.");
                }
              }
              if (OB_SUCCESS == ret &&
                  OB_SUCCESS == (ret = resolve_independ_expr(result_plan, insert_stmt, opt_node->children_[0],
                                                                                  expr_id, T_INSERT_LIMIT)))
              {
                sequence_stmt->set_sequence_expr(column, expr_id);
                sequence_stmt->set_sequence_cache((uint64_t)opt_node->children_[0]->value_);
              }
              else
              {
                TBSYS_LOG(ERROR, "Resolve CACHE failed, ret = %d", ret);
              }
            }
            else
            {
              ParseNode insert_node;
              insert_node.type_ = T_INT;
              insert_node.value_ = OB_SEQUENCE_DEFAULT_START_VALUE;
              if ((ret = resolve_independ_expr(result_plan, insert_stmt, &insert_node, expr_id, T_INSERT_LIMIT)) == OB_SUCCESS)
              {
                sequence_stmt->set_sequence_expr(column, expr_id);
                sequence_stmt->set_sequence_cache((uint64_t)OB_SEQUENCE_DEFAULT_START_VALUE);
              }
              else
              {
                TBSYS_LOG(ERROR, "Resolve CACHE failed, ret = %d", ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "CACHE is repeated");
          }
          break;
        case T_SEQUENCE_ORDER:
          column = ObSequenceStmt::Order;
          if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(column))
          {
            ParseNode insert_node;
            insert_node.type_ = T_INT;
            insert_node.value_ = opt_node->value_;
            if ((ret = resolve_independ_expr(result_plan, insert_stmt, &insert_node, expr_id, T_INSERT_LIMIT)) == OB_SUCCESS)
            {
              sequence_stmt->set_sequence_expr(column, expr_id);
            }
            else
            {
              TBSYS_LOG(ERROR, "Resolve ORDER failed, ret = %d", ret);
            }
          }
          else
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "ORDER is repeated");
          }
          break;
        case T_SEQUENCE_QUICK:
          column = ObSequenceStmt::UseQuickPath;
          if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(column))
          {
            ParseNode insert_node;
            insert_node.type_ = T_INT;
            insert_node.value_ = opt_node->value_;
            if ((ret = resolve_independ_expr(result_plan, insert_stmt, &insert_node, expr_id, T_INSERT_LIMIT)) == OB_SUCCESS)
            {
              sequence_stmt->set_sequence_expr(column, expr_id);
            }
            else
            {
              TBSYS_LOG(ERROR, "Resolve QUICK failed, ret = %d", ret);
            }
          }
          else
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "UseQuickPath is repeated");
          }
          break;
        default:
          ret = OB_ERR_PARSER_SYNTAX;
          TBSYS_LOG(ERROR, "Wrong node type, ret = %d", ret);
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "WRONG OPTION %d", i);
          break;
      }//switch
    }//for
  }//if
  char buf[MAX_PRINTABLE_SIZE];
  int64_t decimal_lenth = 0;
  ObDecimal zero;
  if (OB_SUCCESS == ret
      && OB_SUCCESS != (ret = zero.from((int64_t)0)))
  {
    TBSYS_LOG(ERROR, "Convert zero to decimal failed.");
  }
  for (int8_t i = 0; OB_SUCCESS == ret && i < ObSequenceStmt::OptionFlag; ++i)
  {
    if (ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(i))
    {
      ParseNode insert_node;
      ObDecimal temp;
      insert_node.type_ = T_INT;
      switch (i)
      {
        case ObSequenceStmt::DataType:
          insert_node.value_ = OB_SEQUENCE_DEFAULT_DATA_TYPE;
          sequence_stmt->set_sequence_datatype(OB_SEQUENCE_DEFAULT_DATA_TYPE);
          break;
        case ObSequenceStmt::CurrentValue:
          insert_node.type_ = T_DECIMAL;
          insert_node.str_value_ = OB_SEQUENCE_DEFAULT_START_CHAR;
          if ((ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(ObSequenceStmt::IncrementBy)
               || sequence_stmt->get_sequence_incrementby() >= zero)
              && ObSequenceStmt::SequenceOption::Set == sequence_stmt->is_set_option(ObSequenceStmt::MinValue))
          {
            memset(buf, 0, MAX_PRINTABLE_SIZE);
            decimal_lenth = sequence_stmt->get_sequence_minvalue().to_string(buf,MAX_PRINTABLE_SIZE);
            insert_node.str_value_ = const_cast<char *>(buf);
            temp = sequence_stmt->get_sequence_minvalue();
          }
          else if (ObSequenceStmt::SequenceOption::Set == sequence_stmt->is_set_option(ObSequenceStmt::IncrementBy)
                   && sequence_stmt->get_sequence_incrementby() < zero
                   && ObSequenceStmt::SequenceOption::Set == sequence_stmt->is_set_option(ObSequenceStmt::MaxValue))
          {
            memset(buf, 0, MAX_PRINTABLE_SIZE);
            decimal_lenth = sequence_stmt->get_sequence_maxvalue().to_string(buf,MAX_PRINTABLE_SIZE);
            insert_node.str_value_ = const_cast<char *>(buf);
            temp = sequence_stmt->get_sequence_maxvalue();
          }
          else if (ObSequenceStmt::SequenceOption::Set == sequence_stmt->is_set_option(ObSequenceStmt::IncrementBy)
                   && sequence_stmt->get_sequence_incrementby() < zero
                   && ObSequenceStmt::SequenceOption::NoSet == sequence_stmt->is_set_option(ObSequenceStmt::MaxValue))
          {
            insert_node.str_value_ = OB_SEQUENCE_NEGATIVE_START_CHAR;
            if (OB_SUCCESS != (ret = temp.from(OB_SEQUENCE_NEGATIVE_START_CHAR)))
            {
              TBSYS_LOG(ERROR, "Convert default value of start with to decimal failed.");
              break;
            }
          }
          else if (OB_SUCCESS != (ret = temp.from(OB_SEQUENCE_DEFAULT_START_CHAR)))
          {
            TBSYS_LOG(ERROR, "Convert default value of start with to decimal failed.");
            break;
          }
          sequence_stmt->set_sequence_startwith(temp);
          break;
        case ObSequenceStmt::IncrementBy:
          insert_node.value_ = OB_SEQUENCE_DEFAULT_START_VALUE;
          if (OB_SUCCESS != (ret = temp.from(OB_SEQUENCE_DEFAULT_START_VALUE)))
            break;
          sequence_stmt->set_sequence_incrementby(temp);
          break;
        case ObSequenceStmt::MinValue:
          insert_node.type_ = T_DECIMAL;
          if (sequence_stmt->get_sequence_incrementby() >= zero)
          {
            memset(buf, 0, MAX_PRINTABLE_SIZE);
            decimal_lenth = sequence_stmt->get_sequence_startwith().to_string(buf, MAX_PRINTABLE_SIZE);
            insert_node.str_value_ = const_cast<char *>(buf);
            sequence_stmt->set_sequence_minvalue(sequence_stmt->get_sequence_startwith());
          }
          else if (OB_SEQUENCE_DEFAULT_DATA_TYPE == sequence_stmt->get_sequence_datatype())
          {
            insert_node.str_value_ = OB_SEQUENCE_DEFAULT_MIN_VALUE;
            if (OB_SUCCESS != (ret = temp.from(OB_SEQUENCE_DEFAULT_MIN_VALUE)))
              break;
            sequence_stmt->set_sequence_minvalue(temp);
          }
          else if (OB_SEQUENCE_DEFAULT_DATA_TYPE_FOR_INT64 == sequence_stmt->get_sequence_datatype())
          {
            insert_node.str_value_ = OB_SEQUENCE_DEFAULT_MIN_VALUE_FOR_INT64;
            if (OB_SUCCESS != (ret = temp.from(OB_SEQUENCE_DEFAULT_MIN_VALUE_FOR_INT64)))
              break;
            sequence_stmt->set_sequence_minvalue(temp);
          }
          else
          {
            if (OB_SUCCESS == (ret = sequence_stmt->generate_decimal_value(sequence_stmt->get_sequence_datatype(), temp, ObSequenceStmt::MinValue)))
            {
              memset(buf, 0, MAX_PRINTABLE_SIZE);
              decimal_lenth = temp.to_string(buf, MAX_PRINTABLE_SIZE);
              insert_node.str_value_ = const_cast<char *>(buf);
              sequence_stmt->set_sequence_minvalue(temp);
            }
          }
          break;
        case ObSequenceStmt::MaxValue:
          insert_node.type_ = T_DECIMAL;
          if (sequence_stmt->get_sequence_incrementby() < zero)
          {
            memset(buf, 0, MAX_PRINTABLE_SIZE);
            decimal_lenth = sequence_stmt->get_sequence_startwith().to_string(buf, MAX_PRINTABLE_SIZE);
            insert_node.str_value_ = const_cast<char*>(buf);
            sequence_stmt->set_sequence_maxvalue(sequence_stmt->get_sequence_startwith());
          }
          else if (OB_SEQUENCE_DEFAULT_DATA_TYPE == sequence_stmt->get_sequence_datatype())
          {
            insert_node.str_value_ = OB_SEQUENCE_DEFAULT_MAX_VALUE;
            if (OB_SUCCESS != (ret = temp.from(OB_SEQUENCE_DEFAULT_MAX_VALUE)))
              break;
            sequence_stmt->set_sequence_maxvalue(temp);
          }
          else if (OB_SEQUENCE_DEFAULT_DATA_TYPE_FOR_INT64 == sequence_stmt->get_sequence_datatype())
          {
            insert_node.str_value_ = OB_SEQUENCE_DEFAULT_MAX_VALUE_FOR_INT64;
            if (OB_SUCCESS != (ret = temp.from(OB_SEQUENCE_DEFAULT_MAX_VALUE_FOR_INT64)))
              break;
            sequence_stmt->set_sequence_maxvalue(temp);
          }
          else
          {
            if (OB_SUCCESS == (ret = sequence_stmt->generate_decimal_value(sequence_stmt->get_sequence_datatype(), temp)))
            {
              memset(buf, 0, MAX_PRINTABLE_SIZE);
              decimal_lenth = temp.to_string(buf, MAX_PRINTABLE_SIZE);
              insert_node.str_value_ = const_cast<char *>(buf);
              sequence_stmt->set_sequence_maxvalue(temp);
            }
          }
          break;
        case ObSequenceStmt::Cycle:
          insert_node.value_ = OB_SEQUENCE_DEFAULT_NO_USE_VALUE;
          break;
        case ObSequenceStmt::Cache:
          insert_node.value_ = (int64_t)OB_SEQUENCE_DEFAULT_CACHE;
          break;
        case ObSequenceStmt::Order:
          insert_node.value_ = OB_SEQUENCE_DEFAULT_NO_USE_VALUE;
          break;
        case ObSequenceStmt::Valid:
          insert_node.value_ = OB_SEQUENCE_DEFAULT_VAILD;
          break;
        case ObSequenceStmt::ConstStartWith:
          insert_node.type_ = T_DECIMAL;
          memset(buf, 0 , MAX_PRINTABLE_SIZE);
          decimal_lenth = sequence_stmt->get_sequence_startwith().to_string(buf, MAX_PRINTABLE_SIZE);
          insert_node.str_value_ = const_cast<char*>(buf);
          break;
        case ObSequenceStmt::CanUsePrevval:
          insert_node.value_ = OB_SEQUENCE_DEFAULT_NO_USE_VALUE;
          break;
        case ObSequenceStmt::UseQuickPath:
          insert_node.value_ = OB_SEQUENCE_DEFAULT_NO_USE_VALUE;
          break;
        default:
          ret = OB_ERR_PARSER_SYNTAX;
          break;
      }//end switch
      if (OB_ERR_PARSER_SYNTAX == ret)
      {
        TBSYS_LOG(ERROR, "Wrong enum : %d", i);
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "Convert to decimal failed, option number = %d", i);
      }
      else if (OB_SUCCESS == (ret = resolve_independ_expr(result_plan, insert_stmt, &insert_node, expr_id, T_INSERT_LIMIT)))
      {
        sequence_stmt->set_sequence_expr(i, expr_id);
      }
      else
      {
        TBSYS_LOG(ERROR, "Resolve default value of option[%d] failed, ret = %d", i, ret);
      }
      if (OB_SUCCESS != ret)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Create sequence failed.");
        break;
      }
    }//end if
  }//end for
  return ret;
}
//add 20150327:e
//add liuzy [sequence] [JHOBv0.1] 20150420:b
int resolve_sequence_drop_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  OB_ASSERT(NULL != node && T_SEQUENCE_DROP == node->type_ && 1 == node->num_child_);
  query_id = OB_INVALID_ID;
  ObLogicalPlan* logical_plan = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (NULL == result_plan->plan_tree_)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (NULL == logical_plan)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }
  if (OB_SUCCESS == ret)
  {
    ObDeleteStmt* delete_stmt = (ObDeleteStmt*)parse_malloc(sizeof(ObDeleteStmt), result_plan->name_pool_);
    if (NULL == delete_stmt)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObDeleteStmt");
    }
    else
    {
      delete_stmt = new(delete_stmt) ObDeleteStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      delete_stmt->set_query_id(query_id);
      if (OB_SUCCESS != (ret = logical_plan->add_query(delete_stmt)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add ObSequenceStmt to logical plan");
      }
    }
    if (OB_SUCCESS == ret)
    {
      ParseNode table_node;
      table_node.str_value_ = OB_ALL_SEQUENCE_TABLE_NAME;
      table_node.type_ = T_IDENT;
      ret = resolve_table(result_plan, delete_stmt, &table_node, table_id);
      if (OB_SUCCESS == ret && OB_ALL_SEQUENCE_TID == table_id)
      {
        ret = add_all_rowkey_columns_to_stmt(result_plan, table_id, delete_stmt);
      }
      if (OB_SUCCESS == ret)
      {
        delete_stmt->set_delete_table(table_id);
        const char* column = OB_SEQUENCE_NAME;
        ParseNode where_node,child_node_1,child_node_2;
        where_node.type_ = T_OP_EQ;
        where_node.num_child_ = 2;
        where_node.str_value_ = NULL;
        where_node.value_ = 0;
        int64_t alloc_size = sizeof(ParseNode*) * where_node.num_child_ ;
        where_node.children_ = (ParseNode**)parse_malloc(alloc_size, result_plan->name_pool_);
        child_node_1.type_ = T_IDENT;
        child_node_1.str_value_ = column;
        child_node_1.value_ = static_cast<int64_t>(strlen(column));
        where_node.children_[0] = &child_node_1;
        child_node_2.type_ = T_STRING;
        child_node_2.str_value_ = node->children_[0]->str_value_;
        child_node_2.value_ = static_cast<int64_t>(strlen(node->children_[0]->str_value_));
        where_node.children_[1] = &child_node_2;
        if (OB_SUCCESS != (ret = resolve_where_clause(result_plan, delete_stmt, &where_node)))
        {
          TBSYS_LOG (ERROR, "Resolve drop sequence where caluse failed.");
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Delete sequence failed.");
        }
      }
    }
  }
  return ret;
}
//add 20150420:e

int resolve_update_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  OB_ASSERT(node && node->type_ == T_UPDATE && node->num_child_ >= 5);
  query_id = OB_INVALID_ID;

  ObLogicalPlan* logical_plan = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    ObUpdateStmt* update_stmt = (ObUpdateStmt*)parse_malloc(sizeof(ObUpdateStmt), result_plan->name_pool_);
    if (update_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc ObUpdateStmt");
    }
    else
    {
      update_stmt = new(update_stmt) ObUpdateStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      update_stmt->set_query_id(query_id);
      ret = logical_plan->add_query(update_stmt);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add ObUpdateStmt to logical plan");
      }
      else
      {
        ParseNode* table_node = node->children_[0];
        //if (table_node->type_ != T_IDENT)
        /* mod liumz, [update_table_alias]20151211:b */
        //if (table_node->type_ != T_RELATION)
        if (table_node->type_ != T_RELATION && table_node->type_ != T_ALIAS)
        /* mod:e */
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Only single base table is supported for Update");
        }
        if (ret == OB_SUCCESS)
        {
          /* add liumz, [update_table_alias]20151211:b */
          if (table_node->type_ == T_ALIAS && table_node->children_[0]->type_ != T_RELATION)
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Only single base table is supported for Update");
          }
          /* add:e */
          ret = resolve_table(result_plan, update_stmt, table_node, table_id);
          /* add liumz, [update_table_alias]20151211:b */
          if (ret == OB_SUCCESS && table_node->type_ == T_ALIAS)
          {
            ret = update_stmt->change_table_item_tid(table_id);
          }
          /* add:e */
        }
        if (ret == OB_SUCCESS)
        {
          ret = add_all_rowkey_columns_to_stmt(result_plan, table_id, update_stmt);
        }
        //add lijianqiang [sequence update] 20150912:b
        /*Exp:add sequence expr ids for row key ,all push 0,we wil have the same size with out put columns*/
        if (OB_SUCCESS == ret)
        {
          ret = add_all_row_key_expr_ids(result_plan, table_id, update_stmt);
        }
        //add 20150912:e
        if (ret == OB_SUCCESS)
        {
          update_stmt->set_update_table(table_id);
          ParseNode* assign_list = node->children_[1];
          OB_ASSERT(assign_list && assign_list->type_ == T_ASSIGN_LIST);
          uint64_t ref_id;
          ColumnItem *column_item = NULL;
          for (int32_t i = 0; ret == OB_SUCCESS && i < assign_list->num_child_; i++)
          {
            ParseNode* assgin_node = assign_list->children_[i];
            OB_ASSERT(assgin_node && assgin_node->type_ == T_ASSIGN_ITEM && assgin_node->num_child_ >= 2);

            /* resolve target column */
            ParseNode* column_node = assgin_node->children_[0];
            /* mod liumz, [update_table_alias]20151211:b */
            //OB_ASSERT(column_node && column_node->type_ == T_IDENT);
            OB_ASSERT(column_node && (column_node->type_ == T_IDENT || column_node->type_ == T_OP_NAME_FIELD));
            /* mod:e */

            ObString column_name;
            if (column_node->type_ == T_IDENT)
            {
              column_name.assign_ptr(
                    (char*)(column_node->str_value_),
                    static_cast<int32_t>(strlen(column_node->str_value_))
                    );
            }
            /* add liumz, [update_table_alias]20151211:b */
            else if (column_node->type_ == T_OP_NAME_FIELD)
            {
              ParseNode* col_node = column_node->children_[1];
              OB_ASSERT(col_node && col_node->type_ == T_IDENT);
              column_name.assign_ptr(
                    (char*)(col_node->str_value_),
                    static_cast<int32_t>(strlen(col_node->str_value_))
                    );
            }
            /* add:e */
            column_item = update_stmt->get_column_item(NULL, column_name);
            if (column_item == NULL)
            {
              ret = update_stmt->add_column_item(*result_plan, column_name, NULL, &column_item);
            }
            if (ret == OB_SUCCESS)
            {
              ret = update_stmt->add_update_column(column_item->column_id_);
            }
            /* resolve new value expression */
            if (ret == OB_SUCCESS)
            {
              ParseNode* expr = assgin_node->children_[1];
              //mod dragon [varchar limit] 2016-8-11 10:40:08
              /* ---new: 增加cinfo用于判断varchar的长度是否满足schema的要求---*/
              ObColumnInfo cinfo(column_item->table_id_, column_item->column_id_);
              ret = resolve_independ_expr(result_plan, update_stmt, expr, ref_id, T_UPDATE_LIMIT,
                                          cinfo);
              if(OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "Varchar is too long! Column item:index[%d] tid[%ld], cid[%ld]",
                          i, column_item->table_id_, column_item->column_id_);
              }
              /* ---old---
              ret = resolve_independ_expr(result_plan, update_stmt, expr, ref_id, T_UPDATE_LIMIT);
              ---old---*/
              //mod 2016-8-11e
              //add lijianqiang [sequence update] 20150519:b
              if (update_stmt->current_expr_has_sequence())
              {
                update_stmt->add_sequence_expr_id(ref_id);
                update_stmt->add_sequence_expr_names_and_types();
                update_stmt->set_current_expr_has_sequence(false);
                update_stmt->reset_expr_types_and_names();
              }
              else
              {
                update_stmt->add_sequence_expr_id(0);
              }
              //add 20150519:e
            }
            if (ret == OB_SUCCESS)
            {
              if ((ret = update_stmt->add_update_expr(ref_id)) != OB_SUCCESS)
              {
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         "Add update value error");
              }
            }
          }
          //add lijianqiang [sequence] 20150910:b
          /*Exp:we store the whole the expr ids of update stmt into one array,this boundary can discriminate the set clause ids and where clause ids */
          if (OB_SUCCESS == ret)
          {
            update_stmt->set_set_where_boundary(update_stmt->get_sequence_types_and_names_array_size());
            TBSYS_LOG(DEBUG,"the exprs count is[%ld]",(update_stmt->get_sequence_expr_ids_size()));
            TBSYS_LOG(DEBUG,"the boundary is[%ld]",update_stmt->get_set_where_boundary());
          }
          //add 20150910:e
        }
        if (ret == OB_SUCCESS)
          ret = resolve_where_clause(result_plan, update_stmt, node->children_[2]);
        if (ret == OB_SUCCESS && node->children_[3])
          ret = resolve_when_clause(result_plan, update_stmt, node->children_[3]);
        if (ret == OB_SUCCESS && node->children_[4])
          ret = resolve_hints(result_plan, update_stmt, node->children_[4]);
      }
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302:b
      if(OB_SUCCESS == ret)
      {
          if(true == update_stmt->get_query_hint().is_delete_update_multi_batch_)
          {
              update_stmt->set_is_update_multi_batch(true);
          }
          if(true == update_stmt->get_query_hint().is_all_rowkey_)
          {
              update_stmt->set_is_update_all_rowkey(true);
          }
          update_stmt->set_ud_where_parse_tree(node->children_[2]);
          node->children_[2] = NULL;
      }
      //add gaojt 20160302:e
    }
  }
  return ret;
}

int resolve_when_clause(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    if ((ret = resolve_and_exprs(
           result_plan,
           stmt,
           node,
           stmt->get_when_exprs(),
           T_WHEN_LIMIT
           )) == OB_SUCCESS)
    {
      ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
      stmt->set_when_number(logical_plan->generate_when_number());
    }
  }
  return ret;
}


//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140409:b

/*@author   杜彦荣
 *@brief    函数负责优化select查询中的各个join操作
 *@brief    函数限制为只能被resolve_select_stmt函数调用
 *@param    result_plan 传入逻辑计划和保存报错信息,result_plan->plan_tree_ 必须为 ObLogicalPlan*,不能为 ObMultiLogicPlan*
 *@param    select_stmt 传入select的逻辑计划的sql语句信息
 *@return   操作是否成功的信息
 *@retval   OB_SUCCESS 表示成功，其他值则为失败
 */
int optimize_joins_filter(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt)
{
  if(NULL == result_plan || NULL == select_stmt)
  {
    TBSYS_LOG(ERROR,"function param error");
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "param of optimize_joins_filter can't be NULL");
    return OB_INPUT_PARAM_ERROR;
  }

  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (select_stmt->get_stmt_type() != ObStmt::T_SELECT)
  {
    TBSYS_LOG(ERROR,"stmt type should be T_SELECT");
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "sql_stmt type should be T_SELECT");
    ret = OB_INPUT_PARAM_ERROR;
  }

  int32_t num_table = select_stmt->get_from_item_size();
  for(int32_t i = 0; OB_SUCCESS == ret && i < num_table; i++)
  {
    const FromItem& from_item = select_stmt->get_from_item(i);
    if (from_item.is_joined_ == true)
    {
      JoinedTable* joined_table  = select_stmt->get_joined_table(from_item.table_id_);
      if (joined_table == NULL)
      {
        continue;
      }

      ret = optimize_join_where_expr(result_plan, select_stmt, joined_table);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"optimize join where expr error!");
        break;
      }
    }
  }
  return ret;
}

//add 20140409:e

//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140411:b

/*
 *@author   杜彦荣
 *@brief    函数负责一个具体join的优化
 *@brief    函数暂时限制只允许被optimize_joins_filter函数调用
 *@param    result_plan 传入逻辑计划和保存报错信息
 *@param    select_stmt 传入select的逻辑计划的sql语句信息
 *@param    joined_table 传入一个具体join操作的信息
 *@return   函数操作是否成功的信息
 *@retval   OB_SUCCESS 表示成功，其他值则为失败
 */
int optimize_join_where_expr(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    JoinedTable* joined_table)
{
  if (NULL == result_plan || NULL == select_stmt || NULL == joined_table)
  {
    TBSYS_LOG(ERROR,"function param error!");
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "param of optimize_join_where_expr can't be NULL");
    return OB_INPUT_PARAM_ERROR;
  }

  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  if (logical_plan == NULL)
  {
    ret = OB_ERR_LOGICAL_PLAN_FAILD;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "logical_plan must exist!!!");
    TBSYS_LOG(ERROR,"logical_plan must exist!!!");
  }

  //每个JOIN操作都要有连接条件
  OB_ASSERT(joined_table->join_types_.count() == joined_table->expr_nums_per_join_.count());

  ObVector<ObSqlRawExpr*> new_expr_store;
  if (OB_SUCCESS == ret)
  {
    /****first for******/
    //注意这里的select_stmt->get_condition_size()是个动态变化的值
    for(int32_t it = 0; OB_SUCCESS == ret && it < select_stmt->get_condition_size(); it++)
    {
      ObSqlRawExpr* where_expr = logical_plan->get_expr( select_stmt->get_condition_id(it) );
      if (where_expr == NULL
          || where_expr->is_join_optimized()
          || (! where_expr->get_expr()->is_column_range_filter())
          )
      {
        continue;
      }

      int64_t join_expr_position  = 0;
      int64_t join_expr_num       = 0;
      for (int64_t j = 0; OB_SUCCESS == ret && j < joined_table->join_types_.count(); j++)
      {
        join_expr_num = joined_table->expr_nums_per_join_.at(j);
        if (joined_table->join_types_.at(j) != JoinedTable::T_INNER)
        {
          join_expr_position += join_expr_num;
          continue;
        }

        for(int64_t join_index = 0; OB_SUCCESS == ret && join_index < join_expr_num; join_index++)
        {
          ObSqlRawExpr* join_expr = logical_plan->get_expr(joined_table->expr_ids_.at(join_expr_position + join_index));
          if ( NULL == join_expr || !( join_expr->get_expr()->is_join_cond() ) )
          {
            continue;
          }

          //先判断是否满足优化条件
          bool is_meet_opti_cnd = false;
          ObBinaryRefRawExpr* ref_expr_from_join = NULL;
          ret = optimize_meet_condition(result_plan,
                                        select_stmt,
                                        where_expr,
                                        join_expr,
                                        select_stmt->get_where_exprs(),
                                        is_meet_opti_cnd,
                                        ref_expr_from_join);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN,"where expr and join expr don't meet optimize cnd");
            break;
          }
          else if (is_meet_opti_cnd  && ref_expr_from_join != NULL)
          {//满足优化条件时，生成新表达式
            ret = optimize_gen_new_where_expr(result_plan,
                                              select_stmt,
                                              where_expr,
                                              ref_expr_from_join,
                                              select_stmt->get_where_exprs(),
                                              new_expr_store);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR,"gen new where expr failed!");
              break;
            }
          }

        }//end of first for

        join_expr_position += join_expr_num;

      }//end of second for

    }//end of third for

  }

  if (OB_SUCCESS == ret)
  {
    //所有新生成的表达式，都不能再用于优化其他join操作
    int32_t new_expr_num = new_expr_store.size();
    for (int32_t new_expr_i=0; new_expr_i < new_expr_num; new_expr_i++)
    {
      ObSqlRawExpr* new_expr = new_expr_store.at(new_expr_i);
      new_expr->set_join_optimized(true);
    }
    new_expr_store.clear();
  }
  return ret;
}

//add 20140411:e

//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140411:b

/*
 *@author   杜彦荣
 *@brief    函数判断where_expr和join_expr是否满足优化条件
 *@brief    函数限制为只能被optimize_join_where_expr函数使用
 *@param    where_expr 传入查询条件
 *@param    join_expr  传入连接条件
 *@param    old_where_exprid_store 传入WHERE子句中现有的查询条件
 *@param    is_meet_opti_cnd 保存是否满足优化条件
 *@param    ref_expr_from_join join_expr中准备生成新查询条件的操作数
 *@return   函数操作是否成功的信息
 *@retval   OB_SUCCESS 表示成功，其他值则为失败
 */
int optimize_meet_condition(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    const ObSqlRawExpr* where_expr,
    const ObSqlRawExpr* join_expr,
    const ObVector<uint64_t>& old_where_exprid_store,
    bool& is_meet_opti_cnd,
    ObBinaryRefRawExpr*& ref_expr_from_join)
{
  if (NULL == result_plan || NULL == select_stmt || NULL == where_expr || NULL == join_expr)
  {
    TBSYS_LOG(ERROR,"function param error!");
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "param of optimize_meet_condition can't be NULL");
    return OB_INPUT_PARAM_ERROR;
  }

  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  if (logical_plan == NULL)
  {
    ret = OB_ERR_LOGICAL_PLAN_FAILD;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "logical_plan must exist!!!");
    TBSYS_LOG(ERROR,"logical_plan must exist!!!");
  }

  //init
  ref_expr_from_join  = NULL;
  is_meet_opti_cnd    = false;
  bool sub_ref_expr_is_first  = false;
  bool is_triple_expr         = false;
  ObVector<ObBinaryRefRawExpr*>  sub_ref_exprs_store;
  ObVector<ObConstRawExpr*>      sub_const_exprs_store;
  ObBinaryRefRawExpr* join_lexpr = NULL;
  ObBinaryRefRawExpr* join_rexpr = NULL;
  ObBinaryRefRawExpr* where_sub_ref_expr = NULL;

  //first
  if (OB_SUCCESS == ret)
  {
    if ( !( join_expr->get_expr()->is_join_cond() ) )
    {
      is_meet_opti_cnd = false;
    }
    else if ( !where_expr->get_expr()->is_column_range_filter() )
    {
      is_meet_opti_cnd = false;
    }
    else if ( !( where_expr->get_tables_set().is_subset(join_expr->get_tables_set()) ) )
    {
      is_meet_opti_cnd = false;
    }
    else
    {
      is_meet_opti_cnd = true;
    }
  }

  //second: split join expr
  if (OB_SUCCESS == ret && is_meet_opti_cnd)
  {
    sub_ref_exprs_store.clear();
    sub_const_exprs_store.clear();
    ret = optimize_split_expr(result_plan,
                              join_expr,
                              sub_ref_exprs_store,
                              sub_const_exprs_store,
                              is_triple_expr,
                              sub_ref_expr_is_first);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN,"split join_expr failed");
    }
    else if (sub_ref_exprs_store.size() >= 2)
    {
      join_lexpr = sub_ref_exprs_store.at(0);
      join_rexpr = sub_ref_exprs_store.at(1);
    }
    else
    {/*split join expr failed*/
      is_meet_opti_cnd = false;
    }
  }

  //third:split where expr
  if (OB_SUCCESS == ret && is_meet_opti_cnd)
  {
    sub_ref_exprs_store.clear();
    sub_const_exprs_store.clear();
    ret = optimize_split_expr(result_plan,
                              where_expr,
                              sub_ref_exprs_store,
                              sub_const_exprs_store,
                              is_triple_expr,
                              sub_ref_expr_is_first);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN,"split where_expr failed");
    }
    else if (sub_ref_exprs_store.size() >=1)
    {
      where_sub_ref_expr = sub_ref_exprs_store.at(0);
    }
    else
    {/*split where expr failed*/
      is_meet_opti_cnd = false;
    }
  }

  //fouth
  //where expr和join expr至少有一个操作数，在表名和列名上都相等
  //例如join expr: A.id=B.id,同时where expr A.id > 100 ===> 则ref_expr_from_join=B.id
  if ( OB_SUCCESS == ret && is_meet_opti_cnd )
  {
    if ( optimize_ref_expr_equal(join_lexpr,where_sub_ref_expr) )
    {
      ref_expr_from_join = join_rexpr;
    }
    else if ( optimize_ref_expr_equal(join_rexpr,where_sub_ref_expr) )
    {
      ref_expr_from_join = join_lexpr;
    }
    else
    {
      is_meet_opti_cnd = false;
      ref_expr_from_join = NULL;
    }
  }


  //fifth
  //WHERE子句中，原先没有join_ref_expr时才继续优化操作
  if (OB_SUCCESS == ret && is_meet_opti_cnd && ref_expr_from_join != NULL)
  {
    int32_t join_ref_bit_idx = select_stmt->get_table_bit_index( ref_expr_from_join->get_first_ref_id() );
    int32_t store_size  = old_where_exprid_store.size();

    for(int32_t i=0; OB_SUCCESS==ret && i< store_size; i++)
    {
      const ObSqlRawExpr* old_where_expr = logical_plan->get_expr( old_where_exprid_store.at(i) );
      if ( !old_where_expr->get_expr()->is_column_range_filter() )
      {
        continue;
      }

      if (old_where_expr->get_tables_set().has_member(join_ref_bit_idx))
      {//都是同一张表的表达式

        /*step_1:split old_where_expr*/
        sub_ref_exprs_store.clear();
        sub_const_exprs_store.clear();
        ObBinaryRefRawExpr* old_where_sub_ref_expr = NULL;
        ret = optimize_split_expr(result_plan,
                                  old_where_expr,
                                  sub_ref_exprs_store,
                                  sub_const_exprs_store,
                                  is_triple_expr,
                                  sub_ref_expr_is_first);
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN,"split old where_expr failed");
          break;
        }
        else if (sub_ref_exprs_store.size() >=1)
        {
          old_where_sub_ref_expr = sub_ref_exprs_store.at(0);
        }
        else
        {/*won't be here*/
          is_meet_opti_cnd   = false;
          ref_expr_from_join = NULL;
          break;
        }

        /*step_2:search join ref expr in where stores*/
        if ( optimize_ref_expr_equal(old_where_sub_ref_expr,ref_expr_from_join))
        {//表名和列名都相同，所以已有这个where expr,没有再新生成的价值
          is_meet_opti_cnd   = false;
          ref_expr_from_join = NULL;
          break;
        }
      }
    }
  }


  //sixth
  if ( ref_expr_from_join == NULL || ! is_meet_opti_cnd || OB_SUCCESS != ret )
  {
    ref_expr_from_join  = NULL;
    is_meet_opti_cnd    = false;
  }
  return ret;
}

//add 20140411:e

//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140411:b

/*
 *@author   杜彦荣
 *@brief    函数根据传入的where_expr和ref_expr_of_new_expr生成一个新的表达式
 *@brief    函数目前限制为只能被optimize_join_where_expr函数调用
 *@param    where_expr  传入where表达式指针
 *@param    ref_expr_of_new_expr   新表达式树中的ObBinaryRefRawExpr类型的节点
 *@param    new_expr_id_store 保存新生成的表达式id
 *@param    new_expr_store 保存新生成的表达式指针
 *@return   函数操作是否成功的信息
 *@retval   OB_SUCCESS 表示成功，其他值则为失败
 */
int optimize_gen_new_where_expr(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ObSqlRawExpr* where_expr,
    ObBinaryRefRawExpr* ref_expr_of_new_expr,
    ObVector<uint64_t>& new_expr_id_store,
    ObVector<ObSqlRawExpr*>& new_expr_store)
{
  if (NULL == result_plan
      || NULL == select_stmt
      || NULL == where_expr
      || NULL == ref_expr_of_new_expr
      || (!where_expr->get_expr()->is_column_range_filter())
      )
  {
    TBSYS_LOG(ERROR,"function param error!");
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "param of optimize_gen_new_where_expr error");
    return OB_INPUT_PARAM_ERROR;
  }

  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  if (logical_plan == NULL)
  {
    ret = OB_ERR_LOGICAL_PLAN_FAILD;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "logical_plan must exist!!!");
    TBSYS_LOG(ERROR,"logical_plan must exist!!!");
  }

  bool is_triple_expr = false;
  bool sub_ref_expr_is_first = false;
  ObVector<ObBinaryRefRawExpr*> sub_ref_exprs_store;
  ObVector<ObConstRawExpr*>     sub_const_exprs_store;
  ObTripleOpRawExpr*  where_main_triple_expr  = NULL;
  ObBinaryOpRawExpr*  where_main_binary_expr  = NULL;
  ObConstRawExpr*     where_sub_first_const_expr  = NULL;
  ObConstRawExpr*     where_sub_second_const_expr = NULL;

  //first:split where expr
  if(OB_SUCCESS == ret)
  {
    sub_ref_exprs_store.clear();
    sub_const_exprs_store.clear();
    ret = optimize_split_expr(result_plan,
                              where_expr,
                              sub_ref_exprs_store,
                              sub_const_exprs_store,
                              is_triple_expr,
                              sub_ref_expr_is_first);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN,"split where_expr failed");
    }
    //where expr的类型为ObTripleOpRawExpr
    else if (is_triple_expr && sub_const_exprs_store.size() >= 2)
    {
      where_main_triple_expr = dynamic_cast<ObTripleOpRawExpr*>(where_expr->get_expr());
      where_sub_first_const_expr  = sub_const_exprs_store.at(0);
      where_sub_second_const_expr = sub_const_exprs_store.at(1);
    }
    //where expr的类型为ObBinaryOpRawExpr
    else if (!is_triple_expr && sub_const_exprs_store.size() >= 1)
    {
      where_main_binary_expr      = dynamic_cast<ObBinaryOpRawExpr*>(where_expr->get_expr());
      where_sub_first_const_expr  = sub_const_exprs_store.at(0);
    }
    else
    {/*won't be here*/
      TBSYS_LOG(ERROR,"where expr don't meet optimize condition");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "can't generate new where expr!");
      ret = OB_INPUT_PARAM_ERROR;
    }
  }

  //second:generate new where expr
  ObRawExpr* new_where_expr = NULL;
  if (OB_SUCCESS == ret)
  {
    //where表达式为triple类型
    if (is_triple_expr)
    {
      ret = optimize_gen_triple_expr(result_plan,
                                     where_main_triple_expr,
                                     ref_expr_of_new_expr,
                                     where_sub_first_const_expr,
                                     where_sub_second_const_expr,
                                     new_where_expr);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"gen triple expr error");
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "optimize gen triple expr error");
      }
    }
    //where表达式为binary类型
    else
    {
      ret = optimize_gen_binary_expr(result_plan,
                                     where_main_binary_expr,
                                     ref_expr_of_new_expr,
                                     where_sub_first_const_expr,
                                     sub_ref_expr_is_first,
                                     new_where_expr);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"gen binary expr error");
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "optimize gen binary expr error");
      }
    }
  }


  //third:add new expr into logical_plan
  uint64_t expr_id = OB_INVALID_ID;
  ObSqlRawExpr* new_sql_expr = NULL;
  if (OB_SUCCESS == ret && new_where_expr != NULL )
  {
    //malloc space
    new_sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr), result_plan->name_pool_);
    if (new_sql_expr == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(ERROR, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc space for ObSqlRawExpr");
    }

    //add sql expr into logical_plan
    if (OB_SUCCESS == ret)
    {
      new_sql_expr = new(new_sql_expr) ObSqlRawExpr();
      ret = logical_plan->add_expr(new_sql_expr);//add expr into logical_plan
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"add expr into logical plan failed!");
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Add ObSqlRawExpr error");
      }
    }

    //gen expr id
    if (OB_SUCCESS == ret)
    {
      expr_id = logical_plan->generate_expr_id();
      new_sql_expr->set_expr_id(expr_id);
      if (new_where_expr->get_expr_type() == T_REF_COLUMN)
      {
        ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(new_where_expr);
        new_sql_expr->set_table_id(col_expr->get_first_ref_id());
        new_sql_expr->set_column_id(col_expr->get_second_ref_id());
      }
      else
      {
        new_sql_expr->set_table_id(OB_INVALID_ID);
        new_sql_expr->set_column_id(logical_plan->generate_column_id());//系统生成的column_id，不同于schema中的column_id
      }
      new_sql_expr->set_expr(new_where_expr);
      //add table bitset into sql expr
      int32_t table_bit_index = select_stmt->get_table_bit_index(ref_expr_of_new_expr->get_first_ref_id());
      new_sql_expr->get_tables_set().add_member(table_bit_index);
      if (table_bit_index < 0)
      {
        TBSYS_LOG(ERROR, "negative bitmap values");
      }
    }
  }

  //fourth:save new expr
  if (OB_SUCCESS == ret && expr_id != OB_INVALID_ID && new_sql_expr != NULL)
  {
    //add expr id into new_expr_id_store,
    if (OB_SUCCESS != ( ret = new_expr_id_store.push_back(expr_id) ) )
    {
      snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
               "and optimize expr into where exprs error!");
      TBSYS_LOG(ERROR,"and optimize expr into where exprs error!");
    }
    //将新表达式树的指针保存到new_expr_store数组
    else if (OB_SUCCESS != (ret = new_expr_store.push_back(new_sql_expr)) )
    {

      snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
               "and optimize expr into new expr stores error!");
      TBSYS_LOG(ERROR,"and optimize expr into new expr stores error!");
    }
    else
    {
      //这个where expr不能再用于优化其他jon操作
      where_expr->set_join_optimized(true);
    }
  }
  return ret;
}

//add 20140411:e


//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140416:b
/*
 *@author   杜彦荣
 *@brief    函数负责生成ObTripleOpRawExpr类型的表达式
 *@brief    函数限制暂时只允许被optimize_gen_new_where_expr函数调用
 *@param    old_main_triple_expr 表达式树的根节点类型
 *@param    old_sub_first_ref_expr表达式树中的ObBinaryRefRawExpr类型的子节点
 *@param    new_expr 返回新生成的表达式的指针
 */

int optimize_gen_triple_expr(
    ResultPlan * result_plan,
    const ObTripleOpRawExpr* old_main_triple_expr,
    const ObBinaryRefRawExpr* old_sub_first_ref_expr,
    const ObConstRawExpr* old_sub_second_const_expr,
    const ObConstRawExpr* old_sub_third_const_expr,
    ObRawExpr*& new_expr)
{
  if (NULL == result_plan
      || NULL == old_main_triple_expr
      || NULL == old_sub_first_ref_expr
      || NULL == old_sub_second_const_expr
      || NULL == old_sub_third_const_expr)
  {
    TBSYS_LOG(ERROR,"function param error!");
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "param of optimize_gen_triple_expr error");
    return OB_INPUT_PARAM_ERROR;
  }

  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  if (logical_plan == NULL)
  {
    ret = OB_ERR_LOGICAL_PLAN_FAILD;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "logical_plan must exist!!!");
    TBSYS_LOG(ERROR,"logical_plan must exist!!!");
  }

  //gen new sub first ref expr
  ObBinaryRefRawExpr *new_sub_first_ref_expr = NULL;
  if (OB_SUCCESS == ret)
  {
    if (CREATE_RAW_EXPR(new_sub_first_ref_expr, ObBinaryRefRawExpr, result_plan) == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(ERROR,"can't create ObBinaryRefRawExpr");
    }
    else
    {
      new_sub_first_ref_expr->set_expr_type(old_sub_first_ref_expr->get_expr_type());
      new_sub_first_ref_expr->set_result_type(old_sub_first_ref_expr->get_result_type());
      new_sub_first_ref_expr->set_first_ref_id(old_sub_first_ref_expr->get_first_ref_id());
      new_sub_first_ref_expr->set_second_ref_id(old_sub_first_ref_expr->get_second_ref_id());
    }
  }

  //gen new sub second const expr
  ObConstRawExpr *new_sub_second_const_expr = NULL;
  if (OB_SUCCESS == ret)
  {
    if (CREATE_RAW_EXPR(new_sub_second_const_expr, ObConstRawExpr, result_plan) == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(ERROR,"can't create ObConstRawExpr");
    }
    else
    {
      new_sub_second_const_expr->set_expr_type(old_sub_second_const_expr->get_expr_type());
      new_sub_second_const_expr->set_result_type(old_sub_second_const_expr->get_result_type());
      new_sub_second_const_expr->set_value(old_sub_second_const_expr->get_value());
    }
  }

  //gen new sub third const expr
  ObConstRawExpr *new_sub_third_const_expr = NULL;
  if (OB_SUCCESS == ret)
  {
    if (CREATE_RAW_EXPR(new_sub_third_const_expr, ObConstRawExpr, result_plan) == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(ERROR,"can't create ObConstRawExpr");
    }
    else
    {
      new_sub_third_const_expr->set_expr_type(old_sub_third_const_expr->get_expr_type());
      new_sub_third_const_expr->set_result_type(old_sub_third_const_expr->get_result_type());
      new_sub_third_const_expr->set_value(old_sub_third_const_expr->get_value());
    }
  }

  //gen new main triple expr
  ObTripleOpRawExpr *new_main_triple_expr = NULL;
  if (OB_SUCCESS == ret)
  {
    if (CREATE_RAW_EXPR(new_main_triple_expr, ObTripleOpRawExpr, result_plan) == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(ERROR,"create ObTripleOpRawExpr failed");
    }
    else
    {
      new_main_triple_expr->set_expr_type(old_main_triple_expr->get_expr_type());
      new_main_triple_expr->set_result_type(old_main_triple_expr->get_result_type());
      new_main_triple_expr->set_op_exprs(new_sub_first_ref_expr, new_sub_second_const_expr, new_sub_third_const_expr);

    }
  }

  //save new gen expr
  new_expr = new_main_triple_expr;
  return ret;
}

//add 20140416:e


//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140416:b
/*
 *@author   杜彦荣
 *@brief    函数负责生成ObBinaryOpRawExpr类型的表达式
 *@brief    函数限制暂时只允许被optimize_gen_new_where_expr函数调用
 *@param    old_main_triple_expr 表达式树的根节点类型
 *@param    old_sub_ref_expr 表达式树中的ObBinaryRefRawExpr类型的子节点
 *@param    sub_ref_expr_is_frist 表达式树中ObBinaryRefRawExpr类型的节点是否为第一个子节点
 *@param    new_expr 返回新生成的表达式的指针
 */
int optimize_gen_binary_expr(
    ResultPlan * result_plan,
    const ObBinaryOpRawExpr* old_main_binary_op_expr,
    const ObBinaryRefRawExpr* old_sub_ref_expr,
    const ObConstRawExpr* old_sub_const_expr,
    const bool& sub_ref_expr_is_frist,
    ObRawExpr*& new_expr)
{
  if (NULL == result_plan
      || NULL == old_main_binary_op_expr
      || NULL == old_sub_ref_expr
      || NULL == old_sub_const_expr)
  {
    TBSYS_LOG(ERROR,"function param error!");
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "param of optimize_gen_binary_expr error");
    return OB_INPUT_PARAM_ERROR;
  }

  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  if (logical_plan == NULL)
  {
    ret = OB_ERR_LOGICAL_PLAN_FAILD;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "logical_plan must exist!!!");
    TBSYS_LOG(ERROR,"logical_plan must exist!!!");
  }

  //gen new sub ref expr
  ObBinaryRefRawExpr *new_sub_ref_expr = NULL;
  if (OB_SUCCESS == ret)
  {
    if (CREATE_RAW_EXPR(new_sub_ref_expr, ObBinaryRefRawExpr, result_plan) == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(ERROR,"create ObBinaryRefRawExpr failed");
    }
    else
    {
      new_sub_ref_expr->set_expr_type(old_sub_ref_expr->get_expr_type());
      new_sub_ref_expr->set_result_type(old_sub_ref_expr->get_result_type());
      new_sub_ref_expr->set_first_ref_id(old_sub_ref_expr->get_first_ref_id());
      new_sub_ref_expr->set_second_ref_id(old_sub_ref_expr->get_second_ref_id());
    }
  }

  //gen new sub const expr
  ObConstRawExpr *new_sub_const_expr = NULL;
  if (OB_SUCCESS == ret)
  {
    if (CREATE_RAW_EXPR(new_sub_const_expr, ObConstRawExpr, result_plan) == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(ERROR,"create ObConstRawExpr failed");
    }
    else
    {
      new_sub_const_expr->set_expr_type(old_sub_const_expr->get_expr_type());
      new_sub_const_expr->set_result_type(old_sub_const_expr->get_result_type());
      new_sub_const_expr->set_value(old_sub_const_expr->get_value());
    }
  }

  //gen new main binary op expr
  ObBinaryOpRawExpr* new_main_binary_op_expr = NULL;
  if (OB_SUCCESS == ret)
  {
    if (CREATE_RAW_EXPR(new_main_binary_op_expr, ObBinaryOpRawExpr, result_plan) == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(ERROR,"create ObBinaryOpRawExpr failed");
    }
    else
    {
      new_main_binary_op_expr->set_expr_type(old_main_binary_op_expr->get_expr_type());
      new_main_binary_op_expr->set_result_type(old_main_binary_op_expr->get_result_type());
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (sub_ref_expr_is_frist)
    {
      new_main_binary_op_expr->set_op_exprs(new_sub_ref_expr, new_sub_const_expr);
    }
    else
    {
      new_main_binary_op_expr->set_op_exprs(new_sub_const_expr, new_sub_ref_expr);
    }
  }

  new_expr = new_main_binary_op_expr;
  return ret;
}

//add 20140416:e


//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140512:b
/*
 *@author  杜彦荣
 *@brief   将一个表达式树进行分解，获得相关节点信息
 *@param   source_expr 待分解的表达式
 *@param   sub_ref_exprs_store 保存表达式树中ObBinaryRefRawExpr类型的子表达式的指针
 *@param   sub_const_exprs_store 保存表达式树中ObConstRawExpr类型的字表达式的指针
 *@param   is_triple_expr  返回source_expr是否为ObTripleOpRawExpr类型
 *@param   sub_ref_expr_is_first 返回source_expr中ObBinaryRefRawExpr类型的子表达式是否为第一个子表达式
 *@return   函数操作是否成功的信息
 *@retval   OB_SUCCESS 表示成功，其他值则为失败
 */
int optimize_split_expr(
    ResultPlan * result_plan,
    const ObSqlRawExpr* source_expr,
    ObVector<ObBinaryRefRawExpr*>& sub_ref_exprs_store,
    ObVector<ObConstRawExpr*>& sub_const_exprs_store,
    bool& is_triple_expr,
    bool& sub_ref_expr_is_frist
    )
{
  if ( NULL == result_plan || NULL == source_expr )
  {
    TBSYS_LOG(ERROR,"function param error");
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "param of optimize_split_expr can't be NULL");
    return OB_INPUT_PARAM_ERROR;
  }
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;

  //init
  sub_ref_exprs_store.clear();
  sub_const_exprs_store.clear();
  is_triple_expr = false;
  sub_ref_expr_is_frist = false;

  //暂时只支持这三种类型表达式的分解，后期可扩充，但必须保证各个分支为互斥的
  if (source_expr->get_expr()->is_join_cond())
  {
    ObBinaryOpRawExpr*  join_cnd    = NULL;
    ObBinaryRefRawExpr* join_lexpr  = NULL;
    ObBinaryRefRawExpr* join_rexpr  = NULL;
    join_cnd    = dynamic_cast<ObBinaryOpRawExpr*>(source_expr->get_expr());
    join_lexpr  = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_first_op_expr());
    join_rexpr  = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_second_op_expr());

    if (join_lexpr == NULL || join_rexpr == NULL)
    {
      TBSYS_LOG(WARN,"dynamic_cast fail!");
    }
    else if (OB_SUCCESS != (ret = sub_ref_exprs_store.push_back(join_lexpr)) )
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "optimize split expr error");
      TBSYS_LOG(ERROR,"Add join_lexpr into Vector error");
    }
    else if (OB_SUCCESS != (ret = sub_ref_exprs_store.push_back(join_rexpr)) )
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "optimize split expr error");
      TBSYS_LOG(ERROR,"Add join_rexpr into Vector error");
    }

  }
  else if ( source_expr->get_expr()->is_column_range_filter() )
  {
    ObTripleOpRawExpr*  main_triple_expr    = NULL;
    ObBinaryOpRawExpr*  main_binary_expr    = NULL;
    ObBinaryRefRawExpr* sub_ref_expr        = NULL;
    ObConstRawExpr*     sub_first_const_expr  = NULL;
    ObConstRawExpr*     sub_second_const_expr = NULL;

    //表达式树的类型为ObTripleOpRawExpr
    if (source_expr->get_expr()->get_expr_type() == T_OP_BTW)
    {
      main_triple_expr = dynamic_cast<ObTripleOpRawExpr*>(source_expr->get_expr());
      if (main_triple_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN)
      {
        is_triple_expr = true;
        sub_ref_expr_is_frist = true;
        sub_ref_expr = dynamic_cast<ObBinaryRefRawExpr*>(main_triple_expr->get_first_op_expr());
        sub_first_const_expr  = dynamic_cast<ObConstRawExpr*>(main_triple_expr->get_second_op_expr());
        sub_second_const_expr = dynamic_cast<ObConstRawExpr*>(main_triple_expr->get_third_op_expr());

        if (sub_ref_expr == NULL
            || sub_first_const_expr == NULL
            || sub_second_const_expr == NULL)
        {
          TBSYS_LOG(WARN,"dynamic_cast fail!");
        }
        else if (OB_SUCCESS != (ret = sub_ref_exprs_store.push_back(sub_ref_expr)) )
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "optimize split expr error");
          TBSYS_LOG(ERROR,"Add sub_ref_expr into Vector error");
        }
        else if (OB_SUCCESS != (ret = sub_const_exprs_store.push_back(sub_first_const_expr)) )
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "optimize split expr error");
          TBSYS_LOG(ERROR,"Add sub_first_const_expr into Vector error");
        }
        else if (OB_SUCCESS != (ret = sub_const_exprs_store.push_back(sub_second_const_expr)) )
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "optimize split expr error");
          TBSYS_LOG(ERROR,"Add sub_second_const_expr into Vector error");
        }
      }
    }
    else
    {//表达式树的类型为ObBinaryOpRawExpr
      is_triple_expr = false;
      main_binary_expr = dynamic_cast<ObBinaryOpRawExpr*>(source_expr->get_expr());
      if (main_binary_expr->get_first_op_expr()->is_const()
          && main_binary_expr->get_second_op_expr()->get_expr_type() == T_REF_COLUMN)
      {
        sub_ref_expr_is_frist = false;
        sub_ref_expr = dynamic_cast<ObBinaryRefRawExpr*>(main_binary_expr->get_second_op_expr());
        sub_first_const_expr = dynamic_cast<ObConstRawExpr*>(main_binary_expr->get_first_op_expr());
      }
      else if (main_binary_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN
               && main_binary_expr->get_second_op_expr()->is_const())
      {
        sub_ref_expr_is_frist = true;
        sub_ref_expr = dynamic_cast<ObBinaryRefRawExpr*>(main_binary_expr->get_first_op_expr());
        sub_first_const_expr = dynamic_cast<ObConstRawExpr*>(main_binary_expr->get_second_op_expr());
      }

      if (sub_ref_expr == NULL
          || sub_first_const_expr == NULL)
      {
        TBSYS_LOG(WARN,"dynamic_cast fail!");
      }
      else if (OB_SUCCESS != (ret = sub_ref_exprs_store.push_back(sub_ref_expr)) )
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "optimize split expr error");
        TBSYS_LOG(ERROR,"Add sub_ref_expr into Vector error");
      }
      else if (OB_SUCCESS != (ret = sub_const_exprs_store.push_back(sub_first_const_expr)) )
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "optimize split expr error");
        TBSYS_LOG(ERROR,"Add sub_first_const_expr into Vector error");
      }
    }


  }
  else
  {
    sub_ref_exprs_store.clear();
    sub_const_exprs_store.clear();
    sub_ref_expr_is_frist = false;
    is_triple_expr = false;
  }

  return ret;
}

//add 20140512:e

//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140512:b
//判断两个ObBinaryRefRawExpr类型的表达式是否为同一张表的同一列
bool optimize_ref_expr_equal(
    ObBinaryRefRawExpr* ref_expr_1,
    ObBinaryRefRawExpr* ref_expr_2
    )
{
  bool ret = false;

  if (ref_expr_1 == NULL || ref_expr_2 == NULL)
  {
    TBSYS_LOG(WARN,"can't use NULL expr to decide if equal");
    ret = false;
  }
  else if (ref_expr_1->get_first_ref_id() == ref_expr_2->get_first_ref_id()
           && ref_expr_1->get_second_ref_id() == ref_expr_2->get_second_ref_id())
  {
    ret = true;
  }
  return ret;
}
//add 20140512:e
//add 20140512:e
// add by zcd 20141222:b
// 根据索引表id和索引表名称生成对应的表名
// index_name为索引的名称
// src_tid为原表的tableid
// out_buff为结果名称的保存位置
// buf_size为out_buff的最大长度
// str_len为结果名称的长度
// 拼接规则：__tid__idx__indexname
int generate_index_table_name(ObString index_name, uint64_t src_tid, char *out_buff, int64_t& str_len)
{
  int ret = OB_SUCCESS;
  char str[OB_MAX_TABLE_NAME_LENGTH];
  memset(str,0,OB_MAX_TABLE_NAME_LENGTH);
  if(index_name.length()>=OB_MAX_TABLE_NAME_LENGTH)//mod liumz [name_length]
  {
    TBSYS_LOG(WARN,"buff is not enouth to gen index table name");
    ret=OB_ERROR;
  }
  else
  {
    strncpy(str,index_name.ptr(),index_name.length());
    // add zhangcd [multi_database.seconary_index] 20150721:b
    str[index_name.length()] = 0;
    // add:e
    int wlen = snprintf(out_buff, OB_MAX_TABLE_NAME_LENGTH, "__%ld__idx__%s", src_tid, str);
    if((size_t)wlen >= (size_t)OB_MAX_TABLE_NAME_LENGTH)
    {
      ret = OB_ERR_TABLE_NAME_LENGTH;
    }
    str_len = wlen;
  }

  return ret;
}
// add :e
//add wenghaixing[secondary index create fix]20141226
int generate_expire_col_list(ObString input, ObStrings &out)
{
  int ret = OB_SUCCESS;
  ObExpressionParser parser;
  ObArrayHelper<ObObj> obj_array;
  ObObj sym_list_[OB_MAX_COMPOSITE_SYMBOL_COUNT];
  obj_array.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, sym_list_);
  ObString val;
  int i                 = 0;
  int64_t type          = 0;
  int64_t postfix_size  = 0;
  if(OB_SUCCESS != (ret = (parser.parse(input,obj_array))))
  {
    TBSYS_LOG(ERROR,"generate_expire_col_list parse error,ret[%d]",ret);
  }
  else
  {
    postfix_size=obj_array.get_array_index();
  }
  if(OB_SUCCESS == ret)
  {
    i=0;
    while(i<postfix_size-1)
    {
      if(OB_SUCCESS != obj_array.at(i)->get_int(type))
      {
        TBSYS_LOG(WARN, "unexpected data type. int expected, but actual type is %d",
                  obj_array.at(i)->get_type());
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      else
      {
        if(ObExpression::COLUMN_IDX == type)
        {
          if (OB_SUCCESS != obj_array.at(i+1)->get_varchar(val))
          {
            TBSYS_LOG(WARN, "unexpected data type. varchar expected, "
                      "but actual type is %d",
                      obj_array.at(i+1)->get_type());
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          else
          {
            out.add_string(val);;
          }
        }
      }
      i += 2;
    }
  }
  return ret;
}

//add e
