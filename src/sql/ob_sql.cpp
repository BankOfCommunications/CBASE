/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_values.h"
#include "sql/ob_sql.h"
#include "sql/parse_node.h"
#include "sql/build_plan.h"
#include "sql/ob_transformer.h"
#include "sql/ob_schema_checker.h"
#include "sql/parse_malloc.h"
#include "sql/ob_select_stmt.h"
#include "sql/ob_update_stmt.h"
#include "sql/ob_delete_stmt.h"
#include "common/ob_schema.h"
#include "common/ob_privilege.h"
#include "common/ob_array.h"
#include "common/ob_privilege_type.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_type.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_trace_id.h"
#include "common/ob_encrypted_helper.h"
#include "sql/ob_priv_executor.h"
#include "sql/ob_grant_stmt.h"
#include "sql/ob_create_user_stmt.h"
#include "sql/ob_drop_user_stmt.h"
#include "sql/ob_alter_table_stmt.h"
#include "sql/ob_drop_database_stmt.h"
#include "sql/ob_create_table_stmt.h"
#include "sql/ob_drop_table_stmt.h"
#include "sql/ob_truncate_table_stmt.h" //add zhaoqiong [Truncate Table]:20160318
#include "sql/ob_revoke_stmt.h"
#include "sql/ob_lock_user_stmt.h"
#include "sql/ob_set_password_stmt.h"
#include "sql/ob_rename_user_stmt.h"
#include "sql/ob_show_stmt.h"
#include "common/ob_profile_fill_log.h"
#include "ob_start_trans.h"
#include "ob_end_trans.h"
#include "ob_ups_executor.h"
#include "ob_table_rpc_scan.h"
#include "ob_get_cur_time_phy_operator.h"
//add wenghaixing [secondary index drop index]20141223
#include "ob_drop_index_stmt.h"
//add e
//add liumz, [create index privilege management]20150828:b
#include "ob_create_index_stmt.h"
//add:e
#include "ob_sequence_stmt.h" //add liuzy [sequence] 20150430
using namespace oceanbase::common;
using namespace oceanbase::sql;

template <typename OperatorT>
static int init_hook_env(ObSqlContext &context, ObPhysicalPlan *&phy_plan, OperatorT *&op)
{
  int ret = OB_SUCCESS;
  phy_plan = NULL;
  op = NULL;
  StackAllocator& allocator = context.session_info_->get_transformer_mem_pool();
  void *ptr1 = allocator.alloc(sizeof(ObPhysicalPlan));
  void *ptr2 = allocator.alloc(sizeof(OperatorT));
  if (NULL == ptr1 || NULL == ptr2)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "fail to malloc memory for ObValues, op=%p", op);
  }
  else
  {
    op = new(ptr2) OperatorT();
    phy_plan = new(ptr1) ObPhysicalPlan();
    if (OB_SUCCESS != (ret = phy_plan->store_phy_operator(op)))
    {
      TBSYS_LOG(WARN, "failed to add operator, err=%d", ret);
      op->~OperatorT();
      phy_plan->~ObPhysicalPlan();
    }
    else if (OB_SUCCESS != (ret = phy_plan->add_phy_query(op, NULL, true)))
    {
      TBSYS_LOG(WARN, "failed to add query, err=%d", ret);
      phy_plan->~ObPhysicalPlan();
    }
    else
    {
      ob_inc_phy_operator_stat(op->get_type());
    }
  }
  return ret;
}
//mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160418:b
//int ObSql::direct_execute(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context)
int ObSql::direct_execute(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context, ParseResult* ud_select_parse_result)
//mod gaojt 20160418:e
{
  int ret = OB_SUCCESS;
  result.set_session(context.session_info_);
  // Step special: process some special statment here, like "show warning" etc
  if (OB_UNLIKELY(no_enough_memory()))
  {
    static int count = 0;
    TBSYS_LOG(WARN, "no memory");
    result.set_message("no memory");
    result.set_errcode(OB_ALLOCATE_MEMORY_FAILED);
    if (__sync_fetch_and_add(&count, 1) == 0)
    {
      ob_print_mod_memory_usage();
    }
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (true == process_special_stmt_hook(stmt, result, context))
  {
    if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_TRACE))
    {
      TBSYS_LOG(TRACE, "execute special sql statement success [%.*s]", stmt.length(), stmt.ptr());
    }
  }
  else
  {
    ResultPlan result_plan;
    ObMultiPhyPlan multi_phy_plan;
    ObMultiLogicPlan *multi_logic_plan = NULL;
    ObLogicalPlan *logic_plan = NULL;
    result_plan.is_prepare_ = context.is_prepare_protocol_ ? 1 : 0;
    //result.set_query_string(stmt);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "store cur query string failed ret=%d", ret);
    }
    else
    {
      //mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160418:b
//      ret = generate_logical_plan(stmt, context, result_plan, result);
      if (ud_select_parse_result)
      {
          ret = generate_logical_plan_ud(stmt,*ud_select_parse_result, context, result_plan, result);
      }
      else
      {
          ret = generate_logical_plan(stmt, context, result_plan, result);
      }
      //mod gaojt 20160418:e
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "generate logical plan failed, ret=%d sql=%.*s", ret, stmt.length(), stmt.ptr());
      }
      else
      {
        TBSYS_LOG(DEBUG, "generate logical plan succ");
        multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
        logic_plan = multi_logic_plan->at(0);
        if (!context.disable_privilege_check_)
        {
          ret = do_privilege_check(context.session_info_->get_user_name(), context.pp_privilege_, logic_plan);
          if (OB_SUCCESS != ret)
          {
            result.set_message("no privilege");
            TBSYS_LOG(WARN, "no privilege,sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          ObStmt *dml_stmt = dynamic_cast<ObStmt*>(logic_plan->get_main_stmt());
          if (dml_stmt)
          {
            result.set_compound_stmt(dml_stmt->get_when_fun_size() > 0);
          }
          ObBasicStmt::StmtType stmt_type = logic_plan->get_main_stmt()->get_stmt_type();
          result.set_stmt_type(stmt_type);
          result.set_inner_stmt_type(stmt_type);
          if (NULL == context.transformer_allocator_)
          {
            OB_ASSERT(!context.is_prepare_protocol_);
            context.transformer_allocator_ = &context.session_info_->get_transformer_mem_pool();
          }
          if (OB_SUCCESS != (ret = logic_plan->fill_result_set(result, context.session_info_,
                                                               *context.transformer_allocator_)))
          {
            TBSYS_LOG(WARN, "fill result set failed,ret=%d", ret);
          }
          else if(OB_SUCCESS != (ret = generate_physical_plan(context, result_plan, multi_phy_plan, result)))
          {
            TBSYS_LOG(WARN, "generate physical plan failed, ret=%d", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "generete physical plan success, sql=%.*s", stmt.length(), stmt.ptr());
            ObPhysicalPlan *phy_plan = multi_phy_plan.at(0);
            OB_ASSERT(phy_plan);
            if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_TRACE))
            {
              TBSYS_LOG(TRACE, "ExecutionPlan: \n%s", to_cstring(*phy_plan));
            }
            result.set_physical_plan(phy_plan, true);
            multi_phy_plan.clear();
          }
        }
        clean_result_plan(result_plan);
      }
    }
  }
  result.set_errcode(ret);
  return ret;
}

int ObSql::generate_logical_plan_ud(const ObString &stmt, ParseResult ud_select_parse_result, ObSqlContext & context, ResultPlan  &result_plan, ObResultSet & result)
{
    int ret = OB_SUCCESS;
    common::ObStringBuf &parser_mem_pool = context.session_info_->get_parser_mem_pool();
//    ParseResult parse_result;
    static const int MAX_ERROR_LENGTH = 80;

//    parse_result.malloc_pool_ = &parser_mem_pool;
//    if (0 != (ret = parse_init(&parse_result)))
//    {
//      TBSYS_LOG(WARN, "parser init err, err=%s", strerror(errno));
//      ret = OB_ERR_PARSER_INIT;
//    }
//    else
//    {
      // generate syntax tree
      PFILL_ITEM_START(sql_to_logicalplan);
      FILL_TRACE_LOG("before_parse");
      if (NULL == ud_select_parse_result.result_tree_)
      {
        TBSYS_LOG(WARN, "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], line_[%d], yycolumn[%d], yylineno_[%d]",
            ud_select_parse_result.yyscan_info_,
            ud_select_parse_result.result_tree_,
            ud_select_parse_result.malloc_pool_,
            ud_select_parse_result.error_msg_,
            ud_select_parse_result.start_col_,
            ud_select_parse_result.end_col_,
            ud_select_parse_result.line_,
            ud_select_parse_result.yycolumn_,
            ud_select_parse_result.yylineno_);

        int64_t error_length = min(stmt.length() - (ud_select_parse_result.start_col_ - 1), MAX_ERROR_LENGTH);
        snprintf(ud_select_parse_result.error_msg_, MAX_ERROR_MSG,
            "You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use near '%.*s' at line %d", static_cast<int32_t>(error_length), stmt.ptr() + ud_select_parse_result.start_col_ - 1, ud_select_parse_result.line_);
        result.set_message(ud_select_parse_result.error_msg_);
        ret = OB_ERR_PARSE_SQL;
      }
      else if (NULL == context.schema_manager_)
      {
        TBSYS_LOG(WARN, "context.schema_manager_ is null");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        FILL_TRACE_LOG("parse");
        result_plan.name_pool_ = &parser_mem_pool;
        ObSchemaChecker *schema_checker =  (ObSchemaChecker*)parse_malloc(sizeof(ObSchemaChecker), result_plan.name_pool_);
        if (NULL == schema_checker)
        {
          TBSYS_LOG(WARN, "out of memory");
          ret = OB_ERR_PARSER_MALLOC_FAILED;
        }
        else
        {
          schema_checker->set_schema(*context.schema_manager_);
          result_plan.schema_checker_ = schema_checker;
          result_plan.plan_tree_ = NULL;
          // generate logical plan
          //add liumz, [multi_database.sql]:20150613
          if (NULL == context.session_info_)
          {
            ret = OB_ERR_SESSION_UNSET;
          }
          //add liuzy [sequence] 20150430:b
          else if(ud_select_parse_result.result_tree_->children_[0]->type_ == T_SEQUENCE_ALTER)
          {
            if (OB_SUCCESS != (ret = sequence_alter_rebuild_parse(context, ud_select_parse_result, result_plan, result)))
            {
              TBSYS_LOG(ERROR, "Alter sequence falied.");
            }
          }
          //20150430:e
          else
          {
            result_plan.session_info_ = context.session_info_;
            //add:e
            ret = resolve(&result_plan, ud_select_parse_result.result_tree_);
            PFILL_ITEM_END(sql_to_logicalplan);
            FILL_TRACE_LOG("resolve");
          }
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to generate logical plan, err=%d sql=%.*s result_plan.err_stat_.err_msg_=[%s]",
                ret, stmt.length(), stmt.ptr(), result_plan.err_stat_.err_msg_);
            result.set_message(result_plan.err_stat_.err_msg_);
          }
          else
          {
            ObMultiLogicPlan *multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
            ////TODO 为了prepare stmtname from select from test a 也使用psstore 需要记录下绑定的语句
            //ObLogicalPlan *logical_plan = NULL;
            //for (int32_t i = 0; ret = OB_SUCCESS && i < multi_logic_plan.size(); ++i)
            //{
            //  if (T_PREPARE == login_plan->get_main_stmt()->get_stmt_type())
            //  {
            //    //TODO
            //    //log sql to ob_prepare_stmt >>>> ob_prepare构造执行计划的时候可以去 ObPsStore里面找 在session上保存一个stmt_name->sql id的映射
            //    //ob_prepare  ob_execute连个操作符号需要修改
            //  }
            //}
            if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG))
            {
              multi_logic_plan->print();
            }
          }
        }
      }
//    }
    // destroy syntax tree
    destroy_tree(ud_select_parse_result.result_tree_);
    parse_terminate(&ud_select_parse_result);
    result.set_errcode(ret);
    return ret;
}

int ObSql::generate_logical_plan(const common::ObString &stmt, ObSqlContext & context, ResultPlan  &result_plan, ObResultSet & result)
{
  int ret = OB_SUCCESS;
  common::ObStringBuf &parser_mem_pool = context.session_info_->get_parser_mem_pool();
  ParseResult parse_result;
  static const int MAX_ERROR_LENGTH = 80;

  parse_result.malloc_pool_ = &parser_mem_pool;
  if (0 != (ret = parse_init(&parse_result)))
  {
    TBSYS_LOG(WARN, "parser init err, err=%s", strerror(errno));
    ret = OB_ERR_PARSER_INIT;
  }
  else
  {
    // generate syntax tree
    PFILL_ITEM_START(sql_to_logicalplan);
    FILL_TRACE_LOG("before_parse");
    if (parse_sql(&parse_result, stmt.ptr(), static_cast<size_t>(stmt.length())) != 0
      || NULL == parse_result.result_tree_)
    {
      TBSYS_LOG(WARN, "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], line_[%d], yycolumn[%d], yylineno_[%d]",
          parse_result.yyscan_info_,
          parse_result.result_tree_,
          parse_result.malloc_pool_,
          parse_result.error_msg_,
          parse_result.start_col_,
          parse_result.end_col_,
          parse_result.line_,
          parse_result.yycolumn_,
          parse_result.yylineno_);

      int64_t error_length = min(stmt.length() - (parse_result.start_col_ - 1), MAX_ERROR_LENGTH);
      snprintf(parse_result.error_msg_, MAX_ERROR_MSG,
          "You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use near '%.*s' at line %d", static_cast<int32_t>(error_length), stmt.ptr() + parse_result.start_col_ - 1, parse_result.line_);
      TBSYS_LOG(WARN, "failed to parse sql=%.*s err=%s", stmt.length(), stmt.ptr(), parse_result.error_msg_);
      result.set_message(parse_result.error_msg_);
      ret = OB_ERR_PARSE_SQL;
    }
    else if (NULL == context.schema_manager_)
    {
      TBSYS_LOG(WARN, "context.schema_manager_ is null");
      ret = OB_ERR_UNEXPECTED;
    }
    else
    {
      FILL_TRACE_LOG("parse");
      result_plan.name_pool_ = &parser_mem_pool;
      ObSchemaChecker *schema_checker =  (ObSchemaChecker*)parse_malloc(sizeof(ObSchemaChecker), result_plan.name_pool_);
      if (NULL == schema_checker)
      {
        TBSYS_LOG(WARN, "out of memory");
        ret = OB_ERR_PARSER_MALLOC_FAILED;
      }
      else
      {
        schema_checker->set_schema(*context.schema_manager_);
        result_plan.schema_checker_ = schema_checker;
        result_plan.plan_tree_ = NULL;
        // generate logical plan
        //add liumz, [multi_database.sql]:20150613
        if (NULL == context.session_info_)
        {
          ret = OB_ERR_SESSION_UNSET;
        }
        //add liuzy [sequence] 20150430:b
        else if(parse_result.result_tree_->children_[0]->type_ == T_SEQUENCE_ALTER)
        {
          if (OB_SUCCESS != (ret = sequence_alter_rebuild_parse(context, parse_result, result_plan, result)))
          {
            TBSYS_LOG(ERROR, "Alter sequence falied.");
          }
        }
        //20150430:e
        else
        {
          result_plan.session_info_ = context.session_info_;
          //add:e
          ret = resolve(&result_plan, parse_result.result_tree_);
          PFILL_ITEM_END(sql_to_logicalplan);
          FILL_TRACE_LOG("resolve");
        }
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to generate logical plan, err=%d sql=%.*s result_plan.err_stat_.err_msg_=[%s]",
              ret, stmt.length(), stmt.ptr(), result_plan.err_stat_.err_msg_);
          result.set_message(result_plan.err_stat_.err_msg_);
        }
        else
        {
          ObMultiLogicPlan *multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
          ////TODO 为了prepare stmtname from select from test a 也使用psstore 需要记录下绑定的语句
          //ObLogicalPlan *logical_plan = NULL;
          //for (int32_t i = 0; ret = OB_SUCCESS && i < multi_logic_plan.size(); ++i)
          //{
          //  if (T_PREPARE == login_plan->get_main_stmt()->get_stmt_type())
          //  {
          //    //TODO
          //    //log sql to ob_prepare_stmt >>>> ob_prepare构造执行计划的时候可以去 ObPsStore里面找 在session上保存一个stmt_name->sql id的映射
          //    //ob_prepare  ob_execute连个操作符号需要修改
          //  }
          //}
          if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG))
          {
            multi_logic_plan->print();
          }
        }
      }
    }
  }
  // destroy syntax tree
  destroy_tree(parse_result.result_tree_);
  parse_terminate(&parse_result);
  result.set_errcode(ret);
  return ret;
}

void ObSql::clean_result_plan(ResultPlan &result_plan)
{
  ObMultiLogicPlan *multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
  multi_logic_plan->~ObMultiLogicPlan();
  destroy_plan(&result_plan);
}

int ObSql::generate_physical_plan(ObSqlContext & context, ResultPlan &result_plan, ObMultiPhyPlan & multi_phy_plan, ObResultSet & result)
{
  int ret = OB_SUCCESS;
  //mod peiouya  [NotNULL_check] [JHOBv0.1] 20131222:b
  /*expr: it is convenient to use if context is added to transformer construct*/
  //ObTransformer trans(context);
  ObTransformer trans(context,result); 
  //mod 20131222:e
  ErrStat err_stat;
  //add liuzy [bugfix] [sequence messy error code] 20160621:b
  memset(err_stat.err_msg_, 0, MAX_ERROR_MSG);
  //add 20160621:e
  ObMultiLogicPlan *multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
  PFILL_ITEM_START(logicalplan_to_physicalplan);
  if (OB_SUCCESS != (ret = trans.generate_physical_plans(*multi_logic_plan, multi_phy_plan, err_stat)))
  {
      TBSYS_LOG(WARN, "failed to transform to physical plan");
      result.set_message(err_stat.err_msg_);
  }
  else
  {
    PFILL_ITEM_END(logicalplan_to_physicalplan);
    for (int32_t i = 0; i < multi_phy_plan.size(); ++i)
    {
      multi_phy_plan.at(i)->set_result_set(&result);
    }
  }
  return ret;
}

int ObSql::stmt_prepare(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  bool do_prepare = false;
  ObPsStoreItem * item = NULL;
  ObPsStore *store = context.ps_store_;
  if (NULL == store)
  {
    TBSYS_LOG(ERROR, "ObPsStore in context is null");
    ret = OB_ERROR;
  }
  else
  {
    ret = store->get(stmt, item);  // rlock  防止在close得到写锁的时候还有线程可以得到这个item
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "stmt_prepare failed when get ObPsStoreItem ret=%d sql=\'%.*s\'", ret,
                stmt.length(), stmt.ptr());
    }
    else
    {
      item->unlock();
      result.set_stmt_type(ObBasicStmt::T_PREPARE);
      if (PS_ITEM_VALID == item->get_status())
      {
        TBSYS_LOG(DEBUG, "Get ObPsStore success stmt is %.*s ref count is %ld", stmt.length(),
                  stmt.ptr(), item->get_ps_count());
        if (need_rebuild_plan(context.schema_manager_, item))
        {
          //get wlock of item free old plan construct new plan if possible
          ret = try_rebuild_plan(stmt, result, context, item, do_prepare, false);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "can not rebuild prepare ret=%d", ret);
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "Latest table schema is same with phyplan in ObPsStore");
        }
        //END check schema
      }
      else if (PS_ITEM_INVALID == item->get_status())
      {
        TBSYS_LOG(DEBUG, "Get ObPsStore success but status PS_ITEM_INVALID stmt is %.*s ref count is %ld", stmt.length(),
                  stmt.ptr(), item->get_ps_count());
        //构造执行计划 copy data into psstore  result里面plan的内存还是用原来的方式管理
        ret = do_real_prepare(stmt, result, context, item, do_prepare);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "do_real_prepare failed, stmt is %.*s ret=%d", stmt.length(), stmt.ptr(), ret);
        }
      }
    }

    //read physical plan and copy data needed into result add read lock
    if (!do_prepare && OB_SUCCESS == ret)
    {
      ret = store->get_plan(item->get_sql_id(), item);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "never reach here ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "Get plan succ sql_id is %ld, ref count is %ld",
                  item->get_sql_id(), item->get_ps_count());
        uint64_t new_stmt_id = 0;
        if (OB_SUCCESS != (ret = context.session_info_->store_ps_session_info(item, new_stmt_id)))
        {
          TBSYS_LOG(WARN, "Store result set to session failed ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = copy_plan_from_store(&result, item, &context, new_stmt_id)))
        {
          TBSYS_LOG(WARN, "copy plan to ObPsStore failed ret=%d", ret);
        }
        result.set_plan_from_assign(true);
        result.set_statement_id(new_stmt_id);
        result.set_sql_id(item->get_sql_id());
        item->unlock(); //unlock whaterver
      }
    }
  }
  result.set_errcode(ret);
  return ret;
}

int ObSql::do_real_prepare(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context, ObPsStoreItem *item, bool &do_prepare)
{
  int ret = OB_SUCCESS;
  do_prepare = false;
  int64_t trywcount = 0;
  while(OB_SUCCESS == ret)
  {
    trywcount++;
    if (OB_UNLIKELY(0 == trywcount % 100))
    {
      TBSYS_LOG(ERROR, "try wlock when do real prepare %ld times sql=\"%.*s\"stmt_id=%ld", trywcount,
                stmt.length(), stmt.ptr(), item->get_sql_id());
    }
    if (item->is_valid())
    {
      ret = OB_SUCCESS;
      break;
    }
    else
    {
      if (0 == item->try_wlock())
      {
        //do prepare  result的内存在拷贝到ObPsStore 并返回数据给客户端后可以释放掉 这里使用的是pagearena
        TBSYS_LOG(DEBUG, "do real prepare %.*s stmt_id=%ld", stmt.length(), stmt.ptr(), item->get_sql_id());
        do_prepare = true;
        result.set_statement_id(item->get_sql_id()); //stmt_id equals sql_id when sql first prepared in one session
        result.set_sql_id(item->get_sql_id()); //stmt_id equals sql_id when sql first prepared in one session
        result.set_session(context.session_info_);
        ObArenaAllocator *allocator = NULL;
        context.is_prepare_protocol_ = true;
        if (OB_UNLIKELY(no_enough_memory()))
        {
          TBSYS_LOG(WARN, "no memory");
          result.set_message("no memory");
          result.set_errcode(OB_ALLOCATE_MEMORY_FAILED);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == context.session_info_)
        {
          TBSYS_LOG(WARN, "context.session_info_(null)");
          ret = OB_NOT_INIT;
        }
        else if (NULL == (allocator = context.session_info_->get_transformer_mem_pool_for_ps()))
        {
          TBSYS_LOG(WARN, "failed to get new allocator");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          OB_ASSERT(NULL == context.transformer_allocator_);
          context.transformer_allocator_ = allocator;
          // the transformer's allocator is owned by the result set now, and will be free by the result set
          result.set_ps_transformer_allocator(allocator);

          if (OB_SUCCESS != (ret = direct_execute(stmt, result, context)))
          {
            TBSYS_LOG(WARN, "direct execute failed");
          }
          else if (OB_SUCCESS != (ret = context.session_info_->store_ps_session_info(result)))
          {
            TBSYS_LOG(WARN, "Store result set to session failed");
          }
        }

        if (OB_SUCCESS == ret)
        {
          result.set_stmt_type(ObBasicStmt::T_PREPARE);
          result.set_errcode(ret);
          item->store_ps_sql(stmt);
          TBSYS_LOG(DEBUG, "ExecutionPlan in result: \n%s", to_cstring(*result.get_physical_plan()));
          //copy field/plan from resultset to item
          ret = copy_plan_to_store(&result, item);
          TBSYS_LOG(DEBUG, "ExecutionPlan in item: \n%s", to_cstring(item->get_physical_plan()));
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "copy phy paln from resultset to PbPsStoreItem failed ret=%d", ret);
            //assume prepare success will redo real_prepare next and copy to ObPsStore
            ret = OB_SUCCESS;
          }
          else
          {
            item->set_schema_version(context.schema_manager_->get_version());//for check schema
            item->set_status(PS_ITEM_VALID);
          }
        }
        else //build plan failed dec ps count beacuse ps count inc when get ObPsStoreItem
        {
          item->dec_ps_count();
        }
        item->unlock();
        break;
      }
    }
  }
  return ret;
}

int ObSql::stmt_execute(const uint64_t stmt_id,
                        const common::ObIArray<obmysql::EMySQLFieldType> &params_type,
                        const common::ObIArray<common::ObObj> &params,
                        ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  bool rebuild_plan = false;
  uint64_t inner_stmt_id = stmt_id;
  result.set_session(context.session_info_);
  ObPsStoreItem *item = NULL;
  ObPsSessionInfo *info = NULL;
  //add peiouya [NotNULL_check] [JHOBv0.1] 20140306:b
  int64_t para_idx = 0;
  //add 20140306:e
  if (OB_UNLIKELY(no_enough_memory()))
  {
    static int count = 0;
    if (__sync_fetch_and_add(&count, 1) == 0)
    {
      ob_print_mod_memory_usage();
    }
    TBSYS_LOG(WARN, "no memory");
    result.set_message("no memory");
    result.set_errcode(OB_ALLOCATE_MEMORY_FAILED);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (NULL == context.session_info_)
  {
    TBSYS_LOG(WARN, "context.session_info_(null)");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = context.session_info_->get_ps_session_info(stmt_id, info)))
  {
    TBSYS_LOG(WARN, "can not get ObPsSessionInfo from id_psinfo_map stmt_id = %ld, ret=%d", stmt_id, ret);
  }
  else
  {
    result.set_sql_id(info->sql_id_);
  }

  if (OB_SUCCESS == ret)
  {
    inner_stmt_id = info->sql_id_;
    if(OB_SUCCESS != context.ps_store_->get_plan(inner_stmt_id, item))
    {
      ret = OB_ERR_PREPARE_STMT_UNKNOWN;
      TBSYS_LOG(USER_ERROR, "statement not prepared, stmt_id=%lu ret=%d", inner_stmt_id, ret);
    }
    else
    {
      //check schema
      if (need_rebuild_plan(context.schema_manager_, item))
      {
        item->unlock();
        ret = try_rebuild_plan(item->get_ps_sql(), result, context, item, rebuild_plan, true);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "rebuild plan failed stmt_id=%lu, schema_version=%ld", inner_stmt_id, item->get_schema_version());
          //ret = stmt_close(inner_stmt_id, context);
        }
        else
        {
          TBSYS_LOG(DEBUG, "rebuild plan success stmt_id=%lu, schema_version=%ld", inner_stmt_id, item->get_schema_version());
        }
      }
      //end check schema
      else
      {
        if (OB_SUCCESS != (ret=copy_plan_from_store(&result, item, &context, stmt_id)))
        {
          TBSYS_LOG(ERROR, "Copy plan from ObPsStore to  result set failed ret=%d", ret);
        }
        item->unlock();           // rlocked when get_plan
      }
      if (OB_SUCCESS == ret)
      {
        //for show processlist do_com_execute show ori prepare sql
        if(OB_SUCCESS != context.session_info_->store_query_string(item->get_ps_sql()))
        {
          TBSYS_LOG(WARN, "failed to store cur query string ret=%d", ret);
        }
        else if(OB_SUCCESS != context.session_info_->store_params_type(stmt_id, params_type))
        {
          ret = OB_ERR_PREPARE_STMT_UNKNOWN;
          TBSYS_LOG(USER_ERROR, "statement not prepared, stmt_id=%lu ret=%d", stmt_id, ret);
        }
        // set running param values
		//mod peiouya [NotNULL_check] [JHOBv0.1] 20140306:b
		//else if (OB_SUCCESS != (ret = result.fill_params(params_type, params)))
        else if (OB_SUCCESS != (ret = result.fill_params(params_type, params,para_idx)))
		//mod 20130306:e
        {
          TBSYS_LOG(WARN, "Incorrect arguments to EXECUTE ret=%d", ret);
        }
        else
        {
          if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_TRACE))
          {
            TBSYS_LOG(TRACE, "ExecutionPlan: \n%s", to_cstring(*result.get_physical_plan()->get_main_query()));
          }
        }
        result.set_statement_id(stmt_id);
        if (!rebuild_plan)
        {
          result.set_plan_from_assign(true);
        }
        TBSYS_LOG(DEBUG, "stmt execute end print field info");
        //TBSYS_LOG(DEBUG, "%s", to_cstring(result.get_field_columns()));
        result.set_stmt_type(ObBasicStmt::T_NONE);
        //store errcode in resultset
        result.set_errcode(ret);
      }
    }
  }
  result.set_errcode(ret);
  return ret;
}

int ObSql::stmt_close(const uint64_t stmt_id, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  uint64_t inner_sql_id = stmt_id;
  ObPsSessionInfo *info = NULL;
  if (NULL == context.session_info_)
  {
    TBSYS_LOG(WARN, "context.session_info_(null)");
    ret = OB_NOT_INIT;
  }
  else if(OB_SUCCESS != (ret = context.session_info_->get_ps_session_info(stmt_id, info)))
  {
    TBSYS_LOG(WARN, "can not get ObPsSessionInfo from id_psinfo_map stmt_id = %ld, ret=%d", stmt_id, ret);
  }
  if (OB_SUCCESS == ret)
  {
    inner_sql_id = info->sql_id_;
    if(OB_SUCCESS != (ret = context.ps_store_->remove_plan(inner_sql_id))) //从全局池里面删除执行计划
    {
      TBSYS_LOG(WARN, "close prepared statement failed, sql_id=%lu", inner_sql_id);
    }
    else if(OB_SUCCESS != (ret = context.session_info_->remove_ps_session_info(stmt_id)))//删除session中stmtid对应的ObPsSessionInfo
    {
      TBSYS_LOG(WARN, "remove ps session info failed, stmt_id=%lu", stmt_id);
    }
    else
    {
      TBSYS_LOG(DEBUG, "close stmt, stmt_id=%lu sql_id=%ld", stmt_id, inner_sql_id);
    }
  }
  return ret;
}

bool ObSql::process_special_stmt_hook(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  const char *select_collation = (const char *)"SHOW COLLATION";
  int64_t select_collation_len = strlen(select_collation);
  const char *show_charset = (const char *)"SHOW CHARACTER SET";
  int64_t show_charset_len = strlen(show_charset);
  // SET NAMES latin1/gb2312/utf8/etc...
  const char *set_names = (const char *)"SET NAMES ";
  int64_t set_names_len = strlen(set_names);
  const char *set_session_transaction_isolation = (const char *)"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED";
  int64_t set_session_transaction_isolation_len = strlen(set_session_transaction_isolation);

  ObRow row;
  ObRowDesc row_desc;
  ObValues *op = NULL;
  ObPhysicalPlan *phy_plan = NULL;

  if (stmt.length() >= select_collation_len && 0 == strncasecmp(stmt.ptr(), select_collation, select_collation_len))
  {
    /*
       mysql> SHOW COLLATION;
        +------------------------+----------+-----+---------+----------+---------+
        | Collation              | Charset  | Id  | Default | Compiled | Sortlen |
        +------------------------+----------+-----+---------+----------+---------+
        | big5_chinese_ci    | big5       |   1 | Yes       | Yes         |       1 |
        | ...                        | ...          |  ... |   ...      | ...            |       ... |
       +------------------------+----------+-----+---------+----------+---------+
       */
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {

      // construct table header
      ObResultSet::Field field;
      ObString tname = ObString::make_string("tmp_table");
      field.tname_ = tname;
      field.org_tname_ = tname;
      ObString cname[6];
      cname[0] = ObString::make_string("Collation");
      cname[1] = ObString::make_string("Charset");
      cname[2] = ObString::make_string("Id");
      cname[3] = ObString::make_string("Default");
      cname[4] = ObString::make_string("Compiled");
      cname[5] = ObString::make_string("Sortlen");
      ObObjType type[6];
      type[0] = ObVarcharType;
      type[1] = ObVarcharType;
      type[2] = ObIntType;
      type[3] = ObVarcharType;
      type[4] = ObVarcharType;
      type[5] = ObIntType;
      for (int i = 0; i < 6; i++)
      {
        field.cname_ = cname[i];
        field.org_cname_ = cname[i];
        field.type_.set_type(type[i]);
        if (OB_SUCCESS != (ret = result.add_field_column(field)))
        {
          TBSYS_LOG(WARN, "fail to add field column %d", i);
          break;
        }
      }

      // construct table body
      for (int i = 0; i < 6; ++i)
      {
        ret = row_desc.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      row.set_row_desc(row_desc);
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
      // | binary               | binary   |  63 | Yes     | Yes      |       1 |
      ObObj cells[6];
      ObString cell0 = ObString::make_string("binary");
      cells[0].set_varchar(cell0);
      ObString cell1 = ObString::make_string("binary");
      cells[1].set_varchar(cell1);
      cells[2].set_int(63);
      ObString cell3 = ObString::make_string("Yes");
      cells[3].set_varchar(cell3);
      ObString cell4 = ObString::make_string("Yes");
      cells[4].set_varchar(cell4);
      cells[5].set_int(1);
      ObRow one_row;
      one_row.set_row_desc(row_desc);
      for (int i = 0; i < 6; ++i)
      {
        ret = one_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i, cells[i]);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      ret = op->add_values(one_row);
      OB_ASSERT(OB_SUCCESS == ret);
    }
  }
  else if (stmt.length() >= show_charset_len && 0 == strncasecmp(stmt.ptr(), show_charset, show_charset_len))
  {
    /*
       mysql> SHOW CHARACTER SET
       +----------+-----------------------------+---------------------+--------+
       | Charset  | Description                 | Default collation   | Maxlen |
       +----------+-----------------------------+---------------------+--------+
       | binary   | Binary pseudo charset       | binary              |      1 |
       +----------+-----------------------------+---------------------+--------+
    */
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {

      // construct table header
      ObResultSet::Field field;
      ObString tname = ObString::make_string("tmp_table");
      field.tname_ = tname;
      field.org_tname_ = tname;
      ObString cname[4];
      cname[0] = ObString::make_string("Charset");
      cname[1] = ObString::make_string("Description");
      cname[2] = ObString::make_string("Default collation");
      cname[3] = ObString::make_string("Maxlen");
      ObObjType type[4];
      type[0] = ObVarcharType;
      type[1] = ObVarcharType;
      type[2] = ObVarcharType;
      type[3] = ObIntType;
      for (int i = 0; i < 4; i++)
      {
        field.cname_ = cname[i];
        field.org_cname_ = cname[i];
        field.type_.set_type(type[i]);
        if (OB_SUCCESS != (ret = result.add_field_column(field)))
        {
          TBSYS_LOG(WARN, "fail to add field column %d", i);
          break;
        }
      }

      // construct table body
      for (int i = 0; i < 4; ++i)
      {
        ret = row_desc.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      row.set_row_desc(row_desc);
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
      // | binary   | Binary pseudo charset       | binary              |      1 |
      ObObj cells[4];
      ObString cell0 = ObString::make_string("binary");
      cells[0].set_varchar(cell0);
      ObString cell1 = ObString::make_string("Binary pseudo charset");
      cells[1].set_varchar(cell1);
      ObString cell2 = ObString::make_string("binary");
      cells[2].set_varchar(cell2);
      cells[3].set_int(1);
      ObRow one_row;
      one_row.set_row_desc(row_desc);
      for (int i = 0; i < 4; ++i)
      {
        ret = one_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i, cells[i]);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      ret = op->add_values(one_row);
      OB_ASSERT(OB_SUCCESS == ret);
    }
  }
  else if (stmt.length() >= set_names_len && 0 == strncasecmp(stmt.ptr(), set_names, set_names_len))
  {
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {
      // SET NAMES ...
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
    }
  }
  else if (stmt.length() >= set_session_transaction_isolation_len &&
      0 == strncasecmp(stmt.ptr(), set_session_transaction_isolation, set_session_transaction_isolation_len))
  {
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {
      // SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
    }
  }
  else
  {
    ret = OB_NOT_SUPPORTED;
  }

  if (OB_SUCCESS != ret)
  {
    if (NULL != phy_plan)
    {
      phy_plan->~ObPhysicalPlan(); // will destruct op automatically
    }
  }
  else
  {
    result.set_physical_plan(phy_plan, true);
  }
  return (OB_SUCCESS == ret);
}

int ObSql::do_privilege_check(const ObString & username, const ObPrivilege **pp_privilege, ObLogicalPlan *plan)
{
  int err = OB_SUCCESS;
  ObBasicStmt *stmt = NULL;
  ObArray<ObPrivilege::TablePrivilege> table_privileges;
  bool is_global_priv = false;
  for (int32_t i = 0;i < plan->get_stmts_count(); ++i)
  {
    stmt = plan->get_stmt(i);
    switch (stmt->get_stmt_type())
    {
      case ObBasicStmt::T_SELECT:
        {
          ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
          if (OB_UNLIKELY(NULL == select_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObSelectStmt::SetOperator set_op  = select_stmt->get_set_op();
            if (set_op != ObSelectStmt::NONE)
            {
              continue;
            }
            else
            {
              int32_t table_item_size = select_stmt->get_table_size();
              //mod liumz, [UT_bugfix]20150813:b
              //for (int32_t j = 0; j < table_item_size; ++j)
              for (int32_t j = 0; j < table_item_size && OB_LIKELY(OB_SUCCESS == err); ++j)
              //mod:e
              {
                ObPrivilege::TablePrivilege table_privilege;
                const TableItem &table_item = select_stmt->get_table_item(j);
                uint64_t table_id = OB_INVALID_ID;
                ObString db_name;//add liumz, [multi_database.priv_management.check_privilege]:20150615
                if (table_item.type_ == TableItem::BASE_TABLE || table_item.type_ == TableItem::ALIAS_TABLE)
                {
                  table_id = table_item.ref_id_;
                  db_name = table_item.db_name_;//add liumz, [multi_database.priv_management.check_privilege]:20150615
                }
                else
                {
                  continue;
                }
                table_privilege.table_id_ = table_id;
                table_privilege.db_name_ = db_name;//add liumz, [multi_database.priv_management.check_privilege]:20150615
                OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_SELECT));
                err = table_privileges.push_back(table_privilege);
                if (OB_UNLIKELY(OB_SUCCESS != err))
                {
                  TBSYS_LOG(WARN, "push table_privilege to array failed,err=%d", err);
                }
              }
            }
          }
          break;
        }
      case ObBasicStmt::T_INSERT:
      case ObBasicStmt::T_REPLACE:
        {
          ObInsertStmt *insert_stmt = dynamic_cast<ObInsertStmt*>(stmt);
          if (OB_UNLIKELY(NULL == insert_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            //add liumz, [multi_database.priv_management.check_privilege]:20150615:b
            const TableItem &table_item = insert_stmt->get_table_item(0);
            table_privilege.db_name_ = table_item.db_name_;
            //add:e
            table_privilege.table_id_ = insert_stmt->get_table_id();
            if (!insert_stmt->is_replace())
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_INSERT));
            }
            else
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_REPLACE));
            }
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_UPDATE:
        {
          ObUpdateStmt *update_stmt = dynamic_cast<ObUpdateStmt*>(stmt);
          if (OB_UNLIKELY(NULL == update_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            //add liumz, [multi_database.priv_management.check_privilege]:20150615:b
            const TableItem &table_item = update_stmt->get_table_item(0);
            table_privilege.db_name_ = table_item.db_name_;
            //add:e
            table_privilege.table_id_ = update_stmt->get_update_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_DELETE:
        {
          ObDeleteStmt *delete_stmt = dynamic_cast<ObDeleteStmt*>(stmt);
          if (OB_UNLIKELY(NULL == delete_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            //add liumz, [multi_database.priv_management.check_privilege]:20150615:b
            const TableItem &table_item = delete_stmt->get_table_item(0);
            table_privilege.db_name_ = table_item.db_name_;
            //add:e
            table_privilege.table_id_ = delete_stmt->get_delete_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_DELETE));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_GRANT:
        {
          OB_STAT_INC(SQL, SQL_GRANT_PRIVILEGE_COUNT);

          ObGrantStmt *grant_stmt = dynamic_cast<ObGrantStmt*>(stmt);
          if (OB_UNLIKELY(NULL == grant_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            // if grant priv_xx* on * then table_id == 0
            // if grant priv_xx* on table_name
            //add liumz, [multi_database.priv_management.check_privilege]:20150610:b
            is_global_priv = grant_stmt->is_global_priv();
            table_privilege.db_name_ = grant_stmt->get_db_name();
            //add:e
            table_privilege.table_id_ = grant_stmt->get_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_GRANT_OPTION));
            const common::ObArray<ObPrivilegeType> *privileges = grant_stmt->get_privileges();
            int i = 0;
            for (i = 0;i < privileges->count();++i)
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(privileges->at(i)));
            }
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_REVOKE:
        {
          OB_STAT_INC(SQL, SQL_REVOKE_PRIVILEGE_COUNT);

          ObRevokeStmt *revoke_stmt = dynamic_cast<ObRevokeStmt*>(stmt);
          if (OB_UNLIKELY(NULL == revoke_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            // if revoke priv_xx* on * from user, then table_id == 0
            // elif revoke priv_xx* on table_name from user, then table_id != 0 && table_id != OB_INVALID_ID
            // elif revoke ALL PRIVILEGES, GRANT OPTION from user, then table_id == OB_INVALID_ID
            //add liumz, [multi_database.priv_management.check_privilege]:20150610:b            
            is_global_priv = revoke_stmt->is_global_priv();            
            table_privilege.db_name_ = revoke_stmt->get_db_name();
            //add:e
            table_privilege.table_id_ = revoke_stmt->get_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_GRANT_OPTION));
            const common::ObArray<ObPrivilegeType> *privileges = revoke_stmt->get_privileges();
            int i = 0;
            for (i = 0;i < privileges->count();++i)
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(privileges->at(i)));
            }
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_CREATE_USER:
      case ObBasicStmt::T_DROP_USER:
        {
          ObPrivilege::TablePrivilege table_privilege;
          if (ObBasicStmt::T_CREATE_USER == stmt->get_stmt_type())
            OB_STAT_INC(SQL, SQL_CREATE_USER_COUNT);
          else if (ObBasicStmt::T_DROP_USER == stmt->get_stmt_type())
            OB_STAT_INC(SQL, SQL_DROP_USER_COUNT);

          // create user是全局权限
          // comment liumz: 现在全局权限用 is_global_priv && OB_NOT_EXIST_TABLE_TID 表示
          is_global_priv = true;//add liumz, [multi_database.priv_management.check_privilege]:20150615
          table_privilege.table_id_ = OB_NOT_EXIST_TABLE_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_CREATE_USER));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      //add wenghaixing [database manage]20150616
      case ObBasicStmt::T_CREATE_DATABASE:
        {
          ObPrivilege::TablePrivilege table_privilege;
          if(ObBasicStmt::T_CREATE_DATABASE == stmt->get_stmt_type())
          {
            OB_STAT_INC(SQL, SQL_CREATE_DB_COUNT);
          }
          //global privilege
          is_global_priv = true;
          table_privilege.table_id_ = OB_NOT_EXIST_TABLE_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_CREATE));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      case ObBasicStmt::T_DROP_DATABASE:
       {
          ObPrivilege::TablePrivilege table_privilege;
          if(ObBasicStmt::T_DROP_DATABASE == stmt->get_stmt_type())
          {
            OB_STAT_INC(SQL, SQL_DROP_DB_COUNT);
          }
          //add liumz, [multi_database.priv_management.drop_db_priv]:20150709:b
          ObDropDbStmt *drop_db_stmt = dynamic_cast<ObDropDbStmt*>(stmt);
          if (OB_UNLIKELY(NULL == drop_db_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {//add:e
            //mod liumz, [multi_database.priv_management.drop_db_priv]:20150709:b
            //global privilege -> db privilege
            //is_global_priv = true;
            if (OB_SUCCESS != (err = drop_db_stmt->get_database()->get_string(0, table_privilege.db_name_)))
            {
              TBSYS_LOG(WARN, "get db name from drop db stmt failed, err=%d", err);
            }
            //mod:e
            table_privilege.table_id_ = OB_NOT_EXIST_TABLE_TID;
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_DROP));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
       }
      //add e
      case ObBasicStmt::T_LOCK_USER:
        {
          ObPrivilege::TablePrivilege table_privilege;
          OB_STAT_INC(SQL, SQL_LOCK_USER_COUNT);

          table_privilege.table_id_ = OB_USERS_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      case ObBasicStmt::T_SET_PASSWORD:
        {
          OB_STAT_INC(SQL, SQL_SET_PASSWORD_COUNT);

          ObSetPasswordStmt *set_pass_stmt = dynamic_cast<ObSetPasswordStmt*>(stmt);
          if (OB_UNLIKELY(NULL == set_pass_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            const common::ObStrings* user_pass = set_pass_stmt->get_user_password();
            ObString user;
            err  = user_pass->get_string(0,user);
            OB_ASSERT(OB_SUCCESS == err);
            if (user.length() == 0)
            {
              TBSYS_LOG(DEBUG, "EMPTY");
              // do nothing
            }
            else
            {
              ObPrivilege::TablePrivilege table_privilege;
              table_privilege.table_id_ = OB_USERS_TID;
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
              err = table_privileges.push_back(table_privilege);
              if (OB_UNLIKELY(OB_SUCCESS != err))
              {
                TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
              }
            }
          }
          break;
        }
      case ObBasicStmt::T_RENAME_USER:
        {
          ObPrivilege::TablePrivilege table_privilege;

          OB_STAT_INC(SQL, SQL_RENAME_USER_COUNT);
          table_privilege.table_id_ = OB_USERS_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      //add liumz, [multi_database.priv_management.check_alter_privilege]:20150707:b
      case ObBasicStmt::T_ALTER_TABLE:
        {
          ObPrivilege::TablePrivilege table_privilege;
          OB_STAT_INC(SQL, SQL_ALTER_TABLE_COUNT);
          ObAlterTableStmt *alter_table_stmt = dynamic_cast<ObAlterTableStmt*>(stmt);
          if (OB_UNLIKELY(NULL == alter_table_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            table_privilege.db_name_ = alter_table_stmt->get_db_name();
            table_privilege.table_id_ = alter_table_stmt->get_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_ALTER));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      //add:e      
      case ObBasicStmt::T_CREATE_TABLE:
        {
          ObPrivilege::TablePrivilege table_privilege;
          OB_STAT_INC(SQL, SQL_CREATE_TABLE_COUNT);
          // create table 是库级权限
          //add liumz, [multi_database.priv_management.check_privilege]:20150618:b
          ObCreateTableStmt *create_table_stmt = dynamic_cast<ObCreateTableStmt*>(stmt);
          if (OB_UNLIKELY(NULL == create_table_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            table_privilege.db_name_ = create_table_stmt->get_db_name();
            //add:e
            table_privilege.table_id_ = OB_NOT_EXIST_TABLE_TID;
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_CREATE));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_DROP_TABLE:
        {
          OB_STAT_INC(SQL, SQL_DROP_TABLE_COUNT);
          ObDropTableStmt *drop_table_stmt = dynamic_cast<ObDropTableStmt*>(stmt);
          if (OB_UNLIKELY(NULL == drop_table_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            //add liumz, [multi_database.priv_management.check_privilege]:20150628:b
            ObString db_name, table_name;
            char db_buf[OB_MAX_DATBASE_NAME_LENGTH];
            char table_buf[OB_MAX_TABLE_NAME_LENGTH];
            //add:e
            int64_t i = 0;
            //mod liumz, [UT_bugfix]20150813:b
            //for (i = 0;i < drop_table_stmt->get_table_count();++i)
            for (i = 0;i < drop_table_stmt->get_table_count() && OB_LIKELY(OB_SUCCESS == err);++i)
            //mod:e
            {
              ObPrivilege::TablePrivilege table_privilege;
              //add liumz, [multi_database.priv_management.check_privilege]:20150218:b
              db_name.assign_buffer(db_buf, OB_MAX_DATBASE_NAME_LENGTH);
              table_name.assign_buffer(table_buf, OB_MAX_TABLE_NAME_LENGTH);
              const ObString &db_table_name = drop_table_stmt->get_table_name(i);
              bool split_ret = db_table_name.split_two(db_name, table_name);
              if (!split_ret)
              {
                err = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN, "split db_name.table_name failed, db_name.table_name[%.*s], db_name[%.*s], table_name[%.*s]",
                          db_table_name.length(), db_table_name.ptr(), db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
              }
              else if (OB_SUCCESS != (err = ob_write_string(*(plan->get_name_pool()), db_name, table_privilege.db_name_)))
              {
                TBSYS_LOG(WARN, "write db_name to name_pool failed, err=%d", err);
              }
              //add:e
              table_privilege.table_id_ = drop_table_stmt->get_table_id(i);
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_DROP));
              err = table_privileges.push_back(table_privilege);
              if (OB_UNLIKELY(OB_SUCCESS != err))
              {
                TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
              }
            }
          }
          break;
        }

      //add zhaoqiong [Truncate Table]:20160318:b
      case ObBasicStmt::T_TRUNCATE_TABLE:
      {
        OB_STAT_INC(SQL, SQL_TRUNCATE_TABLE_COUNT);
        ObTruncateTableStmt *trun_table_stmt = dynamic_cast<ObTruncateTableStmt*>(stmt);
        if (OB_UNLIKELY(NULL == trun_table_stmt))
        {
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
        }
        else
        {
          //multi_database.priv_management.check_privilege
          ObString db_name, table_name;
          char db_buf[OB_MAX_DATBASE_NAME_LENGTH];
          char table_buf[OB_MAX_TABLE_NAME_LENGTH];
          for (int64_t i = 0;i < trun_table_stmt->get_table_count() && OB_LIKELY(OB_SUCCESS == err);++i)
          {
            ObPrivilege::TablePrivilege table_privilege;
            db_name.assign_buffer(db_buf, OB_MAX_DATBASE_NAME_LENGTH);
            table_name.assign_buffer(table_buf, OB_MAX_TABLE_NAME_LENGTH);
            const ObString &db_table_name = trun_table_stmt->get_table_name(i);
            bool split_ret = db_table_name.split_two(db_name, table_name);
            if (!split_ret)
            {
              err = OB_ERR_UNEXPECTED;
              TBSYS_LOG(WARN, "split db_name.table_name failed, db_name.table_name[%.*s], db_name[%.*s], table_name[%.*s]",
                        db_table_name.length(), db_table_name.ptr(), db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
            }
            else if (OB_SUCCESS != (err = ob_write_string(*(plan->get_name_pool()), db_name, table_privilege.db_name_)))
            {
              TBSYS_LOG(WARN, "write db_name to name_pool failed, err=%d", err);
            }
            table_privilege.table_id_ = trun_table_stmt->get_table_id(i);
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_DROP));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
        }
        break;
      }
      //add:e
      //add liumz, [bugfix:create index privilege management]20150828:b
      case ObBasicStmt::T_CREATE_INDEX:
        {
          ObPrivilege::TablePrivilege table_privilege;
          OB_STAT_INC(SQL, SQL_CREATE_TABLE_COUNT);
          ObCreateIndexStmt *create_index_stmt = dynamic_cast<ObCreateIndexStmt*>(stmt);
          if (OB_UNLIKELY(NULL == create_index_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObString db_name, table_name;
            char db_buf[OB_MAX_DATBASE_NAME_LENGTH];
            char table_buf[OB_MAX_TABLE_NAME_LENGTH];
            db_name.assign_buffer(db_buf, OB_MAX_DATBASE_NAME_LENGTH);
            table_name.assign_buffer(table_buf, OB_MAX_TABLE_NAME_LENGTH);
            const ObString &db_table_name = create_index_stmt->get_idxed_name();
            bool split_ret = db_table_name.split_two(db_name, table_name);
            if (!split_ret)
            {
              err = OB_ERR_UNEXPECTED;
              TBSYS_LOG(WARN, "split db_name.table_name failed, db_name.table_name[%.*s], db_name[%.*s], table_name[%.*s]",
                        db_table_name.length(), db_table_name.ptr(), db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
            }
            else if (OB_SUCCESS != (err = ob_write_string(*(plan->get_name_pool()), db_name, table_privilege.db_name_)))
            {
              TBSYS_LOG(WARN, "write db_name to name_pool failed, err=%d", err);
            }
            table_privilege.table_id_ = OB_NOT_EXIST_TABLE_TID;
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_CREATE));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      //add:e
      //add wenghaixing [secondary index drop index]20141223
      case ObBasicStmt::T_DROP_INDEX:
        {
          OB_STAT_INC(SQL, SQL_DROP_TABLE_COUNT);
          ObDropIndexStmt *drop_index_stmt = dynamic_cast<ObDropIndexStmt*>(stmt);
          if (OB_UNLIKELY(NULL == drop_index_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            int64_t i = 0;
            //add liumz, [bugfix:drop index privilege management]20150827:b
            ObString db_name, table_name;
            char db_buf[OB_MAX_DATBASE_NAME_LENGTH];
            char table_buf[OB_MAX_TABLE_NAME_LENGTH];
            //add:e
            //mod liumz, [UT_bugfix]20150813:b
            //for (i = 0;i < drop_index_stmt->get_table_count();++i)
            for (i = 0;i < drop_index_stmt->get_table_count() && OB_LIKELY(OB_SUCCESS == err);++i)
              //mod:e
            {
              ObPrivilege::TablePrivilege table_privilege;
              //add liumz, [bugfix:drop index privilege management]20150827:b
              db_name.assign_buffer(db_buf, OB_MAX_DATBASE_NAME_LENGTH);
              table_name.assign_buffer(table_buf, OB_MAX_TABLE_NAME_LENGTH);
              const ObString &db_table_name = drop_index_stmt->get_table_name(i);
              bool split_ret = db_table_name.split_two(db_name, table_name);
              if (!split_ret)
              {
                err = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN, "split db_name.table_name failed, db_name.table_name[%.*s], db_name[%.*s], table_name[%.*s]",
                          db_table_name.length(), db_table_name.ptr(), db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
              }
              else if (OB_SUCCESS != (err = ob_write_string(*(plan->get_name_pool()), db_name, table_privilege.db_name_)))
              {
                TBSYS_LOG(WARN, "write db_name to name_pool failed, err=%d", err);
              }
              //add:e
              //mod liumz, [bugfix:drop index privilege management]20150827:b
              //table_privilege.table_id_ = drop_index_stmt->get_table_id(i);
              table_privilege.table_id_ = drop_index_stmt->get_data_table_id(i);
              //mod:e
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_DROP));
              err = table_privileges.push_back(table_privilege);
              if (OB_UNLIKELY(OB_SUCCESS != err))
              {
                TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
              }
            }
          }
          break;
        }
        //add e
      case ObBasicStmt::T_SHOW_GRANTS:
        {
          ObShowStmt *show_grant_stmt = dynamic_cast<ObShowStmt*>(stmt);
          ObString user_name = show_grant_stmt->get_user_name();
          ObPrivilege::TablePrivilege table_privilege;
          ObPrivilege::TablePrivilege table_privilege2;
          ObPrivilege::TablePrivilege table_privilege3;
          OB_STAT_INC(SQL, SQL_SHOW_GRANTS_COUNT);
          // show grants for user
          //mod liumz, [multi_database.priv_management.check_privilege]:20150627:b
          //if (user_name.length() > 0)
          if (user_name.length() > 0 && user_name != username)
            //mod:e
          {
            table_privilege.table_id_ = OB_USERS_TID;
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_SELECT));
            //add liumz, [multi_database.priv_management.check_privilege]:20150610:b
            table_privilege2.table_id_ = OB_ALL_DATABASE_PRIVILEGE_TID;
            OB_ASSERT(true == table_privilege2.privileges_.add_member(OB_PRIV_SELECT));
            //add:e
            table_privilege3.table_id_ = OB_TABLE_PRIVILEGES_TID;
            OB_ASSERT(true == table_privilege3.privileges_.add_member(OB_PRIV_SELECT));
            if (OB_SUCCESS != (err = table_privileges.push_back(table_privilege)))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
            else if (OB_SUCCESS != (err = table_privileges.push_back(table_privilege2)))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
            else if (OB_SUCCESS != (err = table_privileges.push_back(table_privilege3)))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          else
          {
            // show grants 当前用户,不需要权限
          }
          break;
        }
      default:
        //err = OB_ERR_NO_PRIVILEGE;
        break;
    }
  }
  //mod liumz, [multi_database.priv_management.check_privilege]:20150628:b
  //err = (*pp_privilege)->has_needed_privileges(username, table_privileges);
  //add liumz, [UT_bugfix]20150813:b
  if (OB_SUCCESS == err)
  //add:e
  {
    err = (*pp_privilege)->has_needed_privileges_v2(username, table_privileges, is_global_priv);
  }
  //mod:e
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "username %.*s don't have enough privilege,err=%d", username.length(), username.ptr(), err);
  }
  else
  {
    TBSYS_LOG(DEBUG, "%.*s do privilege check success", username.length(), username.ptr());
  }
  return err;
}

bool ObSql::no_enough_memory()
{
  static const int64_t reserved_mem_size = 512*1024*1024LL; // 512MB
  bool ret = (ob_get_memory_size_limit() > reserved_mem_size)
    && (ob_get_memory_size_limit() - ob_get_memory_size_used() < reserved_mem_size);
  if (OB_UNLIKELY(ret))
  {
    TBSYS_LOG(WARN, "not enough memory, limit=%ld used=%ld",
              ob_get_memory_size_limit(), ob_get_memory_size_used());
    ob_print_mod_memory_usage();
    ob_print_phy_operator_stat();
  }
  return ret;
}

int ObSql::copy_plan_from_store(ObResultSet *result, ObPsStoreItem *item, ObSqlContext *context, uint64_t sql_id)
{
  int ret = OB_SUCCESS;
  if (NULL == result || NULL == item)
  {
    TBSYS_LOG(ERROR, "invalid argument result is %p, item is %p", result, item);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObPsStoreItemValue *value = item->get_item_value();
    if (NULL != value)
    {
      ObPhysicalPlan *phy_plan = ObPhysicalPlan::alloc();
      if (NULL == phy_plan)
      {
        TBSYS_LOG(ERROR, "can not alloc mem for ObPhysicalPlan");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        TBSYS_LOG(DEBUG, "copy from store ob_malloc plan is %p store item %p, store plan %p", phy_plan, item, &item->get_physical_plan());
        phy_plan->set_result_set(result);
        ret = copy_physical_plan(*phy_plan, value->plan_, context);//phy_plan->assign(value->plan_);
        if (OB_SUCCESS != ret)
        {
          phy_plan->clear();
          ObPhysicalPlan::free(phy_plan);
          TBSYS_LOG(ERROR, "Copy Physical plan from ObPsStoreItem to Current ResultSet failed ret=%d", ret);
        }
        else
        {
          result->set_physical_plan(phy_plan, true);
          result->set_session(context->session_info_);

          ObPsSessionInfo *info = NULL;
          if (OB_SUCCESS != (ret = context->session_info_->get_ps_session_info(sql_id, info)))
          {
            TBSYS_LOG(WARN, "Get ps session info failed sql_id=%lu, ret=%d", sql_id, ret);
          }
          else if (OB_SUCCESS != (ret = result->from_store(value, info)))
          {
            TBSYS_LOG(WARN, "Assemble Result from ObPsStoreItem(%p) and ObPsSessionInfo(%p) failed, ret=%d",
                      value, info, ret);
          }
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "ObPsStoreItemValue is null");
      ret = OB_ERROR;
    }
  }
  return ret;
}

int ObSql::copy_plan_to_store(ObResultSet *result, ObPsStoreItem *item)
{
  int ret = OB_SUCCESS;
  if (NULL == result || NULL == item)
  {
    TBSYS_LOG(ERROR, "invalid argument result is %p, item is %p", result, item);
    ret = OB_ERROR;
  }
  else
  {
    ObPhysicalPlan *plan = result->get_physical_plan();
    ObPsStoreItemValue *value = item->get_item_value();
    if (NULL != plan && NULL != value)
    {
      ObPhysicalPlan *storeplan = &item->get_physical_plan();
      ret = storeplan->assign(*plan);
      if (OB_SUCCESS == ret)
      {
        value->field_columns_.clear();
        ObResultSet::Field field;
        ObResultSet::Field ofield;
        for (int i = 0; OB_SUCCESS == ret && i < result->get_field_columns().count(); ++i)
        {
          ofield = result->get_field_columns().at(i);
          ret = field.deep_copy(ofield, &value->str_buf_);
          if (OB_SUCCESS == ret)
          {
            value->field_columns_.push_back(field);
          }
          else
          {
            TBSYS_LOG(WARN, "deep copy field failed, ret=%d", ret);
          }
        }

        value->param_columns_.clear();
		//add peiouya [NotNULL_check] [JHOBv0.1] 20140306:b
		value->params_constraint_.clear();
		//add 20140306:e
        for (int i = 0; OB_SUCCESS == ret && i < result->get_param_columns().count(); ++i)
        {
          ofield = result->get_param_columns().at(i);
          ret = field.deep_copy(ofield, &value->str_buf_);
          if (OB_SUCCESS == ret)
          {
            value->param_columns_.push_back(field);
          }
          else
          {
            TBSYS_LOG(WARN, "deep copy parma failed, ret=%d", ret);
          }
		  //add peiouya  [NotNULL_check] [JHOBv0.1] 20140306:b
		  bool flag = true;
		  ret = result->get_params_constraint(static_cast<int64_t>(i),flag);
		  if (OB_SUCCESS == ret)
          {
            value->params_constraint_.push_back(flag);
          }
          else
          {
            TBSYS_LOG(WARN, "Failed to store column NULL info, ret=%d", ret);
          }
		  //add 20140306:e
        }
        if (OB_SUCCESS == ret)
        {
          value->param_columns_ = result->get_param_columns();
//        value->stmt_type_ = result->get_stmt_type();
          value->inner_stmt_type_ = result->get_inner_stmt_type();
          value->compound_ = result->is_compound_stmt();
        }

        if (NULL != result->get_cur_time_place())
        {
          value->has_cur_time_ = true;
        }
      }
      else
      {
        storeplan->clear();
        TBSYS_LOG(ERROR, "Copy Physical Plan from ResultSet to ObPsStoreItem failed");
        ret = OB_ERROR;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "PhysicalPlan is null in ObPsStroreItemg");
    }
  }
  return ret;
}

int ObSql::try_rebuild_plan(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context, ObPsStoreItem *item, bool &flag, bool substitute)
{
  int ret = OB_SUCCESS;
  flag = false;
  TBSYS_LOG(DEBUG, "call try rebuild plan");
  if (0 == item->wlock())//wait for wlock
  {
    TBSYS_LOG(DEBUG, "get wlock");
    if (item->is_valid() && !need_rebuild_plan(context.schema_manager_, item))
    {
      ret = OB_SUCCESS;
      TBSYS_LOG(INFO, "other thread rebuild plan already");
    }
    else
    {
      flag = true;
      result.set_statement_id(item->get_sql_id());
      result.set_sql_id(item->get_sql_id());
      result.set_session(context.session_info_);
      ObArenaAllocator *allocator = NULL;
      context.is_prepare_protocol_ = true;
      if (OB_UNLIKELY(no_enough_memory()))
      {
        TBSYS_LOG(WARN, "no memory");
        result.set_message("no memory");
        result.set_errcode(OB_ALLOCATE_MEMORY_FAILED);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (NULL == context.session_info_)
      {
        TBSYS_LOG(WARN, "context.session_info_(null)");
        ret = OB_NOT_INIT;
      }
      else if (NULL == (allocator = context.session_info_->get_transformer_mem_pool_for_ps()))
      {
        TBSYS_LOG(WARN, "failed to get new allocator");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        OB_ASSERT(NULL == context.transformer_allocator_);
        context.transformer_allocator_ = allocator;
        // the transformer's allocator is owned by the result set now, and will be free by the result set
        result.set_ps_transformer_allocator(allocator);
        if (OB_SUCCESS != (ret = direct_execute(stmt, result, context)))
        {
          TBSYS_LOG(WARN, "direct execute failed");
          TBSYS_LOG(WARN, "try rebuild plan failed ret is %d", ret);
          item->set_status(PS_ITEM_EXPIRED);
        }

        if (OB_SUCCESS == ret)
        {
          item->clear();
          result.set_errcode(ret);
          if (!substitute)
          {
            //insert ps session
            ret = context.session_info_->store_ps_session_info(result);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "store ps session info failed ret=%d", ret);
            }
          }

          if (OB_SUCCESS == ret)
          {
            //copy plan from resultset to item
            ret = copy_plan_to_store(&result, item);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "copy phy paln from resultset to PbPsStoreItem failed ret=%d", ret);
            }
            else
            {
              item->set_status(PS_ITEM_VALID);
            }
          }

        }
      }
    }
    if (NULL != result.get_physical_plan())
    {
      TBSYS_LOG(DEBUG, "new physical plan is %s", to_cstring(*result.get_physical_plan()));
    }
    item->unlock();//unlock whatever happen
  }
  TBSYS_LOG(DEBUG, "end call try rebuild plan");
  return ret;
}

int ObSql::copy_physical_plan(ObPhysicalPlan& new_plan, ObPhysicalPlan& old_plan, ObSqlContext *context)
{
  int ret = OB_SUCCESS;
  if ((ret = new_plan.assign(old_plan)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Assign physical plan failed, ret=%d", ret);
  }
  else if (context && (ret = set_execute_context(new_plan, *context)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Set execute context failed, ret=%d", ret);
  }
  return ret;
}

int ObSql::set_execute_context(ObPhysicalPlan& plan, ObSqlContext& context)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; ret == OB_SUCCESS && i < plan.get_operator_size(); i++)
  {
    ObPhyOperator *op = plan.get_phy_operator(i);
    TBSYS_LOG(DEBUG, "plan %ld op is type=%s %p", i, ob_phy_operator_type_str(op->get_type()), op);
    if (!op)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "Wrong physical plan, ret=%d, operator idx=%ld", ret, i);
    }
    switch (op->get_type())
    {
      case PHY_UPS_EXECUTOR:
      {
        ObUpsExecutor *exe_op = dynamic_cast<ObUpsExecutor*>(op);
        ObPhysicalPlan *inner_plan = exe_op->get_inner_plan();
        if (!inner_plan)
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "Empty inner plan of ObUpsExecutor, ret=%d", ret);
        }
        else
        {
          exe_op->set_rpc_stub(context.merger_rpc_proxy_);
          ret = ObSql::set_execute_context(*inner_plan, context);
        }
        break;
      }
      case PHY_START_TRANS:
      {
        ObStartTrans *start_op = dynamic_cast<ObStartTrans*>(op);
        start_op->set_rpc_stub(context.merger_rpc_proxy_);
        break;
      }
      case PHY_END_TRANS:
      {
        ObEndTrans *end_op = dynamic_cast<ObEndTrans*>(op);
        end_op->set_rpc_stub(context.merger_rpc_proxy_);
        break;
      }
      case PHY_TABLE_RPC_SCAN:
      {
        ObTableRpcScan *table_rpc_op = dynamic_cast<ObTableRpcScan*>(op);
        table_rpc_op->init(&context);
        break;
      }
      case PHY_CUR_TIME:
      {
        ObGetCurTimePhyOperator *get_cur_time_op = dynamic_cast<ObGetCurTimePhyOperator*>(op);
        get_cur_time_op->set_rpc_stub(context.merger_rpc_proxy_);
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

bool ObSql::need_rebuild_plan(const common::ObSchemaManagerV2 *schema_manager, ObPsStoreItem *item)
{
  bool ret = false;
  if (NULL == schema_manager || NULL == item)
  {
    TBSYS_LOG(WARN, "invalid argument schema_manager=%p, item=%p", schema_manager, item);
  }
  else
  {
    ObPhysicalPlan &plan = item->get_physical_plan();
    int64_t tcount = plan.get_base_table_version_count();
    TBSYS_LOG(DEBUG, "table_version_count=%ld", tcount);
    for (int64_t idx = 0; idx < tcount; ++idx)
    {
      ObPhysicalPlan::ObTableVersion tversion = plan.get_base_table_version(idx);
      const common::ObTableSchema *ts = schema_manager->get_table_schema(tversion.table_id_);
      TBSYS_LOG(DEBUG, "table_id=%ld schema_version=%ld", tversion.table_id_, tversion.version_);
      if (NULL == ts)
      {
        TBSYS_LOG(DEBUG, "table(id=%ld) no longer exist in schema manager", tversion.table_id_);
        ret = true;
        break;
      }
      else
      {
        if (ts->get_schema_version() != tversion.version_)
        {
          TBSYS_LOG(DEBUG, "table(id=%ld) version not match ObPsStore version=%ld, SchemaManager version=%ld",
                    tversion.table_id_, tversion.version_, ts->get_schema_version());
          ret = true;
          break;
        }
      }
    }
  }
  return ret;
}
// add liuzy [sequence] 20150502:b
/*Exp:add for sequence alter */
int ObSql::sequence_alter_rebuild_parse(
    ObSqlContext & context,ParseResult & parse_result,
    ResultPlan  &result_plan, ObResultSet &result)
{
  int ret = OB_SUCCESS;
  int temp_select_ret = OB_SUCCESS;
  ParseNode* parse_node = parse_result.result_tree_->children_[0];
  std::string seq_name = parse_node->children_[0]->str_value_;
  std::string alter_select_string;
  alter_select_string.append("select * from ");
  alter_select_string.append(OB_ALL_SEQUENCE_TABLE_NAME);
  alter_select_string.append(" where ");
  alter_select_string.append(OB_SEQUENCE_NAME);
  alter_select_string.append(" = ");
  alter_select_string.append(SINGLE_QUOTATION);
  alter_select_string.append(seq_name);
  alter_select_string.append(SINGLE_QUOTATION);
  std::string alter_update_string;
  alter_update_string.append("update ");
  alter_update_string.append(OB_ALL_SEQUENCE_TABLE_NAME);
  alter_update_string.append(" set ");
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan.name_pool_);
  ObString sequence_select;
  ObSequenceStmt* seq_alter_stmt = (ObSequenceStmt*)parse_malloc(sizeof(ObSequenceStmt), result_plan.name_pool_);
  if((ret = ob_write_string(*name_pool,
                            ObString::make_string(alter_select_string.data()),
                            sequence_select))!= OB_SUCCESS)
  {
    TBSYS_LOG(ERROR,"Can not malloc space for select stmt.");
  }
  else if(OB_SUCCESS != (ret = direct_execute(sequence_select,result,context)))
  {
    TBSYS_LOG(ERROR,"Sequence select direct execute failed.");
  }
  else
  {
    const common::ObRow *row = NULL;
    const common::ObObj *row_column_val = NULL;
    int64_t data_type = 0;
    ObString increment_by;
    ObString min_value;
    ObString max_value;
    ObString const_start_with;
    if (OB_SUCCESS != (ret = result.open()))
    {
      TBSYS_LOG(ERROR, "failed to execute plan,ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = result.get_next_row(row)))
    {
      if(OB_ITER_END == ret)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(USER_ERROR,"sequence %s does not exist", parse_node->children_[0]->str_value_);
      }
      else
      {
        TBSYS_LOG(ERROR,"sequence call failed to get sequence row information, ret = %d",ret);
      }
    }
    else if(NULL == row)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR,"get next row failed");
    }
    else if(OB_SUCCESS != (ret = row->raw_get_cell((DATA_TYPE_ID - OB_APP_MIN_COLUMN_ID),row_column_val)))
    {
      TBSYS_LOG(ERROR,"sequence call failed to get sequence field information, ret = %d",ret);
    }
    else if(OB_SUCCESS != (ret = row_column_val->get_int(data_type)))
    {
      TBSYS_LOG(ERROR,"sequence call failed to get field data_type,ret = %d",ret);
    }
    else if(OB_SUCCESS != (ret = row->raw_get_cell((INC_BY_ID - OB_APP_MIN_COLUMN_ID),row_column_val)))
    {
      TBSYS_LOG(ERROR,"sequence call failed to get sequence field information, ret = %d",ret);
    }
    else if(OB_SUCCESS != (ret = row_column_val->get_decimal(increment_by)))
    {
      TBSYS_LOG(ERROR,"sequence call failed to get field increment_by,ret = %d",ret);
    }
    else if(OB_SUCCESS != (ret = row->raw_get_cell((MIN_VALUE_ID - OB_APP_MIN_COLUMN_ID),row_column_val)))
    {
      TBSYS_LOG(ERROR,"sequence call failed to get sequence field information, ret = %d",ret);
    }
    else if(OB_SUCCESS != (ret = row_column_val->get_decimal(min_value)))
    {
      TBSYS_LOG(ERROR,"sequence call failed to get field min_value,ret = %d",ret);
    }
    else if(OB_SUCCESS != (ret = row->raw_get_cell((MAX_VALUE_ID - OB_APP_MIN_COLUMN_ID),row_column_val)))
    {
      TBSYS_LOG(ERROR,"sequence call failed to get sequence field information, ret = %d",ret);
    }
    else if(OB_SUCCESS != (ret = row_column_val->get_decimal(max_value)))
    {
      TBSYS_LOG(ERROR,"sequence call failed to get field max_value,ret = %d",ret);
    }
    else if(OB_SUCCESS != (ret = row->raw_get_cell((CONST_START_WITH_ID - OB_APP_MIN_COLUMN_ID),row_column_val)))
    {
      TBSYS_LOG(ERROR,"sequence call failed to get sequence field information, ret = %d",ret);
    }
    else if(OB_SUCCESS != (ret = row_column_val->get_decimal(const_start_with)))
    {
      TBSYS_LOG(ERROR,"sequence call failed to get field const_start,ret = %d",ret);
    }
    if(OB_SUCCESS != (temp_select_ret = result.close()))
    {
      TBSYS_LOG(ERROR,"result close failed");
    }
    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != temp_select_ret)
      {  /*if ret is OB_SUCCESS and result close failed,temp_select_ret value need return*/
        ret = temp_select_ret;
      }
      else if(OB_SUCCESS != (ret = result.reset()))
      {
        TBSYS_LOG(WARN,"result reset failed");
      }
      else
      {
        seq_alter_stmt->set_sequence_datatype((uint8_t)data_type);
        seq_alter_stmt->set_sequence_incrementby(increment_by);
        seq_alter_stmt->set_sequence_minvalue(min_value);
        seq_alter_stmt->set_sequence_maxvalue(max_value);
        seq_alter_stmt->set_sequence_startwith(const_start_with);
      }
    }
    if (OB_SUCCESS == ret && parse_node->children_[1]->type_ == T_SEQUENCE_LABEL_LIST)
    {
      ParseNode* lable_list = parse_node->children_[1];
      char middle_val[MAX_PRINTABLE_SIZE];
      int64_t decimal_length = 0;
      std::string value;
      ObDecimal temp, zero;
      if (OB_SUCCESS != (ret = zero.from((int64_t)0)))
      {
        TBSYS_LOG(ERROR, "Convert zero to decimal failed.");
      }
      for (int8_t i = 0; i < lable_list->num_child_ && ret == OB_SUCCESS; i++)
      {
        ParseNode* option_node = lable_list->children_[i];
        switch (option_node->type_)
        {
        case T_SEQUENCE_RESTART_WITH:
          if (1 == option_node->num_child_ && NULL != option_node->children_[0])
          {
            if (OB_SUCCESS != (ret = seq_alter_stmt->sequence_cons_decimal_value(temp, option_node, OB_SEQUENCE_RESTART_WITH)))
            {
              TBSYS_LOG(ERROR, "Construction decimal value with RestartWith failed.");
              break;
            }
            else
            {
              seq_alter_stmt->set_sequence_startwith(temp);
            }
          }
          break;
        case T_SEQUENCE_INCREMENT_BY:
          if (OB_SUCCESS != (ret = seq_alter_stmt->sequence_cons_decimal_value(temp, option_node, OB_SEQUENCE_INCREMENT_BY)))
          {
            TBSYS_LOG(ERROR, "Construction decimal value with IncrementBy failed.");
            break;
          }
          else
          {
            seq_alter_stmt->set_sequence_incrementby(temp);
          }
          break;
        default:
          break;
        }//end switch
      }//end for
      for (int8_t i = 0; i < lable_list->num_child_ && ret == OB_SUCCESS; i++)
      {
        ParseNode* option_node = lable_list->children_[i];
        switch (option_node->type_)
        {
        case T_SEQUENCE_RESTART_WITH:
          alter_update_string.append(OB_SEQUENCE_CAN_USE_PREVVAL);
          alter_update_string.append(" = ");
          alter_update_string.append(OB_SEQUENCE_DEFAULT_NO_USE_CHAR);
          alter_update_string.append(", ");
          alter_update_string.append(OB_SEQUENCE_CURRENT_VALUE);
          memset(middle_val, 0, MAX_PRINTABLE_SIZE);
          decimal_length = seq_alter_stmt->get_sequence_startwith().to_string(middle_val, MAX_PRINTABLE_SIZE);
          value.assign(middle_val,decimal_length);
          break;
        case T_SEQUENCE_INCREMENT_BY:
          alter_update_string.append(OB_SEQUENCE_INCREMENT_BY);
          memset(middle_val, 0, MAX_PRINTABLE_SIZE);
          decimal_length = seq_alter_stmt->get_sequence_incrementby().to_string(middle_val, MAX_PRINTABLE_SIZE);
          value.assign(middle_val,decimal_length);
          break;
        case T_SEQUENCE_MINVALUE:
          alter_update_string.append(OB_SEQUENCE_MIN_VALUE);
          if (1 == option_node->num_child_ && NULL != option_node->children_[0])
          {
            if (OB_SUCCESS != (ret = seq_alter_stmt->sequence_cons_decimal_value(temp, option_node, OB_SEQUENCE_MIN_VALUE)))
            {
              TBSYS_LOG(ERROR, "Construction decimal value with Minvalue failed.");
              break;
            }
          }
          else if (seq_alter_stmt->get_sequence_incrementby() >= zero)
          {
            temp = seq_alter_stmt->get_sequence_startwith();
          }
          else
          {
            if (OB_SUCCESS != (ret = seq_alter_stmt->sequence_cons_decimal_value(temp, option_node, OB_SEQUENCE_MIN_VALUE, (uint8_t)data_type, ObSequenceStmt::MinValue)))
            {
              TBSYS_LOG(ERROR, "Construction decimal value with Minvalue failed.");
              break;
            }
          }
          if (OB_SUCCESS == ret)
          {
            seq_alter_stmt->set_sequence_minvalue(temp);
            memset(middle_val, 0, MAX_PRINTABLE_SIZE);
            decimal_length = temp.to_string(middle_val, MAX_PRINTABLE_SIZE);
            value.assign(middle_val,decimal_length);
          }
          break;
        case T_SEQUENCE_MAXVALUE:
          alter_update_string.append(OB_SEQUENCE_MAX_VALUE);
          if (1 == option_node->num_child_ && NULL != option_node->children_[0])
          {
            if (OB_SUCCESS != (ret = seq_alter_stmt->sequence_cons_decimal_value(temp, option_node, OB_SEQUENCE_MAX_VALUE)))
            {
              TBSYS_LOG(ERROR, "Construction decimal value with Maxvalue failed.");
              break;
            }
          }
          else if (seq_alter_stmt->get_sequence_incrementby() < zero)
          {
            temp = seq_alter_stmt->get_sequence_startwith();
          }
          else
          {
            if (OB_SUCCESS != (ret = seq_alter_stmt->sequence_cons_decimal_value(temp, option_node, OB_SEQUENCE_MAX_VALUE, (uint8_t)data_type)))
            {
              TBSYS_LOG(ERROR, "Construction decimal value with Maxvalue failed.");
              break;
            }
          }
          if (OB_SUCCESS == ret)
          {
            seq_alter_stmt->set_sequence_maxvalue(temp);
            memset(middle_val, 0, MAX_PRINTABLE_SIZE);
            decimal_length = temp.to_string(middle_val, MAX_PRINTABLE_SIZE);
            value.assign(middle_val,decimal_length);
          }
          break;
        case T_SEQUENCE_CYCLE:
          alter_update_string.append(OB_SEQUENCE_IS_CYCLE);
          value = OB_SEQUENCE_DEFAULT_NO_USE_CHAR;
          if (1 == option_node->value_)
          {
            value = OB_SEQUENCE_DEFAULT_START_CHAR;
          }
          break;
        case T_SEQUENCE_CACHE:
          alter_update_string.append(OB_SEQUENCE_CACHE_NUM);
          value = OB_SEQUENCE_DEFAULT_START_CHAR;
          if (1 == option_node->num_child_ && NULL != option_node->children_[0])
          {
            value = option_node->children_[0]->str_value_;
            seq_alter_stmt->set_sequence_cache((uint64_t)option_node->children_[0]->value_);
            if (option_node->children_[0]->type_ == T_OP_NEG)
            {
              ret = OB_VALUE_OUT_OF_RANGE;
              TBSYS_LOG(USER_ERROR, "The value of Cache is out of range.");
            }
            else if (option_node->children_[0]->type_ == T_OP_POS)
            {
              if (option_node->children_[0]->num_child_ > 0 && option_node->children_[0]->children_[0] != NULL)
              {
                ParseNode* middle_node = option_node->children_[0]->children_[0];
                if (middle_node->value_ < 2 || middle_node->value_ > 2147483647)
                {
                  ret = OB_VALUE_OUT_OF_RANGE;
                  TBSYS_LOG(USER_ERROR, "The value of Cache is out of range.");
                }
              }
            }
            else
            {
              if (option_node->children_[0]->value_ < 2 || option_node->children_[0]->value_ > 2147483647)
              {
                ret = OB_VALUE_OUT_OF_RANGE;
                TBSYS_LOG(USER_ERROR, "The value of Cache is out of range.");
              }
            }
          }
          break;
        case T_SEQUENCE_ORDER:
          alter_update_string.append(OB_SEQUENCE_IS_ORDER);
          value = OB_SEQUENCE_DEFAULT_NO_USE_CHAR;
          if (1 == option_node->value_)
          {
            value = OB_SEQUENCE_DEFAULT_START_CHAR;
          }
          break;
        case T_SEQUENCE_QUICK:
          alter_update_string.append(OB_SEQUENCE_USE_QUICK_PATH);
          value = OB_SEQUENCE_DEFAULT_NO_USE_CHAR;
          if (1 == option_node->value_)
          {
            value = OB_SEQUENCE_DEFAULT_START_CHAR;
          }
          break;
        default:
          TBSYS_LOG(ERROR, "Unknow node type of alter sequence");
          ret = OB_ERR_UNEXPECTED;
          break;
        }//end switch
        if (OB_SUCCESS == ret)
        {
          alter_update_string.append("=");
          value.append(".");//change node type from T_INT to T_DECIMAL
          alter_update_string.append(value);
          if (i < lable_list->num_child_ - 1)
          {
            alter_update_string.append(", ");
          }
        }
      }//end for
      if (OB_SUCCESS == ret)
      {
        alter_update_string.append(" where ");
        alter_update_string.append(OB_SEQUENCE_NAME);
        alter_update_string.append("=");
        alter_update_string.append(SINGLE_QUOTATION);
        alter_update_string.append(seq_name);
        alter_update_string.append(SINGLE_QUOTATION);
      }
    }//end if
    if (OB_SUCCESS == ret && OB_SUCCESS == (ret = seq_alter_stmt->sequence_option_validity(T_SEQUENCE_ALTER)))
    {
      ObString sequence_update;
      if(OB_SUCCESS != (ret = ob_write_string(*name_pool,
                                ObString::make_string(alter_update_string.data()),
                                sequence_update)))
      {
        TBSYS_LOG(ERROR,"Can not malloc space for update stmt");
      }
      else if(OB_SUCCESS != (ret = generate_logical_plan(sequence_update,context,result_plan,result)))
      {
        TBSYS_LOG(ERROR,"sequence update generate_logical_plan failed");
        TBSYS_LOG(DEBUG,"update stmt: %.*s", sequence_update.length(), sequence_update.ptr());
      }
    }
  }
  return ret;
}
//20150430:e
