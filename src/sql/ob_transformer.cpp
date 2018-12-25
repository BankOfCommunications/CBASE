/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_transformer.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *\
 */
#include "ob_transformer.h"
#include "ob_table_rpc_scan.h"
#include "ob_table_mem_scan.h"
#include "ob_merge_join.h"
#include "ob_sql_expression.h"
#include "ob_filter.h"
#include "ob_project.h"
#include "ob_set_operator.h"
#include "ob_merge_union.h"
#include "ob_merge_intersect.h"
#include "ob_merge_except.h"
#include "ob_sort.h"
#include "ob_merge_distinct.h"
#include "ob_merge_groupby.h"
#include "ob_merge_join.h"
#include "ob_scalar_aggregate.h"
#include "ob_limit.h"
#include "ob_physical_plan.h"
#include "ob_add_project.h"
#include "ob_insert.h"
#include "ob_update.h"
#include "ob_delete.h"
#include "ob_explain.h"
#include "ob_explain_stmt.h"
#include "ob_delete_stmt.h"
#include "ob_update_stmt.h"
#include "ob_create_table.h"
#include "ob_create_table_stmt.h"
#include "ob_drop_table.h"
#include "ob_drop_table_stmt.h"
#include "ob_truncate_table.h" //add zhaoqiong [Truncate Table]:20160318
#include "ob_truncate_table_stmt.h" //add zhaoqiong [Truncate Table]:20160318:b
#include "common/ob_row_desc_ext.h"
#include "ob_create_user_stmt.h"
#include "ob_prepare.h"
#include "ob_prepare_stmt.h"
#include "ob_variable_set.h"
#include "ob_variable_set_stmt.h"
#include "ob_kill_stmt.h"
#include "ob_execute.h"
#include "ob_execute_stmt.h"
#include "ob_deallocate.h"
#include "ob_deallocate_stmt.h"
#include "tblog.h"
#include "WarningBuffer.h"
#include "common/ob_obj_cast.h"
#include "ob_ups_modify.h"
#include "ob_insert_dbsem_filter.h"
#include "ob_inc_scan.h"
#include "ob_mem_sstable_scan.h"
#include "ob_multiple_scan_merge.h"
#include "ob_multiple_get_merge.h"
#include "ob_start_trans_stmt.h"
#include "ob_start_trans.h"
#include "ob_end_trans_stmt.h"
#include "ob_end_trans.h"
#include "ob_expr_values.h"
#include "ob_ups_executor.h"
#include "ob_lock_filter.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_privilege.h"
#include "common/ob_privilege_type.h"
#include "common/ob_hint.h"
#include "ob_create_user_stmt.h"
#include "ob_drop_user_stmt.h"
#include "ob_grant_stmt.h"
#include "ob_revoke_stmt.h"
#include "ob_set_password_stmt.h"
#include "ob_lock_user_stmt.h"
#include "ob_rename_user_stmt.h"
#include "sql/ob_priv_executor.h"
#include "ob_dual_table_scan.h"
#include "common/ob_trace_log.h"
#include "ob_empty_row_filter.h"
#include "ob_sql_read_strategy.h"
#include "ob_alter_table_stmt.h"
#include "ob_alter_table.h"
#include "ob_alter_sys_cnf_stmt.h"
#include "ob_alter_sys_cnf.h"
#include "ob_schema_checker.h"
#include "ob_row_count.h"
#include "ob_when_filter.h"
#include "ob_kill_session.h"
#include "ob_get_cur_time_phy_operator.h"
#include "ob_change_obi_stmt.h"
#include "ob_change_obi.h"
#include "mergeserver/ob_merge_server_main.h"
#include "common/ob_define.h"
#include "common/roottable/ob_first_tablet_entry_schema.h"
#include <vector>
#include "ob_multi_bind.h" //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140404
#include "ob_bind_values.h"//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715
#include "ob_iud_loop_control.h"//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715
//add wenghaixing for fix insert bug decimal key 2014/10/11
#include "ob_postfix_expression.h"
//add e
//add wenghaixing [database manage]20150609
#include "ob_create_db_stmt.h"
#include "ob_use_db_stmt.h"
#include "ob_drop_database_stmt.h"
#include "parse_node.h"
//add e
//add wenghaixing [secondary index] 20141025
#include "ob_create_index_stmt.h"
//add e
//add fanqiushi_index
#include "ob_index_trigger.h"
#include "ob_sql_expression.h"
#include "common/page_arena.h"
#include "common/ob_se_array.h"
#include "common/ob_array.h"
//add:e
//add wenghaixing [secondary index upd] 20141127
#include "ob_index_trigger_upd.h"
/*add by wanglei [semi join] 20151231*/
#include "ob_semi_join.h"
//add e
//add steven.h.d [hybrid_join] 20150325
#include "ob_bloomfilter_join.h"
//add wenghaixing [secondary index drop index]20141223
#include "ob_drop_index.h"
#include "ob_drop_index_stmt.h"
#include "dml_build_plan.h"
//add e
// add by liyongfeng:20150105 [secondary index replace]
#include "ob_index_trigger_rep.h"
// add:e
//add lijianqiang [sequence insert] 20150407:b
#include "ob_sequence_insert.h"
//add 20150407:e
//add lijianqiang [sequence delete] 20150515:b
#include "ob_sequence_delete.h"
//add 20150515:e
//add lijianqiang [sequence update] 20150525
#include "ob_sequence_update.h"
//add 20150525:e
//add liuzy [sequence select] 20150608 :b
#include "ob_sequence_select.h"
//add 20150608:e
//add zhujun [fix equal-subquery bug] 20151013:b
using namespace oceanbase::common;
using namespace oceanbase::sql;
typedef int ObMySQLSessionKey;
#define TRANS_LOG(...)                                                  \
    do{                                                                   \
    snprintf(err_stat.err_msg_, MAX_ERROR_MSG, __VA_ARGS__);            \
    TBSYS_LOG(WARN, __VA_ARGS__);                                       \
    } while(0)

#define CREATE_PHY_OPERRATOR(op, type_name, physical_plan, err_stat)    \
  ({                                                                    \
  op = (type_name*)trans_malloc(sizeof(type_name));   \
  if (op == NULL) \
  { \
    err_stat.err_code_ = OB_ERR_PARSER_MALLOC_FAILED; \
    TRANS_LOG("Can not malloc space for %s", #type_name);  \
  } \
  else\
  {\
    op = new(op) type_name();    \
    op->set_phy_plan(physical_plan);              \
    if ((err_stat.err_code_ = physical_plan->store_phy_operator(op)) != OB_SUCCESS) \
    { \
      TRANS_LOG("Add physical operator failed");  \
    } \
    else                                        \
    {                                           \
      ob_inc_phy_operator_stat(op->get_type()); \
    }                                           \
  } \
  op;})

//mod peiouya  [NotNULL_check] [JHOBv0.1] 20131222:b
/*expr: it is convenient to use if context is added to transformer construct*/
//ObTransformer::ObTransformer(ObSqlContext &context)
ObTransformer::ObTransformer(ObSqlContext &context, ObResultSet & result)
//mod 20131222:e
{
    mem_pool_ = context.transformer_allocator_;
    OB_ASSERT(mem_pool_);
    sql_context_ = &context;
    //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
    result_ = &result;
    //add 20131222:e
    group_agg_push_down_param_ = false;
    is_insert_select_ = false;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507
    is_insert_multi_batch_ = false;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507
    is_delete_update_ = false;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20151223
    is_multi_batch_ = false;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302
    questionmark_num_in_update_assign_list_ = 0;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160519
}

ObTransformer::~ObTransformer()
{
}

inline void *ObTransformer::trans_malloc(const size_t nbyte)
{
    OB_ASSERT(mem_pool_);
    return mem_pool_->alloc(nbyte);
}

inline void ObTransformer::trans_free(void* p)
{
    OB_ASSERT(mem_pool_);
    mem_pool_->free(p);
}

int ObTransformer::generate_physical_plans(
        ObMultiLogicPlan &logical_plans,
        ObMultiPhyPlan &physical_plans,
        ErrStat& err_stat)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    // check environment
    if (NULL == sql_context_
            || NULL == sql_context_->merger_rpc_proxy_
            || NULL == sql_context_->schema_manager_
            || NULL == sql_context_->session_info_)
    {
        ret = OB_NOT_INIT;
        TRANS_LOG("sql_context not init");
    }
    else
    {
        // get group_agg_push_down_param_
        ObString param_str = ObString::make_string(OB_GROUP_AGG_PUSH_DOWN_PARAM);
        ObObj val;
        if (sql_context_->session_info_->get_sys_variable_value(param_str, val) != OB_SUCCESS
                || val.get_bool(group_agg_push_down_param_) != OB_SUCCESS)
        {
            TBSYS_LOG(DEBUG, "Can not get param %s", OB_GROUP_AGG_PUSH_DOWN_PARAM);
            // default off
            group_agg_push_down_param_ = false;
        }
    }
    ObLogicalPlan *logical_plan = NULL;
    ObPhysicalPlan *physical_plan = NULL;
    for (int32_t i = 0; ret == OB_SUCCESS && i < logical_plans.size(); i++)
    {
        logical_plan = logical_plans.at(i);
        if ((ret = generate_physical_plan(logical_plan, physical_plan, err_stat)) == OB_SUCCESS)
        {
            if ((ret = physical_plans.push_back(physical_plan)) != OB_SUCCESS)
            {
                TRANS_LOG("Add physical plan failed");
                break;
            }
        }
    }
    return ret;
}

int ObTransformer::generate_physical_plan(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan*& physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    bool new_generated = false;
    if (logical_plan)
    {
        if (OB_LIKELY(NULL == physical_plan))
        {
            if ((physical_plan = (ObPhysicalPlan*)trans_malloc(sizeof(ObPhysicalPlan))) == NULL)
            {
                ret = OB_ERR_PARSER_MALLOC_FAILED;
                TRANS_LOG("Can not malloc space for ObPhysicalPlan");
            }
            else
            {
                physical_plan = new(physical_plan) ObPhysicalPlan();
                TBSYS_LOG(DEBUG, "new physical plan, addr=%p", physical_plan);
                new_generated = true;
            }
        }
        ObBasicStmt *stmt = NULL;
        if (ret == OB_SUCCESS)
        {
            if (query_id == OB_INVALID_ID)
                stmt = logical_plan->get_main_stmt();
            else
                stmt = logical_plan->get_query(query_id);
            if (stmt == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong query id to find query statement");
            }
        }
        TBSYS_LOG(DEBUG, "generate physical plan for query_id=%lu stmt_type=%d",
                  query_id, stmt->get_stmt_type());
        if (OB_LIKELY(ret == OB_SUCCESS))
        {
            switch (stmt->get_stmt_type())
            {
            case ObBasicStmt::T_SELECT:
                ret = gen_physical_select(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_DELETE:
                ret = gen_physical_delete_new(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_INSERT:
                ret = gen_physical_insert_new(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_REPLACE:
                // mod by liyongfeng:20150105 [secondary index replace]: gen_physical_replace ==> gen_physical_replace_new
                //ret = gen_physical_replace(logical_plan, physical_plan, err_stat, query_id, index);
                ret = gen_physical_replace_new(logical_plan, physical_plan, err_stat, query_id, index);
                // mod:e
                break;
            case ObBasicStmt::T_UPDATE:
                ret = gen_physical_update_new(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_EXPLAIN:
                ret = gen_physical_explain(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_CREATE_TABLE:
                ret = gen_physical_create_table(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            //add zhaoqiong [Truncate Table]:20160318:b
            case ObBasicStmt::T_TRUNCATE_TABLE:
                ret = gen_physical_truncate_table(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            //add:e
            case ObBasicStmt::T_DROP_TABLE:
                ret = gen_physical_drop_table(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_ALTER_TABLE:
                ret = gen_physical_alter_table(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_SHOW_TABLES:
            case ObBasicStmt::T_SHOW_SYSTEM_TABLES:// add by zhangcd [multi_database.show_tables] 20150616
                //add liumengzhan_show_index [20141208]
            case ObBasicStmt::T_SHOW_INDEX:
                //add:e
            case ObBasicStmt::T_SHOW_VARIABLES:
            case ObBasicStmt::T_SHOW_COLUMNS:
            case ObBasicStmt::T_SHOW_SCHEMA:
            case ObBasicStmt::T_SHOW_CREATE_TABLE:
            case ObBasicStmt::T_SHOW_TABLE_STATUS:
            case ObBasicStmt::T_SHOW_SERVER_STATUS:
            case ObBasicStmt::T_SHOW_WARNINGS:
            case ObBasicStmt::T_SHOW_GRANTS:
            case ObBasicStmt::T_SHOW_PARAMETERS:
            case ObBasicStmt::T_SHOW_PROCESSLIST:
            case ObBasicStmt::T_SHOW_DATABASES:  //add dolphin [show database]@20150604
            case ObBasicStmt::T_SHOW_CURRENT_DATABASE:  // add by zhangcd [multi_database.show_databases] 20150617
                ret = gen_physical_show(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_PREPARE:
                ret = gen_physical_prepare(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_VARIABLE_SET:
                ret = gen_physical_variable_set(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_EXECUTE:
                ret = gen_physical_execute(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_DEALLOCATE:
                ret = gen_physical_deallocate(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_START_TRANS:
                ret = gen_physical_start_trans(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_END_TRANS:
                ret = gen_physical_end_trans(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_ALTER_SYSTEM:
                ret = gen_physical_alter_system(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_CREATE_USER:
            case ObBasicStmt::T_DROP_USER:
            case ObBasicStmt::T_SET_PASSWORD:
            case ObBasicStmt::T_LOCK_USER:
            case ObBasicStmt::T_RENAME_USER:
            case ObBasicStmt::T_GRANT:
            case ObBasicStmt::T_REVOKE:
                //add wenghaixing [database manage]20150609
            case ObBasicStmt::T_CREATE_DATABASE:
            case ObBasicStmt::T_USE_DATABASE:
            case ObBasicStmt::T_DROP_DATABASE:
                ret = gen_physical_priv_stmt(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_KILL:
                ret = gen_physical_kill_stmt(logical_plan, physical_plan, err_stat, query_id, index);
                break;
            case ObBasicStmt::T_CHANGE_OBI:
                ret = gen_physical_change_obi_stmt(logical_plan, physical_plan, err_stat, query_id, index);
                break;
                //add by wenghaixing [secondary index 20141024]
            case ObBasicStmt::T_CREATE_INDEX:
                ret =gen_physical_create_index(logical_plan, physical_plan, err_stat, query_id, index);
                break;
                //add e
                //add wenghaixing[secondary index drop index]20141223
            case ObBasicStmt::T_DROP_INDEX:
                ret = gen_physical_drop_index(logical_plan, physical_plan, err_stat, query_id, index);
                break;
                //add e
            default:
                ret = OB_NOT_SUPPORTED;
                TRANS_LOG("Unknown logical plan, stmt_type=%d", stmt->get_stmt_type());
                break;
            }
        }

        if (OB_SUCCESS == ret
                //mod liuzy [datetime func] 20150909:b
                /*Exp: use count of ObArray*/
                //      && NO_CUR_TIME != logical_plan->get_cur_time_fun_type()
                && 0 != logical_plan->get_cur_time_fun_type_size()
                //mod 20150909:e
                && OB_INVALID_ID == query_id)
        {
            ret = add_cur_time_plan(physical_plan, err_stat, logical_plan->get_cur_time_fun_type());
            if (OB_SUCCESS != ret)
            {
                TRANS_LOG("failed to add cur_time_plan: ret=[%d]", ret);
            }
        }

        if (ret != OB_SUCCESS && new_generated && physical_plan != NULL)
        {
            physical_plan->~ObPhysicalPlan();
            trans_free(physical_plan);
            physical_plan = NULL;
        }
    }
    return ret;
}

//mod liuzy [datetime func] 20150909:b
/*Exp: modify "const ObCurTimeType&" to "ObArray<ObCurTimeType>&"*/
//int ObTransformer::add_cur_time_plan(ObPhysicalPlan *physical_plan, ErrStat& err_stat, const ObCurTimeType& type)
int ObTransformer::add_cur_time_plan(ObPhysicalPlan *physical_plan, ErrStat& err_stat, const ObArray<ObCurTimeType>& type)
//mod 20150909:b
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObGetCurTimePhyOperator *get_cur_time_op = NULL;

    if (NULL == physical_plan)
    {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "physical_plan must not be NULL");
    }

    if (OB_SUCCESS == ret)
    {
        CREATE_PHY_OPERRATOR(get_cur_time_op, ObGetCurTimePhyOperator, physical_plan, err_stat);
        if (OB_SUCCESS == ret)
        {
            //add liuzy [datetime func] 20150909:b
            for (int64_t idx = 0, size = type.count(); idx < size; ++idx)
            {
                TBSYS_LOG(DEBUG, "add_cur_time_plan: idx = [%ld], size = [%ld]", idx, size);
                //add 20150909:e
                //mod liuzy [datetime func] 20150909:b
                //        get_cur_time_op->set_cur_time_fun_type(type);
                get_cur_time_op->set_cur_time_fun_type(type.at(idx));
                //mod 20150909:e
            }//add liuzy [datetime func] /*Exp:end for*/
            if (OB_SUCCESS != (ret = physical_plan->set_pre_phy_query(get_cur_time_op)))
            {
                TRANS_LOG("Add physical operator(get_cur_time_op) failed, err=%d", ret);
            }
        }
    }
    //add liuzy [datetime func] 20150909:b
    for (int64_t idx = 0, size = type.count(); idx < size; ++idx)
    {
        //add 20150909:e
        if (CUR_TIME_UPS == type.at(idx) && OB_SUCCESS == ret)
        {
            get_cur_time_op->set_rpc_stub(sql_context_->merger_rpc_proxy_);
            // physical plan to be done on ups
            if (OB_SUCCESS == ret)
            {
                ObPhysicalPlan *ups_physical_plan = NULL;
                if (NULL == (ups_physical_plan = (ObPhysicalPlan*)trans_malloc(sizeof(ObPhysicalPlan))))
                {
                    ret = OB_ERR_PARSER_MALLOC_FAILED;
                    TRANS_LOG("Can not malloc space for ObPhysicalPlan");
                }
                else
                { // result set of ups_physical_plan will be set in get_cur_time_op.open
                    ups_physical_plan = new(ups_physical_plan) ObPhysicalPlan();
                    TBSYS_LOG(DEBUG, "new physical plan, addr=%p", ups_physical_plan);
                }

                if (OB_SUCCESS == ret)
                {
                    int32_t idx = 0;
                    ObProject *project = NULL;
                    CREATE_PHY_OPERRATOR(project, ObProject, ups_physical_plan, err_stat);
                    if (OB_SUCCESS == ret && OB_SUCCESS != (ret = ups_physical_plan->add_phy_query(project, &idx, true)))
                    {
                        TRANS_LOG("Add physical operator(cur_time_op) failed, err=%d", ret);
                    }

                    if (OB_SUCCESS == ret)
                    {
                        ObSqlExpression expr;
                        ExprItem item;

                        expr.set_tid_cid(OB_INVALID_ID, OB_MAX_TMP_COLUMN_ID);
                        item.type_ = T_CUR_TIME_OP;

                        if (OB_SUCCESS != (ret = expr.add_expr_item(item)))
                        {
                            TRANS_LOG("add expr item failed, ret=%d", ret);
                        }
                        else if (OB_SUCCESS != (ret = expr.add_expr_item_end()))
                        {
                            TRANS_LOG("add expr end failed, ret=%d", ret);
                        }
                        else if (OB_SUCCESS != (ret = project->add_output_column(expr)))
                        {
                            TRANS_LOG("add expr item failed, ret=%d", ret);
                        }
                    }

                    if (OB_SUCCESS == ret)
                    {
                        ObDualTableScan *dual_table_op = NULL;
                        CREATE_PHY_OPERRATOR(dual_table_op, ObDualTableScan, physical_plan, err_stat);
                        if (OB_SUCCESS == ret && OB_SUCCESS != (ret = project->set_child(0, *dual_table_op)))
                        {
                            TRANS_LOG("add ObDualTableScan on ObProject failed, ret=%d", ret);
                        }
                    }
                }

                if (OB_SUCCESS == ret)
                {
                    get_cur_time_op->set_ups_plan(ups_physical_plan);
                }
            }
            break;//add liuzy [datetime func] 20150910 /*Exp: jump our of for-loop*/
        }
    }//add liuzy [datetime func] 20150909 /*Exp: end for*/
    return ret;
}

template <class T>
int ObTransformer::get_stmt(
        ObLogicalPlan *logical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        T *& stmt)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    /* get statement */
    if (query_id == OB_INVALID_ID)
    {
        stmt = dynamic_cast<T*>(logical_plan->get_main_stmt());
    }
    else
    {
        stmt = dynamic_cast<T*>(logical_plan->get_query(query_id));
    }
    if (stmt == NULL)
    {
        err_stat.err_code_ = OB_ERR_PARSER_SYNTAX;
        TRANS_LOG("Get Stmt error");
    }
    return ret;
}

template <class T>
int ObTransformer::add_phy_query(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        T * stmt,
        ObPhyOperator *phy_op,
        int32_t* index
        )
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    if (query_id == OB_INVALID_ID || stmt == dynamic_cast<T*>(logical_plan->get_main_stmt()))
        ret = physical_plan->add_phy_query(phy_op, index, true);
    else
        ret = physical_plan->add_phy_query(phy_op, index);
    if (ret != OB_SUCCESS)
        TRANS_LOG("Add query of physical plan failed");
    return ret;
}

//add fanqiushi_index

bool ObTransformer::is_index_table_has_all_cid_V2(uint64_t index_tid,Expr_Array *filter_array,Expr_Array *project_array)
{
    //判断索引表是否包含sql语句中出现的所有列 //repaired from messy code by zhuxh 20151014
    bool return_ret=true;
    if(sql_context_->schema_manager_->is_this_table_avalibale(index_tid))
    {
        int64_t w_num=project_array->count();
        for(int32_t i=0;i<w_num;i++)
        {
            ObSqlExpression  col_expr=project_array->at(i);
            //TBSYS_LOG(ERROR,"test::fanqs,,col_expr=%s",to_cstring(col_expr));
            if(!col_expr.is_all_expr_cid_in_indextable(index_tid,sql_context_->schema_manager_))
            {
                return_ret=false;
                break;
            }

        }
        int64_t c_num=filter_array->count();
        for(int32_t j=0;j<c_num;j++)
        {

            ObSqlExpression c_filter=filter_array->at(j);
            if(!c_filter.is_all_expr_cid_in_indextable(index_tid,sql_context_->schema_manager_))
            {
                return_ret=false;
                break;
            }


        }

    }
    //TBSYS_LOG(ERROR,"test::fanqs,,return_ret=%d,,index_tid=%ld",return_ret,index_tid);
    return return_ret;

}


int64_t ObTransformer::is_cid_in_index_table(uint64_t cid,uint64_t tid)
{
    //判断该列是否在该索引表中。结果是0，表示不在；结果是1，表示该列是索引表的主键；结果是2，表示该列是索引表的非主键 //repaired from messy code by zhuxh 20151014
    int64_t return_ret=0;
    int ret=OB_SUCCESS;
    bool is_in_rowkey=false;
    bool is_in_other_column=false;
    const ObTableSchema *index_table_schema = NULL;
    if (NULL == (index_table_schema = sql_context_->schema_manager_->get_table_schema(tid)))
    {
        ret = OB_ERROR;
        TBSYS_LOG(WARN,"Fail to get table schema for table[%ld]", tid);
    }
    else
    {
        uint64_t tmp_cid=OB_INVALID_ID;
        int64_t rowkey_column=index_table_schema->get_rowkey_info().get_size();
        for(int64_t j=0;j<rowkey_column;j++)
        {
            if(OB_SUCCESS!=(ret=index_table_schema->get_rowkey_info().get_column_id(j,tmp_cid)))
            {
                TBSYS_LOG(ERROR,"get column schema failed,cid[%ld]",tmp_cid);
                ret=OB_SCHEMA_ERROR;
            }
            else
            {
                if(tmp_cid==cid)
                {
                    is_in_rowkey=true;
                    break;
                }
            }
        }
        // TBSYS_LOG(ERROR,"test::fanqs,,cid=%ld,is_in_rowkey=%d",cid,is_in_rowkey);
        if(!is_in_rowkey)
        {
            /* uint64_t max_cid=OB_INVALID_ID;
            max_cid=index_table_schema->get_max_column_id();
            //TBSYS_LOG(ERROR,"test::fanqs,,max_cid=%ld,",max_cid);
            for(uint64_t k=OB_APP_MIN_COLUMN_ID;k<=max_cid;k++)
            {
                if(cid==k)
                {
                    is_in_other_column=true;
                    break;
                }
            }*/
            const ObColumnSchemaV2* index_column_schema=NULL;
            index_column_schema=sql_context_->schema_manager_->get_column_schema(tid,cid);
            if(index_column_schema!=NULL)
            {
                is_in_other_column=true;
            }
        }
    }
    if(is_in_rowkey)
        return_ret=1;
    else if(is_in_other_column)
        return_ret=2;
    return return_ret;
}
//add by [semi join second index] 20151231:b
bool ObTransformer::is_this_expr_can_use_index_for_join(uint64_t cid,uint64_t &index_tid,uint64_t main_tid,const ObSchemaManagerV2 *sm_v2)
{
    bool return_ret = false;
    uint64_t tmp_index_tid[OB_MAX_INDEX_NUMS];
    for(int32_t m=0;m<OB_MAX_INDEX_NUMS;m++)
    {
        tmp_index_tid[m]=OB_INVALID_ID;
    }
    if(sm_v2->is_cid_in_index(cid,main_tid,tmp_index_tid))
    {
        index_tid=tmp_index_tid[0];
        return_ret=true;
        //TBSYS_LOG(ERROR,"test::fanqs,column_count=%d,EQ_count=%d",column_count,EQ_count);
    }
    return return_ret;
}
bool ObTransformer::is_expr_can_use_storing_for_join(uint64_t cid,uint64_t mian_tid,uint64_t &index_tid,Expr_Array * filter_array,Expr_Array *project_array)
{
    bool ret=false;
    uint64_t expr_cid=cid;
    uint64_t tmp_index_tid=OB_INVALID_ID;
    uint64_t index_tid_array[OB_MAX_INDEX_NUMS];
    for(int32_t k=0;k<OB_MAX_INDEX_NUMS;k++)
    {
        index_tid_array[k]=OB_INVALID_ID;
    }

    if(sql_context_->schema_manager_->is_cid_in_index(expr_cid,mian_tid,index_tid_array))  //根据原表的tid，找到该表的所有的第一主键为expr_cid的索引表，存到index_tid_array里面 //repaired from messy code by zhuxh 20151014
    {
        for(int32_t i=0;i<OB_MAX_INDEX_NUMS;i++)  //对每张符合条件的索引表  //repaired from messy code by zhuxh 20151014
        {
            // TBSYS_LOG(ERROR,"test::fanqs,,index_tid_array[i]=%ld",index_tid_array[i]);
            //uint64_t tmp_tid=index_tid_array[i];
            if(index_tid_array[i]!=OB_INVALID_ID)
            {
                if(is_index_table_has_all_cid_V2(index_tid_array[i],filter_array,project_array)) //判断是否所有在sql语句里面出现的列，都在这张索引表中  //repaired from messy code by zhuxh 20151014
                {
                    tmp_index_tid=index_tid_array[i];
                    //TBSYS_LOG(ERROR,"test::fanqs,,tmp_index_tid=%ld",tmp_index_tid);
                    ret=true;
                    break;
                }
            }
        }
        index_tid=tmp_index_tid;
    }

    return ret;
}
//add:e
bool ObTransformer::is_expr_can_use_storing_V2(ObSqlExpression c_filter,uint64_t mian_tid,uint64_t &index_tid,Expr_Array * filter_array,Expr_Array *project_array)
{
    //uint64_t &index_tid：索引表的tid  //repaired from messy code by zhuxh 20151014
    bool ret=false;
    uint64_t expr_cid=OB_INVALID_ID;
    uint64_t tmp_index_tid=OB_INVALID_ID;
    uint64_t index_tid_array[OB_MAX_INDEX_NUMS];
    for(int32_t k=0;k<OB_MAX_INDEX_NUMS;k++)
    {
        index_tid_array[k]=OB_INVALID_ID;
    }
    if(OB_SUCCESS==c_filter.find_cid(expr_cid))  //获得表达式中存的列的column id:expr_cid。如果表达式中有多列，返回ret不等于OB_SUCCESS
    {
        if(sql_context_->schema_manager_->is_cid_in_index(expr_cid,mian_tid,index_tid_array))  //根据原表的tid，找到该表的所有的第一主键为expr_cid的索引表，存到index_tid_array里面 //repaired from messy code by zhuxh 20151014
        {
            for(int32_t i=0;i<OB_MAX_INDEX_NUMS;i++)  //对每张符合条件的索引表  //repaired from messy code by zhuxh 20151014
            {
                // TBSYS_LOG(ERROR,"test::fanqs,,index_tid_array[i]=%ld",index_tid_array[i]);
                //uint64_t tmp_tid=index_tid_array[i];
                if(index_tid_array[i]!=OB_INVALID_ID)
                {
                    if(is_index_table_has_all_cid_V2(index_tid_array[i],filter_array,project_array)) //判断是否所有在sql语句里面出现的列，都在这张索引表中  //repaired from messy code by zhuxh 20151014
                    {
                        tmp_index_tid=index_tid_array[i];
                        //TBSYS_LOG(ERROR,"test::fanqs,,tmp_index_tid=%ld",tmp_index_tid);
                        ret=true;
                        break;
                    }
                }
            }
            index_tid=tmp_index_tid;
        }
    }
    return ret;
}
//add liumz, [optimize group_order by index]20170419:b
int ObTransformer::optimize_order_by_index(ObArray<uint64_t> &idx_tids, uint64_t main_tid, uint64_t &index_table_id, bool &hit_index_ret, ObStmt *stmt, ObLogicalPlan *logical_plan)
{
  int ret = OB_SUCCESS;
  if (ObBasicStmt::T_SELECT == stmt->get_stmt_type() && 1 == stmt->get_table_size())
  {
    ObSelectStmt* select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
    int32_t num = select_stmt->get_order_item_size();
    TBSYS_LOG(DEBUG, "idx_tids.count()[%ld]", idx_tids.count());
    for (int64_t j = 0; ret == OB_SUCCESS && num > 0 && j < idx_tids.count(); j++)
    {
      uint64_t index_tid = idx_tids.at(j);
      const ObTableSchema *idx_table_schema = NULL;
      if (NULL == (idx_table_schema = sql_context_->schema_manager_->get_table_schema(index_tid)))
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN,"Fail to get table schema for table[%ld]",index_tid);
      }
      else
      {
        const ObRowkeyInfo &idx_rk_info = idx_table_schema->get_rowkey_info();
        //TODO
        bool hit_index = false;
        for (int32_t i = 0; ret == OB_SUCCESS && num <= idx_rk_info.get_size() && i < num; i++)
        {
            uint64_t column_id = OB_INVALID_ID;
            const OrderItem& order_item = select_stmt->get_order_item(i);
            ObSqlRawExpr *order_expr = logical_plan->get_expr(order_item.expr_id_);
            if (OB_SUCCESS != (ret = idx_rk_info.get_column_id(i, column_id)))
            {
              hit_index = false;
              break;
            }
            if (OrderItem::ASC == order_item.order_type_ && order_expr && order_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
            {
              if (main_tid == order_expr->get_table_id() && column_id == order_expr->get_column_id())
              {
                hit_index = true;
                continue;
              }
              else
              {
                hit_index = false;
                break;
              }
            }
            else
            {
              hit_index = false;
              break;
            }
        }//end for
        if (ret == OB_SUCCESS && hit_index)
        {
          hit_index_ret = true;
          index_table_id = index_tid;
          for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
          {
            select_stmt->get_order_item_flag(i) = 1;//set applied flag
          }
          break;
        }
      }
    }//end for
  }
  return ret;
}

int ObTransformer::optimize_group_by_index(ObArray<uint64_t> &idx_tids, uint64_t main_tid, uint64_t &index_table_id, bool &hit_index_ret, ObStmt *stmt, ObLogicalPlan *logical_plan)
{
  int ret = OB_SUCCESS;
  if (ObBasicStmt::T_SELECT == stmt->get_stmt_type() && 1 == stmt->get_table_size())
  {
    ObSelectStmt* select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
    int32_t num = select_stmt->get_group_expr_size();
    TBSYS_LOG(DEBUG, "idx_tids.count()[%ld]", idx_tids.count());
    for (int64_t j = 0; ret == OB_SUCCESS && num > 0 && j < idx_tids.count(); j++)
    {
      uint64_t index_tid = idx_tids.at(j);
      const ObTableSchema *idx_table_schema = NULL;
      if (NULL == (idx_table_schema = sql_context_->schema_manager_->get_table_schema(index_tid)))
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN,"Fail to get table schema for table[%ld]",index_tid);
      }
      else
      {
        const ObRowkeyInfo &idx_rk_info = idx_table_schema->get_rowkey_info();
        //TODO
        bool hit_index = false;
        for (int32_t i = 0; ret == OB_SUCCESS && num <= idx_rk_info.get_size() && i < num; i++)
        {
            uint64_t column_id = OB_INVALID_ID;
            ObSqlRawExpr *group_expr = logical_plan->get_expr(select_stmt->get_group_expr_id(i));
            if (OB_SUCCESS != (ret = idx_rk_info.get_column_id(i, column_id)))
            {
              hit_index = false;
              break;
            }
            if (group_expr && group_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
            {
              if (main_tid == group_expr->get_table_id() && column_id == group_expr->get_column_id())
              {
                hit_index = true;
                continue;
              }
              else
              {
                hit_index = false;
                break;
              }
            }
            else
            {
              hit_index = false;
              break;
            }
        }//end for
        if (ret == OB_SUCCESS && hit_index)
        {
          hit_index_ret = true;
          index_table_id = index_tid;
          for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
          {
            select_stmt->get_group_expr_flag(i) = 1;//set applied flag
          }
          break;
        }
      }
    }//end for
  }
  return ret;
}
//add:e

bool ObTransformer::is_wherecondition_have_main_cid_V2(Expr_Array *filter_array,uint64_t main_cid)
{   //如果where条件的某个表达式有main_cid或某个表达式有多个列,返回true //repaired from messy code by zhuxh 20151014
    bool return_ret=false;
    int ret=OB_SUCCESS;

    int64_t c_num = filter_array->count();
    int32_t i = 0;
    for ( ;ret == OB_SUCCESS && i < c_num; i++)
    {
        ObSqlExpression c_filter = filter_array->at(i);
        //add wanglei [second index fix] 20160425:b
        if(!c_filter.is_expr_has_more_than_two_columns())
        {
            if(c_filter.is_have_main_cid(main_cid))
            {
                return_ret=true;
                break;
            }
        }
        //add wanglei [second index fix] 20160425:e
//        if(c_filter.is_have_main_cid(main_cid))
//        {
//            return_ret=true;
//            break;
//        }
    }
    return return_ret;
}

//add wenghaixing [secondary index for paper]20150505
bool ObTransformer::if_rowkey_in_expr(Expr_Array *filter_array, uint64_t main_tid)
{
    bool return_ret=false;
    uint64_t tid=main_tid;
    // uint64_t index_tid=OB_INVALID_ID;
    const ObTableSchema *mian_table_schema = NULL;
    if (NULL == (mian_table_schema = sql_context_->schema_manager_->get_table_schema(tid)))
    {
        TBSYS_LOG(WARN,"Fail to get table schema for table[%ld]",tid);
    }
    else
    {
        const ObRowkeyInfo *rowkey_info = &mian_table_schema->get_rowkey_info();
        uint64_t main_cid=OB_INVALID_ID;
        rowkey_info->get_column_id(0,main_cid);
        return_ret = is_wherecondition_have_main_cid_V2(filter_array,main_cid);

    }
    return return_ret;
}
//add e
bool ObTransformer::decide_is_use_storing_or_not_V2(Expr_Array  *filter_array,
                                                    Expr_Array *project_array,
                                                    uint64_t &index_table_id,
                                                    uint64_t main_tid,
                                                    Join_column_Array *join_column,//add by wanglei [semi join second index] 20151231
                                                    ObStmt *stmt,//add by wanglei [semi join second index] 20151231
                                                    //add liumz, [optimize group_order by index]20170419:b
                                                    ObLogicalPlan *logical_plan,
                                                    Expr_Array *order_array,
                                                    Expr_Array *group_array
                                                    //add:e
                                                    )
{
    //输出：bool类型   返回值： uint64_t &index_table_id：索引表的tid
    bool return_ret=false;
    int ret=OB_SUCCESS;

    uint64_t tid=main_tid;
    uint64_t index_tid=OB_INVALID_ID;
    const ObTableSchema *mian_table_schema = NULL;
    if (NULL == (mian_table_schema = sql_context_->schema_manager_->get_table_schema(tid)))
    {
        TBSYS_LOG(WARN,"Fail to get table schema for table[%ld]",tid);
    }
    else
    {
        const ObRowkeyInfo *rowkey_info = &mian_table_schema->get_rowkey_info();
        uint64_t main_cid=OB_INVALID_ID;
        rowkey_info->get_column_id(0,main_cid);  //获得原表的第一主键的column id,存到main_cid里面 //repaired from messy code by zhuxh 20151014
        if(!is_wherecondition_have_main_cid_V2(filter_array,main_cid))  //判断where条件中是否有原表的第一主键，如果有，则不用索引 //repaired from messy code by zhuxh 20151014
        {
            //add by wanglei [semi join second index] 20151231:b
            //如果where条件中的表达式的列不是原表的主键，判断其是否可以用作不回表的索引。
            if(stmt ==NULL)
            {
                ret = OB_ERR_POINTER_IS_NULL;
                TBSYS_LOG(WARN,"[semi join] stmt is null!");
            }
            else  if(stmt->get_query_hint().join_array_.size()>0)
            {
                ObJoinTypeArray tmp_join_type = stmt->get_query_hint().join_array_.at(0);
                if(tmp_join_type.join_type_ == T_SEMI_JOIN ||tmp_join_type.join_type_ == T_SEMI_BTW_JOIN)
                {
                    for(int l=0;l<join_column->count();l++)
                    {
                        if(is_expr_can_use_storing_for_join(join_column->at(l),tid,index_tid,filter_array,project_array))
                        {
                            index_table_id=index_tid;
                            return_ret=true;
                            break;
                        }
                    }
                }
            }
            //add:e
            int64_t c_num = filter_array->count();
            ObArray<uint64_t> idx_tids;//add liumz, [optimize group_order by index]20170419
            for (int32_t i = 0; ret == OB_SUCCESS && i < c_num; i++)    //对where条件中的所有表达式依次处理 //repaired from messy code by zhuxh 20151014
            {
                ObSqlExpression c_filter=filter_array->at(i);
                //add wanglei [second index fix] 20160425:b
                if(!c_filter.is_expr_has_more_than_two_columns ())
                {
                    if(is_expr_can_use_storing_V2(c_filter,tid,index_tid,filter_array,project_array))  //判断该表达式能否使用不回表的索引
                    {
                        //mod liumz, [optimize group_order by index]20170419:b
                        TBSYS_LOG(DEBUG, "index_tid[%ld]", index_tid);
                        idx_tids.push_back(index_tid);
                        return_ret=true;
                        /*index_table_id=index_tid;
                        return_ret=true;
                        break;*/
                        //mod:e
                    }
                }
                //add wanglei [second index fix] 20160425:e
//                if(is_expr_can_use_storing_V2(c_filter,tid,index_tid,filter_array,project_array))  //判断该表达式能否使用不回表的索引
//                {
//                    index_table_id=index_tid;
//                    return_ret=true;
//                    break;
//                }
            }
            //add liumz, [optimize group_order by index]20170419:b
            if (OB_SUCCESS == ret)
            {
              bool index_group = false;
              bool index_order = false;
              uint64_t index_group_table_id = OB_INVALID_ID;
              uint64_t index_order_table_id = OB_INVALID_ID;
              if (return_ret && idx_tids.count() > 0)
              {
                index_table_id = idx_tids.at(0);//pick the first index table, default
                index_group_table_id = idx_tids.at(0);//pick the first index table, default
                index_order_table_id = idx_tids.at(0);//pick the first index table, default
                if (group_array->count() > 0 && OB_SUCCESS != (ret = optimize_group_by_index(idx_tids, tid, index_group_table_id, index_group, stmt, logical_plan)))
                {
                  TBSYS_LOG(WARN, "optimize_group_by_index failed, ret = %d", ret);
                }
                if (OB_SUCCESS == ret && order_array->count() > 0)
                {
                  if (index_group)
                  {
                    idx_tids.clear();
                    idx_tids.push_back(index_group_table_id);
                  }
                  if ((index_group || 0 == group_array->count())
                      && OB_SUCCESS != (ret = optimize_order_by_index(idx_tids, tid, index_order_table_id, index_order, stmt, logical_plan)))
                  {
                    TBSYS_LOG(WARN, "optimize_order_by_index failed, ret = %d", ret);
                  }
                }
              }
              else
              {
                //check whether we can use index without storing, if not, check group or order column
                bool is_use_index_without_storing=false;
                for(int32_t j=0; j<c_num; j++)
                {
                  ObSqlExpression &c_filter=filter_array->at(j);
                  if(c_filter.is_this_expr_can_use_index(index_tid,tid,sql_context_->schema_manager_))
                  {
                    is_use_index_without_storing=true;
                    break;
                  }
                }
                if (!is_use_index_without_storing && group_array->count() > 0)
                {
                  idx_tids.clear();
                  ObSqlExpression &c_filter = group_array->at(0);
                  if(!c_filter.is_expr_has_more_than_two_columns ())
                  {
                    if(is_expr_can_use_storing_V2(c_filter,tid,index_tid,filter_array,project_array))
                    {
                      idx_tids.push_back(index_tid);
                      if (OB_SUCCESS != (ret = optimize_group_by_index(idx_tids, tid, index_group_table_id, index_group, stmt, logical_plan)))
                      {
                        TBSYS_LOG(WARN, "optimize_group_by_index failed, ret = %d", ret);
                      }
                    }
                  }
                }//end if
                if (OB_SUCCESS == ret && !is_use_index_without_storing && order_array->count() > 0)
                {
                  idx_tids.clear();
                  if (index_group)
                  {
                    idx_tids.push_back(index_group_table_id);
                    if (OB_SUCCESS != (ret = optimize_order_by_index(idx_tids, tid, index_order_table_id, index_order, stmt, logical_plan)))
                    {
                      TBSYS_LOG(WARN, "optimize_order_by_index failed, ret = %d", ret);
                    }
                  }
                  else if (0 == group_array->count())
                  {
                    ObSqlExpression &c_filter = order_array->at(0);
                    if(!c_filter.is_expr_has_more_than_two_columns ())
                    {
                      if(is_expr_can_use_storing_V2(c_filter,tid,index_tid,filter_array,project_array))
                      {
                        idx_tids.push_back(index_tid);
                        if (OB_SUCCESS != (ret = optimize_order_by_index(idx_tids, tid, index_order_table_id, index_order, stmt, logical_plan)))
                        {
                          TBSYS_LOG(WARN, "optimize_order_by_index failed, ret = %d", ret);
                        }
                      }
                    }
                  }
                }//end if
              }
              //set return_ret & index_table_id
              if (index_group)
              {
                return_ret = index_group;
                index_table_id = index_group_table_id;
              }
              else if (index_order)
              {
                return_ret = index_order;
                index_table_id = index_order_table_id;
              }
            }
            //add liumz, [optimize group_order by index]20170419:e
        }
        //add by wanglei [semi join second index] 20151231:b
        //如果hint中第一个不是semi join那么索引使用失效
        else
        {
            if(stmt ==NULL)
            {
                ret = OB_ERR_POINTER_IS_NULL;
                TBSYS_LOG(WARN,"[semi join] stmt is null!");
            }
            else if(stmt->get_query_hint().join_array_.size()>0)
            {
                ObJoinTypeArray tmp_join_type = stmt->get_query_hint().join_array_.at(0);
                if(tmp_join_type.join_type_ == T_SEMI_JOIN ||tmp_join_type.join_type_ == T_SEMI_BTW_JOIN)
                {
                    for(int l=0;l<join_column->count();l++)
                    {
                        if(is_expr_can_use_storing_for_join(join_column->at(l),tid,index_tid,filter_array,project_array))
                        {
                            index_table_id=index_tid;
                            return_ret=true;
                        }
                    }
                }
            }
        }
        //add:e
    }
    // TBSYS_LOG(ERROR,"test::fanqs,,return_ret=%d,index_table_id=%ld",return_ret,index_table_id);
    return return_ret;
}


bool ObTransformer::is_can_use_hint_for_storing_V2(
        Expr_Array *filter_array,
        Expr_Array *project_array,
        uint64_t index_table_id,
        Join_column_Array *join_column,//add by wanglei [semi join second index] 20151231
        ObStmt *stmt)//add by wanglei [semi join second index] 20151231
{
    bool cond_has_main_cid = false;
    bool can_use_hint_for_storing = false;
    const ObTableSchema *index_table_schema = NULL;
    if (NULL == (index_table_schema = sql_context_->schema_manager_->get_table_schema(index_table_id)))
    {
        TBSYS_LOG(WARN, "Fail to get table schema for table[%ld]", index_table_id);
    }
    else if(sql_context_->schema_manager_->is_this_table_avalibale(index_table_id))
    {
        const ObRowkeyInfo& rowkey_info = index_table_schema->get_rowkey_info();
        uint64_t index_key_cid = OB_INVALID_ID;
        // 获得索引表的第一主键的column id
        if(OB_SUCCESS != rowkey_info.get_column_id(0, index_key_cid))
        {
            TBSYS_LOG(WARN, "Fail to get column id, index_table name:[%s], index_table id: [%ld]",
                      index_table_schema->get_table_name(), index_table_schema->get_table_id());
            cond_has_main_cid = false;
        }
        // 判断where条件的表达式中是否包含索引表的第一主键，每个表达式都只有一列且其中有一列是索引表的第一主键时返回true

        //add by wanglei [semi join second index] 20151231:b
        //注意
        //这里有没有考虑到的情形，如果where表达式中有满足主键cid的表达式，那么就不会进入semi join的检查流程，就不知道
        //hint中的索引表的主键是否与on表达式中的cid相同与否了,因此在where表达式中有符合is_wherecondition_have_main_cid_V2
        //的表达式时，还要判断一下on中的列id与指定的索引表的主键id是否相同。
        else if(!is_wherecondition_have_main_cid_V2(filter_array,index_key_cid))
        {
            //add by wanglei [semi join second index] 20151231:b
            //如果where条件中的表达式不包含索引表的第一主键，判断其是否可以用作不回表的索引。
            if(stmt ==NULL)
            {
                TBSYS_LOG(WARN,"[semi join] stmt is null!");
            }
            else if(stmt->get_query_hint().join_array_.size()>0)
            {
                ObJoinTypeArray tmp_join_type = stmt->get_query_hint().join_array_.at(0);
                if(tmp_join_type.join_type_ == T_SEMI_JOIN ||tmp_join_type.join_type_ == T_SEMI_BTW_JOIN)
                {
                    //实际join_column中只有一个元素
                    for(int l=0;l<join_column->count();l++)
                    {
                        if(join_column->at(l) == index_key_cid )
                        {
                            cond_has_main_cid = true;
                        }
                        else
                        {
                            cond_has_main_cid = false;
                        }
                    }
                }
                else
                {
                    cond_has_main_cid = false;
                }
            }
            else
            {
                cond_has_main_cid = false;
            }
            //add:e
            //原流程代码：b
            //cond_has_main_cid = false;
            //e
        }
        else
        {
            //add wanglei [semi join second index] 20160106 :b
            //如果where表达式中有符合条件的表达式，那么还要检查on表达式中的对应表的列id是否与
            //指定的索引表的主键id相同
            if(stmt ==NULL)
            {
                TBSYS_LOG(WARN,"[semi join] stmt is null!");
            }
            else if(stmt->get_query_hint().join_array_.size()>0)
            {
                ObJoinTypeArray tmp_join_type = stmt->get_query_hint().join_array_.at(0);
                if(tmp_join_type.join_type_ == T_SEMI_JOIN ||tmp_join_type.join_type_ == T_SEMI_BTW_JOIN)
                {
                    //实际join_column中只有一个元素
                    for(int l=0;l<join_column->count();l++)
                    {
                        if(join_column->at(l) != index_key_cid )
                        {
                            cond_has_main_cid = false;
                        }
                        else
                        {
                            cond_has_main_cid = true;
                        }
                    }
                }
                else
                {
                    cond_has_main_cid = true;
                }
            }
            else
            {
                cond_has_main_cid = true;
            }
            //add e
            //原流程代码：b
            //cond_has_main_cid = true;
            //e
        }
    }

    if(cond_has_main_cid)
    {
        // 如果where条件中包含索引表的第一主键再判断这些表达式中的列和select的输出列是不是都在索引表中
        can_use_hint_for_storing = is_index_table_has_all_cid_V2(index_table_id,filter_array,project_array );
    }
    // 如果对于where条件不能使用主键索引的情况则认为不能使用索引表的storing
    else
    {
        can_use_hint_for_storing = false;
    }

    return can_use_hint_for_storing;
}

bool ObTransformer::is_can_use_hint_index_V2(
        Expr_Array *filter_ayyay,
        uint64_t index_table_id,
        Join_column_Array *join_column,//add by wanglei [semi join second index] 20151231
        ObStmt *stmt  //add by wanglei [semi join second index] 20151231
        )
{
    bool can_use_hint_index = false;
    bool cond_has_main_cid = false;
    const ObTableSchema *index_table_schema = NULL;
    if (NULL == (index_table_schema = sql_context_->schema_manager_->get_table_schema(index_table_id)))
    {
        TBSYS_LOG(WARN, "Fail to get table schema for table[%ld]", index_table_id);
    }
    else
    {
        const ObRowkeyInfo& rowkey_info = index_table_schema->get_rowkey_info();
        uint64_t index_key_cid = OB_INVALID_ID;
        // 获得索引表的第一主键的column id
        if(OB_SUCCESS != rowkey_info.get_column_id(0, index_key_cid))
        {
            TBSYS_LOG(WARN, "Fail to get column id, index_table name:[%s], index_table id: [%ld]",
                      index_table_schema->get_table_name(), index_table_schema->get_table_id());
            cond_has_main_cid = false;
        }
        // 判断where条件的表达式中是否包含索引表的第一主键，每个表达式都只有一列且其中有一列是索引表的第一主键时返回true
        else if(!is_wherecondition_have_main_cid_V2(filter_ayyay,index_key_cid))
        {
            //add by wanglei [semi join second index] 20151231:b
            //如果where条件中的表达式不包含索引表的第一主键，判断其是否可以用作不回表的索引。
            if(stmt ==NULL)
            {
                TBSYS_LOG(WARN,"[semi join] stmt is null!");
            }
            else if(stmt->get_query_hint().join_array_.size()>0)
            {
                ObJoinTypeArray tmp_join_type = stmt->get_query_hint().join_array_.at(0);
                if(tmp_join_type.join_type_ == T_SEMI_JOIN ||tmp_join_type.join_type_ == T_SEMI_BTW_JOIN)
                {
                  //mod dragon [] 2016-11-9
                  //多个join_column,只要有一个包含索引表的第一主键，返回true
                  for(int l=0;l<join_column->count();l++)
                  {
                      if(join_column->at(l) == index_key_cid )
                      {
                          cond_has_main_cid = true;
                          break;
                      }
                      else
                      {
                          cond_has_main_cid = false;
                      }
                  }

                  /*---old code as below---
                    //实际join_column中只有一个元素
                    for(int l=0;l<join_column->count();l++)
                    {
                        if(join_column->at(l) == index_key_cid )
                        {
                            cond_has_main_cid = true;
                        }
                        else
                        {
                            cond_has_main_cid = false;
                        }
                    }
                  -------old code-----*/

                  //mod e
                }
                else
                {
                    cond_has_main_cid = false;
                }
            }
            else
            {
                cond_has_main_cid = false;
            }
            //add:e
            //cond_has_main_cid = false;
        }
        else
        {
            //add by wanglei [semi join second index] 20151231:b
            if(stmt ==NULL)
            {
                TBSYS_LOG(WARN,"[semi join] stmt is null!");
            }
            else  if(stmt->get_query_hint().join_array_.size()>0)
            {
                ObJoinTypeArray tmp_join_type = stmt->get_query_hint().join_array_.at(0);
                if(tmp_join_type.join_type_ == T_SEMI_JOIN ||tmp_join_type.join_type_ == T_SEMI_BTW_JOIN)
                {
                    //实际join_column中只有一个元素
                    for(int l=0;l<join_column->count();l++)
                    {
                        if(join_column->at(l) == index_key_cid )
                        {
                            cond_has_main_cid = true;
                        }
                        else
                        {
                            cond_has_main_cid = false;
                        }
                    }
                }
                else
                {
                    cond_has_main_cid = true;
                }
            }
            else
            {
                cond_has_main_cid = true;
            }
            //add:e
            // cond_has_main_cid = true;
        }
    }

    if(cond_has_main_cid)
    {
        can_use_hint_index = true;
    }
    if(!sql_context_->schema_manager_->is_this_table_avalibale(index_table_id))
    {
        can_use_hint_index=false;
    }
    return can_use_hint_index;
}


//add:e


int ObTransformer::gen_physical_select(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObSelectStmt  *select_stmt = NULL;
    ObPhyOperator *result_op = NULL;
    /* get statement */
    if ((ret = get_stmt(logical_plan, err_stat, query_id, select_stmt)) != OB_SUCCESS)
    {
    }
    else if (select_stmt->is_for_update())
    {
        if ((ret = gen_phy_select_for_update(
                 logical_plan,
                 physical_plan,
                 err_stat,
                 query_id,
                 index)) != OB_SUCCESS)
        {
            //TRANS_LOG("Transform select for update statement failed");
        }
        //mod 20150917:e
    }
    else
    {
        ObSelectStmt::SetOperator set_type = select_stmt->get_set_op();
        if (set_type != ObSelectStmt::NONE)
        {
        ObSetOperator *set_op = NULL;
        common::ObArray<common::ObObjType> tmp_result_columns_type;
        tmp_result_columns_type = select_stmt->get_result_type_array();
            if (ret == OB_SUCCESS)
            {
                switch (set_type)
                {
                case ObSelectStmt::UNION :
                {
                    ObMergeUnion *union_op = NULL;
                    CREATE_PHY_OPERRATOR(union_op, ObMergeUnion, physical_plan, err_stat);
              /*del qianzm [set_operation] 20160115:b
                    for (int i = 0; i < select_stmt->get_result_type_array().count(); i ++)
                    {
                      ObObjType res_type = select_stmt->get_result_type_array().at(i);
                      union_op->get_result_type_array().push_back(res_type);
                    }
                    del 20160115:e
          */
                    //add qianzm [set_operation] 20160115
                    if (OB_SUCCESS == ret)//add qianzm[null operator unjudgement bug1181] 20160520
                    {
                        union_op->add_result_type_array(tmp_result_columns_type);
                    }
                    //add 20160115:e
                    set_op = union_op;
                    break;
                }
            case ObSelectStmt::INTERSECT :
            {
              ObMergeIntersect *intersect_op = NULL;
              CREATE_PHY_OPERRATOR(intersect_op, ObMergeIntersect, physical_plan, err_stat);
              /*del qianzm [set_operation] 20160115:b
                    for (int i = 0; i < select_stmt->get_result_type_array().count(); i ++)
                    {
                      ObObjType res_type = select_stmt->get_result_type_array().at(i);
                      intersect_op->get_result_type_array().push_back(res_type);
                    }
                    del 20160115:e
          */
              //add qianzm [set_operation] 20160115
              if (OB_SUCCESS == ret)//add qianzm[null operator unjudgement bug1181] 20160520
              {
                  intersect_op->add_result_type_array(tmp_result_columns_type);
              }
              //add 20160115:e
              set_op = intersect_op;
              break;
            }
            case ObSelectStmt::EXCEPT :
            {
              ObMergeExcept *except_op = NULL;
              CREATE_PHY_OPERRATOR(except_op, ObMergeExcept, physical_plan, err_stat);
              /*del qianzm [set_operation] 20160115:b
                    for (int i = 0; i < select_stmt->get_result_type_array().count(); i ++)
                    {
                      ObObjType res_type = select_stmt->get_result_type_array().at(i);
                      except_op->get_result_type_array().push_back(res_type);
                    }
                    del 20160115:e
          */
              //add qianzm [set_operation] 20160115
              if (OB_SUCCESS == ret)//add qianzm[null operator unjudgement bug1181] 20160520
              {
                  except_op->add_result_type_array(tmp_result_columns_type);
              }
              //add 20160115:e
              set_op = except_op;
              break;
            }
            default:
              break;
          }
          if (OB_SUCCESS == ret)  // ret is a reference to err_stat.err_code_
          {
            set_op->set_distinct(select_stmt->is_set_distinct() ? true : false);
          }
        }
        int32_t lidx = OB_INVALID_INDEX;
        int32_t ridx = OB_INVALID_INDEX;
        if (ret == OB_SUCCESS)
        {
          ret = gen_physical_select(
                logical_plan,
                physical_plan,
                err_stat,
                select_stmt->get_left_query_id(),
                &lidx);
        }
        if (ret == OB_SUCCESS)
        {
          ret = gen_physical_select(
                logical_plan,
                physical_plan,
                err_stat,
                select_stmt->get_right_query_id(),
                &ridx);
        }

            if (ret == OB_SUCCESS)
            {
                ObPhyOperator *left_op = physical_plan->get_phy_query(lidx);
                ObPhyOperator *right_op = physical_plan->get_phy_query(ridx);
                ObSelectStmt *lselect = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(select_stmt->get_left_query_id()));
                ObSelectStmt *rselect = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(select_stmt->get_right_query_id()));
                if (set_type != ObSelectStmt::UNION || select_stmt->is_set_distinct())
                {
                    // 1
                    // select c1+c2 from tbl
                    // union
                    // select c3+c4 rom tbl
                    // order by 1;

                    // 2
                    // select c1+c2 as cc from tbl
                    // union
                    // select c3+c4 from tbl
                    // order by cc;

                    // there must be a Project operator on union part,
                    // so do not worry non-column expr appear in sot operator

                    //CREATE sort operators
                    /* Create first sort operator */
                    ObSort *left_sort = NULL;
                    if (CREATE_PHY_OPERRATOR(left_sort, ObSort, physical_plan, err_stat) == NULL)
                    {
                    }
                    else if (ret == OB_SUCCESS && (ret = left_sort->set_child(0, *left_op)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Set child of sort operator failed");
                    }
                    ObSqlRawExpr *sort_expr = NULL;
                    for (int32_t i = 0; ret == OB_SUCCESS && i < lselect->get_select_item_size(); i++)
                    {
                        sort_expr = logical_plan->get_expr(lselect->get_select_item(i).expr_id_);
                        if (sort_expr == NULL || sort_expr->get_expr() == NULL)
                        {
                            ret = OB_ERR_ILLEGAL_ID;
                            TRANS_LOG("Get internal expression failed");
                            break;
                        }
                        ret = left_sort->add_sort_column(sort_expr->get_table_id(), sort_expr->get_column_id(), true);
                        if (ret != OB_SUCCESS)
                        {
                            TRANS_LOG("Add sort column failed");
                        }
                    }

                    /* Create second sort operator */
                    ObSort *right_sort = NULL;
                    if (ret == OB_SUCCESS)
                        CREATE_PHY_OPERRATOR(right_sort, ObSort, physical_plan, err_stat);
                    if (ret == OB_SUCCESS && (ret = right_sort->set_child(0 /* first child */, *right_op)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Set child of sort operator failed");
                    }
                    for (int32_t i = 0; ret == OB_SUCCESS && i < rselect->get_select_item_size(); i++)
                    {
                        sort_expr = logical_plan->get_expr(rselect->get_select_item(i).expr_id_);
                        if (sort_expr == NULL || sort_expr->get_expr() == NULL)
                        {
                            ret = OB_ERR_ILLEGAL_ID;
                            TRANS_LOG("Get internal expression failed");
                            break;
                        }
                        ret = right_sort->add_sort_column(sort_expr->get_table_id(), sort_expr->get_column_id(), true);
                        if (ret != OB_SUCCESS)
                        {
                            TRANS_LOG("Add sort column failed");
                            break;
                        }
                    }
                    //add qianzm [set_operation] 20151222 :b
                    if (set_type == ObSelectStmt::UNION && !select_stmt->is_set_distinct())
                    {
                    }
                    else
                    {
                        left_sort->set_is_set_op_flag();
              right_sort->set_is_set_op_flag();
              /*del qianzm [set_operation] 20160115:b
                        for (int i = 0; i < select_stmt->get_result_type_array().count(); i ++)
                        {
                            ObObjType res_type = select_stmt->get_result_type_array().at(i);
                            left_sort->get_result_type_array().push_back(res_type);
                            right_sort->get_result_type_array().push_back(res_type);
                        }
                        //del 20160115:e*/
              //add qianzm [set_operation] 20160115:b
              left_sort->add_result_type_array_for_setop(tmp_result_columns_type);
              right_sort->add_result_type_array_for_setop(tmp_result_columns_type);
              //add 20160115:e
                    }
                    //add e
                    left_op = left_sort;
                    right_op = right_sort;
                }
                OB_ASSERT(NULL != set_op);
                set_op->set_child(0 /* first child */, *left_op);
                set_op->set_child(1 /* second child */, *right_op);
            }
            result_op = set_op;

            // generate physical plan for order by
            if (ret == OB_SUCCESS && select_stmt->get_order_item_size() > 0)
                ret = gen_phy_order_by(logical_plan, physical_plan, err_stat, select_stmt, result_op, result_op, true);

            // generate physical plan for limit
            if (ret == OB_SUCCESS && select_stmt->has_limit())
            {
                ret = gen_phy_limit(logical_plan, physical_plan, err_stat, select_stmt, result_op, result_op);
            }

            if (ret == OB_SUCCESS)
            {
                ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, select_stmt, result_op, index);
            }
        }
        else
        {
            /* Normal Select Statement */
            bool group_agg_pushed_down = false;
            bool limit_pushed_down = false;
            //add liuzy [sequence select] [JHOBv0.1] 20150525:b
            ObSequenceSelect *sequence_select_op = NULL;
            if (select_stmt->has_sequence())
            {
                ObSEArray<int64_t, 64> row_desc_map;
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    CREATE_PHY_OPERRATOR(sequence_select_op, ObSequenceSelect, physical_plan, err_stat);
                }
                if (OB_SUCCESS != ret)//add qianzm [null operator unjudgement bug1181] 20160520
                {
                }
                else if (OB_SUCCESS != (ret = wrap_sequence(logical_plan, physical_plan, err_stat,
                                                       row_desc_map, sequence_select_op, select_stmt)))
                {
                    TRANS_LOG("wrap sequence failed");
                }
                /*get the sequence info from the "__all_sequences" for all prevval use*/
                else if (OB_SUCCESS != (ret = sequence_select_op->prepare_sequence_prevval()))
                {
                    TRANS_LOG("prepare sequence info failed!");
                }
                else
                {
                    sequence_select_op->copy_sequence_info_from_select_stmt(select_stmt);
                }
            }
            //add 20150525:e
            // 1. generate physical plan for base-table/outer-join-table/temporary table
            ObList<ObPhyOperator*> phy_table_list;
            ObList<ObBitSet<> > bitset_list;
            ObList<ObSqlRawExpr*> remainder_cnd_list;
            ObList<ObSqlRawExpr*> none_columnlize_alias;
            if (ret == OB_SUCCESS)
                ret = gen_phy_tables(
                            logical_plan,
                            physical_plan,
                            err_stat,
                            select_stmt,
                            group_agg_pushed_down,
                            limit_pushed_down,
                            phy_table_list,
                            bitset_list,
                            remainder_cnd_list,
                            none_columnlize_alias
                            ,sequence_select_op //add liuzy [sequence select]20160616:", sequence_select_op"
                            );

            // 2. Join all tables
            //add by wanglei [semi join] 20151231:b
            //select_stmt->get_query_hint().join_array_.clear();
            //add:e
            if (ret == OB_SUCCESS && phy_table_list.size() > 1)
                ret = gen_phy_joins(
                            logical_plan,
                            physical_plan,
                            err_stat,
                            select_stmt,
                            ObJoin::INNER_JOIN,
                            phy_table_list,
                            bitset_list,
                            remainder_cnd_list,
                            none_columnlize_alias);
            if (ret == OB_SUCCESS)
                phy_table_list.pop_front(result_op);

            // 3. add filter(s) to the join-op/table-scan-op result
            if (ret == OB_SUCCESS && remainder_cnd_list.size() >= 1)
            {
                ObFilter *filter_op = NULL;
                CREATE_PHY_OPERRATOR(filter_op, ObFilter, physical_plan, err_stat);
                if (ret == OB_SUCCESS && (ret = filter_op->set_child(0, *result_op)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set child of filter plan failed");
                }
                oceanbase::common::ObList<ObSqlRawExpr*>::iterator cnd_it;
                for (cnd_it = remainder_cnd_list.begin();
                     ret == OB_SUCCESS && cnd_it != remainder_cnd_list.end();
                     cnd_it++)
                {
                    ObSqlExpression *filter = ObSqlExpression::alloc();
                    if (NULL == filter
                            || (ret = (*cnd_it)->fill_sql_expression(
                                    *filter,
                                    this,
                                    logical_plan,
                                    physical_plan)) != OB_SUCCESS
                            || (ret = filter_op->add_filter(filter)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add filters to filter plan failed");
                        break;
                    }
                    //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
                    else if (filter->get_sub_query_num() > 0)
                    {
                        filter_op->set_need_handle_sub_query(true);
                    }
                    //add tianz 20151016:e
                }
                if (ret == OB_SUCCESS)
                    result_op = filter_op;
            }

            // 4. generate physical plan for group by/aggregate
            if (ret == OB_SUCCESS && (select_stmt->get_group_expr_size() > 0 || select_stmt->get_agg_fun_size() > 0))
            {
                if (group_agg_pushed_down == false)
                {
                    if (select_stmt->get_group_expr_size() > 0)
                        ret = gen_phy_group_by(logical_plan, physical_plan, err_stat, select_stmt, result_op, result_op);
                    else if (select_stmt->get_agg_fun_size() > 0)
                        ret = gen_phy_scalar_aggregate(logical_plan, physical_plan, err_stat, select_stmt, result_op, result_op);
                }
                if (ret == OB_SUCCESS && none_columnlize_alias.size() > 0)
                {
                    // compute complex expressions that contain aggreate functions
                    ObAddProject *project_op = NULL;
                    oceanbase::common::ObList<ObSqlRawExpr*>::iterator alias_it;
                    for (alias_it = none_columnlize_alias.begin(); ret == OB_SUCCESS && alias_it != none_columnlize_alias.end();)
                    {
                        if ((*alias_it)->is_columnlized() == false
                                && (*alias_it)->is_contain_aggr() && (*alias_it)->get_expr()->get_expr_type() != T_REF_COLUMN)
                        {
                            if (project_op == NULL)
                            {
                                CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan, err_stat);
                                if (ret == OB_SUCCESS && (ret = project_op->set_child(0, *result_op)) != OB_SUCCESS)
                                {
                                    TRANS_LOG("Set child of filter plan failed");
                                    break;
                                }
                            }
                            (*alias_it)->set_columnlized(true);
                            ObSqlExpression alias_expr;
                            if ((ret = (*alias_it)->fill_sql_expression(
                                     alias_expr,
                                     this,
                                     logical_plan,
                                     physical_plan)) != OB_SUCCESS
                                    || (ret = project_op->add_output_column(alias_expr)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add project on aggregate plan failed");
                                break;
                            }
                        }
                        common::ObList<ObSqlRawExpr*>::iterator del_alias = alias_it;
                        alias_it++;
                        if ((ret = none_columnlize_alias.erase(del_alias)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add project on aggregate plan failed");
                            break;
                        }
                    }
                    if (ret == OB_SUCCESS && project_op != NULL)
                        result_op = project_op;
                }
            }

            // 5. generate physical plan for having
            if (ret == OB_SUCCESS && select_stmt->get_having_expr_size() > 0)
            {
                ObFilter *having_op = NULL;
                CREATE_PHY_OPERRATOR(having_op, ObFilter, physical_plan, err_stat);
                ObSqlRawExpr *having_expr;
                int32_t num = select_stmt->get_having_expr_size();
                for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
                {
                    having_expr = logical_plan->get_expr(select_stmt->get_having_expr_id(i));
                    OB_ASSERT(NULL != having_expr);
                    ObSqlExpression *having_filter = ObSqlExpression::alloc();
                    if (NULL == having_filter
                            || (ret = having_expr->fill_sql_expression(
                                    *having_filter,
                                    this,
                                    logical_plan,
                                    physical_plan)) != OB_SUCCESS
                            || (ret = having_op->add_filter(having_filter)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add filters to having plan failed");
                        break;
                    }
                }
                if (ret == OB_SUCCESS)
                {
                    if ((ret = having_op->set_child(0, *result_op)) == OB_SUCCESS)
                    {
                        result_op = having_op;
                    }
                    else
                    {
                        TRANS_LOG("Add child of having plan failed");
                    }
                }
            }

            // 6. generate physical plan for distinct
            if (ret == OB_SUCCESS && select_stmt->is_distinct())
                ret = gen_phy_distinct(logical_plan, physical_plan, err_stat, select_stmt, result_op, result_op);

            // 7. generate physical plan for order by
            if (ret == OB_SUCCESS && select_stmt->get_order_item_size() > 0)
                ret = gen_phy_order_by(logical_plan, physical_plan, err_stat, select_stmt, result_op, result_op);

            // 8. generate physical plan for limit
            if (ret == OB_SUCCESS && limit_pushed_down == false && select_stmt->has_limit())
            {
                ret = gen_phy_limit(logical_plan, physical_plan, err_stat, select_stmt, result_op, result_op);
            }

            //add liumz, [ROW_NUMBER]20150825:b
            // 9. generate physical plan for analytic func
            if (OB_SUCCESS == ret)
            {
                if (select_stmt->get_partition_expr_size() > 0 || select_stmt->get_order_item_for_rownum_size() > 0)
                {
                    if(OB_SUCCESS != (ret = gen_phy_partition_by(logical_plan, physical_plan, err_stat, select_stmt, result_op, result_op)))
                    {
                        TRANS_LOG("Gen partition physical plan failed");
                    }
                }
                else if (select_stmt->get_anal_fun_size() > 0)
                {
                    if (OB_SUCCESS != (ret = gen_phy_scalar_analytic(logical_plan, physical_plan, err_stat, select_stmt, result_op, result_op)))
                    {
                        TRANS_LOG("Gen scalar_analytic physical plan failed");
                    }
                }
            }
            //add:e

            // 10. generate physical plan for select clause
            if (ret == OB_SUCCESS && select_stmt->get_select_item_size() > 0)
            {
                ObProject *project_op = NULL;
                CREATE_PHY_OPERRATOR(project_op, ObProject, physical_plan, err_stat);
                if (ret == OB_SUCCESS && (ret = project_op->set_child(0, *result_op)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add child of project plan failed");
                }
                //add liuzy [sequence select] 20150703:b
                if (OB_SUCCESS == ret && select_stmt->select_clause_has_sequence())
                {
                    if (OB_SUCCESS != (ret = sequence_select_op->set_child(1, *project_op)))
                    {
                        TRANS_LOG("Add child of select_op plan failed");
                    }
                    if (OB_SUCCESS == ret)
                    {
                        project_op->set_sequence_select_op();
                    }
                }
                //add 20150703:e

                ObSqlRawExpr *select_expr;
                int32_t num = select_stmt->get_select_item_size();
                for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
                {
                    const SelectItem& select_item = select_stmt->get_select_item(i);
                    select_expr = logical_plan->get_expr(select_item.expr_id_);
                    OB_ASSERT(NULL != select_expr);
                    //TBSYS_LOG(ERROR,"test::fanqs,,select_item.is_real_alias_=%d,select_expr->is_columnlized()=%d",select_item.is_real_alias_,select_expr->is_columnlized());
                    //add liuzy [sequence select] 20150728:b
                    ObSqlExpression real_alias_expr;
                    if (select_stmt->select_clause_has_sequence()
                            && OB_SUCCESS != (ret = select_expr->fill_sql_expression(real_alias_expr,
                                                                                     this,
                                                                                     logical_plan,
                                                                                     physical_plan)))
                    {
                        TRANS_LOG("Fill sql expression failed.");
                        break;
                    }
                    //add 20150728:e

                    //delete by xionghui 20151109 b:
                    /*
          if (select_item.is_real_alias_ && select_expr->is_columnlized())
          {
              ObBinaryRefRawExpr col_raw(OB_INVALID_ID, select_expr->get_column_id(), T_REF_COLUMN);
              ObSqlRawExpr col_sql_raw(*select_expr);
              col_sql_raw.set_expr(&col_raw);
              ObSqlExpression  col_expr;
              if ((ret = col_sql_raw.fill_sql_expression(col_expr)) != OB_SUCCESS
                      || (ret = project_op ->add_output_column(col_expr)) != OB_SUCCESS)
              {
                  TRANS_LOG("Add output column to project plan failed");
                  break;
              }
              //add liuzy [sequence select]20150703:b
              else if (select_stmt->select_clause_has_sequence()
                       && OB_SUCCESS != (ret = sequence_select_op->add_output_column(real_alias_expr)))
              {
                  TRANS_LOG("Add output column to sequence select plan failed");
                  break;
              }
              //add 20150703:e
          }
            */
                    //delete by 20151109 e:

                    //add xionghui 20151109 [fix select NULL bug] b:
                    //在这里不强行的变为COL的表达式，分为两种情况，如果有列引用则走以前的流程，否则则把这个表达式重新计算，再加到最上层的project中
                    if (select_item.is_real_alias_ && select_expr->is_columnlized())
                    {
                        bool is_column_expr = select_expr->get_expr()->is_column();
                        if(is_column_expr)
                        {
                            ObBinaryRefRawExpr col_raw(OB_INVALID_ID, select_expr->get_column_id(), T_REF_COLUMN);
                            ObSqlRawExpr col_sql_raw(*select_expr);
                            col_sql_raw.set_expr(&col_raw);
                            ObSqlExpression  col_expr;
                            if ((ret = col_sql_raw.fill_sql_expression(col_expr)) != OB_SUCCESS
                                    || (ret = project_op ->add_output_column(col_expr)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add output column to project plan failed");
                                break;
                            }
                        }
                        else
                        {
                            ObSqlExpression  col_expr;
                            if ((ret = select_expr->fill_sql_expression(
                                     col_expr,
                                     this,
                                     logical_plan,
                                     physical_plan)) != OB_SUCCESS
                                    || (ret = project_op ->add_output_column(col_expr)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add output column to project plan failed");
                                break;
                            }
                        }
                        //add liuzy [sequence select]20150703:b
                        if(ret == OB_SUCCESS )
                        {
                            if( select_stmt->select_clause_has_sequence()
                                    && OB_SUCCESS != (ret = sequence_select_op->add_output_column(real_alias_expr)))
                            {
                                TRANS_LOG("Add output column to sequence select plan failed");
                                break;
                            }
                        }
                        //add 20150703:e
                    }
                    //add 20151109 e:
                    else
                    {
                        ObSqlExpression  col_expr;
                        if ((ret = select_expr->fill_sql_expression(
                                 col_expr,
                                 this,
                                 logical_plan,
                                 physical_plan)) != OB_SUCCESS
                                || (ret = project_op ->add_output_column(col_expr)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add output column to project plan failed");
                            break;
                        }
                        //add liuzy [sequence select]20150703:b
                        else if (select_stmt->select_clause_has_sequence()
                                 && (ret = sequence_select_op->add_output_column(col_expr)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add output column to sequence select plan failed");
                            break;
                        }
                        //add 20150703:e
                    }
                    select_expr->set_columnlized(true);
                }
                //add liuzy [sequence select]20150706:b
                if (OB_SUCCESS == ret)
                {
                    if (select_stmt->select_clause_has_sequence())
                    {
                        result_op = sequence_select_op;
                    }
                    else
                    {
                        result_op = project_op;
                    }
                }
                //add 20150706:e
                //del liuzy [sequence select]20150706:b
                //        if (ret == OB_SUCCESS)
                //          result_op = project_op;
                //del 20150706:e
            }

            if (ret == OB_SUCCESS)
            {
                ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, select_stmt, result_op, index);
            }
            //add tianz [EXPORT_TOOL] 20141120:b
            if (select_stmt->get_has_range())
            {
                physical_plan->set_has_range();
                if (select_stmt->start_is_min())
                {
                    physical_plan->set_start_is_min();
                }
                else if (select_stmt->end_is_max())
                {
                    physical_plan->set_end_is_max();
                }
                const ObTableSchema *table_schema = NULL;
                if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(select_stmt->get_table_id())))
                {
                    ret = OB_ERR_ILLEGAL_ID;
                    TRANS_LOG("fail to get table schema for table[%ld]", select_stmt->get_table_id());
                }
                else
                {
                    ObRowkeyInfo rowkey_info = table_schema->get_rowkey_info();
                    int64_t rowkey_col_num = rowkey_info.get_size();
                    for(int i = 0; ret == OB_SUCCESS && i < select_stmt->get_range_vector_count(); i++)
                    {
                        const ObArray<uint64_t>& value_row = select_stmt->get_value_row(i);
                        if ( value_row.count() != rowkey_col_num)
                        {
                            ret = OB_ERR_INSERT_NULL_ROWKEY;
                            TRANS_LOG("range values num not match the rowkey num");
                        }
                        for (int j = 0; ret == OB_SUCCESS && j < value_row.count(); j++)
                        {
                            ObSqlRawExpr *value_expr = logical_plan->get_expr(value_row.at(j));
                            ObConstRawExpr* const_value = NULL;
                            ObObj obj_value;
              ObObj obj_value2;//add qianzm [EXPORT_TOOL bug76] 20160421
                            if (value_expr == NULL)
                            {
                                ret = OB_ERR_ILLEGAL_ID;
                                TRANS_LOG("Get value failed");
                            }
                            else if(NULL == (const_value = dynamic_cast<ObConstRawExpr*>(value_expr->get_expr())))
                            {
                                ret = OB_ERR_ILLEGAL_ID;
                                TRANS_LOG("Get const value failed, should be const value");
                            }
                            else if ((ret = ob_write_obj(*mem_pool_,
                                                         const_value->get_value(),
                                                         obj_value)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Failed to copy range value, err=%d", ret);
                                break;
                            }
                            else
                            {
                                const ObRowkeyColumn *rowkey_column = rowkey_info.get_column(j);
                                OB_ASSERT(rowkey_column);
                                if (rowkey_column->type_ == ObPreciseDateTimeType)
                                {
                                    if (obj_value.get_type()!= ObIntType)
                                    {
                                        ret = OB_OBJ_TYPE_ERROR;
                                    }
                                    else
                                    {
                                        obj_value.set_precise_datetime_type();
                                    }
                                }
                //add qianzm [EXPORT_TOOL bug76] 20160421
                                if (rowkey_column->type_ != obj_value.get_type())
                                {
                                    if (rowkey_column->type_ == ObDateTimeType)
                                    {
                                        if (obj_value.get_type()!= ObIntType)
                                        {
                                            ret = OB_OBJ_TYPE_ERROR;
                                        }
                                        else
                                        {
                                            int64_t date_time_val = 0;
                                            ret = obj_value.get_int(date_time_val);
                                            obj_value.set_datetime(static_cast<ObDateTime>(date_time_val));
                                        }
                                    }
                                    else if (rowkey_column->type_ == ObDateType)
                                    {
                                        if (obj_value.get_type()!= ObIntType)
                                        {
                                            ret = OB_OBJ_TYPE_ERROR;
                                        }
                                        else
                                        {
                                            int64_t date_val = 0;
                                            ret = obj_value.get_int(date_val);
                                            obj_value.set_date(static_cast<ObPreciseDateTime>(date_val));
                                        }
                                    }
                                    else if (rowkey_column->type_ == ObTimeType)
                                    {
                                        if (obj_value.get_type()!= ObIntType)
                                        {
                                            ret = OB_OBJ_TYPE_ERROR;
                                        }
                                        else
                                        {
                                            int64_t time_val = 0;
                                            ret = obj_value.get_int(time_val);
                                            obj_value.set_time(static_cast<ObPreciseDateTime>(time_val));
                                        }
                                    }
                                    else if (rowkey_column->type_ == ObInt32Type)
                                    {
                                        if (obj_value.get_type()!= ObIntType)
                                        {
                                            ret = OB_OBJ_TYPE_ERROR;
                                        }
                                        else
                                        {
                                            int64_t int_val = 0;
                                            ret = obj_value.get_int(int_val);
                                            obj_value.set_int32(static_cast<int32_t>(int_val));
                                        }
                                    }
                                    else if (rowkey_column->type_ == ObDecimalType)
                                    {
                                        if (obj_value.get_type()!= ObIntType)
                                        {
                                            ret = OB_OBJ_TYPE_ERROR;
                                        }
                                        else
                                        {
                                            int64_t int_val = 0;
                                            ret = obj_value.get_int(int_val);
                                            char tmp_buf[128];
                                            std::string int_str;
                                            sprintf(tmp_buf, "%ld", int_val);
                                            int_str = tmp_buf;
                                            ObString tmp_os = ObString::make_string(int_str.c_str());
                                            obj_value2.set_decimal(tmp_os);
                                            if ((ret = ob_write_obj(*mem_pool_,
                                                                    obj_value2,
                                                                    obj_value)) != OB_SUCCESS)
                                            {
                                                TRANS_LOG("Failed to copy range value, err=%d", ret);
                                                break;
                                            }
                                        }
                                    }
                                }
                                //add 20160421:e
                            }

                            if (OB_SUCCESS != ret)
                            {
                            }
                            else if (0 == i && !select_stmt->start_is_min())
                            {
                                physical_plan->add_start_range_values(j,obj_value);
                            }
                            else
                            {
                                physical_plan->add_end_range_values(j,obj_value);
                            }
                        }
                    }
                }
            }
            //add 20141120:e
        }
    }
    return ret;
}

int ObTransformer::gen_phy_limit(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObSelectStmt *select_stmt,
        ObPhyOperator *in_op,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObLimit *limit_op = NULL;
    if (!select_stmt->has_limit())
    {
        /* skip */
    }
    else if (CREATE_PHY_OPERRATOR(limit_op, ObLimit, physical_plan, err_stat) == NULL)
    {
    }
    else if ((ret = limit_op->set_child(0, *in_op)) != OB_SUCCESS)
    {
        TRANS_LOG("Add child of limit plan failed");
    }
    else
    {
        ObSqlExpression limit_count;
        ObSqlExpression limit_offset;
        ObSqlExpression *ptr = &limit_count;
        uint64_t id = select_stmt->get_limit_expr_id();
        int64_t i = 0;
        for (; ret == OB_SUCCESS && i < 2;
             i++, id = select_stmt->get_offset_expr_id(), ptr = &limit_offset)
        {
            ObSqlRawExpr *raw_expr = NULL;
            if (id == OB_INVALID_ID)
            {
                continue;
            }
            else if ((raw_expr = logical_plan->get_expr(id)) == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong internal expression id = %lu, ret=%d", id, ret);
                break;
            }
            else if ((ret = raw_expr->fill_sql_expression(
                          *ptr,
                          this,
                          logical_plan,
                          physical_plan)) != OB_SUCCESS)
            {
                TRANS_LOG("Add limit/offset faild");
                break;
            }
        }
        if (ret == OB_SUCCESS && (ret = limit_op->set_limit(limit_count, limit_offset)) != OB_SUCCESS)
        {
            TRANS_LOG("Set limit/offset failed, ret=%d", ret);
        }
    }
    if (ret == OB_SUCCESS)
        out_op = limit_op;
    return ret;
}

int ObTransformer::gen_phy_order_by(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObSelectStmt *select_stmt,
        ObPhyOperator *in_op,
        ObPhyOperator *&out_op,
        bool use_generated_id)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObSort *sort_op = NULL;
    ObProject *project_op = NULL;
    CREATE_PHY_OPERRATOR(sort_op, ObSort, physical_plan, err_stat);

    ObSqlRawExpr *order_expr;
    int32_t num = select_stmt->get_order_item_size();
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
        //add liumz, [optimize group_order by index]20170419:b
        if (select_stmt->get_order_item_flag(i))
        {
          continue;
        }
        //add:e
        const OrderItem& order_item = select_stmt->get_order_item(i);
        order_expr = logical_plan->get_expr(order_item.expr_id_);
        if (order_expr->get_expr()->is_const())
        {
            // do nothing, const column is of no usage for sorting
        }
        else if (order_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
        {
            ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(order_expr->get_expr());
            if ((ret = sort_op->add_sort_column(
                     use_generated_id? order_expr->get_table_id() : col_expr->get_first_ref_id(),
                     use_generated_id? order_expr->get_column_id() : col_expr->get_second_ref_id(),
                     order_item.order_type_ == OrderItem::ASC ? true : false
                     )) != OB_SUCCESS)
            {
                TRANS_LOG("Add sort column to sort plan failed");
                break;
            }
        }
        else
        {
            if (!project_op)
            {
                CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan, err_stat);
                if (ret != OB_SUCCESS)
                    break;
                if ((ret = project_op->set_child(0, *in_op)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add child of project plan failed");
                    break;
                }
            }
            ObSqlExpression col_expr;
            if ((ret = order_expr->fill_sql_expression(
                     col_expr,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS
                    || (ret = project_op->add_output_column(col_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add output column to project plan failed");
                break;
            }
            if ((ret = sort_op->add_sort_column(
                     order_expr->get_table_id(),
                     order_expr->get_column_id(),
                     order_item.order_type_ == OrderItem::ASC ? true : false
                     )) != OB_SUCCESS)
            {
                TRANS_LOG("Add sort column to sort plan failed");
                break;
            }
        }
    }
    if (ret == OB_SUCCESS)
    {
        if (project_op)
            ret = sort_op->set_child(0, *project_op);
        else
            ret = sort_op->set_child(0, *in_op);
        if (ret != OB_SUCCESS)
        {
            TRANS_LOG("Add child of sort plan failed");
        }
    }
    if (ret == OB_SUCCESS)
    {
        if (sort_op->get_sort_column_size() > 0)
            out_op = sort_op;
        else
            out_op = in_op;
    }

    return ret;
}

int ObTransformer::gen_phy_distinct(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObSelectStmt *select_stmt,
        ObPhyOperator *in_op,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObMergeDistinct *distinct_op = NULL;
    ObSort *sort_op = NULL;
    ObProject *project_op = NULL;
    if (ret == OB_SUCCESS)
        CREATE_PHY_OPERRATOR(sort_op, ObSort, physical_plan, err_stat);
    if (ret == OB_SUCCESS)
        CREATE_PHY_OPERRATOR(distinct_op, ObMergeDistinct, physical_plan, err_stat);
    if (ret == OB_SUCCESS && (ret = distinct_op->set_child(0, *sort_op)) != OB_SUCCESS)
    {
        TRANS_LOG("Add child of distinct plan failed");
    }

    ObSqlRawExpr *select_expr;
    int32_t num = select_stmt->get_select_item_size();
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
        const SelectItem& select_item = select_stmt->get_select_item(i);
        select_expr = logical_plan->get_expr(select_item.expr_id_);
        if (select_expr->get_expr()->is_const())
        {
            // do nothing, const column is of no usage for sorting
        }
        else if (select_item.is_real_alias_)
        {
            ret = sort_op->add_sort_column(select_expr->get_table_id(), select_expr->get_column_id(), true);
            if (ret != OB_SUCCESS)
            {
                TRANS_LOG("Add sort column of sort plan failed");
                break;
            }
            ret = distinct_op->add_distinct_column(select_expr->get_table_id(), select_expr->get_column_id());
            if (ret != OB_SUCCESS)
            {
                TRANS_LOG("Add distinct column of distinct plan failed");
                break;
            }
        }
        else if (select_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
        {
            ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(select_expr->get_expr());
            ret = sort_op->add_sort_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id(), true);
            if (ret != OB_SUCCESS)
            {
                TRANS_LOG("Add sort column to sort plan failed");
                break;
            }
            ret = distinct_op->add_distinct_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
            if (ret != OB_SUCCESS)
            {
                TRANS_LOG("Add distinct column to distinct plan failed");
                break;
            }
        }
        else
        {
            if (!project_op)
            {
                CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan, err_stat);
                if (ret != OB_SUCCESS)
                    break;
                if ((ret = project_op->set_child(0, *in_op)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add child of project plan failed");
                    break;
                }
            }
            ObSqlExpression col_expr;
            if ((ret = select_expr->fill_sql_expression(
                     col_expr,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS
                    || (ret = project_op->add_output_column(col_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add output column to project plan failed");
                break;
            }
            if ((ret = sort_op->add_sort_column(
                     select_expr->get_table_id(),
                     select_expr->get_column_id(),
                     true)) != OB_SUCCESS)
            {
                TRANS_LOG("Add sort column to sort plan failed");
                break;
            }
            if ((ret = distinct_op->add_distinct_column(
                     select_expr->get_table_id(),
                     select_expr->get_column_id())) != OB_SUCCESS)
            {
                TRANS_LOG("Add distinct column to distinct plan failed");
                break;
            }
        }
    }
    if (ret == OB_SUCCESS)
    {
        if (project_op)
            ret = sort_op->set_child(0, *project_op);
        else
            ret = sort_op->set_child(0, *in_op);
        if (ret != OB_SUCCESS)
        {
            TRANS_LOG("Add child to sort plan failed");
        }
    }
    if (ret == OB_SUCCESS)
    {
        out_op = distinct_op;
    }

    return ret;
}

int ObTransformer::gen_phy_group_by(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObSelectStmt *select_stmt,
        ObPhyOperator *in_op,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObMergeGroupBy *group_op = NULL;
    ObSort *sort_op = NULL;
    ObProject *project_op = NULL;
    if (ret == OB_SUCCESS && !select_stmt->is_indexed_group())//mod liumz, [optimize group_order by index]20170419
        CREATE_PHY_OPERRATOR(sort_op, ObSort, physical_plan, err_stat);
    if (ret == OB_SUCCESS)
        CREATE_PHY_OPERRATOR(group_op, ObMergeGroupBy, physical_plan, err_stat);
    if (ret == OB_SUCCESS && !select_stmt->is_indexed_group() && (ret = group_op->set_child(0, *sort_op)) != OB_SUCCESS)//mod liumz, [optimize group_order by index]20170419
    {
        TRANS_LOG("Add child of group by plan faild");
    }

    ObSqlRawExpr *group_expr;
    int32_t num = select_stmt->get_group_expr_size();
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
        group_expr = logical_plan->get_expr(select_stmt->get_group_expr_id(i));
        OB_ASSERT(NULL != group_expr);
        if (group_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
        {
            ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(group_expr->get_expr());
            OB_ASSERT(NULL != col_expr);
            if (!select_stmt->is_indexed_group())//add liumz, [optimize group_order by index]20170419
              ret = sort_op->add_sort_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id(), true);
            if (ret != OB_SUCCESS)
            {
                TRANS_LOG("Add sort column faild, table_id=%lu, column_id=%lu",
                          col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
                break;
            }
            ret = group_op->add_group_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
            if (ret != OB_SUCCESS)
            {
                TRANS_LOG("Add group column faild, table_id=%lu, column_id=%lu",
                          col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
                break;
            }
        }
        else if (group_expr->get_expr()->is_const())
        {
            // do nothing, const column is of no usage for sorting
        }
        else
        {
            if (!project_op)
            {
                CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan, err_stat);
                if (ret != OB_SUCCESS)
                    break;
                if ((ret = project_op->set_child(0, *in_op)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add child of project plan faild");
                    break;
                }
            }
            ObSqlExpression col_expr;
            if ((ret = group_expr->fill_sql_expression(
                     col_expr,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS
                    || (ret = project_op->add_output_column(col_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add output column to project plan faild");
                break;
            }
            if (!select_stmt->is_indexed_group() &&//add liumz, [optimize group_order by index]20170419
                (ret = sort_op->add_sort_column(
                     group_expr->get_table_id(),
                     group_expr->get_column_id(),
                     true)) != OB_SUCCESS)
            {
                TRANS_LOG("Add sort column to sort plan faild");
                break;
            }
            if ((ret = group_op->add_group_column(
                     group_expr->get_table_id(),
                     group_expr->get_column_id())) != OB_SUCCESS)
            {
                TRANS_LOG("Add group column to group plan faild");
                break;
            }
        }
    }
    if (ret == OB_SUCCESS)
    {
	  //add liumz, [optimize group_order by index]20170419:b
      if (select_stmt->is_indexed_group())
      {
        if (project_op)
            ret = group_op->set_child(0, *project_op);
        else
            ret = group_op->set_child(0, *in_op);
      }
      else
      {
	  //add:e
        if (project_op)
            ret = sort_op->set_child(0, *project_op);
        else
            ret = sort_op->set_child(0, *in_op);
      }
        if (ret != OB_SUCCESS)
        {
            TRANS_LOG("Add child to sort plan faild");
        }
    }

    num = select_stmt->get_agg_fun_size();
    ObSqlRawExpr *agg_expr = NULL;
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
        agg_expr = logical_plan->get_expr(select_stmt->get_agg_expr_id(i));
        OB_ASSERT(NULL != agg_expr);
        if (agg_expr->get_expr()->is_aggr_fun())
        {
            ObSqlExpression new_agg_expr;
            if ((ret = agg_expr->fill_sql_expression(
                     new_agg_expr,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS
                    || (ret = group_op->add_aggr_column(new_agg_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add aggregate function to group plan faild");
                break;
            }
        }
        else
        {
            TRANS_LOG("Wrong aggregate function, exp_id = %lu", agg_expr->get_expr_id());
            break;
        }
        agg_expr->set_columnlized(true);
    }
    if (ret == OB_SUCCESS)
        out_op = group_op;

    return ret;
}

int ObTransformer::gen_phy_scalar_aggregate(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObSelectStmt *select_stmt,
        ObPhyOperator *in_op,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObScalarAggregate *scalar_agg_op = NULL;
    CREATE_PHY_OPERRATOR(scalar_agg_op, ObScalarAggregate, physical_plan, err_stat);
    //del gaojt [ListAgg][JHOBv0.1]20150104:b
    //  if (ret == OB_SUCCESS && (ret = scalar_agg_op->set_child(0, *in_op)) != OB_SUCCESS)
    //  {
    //    TRANS_LOG("Add child of scalar aggregate plan failed");
    //  }
    // del 20150104:e

    int32_t num = select_stmt->get_agg_fun_size();
    ObSqlRawExpr *agg_expr = NULL;
    bool is_listagg_and_order_exist = false;//add gaojt [ListAgg][JHOBv0.1]20150104
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
        agg_expr = logical_plan->get_expr(select_stmt->get_agg_expr_id(i));
        OB_ASSERT(NULL != agg_expr);
        if (agg_expr->get_expr()->is_aggr_fun())
        {
            //add gaojt [ListAgg][JHOBv0.1]20150104:b
            /* Exp:not push down listagg */
            if(T_FUN_LISTAGG == agg_expr->get_expr()->get_expr_type()&&select_stmt->get_order_item_for_listagg_size()>0)
            {
                is_listagg_and_order_exist = true;
            }
            // add 20150104:e
            ObSqlExpression new_agg_expr;
            if ((ret = agg_expr->fill_sql_expression(
                     new_agg_expr,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS
                    || (ret = scalar_agg_op->add_aggr_column(new_agg_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add aggregate function to scalar aggregate plan failed");
                break;
            }
        }
        else
        {
            TRANS_LOG("wrong aggregate function, exp_id = %ld", agg_expr->get_expr_id());
            break;
        }
        agg_expr->set_columnlized(true);
    }
    //add gaojt [ListAgg][JHOBv0.1]20150104:b
    /*Exp: If the agg's type is T_FUN_LISTAGG,and there is order-by,then set the sort operator as
      scalar_agg_op operator's child.
    */
    if(true == is_listagg_and_order_exist)
    {
        ObSort *sort_op = NULL;
        CREATE_PHY_OPERRATOR(sort_op, ObSort, physical_plan, err_stat);
        if (OB_SUCCESS != ret)//add qianzm [null operator unjudgement bug1181] 20160520
        {
        }
        else if ((ret = scalar_agg_op->set_child(0, *sort_op)) != OB_SUCCESS)
        {
            TRANS_LOG("Add child of scalar aggregate plan failed");
        }
        else if ((ret = sort_op->set_child(0, *in_op)) != OB_SUCCESS)
        {
            TRANS_LOG("Add child of scalar aggregate plan failed");
        }
        else
        {
            ObSqlRawExpr *order_expr;
            int32_t num = select_stmt->get_order_item_for_listagg_size();
            for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
            {
                const OrderItem& order_item = select_stmt->get_order_item_for_listagg(i);
                order_expr = logical_plan->get_expr(order_item.expr_id_);
                if (order_expr->get_expr()->is_const())
                {
                    // do nothing, const column is of no usage for sorting
                }
                else if (order_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
                {
                    ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(order_expr->get_expr());
                    if ((ret = sort_op->add_sort_column(
                             col_expr->get_first_ref_id(),
                             col_expr->get_second_ref_id(),
                             order_item.order_type_ == OrderItem::ASC ? true : false
                             )) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add sort column to sort plan failed");
                        break;
                    }
                }
                else
                {
                    ObSqlExpression col_expr;
                    if ((ret = order_expr->fill_sql_expression(
                             col_expr,
                             this,
                             logical_plan,
                             physical_plan)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add output column to project plan failed");
                        break;
                    }
                    else if ((ret = sort_op->add_sort_column(
                                  order_expr->get_table_id(),
                                  order_expr->get_column_id(),
                                  order_item.order_type_ == OrderItem::ASC ? true : false
                                  )) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add sort column to sort plan failed");
                        break;
                    }
                }
            }
        }
    }
    if(OB_SUCCESS == ret&&is_listagg_and_order_exist==false)
    {
        if ((ret = scalar_agg_op->set_child(0, *in_op)) != OB_SUCCESS)
        {
            TRANS_LOG("Add child of scalar aggregate plan failed");
        }
    }
    //add 20150104 e
    if (ret == OB_SUCCESS)
        out_op = scalar_agg_op;

    return ret;
}
//add liumz, [ROW_NUMBER]20150825
int ObTransformer::gen_phy_scalar_analytic(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObSelectStmt *select_stmt,
        ObPhyOperator *in_op,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObScalarAggregate *scalar_agg_op = NULL;
    CREATE_PHY_OPERRATOR(scalar_agg_op, ObScalarAggregate, physical_plan, err_stat);
    if (ret == OB_SUCCESS && (ret = scalar_agg_op->set_child(0, *in_op)) != OB_SUCCESS)
    {
        TRANS_LOG("Add child of scalar analytic plan failed");
    }

    int32_t num = select_stmt->get_anal_fun_size();
    ObSqlRawExpr *anal_expr = NULL;
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
        anal_expr = logical_plan->get_expr(select_stmt->get_anal_expr_id(i));
        OB_ASSERT(NULL != anal_expr);
        if (anal_expr->get_expr()->is_aggr_fun())
        {
            ObSqlExpression new_anal_expr;
            if ((ret = anal_expr->fill_sql_expression(
                     new_anal_expr,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS
                    || (ret = scalar_agg_op->add_anal_column(new_anal_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add analytic function to scalar analytic plan failed");
                break;
            }
        }
        else
        {
            TRANS_LOG("wrong analytic function, exp_id = %ld", anal_expr->get_expr_id());
            break;
        }
        anal_expr->set_columnlized(true);
    }

    if (ret == OB_SUCCESS)
    {
        scalar_agg_op->set_analytic_func(true);
        out_op = scalar_agg_op;
    }
    return ret;
}
//add:e

//add by hushuang [bloomfilter_join]20150429
int ObTransformer::add_bloomfilter_join_expr(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ObBloomFilterJoin& join_op,
        ObSort& l_sort,
        ObSort& r_sort,
        ObSqlRawExpr& expr,
        const bool is_table_expr_same_order)
{
    int ret = OB_SUCCESS;
    ObSqlRawExpr join_expr = expr;
    ObBinaryOpRawExpr equal_expr = *(dynamic_cast<ObBinaryOpRawExpr*>(expr.get_expr()));
    join_expr.set_expr(&equal_expr);
    if (OB_UNLIKELY(!expr.get_expr() || expr.get_expr()->get_expr_type() != T_OP_EQ))
    {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Wrong expression of merge join, ret=%d", ret);
    }
    else
    {
        ObBinaryRefRawExpr *expr1 = NULL;
        ObBinaryRefRawExpr *expr2 = NULL;
        if (is_table_expr_same_order)
        {
            expr1 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_first_op_expr());
            expr2 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_second_op_expr());
        }
        else
        {
            expr2 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_first_op_expr());
            expr1 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_second_op_expr());
            equal_expr.set_op_exprs(expr1, expr2);
        }
        if ((ret = l_sort.add_sort_column(expr1->get_first_ref_id(), expr1->get_second_ref_id(), true)) != OB_SUCCESS
                ||(ret = r_sort.add_sort_column(expr2->get_first_ref_id(), expr2->get_second_ref_id(), true)) != OB_SUCCESS)
        {
            TBSYS_LOG(WARN, "Add sort column faild, ret=%d", ret);
        }
        else
        {
            ObSqlExpression join_op_cnd;
            if ((ret = join_expr.fill_sql_expression(
                     join_op_cnd,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS)
            {
            }
            else if ((ret = join_op.add_equijoin_condition(join_op_cnd)) != OB_SUCCESS)
            {
                TBSYS_LOG(WARN, "Add condition of join plan faild");
            }
        }
    }
    return ret;
}
//add:e
/*add by wanglei [semi join] 20151231*/
int ObTransformer::add_semi_join_expr(
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan,
    ObSemiJoin& join_op,
    ObSort& l_sort,
    ObSort& r_sort,
    ObSqlRawExpr& expr,
    const bool is_table_expr_same_order,
    oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list,
    bool &is_add_other_join_cond,
    ObJoin::JoinType join_type,
    ObSelectStmt *select_stmt,
    int id,
    ObJoinTypeArray& hint_temp)
{
  int ret = OB_SUCCESS;
  //    UNUSED(remainder_cnd_list);
  //    UNUSED(join_type);
  //    UNUSED(select_stmt);
  //    UNUSED(is_add_other_join_cond);
  UNUSED(id);
  UNUSED(hint_temp);
  //各个模块开关：b
  bool is_on_expr_push_down = true; //on表达式中关于右表的过滤条件是否下压模块
  bool is_cons_right_table_filter = true; //是否构造右表的filter操作符模块
  bool is_get_all_index_table_for_right_table = false;//是否获取右表的所有index table模块
  bool is_decide_use_index = true; //是否使用索引模块 要配合原有二级索引使用流程使用
  //各个模块开关：e

  ObSqlRawExpr join_expr = expr;
  ObBinaryOpRawExpr equal_expr = *(dynamic_cast<ObBinaryOpRawExpr*>(expr.get_expr()));
  join_expr.set_expr(&equal_expr);
  ObBinaryRefRawExpr *expr1 = NULL;
  ObBinaryRefRawExpr *expr2 = NULL;
  //[second index]:b
  uint64_t first_table_id = OB_INVALID_ID; //左表的index table id，目前是最后一个，暂时不用
  uint64_t second_table_id = OB_INVALID_ID;//右表的index table id，目前是最后一个，暂时不用
  uint64_t index_table_id = OB_INVALID_ID;  //实际传到右表的索引表的table id
  uint64_t left_main_cid = OB_INVALID_ID;//左表主键
  uint64_t right_main_cid = OB_INVALID_ID;//右表主键
  uint64_t hint_tid = OB_INVALID_ID;      //hint中的index table id
  //判断是否是t1.c1>t2.id这种表达式：b
  bool is_non_equal_cond =false;
  bool is_use_hint = false; //判断是否使用hint
  bool is_use_index = false;//判断是否使用索引，因为即使hint中有直指定索引表，但是还要检查一下其是否可用
  uint64_t left_table_id = OB_INVALID_ID;
  uint64_t right_table_id = OB_INVALID_ID;
  TableItem* left_table_item = NULL;
  TableItem* right_table_item = NULL;
  ObSqlExpression join_op_cnd;
  if(hint_temp.join_type_ == T_SEMI_BTW_JOIN)
  {
    join_op.set_use_btw (true);
  }
  else if(hint_temp.join_type_ == T_SEMI_JOIN)
  {
    join_op.set_use_in (true);
  }
  if (OB_UNLIKELY(!expr.get_expr() || expr.get_expr()->get_expr_type() != T_OP_EQ))
  {
    ret = OB_ERR_GEN_PLAN;
    TBSYS_LOG(WARN, "Wrong expression of semi join, ret=%d", ret);
  }
  else
  {
    if (is_table_expr_same_order)
    {
      expr1 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_first_op_expr());
      expr2 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_second_op_expr());
    }
    else
    {
      expr2 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_first_op_expr());
      expr1 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_second_op_expr());
      equal_expr.set_op_exprs(expr1, expr2);
    }
    if ((ret = l_sort.add_sort_column(expr1->get_first_ref_id(), expr1->get_second_ref_id(), true)) != OB_SUCCESS
        ||(ret = r_sort.add_sort_column(expr2->get_first_ref_id(), expr2->get_second_ref_id(), true)) != OB_SUCCESS)
    {
      join_op.set_is_can_use_semi_join(false);
      TBSYS_LOG(WARN, "Add sort column faild, ret = %d", ret);
    }
    else
    {
      //获取左右两张表的原表table id，注意不是别名的id:b
      if(select_stmt == NULL)
      {
        join_op.set_is_can_use_semi_join(false);
        ret = OB_ERR_POINTER_IS_NULL;
        TBSYS_LOG(WARN,"[semi join] select stmt is null!");
      }
      else
      {
        left_table_item = select_stmt->get_table_item_by_id(expr1->get_first_ref_id());
        right_table_item = select_stmt->get_table_item_by_id(expr2->get_first_ref_id());
        if(left_table_item == NULL || right_table_item == NULL)
        {
          join_op.set_is_can_use_semi_join(false);
          ret = OB_ERR_POINTER_IS_NULL;
          TBSYS_LOG(WARN,"[semi join] left table item is null or right item is null!");
        }
        else
        {
          left_table_id = left_table_item->ref_id_;
          right_table_id = right_table_item->ref_id_;
        }
      }
      //:e
      //根据table id从Schema中获取这张表所有的index table：b
      if(OB_SUCCESS == ret && is_get_all_index_table_for_right_table)
      {
        if(sql_context_ == NULL)
        {
          join_op.set_is_can_use_semi_join(false);
          ret = OB_ERR_POINTER_IS_NULL;
          TBSYS_LOG(WARN,"[semi join] sql_context is null!");
        }
        else
        {
          const common::ObSchemaManagerV2 *schema_manager = sql_context_->schema_manager_;
          IndexList first_table_index_table_list;//用与存放左表的所有index table的table id。
          IndexList second_table_index_table_list;//用与存放右表的所有index table的table id。
          if(schema_manager == NULL)
          {
            join_op.set_is_can_use_semi_join(false);
            ret = OB_ERR_POINTER_IS_NULL;
            TBSYS_LOG(WARN,"[semi join] schema manager is null!");
          }
          else
          {
            schema_manager->get_index_list(left_table_id,first_table_index_table_list);
            schema_manager->get_index_list(right_table_id,second_table_index_table_list);
            for(int64_t i=0;i<first_table_index_table_list.get_count();i++)
            {
              first_table_index_table_list.get_idx_id(i,first_table_id);
              TBSYS_LOG(DEBUG, "[semi join] first_table_index_table_list = [%ld]",first_table_id);
            }
            for(int64_t i=0;i<second_table_index_table_list.get_count();i++)
            {
              second_table_index_table_list.get_idx_id(i,second_table_id);
              TBSYS_LOG(DEBUG, "[semi join] second_table_index_table_list = [%ld]",second_table_id);
            }
          }
        }
      }
      //:e

      //add dragon [Bugfix 1224] 2016-8-25 10:02:26
      //判断是否设置别名
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = join_op.set_alias_table (right_table_item->table_id_, select_stmt)))
        {
          join_op.set_is_can_use_semi_join (false);
          TBSYS_LOG(WARN, "don't know right table[%ld],ret[%d]", right_table_item->table_id_, ret);
        }
      }
      //add e

      //判断是否使用索引模块:b
      if(OB_SUCCESS == ret && is_decide_use_index)
      {
        if ((ret = join_expr.fill_sql_expression(
               join_op_cnd,
               this,
               logical_plan,
               physical_plan)) != OB_SUCCESS)
        {
          join_op.set_is_can_use_semi_join(false);
          TBSYS_LOG(WARN, "[semi join] fill join op condition faild!");
        }
        else
        {
          //判断是否使用索引
          //先判断on左右表达式中的列是否为主键列，如果为主键列则不使用索引。
          //左表主键id left_main_cid
          const ObTableSchema *left_mian_table_schema = NULL;
          if (NULL == (left_mian_table_schema = sql_context_->schema_manager_->get_table_schema(left_table_id)))
          {
            join_op.set_is_can_use_semi_join(false);
            ret = OB_ERR_POINTER_IS_NULL;
            TBSYS_LOG(ERROR,"[semi join] get left table schema faild");
          }
          else
          {
            const ObRowkeyInfo *left_rowkey_info = &left_mian_table_schema->get_rowkey_info();
            left_rowkey_info->get_column_id(0,left_main_cid);
            //右表主键id right_main_cid
            const ObTableSchema *right_mian_table_schema = NULL;
            if (NULL == (right_mian_table_schema = sql_context_->schema_manager_->get_table_schema(right_table_id)))
            {
              join_op.set_is_can_use_semi_join(false);
              ret = OB_ERR_POINTER_IS_NULL;
              TBSYS_LOG(ERROR,"[semi join] get right table schema faild");
            }
            else
            {
              const ObRowkeyInfo *right_rowkey_info = &right_mian_table_schema->get_rowkey_info();
              if(right_rowkey_info != NULL)
                right_rowkey_info->get_column_id(0,right_main_cid);
              else
              {
                join_op.set_is_can_use_semi_join(false);
                ret = OB_ERR_POINTER_IS_NULL;
                TBSYS_LOG(ERROR,"[semi join] get right table row key info faild");
              }
            }
          }
          //判断是否使用用户hint中指定的索引表:b
          if(OB_SUCCESS == ret)
          {
            if(select_stmt != NULL)
            {
              if(select_stmt->get_query_hint().has_index_hint())
              {
                //在hint中每张表的索引只能出现一次，如果有同一张表的多张索引表，则默认使用列表中的第一张 [20151225 9:07]
                for(int i=0;i < select_stmt->get_query_hint().use_index_array_.size ();i++)
                {
                  ObIndexTableNamePair tmp = select_stmt->get_query_hint().use_index_array_.at(i);
                  hint_tid=tmp.index_table_id_;
                  //TBSYS_LOG(ERROR,"test::fanqs,,tmp.src_table_id_=%ld,,from_item.table_id_=%ld",tmp.src_table_id_,from_item.table_id_);
                  if(tmp.src_table_id_ == right_table_id)
                  {
                    is_use_hint = true;
                    break;
                  }
                }
              }
            }
            else
            {
              join_op.set_is_can_use_semi_join(false);
              ret = OB_ERR_POINTER_IS_NULL;
              TBSYS_LOG(ERROR,"[semi join] get select stmt faild");
            }
          }
          //判断是否使用用户hint中指定的索引表:e
          //获取索引表信息:b
          if(OB_SUCCESS == ret)
          {
            if(ObJoin::INNER_JOIN == join_type ||
               ObJoin::LEFT_OUTER_JOIN == join_type)
            {
              if(is_use_hint)
              {
                //如果右表的主键与右表on表达式中的列cid不同，才可以使用二级索引
                if(expr2 != NULL && right_main_cid != expr2->get_second_ref_id())
                {
                  is_use_index = true;
                  join_op.set_is_use_second_index(true);
                  //暂不支持right join
                  join_op.set_index_table_id(first_table_id,hint_tid);
                }
              }
              else
              {
                //判断右表的主键列id是否与on表达式中右表的列的id相同，相同就不使用索引
                if(expr2 != NULL && right_main_cid != expr2->get_second_ref_id())
                {
                  //并且连接条件中的右表的cid要是索引表的主键
                  if(is_this_expr_can_use_index_for_join(expr2->get_second_ref_id(),
                                                         index_table_id,
                                                         right_table_id,
                                                         sql_context_->schema_manager_))
                  {
                    is_use_index = true;
                    //join_op.set_id(id);//add wanglei [semi join on expr] 20160511
                    join_op.set_is_use_second_index(true);
                    join_op.set_index_table_id(first_table_id,index_table_id);
                  }
                }
              }
            }
          }
          //获取索引表信息:e
        }
      }
      //判断是否使用索引模块:e

      //add dragon [Bugfix 1224] 2016-8-29 14:53:42
      /*
       * 判断使用二级索引的类型:回表还是不回表
       */
      if(OB_SUCCESS == ret && is_use_index)
      {
        //1.project
        //2.filter
        //if(all this index tab include all using column)
        // then: not back
        //else
        //  go back
        //end
        Expr_Array filter_array;
        Expr_Array project_array;
        common::ObArray<ObSqlExpression*> fp_array;
        ObArray<uint64_t> alias_exprs;
        uint64_t tid = OB_INVALID_ID;
        if(join_op.get_aliasT ())
          tid = right_table_item->table_id_;
        else
          tid= right_table_id;
        if(OB_SUCCESS != (ret = get_filter_array (
                            logical_plan,
                            physical_plan,
                            tid,
                            select_stmt,
                            filter_array,
                            fp_array)))
        {
          TBSYS_LOG(WARN, "failed in get_filter_array, ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = get_project_array (
                            logical_plan,
                            physical_plan,
                            tid,
                            select_stmt,
                            project_array,
                            alias_exprs)))
        {
          TBSYS_LOG(WARN, "failed in get_filter_array, ret=%d", ret);
        }

        uint64_t idx_id_used = OB_INVALID_ID;
        if(is_use_hint)
          idx_id_used = hint_tid;
        else
          idx_id_used = index_table_id;
        if(OB_SUCCESS == ret)
        {
          bool res = false;
          //判断索引表是否包含sql语句中出现的所有列
          res = is_index_table_has_all_cid_V2(
                idx_id_used,
                &filter_array,
                &project_array);
//          TBSYS_LOG(INFO, "result for is_index_table_has_all_cid_v2 is %s",
//                    res ? "true" : "false");
          if(res)
          {
            join_op.set_is_use_second_index_storing (true);
            join_op.set_is_use_second_index_without_storing (false);
          }
          else
          {
            join_op.set_is_use_second_index_storing (false);
            join_op.set_is_use_second_index_without_storing (true);
          }
        }
        for(int64_t i = 0; i < fp_array.count();i++)
        {
            ObSqlExpression* filter = fp_array.at(i);
            if(NULL != filter)
            {
                ObSqlExpression::free(filter);
            }
        }
      }
      //add 2016-8-29 14:53:48e

      if(OB_SUCCESS == ret)
      {
        if ((ret = join_op.add_equijoin_condition(join_op_cnd)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "[semi join] Add condition of join plan faild");
        }
        else
        {
          //可以将on表达式中的过滤条件下压：b
          //is_add_other_join_cond确保只下压一次
          if(!is_add_other_join_cond)
          {
            //add wanglei [semi join update] 20160510:b
            if(ret == OB_SUCCESS && is_cons_right_table_filter)
            {
              if(PHY_TABLE_MEM_SCAN == r_sort.get_child(0)->get_type())
              {
                join_op.set_is_can_use_semi_join(false);
                TBSYS_LOG(WARN,"[semi join] can not use semi join ! right table is memory table or sub query!");
              }
              else
              {
                ErrStat err_stat;
                ObFilter *right_table_filter = NULL;
                CREATE_PHY_OPERRATOR(right_table_filter, ObFilter, physical_plan, err_stat);
                ret = err_stat.err_code_;
                if(right_table_filter != NULL && ret == OB_SUCCESS)
                {
                  ObBitSet<> table_bitset;
                  int32_t num = 0;
                  if(right_table_item == NULL)
                  {
                    join_op.set_is_can_use_semi_join(false);
                    ret = OB_ERR_POINTER_IS_NULL;
                    TBSYS_LOG(ERROR,"[semi join] right table item is null!");
                  }
                  else if(select_stmt != NULL)
                  {
                    int32_t bit_index = select_stmt->get_table_bit_index(right_table_item->table_id_);
                    table_bitset.add_member(bit_index);
                    num = select_stmt->get_condition_size();
                    //将右表的所有filter都放到right_table_filter中：b
                    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
                    {
                      ObSqlRawExpr *cnd_expr = logical_plan->get_expr(select_stmt->get_condition_id(i));
                      if (cnd_expr && table_bitset.is_superset(cnd_expr->get_tables_set()))
                      {
                        ObSqlExpression *filter = ObSqlExpression::alloc();
                        if (NULL == filter)
                        {
                          join_op.set_is_can_use_semi_join(false);
                          ret = OB_ALLOCATE_MEMORY_FAILED;
                          break;
                        }
                        else if ((ret = cnd_expr->fill_sql_expression(*filter, this, logical_plan, physical_plan)) != OB_SUCCESS)
                        {
                          join_op.set_is_can_use_semi_join(false);
                          ObSqlExpression::free(filter);
                          break;
                        }
                        else
                        {
                          //add dragon [Bugfix lmz-2017-2-6] 2017-2-7 b
                          // A.a IS NULL不能在left join之前就加入右表的filter中
                          int64_t start = tbsys::CTimeUtil::getTime();
                          bool is_null_cond = false;
                          if(ObJoin::INNER_JOIN != join_type)
                          {
                            is_null_cond = filter->is_op_is_null();
                            TBSYS_LOG(DEBUG, "FILTER IS %s, is_null_cond[%s]", to_cstring(*filter), STR_BOOL(is_null_cond));
                          }
                          TBSYS_LOG(DEBUG, "Bugfix(lmz-2017-2-6) consume time is %ldus", tbsys::CTimeUtil::getTime() - start);
                          if(!is_null_cond)
                          //add dragon e
                            ret = right_table_filter->add_filter (filter);
                          if(ret != OB_SUCCESS)
                          {
                            ObSqlExpression::free(filter);
                            TBSYS_LOG(WARN,"[semi join] add filter = {%s} to right table filter failed! ",to_cstring(*filter));
                            join_op.set_is_can_use_semi_join(false);
                            break;
                          }
//                          判断是否是now（）函数
//                          ObSEArray<ObObj, 64> &item_array = filter->get_expr_array();
//                          TBSYS_LOG(DEBUG,"wanglei::filter = %s",to_cstring(*filter));
//                          TBSYS_LOG(DEBUG,"wanglei::item_array = %s",to_cstring(item_array));
//                          int64_t idx =0;
//                          int64_t count = item_array.count ();
//                          int64_t type = 0;
//                          bool is_time_type = false;
//                          while(idx<count)
//                          {
//                              if (OB_SUCCESS != (ret = item_array[idx].get_int(type)))
//                              {
//                                  TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
//                                  break;
//                              }
//                              else if(type == ObPostfixExpression::CUR_TIME_OP)
//                              {
//                                  is_time_type =true;
//                                  break;
//                              }
//                              else if(type == ObPostfixExpression::CUR_TIME_HMS_OP)
//                              {
//                                  is_time_type =true;
//                                  break;
//                              }
//                              else if(type == ObPostfixExpression::CUR_DATE_OP)
//                              {
//                                  is_time_type =true;
//                                  break;
//                              }
//                              else if(type == ObPostfixExpression::OP
//                                      ||type == ObPostfixExpression::COLUMN_IDX
//                                      ||type == T_OP_ROW
//                                      ||type == ObPostfixExpression::CONST_OBJ
//                                      ||type == ObPostfixExpression::QUERY_ID
//                                      ||type == ObPostfixExpression::END
//                                      ||type == ObPostfixExpression::UPS_TIME_OP
//                                      ||type == ObPostfixExpression::PARAM_IDX
//                                      ||type == ObPostfixExpression::SYSTEM_VAR
//                                      ||type == ObPostfixExpression::TEMP_VAR
//                                      ||type == ObPostfixExpression::CUR_TIME_OP
//                                      ||type == ObPostfixExpression::CUR_TIME_HMS_OP
//                                      ||type == ObPostfixExpression::CUR_DATE_OP
//                                      ||type == ObPostfixExpression::END_TYPE
//                                      ||type == ObPostfixExpression::BEGIN_TYPE
//                                      )
//                              {
//                                  int64_t tmp = get_type_num(idx,type,item_array);
//                                  idx = idx + tmp;
//                                  if(tmp == -1)
//                                  {
//                                      break;
//                                  }
//                              }
//                              else
//                              {
//                                  TBSYS_LOG(WARN,"wrong expr type: %ld",type);
//                                  break;
//                              }
//                          }
//                          //先不判断什么类型的表达式了
//                          if(!is_time_type)
//                              join_op.add_right_table_filter (filter);
//                          else
//                              ObSqlExpression::free (filter);
                        }
                      }
                    }
                    //将右表的所有filter都放到right_table_filter中：e
                    if(ret == OB_SUCCESS)
                      join_op.set_right_table_filter (right_table_filter);
                  }
                  else
                  {
                    join_op.set_is_can_use_semi_join(false);
                    ret = OB_ERR_POINTER_IS_NULL;
                    TBSYS_LOG(ERROR,"[semi join] select stmt is null!");
                  }
                }
                else
                {
                  join_op.set_is_can_use_semi_join(false);
                  ret = OB_ERR_POINTER_IS_NULL;
                  TBSYS_LOG(ERROR,"[semi join] right table filter create failed!");
                }
              }
            }
            //add wanglei [semi join update] 20160510:e
            if(ObJoin::INNER_JOIN == join_type && ret == OB_SUCCESS && is_on_expr_push_down)
            {
              int il = 0;
              oceanbase::common::ObList<ObSqlRawExpr*>::iterator cnd_it_ll;
              for (cnd_it_ll = remainder_cnd_list.begin(); cnd_it_ll != remainder_cnd_list.end();++cnd_it_ll )
              {
                if(ret != OB_SUCCESS)
                  break;
                //判断表达式是否是等值表达式，如果是则继续否则判断是否可以下压
                if((*cnd_it_ll)->get_expr()->is_join_cond())
                {
                  continue;
                }
                else
                {
                  //on里面的表达式下压在有表别名的情况下会失效，这个问题已经解决
                  //目前仅考虑sort操作符下有ObTableRpcScan与ObTableMemScan两种情况，如果挂的是其他的操作符则无法继续使用semi join
                  ObTableRpcScan * t_r_operator = NULL;
                  ObTableMemScan * t_m_operator = NULL;
                  int64_t table_id = OB_INVALID_ID;
                  ObSqlExpression *expr_temp = ObSqlExpression::alloc();
                  if(expr_temp == NULL)
                  {
                    join_op.set_is_can_use_semi_join(false);
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    TBSYS_LOG(WARN,"[semi join] expression is null!");
                    break;
                  }
                  else
                  {
                    //every repeats will reset the value of ret
                    ret = (*cnd_it_ll)->fill_sql_expression(*expr_temp,this,logical_plan,physical_plan);
                    il++;
                    if(ret != OB_SUCCESS)
                    {
                      ObSqlExpression::free(expr_temp);
                      TBSYS_LOG(WARN,"[semi join] expression is null!");
                      break;
                    }
                    else
                    {
                      ObSEArray<ObObj, 64> &item_array = expr_temp->get_expr_array();
                      //默认都是左边是列右边是值的情况，否则不可以使用semi join
                      item_array[1].get_int(table_id);
                      //判断是否是t1.c1>t2.id这种表达式：b
                      int64_t val = 0;
                      if(item_array.count () >3 && ObIntType == item_array[3].get_type()
                         && OB_SUCCESS == item_array[3].get_int(val)
                         && ObPostfixExpression::COLUMN_IDX == val)
                      {
                        is_non_equal_cond = true;
                      }
                      else
                      {
                        //TBSYS_LOG(WARN,"[semi join] item array count less then 3!");
                      }
                    }
                    //判断t1.c1>t2.id这种表达式，如果是这种表达式也会下压，会出现错误
                    if(!is_non_equal_cond && ret == OB_SUCCESS && expr_temp != NULL)
                    {
                      if(table_id == (int64_t)expr1->get_first_ref_id())
                      {
                        //暂不考虑左边使用索引的情况，也就是right join暂不支持
                        if(NULL == l_sort.get_child(0))
                        {
                          join_op.set_is_can_use_semi_join(false);
                          ret = OB_ERR_POINTER_IS_NULL;
                          TBSYS_LOG(WARN,"[semi join] left sort op is null!");
                          break;
                        }
                        else
                        {
                          //左表目前只考虑table rpc scan 与 table mem scan 两种情况
                          if(l_sort.get_child(0)!= NULL && PHY_TABLE_MEM_SCAN == l_sort.get_child(0)->get_type())
                          {
                            if(NULL != (t_m_operator = dynamic_cast<ObTableMemScan *>(l_sort.get_child(0))))
                            {
                              //暂不支持子查询，filter下压到底层会出现问题
                              ret = t_m_operator->add_filter(expr_temp);
                              if(ret != OB_SUCCESS)
                              {
                                ObSqlExpression::free(expr_temp);
                                TBSYS_LOG(WARN,"[semi join] add expression failed!");
                                break;
                              }
                            }
                            else
                            {
                              join_op.set_is_can_use_semi_join(false);
                              ret = OB_ERR_POINTER_IS_NULL;
                              TBSYS_LOG(WARN,"[semi join] right table memory scan op is null!");
                              break;
                            }
                          }
                          else if(l_sort.get_child(0)!= NULL && PHY_TABLE_RPC_SCAN == l_sort.get_child(0)->get_type())
                          {
                            if(NULL != (t_r_operator = dynamic_cast<ObTableRpcScan *>(l_sort.get_child(0))))
                            {
                              // 左表如果使用的是回表的索引查询，暂不下压了
                              if(!t_r_operator->get_rpc_scan().get_is_use_index())
                                ret = t_r_operator->add_filter(expr_temp);
                              else
                              {
                                ObSqlExpression::free(expr_temp);
                                expr_temp = NULL;
                              }
                              if(ret != OB_SUCCESS)
                              {
                                ObSqlExpression::free(expr_temp);
                                TBSYS_LOG(WARN,"[semi join] add expression failed!");
                                break;
                              }
                            }
                            else
                            {
                              join_op.set_is_can_use_semi_join(false);
                              ret = OB_ERR_POINTER_IS_NULL;
                              TBSYS_LOG(WARN,"[semi join] right table rpc scan op is null!");
                              break;
                            }
                          }
                          else
                          {
                            TBSYS_LOG(INFO, "Not supported feature or function, "
                                            "the child of sort is not scan operation");
                          }
                        }
                      }
                      else //右表的所有filter //这部分为on表达式中涉及到的右表的filter
                      {
                        if(r_sort.get_child(0) == NULL)
                        {
                          join_op.set_is_can_use_semi_join(false);
                          ret = OB_ERR_POINTER_IS_NULL;
                          TBSYS_LOG(WARN,"[semi join] right sort op is null!");
                          break;
                        }
                        else
                        {
                          if(PHY_TABLE_MEM_SCAN == r_sort.get_child(0)->get_type())
                          {
                            if(NULL != (t_m_operator = dynamic_cast<ObTableMemScan *>(r_sort.get_child(0))))
                            {
                              //暂不支持子查询，filter下压到底层会出现问题
                              ret = t_m_operator->add_filter(expr_temp);
                              if(ret != OB_SUCCESS)
                              {
                                ObSqlExpression::free(expr_temp);
                                TBSYS_LOG(WARN,"[semi join] add expression failed!");
                                break;
                              }
                            }
                          }
                          else if(PHY_TABLE_RPC_SCAN == r_sort.get_child(0)->get_type())
                          {
                            if(NULL != (t_r_operator = dynamic_cast<ObTableRpcScan *>(r_sort.get_child(0))))
                            {

                              if(is_use_index && ret == OB_SUCCESS) //判断是否使用索引
                              {
                                //index_table_id
                                //要对这个表达式进行处理，判断其是否可以用于索引
                                //判断条件：首先判断其列在不在索引表中，如果在返回索引表id
                                //然后在判断，其索引表与on表达式中列返回的索引表是否一样，如果一样则可以使用，否则不使用。
                                uint64_t expr_temp_cid = OB_INVALID_ID;
                                uint64_t expr_temp_tid = OB_INVALID_ID;
                                expr_temp->find_cid(expr_temp_cid);
                                if (right_main_cid == expr_temp_cid)
                                {
                                  //如果是主键目前的方法是不下压
                                  //t_r_operator->add_filter(expr_temp);
                                  ret = t_r_operator->add_main_filter(expr_temp);
                                  if(ret != OB_SUCCESS)
                                  {
                                    ObSqlExpression::free(expr_temp);
                                    TBSYS_LOG(WARN,"[semi join] add expression failed!");
                                    break;
                                  }
                                }
                                else if(is_this_expr_can_use_index_for_join(expr_temp_cid,
                                                                            expr_temp_tid,
                                                                            right_table_id,
                                                                            sql_context_->schema_manager_))
                                {
                                  if(expr_temp_tid == index_table_id) //如果表达式的索引表与on的是一个，否则不下压
                                  {
                                    if(OB_SUCCESS != (ret = t_r_operator->add_main_filter(expr_temp)))
                                    {
                                      join_op.set_is_can_use_semi_join(false);
                                      ObSqlExpression::free(expr_temp);
                                      TBSYS_LOG(ERROR,"[semi join] add expr to right table faild! ret=%d",ret);
                                      break;
                                    }
                                    //改变expr的table id为索引表的table id
                                    ObPostfixExpression& ops = expr_temp->get_decoded_expression_v2();
                                    uint64_t index_of_expr_array=OB_INVALID_ID;
                                    if(OB_SUCCESS == ret && OB_SUCCESS != (ret = expr_temp->change_tid(index_of_expr_array)))
                                    {
                                      join_op.set_is_can_use_semi_join(false);
                                      ObSqlExpression::free(expr_temp);
                                      TBSYS_LOG(ERROR,"[semi join] faild to change tid,filter=%s",to_cstring(*expr_temp));
                                      break;
                                    }
                                    else
                                    {
                                      ObObj& obj = ops.get_expr_by_index(index_of_expr_array);
                                      if(obj.get_type() == ObIntType)
                                        obj.set_int(index_table_id);
                                      if(OB_SUCCESS == ret)
                                      {
                                        if(OB_SUCCESS != (ret = t_r_operator->add_filter(expr_temp)))
                                        {
                                          join_op.set_is_can_use_semi_join(false);
                                          ObSqlExpression::free(expr_temp);
                                          TBSYS_LOG(WARN,"[semi join] add expr to right table faild! ret=%d",ret);
                                          break;
                                        }
                                        else if(OB_SUCCESS != (ret = t_r_operator->add_index_filter_ll ((expr_temp))))
                                        {
                                          join_op.set_is_can_use_semi_join(false);
                                          ObSqlExpression::free(expr_temp);
                                          TBSYS_LOG(WARN,"[semi join] add expr to right table faild! ret=%d",ret);
                                          break;
                                        }
                                      }
                                    }
                                  }
                                  else
                                  {
                                    if(OB_SUCCESS == ret && OB_SUCCESS!=(ret = t_r_operator->add_main_filter(expr_temp)))
                                    {
                                      join_op.set_is_can_use_semi_join(false);
                                      ObSqlExpression::free(expr_temp);
                                      TBSYS_LOG(ERROR,"[semi join] faild to change tid,filter=%s",to_cstring(*expr_temp));
                                    }
                                  }
                                }
                                else
                                {
                                  if(OB_SUCCESS == ret && OB_SUCCESS!=(ret = t_r_operator->add_main_filter(expr_temp)))
                                  {
                                    join_op.set_is_can_use_semi_join(false);
                                    ObSqlExpression::free(expr_temp);
                                    TBSYS_LOG(ERROR,"[semi join] faild to change tid,filter=%s",to_cstring(*expr_temp));
                                  }
                                }
                              }
                              else
                              {
                                if(OB_SUCCESS == ret && OB_SUCCESS!=(ret = t_r_operator->add_filter(expr_temp)))
                                {
                                  join_op.set_is_can_use_semi_join(false);
                                  ObSqlExpression::free(expr_temp);
                                  TBSYS_LOG(ERROR,"[semi join] faild to change tid,filter=%s",to_cstring(*expr_temp));
                                  break;
                                }
                              }
                            }
                            else
                            {
                              join_op.set_is_can_use_semi_join(false);
                              ObSqlExpression::free(expr_temp);
                              ret = OB_ERR_POINTER_IS_NULL;
                              TBSYS_LOG(WARN,"[semi join] right table rpc scan op is null!");
                              break;
                            }
                          }
                          else
                          {
                            TBSYS_LOG(INFO, "Not supported feature or function, "
                                            "the child of sort is not scan operation");
                          }
                        }
                      }
                    }
                  }
                  //when ret is not success, expr_temp must be release
                  if(ret != OB_SUCCESS && expr_temp!=NULL)
                  {
                    ObSqlExpression::free(expr_temp);
                    break;
                  }
                }
              }
              is_add_other_join_cond=true;
            }
          }
        }
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(r_sort.get_child(0) == NULL || l_sort.get_child(0) == NULL)
    {
      ret = OB_ERR_POINTER_IS_NULL;
      TBSYS_LOG(WARN,"[semi join] left sort op is null or right sort op is null");
    }
    else if(PHY_TABLE_MEM_SCAN == r_sort.get_child(0)->get_type())// ||PHY_TABLE_MEM_SCAN == l_sort.get_child(0)->get_type())

    {
      join_op.set_is_can_use_semi_join(false);
    }
    else
    {
      //目前先弃用
      /*for(int i = 0;i< logical_plan->get_expr_list_num();i++)
            {
                ObSqlRawExpr * ob_sql_raw_expr = logical_plan->get_expr_for_something(i);
                ObSqlExpression cnd_ll_;
                ob_sql_raw_expr->fill_sql_expression(cnd_ll_,this,logical_plan,physical_plan);
                ObSEArray<ObObj, 64> &item_array = cnd_ll_.get_expr_array();
                int64_t table_id = OB_INVALID_ID;
                item_array[1].get_int(table_id);
                // TBSYS_LOG(WARN, "wl filter list = [%s]",to_cstring(cnd_ll_));
                if(!ob_sql_raw_expr->get_expr()->is_join_cond())
                {
                    if(ObJoin::INNER_JOIN == join_type ||
                            ObJoin::LEFT_OUTER_JOIN == join_type)
                    {
                        if(table_id == (int64_t)expr2->get_first_ref_id())
                        {
                            if(item_array.count() == project_column_item_array_length)//project 中的每一列由4个obj组成
                            {
                                //nothing to do here
                                continue;
                            }
                            else
                            {
                                join_op.set_is_can_use_semi_join(false);
                                break;
                            }
                        }
                    }
                    else
                    {
                        if(table_id == (int64_t)expr1->get_first_ref_id())
                        {
                            if(item_array.count() == project_column_item_array_length)//project 中的每一列由4个obj组成
                            {
                                //nothing to do here
                                continue;
                            }
                            else
                            {
                                join_op.set_is_can_use_semi_join(false);
                                break;
                            }
                        }
                    }

                }
            }*/
      /*if(is_left_has_filter && is_right_has_filter)
            {
                join_op.set_is_can_use_semi_join(false);
            }*/
    }
  }
  //add 2016/1/5 for explain
  //    if(OB_SUCCESS == ret)
  //    {
  //        ObBasicStmt *stmt = NULL;
  //        if (ret == OB_SUCCESS)
  //        {
  //            if (query_id == OB_INVALID_ID)
  //                stmt = logical_plan->get_main_stmt();
  //            else
  //                stmt = logical_plan->get_query(query_id);
  //            if (stmt == NULL)
  //            {
  //                ret = OB_ERR_ILLEGAL_ID;
  //                TRANS_LOG("Wrong query id to find query statement");
  //            }
  //        }
  //    }
  //add e
  //    if(ret == OB_SUCCESS)
  //    {
  //        char sql_str_buf[OB_MAX_VARCHAR_LENGTH];
  //        char table_name[50];
  //        memset(table_name,'\0',sizeof(table_name));
  //        snprintf(table_name, sizeof(table_name), "%.*s", left_table_item->table_name_.length(), left_table_item->table_name_.ptr());
  //        snprintf(sql_str_buf, OB_MAX_VARCHAR_LENGTH, "SELECT count(*) FROM %s;",table_name);
  //        ObString get_params_sql_str = ObString::make_string(sql_str_buf);
  //        ObMySQLResultSet *raw_result = (ObMySQLResultSet*)sql_context_->session_info_->get_current_result_set ();
  //        if ((ret = raw_result->init()) != OB_SUCCESS)
  //        {
  //            TBSYS_LOG(ERROR, "Init result set failed, ret = %d", ret);
  //        }
  //        else if ((ret = ObSql::direct_execute(get_params_sql_str, *raw_result, *sql_context_)) != OB_SUCCESS)
  //        {
  //            TBSYS_LOG(ERROR,"Execute sql: %s, failed, ret=%d", sql_str_buf, ret);
  //        }
  //        else if ((ret = raw_result->open()) != OB_SUCCESS)
  //        {
  //            TBSYS_LOG(ERROR, "Failed to open result set, ret=%d", ret);
  //            if (raw_result->close() != OB_SUCCESS)
  //            {
  //                TBSYS_LOG(WARN, "Failed to close result set");
  //            }
  //        }
  //        else if (raw_result->is_with_rows() != true)
  //        {
  //            TBSYS_LOG(WARN, "Sql: '%s', get nothing, ret=%d", sql_str_buf, ret);
  //        }
  //        else
  //        {
  //            ObMySQLRow sql_row;
  //            while ((ret = raw_result->next_row(sql_row)) == OB_SUCCESS)
  //            {
  //                const ObRow *row = sql_row.get_inner_row();
  //                if(ret == OB_SUCCESS)
  //                {
  //                    TBSYS_LOG(ERROR,"wangelei::direct test row:%s",to_cstring(*row));
  //                }
  //            } // end while
  //            if (ret == OB_ITER_END)
  //            {
  //                ret = OB_SUCCESS;
  //            }
  //            else if (ret != OB_SUCCESS)
  //            {
  //                TBSYS_LOG(ERROR, "Get next row from ObMySQLResultSet failed, ret=%d", ret);
  //            }
  //            if (raw_result->close() != OB_SUCCESS)
  //            {
  //                TBSYS_LOG(ERROR, "Failed to close result set");
  //            }
  //        }
  //        if (OB_SUCCESS == ret)
  //        {
  //          if (OB_SUCCESS != (ret = sql_context_->session_info_->update_session_timeout()))
  //          {
  //            TBSYS_LOG(ERROR, "failed to update_session_timeout, ret=%d", ret);
  //          }
  //        }
  //    }
  return ret;
}
//add:e
int ObTransformer::add_merge_join_expr(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ObMergeJoin& join_op,
        ObSort& l_sort,
        ObSort& r_sort,
        ObSqlRawExpr& expr,
        const bool is_table_expr_same_order)
{
    int ret = OB_SUCCESS;
    ObSqlRawExpr join_expr = expr;
    ObBinaryOpRawExpr equal_expr = *(dynamic_cast<ObBinaryOpRawExpr*>(expr.get_expr()));
    join_expr.set_expr(&equal_expr);
    if (OB_UNLIKELY(!expr.get_expr() || expr.get_expr()->get_expr_type() != T_OP_EQ))
    {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Wrong expression of merge join, ret=%d", ret);
    }
    else
    {
        ObBinaryRefRawExpr *expr1 = NULL;
        ObBinaryRefRawExpr *expr2 = NULL;
        if (is_table_expr_same_order)
        {
            expr1 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_first_op_expr());
            expr2 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_second_op_expr());
        }
        else
        {
            expr2 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_first_op_expr());
            expr1 = dynamic_cast<ObBinaryRefRawExpr*>(equal_expr.get_second_op_expr());
            equal_expr.set_op_exprs(expr1, expr2);
        }
        if ((ret = l_sort.add_sort_column(expr1->get_first_ref_id(), expr1->get_second_ref_id(), true)) != OB_SUCCESS
                ||(ret = r_sort.add_sort_column(expr2->get_first_ref_id(), expr2->get_second_ref_id(), true)) != OB_SUCCESS)
        {
            TBSYS_LOG(WARN, "Add sort column faild, ret=%d", ret);
        }
        else
        {
            ObSqlExpression join_op_cnd;
            if ((ret = join_expr.fill_sql_expression(
                     join_op_cnd,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS)
            {
            }
            else if ((ret = join_op.add_equijoin_condition(join_op_cnd)) != OB_SUCCESS)
            {
                TBSYS_LOG(WARN, "Add condition of join plan faild");
            }
        }
    }
    return ret;
}

int ObTransformer::gen_phy_joins(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObSelectStmt *select_stmt,
        ObJoin::JoinType join_type,
        oceanbase::common::ObList<ObPhyOperator*>& phy_table_list,
        oceanbase::common::ObList<ObBitSet<> >& bitset_list,
        oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list,
        oceanbase::common::ObList<ObSqlRawExpr*>& none_columnlize_alias
        ,bool outer_join_scope//add liumz, [outer_join_on_where]20150927
        )
{
    bool is_add_other_join_cond = false;//add wanglei [semi join] 20151225:b
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    int32_t index_ = 0;
    while (ret == OB_SUCCESS && phy_table_list.size() > 1)
    {
      TBSYS_LOG(DEBUG, "index[%d], size[%ld], join_size[%d]", index_, phy_table_list.size(), select_stmt->get_query_hint().join_array_.size());
        /*
    if(phy_table_list.size() > 2)
    {
       select_stmt->get_query_hint().join_array_.clear();
    }*/
        //bool is_table_expr_same_order = true;
        ObAddProject *project_op = NULL;
        ObMergeJoin *join_op = NULL;
        /*add by wanglei [semi join] 20151231*/
        ObSemiJoin *semi_join_op = NULL;
        //add e
        ObBloomFilterJoin *bloomfilter_join_op = NULL;
        if(select_stmt->get_query_hint().join_array_.size()>index_)
        {
            ObJoinTypeArray tmp = select_stmt->get_query_hint().join_array_.at(index_);
            if(tmp.join_type_ == T_BLOOMFILTER_JOIN&&(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN))
            {
                CREATE_PHY_OPERRATOR(bloomfilter_join_op, ObBloomFilterJoin, physical_plan, err_stat);
                if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
                {
                    bloomfilter_join_op->set_join_type(join_type);
                }
            }
            else if((tmp.join_type_ == T_SEMI_JOIN ||tmp.join_type_ == T_SEMI_BTW_JOIN ) && (join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN))
            {
                CREATE_PHY_OPERRATOR(semi_join_op, ObSemiJoin, physical_plan, err_stat);
                if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
                {
                    semi_join_op->set_join_type(join_type);
                }
            }
            else
            {
                CREATE_PHY_OPERRATOR(join_op, ObMergeJoin, physical_plan, err_stat);
                if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
                {
                    join_op->set_join_type(join_type);
                }
            }
        }
        else
        {
            CREATE_PHY_OPERRATOR(join_op, ObMergeJoin, physical_plan, err_stat);
            if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
            {
                join_op->set_join_type(join_type);
            }
        }
        if (ret != OB_SUCCESS)
            break;
        ObBitSet<> join_table_bitset;
        ObBitSet<> l_expr_tab_bitset;
        ObBitSet<> r_expr_tab_bitset;
        ObBitSet<>* l_tab_bitset = NULL;
        ObSort *left_sort = NULL;
        ObSort *right_sort = NULL;
        oceanbase::common::ObList<ObSqlRawExpr*>::iterator cnd_it;
        oceanbase::common::ObList<ObSqlRawExpr*>::iterator del_it;
        ObPhyOperator *l_expr_tab_op = NULL;
        ObPhyOperator *r_expr_tab_op = NULL;
        int id = 0; //add wanglei[semi join on expr] 20160511
        for (cnd_it = remainder_cnd_list.begin(); ret == OB_SUCCESS && cnd_it != remainder_cnd_list.end(); )
        {
            if ((*cnd_it)->get_expr()->is_join_cond() && join_table_bitset.is_empty())
            {
                ObBinaryOpRawExpr *join_cnd = dynamic_cast<ObBinaryOpRawExpr*>((*cnd_it)->get_expr());
                ObBinaryRefRawExpr *lexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_first_op_expr());
                ObBinaryRefRawExpr *rexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_second_op_expr());
                int32_t left_bit_idx = select_stmt->get_table_bit_index(lexpr->get_first_ref_id());
                int32_t right_bit_idx = select_stmt->get_table_bit_index(rexpr->get_first_ref_id());
                // TBSYS_LOG(DEBUG, "left_bit_idx=%d right_bit_idx=%d left_ref_id=%ld right_ref_id=%ld",
                //           left_bit_idx, right_bit_idx,
                //           lexpr->get_first_ref_id(), rexpr->get_first_ref_id());
                CREATE_PHY_OPERRATOR(left_sort, ObSort, physical_plan, err_stat);
                if (ret != OB_SUCCESS)
                    break;
                CREATE_PHY_OPERRATOR(right_sort, ObSort, physical_plan, err_stat);
                if (ret != OB_SUCCESS)
                    break;

                bool is_table_expr_same_order = true;
                oceanbase::common::ObList<ObPhyOperator*>::iterator table_it = phy_table_list.begin();
                oceanbase::common::ObList<ObPhyOperator*>::iterator del_table_it;
                oceanbase::common::ObList<ObBitSet<> >::iterator bitset_it = bitset_list.begin();
                oceanbase::common::ObList<ObBitSet<> >::iterator del_bitset_it;
                //ObPhyOperator *l_expr_tab_op = NULL;
                //ObPhyOperator *r_expr_tab_op = NULL;
                // find table scan operator
                while (ret == OB_SUCCESS
                       && (!l_expr_tab_op || !r_expr_tab_op)
                       && table_it != phy_table_list.end()
                       && bitset_it != bitset_list.end())
                {
                    if (bitset_it->has_member(left_bit_idx))
                    {
                        //TBSYS_LOG(DEBUG, "left table is generated, left_bit_idx=%d", left_bit_idx);
                        //TBSYS_LOG(ERROR, "left table is generated, left_bit_idx=%d", left_bit_idx);
                        l_expr_tab_op = *table_it;
                        l_expr_tab_bitset = *bitset_it;
                        if (!l_tab_bitset)
                        {
                            l_tab_bitset = &l_expr_tab_bitset;
                        }
                        del_table_it = table_it;
                        del_bitset_it = bitset_it;
                        table_it++;
                        bitset_it++;
                        if ((ret = phy_table_list.erase(del_table_it)) != OB_SUCCESS)
                            break;
                        if ((ret = bitset_list.erase(del_bitset_it)) != OB_SUCCESS)
                            break;
                        if (r_expr_tab_op)
                        {
                            // right table of join expr has already been found,
                            // so table and join expr has different order
                            // E.g.  t1 LEFT JOIN t2 ON t2.id=t1.id
                            is_table_expr_same_order = false;
                        }
                    }
                    else if (bitset_it->has_member(right_bit_idx))
                    {
                        // TBSYS_LOG(DEBUG, "right table is generated, right_bit_idx=%d", right_bit_idx);
                        //TBSYS_LOG(ERROR, "right table is generated, right_bit_idx=%d", right_bit_idx);
                        r_expr_tab_op = *table_it;
                        r_expr_tab_bitset = *bitset_it;
                        if (!l_tab_bitset)
                        {
                            l_tab_bitset = &r_expr_tab_bitset;
                        }
                        del_table_it = table_it;
                        del_bitset_it = bitset_it;
                        table_it++;
                        bitset_it++;
                        if ((ret = phy_table_list.erase(del_table_it)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Generate join plan faild");
                            break;
                        }
                        if ((ret = bitset_list.erase(del_bitset_it)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Generate join plan faild");
                            break;
                        }
                    }
                    else
                    {
                        table_it++;
                        bitset_it++;
                    }
                }
                if (ret != OB_SUCCESS)
                    break;

                // Two columns must from different table, that expression from one table has been erased in gen_phy_table()
                OB_ASSERT(l_expr_tab_op && r_expr_tab_op);
                if (is_table_expr_same_order)
                {
                    if ((ret = left_sort->set_child(0, *l_expr_tab_op)) != OB_SUCCESS
                            || (ret = right_sort->set_child(0, *r_expr_tab_op)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add child of join plan faild");
                        break;
                    }
                }
                else
                {
                    if ((ret = left_sort->set_child(0, *r_expr_tab_op)) != OB_SUCCESS
                            || (ret = right_sort->set_child(0, *l_expr_tab_op)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add child of join plan faild");
                        break;
                    }
                }
                if(select_stmt->get_query_hint().join_array_.size()>index_)
                {
                    ObJoinTypeArray tmp = select_stmt->get_query_hint().join_array_.at(index_);
                    switch (tmp.join_type_) {
                    case T_BLOOMFILTER_JOIN:
                    {
                        if(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN)
                        {
                            if ((ret = add_bloomfilter_join_expr(
                                     logical_plan,
                                     physical_plan,
                                     *bloomfilter_join_op,
                                     *left_sort,
                                     *right_sort,
                                     *(*cnd_it),
                                     is_table_expr_same_order)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add condition of join plan faild");
                                break;
                            }
                        }
                        else if ((ret = add_merge_join_expr(
                                      logical_plan,
                                      physical_plan,
                                      *join_op,
                                      *left_sort,
                                      *right_sort,
                                      *(*cnd_it),
                                      is_table_expr_same_order)) != OB_SUCCESS)
                             {
                                 TRANS_LOG("Add condition of join plan faild");
                             }
                    }
                        break;
                    case T_SEMI_JOIN:
                    {
                        if(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN)
                        {
                            if ((ret = add_semi_join_expr(
                                     logical_plan,
                                     physical_plan,
                                     *semi_join_op,
                                     *left_sort,
                                     *right_sort,
                                     *(*cnd_it),
                                     is_table_expr_same_order,
                                     remainder_cnd_list,is_add_other_join_cond,join_type,
                                     select_stmt,id++,tmp)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add condition of join plan faild");
                                break;
                            }
                        }
                        else if ((ret = add_merge_join_expr(
                                      logical_plan,
                                      physical_plan,
                                      *join_op,
                                      *left_sort,
                                      *right_sort,
                                      *(*cnd_it),
                                      is_table_expr_same_order)) != OB_SUCCESS)
                             {
                                 TRANS_LOG("Add condition of join plan faild");
                             }
                    }
                        break;
                    case T_SEMI_BTW_JOIN:
                    {
                        if(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN)
                        {
                            if ((ret = add_semi_join_expr(
                                     logical_plan,
                                     physical_plan,
                                     *semi_join_op,
                                     *left_sort,
                                     *right_sort,
                                     *(*cnd_it),
                                     is_table_expr_same_order,
                                     remainder_cnd_list,is_add_other_join_cond,join_type,
                                     select_stmt,id++,tmp)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add condition of join plan faild");
                                break;
                            }
                        }
                        else if ((ret = add_merge_join_expr(
                                      logical_plan,
                                      physical_plan,
                                      *join_op,
                                      *left_sort,
                                      *right_sort,
                                      *(*cnd_it),
                                      is_table_expr_same_order)) != OB_SUCCESS)
                             {
                                 TRANS_LOG("Add condition of join plan faild");
                             }
                    }
                        break;
                    default:
                    {
                        if ((ret = add_merge_join_expr(
                                 logical_plan,
                                 physical_plan,
                                 *join_op,
                                 *left_sort,
                                 *right_sort,
                                 *(*cnd_it),
                                 is_table_expr_same_order)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                        }
                    }
                        break;
                    }
                    /* if(tmp.join_type_ == T_BLOOMFILTER_JOIN&&(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN))
            {
                //CREATE_PHY_OPERRATOR(bloomfilter_join_op, ObBloomFilterJoin, physical_plan, err_stat);
                //bloomfilter_join_op->set_join_type(join_type);
                if ((ret = add_bloomfilter_join_expr(
                         logical_plan,
                         physical_plan,
                         *bloomfilter_join_op,
                         *left_sort,
                         *right_sort,
                         *(*cnd_it),
                         is_table_expr_same_order)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add condition of join plan faild");
                    break;
                }
            }
            else if(tmp.join_type_ == T_SEMI_JOIN && (join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN))
            {
                if ((ret = add_semi_join_expr(
                         logical_plan,
                         physical_plan,
                         *semi_join_op,
                         *left_sort,
                         *right_sort,
                         *(*cnd_it),
                         is_table_expr_same_order,
                         remainder_cnd_list,is_add_other_join_cond,join_type)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add condition of join plan faild");
                    break;
                }
            }
            else
            {
                if ((ret = add_merge_join_expr(
                         logical_plan,
                         physical_plan,
                         *join_op,
                         *left_sort,
                         *right_sort,
                         *(*cnd_it),
                         is_table_expr_same_order)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add condition of join plan faild");
                    break;
                }
            }*/
                }
                else
                {
                    //CREATE_PHY_OPERRATOR(join_op, ObMergeJoin, physical_plan, err_stat);
                    //join_op->set_join_type(join_type);
                    // add equal join condition to merge join
                    if ((ret = add_merge_join_expr(
                             logical_plan,
                             physical_plan,
                             *join_op,
                             *left_sort,
                             *right_sort,
                             *(*cnd_it),
                             is_table_expr_same_order)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add condition of join plan faild");
                        break;
                    }
                }
                join_table_bitset.add_members(l_expr_tab_bitset);
                join_table_bitset.add_members(r_expr_tab_bitset);

                del_it = cnd_it;
                cnd_it++;
                if (!outer_join_scope && (ret = remainder_cnd_list.erase(del_it)) != OB_SUCCESS)
                {
                    TRANS_LOG("Generate join plan faild");
                    break;
                }
            }
            else if ((*cnd_it)->get_expr()->is_join_cond()
                     && (*cnd_it)->get_tables_set().is_subset(join_table_bitset))
            {
                ObBinaryOpRawExpr *join_cnd = dynamic_cast<ObBinaryOpRawExpr*>((*cnd_it)->get_expr());
                ObBinaryRefRawExpr *expr1 = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_first_op_expr());
                int32_t bit_idx1 = select_stmt->get_table_bit_index(expr1->get_first_ref_id());
                bool is_table_expr_same_order = true;
                OB_ASSERT(l_tab_bitset);
                if (!(l_tab_bitset->has_member(bit_idx1)))
                {
                    is_table_expr_same_order = false;
                }
                if(select_stmt->get_query_hint().join_array_.size()>index_)
                {
                    ObJoinTypeArray tmp = select_stmt->get_query_hint().join_array_.at(index_);
                    if(tmp.join_type_ == T_BLOOMFILTER_JOIN&&(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN))
                    {
                        //CREATE_PHY_OPERRATOR(bloomfilter_join_op, ObBloomFilterJoin, physical_plan, err_stat);
                        //bloomfilter_join_op->set_join_type(join_type);
                        if ((ret = add_bloomfilter_join_expr(
                                 logical_plan,
                                 physical_plan,
                                 *bloomfilter_join_op,
                                 *left_sort,
                                 *right_sort,
                                 *(*cnd_it),
                                 is_table_expr_same_order)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                            break;
                        }
                    }
                    else  if(tmp.join_type_ == T_SEMI_JOIN && (join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN))
                    {
                        if ((ret = add_semi_join_expr(
                                 logical_plan,
                                 physical_plan,
                                 *semi_join_op,
                                 *left_sort,
                                 *right_sort,
                                 *(*cnd_it),
                                 is_table_expr_same_order,
                                 remainder_cnd_list,is_add_other_join_cond,join_type,select_stmt,id++,tmp)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                            break;
                        }
                    }
                    else  if(tmp.join_type_ == T_SEMI_BTW_JOIN && (join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN))
                    {
                        if ((ret = add_semi_join_expr(
                                 logical_plan,
                                 physical_plan,
                                 *semi_join_op,
                                 *left_sort,
                                 *right_sort,
                                 *(*cnd_it),
                                 is_table_expr_same_order,
                                 remainder_cnd_list,is_add_other_join_cond,join_type,select_stmt,id++,tmp)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                            break;
                        }
                    }
                    else
                    {
                        //CREATE_PHY_OPERRATOR(join_op, ObMergeJoin, physical_plan, err_stat);
                        //join_op->set_join_type(join_type);
                        // add equal join condition to merge join
                        if ((ret = add_merge_join_expr(
                                 logical_plan,
                                 physical_plan,
                                 *join_op,
                                 *left_sort,
                                 *right_sort,
                                 *(*cnd_it),
                                 is_table_expr_same_order)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                            break;
                        }
                    }
                }
                else
                {
                    //CREATE_PHY_OPERRATOR(join_op, ObMergeJoin, physical_plan, err_stat);
                    //join_op->set_join_type(join_type);
                    // add equal join condition to merge join
                    if ((ret = add_merge_join_expr(
                             logical_plan,
                             physical_plan,
                             *join_op,
                             *left_sort,
                             *right_sort,
                             *(*cnd_it),
                             is_table_expr_same_order)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add condition of join plan faild");
                        break;
                    }
                }
                del_it = cnd_it;
                cnd_it++;
                if (!outer_join_scope && (ret = remainder_cnd_list.erase(del_it)) != OB_SUCCESS)
                {
                    TRANS_LOG("Generate join plan faild");
                    break;
                }
            }
            else if ((*cnd_it)->get_tables_set().is_subset(join_table_bitset)
                     && !((*cnd_it)->is_contain_alias()
                          && (*cnd_it)->get_tables_set().overlap(l_expr_tab_bitset)
                          && (*cnd_it)->get_tables_set().overlap(r_expr_tab_bitset)))
            {
                ObSqlExpression join_other_cnd;
                if(select_stmt->get_query_hint().join_array_.size()>index_)
                {
                    ObJoinTypeArray tmp = select_stmt->get_query_hint().join_array_.at(index_);
                    if(tmp.join_type_ == T_BLOOMFILTER_JOIN&&(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN))
                    {
                        if ((ret = ((*cnd_it)->fill_sql_expression(
                                        join_other_cnd,
                                        this,
                                        logical_plan,
                                        physical_plan))) != OB_SUCCESS
                                || (ret = bloomfilter_join_op->add_other_join_condition(join_other_cnd)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                            break;
                        }
                    }
                    else  if((tmp.join_type_ == T_SEMI_JOIN||tmp.join_type_ == T_SEMI_BTW_JOIN )&& (join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN))
                    {
                        if ((ret = ((*cnd_it)->fill_sql_expression(
                                        join_other_cnd,
                                        this,
                                        logical_plan,
                                        physical_plan))) != OB_SUCCESS
                                || (ret = semi_join_op->add_other_join_condition(join_other_cnd)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                            break;
                        }
                    }
                    else
                    {
                        if ((ret = ((*cnd_it)->fill_sql_expression(
                                        join_other_cnd,
                                        this,
                                        logical_plan,
                                        physical_plan))) != OB_SUCCESS
                                || (ret = join_op->add_other_join_condition(join_other_cnd)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                            break;
                        }
                    }
                }
                else
                {
                    if ((ret = ((*cnd_it)->fill_sql_expression(
                                    join_other_cnd,
                                    this,
                                    logical_plan,
                                    physical_plan))) != OB_SUCCESS
                            || (ret = join_op->add_other_join_condition(join_other_cnd)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add condition of join plan faild");
                        break;
                    }
                }
                del_it = cnd_it;
                cnd_it++;
                if (!outer_join_scope && (ret = remainder_cnd_list.erase(del_it)) != OB_SUCCESS)
                {
                    TRANS_LOG("Generate join plan faild");
                    break;
                }
            }
            else
            {
                cnd_it++;
            }
        } //end for

        if (ret == OB_SUCCESS)
        {
            if (join_table_bitset.is_empty() == false)
            {
                if(select_stmt->get_query_hint().join_array_.size()>index_)
                {
                    ObJoinTypeArray tmp = select_stmt->get_query_hint().join_array_.at(index_);
                    if(tmp.join_type_ == T_BLOOMFILTER_JOIN&&(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN))
                    {
                        if ((ret = bloomfilter_join_op->set_child(0, *left_sort)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add child of join plan faild");
                            break;
                        }
                        if ((ret = bloomfilter_join_op->set_child(1, *right_sort)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add child of join plan faild");
                            break;
                        }
                    }
                    else  if((tmp.join_type_ == T_SEMI_JOIN ||tmp.join_type_ == T_SEMI_BTW_JOIN ) && (join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN))
                    {
                        // find a join condition, a merge join will be used here
                        OB_ASSERT(left_sort != NULL);
                        OB_ASSERT(right_sort != NULL);
                        if ((ret = semi_join_op->set_child(0, *left_sort)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add child of join plan faild");
                            break;
                        }
                        if ((ret = semi_join_op->set_child(1, *right_sort)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add child of join plan faild");
                            break;
                        }
                    }
                    else
                    {
                        // find a join condition, a merge join will be used here
                        OB_ASSERT(left_sort != NULL);
                        OB_ASSERT(right_sort != NULL);
                        if ((ret = join_op->set_child(0, *left_sort)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add child of join plan faild");
                            break;
                        }
                        if ((ret = join_op->set_child(1, *right_sort)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add child of join plan faild");
                            break;
                        }
                    }
                }
                else
                {
                    // find a join condition, a merge join will be used here
                    OB_ASSERT(left_sort != NULL);
                    OB_ASSERT(right_sort != NULL);
                    if ((ret = join_op->set_child(0, *left_sort)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add child of join plan faild");
                        break;
                    }
                    if ((ret = join_op->set_child(1, *right_sort)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add child of join plan faild");
                        break;
                    }
                }
            }

            else
            {
                // Can not find a join condition, a product join will be used here
                // FIX me, should be ObJoin, it will be fixed when Join is supported
                //ObPhyOperator *op = NULL;
                if(select_stmt->get_query_hint().join_array_.size()>index_)
                {
                    ObPhyOperator *op = NULL;
                    ObJoinTypeArray tmp = select_stmt->get_query_hint().join_array_.at(index_);
                    if(tmp.join_type_ == T_BLOOMFILTER_JOIN&&(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN))
                    {
                        CREATE_PHY_OPERRATOR(bloomfilter_join_op, ObBloomFilterJoin, physical_plan, err_stat);
                        if (OB_SUCCESS != ret)//add qianzm [null operator unjudgement bug1181] 20160520
                        {
                        }
                        else
                        {
                            bloomfilter_join_op->set_join_type(join_type);
                            if ((ret = phy_table_list.pop_front(op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Generate join plan faild");
                                break;
                            }
                            if ((ret = bloomfilter_join_op->set_child(0, *op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add child of join plan faild");
                                break;
                            }
                            if ((ret = phy_table_list.pop_front(op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Generate join plan faild");
                                break;
                            }
                            if ((ret = bloomfilter_join_op->set_child(1, *op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add child of join plan faild");
                                break;
                            }
                        }
                    }
                    else  if((tmp.join_type_ == T_SEMI_JOIN ||tmp.join_type_ == T_SEMI_BTW_JOIN ) && (join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN))
                    {
                        CREATE_PHY_OPERRATOR(semi_join_op, ObSemiJoin, physical_plan, err_stat);
                        if (OB_SUCCESS != ret)//add qianzm [null operator unjudgement bug1181] 20160520
                        {
                        }
                        else
                        {
                            semi_join_op->set_join_type(join_type);
                            ObPhyOperator *op = NULL;
                            //TBSYS_LOG(ERROR, "hushuang 2-2--*op+*op_1");
                            if ((ret = phy_table_list.pop_front(op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Generate join plan faild");
                                break;
                            }
                            if ((ret = semi_join_op->set_child(0, *op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add child of join plan faild");
                                break;
                            }
                            if ((ret = phy_table_list.pop_front(op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Generate join plan faild");
                                break;
                            }
                            if ((ret = semi_join_op->set_child(1, *op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add child of join plan faild");
                                break;
                            }
                        }
                    }
                    else
                    {
                        CREATE_PHY_OPERRATOR(join_op, ObMergeJoin, physical_plan, err_stat);
                        if (OB_SUCCESS != ret)//add qianzm [null operator unjudgement bug1181] 20160520
                        {
                        }
                        else
                        {
                            join_op->set_join_type(join_type);
                            if ((ret = phy_table_list.pop_front(op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Generate join plan faild");
                                break;
                            }
                            if ((ret = join_op->set_child(0, *op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add child of join plan faild");
                                break;
                            }
                            if ((ret = phy_table_list.pop_front(op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Generate join plan faild");
                                break;
                            }
                            if ((ret = join_op->set_child(1, *op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add child of join plan faild");
                                break;
                            }
                        }
                    }
                }
                else
                {
                    CREATE_PHY_OPERRATOR(join_op, ObMergeJoin, physical_plan, err_stat);
                    if (OB_SUCCESS != ret)//add qianzm [null operator unjudgement bug1181] 20160520
                    {
                    }
                    else
                    {
                        join_op->set_join_type(join_type);
                        ObPhyOperator *op = NULL;
                        //TBSYS_LOG(ERROR, "hushuang 2-2--*op+*op_1");
                        if ((ret = phy_table_list.pop_front(op)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Generate join plan faild");
                            break;
                        }
                        if ((ret = join_op->set_child(0, *op)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add child of join plan faild");
                            break;
                        }
                        if ((ret = phy_table_list.pop_front(op)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Generate join plan faild");
                            break;
                        }
                        if ((ret = join_op->set_child(1, *op)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add child of join plan faild");
                            break;
                        }
                    }
                }
                bitset_list.pop_front(l_expr_tab_bitset);
                join_table_bitset.add_members(l_expr_tab_bitset);
                bitset_list.pop_front(r_expr_tab_bitset);
                join_table_bitset.add_members(r_expr_tab_bitset);
            }
        }

        // add other join conditions
        for (cnd_it = remainder_cnd_list.begin(); ret == OB_SUCCESS && cnd_it != remainder_cnd_list.end(); )
        {
            if ((*cnd_it)->is_contain_alias()
                    && (*cnd_it)->get_tables_set().overlap(l_expr_tab_bitset)
                    && (*cnd_it)->get_tables_set().overlap(r_expr_tab_bitset))
            {
                cnd_it++;
            }
            else if ((*cnd_it)->get_tables_set().is_subset(join_table_bitset))
            {
                ObSqlExpression other_cnd;
                if(select_stmt->get_query_hint().join_array_.size()>index_)
                {
                    ObJoinTypeArray tmp = select_stmt->get_query_hint().join_array_.at(index_);
                    if(tmp.join_type_ == T_BLOOMFILTER_JOIN&&(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN))
                    {
                        if ((ret = (*cnd_it)->fill_sql_expression(
                                 other_cnd,
                                 this,
                                 logical_plan,
                                 physical_plan)) != OB_SUCCESS
                                || (ret = bloomfilter_join_op->add_other_join_condition(other_cnd)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                            break;
                        }
                    }
                    else if((tmp.join_type_ == T_SEMI_JOIN ||tmp.join_type_ == T_SEMI_BTW_JOIN ) && (join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN))
                    {
                       // TBSYS_LOG(ERROR,"wl add other join conditions");
                        if ((ret = (*cnd_it)->fill_sql_expression(
                                 other_cnd,
                                 this,
                                 logical_plan,
                                 physical_plan)) != OB_SUCCESS
                                || (ret = semi_join_op->add_other_join_condition(other_cnd)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                            break;
                        }
                    }
                    else
                    {
                        if ((ret = (*cnd_it)->fill_sql_expression(
                                 other_cnd,
                                 this,
                                 logical_plan,
                                 physical_plan)) != OB_SUCCESS
                                || (ret = join_op->add_other_join_condition(other_cnd)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add condition of join plan faild");
                            break;
                        }
                    }
                }
                else
                {
                    if ((ret = (*cnd_it)->fill_sql_expression(
                             other_cnd,
                             this,
                             logical_plan,
                             physical_plan)) != OB_SUCCESS
                            || (ret = join_op->add_other_join_condition(other_cnd)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add condition of join plan faild");
                        break;
                    }
                }
                del_it = cnd_it;
                cnd_it++;
                if (!outer_join_scope && (ret = remainder_cnd_list.erase(del_it)) != OB_SUCCESS)
                {
                    TRANS_LOG("Generate join plan faild");
                    break;
                }
            }
            else
            {
                cnd_it++;
            }
        }

        // columnlize the alias expression

        oceanbase::common::ObList<ObSqlRawExpr*>::iterator alias_it;
        for (alias_it = none_columnlize_alias.begin(); ret == OB_SUCCESS && alias_it != none_columnlize_alias.end(); )
        {
            if ((*alias_it)->is_columnlized())
            {
                common::ObList<ObSqlRawExpr*>::iterator del_alias = alias_it;
                alias_it++;
                if ((ret = none_columnlize_alias.erase(del_alias)) != OB_SUCCESS)
                {
                    TRANS_LOG("Generate join plan faild");
                    break;
                }
            }
            else if ((*alias_it)->get_tables_set().is_subset(join_table_bitset))
            {
                (*alias_it)->set_columnlized(true);
                if (project_op == NULL)
                {
                    CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan, err_stat);
                    if (ret != OB_SUCCESS)
                        break;
                    if(select_stmt->get_query_hint().join_array_.size()>index_)
                    {
                        ObJoinTypeArray tmp = select_stmt->get_query_hint().join_array_.at(index_);
                        if(tmp.join_type_ == T_BLOOMFILTER_JOIN&&(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN))
                        {
                            if ((ret = project_op->set_child(0, *bloomfilter_join_op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Generate project operator on join plan faild");
                                break;
                            }
                        }
                        else  if((tmp.join_type_ == T_SEMI_JOIN ||tmp.join_type_ == T_SEMI_BTW_JOIN ) && (join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN))
                        {
                            if ((ret = project_op->set_child(0, *semi_join_op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Generate project operator on join plan faild");
                                break;
                            }
                        }
                        else
                        {
                            if ((ret = project_op->set_child(0, *join_op)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Generate project operator on join plan faild");
                                break;
                            }
                        }
                    }
                    else
                    {
                        if ((ret = project_op->set_child(0, *join_op)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Generate project operator on join plan faild");
                            break;
                        }
                    }
                }
                ObSqlExpression alias_expr;
                if ((ret = (*alias_it)->fill_sql_expression(
                         alias_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS
                        || (ret = project_op->add_output_column(alias_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add project column on join plan faild");
                    break;
                }
                common::ObList<ObSqlRawExpr*>::iterator del_alias = alias_it;
                alias_it++;
                if ((ret = none_columnlize_alias.erase(del_alias)) != OB_SUCCESS)
                {
                    TRANS_LOG("Generate join plan faild");
                    break;
                }

            }
            else
            {
                alias_it++;
            }

        }

        if (ret == OB_SUCCESS)
        {
            ObPhyOperator *result_op = NULL;
            if (project_op == NULL)
            {
                if(select_stmt->get_query_hint().join_array_.size()>index_)
                {
                    ObJoinTypeArray tmp = select_stmt->get_query_hint().join_array_.at(index_);
                    if(tmp.join_type_ == T_BLOOMFILTER_JOIN&&(join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN))
                    {
                        result_op = bloomfilter_join_op;
                    }
                    else  if((tmp.join_type_ == T_SEMI_JOIN ||tmp.join_type_ == T_SEMI_BTW_JOIN )&& (join_type == ObJoin::INNER_JOIN || join_type == ObJoin::LEFT_OUTER_JOIN || join_type == ObJoin::RIGHT_OUTER_JOIN))
                    {
                        result_op = semi_join_op ;
                    }
                    else
                    {
                        result_op = join_op ;
                    }
                }
                else
                {
                    result_op = join_op ;
                }
            }

            else
            {
                result_op = project_op;
            }
            if ((ret = phy_table_list.push_back(result_op)) != OB_SUCCESS
                    || (ret = bitset_list.push_back(join_table_bitset)) != OB_SUCCESS)
            {
                TRANS_LOG("Generate join plan failed");
                break;
            }
            join_table_bitset.clear();
        }
        index_++;
    }
    if(index_ == 1)
    {
        select_stmt->get_query_hint().join_array_.remove(0);
    }
    return ret;
}
//modify:e

int ObTransformer::gen_phy_tables(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObSelectStmt *select_stmt,
        bool& group_agg_pushed_down,
        bool& limit_pushed_down,
        oceanbase::common::ObList<ObPhyOperator*>& phy_table_list,
        oceanbase::common::ObList<ObBitSet<> >& bitset_list,
        oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list,
        oceanbase::common::ObList<ObSqlRawExpr*>& none_columnlize_alias
        , ObPhyOperator *sequence_op //add liuzy [sequence select] 20150525:", ObPhyOperator *sequence_op"
        )
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObPhyOperator *table_op = NULL;
    ObBitSet<> bit_set;

    int32_t num = select_stmt->get_select_item_size();
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
        const SelectItem& select_item = select_stmt->get_select_item(i);
        if (select_item.is_real_alias_)
        {
            ObSqlRawExpr *alias_expr = logical_plan->get_expr(select_item.expr_id_);
            if (alias_expr == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Add alias to internal list failed");
                break;
            }
            else if (alias_expr->is_columnlized() == false
                     && (ret = none_columnlize_alias.push_back(alias_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add alias to internal list failed");
                break;
            }
        }
    }

    int32_t num_table = select_stmt->get_from_item_size();
    // no from clause of from DUAL
    if (ret == OB_SUCCESS && num_table <= 0)
    {
        ObDualTableScan *dual_table_op = NULL;
        if (CREATE_PHY_OPERRATOR(dual_table_op, ObDualTableScan, physical_plan, err_stat) == NULL)
        {
            TRANS_LOG("Generate dual table operator failed, ret=%d", ret);
        }
        if (OB_SUCCESS != ret)//add qianzm [null operator unjudgement bug1181] 20160520
        {
        }
        else if ((ret = phy_table_list.push_back(dual_table_op)) != OB_SUCCESS)
        {
            TRANS_LOG("Add table to internal list failed");
        }
        // add empty bit set
        else if ((ret = bitset_list.push_back(bit_set)) != OB_SUCCESS)
        {
            TRANS_LOG("Add bitset to internal list failed");
        }
    }
    for(int32_t i = 0; ret == OB_SUCCESS && i < num_table; i++)
    {
        const FromItem& from_item = select_stmt->get_from_item(i);
        if (from_item.is_joined_ == false)
        {
            /* base-table or temporary table */
            if ((ret = gen_phy_table(
                     logical_plan,
                     physical_plan,
                     err_stat,
                     select_stmt,
                     from_item.table_id_,
                     table_op,
                     &group_agg_pushed_down,
                     &limit_pushed_down
                     , sequence_op//add liuzy ", sequence_op" [sequence select]20150616
                     )) != OB_SUCCESS)
                break;
            bit_set.add_member(select_stmt->get_table_bit_index(from_item.table_id_));
            if (select_stmt->get_table_bit_index(from_item.table_id_) < 0)
            {
              TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,from_item.table_id_);
            }
        }
        else
        {
            /* Outer Join */
            JoinedTable *joined_table = select_stmt->get_joined_table(from_item.table_id_);
            if (joined_table == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong joined table id '%lu'", from_item.table_id_);
                break;
            }
            OB_ASSERT(joined_table->table_ids_.count() >= 2);
            OB_ASSERT(joined_table->table_ids_.count() - 1 == joined_table->join_types_.count());
            OB_ASSERT(joined_table->join_types_.count() == joined_table->expr_nums_per_join_.count());
            //add liumz, [outer_join_on_where]20151019:b
            bool outer_join_scope = false;
            for (int32_t j = 1; ret == OB_SUCCESS && !outer_join_scope && j < joined_table->table_ids_.count(); j++)
            {
                if(ret == OB_SUCCESS)
                {
                    switch (joined_table->join_types_.at(j - 1))
                    {
                    case JoinedTable::T_FULL:
                        outer_join_scope = true;
                        break;
                    case JoinedTable::T_LEFT:
                        outer_join_scope = true;
                        break;
                    case JoinedTable::T_RIGHT:
                        outer_join_scope = true;
                        break;
                    case JoinedTable::T_INNER:
                        break;
                    default:
                        /* won't be here */
                        break;
                    }
                }
            }
            //add:e
            if ((ret = gen_phy_table(
                     logical_plan,
                     physical_plan,
                     err_stat,
                     select_stmt,
                     joined_table->table_ids_.at(0),
                     table_op,
                     /*add liumz, [outer_join_on_where]20150927:b*/
                     NULL,
                     NULL,
                     NULL,
                     outer_join_scope/*add:e*/)) != OB_SUCCESS)
            {
                break;
            }
            bit_set.add_member(select_stmt->get_table_bit_index(joined_table->table_ids_.at(0)));
            if (select_stmt->get_table_bit_index(joined_table->table_ids_.at(0)) < 0)
            {
              TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,joined_table->table_ids_.at(0));
            }

            ObPhyOperator *right_op = NULL;
            ObSqlRawExpr *join_expr = NULL;
            int64_t join_expr_position = 0;
            int64_t join_expr_num = 0;
            for (int32_t j = 1; ret == OB_SUCCESS && j < joined_table->table_ids_.count(); j++)
            {
                if ((ret = gen_phy_table(
                         logical_plan,
                         physical_plan,
                         err_stat,
                         select_stmt,
                         joined_table->table_ids_.at(j),
                         right_op,
                         /*add liumz, [outer_join_on_where]20150927:b*/
                         NULL,
                         NULL,
                         NULL,
                         outer_join_scope/*add:e*/)) != OB_SUCCESS)
                {
                    break;
                }
                ObList<ObPhyOperator*>  outer_join_tabs;
                ObList<ObBitSet<> >        outer_join_bitsets;
                ObList<ObSqlRawExpr*>   outer_join_cnds;
                if (OB_SUCCESS != (ret = outer_join_tabs.push_back(table_op))
                        || OB_SUCCESS != (ret = outer_join_tabs.push_back(right_op))
                        || OB_SUCCESS != (ret = outer_join_bitsets.push_back(bit_set)))
                {
                    TBSYS_LOG(WARN, "fail to push op to outer_join tabs. ret=%d", ret);
                    break;
                }
                ObBitSet<> right_table_bitset;
                int32_t  right_table_bit_index = select_stmt->get_table_bit_index(joined_table->table_ids_.at(j));
                right_table_bitset.add_member(right_table_bit_index);
                bit_set.add_member(right_table_bit_index);
                if (right_table_bit_index < 0)
                {
                  TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,joined_table->table_ids_.at(j));
                }
                if (OB_SUCCESS != (ret = outer_join_bitsets.push_back(right_table_bitset)))
                {
                    TBSYS_LOG(WARN, "fail to push bitset to list. ret=%d", ret);
                    break;
                }
                join_expr_num = joined_table->expr_nums_per_join_.at(j - 1);
                for(int64_t join_index = 0; join_index < join_expr_num; ++join_index)
                {
                    join_expr = logical_plan->get_expr(joined_table->expr_ids_.at(join_expr_position + join_index));
                    if (join_expr == NULL)
                    {
                        ret = OB_ERR_ILLEGAL_INDEX;
                        TRANS_LOG("Add outer join condition faild");
                        break;
                    }
                    else if (OB_SUCCESS != (ret = outer_join_cnds.push_back(join_expr)))
                    {
                        TBSYS_LOG(WARN, "fail to push bitset to list. ret=%d", ret);
                        break;
                    }
                }

                if (OB_SUCCESS == ret)
                {
                    join_expr_position += join_expr_num;
                }
                else
                {
                    break;
                }

                ObJoin::JoinType join_type = ObJoin::INNER_JOIN;
                if(ret == OB_SUCCESS)
                {
                    switch (joined_table->join_types_.at(j - 1))
                    {
                    case JoinedTable::T_FULL:
                        join_type = ObJoin::FULL_OUTER_JOIN;
                        break;
                    case JoinedTable::T_LEFT:
                        join_type = ObJoin::LEFT_OUTER_JOIN;
                        break;
                    case JoinedTable::T_RIGHT:
                        join_type = ObJoin::RIGHT_OUTER_JOIN;
                        break;
                    case JoinedTable::T_INNER:
                        join_type = ObJoin::INNER_JOIN;
                        break;
                    case JoinedTable::T_SEMI:
                        join_type = ObJoin::SEMI_JOIN;
                        break;
                    case JoinedTable::T_SEMI_LEFT:
                        join_type = ObJoin::LEFT_SEMI_JOIN;
                        break;
                    case JoinedTable::T_SEMI_RIGHT:
                        join_type = ObJoin::RIGHT_SEMI_JOIN;
                        break;
                    default:
                        /* won't be here */
                        join_type = ObJoin::INNER_JOIN;
                        break;
                    }
                }
                // Now we don't optimize outer join
                // outer_join_cnds is empty, we will do something when optimizing.
                if ((ret = gen_phy_joins(
                         logical_plan,
                         physical_plan,
                         err_stat,
                         select_stmt,
                         join_type,
                         outer_join_tabs,
                         outer_join_bitsets,
                         outer_join_cnds,
                         none_columnlize_alias)) != OB_SUCCESS)
                {
                    break;
                }
                else if ((ret = outer_join_tabs.pop_front(table_op)) != OB_SUCCESS)
                {
                    break;
                }
            }
        }
        if (ret == OB_SUCCESS && (ret = phy_table_list.push_back(table_op)) != OB_SUCCESS)
        {
            TRANS_LOG("Add table to internal list failed");
            break;
        }
        if (ret == OB_SUCCESS && (ret = bitset_list.push_back(bit_set)) != OB_SUCCESS)
        {
            TRANS_LOG("Add bitset to internal list failed");
            break;
        }
        bit_set.clear();
    }

    num = select_stmt->get_condition_size();
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
        uint64_t expr_id = select_stmt->get_condition_id(i);
        ObSqlRawExpr *where_expr = logical_plan->get_expr(expr_id);
        if (where_expr && where_expr->is_apply() == false
                && (ret = remainder_cnd_list.push_back(where_expr)) != OB_SUCCESS)
        {
            TRANS_LOG("Add condition to internal list failed");
            break;
        }
    }

    return ret;
}
//add fanqiushi_index
int ObTransformer::gen_phy_table_for_storing(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObStmt *stmt,
        uint64_t table_id,
        ObPhyOperator*& table_op,
        bool* group_agg_pushed_down,
        bool* limit_pushed_down,
        bool is_use_storing_column,
        uint64_t index_tid,
        Expr_Array *filter_array,
        Expr_Array *project_array)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    TableItem* table_item = NULL;
    ObSqlReadStrategy sql_read_strategy;
    int64_t num = 0;
    int64_t sub_query_num = 0;//add fanqiushi_index_in  tianz
    bool is_ailias_table=false;
    ObRpcScanHint hint;
    uint64_t source_tid=OB_INVALID_ID;
    ObTableScan *table_scan_op = NULL;
    UNUSED(logical_plan);

    if (table_id == OB_INVALID_ID || (table_item = stmt->get_table_item_by_id(table_id)) == NULL)
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("Wrong table id");
    }
    else if(filter_array == NULL || project_array == NULL)
    {
        ret=OB_ERROR;
        TBSYS_LOG(ERROR,"filter_array==NULL or project_array == NULL");
    }
    else
    {
        source_tid=table_item->ref_id_;
        if(table_item->type_ == TableItem::ALIAS_TABLE)
        {
            is_ailias_table=true;
        }
        if(is_use_storing_column)
        {
            table_item->ref_id_=index_tid;
        }
    }
    //add huangcc [fix transaction read uncommit bug]2016/11/21
    if (OB_SUCCESS == ret)
    {
        hint.read_consistency_ = stmt->get_query_hint().read_consistency_;
    }
    //add end

    if (ret == OB_SUCCESS)
    {
        switch (table_item->type_)
        {
        case TableItem::BASE_TABLE:
            /* get through */
        case TableItem::ALIAS_TABLE:
        {
            ObTableRpcScan *table_rpc_scan_op = NULL;
            CREATE_PHY_OPERRATOR(table_rpc_scan_op, ObTableRpcScan, physical_plan, err_stat);
            if(is_ailias_table==false)
            {   if (ret == OB_SUCCESS
                        && (ret = table_rpc_scan_op->set_table(table_item->ref_id_, table_item->ref_id_)) != OB_SUCCESS)
                {
                    TRANS_LOG("ObTableRpcScan set table faild");
                }
            }
            else
            {
                if (ret == OB_SUCCESS && (ret = table_rpc_scan_op->set_table(table_item->table_id_, table_item->ref_id_)) != OB_SUCCESS)
                {
                    TRANS_LOG("ObTableRpcScan set table faild");
                }
            }
            //TBSYS_LOG(ERROR,"test::fanqs,,table_item->table_id_=%ld,table_item->ref_id_=%ld",table_item->table_id_,table_item->ref_id_);
            table_rpc_scan_op->set_is_index_for_storing(true,index_tid);

            num = project_array->count();
            ObRowDesc desc_for_storing;
            for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
            {
                ObSqlExpression output_expr=project_array->at(i);
                if (OB_SUCCESS != (ret = desc_for_storing.add_column_desc(output_expr.get_table_id(), output_expr.get_column_id())))
                {
                    TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
                }
            }
            if(OB_SUCCESS == ret)
                table_rpc_scan_op->set_is_use_index_for_storing(source_tid,desc_for_storing);



            /// determin request type: scan/get
            if (OB_SUCCESS == ret)
            {
                const ObTableSchema *table_schema = NULL;
                if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_item->ref_id_)))
                {
                    ret = OB_ERROR;
                    TRANS_LOG("Fail to get table schema for table[%ld]", table_item->ref_id_);
                }
                else
                {
                    sql_read_strategy.set_rowkey_info(table_schema->get_rowkey_info());
                    if ((ret = physical_plan->add_base_table_version(
                             table_item->ref_id_,
                             table_schema->get_schema_version()
                             )) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", table_item->ref_id_, ret);
                    }
                }
            }
            if ( OB_SUCCESS == ret)
            {
                int32_t read_method = ObSqlReadStrategy::USE_SCAN;
                hint.read_method_ = read_method;
            }

            if (ret == OB_SUCCESS)
            {
                ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
                if (select_stmt
                        && select_stmt->get_group_expr_size() <= 0
                        && select_stmt->get_having_expr_size() <= 0
                        && select_stmt->get_order_item_size() <= 0
                        && hint.read_method_ != ObSqlReadStrategy::USE_GET)
                {
                    hint.max_parallel_count = 1;
                }
                if ((ret = table_rpc_scan_op->init(sql_context_, &hint)) != OB_SUCCESS)
                {
                    TRANS_LOG("ObTableRpcScan init faild");
                }
				//add liumz, [optimize group_order by index]20170419:b
                else
                {
                    table_rpc_scan_op->set_indexed_group(select_stmt->is_indexed_group());
                }
				//add:e
            }
            if (ret == OB_SUCCESS)
                table_scan_op = table_rpc_scan_op;
            break;
        }
        default:
            // won't be here
            OB_ASSERT(0);
            break;
        }
    }

    //add dragon [Bugfix 1224] 2016-8-24 13:35:03
    //没有必要改变table_item的ref_id_的值，会导致semijoin的bug，详见bug号，这儿在使用完之后更改回来
    if(is_use_storing_column && table_item->table_id_ != table_item->ref_id_)
    {
      table_item->ref_id_ = source_tid;
    }
    //add e

    if(OB_SUCCESS == ret)
    {
        num = filter_array->count();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            ObSqlExpression *filter = ObSqlExpression::alloc();
            if (NULL == filter)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TRANS_LOG("no memory");
                break;
            }
            else
            {
                *filter=filter_array->at(i);
                //如果有多个列的表达式这里没有进行判断
                //add wanglei [second index fix] 20160425:b
                if(filter->is_expr_has_more_than_two_columns ())
                {
                    sub_query_num = sub_query_num + filter->get_sub_query_num();
                    if(is_use_storing_column==true &&is_ailias_table==false)
                    {
                        ObPostfixExpression& ops=filter->get_decoded_expression_v2();
                        ObArray<uint64_t> index_of_expr_array=OB_INVALID_ID;
                        if(OB_SUCCESS!=(ret=filter->get_tid_indexs (index_of_expr_array)))
                        {
                            TBSYS_LOG(ERROR,"faild to change tid,filter=%s",to_cstring(*filter));
                        }
                        for(int i=0;i<index_of_expr_array.count ();i++)
                        {
                            ObObj& obj=ops.get_expr_by_index(index_of_expr_array.at (i));
                            if(obj.get_type()==ObIntType)
                                obj.set_int(index_tid);
                        }
                    }
                }
                else
                {
                    sub_query_num = sub_query_num + filter->get_sub_query_num();
                    //add:e
                    if(is_use_storing_column==true &&is_ailias_table==false)
                    {
                        ObPostfixExpression& ops=filter->get_decoded_expression_v2();
                        uint64_t index_of_expr_array=OB_INVALID_ID;
                        //mod dragon [Bugfix] 2016-12-20
                        //如果query中有1=1这种条件，原流程日志会打印ERROR日志，其实这种条件并不会影响到实际的结果，改为WARN
                        if(OB_SUCCESS == (ret = filter->change_tid(index_of_expr_array)))
                        {
                          ObObj& obj=ops.get_expr_by_index(index_of_expr_array);
                          if(obj.get_type()==ObIntType)
                              obj.set_int(index_tid);
                        }
                        else
                        {
                          TBSYS_LOG(WARN,"filter get cid failed, if sql has 1 = 1 filter, it dosent matter");
                        }
                        /*---old---
                        if(OB_SUCCESS!=(ret=filter->change_tid(index_of_expr_array)))
                        {
                            TBSYS_LOG(ERROR,"faild to change tid,filter=%s",to_cstring(*filter));

                        }
                        ObObj& obj=ops.get_expr_by_index(index_of_expr_array);
                        if(obj.get_type()==ObIntType)
                            obj.set_int(index_tid);
                            ---old---*/
                        //mod e
                    }
                }

                //TBSYS_LOG(ERROR,"test::fanqs,,cid=%ld,tid=%ld,filter=%s",filter->get_column_id(),filter->get_table_id(),to_cstring(*filter));
                if(OB_SUCCESS == ret && (ret = table_scan_op->add_filter(filter)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add table filter condition faild");
                    //break;
                }
                if (OB_SUCCESS != ret && NULL != filter)
                {
                    ObSqlExpression::free(filter);
                    break;
                }
            }
        }

        // add output columns
        num = project_array->count();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {

            ObSqlExpression output_expr=project_array->at(i);
            if(is_use_storing_column)
            {
                if(is_ailias_table==false)
                {
                    ObArray<uint64_t> index_column_array;
                    if(OB_SUCCESS==output_expr.get_all_cloumn(index_column_array))
                    {
                        for(int32_t i=0;i<index_column_array.count();i++)
                        {
                            ObPostfixExpression& ops=output_expr.get_decoded_expression_v2();
                            ObObj& obj=ops.get_expr_by_index(index_column_array.at(i));
                            if(obj.get_type()==ObIntType)
                                obj.set_int(index_tid);
                        }
                    }
                }
                //output_expr.set_tid_cid();
                //TBSYS_LOG(ERROR,"test::fanqs,,output_expr.get_tid=%ld,,output_expr=%s",output_expr.get_table_id(),to_cstring(output_expr));
                if(output_expr.get_table_id() == source_tid)
                {
                    output_expr.set_table_id(index_tid);
                }
                if((ret = table_scan_op->add_output_column(output_expr, !is_ailias_table)) != OB_SUCCESS)//mod liumz, [optimize group_order by index]20170419
                {
                    TRANS_LOG("Add table output columns faild");
                    break;
                }

            }
        }
    }

    if (ret == OB_SUCCESS)
        table_op = table_scan_op;

    *group_agg_pushed_down = false;
    //*limit_pushed_down = false;

    //add liumz, [bugfix_limit_push_down]20160822:b
    bool group_down = false;//add liumz, [optimize group_order by index]20170419
    bool limit_down = false;
    ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
	//add liumz, [optimize group_order by index]20170419:b
    /* Try to push down aggregations */
    if (ret == OB_SUCCESS && group_agg_push_down_param_ && select_stmt)
    {
        ret = try_push_down_group_agg_for_storing(
                    logical_plan,
                    physical_plan,
                    err_stat,
                    select_stmt,
                    group_down,
                    table_op,
                    index_tid,
                    is_ailias_table);
        if (group_agg_pushed_down)
            *group_agg_pushed_down = group_down;
    }
    /* Try to push down limit */
	//add:e
    if (ret == OB_SUCCESS && select_stmt)
    {
        ret = try_push_down_limit(
                    logical_plan,
                    physical_plan,
                    err_stat,
                    select_stmt,
                    limit_down,
                    table_op);
        if (limit_pushed_down)
            *limit_pushed_down = limit_down;
    }
    //add:e

    //add by fanqiushi_index_in tianz
    ObMultiBind *multi_bind_op = NULL;
    if(ret == OB_SUCCESS && (sub_query_num>0))
        //if(ret!=OB_SUCCESS)
    {
        physical_plan->need_extend_time();
        int32_t main_query_idx= 0;
        if (CREATE_PHY_OPERRATOR(multi_bind_op, ObMultiBind, physical_plan, err_stat) == NULL
                || (ret = multi_bind_op->set_child(main_query_idx, *table_op)) != OB_SUCCESS)
        {
            TRANS_LOG("fail to set child:ret[%d]",ret);
        }
        else if (table_scan_op->is_base_table_id_valid())//mod zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118
        {
            ObTableRpcScan *table_rpc_scan_op = dynamic_cast<ObTableRpcScan *>( table_scan_op);
            if(NULL == table_rpc_scan_op )
            {
                ret = OB_ERROR;
                TRANS_LOG("wrong get table_rpc_scan_op, can't dynamic cast");
            }
            else
            {
                //for every sub_query
                //1.get it's physical plan index from main query(ObTableRpcScan)
                //2.get it's top physical operator address from ObPhysicalPlan by it's physical plan index
                //3.bind it's top physical operator address to ObMultiBind
                for(int32_t sub_query_idx = 1;sub_query_idx<=sub_query_num;sub_query_idx++)
                {
                    int32_t index = OB_INVALID_INDEX;
                    ObPhyOperator * sub_operator = NULL;
                    if((ret = table_rpc_scan_op->get_sub_query_index(sub_query_idx,index))!=OB_SUCCESS)
                    {
                        TRANS_LOG("wrong get sub query index");
                    }
                    else if(NULL == (sub_operator = physical_plan->get_phy_query(index)))
                    {
                        ret = OB_INVALID_INDEX;
                        TRANS_LOG("wrong get sub query operator");
                    }
                    else if(OB_SUCCESS != (ret = multi_bind_op->set_child(sub_query_idx, *sub_operator)))
                    {
                        TRANS_LOG("fail to set child:ret[%d]",ret);
                    }

                }
            }
        }
        //add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
        else
        {
            ObTableMemScan *table_mem_scan_op = dynamic_cast<ObTableMemScan*>( table_scan_op);
            if(NULL == table_mem_scan_op )
            {
                ret = OB_ERROR;
                TRANS_LOG("wrong get table_mem_scan_op, can't dynamic cast");
            }
            else
            {
                //for every sub_query
                //1.get it's physical plan index from main query(ObTableMemScan)
                //2.get it's top physical operator address from ObPhysicalPlan by it's physical plan index
                //3.bind it's top physical operator address to ObMultiBind
                for(int32_t sub_query_idx = 1;sub_query_idx<=sub_query_num;sub_query_idx++)
                {
                    int32_t index = OB_INVALID_INDEX;
                    ObPhyOperator * sub_operator = NULL;
                    if((ret = table_mem_scan_op->get_sub_query_index(sub_query_idx,index))!=OB_SUCCESS)
                    {
                        TRANS_LOG("wrong get sub query index");
                    }
                    else if(NULL == (sub_operator = physical_plan->get_phy_query(index)))
                    {
                        ret = OB_INVALID_INDEX;
                        TRANS_LOG("wrong get sub query operator");
                    }
                    else if(OB_SUCCESS != (ret = multi_bind_op->set_child(sub_query_idx, *sub_operator)))
                    {
                        TRANS_LOG("fail to set child:ret[%d]",ret);
                    }

                }
            }
        }
        //add:e
        //set multi_bind_op as top opertator
        if (ret == OB_SUCCESS)
            table_op = multi_bind_op;
    }



    //add:e
    return ret;
}

//add fanqiushi_index
//add fanqiushi_index
int ObTransformer::gen_phy_table_without_storing(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObStmt *stmt,
        uint64_t table_id,
        ObPhyOperator*& table_op,
        bool* group_agg_pushed_down,
        bool* limit_pushed_down ,
        uint64_t index_tid_without_storing,
        Expr_Array * filter_array,
        Expr_Array * project_array,
        Join_column_Array *join_column//add by wanglei [semi join second index] 20151231
        )
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    TableItem* table_item = NULL;
    ObSqlReadStrategy sql_read_strategy;
    int64_t num = 0;
    ObRpcScanHint hint;
    ObTableScan *table_scan_op = NULL;


    if (table_id == OB_INVALID_ID || (table_item = stmt->get_table_item_by_id(table_id)) == NULL)
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("Wrong table id");
    }
    //add huangcc [fix transaction read uncommit bug]2016/11/21
    if (OB_SUCCESS == ret)
    {
        hint.read_consistency_ = stmt->get_query_hint().read_consistency_;
    }
    //add end
    if (ret == OB_SUCCESS)
    {
        // TBSYS_LOG(ERROR,"test::fanqs,,,table_item->type_=%d",table_item->type_);
        switch (table_item->type_)
        {
        case TableItem::BASE_TABLE:
            /* get through */
        case TableItem::ALIAS_TABLE:
        {
            ObTableRpcScan *table_rpc_scan_op = NULL;
            CREATE_PHY_OPERRATOR(table_rpc_scan_op, ObTableRpcScan, physical_plan, err_stat);
            if (ret == OB_SUCCESS
                    && (ret = table_rpc_scan_op->set_table(table_item->table_id_, index_tid_without_storing)) != OB_SUCCESS)
            {
                TRANS_LOG("ObTableRpcScan set table faild");
            }

            //mod dragon [Nullptr] 2016-9-22b
            /*---new---*/
            if(OB_SUCCESS == ret)
            {
              table_rpc_scan_op->set_main_tid(table_item->ref_id_);
              table_rpc_scan_op->set_is_use_index_without_storing();
              table_rpc_scan_op->set_is_index_without_storing(true,index_tid_without_storing);
            }
            /*---old
            table_rpc_scan_op->set_main_tid(table_item->ref_id_);
            table_rpc_scan_op->set_is_use_index_without_storing();
            table_rpc_scan_op->set_is_index_without_storing(true,index_tid_without_storing);
            ---old---*/
            //mod 2016-9-22e

            const ObTableSchema *main_table_schema = NULL;
            if (NULL == (main_table_schema = sql_context_->schema_manager_->get_table_schema(table_item->ref_id_)))
            {
                ret = OB_ERROR;
                TRANS_LOG("Fail to get table schema for table[%ld]", index_tid_without_storing);
            }
            else
            {
                table_rpc_scan_op->set_main_rowkey_info(main_table_schema->get_rowkey_info());
            }

            /// determin request type: scan/get
            if (OB_SUCCESS == ret)
            {
                const ObTableSchema *table_schema = NULL;
                if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(index_tid_without_storing)))
                {
                    ret = OB_ERROR;
                    TRANS_LOG("Fail to get table schema for table[%ld]", index_tid_without_storing);
                }
                else
                {
                    sql_read_strategy.set_rowkey_info(table_schema->get_rowkey_info());
                    if ((ret = physical_plan->add_base_table_version(
                             index_tid_without_storing,
                             table_schema->get_schema_version()
                             )) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", index_tid_without_storing, ret);
                    }
                }
            }
            if ( OB_SUCCESS == ret)
            {
                int32_t read_method = ObSqlReadStrategy::USE_SCAN;
                hint.read_method_ = read_method;
            }

            if (ret == OB_SUCCESS)
            {
                ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
                if (select_stmt
                        && select_stmt->get_group_expr_size() <= 0
                        && select_stmt->get_having_expr_size() <= 0
                        && select_stmt->get_order_item_size() <= 0
                        && hint.read_method_ != ObSqlReadStrategy::USE_GET)
                {
                    hint.max_parallel_count = 1;
                }
                if ((ret = table_rpc_scan_op->init(sql_context_, &hint)) != OB_SUCCESS)
                {
                    TRANS_LOG("ObTableRpcScan init faild");
                }
            }
            if (ret == OB_SUCCESS)
                table_scan_op = table_rpc_scan_op;
            break;
        }
        default:
            // won't be here
            OB_ASSERT(0);
            break;
        }
    }

    if(OB_SUCCESS == ret && filter_array != NULL && project_array != NULL)
    {
        num = filter_array->count();
        //add by wanglei [semi join] 20151231
        //add wanglei:如果num为零，说明很有可能是因为join的on子句中所涉及的列在表的索引表中，
        //这种情况下，filter的构造会在semi join的open阶段进行，因此，此时以下的操作都会失效
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            ObSqlExpression *filter = ObSqlExpression::alloc();
            if(filter == NULL)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR,"[semi join] there is no memory left!");
            }
            else
            {
                *filter=filter_array->at(i);
                //TBSYS_LOG(ERROR,"test::fanqs,,filter=%s,,filter->get_sub_query_num()=%ld",to_cstring(*filter),filter->get_sub_query_num());
                if((ret = table_scan_op->add_main_filter(filter)) != OB_SUCCESS)
                {
                    ObSqlExpression::free (filter);
                    TRANS_LOG("Add table filter condition faild");
                    break;
                }

                bool need_free = true;//add liumz, [bugfix: index memory overflow]20170105
                //TBSYS_LOG(ERROR,"test::fanqs,,cid=%ld,tid=%ld,filter=%s",filter->get_column_id(),filter->get_table_id(),to_cstring(*filter));
                uint64_t f_cid=OB_INVALID_ID;
                if(!filter->is_expr_has_more_than_two_columns ())//add wanglei [second index fix] 20160425
                {
                    if(OB_SUCCESS == filter->get_cid(f_cid))
                    {
                        int64_t bool_result=is_cid_in_index_table(f_cid,index_tid_without_storing);
                        //TBSYS_LOG(ERROR,"test::fanqs,,bool_result=%ld",bool_result);
                        if(bool_result!=0)
                        {
                            ObPostfixExpression& ops=filter->get_decoded_expression_v2();
                            uint64_t index_of_expr_array = OB_INVALID_ID;
                            if(OB_SUCCESS!=(ret=filter->change_tid(index_of_expr_array)))
                            {
                                TBSYS_LOG(ERROR,"faild to change tid,filter=%s",to_cstring(*filter));

                            }
                            ObObj& obj=ops.get_expr_by_index(index_of_expr_array);
                            if(obj.get_type()==ObIntType)
                                obj.set_int(index_tid_without_storing);
                            if((ret = table_scan_op->add_filter(filter)) != OB_SUCCESS)
                            {
                                ObSqlExpression::free (filter);
                                TRANS_LOG("Add table filter condition faild");
                                break;
                            }
                            else if((ret = table_scan_op->add_index_filter_ll(filter)) != OB_SUCCESS)// add wanglei [semi join] 20160108
                            {
                                ObSqlExpression::free (filter);
                                TRANS_LOG("Add table filter condition faild");
                                break;
                            }
                            else
                            {
                              need_free = false;
                            }
                        }
                    }
                    else
                    {
                        TBSYS_LOG(WARN,"filter get cid failed, if sql has 1 = 1 filter, it dosent matter");
                    }
                }
                //add liumz, [bugfix: index memory overflow]20170105:b
                if(need_free && NULL != filter)
                {
                    ObSqlExpression::free(filter);
                }
                //add:e
            }
        }

        // add output columns
        num = project_array->count();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            ObSqlExpression output_expr=project_array->at(i);
            //TBSYS_LOG(ERROR,"test::fanqs,,output_expr=%s",to_cstring(output_expr));
            if((ret = table_scan_op->add_main_output_column(output_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add table output columns faild");
                break;
            }


        }

        if(OB_SUCCESS == ret)
        {
            ObRowDesc row_desc;
            if(OB_SUCCESS!=(ret=table_scan_op->cons_second_row_desc(row_desc)))
            {
                TBSYS_LOG(WARN,"faild to cons_second_row_desc,ret=%d",ret);
            }
            else if(OB_SUCCESS!=(ret=table_scan_op->set_second_row_desc(&row_desc)))
            {
                TBSYS_LOG(WARN,"faild to set_second_row_desc,ret=%d",ret);
            }
        }
        if(OB_SUCCESS == ret)
        {
            const ObTableSchema *index_table_schema = NULL;
            if (NULL == (index_table_schema = sql_context_->schema_manager_->get_table_schema(index_tid_without_storing)))
            {
                ret = OB_ERROR;
                TRANS_LOG("Fail to get table schema for table[%ld]", index_tid_without_storing);
            }
            else
            {
                uint64_t cid=OB_INVALID_ID;
                int64_t rowkey_column=index_table_schema->get_rowkey_info().get_size();
                for(int64_t j=0;j<rowkey_column;j++)
                {
                    if(OB_SUCCESS!=(ret=index_table_schema->get_rowkey_info().get_column_id(j,cid)))
                    {
                        TBSYS_LOG(ERROR,"get column schema failed,cid[%ld]",cid);
                        ret=OB_SCHEMA_ERROR;
                    }
                    else
                    {
                        ObBinaryRefRawExpr col_expr(index_tid_without_storing, cid, T_REF_COLUMN);
                        ObSqlRawExpr col_raw_expr(
                                    common::OB_INVALID_ID,
                                    index_tid_without_storing,
                                    cid,
                                    &col_expr);
                        ObSqlExpression output_expr;
                        if ((ret = col_raw_expr.fill_sql_expression(
                                 output_expr,
                                 this,
                                 logical_plan,
                                 physical_plan)) != OB_SUCCESS)

                        {
                            TRANS_LOG("Add table output columns faild");
                            break;
                        }
                        else if((ret = table_scan_op->add_output_column(output_expr)) != OB_SUCCESS )
                        {
                            TRANS_LOG("Add table output columns faild");
                            break;
                        }
                        else if((ret = table_scan_op->add_index_output_column_ll(output_expr))!= OB_SUCCESS)// add wanglei [semi join] 20160108
                        {
                            TRANS_LOG("Add table output columns faild");
                            break;
                        }
                    }
                }

            }

        }
        //add wanglei [semi join second index] 20160107 :b
        //将on表达式中的列加入到输出列
        if(join_column == NULL)
        {
            ret = OB_ERR_POINTER_IS_NULL;
            TBSYS_LOG(WARN,"on expression join column array is null!");
        }
        else
        {
            for(int l=0;l<join_column->count();l++)
            {
                uint64_t tmp_cid = join_column->at(l);
                int64_t bool_result = is_cid_in_index_table(tmp_cid,index_tid_without_storing);
                if(bool_result == 2)
                {
                    ObBinaryRefRawExpr col_expr(index_tid_without_storing, tmp_cid, T_REF_COLUMN);
                    ObSqlRawExpr col_raw_expr(
                                common::OB_INVALID_ID,
                                index_tid_without_storing,
                                tmp_cid,
                                &col_expr);
                    ObSqlExpression output_expr;
                    if ((ret = col_raw_expr.fill_sql_expression(
                             output_expr,
                             this,
                             logical_plan,
                             physical_plan)) != OB_SUCCESS)

                    {
                        TRANS_LOG("Add table output columns faild");
                        break;
                    }
                    else if((ret = table_scan_op->add_output_column(output_expr)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add table output columns faild");
                        break;
                    }
                }
            }
        }
        //add:e
        ObSqlExpression *filter = NULL;//add liumz, [bugfix: index memory overflow]20170105
        num = filter_array->count();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            //mod liumz, [bugfix: index memory overflow]20170105:b
            //ObSqlExpression *filter = ObSqlExpression::alloc();
            if(NULL != filter)
            {
                ObSqlExpression::free(filter);
            }
            filter = ObSqlExpression::alloc();
            if (NULL == filter)
            {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              TBSYS_LOG(ERROR, "no memory");
              break;
            }
            //mod:e
            *filter=filter_array->at(i);
            //TBSYS_LOG(ERROR,"wanglei::filter:%s",to_cstring(*filter));
            uint64_t cid=OB_INVALID_ID;
            if(!filter->is_expr_has_more_than_two_columns ())//add wanglei [second index fix] 20160425
            {
                if(OB_SUCCESS==filter->get_cid(cid))
                {
                    int64_t bool_result=is_cid_in_index_table(cid,index_tid_without_storing);
                    //TBSYS_LOG(ERROR,"wanglei::,,bool_result=%ld",bool_result);
                    if(bool_result==2)
                    {
                        ObBinaryRefRawExpr col_expr(index_tid_without_storing, cid, T_REF_COLUMN);
                        ObSqlRawExpr col_raw_expr(
                                    common::OB_INVALID_ID,
                                    index_tid_without_storing,
                                    cid,
                                    &col_expr);
                        ObSqlExpression output_expr;
                        if ((ret = col_raw_expr.fill_sql_expression(
                                 output_expr,
                                 this,
                                 logical_plan,
                                 physical_plan)) != OB_SUCCESS)

                        {
                            TRANS_LOG("Add table output columns faild");
                            break;
                        }
                        else if((ret = table_scan_op->add_output_column(output_expr)) != OB_SUCCESS )
                        {
                            TRANS_LOG("Add table output columns faild");
                            break;
                        }
                        else if((ret = table_scan_op->add_index_output_column_ll(output_expr))!= OB_SUCCESS)// add wanglei [semi join] 20160108
                        {
                            TRANS_LOG("Add table output columns faild");
                            break;
                        }
                    }
                    else if(bool_result==0)
                    {
                        if((ret=table_scan_op->add_index_filter(filter))!=OB_SUCCESS )
                        {
                            TBSYS_LOG(WARN,"faild to add index_filter,ret=%d",ret);
                        }
                    }
                }
                else
                {
                    TBSYS_LOG(WARN,"filter get cid failed, if sql has 1 = 1 filter, it dose not matter");
                }
            }
            else
            {
                if((ret=table_scan_op->add_index_filter(filter))!=OB_SUCCESS )
                {
                    TBSYS_LOG(WARN,"faild to add index_filter,ret=%d",ret);
                }
            }            
        }
        //mod liumz, [bugfix: index memory overflow]20170105:b
        if(NULL != filter)
        {
            ObSqlExpression::free(filter);
        }
        //add:e

    }

    if (ret == OB_SUCCESS)
        table_op = table_scan_op;
    *group_agg_pushed_down = false;
    //*limit_pushed_down = false;

    //add liumz, [bugfix_limit_push_down]20160822:b
    bool limit_down = false;
    ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
    if (ret == OB_SUCCESS && select_stmt)
    {
        ret = try_push_down_limit(
                    logical_plan,
                    physical_plan,
                    err_stat,
                    select_stmt,
                    limit_down,
                    table_op);
        if (limit_pushed_down)
            *limit_pushed_down = limit_down;
    }
    //add:e

    return ret;
}



bool ObTransformer::handle_index_for_one_table(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObStmt *stmt,
        uint64_t table_id,
        ObPhyOperator*& table_op,
        ObPhyOperator *sequence_op,//add liumz, [index_sequence_filter]20170704
        bool* group_agg_pushed_down,
        bool* limit_pushed_down,
        bool outer_join_scope//add liumz, [outer_join_on_where]20150927
        )
{

    Expr_Array filter_array;
    common::ObArray<ObSqlExpression*>  fp_array; //add wenghaixing 20150909 fix memory overflow bug for se_index    
    Expr_Array project_array;
	//add liumz, [optimize group_order by index]20170419:b
    Expr_Array order_array;
    Expr_Array group_array;
	//add:e
    Join_column_Array join_column;//add wanglei [semi join] 20160106
    ObArray<uint64_t> alias_exprs;
    bool not_use_index = false;//add zhuyanchao secondary index20150708
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    bool return_ret=false;
    //add BUG
    bool is_gen_table = false;
    //add:e
    TableItem* table_item = NULL;
    ObBitSet<> table_bitset;
    int32_t num = 0;
    UNUSED(group_agg_pushed_down);
    UNUSED(limit_pushed_down);

    if(NULL == stmt)
    {
        TBSYS_LOG(ERROR,"enter this  stmt=NULL");
    }
    else
    {
        table_item = stmt->get_table_item_by_id(table_id);
        not_use_index = stmt->get_query_hint().not_use_index_;
    }
    if(table_item!=NULL)
    {
        if(table_item->type_!=TableItem::BASE_TABLE&&table_item->type_!=TableItem::ALIAS_TABLE)
        {
            ret = OB_NOT_SUPPORTED;
            //add BUG
            is_gen_table = true;
            //add:e
            TBSYS_LOG(DEBUG," not support this type, table_item->type_=%d",table_item->type_);
        }
    }
    else
    {
        TBSYS_LOG(WARN,"  table_item=NULL");
        ret = OB_NOT_SUPPORTED;
    }
    //TBSYS_LOG(ERROR,"enter this2,table_id=%ld",table_id);

    //add liumz, [range_index_bug]20170511:b
    ObSelectStmt *sel_stmt = NULL;
    if (NULL != stmt)
    {
      sel_stmt = dynamic_cast<ObSelectStmt*>(stmt);
    }

    if (NULL != sel_stmt && sel_stmt->get_has_range())
    {
      return_ret = false;
    }
    //add:e
    else if(not_use_index)
    {
        return_ret = false;
    }
    else
    {
        //add liumz, [index_sequence_filter]20170704:b
        ObSequenceSelect *sequence_select_op = NULL;
        if (sequence_op != NULL)
        {
            sequence_select_op = dynamic_cast<ObSequenceSelect *>(sequence_op);
        }
        if (NULL != sequence_select_op)
        {
            ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
            sequence_select_op->reset_sequence_names_idx(select_stmt->get_column_has_sequene_count());
        }
        //add:e
        if(OB_SUCCESS == ret)    //很据table_bitset，把sql语句中与该表有关的filter和输出列都存到相应的数组里面
        {
            int32_t bit_index = stmt->get_table_bit_index(table_item->table_id_);
            table_bitset.add_member(bit_index);

            if (bit_index < 0)
            {
              TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,table_item->table_id_);
            }
            //add filter
            num = stmt->get_condition_size();
            for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
            {
                ObSqlRawExpr *cnd_expr = logical_plan->get_expr(stmt->get_condition_id(i));
                if (cnd_expr && table_bitset.is_superset(cnd_expr->get_tables_set()))
                {
                    if (!outer_join_scope)//add liumz, [outer_join_on_where]20150927
                    {
                        cnd_expr->set_applied(true);
                    }

                    //add duyr [join_without_pushdown_is_null] 20151214:b
                    if (outer_join_scope && !cnd_expr->can_push_down_with_outerjoin())
                    {
                        continue;
                    }
                    //add duyr 20151214:e

                    ObSqlExpression *filter = ObSqlExpression::alloc();
                    if (NULL == filter)
                    {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        TRANS_LOG("no memory");
                        break;
                    }
                    else if ((ret = cnd_expr->fill_sql_expression(*filter, this, logical_plan, physical_plan)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add table filter condition faild");
                        ObSqlExpression::free(filter);
                        break;
                    }
                    else
                    {
                        //add liumz, [index_sequence_filter]20170704:b
                        if (OB_SUCCESS == ret && NULL != sequence_select_op)
                        {
                            if(sequence_select_op->can_fill_sequence_info())
                            {
                                uint64_t temp_id = stmt->get_condition_id(i);
                                if (sequence_select_op->is_sequence_cond_id(temp_id))
                                {
                                    if (OB_SUCCESS != (ret = sequence_select_op->fill_the_sequence_info_to_cond_expr(filter, OB_INVALID_ID)))
                                    {
                                        TRANS_LOG("Failed deal the sequence condition filter,err=%d",ret);
                                        break;
                                    }
                                }
                            }
                        }
                        //add:e
                        if(OB_SUCCESS != (ret = filter_array.push_back(*filter)))
                        {
                            TRANS_LOG("push back to filter array failed");
                            ObSqlExpression::free(filter);
                            break;
                        }
                        else if(OB_SUCCESS != (ret = fp_array.push_back(filter)))
                        {
                            ObSqlExpression::free(filter);
                            TRANS_LOG("push back to filter array ptr failed");
                            break;
                        }
                    }
                }
            }

            //add by wanlei [semi join]:b
            //add join cond filter
            //获取select语句中的from后面的表的顺序：b
            ObVector<TableItem> table_item_v;
            for(int i = 0; i < stmt->get_table_size(); i++)
            {
              TableItem tmp;
              tmp = stmt->get_table_item(i);
              table_item_v.push_back(tmp);
            }
            //获取select语句中的from后面的表的顺序：e
            for(int i = 0; i < logical_plan->get_expr_list_num(); i++)
            {
              bool is_same_order = false;
              ExprItem::SqlCellInfo c1;
              ExprItem::SqlCellInfo c2;
              ObSqlRawExpr * ob_sql_raw_expr = logical_plan->get_expr_for_something(i);
              if(NULL == ob_sql_raw_expr)
              {
                ret = OB_ERR_POINTER_IS_NULL;
                TBSYS_LOG(WARN, "logical plan expression is null!");
              }
              else
              {
                ObSqlExpression *cond_expr= ObSqlExpression::alloc();
                if(cond_expr == NULL)
                {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  TBSYS_LOG(WARN, "no memory");
                }
                else if(ob_sql_raw_expr->get_expr()->is_join_cond())
                {
                  if(OB_SUCCESS != (ret = ob_sql_raw_expr->fill_sql_expression(
                                      *cond_expr,
                                      this,
                                      logical_plan,
                                      physical_plan)))
                  {
                      TBSYS_LOG(WARN, "get equijoin_cond faild! ret[%d]", ret);
                  }
                  else
                  {
                    cond_expr->is_equijoin_cond(c1, c2);

                    //注意，这里的等值表达式左右两表的顺序与from中表的顺序可能是反的
                    //判断顺序是否相同：b
                    for(int i = 0; i < table_item_v.size(); i++)
                    {
                      uint64_t tmp_tid = table_item_v.at(i).table_id_;
                      //TBSYS_LOG(DEBUG,"wanglei:test c1.tid = [%ld], c2.tid = [%ld], table ref_id=[%ld],table id=[%ld]",
                      //          c1.tid, c2.tid,table_item_v.at(i).ref_id_,table_item_v.at(i).table_id_);
                      if(c1.tid == tmp_tid)
                      {
                        i++;
                        for(; i < table_item_v.size(); i++)
                        {
                          tmp_tid = table_item_v.at(i).table_id_;
                          if(c2.tid == tmp_tid)
                          {
                            is_same_order = true;
                            break;
                          }
                        }
                      }
                    }
                  }
                  //只将右表的cid放入列表，左表不予考虑
                  //这里有别名问题,修改成让表达式中的table id与上面传过来的没经过处理的table id进行比较，
                  //这样即使表达式中的table id是别名也能与上层串过来的匹配上
                  if(is_same_order)
                  {
                    if(c2.tid == table_id)
                    {
                      join_column.push_back(c2.cid);
                    }
                  }
                  else
                  {
                    if(c1.tid == table_id)
                    {
                      join_column.push_back(c1.cid);
                    }
                  }
                }
                if(cond_expr != NULL)
                {
                    ObSqlExpression::free(cond_expr);
                    cond_expr = NULL;
                }
              }
            }
            //add:e

            // add output columns
            num = stmt->get_column_size();
            for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
            {
                const ColumnItem *col_item = stmt->get_column_item(i);
                if (col_item && col_item->table_id_ == table_item->table_id_)
                {
                    ObBinaryRefRawExpr col_expr(col_item->table_id_, col_item->column_id_, T_REF_COLUMN);
                    ObSqlRawExpr col_raw_expr(
                                common::OB_INVALID_ID,
                                col_item->table_id_,
                                col_item->column_id_,
                                &col_expr);
                    ObSqlExpression output_expr;
                    if ((ret = col_raw_expr.fill_sql_expression(
                             output_expr,
                             this,
                             logical_plan,
                             physical_plan)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add table output columns faild");
                        break;
                    }
                    else
                    {
                        project_array.push_back(output_expr);
                    }
                    //add fanqiushi_index
                    //TBSYS_LOG(ERROR,"test::fanqs,,,output_expr=%s",to_cstring(output_expr));
                    //add:e
                }
            }
            ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
            if (ret == OB_SUCCESS && select_stmt)
            {
                num = select_stmt->get_select_item_size();
                for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
                {
                    const SelectItem& select_item = select_stmt->get_select_item(i);
                    if (select_item.is_real_alias_)
                    {
                        ObSqlRawExpr *alias_expr = logical_plan->get_expr(select_item.expr_id_);
                        if (alias_expr && alias_expr->is_columnlized() == false
                                && table_bitset.is_superset(alias_expr->get_tables_set()))
                        {
                            ObSqlExpression output_expr;
                            if ((ret = alias_expr->fill_sql_expression(
                                     output_expr,
                                     this,
                                     logical_plan,
                                     physical_plan)) != OB_SUCCESS)
                            {
                                TRANS_LOG("Add table output columns faild");
                                break;
                            }
                            else
                            {
                                project_array.push_back(output_expr);
                            }
                            //add fanqiushi_index
                            // TBSYS_LOG(ERROR,"test::fanqs,,,output_expr=%s",to_cstring(output_expr));
                            //add:e
                            alias_exprs.push_back(select_item.expr_id_);
                            alias_expr->set_columnlized(true);
                        }
                    }
                }
                //add liumz, [optimize group_order by index]20170419:b
                num = select_stmt->get_order_item_size();
                for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
                {
                  const OrderItem& order_item = select_stmt->get_order_item(i);
                  ObSqlRawExpr *order_expr = logical_plan->get_expr(order_item.expr_id_);
                  //put all order items into order_array without concerning table_bitset
                  if (order_expr)
                  {
                    ObSqlExpression output_expr;
                    if ((ret = order_expr->fill_sql_expression(
                             output_expr,
                             this,
                             logical_plan,
                             physical_plan)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add table output columns faild");
                        break;
                    }
                    else
                    {
                        order_array.push_back(output_expr);
                    }
                  }
                }//end for

                num = select_stmt->get_group_expr_size();
                for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
                {
                  uint64_t expr_id = select_stmt->get_group_expr_id(i);
                  ObSqlRawExpr* group_expr = logical_plan->get_expr(expr_id);
                  //put all group expr into order_array without concerning table_bitset
                  if (group_expr)
                  {
                    ObSqlExpression output_expr;
                    if ((ret = group_expr->fill_sql_expression(
                             output_expr,
                             this,
                             logical_plan,
                             physical_plan)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add table output columns faild");
                        break;
                    }
                    else
                    {
                        group_array.push_back(output_expr);
                    }
                  }
                }//end for
                //add:e
            }
        }

        if(OB_SUCCESS == ret)
        {
            bool is_use_hint=false;    //判断是否使用用户输入的hint
            uint64_t hint_tid=OB_INVALID_ID;     //用户输入的hint中的索引表的tid
            bool use_hint_for_storing=false;     //判断hint中的索引表能否使用不回表的索引
            bool use_hint_without_storing=false;  //判断hint中的索引表能否使用回表的索引

            bool is_use_storing_column=false;   //最终的: 判断是否使用不回表的索引的变量 //repaired from messy code by zhuxh 20151014
            bool is_use_index_without_storing=false;  //最终的: 判断是否使用回表的索引的变量
            uint64_t index_id=OB_INVALID_ID;       //最终的: 如果用不回表的索引，索引表的tid
            uint64_t index_id_without_storing=OB_INVALID_ID;  //最终的: 如果用回表的索引，索引表的tid

            if(stmt->get_query_hint().has_index_hint())
            {
                for(int i=0;i<stmt->get_query_hint().use_index_array_.size();i++)//add by wanglei [semi join] 20151231 for many index hint
                {
                    ObIndexTableNamePair tmp=stmt->get_query_hint().use_index_array_.at(i);
                    //ObIndexTableNamePair tmp=stmt->get_query_hint().use_index_array_.at(0);

                    hint_tid=tmp.index_table_id_;
                    //TBSYS_LOG(ERROR,"test::fanqs,,tmp.src_table_id_=%ld,,table_item->ref_id_=%ld",tmp.src_table_id_,table_item->ref_id_);
                    if(tmp.src_table_id_ == table_item->ref_id_)
                    {
                        is_use_hint=true;
                        use_hint_for_storing = is_can_use_hint_for_storing_V2(&filter_array,&project_array,tmp.index_table_id_,&join_column,stmt);//add wanglei [semi join ] &join_column,stmt //判断hint中的索引表能否使用不回表的索引的函数
                        //TBSYS_LOG(ERROR,"test::fanqs,,use_hint_for_storing=%d",use_hint_for_storing);
                        if(!use_hint_for_storing)
                            use_hint_without_storing=is_can_use_hint_index_V2(&filter_array,tmp.index_table_id_,&join_column,stmt);//add wanglei [semi join ] &join_column,stmt // 判断hint中的索引表能否使用回表的索引的函数
                        break;
                    }

                }
                if(use_hint_for_storing==false&&use_hint_without_storing==false)
                {
                    is_use_hint=false;
                }
            }
            if(!is_use_hint)      //如果没有hint
            {
                is_use_storing_column=decide_is_use_storing_or_not_V2(&filter_array,&project_array,index_id,table_item->ref_id_,&join_column,stmt,logical_plan,&order_array,&group_array);   //add wanglei [semi join ] &join_column,stmt //如果用户没有输入hint，根据简单的规则判断是否能够使用不回表的索引
                if(is_use_storing_column==false)  //如果不能使用不回表的索引，再判断是否能使用回表的索引
                {
                    //TBSYS_LOG(ERROR,"test::fanqs,,enter this 991,from_item.table_id_=%lu",from_item.table_id_);
                    const ObTableSchema *mian_table_schema = NULL;
                    if (NULL == (mian_table_schema = sql_context_->schema_manager_->get_table_schema(table_item->ref_id_)))
                    {
                        TBSYS_LOG(WARN,"Fail to get table schema for table[%ld]",table_item->ref_id_);
                    }
                    else
                    {
                        const ObRowkeyInfo *rowkey_info = &mian_table_schema->get_rowkey_info();
                        uint64_t main_cid=OB_INVALID_ID;
                        rowkey_info->get_column_id(0,main_cid);
                        if(!is_wherecondition_have_main_cid_V2(&filter_array,main_cid))
                        {
                            //add by wanglei [semi join] 20151231:b
                            //判断右表是否有可用的索引,如果hint中有semi join才执行
                             bool is_semi_join =false;
                            if(stmt ==NULL)
                            {
                                ret = OB_ERR_POINTER_IS_NULL;
                                TBSYS_LOG(WARN,"[semi join] stmt is null!");
                            }
                            else  if(stmt->get_query_hint().join_array_.size()>0)
                            {
                                ObJoinTypeArray tmp_join_type = stmt->get_query_hint().join_array_.at(0);
                                if(tmp_join_type.join_type_ == T_SEMI_JOIN ||tmp_join_type.join_type_ == T_SEMI_BTW_JOIN)
                                {
                                    //mod dragon [SEMI JOIN] 2016-10-21
                                    //原来的流程左表不能正常判断是否使用不回表的索引
                                    if(0 != join_column.count())
                                    {
                                      is_semi_join = true;
                                    }
                                    //below is original code
                                    //is_semi_join = true;
                                    //mod e
                                    for(int l=0;l<join_column.count();l++)
                                    {
                                        uint64_t tmp_cid = join_column.at(l);
                                        if(is_this_expr_can_use_index_for_join(tmp_cid,index_id_without_storing,table_item->ref_id_,sql_context_->schema_manager_))
                                        {
                                            //TBSYS_LOG(WARN,"wl is_this_expr_can_use_index_for_join");
                                            is_use_index_without_storing=true;
                                            break;
                                        }
                                    }
                                }
                            }
                            //add:e
                            int64_t c_num=filter_array.count();
                            if(is_semi_join /*&& is_use_index_without_storing*/) //如果在semi join时判断了使用索引，那么就不会再判断where中是否有符合条件的索引
                            {
                                //这说明这个索引是专门用于semi join的过滤使用的，在其他join情况下，如果where中没有这张表的过滤条件
                                //是会报错的。
                            }
                            else
                            {
                                //add liumz, [optimize group_order by index]20170419:b
                                bool index_group = false;
                                bool index_order = false;
                                uint64_t index_group_table_id = OB_INVALID_ID;
                                uint64_t index_order_table_id = OB_INVALID_ID;
                                ObArray<uint64_t> idx_tids;
                                //add:e
                                for(int32_t j=0;j<c_num;j++)
                                {
                                    ObSqlExpression c_filter=filter_array.at(j);
                                    if(c_filter.is_this_expr_can_use_index(index_id_without_storing,table_item->ref_id_,sql_context_->schema_manager_))
                                    {
                                        //mod liumz, [optimize group_order by index]20170419:b
                                        idx_tids.push_back(index_id_without_storing);
                                        is_use_index_without_storing=true;
                                        //break;
                                        //mod:e
                                    }
                                }
                                //add liumz, [optimize group_order by index]20170419:b
                                if (is_use_index_without_storing && idx_tids.count() > 0)
                                {
                                  index_id_without_storing = idx_tids.at(0);//pick the first index table, default
                                  index_group_table_id = idx_tids.at(0);//pick the first index table, default
                                  index_order_table_id = idx_tids.at(0);//pick the first index table, default
                                  if (group_array.count() > 0 && OB_SUCCESS != (ret = optimize_group_by_index(idx_tids, table_item->ref_id_, index_group_table_id, index_group, stmt, logical_plan)))
                                  {
                                    TBSYS_LOG(WARN, "optimize_order_by_index failed, ret = %d", ret);
                                  }
                                  if (OB_SUCCESS == ret && order_array.count() > 0)
                                  {
                                    if (index_group)
                                    {
                                      idx_tids.clear();
                                      idx_tids.push_back(index_group_table_id);
                                    }
                                    if ((index_group || 0 == group_array.count())
                                        && OB_SUCCESS != (ret = optimize_order_by_index(idx_tids, table_item->ref_id_, index_order_table_id, index_order, stmt, logical_plan)))
                                    {
                                      TBSYS_LOG(WARN, "optimize_order_by_index failed, ret = %d", ret);
                                    }
                                  }
                                }
                                else
                                {
                                  if (group_array.count() > 0)
                                  {
                                    idx_tids.clear();
                                    ObSqlExpression c_filter = group_array.at(0);
                                    if(c_filter.is_this_expr_can_use_index(index_id_without_storing,table_item->ref_id_,sql_context_->schema_manager_))
                                    {
                                        idx_tids.push_back(index_id_without_storing);
                                        if (OB_SUCCESS != (ret = optimize_group_by_index(idx_tids, table_item->ref_id_, index_group_table_id, index_group, stmt, logical_plan)))
                                        {
                                          TBSYS_LOG(WARN, "optimize_order_by_index failed, ret = %d", ret);
                                        }
                                    }
                                  }
                                  if (OB_SUCCESS == ret && order_array.count() > 0)
                                  {

                                    idx_tids.clear();
                                    if (index_group)
                                    {
                                      idx_tids.push_back(index_group_table_id);
                                      if (OB_SUCCESS != (ret = optimize_order_by_index(idx_tids, table_item->ref_id_, index_order_table_id, index_order, stmt, logical_plan)))
                                      {
                                        TBSYS_LOG(WARN, "optimize_order_by_index failed, ret = %d", ret);
                                      }
                                    }
                                    else if (0 == group_array.count())
                                    {
                                      ObSqlExpression c_filter = order_array.at(0);
                                      if(c_filter.is_this_expr_can_use_index(index_id_without_storing,table_item->ref_id_,sql_context_->schema_manager_))
                                      {
                                        idx_tids.push_back(index_id_without_storing);
                                        if (OB_SUCCESS != (ret = optimize_order_by_index(idx_tids, table_item->ref_id_, index_order_table_id, index_order, stmt, logical_plan)))
                                        {
                                          TBSYS_LOG(WARN, "optimize_order_by_index failed, ret = %d", ret);
                                        }
                                      }
                                    }
                                  }
                                  if (OB_SUCCESS == ret && !index_group && !index_order)
                                  {
                                    uint64_t unused_idx_tid = OB_INVALID_ID;
                                    bool unused_ret = false;
                                    idx_tids.clear();
                                    idx_tids.push_back(table_item->ref_id_);
                                    if (group_array.count() > 0 && OB_SUCCESS != (ret = optimize_group_by_index(idx_tids, table_item->ref_id_, unused_idx_tid, unused_ret, stmt, logical_plan)))
                                    {
                                      TBSYS_LOG(WARN, "optimize_group_by_index failed, ret = %d", ret);
                                    }
                                    if (OB_SUCCESS == ret && (unused_ret || 0 == group_array.count()) && order_array.count() > 0
                                        && OB_SUCCESS != (ret = optimize_order_by_index(idx_tids, table_item->ref_id_, unused_idx_tid, unused_ret, stmt, logical_plan)))
                                    {
                                      TBSYS_LOG(WARN, "optimize_order_by_index failed, ret = %d", ret);
                                    }
                                  }
                                }
                                //set return_ret & index_table_id
                                if (index_group)
                                {
                                  is_use_index_without_storing = index_group;
                                  index_id_without_storing = index_group_table_id;
                                }
                                else if (index_order)
                                {
                                  is_use_index_without_storing = index_order;
                                  index_id_without_storing = index_order_table_id;
                                }
                                //add liumz, [optimize group_order by index]20170419:e
                            }
                        }
                        //add by wanglei [semi join] 20151231:b
                        else
                        {
                            if(stmt ==NULL)
                            {
                                ret = OB_ERR_POINTER_IS_NULL;
                                TBSYS_LOG(WARN,"[semi join] stmt is null!");
                            }
                            else if(stmt->get_query_hint().join_array_.size()>0)
                            {
                                ObJoinTypeArray tmp_join_type = stmt->get_query_hint().join_array_.at(0);
                                if(tmp_join_type.join_type_ == T_SEMI_JOIN ||tmp_join_type.join_type_ == T_SEMI_BTW_JOIN)
                                {
                                    for(int l=0;l<join_column.count();l++)
                                    {
                                        uint64_t tmp_cid = join_column.at(l);
                                        if(is_this_expr_can_use_index_for_join(tmp_cid,index_id_without_storing,table_item->ref_id_,sql_context_->schema_manager_))
                                        {
                                            //TBSYS_LOG(WARN,"wl is_this_expr_can_use_index_for_join");
                                            is_use_index_without_storing=true;
                                        }
                                    }
                                }
                            }
                            //add liumz, [optimize group_order by index]20170419:b
                            if (!is_use_index_without_storing)
                            {
                              uint64_t unused_idx_tid = OB_INVALID_ID;
                              bool unused_ret = false;
                              ObArray<uint64_t> idx_tids;
                              idx_tids.push_back(table_item->ref_id_);
                              if (group_array.count() > 0 && OB_SUCCESS != (ret = optimize_group_by_index(idx_tids, table_item->ref_id_, unused_idx_tid, unused_ret, stmt, logical_plan)))
                              {
                                TBSYS_LOG(WARN, "optimize_group_by_index failed, ret = %d", ret);
                              }
                              if (OB_SUCCESS == ret && (unused_ret || 0 == group_array.count()) && order_array.count() > 0
                                  && OB_SUCCESS != (ret = optimize_order_by_index(idx_tids, table_item->ref_id_, unused_idx_tid, unused_ret, stmt, logical_plan)))
                              {
                                TBSYS_LOG(WARN, "optimize_order_by_index failed, ret = %d", ret);
                              }
                            }
                            //add liumz, [optimize group_order by index]20170419:e
                        }
                        //add:e
                    }
                    //TBSYS_LOG(ERROR,"test::fanqs,,is_use_index_without_storing=%d",is_use_index_without_storing);
                }
            }
            else   //如果用户使用了hint，根据进来的参数判断是使用回表的还是不回表的索引 //repaired from messy code by zhuxh 20151014
            {
                if(use_hint_for_storing)
                {
                    is_use_storing_column=true;
                    index_id=hint_tid;
                    //return_ret=true;
                }
                else if(use_hint_without_storing)
                {
                    is_use_index_without_storing=true;
                    index_id_without_storing=hint_tid;

                }
            }
            if(is_use_storing_column==true||is_use_index_without_storing==true)
                return_ret=true;
            // TBSYS_LOG(ERROR,"test::fanqs,,is_use_storing_column=%d,is_use_index_without_storing=%d,,hint_tid=%ld",is_use_storing_column,is_use_index_without_storing,hint_tid);
            bool group_down=false;
            bool limit_down=false;

            //add fanqiushi_index_in
            int64_t sub_query_num=0;
            int64_t num = 0;
            num = filter_array.count();
            for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
            {
                sub_query_num = sub_query_num + filter_array.at(i).get_sub_query_num();
            }
            if(OB_SUCCESS == ret && sub_query_num==0)
            {
                if(is_use_storing_column)
                {
                    ret = gen_phy_table_for_storing(
                                logical_plan,
                                physical_plan,
                                err_stat,
                                stmt,
                                table_id,
                                table_op,
                                &group_down,
                                &limit_down,
                                is_use_storing_column,
                                index_id,
                                &filter_array,
                                &project_array
                                );

                }
                else if(is_use_index_without_storing)
                {


                    ret = gen_phy_table_without_storing(
                                logical_plan,
                                physical_plan,
                                err_stat,
                                stmt,
                                table_id,
                                table_op,
                                &group_down,
                                &limit_down,
                                //is_use_storing_column,
                                index_id_without_storing,
                                &filter_array,
                                &project_array,
                                &join_column //add by wanglei [semi join second index] 20151231
                                );

                }
                //add liumz, [bugfix_limit_push_down]20160822:b
                if (group_agg_pushed_down)
                    *group_agg_pushed_down = group_down;//add liumz, [optimize group_order by index]20170419
                if (limit_pushed_down)
                    *limit_pushed_down = limit_down;
                //add:e
            }
            else
                return_ret=false;
            //add:e

            if(OB_SUCCESS!=ret)
                return_ret=false;

            if((is_use_index_without_storing == false && is_use_storing_column == false)||(OB_SUCCESS!=ret)||!return_ret)  //如果不用索引，这里要把相应的alias改成原表，这样gen_phy_table在处理没有索引的情况下才不会报错
            {
                for(int32_t i=0;i<alias_exprs.count();i++)
                {
                    ObSqlRawExpr *alias_expr = logical_plan->get_expr(alias_exprs.at(i));
                    if (alias_expr)
                    {
                        //add fanqiushi_index
                        // TBSYS_LOG(ERROR,"test::fanqs,,,alias_expr->set_columnlized(false)");
                        //add:e
                        alias_expr->set_columnlized(false);
                    }
                }
            }
        }
        //add BUG
        if(OB_SUCCESS !=  ret && is_gen_table==true)
            ret=OB_SUCCESS;
        //add:e
    }
    //add wenghaixing [secondary index]20150909 fix memory overflow bug
    for(int64_t i = 0; i < fp_array.count();i++)
    {
        ObSqlExpression* filter = fp_array.at(i);
        if(NULL != filter)
        {
            ObSqlExpression::free(filter);
        }
    }
    //add e
    //add wanglei [semi join]:b
    join_column.clear();
    //add e
    return return_ret;
}
//add:e
int ObTransformer::gen_phy_table(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObStmt *stmt,
        uint64_t table_id,
        ObPhyOperator*& table_op,
        bool* group_agg_pushed_down,
        bool* limit_pushed_down
        , ObPhyOperator *sequence_op//add liuzy [sequence select] 20150525:", ObPhyOperator *sequence_op"
        , bool outer_join_scope//add liumz, [outer_join_on_where]20150927
        )
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    //add fanqiushi_index
    bool handle_index_ret=false;
    ObPhyOperator* tmp_table_op=NULL;
    handle_index_ret=handle_index_for_one_table(logical_plan,physical_plan,err_stat,stmt,table_id,tmp_table_op,sequence_op,group_agg_pushed_down,limit_pushed_down,outer_join_scope);
    //add:e
    if(!handle_index_ret)   //add fanqiushi_index
    {
        TableItem* table_item = NULL;
        ObSqlReadStrategy sql_read_strategy;
        ObBitSet<> table_bitset;
        int32_t num = 0;
        int64_t sub_query_num = 0;//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140528
        /*Exp:used for count the num of sub_query,if main query has sub_query, alter physical plan*/
        if (table_id == OB_INVALID_ID || (table_item = stmt->get_table_item_by_id(table_id)) == NULL)
        {
            ret = OB_ERR_ILLEGAL_ID;
            TRANS_LOG("Wrong table id");
        }

        ObRpcScanHint hint;
        //del dyr [From_MaxTmpTable_BugFix] [JHOBv0.1] 20140807:b
        /**将这部分代码移动到了case TableItem::ALIAS_TABLE之后
    解决from子句中子查询过多时报Fail to get table schema for table的错误**/
        //  if (OB_SUCCESS == ret)
        //  {
        //    const ObTableSchema *table_schema = NULL;
        //    if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_item->ref_id_)))
        //    {
        //      ret = OB_ERROR;
        //      TRANS_LOG("Fail to get table schema for table[%ld]", table_item->ref_id_);
        //    }
        //    else
        //    {
        //      if (stmt->get_query_hint().read_consistency_ != NO_CONSISTENCY)
        //      {
        //        hint.read_consistency_ = stmt->get_query_hint().read_consistency_;
        //      }
        //      else
        //      {
        //        // no hint
        //        // 由于要和041兼容，目前不用consistency_level，使用is_merge_dynamic_data //repaired from messy code by zhuxh 20151014
        //        //hint.read_consistency_ = table_schema->get_consistency_level();
        //        if (table_schema->is_merge_dynamic_data())
        //        {
        //          hint.read_consistency_ = NO_CONSISTENCY;
        //        }
        //        else
        //        {
        //          hint.read_consistency_ = STATIC;
        //        }
        //        if (hint.read_consistency_ == NO_CONSISTENCY)
        //        {
        //          ObString name = ObString::make_string(OB_READ_CONSISTENCY);
        //          ObObj value;
        //          int64_t read_consistency_level_val = 0;
        //          hint.read_consistency_ = common::STRONG;
        //          if (OB_SUCCESS != (ret = sql_context_->session_info_->get_sys_variable_value(name, value)))
        //          {
        //            TBSYS_LOG(WARN, "get system variable %.*s failed, ret=%d", name.length(), name.ptr(), ret);
        //            ret = OB_SUCCESS;
        //          }
        //          else if (OB_SUCCESS != (ret = value.get_int(read_consistency_level_val)))
        //          {
        //            TBSYS_LOG(WARN, "get int failed, ret=%d", ret);
        //            ret = OB_SUCCESS;
        //          }
        //          else
        //          {
        //            hint.read_consistency_ = static_cast<ObConsistencyLevel>(read_consistency_level_val);
        //          }
        //        }
        //      }
        //    }
        //  }
        //del 20140807:e
        //add liuzy [sequence select] 20150616:b
        ObSequenceSelect *sequence_select_op = NULL;
        if (sequence_op != NULL)
        {
            sequence_select_op = dynamic_cast<ObSequenceSelect *>(sequence_op);
        }
        //20150616:e
        ObTableScan *table_scan_op = NULL;
        if (ret == OB_SUCCESS)
        {
            switch (table_item->type_)
            {
            case TableItem::BASE_TABLE:
                /* get through */
            case TableItem::ALIAS_TABLE:
            {
                //add dyr [From_MaxTmpTable_BugFix] [JHOBv0.1] 20140807:b
                if (OB_SUCCESS == ret)
                {
                    const ObTableSchema *table_schema = NULL;
                    if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_item->ref_id_)))
                    {
                        ret = OB_ERROR;
                        TRANS_LOG("Fail to get table schema for table[%ld]", table_item->ref_id_);
                    }
                    else
                    {
                        if (stmt->get_query_hint().read_consistency_ != NO_CONSISTENCY)
                        {
                            hint.read_consistency_ = stmt->get_query_hint().read_consistency_;
                        }
                        else
                        {
                            // no hint
                            //hint.read_consistency_ = table_schema->get_consistency_level();
                            if (table_schema->is_merge_dynamic_data())
                            {
                                hint.read_consistency_ = NO_CONSISTENCY;
                            }
                            else
                            {
                                hint.read_consistency_ = STATIC;
                            }
                            if (hint.read_consistency_ == NO_CONSISTENCY)
                            {
                                ObString name = ObString::make_string(OB_READ_CONSISTENCY);
                                ObObj value;
                                int64_t read_consistency_level_val = 0;
                                hint.read_consistency_ = common::STRONG;
                                if (OB_SUCCESS != (ret = sql_context_->session_info_->get_sys_variable_value(name, value)))
                                {
                                    TBSYS_LOG(WARN, "get system variable %.*s failed, ret=%d", name.length(), name.ptr(), ret);
                                    ret = OB_SUCCESS;
                                }
                                else if (OB_SUCCESS != (ret = value.get_int(read_consistency_level_val)))
                                {
                                    TBSYS_LOG(WARN, "get int failed, ret=%d", ret);
                                    ret = OB_SUCCESS;
                                }
                                else
                                {
                                    hint.read_consistency_ = static_cast<ObConsistencyLevel>(read_consistency_level_val);
                                }
                            }
                        }
                    }
                }
                //add 20140807:e
                ObTableRpcScan *table_rpc_scan_op = NULL;
                CREATE_PHY_OPERRATOR(table_rpc_scan_op, ObTableRpcScan, physical_plan, err_stat);
                if (ret == OB_SUCCESS
                        && (ret = table_rpc_scan_op->set_table(table_item->table_id_, table_item->ref_id_)) != OB_SUCCESS)
                {
                    TRANS_LOG("ObTableRpcScan set table faild");
                }
                //add fanqiushi_index
                //TBSYS_LOG(ERROR,"test::fanqs,,table_item->table_id_=%ld,,table_item->ref_id_=%ld",table_item->table_id_,table_item->ref_id_);
                //add:e
                /// determin request type: scan/get
                if (ret == OB_SUCCESS)
                {
                    int32_t bit_index = stmt->get_table_bit_index(table_item->table_id_);
                    table_bitset.add_member(bit_index);
                    if (bit_index < 0)
                    {
                      TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,table_item->table_id_);
                    }
                }
                if (OB_SUCCESS == ret)
                {
                    const ObTableSchema *table_schema = NULL;
                    if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_item->ref_id_)))
                    {
                        ret = OB_ERROR;
                        TRANS_LOG("Fail to get table schema for table[%ld]", table_item->ref_id_);
                    }
                    else
                    {
                        sql_read_strategy.set_rowkey_info(table_schema->get_rowkey_info());
                        if ((ret = physical_plan->add_base_table_version(
                                 table_item->ref_id_,
                                 table_schema->get_schema_version()
                                 )) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", table_item->ref_id_, ret);
                        }
                    }
                }
                //add liuzy [sequence select]20150616:b
                if (NULL != sequence_select_op)
                {
                    ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
                    sequence_select_op->reset_sequence_names_idx(select_stmt->get_column_has_sequene_count());
                }
                //add 20150616:e
                num = stmt->get_condition_size();
                for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
                {
                    ObSqlRawExpr *cnd_expr = logical_plan->get_expr(stmt->get_condition_id(i));
                    if (cnd_expr && table_bitset.is_superset(cnd_expr->get_tables_set()))
                    {
                        if (!outer_join_scope)//add liumz, [outer_join_on_where]20150927
                        {
                            cnd_expr->set_applied(true);
                        }

                        //add duyr [join_without_pushdown_is_null] 20151214:b
                        if (outer_join_scope && !cnd_expr->can_push_down_with_outerjoin())
                        {
                            continue;
                        }
                        //add duyr 20151214:e

                        ObSqlExpression filter;
                        if ((ret = cnd_expr->fill_sql_expression(filter, this, logical_plan, physical_plan)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add table filter condition faild");
                            break;
                        }
                        //add liuzy [sequence select] 20150515:b
                        if (OB_SUCCESS == ret && NULL != sequence_select_op)
                        {
                            if(sequence_select_op->can_fill_sequence_info())
                            {
                                uint64_t temp_id = stmt->get_condition_id(i);
                                if (sequence_select_op->is_sequence_cond_id(temp_id))
                                {
                                    if (OB_SUCCESS != (ret = sequence_select_op->fill_the_sequence_info_to_cond_expr(&filter, OB_INVALID_ID)))
                                    {
                                        TRANS_LOG("Failed deal the sequence condition filter,err=%d",ret);
                                        break;
                                    }
                                }
                                else if (OB_SUCCESS != ret)
                                {
                                    break;
                                }
                            }
                        }
                        //20150515:e
                        filter.set_owner_op(table_rpc_scan_op);
                        if (OB_SUCCESS != (ret = sql_read_strategy.add_filter(filter)))
                        {
                            TBSYS_LOG(WARN, "fail to add filter:ret[%d]", ret);
                            break;
                        }
                    }
                }
                if ( OB_SUCCESS == ret)
                {
                    int32_t read_method = ObSqlReadStrategy::USE_SCAN;
                    // Determine Scan or Get?
                    ObArray<ObRowkey> rowkey_array;
                    // TODO: rowkey obj storage needed. varchar use orginal buffer, will be copied later
                    PageArena<ObObj,ModulePageAllocator> rowkey_objs_allocator(
                                PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
                    // ObObj rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];

                    if (OB_SUCCESS != (ret = sql_read_strategy.get_read_method(rowkey_array, rowkey_objs_allocator, read_method)))
                    {
                        TBSYS_LOG(WARN, "fail to get read method:ret[%d]", ret);
                    }
                    else
                    {
                        TBSYS_LOG(DEBUG, "use [%s] method", read_method == ObSqlReadStrategy::USE_SCAN ? "SCAN" : "GET");
                    }
                    hint.read_method_ = read_method;
                }

                if (ret == OB_SUCCESS)
                {
                    ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
                    if (select_stmt
                            && select_stmt->get_group_expr_size() <= 0
                            && select_stmt->get_having_expr_size() <= 0
                            && select_stmt->get_order_item_size() <= 0
                            && hint.read_method_ != ObSqlReadStrategy::USE_GET)
                    {
                        hint.max_parallel_count = 1;
                    }
                    if ((ret = table_rpc_scan_op->init(sql_context_, &hint)) != OB_SUCCESS)
                    {
                        TRANS_LOG("ObTableRpcScan init faild");
                    }
                    //add liumz, [optimize group_order by index]20170419:b
                    else
                    {
                        table_rpc_scan_op->set_indexed_group(select_stmt->is_indexed_group());
                    }
                    //add:e
                    //add gaojt [ListAgg][JHOBv0.1]20150104:b
                    /* Exp: if push down listagg,add the order column to table_rpc_scan */
                    if(select_stmt->get_order_item_for_listagg_size()>0)
                    {
                        ObSqlRawExpr *order_expr;
                        int32_t num = select_stmt->get_order_item_for_listagg_size();
                        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
                        {
                            const OrderItem& order_item = select_stmt->get_order_item_for_listagg(i);
                            order_expr = logical_plan->get_expr(order_item.expr_id_);
                            if (order_expr->get_expr()->is_const())
                            {
                                // do nothing, const column is of no usage for sorting
                            }
                            else if (order_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
                            {
                                ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(order_expr->get_expr());
                                if ((ret = table_rpc_scan_op->add_sort_column_for_listagg(
                                         col_expr->get_first_ref_id(),
                                         col_expr->get_second_ref_id(),
                                         order_item.order_type_ == OrderItem::ASC ? true : false
                                         )) != OB_SUCCESS)
                                {
                                    TRANS_LOG("Add sort column to table rpc scan failed");
                                    break;
                                }
                            }
                            else
                            {
                                ObSqlExpression col_expr;
                                if ((ret = order_expr->fill_sql_expression(
                                         col_expr,
                                         this,
                                         logical_plan,
                                         physical_plan)) != OB_SUCCESS)
                                {
                                    TRANS_LOG("Add output column to project plan failed");
                                    break;
                                }
                                else if ((ret = table_rpc_scan_op->add_sort_column_for_listagg(
                                              order_expr->get_table_id(),
                                              order_expr->get_column_id(),
                                              order_item.order_type_ == OrderItem::ASC ? true : false
                                              )) != OB_SUCCESS)
                                {
                                    TRANS_LOG("Add sort column to table rpc scan failed");
                                    break;
                                }
                            }
                        }
                        if(OB_SUCCESS == ret)
                        {
                            table_rpc_scan_op->set_is_listagg(true);
                        }
                    }
                }
                //add 20150104:e
                if (ret == OB_SUCCESS)
                    table_scan_op = table_rpc_scan_op;
                break;
            }
            case TableItem::GENERATED_TABLE:
            {
                ObTableMemScan *table_mem_scan_op = NULL;
                int32_t idx = OB_INVALID_INDEX;
                ret = gen_physical_select(logical_plan, physical_plan, err_stat, table_item->ref_id_, &idx);
                if (ret == OB_SUCCESS)
                    CREATE_PHY_OPERRATOR(table_mem_scan_op, ObTableMemScan, physical_plan, err_stat);
                // the sub-query's physical plan is set directly, so base_table_id is no need to set
                if (ret == OB_SUCCESS
                        && (ret = table_mem_scan_op->set_table(table_item->table_id_, OB_INVALID_ID)) != OB_SUCCESS)
                {
                    TRANS_LOG("ObTableMemScan set table faild,ret=%d",ret);
                }
                if (ret == OB_SUCCESS
                        && (ret = table_mem_scan_op->set_child(0, *(physical_plan->get_phy_query(idx)))) != OB_SUCCESS)
                {
                    TRANS_LOG("Set child of ObTableMemScan operator faild,ret=%d",ret);
                }
                if (ret == OB_SUCCESS)
                {
                    int32_t bit_index = stmt->get_table_bit_index(table_item->table_id_);
                    table_bitset.add_member(bit_index);
                    if (bit_index < 0)
                    {
                      TBSYS_LOG(ERROR, "negative bitmap values,table_id=%ld" ,table_item->table_id_);
                    }
                }
                if (ret == OB_SUCCESS)
                    table_scan_op = table_mem_scan_op;
                break;
            }
            default:
                // won't be here
                OB_ASSERT(0);
                break;
            }
        }
        //add liuzy [sequence select]20150616:b
        if (NULL != sequence_select_op)
        {
            ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
            sequence_select_op->reset_sequence_names_idx(select_stmt->get_column_has_sequene_count());
        }
        //add 20150616:e
        num = stmt->get_condition_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            ObSqlRawExpr *cnd_expr = logical_plan->get_expr(stmt->get_condition_id(i));
            if (cnd_expr && table_bitset.is_superset(cnd_expr->get_tables_set()))
            {
                if (!outer_join_scope)//add liumz, [outer_join_on_where]20150927
                {
                    cnd_expr->set_applied(true);
                }

                //add duyr [join_without_pushdown_is_null] 20151214:b
                if (outer_join_scope && !cnd_expr->can_push_down_with_outerjoin())
                {
                    continue;
                }
                //add duyr 20151214:e

                ObSqlExpression *filter = ObSqlExpression::alloc();
                if (NULL == filter)
                {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    TRANS_LOG("no memory");
                    break;
                }
                //mod liuzy [sequence select] 20150515:b
                else if ((ret = cnd_expr->fill_sql_expression(*filter, this, logical_plan, physical_plan)) != OB_SUCCESS
                         /*Exp: before this statement, add "fill_the_sequence_info_to_cond_expr()"*/
                         //              || (ret = table_scan_op->add_filter(filter)) != OB_SUCCESS
                         )
                {
                    TRANS_LOG("Add table filter condition faild");
                    break;
                }
                //add liuzy [sequence select] 20150515:b
                else if (NULL != sequence_select_op
                         && sequence_select_op->can_fill_sequence_info()
                         && sequence_select_op->is_sequence_cond_id(stmt->get_condition_id(i))
                         && (OB_SUCCESS != (ret = sequence_select_op->fill_the_sequence_info_to_cond_expr(filter, OB_INVALID_ID))))
                {
                    TRANS_LOG("Failed deal the sequence condition filter,err=%d",ret);
                    break;
                }
                else if (OB_SUCCESS != ret)
                {
                    break;
                }
                //add 20150515:e
                else if ((ret = table_scan_op->add_filter(filter)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add table filter condition faild");
                    break;
                }
                //mod 20150515:e

                //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140416:b
                /*Exp:add sub_query num of every filter expression together*/
                else
                {
                    sub_query_num = sub_query_num + filter->get_sub_query_num();
                }
                //add 20140416:e
            }
        }

        // add output columns
        num = stmt->get_column_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            const ColumnItem *col_item = stmt->get_column_item(i);
            if (col_item && col_item->table_id_ == table_item->table_id_)
            {
                ObBinaryRefRawExpr col_expr(col_item->table_id_, col_item->column_id_, T_REF_COLUMN);
                ObSqlRawExpr col_raw_expr(
                            common::OB_INVALID_ID,
                            col_item->table_id_,
                            col_item->column_id_,
                            &col_expr);
                ObSqlExpression output_expr;
                if ((ret = col_raw_expr.fill_sql_expression(
                         output_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS
                        || (ret = table_scan_op->add_output_column(output_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add table output columns faild");
                    break;
                }
                //add fanqiushi_index
                // TBSYS_LOG(ERROR,"test::fanqs,,,output_expr=%s",to_cstring(output_expr));
                //add:e
            }
        }
        ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
        if (ret == OB_SUCCESS && select_stmt)
        {
            num = select_stmt->get_select_item_size();
            for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
            {
                const SelectItem& select_item = select_stmt->get_select_item(i);
                if (select_item.is_real_alias_)
                {
                    //TBSYS_LOG(ERROR,"test::fanqs,,,select_item.is_real_alias_=ture");
                    ObSqlRawExpr *alias_expr = logical_plan->get_expr(select_item.expr_id_);
                    // TBSYS_LOG(ERROR,"test::fanqs,,,alias_expr->is_columnlized()=%d,table_bitset.is_superset(alias_expr->get_tables_set()=%d",alias_expr->is_columnlized(),table_bitset.is_superset(alias_expr->get_tables_set()));
                    if (alias_expr && alias_expr->is_columnlized() == false
                            && table_bitset.is_superset(alias_expr->get_tables_set()))
                    {
                        ObSqlExpression output_expr;
                        if ((ret = alias_expr->fill_sql_expression(
                                 output_expr,
                                 this,
                                 logical_plan,
                                 physical_plan)) != OB_SUCCESS
                                || (ret = table_scan_op->add_output_column(output_expr)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add table output columns faild");
                            break;
                        }
                        //add fanqiushi_index
                        //TBSYS_LOG(ERROR,"gen_phy_table:fanqs,,,output_expr=%s",to_cstring(output_expr));
                        //add:e
                        alias_expr->set_columnlized(true);
                    }
                    //add peiouya [BUG_FIX] 20151029:b
                    //process the case:alias_expr not be added in output_column of subquery
                    else if (alias_expr && alias_expr->is_columnlized() == true
                             && table_bitset.is_superset(alias_expr->get_tables_set()))
                    {
                        const ObRowDesc *row_desc = NULL;
                        ObSqlExpression output_expr;
                        // bool is_include_column = false;  //del peiouya [alias_expr_in_subquery_bug] 20160520
                        if (OB_SUCCESS != (ret = alias_expr->fill_sql_expression(
                                               output_expr,
                                               this,
                                               logical_plan,
                                               physical_plan)))
                        {
                            TRANS_LOG("faild to fill alias_expr");
                            break;
                        }
                        //del peiouya [alias_expr_in_subquery_bug] 20160520:e
                        //ret = output_expr.is_column_index_expr(is_include_column);
                        //is_include_column = true;
                        //del peiouya [alias_expr_in_subquery_bug] 20160520:e
                        if (OB_SUCCESS != ret)
                        {
                            TRANS_LOG("invalid expr");
                            break;
                        }
                        //mod peiouya [alias_pushdown_bug] 20160525:b
                        ////if output_expr doesn't contain "COLUMN_IDX", it cannot be processed at this step
                        ////mod peiouya [alias_expr_in_subquery_bug] 20160520:b
                        ////if (OB_INVALID_ID == output_expr.get_table_id() && !is_include_column)
                        ////{
                        ////    //NOTHING TODO
                        ////}
                        ////check output_expr has been added in table_scan_op. if false, add it to table_scan_op; if true, nothing todo
                        ////else if (OB_SUCCESS == (ret = table_scan_op->get_row_desc(row_desc)))
                        //if (OB_SUCCESS == (ret = table_scan_op->get_row_desc(row_desc)))
                        ////mod peiouya [alias_expr_in_subquery_bug] 20160520:e
                        if (!alias_expr->get_expr ()->is_column ())
                        {
                           //NOTHING TODO
                        }
                        else if (OB_SUCCESS == (ret = table_scan_op->get_row_desc(row_desc)))
                        //mod peiouya [alias_pushdown_bug] 20160525:e
                        {
                            if (OB_INVALID_INDEX != row_desc->get_idx(output_expr.get_table_id(),output_expr.get_column_id()))
                            {
                                //NOTHING TODO
                            }
                            else if (OB_SUCCESS != (ret = table_scan_op->add_output_column(output_expr)))
                            {
                                TRANS_LOG("Add table output columns faild");
                                break;
                            }
                            else
                            {
                                //NOTHING TODO
                            }
                        }
                        //special process if none columns have been added in table_scan_op
                        else
                        {
                            if (OB_SUCCESS != (ret = table_scan_op->add_output_column(output_expr)))
                            {
                                TRANS_LOG("Add table output columns faild");
                                break;
                            }
                        }
                    }
                    //add end
                }
            }
        }

        if (ret == OB_SUCCESS)
            table_op = table_scan_op;

        bool group_down = false;
        bool limit_down = false;

        //mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140516:b
        /*Exp: stmt which has sub_query do not push Aggregate Functions down,else client will get wrong result*/
        //if (hint.read_method_ == ObSqlReadStrategy::USE_SCAN)
        if ((hint.read_method_ == ObSqlReadStrategy::USE_SCAN)&& (0 ==sub_query_num))
            //mod 20140516:e
        {
            /* Try to push down aggregations */
            if (ret == OB_SUCCESS && group_agg_push_down_param_ && select_stmt)
            {
                ret = try_push_down_group_agg(
                            logical_plan,
                            physical_plan,
                            err_stat,
                            select_stmt,
                            group_down,
                            table_op);
                if (group_agg_pushed_down)
                    *group_agg_pushed_down = group_down;
            }
            /* Try to push down limit */
            if (ret == OB_SUCCESS && select_stmt)
            {
                ret = try_push_down_limit(
                            logical_plan,
                            physical_plan,
                            err_stat,
                            select_stmt,
                            limit_down,
                            table_op);
                if (limit_pushed_down)
                    *limit_pushed_down = limit_down;
            }
        }
        else
        {
            if (group_agg_pushed_down)
                *group_agg_pushed_down = false;
            if (limit_pushed_down)
                *limit_pushed_down = false;
        }
        //delete by xionghui [subquery_final] 20160216 b:
        /*
        //add zhujun [fix equal-subquery bug] 20151013:b
        //add a if-else branch to distinguish in-subquery and equal-subquery
        if(select_stmt->get_is_equal_subquery() && (sub_query_num>0))
        {
            ObMultiEqualBind *equal_bind_op = NULL;
            physical_plan->need_extend_time();
            int32_t main_query_idx= 0;
            if (CREATE_PHY_OPERRATOR(equal_bind_op, ObMultiEqualBind, physical_plan, err_stat) == NULL
                    || (ret = equal_bind_op->set_child(main_query_idx, *table_op)) != OB_SUCCESS)
            {
                TRANS_LOG("fail to set child:ret[%d]",ret);
            }
            else if (table_scan_op->is_base_table_id_valid())//mod zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118
            {
                ObTableRpcScan *table_rpc_scan_op = dynamic_cast<ObTableRpcScan *>( table_scan_op);
                equal_bind_op->set_table_id(table_id);
                if(NULL == table_rpc_scan_op )
                {
                    ret = OB_ERROR;
                    TRANS_LOG("wrong get table_rpc_scan_op, can't dynamic cast");
                }
                else
                {
                    //for one sub_query because equal need one subquery

                    for(int32_t sub_query_idx = 1;sub_query_idx<=sub_query_num;sub_query_idx++)
                    {
                        int32_t index = OB_INVALID_INDEX;
                        ObPhyOperator * sub_operator = NULL;
                        if((ret = table_rpc_scan_op->get_sub_query_index(sub_query_idx,index))!=OB_SUCCESS)
                        {
                            TRANS_LOG("wrong get sub query index");
                        }
                        else if(NULL == (sub_operator = physical_plan->get_phy_query(index)))
                        {
                            ret = OB_INVALID_INDEX;
                            TRANS_LOG("wrong get sub query operator");
                        }
                        else if(OB_SUCCESS != (ret = equal_bind_op->set_child(sub_query_idx, *sub_operator)))
                        {
                            TRANS_LOG("fail to set child:ret[%d]",ret);
                        }

                    }

                    //        int32_t sub_query_idx = 1;
                    //        int32_t index = OB_INVALID_INDEX;
                    //        ObPhyOperator * sub_operator = NULL;
                    //        if((ret = table_rpc_scan_op->get_sub_query_index(sub_query_idx,index))!=OB_SUCCESS)
                    //        {
                    //            TRANS_LOG("wrong get sub query index");
                    //        }
                    //        else if(NULL == (sub_operator = physical_plan->get_phy_query(index)))
                    //        {
                    //            ret = OB_INVALID_INDEX;
                    //            TRANS_LOG("wrong get sub query operator");
                    //        }
                    //        else if(OB_SUCCESS != (ret = equal_bind_op->set_child(sub_query_idx, *sub_operator)))
                    //        {
                    //            TRANS_LOG("fail to set child:ret[%d]",ret);
                    //        }
                }
            }

            //add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
            else
            {
                ObTableMemScan *table_mem_scan_op = dynamic_cast<ObTableMemScan *>( table_scan_op);
                equal_bind_op->set_table_id(table_id);
                if(NULL == table_mem_scan_op )
                {
                    ret = OB_ERROR;
                    TRANS_LOG("wrong get table_mem_scan_op, can't dynamic cast");
                }
                else
                {
                    //for one sub_query because equal need one subquery

                    for(int32_t sub_query_idx = 1;sub_query_idx<=sub_query_num;sub_query_idx++)
                    {
                        int32_t index = OB_INVALID_INDEX;
                        ObPhyOperator * sub_operator = NULL;
                        if((ret = table_mem_scan_op->get_sub_query_index(sub_query_idx,index))!=OB_SUCCESS)
                        {
                            TRANS_LOG("wrong get sub query index");
                        }
                        else if(NULL == (sub_operator = physical_plan->get_phy_query(index)))
                        {
                            ret = OB_INVALID_INDEX;
                            TRANS_LOG("wrong get sub query operator");
                        }
                        else if(OB_SUCCESS != (ret = equal_bind_op->set_child(sub_query_idx, *sub_operator)))
                        {
                            TRANS_LOG("fail to set child:ret[%d]",ret);
                        }

                    }
                }
            }
            //add:e
            //set equal_bind_op as top opertator


            if (ret == OB_SUCCESS)
                table_op = equal_bind_op;
        }
        //add xionghui [fix like-subquery bug] 20151015:b
        else if(select_stmt->get_is_like_subquery()&&(sub_query_num>0))
        {
            ObMultiLikeBind *like_bind_op = NULL;
            physical_plan->need_extend_time();
            int32_t main_query_idx= 0;
            if (CREATE_PHY_OPERRATOR(like_bind_op, ObMultiLikeBind, physical_plan, err_stat) == NULL
                    || (ret = like_bind_op->set_child(main_query_idx, *table_op)) != OB_SUCCESS)
            {
                TRANS_LOG("fail to set child:ret[%d]",ret);
            }
            else if (table_scan_op->is_base_table_id_valid())
            {
                ObTableRpcScan *table_rpc_scan_op = dynamic_cast<ObTableRpcScan *>( table_scan_op);
                like_bind_op->set_table_id(table_id);
                if(NULL == table_rpc_scan_op )
                {
                    ret = OB_ERROR;
                    TRANS_LOG("wrong get table_rpc_scan_op, can't dynamic cast");
                }
                else
                {
                    //for one sub_query because equal need one subquery

                    for(int32_t sub_query_idx = 1;sub_query_idx<=sub_query_num;sub_query_idx++)
                    {
                        int32_t index = OB_INVALID_INDEX;
                        ObPhyOperator * sub_operator = NULL;
                        if((ret = table_rpc_scan_op->get_sub_query_index(sub_query_idx,index))!=OB_SUCCESS)
                        {
                            TRANS_LOG("wrong get sub query index");
                        }
                        else if(NULL == (sub_operator = physical_plan->get_phy_query(index)))
                        {
                            ret = OB_INVALID_INDEX;
                            TRANS_LOG("wrong get sub query operator");
                        }
                        else if(OB_SUCCESS != (ret = like_bind_op->set_child(sub_query_idx, *sub_operator)))
                        {
                            TRANS_LOG("fail to set child:ret[%d]",ret);
                        }

                    }
                }
            }
            else
            {
                ObTableMemScan *table_mem_scan_op = dynamic_cast<ObTableMemScan *>( table_scan_op);
                like_bind_op->set_table_id(table_id);
                if(NULL == table_mem_scan_op )
                {
                    ret = OB_ERROR;
                    TRANS_LOG("wrong get table_mem_scan_op, can't dynamic cast");
                }
                else
                {
                    //for one sub_query because equal need one subquery

                    for(int32_t sub_query_idx = 1;sub_query_idx<=sub_query_num;sub_query_idx++)
                    {
                        int32_t index = OB_INVALID_INDEX;
                        ObPhyOperator * sub_operator = NULL;
                        if((ret = table_mem_scan_op->get_sub_query_index(sub_query_idx,index))!=OB_SUCCESS)
                        {
                            TRANS_LOG("wrong get sub query index");
                        }
                        else if(NULL == (sub_operator = physical_plan->get_phy_query(index)))
                        {
                            ret = OB_INVALID_INDEX;
                            TRANS_LOG("wrong get sub query operator");
                        }
                        else if(OB_SUCCESS != (ret = like_bind_op->set_child(sub_query_idx, *sub_operator)))
                        {
                            TRANS_LOG("fail to set child:ret[%d]",ret);
                        }

                    }
                }
            }
            //set like_bind_op as top opertator
            if (ret == OB_SUCCESS)
                table_op = like_bind_op;

        }
        else
        //add 20151015:e:
        */
        //delete e:

            //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140416:b
            /*Exp:add new operator to physical plan,put ObTableRpcScan as main query,
    bind to ObMultiBind's children[0], bind other sub_query to ObMultiBind's later children*/
            ObMultiBind *multi_bind_op = NULL;
            if(ret == OB_SUCCESS && (sub_query_num>0))
            {
                physical_plan->need_extend_time();
                int32_t main_query_idx= 0;
                if (CREATE_PHY_OPERRATOR(multi_bind_op, ObMultiBind, physical_plan, err_stat) == NULL
                        || (ret = multi_bind_op->set_child(main_query_idx, *table_op)) != OB_SUCCESS)
                {
                    TRANS_LOG("fail to set child:ret[%d]",ret);
                }
                else if (table_scan_op->is_base_table_id_valid())
                {
                    ObTableRpcScan *table_rpc_scan_op = dynamic_cast<ObTableRpcScan *>( table_scan_op);
                    if(NULL == table_rpc_scan_op )
                    {
                        ret = OB_ERROR;
                        TRANS_LOG("wrong get table_rpc_scan_op, can't dynamic cast");
                    }
                    else
                    {
                        //for every sub_query
                        //1.get it's physical plan index from main query(ObTableRpcScan)
                        //2.get it's top physical operator address from ObPhysicalPlan by it's physical plan index
                        //3.bind it's top physical operator address to ObMultiBind
                        for(int32_t sub_query_idx = 1;sub_query_idx<=sub_query_num;sub_query_idx++)
                        {
                            int32_t index = OB_INVALID_INDEX;
                            ObPhyOperator * sub_operator = NULL;
                            if((ret = table_rpc_scan_op->get_sub_query_index(sub_query_idx,index))!=OB_SUCCESS)
                            {
                                TRANS_LOG("wrong get sub query index");
                            }
                            else if(NULL == (sub_operator = physical_plan->get_phy_query(index)))
                            {
                                ret = OB_INVALID_INDEX;
                                TRANS_LOG("wrong get sub query operator");
                            }
                            else if(OB_SUCCESS != (ret = multi_bind_op->set_child(sub_query_idx, *sub_operator)))
                            {
                                TRANS_LOG("fail to set child:ret[%d]",ret);
                            }

                        }
                    }

                }
                else
                {
                    ObTableMemScan *table_mem_scan_op = dynamic_cast<ObTableMemScan *>( table_scan_op);
                    if(NULL == table_mem_scan_op )
                    {
                        ret = OB_ERROR;
                        TRANS_LOG("wrong get table_mem_scan_op, can't dynamic cast");
                    }
                    else
                    {
                        //for every sub_query
                        //1.get it's physical plan index from main query(ObTableRpcScan)
                        //2.get it's top physical operator address from ObPhysicalPlan by it's physical plan index
                        //3.bind it's top physical operator address to ObMultiBind
                        for(int32_t sub_query_idx = 1;sub_query_idx<=sub_query_num;sub_query_idx++)
                        {
                            int32_t index = OB_INVALID_INDEX;
                            ObPhyOperator * sub_operator = NULL;
                            if((ret = table_mem_scan_op->get_sub_query_index(sub_query_idx,index))!=OB_SUCCESS)
                            {
                                TRANS_LOG("wrong get sub query index");
                            }
                            else if(NULL == (sub_operator = physical_plan->get_phy_query(index)))
                            {
                                ret = OB_INVALID_INDEX;
                                TRANS_LOG("wrong get sub query operator");
                            }
                            else if(OB_SUCCESS != (ret = multi_bind_op->set_child(sub_query_idx, *sub_operator)))
                            {
                                TRANS_LOG("fail to set child:ret[%d]",ret);
                            }

                        }
                    }

                }
                //set multi_bind_op as top opertator
                if (ret == OB_SUCCESS)
                    table_op = multi_bind_op;
            }
            //add 20140416:e


    }
    else   //add fanqiushi_index
    {
        table_op = tmp_table_op;
        // TBSYS_LOG(ERROR,"test::fanqs,,enter this");
    }
    return ret;
}


int ObTransformer::try_push_down_group_agg(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const ObSelectStmt *select_stmt,
        bool& group_agg_pushed_down,
        ObPhyOperator *& scan_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObTableRpcScan *table_rpc_scan_op = dynamic_cast<ObTableRpcScan*>(scan_op);
    ObAddProject *project_op = NULL;

    if (table_rpc_scan_op == NULL)
    {
        // ignore
    }
    // 1. normal select statement, not UNION/EXCEPT/INTERSECT
    // 2. only one table, whose type is BASE_TABLE or ALIAS_TABLE
    // 3. can not be joined table.
    // 4. has group clause or aggregate function(s)
    // 6. no distinct aggregate function(s)
    else if (select_stmt->get_from_item_size() == 1
             && select_stmt->get_from_item(0).is_joined_ == false
             && select_stmt->get_table_size() == 1
             && (select_stmt->get_table_item(0).type_ == TableItem::BASE_TABLE
                 || select_stmt->get_table_item(0).type_ == TableItem::ALIAS_TABLE)
             && (select_stmt->get_group_expr_size() > 0
                 || select_stmt->get_agg_fun_size() > 0))
    {
        ObSqlRawExpr *expr = NULL;
        ObAggFunRawExpr *agg_expr = NULL;
        int32_t agg_num = select_stmt->get_agg_fun_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < agg_num; i++)
        {
            if ((expr = logical_plan->get_expr(select_stmt->get_agg_expr_id(i))) == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong expression id of aggregate function");
                break;
            }
            else if ((agg_expr = dynamic_cast<ObAggFunRawExpr*>(expr->get_expr())) == NULL)
            {
                // agg(*), skip
                continue;
            }
            else if (agg_expr->is_param_distinct())
            {
                break;
            }
            else if (i == agg_num - 1)
            {
                group_agg_pushed_down = true;
            }
        }
    }

    // push down aggregate function(s)
    if (ret == OB_SUCCESS && group_agg_pushed_down)
    {
        // push down group column(s)
        ObSqlRawExpr *group_expr = NULL;
        int32_t num = select_stmt->get_group_expr_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            if ((group_expr = logical_plan->get_expr(select_stmt->get_group_expr_id(i))) == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong expression id  of group column");
            }
            else if (group_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
            {
                ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(group_expr->get_expr());
                if ((ret = table_rpc_scan_op->add_group_column(
                         col_expr->get_first_ref_id(),
                         col_expr->get_second_ref_id())) != OB_SUCCESS)
                {
                    TRANS_LOG("Add group column faild, table_id=%lu, column_id=%lu",
                              col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
                }
            }
            else if (group_expr->get_expr()->is_const())
            {
                // do nothing, const column is of no usage for sorting
                continue;
            }
            else
            {
                ObSqlExpression col_expr;
                if ((ret = group_expr->fill_sql_expression(
                         col_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS
                        || (ret = table_rpc_scan_op->add_output_column(col_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add complex group column to project plan faild");
                }
                else if ((ret = table_rpc_scan_op->add_group_column(
                              group_expr->get_table_id(),
                              group_expr->get_column_id())) != OB_SUCCESS)
                {
                    TRANS_LOG("Add group column to group plan faild");
                }
            }
        }

        // push down function(s)
        num = select_stmt->get_agg_fun_size();
        ObSqlRawExpr *agg_expr = NULL;
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            if ((agg_expr = logical_plan->get_expr(select_stmt->get_agg_expr_id(i))) == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong expression id  of aggregate function");
                break;
            }
            else if (agg_expr->get_expr()->get_expr_type() ==  T_FUN_AVG)
            {
                // avg() ==> sum() / count()
                ObAggFunRawExpr *avg_expr = NULL;
                if ((avg_expr = dynamic_cast<ObAggFunRawExpr*>(agg_expr->get_expr())) == NULL)
                {
                    ret = OB_ERR_RESOLVE_SQL;
                    TRANS_LOG("Wrong aggregate function, exp_id = %lu", agg_expr->get_expr_id());
                    break;
                }

                // add sum(), count() to TableRpcScan
                uint64_t table_id = agg_expr->get_table_id();
                uint64_t sum_cid = logical_plan->generate_range_column_id();
                uint64_t count_cid = logical_plan->generate_range_column_id();
                ObAggFunRawExpr sum_node;
                ObAggFunRawExpr count_node;
                sum_node.set_expr_type(T_FUN_SUM);
                sum_node.set_param_expr(avg_expr->get_param_expr());
                count_node.set_expr_type(T_FUN_COUNT);
                count_node.set_param_expr(avg_expr->get_param_expr());
                ObSqlRawExpr raw_sum_expr(OB_INVALID_ID, table_id, sum_cid, &sum_node);
                ObSqlRawExpr raw_count_expr(OB_INVALID_ID, table_id, count_cid, &count_node);
                ObSqlExpression sum_expr;
                ObSqlExpression count_expr;
                if ((ret = raw_sum_expr.fill_sql_expression(
                         sum_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS
                        || (ret = table_rpc_scan_op->add_aggr_column(sum_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add sum aggregate function failed, ret = %d", ret);
                    break;
                }
                else if ((ret = raw_count_expr.fill_sql_expression(
                              count_expr,
                              this,
                              logical_plan,
                              physical_plan)) != OB_SUCCESS
                         || (ret = table_rpc_scan_op->add_aggr_column(count_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add count aggregate function failed, ret = %d", ret);
                    break;
                }

                // add a '/' expression
                ObBinaryRefRawExpr sum_col_node(table_id, sum_cid, T_REF_COLUMN);
                ObBinaryRefRawExpr count_col_node(table_id, count_cid, T_REF_COLUMN);
                ObBinaryOpRawExpr div_node(&sum_col_node, &count_col_node, T_OP_DIV);
                ObSqlRawExpr div_raw_expr(OB_INVALID_ID, table_id, agg_expr->get_column_id(), &div_node);
                ObSqlExpression div_expr;
                if (project_op == NULL)
                {
                    if (CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan, err_stat) == NULL
                            || (ret = project_op->set_child(0, *table_rpc_scan_op)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add ObAddProject on ObTableRpcScan failed, ret = %d", ret);
                        break;
                    }
                    else
                    {
                        scan_op = project_op;
                    }
                }
                if ((ret = div_raw_expr.fill_sql_expression(div_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Generate div expr for avg() function failed, ret = %d", ret);
                }
                else if ((ret = project_op->add_output_column(div_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add column to ObAddProject operator failed, ret = %d", ret);
                }
            }
            else if (agg_expr->get_expr()->is_aggr_fun())
            {
                ObSqlExpression new_agg_expr;
                if ((ret = agg_expr->fill_sql_expression(
                         new_agg_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS
                        || (ret = table_rpc_scan_op->add_aggr_column(new_agg_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add aggregate function to group plan faild");
                    break;
                }
            }
            else
            {
                ret = OB_ERR_RESOLVE_SQL;
                TRANS_LOG("Wrong aggregate function, exp_id = %lu", agg_expr->get_expr_id());
                break;
            }
            agg_expr->set_columnlized(true);
        }
    }
    return ret;
}
//add liumz, [optimize group_order by index]20170419:b
int ObTransformer::try_push_down_group_agg_for_storing(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const ObSelectStmt *select_stmt,
        bool& group_agg_pushed_down,
        ObPhyOperator *& scan_op,
        const uint64_t index_tid,
        bool is_ailias_table)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObTableRpcScan *table_rpc_scan_op = dynamic_cast<ObTableRpcScan*>(scan_op);
    ObAddProject *project_op = NULL;

    if (table_rpc_scan_op == NULL)
    {
        // ignore
    }
    // 1. normal select statement, not UNION/EXCEPT/INTERSECT
    // 2. only one table, whose type is BASE_TABLE or ALIAS_TABLE
    // 3. can not be joined table.
    // 4. has group clause or aggregate function(s)
    // 6. no distinct aggregate function(s)
    else if (select_stmt->get_from_item_size() == 1
             && select_stmt->get_from_item(0).is_joined_ == false
             && select_stmt->get_table_size() == 1
             && (select_stmt->get_table_item(0).type_ == TableItem::BASE_TABLE
                 || select_stmt->get_table_item(0).type_ == TableItem::ALIAS_TABLE)
             && (select_stmt->get_group_expr_size() > 0
                 || select_stmt->get_agg_fun_size() > 0))
    {
        ObSqlRawExpr *expr = NULL;
        ObAggFunRawExpr *agg_expr = NULL;
        int32_t agg_num = select_stmt->get_agg_fun_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < agg_num; i++)
        {
            if ((expr = logical_plan->get_expr(select_stmt->get_agg_expr_id(i))) == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong expression id of aggregate function");
                break;
            }
            else if ((agg_expr = dynamic_cast<ObAggFunRawExpr*>(expr->get_expr())) == NULL)
            {
                // agg(*), skip
                continue;
            }
            else if (agg_expr->is_param_distinct())
            {
                break;
            }
            else if (i == agg_num - 1)
            {
                group_agg_pushed_down = true;
            }
        }
    }

    // push down aggregate function(s)
    if (ret == OB_SUCCESS && group_agg_pushed_down)
    {
        // push down group column(s)
        ObSqlRawExpr *group_expr = NULL;
        int32_t num = select_stmt->get_group_expr_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            if ((group_expr = logical_plan->get_expr(select_stmt->get_group_expr_id(i))) == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong expression id  of group column");
            }
            else if (group_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
            {
                ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(group_expr->get_expr());
                if ((ret = table_rpc_scan_op->add_group_column(
                         is_ailias_table?col_expr->get_first_ref_id():index_tid,
                         col_expr->get_second_ref_id(),
                         !is_ailias_table)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add group column faild, table_id=%lu, column_id=%lu",
                              index_tid, col_expr->get_second_ref_id());
                }
            }
            else if (group_expr->get_expr()->is_const())
            {
                // do nothing, const column is of no usage for sorting
                continue;
            }
            else
            {
                ObSqlExpression col_expr;
                if ((ret = group_expr->fill_sql_expression(
                         col_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS)
                {
                    TRANS_LOG("fill sql expression failed!");
                }
                else if (!is_ailias_table)
                {
                  //change tid to index_tid
                  ObArray<uint64_t> index_column_array;
                  if(OB_SUCCESS==(ret=col_expr.get_all_cloumn(index_column_array)))
                  {
                      for(int32_t i=0;i<index_column_array.count();i++)
                      {
                          ObPostfixExpression& ops=col_expr.get_decoded_expression_v2();
                          ObObj& obj=ops.get_expr_by_index(index_column_array.at(i));
                          if(obj.get_type()==ObIntType)
                              obj.set_int(index_tid);
                      }
                  }
                }

                if (OB_SUCCESS == ret)
                {
                  if((ret = table_rpc_scan_op->add_output_column(col_expr)) != OB_SUCCESS)
                  {
                      TRANS_LOG("Add complex group column to project plan faild");
                  }
                  else if ((ret = table_rpc_scan_op->add_group_column(
                                group_expr->get_table_id(),
                                group_expr->get_column_id())) != OB_SUCCESS)
                  {
                      TRANS_LOG("Add group column to group plan faild");
                  }
                }
            }
        }

        // push down function(s)
        num = select_stmt->get_agg_fun_size();
        ObSqlRawExpr *agg_expr = NULL;
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            if ((agg_expr = logical_plan->get_expr(select_stmt->get_agg_expr_id(i))) == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong expression id  of aggregate function");
                break;
            }
            else if (agg_expr->get_expr()->get_expr_type() ==  T_FUN_AVG)
            {
                // avg() ==> sum() / count()
                ObAggFunRawExpr *avg_expr = NULL;
                if ((avg_expr = dynamic_cast<ObAggFunRawExpr*>(agg_expr->get_expr())) == NULL)
                {
                    ret = OB_ERR_RESOLVE_SQL;
                    TRANS_LOG("Wrong aggregate function, exp_id = %lu", agg_expr->get_expr_id());
                    break;
                }

                // add sum(), count() to TableRpcScan
                uint64_t table_id = agg_expr->get_table_id();
                uint64_t sum_cid = logical_plan->generate_range_column_id();
                uint64_t count_cid = logical_plan->generate_range_column_id();
                ObAggFunRawExpr sum_node;
                ObAggFunRawExpr count_node;
                sum_node.set_expr_type(T_FUN_SUM);
                sum_node.set_param_expr(avg_expr->get_param_expr());
                count_node.set_expr_type(T_FUN_COUNT);
                count_node.set_param_expr(avg_expr->get_param_expr());
                ObSqlRawExpr raw_sum_expr(OB_INVALID_ID, table_id, sum_cid, &sum_node);
                ObSqlRawExpr raw_count_expr(OB_INVALID_ID, table_id, count_cid, &count_node);
                ObSqlExpression sum_expr;
                ObSqlExpression count_expr;
                if ((ret = raw_sum_expr.fill_sql_expression(
                         sum_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS)
                {
                    TRANS_LOG("fill sql expression failed, ret = %d", ret);
                    break;
                }
                else if (!is_ailias_table)
                {
                  //change tid to index_tid
                  ObArray<uint64_t> index_column_array;
                  if(OB_SUCCESS==(ret=sum_expr.get_all_cloumn(index_column_array)))
                  {
                      for(int32_t i=0;i<index_column_array.count();i++)
                      {
                          ObPostfixExpression& ops=sum_expr.get_decoded_expression_v2();
                          ObObj& obj=ops.get_expr_by_index(index_column_array.at(i));
                          if(obj.get_type()==ObIntType)
                              obj.set_int(index_tid);
                      }
                  }
                }
                if (OB_SUCCESS == ret)
                {
                  if ((ret = table_rpc_scan_op->add_aggr_column(sum_expr)) != OB_SUCCESS)
                  {
                    TRANS_LOG("Add sum aggregate function failed, ret = %d", ret);
                    break;
                  }
                }

                if (OB_SUCCESS == ret)
                {
                  if ((ret = raw_count_expr.fill_sql_expression(
                                count_expr,
                                this,
                                logical_plan,
                                physical_plan)) != OB_SUCCESS)
                  {
                      TRANS_LOG("fill sql expression failed, ret = %d", ret);
                      break;
                  }
                  else if (!is_ailias_table)
                  {
                    //change tid to index_tid
                    ObArray<uint64_t> index_column_array;
                    if(OB_SUCCESS==(ret=count_expr.get_all_cloumn(index_column_array)))
                    {
                        for(int32_t i=0;i<index_column_array.count();i++)
                        {
                            ObPostfixExpression& ops=count_expr.get_decoded_expression_v2();
                            ObObj& obj=ops.get_expr_by_index(index_column_array.at(i));
                            if(obj.get_type()==ObIntType)
                                obj.set_int(index_tid);
                        }
                    }
                  }
                }
                if (OB_SUCCESS == ret)
                {
                  if ((ret = table_rpc_scan_op->add_aggr_column(count_expr)) != OB_SUCCESS)
                  {
                      TRANS_LOG("Add count aggregate function failed, ret = %d", ret);
                      break;
                  }
                }

                // add a '/' expression
                ObBinaryRefRawExpr sum_col_node(table_id, sum_cid, T_REF_COLUMN);
                ObBinaryRefRawExpr count_col_node(table_id, count_cid, T_REF_COLUMN);
                ObBinaryOpRawExpr div_node(&sum_col_node, &count_col_node, T_OP_DIV);
                ObSqlRawExpr div_raw_expr(OB_INVALID_ID, table_id, agg_expr->get_column_id(), &div_node);
                ObSqlExpression div_expr;
                if (project_op == NULL)
                {
                    if (CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan, err_stat) == NULL
                            || (ret = project_op->set_child(0, *table_rpc_scan_op)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add ObAddProject on ObTableRpcScan failed, ret = %d", ret);
                        break;
                    }
                    else
                    {
                        scan_op = project_op;
                    }
                }
                if ((ret = div_raw_expr.fill_sql_expression(div_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Generate div expr for avg() function failed, ret = %d", ret);
                }
                else if ((ret = project_op->add_output_column(div_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add column to ObAddProject operator failed, ret = %d", ret);
                }
            }
            else if (agg_expr->get_expr()->is_aggr_fun())
            {
                ObSqlExpression new_agg_expr;
                if ((ret = agg_expr->fill_sql_expression(
                         new_agg_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS)
                {
                    TRANS_LOG("fill sql expression faild");
                    break;
                }
                else if (!is_ailias_table)
                {
                  //change tid to index_tid
                  ObArray<uint64_t> index_column_array;
                  if(OB_SUCCESS==(ret=new_agg_expr.get_all_cloumn(index_column_array)))
                  {
                      for(int32_t i=0;i<index_column_array.count();i++)
                      {
                          ObPostfixExpression& ops=new_agg_expr.get_decoded_expression_v2();
                          ObObj& obj=ops.get_expr_by_index(index_column_array.at(i));
                          if(obj.get_type()==ObIntType)
                              obj.set_int(index_tid);
                      }
                  }
                }
                if (OB_SUCCESS == ret)
                {
                  if ((ret = table_rpc_scan_op->add_aggr_column(new_agg_expr)) != OB_SUCCESS)
                  {
                    TRANS_LOG("Add aggregate function to group plan faild");
                    break;
                  }
                }
            }
            else
            {
                ret = OB_ERR_RESOLVE_SQL;
                TRANS_LOG("Wrong aggregate function, exp_id = %lu", agg_expr->get_expr_id());
                break;
            }
            agg_expr->set_columnlized(true);
        }
    }
    return ret;
}
//add:e
int ObTransformer::try_push_down_limit(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const ObSelectStmt *select_stmt,
        bool& limit_pushed_down,
        ObPhyOperator *scan_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObTableRpcScan *table_rpc_scan_op = dynamic_cast<ObTableRpcScan*>(scan_op);

    if (table_rpc_scan_op == NULL)
    {
        // ignore
    }
    // 1. normal select statement, not UNION/EXCEPT/INTERSECT
    // 2. only one table, whose type is BASE_TABLE or ALIAS_TABLE
    // 3. can not be joined table.
    // 4. does not have group clause or aggregate function(s)
    // 5. does not have order by caluse
    // 6. limit is initialed
    // 7. does not have distinct flag
    else if (select_stmt->get_from_item_size() == 1
             && select_stmt->get_from_item(0).is_joined_ == false
             && select_stmt->get_table_size() == 1
             && (select_stmt->get_table_item(0).type_ == TableItem::BASE_TABLE
                 || select_stmt->get_table_item(0).type_ == TableItem::ALIAS_TABLE)
             && select_stmt->get_group_expr_size() == 0
             && select_stmt->get_agg_fun_size() == 0
             && (select_stmt->get_order_item_size() == 0 || select_stmt->get_unindexed_order_item_size() == 0)//add liumz, [optimize group_order by index]20170419
             && !select_stmt->is_distinct())//add liumz, [bugfix: do not push down limit when select_stmt->is_distinct]20150901
    {
        limit_pushed_down = true;
        ObSqlExpression limit_count;
        ObSqlExpression limit_offset;
        ObSqlExpression *ptr = &limit_count;
        uint64_t id = select_stmt->get_limit_expr_id();
        int64_t i = 0;
        for (; ret == OB_SUCCESS && i < 2;
             i++, id = select_stmt->get_offset_expr_id(), ptr = &limit_offset)
        {
            ObSqlRawExpr *raw_expr = NULL;
            if (id == OB_INVALID_ID)
            {
                continue;
            }
            else if ((raw_expr = logical_plan->get_expr(id)) == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong internal expression id = %lu, ret=%d", id, ret);
                break;
            }
            else if ((ret = raw_expr->fill_sql_expression(
                          *ptr,
                          this,
                          logical_plan,
                          physical_plan)) != OB_SUCCESS)
            {
                TRANS_LOG("Add limit/offset faild");
                break;
            }
        }
        if (ret == OB_SUCCESS && (ret = table_rpc_scan_op->set_limit(limit_count, limit_offset)) != OB_SUCCESS)
        {
            TRANS_LOG("Set limit/offset failed, ret=%d", ret);
        }
    }
    return ret;
}
//add wenghaixing [secondary index replace bug_fix]20150517
int ObTransformer::row_desc_intersect(ObRowDesc &row_desc,
                                      ObRowDescExt &row_desc_ext,
                                      ObRowDesc row_desc_idx,
                                      ObRowDescExt row_desc_ext_idx)
{
    int ret = OB_SUCCESS;
    ObRowDesc tmp_desc = row_desc;
    //ObRowDescExt tmp_ext_desc = row_desc_ext;
    uint64_t tid = OB_INVALID_ID;
    uint64_t cid = OB_INVALID_ID;
    //int64_t idx = 0;
    for (int64_t i = 0; i < row_desc_idx.get_column_num(); i++)
    {
        //idx = 0;
        if(OB_SUCCESS != (ret = row_desc_idx.get_tid_cid(i, tid, cid)))
        {
            TBSYS_LOG(WARN, "get table id and column id failed,ret = %d, idx = %ld", ret, i);
            break;
        }
        else if(OB_INVALID_INDEX == tmp_desc.get_idx(tid, cid))
        {
            ObObj data_type;
            uint64_t tmp_tid = OB_INVALID_ID;
            uint64_t tmp_cid = OB_INVALID_ID;
            if(OB_SUCCESS != (ret = row_desc_ext_idx.get_by_idx(i, tmp_tid, tmp_cid, data_type)))
            {
                TBSYS_LOG(WARN, "get data_type from row_desc_ext failed!ret = %d, idx = %ld", ret, i);
                break;
            }
            else
            {
                if(OB_SUCCESS != (ret = row_desc.add_column_desc(tid, cid)))
                {
                    TBSYS_LOG(WARN, "add column desc failed ,tid[%ld], cid[%ld],ret[%d]",tid, cid,ret);
                    break;
                }
                else if(OB_SUCCESS != (ret = row_desc_ext.add_column_desc(tid, cid, data_type)))
                {
                    TBSYS_LOG(WARN, "add column desc failed ,tid[%ld], cid[%ld],ret[%d]",tid, cid,ret);
                    break;
                }
            }
        }
    }

    return ret;
}

int ObTransformer::gen_phy_values_idx(ObLogicalPlan *logical_plan,
                                      ObPhysicalPlan *physical_plan,
                                      ErrStat &err_stat,
                                      const ObInsertStmt *insert_stmt,
                                      ObRowDesc &row_desc,
                                      ObRowDescExt &row_desc_ext,
                                      const ObRowDesc &row_desc_idx,
                                      const ObRowDescExt &row_desc_ext_idx,
                                      const ObSEArray<int64_t, 64> *row_desc_map,
                                      ObExprValues &value_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    OB_ASSERT(logical_plan);
    OB_ASSERT(insert_stmt);
    ret = row_desc_intersect(row_desc, row_desc_ext, row_desc_idx, row_desc_ext_idx);
    value_op.set_row_desc(row_desc, row_desc_ext);
    int64_t num = insert_stmt->get_value_row_size();
    for (int64_t i = 0; ret == OB_SUCCESS && i < num; i++) // for each row
    {
        const ObArray<uint64_t>& value_row = insert_stmt->get_value_row(i);
        if (OB_UNLIKELY(0 == i))
        {
            value_op.reserve_values(num * value_row.count());
            FILL_TRACE_LOG("expr_values_count=%ld", num * value_row.count());
        }
        for (int64_t j = 0; ret == OB_SUCCESS && j < value_row.count(); j++)
        {
            ObSqlExpression val_expr;
            int64_t expr_idx = OB_INVALID_INDEX;
            if (NULL != row_desc_map)
            {
                OB_ASSERT(value_row.count() == row_desc_map->count());
                expr_idx = value_row.at(row_desc_map->at(j));
            }
            else
            {
                expr_idx = value_row.at(j);
            }
            ObSqlRawExpr *value_expr = logical_plan->get_expr(expr_idx);
            OB_ASSERT(NULL != value_expr);
            if (OB_SUCCESS != (ret = value_expr->fill_sql_expression(val_expr, this, logical_plan, physical_plan)))
            {
                TRANS_LOG("Failed to fill expr, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = value_op.add_value(val_expr)))
            {
                TRANS_LOG("Failed to add value into expr_values, err=%d", ret);
            }
        } // end for
        for(int64_t j = value_row.count(); j < row_desc.get_column_num(); j++)
        {
            uint64_t table_id = OB_INVALID_ID;
            uint64_t column_id = OB_INVALID_ID;
            if(OB_SUCCESS != (ret = row_desc.get_tid_cid(j, table_id, column_id)))
            {
                TBSYS_LOG(WARN, "get tid cid falied!ret = %d, idx = %ld", ret, j);
                break;
            }
            else
            {
                ObBinaryRefRawExpr col_expr(table_id, column_id, T_REF_COLUMN);
                ObSqlRawExpr col_raw_expr(
                            common::OB_INVALID_ID,
                            table_id,
                            column_id,
                            &col_expr);
                ObSqlExpression output_expr;
                if ((ret = col_raw_expr.fill_sql_expression(
                         output_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add table output columns failed");
                    break;
                }
                else if (OB_SUCCESS != (ret = value_op.add_value(output_expr)))
                {
                    TRANS_LOG("Failed to add cell into get param, err=%d", ret);
                    break;
                }
            }
        }
    } // end for
    return ret;
}
//add e
int ObTransformer::gen_phy_values(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const ObInsertStmt *insert_stmt,
        const ObRowDesc& row_desc,
        const ObRowDescExt& row_desc_ext,
        const ObSEArray<int64_t, 64> *row_desc_map,
        ObExprValues& value_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    OB_ASSERT(logical_plan);
    OB_ASSERT(insert_stmt);
    value_op.set_row_desc(row_desc, row_desc_ext);
    int64_t num = insert_stmt->get_value_row_size();
    for (int64_t i = 0; ret == OB_SUCCESS && i < num; i++) // for each row
    {
        const ObArray<uint64_t>& value_row = insert_stmt->get_value_row(i);
        if (OB_UNLIKELY(0 == i))
        {
            value_op.reserve_values(num * value_row.count());
            FILL_TRACE_LOG("expr_values_count=%ld", num * value_row.count());
        }
        for (int64_t j = 0; ret == OB_SUCCESS && j < value_row.count(); j++)
        {
            ObSqlExpression val_expr;
            int64_t expr_idx = OB_INVALID_INDEX;
            if (NULL != row_desc_map)
            {
                OB_ASSERT(value_row.count() == row_desc_map->count());
                expr_idx = value_row.at(row_desc_map->at(j));
            }
            else
            {
                expr_idx = value_row.at(j);
            }
            ObSqlRawExpr *value_expr = logical_plan->get_expr(expr_idx);
            OB_ASSERT(NULL != value_expr);
            if (OB_SUCCESS != (ret = value_expr->fill_sql_expression(val_expr, this, logical_plan, physical_plan)))
            {
                TRANS_LOG("Failed to fill expr, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = value_op.add_value(val_expr)))
            {
                TRANS_LOG("Failed to add value into expr_values, err=%d", ret);
            }
        } // end for
    } // end for
    return ret;
}

// merge tables' versions from inner physical plan to outer plan
int ObTransformer::merge_tables_version(ObPhysicalPlan & outer_plan,ObPhysicalPlan & inner_plan)
{
    int ret = OB_SUCCESS;
    if (&outer_plan != &inner_plan)
    {
        for (int64_t i = 0; i < inner_plan.get_base_table_version_count(); i++)
        {
            if ((ret = outer_plan.add_base_table_version(inner_plan.get_base_table_version(i))) != OB_SUCCESS)
            {
                TBSYS_LOG(WARN, "Failed to add %ldth base tables version, err=%d", i, ret);
                break;
            }
        }
    }
    return ret;
}

int ObTransformer::gen_physical_replace(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObInsertStmt *insert_stmt = NULL;
    ObPhysicalPlan* inner_plan = NULL;
    ObUpsModify *ups_modify = NULL;
    ObSEArray<int64_t, 64> row_desc_map;
    ObRowDesc row_desc;
    ObRowDescExt row_desc_ext;
    const ObRowkeyInfo *rowkey_info = NULL;

    if (OB_SUCCESS != (ret = wrap_ups_executor(physical_plan, query_id, inner_plan, index, err_stat)))
    {
        TBSYS_LOG(WARN, "err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, insert_stmt)))
    {
        TRANS_LOG("Fail to get statement");
    }
    else if (NULL == CREATE_PHY_OPERRATOR(ups_modify, ObUpsModify, inner_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(
                                ups_modify,
                                physical_plan == inner_plan ? index : NULL,
                                physical_plan != inner_plan)))
    {
        TRANS_LOG("Failed to add phy query, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = cons_row_desc(insert_stmt->get_table_id(), insert_stmt, row_desc_ext, row_desc, rowkey_info, row_desc_map, err_stat)))
    {
        TRANS_LOG("Failed to cons row desc, err=%d", ret);
    }
    else
    {
        uint64_t tid = insert_stmt->get_table_id();
        // check primary key columns
        uint64_t cid = OB_INVALID_ID;
        for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_info->get_size(); ++i)
        {
            if (OB_SUCCESS != (ret = rowkey_info->get_column_id(i, cid)))
            {
                TBSYS_LOG(USER_ERROR, "primary key can not be empty");
                ret = OB_ERR_INSERT_NULL_ROWKEY;
                break;
            }
            else if (OB_INVALID_INDEX == row_desc.get_idx(tid, cid))
            {
                TBSYS_LOG(USER_ERROR, "primary key can not be empty");
                ret = OB_ERR_INSERT_NULL_ROWKEY;
                break;
            }
        } // end for

        // add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            ret = column_null_check(logical_plan, insert_stmt, err_stat, OP_REPLACE);
        }
        // add 20131222:e

        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            // check column data type
            ObObj data_type;
            for (int i = 0; i < row_desc_ext.get_column_num(); ++i)
            {
                if (OB_SUCCESS != (ret = row_desc_ext.get_by_idx(i, tid, cid, data_type)))
                {
                    TBSYS_LOG(ERROR, "failed to get type, err=%d", ret);
                    ret = OB_ERR_UNEXPECTED;
                    break;
                }
                else if (data_type.get_type() == ObCreateTimeType
                         || data_type.get_type() == ObModifyTimeType)
                {
                    ret = OB_ERR_CREAT_MODIFY_TIME_COLUMN;
                    TRANS_LOG("Column of type ObCreateTimeType/ObModifyTimeType can not be inserted");
                    break;
                }
            } // end for
        }
    }
    FILL_TRACE_LOG("cons_row_desc");
    ObExprValues *value_op = NULL;
    if (ret == OB_SUCCESS)
    {
        if (OB_LIKELY(insert_stmt->get_insert_query_id() == OB_INVALID_ID))
        {
            CREATE_PHY_OPERRATOR(value_op, ObExprValues, inner_plan, err_stat);
            if (OB_SUCCESS != ret)
            {
            }
            else if ((ret = value_op->set_row_desc(row_desc, row_desc_ext)) != OB_SUCCESS)
            {
                TRANS_LOG("Set descriptor of value operator failed");
            }
            else if (OB_SUCCESS != (ret = gen_phy_values(logical_plan, inner_plan, err_stat, insert_stmt,
                                                         row_desc, row_desc_ext, &row_desc_map, *value_op)))
            {
                TRANS_LOG("Failed to gen expr values, err=%d", ret);
            }
            else
            {
                value_op->set_do_eval_when_serialize(true);
            }
            FILL_TRACE_LOG("gen_phy_values");
        }
        else
        {
            // replace ... select
            TRANS_LOG("REPLACE INTO ... SELECT is not supported yet");
            ret = OB_NOT_SUPPORTED;
        }
    }
    if (OB_SUCCESS == ret)
    {
        ObWhenFilter *when_filter_op = NULL;
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            if (insert_stmt->get_when_expr_size() > 0)
            {
                if ((ret = gen_phy_when(logical_plan,
                                        inner_plan,
                                        err_stat,
                                        query_id,
                                        *value_op,
                                        when_filter_op
                                        )) != OB_SUCCESS)
                {
                }
                else if ((ret = ups_modify->set_child(0, *when_filter_op)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
                }
            }
            else if ((ret = ups_modify->set_child(0, *value_op)) != OB_SUCCESS)
            {
                TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
            }
        }
    }
    if (OB_SUCCESS == ret)
    {
        // record table's schema version
        uint64_t tid = insert_stmt->get_table_id();
        const ObTableSchema *table_schema = NULL;
        if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(tid)))
        {
            ret = OB_ERR_ILLEGAL_ID;
            TRANS_LOG("fail to get table schema for table[%ld]", tid);
        }
        else if ((ret = physical_plan->add_base_table_version(
                      tid,
                      table_schema->get_schema_version()
                      )) != OB_SUCCESS)
        {
            TRANS_LOG("Failed to add table version into physical_plan, err=%d", ret);
        }
    }
    if (ret == OB_SUCCESS)
    {
        if ((ret = merge_tables_version(*physical_plan, *inner_plan)) != OB_SUCCESS)
        {
            TRANS_LOG("Failed to add base tables version, err=%d", ret);
        }
    }
    return ret;
}

//add liumz, [optimize replace index]20161110:b
bool ObTransformer::is_need_static_data_for_index(uint64_t main_tid)
{
  int ret = OB_SUCCESS;
  bool need_static_data = false;
  uint64_t index_tid_array[OB_MAX_INDEX_NUMS];
  uint64_t index_num = 0;

  const ObTableSchema *table_schema = NULL;
  const ObTableSchema *index_schema = NULL;
  const ObRowkeyInfo *rowkey_info = NULL;
  const ObRowkeyInfo *index_rowkey_info = NULL;

  if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(main_tid)))
  {
    ret = OB_ERR_ILLEGAL_ID;
    TBSYS_LOG(WARN, "fail to get table schema, tid=%ld", main_tid);
  }
  else if (NULL == (rowkey_info = &table_schema->get_rowkey_info()))
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "fail to get table rowkey info");
  }
  else if (OB_SUCCESS != (ret = sql_context_->schema_manager_->get_all_modifiable_index_tid(main_tid, index_tid_array, index_num)))
  {
    TBSYS_LOG(WARN, "fail to get index array for table[%ld], err=%d", main_tid, ret);
  }
  else if (index_num > 0 && index_num <= static_cast<uint64_t>(OB_MAX_INDEX_NUMS))
  {
    for (uint64_t idx = 0; idx < index_num && OB_SUCCESS == ret; idx++)
    {
      if (NULL == (index_schema = sql_context_->schema_manager_->get_table_schema(index_tid_array[idx])))
      {
        ret = OB_ERR_ILLEGAL_ID;
        TBSYS_LOG(WARN, "fail to get index schema, index tid=%ld", index_tid_array[idx]);
        break;
      }
      else if (NULL == (index_rowkey_info = &index_schema->get_rowkey_info()))
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "fail to get index rowkey info");
        break;
      }
      else
      {
        TBSYS_LOG(DEBUG, "LMZ rowkey_info:%ld         index_rowkey_info:%ld", rowkey_info->get_size(), index_rowkey_info->get_size());
        int64_t rowkey_col_num = index_rowkey_info->get_size();
        if (rowkey_col_num != rowkey_info->get_size())
        {
          need_static_data = true;
          break;
        }
      }
    }//end for
  }
  if (OB_SUCCESS != ret)
  {
    need_static_data = true;
  }
  TBSYS_LOG(DEBUG, "LMZ need_static_data[%d]", need_static_data);
  return need_static_data;
}
//add:e

int ObTransformer::gen_physical_replace_new(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObInsertStmt *insert_stmt = NULL;
    ObPhysicalPlan *inner_plan = NULL;
    ObUpsModifyWithDmlType *ups_modify = NULL;
    ObSEArray<int64_t, 64> row_desc_map;
    ObRowDesc row_desc;
    ObRowDesc row_desc_for_static_data;
    ObRowDescExt row_desc_ext;
    ObRowDescExt row_desc_ext_for_static_data;
    ObRowDesc main_desc;
    const ObRowkeyInfo *rowkey_info = NULL;
    bool is_need_modify_index = false;

    // first, if replace table has index
    if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, insert_stmt)))
    {
        TRANS_LOG("fail to get statement");
    }
    else
    {
        uint64_t main_tid = insert_stmt->get_table_id();
        if (sql_context_->schema_manager_->is_have_modifiable_index(main_tid))
        {
            is_need_modify_index = true;
        }
    }

    if (OB_SUCCESS == ret && OB_SUCCESS != (ret = wrap_ups_executor(physical_plan, query_id, inner_plan, index, err_stat)))
    {
        TBSYS_LOG(WARN, "err=%d", ret);
    }
    else if (NULL == CREATE_PHY_OPERRATOR(ups_modify, ObUpsModifyWithDmlType, inner_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(
                                ups_modify,
                                physical_plan == inner_plan ? index : NULL,
                                physical_plan != inner_plan)))
    {
        TRANS_LOG("failed to add phy query, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = cons_row_desc(insert_stmt->get_table_id(), insert_stmt, row_desc_ext, row_desc, rowkey_info, row_desc_map, err_stat)))
    {
        TRANS_LOG("fail to cons row desc, err=%d", ret);
    }
    else
    {
        ups_modify->set_dml_type(OB_DML_REPLACE);
        uint64_t tid = insert_stmt->get_table_id();
        uint64_t cid = OB_INVALID_ID;
        // if replace columns must include rowkey column
        for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_info->get_size(); ++i)
        {
            if (OB_SUCCESS != (ret = rowkey_info->get_column_id(i, cid)))
            {
                TBSYS_LOG(USER_ERROR, "primary key can not be empty");
                ret = OB_ERR_INSERT_NULL_ROWKEY;
                break;
            }
            else if (OB_INVALID_INDEX == row_desc.get_idx(tid, cid))
            {
                TBSYS_LOG(USER_ERROR, "primary key can not be empty");
                ret = OB_ERR_INSERT_NULL_ROWKEY;
                break;
            }
        }//end for

        // column NULL check
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            ret = column_null_check(logical_plan, insert_stmt, err_stat, OP_REPLACE);
        }

        // column type check, ObCreateTimeType & ObModifyTimeType must not replace
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            if (OB_LIKELY(OB_INVALID_ID == insert_stmt->get_insert_query_id()))
            {
                // check column data type
                ObObj data_type;
                for (int64_t i = 0; i < row_desc_ext.get_column_num(); ++i)
                {
                    if (OB_SUCCESS != (ret = row_desc_ext.get_by_idx(i, tid, cid, data_type)))
                    {
                        TBSYS_LOG(ERROR, "fail to get type, err=%d", ret);
                        ret = OB_ERR_UNEXPECTED;
                        break;
                    }
                    else if (ObCreateTimeType == data_type.get_type()
                             || ObModifyTimeType == data_type.get_type())
                    {
                        ret = OB_ERR_CREAT_MODIFY_TIME_COLUMN;
                        TRANS_LOG("column of type ObCreateTimeType/ObModifyTimeType can not be inserted");
                        break;
                    }
                }//end for
            }
            else
            {
                TRANS_LOG("REPLACE INTO ... SELECT is not supported yet");
                ret = OB_NOT_SUPPORTED;
            }
        }
    }// end else

    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        const ObTableSchema *data_table_schema = NULL;
        if (is_need_modify_index)// replace table has index, need modify index
        {
          //add liumz, [optimize replace index]20161110:b
          if (NULL == (data_table_schema = sql_context_->schema_manager_->get_table_schema(insert_stmt->get_table_id())))
          {
            ret = OB_ERR_ILLEGAL_ID;
            TRANS_LOG("fail to get table schema for table, table id=%ld", insert_stmt->get_table_id());
          }
          bool need_static_data = is_need_static_data_for_index(insert_stmt->get_table_id());
          ObIncScan *inc_scan = NULL;
          ObMultipleGetMerge *fuse_op = NULL;
          if (OB_SUCCESS == ret && need_static_data)
          {
          //add:e
            // construct table scan to get static data
            ObTableRpcScan *table_scan = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                ObRpcScanHint hint;
                hint.read_method_ = ObSqlReadStrategy::USE_GET;
                hint.is_get_skip_empty_row_ = false;
                hint.read_consistency_ = FROZEN;
                int64_t table_id = insert_stmt->get_table_id();
                CREATE_PHY_OPERRATOR(table_scan, ObTableRpcScan, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if(OB_SUCCESS != (ret = cons_row_desc_with_index(insert_stmt->get_table_id(), insert_stmt, row_desc_ext_for_static_data, row_desc_for_static_data,rowkey_info, err_stat)))
                {
                    TRANS_LOG("fail to cons row desc for static data, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = table_scan->set_table(table_id, table_id)))
                {
                    TRANS_LOG("fail to set table id, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = table_scan->init(sql_context_, &hint)))
                {
                    TRANS_LOG("fail to init table scan, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = gen_phy_static_data_scan_for_replace(logical_plan, inner_plan, err_stat,
                                                                                   insert_stmt, row_desc_for_static_data, row_desc_map,
                                                                                   table_id, *rowkey_info, *table_scan)))
                {
                    TRANS_LOG("err=%d", ret);
                }
                /* del liumz, [optimize replace index]20161110
                 * else if (NULL == (data_table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
                {
                    ret = OB_ERR_ILLEGAL_ID;
                    TRANS_LOG("fail to get table schema for table, table id=%ld", table_id);
                }*/
                else if (OB_SUCCESS != (ret = physical_plan->add_base_table_version(
                                            table_id,
                                            data_table_schema->get_schema_version())))
                {
                    TRANS_LOG("add base table version failed, table_id=%ld, err=%d", table_id, ret);
                }
                else
                {
                    table_scan->set_rowkey_cell_count(row_desc.get_rowkey_cell_count());
                    table_scan->set_cache_bloom_filter(true);
                }
            }
            // static data store into tmp table
            ObValues *tmp_table = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(tmp_table, ObValues, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if (OB_SUCCESS != (ret = tmp_table->set_child(0, *table_scan)))
                {
                    TBSYS_LOG(WARN, "fail to set child, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(tmp_table)))
                {
                    TBSYS_LOG(WARN, "fail to add phy query, err=%d", ret);
                }
            }

            ObMemSSTableScan *static_data = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(static_data, ObMemSSTableScan, inner_plan, err_stat);
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    static_data->set_tmp_table(tmp_table->get_id());
                }
            }
            // construct inc scan to get increased data
            //ObIncScan *inc_scan = NULL; //del liumz, [optimize replace index]20161110
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(inc_scan, ObIncScan, inner_plan, err_stat);
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    inc_scan->set_scan_type(ObIncScan::ST_MGET);
                    inc_scan->set_write_lock_flag();
                }
            }
            // merge static data and inc data
            //ObMultipleGetMerge *fuse_op = NULL; //del liumz, [optimize replace index]20161110
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(fuse_op, ObMultipleGetMerge, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if (OB_SUCCESS != (ret = fuse_op->set_child(0, *static_data)))
                {
                    TRANS_LOG("fail to set child of fuse_op operator, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = fuse_op->set_child(1, *inc_scan)))
                {
                    TRANS_LOG("fail to set child of fuse_op operator, err=%d", ret);
                }
                else
                {
                    fuse_op->set_is_ups_row(false);
                }
            }
          }
            // construct replace values from logical plan
            ObExprValues *input_values = NULL;
            ObExprValues *input_index_values = NULL;
            //ObRowDesc main_desc = row_desc;
            main_desc = row_desc;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(input_values, ObExprValues, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(input_values)))
                {
                    TBSYS_LOG(WARN, "fail to add phy query, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = input_values->set_row_desc(row_desc, row_desc_ext)))
                {
                    TRANS_LOG("fail to set descriptor of values operator, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = gen_phy_values(logical_plan, inner_plan, err_stat, insert_stmt,
                                                             row_desc, row_desc_ext,&row_desc_map, *input_values)))
                {
                    TRANS_LOG("fail to generate values, err=%d", ret);
                }
                else
                {    //modify by hushuang [Secondary Index] for replace bug:20161108
                     //input_values->set_check_rowkey_duplicate(true);
                    input_values->set_check_rowkey_duplicate_rep(true);

                }
            }

            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(input_index_values, ObExprValues, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(input_index_values)))
                {
                    TBSYS_LOG(WARN, "fail to add phy query, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = gen_phy_values_idx(logical_plan, inner_plan, err_stat, insert_stmt,
                                                                 row_desc, row_desc_ext, row_desc_for_static_data, row_desc_ext_for_static_data, &row_desc_map, *input_index_values)))
                {
                    TRANS_LOG("fail to generate values, err=%d", ret);
                }
                else
                {
                    //modify by hushuang [Secondary Index] for replace bug:20161108
                    //input_index_values->set_check_rowkey_duplicate(true);
                    input_index_values->set_check_rowkey_duplicate_rep(true);
                }
            }

            ObIndexTriggerRep *index_trigger_rep = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(index_trigger_rep, ObIndexTriggerRep, inner_plan, err_stat);
                //add liumz, [optimize replace index]20161110:b
                if (need_static_data)
                {
                //add:e
                  if (OB_UNLIKELY(OB_SUCCESS != ret))
                  {
                  }
                  else if (OB_SUCCESS != (ret = index_trigger_rep->set_child(0, *fuse_op)))
                  {
                    TRANS_LOG("fail to set child of index_trigger_rep operator, err=%d", ret);
                  }
                  else
                  {
                    inc_scan->set_values(input_index_values->get_id(), false);
                    index_trigger_rep->set_input_values(input_values->get_id());
                    index_trigger_rep->set_input_index_values(input_index_values->get_id());
                    //add wenghaixing [secondary index replace.fix_replace_bug]20150422
                    index_trigger_rep->set_data_max_cid(data_table_schema->get_max_column_id());
                    index_trigger_rep->set_main_desc(main_desc);
                    //add
                    //index_trigger_rep->set_main_desc(main_desc);
                  }
                //add liumz, [optimize replace index]20161110:b
                }
                else
                {
                  if (OB_UNLIKELY(OB_SUCCESS != ret))
                  {
                  }
                  else if (OB_SUCCESS != (ret = index_trigger_rep->set_child(0, *input_index_values)))
                  {
                    TRANS_LOG("fail to set child of index_trigger_rep operator, err=%d", ret);
                  }
                  else
                  {
                    //inc_scan->set_values(input_index_values->get_id(), false);
                    index_trigger_rep->set_input_values(input_values->get_id());
                    index_trigger_rep->set_input_index_values(input_index_values->get_id());
                    //add wenghaixing [secondary index replace.fix_replace_bug]20150422
                    index_trigger_rep->set_data_max_cid(data_table_schema->get_max_column_id());
                    index_trigger_rep->set_main_desc(main_desc);
                    //add
                    //index_trigger_rep->set_main_desc(main_desc);
                  }
                }
                //add:e
            }

            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                uint64_t index_tid_array[OB_MAX_INDEX_NUMS];
                uint64_t index_num = 0;

                const ObTableSchema *index_schema = NULL;
                const ObRowkeyInfo *rowkey_info = NULL;
                const ObColumnSchemaV2 *index_column_schema = NULL;

                uint64_t main_tid = insert_stmt->get_table_id();

                if (OB_SUCCESS != (ret = sql_context_->schema_manager_->get_all_modifiable_index_tid(main_tid, index_tid_array, index_num)))
                {
                    TBSYS_LOG(WARN, "fail to get index array for table[%ld], err=%d", main_tid, ret);
                }
                else if (index_num > 0 && index_num <= static_cast<uint64_t>(OB_MAX_INDEX_NUMS))
                {
                    index_trigger_rep->set_index_num(index_num);
                    index_trigger_rep->set_main_tid(insert_stmt->get_table_id());
                    for (uint64_t idx = 0; idx < index_num; idx++)
                    {
                        if (NULL == (index_schema = sql_context_->schema_manager_->get_table_schema(index_tid_array[idx])))
                        {
                            ret = OB_ERR_ILLEGAL_ID;
                            TBSYS_LOG(ERROR, "fail to get index schema, index tid=%ld", index_tid_array[idx]);
                            break;
                        }
                        else if (NULL == (rowkey_info = &index_schema->get_rowkey_info()))
                        {
                            ret = OB_ERROR;
                            TBSYS_LOG(WARN, "fail to get index rowkey info");
                            break;
                        }
                        else
                        {
                            uint64_t index_cid = OB_INVALID_ID;
                            int64_t rowkey_col_num = rowkey_info->get_size();
                            uint64_t max_column_id = index_schema->get_max_column_id();

                            ObRowDesc index_del_row_desc;
                            ObRowDesc index_ins_row_desc;
                            index_del_row_desc.reset();
                            index_ins_row_desc.reset();
                            index_del_row_desc.set_rowkey_cell_count(rowkey_col_num);
                            index_ins_row_desc.set_rowkey_cell_count(rowkey_col_num);
                            for (int64_t i = 0; i < rowkey_col_num; i++)// rowkey columns ==> row desc
                            {
                                if (OB_SUCCESS != (ret = rowkey_info->get_column_id(i, index_cid)))
                                {
                                    TBSYS_LOG(WARN, "fail to get column id");
                                    ret = OB_ERROR;
                                    break;
                                }
                                else if (OB_SUCCESS != (ret = index_del_row_desc.add_column_desc(index_tid_array[idx], index_cid))) // rowkey column ==> index delete row desc
                                {
                                    TBSYS_LOG(WARN, "fail to add column desc, err=%d", ret);
                                }
                                else if (OB_SUCCESS != (ret = index_ins_row_desc.add_column_desc(index_tid_array[idx], index_cid))) // rowkey column ==> index insert row desc
                                {
                                    TBSYS_LOG(WARN, "fail to add column desc, err=%d", ret);
                                }
                            }//end for
                            for (index_cid = OB_APP_MIN_COLUMN_ID; OB_SUCCESS == ret && index_cid <= max_column_id; index_cid++)// other columns ==> row desc
                            {
                                if (NULL == (index_column_schema = sql_context_->schema_manager_->get_column_schema(index_tid_array[idx], index_cid)))
                                {
                                    TBSYS_LOG(DEBUG, "fail to get column schema, index tid=%lu, cid=%lu", index_tid_array[idx], index_cid);
                                }
                                else if (rowkey_info->is_rowkey_column(index_cid))
                                {
                                    TBSYS_LOG(DEBUG, "column in index is rowkey column, cid=%lu", index_cid);
                                }
                                else if (OB_SUCCESS != (ret = index_ins_row_desc.add_column_desc(index_tid_array[idx], index_cid))) // other column ==> index insert row desc
                                {
                                    TBSYS_LOG(WARN, "fail to add column desc, err=%d", ret);
                                    break;
                                }
                            }//end for
                            //add wenghaixing [secondary index alter_table_debug]20150611
                            if(OB_SUCCESS == ret && sql_context_->schema_manager_->is_index_has_storing(index_tid_array[idx]))
                            {
                                ret = index_ins_row_desc.add_column_desc(index_tid_array[idx], OB_INDEX_VIRTUAL_COLUMN_ID);
                            }
                            //add e
                            if (OB_SUCCESS ==ret)
                            {
                                if (OB_SUCCESS != (ret = index_del_row_desc.add_column_desc(index_tid_array[idx], OB_ACTION_FLAG_COLUMN_ID)))// OB_ACTION_FLAG_COLUMN_ID ==> index delete row desc
                                {
                                    TBSYS_LOG(WARN, "fail to add column desc, err=%d", ret);
                                }
                                else if (OB_SUCCESS != (ret = index_trigger_rep->add_index_del_row_desc(idx, index_del_row_desc)))
                                {
                                    TBSYS_LOG(WARN, "fail to add index del row desc, err=%d", ret);
                                }
                                else if (OB_SUCCESS != (ret = index_trigger_rep->add_index_ins_row_desc(idx, index_ins_row_desc)))
                                {
                                    TBSYS_LOG(WARN, "fail to add index ins row desc, err=%d", ret);
                                }
                            }
                        }
                    }
                }
                else
                {
                    TBSYS_LOG(WARN, "illegal index num[%ld]", index_num);
                    ret = OB_INVALID_ARGUMENT;
                }
            }

            ObWhenFilter *when_filter_op = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                if (insert_stmt->get_when_expr_size() > 0)
                {
                    if (OB_SUCCESS != (ret = gen_phy_when(logical_plan,
                                                          inner_plan,
                                                          err_stat,
                                                          query_id,
                                                          *index_trigger_rep,
                                                          when_filter_op)))
                    {
                    }
                    else if (OB_SUCCESS != (ret = ups_modify->set_child(0, *when_filter_op)))
                    {
                        TRANS_LOG("fail to set child of ups_modify operator, err=%d", ret);
                    }
                }
                else
                {
                    if (OB_SUCCESS != (ret = ups_modify->set_child(0, *index_trigger_rep)))
                    {
                        TRANS_LOG("fail to set child of ups_modify operator, err=%d", ret);
                    }
                }
            }
        }
        else// no index, only replace into main table
        {
            ObExprValues *value_op = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(value_op, ObExprValues, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if (OB_SUCCESS != (ret = value_op->set_row_desc(row_desc, row_desc_ext)))
                {
                    TRANS_LOG("fail to set descriptor of values operator, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = gen_phy_values(logical_plan, inner_plan, err_stat, insert_stmt,
                                                             row_desc, row_desc_ext, &row_desc_map, *value_op)))
                {
                    TRANS_LOG("fail to generate values, err=%d", ret);
                }
                else
                {
                    value_op->set_do_eval_when_serialize(true);
                }
                FILL_TRACE_LOG("gen_phy_values");
            }
            ObWhenFilter *when_filter_op = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                if (insert_stmt->get_when_expr_size() > 0)
                {
                    if (OB_SUCCESS != (ret = gen_phy_when(logical_plan,
                                                          inner_plan,
                                                          err_stat,
                                                          query_id,
                                                          *value_op,
                                                          when_filter_op)))
                    {
                    }
                    else if (OB_SUCCESS != (ret = ups_modify->set_child(0, *when_filter_op)))
                    {
                        TRANS_LOG("fail to set child of ups_modify operator, err=%d", ret);
                    }
                }
                else if (OB_SUCCESS != (ret = ups_modify->set_child(0, *value_op)))
                {
                    TRANS_LOG("fail to set child of ups_modify operator, err=%d", ret);
                }
            }

            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                // reocrd table's schema version
                uint64_t tid = insert_stmt->get_table_id();
                const ObTableSchema *table_schema = NULL;
                if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(tid)))
                {
                    ret = OB_ERR_ILLEGAL_ID;
                    TRANS_LOG("fail to get table schema for table, table id=%lu", tid);
                }
                else if (OB_SUCCESS != (ret = physical_plan->add_base_table_version(
                                            tid,
                                            table_schema->get_schema_version()
                                            )))
                {
                    TRANS_LOG("fail to add table version into physical_plan, err=%d", ret);
                }
            }
        }

        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            if (OB_SUCCESS != (ret = merge_tables_version(*physical_plan, *inner_plan)))
            {
                TRANS_LOG("fail to add base tables version, err=%d", ret);
            }
        }
    }

    return ret;
}
// add:e

int ObTransformer::gen_physical_delete(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObDeleteStmt *delete_stmt = NULL;
    ObDelete     *delete_op = NULL;

    /* get statement */
    if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, delete_stmt)))
    {
        TRANS_LOG("Fail to get statement");
    }

    /* generate operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(delete_op, ObDelete, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, delete_stmt, delete_op, index);
        }
    }

    ObRowDescExt row_desc_ext;
    const ObTableSchema *table_schema = NULL;
    if (OB_SUCCESS == ret)
    {
        if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(delete_stmt->get_delete_table_id())))
        {
            ret = OB_ERROR;
            TRANS_LOG("Fail to get table schema for table[%ld]", delete_stmt->get_delete_table_id());
        }
    }
    if (ret == OB_SUCCESS)
    {
        delete_op->set_table_id(delete_stmt->get_delete_table_id());
        delete_op->set_rpc_stub(sql_context_->merger_rpc_proxy_);
        delete_op->set_rowkey_info(table_schema->get_rowkey_info());
        int32_t num = delete_stmt->get_column_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            const ColumnItem* column_item = delete_stmt->get_column_item(i);
            if (column_item == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Get column item failed");
                break;
            }
            const ObColumnSchemaV2* column_schema = sql_context_->schema_manager_->get_column_schema(
                        column_item->table_id_, column_item->column_id_);
            if (NULL == column_schema)
            {
                ret = OB_ERR_COLUMN_NOT_FOUND;
                TRANS_LOG("Get column item failed");
                break;
            }
            ObObj data_type;
            data_type.set_type(column_schema->get_type());
            ret = row_desc_ext.add_column_desc(column_item->table_id_, column_item->column_id_, data_type);
            if (ret != OB_SUCCESS)
            {
                TRANS_LOG("Add column '%.*s' to descriptor failed",
                          column_item->column_name_.length(), column_item->column_name_.ptr());
                break;
            }
        }
        if (ret == OB_SUCCESS && (ret = delete_op->set_columns_desc(row_desc_ext)) != OB_SUCCESS)
        {
            TRANS_LOG("Set descriptor of delete operator failed");
        }
    }

    if (ret == OB_SUCCESS)
    {
        if (OB_UNLIKELY(delete_stmt->get_delete_table_id() == OB_INVALID_ID))
        {
            ret = OB_NOT_INIT;
            TRANS_LOG("table is not given in delete statment. check syntax");
        }
        else
        {
            ObPhyOperator *table_op = NULL;
            if ((ret = gen_phy_table(
                     logical_plan,
                     physical_plan,
                     err_stat,
                     delete_stmt,
                     delete_stmt->get_delete_table_id(),
                     table_op)) == OB_SUCCESS
                    && NULL != table_op
                    && (ret = delete_op->set_child(0, *table_op)) == OB_SUCCESS)
            {
                // success
            }
            else
            {
                TRANS_LOG("Set child of delete operator failed");
            }
        }
    }

    return ret;
}

int ObTransformer::gen_physical_update(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObUpdateStmt *update_stmt = NULL;
    ObUpdate     *update_op = NULL;
    uint64_t table_id = OB_INVALID_ID;
    int64_t column_idx = 0;
    uint64_t column_id = OB_INVALID_ID;
    uint64_t expr_id = OB_INVALID_ID;
    ObSqlExpression expr;
    ObSqlRawExpr *raw_expr = NULL;
    const ObTableSchema *table_schema = NULL;
    const ObColumnSchemaV2* column_schema = NULL;
    ObObj data_type;
    ObRowDescExt row_desc_ext;

    /* get statement */
    if (ret == OB_SUCCESS)
    {
        get_stmt(logical_plan, err_stat, query_id, update_stmt);
    }
    /* generate operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(update_op, ObUpdate, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, update_stmt, update_op, index);
        }
    }

    /* init update op param */
    /* set table id and other stuff, only support update single table now */
    if (ret == OB_SUCCESS)
    {
        if (OB_INVALID_ID == (table_id = update_stmt->get_update_table_id()))
        {
            ret = OB_ERR_ILLEGAL_ID;
            TRANS_LOG("Get update statement table ID error");
        }
        else if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
        {
            ret = OB_ERR_ILLEGAL_ID;
            TRANS_LOG("Fail to get table schema for table[%ld]", table_id);
        }
    }

    if (ret == OB_SUCCESS)
    {
        update_op->set_table_id(table_id);
        update_op->set_rpc_stub(sql_context_->merger_rpc_proxy_);
        update_op->set_rowkey_info(table_schema->get_rowkey_info());
    }
    if (ret == OB_SUCCESS)
    {
        // construct row desc ext
        int32_t num = update_stmt->get_column_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            const ColumnItem* column_item = update_stmt->get_column_item(i);
            if (column_item == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Get column item failed");
                break;
            }
            const ObColumnSchemaV2* column_schema = sql_context_->schema_manager_->get_column_schema(
                        column_item->table_id_, column_item->column_id_);
            if (NULL == column_schema)
            {
                ret = OB_ERR_COLUMN_NOT_FOUND;
                TRANS_LOG("Get column item failed");
                break;
            }
            else if (column_schema->get_type() == ObCreateTimeType || column_schema->get_type() == ObModifyTimeType)
            {
                ret = OB_ERR_CREAT_MODIFY_TIME_COLUMN;
                TRANS_LOG("Column '%s' of type ObCreateTimeType/ObModifyTimeType can not be updated",
                          column_schema->get_name());
                break;
            }
            data_type.set_type(column_schema->get_type());
            ret = row_desc_ext.add_column_desc(column_item->table_id_, column_item->column_id_, data_type);
            if (ret != OB_SUCCESS)
            {
                TRANS_LOG("Add column '%.*s' to descriptor faild",
                          column_item->column_name_.length(), column_item->column_name_.ptr());
                break;
            }
        }
        if (ret == OB_SUCCESS && (ret = update_op->set_columns_desc(row_desc_ext)) != OB_SUCCESS)
        {
            TRANS_LOG("Set ext descriptor of update operator failed");
        }
    }
    /* fill column=expr pairs to update operator */
    if (OB_SUCCESS == ret)
    {
        for (column_idx = 0; column_idx < update_stmt->get_update_column_count(); column_idx++)
        {
            expr.reset();
            // valid check
            // 1. rowkey can't be updated
            // 2. joined column can't be updated
            if (OB_SUCCESS != (ret = update_stmt->get_update_column_id(column_idx, column_id)))
            {
                TBSYS_LOG(WARN, "fail to get update column id for table %lu column_idx=%lu", table_id, column_idx);
                break;
            }
            else if (NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(table_id, column_id)))
            {
                ret = OB_ERR_COLUMN_NOT_FOUND;
                TRANS_LOG("Get column item failed");
                break;
            }
            else if (true == column_schema->is_join_column())
            {
                ret = OB_ERR_UPDATE_JOIN_COLUMN;
                TRANS_LOG("join column '%s' can not be updated", column_schema->get_name());
                break;
            }
            else if (table_schema->get_rowkey_info().is_rowkey_column(column_id))
            {
                ret = OB_ERR_UPDATE_ROWKEY_COLUMN;
                TRANS_LOG("rowkey column '%s' can not be updated", column_schema->get_name());
                break;
            }

            // get expression
            if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = update_stmt->get_update_expr_id(column_idx, expr_id))))
            {
                TBSYS_LOG(WARN, "fail to get update expr for table %lu column %lu. column_idx=%ld", table_id, column_id, column_idx);
                break;
            }
            else if (NULL == (raw_expr = logical_plan->get_expr(expr_id)))
            {
                TBSYS_LOG(WARN, "fail to get expr from logical plan for table %lu column %lu. column_idx=%ld", table_id, column_id, column_idx);
                ret = OB_ERR_UNEXPECTED;
                break;
            }
            else if (OB_SUCCESS != (ret = raw_expr->fill_sql_expression(expr, this, logical_plan, physical_plan)))
            {
                TBSYS_LOG(WARN, "fail to fill sql expression. ret=%d", ret);
                break;
            }
            // add <column_id, expression> to update operator
            else if (OB_SUCCESS != (ret = update_op->add_update_expr(column_id, expr)))
            {
                TBSYS_LOG(WARN, "fail to add update expr to update operator");
                break;
            }
        }
    }
    if (OB_SUCCESS == ret)
    {
        ObPhyOperator *table_op = NULL;
        if ((ret = gen_phy_table(
                 logical_plan,
                 physical_plan,
                 err_stat,
                 update_stmt,
                 table_id,
                 table_op)) == OB_SUCCESS
                && NULL != table_op
                && (ret = update_op->set_child(0, *table_op)) == OB_SUCCESS)
        {
            // success
        }
        else
        {
            TRANS_LOG("Set child of update operator failed");
        }
    }

    return ret;
}

int ObTransformer::gen_physical_explain(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObExplainStmt *explain_stmt = NULL;
    ObExplain     *explain_op = NULL;
    if (ret == OB_SUCCESS)
    {
        get_stmt(logical_plan, err_stat, query_id, explain_stmt);
    }
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(explain_op, ObExplain, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, explain_stmt, explain_op, index);
        }
    }

    int32_t idx = OB_INVALID_INDEX;
    if (ret == OB_SUCCESS)
    {
        ret = generate_physical_plan(
                    logical_plan,
                    physical_plan,
                    err_stat,
                    explain_stmt->get_explain_query_id(),
                    &idx);
    }
    if (ret == OB_SUCCESS)
    {

        //add by wanglei [show semi join physical plan]:b
        //TBSYS_LOG(ERROR,"wanglei:test explain_stmt->is_semi_join () = [%d]",explain_stmt->is_semi_join ());
        /*if(explain_stmt->is_semi_join ())
            explain_op->set_semi_join (true);*/
        //add :e
        ObPhyOperator* op = physical_plan->get_phy_query(idx);
        if ((ret = explain_op->set_child(0, *op)) != OB_SUCCESS)
            TRANS_LOG("Set child of Explain Operator failed");
    }

    return ret;
}

bool ObTransformer::check_join_column(const int32_t column_index,
                                      const char* column_name, const char* join_column_name,
                                      TableSchema& schema, const ObTableSchema& join_table_schema)
{
    bool parse_ok = true;
    uint64_t join_column_id = 0;

    const ColumnSchema* cs = schema.get_column_schema(column_name);
    const ObColumnSchemaV2* jcs = sql_context_->schema_manager_->get_column_schema(join_table_schema.get_table_name(), join_column_name);

    if (NULL == cs || NULL == jcs)
    {
        TBSYS_LOG(ERROR, "column(%s,%s) not a valid column.", column_name, join_column_name);
        parse_ok = false;
    }
    else if (cs->data_type_ != jcs->get_type())
    {
        //the join should be happen between too columns have the same type
        TBSYS_LOG(ERROR, "join column have different types (%s,%d), (%s,%d) ",
                  column_name, cs->data_type_, join_column_name, jcs->get_type());
        parse_ok = false;
    }
    else if (OB_SUCCESS != join_table_schema.get_rowkey_info().get_column_id(column_index, join_column_id))
    {
        TBSYS_LOG(ERROR, "join table (%s) has not rowkey column on index(%d)",
                  join_table_schema.get_table_name(), column_index);
        parse_ok = false;
    }
    else if (join_column_id != jcs->get_id())
    {
        TBSYS_LOG(ERROR, "join column(%s,%ld) not match join table rowkey column(%ld)",
                  join_table_schema.get_table_name(), jcs->get_id(), join_column_id);
        parse_ok = false;
    }

    if (parse_ok)
    {
        int64_t rowkey_idx = -1;
        if (OB_SUCCESS == schema.get_column_rowkey_index(cs->column_id_, rowkey_idx))
        {
            if (-1 == rowkey_idx)
            {
                TBSYS_LOG(ERROR, "left column (%s,%lu) not a rowkey column of left table(%s)",
                          column_name, cs->column_id_, schema.table_name_);
                parse_ok = false;
            }
        }
        else
        {
            TBSYS_LOG(WARN, "fail to get column rowkey index");
            parse_ok = false;
        }
    }
    return parse_ok;
}

bool ObTransformer::parse_join_info(const ObString &join_info_str, TableSchema &table_schema)
{
    bool parse_ok = true;
    char *str = NULL;
    std::vector<char*> node_list;
    const ObTableSchema *table_joined = NULL;
    uint64_t table_id_joined = OB_INVALID_ID;

    char *s = NULL;
    int len = 0;
    char *p = NULL;
    str = strndup(join_info_str.ptr(), join_info_str.length());
    s = str;
    len = static_cast<int32_t>(strlen(s));

    // str like [r1$jr1,r2$jr2]%joined_table_name:f1$jf1,f2$jf2,...
    if (*s != '[')
    {
        TBSYS_LOG(ERROR, "join info (%s) incorrect, first character must be [", str);
        parse_ok = false;
    }
    else
    {
        ++s;
    }

    if (parse_ok)
    {
        // find another bracket
        p = strchr(s, ']');
        if (NULL == p)
        {
            TBSYS_LOG(ERROR, "join info (%s) incorrect, cannot found ]", str);
            parse_ok = false;
        }
        else
        {
            // s now be the join rowkey columns array.
            *p = '\0';
        }
    }

    if (parse_ok)
    {
        node_list.clear();
        s = str_trim(s);
        tbsys::CStringUtil::split(s, ",", node_list);
        if (node_list.empty())
        {
            TBSYS_LOG(ERROR, "join info (%s) incorrect, left join columns not exist.", str);
            parse_ok = false;
        }
        else
        {
            // skip join rowkey columns string, now s -> %joined_table_name:f1$jf1...
            s = p + 1;
        }
    }

    if (parse_ok && *s != '%')
    {
        TBSYS_LOG(ERROR, "%s format error, should be rowkey", str);
        parse_ok = false;
    }

    if (parse_ok)
    {
        // skip '%', find join table name.
        s++;
        p = strchr(s, ':');
        if (NULL == p)
        {
            TBSYS_LOG(ERROR, "%s format error, could not find ':'", str);
            parse_ok = false;
        }
        else
        {
            // now s is the joined table name.
            *p = '\0';
        }
    }

    if (parse_ok)
    {
        table_joined = sql_context_->schema_manager_->get_table_schema(s);
        if (NULL != table_joined)
        {
            table_id_joined = table_joined->get_table_id();
        }

        if (NULL == table_joined || table_id_joined == OB_INVALID_ID)
        {
            TBSYS_LOG(ERROR, "%s table not exist ", s);
            parse_ok = false;
        }
    }

    // parse join rowkey columns.
    if (parse_ok)
    {
        char* cp = NULL;
        for(uint32_t i = 0; parse_ok && i < node_list.size(); ++i)
        {
            cp = strchr(node_list[i], '$');
            if (NULL == cp)
            {
                TBSYS_LOG(ERROR, "error can not find '$' (%s) ", node_list[i]);
                parse_ok = false;
                break;
            }
            else
            {
                *cp = '\0';
                ++cp;
                // now node_list[i] is left column, cp is join table rowkey column;
                parse_ok = check_join_column(i, node_list[i], cp, table_schema, *table_joined);
                if (parse_ok)
                {
                    JoinInfo join_info;

                    strncpy(join_info.left_table_name_, table_schema.table_name_, OB_MAX_TABLE_NAME_LENGTH);
                    join_info.left_table_name_[OB_MAX_TABLE_NAME_LENGTH - 1] = '\0';
                    join_info.left_table_id_ = table_schema.table_id_;

                    strncpy(join_info.left_column_name_, node_list[i], OB_MAX_COLUMN_NAME_LENGTH);
                    join_info.left_column_name_[OB_MAX_COLUMN_NAME_LENGTH - 1] = '\0';
                    join_info.left_column_id_ = table_schema.get_column_schema(node_list[i])->column_id_;

                    strncpy(join_info.right_table_name_, table_joined->get_table_name(), OB_MAX_TABLE_NAME_LENGTH);
                    join_info.right_table_name_[OB_MAX_TABLE_NAME_LENGTH - 1] = '\0';
                    join_info.right_table_id_ = table_joined->get_table_id();

                    strncpy(join_info.right_column_name_, cp, OB_MAX_COLUMN_NAME_LENGTH);
                    join_info.right_column_name_[OB_MAX_COLUMN_NAME_LENGTH - 1] = '\0';
                    join_info.right_column_id_ = sql_context_->schema_manager_->get_column_schema(table_joined->get_table_name(), cp)->get_id();
                    if (OB_SUCCESS != table_schema.join_info_.push_back(join_info))
                    {
                        parse_ok = false;
                        TBSYS_LOG(WARN, "fail to push join info");
                    }
                    else
                    {
                        TBSYS_LOG(DEBUG, "add join info [%s]", to_cstring(join_info));
                    }
                }
            }
        }
    }

    // parse join columns
    if (parse_ok)
    {
        s = p + 1;
        s = str_trim(s);
        node_list.clear();
        tbsys::CStringUtil::split(s, ",", node_list);
        if (node_list.empty())
        {
            TBSYS_LOG(ERROR, "%s can not find correct info", str);
            parse_ok = false;
        }
    }

    uint64_t ltable_id = OB_INVALID_ID;

    if (parse_ok)
    {
        ltable_id = table_schema.table_id_;
        char *fp = NULL;
        for(uint32_t i = 0; parse_ok && i < node_list.size(); ++i)
        {
            fp = strchr(node_list[i], '$');
            if (NULL == fp)
            {
                TBSYS_LOG(ERROR, "error can not find '$' %s ", node_list[i]);
                parse_ok = false;
                break;
            }
            *fp = '\0';
            fp++;

            const ObColumnSchemaV2 * right_column_schema = NULL;
            ColumnSchema *column_schema = table_schema.get_column_schema(node_list[i]);
            if (NULL == column_schema)
            {
                TBSYS_LOG(WARN, "column %s is not valid", node_list[i]);
                parse_ok = false;
            }
            else if (NULL == (right_column_schema = sql_context_->schema_manager_->get_column_schema(table_joined->get_table_name(), fp)))
            {
                TBSYS_LOG(WARN, "column %s is not valid", fp);
                parse_ok = false;
            }
            else
            {
                column_schema->join_table_id_ = table_id_joined;
                column_schema->join_column_id_ = right_column_schema->get_id();
                TBSYS_LOG(DEBUG, "column schema join_table_id[%lu], join_column_id[%lu]", column_schema->join_table_id_, column_schema->join_column_id_);
            }
        }
    }
    free(str);
    str = NULL;
    return parse_ok;
}
//add gaojt [Delete_Update_Function] [JHOBv0.1] 20150817:b
/*
 * This function is used to instead of original function:gen_phy_table_for_update
 * Now only used to delete and update statement,but not select...for update
 * The diff with original is:
 * 1.modify where condition to select type's where condition
 * 2.add another table_rpc_scan operator to get static date according to where condition
 * 3.make these rows fill into ObExprValues
 */
int ObTransformer::gen_phy_table_for_update_new(ObLogicalPlan *logical_plan,
                                                ObPhysicalPlan*& physical_plan,
                                                ErrStat& err_stat,
                                                ObStmt *stmt,
                                                uint64_t table_id,
                                                const ObRowkeyInfo &rowkey_info,
                                                const ObRowDesc &row_desc,
                                                const ObRowDescExt &row_desc_ext,
                                                ObPhyOperator*& table_op
                                                , bool is_delete_update
                                                //add lijianqiang [sequence] 20150909:b
                                                , ObPhyOperator* sequence_op
                                                //add 20150909:e
                                                , bool is_column_hint_index
                                                )
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    TableItem* table_item = NULL;
    ObTableRpcScan *table_rpc_scan_for_obvalues = NULL;
    ObIncScan *inc_scan_op = NULL;
    ObMultipleGetMerge *fuse_op = NULL;
    ObMemSSTableScan *static_data = NULL;
    ObValues *tmp_table = NULL;
    ObExprValues* get_param_values = NULL;
    ObFillValues* fill_values = NULL;
    const ObTableSchema *table_schema = NULL;
    ObRpcScanHint hint_values;
    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
    ObDataMarkParam data_mark_param;
    //add duyr 20160531:e

    if (table_id == OB_INVALID_ID
            || (table_item = stmt->get_table_item_by_id(table_id)) == NULL
            || TableItem::BASE_TABLE != table_item->type_)
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("Wrong table id, tid=%lu", table_id);
    }
    else
    {
        hint_values.read_method_ = ObSqlReadStrategy::USE_GET;
        hint_values.read_consistency_ = STRONG;
        hint_values.is_get_skip_empty_row_ = false;
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        // add by maosy [Delete_Update_Function_isolation_RC] 20161218
    //if (stmt->get_query_hint().is_parallal_&&!is_multi_batch_)
        if (stmt->get_query_hint().is_parallal_)
    //add e
        {
            uint64_t max_used_cid = 0;
            if (OB_SUCCESS != (ret = get_table_max_used_cid(sql_context_,
                                                            table_item->table_id_,
                                                            max_used_cid)))
            {
                TBSYS_LOG(WARN,"fail to get max used cid[%ld],tid[%ld],ret=%d",
                          max_used_cid,table_item->table_id_,ret);
            }
            else
            {
                hint_values.read_consistency_         = STRONG;
                data_mark_param.need_modify_time_     = true;
                data_mark_param.need_major_version_   = false;
                data_mark_param.need_minor_version_   = false;
                data_mark_param.need_data_store_type_ = false;
                data_mark_param.modify_time_cid_      = ++max_used_cid;
                data_mark_param.major_version_cid_    = ++max_used_cid;
                data_mark_param.minor_ver_start_cid_  = ++max_used_cid;
                data_mark_param.minor_ver_end_cid_    = ++max_used_cid;
                data_mark_param.data_store_type_cid_  = ++max_used_cid;
                data_mark_param.table_id_             = table_item->table_id_;
                if (!data_mark_param.is_valid())
                {
                    ret = OB_ERR_UNEXPECTED;
                    TBSYS_LOG(ERROR,"invalid data mark param[%s]!ret=%d",
                              to_cstring(data_mark_param),ret);
                }
            }
        }
        //add duyr 20160531:e

    }
    if(OB_SUCCESS == ret)
    {
        if (NULL == CREATE_PHY_OPERRATOR(table_rpc_scan_for_obvalues, ObTableRpcScan, physical_plan, err_stat))
        {
            TRANS_LOG("table_rpc_scan_for_obvalues is NULL");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if ((ret = table_rpc_scan_for_obvalues->set_table(table_item->table_id_, table_item->ref_id_)) != OB_SUCCESS)
        {
            TRANS_LOG("table_rpc_scan_for_obvalues set table failed");
        }
        else if (OB_SUCCESS != (ret = table_rpc_scan_for_obvalues->init(sql_context_, &hint_values)))
        {
            TRANS_LOG("table_rpc_scan_for_obvalues init failed");
        }
        // del by maosy [Delete_Update_Function_isolation_RC] 20161218
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//        else if (data_mark_param.is_valid()
//                 && OB_SUCCESS != (ret = table_rpc_scan_for_obvalues->set_data_mark_param(data_mark_param)))
//        {
//            TBSYS_LOG(WARN,"fail to set data mark param[%s]!ret=%d",
//                      to_cstring(data_mark_param),ret);
//        }
        //add duyr 20160531:e
    // del bt maosy e
        else if (NULL == CREATE_PHY_OPERRATOR(tmp_table, ObValues, physical_plan, err_stat))
        {
            TRANS_LOG("ObValues is NULL");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (OB_SUCCESS != (ret = tmp_table->set_child(0, *table_rpc_scan_for_obvalues)))
        {
            TBSYS_LOG(WARN, "failed to set child op for ObValues, err=%d", ret);
        }

        else if (NULL == CREATE_PHY_OPERRATOR(inc_scan_op, ObIncScan, physical_plan, err_stat))
        {
            TRANS_LOG("ObIncScan is NULL");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == CREATE_PHY_OPERRATOR(fuse_op, ObMultipleGetMerge, physical_plan, err_stat))
        {
            TRANS_LOG("ObMultipleGetMerge is NULL");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == CREATE_PHY_OPERRATOR(static_data, ObMemSSTableScan, physical_plan, err_stat))
        {
            TRANS_LOG("ObMemSSTableScan is NULL");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        else if (data_mark_param.is_valid()
                 && OB_SUCCESS != (ret = inc_scan_op->set_data_mark_param(data_mark_param)))
        {
            TBSYS_LOG(WARN,"fail to set data mark param into inc scan!ret=%d",ret);
        }
        //add duyr 20160531:e
        else if (OB_SUCCESS != (ret = fuse_op->set_child(0, *static_data)))
        {
            TBSYS_LOG(WARN, "failed to set child op for ObMultipleGetMerge, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = fuse_op->set_child(1, *inc_scan_op)))
        {
            TBSYS_LOG(WARN, "failed to set child op for ObIncScan, err=%d", ret);
        }
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        else if (data_mark_param.is_valid()
                 && OB_SUCCESS != (ret = fuse_op->set_data_mark_param(data_mark_param)))
        {
            TBSYS_LOG(WARN,"fail to set data mark param into ObMultipleGetMerge!ret=%d",ret);
        }
        //add duyr 20160531:e
        else if (NULL == CREATE_PHY_OPERRATOR(get_param_values, ObExprValues, physical_plan, err_stat))
        {
            TRANS_LOG("ObExprValues is NULL");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == CREATE_PHY_OPERRATOR(fill_values, ObFillValues, physical_plan, err_stat))
        {
            TRANS_LOG("fill_values is NULL");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
        {
            ret = OB_ERR_ILLEGAL_ID;
            TRANS_LOG("Fail to get table schema for table[%ld]", table_id);
        }
        else if ((ret = physical_plan->add_base_table_version(
                      table_id,
                      table_schema->get_schema_version()
                      )) != OB_SUCCESS)
        {
            TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", table_id, ret);
        }
        else
        {
            //add lijianqiang [sequence] 20150909:b
            TBSYS_LOG(DEBUG, "handle sequence info for update new");
            ObSequence * base_sequence_op = NULL;
            base_sequence_op = dynamic_cast<ObSequence *> (sequence_op);
            //add 20150909:e
            fuse_op->set_is_ups_row(false);

            inc_scan_op->set_scan_type(ObIncScan::ST_MGET);
            inc_scan_op->set_write_lock_flag();
            inc_scan_op->set_hotspot(stmt->get_query_hint().hotspot_);
            inc_scan_op->set_values(get_param_values->get_id(), false);

            static_data->set_tmp_table(tmp_table->get_id());

            table_rpc_scan_for_obvalues->set_rowkey_cell_count(row_desc.get_rowkey_cell_count());
            table_rpc_scan_for_obvalues->set_need_cache_frozen_data(true);

            get_param_values->set_row_desc(row_desc, row_desc_ext);
            //add lbzhong [Update rowkey] 20151221:b
            bool is_update_rowkey = get_is_update_rowkey(stmt, &rowkey_info);
            //add:e
            //add output column for table_rpc_scan
            if (OB_SUCCESS == ret)
            {
                if (is_column_hint_index)
                {
                    //add rowkey columns
                    for(int64_t key_col_num=0;OB_SUCCESS == ret && key_col_num<rowkey_info.get_size();key_col_num++)
                    {
                        uint64_t key_cid=OB_INVALID_ID;
                        if(OB_SUCCESS!=(ret = rowkey_info.get_column_id(key_col_num,key_cid)))
                        {
                            TBSYS_LOG(WARN, "Failed to get tid cid, err=%d", ret);
                        }
                        else
                        {
                            if(rowkey_info.is_rowkey_column(key_cid))
                            {
                                ObBinaryRefRawExpr col_expr(table_id, key_cid, T_REF_COLUMN);
                                ObSqlRawExpr col_raw_expr(
                                            common::OB_INVALID_ID,
                                            table_id,
                                            key_cid,
                                            &col_expr);
                                ObSqlExpression output_expr;
                                if ((ret = col_raw_expr.fill_sql_expression(
                                         output_expr,
                                         this,
                                         logical_plan,
                                         physical_plan)) != OB_SUCCESS
                                        || (ret = table_rpc_scan_for_obvalues->add_output_column(output_expr)) != OB_SUCCESS)
                                {
                                    TRANS_LOG("Add table output columns faild");
                                    break;
                                }
                                if (OB_SUCCESS == ret)
                                {
                                    uint64_t cid = OB_INVALID_ID;
                                    ret = rowkey_info.get_column_id(key_col_num,cid);
                                    if (OB_SUCCESS != ret || OB_INVALID_ID == cid)
                                    {
                                        ret = (ret != OB_SUCCESS ? ret : OB_ERROR);
                                        TBSYS_LOG(WARN,"get cid failed!key_col_num=%ld,cid=%ld,ret=%d",
                                                  key_col_num,cid,ret);
                                    }
                                    else
                                    {
                                        //add wenghaixing[decimal] for fix delete bug 2014/10/10
                                        ObObjType cond_val_type;
                                        uint32_t cond_val_precision;
                                        uint32_t cond_val_scale;
                                        ObObj static_obj;
                                        if(OB_SUCCESS!=sql_context_->schema_manager_->get_cond_val_info(table_id,cid,cond_val_type,cond_val_precision,cond_val_scale))
                                        {

                                        }
                                        else
                                        {
                                            tmp_table->add_rowkey_array(table_id,cid,cond_val_type,cond_val_precision,cond_val_scale);
                                            if(ObDecimalType==cond_val_type){
                                                static_obj.set_precision(cond_val_precision);
                                                static_obj.set_scale(cond_val_scale);
                                                static_obj.set_type(cond_val_type);
                                            }
                                        }
                                        //add e
                                    }
                                }
                            }
                        }
                    }
                    for (int32_t i = 0; ret == OB_SUCCESS && i < row_desc.get_column_num(); i++)
                    {
                        //const ColumnItem *col_item = stmt->get_column_item(i);
                        uint64_t key_cid=OB_INVALID_ID;
                        uint64_t tid = OB_INVALID_ID;
                        if(OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, key_cid)))
                        {
                            TBSYS_LOG(WARN, "Failed to get tid cid, err=%d", ret);
                        }
                        else if(!rowkey_info.is_rowkey_column(key_cid))
                        {
                            //TBSYS_LOG(ERROR, "test::whx out put column[%ld]",i);
                            if (table_id == table_item->table_id_)
                            {
                                ObBinaryRefRawExpr col_expr(table_id, key_cid, T_REF_COLUMN);
                                ObSqlRawExpr col_raw_expr(
                                            common::OB_INVALID_ID,
                                            table_id,
                                            key_cid,
                                            &col_expr);
                                ObSqlExpression output_expr;
                                if ((ret = col_raw_expr.fill_sql_expression(
                                         output_expr,
                                         this,
                                         logical_plan,
                                         physical_plan)) != OB_SUCCESS
                                        || (ret = table_rpc_scan_for_obvalues->add_output_column(output_expr)) != OB_SUCCESS)
                                {
                                    TRANS_LOG("Add table output columns faild");
                                    break;
                                }
                            }
                        }
                    }

                }
                else
                {
                    //add lbzhong[Update rowkey] 20151221:b
                    if(!is_update_rowkey)
                    {
                    //add:e
                      int32_t num = stmt->get_column_size();
                      ColumnItem col_item_for_fill_val;
                      for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
                      {
                          const ColumnItem *col_item = stmt->get_column_item(i);
                          OB_ASSERT(col_item);
                          if (col_item->table_id_ == table_item->table_id_)
                          {
                              ObBinaryRefRawExpr col_expr(col_item->table_id_, col_item->column_id_, T_REF_COLUMN);
                              ObSqlRawExpr col_raw_expr(
                                          common::OB_INVALID_ID,
                                          col_item->table_id_,
                                          col_item->column_id_,
                                          &col_expr);
                              ObSqlExpression output_expr;
                              if ((ret = col_raw_expr.fill_sql_expression(
                                       output_expr,
                                       this,
                                       logical_plan,
                                       physical_plan)) != OB_SUCCESS
                                      || (ret = table_rpc_scan_for_obvalues->add_output_column(output_expr)) != OB_SUCCESS)
                              {
                                  TRANS_LOG("Add columns faild");
                                  break;
                              }
                              else
                              {
                                  col_item_for_fill_val.column_id_ = col_item->column_id_;
                                  col_item_for_fill_val.table_id_ = col_item->table_id_;
                                  col_item_for_fill_val.data_type_ = col_item->data_type_;
                                  if(OB_SUCCESS != (ret = fill_values->add_column_item(col_item_for_fill_val)))
                                  {
                                      TRANS_LOG("fail to add column item to ob_fill_values");
                                  }
                              }
                          }
                      }
                    //add lbzhong[Update rowkey] 20151221:b
                    }
                    else if(OB_SUCCESS != (ret = add_full_output_columns(table_rpc_scan_for_obvalues, table_id,
                                                                         logical_plan, physical_plan, row_desc, err_stat)))
                    {
                        TRANS_LOG("fail to add full output column to table_rpc_scan_for_obvalues");
                    }
                    //add:e
                    if (OB_SUCCESS == ret)
                    {
                        for(int64_t key_col_num=0;OB_SUCCESS == ret && key_col_num<rowkey_info.get_size();key_col_num++)
                        {
                            uint64_t cid = OB_INVALID_ID;
                            ret = rowkey_info.get_column_id(key_col_num,cid);
                            if (OB_SUCCESS != ret || OB_INVALID_ID == cid)
                            {
                                ret = (ret != OB_SUCCESS ? ret : OB_ERROR);
                                TBSYS_LOG(WARN,"get cid failed!key_col_num=%ld,cid=%ld,ret=%d",
                                          key_col_num,cid,ret);
                            }
                            else
                            {
                                //add wenghaixing[decimal] for fix delete bug 2014/10/10
                                ObObjType cond_val_type;
                                uint32_t cond_val_precision;
                                uint32_t cond_val_scale;
                                ObObj static_obj;
                                if(OB_SUCCESS!=sql_context_->schema_manager_->get_cond_val_info(table_id,cid,cond_val_type,cond_val_precision,cond_val_scale))
                                {

                                }
                                else
                                {
                                    tmp_table->add_rowkey_array(table_id,cid,cond_val_type,cond_val_precision,cond_val_scale);
                                    if(ObDecimalType==cond_val_type){
                                        static_obj.set_precision(cond_val_precision);
                                        static_obj.set_scale(cond_val_scale);
                                        static_obj.set_type(cond_val_type);
                                    }
                                }
                                //add e
                            }
                        }
                    }

                }
                if (ret == OB_SUCCESS && OB_SUCCESS != (ret = fill_values->set_child(0, *tmp_table)))
                {
                    TBSYS_LOG(WARN, "failed to set table_rpc_scan_for_obexpr op, err=%d", ret);
                }
                else if (ret == OB_SUCCESS && OB_SUCCESS != (ret = fill_values->set_child(1, *get_param_values)))
                {
                    TBSYS_LOG(WARN, "failed to set get_param_values op, err=%d", ret);
                }
                else if (ret == OB_SUCCESS && OB_SUCCESS != (ret = physical_plan->add_phy_query(fill_values)))
                {
                    TBSYS_LOG(WARN, "failed to add fill_values to sub query, err=%d", ret);
                }
                else if (ret == OB_SUCCESS)
                {
                    table_op = fuse_op;
                    if (OB_SUCCESS == ret)
                    {
                        fill_values->set_rowkey_info(rowkey_info);
                        fill_values->set_row_desc(row_desc);
                        fill_values->set_table_id(table_id);
                        fill_values->set_is_delete_update(is_delete_update);
                        fill_values->set_is_column_hint_index(is_column_hint_index);
                        fill_values->set_generated_column_id(logical_plan->generate_column_id());
                        fill_values->set_sql_context(*sql_context_);
                        get_param_values->set_from_ud(true);
                        // add by maosy [Delete_Update_Function_isolation_RC] 20161218
//                        if(is_multi_batch_)
//                        {
                          fill_values->set_is_multi_batch(true);
//                          is_multi_batch_ = false;
//                        }
// add by maosy e
                        // add by maosy [Delete_Update_Function]
                        if(stmt->get_query_hint ().change_value_size_)
                        {
                            int64_t max_row_size = static_cast<int64_t>(0.8*1024L*1024L);
                            fill_values->set_max_row_value_size (max_row_size);
                        }
                        // add e
                        //add lbzhong [Update rowkey] 20151221:b
                        ObArray<uint64_t> update_columns;
                        stmt->get_update_columns(update_columns);
                        fill_values->set_update_columns_flag(update_columns);
                        if(is_update_rowkey)
                        {
                            ObTableRpcScan *table_rpc_scan_for_update_rowkey = NULL;
                            ObRowDesc all_row_desc;
                            if (NULL == CREATE_PHY_OPERRATOR(table_rpc_scan_for_update_rowkey, ObTableRpcScan, physical_plan, err_stat))
                            {
                                ret = OB_ALLOCATE_MEMORY_FAILED;
                            }
                            else if(OB_SUCCESS != (ret = cons_all_row_desc(table_id, &all_row_desc, err_stat)))
                            {
                              TBSYS_LOG(WARN, "failed to cons all_row_desc, err=%d", ret);
                            }
                            else
                            {
                                fill_values->set_table_rpc_scan_for_update_rowkey(table_rpc_scan_for_update_rowkey);
                                fill_values->set_is_update_rowkey(true);
                                fill_values->set_column_items(stmt);
                                fill_values->set_sql_context(*sql_context_);
                                fill_values->set_table_item(*table_item);
                                fill_values->set_exprs(logical_plan, stmt);
                            }
                        }
                        //add:e
                        //add lijianqiang [update_sequence] 20151218:b
                        fill_values->set_sequence_update(sequence_op);
                        //add 20151218:e
                        //add wenghaixing[decimal] for fix delete bug 2014/10/10
                        tmp_table->set_fix_obvalues();
                        get_param_values->set_del_upd();
                        //add e
                        if (OB_SUCCESS == ret)
                        {
                            int64_t  rowkey_num = rowkey_info.get_size();
                            uint64_t column_id  = OB_INVALID_ID;
                            if (OB_SUCCESS == ret)
                            {
                                tmp_table->set_table_id(table_id);
                                tmp_table->set_rowkey_num(rowkey_num);
                                tmp_table->set_row_desc(row_desc);
                TBSYS_LOG(DEBUG,"row_desc[%s]",to_cstring(row_desc));
                                for (int64_t index=0;OB_SUCCESS == ret && index<rowkey_num;index++)
                                {
                                    if (OB_SUCCESS != (ret = rowkey_info.get_column_id(index,column_id)))
                                    {
                                        TBSYS_LOG(WARN,"fail to get column id,ret[%d]",ret);
                                    }
                                    else if (OB_SUCCESS != (ret = tmp_table->set_column_ids(column_id)))
                                    {
                                        TBSYS_LOG(WARN,"fail to set column id,ret[%d]",ret);
                                    }
                                }
                            }
                        }
                        if(OB_SUCCESS == ret && ObBasicStmt::T_EXPLAIN != logical_plan->get_main_stmt()->get_stmt_type())
                        {
                            bool is_non_where_conditon = false;
                            ParseNode* ud_where_parse_tree = stmt->get_ud_where_parse_tree();
                            std::string column_names;
                            std::string table_name;
                            ObMySQLResultSet ud_select_result_set;
                            //get column_names and table_name from udpate stmt
                            if ( OB_SUCCESS != (ret=get_column_name_table_name(column_names,table_name,table_item,stmt,rowkey_info,fill_values
                                                                               //add lbzhong [Update rowkey] 20160713:b
                                                                               , is_update_rowkey
                                                                               //add:e
                                                                               ,row_desc
                                                                               )))
                            {
                                TBSYS_LOG(WARN,"fail to get column_name[%s] and table_name[%s]",column_names.data(),table_name.data());
                            }
                            else
                            {
                                if (0 >= stmt->get_condition_size())
                                {
                                    is_non_where_conditon = true;
                                }
                                bool is_prepare = false;
                                ObString prepare_name;
                                if (ObBasicStmt::T_PREPARE == logical_plan->get_main_stmt()->get_stmt_type())
                                {
                                    ObPrepareStmt* prepare_stmt = dynamic_cast<ObPrepareStmt*>(logical_plan->get_main_stmt());
                                    if( NULL == prepare_stmt)
                                    {
                                        ret = OB_ERROR;
                                        TBSYS_LOG(WARN,"This is the prepare stmt, but the prepare stmt is NULL");
                                    }
                                    else
                                    {
                                        prepare_name = prepare_stmt->get_stmt_name();
                                        is_prepare = true;
                                        fill_values->set_is_in_prepare(true);
                                        TBSYS_LOG(ERROR,"question_num=%ld",questionmark_num_in_update_assign_list_);
                                        fill_values->set_questionmark_num_in_assginlist_of_update(questionmark_num_in_update_assign_list_);
                                    }
                                }
                                if (OB_SUCCESS != (ret = constuct_top_operator_of_select(ud_select_result_set,
                                                                                         is_non_where_conditon,
                                                                                               ud_where_parse_tree,
                                                                                         column_names,
                                                                                         table_name,
                                                                                         is_prepare,
                                                                                         prepare_name,
                                                                                         *fill_values,
                                                                                         err_stat)))
                                {
                                    TBSYS_LOG(WARN,"fail to construct top operator of select");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return ret;
}

/***************************************************************
Function:    constuct_top_operator_of_select
Description: construct the top operator of select stmt and store this operator into ObFillValues.
             Constructed operator is opened in function open of ObFillValues, and used to fetch ud
             data from CS.
Calls:
Input:       logical_plan and physical_plan are used in fill_sql_expression function,and rowkey_info supplys the rowkey information
Output:      ud_result_set,
Returns:     ret
Others:
***************************************************************/

int ObTransformer::constuct_top_operator_of_select(ObMySQLResultSet& ud_select_result_set,
                                                   bool is_non_where_condition,
                                                   ParseNode* ud_where_parse_tree,
                                                   std::string column_names,
                                                   std::string table_name,
                                                   bool is_prepare,
                                                   const ObString& prepare_name,
                                                   ObFillValues& fill_values,
                                                   ErrStat& err_stat)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ParseResult select_parse_result;
    // mod by maosy [Delete_Update_Function]
    common::ObString prepare_select_string;
    ParseNode* select_syntax_tree = NULL;
    std::string select_part = "prepare gaojt from select/*+READ_CONSISTENCY(STRONG)*/ ";
    std::string from_part = " from ";
    std::string temp_result = "";
    temp_result = select_part + column_names + from_part + table_name;
    prepare_select_string = ObString::make_string(temp_result.data());
    //construct select stmt
   // construct_select_string(prepare_select_string,is_update_rowkey,column_names,table_name);
    char temp_prepare_name[prepare_name.length()+1];
    memset(temp_prepare_name, 0, sizeof(temp_prepare_name));
    sprintf(temp_prepare_name,"%.*s",prepare_name.length(),prepare_name.ptr());
    std::string temp_name = temp_prepare_name;

    if(OB_SUCCESS != (ret = (get_parse_result(prepare_select_string,select_parse_result))))
    {
        TBSYS_LOG(WARN,"fail to get parse result");
    }
    else
    {
        select_syntax_tree = select_parse_result.result_tree_;
        ParseNode prepare_stmt_name;
		// add by maosy [Delete_Update_Function_isolation_RC] 20161228
        int64_t current_time =  tbsys::CTimeUtil::getTime();
        char tmp_time[17];
        memset(tmp_time,0,sizeof(tmp_time));
        sprintf (tmp_time,"%ld",current_time);
        tmp_time[16] = '\0';
        std::string time = tmp_time ;
		// add e
         if(select_syntax_tree && select_syntax_tree->children_[0] && select_syntax_tree->children_[0]->children_[0])
        {
            prepare_stmt_name.children_=NULL;
            prepare_stmt_name.num_child_=0;
            TBSYS_LOG(DEBUG,"prepare-stmt-name=%s",select_syntax_tree->children_[0]->children_[0]->str_value_);
            if(is_prepare)
            {
			// add by maosy [Delete_Update_Function_isolation_RC] 20161228
			//temp_name = temp_name+".";
               temp_name = "__batch" + temp_name + time;
			   //add e
               prepare_stmt_name.str_value_ = temp_name.data();
            }
            else
            {
			// add by maosy [Delete_Update_Function_isolation_RC] 20161228
			//temp_name = ".";
               temp_name = "__batch" + time ;
			   // add e
               prepare_stmt_name.str_value_ = temp_name.data();
            }
            select_syntax_tree->children_[0]->children_[0] = &prepare_stmt_name;
            fill_values.set_prepare_name(temp_name);
        }
        else
        {
            TBSYS_LOG(WARN,"fail to get the top node of select-parse-tree");
            ret = OB_ERROR;
        }
    }
    //generate the parse tree of select stmt
    if(OB_SUCCESS ==ret && !is_non_where_condition)
    {
        //glue the where-parse-tree of ud to select-parse-tree generated
        if(select_syntax_tree->children_[0] && select_syntax_tree->children_[0]->children_[1])
        {
            select_syntax_tree->children_[0]->children_[1]->children_[3] = ud_where_parse_tree;
        }
        else
        {
            TBSYS_LOG(WARN,"fail to get the first node of select-parse-tree,select_syntax_tree->children_[1]=%p\
                      select_syntax_tree->children_[1]->children_[0]=%p",select_syntax_tree->children_[0]\
                    ,select_syntax_tree->children_[0]->children_[1]);
            ret = OB_ERROR;
        }
    }
    //call ObSql::direct_execute to generate top operator of select stmt
    if (OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = ud_select_result_set.init()))
        {
            TBSYS_LOG(WARN, "init result set failed, ret = %d", ret);
        }
        else
        {
            if ((OB_SUCCESS != (ret = ObSql::direct_execute(prepare_select_string, ud_select_result_set, *sql_context_,&select_parse_result))))
            {
                TBSYS_LOG(WARN,"fail to do direct execute,ret = %d",ret);
            }
            else if (OB_SUCCESS != (ret = ud_select_result_set.open()))
            {
                TBSYS_LOG(WARN,"fail to open ud_select_result_set,ret = %d",ret);
            }
//            else if (OB_SUCCESS != (ret = ud_select_result_set.close()))
//            {
//                TBSYS_LOG(WARN,"fail to close ud_select_result_set,ret = %d",ret);
//            }
        }
    }
    return ret;
}

int ObTransformer::get_column_name_table_name(std::string &column_names,
                                                  std::string &table_name,
                                                  TableItem* table_item,
                                                  ObStmt*& stmt,
                                                  ObRowkeyInfo rowkey_info,
                                                  ObFillValues*& fill_values
                                              //add lbzhong [Update rowkey] 20160713:b
                                              , const bool is_update_rowkey
                                              //add:e
                                              // add by maosy [Delete_Update_Function] for fix no gard for row size 20161031 b:
                                              , const ObRowDesc &row_desc
                                              // add  by maosy 20161031:e
                                              )
{
    int ret = OB_SUCCESS;
    std::string comm = ",";
    UNUSED(is_update_rowkey) ;
    // add by maosy [Delete_Update_Function] for fix no gard for row size 20161031 b:
    std::string mark_l = "\"";// for fix some column is daxie
    int64_t num = row_desc.get_column_num ();
    for (int64_t i = 0; i < num; i++)
    {
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        const ObColumnSchemaV2* column_schema = NULL;
        const ColumnItem *col_item = NULL;
        if(OB_SUCCESS != (ret = row_desc.get_tid_cid (i,tid,cid)))
        {
            TBSYS_LOG(WARN,"failed to get tid and cid from row desc =%s,ret = %d",to_cstring(row_desc),ret);
        }
        else if (NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(tid, cid)))
        {
            TBSYS_LOG(WARN,"failed to get table = %lu, column = %lu schema",tid,cid);
        }
        else
        {
            const char* column_name = column_schema->get_name ();
            if(0 == i)
            {
                column_names = mark_l + column_name + mark_l;
            }
            else
            {
                column_names = column_names + comm + mark_l + column_name + mark_l ;
            }
            if(NULL != (col_item = stmt->get_column_item_by_id (tid,cid)))
            {
                if (rowkey_info.is_rowkey_column(col_item->column_id_))
                {
                    if (OB_SUCCESS != (ret = fill_values->set_rowkey_ids(col_item->column_id_)))
                    {
                    }
                }
            }
        }
        TBSYS_LOG(DEBUG,"COLUMN-NAMES=%s",column_names.data());//add by gaojt-test
    }
// add  maosy 20161031 :e

    char tmp_table_name[table_item->table_name_.length()+1];
    char alias_table_name[table_item->alias_name_.length()+1];

    memset(tmp_table_name, 0, sizeof(tmp_table_name));
    sprintf(tmp_table_name,"%.*s",table_item->table_name_.length(),table_item->table_name_.ptr());

    table_name = tmp_table_name;
    int pos = -1;
    if(-1 != (pos = static_cast<int>(table_name.find('.'))))
    {
        // add by maosy  [Delete_Update_Function]
        char database_name [pos+1];
//        char need_erase[5]="TANG";
        memset(database_name,0,sizeof(database_name));
        strncpy(database_name,tmp_table_name,pos);
        database_name[pos] = '\0';
        table_name = table_name.erase(0,pos+1);
//        if(strcmp(need_erase,database_name)==0)
//        {
//             table_name = mark_l+table_name+mark_l  ;
//        }
//        else
//        {
            table_name = mark_l +database_name +mark_l + "." +mark_l+table_name+mark_l ;
//        }
        // add  e
        if (table_item->alias_name_.length() > 0)
        {
            memset(alias_table_name, 0, sizeof(alias_table_name));
            sprintf(alias_table_name,"%.*s",table_item->alias_name_.length(),table_item->alias_name_.ptr());
            table_name = table_name + " as " + mark_l + alias_table_name + mark_l;
        }
    }
    return ret;
}

// del by maosy [Delete_Update_Function_isolation_RC] 20161218
//void ObTransformer::construct_select_string(common::ObString& prepare_select_string,
//                                            bool is_update_rowkey,
//                                            std::string column_names,
//                                            const std::string& table_name)
//{
//    std::string select_part = "prepare gaojt from select/*+READ_CONSISTENCY(STRONG)*/ ";
//    std::string from_part = " from ";
//    std::string temp_result = "";
//    //add lbzhong [update rowkey] 20160319:b
//    std::string columns = column_names;
//    if(is_update_rowkey)
//    {
//      columns = "*";
//    }
//    //add:b
//    // add by maosy [Delete_Update_Function] for fix daxie
//    //temp_result = select_part + columns + from_part + table_name;
//    std::string mark ="\"";
//    temp_result = select_part + columns + from_part + mark +table_name +mark;
//    // add e
//    temp_result = select_part + columns + from_part + table_name;
//    prepare_select_string = ObString::make_string(temp_result.data());
//    TBSYS_LOG(DEBUG,"select-string=%.*s",prepare_select_string.length(),prepare_select_string.ptr());
//}
//del by maosy
int ObTransformer::get_parse_result(const ObString &select_stmt, ParseResult &select_parse_result)
{
    int ret = OB_SUCCESS;
    //generate select syntax_tree
    if (OB_SUCCESS != (ret = (gene_parse_result(select_stmt,select_parse_result))))
    {
        TBSYS_LOG(WARN,"fail to get select parse result");
    }
    return ret;
}

int ObTransformer::gene_parse_result(const ObString &query_string, ParseResult &syntax_tree)
{
    int ret = OB_SUCCESS;

    common::ObStringBuf &parser_mem_pool = sql_context_->session_info_->get_parser_mem_pool();

    syntax_tree.malloc_pool_ = &parser_mem_pool;
    if (OB_SUCCESS != (ret = parse_init(&syntax_tree)))
    {
      ret = OB_ERR_PARSER_INIT;
    }
    else
    {
        if (parse_sql(&syntax_tree, query_string.ptr(), static_cast<size_t>(query_string.length())) != OB_SUCCESS
          || NULL == syntax_tree.result_tree_)
        {
            ret = OB_ERR_PARSER_SYNTAX;
        }
    }
    return ret;
}

/****************************************************************
 Function:    is_multi_delete_update
 Description: used to determine the inputting SQL whether fits the conditions of original delete SQL
 Calls:
 Input:       logical_plan and physical_plan are used in fill_sql_expression function,and rowkey_info
              supplys the rowkey information
 Output:      is_multi_delete
 Returns:     ret
 Others:
 ***************************************************************/
int ObTransformer::is_multi_delete_update(ObLogicalPlan *logical_plan,
                    ObPhysicalPlan* physical_plan,
                    const ObRowkeyInfo &rowkey_info,
                    ObStmt *stmt,
                    ErrStat& err_stat,
                    bool& is_multi_delete_update)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    int32_t num = stmt->get_condition_size();
    uint64_t cid = OB_INVALID_ID;
    int64_t cond_op = T_INVALID;
    //int cnd_expr_type = 0;//add by maosy [Delete_Update_Function][prepare question mark ] 20170519
    ObObj cond_val;
    ObPostfixExpression::ObPostExprNodeType val_type = ObPostfixExpression::BEGIN_TYPE;
    ObRowDesc rowkey_col_map;
    for (int32_t i = 0; i < num; i++)
    {
        ObSqlRawExpr *cnd_expr = logical_plan->get_expr(stmt->get_condition_id(i));
        OB_ASSERT(cnd_expr);
        //add by maosy [Delete_Update_Function][prepare question mark ] 20170519
        //cnd_expr_type = static_cast<int>(cnd_expr->get_expr()->get_expr_type());
        //if(cnd_expr->get_expr()->is_equal_filter())
        if(cnd_expr->get_expr()->is_equal_filter_expend())
        {
            cnd_expr->set_applied(true);
            // add by maosy e
            TBSYS_LOG(DEBUG,"The %dth condition' is not sub-query's type is %d",i,cnd_expr->get_expr()->get_expr_type());
            ObSqlExpression *filter = ObSqlExpression::alloc();
            if (NULL == filter)
            {
                TRANS_LOG("no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
                break;
            }
            else if (OB_SUCCESS != (ret = cnd_expr->fill_sql_expression(*filter, this, logical_plan, physical_plan)))
            {
                ObSqlExpression::free(filter);
                TRANS_LOG("Failed to fill expression, err=%d", ret);
                break;
            }
            else if (filter->is_simple_condition(false, cid, cond_op, cond_val, &val_type)
                     && (T_OP_EQ == cond_op || T_OP_IS == cond_op)
                     && rowkey_info.is_rowkey_column(cid))
            {
                if (OB_SUCCESS != (ret = rowkey_col_map.add_column_desc(OB_INVALID_ID, cid)))
                {
                    TRANS_LOG("Failed to add column desc, err=%d", ret);
                    break;
                }
            }
            if(NULL != filter)
            {
                ObSqlExpression::free(filter);
            }
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        int64_t rowkey_col_num = rowkey_info.get_size();
        uint64_t cid = OB_INVALID_ID;
        for (int64_t i = 0; i < rowkey_col_num; ++i)
        {
            if (OB_SUCCESS != (ret = rowkey_info.get_column_id(i, cid)))
            {
                TRANS_LOG("Failed to get column id, err=%d", ret);
                break;
            }
            else if (OB_INVALID_INDEX == rowkey_col_map.get_idx(OB_INVALID_ID, cid))
            {
                is_multi_delete_update = true;
                TBSYS_LOG(DEBUG,"is_multi_delete_update=%d",is_multi_delete_update);
                break;
            }
        } // end for
    }
    return ret;
 }

int ObTransformer::get_fillvalues_operator(ObPhyOperator *main_query,ObPhyOperator *&fill_values)
{
    int ret = OB_SUCCESS;
    ObIudLoopControl *insert_loop = NULL;
    ObPhysicalPlan* inner_plan = NULL;
    ObUpsExecutor* ups_executor = NULL;
    if ((insert_loop = dynamic_cast<ObIudLoopControl*>(main_query)) == NULL
            || (ups_executor = dynamic_cast<ObUpsExecutor*>(insert_loop->get_child(0))) == NULL
            || (inner_plan = ups_executor->get_inner_plan()) == NULL)
    {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "This is the wrong Update or delete physical tree, insert_loop=%p inner_plan=%p"\
                         "ups_executor=%p",insert_loop, inner_plan, ups_executor);
    }
    else
    {
        for (int32_t index = 0; OB_SUCCESS == ret && index < inner_plan->get_query_size(); ++index)
        {
            ObPhyOperator* aux_query = inner_plan->get_phy_query(index);
            if ( (fill_values = dynamic_cast<ObFillValues*>(aux_query))!=NULL)
            {
                break;
            }
        }
    }
    return ret;
}

//add gaojt 20150817:e
//add dolphin [database manager]@20150617:b
int ObTransformer::check_dbname_for_table(ErrStat& err_stat,const ObString& dbname)
{
    int ret = OB_SUCCESS;
    const ObPrivilege ** privilege = NULL;
    sql_context_->privilege_mgr_->get_newest_privilege(privilege);
    if (privilege == NULL)
    {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR,"get privilege failed.");
    }
    if(OB_SUCCESS == ret )
    {
        const ObPrivilege::NameDbMap *ndm = (*privilege)->get_database_name_map();
        const ObPrivilege::ObDatabase *odb = ndm->get(dbname);

        if (odb == NULL)
        {
            ret = OB_ERR_DATABASE_NOT_EXSIT;
            TRANS_LOG("database %.*s not exist, may have not been sychronize to schema manager,try it again.", dbname.length(), dbname.ptr());
        }
    }
    if (privilege != NULL)
    {
        int tmp_err = sql_context_->privilege_mgr_->release_privilege(privilege);
        if (OB_SUCCESS != tmp_err)
        {
            TBSYS_LOG(WARN, "release privilege failed, ret=%d", tmp_err);
        }
    }
    return ret;
}
//add:e
//add by wenghaixing [secondary index]20141024
int ObTransformer::gen_physical_create_index(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObCreateIndexStmt *crt_idx_stmt = NULL;
    ObCreateTable *crt_tab_op = NULL;
    //add wenghaixing 20141029
    // uint64_t magic_cid=OB_APP_MIN_COLUMN_ID;
    IndexHelper ih;
    //add e
    //add wenghaixing [secondary index] 20141111
    uint64_t max_col_id = 0;
    //add e
    bool rowkey_will_add_in = true;
    int64_t column_num = 0;
    int64_t this_index_col_num = 0;
    ObStrings expire_col;//for expire infomation
    ObString val;//for expire infomation
    /*get create index statement*/
    if(OB_SUCCESS == ret)
    {
        get_stmt(logical_plan,err_stat,query_id,crt_idx_stmt);
    }
    /*generate operator to create index table*/
    if(OB_SUCCESS == ret)
    {
        CREATE_PHY_OPERRATOR(crt_tab_op,ObCreateTable,physical_plan,err_stat);
        if(OB_SUCCESS == ret)
        {
            crt_tab_op->set_sql_context(*sql_context_);
            ret = add_phy_query(logical_plan,physical_plan,err_stat,query_id,crt_idx_stmt,crt_tab_op,index);
        }
    }
    /*if(ret==OB_SUCCESS)
    {
        if(crt_idx_stmt->get_index_colums_count()>OB_MAX_INDEX_COLUMNS)
        {
            TRANS_LOG("Too many columns in index!(max allowed is %ld).",
                     OB_MAX_USER_DEFINED_COLUMNS_COUNT);
            ret = OB_ERR_INVALID_COLUMN_NUM;
        }
    }
    */
    /*set table schema for phy_oprator*/
    if(OB_SUCCESS == ret)
    {
        int buf_len = 0;
        int len = 0;
        TableSchema& table_schema = crt_tab_op->get_table_schema();
        const ObTableSchema* idxed_tab_schema=NULL;
        const ObString& index_name = crt_idx_stmt->get_index_label();
        // buf_len = sizeof(table_schema.table_name_);
        if (index_name.length() < OB_MAX_TABLE_NAME_LENGTH)
        {
            len = index_name.length();
        }
        else
        {
            len = OB_MAX_TABLE_NAME_LENGTH - 1;
            TRANS_LOG("Table name is truncated to '%.*s'", len, index_name.ptr());
        }
        memcpy(table_schema.table_name_, index_name.ptr(), len);
        table_schema.table_name_[len] = '\0';
        /*Now We Must take source table's schema to fix index schema*/
        const ObString& idxed_tab_name=crt_idx_stmt->get_idxed_name();
        //add liumz [database manager]@20150721:b
        ObString db_name, table_name;
        char db_name_buf[OB_MAX_DATBASE_NAME_LENGTH] = {0};
        char table_name_buf[OB_MAX_TABLE_NAME_LENGTH] = {0};
        db_name.assign_buffer(db_name_buf, OB_MAX_DATBASE_NAME_LENGTH);
        table_name.assign_buffer(table_name_buf, OB_MAX_TABLE_NAME_LENGTH);
        bool split_ret = idxed_tab_name.split_two(db_name, table_name);
        if(!split_ret)
        {
            TRANS_LOG("Wrong table name [%.*s]!", idxed_tab_name.length(), idxed_tab_name.ptr());
            ret = OB_ERROR;
        }
        else
        {
            if (db_name.length() < OB_MAX_DATBASE_NAME_LENGTH)
            {
                len = db_name.length();
            }
            else
            {
                len = OB_MAX_DATBASE_NAME_LENGTH - 1;
                TRANS_LOG("Database name is truncated to '%.*s'", len, db_name.ptr());
            }
            memcpy(table_schema.dbname_, db_name.ptr(), len);
            table_schema.dbname_[len] = '\0';
        }
        //add:e
        //modify liuxiao [secondary index bug fixed] 20150722
        //char str_tname[common::OB_MAX_COLUMN_NAME_LENGTH],str_cname[common::OB_MAX_COLUMN_NAME_LENGTH];
        //memset(str_tname,0,common::OB_MAX_COLUMN_NAME_LENGTH);
        //memcpy(str_tname,idxed_tab_name.ptr(),idxed_tab_name.length());

        char str_tname[common::OB_MAX_TABLE_NAME_LENGTH],str_cname[common::OB_MAX_COLUMN_NAME_LENGTH];
        memset(str_tname,0,common::OB_MAX_COLUMN_NAME_LENGTH);
        memcpy(str_tname,idxed_tab_name.ptr(),idxed_tab_name.length());
        str_tname[common::OB_MAX_TABLE_NAME_LENGTH-1]='\0';
        //modify e
        if (NULL == (idxed_tab_schema = sql_context_->schema_manager_->get_table_schema(str_tname)))
        {
            ret = OB_ERR_ILLEGAL_ID;
            TRANS_LOG("Fail to get table schema for table[%s]", str_tname);
        }
        if(OB_SUCCESS == ret)
        {
            /* refresh expior condition of index schema*/
            ObString expire_info;
            expire_info.assign_ptr((char*)idxed_tab_schema->get_expire_condition(),static_cast<int32_t>(strlen(idxed_tab_schema->get_expire_condition())));
            if (expire_info.length() < OB_MAX_EXPIRE_CONDITION_LENGTH)
            {
                len = expire_info.length();
            }
            else
            {
                len = OB_MAX_EXPIRE_CONDITION_LENGTH - 1;
                TRANS_LOG("Expire_info is truncated to '%.*s'", len, expire_info.ptr());
            }
            memcpy(table_schema.expire_condition_, expire_info.ptr(), len);
            table_schema.expire_condition_[len] = '\0';
            if(idxed_tab_schema->get_expire_condition()[0]!='\0')
            {
                /*如果超时信息当中的列没有在索引列当中，那么就要在索引表里添加超时信息涉及到的列*/ //repaired from messy code by zhuxh 20151014
                const ObColumnSchemaV2* oc_expire=NULL;
                if(OB_SUCCESS != (ret = generate_expire_col_list(expire_info,expire_col)))
                {
                    TBSYS_LOG(WARN,"generate expire col list error");
                }
                else
                {
                    for(int64_t i=0;i<expire_col.count();i++)
                    {
                        val.reset();
                        if(OB_SUCCESS != (ret = expire_col.get_string(i,val)))
                        {
                            TBSYS_LOG(WARN,"get expire error");
                            break;
                        }
                        else if(NULL == (oc_expire = sql_context_->schema_manager_->get_column_schema(idxed_tab_name,val)))
                        {
                            TBSYS_LOG(WARN,"get expire column schema error,col [%.*s]",val.length(),val.ptr());
                        }
                        else if(!crt_idx_stmt->is_col_expire_in_storing(val)&&!idxed_tab_schema->get_rowkey_info().is_rowkey_column(oc_expire->get_id()))
                        {
                            crt_idx_stmt->set_storing_columns_simple(val);
                            //TBSYS_LOG(ERROR,"test::whx set_storing_columns_simple,[%.*s]",val.length(),val.ptr());
                        }
                    }
                }
            }
            /* refresh comment_str condition of index schema*/
            ObString comment_str;
            comment_str.assign_ptr((char*)idxed_tab_schema->get_comment_str(),static_cast<int32_t>(strlen(idxed_tab_schema->get_comment_str())));
            //buf_len = sizeof(table_schema.comment_str_);
            if (comment_str.length() < OB_MAX_TABLE_COMMENT_LENGTH)
            {
                len = comment_str.length();
            }
            else
            {
                len = OB_MAX_TABLE_COMMENT_LENGTH - 1;
                TRANS_LOG("Comment_str is truncated to '%.*s'", len, comment_str.ptr());
            }
            memcpy(table_schema.comment_str_, comment_str.ptr(), len);
            table_schema.comment_str_[len] = '\0';
            /*refresh other infomation*/
            crt_tab_op->set_if_not_exists(false);
            //if (crt_tab_stmt->get_tablet_max_size() > 0)
            //we set some paramer with default value
            table_schema.tablet_max_size_ =idxed_tab_schema->get_max_sstable_size();
            table_schema.tablet_block_size_ = idxed_tab_schema->get_block_size();
            table_schema.replica_num_ = (int32_t)idxed_tab_schema->get_replica_count();
            table_schema.is_use_bloomfilter_ = idxed_tab_schema->is_use_bloomfilter();
            table_schema.consistency_level_ = idxed_tab_schema->get_consistency_level();
            table_schema.rowkey_column_num_ = (int32_t)idxed_tab_schema->get_rowkey_info().get_size()+(int32_t)crt_idx_stmt->get_index_colums_count();
            //add wenghaixing 20141029
            table_schema.table_id_=OB_INVALID_ID;
            table_schema.max_used_column_id_=OB_ALL_MAX_COLUMN_ID;
            //add e
            ObString compress_method;
            compress_method.assign_ptr((char*)idxed_tab_schema->get_compress_func_name(),
                                       static_cast<int32_t>(strlen(idxed_tab_schema->get_compress_func_name())));
            //buf_len = sizeof(table_schema.compress_func_name_);
            const char *func_name = compress_method.ptr();
            len = compress_method.length();
            if (len <= 0)
            {
                func_name = OB_DEFAULT_COMPRESS_FUNC_NAME;
                len = static_cast<int>(strlen(OB_DEFAULT_COMPRESS_FUNC_NAME));
            }
            if (len >= OB_MAX_TABLE_NAME_LENGTH)
            {
                len = OB_MAX_TABLE_NAME_LENGTH - 1;
                TRANS_LOG("Compress method name is truncated to '%.*s'", len, func_name);
            }
            memcpy(table_schema.compress_func_name_, func_name, len);
            table_schema.compress_func_name_[len] = '\0';
            /* Now We Refresh Columns Info of index*/
            //ObString idxed_tname=crt_idx_stmt->get_idxed_name();
            uint64_t tid = idxed_tab_schema->get_table_id();
            ObRowkeyInfo ori = idxed_tab_schema->get_rowkey_info();
            //modify wenghaixing [secondary index static_index_build]20150317
            //ih.idx_tid=OB_FLAG_TID;
            //add liuxiao [muti database] 20150702
            /*const ObString index_table_name_with_dbname = ObString::make_string(idxed_tab_schema->get_table_name());
       ObString db_name;
       ObString org_table_name;
       char ptr_db_name[OB_MAX_DATBASE_NAME_LENGTH + 1];
       db_name.assign_buffer(ptr_db_name, OB_MAX_DATBASE_NAME_LENGTH + 1);

       char ptr_table_name[OB_MAX_TABLE_NAME_LENGTH + 1];
       org_table_name.assign_buffer(ptr_table_name, OB_MAX_TABLE_NAME_LENGTH + 1);

       bool split_ret = index_table_name_with_dbname.split_two(db_name, org_table_name);
       if(!split_ret)
       {
         TRANS_LOG("Name is too long!");
         ret = OB_ERROR;
       }
       else
       {
         strncpy(table_schema.dbname_, db_name.ptr(), db_name.length());
         // add zhangcd [multi_database.seconary_index] 20150721:b
         table_schema.dbname_[db_name.length()] = 0;
         // add:e
       }*/
            //add e
            ih.tbl_tid = tid;
            ih.status = INDEX_INIT;
            //modify e
            int64_t rowkey_id=0;
            for(int64_t i = 0;OB_SUCCESS == ret&&i<crt_idx_stmt->get_index_colums_count()+ori.get_size();i++)
            {
                ColumnSchema col;
                ObString col_name;
                uint64_t cid;
                /*这个地方可能会出问题，要注意review下代码*/
                const ObColumnSchemaV2* ocs2=NULL;
                if(i<crt_idx_stmt->get_index_colums_count())
                {
                    col_name.reset();
                    col_name = crt_idx_stmt->get_index_columns(i);
                    memset(str_cname,0,common::OB_MAX_COLUMN_NAME_LENGTH);
                    memcpy(str_cname,col_name.ptr(),col_name.length());
                    ocs2 = sql_context_->schema_manager_->get_column_schema(str_tname,str_cname);
                    if(NULL == ocs2)
                    {
                        ret=OB_ERR_INVALID_SCHEMA;
                        TBSYS_LOG(ERROR,"get source table column schema error,t_name=%s,col_name=%s",
                                  str_tname,str_cname);
                    }
                    else
                    {
                        cid = ocs2->get_id();
                        if(idxed_tab_schema->get_rowkey_info().is_rowkey_column(cid))
                        {
                            if(OB_SUCCESS != (ret = crt_idx_stmt->push_hit_rowkey(cid)))
                            {
                                ret = OB_ERROR;
                                TBSYS_LOG(WARN,"push rowkey in hit array failed");
                            }
                            else
                            {
                                table_schema.rowkey_column_num_--;
                            }
                        }
                        rowkey_will_add_in = true;
                        rowkey_id++;
                        //column_num++;
                    }
                }
                else
                {
                    col_name.reset();
                    int64_t rowkey_seq = i-crt_idx_stmt->get_index_colums_count();
                    ori.get_column_id(rowkey_seq,cid);
                    ocs2 = sql_context_->schema_manager_->get_column_schema(tid,cid);
                    if(NULL == ocs2)
                    {
                        ret = OB_ERR_INVALID_SCHEMA;
                        TBSYS_LOG(ERROR,"get source table column schema error,t_name=%s,col_name=%s",
                                  str_tname,str_cname);
                    }
                    else
                    {
                        col_name.assign_ptr((char*)ocs2->get_name(),static_cast<int32_t>(strlen(ocs2->get_name())));
                        if(crt_idx_stmt->is_rowkey_hit(cid))
                        {
                            rowkey_will_add_in = false;
                        }
                        else
                        {
                            rowkey_will_add_in = true;
                            rowkey_id++;
                            //column_num++;
                        }
                    }
                }
                if(OB_SUCCESS == ret&&rowkey_will_add_in)
                {
                    col.column_id_ = ocs2->get_id();
                    //add wenghaixing [secondary index] 20141111
                    if(col.column_id_ > max_col_id)
                    {
                        max_col_id = col.column_id_;
                    }
                    //add e
                    buf_len = sizeof(col.column_name_);
                    if (col_name.length() < buf_len)
                    {
                        len = col_name.length();
                    }
                    else
                    {
                        len = buf_len - 1;
                        TRANS_LOG("Column name is truncated to '%s'",str_cname);
                    }
                    memcpy(col.column_name_, col_name.ptr(), len);
                    col.column_name_[len] = '\0';
                    col.data_type_ = ocs2->get_type();
                    //modify liuxiao [secondary index bug fix] 20150715
                    //col.data_length_ = ocs2->get_default_value().get_val_len();
                    col.data_length_ = ocs2->get_size();
                    //modify e
                    if (col.data_type_ == ObVarcharType && 0 > col.data_length_)
                    {
                        col.data_length_ = OB_MAX_VARCHAR_LENGTH;
                    }
                    col.length_in_rowkey_ = ocs2->get_default_value().get_val_len();
                    col.data_precision_ = ocs2->get_precision();
                    col.data_scale_ = ocs2->get_scale();
                    col.nullable_ = ocs2->is_nullable();
                    col.rowkey_id_ = rowkey_id;
                    col.column_group_id_ = ocs2->get_column_group_id();
                    col.join_table_id_ = OB_INVALID_ID;
                    col.join_column_id_ = OB_INVALID_ID;
                    // col.column_id_=cid;
                    this_index_col_num ++;
                    if (OB_SUCCESS != (ret = table_schema.add_column(col)))
                    {
                        TRANS_LOG("Add column definition of '%s' failed", table_schema.table_name_);
                        break;
                    }

                    if (OB_SUCCESS == ret)
                    {
                        /*if (OB_SUCCESS != (ret = allocate_column_id(table_schema)))
              {
                TBSYS_LOG(WARN, "fail to allocate column id:ret[%d]", ret);
              }
              */
                        //column_num++;
                    }
                }
            }
            if(OB_SUCCESS == ret)
            {
                if(crt_idx_stmt->get_storing_columns_count()>0)
                {
                    crt_idx_stmt->set_has_storing(true);
                }
                for(int64_t i = 0;OB_SUCCESS == ret&&i<crt_idx_stmt->get_storing_columns_count();i++)
                {
                    const ObColumnSchemaV2* ocs2=NULL;
                    ColumnSchema col;
                    ObString col_name = crt_idx_stmt->get_storing_columns(i);
                    //ObString storing_col=crt_idx_stmt->get_storing_columns(i);
                    memset(str_cname,0,common::OB_MAX_COLUMN_NAME_LENGTH);
                    memcpy(str_cname,col_name.ptr(),col_name.length());
                    ocs2 = sql_context_->schema_manager_->get_column_schema(str_tname,str_cname);
                    if(NULL == ocs2)
                    {
                        ret = OB_ERR_INVALID_SCHEMA;
                        TBSYS_LOG(ERROR,"get source table column schema error,t_name=%s,col_name=%s,i=[%ld]",
                                  str_tname,str_cname,i);
                    }
                    else
                    {
                        col.column_id_ = ocs2->get_id();
                        //add wenghaixing [secondary index] 20141111
                        if(col.column_id_>max_col_id)
                        {
                            max_col_id=col.column_id_;
                        }
                        //add e
                        buf_len = sizeof(col.column_name_);
                        if (col_name.length() < buf_len)
                        {
                            len = col_name.length();
                        }
                        else
                        {
                            len = buf_len - 1;
                            TRANS_LOG("Column name is truncated to '%s'", str_cname);
                        }
                        memcpy(col.column_name_, col_name.ptr(), len);
                        col.column_name_[len] = '\0';
                        col.data_type_ = ocs2->get_type();
                        col.data_length_ = ocs2->get_default_value().get_val_len();
                        if (col.data_type_ == ObVarcharType && 0 > col.data_length_)
                        {
                            col.data_length_ = OB_MAX_VARCHAR_LENGTH;
                        }
                        col.length_in_rowkey_ = ocs2->get_default_value().get_val_len();
                        col.data_precision_ = ocs2->get_precision();
                        col.data_scale_ = ocs2->get_scale();
                        col.nullable_ = ocs2->is_nullable();
                        col.rowkey_id_ = 0;
                        col.column_group_id_ = ocs2->get_column_group_id();
                        col.join_table_id_ = OB_INVALID_ID;
                        col.join_column_id_ = OB_INVALID_ID;
                        this_index_col_num ++;
                        // @todo default_value_;
                        if (OB_SUCCESS != (ret = table_schema.add_column(col)))
                        {
                            TRANS_LOG("Add column definition of '%s' failed", table_schema.table_name_);
                        }
                        if (OB_SUCCESS == ret)
                        {
                            /*
                if (OB_SUCCESS != (ret = allocate_column_id(table_schema)))
                {
                  TBSYS_LOG(WARN, "fail to allocate column id:ret[%d]", ret);
                }
                */
                            //column_num++;
                        }
                    }
                }
            }
            //add wenghaixing [secondary index create fix]20141225
            if(OB_SUCCESS == ret&&!crt_idx_stmt->get_has_storing())
            {
                ColumnSchema col;
                col.rowkey_id_ = 0;
                //modify wenghaixing[secondary index alter_table_debug]20150611
                //col.column_id_ = idxed_tab_schema->get_max_column_id()+1;
                col.column_id_ = OB_INDEX_VIRTUAL_COLUMN_ID;
                //modify e
                col.data_type_ = ObIntType;
                memcpy(col.column_name_,OB_INDEX_VIRTUAL_COL_NAME,strlen(OB_INDEX_VIRTUAL_COL_NAME));
                col.column_name_[strlen(OB_INDEX_VIRTUAL_COL_NAME)]='\0';
                col.column_group_id_ = OB_DEFAULT_COLUMN_GROUP_ID;
                //max_col_id = col.column_id_;
                if (OB_SUCCESS != (ret = table_schema.add_column(col)))
                {
                    TRANS_LOG("Add column definition of '%s' failed", table_schema.table_name_);
                }
            }
            //add e
            //add wenghaixing 20141029
            if(OB_SUCCESS == ret)
            {
                TableSchema& table_schema = crt_tab_op->get_table_schema();
                //table_schema.is_index=true;
                table_schema.add_index_helper(ih);
                //add wenghaixing [secondary index]20141111
                table_schema.max_used_column_id_ = max_col_id;
                //add e
                if(table_schema.rowkey_column_num_ > OB_MAX_ROWKEY_COLUMN_NUMBER)
                {
                    TRANS_LOG("index's rowkey column num cannot be greater than 16");
                    ret = OB_ERR_COLUMN_SIZE;
                }
                if(OB_SUCCESS == ret)
                {
                    if(OB_SUCCESS != (ret = sql_context_->schema_manager_->get_index_column_num(tid,column_num)))
                    {
                        ret = OB_ERROR;
                    }
                    else if(column_num + this_index_col_num > OB_MAX_INDEX_COLUMNS)
                    {
                        TRANS_LOG("index's column num cannot be greater than 100");
                        ret = OB_ERR_INVALID_COLUMN_NUM;
                    }
                }
            }
            //add e
            if(OB_SUCCESS == ret&&0<expire_info.length())
            {
                TableSchema& table_schema = crt_tab_op->get_table_schema();
                // check expire condition
                void *ptr = ob_malloc(sizeof(ObSchemaManagerV2), ObModIds::OB_SCHEMA);
                if (NULL == ptr)
                {
                    TRANS_LOG("no memory");
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                }
                else
                {
                    ObSchemaManagerV2 *tmp_schema_mgr = new(ptr) ObSchemaManagerV2();
                    table_schema.table_id_ = OB_NOT_EXIST_TABLE_TID;
                    if (OB_SUCCESS != (ret = tmp_schema_mgr->add_new_table_schema(table_schema)))
                    {
                        TRANS_LOG("failed to add new table, err=%d", ret);
                    }
                    else
                        if (OB_SUCCESS != (ret = tmp_schema_mgr->sort_column()))
                        {
                            TRANS_LOG("failed to sort column for schema manager, err=%d", ret);
                        }
                        else if (!tmp_schema_mgr->check_table_expire_condition())
                        {
                            ret = OB_ERR_INVALID_SCHEMA;
                            TRANS_LOG("invalid expire info `%.*s'", expire_info.length(),
                                      expire_info.ptr());
                        }
                    tmp_schema_mgr->~ObSchemaManagerV2();
                    ob_free(tmp_schema_mgr);
                    tmp_schema_mgr = NULL;
                    table_schema.table_id_ = OB_INVALID_ID;
                }
            }
        }
    }

    return ret;
}
//add e
//add wenghaixing [secondary index upd] 20141204
int ObTransformer::cons_whole_row_desc(uint64_t table_id, ObRowDesc &desc, ObRowDescExt &desc_ext)
{
    int ret = OB_SUCCESS;
    const ObTableSchema *table_schema = sql_context_->schema_manager_->get_table_schema(table_id);
    ObRowkeyInfo ori;
    uint64_t cid = OB_INVALID_ID;
    uint64_t max_col_id = OB_INVALID_ID;
    ObObj obj_type;
    if(NULL == table_schema)
    {
        TBSYS_LOG(ERROR,"Table_Schema pointer is NULL");
        ret = OB_SCHEMA_ERROR;
    }
    else
    {
        ori = table_schema->get_rowkey_info();
        desc.set_rowkey_cell_count(ori.get_size());
        for(int64_t i = 0;i<ori.get_size();i++)
        {
            const ObColumnSchemaV2* ocs = NULL;
            if(OB_SUCCESS != (ret = ori.get_column_id(i,cid)))
            {
                TBSYS_LOG(WARN,"get_column_id for rowkey failed!");
                ret = OB_SCHEMA_ERROR;
                break;
            }
            else
            {
                ocs = sql_context_->schema_manager_->get_column_schema(table_id,cid);
                if(NULL == ocs)
                {
                    TBSYS_LOG(WARN,"NULL Pointer of column schmea");
                    ret = OB_SCHEMA_ERROR;
                    break;
                }
                if(OB_SUCCESS != (ret = desc.add_column_desc(table_id,cid)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc!");
                    ret = OB_ERROR;
                    break;
                }
                else
                {
                    obj_type.set_type(ocs->get_type());
                    obj_type.set_precision(ocs->get_precision());
                    obj_type.set_scale(ocs->get_scale());
                }
                if(OB_SUCCESS == ret&&OB_SUCCESS != (ret = desc_ext.add_column_desc(ocs->get_table_id(),ocs->get_id(),obj_type)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc_ext!");
                    ret = OB_ERROR;
                    break;
                }
            }
        }
        max_col_id = table_schema->get_max_column_id();
        for (int64_t j = OB_APP_MIN_COLUMN_ID; j <= (int64_t)max_col_id;  j++)
        {
            bool column_hit_index = false;
            const ObColumnSchemaV2* ocs=sql_context_->schema_manager_->get_column_schema(table_id,j);
            if(NULL == ocs)
            {
                TBSYS_LOG(WARN,"get column schema error!");
                ret = OB_SCHEMA_ERROR;
            }
            else if(OB_SUCCESS != (ret = sql_context_->schema_manager_->column_hint_index(table_id,(uint64_t)j,column_hit_index)))
            {
                TBSYS_LOG(WARN, "failed to check if column hit index");
                ret = OB_ERROR;
            }
            else if(ori.is_rowkey_column(j) || !column_hit_index)
            {}
            else
            {
                if(OB_SUCCESS != (ret = desc.add_column_desc(table_id,j)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc!");
                    ret = OB_ERROR;
                }
                else
                {
                    obj_type.set_type(ocs->get_type());
                    obj_type.set_precision(ocs->get_precision());
                    obj_type.set_scale(ocs->get_scale());
                }
                if(OB_SUCCESS == ret&&OB_SUCCESS != (ret = desc_ext.add_column_desc(ocs->get_table_id(),ocs->get_id(),obj_type)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc_ext!");
                    ret = OB_ERROR;
                    break;
                }
            }
        }
    }
    return ret;
}

int ObTransformer::cons_whole_row_desc(ObUpdateStmt *upd_stmt, uint64_t table_id, ObRowDesc &desc, ObRowDescExt &desc_ext)
{
    int ret = OB_SUCCESS;
    const ObTableSchema *table_schema = sql_context_->schema_manager_->get_table_schema(table_id);
    ObRowkeyInfo ori;
    uint64_t cid = OB_INVALID_ID;
    uint64_t max_col_id = OB_INVALID_ID;
    ObObj obj_type;
    if(NULL == table_schema || NULL == upd_stmt)
    {
        TBSYS_LOG(ERROR,"Table_Schema pointer is NULL");
        ret = OB_SCHEMA_ERROR;
    }
    else
    {
        ori = table_schema->get_rowkey_info();
        desc.set_rowkey_cell_count(ori.get_size());
        for(int64_t i = 0;i<ori.get_size();i++)
        {
            const ObColumnSchemaV2* ocs = NULL;
            if(OB_SUCCESS != (ret = ori.get_column_id(i,cid)))
            {
                TBSYS_LOG(WARN,"get_column_id for rowkey failed!");
                ret = OB_SCHEMA_ERROR;
                break;
            }
            else
            {
                ocs = sql_context_->schema_manager_->get_column_schema(table_id,cid);
                if(NULL == ocs)
                {
                    TBSYS_LOG(WARN,"NULL Pointer of column schmea");
                    ret = OB_SCHEMA_ERROR;
                    break;
                }
                if(OB_SUCCESS != (ret = desc.add_column_desc(table_id,cid)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc!");
                    ret = OB_ERROR;
                    break;
                }
                else
                {
                    obj_type.set_type(ocs->get_type());
                    obj_type.set_precision(ocs->get_precision());
                    obj_type.set_scale(ocs->get_scale());
                }
                if(OB_SUCCESS == ret&&OB_SUCCESS != (ret = desc_ext.add_column_desc(ocs->get_table_id(),ocs->get_id(),obj_type)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc_ext!");
                    ret = OB_ERROR;
                    break;
                }
            }
        }
        max_col_id = table_schema->get_max_column_id();
        for (int64_t j = OB_APP_MIN_COLUMN_ID; j <= (int64_t)max_col_id;  j++)
        {
            bool column_hit_index = false;
            const ObColumnSchemaV2* ocs=sql_context_->schema_manager_->get_column_schema(table_id,j);
            if(NULL == ocs)
            {
                TBSYS_LOG(WARN,"get column schema error!");
                ret = OB_SCHEMA_ERROR;
            }
            else if(OB_SUCCESS != (ret = sql_context_->schema_manager_->column_hint_index(table_id,(uint64_t)j,column_hit_index)))
            {
                TBSYS_LOG(WARN, "failed to check if column hit index");
                ret = OB_ERROR;
            }
            else if(ori.is_rowkey_column(j) || !column_hit_index)
            {
                uint64_t cid_set = OB_INVALID_ID;
                for(int64_t index = 0; index < upd_stmt->get_update_column_count(); index ++)
                {
                    if (OB_SUCCESS != (ret = upd_stmt->get_update_column_id(index, cid_set)))
                    {
                        TBSYS_LOG(WARN, "fail to get update column id for table %lu column_idx=%lu", table_id, index);
                        break;
                    }
                    else if((int64_t)cid_set == j)
                    {
                        ret = desc.add_column_desc(table_id, j);
                        obj_type.set_type(ocs->get_type());
                        obj_type.set_precision(ocs->get_precision());
                        obj_type.set_scale(ocs->get_scale());
                        if(OB_SUCCESS == ret)
                        {
                            desc_ext.add_column_desc(ocs->get_table_id(),ocs->get_id(),obj_type);
                        }
                    }
                }
            }
            else
            {
                if(OB_SUCCESS != (ret = desc.add_column_desc(table_id, j)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc!");
                    ret = OB_ERROR;
                }
                else
                {
                    obj_type.set_type(ocs->get_type());
                    obj_type.set_precision(ocs->get_precision());
                    obj_type.set_scale(ocs->get_scale());
                }
                if(OB_SUCCESS == ret&&OB_SUCCESS != (ret = desc_ext.add_column_desc(ocs->get_table_id(),ocs->get_id(),obj_type)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc_ext!");
                    ret = OB_ERROR;
                    break;
                }
            }
        }
    }
    return ret;
}

int ObTransformer::cons_del_upd_row_desc(ObStmt *stmt, uint64_t table_id, ObRowDesc &desc, ObRowDescExt &desc_ext)
{
    int ret = OB_SUCCESS;
    const ObTableSchema *table_schema = sql_context_->schema_manager_->get_table_schema(table_id);
    ObRowkeyInfo ori;
    uint64_t cid = OB_INVALID_ID;
    uint64_t max_col_id = OB_INVALID_ID;
    ObObj obj_type;
    if(NULL == table_schema || NULL == stmt)
    {
        TBSYS_LOG(ERROR,"Table_Schema pointer is NULL");
        ret = OB_SCHEMA_ERROR;
    }
    else
    {
        ori = table_schema->get_rowkey_info();
        desc.set_rowkey_cell_count(ori.get_size());
        for(int64_t i = 0;i<ori.get_size();i++)
        {
            const ObColumnSchemaV2* ocs = NULL;
            if(OB_SUCCESS != (ret = ori.get_column_id(i,cid)))
            {
                TBSYS_LOG(WARN,"get_column_id for rowkey failed!");
                ret = OB_SCHEMA_ERROR;
                break;
            }
            else
            {
                ocs = sql_context_->schema_manager_->get_column_schema(table_id,cid);
                if(NULL == ocs)
                {
                    TBSYS_LOG(WARN,"NULL Pointer of column schmea");
                    ret = OB_SCHEMA_ERROR;
                    break;
                }
                if(OB_SUCCESS != (ret = desc.add_column_desc(table_id,cid)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc!");
                    ret = OB_ERROR;
                    break;
                }
                else
                {
                    obj_type.set_type(ocs->get_type());
                    obj_type.set_precision(ocs->get_precision());
                    obj_type.set_scale(ocs->get_scale());
                }
                if(OB_SUCCESS == ret&&OB_SUCCESS != (ret = desc_ext.add_column_desc(ocs->get_table_id(),ocs->get_id(),obj_type)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc_ext!");
                    ret = OB_ERROR;
                    break;
                }
            }
        }
        max_col_id = table_schema->get_max_column_id();
    }
    if(OB_SUCCESS == ret)
    {
        bool hit_index = false;
        const ColumnItem* item = NULL;
        for(uint64_t i = 0; i <= max_col_id; i++)
        {
            const ObColumnSchemaV2* ocs = NULL;
            if(NULL == (ocs = sql_context_->schema_manager_->get_column_schema(table_id,i)))
            {
                continue;
            }
            if(OB_SUCCESS != (ret = sql_context_->schema_manager_->column_hint_index(table_id, i,hit_index)))
            {
                TBSYS_LOG(WARN,"failed to check hit index,tid[%ld],cid[%ld], ret[%d]!", table_id, i, ret);
                break;
            }
            else
            {
                item = stmt->get_column_item_by_id(table_id, i);
            }
            if((hit_index && OB_INVALID_INDEX == desc.get_idx(table_id, i)) || (NULL != item && OB_INVALID_INDEX == desc.get_idx(table_id, i)))
            {
                if(OB_SUCCESS != (ret = desc.add_column_desc(table_id,i)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc!");
                    ret = OB_ERROR;
                    break;
                }
                else
                {
                    obj_type.set_type(ocs->get_type());
                    obj_type.set_precision(ocs->get_precision());
                    obj_type.set_scale(ocs->get_scale());
                }
                if(OB_SUCCESS == ret&&OB_SUCCESS != (ret = desc_ext.add_column_desc(ocs->get_table_id(),ocs->get_id(),obj_type)))
                {
                    TBSYS_LOG(WARN,"failed to add column desc_ext!");
                    ret = OB_ERROR;
                    break;
                }
            }
        }
    }
    return ret;
}
int ObTransformer::column_in_update_stmt(ObUpdateStmt *upd_stmt, uint64_t table_id, uint64_t cid, bool &in_stmt)
{
    int ret = OB_SUCCESS;
    if(NULL == upd_stmt)
    {
        ret = OB_INVALID_ARGUMENT;
    }
    else if(table_id != upd_stmt->get_update_table_id())
    {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "upd stmt's tid [%ld] is not equal table_id [%ld]",upd_stmt->get_update_table_id(), table_id );
    }
    else if(!in_stmt)
    {
        uint64_t stmt_cid = OB_INVALID_ID;
        for(int64_t i = 0; i < upd_stmt->get_update_column_count();i++)
        {
            if(OB_SUCCESS != (ret = upd_stmt->get_update_column_id(i, stmt_cid)))
            {
                ret = OB_ERROR;
            }
            else if(cid == stmt_cid)
            {
                in_stmt = true;
                break;
            }
        }
    }
    return ret;
}
//add e
int ObTransformer::gen_physical_create_table(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObCreateTableStmt *crt_tab_stmt = NULL;
    ObCreateTable     *crt_tab_op = NULL;

    /* get statement */
    if (ret == OB_SUCCESS)
    {
        get_stmt(logical_plan, err_stat, query_id, crt_tab_stmt);
    }

    if (OB_SUCCESS == ret)
    {
        const ObString& table_name = crt_tab_stmt->get_table_name();
        //modify by wenghaixing [secondary index drop table_with_index]20150126
        //if (TableSchema::is_system_table(table_name) //old code
        if (TableSchema::is_system_table_v2(table_name)
                //modify e
                && sql_context_->session_info_->is_create_sys_table_disabled())
        {
            ret = OB_ERR_NO_PRIVILEGE;
            TBSYS_LOG(USER_ERROR, "invalid table name to create, table_name=%.*s",
                      table_name.length(), table_name.ptr());
        }
    }
    //add dolphin [database manager]@20150617:b
    if(OB_SUCCESS == ret)
    {

        ret = check_dbname_for_table(err_stat,crt_tab_stmt->get_db_name());
    }
    //add:e
    /* generate operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(crt_tab_op, ObCreateTable, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            crt_tab_op->set_sql_context(*sql_context_);
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, crt_tab_stmt, crt_tab_op, index);
        }
    }

    if (OB_SUCCESS == ret)
    {
        if (crt_tab_stmt->get_column_size() > OB_MAX_USER_DEFINED_COLUMNS_COUNT)
        {
            TRANS_LOG("Too many columns (max allowed is %ld).",
                      OB_MAX_USER_DEFINED_COLUMNS_COUNT);
            ret = OB_ERR_INVALID_COLUMN_NUM;
        }
    }

    //add zhaoqiong [roottable tablet management]20150318:b
    if (OB_SUCCESS == ret)
    {
        if ((crt_tab_stmt->get_replica_num() > OB_MAX_COPY_COUNT || crt_tab_stmt->get_replica_num() <= 0)
            //add lbzhong [Paxos Cluster.Balance] 20160707:b
            && OB_DEFAULT_COPY_COUNT != crt_tab_stmt->get_replica_num()
            )
        {
            TRANS_LOG("Illegal replica num (max allowed is %d).",
                      OB_MAX_COPY_COUNT);
            ret = OB_ERR_ILLEGAL_VALUE;
        }
    }
    //add e
    if (ret == OB_SUCCESS)
    {
        int buf_len = 0;
        int len = 0;
        //schema is not available yet
        TableSchema& table_schema = crt_tab_op->get_table_schema();
        const ObString& table_name = crt_tab_stmt->get_table_name();
        buf_len = sizeof(table_schema.table_name_);//OB_MAX_TABLE_NAME_LENGTH
        if (table_name.length() < buf_len)//strlen() not including the terminating '\0' character
        {
            len = table_name.length();
        }
        else
        {
            len = buf_len - 1;
            TRANS_LOG("Table name is truncated to '%.*s'", len, table_name.ptr());
        }
        memcpy(table_schema.table_name_, table_name.ptr(), len);
        table_schema.table_name_[len] = '\0';
        //add dolphin [database manager]@20150614:b
        buf_len = sizeof(table_schema.dbname_);
        const ObString& dbname = crt_tab_stmt->get_db_name();
        if (dbname.length() < buf_len)
        {
            len = dbname.length();
        }
        else
        {
            len = buf_len - 1;
            TRANS_LOG("DataBase name is truncated to '%.*s'", len, dbname.ptr());
        }
        memcpy(table_schema.dbname_, dbname.ptr(), len);
        table_schema.dbname_[len] = '\0';
        //add:e
        const ObString& expire_info = crt_tab_stmt->get_expire_info();
        buf_len = sizeof(table_schema.expire_condition_);
        if (expire_info.length() < buf_len)
        {
            len = expire_info.length();
        }
        else
        {
            len = buf_len - 1;
            TRANS_LOG("Expire_info is truncated to '%.*s'", len, expire_info.ptr());
        }
        memcpy(table_schema.expire_condition_, expire_info.ptr(), len);
        table_schema.expire_condition_[len] = '\0';
        const ObString& comment_str = crt_tab_stmt->get_comment_str();
        buf_len = sizeof(table_schema.comment_str_);
        if (comment_str.length() < buf_len)
        {
            len = comment_str.length();
        }
        else
        {
            len = buf_len - 1;
            TRANS_LOG("Comment_str is truncated to '%.*s'", len, comment_str.ptr());
        }
        memcpy(table_schema.comment_str_, comment_str.ptr(), len);
        table_schema.comment_str_[len] = '\0';
        crt_tab_op->set_if_not_exists(crt_tab_stmt->get_if_not_exists());
        if (crt_tab_stmt->get_tablet_max_size() > 0)
            table_schema.tablet_max_size_ = crt_tab_stmt->get_tablet_max_size();
        if (crt_tab_stmt->get_tablet_block_size() > 0)
            table_schema.tablet_block_size_ = crt_tab_stmt->get_tablet_block_size();
        if (crt_tab_stmt->get_table_id() != OB_INVALID_ID)
            table_schema.table_id_ = crt_tab_stmt->get_table_id();
        table_schema.replica_num_ = crt_tab_stmt->get_replica_num();
        table_schema.is_use_bloomfilter_ = crt_tab_stmt->use_bloom_filter();
        table_schema.consistency_level_ = crt_tab_stmt->get_consistency_level();
        table_schema.rowkey_column_num_ = static_cast<int32_t>(crt_tab_stmt->get_primary_key_size());
        const ObString& compress_method = crt_tab_stmt->get_compress_method();
        buf_len = sizeof(table_schema.compress_func_name_);
        const char *func_name = compress_method.ptr();
        len = compress_method.length();
        if (len <= 0)
        {
            func_name = OB_DEFAULT_COMPRESS_FUNC_NAME;
            len = static_cast<int>(strlen(OB_DEFAULT_COMPRESS_FUNC_NAME));
        }
        if (len >= buf_len)
        {
            len = buf_len - 1;
            TRANS_LOG("Compress method name is truncated to '%.*s'", len, func_name);
        }
        memcpy(table_schema.compress_func_name_, func_name, len);
        table_schema.compress_func_name_[len] = '\0';

        for (int64_t i = 0; ret == OB_SUCCESS && i < crt_tab_stmt->get_column_size(); i++)
        {
            const ObColumnDef& col_def = crt_tab_stmt->get_column_def(i);
            ColumnSchema col;
            col.column_id_ = col_def.column_id_;
            if (static_cast<int64_t>(col.column_id_) > table_schema.max_used_column_id_)
            {
                table_schema.max_used_column_id_ = col.column_id_;
            }
            buf_len = sizeof(col.column_name_);
            if (col_def.column_name_.length() < buf_len)
            {
                len = col_def.column_name_.length();
            }
            else
            {
                len = buf_len - 1;
                TRANS_LOG("Column name is truncated to '%.*s'", len, col_def.column_name_.ptr());
            }
            memcpy(col.column_name_, col_def.column_name_.ptr(), len);
            col.column_name_[len] = '\0';
            col.data_type_ = col_def.data_type_;
            col.data_length_ = col_def.type_length_;
            if (col.data_type_ == ObVarcharType && 0 > col_def.type_length_)
            {
                col.data_length_ = OB_MAX_VARCHAR_LENGTH;
            }
            col.length_in_rowkey_ = col_def.type_length_;
            col.data_precision_ = col_def.precision_;
            col.data_scale_ = col_def.scale_;
            col.nullable_ = !col_def.not_null_;
            col.rowkey_id_ = col_def.primary_key_id_;
            col.column_group_id_ = 0;
            col.join_table_id_ = OB_INVALID_ID;
            col.join_column_id_ = OB_INVALID_ID;

            // @todo default_value_;
            if ((ret = table_schema.add_column(col)) != OB_SUCCESS)
            {
                TRANS_LOG("Add column definition of '%s' failed", table_schema.table_name_);
                break;
            }

            if (OB_SUCCESS == ret)
            {
                if (OB_SUCCESS != (ret = allocate_column_id(table_schema)))
                {
                    TBSYS_LOG(WARN, "fail to allocate column id:ret[%d]", ret);
                }
            }
        }
    }

    if (OB_SUCCESS == ret && 0 < crt_tab_stmt->get_join_info().length())
    {
        const ObString &join_info_str = crt_tab_stmt->get_join_info();
        TBSYS_LOG(DEBUG, "create table join info[%.*s]", join_info_str.length(), join_info_str.ptr());
        if ( !parse_join_info(join_info_str, crt_tab_op->get_table_schema()) )
        {
            ret = OB_ERR_PARSE_JOIN_INFO;
            TRANS_LOG("Wrong join info, please check join info");
        }
    }

    if (OB_SUCCESS == ret && 0 < crt_tab_stmt->get_expire_info().length())
    {
        TableSchema& table_schema = crt_tab_op->get_table_schema();
        // check expire condition
        void *ptr = ob_malloc(sizeof(ObSchemaManagerV2), ObModIds::OB_SCHEMA);
        if (NULL == ptr)
        {
            TRANS_LOG("no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
            ObSchemaManagerV2 *tmp_schema_mgr = new(ptr) ObSchemaManagerV2();
            table_schema.table_id_ = OB_NOT_EXIST_TABLE_TID;
            if (OB_SUCCESS != (ret = tmp_schema_mgr->add_new_table_schema(table_schema)))
            {
                TRANS_LOG("failed to add new table, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = tmp_schema_mgr->sort_column()))
            {
                TRANS_LOG("failed to sort column for schema manager, err=%d", ret);
            }
            else if (!tmp_schema_mgr->check_table_expire_condition())
            {
                ret = OB_ERR_INVALID_SCHEMA;
                TRANS_LOG("invalid expire info `%.*s'", crt_tab_stmt->get_expire_info().length(),
                          crt_tab_stmt->get_expire_info().ptr());
            }
            tmp_schema_mgr->~ObSchemaManagerV2();
            ob_free(tmp_schema_mgr);
            tmp_schema_mgr = NULL;
            table_schema.table_id_ = OB_INVALID_ID;
        }
    }
    return ret;
}

/// TODO: not allocate column group id
int ObTransformer::allocate_column_id(TableSchema & table_schema)
{
    int ret = OB_SUCCESS;
    bool has_got_create_time_type = false;
    bool has_got_modify_time_type = false;
    ColumnSchema * column = NULL;
    uint64_t column_id = OB_APP_MIN_COLUMN_ID;

    table_schema.max_used_column_id_ = column_id;
    for (int64_t i = 0; i < table_schema.get_column_count(); ++i)
    {
        column = table_schema.get_column_schema(i);
        if (NULL == column)
        {
            ret = OB_INPUT_PARAM_ERROR;
            TBSYS_LOG(WARN, "check column schema failed:table_name[%s], index[%ld]",
                      table_schema.table_name_, i);
            break;
        }
        else if (ObCreateTimeType == column->data_type_) // create time
        {
            column->column_id_ = OB_CREATE_TIME_COLUMN_ID;
            if (has_got_create_time_type)
            {
                // duplication case checked by parser, double check
                ret = OB_INPUT_PARAM_ERROR;
                TBSYS_LOG(WARN, "find duplicated create time column:table_name[%s], index[%ld]",
                          table_schema.table_name_, i);
                break;
            }
            else
            {
                has_got_create_time_type = true;
            }
        }
        else if (ObModifyTimeType == column->data_type_) // last_modify time
        {
            column->column_id_ = OB_MODIFY_TIME_COLUMN_ID;
            if (has_got_modify_time_type)
            {
                // duplication case checked by parser, double check
                ret = OB_INPUT_PARAM_ERROR;
                TBSYS_LOG(WARN, "find duplicated modify time column:table_name[%s], index[%ld]",
                          table_schema.table_name_, i);
                break;
            }
            else
            {
                has_got_modify_time_type = true;
            }
        }
        else
        {
            table_schema.max_used_column_id_ = column_id;
            column->column_id_ = column_id++;
        }
    }
    return ret;
}

int ObTransformer::gen_physical_alter_table(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObAlterTableStmt *alt_tab_stmt = NULL;
    ObAlterTable     *alt_tab_op = NULL;

    /* get statement */
    if (ret == OB_SUCCESS)
    {
        get_stmt(logical_plan, err_stat, query_id, alt_tab_stmt);
    }

    if (OB_SUCCESS == ret)
    {
        const ObString& table_name = alt_tab_stmt->get_table_name();
        if (TableSchema::is_system_table(table_name)
                && sql_context_->session_info_->is_create_sys_table_disabled())
        {
            ret = OB_ERR_NO_PRIVILEGE;
            TBSYS_LOG(USER_ERROR, "invalid table name to alter, table_name=%.*s",
                      table_name.length(), table_name.ptr());
        }
    }

    /* generate operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(alt_tab_op, ObAlterTable, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            alt_tab_op->set_sql_context(*sql_context_);
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, alt_tab_stmt, alt_tab_op, index);
        }
    }

    if (ret == OB_SUCCESS)
    {
        AlterTableSchema& alter_schema = alt_tab_op->get_alter_table_schema();
        alter_schema.clear();//Add By LiuJun.
        const ObString& table_name = alt_tab_stmt->get_table_name();
        memcpy(alter_schema.table_name_, table_name.ptr(), table_name.length());
        alter_schema.table_name_[table_name.length()] = '\0';
        alter_schema.table_id_ = alt_tab_stmt->get_table_id();

        //add dolphin [database manager]@20150618:b
        memcpy(alter_schema.dbname_,alt_tab_stmt->get_db_name().ptr(),alt_tab_stmt->get_db_name().length());
        alter_schema.dbname_[alt_tab_stmt->get_db_name().length()] = 0;
        //add:e
        //add liuj [Alter_Rename] [JHOBv0.1] 20150104
        if(alt_tab_stmt->has_table_rename())
        {
            alter_schema.has_table_rename_ = true;
            const ObString& new_table_name = alt_tab_stmt->get_new_table_name();
            memcpy(alter_schema.new_table_name_,new_table_name.ptr(),new_table_name.length());
            alter_schema.new_table_name_[new_table_name.length()] = '\0';

            if (TableSchema::is_system_table(new_table_name)
                    && sql_context_->session_info_->is_create_sys_table_disabled())
            {
                ret = OB_ERR_NO_PRIVILEGE;
                TBSYS_LOG(USER_ERROR, "invalid retable name to alter, table_name=%.*s",
                          new_table_name.length(), new_table_name.ptr());
            }
        }
        //add e

        hash::ObHashMap<common::ObString, ObColumnDef>::iterator iter;
        for (iter = alt_tab_stmt->column_begin();
             ret == OB_SUCCESS && iter != alt_tab_stmt->column_end();
             iter++)
        {
            AlterTableSchema::AlterColumnSchema alt_col;
            ObColumnDef& col_def = iter->second;
            alt_col.column_.column_id_ = col_def.column_id_;
            memcpy(alt_col.column_.column_name_, col_def.column_name_.ptr(), col_def.column_name_.length());
            alt_col.column_.column_name_[col_def.column_name_.length()] = '\0';
            switch (col_def.action_)
            {
            case ADD_ACTION:
                alt_col.type_ = AlterTableSchema::ADD_COLUMN;
                alt_col.column_.data_type_ = col_def.data_type_;
                alt_col.column_.data_length_ = col_def.type_length_;
                alt_col.column_.data_precision_ = col_def.precision_;
                alt_col.column_.data_scale_ = col_def.scale_;
                alt_col.column_.nullable_ = !col_def.not_null_;
                alt_col.column_.rowkey_id_ = col_def.primary_key_id_;
                alt_col.column_.column_group_id_ = 0;
                alt_col.column_.join_table_id_ = OB_INVALID_ID;
                alt_col.column_.join_column_id_ = OB_INVALID_ID;
                break;
            case DROP_ACTION:
            {


                alt_col.type_ = AlterTableSchema::DEL_COLUMN;
                //add wenghaixing [secondary index alter table]20141227
                bool alt_col_hit_index = false;
                if(OB_SUCCESS != (ret = sql_context_->schema_manager_->column_hint_index(alter_schema.table_id_,alt_col.column_.column_id_,alt_col_hit_index)))
                {
                    TBSYS_LOG(WARN,"failed to get alt_col_hit_index[%d] ",ret);
                    //ret=OB_SCHEMA_ERROR;
                }
                else if(alt_col_hit_index)
                {
                    TRANS_LOG("column [%ld] cannot be deleted,there is a index use it!",alt_col.column_.column_id_);
                    ret = OB_ERROR;
                }
            }
                //add e
                break;
                //mod fyd [NotNULL_check] [JHOBv0.1] 20140108:b
                //case ALTER_ACTION:{
                //alt_col.type_ = AlterTableSchema::MOD_COLUMN;
            case ALTER_ACTION_NULL:
            {
                alt_col.type_ = AlterTableSchema::MOD_COLUMN_NULL;
                //mod 20140108:e
                alt_col.column_.nullable_ = !col_def.not_null_;
                /* default value doesn't exist in ColumnSchema */
                /* FIX ME, get other attributs from schema */
                const ObColumnSchemaV2 *col_schema = NULL;
                if ((col_schema = sql_context_->schema_manager_->get_column_schema(
                         alter_schema.table_id_,
                         col_def.column_id_)) == NULL)
                {
                    ret = OB_ERR_TABLE_UNKNOWN;
                    TRANS_LOG("Can not find schema of table '%s'", alter_schema.table_name_);
                    break;
                }
                else
                {
                    alt_col.column_.data_type_ = col_schema->get_type();
                    // alt_col.column_.data_length_ = iter->type_length_;
                    // alt_col.column_.data_precision_ = iter->precision_;
                    // alt_col.column_.data_scale_ = iter->scale_;
                    // alt_col.column_.rowkey_id_ = iter->primary_key_id_;
                    alt_col.column_.column_group_id_ = col_schema->get_column_group_id();
                    //mod fyd [NotNULL_check] [JHOBv0.1] 20140108:b
                    //alt_col.column_.join_table_id_ = col_schema->get_join_info()->join_table_;
                    //alt_col.column_.join_column_id_ = col_schema->get_join_info()->correlated_column_;
                    if(col_schema->get_join_info())
                    {
                        alt_col.column_.join_table_id_ = col_schema->get_join_info()->join_table_;
                        alt_col.column_.join_column_id_ = col_schema->get_join_info()->correlated_column_;
                    }
                    //mod 20140108:e
                }
                break;
            }
                //add liuj [Alter_Rename] [JHOBv0.1] 20150104
            case RENAME_ACTION:
                alt_col.type_ = AlterTableSchema::RENAME_COLUMN;
                memcpy(alt_col.column_.new_column_name_,col_def.new_column_name_.ptr(),col_def.new_column_name_.length());
                alt_col.column_.new_column_name_[col_def.new_column_name_.length()] = '\0';
                break;
                //add e
            default:
                ret = OB_ERR_GEN_PLAN;
                TRANS_LOG("Alter action '%d' is not supported", col_def.action_);
                break;
            }
            //mod fyd [NotNULL_check] [JHOBv0.1] 20140108:b
            if (ret == OB_SUCCESS && (ret = alter_schema.add_column(alt_col.type_, alt_col.column_)) != OB_SUCCESS)
                //if (ret == OB_SUCCESS && (ret = alter_schema.add_column(alt_col.type_, alt_col.column_,alt_col.new_column_name_)) != OB_SUCCESS)
                //mod 20140108:e
            {
                TRANS_LOG("Add alter column '%s' failed", alt_col.column_.column_name_);
                break;
            }
        }
    }
    //add wenghaixing [secondary index alter table]20150126
    const ObTableSchema* schema =NULL;
    if(NULL == (schema = (sql_context_->schema_manager_->get_table_schema(alt_tab_stmt->get_table_id()))))
    {
        TBSYS_LOG(WARN,"failed to get table[%ld] schema",alt_tab_stmt->get_table_id());
        ret = OB_SCHEMA_ERROR;
    }
    else if(OB_INVALID_ID != schema->get_index_helper().tbl_tid)
    {
        TRANS_LOG("can not alter an index table[%ld]",alt_tab_stmt->get_table_id());
        ret = OB_ERROR;
    }
    //add e
    return ret;
}

//add wenghaixing [secondary index drop index]20141223
int ObTransformer::gen_physical_drop_index(ObLogicalPlan *logical_plan, ObPhysicalPlan *physical_plan, ErrStat &err_stat, const uint64_t &query_id, int32_t *index)
{
    int &ret = err_stat.err_code_=OB_SUCCESS;
    ObDropIndexStmt *drp_idx_stmt=NULL;
    ObDropIndex *drp_idx_op =NULL;
    /* get statement */
    if (OB_SUCCESS == ret)
    {
        get_stmt(logical_plan, err_stat, query_id, drp_idx_stmt);
    }
    /* generate operator */
    if(OB_SUCCESS == ret)
    {
        CREATE_PHY_OPERRATOR(drp_idx_op, ObDropIndex, physical_plan, err_stat);
        if (OB_SUCCESS == ret)
        {
            drp_idx_op->set_rpc_stub(sql_context_->rs_rpc_proxy_);
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, drp_idx_stmt, drp_idx_op, index);
        }
    }
    for (int64_t i = 0; OB_SUCCESS == ret && i < drp_idx_stmt->get_table_size(); i++)
    {
        const ObString& table_name = drp_idx_stmt->get_table_name(i);
        // add by zhangcd [multi_database.secondary_index] 20150703:b
        // add:e
        if (OB_SUCCESS != (ret = drp_idx_op->add_index_name(table_name)))
        {
            TRANS_LOG("Add drop index %.*s failed", table_name.length(), table_name.ptr());
            break;
        }
    }
    return ret;
}
//add e
int ObTransformer::gen_physical_drop_table(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObDropTableStmt *drp_tab_stmt = NULL;
    ObDropTable     *drp_tab_op = NULL;

    /* get statement */
    if (ret == OB_SUCCESS)
    {
        get_stmt(logical_plan, err_stat, query_id, drp_tab_stmt);
    }
    bool disallow_drop_sys_table = sql_context_->session_info_->is_create_sys_table_disabled();
    //add dolphin [database manager]@20150617:b
    if(OB_SUCCESS == ret)
    {

        int64_t len = drp_tab_stmt->get_table_size();
        ObString table_name;
        char dn[OB_MAX_DATBASE_NAME_LENGTH];
        char tn[OB_MAX_TABLE_NAME_LENGTH];
        ObString dname;
        ObString tname;
        dname.assign_buffer(dn,OB_MAX_DATBASE_NAME_LENGTH);
        tname.assign_buffer(tn,OB_MAX_TABLE_NAME_LENGTH);
        for (int64_t i = 0;i<len;++i)
        {
            table_name = drp_tab_stmt->get_table_name(i);
            if (!table_name.split_two(dname,tname))
            {
                TRANS_LOG("table name[%.*s] is invalid", table_name.length(), table_name.ptr());
                break;
            }
            else if(OB_SUCCESS != (ret = check_dbname_for_table(err_stat,dname)))
            {
                TRANS_LOG("drop table failed because the database name %.*s not exist", dname.length(), dname.ptr());
                break;
            }
        }

    }
    //add:e
    /* generate operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(drp_tab_op, ObDropTable, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            drp_tab_op->set_rpc_stub(sql_context_->rs_rpc_proxy_);
            drp_tab_op->set_sql_context(*sql_context_);//add liumz, [drop table -> clean table priv]20150902
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, drp_tab_stmt, drp_tab_op, index);
        }
    }

    if (ret == OB_SUCCESS)
    {
        drp_tab_op->set_if_exists(drp_tab_stmt->get_if_exists());
        for (int64_t i = 0; ret == OB_SUCCESS && i < drp_tab_stmt->get_table_size(); i++)
        {
            const ObString& table_name = drp_tab_stmt->get_table_name(i);
            uint64_t tid = drp_tab_stmt->get_table_id(i);//add liumz, [drop table -> clean table priv]20150902
            if (TableSchema::is_system_table(table_name)
                    && disallow_drop_sys_table)
            {
                ret = OB_ERR_NO_PRIVILEGE;
                TBSYS_LOG(USER_ERROR, "system table can not be dropped, table_name=%.*s",
                          table_name.length(), table_name.ptr());
                break;
            }
            if ((ret = drp_tab_op->add_table_name(table_name)) != OB_SUCCESS)
            {
                TRANS_LOG("Add drop table %.*s failed", table_name.length(), table_name.ptr());
                break;
            }
            //add liumz, [drop table -> clean table priv]20150902:b
            else if ((ret = drp_tab_op->add_table_id(tid)) != OB_SUCCESS)
            {
                TRANS_LOG("Add drop table %.*s failed", table_name.length(), table_name.ptr());
                break;
            }
            //add:e
        }
    }

    return ret;
}

//add zhaoqiong [Truncate Table]:20160318:b
int ObTransformer::gen_physical_truncate_table(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObTruncateTableStmt *trun_tab_stmt = NULL;
    ObTruncateTable     *trun_tab_op = NULL;

    /* get statement */
    if (ret == OB_SUCCESS)
    {
        get_stmt(logical_plan, err_stat, query_id, trun_tab_stmt);
    }
    bool disallow_trun_sys_table = sql_context_->session_info_->is_create_sys_table_disabled();
    //add dolphin [database manager]@20150617:b
    if(OB_SUCCESS == ret)
    {

        int64_t len = trun_tab_stmt->get_table_size();
        ObString table_name;
        char dn[OB_MAX_DATBASE_NAME_LENGTH];
        char tn[OB_MAX_TABLE_NAME_LENGTH];
        ObString dname;
        ObString tname;
        dname.assign_buffer(dn,OB_MAX_DATBASE_NAME_LENGTH);
        tname.assign_buffer(tn,OB_MAX_TABLE_NAME_LENGTH);
        for (int64_t i = 0;i<len;++i)
        {
            table_name = trun_tab_stmt->get_table_name(i);
            if (!table_name.split_two(dname,tname))
            {
                TRANS_LOG("table name[%.*s] is invalid", table_name.length(), table_name.ptr());
                break;
            }
            else if(OB_SUCCESS != (ret = check_dbname_for_table(err_stat,dname)))
            {
                TRANS_LOG("truncate table failed because the database name %.*s not exist", dname.length(), dname.ptr());
                break;
            }
        }

    }
    //add:e
    /* generate operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(trun_tab_op, ObTruncateTable, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            trun_tab_op->set_rpc_stub(sql_context_->rs_rpc_proxy_);
            trun_tab_op->set_sql_context(*sql_context_);
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, trun_tab_stmt, trun_tab_op, index);
        }
    }

    if (ret == OB_SUCCESS)
    {
        trun_tab_op->set_if_exists(trun_tab_stmt->get_if_exists());
        for (int64_t i = 0; ret == OB_SUCCESS && i < trun_tab_stmt->get_table_size(); i++)
        {
            const ObString& table_name = trun_tab_stmt->get_table_name(i);
            if (TableSchema::is_system_table(table_name)
                    && disallow_trun_sys_table)
            {
                ret = OB_ERR_NO_PRIVILEGE;
                TBSYS_LOG(USER_ERROR, "system table can not be truncated, table_name=%.*s",
                          table_name.length(), table_name.ptr());
                break;
            }
            if ((ret = trun_tab_op->add_table_name(table_name)) != OB_SUCCESS)
            {
                TRANS_LOG("Add trun table %.*s failed", table_name.length(), table_name.ptr());
                break;
            }
        }
    }

    if (ret == OB_SUCCESS && trun_tab_stmt->get_comment().length() != 0)
    {
      trun_tab_op->set_comment(trun_tab_stmt->get_comment());
      TBSYS_LOG(DEBUG, "add stmt comment, comment=%.*s",
                trun_tab_stmt->get_comment().length(), trun_tab_stmt->get_comment().ptr());
    }
    return ret;
}
//add:e

// add by zhangcd [multi_database.bugfix] 20150805:e
int ObTransformer::get_all_databases(ErrStat& err_stat, ObStrings &databases)
{
    int ret = OB_SUCCESS;
    char select_databases_buff[512];
    int cnt = snprintf(select_databases_buff, 512, "select db_name from __all_database");
    ObString select_databases;
    select_databases.assign_ptr(select_databases_buff, cnt);
    ObResultSet tmp_result;
    if (OB_SUCCESS != (ret = tmp_result.init()))
    {
        TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_databases, tmp_result, *sql_context_)))
    {
        TRANS_LOG(tmp_result.get_message());
        TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_databases.length(), select_databases.ptr(), ret);
    }
    else if (OB_SUCCESS != (ret = tmp_result.open()))
    {
        TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_databases.length(), select_databases.ptr(), ret);
    }
    else
    {
        OB_ASSERT(tmp_result.is_with_rows() == true);
        const ObRow* row = NULL;
        while (ret == OB_SUCCESS)
        {
            ret = tmp_result.get_next_row(row);
            if (OB_ITER_END == ret)
            {
                ret = OB_SUCCESS;
                break;
            }
            else if (OB_SUCCESS != ret)
            {
                TBSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
            }
            else
            {
                const ObObj *pcell = NULL;
                uint64_t table_id = OB_INVALID_ID;
                uint64_t column_id = OB_INVALID_ID;
                ret = row->raw_get_cell(0, pcell, table_id, column_id);

                if (OB_SUCCESS == ret)
                {
                    if (pcell->get_type() == ObVarcharType)
                    {
                        ObString db_name;
                        if (OB_SUCCESS != (ret = pcell->get_varchar(db_name)))
                        {
                            TBSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
                        }
                        else
                        {
                            if (OB_SUCCESS != (ret = databases.add_string(db_name)))
                            {
                                TBSYS_LOG(WARN, "add db_name[%.*s] to array failed, ret=%d", db_name.length(), db_name.ptr(), ret);
                            }
                        }
                    }
                    else
                    {
                        ret = OB_ERR_UNEXPECTED;
                        TBSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObVarcharType);
                    }
                }
                else
                {
                    TBSYS_LOG(WARN, "raw get cell(db_name) failed, ret=%d", ret);
                }
            }
        }// while
        //add liumz, [bugfix: close result set]20150914:b
        int err = tmp_result.close();
        if (OB_SUCCESS != err)
        {
            TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
        }
        tmp_result.reset();
        //add:e
    }
    return ret;
}
// add:e

// add by zhangcd [multi_database.show_databases] 20150617:b
int ObTransformer::gen_phy_show_databases(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObRowDesc row_desc;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    ObValues *values_op = NULL;
    CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat);
    if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
    {
        if (show_stmt->get_column_size() != 1)
        {
            TBSYS_LOG(WARN, "wrong columns' number of %s, column size %d", OB_DATABASES_SHOW_TABLE_NAME, show_stmt->get_column_size());
            ret = OB_ERR_COLUMN_SIZE;
            TRANS_LOG("wrong columns' number of %s", OB_DATABASES_SHOW_TABLE_NAME);
        }
        else
        {
            const ColumnItem* column_item = show_stmt->get_column_item(0);
            table_id = column_item->table_id_;
            column_id = column_item->column_id_;
            if ((ret = row_desc.add_column_desc(table_id, column_id)) != OB_SUCCESS
                    || (ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
            {
                TRANS_LOG("add row desc error, err=%d", ret);
            }
        }

    // mod by zhangcd [multi_database.bugfix] 20150805:e
    //  oceanbase::common::ObPrivilege *p_privilege_ = const_cast<oceanbase::common::ObPrivilege *>(*sql_context_->pp_privilege_);
    //  const ObPrivilege::NameDbMap* name_db_map = p_privilege_->get_database_name_map();

        ObStrings db_names;
        //add liumz, [bugfix: disable privilege check when show databases]20150914:b
        bool before_switch = sql_context_->disable_privilege_check_;
        sql_context_->disable_privilege_check_ = true;
        //add:e
        if(OB_SUCCESS != (ret = get_all_databases(err_stat, db_names)))
        {
            TBSYS_LOG(WARN, "failed to get databases name from __all_databses! ret=[%d]", ret);
        }
        sql_context_->disable_privilege_check_ = before_switch;//add liumz, [bugfix: disable privilege check when show databases]
        //  ObPrivilege::NameDbMap::const_iterator it = name_db_map->begin();
        for(int i = 0; ret == OB_SUCCESS && i < db_names.count(); ++i)
        {
            ObRow val_row;
            ObString db_name;
            ObObj value;
            if(OB_SUCCESS != (ret = db_names.get_string(i, db_name)))
            {
                TBSYS_LOG(WARN, "get string from db_names failed! ret=[%d]", ret);
                break;
            }
            value.set_varchar(db_name);
            val_row.set_row_desc(row_desc);
            if ((ret = val_row.set_cell(table_id, column_id, value)) != OB_SUCCESS)
            {
                TRANS_LOG("Add value to ObRow failed");
                break;
            }
            else if (ret == OB_SUCCESS && (ret = values_op->add_values(val_row)) != OB_SUCCESS)
            {
                TRANS_LOG("Add value row failed");
                break;
            }
        }
    }
    // mod:e
    if (ret == OB_SUCCESS)
    {
        out_op = values_op;
    }

    return ret;
}
// add by zhangcd [multi_database.show_databases] 20150617:e

// add by zhangcd [multi_database.show_databases] 20150617:b
int ObTransformer::gen_phy_show_current_database(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObRowDesc row_desc;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    ObValues *values_op = NULL;
    CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat);
    if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
    {
        if (show_stmt->get_column_size() != 1)
        {
            TBSYS_LOG(WARN, "wrong columns' number of %s, column size %d", OB_DATABASES_SHOW_TABLE_NAME, show_stmt->get_column_size());
            ret = OB_ERR_COLUMN_SIZE;
            TRANS_LOG("wrong columns' number of %s", OB_DATABASES_SHOW_TABLE_NAME);
        }
        else
        {
            const ColumnItem* column_item = show_stmt->get_column_item(0);
            table_id = column_item->table_id_;
            column_id = column_item->column_id_;
            if ((ret = row_desc.add_column_desc(table_id, column_id)) != OB_SUCCESS
                    || (ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
            {
                TRANS_LOG("add row desc error, err=%d", ret);
            }
        }

    const common::ObString& db_name = sql_context_->session_info_->get_db_name();

        if(db_name.length() != 0)
        {
            ObRow val_row;
            ObObj value;
            value.set_varchar(db_name);
            val_row.set_row_desc(row_desc);
            if ((ret = val_row.set_cell(table_id, column_id, value)) != OB_SUCCESS)
            {
                TRANS_LOG("Add value to ObRow failed");
            }
            else if (ret == OB_SUCCESS && (ret = values_op->add_values(val_row)) != OB_SUCCESS)
            {
                TRANS_LOG("Add value row failed");
            }
        }
    }
    if (ret == OB_SUCCESS)
    {
        out_op = values_op;
    }

    return ret;
}
// add by zhangcd [multi_database.show_databases] 20150617:e

int ObTransformer::gen_phy_show_tables(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObRowDesc row_desc;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    ObValues *values_op = NULL;
    CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat);
    if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
    {
        if (show_stmt->get_column_size() != 1)
        {
            TBSYS_LOG(WARN, "wrong columns' number of %s", OB_TABLES_SHOW_TABLE_NAME);
            ret = OB_ERR_COLUMN_SIZE;
            TRANS_LOG("wrong columns' number of %s", OB_TABLES_SHOW_TABLE_NAME);
        }
        else
        {
            const ColumnItem* column_item = show_stmt->get_column_item(0);
            table_id = column_item->table_id_;
            column_id = column_item->column_id_;
            if ((ret = row_desc.add_column_desc(table_id, column_id)) != OB_SUCCESS
                    || (ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
            {
                TRANS_LOG("add row desc error, err=%d", ret);
            }
        }

    // modify by zhangcd [multi_database.show_tables] 20150615:b
    // user tables show
    const common::ObString& db_name = sql_context_->session_info_->get_db_name();
    const ObTableSchema* it = sql_context_->schema_manager_->table_begin();
    for(; ret == OB_SUCCESS && it != sql_context_->schema_manager_->table_end(); it++)
    {
        ObString table_name_str;
        if(IS_SYS_TABLE_ID(it->get_table_id()))
        {
            /* skip system tables */
            continue;
        }
        else
        {
            ObString complete_table_name = ObString::make_string(it->get_table_name());
            char db_name_array[OB_MAX_DATBASE_NAME_LENGTH];
            void *table_name_ptr = NULL;
            ObString db_name_str(sizeof(db_name_array), 0, db_name_array);

            if (NULL == (table_name_ptr = mem_pool_->alloc(OB_MAX_TABLE_NAME_LENGTH)))
            {
                TRANS_LOG("mem-over flow, generate show table plan failed!");
                ret = OB_ALLOCATE_MEMORY_FAILED;
                break;
            }
            table_name_str.assign_buffer((char*)table_name_ptr, OB_MAX_TABLE_NAME_LENGTH);

            bool split_ret = complete_table_name.split_two(db_name_str, table_name_str, '.');
            // sys tables, not belong to any db
            if(!split_ret)
            {
                TRANS_LOG("table name[%.*s] is invalid!", complete_table_name.length(), complete_table_name.ptr());
                continue;
            }
            else if(db_name_str.length() == 0)
            {
                TRANS_LOG("table name[%.*s] is invalid!", complete_table_name.length(), complete_table_name.ptr());
                continue;
            }
            // user tables belong to current db
            else if(db_name_str == db_name)
            {

            }
            // user tables not belong to current db
            else
            {
                /* skip tables not belong to current db */
                continue;
            }
        }

        ObRow val_row;
        ObString val = table_name_str;
        ObObj value;
        value.set_varchar(val);
        val_row.set_row_desc(row_desc);

        if (it->get_table_id() >= OB_TABLES_SHOW_TID
                && it->get_table_id() <= OB_SERVER_STATUS_SHOW_TID)
        {
            /* skip local show tables */
            continue;
        }
        /* add liumengzhan_show_tables [20141210]
     * filter index tables
     */
        else if (it->get_index_helper().tbl_tid != OB_INVALID_ID)
        {
            /* skip index tables */
            continue;
        }
        //add:e
        else if ((ret = val_row.set_cell(table_id, column_id, value)) != OB_SUCCESS)
        {
            TRANS_LOG("Add value to ObRow failed");
            break;
        }
        else if (ret == OB_SUCCESS && (ret = values_op->add_values(val_row)) != OB_SUCCESS)
        {
            TRANS_LOG("Add value row failed");
            break;
        }
    }

    //  const ObTableSchema* it = sql_context_->schema_manager_->table_begin();
    //  for(; ret == OB_SUCCESS && it != sql_context_->schema_manager_->table_end(); it++)
    //  {
    //    ObRow val_row;
    //    int32_t len = static_cast<int32_t>(strlen(it->get_table_name()));
    //    ObString val(len, len, it->get_table_name());
    //    ObObj value;
    //    value.set_varchar(val);
    //    val_row.set_row_desc(row_desc);
    //    if (it->get_table_id() >= OB_TABLES_SHOW_TID
    //      && it->get_table_id() <= OB_SERVER_STATUS_SHOW_TID)
    //    {
    //      /* skip local show tables */
    //      continue;
    //    }
    //    else if ((ret = val_row.set_cell(table_id, column_id, value)) != OB_SUCCESS)
    //    {
    //      TRANS_LOG("Add value to ObRow failed");
    //      break;
    //    }
    //    else if (ret == OB_SUCCESS && (ret = values_op->add_values(val_row)) != OB_SUCCESS)
    //    {
    //      TRANS_LOG("Add value row failed");
    //      break;
    //    }
    //  }

        // modify by zhangcd [multi_database.show_tables] 20150615:e
    }
    if (ret == OB_SUCCESS)
    {
        out_op = values_op;
    }

    return ret;
}

// add by zhangcd [multi_database.show_tables] 20150617:b
int ObTransformer::gen_phy_show_system_tables(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObRowDesc row_desc;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    ObValues *values_op = NULL;
    CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat);
    if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
    {
        if (show_stmt->get_column_size() != 1)
        {
            TBSYS_LOG(WARN, "wrong columns' number of %s", OB_TABLES_SHOW_TABLE_NAME);
            ret = OB_ERR_COLUMN_SIZE;
            TRANS_LOG("wrong columns' number of %s", OB_TABLES_SHOW_TABLE_NAME);
        }
        else
        {
            const ColumnItem* column_item = show_stmt->get_column_item(0);
            table_id = column_item->table_id_;
            column_id = column_item->column_id_;
            if ((ret = row_desc.add_column_desc(table_id, column_id)) != OB_SUCCESS
                    || (ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
            {
                TRANS_LOG("add row desc error, err=%d", ret);
            }
        }

    // sys_tables show
    const ObTableSchema* it = sql_context_->schema_manager_->table_begin();
    for(; ret == OB_SUCCESS && it != sql_context_->schema_manager_->table_end(); it++)
    {
        ObRow val_row;
        int32_t len = static_cast<int32_t>(strlen(it->get_table_name()));
        ObString val(len, len, it->get_table_name());
        ObObj value;
        value.set_varchar(val);
        val_row.set_row_desc(row_desc);
        if(!IS_SYS_TABLE_ID(it->get_table_id()))
        {
            /* skip user tables */
            continue;
        }

            if (it->get_table_id() >= OB_TABLES_SHOW_TID
                    && it->get_table_id() <= OB_SERVER_STATUS_SHOW_TID)
            {
                /* skip local show tables */
                continue;
            }
            else if ((ret = val_row.set_cell(table_id, column_id, value)) != OB_SUCCESS)
            {
                TRANS_LOG("Add value to ObRow failed");
                break;
            }
            else if (ret == OB_SUCCESS && (ret = values_op->add_values(val_row)) != OB_SUCCESS)
            {
                TRANS_LOG("Add value row failed");
                break;
            }
        }
    }
    if (ret == OB_SUCCESS)
    {
        out_op = values_op;
    }

    return ret;
}
// add by zhangcd [multi_database.show_tables] 20150617:e
//add liumengzhan_show_index [20141208]
int ObTransformer::gen_phy_show_index(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObRowDesc row_desc;
    ObValues *values_op = NULL;
    IndexList idx_list;
    uint64_t show_tid = OB_INVALID_ID;
    uint64_t sys_tid = OB_INVALID_ID;
    uint64_t idx_tid = OB_INVALID_ID;
    int64_t idx_num = 0;
    CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat);
    if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
    {
        if (show_stmt->get_column_size() != 2)
        {
            ret = OB_ERR_COLUMN_SIZE;
            TRANS_LOG("wrong columns' number of %s", OB_INDEX_SHOW_TABLE_NAME);
        }
        else
        {
            for (int32_t i = 0; i< show_stmt->get_column_size(); i++)
            {
                const ColumnItem* column_item = show_stmt->get_column_item(i);
                if ((ret = row_desc.add_column_desc(column_item->table_id_, column_item->column_id_)) != OB_SUCCESS)
                {
                    TRANS_LOG("add row desc error, err=%d", ret);
                }
            }
            if ((ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
            {
                TRANS_LOG("add row desc error, err=%d", ret);
            }
        }

    show_tid = show_stmt->get_show_table_id();
    sys_tid = show_stmt->get_sys_table_id();
    if (show_tid == OB_INVALID_ID || (ret=sql_context_->schema_manager_->get_index_list(show_tid, idx_list)) != OB_SUCCESS)
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("get index table list error, err=%d", ret);
    }
    idx_num = idx_list.get_count();
    for (int64_t i = 0; ret == OB_SUCCESS && i < idx_num; i++)
    {
        ObRow val_row;
        val_row.set_row_desc(row_desc);
        //construct index table name_obj
        idx_list.get_idx_id(i, idx_tid);
        const ObTableSchema *idx_tschema = sql_context_->schema_manager_->get_table_schema(idx_tid);
        //add by zhangcd [multi_database.secondary_index] 20150703:b
        ObString db_name;
        ObString short_table_name;
        char *db_name_ptr = NULL;
        char *short_table_name_ptr = NULL;
        if(NULL == (db_name_ptr = (char *)mem_pool_->alloc(OB_MAX_DATBASE_NAME_LENGTH)))
        {
            ret = OB_ERROR;
            TRANS_LOG("Memory OverFlow!");
        }
        else
        {
            db_name.assign_buffer(db_name_ptr, OB_MAX_DATBASE_NAME_LENGTH);
        }
        if(NULL == (short_table_name_ptr = (char *)mem_pool_->alloc(OB_MAX_DATBASE_NAME_LENGTH)))
        {
            ret = OB_ERROR;
            TRANS_LOG("Memory OverFlow!");
        }
        else
        {
            short_table_name.assign_buffer(short_table_name_ptr, OB_MAX_TABLE_NAME_LENGTH);
        }
        ObString complete_table_name = ObString::make_string(idx_tschema->get_table_name());
        bool split_ret = complete_table_name.split_two(db_name, short_table_name, '.');
        if(!split_ret)
        {
            ret = OB_ERROR;
            TRANS_LOG("Generate physical show index failed!");
            break;
        }
        //add:e

        //modify by zhangcd [multi_database.secondary_index] 20150703:b
        ObString name = short_table_name;
        //int32_t len = static_cast<int32_t>(strlen(idx_tschema->get_table_name()));
        //ObString name(len, len, idx_tschema->get_table_name());
        //modify:e
        ObObj name_obj;
        name_obj.set_varchar(name);
        //construct index table status_obj
        IndexHelper idx_hp = idx_tschema->get_index_helper();
        ObObj status_obj;
        status_obj.set_int(idx_hp.status);

            uint64_t column_id = OB_APP_MIN_COLUMN_ID;
            if ((ret = val_row.set_cell(sys_tid, column_id++, name_obj)) != OB_SUCCESS)
            {
                TRANS_LOG("Add value to ObRow failed");
                break;
            }
            else if ((ret = val_row.set_cell(sys_tid, column_id++, status_obj)) != OB_SUCCESS)
            {
                TRANS_LOG("Add value to ObRow failed");
                break;
            }
            else if (ret == OB_SUCCESS && (ret = values_op->add_values(val_row)) != OB_SUCCESS)
            {
                TRANS_LOG("Add value row failed");
                break;
            }
        }
    }
    if (ret == OB_SUCCESS)
    {
        out_op = values_op;
    }

    return ret;
}
//add:e

int ObTransformer::gen_phy_show_columns(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObRowDesc row_desc;
    ObValues *values_op = NULL;
    CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat);

    int32_t num = show_stmt->get_column_size();
    if (OB_UNLIKELY(num < 1))
    {
        TBSYS_LOG(WARN, "wrong columns' number of %s", OB_COLUMNS_SHOW_TABLE_NAME);
        ret = OB_ERR_COLUMN_SIZE;
        TRANS_LOG("wrong columns' number of %s", OB_COLUMNS_SHOW_TABLE_NAME);
    }
    else
    {
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            const ColumnItem* column_item = show_stmt->get_column_item(i);
            if ((ret = row_desc.add_column_desc(column_item->table_id_, column_item->column_id_)) != OB_SUCCESS)
            {
                TRANS_LOG("add row desc error, err=%d", ret);
            }
        }
        if (ret == OB_SUCCESS && (ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
        {
            TRANS_LOG("set row desc error, err=%d", ret);
        }
    }

    if (ret == OB_SUCCESS)
    {
        const ObColumnSchemaV2* columns = NULL;
        int32_t column_size = 0;
        ObRowkeyColumn rowkey_column;
        const ObRowkeyInfo& rowkey_info = sql_context_->schema_manager_->get_table_schema(show_stmt->get_show_table_id())->get_rowkey_info();
        columns = sql_context_->schema_manager_->get_table_schema(show_stmt->get_show_table_id(), column_size);
        if (NULL != columns && column_size > 0)
        {
            for (int64_t i = 0; ret == OB_SUCCESS && i < column_size; i++)
            {
                uint64_t table_id = OB_INVALID_ID;
                uint64_t column_id = OB_INVALID_ID;
                ObRow val_row;
                val_row.set_row_desc(row_desc);

                // add name
                if ((ret = row_desc.get_tid_cid(0, table_id, column_id)) != OB_SUCCESS)
                {
                    TRANS_LOG("Get row desc failed");
                    break;
                }
                int32_t name_len = static_cast<int32_t>(strlen(columns[i].get_name()));
                ObString name_val(name_len, name_len, columns[i].get_name());
                ObObj name;
                name.set_varchar(name_val);
                if ((ret = val_row.set_cell(table_id, column_id, name)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add name to ObRow failed");
                    break;
                }

                // add type
                if ((ret = row_desc.get_tid_cid(1, table_id, column_id)) != OB_SUCCESS)
                {
                    TRANS_LOG("Get row desc failed");
                    break;
                }
                char type_str[OB_MAX_SYS_PARAM_NAME_LENGTH];
                int32_t type_len = OB_MAX_SYS_PARAM_NAME_LENGTH;
                switch (columns[i].get_type())
                {
                case ObNullType:
                    type_len = snprintf(type_str, type_len, "null");
                    break;
                case ObIntType:
                    type_len = snprintf(type_str, type_len, "int");
                    break;
                    //add lijianqiang [INT_32] 20151008:b
                case ObInt32Type:
                    type_len = snprintf(type_str, type_len, "int32");
                    break;
                    //add 20151008:e
                case ObFloatType:
                    type_len = snprintf(type_str, type_len, "float");
                    break;
                case ObDoubleType:
                    type_len = snprintf(type_str, type_len, "double");
                    break;
                case ObDateTimeType:
                    type_len = snprintf(type_str, type_len, "datetime");
                    break;
                case ObPreciseDateTimeType:
                    type_len = snprintf(type_str, type_len, "timestamp");
                    break;
                    //add peiouya [DATE_TIME] 20150906:b
                case ObDateType:
                    type_len = snprintf(type_str, type_len, "date");
                    break;
                case ObTimeType:
                    type_len = snprintf(type_str, type_len, "time");
                    break;
                    //add 20150906:e
                case ObVarcharType:
                    type_len = snprintf(type_str, type_len, "varchar(%ld)", columns[i].get_size());
                    break;
                case ObSeqType:
                    type_len = snprintf(type_str, type_len, "seq");
                    break;
                case ObCreateTimeType:
                    type_len = snprintf(type_str, type_len, "createtime");
                    break;
                case ObModifyTimeType:
                    type_len = snprintf(type_str, type_len, "modifytime");
                    break;
                case ObExtendType:
                    type_len = snprintf(type_str, type_len, "extend");
                    break;
                case ObBoolType:
                    type_len = snprintf(type_str, type_len, "bool");
                    break;
                case ObDecimalType:
                    //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
                    //type_len = snprintf(type_str, type_len, "decimal");      //old code
                    type_len = snprintf(type_str, type_len, "decimal(%d,%d)",columns[i].get_precision(), columns[i].get_scale());
                    //modify:e
                    break;
                default:
                    type_len = snprintf(type_str, type_len, "unknown");
                    break;
                }
                ObString type_val(type_len, type_len, type_str);
                ObObj type;
                type.set_varchar(type_val);
                if ((ret = val_row.set_cell(table_id, column_id, type)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add type to ObRow failed");
                    break;
                }

                // add nullable
                if ((ret = row_desc.get_tid_cid(2, table_id, column_id)) != OB_SUCCESS)
                {
                    TRANS_LOG("Get row desc failed");
                    break;
                }
                //del peiouya 20131222
                //ObString nullable_val;
                ObObj nullable;
                int64_t null_value = 0;
                //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
                if (!columns[i].is_nullable())
                {
                    nullable.set_int(null_value);
                } else
                {
                    nullable.set_int(null_value + 1);
                }
                //add 20131222:e
                if ((ret = val_row.set_cell(table_id, column_id, nullable)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add nullable to ObRow failed");
                    break;
                }

                // add key_id
                if ((ret = row_desc.get_tid_cid(3, table_id, column_id) != OB_SUCCESS))
                {
                    TRANS_LOG("Get row desc failed");
                    break;
                }
                int64_t index = -1;
                rowkey_info.get_index(columns[i].get_id(), index, rowkey_column);
                ObObj key_id;
                key_id.set_int(index + 1); /* rowkey id is rowkey index plus 1 */
                if ((ret = val_row.set_cell(table_id, column_id, key_id)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add key_id to ObRow failed");
                    break;
                }

                // add default
                if ((ret = row_desc.get_tid_cid(4, table_id, column_id) != OB_SUCCESS))
                {
                    TRANS_LOG("Get row desc failed");
                    break;
                }
                ObObj def;
                def.set_null();
                if ((ret = val_row.set_cell(table_id, column_id, def)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add default to ObRow failed");
                    break;
                }

                // add extra
                if ((ret = row_desc.get_tid_cid(5, table_id, column_id) != OB_SUCCESS))
                {
                    TRANS_LOG("Get row desc failed");
                    break;
                }
                ObString extra_val;
                ObObj extra;
                extra.set_varchar(extra_val);
                if ((ret = val_row.set_cell(table_id, column_id, extra)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add extra to ObRow failed");
                    break;
                }

                if (ret == OB_SUCCESS && (ret = values_op->add_values(val_row)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add value row failed");
                    break;
                }
            }
        }
    }

    if (ret == OB_SUCCESS)
    {
        out_op = values_op;
    }

    return ret;
}

int ObTransformer::gen_phy_show_variables(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    if (!show_stmt->is_global_scope())
    {
        ObRowDesc row_desc;
        ObValues *values_op = NULL;
        CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat);
        for (int32_t i = 0; ret == OB_SUCCESS && i < show_stmt->get_column_size(); i++)
        {
            const ColumnItem* column_item = show_stmt->get_column_item(i);
            if ((ret = row_desc.add_column_desc(column_item->table_id_,
                                                column_item->column_id_)) != OB_SUCCESS)
            {
                TRANS_LOG("Add row desc error, err=%d", ret);
            }
        }
        if (ret == OB_SUCCESS && (ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
        {
            TRANS_LOG("Set row desc error, err=%d", ret);
        }
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ObSQLSessionInfo::SysVarNameValMap::const_iterator it_begin;
        ObSQLSessionInfo::SysVarNameValMap::const_iterator it_end;
        it_begin = sql_context_->session_info_->get_sys_var_val_map().begin();
        it_end = sql_context_->session_info_->get_sys_var_val_map().end();
        for(; ret == OB_SUCCESS && it_begin != it_end; it_begin++)
        {
            ObRow val_row;
            val_row.set_row_desc(row_desc);
            ObObj var_name;
            var_name.set_varchar(it_begin->first);
            // add Variable_name
            if ((ret = row_desc.get_tid_cid(0, table_id, column_id)) != OB_SUCCESS)
            {
                TRANS_LOG("Get row desc failed");
            }
            else if ((ret = val_row.set_cell(table_id, column_id, var_name)) != OB_SUCCESS)
            {
                TRANS_LOG("Add variable name to ObRow failed");
            }
            // add Value
            else if ((ret = row_desc.get_tid_cid(1, table_id, column_id)) != OB_SUCCESS)
            {
                TRANS_LOG("Get row desc failed");
            }
            else if ((ret = val_row.set_cell(table_id, column_id, *((it_begin->second).first))) != OB_SUCCESS)
            {
                TRANS_LOG("Add value to ObRow failed");
            }
            else if ((ret = values_op->add_values(val_row)) != OB_SUCCESS)
            {
                TRANS_LOG("Add value row failed");
            }
        }
        if (ret == OB_SUCCESS)
        {
            out_op = values_op;
        }
    }
    else
    {
        ObProject *project_op = NULL;
        ObTableRpcScan *rpc_scan_op = NULL;
        ObRpcScanHint hint;
        hint.read_method_ = ObSqlReadStrategy::USE_SCAN;
        if (CREATE_PHY_OPERRATOR(project_op, ObProject, physical_plan, err_stat) == NULL)
        {
            ret = OB_ERR_PARSER_MALLOC_FAILED;
            TRANS_LOG("Generate Project operator failed");
        }
        else if (CREATE_PHY_OPERRATOR(rpc_scan_op, ObTableRpcScan, physical_plan, err_stat) == NULL)
        {
            ret = OB_ERR_PARSER_MALLOC_FAILED;
            TRANS_LOG("Generate TableScan operator failed");
        }
        if ((ret = rpc_scan_op->set_table(OB_ALL_SYS_PARAM_TID, OB_ALL_SYS_PARAM_TID)) != OB_SUCCESS)
        {
            TRANS_LOG("ObTableRpcScan set table faild");
        }
        else if ((ret = rpc_scan_op->init(sql_context_, &hint)) != OB_SUCCESS)
        {
            TRANS_LOG("ObTableRpcScan init faild");
        }
        else if ((ret = project_op->set_child(0, *rpc_scan_op)) != OB_SUCCESS)
        {
            TRANS_LOG("Set child of Project operator faild");
        }
        else if ((ret = physical_plan->add_base_table_version(OB_ALL_SYS_PARAM_TID, 0)) != OB_SUCCESS)
        {
            TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", OB_ALL_SYS_PARAM_TID, ret);
        }
        else
        {
            const ObSchemaManagerV2 *schema = sql_context_->schema_manager_;
            const ObColumnSchemaV2* none_concern_keys[1];
            const ObColumnSchemaV2 *name_column = NULL;
            const ObColumnSchemaV2 *value_column = NULL;
            if ((none_concern_keys[0] = schema->get_column_schema(OB_ALL_SYS_PARAM_TABLE_NAME,
                                                                  "cluster_id")) == NULL
                    || (name_column = schema->get_column_schema(OB_ALL_SYS_PARAM_TABLE_NAME,
                                                                "name")) == NULL
                    || (value_column = schema->get_column_schema(OB_ALL_SYS_PARAM_TABLE_NAME,
                                                                 "value")) == NULL)
            {
                ret = OB_ERR_COLUMN_UNKNOWN;
                TRANS_LOG("Get column of %s faild, ret = %d", OB_ALL_SYS_PARAM_TABLE_NAME, ret);
            }
            for (int32_t i = 0; ret == OB_SUCCESS && i < 1; i++)
            {
                ObObj val;
                val.set_int(0);
                ObConstRawExpr value(val, T_INT);
                ObBinaryRefRawExpr col(OB_ALL_SYS_PARAM_TID, none_concern_keys[i]->get_id(), T_REF_COLUMN);
                ObBinaryOpRawExpr equal_op(&col, &value, T_OP_EQ);
                ObSqlRawExpr col_expr(OB_INVALID_ID,
                                      OB_ALL_SYS_PARAM_TID,
                                      none_concern_keys[i]->get_id(),
                                      &col);
                ObSqlRawExpr equal_expr(OB_INVALID_ID,
                                        OB_ALL_SYS_PARAM_TID,
                                        none_concern_keys[i]->get_id(),
                                        &equal_op);
                ObSqlExpression output_col;
                ObSqlExpression *filter = ObSqlExpression::alloc();
                if (NULL == filter)
                {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    TRANS_LOG("no memory");
                }
                else if ((ret = col_expr.fill_sql_expression(output_col)) != OB_SUCCESS)
                {
                    ObSqlExpression::free(filter);
                    TRANS_LOG("Generate output column of TableScan faild, ret = %d", ret);
                }
                else if ((ret = rpc_scan_op->add_output_column(output_col)) != OB_SUCCESS)
                {
                    ObSqlExpression::free(filter);
                    TRANS_LOG("Add output column to TableScan faild, ret = %d", ret);
                }
                else if ((ret = equal_expr.fill_sql_expression(*filter)) != OB_SUCCESS)
                {
                    ObSqlExpression::free(filter);
                    TRANS_LOG("Generate filter faild, ret = %d", ret);
                }
                else if ((ret = rpc_scan_op->add_filter(filter)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add filter to TableScan faild, ret = %d", ret);
                }
            }
            if (ret == OB_SUCCESS)
            {
                ObBinaryRefRawExpr col(OB_ALL_SYS_PARAM_TID, name_column->get_id(), T_REF_COLUMN);
                ObSqlRawExpr expr(OB_INVALID_ID, OB_ALL_SYS_PARAM_TID, name_column->get_id(), &col);
                ObSqlExpression output_expr;
                const ColumnItem* column_item = NULL;
                if ((ret = expr.fill_sql_expression(output_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Generate output column faild, ret = %d", ret);
                }
                else if ((ret = rpc_scan_op->add_output_column(output_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add output column to TableScan faild, ret = %d", ret);
                }
                else if ((column_item = show_stmt->get_column_item(0)) == NULL)
                {
                    TRANS_LOG("Can not get column item of 'name'");
                }
                else
                {
                    output_expr.set_tid_cid(column_item->table_id_, column_item->column_id_);
                    if ((ret = project_op->add_output_column(output_expr)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add output column to Project faild, ret = %d", ret);
                    }
                }
            }
            if (ret == OB_SUCCESS)
            {
                ObBinaryRefRawExpr col(OB_ALL_SYS_PARAM_TID, value_column->get_id(), T_REF_COLUMN);
                ObSqlRawExpr expr(OB_INVALID_ID, OB_ALL_SYS_PARAM_TID, value_column->get_id(), &col);
                ObSqlExpression output_expr;
                const ColumnItem* column_item = NULL;
                if ((ret = expr.fill_sql_expression(output_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Generate output column faild, ret = %d", ret);
                }
                else if ((ret = rpc_scan_op->add_output_column(output_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add output column to TableScan faild, ret = %d", ret);
                }
                else if ((column_item = show_stmt->get_column_item(1)) == NULL)
                {
                    TRANS_LOG("Can not get column item of 'value'");
                }
                else
                {
                    output_expr.set_tid_cid(column_item->table_id_, column_item->column_id_);
                    if ((ret = project_op->add_output_column(output_expr)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add output column to Project faild, ret = %d", ret);
                    }
                }
            }
        }
        if (ret == OB_SUCCESS)
        {
            out_op = project_op;
        }
    }
    return ret;
}

int ObTransformer::gen_phy_show_warnings(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObRowDesc row_desc;
    ObValues *values_op = NULL;
    CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat);
    if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
    {
        if (sql_context_->session_info_ == NULL)
        {
            ret = OB_ERR_GEN_PLAN;
            TRANS_LOG("can not get current session info, err=%d", ret);
        }
        else
        {
            const tbsys::WarningBuffer& warnings_buf = sql_context_->session_info_->get_warnings_buffer();
            if (show_stmt->is_count_warnings())
            {
                /* show COUNT(*) warnings */
                if ((ret = row_desc.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID)) != OB_SUCCESS)
                {
                    TRANS_LOG("add row desc error, err=%d", ret);
                }
                else if (ret == OB_SUCCESS && (ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
                {
                    TRANS_LOG("set row desc error, err=%d", ret);
                }
                else
                {
                    ObRow val_row;
                    val_row.set_row_desc(row_desc);
                    ObObj num;
                    num.set_int(warnings_buf.get_readable_warning_count());
                    if ((ret = val_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID, num)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add 'code' to ObRow failed");
                    }
                    else if ((ret = values_op->add_values(val_row)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add value row failed");
                    }
                }
            }
            else
            {
                /* show warnings [limit] */
                // add descriptor
                for (int32_t i = 0; ret == OB_SUCCESS && i < 3; i++)
                {
                    if ((ret = row_desc.add_column_desc(OB_INVALID_ID, i + OB_APP_MIN_COLUMN_ID)) != OB_SUCCESS)
                    {
                        TRANS_LOG("add row desc error, err=%d", ret);
                        break;
                    }
                }
                if (ret == OB_SUCCESS && (ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
                {
                    TRANS_LOG("set row desc error, err=%d", ret);
                }
                // add values
                else
                {
                    uint32_t j = 0;
                    int64_t k = 0;
                    for (; ret == OB_SUCCESS && j < warnings_buf.get_readable_warning_count()
                         && (k < show_stmt->get_warnings_count() || show_stmt->get_warnings_count() < 0);
                         j++, k++)
                    {
                        ObRow val_row;
                        val_row.set_row_desc(row_desc);
                        // can not get level, get it from string
                        const char* warning_ptr = warnings_buf.get_warning(j);
                        if (warning_ptr == NULL)
                            continue;
                        const char* separator = strchr(warning_ptr, ' ');
                        if (separator == NULL)
                        {
                            TBSYS_LOG(WARN, "Wrong message in warnings buffer: %s", warning_ptr);
                            continue;
                        }
                        ObObj level;
                        level.set_varchar(ObString::make_string("Warning"));
                        if ((ret = val_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID, level)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add 'level' to ObRow failed");
                            break;
                        }
                        // code, can not get it
                        ObObj code;
                        code.set_int(99999);
                        if ((ret = val_row.set_cell(OB_INVALID_ID, 1 + OB_APP_MIN_COLUMN_ID, code)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add 'code' to ObRow failed");
                            break;
                        }
                        // message
                        // pls see the warning format
                        int32_t msg_len = static_cast<int32_t>(strlen(warning_ptr));
                        ObString msg_str(msg_len, msg_len, warning_ptr);
                        ObObj message;
                        message.set_varchar(msg_str);
                        if ((ret = val_row.set_cell(OB_INVALID_ID, 2 + OB_APP_MIN_COLUMN_ID, message)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add 'message' to ObRow failed");
                            break;
                        }
                        else if ((ret = values_op->add_values(val_row)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Add value row failed");
                            break;
                        }
                    }
                }
            }
        }
    }
    if (ret == OB_SUCCESS)
    {
        out_op = values_op;
    }
    return ret;
}

int ObTransformer::gen_phy_show_grants(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObRowDesc row_desc;
    ObValues *values_op = NULL;
    CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat);
    if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
    {
        if ((ret = row_desc.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID)) != OB_SUCCESS)
        {
            TRANS_LOG("add row desc error, err=%d", ret);
        }
        else if ((ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
        {
            TRANS_LOG("set row desc error, err=%d", ret);
        }
        else
        {
            out_op = values_op;
            const ObPrivilege **pp_privilege  = sql_context_->pp_privilege_;
            ObString user_name = show_stmt->get_user_name();
            int64_t pos = 0;
            char buf[512];
            if (show_stmt->get_user_name().length() == 0)
            {
                user_name = sql_context_->session_info_->get_user_name();
            }
            const common::ObSchemaManagerV2 *schema_manager = sql_context_->schema_manager_;
            const ObTableSchema* table_schema = NULL;
            ObPrivilege::NameUserMap *username_map = (const_cast<ObPrivilege*>(*pp_privilege))->get_username_map();
            //add liumz, [multi_database.priv_management.show_grants]:20150610:b
            ObPrivilege::UserDbPriMap *user_db_map = (const_cast<ObPrivilege*>(*pp_privilege))->get_user_database_privilege_map();
            //add:e
            ObPrivilege::UserPrivMap *user_table_map = (const_cast<ObPrivilege*>(*pp_privilege))->get_user_table_map();
            ObPrivilege::User user;
            ret = username_map->get(user_name, user);
            if (-1 == ret || hash::HASH_NOT_EXIST == ret)
            {
                ret = OB_ERR_USER_NOT_EXIST;
                TRANS_LOG("user[%.*s] not exists", user_name.length(), user_name.ptr());
                TBSYS_LOG(WARN, "username:%.*s 's not exist, ret=%d", user_name.length(), user_name.ptr(), ret);
                //ret = OB_ERR_USER_NOT_EXIST;
            }
            else
            {
                ret = OB_SUCCESS;
                const ObBitSet<> &privileges = user.privileges_;
                if (privileges.is_empty())
                {
                }
                else
                {
                    //del liumz, [multi_database.priv_management.show_grants]:20150610:b
                    /*databuff_printf(buf, 512, pos, "GRANT ");
        if (privileges.has_member(OB_PRIV_ALL))
        {
          databuff_printf(buf, 512, pos, "ALL PRIVILEGES ");
          if (privileges.has_member(OB_PRIV_GRANT_OPTION))
          {
            databuff_printf(buf, 512, pos, ",GRANT OPTION ON * TO '%.*s'", user_name.length(), user_name.ptr());
          }
          else
          {
            databuff_printf(buf, 512, pos, "ON * TO '%.*s'", user_name.length(), user_name.ptr());
          }
        }
        else
        {
          ObPrivilege::privilege_to_string(privileges, buf, 512, pos);
          pos = pos - 1;
          databuff_printf(buf, 512, pos, " ON * TO '%.*s'", user_name.length(), user_name.ptr());
        }*/
                    //del:e
                    //add liumz, [multi_database.priv_management.show_grants]:20150610:b
                    ObPrivilege::privilege_to_string_v2(privileges, buf, 512, pos);
                    //add:e
                    pos = pos - 1;
                    databuff_printf(buf, 512, pos, " ON * TO '%.*s'", user_name.length(), user_name.ptr());
                    //add:e
                    ObRow val_row;
                    val_row.set_row_desc(row_desc);
                    ObString grant_str;
                    if (pos >= 511)
                    {
                        // overflow
                        ret = OB_BUF_NOT_ENOUGH;
                        TBSYS_LOG(WARN, "privilege buffer not enough, ret=%d", ret);
                    }
                    else
                    {
                        grant_str.assign_ptr(buf, static_cast<int32_t>(pos));
                        ObObj grant_val;
                        grant_val.set_varchar(grant_str);
                        if (OB_SUCCESS != (ret = val_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID, grant_val)))
                        {
                            TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
                        }
                        else if (OB_SUCCESS != (ret = values_op->add_values(val_row)))
                        {
                            TRANS_LOG("add value row failed");
                        }
                    }
                }
            }
            //add liumz, [multi_database.priv_management.show_grants]:20150610:b
            if (OB_SUCCESS == ret)
            {
                uint64_t user_id = user.user_id_;
                ObPrivilege::UserDbPriMap::iterator iter = user_db_map->begin();
                for (;iter != user_db_map->end();++iter)
                {
                    pos = 0;
                    //databuff_printf(buf, 512, pos, "GRANT ");
                    const ObPrivilege::UserIdDatabaseId &user_id_db_id = iter->first;
                    if (user_id_db_id.userid_ == user_id)
                    {
                        const ObBitSet<> &privileges = (iter->second).privileges_;
                        if (privileges.is_empty())
                        {
                            continue;
                        }
                        else
                        {
                            //ObPrivilege::privilege_to_string(privileges, buf, 512, pos);
                            ObString db_name;
                            if (OB_SUCCESS != (ret = (*pp_privilege)->get_db_name(user_id_db_id.dbid_, db_name)))
                            {
                                TBSYS_LOG(WARN, "can not get db name, db_id=%lu", user_id_db_id.dbid_);
                            }
                            else
                            {
                                ObPrivilege::privilege_to_string_v2(privileges, buf, 512, pos);
                                pos = pos - 1;
                                databuff_printf(buf, 512, pos, " ON %.*s.* TO '%.*s'", db_name.length(), db_name.ptr(), user_name.length(), user_name.ptr());
                                ObRow val_row;
                                val_row.set_row_desc(row_desc);
                                ObString grant_str;
                                if (pos >= 511)
                                {
                                    // overflow
                                    ret = OB_BUF_NOT_ENOUGH;
                                    TBSYS_LOG(WARN, "privilege buffer not enough, ret=%d", ret);
                                }
                                else
                                {
                                    grant_str.assign_ptr(buf, static_cast<int32_t>(pos));
                                    ObObj grant_val;
                                    grant_val.set_varchar(grant_str);
                                    if (OB_SUCCESS != (ret = val_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID, grant_val)))
                                    {
                                        TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
                                    }
                                    else if (OB_SUCCESS != (ret = values_op->add_values(val_row)))
                                    {
                                        TRANS_LOG("add value row failed");
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        continue;
                    }
                }
            }
            //add:e
            if (OB_SUCCESS == ret)
            {
                uint64_t user_id = user.user_id_;
                ObPrivilege::UserPrivMap::iterator iter = user_table_map->begin();
                for (;iter != user_table_map->end();++iter)
                {
                    pos = 0;
                    //del liumz,
                    //databuff_printf(buf, 512, pos, "GRANT ");
                    const ObPrivilege::UserIdTableId &user_id_table_id = iter->first;
                    if (user_id_table_id.user_id_ == user_id)
                    {
                        const ObBitSet<> &privileges = (iter->second).table_privilege_.privileges_;
                        if (privileges.is_empty())
                        {
                            continue;
                        }
                        else
                        {
                            //del liumz,
                            //ObPrivilege::privilege_to_string(privileges, buf, 512, pos);
                            table_schema = schema_manager->get_table_schema(user_id_table_id.table_id_);
                            if (NULL == table_schema)
                            {
                                TBSYS_LOG(WARN, "table id=%lu not exist in schema manager", user_id_table_id.table_id_);
                            }
                            else
                            {
                                const char *table_name = table_schema->get_table_name();
                                //add liumz, [multi_database.priv_management.show_grants]:20150610:b
                                ObPrivilege::privilege_to_string_v2(privileges, buf, 512, pos);
                                //add:e
                                pos = pos - 1;
                                databuff_printf(buf, 512, pos, " ON %s TO '%.*s'", table_name, user_name.length(), user_name.ptr());
                                ObRow val_row;
                                val_row.set_row_desc(row_desc);
                                ObString grant_str;
                                if (pos >= 511)
                                {
                                    // overflow
                                    ret = OB_BUF_NOT_ENOUGH;
                                    TBSYS_LOG(WARN, "privilege buffer not enough, ret=%d", ret);
                                }
                                else
                                {
                                    grant_str.assign_ptr(buf, static_cast<int32_t>(pos));
                                    ObObj grant_val;
                                    grant_val.set_varchar(grant_str);
                                    if (OB_SUCCESS != (ret = val_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID, grant_val)))
                                    {
                                        TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
                                    }
                                    else if (OB_SUCCESS != (ret = values_op->add_values(val_row)))
                                    {
                                        TRANS_LOG("add value row failed");
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        continue;
                    }
                }
            }
        }
    }
    return ret;
}

int ObTransformer::gen_phy_show_table_status(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    UNUSED(show_stmt);
    ObValues *values_op = NULL;
    if (NULL == CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
        // @todo empty
        out_op = values_op;
    }
    return ret;
}

int ObTransformer::gen_phy_show_processlist(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    UNUSED(show_stmt);
    ObTableScan *table_scan_op = NULL;
    if (ret == OB_SUCCESS)
    {
        ObTableRpcScan *table_rpc_scan_op = NULL;
        ObRpcScanHint hint;
        hint.read_method_ = ObSqlReadStrategy::USE_SCAN;
        CREATE_PHY_OPERRATOR(table_rpc_scan_op, ObTableRpcScan, physical_plan, err_stat);
        if (ret == OB_SUCCESS
                && (ret = table_rpc_scan_op->set_table(OB_ALL_SERVER_SESSION_TID, OB_ALL_SERVER_SESSION_TID)) != OB_SUCCESS)
        {
            TRANS_LOG("ObTableRpcScan set table faild");
        }
        if (ret == OB_SUCCESS && (ret = table_rpc_scan_op->init(sql_context_, &hint)) != OB_SUCCESS)
        {
            TRANS_LOG("ObTableRpcScan init faild");
        }
        if (ret == OB_SUCCESS && (ret = physical_plan->add_base_table_version(OB_ALL_SERVER_SESSION_TID, 0)) != OB_SUCCESS)
        {
            TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", OB_ALL_SERVER_SESSION_TID, ret);
        }
        if (ret == OB_SUCCESS)
        {
            table_scan_op = table_rpc_scan_op;
        }
    }

    // add output columns
    int32_t num = 10; //column num of show processlist
    for (int32_t i = 1; ret == OB_SUCCESS && i <= num; i++)
    {
        ObBinaryRefRawExpr col_expr(OB_ALL_SERVER_SESSION_TID, OB_APP_MIN_COLUMN_ID + i, T_REF_COLUMN);
        ObSqlRawExpr col_raw_expr(
                    common::OB_INVALID_ID,
                    OB_ALL_SERVER_SESSION_TID,
                    OB_APP_MIN_COLUMN_ID + i,
                    &col_expr);
        ObSqlExpression output_expr;
        if ((ret = col_raw_expr.fill_sql_expression(
                 output_expr,
                 this,
                 logical_plan,
                 physical_plan)) != OB_SUCCESS
                || (ret = table_scan_op->add_output_column(output_expr)) != OB_SUCCESS)
        {
            TRANS_LOG("Add table output columns faild");
            break;
        }
    }

    if (ret == OB_SUCCESS)
    {
        out_op = table_scan_op;
    }
    return ret;
}

int ObTransformer::gen_physical_show(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObShowStmt *show_stmt = NULL;
    ObPhyOperator *result_op = NULL;

    /* get statement */
    if (ret == OB_SUCCESS)
    {
        get_stmt(logical_plan, err_stat, query_id, show_stmt);
    }

    if (ret == OB_SUCCESS)
    {
        switch (show_stmt->get_stmt_type())
        {
        case ObBasicStmt::T_SHOW_TABLES:
            ret = gen_phy_show_tables(physical_plan, err_stat, show_stmt, result_op);
            break;
            // add by zhangcd [multi_database.show_tables] 20150616:b
        case ObBasicStmt::T_SHOW_SYSTEM_TABLES:
            ret = gen_phy_show_system_tables(physical_plan, err_stat, show_stmt, result_op);
            break;
            // add by zhangcd [multi_database.show_tables] 20150616:e
            // add by zhangcd [multi_database.show_databases] 20150617:b
        case ObBasicStmt::T_SHOW_DATABASES:
            ret = gen_phy_show_databases(physical_plan, err_stat, show_stmt, result_op);
            break;
            // add by zhangcd [multi_database.show_databases] 20150617:e
            // add by zhangcd [multi_database.show_databases] 20150617:b
        case ObBasicStmt::T_SHOW_CURRENT_DATABASE:
            ret = gen_phy_show_current_database(physical_plan, err_stat, show_stmt, result_op);
            break;
            // add by zhangcd [multi_database.show_databases] 20150617:e
            //add liumengzhan_show_index [20141208]
        case ObBasicStmt::T_SHOW_INDEX:
            ret = gen_phy_show_index(physical_plan, err_stat, show_stmt, result_op);
            break;
            //add:e
        case ObBasicStmt::T_SHOW_COLUMNS:
            ret = gen_phy_show_columns(physical_plan, err_stat, show_stmt, result_op);
            break;
        case ObBasicStmt::T_SHOW_VARIABLES:
            ret = gen_phy_show_variables(physical_plan, err_stat, show_stmt, result_op);
            break;
        case ObBasicStmt::T_SHOW_TABLE_STATUS:
            ret = gen_phy_show_table_status(physical_plan, err_stat, show_stmt, result_op);
            break;
        case ObBasicStmt::T_SHOW_SCHEMA:
        case ObBasicStmt::T_SHOW_SERVER_STATUS:
            TRANS_LOG("This statment not support now!");
            break;
        case ObBasicStmt::T_SHOW_CREATE_TABLE:
            ret = gen_phy_show_create_table(physical_plan, err_stat, show_stmt, result_op);
            break;
        case ObBasicStmt::T_SHOW_WARNINGS:
            ret = gen_phy_show_warnings(physical_plan, err_stat, show_stmt, result_op);
            break;
        case ObBasicStmt::T_SHOW_GRANTS:
            ret = gen_phy_show_grants(physical_plan, err_stat, show_stmt, result_op);
            break;
        case ObBasicStmt::T_SHOW_PARAMETERS:
            ret = gen_phy_show_parameters(logical_plan, physical_plan, err_stat, show_stmt, result_op);
            break;
        case ObBasicStmt::T_SHOW_PROCESSLIST:
            ret = gen_phy_show_processlist(logical_plan, physical_plan, err_stat, show_stmt, result_op);
            break;
        default:
            ret = OB_ERR_GEN_PLAN;
            TRANS_LOG("Unknown show statment!");
            break;
        }
    }
    if (ret == OB_SUCCESS)
    {
        ObFilter *filter_op = NULL;
        if (show_stmt->get_like_pattern().length() > 0)
        {
            ObObj pattern_val;
            pattern_val.set_varchar(show_stmt->get_like_pattern());
            ObConstRawExpr pattern_expr(pattern_val, T_STRING);
            pattern_expr.set_result_type(ObVarcharType);
            ObBinaryRefRawExpr col_expr(show_stmt->get_sys_table_id(), OB_INVALID_ID, T_REF_COLUMN);
            const ObColumnSchemaV2* name_col = NULL;
            const ObColumnSchemaV2* columns = NULL;
            int32_t column_size = 0;
            ObSchemaChecker schema_checker;
            schema_checker.set_schema(*sql_context_->schema_manager_);
            if (show_stmt->get_stmt_type() == ObBasicStmt::T_SHOW_PARAMETERS)
            {
                if ((name_col = schema_checker.get_column_schema(
                         show_stmt->get_table_item(0).table_name_,
                         ObString::make_string("name")
                         )) == NULL)
                {
                }
                else
                {
                    col_expr.set_second_ref_id(name_col->get_id());
                    col_expr.set_result_type(name_col->get_type());
                }
            }
            else
            {
                if ((columns = schema_checker.get_table_columns(
                         show_stmt->get_sys_table_id(),
                         column_size)) == NULL
                        || column_size <= 0)
                {
                    ret = OB_ERR_GEN_PLAN;
                    TRANS_LOG("Get show table schema error!");
                }
                else
                {
                    col_expr.set_second_ref_id(columns[0].get_id());
                    col_expr.set_result_type(columns[0].get_type());
                }
            }
            if (ret == OB_SUCCESS)
            {
                ObBinaryOpRawExpr like_op_expr(&col_expr, &pattern_expr, T_OP_LIKE);
                like_op_expr.set_result_type(ObBoolType);
                ObSqlRawExpr raw_like_expr(OB_INVALID_ID, col_expr.get_first_ref_id(),
                                           col_expr.get_second_ref_id(), &like_op_expr);
                ObSqlExpression *like_expr = ObSqlExpression::alloc();
                if (NULL == like_expr
                        || (ret = raw_like_expr.fill_sql_expression(*like_expr, this, logical_plan, physical_plan)) != OB_SUCCESS)
                {
                    TRANS_LOG("Gen like filter failed!");
                }
                else if (CREATE_PHY_OPERRATOR(filter_op, ObFilter, physical_plan, err_stat) == NULL)
                {
                    ObSqlExpression::free(like_expr);
                }
                else if ((ret = filter_op->set_child(0, *result_op)) != OB_SUCCESS)
                {
                    ObSqlExpression::free(like_expr);
                    TRANS_LOG("Add child of filter plan failed");
                }
                else if ((ret = filter_op->add_filter(like_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add filter expression failed");
                }
            }
        }
        else if (show_stmt->get_condition_size() > 0)
        {
            CREATE_PHY_OPERRATOR(filter_op, ObFilter, physical_plan, err_stat);
            for (int32_t i = 0; ret == OB_SUCCESS && i < show_stmt->get_condition_size(); i++)
            {
                ObSqlRawExpr *cnd_expr = logical_plan->get_expr(show_stmt->get_condition_id(i));
                if (cnd_expr->is_apply() == true)
                {
                    continue;
                }
                else
                {
                    ObSqlExpression *filter = ObSqlExpression::alloc();
                    if (NULL == filter)
                    {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        TBSYS_LOG(ERROR, "no memory");
                        break;
                    }
                    else if ((ret = cnd_expr->fill_sql_expression(*filter, this, logical_plan, physical_plan)) != OB_SUCCESS
                             || (ret = filter_op->add_filter(filter)) != OB_SUCCESS)
                    {
                        ObSqlExpression::free(filter);
                        TRANS_LOG("Add table filter condition faild");
                        break;
                    }
                }
            } // end for
            if (ret == OB_SUCCESS && (ret = filter_op->set_child(0, *result_op))!= OB_SUCCESS)
            {
                TRANS_LOG("Add child of filter plan failed");
            }
        }
        if (ret == OB_SUCCESS && filter_op != NULL)
        {
            result_op = filter_op;
        }
    }
    if (ret == OB_SUCCESS)
    {
        ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, show_stmt, result_op, index);
    }

    return ret;
}

int ObTransformer::gen_physical_prepare(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObPrepare *result_op = NULL;
    ObPrepareStmt *stmt = NULL;
    /* get prepare statement */
    get_stmt(logical_plan, err_stat, query_id, stmt);
    /* add prepare operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(result_op, ObPrepare, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, stmt, result_op, index);
        }
    }

    if (ret == OB_SUCCESS)
    {
        ObPhyOperator* op = NULL;
        ObString stmt_name;
        int32_t idx = OB_INVALID_INDEX;
        if ((ret = ob_write_string(*mem_pool_, stmt->get_stmt_name(), stmt_name)) != OB_SUCCESS)
        {
            TRANS_LOG("Add prepare plan for stmt %.*s faild",
                      stmt->get_stmt_name().length(), stmt->get_stmt_name().ptr());
        }
        else
        {
            result_op->set_stmt_name(stmt_name);

      if ((ret = generate_physical_plan(
                          logical_plan,
                          physical_plan,
                          err_stat,
                          stmt->get_prepare_query_id(),
                          &idx)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Create physical plan for query statement failed, err=%d", ret);
      }
      else if ((op = physical_plan->get_phy_query(idx)) == NULL
        || (ret = result_op->set_child(0, *op)) != OB_SUCCESS)
      {
        ret = OB_ERR_ILLEGAL_INDEX;
        TRANS_LOG("Set child of Prepare Operator failed");
      }
    }
  }

    return ret;
}

int ObTransformer::gen_physical_variable_set(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObVariableSet *result_op = NULL;
    ObVariableSetStmt *stmt = NULL;
    /* get variable set statement */
    get_stmt(logical_plan, err_stat, query_id, stmt);
    /* generate operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(result_op, ObVariableSet, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, stmt, result_op, index);
        }
    }
    if (ret == OB_SUCCESS)
    {
        const ObTableSchema *table_schema = NULL;
        const ObColumnSchemaV2* name_column = NULL;
        const ObColumnSchemaV2* type_column = NULL;
        const ObColumnSchemaV2* value_column = NULL;
        if ((table_schema = sql_context_->schema_manager_->get_table_schema(
                 OB_ALL_SYS_PARAM_TID)) == NULL)
        {
            ret = OB_ERR_TABLE_UNKNOWN;
            TRANS_LOG("Fail to get table schema for table[%ld]", OB_ALL_SYS_PARAM_TID);
        }
        else if ((name_column = sql_context_->schema_manager_->get_column_schema(
                      OB_ALL_SYS_PARAM_TABLE_NAME, "name")) == NULL)
        {
            ret = OB_ERR_COLUMN_NOT_FOUND;
            TRANS_LOG("Column name not found");
        }
        else if ((type_column = sql_context_->schema_manager_->get_column_schema(
                      OB_ALL_SYS_PARAM_TABLE_NAME, "data_type")) == NULL)
        {
            ret = OB_ERR_COLUMN_NOT_FOUND;
            TRANS_LOG("Column type not found");
        }
        else if ((value_column = sql_context_->schema_manager_->get_column_schema(
                      OB_ALL_SYS_PARAM_TABLE_NAME, "value")) == NULL)
        {
            ret = OB_ERR_COLUMN_NOT_FOUND;
            TRANS_LOG("Column value not found");
        }
        else
        {
            result_op->set_rpc_stub(sql_context_->merger_rpc_proxy_);
            result_op->set_table_id(OB_ALL_SYS_PARAM_TID);
            result_op->set_name_cid(name_column->get_id());
            result_op->set_rowkey_info(table_schema->get_rowkey_info());
            result_op->set_value_column(value_column->get_id(), value_column->get_type());
        }
    }
    int64_t variables_num = stmt->get_variables_size();
    for (int64_t i = 0; ret == OB_SUCCESS && i < variables_num; i++)
    {
        const ObVariableSetStmt::VariableSetNode& var_stmt_node = stmt->get_variable_node(static_cast<int32_t>(i));
        ObVariableSet::VariableSetNode var_op_node;
        ObSqlRawExpr *expr = NULL;
        var_op_node.is_system_variable_ = var_stmt_node.is_system_variable_;
        var_op_node.is_global_ = (var_stmt_node.scope_type_ == ObVariableSetStmt::GLOBAL);
        if (var_stmt_node.is_system_variable_ &&
                !sql_context_->session_info_->sys_variable_exists(var_stmt_node.variable_name_))
        {
            ret = OB_ERR_VARIABLE_UNKNOWN;
            TRANS_LOG("System variable %.*s Unknown", var_stmt_node.variable_name_.length(),
                      var_stmt_node.variable_name_.ptr());
        }
        else if ((ret = ob_write_string(*mem_pool_,
                                        var_stmt_node.variable_name_,
                                        var_op_node.variable_name_)) != OB_SUCCESS)
        {
            TRANS_LOG("Make place for variable name %.*s failed",
                      var_stmt_node.variable_name_.length(), var_stmt_node.variable_name_.ptr());
        }
        else if ((expr = logical_plan->get_expr(var_stmt_node.value_expr_id_)) == NULL)
        {
            ret = OB_ERR_ILLEGAL_ID;
            TRANS_LOG("Wrong expression id, id=%lu", var_stmt_node.value_expr_id_);
        }
        else if (var_op_node.is_system_variable_
                 &&
                 expr->get_result_type() != ObNullType
                 &&
                 expr->get_result_type() != (sql_context_->session_info_->get_sys_variable_type(var_stmt_node.variable_name_))
                 )
        {
            ret = OB_OBJ_TYPE_ERROR;
            TRANS_LOG("type not match");
            TBSYS_LOG(WARN, "type not match, ret=%d", ret);
        }
        else if ((var_op_node.variable_expr_ = ObSqlExpression::alloc()) == NULL)
        {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TRANS_LOG("no memory");
        }
        else if ((ret = result_op->add_variable_node(var_op_node)) != OB_SUCCESS)
        {
            ObSqlExpression::free(var_op_node.variable_expr_);
            var_op_node.variable_expr_ = NULL;
            TRANS_LOG("Add variable entry failed");
        }
        else if ((ret = expr->fill_sql_expression(
                      *var_op_node.variable_expr_,
                      this,
                      logical_plan,
                      physical_plan)
                  ) != OB_SUCCESS)
        {
            TRANS_LOG("Add value expression failed");
        }
    }
    return ret;
}

int ObTransformer::gen_physical_execute(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObExecute *result_op = NULL;
    ObExecuteStmt *stmt = NULL;
    /* get execute statement */
    get_stmt(logical_plan, err_stat, query_id, stmt);
    /* generate operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(result_op, ObExecute, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, stmt, result_op, index);
        }
    }

    ObSQLSessionInfo *session_info = NULL;
    if (ret == OB_SUCCESS
            && (sql_context_ == NULL || (session_info = sql_context_->session_info_) == NULL))
    {
        ret = OB_NOT_INIT;
        TRANS_LOG("Session info is not initiated");
    }

    if (ret == OB_SUCCESS)
    {
        uint64_t stmt_id = OB_INVALID_ID;
        if (session_info->plan_exists(stmt->get_stmt_name(), &stmt_id) == false)
        {
            ret = OB_ERR_PREPARE_STMT_UNKNOWN;
            TRANS_LOG("Can not find stmt %.*s ", stmt->get_stmt_name().length(), stmt->get_stmt_name().ptr());
        }
        else
        {
            result_op->set_stmt_id(stmt_id);
        }
        for (int64_t i = 0; ret == OB_SUCCESS && i < stmt->get_variable_size(); i++)
        {
            const ObString& var_name = stmt->get_variable_name(i);
            if (session_info->variable_exists(var_name))
            {
                //add peiouya [NotNULL_check] [JHOBv0.1] 20131211:b
                bool val_params_constraint = true;
                if (OB_SUCCESS != (ret = result_->get_params_constraint(i, val_params_constraint)))
                {
                    TRANS_LOG("Fail to get the constraint of  Variable %.*s!", var_name.length(), var_name.ptr());
                }
                else if (OB_SUCCESS != (ret = session_info->variable_constrain_check(val_params_constraint, var_name)))
                {
                    TRANS_LOG("Value of Variable %.*s is NULL, but the bind column can not accept it !", var_name.length(), var_name.ptr());
                }
                else if(ret == OB_SUCCESS)
                {
                    //add 20131222:e
                    ObString tmp_name;
                    if ((ret = ob_write_string(*mem_pool_, var_name, tmp_name)) != OB_SUCCESS
                            || (ret = result_op->add_param_name(var_name)) != OB_SUCCESS)
                    {
                        TRANS_LOG("add variable %.*s failed", var_name.length(), var_name.ptr());
                    }
                }
            }
            else
            {
                ret = OB_ERR_VARIABLE_UNKNOWN;
                TRANS_LOG("Variable %.*s Unknown", var_name.length(), var_name.ptr());
            }
        }
    }

    return ret;
}

int ObTransformer::gen_physical_deallocate(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObDeallocate *result_op = NULL;
    ObDeallocateStmt *stmt = NULL;
    /* get deallocate statement */
    get_stmt(logical_plan, err_stat, query_id, stmt);
    /* generate operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(result_op, ObDeallocate, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, stmt, result_op, index);
        }
    }
    if (ret == OB_SUCCESS)
    {
        uint64_t stmt_id = OB_INVALID_ID;
        if (sql_context_== NULL || sql_context_->session_info_ == NULL)
        {
            ret = OB_NOT_INIT;
            TRANS_LOG("Session info is needed");
        }
        else if (sql_context_->session_info_->plan_exists(stmt->get_stmt_name(), &stmt_id) == false)
        {
            ret = OB_ERR_PREPARE_STMT_UNKNOWN;
            TRANS_LOG("Unknown prepared statement handler (%.*s) given to DEALLOCATE PREPARE",
                      stmt->get_stmt_name().length(), stmt->get_stmt_name().ptr());
        }
        else
        {
            result_op->set_stmt_id(stmt_id);
        }
    }

    //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160524:b
    if (OB_SUCCESS == ret)
    {
        TBSYS_LOG(DEBUG,"gaojt-test-ud: stmt-name=%.*s",stmt->get_stmt_name().length(),stmt->get_stmt_name().ptr());
        result_op->set_stmt_name(stmt->get_stmt_name());
    }
    //add gaojt 20160524:e

    return ret;
}

// add by liyongfeng:20150105 [secondary index for replace] like gen_phy_static_data_scan() used for insert,
// this is used for replace, only a little different from gen_phy_static_data_scan()
int ObTransformer::gen_phy_static_data_scan_for_replace(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const ObInsertStmt *insert_stmt,
        const ObRowDesc& row_desc,
        const ObSEArray<int64_t, 64> &row_desc_map,
        const uint64_t table_id,
        const ObRowkeyInfo &rowkey_info,
        ObTableRpcScan &table_scan)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    OB_ASSERT(logical_plan);
    OB_ASSERT(physical_plan);
    ObSqlExpression *rows_filter = ObSqlExpression::alloc();
    if (NULL == rows_filter)
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "no memory");
    }
    ObSqlExpression column_ref;
    // construct left operand of IN operator
    // the same order with row_desc
    ExprItem expr_item;
    expr_item.type_ = T_REF_COLUMN;
    expr_item.value_.cell_.tid = table_id;
    int64_t rowkey_column_num = rowkey_info.get_size();
    uint64_t tid = OB_INVALID_ID;

    // begin: from here, is different from gen_phy_static_data_scan(),
    // differences: output columns not only include rowkey columns, but also include other columns
    for (int i = 0; OB_SUCCESS == ret && i < row_desc.get_column_num(); ++i)
    {
        if (OB_UNLIKELY(OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, expr_item.value_.cell_.cid))))
        {
            break;
        }

        column_ref.reset();
        column_ref.set_tid_cid(table_id, expr_item.value_.cell_.cid);

        if (rowkey_info.is_rowkey_column(expr_item.value_.cell_.cid))//if rowkey column, add it into row_filter
        {
            if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
            {
                TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
                break;
            }
        }
        if (OB_SUCCESS != (ret = column_ref.add_expr_item(expr_item)))
        {
            TBSYS_LOG(WARN, "fialed to add expr item, err=%d", ret);
            break;
        }
        else if (OB_SUCCESS != (ret = column_ref.add_expr_item_end()))
        {
            TBSYS_LOG(WARN, "fialed to add expr item, err=%d", ret);
            break;
        }
        else if (OB_SUCCESS != (ret = table_scan.add_output_column(column_ref)))
        {
            TBSYS_LOG(WARN, "failed to add output column, err=%d", ret);
            break;
        }
    }// end for
    // end here

    // add action flag column
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        column_ref.reset();
        column_ref.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
        if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, column_ref)))
        {
            TBSYS_LOG(WARN, "fail to make column expr, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = table_scan.add_output_column(column_ref)))
        {
            TBSYS_LOG(WARN, "failed to add output column, err=%d", ret);
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        expr_item.type_ = T_OP_ROW;
        expr_item.value_.int_ = rowkey_column_num;
        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
        {
            TRANS_LOG("failed to add expr item, err=%d", ret);
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        expr_item.type_ = T_OP_LEFT_PARAM_END;
        // a in (a,b,c) => 1 Dim;
        expr_item.value_.int_ = 2;
        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
        {
            TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
        }
    }

    uint64_t column_id = OB_INVALID_ID;
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        int64_t row_num = insert_stmt->get_value_row_size();
        //for (int64_t i = 0; OB_SUCCESS == ret && i < row_num; i++)
        for(int64_t i = row_num - 1; OB_SUCCESS == ret && i>= 0; i--)//modify hushuang 20161103
        {
            const ObArray<uint64_t>& value_row = insert_stmt->get_value_row(i);
            OB_ASSERT(value_row.count() == row_desc_map.count());
            for (int64_t j = 0; OB_SUCCESS == ret && j < row_desc_map.count(); j++)
            {
                ObSqlRawExpr *value_expr = logical_plan->get_expr(value_row.at(row_desc_map.at(j)));
                if (NULL == value_expr)
                {
                    ret = OB_ERR_ILLEGAL_ID;
                    TRANS_LOG("failed to get value");
                }
                else if (OB_SUCCESS != (ret = row_desc.get_tid_cid(j, tid, column_id)))
                {
                    TRANS_LOG("failed to get tid cid, err=%d", ret);
                }
                // success
                else if (rowkey_info.is_rowkey_column(column_id))
                {
                    // add rigth oprands of the IN operator
                    if (OB_SUCCESS != (ret = value_expr->get_expr()->fill_sql_expression(*rows_filter, this, logical_plan, physical_plan)))
                    {
                        TRANS_LOG("failed to fill expr, err=%d", ret);
                    }
                    // add wnghaixing [decimal] for fix delete bug 2014/10/11
                    else
                    {
                        ObObjType cond_val_type;
                        uint32_t cond_val_precision;
                        uint32_t cond_val_scale;

                        // ObObj static_obj
                        if (OB_SUCCESS != sql_context_->schema_manager_->get_cond_val_info(tid, column_id, cond_val_type, cond_val_precision, cond_val_scale))
                        {

                        }
                        //mod liumz, [decimal] for fix delete bug 20160722:b
                        //else
                        else if(ObDecimalType == cond_val_type)
                        //mod:e
                        {
                            ObPostfixExpression& ops = rows_filter->get_decoded_expression_v2();
                            ObObj& obj = ops.get_expr();
                            if (ObDecimalType == obj.get_type())
                            {
                                ops.fix_varchar_and_decimal(cond_val_precision, cond_val_scale);
                            }
                            else if (ObVarcharType == obj.get_type())
                            {
                                ops.fix_varchar_and_decimal(cond_val_precision, cond_val_scale);
                            }
                        }
                    }
                    // add:e
                }
            }// end for
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                if (rowkey_column_num > 0)
                {
                    expr_item.type_ = T_OP_ROW;
                    expr_item.value_.int_ = rowkey_column_num;
                    if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
                    {
                        TRANS_LOG("failed to add expr item, err=%d", ret);
                    }
                }
            }
        }// end for

        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            expr_item.type_ = T_OP_ROW;
            expr_item.value_.int_ = row_num;
            ExprItem expr_in;
            expr_in.type_ = T_OP_IN;
            expr_in.value_.int_ = 2;
            if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
            {
                TRANS_LOG("failed to add expr item, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_in)))
            {
                TRANS_LOG("failed to add expr item, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = rows_filter->add_expr_item_end()))
            {
                TRANS_LOG("failed to add expr item, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = table_scan.add_filter(rows_filter)))
            {
                TRANS_LOG("failed to add filter, err=%d", ret);
            }
        }
    }

    return ret;
}
// add:e

int ObTransformer::gen_phy_static_data_scan(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        //mod lijianqiang [seqeunce] 20150515 :b
        //    const ObInsertStmt * insert_stmt,
        ObStmt *stmt,
        //mod 20150515:e
        const ObRowDesc& row_desc,
        const ObSEArray<int64_t, 64> &row_desc_map,
        const uint64_t table_id,
        const ObRowkeyInfo &rowkey_info,
        ObTableRpcScan &table_scan)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    OB_ASSERT(logical_plan);
    //mod lijianqiang [sequence] 20150515:b
    //  OB_ASSERT(insert_stmt);
    OB_ASSERT(stmt);
    //mod 20150515:e
    //add lijianqiang [sequence insert] 20150419:b
    ObSequenceStmt *sequence_stmt = NULL;
    ObInsertStmt * insert_stmt = NULL;
    bool has_sequence = false;

    if (stmt->get_stmt_type() == ObBasicStmt::T_INSERT
            || stmt->get_stmt_type() == ObBasicStmt::T_DELETE
            || stmt->get_stmt_type() == ObBasicStmt::T_UPDATE
            || stmt->get_stmt_type() == ObBasicStmt::T_SELECT)
    {
        sequence_stmt = dynamic_cast<ObSequenceStmt*>(stmt);
        has_sequence = sequence_stmt->has_sequence();
    }
    if (!has_sequence)
    {
        insert_stmt = dynamic_cast<ObInsertStmt*>(stmt);
    }
    //add 20150419:e
    ObSqlExpression *rows_filter = ObSqlExpression::alloc();
    if (NULL == rows_filter)
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "no memory");
    }
    ObSqlExpression column_ref;
    // construct left operand of IN operator
    // the same order with row_desc
    ExprItem expr_item;
    expr_item.type_ = T_REF_COLUMN;
    expr_item.value_.cell_.tid = table_id;
    int64_t rowkey_column_num = rowkey_info.get_size();
    uint64_t tid = OB_INVALID_ID;
    for (int i = 0; OB_SUCCESS == ret && i < row_desc.get_column_num(); ++i)
    {
        if (OB_UNLIKELY(OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, expr_item.value_.cell_.cid))))
        {
            break;
        }
        else if (rowkey_info.is_rowkey_column(expr_item.value_.cell_.cid))
        {
            column_ref.reset();
            column_ref.set_tid_cid(table_id, expr_item.value_.cell_.cid);
            if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
            {
                TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = column_ref.add_expr_item(expr_item)))
            {
                TBSYS_LOG(WARN, "failed to add expr_item, err=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = column_ref.add_expr_item_end()))
            {
                TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = table_scan.add_output_column(column_ref)))
            {
                TBSYS_LOG(WARN, "failed to add output column, err=%d", ret);
                break;
            }
        }
    } // end for
    // add action flag column
    //mod lijianqiang [sequence insert] 20150419:b
    //  if (OB_LIKELY(OB_SUCCESS == ret))
    if (OB_LIKELY(OB_SUCCESS == ret && !has_sequence))
        //mod 20150419:e
    {
        column_ref.reset();
        column_ref.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
        if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, column_ref)))
        {
            TBSYS_LOG(WARN, "fail to make column expr:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = table_scan.add_output_column(column_ref)))
        {
            TBSYS_LOG(WARN, "failed to add output column, err=%d", ret);
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        expr_item.type_ = T_OP_ROW;
        expr_item.value_.int_ = rowkey_column_num;
        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
        {
            TRANS_LOG("Failed to add expr item, err=%d", ret);
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        expr_item.type_ = T_OP_LEFT_PARAM_END;
        // a in (a,b,c) => 1 Dim;  (a,b) in ((a,b),(c,d)) =>2 Dim; ((a,b),(c,d)) in (...) =>3 Dim
        expr_item.value_.int_ = 2;
        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
        {
            TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
        }
    }
    uint64_t column_id = OB_INVALID_ID;
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        //mod lijianqiang [sequecne insert] 20140410:b
        int64_t row_num = OB_INVALID_INDEX;
        if (!has_sequence)
        {
            row_num = insert_stmt->get_value_row_size();
            for (int64_t i = 0; ret == OB_SUCCESS && i < row_num; i++) // for each row
            {
                const ObArray<uint64_t>& value_row = insert_stmt->get_value_row(i);
                OB_ASSERT(value_row.count() == row_desc_map.count());
                for (int64_t j = 0; ret == OB_SUCCESS && j < row_desc_map.count(); j++)
                {
                    ObSqlRawExpr *value_expr = logical_plan->get_expr(value_row.at(row_desc_map.at(j)));
                    if (value_expr == NULL)
                    {
                        ret = OB_ERR_ILLEGAL_ID;
                        TRANS_LOG("Get value failed");
                    }
                    else if (OB_SUCCESS != (ret = row_desc.get_tid_cid(j, tid, column_id)))
                    {
                        TRANS_LOG("Failed to get tid cid, err=%d", ret);
                    }
                    // success
                    else if (rowkey_info.is_rowkey_column(column_id))
                    {
                        // add right oprands of the IN operator
                        if (OB_SUCCESS != (ret = value_expr->get_expr()->fill_sql_expression(*rows_filter, this, logical_plan, physical_plan)))
                        {
                            TRANS_LOG("Failed to fill expr, err=%d", ret);
                        }
                        //add wenghaixing[decimal] for fix insert bug 2014/10/11
                        else
                        {
                            ObObjType cond_val_type;
                            uint32_t cond_val_precision;
                            uint32_t cond_val_scale;

                            //ObObj static_obj;
                            if(OB_SUCCESS!=sql_context_->schema_manager_->get_cond_val_info(tid,column_id,cond_val_type,cond_val_precision,cond_val_scale))
                            {

                            }
                            else if(ObDecimalType == cond_val_type)
                            {
                                ObPostfixExpression& ops=rows_filter->get_decoded_expression_v2();
                                ObObj& obj=ops.get_expr();
                                if(ObDecimalType==obj.get_type())
                                {
                                    ops.fix_varchar_and_decimal(cond_val_precision,cond_val_scale);
                                }
                                else if(ObVarcharType==obj.get_type())
                                {
                                    ops.fix_varchar_and_decimal(cond_val_precision,cond_val_scale);
                                }

                            }

                        }
                        //add:e
                    }
                } // end for
                if (OB_LIKELY(ret == OB_SUCCESS))
                {
                    if (rowkey_column_num > 0)
                    {
                        expr_item.type_ = T_OP_ROW;
                        expr_item.value_.int_ = rowkey_column_num;
                        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
                        {
                            TRANS_LOG("Failed to add expr item, err=%d", ret);
                        }
                    }
                }
            } // end for
        }//end of  if (!has_sequen...
        else//has_sequence
        {
            ObArray<ObString> *sequence_names_array = NULL;
            row_num = sequence_stmt->get_sequence_names_no_dup_size();
            sequence_names_array = &(sequence_stmt->get_sequence_names_no_dup());
            for (int64_t i = 0; ret == OB_SUCCESS && i < row_num; i++) // for each sequence
            {
                ObObj val;
                const ObString& sequence_name = sequence_names_array->at(i);
                val.set_varchar(sequence_name);
                ObConstRawExpr c_expr;
                c_expr.set_expr_type(T_STRING);
                c_expr.set_result_type(ObVarcharType);
                c_expr.set_value(val);
                //        TBSYS_LOG(ERROR, "the sequence_name_is::[%.*s]",sequence_name.length(),sequence_name.ptr());
                if (OB_SUCCESS != (ret = row_desc.get_tid_cid(0, tid, column_id)))
                {
                    TRANS_LOG("Failed to get sequence tid cid, err=%d", ret);
                }
                if (rowkey_info.is_rowkey_column(column_id))
                {
                    //          TBSYS_LOG(ERROR, "!!!the column_id num is::%ld,table id is::%ld",column_id,tid);
                    if ((ret = c_expr.fill_sql_expression(*rows_filter, this, logical_plan, physical_plan)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Failed to fill sequence expr, err=%d", ret);
                    }
                }

                if (OB_LIKELY(ret == OB_SUCCESS))
                {
                    if (rowkey_column_num > 0)
                    {
                        expr_item.type_ = T_OP_ROW;
                        expr_item.value_.int_ = rowkey_column_num;
                        //            TBSYS_LOG(ERROR, "!!!the rowkey_column_num  is::%ld",rowkey_column_num);
                        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
                        {
                            TRANS_LOG("Failed to add expr item, err=%d", ret);
                        }
                    }
                }
            }//end for

            //add output columns
            ExprItem expr_item;
            expr_item.type_ = T_REF_COLUMN;
            expr_item.value_.cell_.tid = table_id;
            //      int64_t rowkey_column_num = rowkey_info.get_size();
            uint64_t tid = OB_INVALID_ID;
            for (int i = 0; OB_SUCCESS == ret && i < row_desc.get_column_num(); i++)
            {
                if (OB_UNLIKELY(OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, expr_item.value_.cell_.cid))))
                {
                    break;
                }
                else if (!rowkey_info.is_rowkey_column(expr_item.value_.cell_.cid))
                {
                    column_ref.reset();
                    column_ref.set_tid_cid(tid, expr_item.value_.cell_.cid);
                    if (OB_SUCCESS != (ret = column_ref.add_expr_item(expr_item)))
                    {
                        TBSYS_LOG(WARN, "failed to add expr_item, err=%d", ret);
                        break;
                    }
                    else if (OB_SUCCESS != (ret = column_ref.add_expr_item_end()))
                    {
                        TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
                        break;
                    }
                    else if (OB_SUCCESS != (ret = table_scan.add_output_column(column_ref)))
                    {
                        TBSYS_LOG(WARN, "failed to add output column, err=%d", ret);
                        break;
                    }
                    //          TBSYS_LOG(ERROR, "the output column is::[%s]",to_cstring(column_ref));
                }
            }//end for
        }// end else
        //mod 2050410:e

        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            expr_item.type_ = T_OP_ROW;
            expr_item.value_.int_ = row_num;
            ExprItem expr_in;
            expr_in.type_ = T_OP_IN;
            expr_in.value_.int_ = 2;
            if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
            {
                TRANS_LOG("Failed to add expr item, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_in)))
            {
                TRANS_LOG("Failed to add expr item, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = rows_filter->add_expr_item_end()))
            {
                TRANS_LOG("Failed to add expr item end, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = table_scan.add_filter(rows_filter)))
            {
                TRANS_LOG("Failed to add filter, err=%d", ret);
            }
        }
    }
    return ret;
}

int ObTransformer::wrap_ups_executor(
        ObPhysicalPlan *physical_plan,
        const uint64_t query_id,
        ObPhysicalPlan*& new_plan,
        int32_t *index,
        ErrStat& err_stat)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    OB_ASSERT(physical_plan);
    if (query_id == OB_INVALID_ID || !physical_plan->in_ups_executor())
    {
        ObUpsExecutor *ups_executor = NULL;
        new_plan = (ObPhysicalPlan*)trans_malloc(sizeof(ObPhysicalPlan));
        if (NULL == new_plan)
        {
            TRANS_LOG("no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
            new_plan = new(new_plan) ObPhysicalPlan();
            TBSYS_LOG(DEBUG, "new wrapper physical plan, addr=%p", physical_plan);
            //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507:b
            if(is_insert_select_
                    || is_delete_update_)//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160301
            {
                  ObIudLoopControl *insert_loop_control = NULL;
                  if (NULL == CREATE_PHY_OPERRATOR(insert_loop_control, ObIudLoopControl, physical_plan, err_stat))
                  {
                      ret = OB_ALLOCATE_MEMORY_FAILED;
                  }
                  else if(NULL == CREATE_PHY_OPERRATOR(ups_executor, ObUpsExecutor, physical_plan, err_stat))
                  {
                      ret = OB_ALLOCATE_MEMORY_FAILED;
                  }
                  else if (OB_SUCCESS != (ret = insert_loop_control->set_child(0,*(ups_executor))))
                  {
                      TRANS_LOG("failed to set child, err=%d", ret);
                  }
                  else if (OB_SUCCESS != (ret = physical_plan->add_phy_query(
                                              insert_loop_control,
                                              index,
                                              query_id == OB_INVALID_ID)))
                  {
                      TBSYS_LOG(WARN, "failed to add query, err=%d", ret);
                  }
                  else if (NULL == sql_context_->merge_service_)
                  {
                      ret = OB_NOT_INIT;
                      TBSYS_LOG(WARN, "merge_service_ is null");
                  }
                  else
                  {
                      new_plan->set_in_ups_executor(true);
                      ups_executor->set_rpc_stub(sql_context_->merger_rpc_proxy_);
                      ups_executor->set_inner_plan(new_plan);
                  }
                  if (OB_SUCCESS != ret)
                  {
                      new_plan->~ObPhysicalPlan();
                  }
                  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507:b
                  if(OB_SUCCESS==ret)
                  {
                      insert_loop_control->set_sql_context (*sql_context_);
                      if( is_insert_select_ )
                      {
                          if(is_insert_multi_batch_)
                          {
                              // add by maosy [Delete_Update_Function_isolation_RC] 20161218
                              insert_loop_control->set_need_start_trans (false);
                //insert_loop_control->set_is_multi_batch(true);
                              TBSYS_LOG(DEBUG,"is the multi-insert");
                          }
                          else
                          {
                              insert_loop_control->set_need_start_trans (true);
                          }
//                          insert_loop_control->set_is_ud (true);
//add e
                      }
                      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160301:b
                      else if( is_delete_update_ )
                      {
                          ups_executor->set_is_delete_update(true);
                          if(is_multi_batch_)
                          {
                              // add by maosy [Delete_Update_Function_isolation_RC] 20161218
                  //insert_loop_control->set_is_multi_batch(true);
                              TBSYS_LOG(DEBUG,"is the multi-delete_or_update");
//                              insert_loop_control->set_is_ud (true);
//add by maosy
                          }
                      }
                      is_delete_update_ = false;
                      //add gaojt 20160301:e
                      //is_insert_select_ = false;
                  }
            }
            //add gaojt 20150507:e
            else
            {
                if (NULL == CREATE_PHY_OPERRATOR(ups_executor, ObUpsExecutor, physical_plan, err_stat))
                {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                }
                else if (OB_SUCCESS != (ret = physical_plan->add_phy_query(
                                            ups_executor,
                                            index,
                                            query_id == OB_INVALID_ID)))
                {
                    TBSYS_LOG(WARN, "failed to add query, err=%d", ret);
                }
                else if (NULL == sql_context_->merge_service_)
                {
                    ret = OB_NOT_INIT;
                    TBSYS_LOG(WARN, "merge_service_ is null");
                }
                else
                {
                    new_plan->set_in_ups_executor(true);
                    ups_executor->set_rpc_stub(sql_context_->merger_rpc_proxy_);
                    ups_executor->set_inner_plan(new_plan);
                    //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151223:b
                    if(is_delete_update_)
                    {
                        ups_executor->set_is_delete_update(true);
                    }
                    //add gaojt 20151223:e
                }
                if (OB_SUCCESS != ret)
                {
                    new_plan->~ObPhysicalPlan();
                }
            }
        }
    }
    else
    {
        new_plan = physical_plan;
    }
    return ret;
}

int ObTransformer::gen_physical_insert_new(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObInsertStmt *insert_stmt = NULL;
    ObUpsModifyWithDmlType *ups_modify = NULL;
    ObRowDesc row_desc;
    ObRowDescExt row_desc_ext;
    ObSEArray<int64_t, 64> row_desc_map;
    const ObRowkeyInfo *rowkey_info = NULL;
    ObPhysicalPlan* inner_plan = NULL;
    //add fanqiushi_index
    bool is_need_modify_index=false;
    int64_t index_num = 0;
    //add:e
    //mod gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150505:b
    //  if (OB_SUCCESS != (ret = wrap_ups_executor(physical_plan, query_id, inner_plan, index, err_stat)))
    //  {
    //    TBSYS_LOG(WARN, "err=%d", ret);
    //  }
    //  else if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, insert_stmt)))
    //  {
    //  }
    //  else if (NULL == CREATE_PHY_OPERRATOR(ups_modify, ObUpsModifyWithDmlType, inner_plan, err_stat))
    //  {
    //    ret = OB_ALLOCATE_MEMORY_FAILED;
    //  }
    //  else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(
    //                                                ups_modify,
    //                                                physical_plan == inner_plan ? index : NULL,
    //                                                physical_plan != inner_plan)))
    //  {
    //    TRANS_LOG("Failed to add main phy query, err=%d", ret);
    //  }
    //  else if (OB_SUCCESS != (ret = cons_row_desc(insert_stmt->get_table_id(), insert_stmt,
    //                                              row_desc_ext, row_desc, rowkey_info, row_desc_map, err_stat)))
    //  {
    //    ret = OB_ERROR;
    //    TRANS_LOG("Fail to get table schema for table[%ld]", insert_stmt->get_table_id());
    //  }
    //  else
    //  {
    //    ups_modify->set_dml_type(OB_DML_INSERT);
    //    // check primary key columns
    //    uint64_t tid = insert_stmt->get_table_id();
    //    uint64_t cid = OB_INVALID_ID;
    //    for (int64_t i = 0; i < rowkey_info->get_size(); ++i)
    //    {
    //      if (OB_SUCCESS != (ret = rowkey_info->get_column_id(i, cid)))
    //      {
    //        TBSYS_LOG(USER_ERROR, "primary key can not be empty");
    //        ret = OB_ERR_INSERT_NULL_ROWKEY;
    //        break;
    //      }
    //      else if (OB_INVALID_INDEX == row_desc.get_idx(tid, cid))
    //      {
    //        TBSYS_LOG(USER_ERROR, "primary key can not be empty");
    //        ret = OB_ERR_INSERT_NULL_ROWKEY;
    //        break;
    //      }
    //    } // end for
    //    //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
    //    if (OB_LIKELY(OB_SUCCESS == ret))
    //    {
    //      ret = column_null_check(logical_plan, insert_stmt, err_stat, OP_INSERT);
    //    }
    //    //add 20131222:e
    //  }

    is_insert_select_ = false;
    if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, insert_stmt)))
    {

    }
    else if(insert_stmt->get_insert_query_id() != OB_INVALID_ID)
    {
        is_insert_select_ = true; //insert ... select ...
    }

    if(OB_SUCCESS == ret)
    {
        //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507:b
        if(insert_stmt->get_is_insert_multi_batch())
        {
            is_insert_multi_batch_ = true; //
        }
        //add gaojt 20150507:e

        if (OB_SUCCESS != (ret = wrap_ups_executor(physical_plan, query_id, inner_plan, index, err_stat)))
        {
            TBSYS_LOG(WARN, "err=%d", ret);
        }
        // modified end
        else if (NULL == CREATE_PHY_OPERRATOR(ups_modify, ObUpsModifyWithDmlType, inner_plan, err_stat))
        {
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(
                                    ups_modify,
                                    physical_plan == inner_plan ? index : NULL,
                                    physical_plan != inner_plan)))
        {
            TRANS_LOG("Failed to add main phy query, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = cons_row_desc(insert_stmt->get_table_id(), insert_stmt,
                                                    row_desc_ext, row_desc, rowkey_info, row_desc_map, err_stat)))
        {
            ret = OB_ERROR;
            TRANS_LOG("Fail to get table schema for table[%ld]", insert_stmt->get_table_id());
        }
        else
        {
            ups_modify->set_dml_type(OB_DML_INSERT);
            // check primary key columns
            uint64_t tid = insert_stmt->get_table_id();
            uint64_t cid = OB_INVALID_ID;
            for (int64_t i = 0; i < rowkey_info->get_size(); ++i)
            {
                if (OB_SUCCESS != (ret = rowkey_info->get_column_id(i, cid)))
                {
                    TBSYS_LOG(USER_ERROR, "primary key can not be empty");
                    ret = OB_ERR_INSERT_NULL_ROWKEY;
                    break;
                }
                else if (OB_INVALID_INDEX == row_desc.get_idx(tid, cid))
                {
                    TBSYS_LOG(USER_ERROR, "primary key can not be empty");
                    ret = OB_ERR_INSERT_NULL_ROWKEY;
                    break;
                }
            } // end for
            //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                ret = column_null_check(logical_plan, insert_stmt, err_stat, OP_INSERT);
            }
            //add 20131222:e
        }
    }
    //mod gaojt 20150505:e

    //add fanqiushi_index
    if (OB_LIKELY(ret == OB_SUCCESS))
    {
        uint64_t main_tid=insert_stmt->get_table_id();
        if (sql_context_->schema_manager_->is_have_modifiable_index(main_tid))
        {
            is_need_modify_index=true;
        }
    }
    //add:e
    if (OB_LIKELY(ret == OB_SUCCESS))
    {
        if (OB_LIKELY(insert_stmt->get_insert_query_id() == OB_INVALID_ID))
        {
            // INSERT ... VALUES ...
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                uint64_t tid = insert_stmt->get_table_id();
                const ObTableSchema *table_schema = NULL;
                if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(tid)))
                {
                    ret = OB_ERR_ILLEGAL_ID;
                    TRANS_LOG("fail to get table schema for table[%ld]", tid);
                }
                else if (row_desc.get_idx(tid, table_schema->get_create_time_column_id()) != OB_INVALID_INDEX)
                {
                    ret = OB_ERR_CREAT_MODIFY_TIME_COLUMN;
                    ColumnItem *column_item = insert_stmt->get_column_item_by_id(tid, table_schema->get_create_time_column_id());
                    if (column_item != NULL)
                        TRANS_LOG("Column '%.*s' of type ObCreateTimeType can not be inserted",
                                  column_item->column_name_.length(), column_item->column_name_.ptr());
                    else
                        TRANS_LOG("Column '%ld' of type ObCreateTimeType can not be inserted",
                                  table_schema->get_create_time_column_id());
                }
                else if (row_desc.get_idx(tid, table_schema->get_modify_time_column_id()) != OB_INVALID_INDEX)
                {
                    ret = OB_ERR_CREAT_MODIFY_TIME_COLUMN;
                    ColumnItem *column_item = insert_stmt->get_column_item_by_id(tid, table_schema->get_modify_time_column_id());
                    if (column_item != NULL)
                        TRANS_LOG("Column '%.*s' of type ObModifyTimeType can not be inserted",
                                  column_item->column_name_.length(), column_item->column_name_.ptr());
                    else
                        TRANS_LOG("Column '%ld' of type ObModifyTimeType can not be inserted",
                                  table_schema->get_modify_time_column_id());
                }
            }
            if (OB_SUCCESS == ret)
            {
                //==================================================================
                //add lijianqiang [sequence insert] 20150407
                //ObSequenceInsert
                bool has_sequence = insert_stmt->has_sequence();
                ObSequenceInsert *sequence_values = NULL;
                if (has_sequence)
                {
                    if (OB_LIKELY(OB_SUCCESS == ret))
                    {
                        CREATE_PHY_OPERRATOR(sequence_values, ObSequenceInsert, inner_plan, err_stat);
                        if (OB_SUCCESS == ret)//add qianzm [null operator unjudgement bug1181] 20160520
                        {
                            if (OB_SUCCESS != (ret = wrap_sequence(logical_plan, physical_plan, err_stat,
                                                                   row_desc_map, sequence_values, insert_stmt,
                                                                   inner_plan)))
                            {
                                TRANS_LOG("wrap sequence failed");
                            }
                            if (OB_LIKELY(OB_SUCCESS == ret))
                            {
                                sequence_values->init_global_prevval_state_and_lock_sequence_map();
                                sequence_values->copy_sequence_info_from_insert_stmt(row_desc_map);//copy sequence info from insert stmt
                                sequence_values->set_out_row_desc_and_row_desc_ext(row_desc, row_desc_ext);//for column with sequence obj_cast check
                            }
                        }
                    }
                    if (OB_LIKELY(OB_SUCCESS == ret))
                    {
                        if (OB_SUCCESS != (ret = inner_plan->add_phy_query(sequence_values)))
                        {
                            TBSYS_LOG(WARN, "failed to add phy query, err=%d", ret);
                        }
                    }
                }//end if(has_sequence)
                //==================================================================
                //add 20150407:e

                ObTableRpcScan *table_scan = NULL;
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    ObRpcScanHint hint;
                    hint.read_method_ = ObSqlReadStrategy::USE_GET;
                    hint.is_get_skip_empty_row_ = false;
                    hint.read_consistency_ = FROZEN;
                    const ObTableSchema *table_schema = NULL;
                    int64_t table_id = insert_stmt->get_table_id();
                    CREATE_PHY_OPERRATOR(table_scan, ObTableRpcScan, inner_plan, err_stat);
                    if (OB_UNLIKELY(OB_SUCCESS != ret))
                    {
                    }
                    else if (OB_SUCCESS != (ret = table_scan->set_table(table_id, table_id)))
                    {
                        TRANS_LOG("failed to set table id, err=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = table_scan->init(sql_context_, &hint)))
                    {
                        TRANS_LOG("failed to init table scan, err=%d", ret);
                    }
                    //del lijianqiang [sequence insert] 20150407 :b
                    //          else if (OB_SUCCESS != (ret = gen_phy_static_data_scan(logical_plan, inner_plan, err_stat,
                    //                                                                 insert_stmt, row_desc, row_desc_map,
                    //                                                                 table_id, *rowkey_info, *table_scan)))
                    //          {
                    //            TRANS_LOG("err=%d", ret);
                    //          }
                    //del 20150407:e
                    else if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
                    {
                        ret = OB_ERR_ILLEGAL_ID;
                        TRANS_LOG("Fail to get table schema for table[%ld]", table_id);
                    }
                    else if ((ret = physical_plan->add_base_table_version(
                                  table_id,
                                  table_schema->get_schema_version()
                                  )) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", table_id, ret);
                    }
                    else
                    {
                        table_scan->set_rowkey_cell_count(row_desc.get_rowkey_cell_count());
                        table_scan->set_cache_bloom_filter(true);
                        //add lijianqiang [sequence insert] 20150407 :b
                        if (!has_sequence)//if has sequence, this function is finished in sequence op
                        {
                            if (OB_SUCCESS != (ret = gen_phy_static_data_scan(logical_plan, inner_plan, err_stat,
                                                                              insert_stmt, row_desc, row_desc_map,
                                                                              table_id, *rowkey_info, *table_scan)))
                            {
                                TRANS_LOG("err=%d", ret);
                            }
                        }
                        //add 20150407:e
                    }
                }
                ObValues *tmp_table = NULL;
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    CREATE_PHY_OPERRATOR(tmp_table, ObValues, inner_plan, err_stat);
                    if (OB_UNLIKELY(OB_SUCCESS != ret))
                    {
                    }
                    else if (OB_SUCCESS != (ret = tmp_table->set_child(0, *table_scan)))
                    {
                        TBSYS_LOG(WARN, "failed to set child, err=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(tmp_table)))
                    {
                        TBSYS_LOG(WARN, "failed to add phy query, err=%d", ret);
                    }
                }
                ObMemSSTableScan *static_data = NULL;
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    CREATE_PHY_OPERRATOR(static_data, ObMemSSTableScan, inner_plan, err_stat);
                    if (OB_LIKELY(OB_SUCCESS == ret))
                    {
                        static_data->set_tmp_table(tmp_table->get_id());
                    }
                }
                ObIncScan *inc_scan = NULL;
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    CREATE_PHY_OPERRATOR(inc_scan, ObIncScan, inner_plan, err_stat);
                    if (OB_LIKELY(OB_SUCCESS == ret))
                    {
                        inc_scan->set_scan_type(ObIncScan::ST_MGET);
                        inc_scan->set_write_lock_flag();
                    }
                }
                ObMultipleGetMerge *fuse_op = NULL;
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    CREATE_PHY_OPERRATOR(fuse_op, ObMultipleGetMerge, inner_plan, err_stat);
                    if (OB_UNLIKELY(OB_SUCCESS != ret))
                    {
                    }
                    else if ((ret = fuse_op->set_child(0, *static_data)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Set child of fuse_op operator failed, err=%d", ret);
                    }
                    else if ((ret = fuse_op->set_child(1, *inc_scan)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Set child of fuse_op operator failed, err=%d", ret);
                    }
                    else
                    {
                        fuse_op->set_is_ups_row(false);
                    }
                }
                ObExprValues *input_values = NULL;
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    CREATE_PHY_OPERRATOR(input_values, ObExprValues, inner_plan, err_stat);
                    if (OB_UNLIKELY(OB_SUCCESS != ret))
                    {
                    }
                    else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(input_values)))
                    {
                        TBSYS_LOG(WARN, "failed to add phy query, err=%d", ret);
                    }
                    else if ((ret = input_values->set_row_desc(row_desc, row_desc_ext)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Set descriptor of value operator failed, err=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = gen_phy_values(logical_plan, inner_plan, err_stat, insert_stmt,
                                                                 row_desc, row_desc_ext, &row_desc_map, *input_values)))
                    {
                        TRANS_LOG("Failed to generate values, err=%d", ret);
                    }
                    else
                    {
                        input_values->set_check_rowkey_duplicate(true);
                    }
                }
                //add lijianqiang [sequence insert] 20150407:b
                /*Exp:if has sequence,set child after input values and tmp_table created*/
                if (has_sequence && OB_SUCCESS == ret)
                {
                    if (OB_SUCCESS != (ret = sequence_values->set_child(1, *input_values)))
                    {
                        TBSYS_LOG(WARN, "failed to set child input_values, err=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = sequence_values->set_child(2, *tmp_table)))
                    {
                        TBSYS_LOG(WARN, "failed to set child tmp_table, err=%d", ret);
                    }
                }
                //add 20150407:e
                ObEmptyRowFilter * empty_row_filter = NULL;
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    CREATE_PHY_OPERRATOR(empty_row_filter, ObEmptyRowFilter, inner_plan, err_stat);
                    if (OB_UNLIKELY(OB_SUCCESS != ret))
                    {
                    }
                    else if ((ret = empty_row_filter->set_child(0, *fuse_op)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Failed to set child");
                    }
                }
                ObInsertDBSemFilter *insert_sem = NULL;
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    CREATE_PHY_OPERRATOR(insert_sem, ObInsertDBSemFilter, inner_plan, err_stat);
                    if (OB_UNLIKELY(OB_SUCCESS != ret))
                    {
                    }
                    else if ((ret = insert_sem->set_child(0, *empty_row_filter)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Failed to set child, err=%d", ret);
                    }
                    else
                    {
                        inc_scan->set_values(input_values->get_id(), true);
                        insert_sem->set_input_values(input_values->get_id());
                    }
                }
                //add fanqiushi_index
                ObIndexTrigger *index_trigger = NULL;
                if(OB_LIKELY(OB_SUCCESS == ret)&&is_need_modify_index)
                {
                    CREATE_PHY_OPERRATOR(index_trigger, ObIndexTrigger, inner_plan, err_stat);
                    if (OB_UNLIKELY(OB_SUCCESS != ret))
                    {
                    }
                    else if ((ret = index_trigger->set_child(0, *insert_sem)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Failed to set child, err=%d", ret);
                    }
                    else if(OB_SUCCESS != (ret = sql_context_->schema_manager_->get_all_modifiable_index_num(insert_stmt->get_table_id(), index_num)))
                    {

                    }
                    else
                    {
                        //inc_scan->set_values(input_values->get_id(), true);
                        //insert_sem->set_input_values(input_values->get_id());
                        index_trigger->set_i64_values(insert_stmt->get_table_id(),index_num,0);
                    }
                }
                //add:e
                ObWhenFilter *when_filter_op = NULL;
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    if (insert_stmt->get_when_expr_size() > 0)  //fanqiushi_index:insert with when_expr is not considered yet
                    {
                        if ((ret = gen_phy_when(logical_plan,
                                                inner_plan,
                                                err_stat,
                                                query_id,
                                                *insert_sem,
                                                when_filter_op
                                                )) != OB_SUCCESS)
                        {
                        }
                        else if ((ret = ups_modify->set_child(0, *when_filter_op)) != OB_SUCCESS)
                        {
                            TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
                        }
                    }
                    //modify by fanqiushi_index
                    else
                    {
                        if(is_need_modify_index)
                        {
                            if (OB_SUCCESS != (ret = ups_modify->set_child(0, *index_trigger)))
                            {
                                TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
                            }
                        }
                        else
                        {
                            if (OB_SUCCESS != (ret = ups_modify->set_child(0, *insert_sem)))
                            {
                                TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
                            }
                        }
                    }
                    //modify :e
                }
            }
        }
        else
        {
            // @todo insert ... select ...
            //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20141028:b
            /*Exp:do some prepare work*/
            uint64_t tid = insert_stmt->get_table_id();
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                const ObTableSchema *table_schema = NULL;
                if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(tid)))
                {
                    ret = OB_ERR_ILLEGAL_ID;
                    TRANS_LOG("fail to get table schema for table[%ld]", tid);
                }
                else if (row_desc.get_idx(tid, table_schema->get_create_time_column_id()) != OB_INVALID_INDEX)
                {
                    ret = OB_ERR_CREAT_MODIFY_TIME_COLUMN;
                    ColumnItem *column_item = insert_stmt->get_column_item_by_id(tid, table_schema->get_create_time_column_id());
                    if (column_item != NULL)
                        TRANS_LOG("Column '%.*s' of type ObCreateTimeType can not be inserted",
                                  column_item->column_name_.length(), column_item->column_name_.ptr());
                    else
                        TRANS_LOG("Column '%ld' of type ObCreateTimeType can not be inserted",
                                  table_schema->get_create_time_column_id());
                }
                else if (row_desc.get_idx(tid, table_schema->get_modify_time_column_id()) != OB_INVALID_INDEX)
                {
                    ret = OB_ERR_CREAT_MODIFY_TIME_COLUMN;
                    ColumnItem *column_item = insert_stmt->get_column_item_by_id(tid, table_schema->get_modify_time_column_id());
                    if (column_item != NULL)
                        TRANS_LOG("Column '%.*s' of type ObModifyTimeType can not be inserted",
                                  column_item->column_name_.length(), column_item->column_name_.ptr());
                    else
                        TRANS_LOG("Column '%ld' of type ObModifyTimeType can not be inserted",
                                  table_schema->get_modify_time_column_id());
                }
            }
            //add 20141028:e

            //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20141028:b
            /*Exp:build physical plan operators which execute in the MS,including ObBindValues,sub_query,ObValues,
          ObTableRpcScan,ObExprValues
        */
            //transformer sunquery select and get select's operator
            int32_t sub_query_index = OB_INVALID_INDEX;
            ObPhyOperator * sub_query_operator = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                if(OB_SUCCESS != (ret = gen_physical_select(logical_plan, physical_plan, err_stat, insert_stmt->get_insert_query_id(), &sub_query_index)))
                {
                    TRANS_LOG("generating physical plan for sub-query error, err=%d", ret);
                }
                else if(OB_INVALID_INDEX == sub_query_index)
                {
                    TBSYS_LOG(ERROR, "generating physical plan for sub-query error");
                    ret = OB_ERROR;
                }
                else if(NULL == (sub_query_operator = physical_plan->get_phy_query(sub_query_index)))
                {
                    ret = OB_INVALID_INDEX;
                    TRANS_LOG("wrong get sub query operator");
                }
            }
            //trans end
            //generate ObTableRpcScan operator and init
            ObTableRpcScan *table_scan = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                ObRpcScanHint hint;
                hint.read_method_ = ObSqlReadStrategy::USE_GET;
                hint.is_get_skip_empty_row_ = false;
                hint.read_consistency_ = FROZEN;
                const ObTableSchema *table_schema = NULL;
                int64_t table_id = insert_stmt->get_table_id();
                CREATE_PHY_OPERRATOR(table_scan, ObTableRpcScan, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if (OB_SUCCESS != (ret = table_scan->set_table(table_id, table_id)))
                {
                    TRANS_LOG("failed to set table id, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = table_scan->init(sql_context_, &hint)))
                {
                    TRANS_LOG("failed to init table scan, err=%d", ret);
                }
                //        else if (OB_SUCCESS != (ret = gen_phy_static_data_scan_for_sub_query(logical_plan, err_stat,
                //                                      insert_stmt, row_desc,
                //                                      table_id, *rowkey_info, *table_scan)))
                //        {
                //          TRANS_LOG("err=%d", ret);
                //        }
                else if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
                {
                    ret = OB_ERR_ILLEGAL_ID;
                    TRANS_LOG("Fail to get table schema for table[%ld]", table_id);
                }
                else if ((ret = physical_plan->add_base_table_version(
                              table_id,
                              table_schema->get_schema_version()
                              )) != OB_SUCCESS)
                {
                    TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", table_id, ret);
                }
                else
                {
                    table_scan->set_rowkey_cell_count(row_desc.get_rowkey_cell_count());
                    table_scan->set_cache_bloom_filter(true);
                }
            }

            //generate ObValues operator and set ObTableRpcScan operator as its children
            ObValues *tmp_table = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(tmp_table, ObValues, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if (OB_SUCCESS != (ret = tmp_table->set_child(0, *table_scan)))
                {
                    TBSYS_LOG(WARN, "failed to set child, err=%d", ret);
                }
            }

            //add wenghaixing[decimal] for fix delete bug 2014/10/10
            if (OB_SUCCESS == ret)
            {
                for(int64_t key_col_num=0;OB_SUCCESS == ret && key_col_num<rowkey_info->get_size();key_col_num++)
                {
                    uint64_t key_cid=OB_INVALID_ID;
                    if(OB_SUCCESS!=(ret = rowkey_info->get_column_id(key_col_num,key_cid)))
                    {
                        TRANS_LOG("cannot get rowkey id for get param values");
                        break;
                    }
                    else
                    {
                        ObObjType cond_val_type;
                        uint32_t cond_val_precision;
                        uint32_t cond_val_scale;
                        if(OB_SUCCESS!=sql_context_->schema_manager_->get_cond_val_info(tid,key_cid,cond_val_type,cond_val_precision,cond_val_scale))
                        {
                            TRANS_LOG("get rowkey schema failed!");
                            break;
                        }
                        else
                        {
                            tmp_table->add_rowkey_array(tid,key_cid,cond_val_type,cond_val_precision,cond_val_scale);
                        }
                    }
                }
            }
            //add e

            //generate ObExprValues operator
            ObExprValues *input_values = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(input_values, ObExprValues, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if ((ret = input_values->set_row_desc(row_desc, row_desc_ext)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set descriptor of value operator failed, err=%d", ret);
                }
                else
                {
                    input_values->set_from_sub_query();
                    input_values->set_check_rowkey_duplicate(true);
                }
            }

            //generate ObBindValues operator and set sub_query,ObValues,ObExprValues,ObTableRpcScan as its children
            ObBindValues * bind_values = NULL;
            if(OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(bind_values, ObBindValues, inner_plan, err_stat);
                if (OB_SUCCESS != ret)//add qianzm [null operator unjudgement bug1181] 20160520
                {
                }
                else if ((ret = bind_values->set_row_desc(row_desc)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set descriptor of value operator failed, err=%d", ret);
                }
                else if ((ret = bind_values->set_row_desc_map(row_desc_map)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set descriptor of row map info failed, err=%d", ret);
                }
                else if ((ret = bind_values->set_rowkey_info(*rowkey_info)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set rowkey info failed, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(bind_values)))
                {
                    TBSYS_LOG(WARN, "failed to add phy query, err=%d", ret);
                }
                else if((ret = bind_values->set_child(0, *sub_query_operator)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set child of bind_values operator failed, err=%d", ret);
                }
                else if((ret = bind_values->set_child(1, *input_values)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set child of bind_values operator failed, err=%d", ret);
                }
                else if((ret = bind_values->set_child(2, *tmp_table)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set child of bind_values operator failed, err=%d", ret);
                }
                else if((ret = bind_values->set_child(3, *table_scan)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set child of bind_values operator failed, err=%d", ret);
                }
                else
                {
                    // add by maosy [Insert_Subquery_Function]  20161109
                    ObSelectStmt* select_stmt = logical_plan->get_select_query (insert_stmt->get_insert_query_id ());
                    if(select_stmt !=NULL && select_stmt->get_query_hint ().change_value_size_)
                    {
                        int64_t max_row_size = static_cast<int64_t>(0.8*1024L*1024L);
                        bind_values->set_max_row_value_size (max_row_size);
                    }
                    // add by e
                    bind_values->set_table_id(insert_stmt->get_table_id());
                    physical_plan->set_insert_select_flag(1);
                    ColumnItem col_item_for_bind_val;
                    for (int32_t index = 0; index < insert_stmt->get_column_size(); index++)
                    {
                        const ColumnItem *col_item = insert_stmt->get_column_item(index);
                        col_item_for_bind_val.column_id_ = col_item->column_id_;
                        col_item_for_bind_val.table_id_ = col_item->table_id_;
                        col_item_for_bind_val.data_type_ = col_item->data_type_;
                        if(OB_SUCCESS != (ret = bind_values->add_column_item(col_item_for_bind_val)))
                        {
                            TRANS_LOG("fail to add column item to ob_fill_values");
                        }
                    }
                }
            }
            //add 20141028:e

            //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20141028:b
            /*Exp:build physical plan operators which execute in the UPS,including ObMemSSTableScan,ObIncScan,ObMultipleGetMerge,
          ObExprValues,ObEmptyRowFilter,ObInsertDBSemFilter
        */
            //generate ObMemSSTableScan operator
            ObMemSSTableScan *static_data = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(static_data, ObMemSSTableScan, inner_plan, err_stat);
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    static_data->set_tmp_table(tmp_table->get_id());
                }
            }

            //generate ObIncScan operator
            ObIncScan *inc_scan = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(inc_scan, ObIncScan, inner_plan, err_stat);
                if (OB_LIKELY(OB_SUCCESS == ret))
                {
                    inc_scan->set_scan_type(ObIncScan::ST_MGET);
                    inc_scan->set_write_lock_flag();
                }
            }

            //generate ObMultipleGetMerge operator
            ObMultipleGetMerge *fuse_op = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(fuse_op, ObMultipleGetMerge, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if ((ret = fuse_op->set_child(0, *static_data)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set child of fuse_op operator failed, err=%d", ret);
                }
                else if ((ret = fuse_op->set_child(1, *inc_scan)) != OB_SUCCESS)
                {
                    TRANS_LOG("Set child of fuse_op operator failed, err=%d", ret);
                }
                else
                {
                    fuse_op->set_is_ups_row(false);
                }
            }

            //generate ObEmptyRowFilter operator
            ObEmptyRowFilter * empty_row_filter = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(empty_row_filter, ObEmptyRowFilter, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if ((ret = empty_row_filter->set_child(0, *fuse_op)) != OB_SUCCESS)
                {
                    TRANS_LOG("Failed to set child");
                }
            }

            //generate ObInsertDBSemFilter operator
            ObInsertDBSemFilter *insert_sem = NULL;
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                CREATE_PHY_OPERRATOR(insert_sem, ObInsertDBSemFilter, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if ((ret = insert_sem->set_child(0, *empty_row_filter)) != OB_SUCCESS)
                {
                    TRANS_LOG("Failed to set child, err=%d", ret);
                }
                else
                {
                    inc_scan->set_values(input_values->get_id(), true);
                    insert_sem->set_input_values(input_values->get_id());
                }
            }
            //add fanqiushi_index
            ObIndexTrigger *index_trigger = NULL;
            if(OB_LIKELY(OB_SUCCESS == ret)&&is_need_modify_index)
            {
                CREATE_PHY_OPERRATOR(index_trigger, ObIndexTrigger, inner_plan, err_stat);
                if (OB_UNLIKELY(OB_SUCCESS != ret))
                {
                }
                else if ((ret = index_trigger->set_child(0, *insert_sem)) != OB_SUCCESS)
                {
                    TRANS_LOG("Failed to set child, err=%d", ret);
                }
                else if(OB_SUCCESS != (ret = sql_context_->schema_manager_->get_all_modifiable_index_num(insert_stmt->get_table_id(), index_num)))
                {

                }
                else
                {
                    //inc_scan->set_values(input_values->get_id(), true);
                    //insert_sem->set_input_values(input_values->get_id());
                    index_trigger->set_i64_values(insert_stmt->get_table_id(),index_num,0);
                }
            }
            //add:e

            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                //modify by fanqiushi_index
                //        if (OB_SUCCESS != (ret = ups_modify->set_child(0, *insert_sem)))
                //        {
                //          TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
                //        }

                if(is_need_modify_index)
                {
                    if (OB_SUCCESS != (ret = ups_modify->set_child(0, *index_trigger)))
                    {
                        TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
                    }
                }
                else
                {
                    if (OB_SUCCESS != (ret = ups_modify->set_child(0, *insert_sem)))
                    {
                        TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
                    }
                }
                //modify:e
            }

            //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507:b
            if(OB_SUCCESS==ret)
            {
                // add by maosy [Delete_Update_Function_isolation_RC] 20161218
//                if(insert_stmt->get_is_insert_multi_batch())
                if(is_insert_select_)
        // add by maosy e
                {
                    bind_values->set_is_multi_batch(true);
                }
            }
            //add gaojt 20150507:e
            //add 20141028:e
        }
    }
    if (ret == OB_SUCCESS)
    {
        if ((ret = merge_tables_version(*physical_plan, *inner_plan)) != OB_SUCCESS)
        {
            TRANS_LOG("Failed to add base tables version, err=%d", ret);
        }
    }
    return ret;
}

int ObTransformer::gen_phy_table_for_update(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan*& physical_plan,
        ErrStat& err_stat,
        ObStmt *stmt,
        uint64_t table_id,
        const ObRowkeyInfo &rowkey_info,
        const ObRowDesc &row_desc,
        const ObRowDescExt &row_desc_ext,
        ObPhyOperator*& table_op
        , ObPhyOperator *sequence_op//add liuzy [sequence select for update] 20150918:
        )
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    TableItem* table_item = NULL;
    ObTableRpcScan *table_rpc_scan_op = NULL;
    ObFilter *filter_op = NULL;
    ObIncScan *inc_scan_op = NULL;
    ObMultipleGetMerge *fuse_op = NULL;
    ObMemSSTableScan *static_data = NULL;
    ObValues *tmp_table = NULL;
    ObRowDesc rowkey_col_map;
    ObExprValues* get_param_values = NULL;
    const ObTableSchema *table_schema = NULL;
    ObObj rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER]; // used for constructing GetParam
    ObPostfixExpression::ObPostExprNodeType type_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
    ModuleArena rowkey_alloc(OB_MAX_VARCHAR_LENGTH, ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
    ObCellInfo cell_info;
    cell_info.table_id_ = table_id;
    cell_info.row_key_.assign(rowkey_objs, rowkey_info.get_size());

    bool has_other_cond = false;
    ObRpcScanHint hint;
    hint.read_method_ = ObSqlReadStrategy::USE_GET;
    hint.read_consistency_ = FROZEN;
    hint.is_get_skip_empty_row_ = false;
    //add liuzy [sequence select for update] 20150918:b
    ObSelectStmt *select_stmt = NULL;
    ObSequenceSelect *sequence_select_op = NULL;
    //add lijianqiang [sequence_fix] 20151030:b
    ObSequence * base_sequence_op = NULL;
    base_sequence_op = dynamic_cast<ObSequence *> (sequence_op);
    //add 20151030:e
    if (NULL != sequence_op
            && ObBasicStmt::T_SELECT == stmt->get_stmt_type())
    {
        sequence_select_op = dynamic_cast<ObSequenceSelect *>(sequence_op);
        select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
    }
    if (NULL != select_stmt
            && NULL != sequence_select_op
            && select_stmt->select_clause_has_sequence())
    {
        hint.read_consistency_ = STRONG;
    }
    //add 20150918:e
    //add lbzhong [Update rowkey] 20160320:b
    bool is_update_rowkey = get_is_update_rowkey(stmt, &rowkey_info);
    bool is_update_sequence = false;
    ObUpdateDBSemFilter *update_sem = NULL;
    ObArray<ObSqlExpression*> filter_array;
    if(ObBasicStmt::T_UPDATE == stmt->get_stmt_type())
    {
      is_update_sequence = (dynamic_cast<ObUpdateStmt*>(stmt))->has_sequence();
    }
    //add:e
    if (table_id == OB_INVALID_ID
            || (table_item = stmt->get_table_item_by_id(table_id)) == NULL
            || TableItem::BASE_TABLE != table_item->type_)
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("Wrong table id, tid=%lu", table_id);
    }
    else if (NULL == CREATE_PHY_OPERRATOR(table_rpc_scan_op, ObTableRpcScan, physical_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else if ((ret = table_rpc_scan_op->set_table(table_item->table_id_, table_item->ref_id_)) != OB_SUCCESS)
    {
        TRANS_LOG("ObTableRpcScan set table failed");
    }
    else if (OB_SUCCESS != (ret = table_rpc_scan_op->init(sql_context_, &hint)))
    {
        TRANS_LOG("ObTableRpcScan init failed");
    }
    else if (NULL == CREATE_PHY_OPERRATOR(tmp_table, ObValues, physical_plan, err_stat))
    {
    }
    else if (OB_SUCCESS != (ret = tmp_table->set_child(0, *table_rpc_scan_op)))
    {
        TBSYS_LOG(WARN, "failed to set child op, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = physical_plan->add_phy_query(tmp_table)))
    {
        TBSYS_LOG(WARN, "failed to add sub query, err=%d", ret);
    }
    //add liuzy [sequence select for update] 20150922:b
    else if (NULL != sequence_select_op
             && select_stmt->select_clause_has_sequence()
             && OB_SUCCESS != (ret = physical_plan->add_phy_query(sequence_select_op)))
    {
        TBSYS_LOG(WARN, "failed to add sub query, err=%d", ret);
    }
    //add 20150922:e
    else if (NULL == CREATE_PHY_OPERRATOR(filter_op, ObFilter, physical_plan, err_stat))
    {
    }
    else if (NULL == CREATE_PHY_OPERRATOR(inc_scan_op, ObIncScan, physical_plan, err_stat))
    {
    }
    else if (NULL == CREATE_PHY_OPERRATOR(fuse_op, ObMultipleGetMerge, physical_plan, err_stat))
    {
    }
    else if (NULL == CREATE_PHY_OPERRATOR(static_data, ObMemSSTableScan, physical_plan, err_stat))
    {
    }
    else if (OB_SUCCESS != (ret = fuse_op->set_child(0, *static_data)))
    {
    }
    else if (OB_SUCCESS != (ret = fuse_op->set_child(1, *inc_scan_op)))
    {
    }
    else if (NULL == CREATE_PHY_OPERRATOR(get_param_values, ObExprValues, physical_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else if (OB_SUCCESS != (ret = physical_plan->add_phy_query(get_param_values)))
    {
        TBSYS_LOG(WARN, "failed to add sub query, err=%d", ret);
    }
    else if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("Fail to get table schema for table[%ld]", table_id);
    }
    else if ((ret = physical_plan->add_base_table_version(
                  table_id,
                  table_schema->get_schema_version()
                  )) != OB_SUCCESS)
    {
        TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", table_id, ret);
    }
    else
    {
        fuse_op->set_is_ups_row(false);

        inc_scan_op->set_scan_type(ObIncScan::ST_MGET);
        inc_scan_op->set_write_lock_flag();
        inc_scan_op->set_hotspot(stmt->get_query_hint().hotspot_);
        inc_scan_op->set_values(get_param_values->get_id(), false);

        static_data->set_tmp_table(tmp_table->get_id());
        //add liuzy [sequence select for update] 20150922:b
        if (NULL != sequence_select_op
                && select_stmt->select_clause_has_sequence())
        {
            sequence_select_op->set_phy_plan(physical_plan);
            sequence_select_op->set_tmp_table(tmp_table->get_id());
        }
        //add 20150922:e
        table_rpc_scan_op->set_rowkey_cell_count(row_desc.get_rowkey_cell_count());
        table_rpc_scan_op->set_need_cache_frozen_data(true);

        get_param_values->set_row_desc(row_desc, row_desc_ext);
        // set filters
        int32_t num = stmt->get_condition_size();
        uint64_t cid = OB_INVALID_ID;
        int64_t cond_op = T_INVALID;
        ObObj cond_val;
        ObPostfixExpression::ObPostExprNodeType val_type = ObPostfixExpression::BEGIN_TYPE;
        int64_t rowkey_idx = OB_INVALID_INDEX;
        ObRowkeyColumn rowkey_col;
        //add wenghaixing[decimal] for fix delete bug 2014/10/10
        uint64_t tid= table_schema->get_table_id();
        //add e
        //add liuzy [sequence select for update] 20150918:b
        if (NULL != sequence_select_op)
        {
            sequence_select_op->reset_sequence_names_idx(select_stmt->get_column_has_sequene_count());
        }
        //add 20150918:e
        for (int32_t i = 0; i < num; i++)
        {
            ObSqlRawExpr *cnd_expr = logical_plan->get_expr(stmt->get_condition_id(i));
            OB_ASSERT(cnd_expr);
            cnd_expr->set_applied(true);
            ObSqlExpression *filter = ObSqlExpression::alloc();
            if (NULL == filter)
            {
                TRANS_LOG("no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
                break;
            }
            else if (OB_SUCCESS != (ret = cnd_expr->fill_sql_expression(*filter, this, logical_plan, physical_plan)))
            {
                ObSqlExpression::free(filter);
                TRANS_LOG("Failed to fill expression, err=%d", ret);
                break;
            }
            //add liuzy [sequence select for update] 20150918:b
            else if (NULL != base_sequence_op
                     && base_sequence_op->can_fill_sequence_info()
                     && base_sequence_op->is_sequence_cond_id(stmt->get_condition_id(i))
                     && OB_SUCCESS == ret
                     && (OB_SUCCESS != (ret = base_sequence_op->fill_the_sequence_info_to_cond_expr(filter, OB_INVALID_ID))))
            {
                TRANS_LOG("Failed deal the sequence condition filter,err=%d",ret);
                break;
            }
            else if (OB_SUCCESS != ret)
            {
                break;
            }
            //add 20150918:e
            else if (filter->is_simple_condition(false, cid, cond_op, cond_val, &val_type)
                     && (T_OP_EQ == cond_op || T_OP_IS == cond_op)
                     && rowkey_info.is_rowkey_column(cid))
            {
                if (
                    //add lbzhong [Update rowkey] 20160414
                    !is_update_rowkey &&
                    //add:e
                    OB_SUCCESS != (ret = table_rpc_scan_op->add_filter(filter)))
                {
                    ObSqlExpression::free(filter);
                    TRANS_LOG("Failed to add filter, err=%d", ret);
                    break;
                }
                else if (OB_SUCCESS != (ret = rowkey_col_map.add_column_desc(OB_INVALID_ID, cid)))
                {
                    TRANS_LOG("Failed to add column desc, err=%d", ret);
                    break;
                }
                else if (OB_SUCCESS != (ret = rowkey_info.get_index(cid, rowkey_idx, rowkey_col)))
                {
                    TRANS_LOG("Unexpected branch");
                    ret = OB_ERR_UNEXPECTED;
                    break;
                }
                //add wenghaixing[decimal] for fix delete bug 2014/10/10
                else{

                    ObObjType cond_val_type;
                    uint32_t cond_val_precision;
                    uint32_t cond_val_scale;
                    ObObj static_obj;
                    if(OB_SUCCESS!=sql_context_->schema_manager_->get_cond_val_info(tid,cid,cond_val_type,cond_val_precision,cond_val_scale))
                    {

                    }
                    else{
                        tmp_table->add_rowkey_array(tid,cid,cond_val_type,cond_val_precision,cond_val_scale);
                        if(ObDecimalType==cond_val_type){
                            static_obj.set_precision(cond_val_precision);
                            static_obj.set_scale(cond_val_scale);
                            static_obj.set_type(cond_val_type);
                        }
                    }

                    //add e
                    //modify wenghaixing[decimal] for fix delete bug 2014/10/10
                    //else if (OB_SUCCESS != (ret = ob_write_obj(rowkey_alloc, cond_val, rowkey_objs[rowkey_idx]))) // deep copy
                    if (OB_SUCCESS != (ret = ob_write_obj_for_delete(rowkey_alloc, cond_val, rowkey_objs[rowkey_idx],static_obj))) // deep copy
                        //modify e
                    {
                        TRANS_LOG("failed to copy cell, err=%d", ret);
                    }

                    else
                    {
                        type_objs[rowkey_idx] = val_type;
                        TBSYS_LOG(DEBUG, "rowkey obj, i=%ld val=%s", rowkey_idx, to_cstring(cond_val));
                    }
                }
            }
            else
            {
                // other condition
                has_other_cond = true;
                // TBSYS_LOG(ERROR,"test:whx row_desc-p1[%s]",to_cstring(row_desc));
                if (OB_SUCCESS != (ret = filter_op->add_filter(filter)))
                {
                    TRANS_LOG("Failed to add filter, err=%d", ret);
                    break;
                }
            }
            //mod bingo [Update rowkey bugfix] 20161218:b
            //add lbzhong [Update rowkey] 20160505:b
            /*if(is_update_rowkey && OB_SUCCESS != (ret = filter_array.push_back(filter)))
            {
              ObSqlExpression::free(filter);
              TRANS_LOG("Failed to push filter to filter array, err=%d", ret);
              break;
            }*/
            if(is_update_rowkey){
              ObSqlExpression* expr_clone = ObSqlExpression::alloc();
              if (NULL == expr_clone)
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(WARN, "no memory");
              }
              else
              {
                *expr_clone = *filter;
              }
              if(OB_SUCCESS == ret && OB_SUCCESS != (ret = filter_array.push_back(expr_clone)))
              {
                ObSqlExpression::free(expr_clone);
                TRANS_LOG("Failed to push filter to filter array, err=%d", ret);
                break;
              }
            }
            //add:e
            //mod:e
        } // end for
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            int64_t rowkey_col_num = rowkey_info.get_size();
            uint64_t cid = OB_INVALID_ID;
            for (int64_t i = 0; i < rowkey_col_num; ++i)
            {
                if (OB_SUCCESS != (ret = rowkey_info.get_column_id(i, cid)))
                {
                    TRANS_LOG("Failed to get column id, err=%d", ret);
                    break;
                }
                else if (OB_INVALID_INDEX == rowkey_col_map.get_idx(OB_INVALID_ID, cid))
                {
                    TRANS_LOG("Primary key column %lu not specified in the WHERE clause", cid);
                    ret = OB_ERR_LACK_OF_ROWKEY_COL;
                    break;
                }
            } // end for
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        // add output columns
        int32_t num = stmt->get_column_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            const ColumnItem *col_item = stmt->get_column_item(i);
            if (col_item && col_item->table_id_ == table_item->table_id_)
            {
                //add lbzhong [Update rowkey] 20160320:b
                if(!is_update_rowkey)
                {
                //add:e
                  ObBinaryRefRawExpr col_expr(col_item->table_id_, col_item->column_id_, T_REF_COLUMN);
                  ObSqlRawExpr col_raw_expr(
                              common::OB_INVALID_ID,
                              col_item->table_id_,
                              col_item->column_id_,
                              &col_expr);
                  ObSqlExpression output_expr;
                  if ((ret = col_raw_expr.fill_sql_expression(
                           output_expr,
                           this,
                           logical_plan,
                           physical_plan)) != OB_SUCCESS
                          || (ret = table_rpc_scan_op->add_output_column(output_expr)) != OB_SUCCESS)
                  {
                      TRANS_LOG("Add table output columns faild");
                      break;
                  }
                } //add lbzhong [Update rowkey] 20160320:b:e
                // for IncScan
                ObConstRawExpr col_expr2;
                if (i < rowkey_info.get_size()) // rowkey column
                {
                    if (OB_SUCCESS != (ret = col_expr2.set_value_and_type(rowkey_objs[i])))
                    {
                        TBSYS_LOG(WARN, "failed to set value, err=%d", ret);
                        break;
                    }
                    else
                    {
                        switch (type_objs[i])
                        {
                        case ObPostfixExpression::PARAM_IDX:
                            col_expr2.set_expr_type(T_QUESTIONMARK);
                            col_expr2.set_result_type(ObVarcharType);
                            break;
                        case ObPostfixExpression::SYSTEM_VAR:
                            col_expr2.set_expr_type(T_SYSTEM_VARIABLE);
                            col_expr2.set_result_type(ObVarcharType);
                            break;
                        case ObPostfixExpression::TEMP_VAR:
                            col_expr2.set_expr_type(T_TEMP_VARIABLE);
                            col_expr2.set_result_type(ObVarcharType);
                            break;
                        default:
                            break;
                        }
                    }
                }
                //mod lbzhong [Update rowkey] 20160320:b
                //else
                else if(!is_update_rowkey)
                //mod:e
                {
                    ObObj null_obj;
                    col_expr2.set_value_and_type(null_obj);
                }

                //add lbzhong [Update rowkey] 20160320:b
                if(!is_update_rowkey || (is_update_rowkey && i < rowkey_info.get_size()))
                {
                //add:e
                  ObSqlRawExpr col_raw_expr2(
                              common::OB_INVALID_ID,
                              col_item->table_id_,
                              col_item->column_id_,
                              &col_expr2);
                  ObSqlExpression output_expr2;
                  if ((ret = col_raw_expr2.fill_sql_expression(
                           output_expr2,
                           this,
                           logical_plan,
                           physical_plan)) != OB_SUCCESS)
                  {
                      TRANS_LOG("Add table output columns failed");
                      break;
                  }
                  else if (OB_SUCCESS != (ret = get_param_values->add_value(output_expr2)))
                  {
                      TRANS_LOG("Failed to add cell into get param, err=%d", ret);
                      break;
                  }
                  //add wenghaixing[decimal] for fix delete bug 2014/10/10
                  else{
                      get_param_values->set_del_upd();
                  }
                  //add e
                }//add lbzhong [Update rowkey] 200320:b:e //end if
            }
        } // end for
        //add lbzhong [Update rowkey] 20160320:b
        if(is_update_rowkey && OB_SUCCESS == ret)
        {
          //add NULL value to get_param_values
          if(OB_SUCCESS != (ret = (add_empty_value_for_nonrowkey_column(get_param_values, rowkey_info, row_desc))))
          {
            TRANS_LOG("fail to add empty value for nonrowkey column to get_param_values");
          }
          else if(OB_SUCCESS != (ret = add_full_output_columns(table_rpc_scan_op, table_id,
                                                              logical_plan, physical_plan, row_desc, err_stat)))
          {
            TRANS_LOG("fail to add full output column to table_rpc_scan_op");
          }
        }
        //add:e
    }
    // add action flag column
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        ObSqlExpression column_ref;
        column_ref.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
        if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, column_ref)))
        {
            TBSYS_LOG(WARN, "fail to make column expr:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = table_rpc_scan_op->add_output_column(column_ref)))
        {
            TBSYS_LOG(WARN, "failed to add output column, err=%d", ret);
        }
    }
    //add lbzhong [Update rowkey] 20160504:b
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      ObProject *project_for_sequence = NULL;
      if (NULL != sequence_op && is_update_sequence)
      {
        ObSequenceUpdate *sequence_update_op = dynamic_cast<ObSequenceUpdate *>(sequence_op);

        //add lijianqiang [sequence] 20150909:b
        /*Exp:hanlde the set clause of UPDATE stmt*/
        //add lbzhong [Update rowkey] 20160111:b
        ObArray<uint64_t> update_columns;
        stmt->get_update_columns(update_columns);
        int64_t nonrowkey_count = row_desc.get_column_num() - rowkey_info.get_size();
        for(int64_t i = 0; i < update_columns.count(); i++)
        {
          if(!rowkey_info.is_rowkey_column(update_columns.at(i)))
          {
            nonrowkey_count--;
          }
        }
        //add:e
        //sequence_update_op->set_index_trigger_update(index_trigger_upd, is_column_hint_index);
        int row_num = 1;
        if (OB_SUCCESS != (ret = sequence_update_op->handle_the_set_clause_of_seuqence(row_num
                                     //add lbzhong [Update rowkey] 20160107:b
                                     , nonrowkey_count, is_update_rowkey
                                     //add:e
                                     )))
        {
          TRANS_LOG("handle the set clause of sequence failed,ret=%d",ret);
        }
        //add 2050909:e
        project_for_sequence = sequence_update_op->get_project_for_update();
      }
      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        if(is_update_rowkey && OB_SUCCESS != (ret = cons_in_filter_and_expr_values(logical_plan, physical_plan,
                                     table_rpc_scan_op, fuse_op, table_id,
                                     &row_desc, &rowkey_info, stmt, project_for_sequence, err_stat, filter_array,
                                     update_sem, get_param_values)))
        {
            TRANS_LOG("Failed to add cons_in_filter, err=%d", ret);
        }
      }
    }
    //add:e
    if (ret == OB_SUCCESS)
    {
        if (has_other_cond)
        {
          //add lbzhong [Update rowkey] 20160414
          if(!is_update_rowkey)
          {
          //add:e
            if (OB_SUCCESS != (ret = filter_op->set_child(0, *fuse_op)))
            {
                TRANS_LOG("Failed to set child, err=%d", ret);
            }
            else
            {
                table_op = filter_op;
            }
          //add lbzhong [Update rowkey] 20160414
          }
          else
          {
            if (OB_SUCCESS != (ret = filter_op->set_child(0, *update_sem)))
            {
                TRANS_LOG("Failed to set child, err=%d", ret);
            }
            else
            {
                table_op = filter_op;
            }
          }
          //add:e
        }
        else
        {
          //add lbzhong [Update rowkey] 20160414
          if(!is_update_rowkey)
          {
          //add:e
            table_op = fuse_op;
          //add lbzhong [Update rowkey] 20160414
          }
          else
          {
            table_op = update_sem;
          }
          //add:e
        }
        //add wenghaixing[decimal] for fix delete bug 2014/10/10
        tmp_table->set_fix_obvalues();
        //add e
    }
    return ret;
}
//add wenghaixing [secondary index replace bug_fix]20150517
int ObTransformer::cons_row_desc_with_index(const uint64_t table_id,
                                            const ObStmt *stmt,
                                            ObRowDescExt &row_desc_ext,
                                            ObRowDesc &row_desc,
                                            const ObRowkeyInfo *&rowkey_info,
                                            //common::ObSEArray<int64_t, 64> &row_desc_map,
                                            ErrStat &err_stat)
{
    OB_ASSERT(sql_context_);
    OB_ASSERT(sql_context_->schema_manager_);
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    const ObTableSchema *table_schema = NULL;
    if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("fail to get table schema for table[%ld]", table_id);
    }
    else
    {
        rowkey_info = &table_schema->get_rowkey_info();
        int64_t rowkey_col_num = rowkey_info->get_size();
        row_desc.set_rowkey_cell_count(rowkey_col_num);

        int32_t column_num = stmt->get_column_size();
        uint64_t max_column_id = table_schema->get_max_column_id();
        const ColumnItem* column_item = NULL;
        //row_desc_map.clear();
        //row_desc_map.reserve(column_num);
        ObObj data_type;
        // construct rowkey columns first
        //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
        // construct rowkey's precision and scale
        const ObColumnSchemaV2* column_schema_for_rowkey = NULL;
        //add:e
        // construct rowkey columns first
        for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_col_num; ++i) // for each primary key
        {
            const ObRowkeyColumn *rowkey_column = rowkey_info->get_column(i);
            OB_ASSERT(rowkey_column);
            // find it's index in the input columns
            for (int32_t j = 0; ret == OB_SUCCESS && j < column_num; ++j)
            {
                column_item = stmt->get_column_item(j);
                OB_ASSERT(column_item);
                OB_ASSERT(table_id == column_item->table_id_);
                if (rowkey_column->column_id_ == column_item->column_id_)
                {
                    if (OB_SUCCESS != (ret = row_desc.add_column_desc(column_item->table_id_,
                                                                      column_item->column_id_)))
                    {
                        TRANS_LOG("failed to add row desc, err=%d", ret);
                    }
                    else
                    {
                        //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
                        if (NULL == (column_schema_for_rowkey = sql_context_->schema_manager_->get_column_schema(
                                         column_item->table_id_, column_item->column_id_)))
                        {
                            ret = OB_ERR_COLUMN_NOT_FOUND;
                            TRANS_LOG("Get column item failed");
                            break;
                        }
                        data_type.set_precision(column_schema_for_rowkey->get_precision());
                        data_type.set_scale(column_schema_for_rowkey->get_scale());

                        //add e
                        data_type.set_type(rowkey_column->type_);
                        if (OB_SUCCESS != (ret = row_desc_ext.add_column_desc(column_item->table_id_,
                                                                              column_item->column_id_, data_type)))
                        {
                            TRANS_LOG("failed to add row desc, err=%d", ret);
                        }
                    }
                    break;
                }
            } // end for
        }   // end for
        // then construct other columns
        const ObColumnSchemaV2* column_schema = NULL;
        for (uint64_t cid = OB_APP_MIN_COLUMN_ID; ret == OB_SUCCESS && cid <= max_column_id; ++cid)
        {
            bool hit_index = false;
            bool in_stmt = false;
            if (OB_SUCCESS != (ret = sql_context_->schema_manager_->column_hint_index(table_id, cid, hit_index)))
            {
                //ret = OB_ERROR;
                TBSYS_LOG(WARN, "column hint index failed,ret = %d, table_id = %ld, cid = %ld",ret, table_id, cid);
                break;
            }
            else if(OB_SUCCESS != (ret = is_column_in_stmt(table_id, cid, stmt, in_stmt)))
            {
                TBSYS_LOG(WARN, "is coloumn in stmt failed,ret = %d, table_id = %ld, cid = %ld", ret, table_id, cid);
                break;
            }
            if (!rowkey_info->is_rowkey_column(cid)&&(hit_index || in_stmt))
            {
                if (NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(
                                 table_id, cid)))
                {
                    ret = OB_ERR_COLUMN_NOT_FOUND;
                    TRANS_LOG("Get column item failed");
                    break;
                }
                else if (OB_SUCCESS != (ret = row_desc.add_column_desc(table_id,
                                                                       cid)))
                {
                    TRANS_LOG("failed to add row desc, err=%d", ret);
                }
                else
                {
                    data_type.set_type(column_schema->get_type());
                    //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
                    /*
           *该部分代码是将column_schema里面的信息存到data_type里面，最后把data_type里面的信息写到row_desc_ext里面 //repaired from messy code by zhuxh 20151014
           *
           *column_schema是用户定义的表的schema，data_type是中间量，row_desc_ext会在后面做类型转化的时候用到
           *
           *我增加的代码的原理是将column_schema里面存的precision和scale也存到data_type里面
           */
                    data_type.set_precision(column_schema->get_precision());
                    data_type.set_scale(column_schema->get_scale());
                    //add:e
                    if (OB_SUCCESS != (ret = row_desc_ext.add_column_desc(table_id,
                                                                          cid, data_type)))
                    {
                        TRANS_LOG("failed to add row desc, err=%d", ret);
                    }
                }
            } // end if not rowkey column
        }   // end for
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            TBSYS_LOG(DEBUG, "row_desc=%s ", to_cstring(row_desc));
        }
    }
    return ret;
}

int ObTransformer::is_column_in_stmt(const uint64_t table_id, const uint64_t column_id, const ObStmt *stmt, bool &in_stmt)
{
    int ret = OB_SUCCESS;
    in_stmt = false;
    if(OB_UNLIKELY(NULL == stmt))
    {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "stmt pointer cannot be NULL");
    }
    else
    {
        int32_t column_num = stmt->get_column_size();
        const ColumnItem *column_item = NULL;
        for(int32_t i =0; i < column_num; i++)
        {
            if(OB_UNLIKELY(NULL == (column_item = stmt->get_column_item(i))))
            {
                ret = OB_INVALID_ARGUMENT;
                TBSYS_LOG(WARN, "stmt pointer cannot be NULL, i = %d, ret = %d", i, ret);
                break;
            }
            else if(table_id != column_item->table_id_)
            {
                ret = OB_INVALID_ARGUMENT;
                TBSYS_LOG(WARN, "table_id is not equal item,table_id[%ld], column_item_tid[%ld]",table_id, column_item->table_id_);
                break;
            }
            else if(column_id == column_item->column_id_)
            {
                in_stmt = true;
                break;
            }
        }
    }
    return ret;
}
//add e
int ObTransformer::cons_row_desc(const uint64_t table_id,
                                 const ObStmt *stmt,
                                 ObRowDescExt &row_desc_ext,
                                 ObRowDesc &row_desc,
                                 const ObRowkeyInfo *&rowkey_info,
                                 ObSEArray<int64_t, 64> &row_desc_map,
                                 ErrStat& err_stat
                                 //add lbzhong[Update rowkey] 20151221:b
                                 , const bool is_update
                                 //add:e
                                 )
{
    OB_ASSERT(sql_context_);
    OB_ASSERT(sql_context_->schema_manager_);
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    const ObTableSchema *table_schema = NULL;
    if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("fail to get table schema for table[%ld]", table_id);
    }
    else
    {
        rowkey_info = &table_schema->get_rowkey_info();
        int64_t rowkey_col_num = rowkey_info->get_size();
        row_desc.set_rowkey_cell_count(rowkey_col_num);

        int32_t column_num = stmt->get_column_size();
        const ColumnItem* column_item = NULL;
        row_desc_map.clear();
        row_desc_map.reserve(column_num);
        ObObj data_type;
        // construct rowkey columns first
        //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
        // construct rowkey's precision and scale
        const ObColumnSchemaV2* column_schema_for_rowkey = NULL;
        //add:e
        // construct rowkey columns first
        for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_col_num; ++i) // for each primary key
        {
            const ObRowkeyColumn *rowkey_column = rowkey_info->get_column(i);
            OB_ASSERT(rowkey_column);
            // find it's index in the input columns
            for (int32_t j = 0; ret == OB_SUCCESS && j < column_num; ++j)
            {
                column_item = stmt->get_column_item(j);
                OB_ASSERT(column_item);
                OB_ASSERT(table_id == column_item->table_id_);
                if (rowkey_column->column_id_ == column_item->column_id_)
                {
                    if (OB_SUCCESS != (ret = row_desc_map.push_back(j)))
                    {
                        TRANS_LOG("failed to add index map, err=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = row_desc.add_column_desc(column_item->table_id_,
                                                                           column_item->column_id_)))
                    {
                        TRANS_LOG("failed to add row desc, err=%d", ret);
                    }
                    else
                    {
                        //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
                        if (NULL == (column_schema_for_rowkey = sql_context_->schema_manager_->get_column_schema(
                                         column_item->table_id_, column_item->column_id_)))
                        {
                            ret = OB_ERR_COLUMN_NOT_FOUND;
                            TRANS_LOG("Get column item failed");
                            break;
                        }
                        data_type.set_precision(column_schema_for_rowkey->get_precision());
                        data_type.set_scale(column_schema_for_rowkey->get_scale());

                        //add e
                        data_type.set_type(rowkey_column->type_);
                        if (OB_SUCCESS != (ret = row_desc_ext.add_column_desc(column_item->table_id_,
                                                                              column_item->column_id_, data_type)))
                        {
                            TRANS_LOG("failed to add row desc, err=%d", ret);
                        }
                    }
                    break;
                }
            } // end for
        }   // end for
        // then construct other columns
        //add lbzhong[Update rowkey] 20151221:b
        bool is_update_rowkey = false;
        if(is_update)
        {
            is_update_rowkey = get_is_update_rowkey(stmt, rowkey_info);
        }

        if(!is_update_rowkey) // update as usual
        {
        //add:e
            const ObColumnSchemaV2* column_schema = NULL;
            for (int32_t i = 0; ret == OB_SUCCESS && i < column_num; ++i)
            {
                column_item = stmt->get_column_item(i);
                OB_ASSERT(column_item);
                OB_ASSERT(table_id == column_item->table_id_);
                if (!rowkey_info->is_rowkey_column(column_item->column_id_))
                {
                    if (NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(
                                     column_item->table_id_, column_item->column_id_)))
                    {
                        ret = OB_ERR_COLUMN_NOT_FOUND;
                        TRANS_LOG("Get column item failed");
                        break;
                    }
                    else if (OB_SUCCESS != (ret = row_desc_map.push_back(i)))
                    {
                        TRANS_LOG("failed to add index map, err=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = row_desc.add_column_desc(column_item->table_id_,
                                                                           column_item->column_id_)))
                    {
                        TRANS_LOG("failed to add row desc, err=%d", ret);
                    }
                    else
                    {
                        data_type.set_type(column_schema->get_type());
                        //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
                        /*
             *该部分代码是将column_schema里面的信息存到data_type里面，最后把data_type里面的信息写到row_desc_ext里面
             *
             *column_schema是用户定义的表的schema，data_type是中间量，row_desc_ext会在后面做类型转化的时候用到
             *
             *我增加的代码的原理是将column_schema里面存的precision和scale也存到data_type里面
             */
                        data_type.set_precision(column_schema->get_precision());
                        data_type.set_scale(column_schema->get_scale());
                        //add:e
                        if (OB_SUCCESS != (ret = row_desc_ext.add_column_desc(column_item->table_id_,
                                                                              column_item->column_id_, data_type)))
                        {
                            TRANS_LOG("failed to add row desc, err=%d", ret);
                        }
                    }
                } // end if not rowkey column
            }   // end for
        }
        else // cons whole row desc
        {
            uint64_t max_column_id = table_schema->get_max_column_id();
            const ObColumnSchemaV2* column_schema = NULL;
            for (int64_t id = OB_APP_MIN_COLUMN_ID, i = 0; id <= (int64_t)max_column_id;  id++, i++)
            {
                if(NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(table_id, id)))
                {
                    TBSYS_LOG(WARN,"get column schema error!");
                    ret = OB_SCHEMA_ERROR;
                }
                else if(!rowkey_info->is_rowkey_column(id))
                {
                    if (OB_SUCCESS != (ret = row_desc_map.push_back(i)))
                    {
                        TRANS_LOG("failed to add index map, err=%d", ret);
                    }
                    if(OB_SUCCESS != (ret = row_desc.add_column_desc(table_id, id)))
                    {
                        TBSYS_LOG(WARN,"failed to add column desc!");
                        ret = OB_ERROR;
                    }
                    else
                    {
                        data_type.set_type(column_schema->get_type());
                        data_type.set_precision(column_schema->get_precision());
                        data_type.set_scale(column_schema->get_scale());
                        if(OB_SUCCESS != (ret = row_desc_ext.add_column_desc(column_schema->get_table_id(),
                                                                             column_schema->get_id(),data_type)))
                        {
                            TBSYS_LOG(WARN,"failed to add column desc_ext!");
                            ret = OB_ERROR;
                            break;
                        }
                    }
                }
            }
        }
        //add:e

        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            TBSYS_LOG(DEBUG, "row_desc=%s map_count=%ld", to_cstring(row_desc), row_desc_map.count());
        }
    }
    return ret;
}

int ObTransformer::gen_physical_update_new(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan*& physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObUpdateStmt *update_stmt = NULL;
    ObUpsModifyWithDmlType *ups_modify = NULL;
    ObProject *project_op = NULL;
    uint64_t table_id = OB_INVALID_ID;
    const ObRowkeyInfo *rowkey_info = NULL;
    ObRowDesc row_desc;
    ObRowDescExt row_desc_ext;
    ObSEArray<int64_t, 64> row_desc_map;
    ObPhysicalPlan* inner_plan = NULL;
    //add wenghaixing [secondary index upd] 20141126
    bool is_column_hint_index=false;
    IndexList out;
    //add e
  //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302:b
  is_delete_update_ = true;
  if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, update_stmt)))
  {
  }
  else if(update_stmt->get_is_update_multi_batch())
  {
      TBSYS_LOG(DEBUG,"This is multi-batch in update statement");
      is_multi_batch_ = true;
  }
  if(OB_SUCCESS != ret)
  {

  }
  //add gaojt 20160302:e
  //mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160302:b
  else if (OB_SUCCESS != (ret = wrap_ups_executor(physical_plan, query_id, inner_plan, index, err_stat)))
  {
    TBSYS_LOG(WARN, "err=%d", ret);
  }
//  else if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, update_stmt)))
//  {
//  }
  //mod gaojt 20160302:e
    /* generate root operator */
    else if (NULL == CREATE_PHY_OPERRATOR(ups_modify, ObUpsModifyWithDmlType, inner_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator");
    }
    else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(
                                ups_modify,
                                physical_plan == inner_plan ? index : NULL,
                                physical_plan != inner_plan)))
    {
        TRANS_LOG("Add ups_modify operator failed");
    }
    else if (NULL == CREATE_PHY_OPERRATOR(project_op, ObProject, inner_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator");
    }
    else if (OB_SUCCESS != (ret = cons_row_desc(update_stmt->get_update_table_id(), update_stmt,
                                                row_desc_ext, row_desc, rowkey_info, row_desc_map, err_stat
                                                //add lbzhong[Update rowkey] 20151221:b
                                                , true //is_update
                                                //add:e
                                                )))
    {
    }
    else
    {
        table_id = update_stmt->get_update_table_id();
        ups_modify->set_dml_type(OB_DML_UPDATE);
    }
    //add lbzhong [Update rowkey] 20160520:b
    bool is_update_rowkey = get_is_update_rowkey(update_stmt, rowkey_info);
    if(OB_SUCCESS == ret && is_update_rowkey &&
       OB_SUCCESS != (ret = is_rowkey_duplicated(update_stmt, rowkey_info)))
    {
      TBSYS_LOG(USER_ERROR, "OB-145: Duplicated column");
    }
    //add:e
    ObWhenFilter *when_filter_op = NULL;
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        if (update_stmt->get_when_expr_size() > 0)
        {
            if ((ret = gen_phy_when(logical_plan,
                                    inner_plan,
                                    err_stat,
                                    query_id,
                                    *project_op,
                                    when_filter_op
                                    )) != OB_SUCCESS)
            {
            }
            else if ((ret = ups_modify->set_child(0, *when_filter_op)) != OB_SUCCESS)
            {
                TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
            }
        }
        //delete wenghaixing [secondary index upd] 20141127
        /*else if (OB_SUCCESS != (ret = ups_modify->set_child(0, *project_op)))
    {
      TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
    }
    */
        //delete e
    }
    ObSqlExpression expr;
    // fill rowkey columns into the Project op
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        //add lbzhong[Update rowkey] 20151221:b
        int64_t rowkey_count = row_desc.get_rowkey_cell_count();
        int64_t total_count = row_desc.get_column_num();
        if(OB_UNLIKELY(is_update_rowkey))
        {
          for (int64_t i = 0; i < total_count; ++i)
          {
            if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, cid)))
            {
              TRANS_LOG("Failed to get tid cid");
              break;
            }
            else if(i < rowkey_count || (i >= rowkey_count && !update_stmt->exist_update_column(cid)))
            {
              ObBinaryRefRawExpr col_raw_ref(tid, cid, T_REF_COLUMN);
              expr.reset();
              expr.set_tid_cid(tid, cid);
              ObSqlRawExpr col_ref(0, tid, cid, &col_raw_ref);
              if (OB_SUCCESS != (ret = col_ref.fill_sql_expression(expr, this, logical_plan, inner_plan)))
              {
                TRANS_LOG("Failed to fill expression, err=%d", ret);
                break;
              }
              else if (OB_SUCCESS != (ret = project_op->add_output_column(expr)))
              {
                TRANS_LOG("Failed to add output column");
                break;
              }
            }
          }
          project_op->set_rowkey_cell_count(rowkey_count);
        }
        else
        {
        //add:e
          for (int64_t i = 0; i < row_desc.get_rowkey_cell_count(); ++i)
          {
            if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, cid)))
            {
              TRANS_LOG("Failed to get tid cid");
              break;
            }
            else
            {
              ObBinaryRefRawExpr col_raw_ref(tid, cid, T_REF_COLUMN);
              expr.reset();
              expr.set_tid_cid(tid, cid);
              ObSqlRawExpr col_ref(0, tid, cid, &col_raw_ref);
              if (OB_SUCCESS != (ret = col_ref.fill_sql_expression(expr, this, logical_plan, inner_plan)))
              {
                TRANS_LOG("Failed to fill expression, err=%d", ret);
                break;
              }
              else if (OB_SUCCESS != (ret = project_op->add_output_column(expr)))
              {
                TRANS_LOG("Failed to add output column");
                break;
              }
            }
          }
        } // add lbzhong [Update rowkey] 20160511:b:e //end if
    }
    /* check and fill set column=expr pairs */
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        ObSqlRawExpr *raw_expr = NULL;
        uint64_t column_id = OB_INVALID_ID;
        uint64_t expr_id = OB_INVALID_ID;
        const ObColumnSchemaV2* column_schema = NULL;
        //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
        int64_t para_count = 0;
        //add 20131222:e
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160520:b
        if (ObBasicStmt::T_PREPARE == logical_plan->get_main_stmt()->get_stmt_type())
        {
            expr.set_is_update(true);
        }
        //add gaojt 20160520:e

        for (int64_t column_idx = 0; column_idx < update_stmt->get_update_column_count(); column_idx++)
        {
            expr.reset();
            // valid check
            // 1. rowkey can't be updated
            // 2. joined column can't be updated
            if (OB_SUCCESS != (ret = update_stmt->get_update_column_id(column_idx, column_id)))
            {
                TRANS_LOG("fail to get update column id for table %lu column_idx=%lu", table_id, column_idx);
                break;
            }
            else if (NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(table_id, column_id)))
            {
                ret = OB_ERR_COLUMN_NOT_FOUND;
                TRANS_LOG("Get column item failed");
                break;
            }
            else if (true == column_schema->is_join_column())
            {
                ret = OB_ERR_UPDATE_JOIN_COLUMN;
                TRANS_LOG("join column '%s' can not be updated", column_schema->get_name());
                break;
            }
            //mod lijianqiang [set_row_key_ignore] 20151019:b
            /*Exp: for SUPPLY CHAIN: update clause set KEY with the same value int where clause
             e.g. UPDATE TEST SET KEY_COL = 1, CO2 = 3 WHERE KEY_COL = 1*/
            // else if (rowkey_info->is_rowkey_column(column_id))
            // {
            // ret = OB_ERR_UPDATE_ROWKEY_COLUMN;
            // TRANS_LOG("rowkey column '%s' can not be updated", column_schema->get_name());
            // break;
            // }
            //del lbzhong[Update rowkey] 20151221:b
            /*
      else if (rowkey_info->is_rowkey_column(column_id))
      {
        if (can_ignore_current_key(logical_plan, physical_plan, column_idx, table_id, column_id, update_stmt))
        {
          TBSYS_LOG(INFO, "current row key can be ignored");
          continue;
        }
        else
        {
          ret = OB_ERR_UPDATE_ROWKEY_COLUMN;
          TRANS_LOG("rowkey column '%s' can not be updated", column_schema->get_name());
          break;
        }
      }
      */
            //del:e
            //mod 20151019:e
            else if (column_schema->get_type() == ObCreateTimeType || column_schema->get_type() == ObModifyTimeType)
            {
                ret = OB_ERR_CREAT_MODIFY_TIME_COLUMN;
                TRANS_LOG("Column '%s' of type ObCreateTimeType/ObModifyTimeType can not be updated", column_schema->get_name());
                break;
            }
            // get expression
            else if (OB_SUCCESS != (ret = update_stmt->get_update_expr_id(column_idx, expr_id)))
            {
                TBSYS_LOG(WARN, "fail to get update expr for table %lu column %lu. column_idx=%ld", table_id, column_id, column_idx);
                break;
            }
            else if (NULL == (raw_expr = logical_plan->get_expr(expr_id)))
            {
                TBSYS_LOG(WARN, "fail to get expr from logical plan for table %lu column %lu. column_idx=%ld", table_id, column_id, column_idx);
                ret = OB_ERR_UNEXPECTED;
                break;
            }
            //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
            else if (!column_schema->is_nullable() && (ObNullType == raw_expr->get_expr()->get_result_type()))
            {
                TRANS_LOG("column: %s can not be set NULL", column_schema->get_name());
                ret = OB_ERR_UPDATE_NULL_COLUMN;
                break;
            }
            //add 20131222:e
            else if (OB_SUCCESS != (ret = raw_expr->fill_sql_expression(expr, this, logical_plan, inner_plan)))
            {
                TBSYS_LOG(WARN, "fail to fill sql expression. ret=%d", ret);
                break;
            }
            else
            {
                expr.set_tid_cid(table_id, column_id);
                // add <column_id, expression> to project operator
                if (OB_SUCCESS != (ret = project_op->add_output_column(expr)))
                {
                    TRANS_LOG("fail to add update expr to update operator");
                    break;
                }
                //add lbzhong [Update rowkey] 20160519:b
                else if(rowkey_info->is_rowkey_column(column_id) &&
                        OB_SUCCESS != (ret = project_op->add_rowkey_index()))
                {
                  TRANS_LOG("Failed to add rowkey index");
                  break;
                }
                //add:e
                //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
                else if((OB_SUCCESS == ret) && (T_QUESTIONMARK == raw_expr->get_expr()->get_expr_type()))
                {
                    if (para_count >= result_->get_params().count())
                    {
                        para_count -= para_count;
                    }
                    bool is_null = column_schema->is_nullable();
                    if (OB_SUCCESS != (ret = result_->set_params_constraint(para_count, is_null)))
                    {
                        TRANS_LOG("Fail to save the constraint of  column %s!", column_schema->get_name());
                        break;
                    }

                    para_count++;
                }
                //add 20131222:e
            }

            //add gaojt [Delete_Update_Function] [JHOBv0.1] 201605219:b
            /*
             * For every filled ObSqlExpression, we compute the number of
             * questionmarks in every ObSqlExpression that is involved in
             * update-assign-list
             */
            if(OB_SUCCESS == ret)
            {
                questionmark_num_in_update_assign_list_ += expr.get_question_num();
                TBSYS_LOG(DEBUG,"questionmark_num_in_update_assign_list_=%ld",questionmark_num_in_update_assign_list_);
                expr.reset_question_num();
            }
            //add gaojt 201605219:e
            /*del liumz, [allow update expire condition]20161121
            //add wenghaixing [secondary index expire info]20141229
            if(OB_LIKELY(OB_SUCCESS == ret))
            {
                if(sql_context_->schema_manager_->is_modify_expire_condition(table_id,column_id))
                {
                    int32_t num = update_stmt->get_condition_size();
                    uint64_t cid = OB_INVALID_ID;
                    int64_t cond_op = OB_INVALID_ID;
                    ObObj cond_val;
                    ObPostfixExpression::ObPostExprNodeType val_type;
                    ObString  table_name;
                    // add zhangcd [multi_database.bugfix] 20150801:b
                    ObString db_name;
                    bool table_name_found = false;
                    bool db_name_found = false;
                    // add:e
                    uint64_t expire_table_id = OB_INVALID_ID;
                    for(int32_t i = 0;i<num && OB_SUCCESS == ret;i++)
                    {
                        // mod zhangcd [multi_database.bugfix] 20150801:b
                        //            ObSqlRawExpr *cnd_expr = logical_plan->get_expr(update_stmt->get_condition_id(i));
                        //            OB_ASSERT(cnd_expr);
                        //            cnd_expr->set_applied(true);
                        //            ObSqlExpression *filter = ObSqlExpression::alloc();
                        //            if (NULL == filter)
                        //            {
                        //              TRANS_LOG("no memory");
                        //              ret = OB_ALLOCATE_MEMORY_FAILED;
                        //              break;
                        //            }
                        //            else if (OB_SUCCESS != (ret = cnd_expr->fill_sql_expression(*filter, this, logical_plan, physical_plan)))
                        //            {
                        //              ObSqlExpression::free(filter);
                        //              TRANS_LOG("Failed to fill expression, err=%d", ret);
                        //              break;
                        //            }
                        //            else if (filter->is_simple_condition(false, cid, cond_op, cond_val, &val_type)
                        //                       && (T_OP_EQ == cond_op || T_OP_IS == cond_op)
                        //                       //modif liuxiao [secondary index bug fixed] 20150722
                        //                       //&& rowkey_info->is_rowkey_column(cid))
                        //                       && rowkey_info->is_rowkey_column(cid) && cid == 16)
                        //                       //modify e
                        //            {
                        //              table_name.reset();
                        //              if(OB_SUCCESS != (ret = cond_val.get_varchar(table_name)))
                        //              {
                        //                TBSYS_LOG(WARN,"get table name from update sql!ret[%d]",ret);
                        //              }
                        //              else if(NULL == sql_context_->schema_manager_->get_table_schema(table_name))
                        //              {
                        //                TBSYS_LOG(WARN,"failed to get schema from expire info sql");
                        //                ret = OB_SCHEMA_ERROR;
                        //              }
                        //              else
                        //              {
                        //                expire_table_id=sql_context_->schema_manager_->get_table_schema(table_name)->get_table_id();
                        //                break;
                        //              }
                        //             }
                        ObSqlRawExpr *cnd_expr = logical_plan->get_expr(update_stmt->get_condition_id(i));
                        OB_ASSERT(cnd_expr);
                        cnd_expr->set_applied(true);
                        ObSqlExpression *filter = ObSqlExpression::alloc();
                        if (NULL == filter)
                        {
                            TRANS_LOG("no memory");
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            break;
                        }
                        else if (OB_SUCCESS != (ret = cnd_expr->fill_sql_expression(*filter, this, logical_plan, physical_plan)))
                        {
                            ObSqlExpression::free(filter);
                            TRANS_LOG("Failed to fill expression, err=%d", ret);
                            break;
                        }
                        else if (filter->is_simple_condition(false, cid, cond_op, cond_val, &val_type)
                                 && (T_OP_EQ == cond_op || T_OP_IS == cond_op)
                                 && rowkey_info->is_rowkey_column(cid) && (cid == first_tablet_entry_cid::TNAME || cid == first_tablet_entry_cid::DBNAME))
                        {
                            if(cid == first_tablet_entry_cid::TNAME)
                            {
                                if(!table_name_found)
                                {
                                    table_name.reset();
                                    if(OB_SUCCESS != (ret = cond_val.get_varchar(table_name)))
                                    {
                                        TRANS_LOG("get table name from update sql!ret[%d]",ret);
                                    }
                                    table_name_found = true;
                                }
                                else
                                {
                                    TRANS_LOG("duplicated table_names are specified!");
                                    ret = OB_ERROR;
                                }
                            }
                            else if(cid == first_tablet_entry_cid::DBNAME)
                            {
                                if(!db_name_found)
                                {
                                    db_name.reset();
                                    if(OB_SUCCESS != (ret = cond_val.get_varchar(db_name)))
                                    {
                                        TRANS_LOG("get db name from update sql!ret[%d]",ret);
                                    }
                                    db_name_found = true;
                                }
                                else
                                {
                                    TRANS_LOG("duplicated db_names are specified!");
                                    ret = OB_ERROR;
                                }
                            }
                        }
                    }
                    if(OB_SUCCESS == ret)
                    {
                        if(table_name_found && db_name_found)
                        {
                            char complete_table_name_buffer[OB_MAX_COMPLETE_TABLE_NAME_LENGTH];
                            ObString complete_table_name(OB_MAX_COMPLETE_TABLE_NAME_LENGTH, 0, complete_table_name_buffer);
                            if(db_name.length() + table_name.length() + 1 >= OB_MAX_COMPLETE_TABLE_NAME_LENGTH)
                            {
                                TRANS_LOG("db_name or table_name is too long!");
                                ret = OB_ERROR;
                            }
                            else
                            {
                                complete_table_name.write(db_name.ptr(), db_name.length());
                                complete_table_name.write(".", 1);
                                complete_table_name.write(table_name.ptr(), table_name.length());
                            }

                            if(OB_SUCCESS == ret)
                            {
                                if(NULL == sql_context_->schema_manager_->get_table_schema(complete_table_name))
                                {
                                    TRANS_LOG("table [%.*s] not exist!", complete_table_name.length(), complete_table_name.ptr());
                                    ret = OB_SCHEMA_ERROR;
                                }
                                else
                                {
                                    expire_table_id=sql_context_->schema_manager_->get_table_schema(complete_table_name)->get_table_id();
                                    // del by zhangcd [multi_database.bugfix] 20150805:e
                                    //                  break;
                                    // del:e
                                }
                            }
                        }
                        else if(!table_name_found)
                        {
                            TRANS_LOG("table_name is not specified in sql!");
                            ret = OB_ERROR;
                        }
                        else if(!db_name_found)
                        {
                            TRANS_LOG("db_name is not specified!");
                            ret = OB_ERROR;
                        }
                    }
                    if(OB_SUCCESS == ret)
                    {
                        if(sql_context_->schema_manager_->is_have_modifiable_index(expire_table_id))
                        {
                            TRANS_LOG("can not update expire condition ,because table has index!");
                            ret = OB_ERROR;
                        }
                    }
                    // mod:e
                }
            }
            //add e
            */
            //add wenghaixing secondary index upd 20141127
            if(OB_LIKELY(OB_SUCCESS == ret))
            {
                if(OB_SUCCESS != (ret = sql_context_->schema_manager_->column_hint_index(table_id,column_id,out)))
                {
                    TBSYS_LOG(WARN,"failed to query if column hit index!table_id[%ld],column_id[%ld]",table_id,column_id);
                }
                else if(out.get_count()>0)
                {
                    is_column_hint_index=true;
                }
            }
            //add e
        } // end for
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160520:b
        if (ObBasicStmt::T_PREPARE == logical_plan->get_main_stmt()->get_stmt_type())
        {
            expr.set_is_update(false);
        }
        //add gaojt 20160520:e
    }

    //add lijianqiang [sequence update] 20150909:b
    /*Exp:we use this oprator id to find the get_param_values(ObExprValues) in the paysical plan
    we can get the row number from this physical OP,because if U use sequence NEXTVAL in update set clause,
    we can construct the sequence info U need in the update set caluse with the row number*/
    ObSequenceUpdate * sequence_update_op = NULL;
    bool is_update_single_row_ = false;//if update single row whith rowKey,will be set true,or false
    /*Exp:if has sequence in set clause, we replace the sequence expr with the sequence next/prevv id,
    step1:handle the where clause of UPATE stmt with the prevval if have
    step2:handle the set clause of UPDATE stmt,we construct all sequence id in MS and handle the prevval first
    then we store the NEXTVAL in the PROJECT op,when the PROJECT op opened in UPS, the NEXTVAL will be handled
   */
    if (OB_SUCCESS == ret && update_stmt->has_sequence())
    {
        // 1.ObSequenceUpdate
        if (NULL == CREATE_PHY_OPERRATOR(sequence_update_op, ObSequenceUpdate, inner_plan, err_stat))
        {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TRANS_LOG("Failed to create sequence phy operator");
        }
        else if (OB_SUCCESS != (ret = wrap_sequence(logical_plan, physical_plan, err_stat,
                                                    row_desc_map, sequence_update_op, update_stmt,
                                                    inner_plan)))
        {
            TRANS_LOG("wrap sequence failed");
        }
        else
        {
            //get the sequence info from the "__all_sequences for all prevval and check validity of all sequence"
            if (OB_SUCCESS != (ret = sequence_update_op->prepare_sequence_prevval()))
            {
                TRANS_LOG("prepare sequence info failed!");
            }
            else
            {
                sequence_update_op->init_global_prevval_state_and_lock_sequence_map();
                sequence_update_op->copy_sequence_info_from_update_stmt();
                //must be call before reset row_desc and reset row_desc_ext
                sequence_update_op->set_out_row_desc_and_row_desc_ext(row_desc, row_desc_ext);
                sequence_update_op->set_project_for_update(project_op);//we need to store the sequence info to the project op,we get the point to the project here
            }
        }
    }
    //add 20150909:e

  //add wenghaixing secondary index upd 20141127
  ObIndexTriggerUpd *index_trigger_upd = NULL;
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if(is_column_hint_index)
    {
      if (NULL == CREATE_PHY_OPERRATOR(index_trigger_upd, ObIndexTriggerUpd, inner_plan, err_stat))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator index_trigger_upd");
      }
      else if (OB_SUCCESS!=(ret = index_trigger_upd->set_child(0, *project_op)))
      {
        TRANS_LOG("Failed to set child, err=%d", ret);
      }
    }
  }
  //add lbzhong[Update rowkey] 20151221:b
  ObRowDesc update_rowkey_row_desc = row_desc; //copy the old row_desc
  ObRowDescExt update_rowkey_row_desc_ext = row_desc_ext;
  //add:e
  if(OB_LIKELY(OB_SUCCESS == ret)&&NULL != index_trigger_upd)
  {
    //int64_t set_col_num=update_stmt->get_update_column_count();
    index_trigger_upd->set_update_index_num(out.get_count());
    index_trigger_upd->set_data_max_cid(sql_context_->schema_manager_->get_table_schema(table_id)->get_max_column_id());
    for(int64_t i = 0;i<out.get_count();i++)
    {
      //first we construct update expression in phase 1.
      const ObTableSchema* index_schema = NULL;
      uint64_t index_tid = OB_INVALID_ID;
      uint64_t index_cid = OB_INVALID_ID;
      out.get_idx_id(i,index_tid);
      if(index_tid != OB_INVALID_ID)
      {
        index_schema = sql_context_->schema_manager_->get_table_schema(index_tid);
        if(NULL == index_schema)
        {
          TBSYS_LOG(WARN,"get index schema failed!");
          ret = OB_SCHEMA_ERROR;
          break;
        }
        else
        {
          ObSqlExpression expr;
          const ObRowkeyInfo ori = index_schema->get_rowkey_info();
          ObRowDesc ord_del,ord_upd;
          ord_del.reset();
          ord_upd.reset();
          ord_del.set_rowkey_cell_count(ori.get_size());
          ord_upd.set_rowkey_cell_count(ori.get_size());
          for(int64_t rowkey_index = 0;rowkey_index < ori.get_size();rowkey_index++)
          {
            if(OB_SUCCESS != (ret = ori.get_column_id(rowkey_index,index_cid)))
            {
              TBSYS_LOG(WARN,"get_column_id for rowkey failed!");
              ret = OB_ERROR;
              break;
            }
            else
            {
              if(OB_SUCCESS != (ret = ord_del.add_column_desc(index_tid,index_cid)))
              {
                TBSYS_LOG(WARN,"ord_del.add_column_desc occur an error,ret[%d]",ret);
              }
              else if(OB_SUCCESS != (ret = ord_upd.add_column_desc(index_tid,index_cid)))
              {
                TBSYS_LOG(WARN,"ord_upd.add_column_desc occur an error,ret[%d]",ret);
              }
            }
          }
          if(OB_SUCCESS == ret&&OB_SUCCESS != (ret = ord_del.add_column_desc(index_tid,OB_ACTION_FLAG_COLUMN_ID)))
          {
            TBSYS_LOG(WARN,"ord_upd.add_column_desc occur an error,ret[%d]",ret);
          }
          else
          {
            uint64_t max_column_id = index_schema->get_max_column_id();
            for (int64_t j = OB_APP_MIN_COLUMN_ID; j <= (int64_t)max_column_id;  j++)
            {
              const ObColumnSchemaV2* ocs=sql_context_->schema_manager_->get_column_schema(index_tid,j);
              if(ori.is_rowkey_column(j) || NULL == ocs)
              {}
              else
              {
                index_cid = ocs->get_id();
                if(OB_SUCCESS != (ret = ord_upd.add_column_desc(index_tid,index_cid)))
                {
                  TBSYS_LOG(ERROR,"error in add_column_desc");
                  break;
                }
              }
             }
            if(OB_SUCCESS == ret && sql_context_->schema_manager_->is_index_has_storing(index_tid))
            {
              ret = ord_upd.add_column_desc(index_tid, OB_INDEX_VIRTUAL_COLUMN_ID);
            }
           }
           if(OB_SUCCESS == ret&&(OB_SUCCESS != (ret = index_trigger_upd->add_row_desc_del(i,ord_del))||OB_SUCCESS != (ret = index_trigger_upd->add_row_desc_upd(i,ord_upd))))
           {
             TBSYS_LOG(ERROR,"construct row desc error");
             ret = OB_ERROR;
           }
         }
       }
    }
    if(OB_SUCCESS == ret)
    {
      row_desc.reset();
      row_desc_ext.reset();
      if(OB_SUCCESS != (ret = cons_del_upd_row_desc(update_stmt, table_id,row_desc,row_desc_ext)))
      {
        TBSYS_LOG(ERROR,"cons whole row desc error!");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        //add wenghaixing [secondary index upd.bugfix]20150127
        index_trigger_upd->set_data_row_desc(row_desc);
        //add e
      }
    }
    if(OB_SUCCESS == ret)
    {
      int64_t index_num = 0;
      if(OB_SUCCESS != (ret = sql_context_->schema_manager_->get_all_modifiable_index_num(table_id, index_num)))
      {
        TBSYS_LOG(ERROR,"failed get all modifiable index num,table id=%ld  ret=[%d]",table_id ,ret);
      }
      else
      {
        index_trigger_upd->set_index_num(index_num);
      }
    }
    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = gen_expr_array(update_stmt, index_trigger_upd, row_desc, logical_plan, inner_plan, err_stat)))
      {
        TBSYS_LOG(WARN, "failed to gen expr_array,ret = %d", ret);
      }
    }
  }
  if(OB_LIKELY(OB_SUCCESS == ret)&&update_stmt->get_when_expr_size()<=0)
  {
    if(!is_column_hint_index)
    {
      int64_t index_num = 0;

            if(OB_SUCCESS != (ret = sql_context_->schema_manager_->get_all_modifiable_index_num(table_id, index_num)))
            {
                TRANS_LOG("get all modifiable index_num failed, err=%d", ret);
            }
            else
            {
                project_op->set_index_num(index_num);
                if (OB_SUCCESS != (ret = ups_modify->set_child(0, *project_op)))
                {
                    TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
                }
            }
        }
        else
        {
            if (OB_SUCCESS != (ret = ups_modify->set_child(0, *index_trigger_upd)))
            {
                TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
            }
        }
    }
    //add e

    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        ObPhyOperator* table_op = NULL;
        //modify wenghaixing [seconadry index upd.3] 20141128
        /*if (OB_SUCCESS != (ret = gen_phy_table_for_update(logical_plan, inner_plan, err_stat,
                                                      update_stmt, table_id, *rowkey_info,
                                                      row_desc, row_desc_ext, table_op)))
    {
    }
    else if (OB_SUCCESS != (ret = project_op->set_child(0, *table_op)))
    {
      TRANS_LOG("Failed to set child, err=%d", ret);
    }
    */
        //add lbzhong[Update rowkey] 20151221:b
        if(is_update_rowkey)
        {
            row_desc = update_rowkey_row_desc;
            row_desc_ext = update_rowkey_row_desc_ext;
        }
        //add lijianqiang [sequence] 20150909:b
        if (OB_SUCCESS == ret && update_stmt->has_sequence())
        {
          sequence_update_op->set_index_trigger_update(index_trigger_upd, is_column_hint_index);
        }
        //add 20150909:e
        //add: e

      if(!is_column_hint_index)
      {
          //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160222:b
          /*when the condition satisfys the original request of delete statement,
           *then call the original fucntion
           */
          bool multi_update = false;
          if(OB_SUCCESS != (ret = is_multi_delete_update(logical_plan,inner_plan,*rowkey_info,update_stmt,err_stat,multi_update)))
          {
              TBSYS_LOG(WARN,"fail to run the function: is_multi_delete_update in delete statement");
          }
          if(OB_SUCCESS == ret)
          {
              //add gaojt 20160222:e
              //mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160223:b
  //            if (OB_SUCCESS != (ret = gen_phy_table_for_update(logical_plan, inner_plan, err_stat,
  //                                                                               delete_stmt, table_id, *rowkey_info,
  //                                                                               row_desc, row_desc_ext, table_op,

  //                                                                               sequence_delete
  //                                                                               //add 20150518:e
  //                                                                               )))
  //            {
  //            }
              if(!multi_update)
              {
                 //add lijianqiang [sequence update] 20160316:b
                 is_update_single_row_ = true;
                 //add 20160316:e
                  if (OB_SUCCESS != (ret = gen_phy_table_for_update(logical_plan, inner_plan, err_stat,
                                                                    update_stmt, table_id, *rowkey_info,
                                                                    row_desc, row_desc_ext, table_op,

                                                                    sequence_update_op
                                                                    //add 20150518:e
                                                                    )))
                  {
                      TBSYS_LOG(WARN,"fail to run the function: gen_phy_table_for_update");
                  }
              }
              else
              {
                  if (OB_SUCCESS != (ret = gen_phy_table_for_update_new(logical_plan, inner_plan, err_stat,
                                                                                             update_stmt, table_id, *rowkey_info,
                                                                                             row_desc, row_desc_ext, table_op,true,
                                                                                             //add lijianqiang [sequence] 20150909:b
                                                                                             sequence_update_op,
                                                                                             //add 20150909:e
                                                                                             is_column_hint_index)))

                  {
                      TBSYS_LOG(WARN,"fail to run the function: gen_phy_table_for_update_new");
                  }
                  // del by maosy [Delete_Update_Function_isolation_RC] 20161218
                  //add by maosy [Delete_Update_Function_for_snpshot] 20161210
//                  if(index!=NULL && PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (*index)->get_type())
//                 {
//                     (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (*index)))->set_is_ud (true);
//                 }
//                else if( PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (0)->get_type())
//                 {
//                     (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (0)))->set_is_ud (true);
//                  }

                  // add maosy e
          //del by maosy e
              }
              if(OB_SUCCESS == ret)
              {
                  // add by maosy [Delete_Update_Function_isolation_RC] 20161218
                  bool need_start_trans =false;
                  if(multi_update && !is_multi_batch_)
                  {
                      need_start_trans =true;
                  }
                  if(index!=NULL && PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (*index)->get_type())
                  {
                      (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (*index)))->set_need_start_trans (need_start_trans);
                  }
                  else if( PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (0)->get_type())
                  {
                      (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (0)))->set_need_start_trans (need_start_trans);
                  }
                  // add maosy e

  //                else if (OB_SUCCESS != (ret = project_op->set_child(0, *table_op)))
  //                {
  //                    TRANS_LOG("Failed to set child, err=%d", ret);
  //                }
                  if (OB_SUCCESS != (ret = project_op->set_child(0, *table_op)))
                  {
                      TRANS_LOG("Failed to set child, err=%d", ret);
                  }
              }
          }
          //mod gaojt 20160223:e

      }
      else
      {
          //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160222:b
          /*when the condition satisfys the original request of delete statement,
           *then call the original fucntion
           */
          bool multi_update = false;
          if(OB_SUCCESS != (ret = is_multi_delete_update(logical_plan,inner_plan,*rowkey_info,update_stmt,err_stat,multi_update)))
          {
              TBSYS_LOG(WARN,"fail to run the function: is_multi_delete_update in delete statement");
          }
          TBSYS_LOG(DEBUG,"ob-transformer.cpp-second-index");
          if(OB_SUCCESS == ret)
          {
              //add gaojt 20160222:e
              //mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160223:b
  //            if (OB_SUCCESS != (ret = gen_phy_table_for_update(logical_plan, inner_plan, err_stat,
  //                                                                               delete_stmt, table_id, *rowkey_info,
  //                                                                               row_desc, row_desc_ext, table_op,

  //                                                                               sequence_delete
  //                                                                               //add 20150518:e
  //                                                                               )))
  //            {
  //            }
              if(!multi_update)
              {
                  //add lijianqiang [sequence update] 20160316:b
                  is_update_single_row_ = true;
                  //add 20160316:e
                  if (OB_SUCCESS != (ret = gen_phy_table_for_update_v2(logical_plan, inner_plan, err_stat,
                                                                                     update_stmt, table_id, *rowkey_info,
                                                                                     row_desc, row_desc_ext, table_op
                                                                                     //add lijianqiang [sequence update] 20160316:b
                                                                                     ,sequence_update_op
                                                                                     //add 20160316:e
                                                                                     )))
                  {
                      TBSYS_LOG(WARN,"fail to run the function: gen_phy_table_for_update in update statement");
                  }
              }
              else
              {
                  if (OB_SUCCESS != (ret = gen_phy_table_for_update_new(logical_plan, inner_plan, err_stat,
                                                                                             update_stmt, table_id, *rowkey_info,
                                                                                             row_desc, row_desc_ext, table_op,true,
                                                                                             //add lijianqiang [sequence] 20150909:b
                                                                                             sequence_update_op,
                                                                                             //add 20150909:e
                                                                                             is_column_hint_index)))

                  {
                      TBSYS_LOG(WARN,"fail to run the function: gen_phy_table_for_update_new in update statement");
                  }
                  // del by maosy [Delete_Update_Function_isolation_RC] 20161218
                  //add by maosy [Delete_Update_Function_for_snpshot] 20161210
//                  if(index!=NULL && PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (*index)->get_type())
//                 {
//                     (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (*index)))->set_is_ud (true);
//                 }
//                else if( PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (0)->get_type())
//                 {
//                     (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (0)))->set_is_ud (true);
//                  }

                  //add by maosy e
          //del by maosy
              }
              if(OB_SUCCESS == ret)
              {
                  // add by maosy [Delete_Update_Function_isolation_RC] 20161218
                  bool need_start_trans =false;
                  if(multi_update && !is_multi_batch_)
                  {
                      need_start_trans =true;
                  }
                  if(index!=NULL && PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (*index)->get_type())
                  {
                      (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (*index)))->set_need_start_trans (need_start_trans);
                  }
                  else if( PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (0)->get_type())
                  {
                      (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (0)))->set_need_start_trans (need_start_trans);
                  }
                  // add maosy e

  //                else if (OB_SUCCESS != (ret = project_op->set_child(0, *table_op)))
  //                {
  //                    TRANS_LOG("Failed to set child, err=%d", ret);
  //                }
                  if (OB_SUCCESS != (ret = project_op->set_child(0, *table_op)))
                  {
                      TRANS_LOG("Failed to set child, err=%d", ret);
                  }
              }
          }
          //mod gaojt 20160223:e
          //add wenghaixing [secondary index upd.bugfix]20150127
          if(OB_SUCCESS == ret && is_column_hint_index && PHY_FILTER == table_op->get_type())
          {
              index_trigger_upd->set_cond_bool(true);
          }
          //add e
      }
      //modify e
    }
    //del lbzhong [Update rowkey] 20160509:b
    //add lijianqiang [sequence] 20150909:b
    /*Exp:hanlde the set clause of UPDATE stmt*/
    /*
    if (OB_SUCCESS == ret && update_stmt->has_sequence())
    {
        sequence_update_op->set_index_trigger_update(index_trigger_upd, is_column_hint_index);
        if (is_update_single_row_)
        {
          int row_num = 1;
          if (OB_SUCCESS != (ret = sequence_update_op->handle_the_set_clause_of_seuqence(row_num
                                       //add lbzhong [Update rowkey] 20160107:b
                                       , nonrowkey_count, is_update_rowkey
                                       //add:e
                                                                                         )))
          {
            TRANS_LOG("handle the set clause of sequence failed,ret=%d",ret);
          }
        }
    }
    */
    //del:e
    //add 2050909:e
    if (ret == OB_SUCCESS)
    {
        if ((ret = merge_tables_version(*physical_plan, *inner_plan)) != OB_SUCCESS)
        {
            TRANS_LOG("Failed to add base tables version, err=%d", ret);
        }
    }
    return ret;
}

int ObTransformer::gen_expr_array(ObUpdateStmt *update_stmt, ObIndexTriggerUpd *index_trigger_upd, const ObRowDesc &row_desc,
                                  ObLogicalPlan *logical_plan,ObPhysicalPlan* inner_plan,ErrStat& err_stat)
{
    int ret = OB_SUCCESS;
    ObSqlExpression expr;
    ObObj cast_obj;
    uint64_t tid = OB_INVALID_ID;
    uint64_t cid = OB_INVALID_ID;
    // uint64_t column_id = OB_INVALID_ID;
    const ObColumnSchemaV2 *column_schema = NULL;
    if(NULL == update_stmt || NULL == index_trigger_upd)
    {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "update_stmt cannot be NULL");
    }
    if(OB_SUCCESS == ret)
    {
        for (int64_t i = 0; i < row_desc.get_rowkey_cell_count(); ++i)
        {
            if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, cid)))
            {
                TRANS_LOG("Failed to get tid cid");
                break;
            }
            else if(!update_stmt->exist_update_column(cid)) //add lbzhong[Update rowkey] 20151221:e
            {
                ObBinaryRefRawExpr col_raw_ref(tid, cid, T_REF_COLUMN);
                expr.reset();
                expr.set_tid_cid(tid, cid);
                ObSqlRawExpr col_ref(0, tid, cid, &col_raw_ref);
                if (OB_SUCCESS != (ret = col_ref.fill_sql_expression(expr, this, logical_plan, inner_plan)))
                {
                    TRANS_LOG("Failed to fill expression, err=%d", ret);
                    break;
                }
                else if (OB_SUCCESS != (ret = index_trigger_upd->add_set_column(expr)))
                {
                    TRANS_LOG("Failed to add output column");
                    break;
                }
            }
            if(OB_SUCCESS == ret)
            {
                if (NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(tid, cid)))
                {
                    ret = OB_ERR_COLUMN_NOT_FOUND;
                    TRANS_LOG("Get column item failed");
                    break;
                }
                else if(!update_stmt->exist_update_column(cid)) //add lbzhong[Update rowkey] 20151221:e
                {
                    expr.set_tid_cid(tid, cid);
                    cast_obj.set_type(column_schema->get_type());
                    cast_obj.set_precision(column_schema->get_precision());
                    cast_obj.set_scale(column_schema->get_scale());
                    // add <column_id, expression> to project operator
                    if(OB_SUCCESS != (ret = index_trigger_upd->add_cast_obj(cast_obj)))
                    {
                        TRANS_LOG("fail to add cast obj to update operator, ret[%d]", ret);
                        break;
                    }
                }
            }
        }
    }
    //add lijianqiang [set_row_key_ignore] 20151020:b
    const ObTableSchema *table_schema = NULL;
    const ObRowkeyInfo *row_key_info = NULL;
    if (OB_SUCCESS == ret)
    {
        if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(tid)))
        {
            ret = OB_SCHEMA_ERROR;
            TRANS_LOG("fail to get table schema");
        }
        else if (NULL == (row_key_info = &(table_schema->get_rowkey_info())))
        {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG("fail to get row key info");
        }
    }
    ;
    //add 20151020:e
    if(OB_SUCCESS == ret)
    {
        const ObColumnSchemaV2 *column_schema = NULL;
        uint64_t expr_id = OB_INVALID_ID;
        ObSqlRawExpr *raw_expr = NULL;

        for (int64_t column_idx = 0; column_idx < update_stmt->get_update_column_count(); column_idx++)
        {
            expr.reset();
            uint64_t column_id = OB_INVALID_ID;
            if (OB_SUCCESS != (ret = update_stmt->get_update_column_id(column_idx, column_id)))
            {
                TRANS_LOG("fail to get update column id for table %lu column_idx=%lu", tid, column_idx);
                break;
            }
            //del lbzhong[Update rowkey] 20151221:b
            //add lijianqiang [set_row_key_ignore] 20151020:b
            /*Exp: if is row key here, skip*/
            /*
            else if (row_key_info->is_rowkey_column(column_id))
            {
              continue;
            }
            */
            //add 20151020:e
            //del:e
            else if (NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(tid, column_id)))
            {
                ret = OB_ERR_COLUMN_NOT_FOUND;
                TRANS_LOG("Get column item failed");
                break;
            }
            // get expression
            else if (OB_SUCCESS != (ret = update_stmt->get_update_expr_id(column_idx, expr_id)))
            {
                TBSYS_LOG(WARN, "fail to get update expr for table %lu column %lu. column_idx=%ld", tid, column_id, column_idx);
                break;
            }
            else if (NULL == (raw_expr = logical_plan->get_expr(expr_id)))
            {
                TBSYS_LOG(WARN, "fail to get expr from logical plan for table %lu column %lu. column_idx=%ld", tid, column_id, column_idx);
                ret = OB_ERR_UNEXPECTED;
                break;
            }
            else if (OB_SUCCESS != (ret = raw_expr->fill_sql_expression(expr, this, logical_plan, inner_plan)))
            {
                TBSYS_LOG(WARN, "fail to fill sql expression. ret=%d", ret);
                break;
            }
            else if(NULL != index_trigger_upd)
            {
                expr.set_tid_cid(tid, column_id);
                cast_obj.set_type(column_schema->get_type());
                cast_obj.set_precision(column_schema->get_precision());
                cast_obj.set_scale(column_schema->get_scale());
                // add <column_id, expression> to project operator
                if (OB_SUCCESS != (ret = index_trigger_upd->add_set_column(expr)))
                {
                    TRANS_LOG("fail to add update expr to update operator,ret [%d]", ret);
                    break;
                }
                else if(OB_SUCCESS != (ret = index_trigger_upd->add_cast_obj(cast_obj)))
                {
                    TRANS_LOG("fail to add cast obj to update operator, ret[%d]", ret);
                    break;
                }
            }
        }
    }
    return ret;
}

int ObTransformer::gen_physical_delete_new(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan* physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    OB_ASSERT(logical_plan);
    OB_ASSERT(physical_plan);
    ObDeleteStmt *delete_stmt = NULL;
    ObUpsModifyWithDmlType *ups_modify = NULL;
    ObProject *project_op = NULL;
    uint64_t table_id = OB_INVALID_ID;
    const ObRowkeyInfo *rowkey_info = NULL;
    ObRowDesc row_desc;
    ObRowDescExt row_desc_ext;
    ObSEArray<int64_t, 64> row_desc_map;
    ObPhysicalPlan* inner_plan = NULL;
    //add liumengzhan_delete_index
    ObDeleteIndex *delete_index_op = NULL;
    bool is_need_modify_index = false;
    uint64_t index_tid_array[OB_MAX_INDEX_NUMS];
    uint64_t index_array_count = 0;
    //add:e
  //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302:b
      is_delete_update_ = true;
  if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, delete_stmt)))
  {
  }
  else if(delete_stmt->get_is_detete_multi_batch())
  {
      TBSYS_LOG(DEBUG,"is multi-delete_update");
      is_multi_batch_ = true;

  }
  if(OB_SUCCESS != ret)
  {

  }
  //add gaojt 20160302:e
  //mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160302:b
  else if (OB_SUCCESS != (ret = wrap_ups_executor(physical_plan, query_id, inner_plan, index, err_stat)))
  {
    TBSYS_LOG(WARN, "err=%d", ret);
  }
//  else if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, update_stmt)))
//  {
//  }
  //mod gaojt 20160302:e
    /* generate root operator */
    else if (NULL == CREATE_PHY_OPERRATOR(ups_modify, ObUpsModifyWithDmlType, inner_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator");
    }
    else if (OB_SUCCESS != (ret = inner_plan->add_phy_query(
                                ups_modify,
                                physical_plan == inner_plan ? index : NULL,
                                physical_plan != inner_plan)))
    {
        TRANS_LOG("Add ups_modify operator failed");
    }
    else if (NULL == CREATE_PHY_OPERRATOR(project_op, ObProject, inner_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator");
    }

    else if (OB_SUCCESS != (ret = cons_row_desc(delete_stmt->get_delete_table_id(), delete_stmt,
                                                row_desc_ext, row_desc, rowkey_info, row_desc_map, err_stat)))
    {
    }
    else
    {
        table_id = delete_stmt->get_delete_table_id();
        ups_modify->set_dml_type(OB_DML_DELETE);
    }
    //add liumengzhan_delete_index
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        is_need_modify_index = sql_context_->schema_manager_->is_have_modifiable_index(table_id);
    }
    //add:e
    ObWhenFilter *when_filter_op = NULL;
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        if (delete_stmt->get_when_expr_size() > 0)
        {
            if ((ret = gen_phy_when(logical_plan,
                                    inner_plan,
                                    err_stat,
                                    query_id,
                                    *project_op,
                                    when_filter_op
                                    )) != OB_SUCCESS)
            {
            }
            else if ((ret = ups_modify->set_child(0, *when_filter_op)) != OB_SUCCESS)
            {
                TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
            }
        }
        //add liumengzhan_delete_index
        else if (is_need_modify_index)
        {
            if (NULL == CREATE_PHY_OPERRATOR(delete_index_op, ObDeleteIndex, inner_plan, err_stat))
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TRANS_LOG("Failed to create phy operator: ObDeleteIndex");
            }
            else if (OB_SUCCESS != (ret = ups_modify->set_child(0, *delete_index_op)))
            {
                TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = delete_index_op->set_child(0, *project_op)))
            {
                TRANS_LOG("Set child of ObDeleteIndex operator failed, err=%d", ret);
            }
        }
        //add:e
        else if (OB_SUCCESS != (ret = ups_modify->set_child(0, *project_op)))
        {
            TRANS_LOG("Set child of ups_modify operator failed, err=%d", ret);
        }
    }
    ObSqlExpression expr;
    // fill rowkey columns into the Project op
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        for (int64_t i = 0; i < row_desc.get_rowkey_cell_count(); ++i)
        {
            if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, cid)))
            {
                TRANS_LOG("Failed to get tid cid");
                break;
            }
            else
            {
                ObBinaryRefRawExpr col_raw_ref(tid, cid, T_REF_COLUMN);
                expr.reset();
                ObSqlRawExpr col_ref(OB_INVALID_ID, tid, cid, &col_raw_ref);
                if (OB_SUCCESS != (ret = col_ref.fill_sql_expression(expr, this, logical_plan, inner_plan)))
                {
                    TRANS_LOG("Failed to fill expression, err=%d", ret);
                    break;
                }
                else if (OB_SUCCESS != (ret = project_op->add_output_column(expr)))
                {
                    TRANS_LOG("Failed to add output column");
                    break;
                }
            }
        }
        //add liumengzhan_delete_index
        //fill index table rowkey columns into the Project op
        if (OB_LIKELY(OB_SUCCESS == ret) && is_need_modify_index)
        {
            if (OB_SUCCESS != (ret = sql_context_->schema_manager_->get_all_modifiable_index_tid(table_id, index_tid_array, index_array_count)))
            {
                TRANS_LOG("fail to get index table array for table[%ld]", table_id);
            }
            else
            {
                for (uint64_t i=0; i<index_array_count && OB_LIKELY(OB_SUCCESS == ret); i++)
                {
                    uint64_t index_cid = OB_INVALID_ID;
                    uint64_t index_tid = index_tid_array[i];
                    const ObTableSchema *index_table_schema = sql_context_->schema_manager_->get_table_schema(index_tid);
                    if (NULL == index_table_schema)
                    {
                        ret = OB_SCHEMA_ERROR;
                        TRANS_LOG("Failed to get index table schema, tid=%ld, ret=%d", index_tid, ret);
                        break;
                    }
                    const ObRowkeyInfo &index_rowkey_info = index_table_schema->get_rowkey_info();
                    for (int64_t j=0; j<index_rowkey_info.get_size(); j++)
                    {
                        if (OB_SUCCESS != (ret = index_rowkey_info.get_column_id(j, index_cid)))
                        {
                            TRANS_LOG("Failed to get index column id, err=%d", ret);
                            break;
                        }
                        else if (!rowkey_info->is_rowkey_column(index_cid) && !project_op->is_duplicated_output_column(index_cid))
                        {
                            ObBinaryRefRawExpr col_raw_ref(table_id, index_cid, T_REF_COLUMN);
                            expr.reset();
                            ObSqlRawExpr col_ref(OB_INVALID_ID, table_id, index_cid, &col_raw_ref);
                            if (OB_SUCCESS != (ret = col_ref.fill_sql_expression(expr, this, logical_plan, inner_plan)))
                            {
                                TRANS_LOG("Failed to fill expression, err=%d", ret);
                                break;
                            }
                            else if (OB_SUCCESS != (ret = project_op->add_output_column(expr)))
                            {
                                TRANS_LOG("Failed to add output column, err=%d", ret);
                                break;
                            }
                        }
                    }//end for
                }//end for
            }
        }
        //add:e
        // add ObActionFlag::OB_DEL_ROW cell
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            ObObj del_row;
            del_row.set_int(ObActionFlag::OP_DEL_ROW);
            ObConstRawExpr const_expr(del_row, T_INT);
            expr.reset();
            ObSqlRawExpr const_del(OB_INVALID_ID, table_id, OB_ACTION_FLAG_COLUMN_ID, &const_expr);
            if (OB_SUCCESS != (ret = const_del.fill_sql_expression(expr, this, logical_plan, inner_plan)))
            {
                TRANS_LOG("Failed to fill expression, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = project_op->add_output_column(expr)))
            {
                TRANS_LOG("Failed to add output column");
            }
        }
    }
    //add liumengzhan_delete_index
    if (OB_LIKELY(OB_SUCCESS == ret)&&is_need_modify_index)
    {
        ObRowDesc desc_for_whole_row;
        ObRowDescExt desc_ext_whole_row;
        if(OB_SUCCESS != (ret = cons_del_upd_row_desc(delete_stmt, table_id, desc_for_whole_row, desc_ext_whole_row)))
        {
            TRANS_LOG("cons whole row desc error, err=%d", ret);
            ret = OB_INVALID_ARGUMENT;
        }
        else
        {
            row_desc = desc_for_whole_row;
            row_desc_ext = desc_ext_whole_row;

        }
    }
    //add:e
    //add lijianqiang [sequence delete] 20150515:b
    ObSequenceDelete * sequence_delete = NULL;
    if (delete_stmt->has_sequence())
    {
        // 1.ObSequenceDelete
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            CREATE_PHY_OPERRATOR(sequence_delete, ObSequenceDelete, inner_plan, err_stat);
        }
        if (OB_SUCCESS != ret)//add qianzm [null operator unjudgement bug1181] 20160520
        {
        }
        else if (OB_SUCCESS != (ret = wrap_sequence(logical_plan, physical_plan, err_stat,
                                               row_desc_map, sequence_delete, delete_stmt,
                                               inner_plan)))
        {
            TRANS_LOG("wrap sequence failed");
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            if (OB_SUCCESS != (ret = sequence_delete->copy_sequence_info_from_delete_stmt()))
            {
                TRANS_LOG("copy sequence info from delete stmt failed!");
            }
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            //get the sequence info from the "__all_sequences"
            if (OB_SUCCESS != (ret = sequence_delete->prepare_sequence_prevval()))
            {
                TRANS_LOG("prepare sequence info failed!");
            }
        }
    }
    //add 20150515:e

    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        ObPhyOperator* table_op = NULL;
        //add liumengzhan_delete_index
        //change gen_phy_table_for_update to gen_phy_table_for_update_v2
        if (is_need_modify_index)
        {
          //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160222:b
          /*when the condition satisfys the original request of delete statement,
           *then call the original fucntion
           */
          bool multi_delete = false;
          if(OB_SUCCESS != (ret = is_multi_delete_update(logical_plan,inner_plan,*rowkey_info,delete_stmt,err_stat,multi_delete)))
          {
              TBSYS_LOG(WARN,"fail to run the function: is_multi_delete_update in delete statement");
          }
          //TBSYS_LOG(ERROR,"multi_delete=%d",multi_delete);
          if(OB_SUCCESS == ret)
          {
              //add gaojt 20160222:e
              //mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160223:b
  //            if (OB_SUCCESS != (ret = gen_phy_table_for_update(logical_plan, inner_plan, err_stat,
  //                                                                               delete_stmt, table_id, *rowkey_info,
  //                                                                               row_desc, row_desc_ext, table_op,

  //                                                                               sequence_delete
  //                                                                               //add 20150518:e
  //                                                                               )))
  //            {
  //            }
              if(!multi_delete)
              {
                  if (OB_SUCCESS != (ret = gen_phy_table_for_update_v2(logical_plan, inner_plan, err_stat,
                                                                                     delete_stmt, table_id, *rowkey_info,
                                                                                     row_desc, row_desc_ext, table_op
                                                                                     //add lijianqiang [sequence delete] 20160320:b
                                                                                     ,sequence_delete
                                                                                     //add 20160320:e
                                                                                     )))
                  {
                      TBSYS_LOG(WARN,"fail to run the function: gen_phy_table_for_update");
                  }
              }
              else
              {
                  if (OB_SUCCESS != (ret = gen_phy_table_for_update_new(logical_plan, inner_plan, err_stat,
                                                                                             delete_stmt, table_id, *rowkey_info,
                                                                                             row_desc, row_desc_ext, table_op,true,
                                                                                             //add lijianqiang [sequence] 20150909:b
                                                                                             sequence_delete,
                                                                                             //add 20150909:e
                                                                                             is_need_modify_index)))

                  {
                      TBSYS_LOG(WARN,"fail to run the function: gen_phy_table_for_update_new");
                  }
                  // del by maosy [Delete_Update_Function_isolation_RC] 20161218
                  //add by maosy [Delete_Update_Function_for_snpshot] 20161210
//                  if(index!=NULL && PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (*index)->get_type())
//                 {
//                     (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (*index)))->set_is_ud (true);
//                 }
//                else if( PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (0)->get_type())
//                 {
//                     (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (0)))->set_is_ud (true);
//                  }

                  //add e
          //del e
              }
              if(OB_SUCCESS == ret)
              {
                  // add by maosy [Delete_Update_Function_isolation_RC] 20161218
                  bool need_start_trans =false;
                  if(multi_delete && !is_multi_batch_)
                  {
                      need_start_trans =true;
                  }
                  if(index!=NULL && PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (*index)->get_type())
                  {
                      (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (*index)))->set_need_start_trans (need_start_trans);
                  }
                  else if( PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (0)->get_type())
                  {
                      (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (0)))->set_need_start_trans (need_start_trans);
                  }
                  // add maosy e

  //                else if (OB_SUCCESS != (ret = project_op->set_child(0, *table_op)))
  //                {
  //                    TRANS_LOG("Failed to set child, err=%d", ret);
  //                }
                  if (OB_SUCCESS != (ret = project_op->set_child(0, *table_op)))
                  {
                      TRANS_LOG("Failed to set child, err=%d", ret);
                  }
              }
          }
          //mod gaojt 20160223:e

      }
      //add:e
      else
      {
          //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160222:b
          /*when the condition satisfys the original request of delete statement,
           *then call the original fucntion
           */
          bool multi_delete = false;
          if(OB_SUCCESS != (ret = is_multi_delete_update(logical_plan,inner_plan,*rowkey_info,delete_stmt,err_stat,multi_delete)))
          {
              TBSYS_LOG(WARN,"fail to run the function: is_multi_delete_update in delete statement");
          }
          if(OB_SUCCESS == ret)
          {
              //add gaojt 20160222:e
              //mod gaojt [Delete_Update_Function] [JHOBv0.1] 20160223:b
  //            if (OB_SUCCESS != (ret = gen_phy_table_for_update(logical_plan, inner_plan, err_stat,
  //                                                                               delete_stmt, table_id, *rowkey_info,
  //                                                                               row_desc, row_desc_ext, table_op,

  //                                                                               sequence_delete
  //                                                                               //add 20150518:e
  //                                                                               )))
  //            {
  //            }
              if(!multi_delete)
              {
                  if (OB_SUCCESS != (ret = gen_phy_table_for_update(logical_plan, inner_plan, err_stat,
                                                                    delete_stmt, table_id, *rowkey_info,
                                                                    row_desc, row_desc_ext, table_op,
                                                                    //add lijianqiang [sequence] 20150518:b
                                                                    sequence_delete
                                                                    //add 20150518:e
                                                                    )))
                  {
                      TBSYS_LOG(WARN,"fail to run the function: gen_phy_table_for_update in delete statement");
                  }
              }
              else
              {
                  if (OB_SUCCESS != (ret = gen_phy_table_for_update_new(logical_plan, inner_plan, err_stat,
                                                                                             delete_stmt, table_id, *rowkey_info,
                                                                                             row_desc, row_desc_ext, table_op,true,
                                                                                             //add lijianqiang [sequence] 20150909:b
                                                                                             sequence_delete,
                                                                                             //add 20150909:e
                                                                                             is_need_modify_index)))

                  {
                      TBSYS_LOG(WARN,"fail to run the function: gen_phy_table_for_update_new in delete statement");
                  }
                  // del by maosy [Delete_Update_Function_isolation_RC] 20161218
                  //add by maosy [Delete_Update_Function_for_snpshot] 20161210
//                  if(index!=NULL && PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (*index)->get_type())
//                 {
//                     (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (*index)))->set_is_ud (true);
//                 }
//                else if( PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (0)->get_type())
//                 {
//                     (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (0)))->set_is_ud (true);
//                  }
                  //add e
          //del e
              }
              if(OB_SUCCESS == ret)
              {
                  // add by maosy [Delete_Update_Function_isolation_RC] 20161218
                  bool need_start_trans =false;
                  if(multi_delete && !is_multi_batch_)
                  {
                      need_start_trans =true;
                  }
                  if(index!=NULL && PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (*index)->get_type())
                  {
                      (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (*index)))->set_need_start_trans (need_start_trans);
                  }
                  else if( PHY_IUD_LOOP_CONTROL== physical_plan->get_phy_query (0)->get_type())
                  {
                      (dynamic_cast<ObIudLoopControl *> (physical_plan->get_phy_query (0)))->set_need_start_trans (need_start_trans);
                  }
                  // add maosy e

  //                else if (OB_SUCCESS != (ret = project_op->set_child(0, *table_op)))
  //                {
  //                    TRANS_LOG("Failed to set child, err=%d", ret);
  //                }
                  if (OB_SUCCESS != (ret = project_op->set_child(0, *table_op)))
                  {
                      TRANS_LOG("Failed to set child, err=%d", ret);
                  }
              }
          }
          //mod gaojt 20160223:e
      }
    }

    //add liumengzhan_delete_index
    if (OB_LIKELY(OB_SUCCESS == ret) && is_need_modify_index)
    {
        if (OB_SUCCESS != (ret = delete_index_op->set_input_values(table_id, index_array_count)))
        {
            TRANS_LOG("Failed to set input values for phy operator delete_index_op, err=%d", ret);
        }
        ObRowDesc main_row_desc;
        ObRowDescExt main_row_desc_ext;//not used
        if (OB_LIKELY(OB_SUCCESS == ret)&&OB_SUCCESS != (ret = cons_index_row_desc(table_id, main_row_desc, main_row_desc_ext)))
        {
            TRANS_LOG("Failed to construct main row desc, err=%d", ret);
        }
        else
        {
            delete_index_op->set_row_desc(main_row_desc);
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            ObRowDesc index_row_desc;
            ObRowDescExt index_row_desc_ext;//not used
            for (uint64_t i=0; i<index_array_count; i++)
            {
                index_row_desc.reset();
                index_row_desc_ext.reset();
                uint64_t index_table_id = index_tid_array[i];
                if (OB_SUCCESS != (ret = cons_index_row_desc(index_table_id, index_row_desc, index_row_desc_ext)))
                {
                    TRANS_LOG("Failed to construct main row desc, err=%d", ret);
                    break;
                }
                else if (OB_SUCCESS != (ret = delete_index_op->add_index_row_desc(i, index_row_desc)))
                {
                    TRANS_LOG("Failed to add index row desc, err=%d", ret);
                    break;
                }
                else if (OB_SUCCESS != (ret = delete_index_op->add_index_tid(i, index_table_id)))
                {
                    TRANS_LOG("Failed to add index table id, err=%d", ret);
                    break;
                }
            }
        }
    }
    //add:e
    if (ret == OB_SUCCESS)
    {
        if ((ret = merge_tables_version(*physical_plan, *inner_plan)) != OB_SUCCESS)
        {
            TRANS_LOG("Failed to add base tables version, err=%d", ret);
        }
    }
    return ret;
}

//add liumengzhan_delete_index
int ObTransformer::cons_index_row_desc(uint64_t table_id, ObRowDesc &row_desc, ObRowDescExt &row_desc_ext)
{
    UNUSED(row_desc_ext);
    int ret = OB_SUCCESS;
    uint64_t column_id = OB_INVALID_ID;
    //  ObObj data_type;
    const ObTableSchema *table_schema = sql_context_->schema_manager_->get_table_schema(table_id);
    if (NULL == table_schema)
    {
        ret = OB_SCHEMA_ERROR;
        TBSYS_LOG(ERROR, "Failed to get index table schema, tid=%ld, ret=%d", table_id, ret);
    }
    else
    {
        const ObRowkeyInfo rowkey_info = table_schema->get_rowkey_info();
        int64_t rowkey_size = rowkey_info.get_size();
        row_desc.set_rowkey_cell_count(rowkey_size);
        for (int64_t i=0; i<rowkey_size; i++)
        {
            //      const ObColumnSchemaV2 *column_schema = NULL;
            if (OB_SUCCESS != (ret = rowkey_info.get_column_id(i, column_id)))
            {
                TBSYS_LOG(ERROR, "Failed to get column id, ret=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = row_desc.add_column_desc(table_id, column_id)))
            {
                TBSYS_LOG(ERROR, "Failed to add column desc, ret=%d", ret);
                break;
            }
            //      else if (NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(table_id, column_id)))
            //      {
            //        TBSYS_LOG(ERROR, "Failed to get column schema, ret=%d", ret);
            //        break;
            //      }
            //      else
            //      {
            //        data_type.set_type(column_schema->get_type());
            //        data_type.set_precision(column_schema->get_precision());
            //        data_type.set_scale(column_schema->get_scale());
            //        if (OB_SUCCESS != (ret = row_desc_ext.add_column_desc(table_id, column_id, data_type)))
            //        {
            //          TBSYS_LOG(ERROR, "Failed to add column desc ext, ret=%d", ret);
            //          break;
            //        }
            //      }
        }//end for
        //add ObActionFlag: OP_DEL_ROW
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            if (OB_SUCCESS != (ret = row_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID)))
            {
                TBSYS_LOG(ERROR, "Failed to add column desc, ret=%d", ret);
            }
            //      else
            //      {
            //        data_type.set_type(ObIntType);
            //        if (OB_SUCCESS != (ret = row_desc_ext.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID, data_type)))
            //        {
            //          TBSYS_LOG(ERROR, "Failed to add column desc ext, ret=%d", ret);
            //        }
            //      }
        }//end if
    }
    return ret;
}
//add e

int ObTransformer::gen_physical_start_trans(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan* physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    OB_ASSERT(logical_plan);
    OB_ASSERT(physical_plan);
    ObStartTransStmt *stmt = NULL;
    ObStartTrans *start_trans = NULL;
    if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, stmt)))
    {
    }
    /* generate root operator */
    else if (NULL == CREATE_PHY_OPERRATOR(start_trans, ObStartTrans, physical_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator");
    }
    else if (OB_SUCCESS != (ret = add_phy_query(logical_plan, physical_plan, err_stat,
                                                query_id, stmt, start_trans, index)))
    {
        TRANS_LOG("Add ups_modify operator failed");
    }
    else
    {
        start_trans->set_rpc_stub(sql_context_->merger_rpc_proxy_);
        start_trans->set_trans_param(stmt->get_with_consistent_snapshot()?READ_ONLY_TRANS:READ_WRITE_TRANS);
    }
    return ret;
}

int ObTransformer::gen_physical_priv_stmt(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan* physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    OB_ASSERT(logical_plan);
    OB_ASSERT(physical_plan);
    ObBasicStmt * stmt = NULL;
    ObPrivExecutor *priv_executor = NULL;
    if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, stmt)))
    {
    }
    /* generate root operator */
    else if (NULL == CREATE_PHY_OPERRATOR(priv_executor, ObPrivExecutor, physical_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator");
    }
    else if (OB_SUCCESS != (ret = add_phy_query(logical_plan, physical_plan, err_stat,
                                                query_id, stmt, priv_executor, index)))
    {
        TRANS_LOG("Add create user operator failed");
    }
    else
    {
        ObBasicStmt * basic_stmt = NULL;
        if (stmt->get_stmt_type() == ObBasicStmt::T_CREATE_USER)
        {
            void *ptr = trans_malloc(sizeof(ObCreateUserStmt));
            if (ptr == NULL)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR, "malloc ObCreateUserStmt in transform mem pool failed, ret=%d", ret);
            }
            else
            {
                basic_stmt = new (ptr) ObCreateUserStmt(*(dynamic_cast<ObCreateUserStmt*>(stmt)));
            }
        }
        else if (stmt->get_stmt_type() == ObBasicStmt::T_DROP_USER)
        {
            void *ptr = trans_malloc(sizeof(ObDropUserStmt));
            if (ptr == NULL)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR, "malloc ObDropUserStmt in transform mem pool failed, ret=%d", ret);
            }
            else
            {
                basic_stmt = new (ptr) ObDropUserStmt(*(dynamic_cast<ObDropUserStmt*>(stmt)));
            }
        }
        else if (stmt->get_stmt_type() == ObBasicStmt::T_GRANT)
        {
            void *ptr = trans_malloc(sizeof(ObGrantStmt));
            if (ptr == NULL)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR, "malloc ObGrantStmt in transform mem pool failed, ret=%d", ret);
            }
            else
            {
                basic_stmt = new (ptr) ObGrantStmt(*(dynamic_cast<ObGrantStmt*>(stmt)));
            }
        }
        else if (stmt->get_stmt_type() == ObBasicStmt::T_REVOKE)
        {
            void *ptr = trans_malloc(sizeof(ObRevokeStmt));
            if (ptr == NULL)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR, "malloc ObRevokeStmt in transform mem pool failed, ret=%d", ret);
            }
            else
            {
                basic_stmt = new (ptr) ObRevokeStmt(*(dynamic_cast<ObRevokeStmt*>(stmt)));
            }
        }
        else if (stmt->get_stmt_type() == ObBasicStmt::T_RENAME_USER)
        {
            void *ptr = trans_malloc(sizeof(ObRenameUserStmt));
            if (ptr == NULL)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR, "malloc ObRenameUserStmt in transform mem pool failed, ret=%d", ret);
            }
            else
            {
                basic_stmt = new (ptr) ObRenameUserStmt(*(dynamic_cast<ObRenameUserStmt*>(stmt)));
            }
        }
        else if (stmt->get_stmt_type() == ObBasicStmt::T_SET_PASSWORD)
        {
            void *ptr = trans_malloc(sizeof(ObSetPasswordStmt));
            if (ptr == NULL)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR, "malloc ObSetPasswordStmt in transform mem pool failed, ret=%d", ret);
            }
            else
            {
                basic_stmt = new (ptr) ObSetPasswordStmt(*(dynamic_cast<ObSetPasswordStmt*>(stmt)));
            }
        }
        else if (stmt->get_stmt_type() == ObBasicStmt::T_LOCK_USER)
        {
            void *ptr = trans_malloc(sizeof(ObLockUserStmt));
            if (ptr == NULL)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR, "malloc ObGrantStmt in transform mem pool failed, ret=%d", ret);
            }
            else
            {
                basic_stmt = new (ptr) ObLockUserStmt(*(dynamic_cast<ObLockUserStmt*>(stmt)));
            }
        }
        //add wenghaixing [database manage]20150609
        else if (stmt->get_stmt_type() == ObBasicStmt::T_CREATE_DATABASE)
        {
            void *ptr = trans_malloc(sizeof(ObCreateDbStmt));
            if(NULL == ptr)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR, "malloc ObCreateDbStatement in transform mem pool failed, ret=%d", ret);
            }
            else
            {
                basic_stmt = new (ptr) ObCreateDbStmt(*(dynamic_cast<ObCreateDbStmt*>(stmt)));
            }
        }
        else if (stmt->get_stmt_type() == ObBasicStmt::T_USE_DATABASE)
        {
            void *ptr = trans_malloc(sizeof(ObUseDbStmt));
            if(NULL == ptr)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR, "malloc ObUseDbStatement in transform mem pool failed, ret=%d", ret);
            }
            else
            {
                basic_stmt = new (ptr) ObUseDbStmt(*(dynamic_cast<ObUseDbStmt*>(stmt)));
            }
        }
        else if(stmt->get_stmt_type() == ObBasicStmt::T_DROP_DATABASE)
        {
            void *ptr = trans_malloc(sizeof(ObDropDbStmt));
            if(NULL == ptr)
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(ERROR, "malloc ObDropDbStatement in transform mem pool failed, ret=%d", ret);
            }
            else
            {
                basic_stmt = new (ptr) ObDropDbStmt(*(dynamic_cast<ObDropDbStmt*>(stmt)));
            }
        }
        //add e
        priv_executor->set_stmt(basic_stmt);
        priv_executor->set_context(sql_context_);
    }
    return ret;
}

int ObTransformer::gen_physical_change_obi_stmt(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan* physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObChangeObiStmt *change_obi_stmt = NULL;
    ObChangeObi *change_obi = NULL;
    if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, change_obi_stmt)))
    {
    }
    else
    {
        CREATE_PHY_OPERRATOR(change_obi, ObChangeObi, physical_plan, err_stat);
        if (OB_SUCCESS == ret)
        {
            ObString target_server_addr;
            change_obi_stmt->get_target_server_addr(target_server_addr);
            change_obi->set_force(change_obi_stmt->get_force());
            change_obi->set_target_role(change_obi_stmt->get_target_role());
            if (OB_SUCCESS != (ret = change_obi->set_target_server_addr(target_server_addr)))
            {
            }
            else if (OB_SUCCESS != (ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, change_obi_stmt, change_obi, index)))
            {
                TBSYS_LOG(WARN, "add_phy_query failed, ret=%d", ret);
            }
            else
            {
                ObObj old_ob_query_timeout;
                ObObj change_obi_timeout_value;
                change_obi_timeout_value.set_int(mergeserver::ObMergeServerMain::get_instance()->get_merge_server().get_config().change_obi_timeout);
                ObString ob_query_timeout = ObString::make_string(OB_QUERY_TIMEOUT_PARAM);
                if (OB_SUCCESS != (ret = sql_context_->session_info_->get_sys_variable_value(ob_query_timeout, old_ob_query_timeout)))
                {
                    TBSYS_LOG(WARN, "get old session timeout value failed, ret=%d", ret);
                }
                else if (OB_SUCCESS != (ret = change_obi->set_change_obi_timeout(change_obi_timeout_value)))
                {
                    TBSYS_LOG(ERROR, "set change obi timeout failed, ret=%d", ret);
                }
                else
                {
                    change_obi->set_check_ups_log_interval(static_cast<int>(mergeserver::ObMergeServerMain::get_instance()->get_merge_server().get_config().check_ups_log_interval));
                    change_obi->set_old_ob_query_timeout(old_ob_query_timeout);
                    change_obi->set_context(sql_context_);
                }
            }

        }
    }
    return ret;
}
int ObTransformer::gen_physical_kill_stmt(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan* physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObKillStmt *kill_stmt = NULL;
    ObKillSession *kill_op = NULL;

    /* get statement */
    if (ret == OB_SUCCESS)
    {
        get_stmt(logical_plan, err_stat, query_id, kill_stmt);
    }
    /* generate operator */
    if (ret == OB_SUCCESS)
    {
        CREATE_PHY_OPERRATOR(kill_op, ObKillSession, physical_plan, err_stat);
        if (ret == OB_SUCCESS)
        {
            kill_op->set_rpc_stub(sql_context_->merger_rpc_proxy_);
            kill_op->set_session_mgr(sql_context_->session_mgr_);
            ret = add_phy_query(logical_plan, physical_plan, err_stat, query_id, kill_stmt, kill_op, index);
        }
    }

    if (ret == OB_SUCCESS)
    {
        kill_op->set_session_id(kill_stmt->get_thread_id());
        kill_op->set_is_query(kill_stmt->is_query());
        kill_op->set_is_global(kill_stmt->is_global());
    }

    return ret;
}

int ObTransformer::gen_physical_end_trans(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan* physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    OB_ASSERT(logical_plan);
    OB_ASSERT(physical_plan);
    ObEndTransStmt *stmt = NULL;
    ObEndTrans *end_trans = NULL;
    if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, stmt)))
    {
    }
    /* generate root operator */
    else if (NULL == CREATE_PHY_OPERRATOR(end_trans, ObEndTrans, physical_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator");
    }
    else if (OB_SUCCESS != (ret = add_phy_query(logical_plan, physical_plan, err_stat,
                                                query_id, stmt, end_trans, index)))
    {
        TRANS_LOG("Add ups_modify operator failed");
    }
    else
    {
        end_trans->set_rpc_stub(sql_context_->merger_rpc_proxy_);
        end_trans->set_trans_param(sql_context_->session_info_->get_trans_id(), stmt->get_is_rollback());
    }
    return ret;
}

int ObTransformer::gen_phy_select_for_update(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObSelectStmt *select_stmt = NULL;
    ObPhyOperator *result_op = NULL;
    //ObLockFilter *lock_op = NULL;
    ObProject *project_op = NULL;
    uint64_t table_id = OB_INVALID_ID;
    const ObRowkeyInfo *rowkey_info = NULL;
    ObPhysicalPlan *inner_plan = NULL;
    ObRowDesc row_desc;
    ObRowDescExt row_desc_ext;
    ObSEArray<int64_t, 64> row_desc_map;
    //add liuzy [sequence select for update] 20150918:b
    ObSequenceSelect *sequence_select_op = NULL;
    //add 20150918:e
    if ((ret = get_stmt(logical_plan, err_stat, query_id, select_stmt)) != OB_SUCCESS)
    {
    }
    else if (!select_stmt->is_for_update()
             || select_stmt->get_from_item_size() > 1
             || select_stmt->get_table_size() > 1
             || (select_stmt->get_table_size() > 0
                 && select_stmt->get_table_item(0).type_ != TableItem::BASE_TABLE
                 && select_stmt->get_table_item(0).type_ != TableItem::ALIAS_TABLE
                 )
             || select_stmt->get_group_expr_size() > 0
             || select_stmt->get_agg_fun_size() > 0
             || select_stmt->get_order_item_size() > 0
             || select_stmt->has_limit())
    {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG("This select statement is not allowed by implement");
    }
    else if ((ret = wrap_ups_executor(physical_plan, query_id, inner_plan, index, err_stat)) != OB_SUCCESS)
    {
        TBSYS_LOG(WARN, "err=%d", ret);
    }
    /*
  else if (CREATE_PHY_OPERRATOR(lock_op, ObLockFilter, physical_plan, err_stat) == NULL)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG("Failed to ObLockFilter operator");
  }
  */
    else if (CREATE_PHY_OPERRATOR(project_op, ObProject, inner_plan, err_stat) == NULL)
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator");
    }
    //add liuzy [sequence select for update] 20150918:b
    else if (select_stmt->has_sequence()
             && CREATE_PHY_OPERRATOR(sequence_select_op, ObSequenceSelect, physical_plan, err_stat) == NULL)
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator");
    }
    else if (select_stmt->has_sequence()
             && OB_SUCCESS != (ret = wrap_sequence(logical_plan, physical_plan, err_stat,
                                                   row_desc_map, sequence_select_op, select_stmt)))
    {
        TRANS_LOG("wrap sequence failed");
    }
    else if (select_stmt->has_sequence()
             && OB_SUCCESS != (ret = sequence_select_op->prepare_sequence_prevval()))
    {
        TRANS_LOG("prepare sequence info failed!");
    }
    //add 20150918:e
    else if ((ret = inner_plan->add_phy_query(
                  project_op,
                  physical_plan == inner_plan ? index : NULL,
                  physical_plan != inner_plan)))

    {
        TRANS_LOG("Add top operator failed");
    }
    /* select ... from DUAL */
    else if (select_stmt->get_table_size() == 0)
    {
        if (CREATE_PHY_OPERRATOR(result_op, ObDualTableScan, inner_plan, err_stat) == NULL)
        {
            TRANS_LOG("Generate dual table operator failed, ret=%d", ret);
        }
    }
    else
    {
        //add liuzy [sequence select for update] 20150921:b
        if (NULL != sequence_select_op)
        {
            sequence_select_op->copy_sequence_info_from_select_stmt(select_stmt);
            sequence_select_op->set_for_update(select_stmt->is_for_update());
            if (select_stmt->select_clause_has_sequence())
            {
                sequence_select_op->set_project_op(project_op->get_id());
            }
        }
        //add 2050921:e
        table_id = select_stmt->get_table_item(0).table_id_;
        if ((ret = cons_row_desc(table_id,
                                 select_stmt,
                                 row_desc_ext,
                                 row_desc,
                                 rowkey_info,
                                 row_desc_map, err_stat)) != OB_SUCCESS)
        {
        }
        else
        {
            //lock_op->set_write_lock_flag();
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            if ((ret = gen_phy_table_for_update(logical_plan,inner_plan, err_stat,
                                                select_stmt, table_id, *rowkey_info,
                                                row_desc, row_desc_ext, result_op
                                                , sequence_select_op//add liuzy [select for update] 20150918 /*Exp: add default argument ObSequenceOp*/
                                                )) != OB_SUCCESS)
            {
            }
            /*
      else if ((ret = lock_op->set_child(0, *result_op)) != OB_SUCCESS)
      {
        TRANS_LOG("Failed to set child, err=%d", ret);
      }
      else
      {
        result_op = lock_op;
      }
      */
        }
    }
    // add output columns
    for (int32_t i = 0; ret == OB_SUCCESS && i < select_stmt->get_select_item_size(); i++)
    {
        const SelectItem& select_item = select_stmt->get_select_item(i);
        ObSqlExpression output_expr;
        ObSqlRawExpr *expr = NULL;
        if ((expr = logical_plan->get_expr(select_item.expr_id_)) == NULL)
        {
            ret = OB_ERR_ILLEGAL_ID;
            TRANS_LOG("Wrong expression id");
        }
        else if ((ret = expr->fill_sql_expression(
                      output_expr,
                      this,
                      logical_plan,
                      inner_plan)) != OB_SUCCESS)
        {
            TRANS_LOG("Generate post-expression faild");
        }
        else if ((ret = project_op->add_output_column(output_expr)) != OB_SUCCESS)
        {
            TRANS_LOG("Add output column to project operator faild");
        }
    }
    // generate physical plan for order by
    if (ret == OB_SUCCESS && select_stmt->get_order_item_size() > 0)
    {
        ret = gen_phy_order_by(logical_plan, inner_plan, err_stat, select_stmt, result_op, result_op);
    }
    // generate physical plan for limit
    if (ret == OB_SUCCESS && select_stmt->has_limit())
    {
        ret = gen_phy_limit(logical_plan, inner_plan, err_stat, select_stmt, result_op, result_op);
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        ObWhenFilter *when_filter_op = NULL;
        if (select_stmt->get_when_expr_size() > 0)
        {
            if ((ret = gen_phy_when(logical_plan,
                                    inner_plan,
                                    err_stat,
                                    query_id,
                                    *result_op,
                                    when_filter_op
                                    )) != OB_SUCCESS)
            {
            }
            else if ((ret = project_op->set_child(0, *when_filter_op)) != OB_SUCCESS)
            {
                TRANS_LOG("Set child of project_op operator failed, err=%d", ret);
            }
        }
        else if ((ret = project_op->set_child(0, *result_op)) != OB_SUCCESS)
        {
            TRANS_LOG("Set child of project_op operator failed, err=%d", ret);
        }
    }
    if (ret == OB_SUCCESS)
    {
        if ((ret = merge_tables_version(*physical_plan, *inner_plan)) != OB_SUCCESS)
        {
            TRANS_LOG("Failed to add base tables version, err=%d", ret);
        }
    }
    return ret;
}

int ObTransformer::gen_physical_alter_system(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        int32_t* index)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObAlterSysCnfStmt *alt_sys_stmt = NULL;
    ObAlterSysCnf     *alt_sys_op = NULL;

    /* get statement */
    if ((get_stmt(logical_plan, err_stat, query_id, alt_sys_stmt)) != OB_SUCCESS)
    {
    }
    /* generate operator */
    else if (CREATE_PHY_OPERRATOR(alt_sys_op, ObAlterSysCnf, physical_plan, err_stat) == NULL)
    {
    }
    else if ((ret = add_phy_query(logical_plan,
                                  physical_plan,
                                  err_stat,
                                  query_id,
                                  alt_sys_stmt,
                                  alt_sys_op, index)
              ) != OB_SUCCESS)
    {
        TRANS_LOG("Add physical operator failed, err=%d", ret);
    }
    else
    {
        alt_sys_op->set_sql_context(*sql_context_);
        hash::ObHashMap<ObSysCnfItemKey, ObSysCnfItem>::iterator iter;
        for (iter = alt_sys_stmt->sys_cnf_begin(); iter != alt_sys_stmt->sys_cnf_end(); iter++)
        {
            ObSysCnfItem cnf_item = iter->second;
            if ((ret = ob_write_string(*mem_pool_,
                                       iter->second.param_name_,
                                       cnf_item.param_name_)) != OB_SUCCESS)
            {
                TRANS_LOG("Failed to copy param name, err=%d", ret);
                break;
            }
            else if ((ret = ob_write_obj(*mem_pool_,
                                         iter->second.param_value_,
                                         cnf_item.param_value_)) != OB_SUCCESS)
            {
                TRANS_LOG("Failed to copy param value, err=%d", ret);
                break;
            }
            else if ((ret = ob_write_string(*mem_pool_,
                                            iter->second.comment_,
                                            cnf_item.comment_)) != OB_SUCCESS)
            {
                TRANS_LOG("Failed to copy comment, err=%d", ret);
                break;
            }
            else if ((ret = ob_write_string(*mem_pool_,
                                            iter->second.server_ip_,
                                            cnf_item.server_ip_)) != OB_SUCCESS)
            {
                TRANS_LOG("Failed to copy server ip, err=%d", ret);
                break;
            }
            else if ((ret = alt_sys_op->add_sys_cnf_item(cnf_item)) != OB_SUCCESS)
            {
                TRANS_LOG("Failed to add config item, err=%d", ret);
                break;
            }
        }
    }
    return ret;
}

int ObTransformer::gen_phy_show_parameters(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObTableRpcScan *table_op = NULL;
    ObProject *project_op = NULL;
    ObRpcScanHint hint;
    hint.read_method_ = ObSqlReadStrategy::USE_SCAN;
    if (CREATE_PHY_OPERRATOR(table_op, ObTableRpcScan, physical_plan, err_stat) == NULL)
    {
    }
    else if (CREATE_PHY_OPERRATOR(project_op, ObProject, physical_plan, err_stat) == NULL)
    {
    }
    else if ((ret = project_op->set_child(0, *table_op)) != OB_SUCCESS)
    {
        TRANS_LOG("Set child of project failed, ret=%d", ret);
    }
    else if ((ret = table_op->set_table(
                  OB_ALL_SYS_CONFIG_STAT_TID,
                  OB_ALL_SYS_CONFIG_STAT_TID)
              ) != OB_SUCCESS)
    {
        TRANS_LOG("ObTableRpcScan set table faild, table id = %lu", OB_ALL_SYS_CONFIG_STAT_TID);
    }
    else if ((ret = table_op->init(sql_context_, &hint)) != OB_SUCCESS)
    {
        TRANS_LOG("ObTableRpcScan init faild");
    }
    else if ((ret = physical_plan->add_base_table_version(OB_ALL_SYS_CONFIG_STAT_TID, 0)) != OB_SUCCESS)
    {
        TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", OB_ALL_SYS_CONFIG_STAT_TID, ret);
    }
    else
    {
        ObString cnf_name = ObString::make_string(OB_ALL_SYS_CONFIG_STAT_TABLE_NAME);
        ObString ip_name = ObString::make_string("server_ip");
        ObString port_name = ObString::make_string("server_port");
        ObString type_name = ObString::make_string("server_type");
        for (int32_t i = 0; i < show_stmt->get_column_size(); i++)
        {
            const ColumnItem* column_item = show_stmt->get_column_item(i);
            ObString cname;
            if (column_item->column_name_ == ip_name)
            {
                cname = ObString::make_string("svr_ip");
            }
            else if (column_item->column_name_ == port_name)
            {
                cname = ObString::make_string("svr_port");
            }
            else if (column_item->column_name_ == type_name)
            {
                cname = ObString::make_string("svr_type");
            }
            else
            {
                cname = column_item->column_name_;
            }
            const ObColumnSchemaV2* column_schema = NULL;
            if ((column_schema = sql_context_->schema_manager_->get_column_schema(
                     cnf_name,
                     cname
                     )) == NULL)
            {
                ret = OB_ERR_COLUMN_UNKNOWN;
                TRANS_LOG("Can not get relative column %.*s from %s",
                          column_item->column_name_.length(), column_item->column_name_.ptr(),
                          OB_ALL_SYS_CONFIG_STAT_TABLE_NAME);
                break;
            }
            else
            {
                // add table scan columns
                ObBinaryRefRawExpr col_expr(OB_ALL_SYS_CONFIG_STAT_TID, column_schema->get_id(), T_REF_COLUMN);
                ObSqlRawExpr col_raw_expr(
                            common::OB_INVALID_ID,
                            OB_ALL_SYS_CONFIG_STAT_TID,
                            column_schema->get_id(),
                            &col_expr);
                ObSqlExpression output_expr;
                if ((ret = col_raw_expr.fill_sql_expression(
                         output_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS
                        || (ret = table_op->add_output_column(output_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add table output columns faild");
                    break;
                }

                // add project columns
                col_raw_expr.set_table_id(column_item->table_id_);
                col_raw_expr.set_column_id(column_item->column_id_);
                output_expr.reset();
                if ((ret = col_raw_expr.fill_sql_expression(
                         output_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS
                        || (ret = project_op->add_output_column(output_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add project output columns faild");
                    break;
                }
            }
        } // end for
    }
    if (ret == OB_SUCCESS)
    {
        out_op = project_op;
    }
    return ret;
}

int ObTransformer::gen_phy_show_create_table(
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObShowStmt *show_stmt,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObRowDesc row_desc;
    ObValues *values_op = NULL;

    int32_t num = show_stmt->get_column_size();
    if (OB_UNLIKELY(num != 2))
    {
        ret = OB_ERR_COLUMN_SIZE;
        TRANS_LOG("wrong columns' number of %s", OB_CREATE_TABLE_SHOW_TABLE_NAME);
    }
    else if (CREATE_PHY_OPERRATOR(values_op, ObValues, physical_plan, err_stat) == NULL)
    {
    }
    else
    {
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
            const ColumnItem* column_item = show_stmt->get_column_item(i);
            if ((ret = row_desc.add_column_desc(column_item->table_id_, column_item->column_id_)) != OB_SUCCESS)
            {
                TRANS_LOG("Add row desc error, err=%d", ret);
                break;
            }
        }
        if ((ret = values_op->set_row_desc(row_desc)) != OB_SUCCESS)
        {
            TRANS_LOG("Set row desc error, err=%d", ret);
        }
    }

    const ObTableSchema *table_schema = NULL;
    if (ret != OB_SUCCESS)
    {
    }
    else if ((table_schema = sql_context_->schema_manager_->get_table_schema(
                  show_stmt->get_show_table_id()
                  )) == NULL)
    {
        ret = OB_ERR_TABLE_UNKNOWN;
        TRANS_LOG("Unknow table id = %lu, err=%d", show_stmt->get_show_table_id(), ret);
    }
    else
    {
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ObRow val_row;
        val_row.set_row_desc(row_desc);
        int64_t pos = 0;
        char buf[OB_MAX_VARCHAR_LENGTH];

        // add table_name
        int32_t name_len = static_cast<int32_t>(strlen(table_schema->get_table_name()));
        ObString name_val(name_len, name_len, table_schema->get_table_name());
        ObObj name;
        name.set_varchar(name_val);
        if ((ret = row_desc.get_tid_cid(0, table_id, column_id)) != OB_SUCCESS)
        {
            TRANS_LOG("Get table_name desc failed");
        }
        else if ((ret = val_row.set_cell(table_id, column_id, name)) != OB_SUCCESS)
        {
            TRANS_LOG("Add table_name to ObRow failed, ret=%d", ret);
        }
        // add table definition
        else if ((ret = row_desc.get_tid_cid(1, table_id, column_id)) != OB_SUCCESS)
        {
            TRANS_LOG("Get table definition desc failed");
        }
        else if ((ret = cons_table_definition(
                      *table_schema,
                      buf,
                      OB_MAX_VARCHAR_LENGTH,
                      pos,
                      err_stat)) != OB_SUCCESS)
        {
            TRANS_LOG("Generate table definition failed");
        }
        else
        {
            ObString value_str(static_cast<int32_t>(pos), static_cast<int32_t>(pos), buf);
            ObObj value;
            value.set_varchar(value_str);
            if ((ret = val_row.set_cell(table_id, column_id, value)) != OB_SUCCESS)
            {
                TRANS_LOG("Add table_definiton to ObRow failed, ret=%d", ret);
            }
        }
        // add final value row
        if (ret == OB_SUCCESS && (ret = values_op->add_values(val_row)) != OB_SUCCESS)
        {
            TRANS_LOG("Add value row failed");
        }
    }
    if (ret == OB_SUCCESS)
    {
        out_op = values_op;
    }
    return ret;
}

int ObTransformer::cons_table_definition(
        const ObTableSchema& table_schema,
        char* buf,
        const int64_t& buf_len,
        int64_t& pos,
        ErrStat& err_stat)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    const ObColumnSchemaV2* columns = NULL;
    int32_t column_size = 0;
    if ((columns = sql_context_->schema_manager_->get_table_schema(
             table_schema.get_table_id(),
             column_size)) == NULL
            || column_size <= 0)
    {
        ret = OB_ERR_TABLE_UNKNOWN;
        TRANS_LOG("Unknow table id = %lu, err=%d", table_schema.get_table_id(), ret);
    }
    else
    {
        databuff_printf(buf, buf_len, pos, "CREATE TABLE %s (\n",
                        table_schema.get_table_name());
    }

    // add columns
    for (int32_t i = 0; ret == OB_SUCCESS && i < column_size; i++)
    {
        if (i == 0)
        {
            databuff_printf(buf, buf_len, pos, "%s %s\n",
                            columns[i].get_name(), ObObj::get_sql_type(columns[i].get_type()));
        }
        else
        {
            databuff_printf(buf, buf_len, pos, ", %s %s\n",
                            columns[i].get_name(), ObObj::get_sql_type(columns[i].get_type()));
        }
    }

    // add rowkeys
    const ObRowkeyInfo& rowkey_info = table_schema.get_rowkey_info();
    databuff_printf(buf, buf_len, pos, ", PRIMARY KEY(");
    for (int64_t j = 0; ret == OB_SUCCESS && j < rowkey_info.get_size(); j++)
    {
        const ObColumnSchemaV2* col = NULL;
        if ((col = sql_context_->schema_manager_->get_column_schema(
                 table_schema.get_table_id(),
                 rowkey_info.get_column(j)->column_id_
                 )) == NULL)
        {
            ret = OB_ERR_COLUMN_UNKNOWN;
            TRANS_LOG("Get column %lu failed", rowkey_info.get_column(j)->column_id_);
            break;
        }
        else if (j != rowkey_info.get_size() - 1)
        {
            databuff_printf(buf, buf_len, pos, "%s, ", col->get_name());
        }
        else
        {
            databuff_printf(buf, buf_len, pos, "%s)\n", col->get_name());
        }
    }

    // add table options
    if (ret == OB_SUCCESS)
    {
        databuff_printf(buf, buf_len, pos, ") ");
        if (table_schema.get_max_sstable_size() >= 0)
        {
            databuff_printf(buf, buf_len, pos, "TABLET_MAX_SIZE = %ld, ",
                            table_schema.get_max_sstable_size());
        }
        if (table_schema.get_block_size() >= 0)
        {
            databuff_printf(buf, buf_len, pos, "TABLET_BLOCK_SIZE = %d, ",
                            table_schema.get_block_size());
        }
        if (*table_schema.get_expire_condition() != '\0')
        {
            databuff_printf(buf, buf_len, pos, "EXPIRE_INFO = '%s', ",
                            table_schema.get_expire_condition());
        }
        if (*table_schema.get_comment_str() != '\0')
        {
            databuff_printf(buf, buf_len, pos, "COMMENT = '%s', ",
                            table_schema.get_comment_str());
        }
        if( !table_schema.is_merge_dynamic_data())
        {
            databuff_printf(buf, buf_len, pos, "CONSISTENT_MODE=STATIC ");
        }
        databuff_printf(buf, buf_len, pos,
                        // "REPLICA_NUM = %ld, "
                        "USE_BLOOM_FILTER = %s",
                        // table_schema.get_replica_num(),
                        table_schema.is_use_bloomfilter() ? "TRUE" : "FALSE");
    }
    return ret;
}

int ObTransformer::gen_phy_when(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        const uint64_t& query_id,
        ObPhyOperator& child_op,
        ObWhenFilter *& when_filter)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObStmt *stmt = NULL;
    if (OB_SUCCESS != (ret = get_stmt(logical_plan, err_stat, query_id, stmt)))
    {
    }
    /* generate root operator */
    else if (CREATE_PHY_OPERRATOR(when_filter, ObWhenFilter, physical_plan, err_stat) == NULL)
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG("Failed to create phy operator");
    }
    else if ((ret = when_filter->set_child(0, child_op)) != OB_SUCCESS)
    {
        TRANS_LOG("Add first child of ObWhenFilter failed, ret=%d", ret);
    }
    else
    {
        when_filter->set_when_number(stmt->get_when_number());
        int32_t sub_index = OB_INVALID_INDEX;
        uint64_t expr_id = OB_INVALID_ID;
        ObSqlRawExpr *when_expr = NULL;
        ObUnaryOpRawExpr *when_func = NULL;
        ObUnaryRefRawExpr *sub_query = NULL;
        ObPhyOperator *sub_plan = NULL;
        for(int32_t i = 0; ret == OB_SUCCESS && i < stmt->get_when_fun_size(); i++)
        {
            expr_id = stmt->get_when_func_id(i);
            if ((when_expr = logical_plan->get_expr(expr_id)) == NULL
                    || (when_func = static_cast<ObUnaryOpRawExpr*>(when_expr->get_expr())) == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong expr id = %lu of when function, ret=%d", expr_id, ret);
            }
            else if ((sub_query = static_cast<ObUnaryRefRawExpr*>(when_func->get_op_expr())) == NULL)
            {
                ret = OB_ERR_ILLEGAL_VALUE;
                TRANS_LOG("Wrong expr of %dth when function, ret=%d", i, ret);
            }
            else if ((ret = generate_physical_plan(
                          logical_plan,
                          physical_plan,
                          err_stat,
                          sub_query->get_ref_id(),
                          &sub_index)) != OB_SUCCESS)
            {
            }
            else if ((sub_plan = physical_plan->get_phy_query(sub_index)) == NULL)
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong sub-query index %d of when function, ret=%d", sub_index, ret);
            }
            else
            {
                switch (when_func->get_expr_type())
                {
                case T_ROW_COUNT:
                {
                    ObRowCount *row_count_op = NULL;
                    if (CREATE_PHY_OPERRATOR(row_count_op, ObRowCount, physical_plan, err_stat) == NULL)
                    {
                        break;
                    }
                    else if ((ret = row_count_op->set_child(0, *sub_plan)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add child of ObRowCount failed, ret=%d", ret);
                    }
                    else if ((ret = when_filter->set_child(i + 1, *row_count_op)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add child of ObWhenFilter failed, ret=%d", ret);
                    }
                    else
                    {
                        row_count_op->set_tid_cid(when_expr->get_table_id(), when_expr->get_column_id());
                        row_count_op->set_when_func_index(i);
                    }
                    break;
                }
                default:
                {
                    ret = OB_ERR_ILLEGAL_TYPE;
                    TRANS_LOG("Unknown type of %dth when function, ret=%d", i, ret);
                    break;
                }
                }
                if (ret != OB_SUCCESS)
                {
                    break;
                }
                else if ((ret = physical_plan->remove_phy_query(sub_index)) != OB_SUCCESS)
                {
                    TRANS_LOG("Remove sub-query plan failed, ret=%d", ret);
                }
            }
        }
        for(int32_t i = 0; ret == OB_SUCCESS && i < stmt->get_when_expr_size(); i++)
        {
            uint64_t expr_id = stmt->get_when_expr_id(i);
            ObSqlRawExpr *raw_expr = logical_plan->get_expr(expr_id);
            ObSqlExpression expr;
            if (OB_UNLIKELY(raw_expr == NULL))
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Wrong id = %lu to get expression, ret=%d", expr_id, ret);
            }
            else if ((ret = raw_expr->fill_sql_expression(
                          expr,
                          this,
                          logical_plan,
                          physical_plan)
                      ) != OB_SUCCESS)
            {
                TRANS_LOG("Generate ObSqlExpression failed, ret=%d", ret);
            }
            else if ((ret = when_filter->add_filter(expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add when filter failed, ret=%d", ret);
            }
        }
    }
    return ret;
}

//add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
/*expr:
Function:     column_null_check
Calls:        gen_physical_insert_new; gen_physical_replace
Input:        logical_plan  type:ObLogicalPlan*
Input:        insert_stmt   type:ObInsertStmt*
Input:        op_type       type:enum operateType
Output:       err_stat      type:ErrStat&
return:       ret           type:int
*/
int ObTransformer::column_null_check(
        ObLogicalPlan *logical_plan,
        ObInsertStmt *insert_stmt,
        ErrStat& err_stat,
        enum operateType op_type)
{
    int &ret = err_stat.err_code_ = OB_SUCCESS;
    ObArray<uint64_t> colArray;

    if ((OP_INSERT != op_type) && (OP_REPLACE != op_type))
    {
        ret = OB_SUCCESS;
        return ret;
    }

    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        uint64_t tid = insert_stmt->get_table_id();
        int32_t name_len= static_cast<int32_t>(strlen(sql_context_->schema_manager_->get_table_schema(tid)->get_table_name()));
        const ObString table_name(name_len,name_len,sql_context_->schema_manager_->get_table_schema(tid)->get_table_name());

        if( !TableSchema::is_system_table(table_name) && OB_LIKELY(OB_SUCCESS == ret) )
        {
            const ObColumnSchemaV2 *col = NULL;
            int32_t size = 0;
            if (!IS_SHOW_TABLE(tid))
            {
                col = sql_context_->schema_manager_->get_table_schema(tid, size);
            }
            for (int64_t j = 0; j < size; j++)
            {
                if ((!col[j].is_nullable()) && (NULL == insert_stmt->get_column_item_by_id(tid, col[j].get_id())))
                {
                    TRANS_LOG("column:'%s' can not be null",col[j].get_name());   //Corrections TBSYS_LOG->TRANS_LOG add by peiouya 2013/12/22
                    ret = (op_type == OP_INSERT ? OB_ERR_INSERT_NULL_COLUMN:OB_ERR_REPLACE_NULL_COLUMN);
                    break;
                } else if ((!col[j].is_nullable()) && (NULL != insert_stmt->get_column_item_by_id(tid, col[j].get_id())))
                {
                    colArray.push_back(col[j].get_id());
                }
            }
            ////////////////////////////////////////////////////
            if (OB_LIKELY(ret == OB_SUCCESS))
            {
                int64_t value_vectors_size = insert_stmt->get_value_row_size();

                int64_t para_count = 0;

                for (int64_t k = 0; (ret == OB_SUCCESS) && (k < value_vectors_size); k++)
                {
                    const oceanbase::common::ObArray<uint64_t>& value_row = insert_stmt->get_value_row(k);
                    for (int64_t m = 0; (ret == OB_SUCCESS) && (m < value_row.count()); m++)
                    {
                        ObSqlRawExpr* ObSRawExpr = logical_plan->get_expr(value_row.at(m));
                        const ColumnItem* column_item = insert_stmt->get_column_item((int32_t)m);
                        for (int64_t n = 0; (ret == OB_SUCCESS) && (n < colArray.count()); n++)
                        {
                            if ( (column_item->column_id_ == colArray.at(n)) && ( ObNullType == ObSRawExpr->get_expr()->get_result_type()))
                            {
                                TRANS_LOG("column:'%.*s' can not be null!",column_item->column_name_.length(),column_item->column_name_.ptr());
                                ret = (op_type == OP_INSERT ? OB_ERR_INSERT_NULL_COLUMN:OB_ERR_REPLACE_NULL_COLUMN);
                                break;
                            }
                        }
                        //add peiouya [NotNULL_check]  20131222:b
                        /*expr:prepare/execute Null constraint*/
                        if (OB_LIKELY(ret == OB_SUCCESS))
                        {
                            col = sql_context_->schema_manager_->get_column_schema(tid, column_item->column_id_);
                            if((NULL != col) && (T_QUESTIONMARK == ObSRawExpr->get_expr()->get_expr_type()))
                            {
                                if (para_count >= result_->get_params().count())
                                {
                                    para_count -= para_count;
                                }
                                bool is_null = col->is_nullable();
                                if (OB_SUCCESS != (ret = result_->set_params_constraint(para_count, is_null)))
                                {
                                    TRANS_LOG("Fail to save the constraint of  column %s!", col->get_name());
                                    break;
                                }
                                para_count++;
                            }
                        }
                        //add 20131222:e
                    }
                }
            }
        }
    }
    colArray.clear();
    return ret;

}//add 20131222:e

//add wenghaixing[decimal] for fix delete bug 2014/10/10
int ObTransformer::ob_write_obj_for_delete(ModuleArena &allocator, const ObObj &src, ObObj &dst,ObObj type){

    int ret,ret2=OB_SUCCESS;
    if(type.get_type()!=ObDecimalType){

        ret2=ob_write_obj(allocator,src,dst);

    }
    else{
        const ObObj *ob1=NULL;
        ob1=&src;
        ObObj casted_cell;
        char buff[MAX_PRINTABLE_SIZE];
        memset(buff,0,MAX_PRINTABLE_SIZE);
        ObString os2;
        os2.assign_ptr(buff,MAX_PRINTABLE_SIZE);
        casted_cell.set_varchar(os2);
        if(OB_SUCCESS!=(ret=obj_cast(*ob1,type,casted_cell,ob1)))
        {

        }
        else
        {
            ObString str;
            ObString str_clone;
            ob1->get_decimal(str);
            ObDecimal od,od_cmp;
            if(OB_SUCCESS!=(ret=od.from(str.ptr(),str.length())))
            {
                TBSYS_LOG(DEBUG, "faild to do from(),str=%.*s", str.length(),str.ptr());
            }
            else{
                od_cmp=od;
            }
            if(OB_SUCCESS==ret)
            {

                if(OB_SUCCESS!=(ret=od.modify_value(ob1->get_precision(),ob1->get_scale())))
                {
                    //TBSYS_LOG(ERROR, "faild to do modify_value(),od.p=%d,od.s=%d,od.v=%d,src.p=%d,src.s=%d..od=%.*s", od.get_precision(),od.get_scale(),od.get_vscale(),src.get_precision(),src.get_scale(),str.length(),str.ptr());
                }
                else if(od_cmp!=od){
                    ret=OB_ERROR;
                }
                else
                {
                    char buf[MAX_PRINTABLE_SIZE];
                    memset(buf, 0, MAX_PRINTABLE_SIZE);
                    ObString os;
                    int64_t length=od.to_string(buf,MAX_PRINTABLE_SIZE);
                    os.assign_ptr(buf,(int)length);

                    if (OB_SUCCESS == (ret = ob_write_string(allocator, os, str_clone)))
                    {
                        dst.set_decimal(str_clone,ob1->get_precision(),ob1->get_scale(),ob1->get_scale());
                    }
                }
            }
        }

        if(OB_SUCCESS!=ret){
            ret2=ob_write_obj(allocator,src,dst);
        }
    }
    return ret2;

}
//add e
//add wenghaixing [secondary index upd.3] 20141128
int ObTransformer::gen_phy_table_for_update_v2(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan*& physical_plan,
        ErrStat& err_stat,
        ObStmt *stmt,
        uint64_t table_id,
        const ObRowkeyInfo &rowkey_info,
        const ObRowDesc &row_desc,
        const ObRowDescExt &row_desc_ext,
        ObPhyOperator*& table_op
        //add lijianqiang [sequence update] 20160316:b
        ,ObPhyOperator* sequence_op)
        //add 20160316:e
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    TableItem* table_item = NULL;
    ObTableRpcScan *table_rpc_scan_op = NULL;
    ObFilter *filter_op = NULL;
    ObIncScan *inc_scan_op = NULL;
    ObMultipleGetMerge *fuse_op = NULL;
    ObMemSSTableScan *static_data = NULL;
    ObValues *tmp_table = NULL;
    ObRowDesc rowkey_col_map;
    ObExprValues* get_param_values = NULL;
    const ObTableSchema *table_schema = NULL;
    ObObj rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER]; // used for constructing GetParam
    ObPostfixExpression::ObPostExprNodeType type_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
    ModuleArena rowkey_alloc(OB_MAX_VARCHAR_LENGTH, ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
    ObCellInfo cell_info;
    cell_info.table_id_ = table_id;
    cell_info.row_key_.assign(rowkey_objs, rowkey_info.get_size());

    //add lijianqiang [sequence_fix] 20151030:b
    ObSequence * base_sequence_op = NULL;
    base_sequence_op = dynamic_cast<ObSequence *> (sequence_op);
    //add 20151030:e
    bool has_other_cond = false;
    ObRpcScanHint hint;
    hint.read_method_ = ObSqlReadStrategy::USE_GET;
    hint.read_consistency_ = FROZEN;
    hint.is_get_skip_empty_row_ = false;

    //add lbzhong [Update rowkey] 20160320:b
    bool is_update_rowkey = get_is_update_rowkey(stmt, &rowkey_info);
    bool is_update_sequence = false;
    ObUpdateDBSemFilter *update_sem = NULL;
    ObArray<ObSqlExpression*> filter_array;
    if(ObBasicStmt::T_UPDATE == stmt->get_stmt_type()) {
      is_update_sequence = (dynamic_cast<ObUpdateStmt*>(stmt))->has_sequence();
    }
    //add:e
    if (table_id == OB_INVALID_ID
            || (table_item = stmt->get_table_item_by_id(table_id)) == NULL
            || TableItem::BASE_TABLE != table_item->type_)
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("Wrong table id, tid=%lu", table_id);
    }
    else if (NULL == CREATE_PHY_OPERRATOR(table_rpc_scan_op, ObTableRpcScan, physical_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else if ((ret = table_rpc_scan_op->set_table(table_item->table_id_, table_item->ref_id_)) != OB_SUCCESS)
    {
        TRANS_LOG("ObTableRpcScan set table failed");
    }
    else if (OB_SUCCESS != (ret = table_rpc_scan_op->init(sql_context_, &hint)))
    {
        TRANS_LOG("ObTableRpcScan init failed");
    }
    else if (NULL == CREATE_PHY_OPERRATOR(tmp_table, ObValues, physical_plan, err_stat))
    {
    }
    else if (OB_SUCCESS != (ret = tmp_table->set_child(0, *table_rpc_scan_op)))
    {
        TBSYS_LOG(WARN, "failed to set child op, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = physical_plan->add_phy_query(tmp_table)))
    {
        TBSYS_LOG(WARN, "failed to add sub query, err=%d", ret);
    }
    else if (NULL == CREATE_PHY_OPERRATOR(filter_op, ObFilter, physical_plan, err_stat))
    {
    }
    else if (NULL == CREATE_PHY_OPERRATOR(inc_scan_op, ObIncScan, physical_plan, err_stat))
    {
    }
    else if (NULL == CREATE_PHY_OPERRATOR(fuse_op, ObMultipleGetMerge, physical_plan, err_stat))
    {
    }
    else if (NULL == CREATE_PHY_OPERRATOR(static_data, ObMemSSTableScan, physical_plan, err_stat))
    {
    }
    else if (OB_SUCCESS != (ret = fuse_op->set_child(0, *static_data)))
    {
    }
    else if (OB_SUCCESS != (ret = fuse_op->set_child(1, *inc_scan_op)))
    {
    }
    else if (NULL == CREATE_PHY_OPERRATOR(get_param_values, ObExprValues, physical_plan, err_stat))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else if (OB_SUCCESS != (ret = physical_plan->add_phy_query(get_param_values)))
    {
        TBSYS_LOG(WARN, "failed to add sub query, err=%d", ret);
    }
    else if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("Fail to get table schema for table[%ld]", table_id);
    }
    else if ((ret = physical_plan->add_base_table_version(
                  table_id,
                  table_schema->get_schema_version()
                  )) != OB_SUCCESS)
    {
        TRANS_LOG("Add base table version failed, table_id=%ld, ret=%d", table_id, ret);
    }
    else
    {
        fuse_op->set_is_ups_row(false);

        inc_scan_op->set_scan_type(ObIncScan::ST_MGET);
        inc_scan_op->set_write_lock_flag();
        inc_scan_op->set_hotspot(stmt->get_query_hint().hotspot_);
        inc_scan_op->set_values(get_param_values->get_id(), false);

        static_data->set_tmp_table(tmp_table->get_id());

        table_rpc_scan_op->set_rowkey_cell_count(row_desc.get_rowkey_cell_count());
        table_rpc_scan_op->set_need_cache_frozen_data(true);

        get_param_values->set_row_desc(row_desc, row_desc_ext);
        // set filters
        int32_t num = stmt->get_condition_size();
        uint64_t cid = OB_INVALID_ID;
        int64_t cond_op = T_INVALID;
        ObObj cond_val;
        ObPostfixExpression::ObPostExprNodeType val_type = ObPostfixExpression::BEGIN_TYPE;
        int64_t rowkey_idx = OB_INVALID_INDEX;
        ObRowkeyColumn rowkey_col;
        //add wenghaixing[decimal] for fix delete bug 2014/10/10
        uint64_t tid= table_schema->get_table_id();
        //add e
        for (int32_t i = 0; i < num; i++)
        {
            ObSqlRawExpr *cnd_expr = logical_plan->get_expr(stmt->get_condition_id(i));
            OB_ASSERT(cnd_expr);
            cnd_expr->set_applied(true);
            ObSqlExpression *filter = ObSqlExpression::alloc();
            if (NULL == filter)
            {
                TRANS_LOG("no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
                break;
            }
            else if (OB_SUCCESS != (ret = cnd_expr->fill_sql_expression(*filter, this, logical_plan, physical_plan)))
            {
                ObSqlExpression::free(filter);
                TRANS_LOG("Failed to fill expression, err=%d", ret);
                break;
            }
            //add lijianqiang [sequence update] 20151030:b
            else if (NULL != base_sequence_op
                     && base_sequence_op->can_fill_sequence_info()
                     && base_sequence_op->is_sequence_cond_id(stmt->get_condition_id(i))
                     && OB_SUCCESS == ret
                     && (OB_SUCCESS != (ret = base_sequence_op->fill_the_sequence_info_to_cond_expr(filter, OB_INVALID_ID))))
            {
                TRANS_LOG("Failed deal the sequence condition filter,err=%d",ret);
                break;
            }
            //add 20151030:e
            else if (filter->is_simple_condition(false, cid, cond_op, cond_val, &val_type)
                     && (T_OP_EQ == cond_op || T_OP_IS == cond_op)
                     && rowkey_info.is_rowkey_column(cid))
            {
                if (
                    //add lbzhong [Update rowkey] 20160414
                    !is_update_rowkey &&
                    //add:e
                    OB_SUCCESS != (ret = table_rpc_scan_op->add_filter(filter)))
                {
                    ObSqlExpression::free(filter);
                    TRANS_LOG("Failed to add filter, err=%d", ret);
                    break;
                }
                else if (OB_SUCCESS != (ret = rowkey_col_map.add_column_desc(OB_INVALID_ID, cid)))
                {
                    TRANS_LOG("Failed to add column desc, err=%d", ret);
                    break;
                }
                else if (OB_SUCCESS != (ret = rowkey_info.get_index(cid, rowkey_idx, rowkey_col)))
                {
                    TRANS_LOG("Unexpected branch");
                    ret = OB_ERR_UNEXPECTED;
                    break;
                }
                //add wenghaixing[decimal] for fix delete bug 2014/10/10
                else{

                    ObObjType cond_val_type;
                    uint32_t cond_val_precision;
                    uint32_t cond_val_scale;
                    ObObj static_obj;
                    if(OB_SUCCESS!=sql_context_->schema_manager_->get_cond_val_info(tid,cid,cond_val_type,cond_val_precision,cond_val_scale))
                    {

                    }
                    else{
                        tmp_table->add_rowkey_array(tid,cid,cond_val_type,cond_val_precision,cond_val_scale);
                        if(ObDecimalType==cond_val_type){
                            static_obj.set_precision(cond_val_precision);
                            static_obj.set_scale(cond_val_scale);
                            static_obj.set_type(cond_val_type);
                        }
                    }

                    //add e
                    //modify wenghaixing[decimal] for fix delete bug 2014/10/10
                    //else if (OB_SUCCESS != (ret = ob_write_obj(rowkey_alloc, cond_val, rowkey_objs[rowkey_idx]))) // deep copy
                    if (OB_SUCCESS != (ret = ob_write_obj_for_delete(rowkey_alloc, cond_val, rowkey_objs[rowkey_idx],static_obj))) // deep copy
                        //modify e
                    {
                        TRANS_LOG("failed to copy cell, err=%d", ret);
                    }

                    else
                    {
                        type_objs[rowkey_idx] = val_type;
                        TBSYS_LOG(DEBUG, "rowkey obj, i=%ld val=%s", rowkey_idx, to_cstring(cond_val));
                    }
                }
            }
            else
            {
                // other condition
                has_other_cond = true;
                if (OB_SUCCESS != (ret = filter_op->add_filter(filter)))
                {
                    TRANS_LOG("Failed to add filter, err=%d", ret);
                    break;
                }
            }
            //mod bingo [Update rowkey bugfix] 20161218:b
            //add lbzhong [Update rowkey] 20160505:b
            /*if(is_update_rowkey && OB_SUCCESS != (ret = filter_array.push_back(filter)))
            {
              ObSqlExpression::free(filter);
              TRANS_LOG("Failed to push filter to filter array, err=%d", ret);
              break;
            }*/
            if(is_update_rowkey){
              ObSqlExpression* expr_clone = ObSqlExpression::alloc();
              if (NULL == expr_clone)
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(WARN, "no memory");
              }
              else
              {
                *expr_clone = *filter;
              }
              if(OB_SUCCESS == ret && OB_SUCCESS != (ret = filter_array.push_back(expr_clone)))
              {
                ObSqlExpression::free(expr_clone);
                TRANS_LOG("Failed to push filter to filter array, err=%d", ret);
                break;
              }
            }
            //add:e
            //mod:e
        } // end for
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            int64_t rowkey_col_num = rowkey_info.get_size();
            uint64_t cid = OB_INVALID_ID;
            for (int64_t i = 0; i < rowkey_col_num; ++i)
            {
                if (OB_SUCCESS != (ret = rowkey_info.get_column_id(i, cid)))
                {
                    TRANS_LOG("Failed to get column id, err=%d", ret);
                    break;
                }
                else if (OB_INVALID_INDEX == rowkey_col_map.get_idx(OB_INVALID_ID, cid))
                {
                    TRANS_LOG("Primary key column %lu not specified in the WHERE clause", cid);
                    ret = OB_ERR_LACK_OF_ROWKEY_COL;
                    break;
                }
            } // end for
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        // add output columns
        for(int64_t key_col_num=0;key_col_num<rowkey_info.get_size();key_col_num++)
        {
            uint64_t key_cid=OB_INVALID_ID;
            if(OB_SUCCESS!=(rowkey_info.get_column_id(key_col_num,key_cid)))
            {
                TBSYS_LOG(WARN,"cannot get rowkey id for get param values,ret[%d]",ret);
                break;
            }
            else
            {
                ObBinaryRefRawExpr col_expr(table_id, key_cid, T_REF_COLUMN);
                ObSqlRawExpr col_raw_expr(
                            common::OB_INVALID_ID,
                            table_id,
                            key_cid,
                            &col_expr);
                ObSqlExpression output_expr;
                if ((ret = col_raw_expr.fill_sql_expression(
                         output_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS
                        || (ret = table_rpc_scan_op->add_output_column(output_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add table output columns faild");
                    break;
                }
                // for IncScan
                ObConstRawExpr col_expr2;
                ObRowkeyColumn col;
                int64_t idx=0;
                if(OB_SUCCESS!=(ret=rowkey_info.get_index(key_cid,idx,col)))
                {
                    TBSYS_LOG(WARN,"failed to find index for rowkey_column");
                    break;
                }
                else if (OB_SUCCESS != (ret = col_expr2.set_value_and_type(rowkey_objs[idx])))
                {
                    TBSYS_LOG(WARN, "failed to set value, err=%d", ret);
                    break;
                }
                else
                {
                    switch (type_objs[key_col_num])
                    {
                    case ObPostfixExpression::PARAM_IDX:
                        col_expr2.set_expr_type(T_QUESTIONMARK);
                        col_expr2.set_result_type(ObVarcharType);
                        break;
                    case ObPostfixExpression::SYSTEM_VAR:
                        col_expr2.set_expr_type(T_SYSTEM_VARIABLE);
                        col_expr2.set_result_type(ObVarcharType);
                        break;
                    case ObPostfixExpression::TEMP_VAR:
                        col_expr2.set_expr_type(T_TEMP_VARIABLE);
                        col_expr2.set_result_type(ObVarcharType);
                        break;
                    default:
                        break;
                    }
                }
                ObSqlRawExpr col_raw_expr2(
                            common::OB_INVALID_ID,
                            table_id,
                            key_cid,
                            &col_expr2);
                ObSqlExpression output_expr2;
                if ((ret = col_raw_expr2.fill_sql_expression(
                         output_expr2,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add table output columns failed");
                    break;
                }
                else if (OB_SUCCESS != (ret = get_param_values->add_value(output_expr2)))
                {
                    TRANS_LOG("Failed to add cell into get param, err=%d", ret);
                    break;
                }
                //add wenghaixing[decimal] for fix delete bug 2014/10/10
                else{
                    get_param_values->set_del_upd();
                }
            }
        }
        for (int32_t i = 0; ret == OB_SUCCESS && i < row_desc.get_column_num(); i++)
        {
            //const ColumnItem *col_item = stmt->get_column_item(i);
            uint64_t cid = OB_INVALID_ID;
            uint64_t tid = OB_INVALID_ID;
            if(OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, cid)))
            {
                TBSYS_LOG(WARN, "get cid from row_desc failed,ret[%d]", ret);
                break;
            }
            else if(!rowkey_info.is_rowkey_column(cid))
            {
                //TBSYS_LOG(ERROR, "test::whx out put column[%ld]",i);
                if (table_schema->get_table_id() == table_item->table_id_)
                {
                    ObBinaryRefRawExpr col_expr(table_id, cid, T_REF_COLUMN);
                    ObSqlRawExpr col_raw_expr(
                                common::OB_INVALID_ID,
                                table_id,
                                cid,
                                &col_expr);
                    ObSqlExpression output_expr;
                    if ((ret = col_raw_expr.fill_sql_expression(
                             output_expr,
                             this,
                             logical_plan,
                             physical_plan)) != OB_SUCCESS
                            || (ret = table_rpc_scan_op->add_output_column(output_expr)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add table output columns faild");
                        break;
                    }
                    // for IncScan
                    ObConstRawExpr col_expr2;
                    {
                        ObObj null_obj;
                        col_expr2.set_value_and_type(null_obj);
                    }
                    ObSqlRawExpr col_raw_expr2(
                                common::OB_INVALID_ID,
                                table_id,
                                cid,
                                &col_expr2);
                    ObSqlExpression output_expr2;
                    if ((ret = col_raw_expr2.fill_sql_expression(
                             output_expr2,
                             this,
                             logical_plan,
                             physical_plan)) != OB_SUCCESS)
                    {
                        TRANS_LOG("Add table output columns failed");
                        break;
                    }
                    else if (OB_SUCCESS != (ret = get_param_values->add_value(output_expr2)))
                    {
                        TRANS_LOG("Failed to add cell into get param, err=%d", ret);
                        break;
                    }
                    //add wenghaixing[decimal] for fix delete bug 2014/10/10
                    else{
                        get_param_values->set_del_upd();
                    }
                    //add e
                }
            }
        } // end for
    }
    // add action flag column
    //mod peiouya [FIX_INDEX_BUG_WHEN_UPDATE_OR_REPLACE] 20160405:b
    //if (OB_LIKELY(OB_SUCCESS == ret) && ObBasicStmt::T_UPDATE == stmt->get_stmt_type())
    if (OB_LIKELY(OB_SUCCESS == ret) && ObBasicStmt::T_REPLACE != stmt->get_stmt_type())
    {
      ObSqlExpression column_ref;
      column_ref.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
      if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, column_ref)))
      {
        TBSYS_LOG(WARN, "fail to make column expr:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = table_rpc_scan_op->add_output_column(column_ref)))
      {
        TBSYS_LOG(WARN, "failed to add output column, err=%d", ret);
      }
    }
    //mod peiouya 20160405:e
    //add lbzhong [Update rowkey] 20160504:b
    ObProject *project_for_sequence = NULL;
    if (OB_SUCCESS == ret && NULL != sequence_op && is_update_sequence)
    {
      ObSequenceUpdate *sequence_update_op = dynamic_cast<ObSequenceUpdate *>(sequence_op);

      //add lijianqiang [sequence] 20150909:b
      /*Exp:hanlde the set clause of UPDATE stmt*/
      //add lbzhong [Update rowkey] 20160111:b
      ObArray<uint64_t> update_columns;
      stmt->get_update_columns(update_columns);
      int64_t nonrowkey_count = row_desc.get_column_num() - rowkey_info.get_size();
      for(int64_t i = 0; i < update_columns.count(); i++)
      {
        if(!rowkey_info.is_rowkey_column(update_columns.at(i)))
        {
          nonrowkey_count--;
        }
      }
      //add:e
      //sequence_update_op->set_index_trigger_update(index_trigger_upd, is_column_hint_index);
      int row_num = 1;
      if (OB_SUCCESS != (ret = sequence_update_op->handle_the_set_clause_of_seuqence(row_num
                                   //add lbzhong [Update rowkey] 20160107:b
                                   , nonrowkey_count, is_update_rowkey
                                   //add:e
                                   )))
      {
        TRANS_LOG("handle the set clause of sequence failed,ret=%d",ret);
      }
      //add 2050909:e
      project_for_sequence = sequence_update_op->get_project_for_update();
    }
    if(is_update_rowkey && OB_SUCCESS != (ret = cons_in_filter_and_expr_values(logical_plan, physical_plan,
                                       table_rpc_scan_op, fuse_op, table_id,
                                       &row_desc, &rowkey_info, stmt, project_for_sequence, err_stat, filter_array,
                                       update_sem, get_param_values)))
    {
        TRANS_LOG("Failed to add cons_in_filter, err=%d", ret);
    }
    //add:e
    if (ret == OB_SUCCESS)
    {
        if (has_other_cond)
        {
          //add lbzhong [Update rowkey] 20160414
          if(!is_update_rowkey)
          {
          //add:e
            if (OB_SUCCESS != (ret = filter_op->set_child(0, *fuse_op)))
            {
                TRANS_LOG("Failed to set child, err=%d", ret);
            }
            else
            {
                table_op = filter_op;
            }
          //add lbzhong [Update rowkey] 20160414
          }
          else
          {
            if (OB_SUCCESS != (ret = filter_op->set_child(0, *update_sem)))
            {
                TRANS_LOG("Failed to set child, err=%d", ret);
            }
            else
            {
                table_op = filter_op;
            }
          }
          //add:e
        }
        else
        {
          //add lbzhong [Update rowkey] 20160414
          if(!is_update_rowkey)
          {
          //add:e
            table_op = fuse_op;
          //add lbzhong [Update rowkey] 20160414
          }
          else
          {
            table_op = update_sem;
          }
          //add:e
        }
        //add wenghaixing[decimal] for fix delete bug 2014/10/10
        tmp_table->set_fix_obvalues();
        //add e
    }
    return ret;
}
//add e

//add lijianqiang [sequence] 20150717:b
int ObTransformer::wrap_sequence(ObLogicalPlan *&logical_plan,
                                 ObPhysicalPlan *&physical_plan,
                                 ErrStat& err_stat,
                                 const ObSEArray<int64_t, 64> &row_desc_map,
                                 ObSequence *sequence_op,
                                 ObStmt *stmt,
                                 ObPhysicalPlan *inner_plan)
{
    int ret = OB_SUCCESS;
    ObSequenceStmt *sequence_stmt = NULL;
    if (NULL == logical_plan
            || NULL == physical_plan
            || NULL == sequence_op
            || NULL == stmt)
    {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR,"the sequence op is not init! ret::[%d]",ret);
    }
    if (OB_SUCCESS == ret)
    {
        //for select ,select has no inner plan(the plan send to ups)
        if (ObBasicStmt::T_SELECT == stmt->get_stmt_type())
        {
            inner_plan = physical_plan;
        }
        sequence_stmt = dynamic_cast<ObSequenceStmt*>(stmt);
        if (NULL == sequence_stmt)
        {
            ret = OB_INIT_FAIL;
            TBSYS_LOG(WARN, "dynamic cast failed!");
        }
    }
    if (OB_SUCCESS == ret)
    {
        ObRowDesc sequence_row_desc;
        const ObRowkeyInfo *sequence_rowkey_info = NULL;
        //1.do some nint, be careful the order
        sequence_op->set_sql_context(sql_context_);
        sequence_op->set_stmt(sequence_stmt);
        sequence_op->add_sequence_names_no_dup();
        sequence_op->set_result_set(result_);
        if (OB_SUCCESS != (ret = sequence_op->cons_sequence_row_desc(sequence_row_desc, sequence_rowkey_info)))
        {
            TRANS_LOG("Failed to cons sequence row desc!");
        }
        //2. sequence
        const ObTableSchema *sequence_table_schema = NULL;
        ObString sequence_sys_table_name = ObString::make_string(OB_ALL_SEQUENCE_TABLE_NAME);
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            if (NULL == (sequence_table_schema = sql_context_->schema_manager_->get_table_schema(sequence_sys_table_name)))
            {
                ret = OB_ERR_ILLEGAL_ID;
                TRANS_LOG("Fail to get sequence table schema for table[%.*s]", sequence_sys_table_name.length(),sequence_sys_table_name.ptr());
            }
        }
        ObTableRpcScan *sequence_table_scan = NULL;//for sequence info
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            ObRpcScanHint sequence_hint;
            sequence_hint.read_method_ = ObSqlReadStrategy::USE_GET;
            sequence_hint.is_get_skip_empty_row_ = true;//no need of empty row
            sequence_hint.read_consistency_ = STRONG;//be careful! we must get the data you have created!
            int64_t sequence_table_id = OB_ALL_SEQUENCE_TID;
            CREATE_PHY_OPERRATOR(sequence_table_scan, ObTableRpcScan, inner_plan, err_stat);
            if (OB_UNLIKELY(OB_SUCCESS != ret))
            {
            }
            else if (OB_SUCCESS != (ret = sequence_table_scan->set_table(sequence_table_id, sequence_table_id)))
            {
                TRANS_LOG("failed to set table id, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = sequence_table_scan->init(sql_context_, &sequence_hint)))
            {
                TRANS_LOG("failed to init sequence table scan, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = gen_phy_static_data_scan(logical_plan, inner_plan, err_stat,
                                                                   stmt, sequence_row_desc, row_desc_map,
                                                                   OB_ALL_SEQUENCE_TID, *sequence_rowkey_info, *sequence_table_scan)))
            {
                TRANS_LOG("err=%d", ret);
            }
            else if ((ret = physical_plan->add_base_table_version(
                          sequence_table_id,
                          sequence_table_schema->get_schema_version()
                          )) != OB_SUCCESS)
            {
                TRANS_LOG("Add base sequence table version failed, sequence_table_id=%ld, ret=%d", sequence_table_id, ret);
            }
            else
            {
                sequence_table_scan->set_rowkey_cell_count(sequence_table_schema->get_rowkey_info().get_size());
                sequence_table_scan->set_cache_bloom_filter(false);
            }
        }
        ObValues *sequence_table = NULL;//for sequence info
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            CREATE_PHY_OPERRATOR(sequence_table, ObValues, inner_plan, err_stat);
            if (OB_UNLIKELY(OB_SUCCESS != ret))
            {
            }
            else if (OB_SUCCESS != (ret = sequence_table->set_child(0, *sequence_table_scan)))
            {
                TBSYS_LOG(WARN, "failed to set child, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = sequence_op->set_child(0, *sequence_table)))
            {
                TBSYS_LOG(WARN, "failed to set child, err=%d", ret);
            }
        }
    }
    return ret;
}
//add 20150717:e
//add dolphin [ROW_NUMBER-PARTITION_BY]@20150827:b
int ObTransformer::gen_phy_partition_by(
        ObLogicalPlan *logical_plan,
        ObPhysicalPlan *physical_plan,
        ErrStat& err_stat,
        ObSelectStmt *select_stmt,
        ObPhyOperator *in_op,
        ObPhyOperator *&out_op)
{
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    ObMergeGroupBy *group_op = NULL;
    ObSort *sort_op = NULL;
    ObProject *project_op = NULL;
    if (ret == OB_SUCCESS)
        CREATE_PHY_OPERRATOR(sort_op, ObSort, physical_plan, err_stat);
    if (ret == OB_SUCCESS)
        CREATE_PHY_OPERRATOR(group_op, ObMergeGroupBy, physical_plan, err_stat);
    if (ret == OB_SUCCESS && (ret = group_op->set_child(0, *sort_op)) != OB_SUCCESS)
    {
        TRANS_LOG("Add child of group by plan faild");
    }
    if(OB_SUCCESS == ret)
    {
        group_op->set_analytic_func(true);
    }
    ObSqlRawExpr *partition_expr;
    int32_t num = select_stmt->get_partition_expr_size();
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
        partition_expr = logical_plan->get_expr(select_stmt->get_partition_expr_id(i));
        OB_ASSERT(NULL != partition_expr);
        if (partition_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
        {
            ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(partition_expr->get_expr());
            OB_ASSERT(NULL != col_expr);
            ret = sort_op->add_sort_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id(), true);
            if (ret != OB_SUCCESS)
            {
                TRANS_LOG("Add sort column faild, table_id=%lu, column_id=%lu",
                          col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
                break;
            }
            ret = group_op->add_partition_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
            if (ret != OB_SUCCESS)
            {
                TRANS_LOG("Add group column faild, table_id=%lu, column_id=%lu",
                          col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
                break;
            }
        }
        else if (partition_expr->get_expr()->is_const())
        {
            // do nothing, const column is of no usage for sorting
        }
        else
        {
            if (!project_op)
            {
                CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan, err_stat);
                if (ret != OB_SUCCESS)
                    break;
                if ((ret = project_op->set_child(0, *in_op)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add child of project plan faild");
                    break;
                }
            }
            ObSqlExpression col_expr;
            if ((ret = partition_expr->fill_sql_expression(
                     col_expr,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS
                    || (ret = project_op->add_output_column(col_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add output column to project plan faild");
                break;
            }
            if ((ret = sort_op->add_sort_column(
                     partition_expr->get_table_id(),
                     partition_expr->get_column_id(),
                     true)) != OB_SUCCESS)
            {
                TRANS_LOG("Add sort column to sort plan faild");
                break;
            }
            if ((ret = group_op->add_partition_column(
                     partition_expr->get_table_id(),
                     partition_expr->get_column_id())) != OB_SUCCESS)
            {
                TRANS_LOG("Add group column to group plan faild");
                break;
            }
        }
    }
    //int64_t n = sort_op->get_sort_column_size();
    ObSqlRawExpr *order_expr;
    int32_t order_num = select_stmt->get_order_item_for_rownum_size();
    int64_t tid = OB_INVALID;
    int64_t cid = OB_INVALID;
    for (int32_t i = 0; ret == OB_SUCCESS && i < order_num; i++)
    {
        const OrderItem& order_item = select_stmt->get_order_item_for_rownum(i);
        order_expr = logical_plan->get_expr(order_item.expr_id_);
        if (order_expr->get_expr()->is_const())
        {
            // do nothing, const column is of no usage for sorting
        }
        else if (order_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
        {
            ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(order_expr->get_expr());
            tid = col_expr->get_first_ref_id();
            cid = col_expr->get_second_ref_id();
        }
        else
        {
            if (!project_op)
            {
                CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan, err_stat);
                if (ret != OB_SUCCESS)
                    break;
                if ((ret = project_op->set_child(0, *in_op)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add child of project plan failed");
                    break;
                }
            }
            ObSqlExpression col_expr;
            if ((ret = order_expr->fill_sql_expression(
                     col_expr,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS
                    || (ret = project_op->add_output_column(col_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add output column to project plan failed");
                break;
            }
            tid = order_expr->get_table_id();
            cid = order_expr->get_column_id();
        }
        /* del liumz, [ROW_NUMBER bugfix: sort_column in partition_by_clause precede over that in order_by_clause]20160321
         * logical error exists in this code block
        if(n > 0)
        {
            bool exist = false;
            for(int32_t i = 0; i < n; ++i)
            {

                if(OB_SUCCESS != (ret = sort_op->exist_sort_column(tid,cid,exist)))
                {
                    TBSYS_LOG(WARN, "failed to judge exist_sort_column, err=%d", ret);
                    break;
                }
                else if(!exist)
                {
                    if(OB_SUCCESS != (ret = sort_op->add_sort_column(tid,cid,order_item.order_type_ == OrderItem::ASC ? true : false)))
                    {
                        TRANS_LOG("Add sort column to sort plan failed");
                        break;
                    }
                }
                else if(i != 0)
                {
                    if(OB_SUCCESS != (ret = sort_op->replace_sort_column_at(i,tid,cid,order_item.order_type_ == OrderItem::ASC ? true : false)))
                    {
                        TRANS_LOG("Add sort column to sort plan failed");
                        break;
                    }

                }
            }
        }
        else */
        if (OB_SUCCESS != (ret = sort_op->add_sort_column(tid,cid,order_item.order_type_ == OrderItem::ASC ? true : false)))
        {
            TRANS_LOG("Add sort column to sort plan failed");
            break;
        }
    }
    if (ret == OB_SUCCESS)
    {
        if (project_op)
            ret = sort_op->set_child(0, *project_op);
        else
            ret = sort_op->set_child(0, *in_op);
        if (ret != OB_SUCCESS)
        {
            TRANS_LOG("Add child to sort plan faild");
        }
    }

    num = select_stmt->get_anal_fun_size();
    ObSqlRawExpr *agg_expr = NULL;
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
        agg_expr = logical_plan->get_expr(select_stmt->get_anal_expr_id(i));
        OB_ASSERT(NULL != agg_expr);
        if (agg_expr->get_expr()->is_aggr_fun())
        {
            ObSqlExpression new_agg_expr;
            if ((ret = agg_expr->fill_sql_expression(
                     new_agg_expr,
                     this,
                     logical_plan,
                     physical_plan)) != OB_SUCCESS
                    || (ret = group_op->add_anal_column(new_agg_expr)) != OB_SUCCESS)
            {
                TRANS_LOG("Add aggregate function to group plan faild");
                break;
            }
        }
        else
        {
            TRANS_LOG("Wrong aggregate function, exp_id = %lu", agg_expr->get_expr_id());
            break;
        }
        agg_expr->set_columnlized(true);
    }
    if (ret == OB_SUCCESS)
        out_op = group_op;

    return ret;
}
//add:e
//add lijianqiang [set_row_key_ignore] 20151019:b
bool ObTransformer::can_ignore_current_key(ObLogicalPlan *logical_plan,
                                           ObPhysicalPlan*& physical_plan,
                                           int64_t column_idx,
                                           uint64_t table_id,
                                           uint64_t column_id,
                                           ObUpdateStmt *update_stmt)
{
    int ret = OB_SUCCESS;
    bool err = false;
    uint64_t expr_id = OB_INVALID_ID;
    uint64_t where_cid = OB_INVALID_ID;
    int32_t condition_size = update_stmt->get_condition_size();
    for (int32_t i = 0; i < condition_size; i++)
    {
        ObSqlExpression where_expr;
        int64_t cond_op = OB_INVALID_INDEX;
        ObObj where_obj_value;
        ObPostfixExpression::ObPostExprNodeType val_type = ObPostfixExpression::BEGIN_TYPE;
        ObSqlRawExpr *cnd_expr = logical_plan->get_expr(update_stmt->get_condition_id(i));
        OB_ASSERT(cnd_expr);
        //fill where expr
        if (OB_SUCCESS != (ret = cnd_expr->fill_sql_expression(where_expr, this, logical_plan, physical_plan)))
        {
            TBSYS_LOG(WARN, "Failed to fill expression");
            break;
        }
        else if (!(where_expr.is_simple_condition(false, where_cid, cond_op, where_obj_value, &val_type)))
        {
            //not simple condition
            break;
        }
        if ((OB_SUCCESS == ret) && (column_id == where_cid))//find in where clause
        {
            ObSqlRawExpr * set_raw_expr = NULL;
            ObSqlExpression set_expr;
            ObObj set_obj_value;
            //get current set_expr
            if (OB_SUCCESS != (ret = update_stmt->get_update_expr_id(column_idx, expr_id)))
            {
                TBSYS_LOG(WARN, "fail to get update expr for table %lu column %lu. column_idx=%ld", table_id, column_id, column_idx);
                break;
            }
            else if (NULL == (set_raw_expr = logical_plan->get_expr(expr_id)))
            {
                TBSYS_LOG(WARN, "fail to get expr from logical plan for table %lu column %lu. column_idx=%ld", table_id, column_id, column_idx);
                break;
            }
            //fill set expr
            else if (OB_SUCCESS != (ret = set_raw_expr->fill_sql_expression(set_expr, this, logical_plan, physical_plan)))
            {
                TBSYS_LOG(WARN, "Failed to fill expression");
                break;
            }
            else
            {
                bool is_const_expr = false;
                set_expr.set_tid_cid(table_id, column_id);
                if (OB_SUCCESS != (ret = set_expr.is_const_expr(is_const_expr)))
                {
                    TBSYS_LOG(WARN,"judge is const failed");
                    break;
                }
                else//we got one
                {
                    if (is_const_expr)
                    {
                        ObPostfixExpression::ExprArray &post_expr_array = set_expr.get_expr_array();
                        set_obj_value = post_expr_array[1];
                        if((where_obj_value == set_obj_value) && (T_OP_EQ == cond_op))
                        {
                            err = true;
                            break;
                        }
                        else
                        {
                            //do nothing
                        }
                    }
                }
            }//end else
        }//end if
    }//end for
    return err;
}
//add 20151019:e

//add lbzhong [Update rowkey] 20160321:b
//update rowkey: check if new rowkey is existed!
int ObTransformer::cons_in_filter_and_expr_values(ObLogicalPlan *logical_plan, ObPhysicalPlan*& physical_plan,
                                ObTableRpcScan*& table_rpc_scan_op, ObMultipleGetMerge *&fuse_op,
                                uint64_t table_id, const common::ObRowDesc *row_desc,
                                const common::ObRowkeyInfo *rowkey_info, ObStmt *stmt,
                                ObProject *project, ErrStat &err_stat, ObArray<ObSqlExpression*> &filter_array,
                                ObUpdateDBSemFilter*& update_sem, ObExprValues* get_param_values)
{
  int &ret = err_stat.err_code_ = OB_SUCCESS;
  ObTableRpcScan *table_rpc_scan_for_update_rowkey = NULL;
  ObSqlExpression *in_filter = ObSqlExpression::alloc();
  bool is_duplicate = false;
  if (NULL == in_filter)
  {
      TRANS_LOG("no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (NULL == CREATE_PHY_OPERRATOR(table_rpc_scan_for_update_rowkey, ObTableRpcScan, physical_plan, err_stat))
  {
      ObSqlExpression::free(in_filter);
      TRANS_LOG("no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if(NULL == CREATE_PHY_OPERRATOR(update_sem, ObUpdateDBSemFilter, physical_plan, err_stat))
  {
      ObSqlExpression::free(in_filter);
      TRANS_LOG("no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if ((ret = update_sem->set_child(0, *fuse_op)) != OB_SUCCESS)
  {
      ObSqlExpression::free(in_filter);
      TRANS_LOG("Failed to set child, err=%d", ret);
  }
  else
  {
    //------------------------------------------ get old row
    TableItem *table_item = NULL;
    ObRpcScanHint hint;
    if (table_id == OB_INVALID_ID
            || (table_item = stmt->get_table_item_by_id(table_id)) == NULL
            || TableItem::BASE_TABLE != table_item->type_)
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("Wrong table id, tid=%lu", table_id);
    }
    else
    {
      hint.read_method_ = ObSqlReadStrategy::USE_SCAN;
      hint.read_consistency_ = STRONG;
    }
    if(OB_SUCCESS != ret)
    {
      // do nothing
    }
    else if ((ret = table_rpc_scan_for_update_rowkey->set_table(table_item->table_id_, table_item->ref_id_)) != OB_SUCCESS)
    {
        TBSYS_LOG(ERROR, "ObTableRpcScan set table failed");
    }
    else if (OB_SUCCESS != (ret = table_rpc_scan_for_update_rowkey->init(sql_context_, &hint)))
    {
        TBSYS_LOG(ERROR, "ObTableRpcScan init failed");
    }
    else
    {
      table_rpc_scan_for_update_rowkey->set_rowkey_cell_count(row_desc->get_rowkey_cell_count());
      table_rpc_scan_for_update_rowkey->set_need_cache_frozen_data(true);

      int64_t rowkey_size = rowkey_info->get_size();
      //add filters
      for(int32_t i = 0; i < filter_array.count(); i++)
      {
        if(OB_SUCCESS != (ret = table_rpc_scan_for_update_rowkey->add_filter(filter_array.at(i))))
        {
          TRANS_LOG("Failed to add filter, err=%d", ret);
          break;
        }
      } // end for

      // add output columns
      if(OB_SUCCESS == ret)
      {
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        for (int64_t i = 0; i < row_desc->get_column_num(); ++i)
        {
            if (OB_SUCCESS != (ret = row_desc->get_tid_cid(i, tid, cid)))
            {
                TRANS_LOG("Failed to get tid cid");
                break;
            }
            else if (tid == table_item->table_id_)
            {
                ObBinaryRefRawExpr col_expr(tid, cid, T_REF_COLUMN);
                ObSqlRawExpr col_raw_expr(
                            common::OB_INVALID_ID,
                            tid,
                            cid,
                            &col_expr);
                ObSqlExpression output_expr;
                if ((ret = col_raw_expr.fill_sql_expression(
                         output_expr,
                         this,
                         logical_plan,
                         physical_plan)) != OB_SUCCESS
                        || (ret = table_rpc_scan_for_update_rowkey->add_output_column(output_expr)) != OB_SUCCESS)
                {
                    TRANS_LOG("Add table output columns faild");
                    break;
                }
            }
        }
      }
      if(OB_SUCCESS == ret)
      {
        table_rpc_scan_for_update_rowkey->get_phy_plan()->set_curr_frozen_version(sql_context_->session_info_->get_frozen_version());
        table_rpc_scan_for_update_rowkey->get_phy_plan()->set_result_set(sql_context_->session_info_->get_current_result_set());

        //LBZ_LOG(INFO, "table_rpc_scan_for_update_rowkey=%s", to_cstring(*table_rpc_scan_for_update_rowkey));
        if(OB_SUCCESS != (ret = table_rpc_scan_for_update_rowkey->open()))
        {
            TBSYS_LOG(ERROR, "Fail to open table_rpc_scan_for_update_rowkey");
        }
        else
        {
          const ObRow* row = NULL;
          // only one row
          ret = table_rpc_scan_for_update_rowkey->get_next_row(row);
          if(OB_ITER_END == ret)
          {
            ret = OB_SUCCESS;
          }
          else
          {
            is_duplicate = true;
            in_filter->set_tid_cid(0, 0);
            ExprItem expr_item;
            ObConstRawExpr col_val;
            expr_item.type_ = T_REF_COLUMN;
            expr_item.value_.cell_.tid = table_id;
            //------------------------------------------ left column
            for (int64_t rowkey_idx = 0; rowkey_idx < rowkey_size; rowkey_idx++)
            {
              uint64_t rowkey_cid = OB_INVALID_ID;
              if(OB_SUCCESS != (ret = rowkey_info->get_column_id(rowkey_idx, rowkey_cid)))
              {
                TBSYS_LOG(ERROR, "rowkey_info.get_column_id() fail, err=%d", ret);
                break;
              }
              else
              {
                expr_item.value_.cell_.cid = rowkey_cid;
                in_filter->add_expr_item(expr_item);
              }
            }
            //------------------------------------------ left row
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                expr_item.type_ = T_OP_ROW;
                expr_item.value_.int_ = rowkey_size;
                if (OB_SUCCESS != (ret = in_filter->add_expr_item(expr_item)))
                {
                    TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
                }
            }
            //------------------------------------------ left param end
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
                expr_item.type_ = T_OP_LEFT_PARAM_END;
                expr_item.value_.int_ = 2;
                if (OB_SUCCESS != (ret = in_filter->add_expr_item(expr_item)))
                {
                    TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
                }
            }
            //------------------------------------------ calculate new row
            bool is_rowkey_changed = false;
            ObVector<ObSqlExpression*> expr_for_get_param_values;
            ObRow new_row = *row;
            if(NULL == project)
            {
              //replace new rowkey value
              for (int64_t rowkey_idx = 0; rowkey_idx < rowkey_size; rowkey_idx++)
              {
                uint64_t expr_id = OB_INVALID_ID;
                uint64_t rowkey_cid = OB_INVALID_ID;
                if(OB_SUCCESS != (ret = rowkey_info->get_column_id(rowkey_idx, rowkey_cid)))
                {
                  TBSYS_LOG(ERROR, "rowkey_info.get_column_id() fail, err=%d", ret);
                  break;
                }
                else if(OB_SUCCESS == stmt->get_update_expr_id(rowkey_cid, expr_id)) // get value from update stmt
                {
                  const ObObj *value = NULL;
                  ObSqlRawExpr *update_expr = logical_plan->get_expr(expr_id);
                  OB_ASSERT(update_expr);
                  ObSqlExpression update_filter;
                  if (OB_SUCCESS != (ret = update_expr->fill_sql_expression(update_filter)))
                  {
                    TBSYS_LOG(ERROR, "Failed to fill expression, err=%d", ret);
                    break;
                  }
                  else if(OB_SUCCESS != (ret = update_filter.calc(*row, value)))
                  {
                    TBSYS_LOG(WARN, "failed to calculate, err=%d", ret);
                    break;
                  }
                  else
                  {
                    const ObObj *old_cell = NULL;
                    char buf[OB_MAX_ROW_LENGTH];
                    ObString cast_buffer;
                    cast_buffer.assign_ptr(buf, OB_MAX_ROW_LENGTH);
                    ObObj tmp_value = *value;
                    if (OB_SUCCESS != (ret = obj_cast(tmp_value, rowkey_info->get_column(rowkey_idx)->type_, cast_buffer)))
                    {
                      TBSYS_LOG(WARN, "failed to cast obj, value=%s, type=%d, err=%d", to_cstring(tmp_value), rowkey_info->get_column(rowkey_idx)->type_, ret);
                      break;
                    }
                    else if(OB_SUCCESS != (ret = new_row.get_cell(table_id, rowkey_cid, old_cell))) // get value from old row
                    {
                      TBSYS_LOG(ERROR, "get_cell fail, err=%d", ret);
                      break;
                    }
                    else
                    {
                      ObObj old_value = *old_cell;
                      if(OB_SUCCESS != (ret = new_row.set_cell(table_id, rowkey_cid, tmp_value)))
                      {
                        TBSYS_LOG(WARN, "failed to set cell, err=%d", ret);
                        break;
                      }
                      else if(!is_rowkey_changed && old_value != tmp_value) // rowkey is changed in fact
                      {
                        is_rowkey_changed = true;
                      }
                    }
                  }
                }
              }
            }
            else
            {
              //replace update sequence value
              if(OB_SUCCESS != (ret = project->fill_sequence_info()))
              {
                TBSYS_LOG(WARN, "fail to fill sequence info!, ret=%d",ret);
              }
              else
              {
                common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM,
                    ModulePageAllocator,
                    ObArrayExpressionCallBack<ObSqlExpression> > &out_put_columns = project->get_output_columns_for_update();
                for (int64_t i = rowkey_info->get_size(); i < out_put_columns.count(); ++i)
                {
                  const ObObj *value = NULL;
                  ObSqlExpression &expr = out_put_columns.at(i);
                  uint64_t column_id = expr.get_column_id();
                  if(rowkey_info->is_rowkey_column(column_id))
                  {
                    if(OB_SUCCESS != (ret = expr.calc(*row, value)))
                    {
                      TBSYS_LOG(WARN, "failed to calculate, err=%d", ret);
                      break;
                    }
                    else
                    {
                      const ObObj *old_cell = NULL;
                      char buf[OB_MAX_ROW_LENGTH];
                      ObString cast_buffer;
                      cast_buffer.assign_ptr(buf, OB_MAX_ROW_LENGTH);
                      ObObj tmp_value = *value;
                      ObRowkeyColumn column;
                      int64_t rowkey_idx = 0;
                      if(OB_SUCCESS != (ret = rowkey_info->get_index(column_id, rowkey_idx, column)))
                      {
                        TBSYS_LOG(WARN, "failed to get_index from rowkey_info, column_id=%ld, err=%d", column_id, ret);
                        break;
                      }
                      else if (OB_SUCCESS != (ret = obj_cast(tmp_value, column.type_, cast_buffer)))
                      {
                        TBSYS_LOG(WARN, "failed to cast obj, value=%s, type=%d, err=%d", to_cstring(tmp_value), column.type_, ret);
                        break;
                      }
                      else if(OB_SUCCESS != (ret = new_row.get_cell(table_id, column_id, old_cell))) // get value from old row
                      {
                        TBSYS_LOG(ERROR, "get_cell fail, err=%d", ret);
                        break;
                      }
                      else
                      {
                        ObObj old_value = *old_cell;
                        if(OB_SUCCESS != (ret = new_row.set_cell(table_item->table_id_, column_id, tmp_value)))
                        {
                          TBSYS_LOG(WARN, "failed to set cell, err=%d", ret);
                          break;
                        }
                        else if(!is_rowkey_changed && old_value != tmp_value) // rowkey is changed in fact
                        {
                          is_rowkey_changed = true;
                        }
                      }
                    }
                  }
                } // end for
              }
            }
            //------------------------------------------ add new row
            if(OB_SUCCESS == ret && is_rowkey_changed)
            {
              void *tmp_row_buf = ob_malloc(sizeof(ObRow), ObModIds::OB_UPDATE_ROWKEY);
              if (NULL == tmp_row_buf)
              {
                TBSYS_LOG(WARN, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
              }
              else
              {
                ModuleArena row_alloc(OB_MAX_VARCHAR_LENGTH, ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
                ObRow* tmp_row = new(tmp_row_buf) ObRow(new_row, row_alloc); // deep copy
                // add filter and output column
                for (int64_t rowkey_idx = 0; OB_SUCCESS == ret && rowkey_idx < rowkey_size; rowkey_idx++)
                {
                  uint64_t rowkey_cid = OB_INVALID_ID;
                  const ObObj *cell = NULL;
                  ObConstRawExpr col_expr; // for get_param_values
                  if(OB_SUCCESS != (ret = rowkey_info->get_column_id(rowkey_idx, rowkey_cid)))
                  {
                    TBSYS_LOG(ERROR, "rowkey_info.get_column_id() fail, err=%d", ret);
                    break;
                  }
                  else if(OB_SUCCESS != (ret = tmp_row->get_cell(table_id, rowkey_cid, cell))) // get value from new row
                  {
                    TBSYS_LOG(ERROR, "get_cell fail, err=%d", ret);
                    break;
                  }
                  else if(OB_SUCCESS != (ret = col_val.set_value_and_type(*cell)))
                  {
                    TBSYS_LOG(WARN,"fail to set column value,err=%d",ret);
                    break;
                  }
                  else if(OB_SUCCESS != (ret = col_expr.set_value_and_type(*cell)))
                  {
                    TBSYS_LOG(WARN,"fail to set column expr,err=%d",ret);
                    break;
                  }
                  else
                  {
                    // add col_expr to get_param_values
                    ObSqlRawExpr col_raw_expr(
                                common::OB_INVALID_ID,
                                table_id,
                                rowkey_cid,
                                &col_expr);
                    // add col_val to in_filter
                    ObSqlExpression *output_expr = ObSqlExpression::alloc();
                    if (NULL == output_expr)
                    {
                      TBSYS_LOG(ERROR, "no memory");
                      ret = OB_ALLOCATE_MEMORY_FAILED;
                      break;
                    }
                    else if(OB_SUCCESS != (ret = col_val.fill_sql_expression(*in_filter)))
                    {
                      TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
                      ObSqlExpression::free(output_expr);
                      break;
                    }
                    else if ((ret = col_raw_expr.fill_sql_expression(*output_expr)) != OB_SUCCESS)
                    {
                        TBSYS_LOG(WARN, "Add table output columns failed");
                        ObSqlExpression::free(output_expr);
                        break;
                    }
                    else if (OB_SUCCESS != (ret = expr_for_get_param_values.push_back(output_expr)))
                    {
                        TBSYS_LOG(WARN, "Failed to push_back expr, err=%d", ret);
                        ObSqlExpression::free(output_expr);
                        break;
                    }
                  }
                } // end for
                tmp_row->~ObRow(); //free row memory
                ob_free(tmp_row);
                tmp_row = NULL;
              }
            }
            //------------------------------------------ right new row
            if(OB_SUCCESS == ret && is_rowkey_changed)
            {
                expr_item.type_ = T_OP_ROW;
                expr_item.value_.int_ = rowkey_size;
                if (OB_SUCCESS != (ret = in_filter->add_expr_item(expr_item)))
                {
                    TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
                }
            }
            //------------------------------------------ right old value
            if (OB_LIKELY(OB_SUCCESS == ret))
            {
              const ObObj *cell = NULL;
              for (int64_t rowkey_idx = 0; rowkey_idx < rowkey_size; rowkey_idx++)
              {
                uint64_t rowkey_cid = OB_INVALID_ID;
                if(OB_SUCCESS != (ret = rowkey_info->get_column_id(rowkey_idx, rowkey_cid)))
                {
                  TBSYS_LOG(ERROR, "rowkey_info.get_column_id() fail, err=%d", ret);
                  break;
                }
                else if(OB_SUCCESS != (ret = row->get_cell(table_id, rowkey_cid, cell)))
                {
                  TBSYS_LOG(ERROR, "get_cell fail, err=%d", ret);
                  break;
                }
                else if(OB_SUCCESS != (ret = col_val.set_value_and_type(*cell)))
                {
                  TBSYS_LOG(WARN,"fail to set column value,err=%d",ret);
                  break;
                }
                else if(OB_SUCCESS != (ret = col_val.fill_sql_expression(*in_filter)))
                {
                  TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
                  break;
                }
              }
            }
            //------------------------------------------ right old row
            if(OB_SUCCESS == ret)
            {
                expr_item.type_ = T_OP_ROW;
                expr_item.value_.int_ = rowkey_size;
                if (OB_SUCCESS != (ret = in_filter->add_expr_item(expr_item)))
                {
                    TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
                }
            }
            //------------------------------------------ end row
            if(OB_SUCCESS == ret)
            {
                expr_item.type_ = T_OP_ROW;
                if(is_rowkey_changed)
                {
                  expr_item.value_.int_ = 2; // old row + new row = 2
                }
                else
                {
                  expr_item.value_.int_ = 1; // old row = 1
                }
                if (OB_SUCCESS != (ret = in_filter->add_expr_item(expr_item)))
                {
                    TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
                }
            }
            //------------------------------------------ end in
            if(OB_SUCCESS == ret)
            {
                expr_item.type_ = T_OP_IN;
                expr_item.value_.int_ = 2;
                if (OB_SUCCESS != (ret = in_filter->add_expr_item(expr_item)))
                {
                    TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = in_filter->add_expr_item_end()))
                {
                    TBSYS_LOG(WARN,"Failed to add expr item end, err=%d", ret);
                }
            }

            if(OB_SUCCESS == ret && is_rowkey_changed)
            {
              OB_ASSERT(expr_for_get_param_values.size() == rowkey_size);
              //add NULL value to get_param_values
              uint64_t nonkey_tid = OB_INVALID_ID;
              uint64_t nonkey_cid = OB_INVALID_ID;
              for (int64_t j = rowkey_size; j < row_desc->get_column_num(); ++j)
              {
                if (OB_SUCCESS != (ret = row_desc->get_tid_cid(j, nonkey_tid, nonkey_cid)))
                {
                  TBSYS_LOG(WARN, "Failed to get tid cid");
                  break;
                }
                else
                {
                  ObConstRawExpr col_expr;
                  ObObj new_row_obj;
                  new_row_obj.set_int(ObActionFlag::OP_NEW_ROW);
                  col_expr.set_value_and_type(new_row_obj);
                  ObSqlRawExpr col_raw_expr(
                              common::OB_INVALID_ID,
                              nonkey_tid,
                              nonkey_cid,
                              &col_expr);
                  ObSqlExpression *output_expr = ObSqlExpression::alloc();
                  if ((ret = col_raw_expr.fill_sql_expression(*output_expr)) != OB_SUCCESS)
                  {
                      TBSYS_LOG(WARN, "Add table output columns failed");
                      ObSqlExpression::free(output_expr);
                      break;
                  }
                  else if (OB_SUCCESS != (ret = expr_for_get_param_values.push_back(output_expr)))
                  {
                      TBSYS_LOG(WARN, "Failed to add cell into get param, err=%d", ret);
                      ObSqlExpression::free(output_expr);
                      break;
                  }
                }
              }
              //------------------------------------------ move value
              for(int64_t i = 0; i < get_param_values->get_values().count(); i++) // copy old row values
              {
                ObSqlExpression *filter = ObSqlExpression::alloc();
                *filter = get_param_values->get_values().at(i); // copy
                expr_for_get_param_values.push_back(filter);
              }
              get_param_values->get_values().clear(); // clear old row values
              //------------------------------------------ add value
              for(int32_t i = 0; i < expr_for_get_param_values.size(); ++i)
              {
                if (OB_SUCCESS != (ret = get_param_values->add_value(*(expr_for_get_param_values[i]))))
                {
                    TBSYS_LOG(WARN, "Failed to add cell into get param, err=%d", ret);
                    break;
                }
              }
            }
            else // free
            {
              for(int32_t i = 0; i < expr_for_get_param_values.size(); ++i)
              {
                ObSqlExpression::free(expr_for_get_param_values[i]);
              }
              expr_for_get_param_values.clear();
            }
          }
        }
        if(OB_SUCCESS != (table_rpc_scan_for_update_rowkey->close()))
        {
          TBSYS_LOG(WARN,"Failed to close table_rpc_scan_for_update_rowkey");
        }
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(is_duplicate)
    {
      if(OB_SUCCESS != (ret = table_rpc_scan_op->add_filter(in_filter)))
      {
        ObSqlExpression::free(in_filter);
        TRANS_LOG("Failed to add filter, err=%d", ret);
      }
    }
    else
    {
      ObSqlExpression::free(in_filter);
      for(int32_t i = 0; i < filter_array.count(); i++)
      {
        //mod huangcc [Update rowkey bugfix] 20161227:b
        /*if(OB_SUCCESS != (ret = table_rpc_scan_op->add_filter(filter_array.at(i))))
        {
          TRANS_LOG("Failed to add filter, err=%d", ret);
          break;
        }*/
        ObSqlExpression* expr_clone = ObSqlExpression::alloc(); // @todo temporary work around
        if (NULL == expr_clone)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "no memory");
        }
        else
        {
          *expr_clone=*filter_array.at(i);
          if(OB_SUCCESS != (ret = table_rpc_scan_op->add_filter(expr_clone)))
          {
            TRANS_LOG("Failed to add filter, err=%d", ret);
            break;
          }
        }
        //mod:e
      }
    }
  }
  return ret;
}


int ObTransformer::cons_all_row_desc(const uint64_t table_id, ObRowDesc *row_desc, ErrStat& err_stat)
{
    OB_ASSERT(sql_context_);
    OB_ASSERT(sql_context_->schema_manager_);
    int& ret = err_stat.err_code_ = OB_SUCCESS;
    const ObTableSchema *table_schema = NULL;
    if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
    {
        ret = OB_ERR_ILLEGAL_ID;
        TRANS_LOG("fail to get table schema for table[%ld]", table_id);
    }
    else
    {
        row_desc->set_rowkey_cell_count(table_schema->get_rowkey_info().get_size());
        uint64_t max_column_id = table_schema->get_max_column_id();
        const ObColumnSchemaV2* column_schema = NULL;
        for (int64_t id = OB_APP_MIN_COLUMN_ID; id <= (int64_t)max_column_id;  id++)
        {
            if(NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(table_id, id)))
            {
                TBSYS_LOG(WARN,"get column schema error!");
                ret = OB_SCHEMA_ERROR;
            }
            else if(OB_SUCCESS != (ret = row_desc->add_column_desc(table_id, id)))
            {
                TBSYS_LOG(WARN,"failed to add column desc!");
                ret = OB_ERROR;
            }
        }
    }
    return ret;
}

bool ObTransformer::get_is_update_rowkey(const ObStmt *stmt, const common::ObRowkeyInfo *rowkey_info)
{
  bool is_update_rowkey = false;
  for (int64_t rowkey_idx = 0; rowkey_idx < rowkey_info->get_size(); rowkey_idx++)
  {
    uint64_t tmp_cid = OB_INVALID_ID;
    rowkey_info->get_column_id(rowkey_idx, tmp_cid);
    if (stmt->exist_update_column(tmp_cid))
    {
      is_update_rowkey = true;
      break;
    }
  }
  return is_update_rowkey;
}

int ObTransformer::is_rowkey_duplicated(const ObStmt *stmt, const common::ObRowkeyInfo *rowkey_info)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<int64_t> rowkey_set;
  rowkey_set.create(OB_MAX_ROWKEY_COLUMN_NUMBER + 1);
  ObArray<uint64_t> update_columns;
  stmt->get_update_columns(update_columns);
  for(int64_t i = 0; OB_SUCCESS == ret && i < update_columns.count(); i++)
  {
    uint64_t column_id = update_columns.at(i);
    if(rowkey_info->is_rowkey_column(column_id))
    {
      if (common::hash::HASH_EXIST == rowkey_set.exist(column_id)) // check duplicated key
      {
        ret = OB_DUPLICATE_COLUMN;
        TBSYS_LOG(WARN, "insert encounters duplicated key");
      }
      else if (common::hash::HASH_INSERT_SUCC != rowkey_set.set(column_id))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "fail to insert column_id to rowkey_set, column_id=%ld", column_id);
      }
    }
  }
  rowkey_set.destroy();
  return ret;
}

int ObTransformer::add_full_output_columns(ObTableRpcScan *&table_rpc_scan_for_obvalues, const uint64_t table_id,
                                           ObLogicalPlan *logical_plan, ObPhysicalPlan*& physical_plan,
                                           const ObRowDesc &row_desc, ErrStat& err_stat)
{
  int &ret = err_stat.err_code_ = OB_SUCCESS;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  for (int64_t i = 0; i < row_desc.get_column_num(); ++i)
  {
      if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, cid)))
      {
          TRANS_LOG("Failed to get tid cid");
          break;
      }
      else
      {
          if (tid == table_id)
          {
              ObBinaryRefRawExpr col_expr(tid, cid, T_REF_COLUMN);
              ObSqlRawExpr col_raw_expr(
                          common::OB_INVALID_ID,
                          tid,
                          cid,
                          &col_expr);
              ObSqlExpression output_expr;
              if ((ret = col_raw_expr.fill_sql_expression(
                       output_expr,
                       this,
                       logical_plan,
                       physical_plan)) != OB_SUCCESS
                      || (ret = table_rpc_scan_for_obvalues->add_output_column(output_expr)) != OB_SUCCESS)
              {
                  TRANS_LOG("Add table output columns faild");
                  break;
              }
          }
      }
  }
  return ret;
}

int ObTransformer::add_empty_value_for_nonrowkey_column(ObExprValues*& get_param_values, const ObRowkeyInfo &rowkey_info,
                                                        const ObRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  uint64_t rowkey_tid = OB_INVALID_ID;
  uint64_t rowkey_cid = OB_INVALID_ID;
  for (int64_t j = rowkey_info.get_size(); j < row_desc.get_column_num(); ++j)
  {
    if (OB_SUCCESS != (ret = row_desc.get_tid_cid(j, rowkey_tid, rowkey_cid)))
    {
      TBSYS_LOG(WARN, "Failed to get tid cid");
      break;
    }
    else
    {
      ObConstRawExpr col_expr2;
      ObObj null_obj;
      col_expr2.set_value_and_type(null_obj);
      ObSqlRawExpr col_raw_expr2(
                  common::OB_INVALID_ID,
                  rowkey_tid,
                  rowkey_cid,
                  &col_expr2);
      ObSqlExpression output_expr2;
      if ((ret = col_raw_expr2.fill_sql_expression(output_expr2)) != OB_SUCCESS)
      {
          TBSYS_LOG(WARN, "Add table output columns failed");
          break;
      }
      else if (OB_SUCCESS != (ret = get_param_values->add_value(output_expr2)))
      {
          TBSYS_LOG(WARN, "Failed to add cell into get param, err=%d", ret);
          break;
      }
    }
  }
  return ret;
}
//add:e
//add wanglei [second index fix] 20160425:b
bool ObTransformer::is_expr_has_more_than_two_columns(ObSqlExpression * expr)
{
    return expr->is_expr_has_more_than_two_columns ();
}
int64_t ObTransformer::get_type_num(int64_t idx,int64_t type,ObSEArray<ObObj, 64> &expr_)
{
    int64_t num = 0;
    int ret = OB_SUCCESS;
    if(type == ObPostfixExpression::BEGIN_TYPE)
    {
        num = 1;
    }
    else if (type == ObPostfixExpression::OP)
    {
        num = 3;
        int64_t op_type = 0;
        if (OB_SUCCESS != (ret = expr_[idx+1].get_int(op_type)))
        {
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
        }
        else if (T_FUN_SYS == op_type)
        {
            ++num;
        }
    }
    else if (type == ObPostfixExpression::COLUMN_IDX || type == T_OP_ROW)
    {
        num = 3;
    }
    else if (type == ObPostfixExpression::CONST_OBJ ||type == ObPostfixExpression::QUERY_ID||type == ObPostfixExpression::PARAM_IDX||type==ObPostfixExpression::TEMP_VAR)//add wanglei TEMP_VAR [second index fix] 20160513
    {
        num = 2;
    }
    else if (type == ObPostfixExpression::END || type == ObPostfixExpression::UPS_TIME_OP||ObPostfixExpression::CUR_TIME_OP
             ||ObPostfixExpression::CUR_TIME_HMS_OP
             ||ObPostfixExpression::CUR_DATE_OP
             ||ObPostfixExpression::UPS_TIME_OP)
    {
        num = 1;
    }
    else
    {
        TBSYS_LOG(WARN, "Unkown type %ld", type);
        return -1;
    }
    return num;
}
//add wanglei [second index fix] 20160425:e

//add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
int ObTransformer::get_table_max_used_cid(ObSqlContext *context,
                                          const uint64_t table_id,
                                          uint64_t &max_used_cid)
{
    int ret = OB_SUCCESS;
    int32_t column_size = 0;
    const ObColumnSchemaV2 *col       = NULL;
    max_used_cid = OB_INVALID_ID;

    if (NULL == context
        || NULL == context->schema_manager_
        || OB_INVALID_ID == table_id)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR,"invalid argument!"
                "context=%p,table_id=%ld,ret=%d",
                context,table_id,ret);
    }
    else if (NULL == (col = context->schema_manager_->get_table_schema(table_id,column_size)))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR,"fail to get column schema!tid=%ld,ret=%d",table_id,ret);
    }
    else
    {
      max_used_cid  = 0;
      for (int32_t i=0;NULL!=col&&i<column_size;i++)
      {
        uint64_t cid = col[i].get_id();
        max_used_cid = (max_used_cid > cid) ? max_used_cid : cid;
      }
    }
    return ret;
}

//add duyr 20160531:e

//add dragon [Bugfix 1224] 2016-8-29 16:00:49:b
int ObTransformer::get_filter_array (
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan* physical_plan,
    uint64_t table_id,
    ObSelectStmt *select_stmt,
    Expr_Array &filter_array,
    common::ObArray<ObSqlExpression*> &fp_array)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(select_stmt); //select stmt should not be empty!
  ObBitSet<> table_bitset;
  int32_t num = 0;

  //根据table_bitset，把sql语句中与该表有关的filter和输出列都存到相应的数组里面
  int32_t bit_index = select_stmt->get_table_bit_index(table_id);
  table_bitset.add_member(bit_index);
  if (bit_index < 0)
  {
    TBSYS_LOG(ERROR, "negative bitmap values[%d],table_id=%ld" , bit_index, table_id);
  }

  num = select_stmt->get_condition_size();
  for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
  {
    ObSqlRawExpr *cnd_expr = logical_plan->get_expr(select_stmt->get_condition_id(i));
    if (cnd_expr && table_bitset.is_superset(cnd_expr->get_tables_set()))
    {
      ObSqlExpression *filter = ObSqlExpression::alloc(); //申请空间
      if (NULL == filter)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "no memory");
        break;
      }
      else if ((ret = cnd_expr->fill_sql_expression(*filter, this, logical_plan, physical_plan))
               != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "Add table filter condition faild");
        ObSqlExpression::free(filter);
        break;
      }
      else if(OB_SUCCESS != (ret = filter_array.push_back(*filter)))
      {
        TBSYS_LOG(ERROR, "push back to filter array failed");
        ObSqlExpression::free(filter);
        break;
      }
      else if(OB_SUCCESS != (ret = fp_array.push_back(filter)))
      {
        ObSqlExpression::free(filter);
        TBSYS_LOG(ERROR, "push back to filter array ptr failed");
        break;
      }
    }
  }
  return ret;
}

int ObTransformer::get_project_array (
      ObLogicalPlan *logical_plan,
      ObPhysicalPlan *physical_plan,
      uint64_t table_id,
      ObSelectStmt *select_stmt,
      Expr_Array &project_array,
      ObArray<uint64_t> &alias_exprs)
{
  int ret = OB_SUCCESS;
  UNUSED(alias_exprs);
  OB_ASSERT(select_stmt); //select stmt should not be empty!
  int32_t num = 0;
  num = select_stmt->get_column_size();
  for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
  {
    const ColumnItem *col_item = select_stmt->get_column_item(i);
    if (col_item && col_item->table_id_ == table_id)
    {
      ObBinaryRefRawExpr col_expr(col_item->table_id_, col_item->column_id_, T_REF_COLUMN);
      ObSqlRawExpr col_raw_expr(
            common::OB_INVALID_ID,
            col_item->table_id_,
            col_item->column_id_,
            &col_expr);
      ObSqlExpression output_expr;
      if ((ret = col_raw_expr.fill_sql_expression(
             output_expr,
             this,
             logical_plan,
             physical_plan)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "Add table output columns faild");
        break;
      }
      else
      {
        project_array.push_back(output_expr);
      }
    }
  }

  ObBitSet<> table_bitset;
  int32_t bit_index = select_stmt->get_table_bit_index(table_id);
  table_bitset.add_member(bit_index);
  if (bit_index < 0)
  {
    TBSYS_LOG(ERROR, "negative bitmap values[%d],table_id=%ld" , bit_index, table_id);
  }
  if (ret == OB_SUCCESS && select_stmt)
  {
    num = select_stmt->get_select_item_size();
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
      const SelectItem& select_item = select_stmt->get_select_item(i);
      if (select_item.is_real_alias_)
      {
        ObSqlRawExpr *alias_expr = logical_plan->get_expr(select_item.expr_id_);
        if (alias_expr && alias_expr->is_columnlized() == false
            && table_bitset.is_superset(alias_expr->get_tables_set()))
        {
          ObSqlExpression output_expr;
          if ((ret = alias_expr->fill_sql_expression(
                 output_expr,
                 this,
                 logical_plan,
                 physical_plan)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "Add table output columns faild");
            break;
          }
          else
          {
            project_array.push_back(output_expr);
          }
          //alias_exprs.push_back(select_item.expr_id_);
          //alias_expr->set_columnlized(true);
        }
      }
    }
  }
  return ret;
}
//add 2016-8-29 16:00:57e

