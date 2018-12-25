#include "ob_logical_plan.h"
#include "ob_select_stmt.h"
#include "ob_delete_stmt.h"
#include "ob_insert_stmt.h"
#include "ob_update_stmt.h"
#include "ob_show_stmt.h"
#include "ob_execute_stmt.h"
#include "ob_prepare_stmt.h"
#include "ob_sql_session_info.h"
#include "parse_malloc.h"

namespace oceanbase
{
  namespace sql
  {
    using namespace oceanbase::sql;
    using namespace oceanbase::common;

    ObLogicalPlan::ObLogicalPlan(ObStringBuf* name_pool)
      : name_pool_(name_pool)
    {
      question_marks_count_ = 0;
      //del liuzy [datetime func] 20150909:b
      /*Exp: modify ObCurTimeType to ObArray<ObCurTimeType>, don't need init*/
//      cur_time_fun_type_ = NO_CUR_TIME;
      //del 20150909:e
      // from valid max id desc
      new_gen_tid_ = UINT16_MAX - 2;
      new_gen_cid_ = OB_MAX_TMP_COLUMN_ID;
      new_gen_qid_ = 1;
      new_gen_eid_ = 1;
      new_gen_wid_ = 1;
      cur_time_fun_type_idx_ = 0;//add liuzy [datetime func] 20150909
    }

    ObLogicalPlan::~ObLogicalPlan()
    {
      for(int32_t i = 0; i < stmts_.size(); ++i)
      {
        //delete stmts_[i];
        stmts_[i]->~ObBasicStmt();
        parse_free(stmts_[i]);
      }
      stmts_.clear();

      for(int32_t i = 0; i < exprs_.size(); ++i)
      {
        //delete exprs_[i];
        exprs_[i]->~ObSqlRawExpr();
        parse_free(exprs_[i]);
      }
      exprs_.clear();

      for(int32_t i = 0; i < raw_exprs_store_.size(); ++i)
      {
        if (raw_exprs_store_[i])
        {
          raw_exprs_store_[i]->~ObRawExpr();
          parse_free(raw_exprs_store_[i]);
        }
      }
      raw_exprs_store_.clear();
      cur_time_fun_type_.clear();//add liuzy [datetime func] 20150909
    }

    ObBasicStmt* ObLogicalPlan::get_query(uint64_t query_id) const
    {
      ObBasicStmt *stmt = NULL;
      int32_t num = stmts_.size();
      for (int32_t i = 0; i < num; i++)
      {
        if (stmts_[i]->get_query_id() == query_id)
        {
          stmt = stmts_[i];
          break;
        }
      }
      OB_ASSERT(NULL != stmt);
      return stmt;
    }

    ObSelectStmt* ObLogicalPlan::get_select_query(uint64_t query_id) const
    {
      ObSelectStmt *select_stmt = NULL;
      int32_t num = stmts_.size();
      for (int32_t i = 0; i < num; i++)
      {
        if (stmts_[i]->get_query_id() == query_id)
        {
          select_stmt = static_cast<ObSelectStmt*>(stmts_[i]);
          break;
        }
      }
      OB_ASSERT(NULL != select_stmt);
      return select_stmt;
    }
    //add wanglei:b
    oceanbase::common::ObVector<ObSqlRawExpr*> & ObLogicalPlan::get_expr_list()
    {
        return exprs_;
    }
    int32_t ObLogicalPlan::get_expr_list_num()const
    {
        return  exprs_.size();
    }
    ObSqlRawExpr* ObLogicalPlan::get_expr_for_something(int32_t no) const
    {
        ObSqlRawExpr *expr = NULL;
        int32_t num = exprs_.size();
        if(no<0||no>num)
            return expr;
        else
        {
            expr = exprs_[no];
            return expr;
        }

    }
    //add:e

    ObSqlRawExpr* ObLogicalPlan::get_expr(uint64_t expr_id) const
    {
      ObSqlRawExpr *expr = NULL;
      int32_t num = exprs_.size();
      for (int32_t i = 0; i < num; i++)
      {
        if (exprs_[i]->get_expr_id() == expr_id)
        {
          expr = exprs_[i];
          break;
        }
      }
      OB_ASSERT(NULL != expr);
      return expr;
    }

    int ObLogicalPlan::fill_result_set(ObResultSet& result_set, ObSQLSessionInfo* session_info, common::ObIAllocator &alloc)
    {
      int ret = OB_SUCCESS;
      result_set.set_affected_rows(0);
      result_set.set_warning_count(0);
      result_set.set_message("");

      ObSelectStmt *select_stmt = NULL;
      ObResultSet::Field field;
      switch(stmts_[0]->get_stmt_type())
      {
        case ObStmt::T_PREPARE:
        {
          ObPrepareStmt *prepare_stmt = static_cast<ObPrepareStmt*>(stmts_[0]);
          ObBasicStmt *stmt = get_query(prepare_stmt->get_prepare_query_id());
          if (stmt == NULL)
          {
            ret = OB_ERR_ILLEGAL_ID;
            TBSYS_LOG(WARN, "wrong prepared query id, id = %lu, ret = %d", prepare_stmt->get_prepare_query_id(), ret);
            break;
          }
          // only select prepare statement need to fill result set
          else if (stmt->get_stmt_type() != ObBasicStmt::T_SELECT)
          {
            break;
          }
          else
          {
            select_stmt = static_cast<ObSelectStmt*>(stmt);
            /* then get through */
          }
        }
        case ObStmt::T_SELECT:
        {
          if (stmts_[0]->get_stmt_type() == ObBasicStmt::T_SELECT)
            select_stmt = static_cast<ObSelectStmt*>(stmts_[0]);
          if (select_stmt == NULL)
          {
            ret = OB_ERR_PARSE_SQL;
            TBSYS_LOG(WARN, "logical plan of select statement error");
            break;
          }
          int32_t size = select_stmt->get_select_item_size();
          for (int32_t i = 0; ret == OB_SUCCESS && i < size; i++)
          {
            const SelectItem& select_item = select_stmt->get_select_item(i);
            if ((ret = ob_write_string(alloc, select_item.expr_name_, field.cname_)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "fail to alloc string[%.*s] ret=%d", select_item.expr_name_.length(), select_item.expr_name_.ptr(), ret);
              break;
            }

            ObSqlRawExpr* sql_expr = get_expr(select_item.expr_id_);
            if (sql_expr == NULL)
            {
              TBSYS_LOG(WARN, "fail to get expr. select_item.expr_id_=%lu", select_item.expr_id_);
              ret = OB_ERR_ILLEGAL_ID;
              break;
            }
            field.type_.set_type(sql_expr->get_result_type());
            ObRawExpr* expr = sql_expr->get_expr();
            if (select_stmt->get_set_op() != ObSelectStmt::NONE)
            {
              if ((ret = ob_write_string(alloc, select_item.expr_name_, field.org_cname_)) != OB_SUCCESS)
              {
                TBSYS_LOG(WARN, "fail to alloc string[%.*s] ret=%d", select_item.expr_name_.length(), select_item.expr_name_.ptr(), ret);
                break;
              }
            }
            else if (expr->get_expr_type() == T_REF_COLUMN)
            {
              ObBinaryRefRawExpr *column_expr = static_cast<ObBinaryRefRawExpr*>(expr);
              uint64_t table_id = column_expr->get_first_ref_id();
              uint64_t column_id = column_expr->get_second_ref_id();
              if (table_id != OB_INVALID_ID)
              {
                ColumnItem *column_item = select_stmt->get_column_item_by_id(table_id, column_id);
                if (column_item == NULL)
                {
                  TBSYS_LOG(WARN, "fail to get column item by id. table_id=%lu column_id=%lu", table_id, column_id);
                  ret = OB_ERR_ILLEGAL_ID;
                  break;
                }
                if (OB_SUCCESS != (ret = ob_write_string(alloc, column_item->column_name_, field.org_cname_)))
                {
                  TBSYS_LOG(WARN, "fail to alloc string[%.*s] ret=%d", column_item->column_name_.length(), column_item->column_name_.ptr(), ret);
                  break;
                }
                TableItem *table_item = select_stmt->get_table_item_by_id(table_id);
                if (table_item == NULL)
                {
                  TBSYS_LOG(WARN, "fail to get table item by id. table_id=%lu", table_id);
                  ret = OB_ERR_ILLEGAL_ID;
                  break;
                }
                if (table_item->alias_name_.length() > 0)
                {
                  if (OB_SUCCESS != (ret = ob_write_string(alloc, table_item->alias_name_, field.tname_)))
                  {
                    TBSYS_LOG(WARN, "fail to alloc string[%.*s] ret=%d", table_item->alias_name_.length(), table_item->alias_name_.ptr(), ret);
                    break;
                  }
                }
                else
                {
                  if (OB_SUCCESS != (ret = ob_write_string(alloc, table_item->table_name_, field.tname_)))
                  {
                    TBSYS_LOG(WARN, "fail to alloc string[%.*s] ret=%d", table_item->table_name_.length(), table_item->table_name_.ptr(), ret);
                    break;
                  }
                }
                if (OB_SUCCESS != (ret = ob_write_string(alloc, table_item->table_name_, field.org_tname_)))
                {
                  TBSYS_LOG(WARN, "fail to alloc string[%.*s] ret=%d", table_item->table_name_.length(), table_item->table_name_.ptr(), ret);
                  break;
                }
              }
            }
            if (OB_SUCCESS != (ret = result_set.add_field_column(field)))
            {
              TBSYS_LOG(WARN, "fail to add field column to result_set. ret=%d", ret);
              break;
            }

            field.cname_.assign(NULL, 0);
            field.org_cname_.assign(NULL, 0);
            field.tname_.assign(NULL, 0);
            field.org_tname_.assign(NULL, 0);
            field.type_.set_type(ObMinType);
          }
          break;
        }
        case ObStmt::T_EXPLAIN:
        {
          ObString tname = ObString::make_string("explain_table");
          ObString cname = ObString::make_string("Query Plan");
          field.tname_ = tname;
          field.org_tname_ = tname;
          field.cname_ = cname;
          field.org_cname_ = cname;
          field.type_.set_type(ObVarcharType);
          if (OB_SUCCESS != (ret = result_set.add_field_column(field)))
          {
            TBSYS_LOG(WARN, "fail to add field column to result_set. ret=%d", ret);
            break;
          }
          break;
        }
        case ObStmt::T_SHOW_TABLES:
        case ObStmt::T_SHOW_SYSTEM_TABLES:// add by zhangcd [multi_database.show_tables] 20150616
        case ObStmt::T_SHOW_DATABASES: // add by zhangcd [multi_database.show_databases] 20150617
        case ObStmt::T_SHOW_CURRENT_DATABASE: // add by zhangcd [multi_database.show_databases] 20150617
        //add liumengzhan_show_index [20141208]
        case ObStmt::T_SHOW_INDEX:
        //add:e
        case ObStmt::T_SHOW_VARIABLES:
        case ObStmt::T_SHOW_COLUMNS:
        case ObStmt::T_SHOW_SCHEMA:
        case ObStmt::T_SHOW_CREATE_TABLE:
        case ObStmt::T_SHOW_TABLE_STATUS:
        case ObStmt::T_SHOW_SERVER_STATUS:
        case ObStmt::T_SHOW_PARAMETERS:
        {
          ObShowStmt *show_stmt = static_cast<ObShowStmt*>(stmts_[0]);
          if (show_stmt == NULL)
          {
            TBSYS_LOG(WARN, "fail to get Show statement");
            ret = OB_ERR_PARSE_SQL;
            break;
          }
          TableItem *table_item = show_stmt->get_table_item_by_id(show_stmt->get_sys_table_id());
          if (table_item == NULL)
          {
            TBSYS_LOG(WARN, "fail to get table item by id. table_id=%lu", show_stmt->get_sys_table_id());
            ret = OB_ERR_ILLEGAL_ID;
            break;
          }
          ObString tname;
          if (OB_SUCCESS != (ret = ob_write_string(alloc, table_item->table_name_, tname)))
          {
            TBSYS_LOG(WARN, "fail to alloc string \"%.*s\" ret=%d", table_item->table_name_.length(), table_item->table_name_.ptr(), ret);
            break;
          }
          field.tname_ = tname;
          field.org_tname_ = tname;
          for (int32_t i = 0; ret == OB_SUCCESS && i < show_stmt->get_column_size(); i++)
          {
            ObString cname;
            const ColumnItem *col_item = show_stmt->get_column_item(i);
            if (col_item != NULL)
            {
              if (OB_SUCCESS != (ret = ob_write_string(alloc, col_item->column_name_, cname)))
              {
                TBSYS_LOG(WARN, "fail to alloc string \"%.*s\" ret=%d", col_item->column_name_.length(), col_item->column_name_.ptr(), ret);
                break;
              }
              field.cname_ = cname;
              field.org_cname_ = cname;
              field.type_.set_type(col_item->data_type_);
              if (OB_SUCCESS != (ret = result_set.add_field_column(field)))
              {
                TBSYS_LOG(WARN, "fail to add field column to result_set. ret=%d", ret);
                break;
              }
              field.cname_.assign(NULL, 0);
              field.org_cname_.assign(NULL, 0);
              field.type_.set_type(ObMinType);
            }
          }
          break;
        }
        case ObStmt::T_SHOW_WARNINGS:
        {
          ObString tname = ObString::make_string("show warnings");
          ObShowStmt *show_stmt = static_cast<ObShowStmt*>(stmts_[0]);
          if (show_stmt == NULL)
          {
            TBSYS_LOG(WARN, "fail to get Show statement");
            ret = OB_ERR_PARSE_SQL;
            break;
          }
          else
          {
            field.tname_ = tname;
            field.org_tname_ = tname;
          }
          if (show_stmt->is_count_warnings())
          {
            ObString cname = ObString::make_string("warning_count");
            field.cname_ = cname;
            field.org_cname_ = cname;
            field.type_.set_type(ObIntType);
            if (OB_SUCCESS != (ret = result_set.add_field_column(field)))
            {
              TBSYS_LOG(WARN, "fail to add field column to result_set. ret=%d", ret);
              break;
            }
          }
          else
          {
            ObString cname[3];
            cname[0] = ObString::make_string("level");
            cname[1] = ObString::make_string("code");
            cname[2] = ObString::make_string("message");
            for (int32_t i = 0; ret == OB_SUCCESS && i < 3; i++)
            {
              field.cname_ = cname[i];
              field.org_cname_ = cname[i];
              if (i == 1)
                field.type_.set_type(ObIntType);
              else
                field.type_.set_type(ObVarcharType);
              if (OB_SUCCESS != (ret = result_set.add_field_column(field)))
              {
                TBSYS_LOG(WARN, "fail to add field column to result_set. ret=%d", ret);
                break;
              }
            }
          }
          break;
        }
        case ObStmt::T_SHOW_GRANTS:
        {
          ObString tname = ObString::make_string("show grants");
          ObString cname = ObString::make_string("grants");
          field.tname_ = tname;
          field.org_tname_ = tname;
          field.cname_ = cname;
          field.org_cname_ = cname;
          field.type_.set_type(ObVarcharType);
          if (OB_SUCCESS != (ret = result_set.add_field_column(field)))
          {
            TBSYS_LOG(WARN, "fail to add field column to result_set. ret=%d", ret);
            break;
          }
          break;
        }
        case ObStmt::T_SHOW_PROCESSLIST:
        {
          ObString tname = ObString::make_string("show processlist");
          ObShowStmt *show_stmt = static_cast<ObShowStmt*>(stmts_[0]);
          if (show_stmt == NULL)
          {
            TBSYS_LOG(WARN, "fail to get Show statement");
            ret = OB_ERR_PARSE_SQL;
            break;
          }
          else
          {
            field.tname_ = tname;
            field.org_tname_ = tname;
          }
          ObString cname[10]; //  | Id  | User | Host      | db   | Command | Time | State | Info|
          cname[0] = ObString::make_string("Id");
          cname[1] = ObString::make_string("User");
          cname[2] = ObString::make_string("Host");
          cname[3] = ObString::make_string("db");
          cname[4] = ObString::make_string("Command");
          cname[5] = ObString::make_string("Time");
          cname[6] = ObString::make_string("State");
          cname[7] = ObString::make_string("Info");
          cname[8] = ObString::make_string("MergeServer");
          cname[9] = ObString::make_string("Index");
          for (int32_t i = 0; ret == OB_SUCCESS && i < 10; i++)
          {
            field.cname_ = cname[i];
            field.org_cname_ = cname[i];
            if (i == 0 || i == 5 || i == 9)
            {
              field.type_.set_type(ObIntType);
            }
            else
            {
              field.type_.set_type(ObVarcharType);
            }
            if (OB_SUCCESS != (ret = result_set.add_field_column(field)))
            {
              TBSYS_LOG(WARN, "fail to add field column to result_set. ret=%d", ret);
              break;
            }
          }
          break;
        }
        case ObStmt::T_EXECUTE:
        {
          ObExecuteStmt *execute_stmt = static_cast<ObExecuteStmt*>(stmts_[0]);
          ObResultSet *stored_plan = NULL;
          if (session_info == NULL)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "Session Info is needed. ret=%d", ret);
          }
          else if (execute_stmt == NULL)
          {
            ret = OB_ERR_PARSE_SQL;
            TBSYS_LOG(WARN, "fail to get Execute statement");
          }
          else if ((stored_plan = session_info->get_plan(execute_stmt->get_stmt_name())) == NULL)
          {
            ret = OB_ERR_PREPARE_STMT_UNKNOWN;
            TBSYS_LOG(USER_ERROR, "statement %.*s not prepared",
                execute_stmt->get_stmt_name().length(), execute_stmt->get_stmt_name().ptr());
          }
          else if ((ret = result_set.from_prepared(*stored_plan)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fail to fill result set, ret=%d", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "get physical plan, addr=%p", stored_plan->get_physical_plan());
            TBSYS_LOG(DEBUG, "StoredPlan: %s", to_cstring(*stored_plan->get_physical_plan()));
          }
          break;
        }
        default:
          break;
      }
      if (ret == OB_SUCCESS && question_marks_count_ > 0)
      {
        ret = result_set.pre_assign_params_room(question_marks_count_, alloc);
      }
      //mod liuzy [datetime func] 20150909:b
      /*Exp: modify ObCurTimeType to ObArray<ObCurTimeType>*/
//      if (ret == OB_SUCCESS && NO_CUR_TIME != cur_time_fun_type_)
      if (ret == OB_SUCCESS && cur_time_fun_type_.count() != 0)
      //mod 20150909:e
      {
        ret = result_set.pre_assign_cur_time_room(alloc);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to assign cur_time room, ret=%d", ret);
        }
      }

      return ret;
    }

    void ObLogicalPlan::print(FILE* fp, int32_t level) const
    {
      int32_t i;
      fprintf(fp, "<LogicalPlan>\n");
      fprintf(fp, "    <StmtList>\n");
      for (i = 0; i < stmts_.size(); i ++)
      {
        ObBasicStmt* stmt = stmts_[i];
        stmt->print(fp, level + 2, i);
      }
      fprintf(fp, "    </StmtList>\n");
      fprintf(fp, "    <ExprList>\n");
      for (i = 0; i < exprs_.size(); i ++)
      {
        ObSqlRawExpr* sql_expr = exprs_[i];
        sql_expr->print(fp, level + 2, i);
      }
      fprintf(fp, "    </ExprList>\n");
      //add liuzy [datetime func] 20150909:b
      /*Exp: modify ObCurTimeType to ObArray<ObCurTimeType>*/
      for (i = 0; i < cur_time_fun_type_.count(); ++i)
      {
        if (CUR_TIME == cur_time_fun_type_.at(i))
        {
          fprintf(fp, "    <CurrentTimeFun>MS</CurrentTimeFun>\n");
        }
        else if (CUR_DATE == cur_time_fun_type_.at(i))
        {
          fprintf(fp, "    <CurrentDateFun>MS</CurrentDateFun>\n");
        }
        else if (CUR_TIME_HMS == cur_time_fun_type_.at(i))
        {
          fprintf(fp, "    <CurrentTimeHMSFun>MS</CurrentTimeHMSFun>\n");
        }
        else if (CUR_TIME_UPS == cur_time_fun_type_.at(i))
        {
          fprintf(fp, "    <CurrentTimeFun>UPS</CurrentTimeFun>\n");
        }
      }
      //add 20150909:e
      //del liuzy [datetime func] 20150909:b
      /*Exp: modify ObCurTimeType to ObArray<ObCurTimeType>*/
//      if (CUR_TIME == cur_time_fun_type_)
//      {
//        fprintf(fp, "    <CurrentTimeFun>MS</CurrentTimeFun>\n");
//      }
//      else if (CUR_TIME_UPS == cur_time_fun_type_)
//      {
//        fprintf(fp, "    <CurrentTimeFun>UPS</CurrentTimeFun>\n");
//      }
      //del 20150909:e
      fprintf(fp, "</LogicalPlan>\n");
    }
  }
}
