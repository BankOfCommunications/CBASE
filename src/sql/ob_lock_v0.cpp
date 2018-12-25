/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "ob_lock_v0.h"
#include "ob_schema_checker.h"
#include "ob_inc_scan.h"
#include "ob_ups_executor.h"
#include "common/ob_obj_cast.h"
namespace oceanbase
{
  namespace sql
  {
    ObLockV0::ObLockV0(ObSqlContext *sql_context)
      :sql_context_(sql_context)
    {}
    int ObLockV0::lock_by_for_update(ObRow *row,
                                     const uint64_t &table_id,
                                     bool is_lock,
                                     ObMySQLResultSet & mysql_result
                                     )
    {
      int ret = OB_SUCCESS;
      //根据row得到构造where时需要的主键列和主键值
      std::string select_part = "select *";
      std::string from_part = " from ";
      std::string where_part = " where ";
      std::string for_update_part = " for update;";
      ObSchemaChecker schema_checker;
      schema_checker.set_schema(*(sql_context_->schema_manager_));
      const ObTableSchema* table_schema = schema_checker.get_table_schema(table_id);
      if(NULL == table_schema)
      {
        ret = OB_SCHEMA_ERROR;
        TBSYS_LOG(ERROR,"can not get the table schema");
      }
      else
      {
        const char* table_name = table_schema->get_table_name();
        if(NULL == table_name)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN,"the name of table locked must not be NULL");
        }
        else
        {
          from_part += table_name;
        }
      }
      if(OB_SUCCESS==ret)
      {
        std::string column_expr = "";
        std::string equal_part = "=";
        std::string and_part = " and ";
        std::string single_quto = "'";
        const ObObj *res_cell = NULL;
        ObString temp_column_value;
        char* column_value = NULL;
        const char* column_name = NULL;
        //expected_type、char_buf、var_str、casted_cell是用来完成obj_cast函数的
        ObObj expected_type;
        expected_type.set_type(ObVarcharType);
        char var_buf[OB_MAX_VARCHAR_LENGTH];
        ObString var_str;
        var_str.assign_ptr(var_buf, OB_MAX_VARCHAR_LENGTH);
        ObObj casted_cell;
        casted_cell.set_varchar(var_str);
        //获取table_id对应的表中的主键列的个数
        int64_t rowkey_idx;
        uint64_t rowkey_column_id = OB_INVALID_ID;
        const ObObj *cell = NULL;
        const ObColumnSchemaV2* column_schema = NULL;
        const ObRowkeyInfo rowkey_info = table_schema->get_rowkey_info();
        int64_t rowkey_num = rowkey_info.get_size();
        for (rowkey_idx = 0; OB_SUCCESS == ret && rowkey_idx < rowkey_num; rowkey_idx++)
        {
          if (OB_SUCCESS != (ret = rowkey_info.get_column_id(rowkey_idx, rowkey_column_id)))
          {
            TBSYS_LOG(WARN, "fail to get table %lu column %ld. ret=%d", table_id, rowkey_idx, ret);
            break;
          }
          else if (NULL == (column_schema = schema_checker.get_column_schema(table_id, rowkey_column_id)))
          {
            ret = OB_ENTRY_NOT_EXIST;
            TBSYS_LOG(WARN, "fail to get table %lu column %lu", table_id, rowkey_column_id);
          }
          else
          {
            if (OB_SUCCESS != (ret =row->get_cell(table_id,rowkey_column_id,cell)))
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "fail to get the %ld th cell",rowkey_column_id);
            }
            else if (OB_SUCCESS != (ret = obj_cast(*cell, expected_type, casted_cell, res_cell)))
            {
              TBSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
            }
            else
            {
              res_cell->get_varchar(temp_column_value);
              column_value = new char[temp_column_value.length()+1];//bug fix ,must add 1 for '\0'
              memset(column_value, 0, sizeof(column_value));
              sprintf(column_value,"%.*s",temp_column_value.length(),temp_column_value.ptr());
              column_name =  column_schema->get_name();
              if(0==rowkey_idx)
              {
                if(ObVarcharType == cell->get_type())
                {
                  column_expr = column_name+equal_part+single_quto+column_value+single_quto;
                }
                else
                {
                  column_expr = column_name+equal_part+column_value;
                }
              }
              else
              {
                if(ObVarcharType == cell->get_type())
                {
                  column_expr = column_name+equal_part+single_quto+column_value+single_quto;
                }
                else
                {
                  column_expr += and_part+column_name+equal_part+column_value;
                }
              }
              delete []column_value;
              TBSYS_LOG(INFO,"column_expr=%s",column_expr.data());
            }
          }
        }
        if(OB_SUCCESS == ret)
        {
          if(rowkey_idx>0)
          {
            where_part += column_expr;
            TBSYS_LOG(INFO,"where part=%s",where_part.data());
          }
          else
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR,"there must be at least one column");
          }
        }
      }
      //生成加锁语句的物理计划
      if(OB_SUCCESS == ret)
      {
        //构建加锁的sql语句
        std::string temp_lock_sql = select_part+from_part+where_part+for_update_part;
        ObString lock_sql = ObString::make_string(temp_lock_sql.data());
        TBSYS_LOG(INFO,"TEMP LOCK SQL=%s",temp_lock_sql.data());
        TBSYS_LOG(INFO,"LOCK SQL=%.*s",lock_sql.length(),lock_sql.ptr());
        if (OB_SUCCESS == ret)
        {
          sql_context_->session_info_->set_current_result_set(&mysql_result);
          sql_context_->session_info_->set_session_state(QUERY_ACTIVE);
        }
        //add :e
        if (OB_SUCCESS != (ret = mysql_result.init()))
        {
          TBSYS_LOG(ERROR, "init result set failed, ret = %d", ret);
        }
        else
        {
          if (OB_SUCCESS != (ret = ObSql::direct_execute(lock_sql, mysql_result, *sql_context_)))
          {
            TBSYS_LOG(ERROR,"fail to do direct execute,ret = %d",ret);
          }
          else
          {
            //根据加锁还是解锁，设置ObIncScan操作符的标记量
            if(OB_SUCCESS == ret)
            {
              int32_t inc_scan_position = 1;
              ObPhysicalPlan *physical_plan = NULL;
              ObPhysicalPlan *inner_plan = NULL;
              ObIncScan *inc_scan = NULL;
              ObPhyOperator *main_query = NULL;
              ObPhyOperator *multi_get = NULL;
              ObPhyOperator *project = NULL;
              ObUpsExecutor* ups_executor = NULL;
              if ((physical_plan = mysql_result.get_physical_plan()) == NULL
                  || (main_query = physical_plan->get_main_query()) == NULL
                  || (ups_executor=dynamic_cast<ObUpsExecutor*>(main_query))== NULL
                  || (inner_plan = ups_executor->get_inner_plan()) == NULL
                  || (project = inner_plan->get_main_query()) == NULL
                  || (multi_get = project->get_child(0)) == NULL
                  || (inc_scan = dynamic_cast<ObIncScan*>(multi_get->get_child(inc_scan_position))) == NULL
                  )
              {
                ret = OB_NOT_INIT;
                TBSYS_LOG(ERROR, "Stored session plan cursor can not be found or not correct, physical_plan=%p main_query=%p"\
                          "ups_executor=%p,inner_plan=%p,project=%p,multi_get=%p,inc_scan=%p",
                          physical_plan, main_query, ups_executor,inner_plan,project,multi_get,inc_scan);
              }
              else if(is_lock)
              {
                inc_scan->set_cursor_write_lock_flag();
              }
              else
              {
                inc_scan->set_cursor_unwrite_lock_flag();
              }
            }
          }
          //进行加锁或解锁
          if(OB_SUCCESS == ret && OB_SUCCESS != (ret = mysql_result.open()))
          {
            TBSYS_LOG(ERROR,"fail to open lock sql,ret = %d",ret);
          }
        }
      }
      return ret;
    }
  }; // end namespace sql
}; // end namespace oceanbase
