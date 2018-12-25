/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_scan_param_decoder.cpp is for what ...
 *
 * Version: $id: ob_scan_param_decoder.cpp,v 0.1 3/29/2011 10:39a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include <time.h>
#include "ob_read_param_decoder.h"
#include "ob_ms_tsi.h"
#include "common/ob_tsi_factory.h"
#include "common/utility.h"
#include "common/ob_range2.h"
#include <vector>
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
namespace
{
  static const ObString OB_SCAN_PARAM_EMPTY_TABLE_NAME;

  int ob_decode_scan_param_select_all_column(ObScanParam & org_param,
    const ObTableSchema & table_schema,
    const ObSchemaManagerV2 & schema_mgr,
    ObScanParam &decoded_param,
    ObMSSchemaDecoderAssis &schema_assis_out,
    ObResultCode *rc)
  {
    UNUSED(rc);
    int err = OB_SUCCESS;
    uint64_t table_id = table_schema.get_table_id();
    const ObColumnSchemaV2 *column_beg = NULL;
    const ObColumnSchemaV2 *cur_column = NULL;
    int32_t column_idx_in_schema_mgr = -1;
    ObString column_name;
    int32_t column_size = 0;
    column_beg = schema_mgr.get_table_schema(table_id,column_size);
    if (NULL == column_beg || 0 >= column_size)
    {
      TBSYS_LOG(WARN,"fail to get columns of table [table_name:%s,table_id:%lu,column_size:%d,column_beg:%p]",
        table_schema.get_table_name(), table_id, column_size,column_beg);
      err = OB_SCHEMA_ERROR;
    }
    if (OB_SUCCESS == err)
    {
      decoded_param.set(table_id,OB_SCAN_PARAM_EMPTY_TABLE_NAME,*org_param.get_range());
    }
    if (OB_SUCCESS == err)
    {
      int32_t got_cell_num = 0;
      int32_t overwrite_flag = 1;
      for (int32_t i = 0; OB_SUCCESS == err && i < column_size; i ++)
      {
        cur_column = schema_mgr.get_column_schema(column_beg[i].get_table_id(), column_beg[i].get_id(),
          &column_idx_in_schema_mgr);
        if (NULL == cur_column
          || 0 > column_idx_in_schema_mgr
          || column_idx_in_schema_mgr >= OB_MAX_COLUMN_NUMBER)
        {
          TBSYS_LOG(ERROR, "unexpected error, fail to get column schema [table_id:%lu,column_id:%lu,idx:%d]",
            column_beg[i].get_table_id(), column_beg[i].get_id(),i);
          err = OB_ERR_UNEXPECTED;
        }
        /// make sure every column should add only once
        if (OB_SUCCESS == err)
        {
          column_name.assign_ptr(const_cast<char*>(column_beg[i].get_name()),
            static_cast<int32_t>(strlen(column_beg[i].get_name())));
          if (OB_SUCCESS != (err = schema_assis_out.add_column(column_name,got_cell_num, ObMSSchemaDecoderAssis::SELECT_COLUMN, overwrite_flag)))
          {
            TBSYS_LOG(WARN,"fail to add <column_name,idx> pair to hashmap [err:%d]", err);
          }
          else
          {
            err = OB_SUCCESS;
          }
          got_cell_num ++;
          if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = decoded_param.add_column(column_beg[i].get_id(), true))))
          {
            TBSYS_LOG(WARN,"fail to add column to decoded param [err:%d]", err);
          }
          else if (OB_SUCCESS == err)
          {
            /// for result process
            err = org_param.add_column(column_name);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to add column to org param [err:%d]", err);
            }
          }
        }
      }
    }
    return err;
  }

  int ob_decode_scan_param_basic_info(const ObScanParam & org_param,
    const ObTableSchema & table_schema,
    const ObSchemaManagerV2 & schema_mgr,
    ObScanParam &decoded_param,
    ObMSSchemaDecoderAssis &schema_assis_out,
    ObResultCode *rc)
  {
    int err = OB_SUCCESS;
    uint64_t table_id = OB_INVALID_ID;
    const ObColumnSchemaV2 *column_info = NULL;
    table_id = table_schema.get_table_id();
    ObString table_name;
    table_name.assign(const_cast<char*>(table_schema.get_table_name()), static_cast<int32_t>(strlen(table_schema.get_table_name())));
    if (OB_SUCCESS == err)
    {
      decoded_param.set(table_id,OB_SCAN_PARAM_EMPTY_TABLE_NAME,*org_param.get_range());
    }
    int32_t got_cell_num = 0;
    int32_t overwrite_flag = 1;
    if ((OB_SUCCESS == err)
      && (org_param.get_column_name_size()==1)
      &&(org_param.get_column_name()[0] == ObGroupByParam::COUNT_ROWS_COLUMN_NAME))
    {
      int32_t column_count = 0;
      int32_t column_idx_in_schema_mgr = 0;
      const ObColumnSchemaV2 * table_columns =  NULL;
      table_columns = schema_mgr.get_table_schema(table_schema.get_table_id(),column_count);
      if ((NULL != table_columns) && (0 < column_count))
      {
        column_info = schema_mgr.get_column_schema(table_columns[0].get_table_id(),
          table_columns[0].get_id(),
          &column_idx_in_schema_mgr);
        if (NULL != column_info
          && 0 <= column_idx_in_schema_mgr
          && column_idx_in_schema_mgr < OB_MAX_COLUMN_NUMBER)
        {
          got_cell_num ++;
          err = decoded_param.add_column(column_info->get_id(), false);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to add column to decoded param [err:%d]", err);
          }
        }
        else
        {
          TBSYS_LOG(WARN,"fail to decode column name to column id [table_id:%lu,column_id:%lu]",
            table_columns[0].get_table_id(), table_columns[0].get_id());
          err = OB_SCHEMA_ERROR;
        }
      }
      else
      {
        err = OB_SCHEMA_ERROR;
        TBSYS_LOG(WARN,"fail to get column infos [table_name:%.*s, column_count:%d]",
          org_param.get_table_name().length(), org_param.get_table_name().ptr(),
          column_count);
      }
    }
    else if (OB_SUCCESS == err)
    {
      int32_t column_idx_in_schema_mgr = 0;
      for (int64_t cell_idx = 0; OB_SUCCESS == err && cell_idx < org_param.get_column_name_size(); cell_idx ++)
      {
        column_info = schema_mgr.get_column_schema(table_name, org_param.get_column_name()[cell_idx],
          &column_idx_in_schema_mgr);
        if (NULL == column_info
          || 0 > column_idx_in_schema_mgr
          || column_idx_in_schema_mgr >= OB_MAX_COLUMN_NUMBER)
        {
          TBSYS_LOG(WARN,"fail to decode column name to column id [table_name:%.*s,column_name:%.*s]",
            org_param.get_table_name().length(), org_param.get_table_name().ptr(),
            org_param.get_column_name()[cell_idx].length(), org_param.get_column_name()[cell_idx].ptr());
          if (NULL != rc)
          {
            snprintf(rc->message_.ptr(), rc->message_.length(), "undecodable [table_name:%.*s,column_name:%.*s]",
              org_param.get_table_name().length(), org_param.get_table_name().ptr(),
              org_param.get_column_name()[cell_idx].length(), org_param.get_column_name()[cell_idx].ptr());
          }
          err = OB_SCHEMA_ERROR;
        }
        else
        {
          if (OB_SUCCESS != (err = schema_assis_out.add_column(org_param.get_column_name()[cell_idx],got_cell_num, ObMSSchemaDecoderAssis::SELECT_COLUMN, overwrite_flag)))
          {
            TBSYS_LOG(WARN,"fail to add <column_name,idx> pair to hashmap");
          }
          else
          {
            err = OB_SUCCESS;
          }
          got_cell_num ++;
          bool is_return = *org_param.get_return_infos().at(cell_idx);
          if ((OB_SUCCESS == err)
            && (OB_SUCCESS != (err = decoded_param.add_column(column_info->get_id(), is_return))))
          {
            TBSYS_LOG(WARN,"fail to add column to decoded param [err:%d]", err);
          }
        }
      }
    }
    return err;
  }

  static int add_extra_basic_column(
    const ObString& column_name,
    ObScanParam& org_param,
    const ObTableSchema& table_schema,
    const ObSchemaManagerV2& schema_mgr,
    ObScanParam& decoded_param,
    ObMSSchemaDecoderAssis& schema_assis_in,
    ObResultCode* rc)
  {
    int ret             = OB_SUCCESS;
    int64_t column_size = 0;
    const ObColumnSchemaV2* column_schema = NULL;
    ObString table_name;

    if (NULL == column_name.ptr() || column_name.length() <= 0)
    {
      TBSYS_LOG(WARN, "invalid param, column_name_ptr=%p, column_name_len=%d",
        column_name.ptr(), column_name.length());
      ret = OB_ERROR;
    }
    else
    {
      table_name.assign_ptr(const_cast<char*>(table_schema.get_table_name()),
        static_cast<int32_t>(strlen(table_schema.get_table_name())));
      column_schema = schema_mgr.get_column_schema(table_name, column_name);
      if (NULL == column_schema)
      {
        TBSYS_LOG(WARN, "expire condition includes invalid column name, "
                        "table_name=%s, column_name=%.*s",
          table_schema.get_table_name(), column_name.length(), column_name.ptr());
        if (NULL != rc)
        {
          snprintf(rc->message_.ptr(), rc->message_.length(),
            "expire condition includes invalid column name, [table_name:%s,column_name:%.*s]",
            table_schema.get_table_name(), column_name.length(), column_name.ptr());
        }
        ret = OB_SCHEMA_ERROR;
      }
    }

    if (OB_SUCCESS == ret)
    {
      column_size = org_param.get_column_id_size();
      if (OB_SUCCESS != (ret = schema_assis_in.add_column(column_name, column_size,
        ObMSSchemaDecoderAssis::SELECT_COLUMN, 1)))
      {
        TBSYS_LOG(WARN, "fail to add expire column <column_name, idx> pair "
                        "ret=%d, column_name=%.*s, expire_column_idx=%ld",
          ret, column_name.length(), column_name.ptr(), column_size);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = org_param.add_column(column_name, false)))
      {
        TBSYS_LOG(WARN, "failed to add expire column into org_param, "
                        "column_name=%.*s, ret=%d",
          column_name.length(), column_name.ptr(), ret);
      }
      else if (OB_SUCCESS != (ret = decoded_param.add_column(column_schema->get_id(), false)))
      {
        TBSYS_LOG(WARN, "failed to add expire column into decoded_param, "
                        "column_id=%lu, ret=%d",
          column_schema->get_id(), ret);
      }
      else
      {
        // do nothing
      }
    }

    return ret;
  }

  static int add_extra_expire_dependent_columns(
    const ObString& expr,
    ObScanParam& org_param,
    const ObTableSchema& table_schema,
    const ObSchemaManagerV2& schema_mgr,
    ObScanParam& decoded_param,
    ObMSSchemaDecoderAssis& schema_assis_in,
    ObResultCode* rc)
  {
    int ret               = OB_SUCCESS;
    int hash_ret          = 0;
    int i                 = 0;
    int64_t type          = 0;
    int64_t val_index     = 0;
    int64_t postfix_size  = 0;
    ObString key_col_name;
    ObArrayHelper<ObObj> expr_array;
    ObObj post_expr[OB_MAX_COMPOSITE_SYMBOL_COUNT];
    ObExpressionParser& post_expression_parser =
      schema_assis_in.get_post_expression_parser();
    const hash::ObHashMap<common::ObString, int64_t,
      hash::NoPthreadDefendMode>& cname_to_idx_map =
      schema_assis_in.get_select_cname_to_idx_map();

    expr_array.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, post_expr);
    ret = post_expression_parser.parse(expr, expr_array);
    if (OB_SUCCESS == ret)
    {
      postfix_size = expr_array.get_array_index();
    }
    else
    {
      TBSYS_LOG(WARN, "parse infix expression to postfix expression "
                      "error, ret=%d", ret);
    }

    if (OB_SUCCESS == ret)
    {
      // 基本算法如下：
      //  1. 遍历expr_array数组，将其中类型为COLUMN_IDX的Obj读出
      //  2. 将obj中表示列名的值key_col_name读出，到hashmap中查找对应index
      //      scan_param.some_hash_map(key_col_name, val_index)

      i = 0;
      while(i < postfix_size - 1)
      {
        if (OB_SUCCESS != expr_array.at(i)->get_int(type))
        {
          TBSYS_LOG(WARN, "unexpected data type. int expected, but actual type is %d",
              expr_array.at(i)->get_type());
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        else
        {
          if (ObExpression::COLUMN_IDX == type)
          {
            if (OB_SUCCESS != expr_array.at(i+1)->get_varchar(key_col_name))
            {
              TBSYS_LOG(WARN, "unexpected data type. varchar expected, "
                              "but actual type is %d",
                  expr_array.at(i+1)->get_type());
              ret = OB_ERR_UNEXPECTED;
              break;
            }
            else
            {
              hash_ret = cname_to_idx_map.get(key_col_name, val_index);
              if (hash::HASH_EXIST != hash_ret)
              {
                ret = add_extra_basic_column(key_col_name, org_param, table_schema,
                  schema_mgr, decoded_param, schema_assis_in, rc);
                if (OB_SUCCESS != ret || val_index < 0)
                {
                  TBSYS_LOG(WARN, "failed add extra column into scan param, "
                                  "column_name=%.*s, val_index=%ld",
                    key_col_name.length(), key_col_name.ptr(), val_index);
                  break;
                }
              }
            }
          }/* only column name needs to decode. other type is ignored */
          i += 2; // skip <type, data> (2 objects as an element)
        }
      }
    }

    return ret;
  }

  static int replace_system_variable(char* expire_condition,
    const int64_t buf_size)
  {
    int ret = OB_SUCCESS;
    struct tm* tm = NULL;
    time_t sys_date = 0;
    int64_t cur_time = tbsys::CTimeUtil::getTime() / 1000 / 1000;
    const int64_t day_second = 24 * 3600;
    char replace_str_buf[32];

    if (NULL == expire_condition || buf_size <= 0)
    {
      TBSYS_LOG(WARN, "invalid param, expire_condition=%p, buf_size=%ld",
        expire_condition, buf_size);
      ret = OB_ERROR;
    }
    else
    {
      // use the 00:00:00 of day as system date
      sys_date = static_cast<time_t>(cur_time - (cur_time % day_second));
      tm = gmtime(&sys_date);
      if (NULL == tm)
      {
        TBSYS_LOG(WARN, "failed to transfer system date to tm, sys_date=%ld",
          sys_date);
        ret = OB_ERROR;
      }
      else
      {
        /*
        strftime(replace_str_buf, sizeof(replace_str_buf),
          "#%Y-%m-%d %H:%M:%S#", tm);
        ret = replace_str(expire_condition, buf_size, SYS_DATE, replace_str_buf);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to replace $SYS_DATE in expire condition");
        }
        */
        //modify peiouya [Expire_condition_modify] 20140909:b
        if ((NULL != strstr(expire_condition,SYS_DATE)) && (NULL != strstr(expire_condition,SYS_DAY)))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "$SYS_DATE and $SYS_DAY, the two can only and must choose one!");
        }
        else if (NULL != strstr(expire_condition,SYS_DATE))
        {
          strftime(replace_str_buf, sizeof(replace_str_buf),
            "#%Y-%m-%d %H:%M:%S#", tm);
          ret = replace_str(expire_condition, buf_size, SYS_DATE, replace_str_buf);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to replace $SYS_DATE in expire condition");
          }
        }
        else
        {
          strftime(replace_str_buf, sizeof(replace_str_buf),
            "#%Y-%m-%d#", tm);
          ret = replace_str(expire_condition, buf_size, SYS_DAY, replace_str_buf);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to replace $SYS_DAY in expire condition");
          }
        }
          //modify 20140909:b
      }
    }

    return ret;
  }

  static int ob_decode_scan_param_add_expire_condition(
    ObScanParam& org_param,
    const ObTableSchema& table_schema,
    const ObSchemaManagerV2& schema_mgr,
    ObScanParam& decoded_param,
    ObMSSchemaDecoderAssis& schema_assis_in,
    ObResultCode* rc)
  {
    int ret                      = OB_SUCCESS;
    const char* expire_condition = NULL;
    static __thread char infix_condition_expr[OB_MAX_EXPIRE_CONDITION_LENGTH];
    infix_condition_expr[0] = '\0';
    ObString cond_expr;

    if (table_schema.is_expire_effect_immediately()
        && NULL != (expire_condition = table_schema.get_expire_condition())
        && expire_condition[0] != '\0')
    {
      if (static_cast<int64_t>(strlen(expire_condition)) >= OB_MAX_EXPIRE_CONDITION_LENGTH)
      {
        TBSYS_LOG(WARN, "expire condition too large, expire_condition_len=%zu, "
                        "max_condition_len=%ld",
          strlen(expire_condition), OB_MAX_EXPIRE_CONDITION_LENGTH);
        ret = OB_ERROR;
      }
      else
      {
        strcpy(infix_condition_expr, expire_condition);
        ret = replace_system_variable(infix_condition_expr,
          OB_MAX_EXPIRE_CONDITION_LENGTH);
        if (OB_SUCCESS == ret)
        {
          cond_expr.assign_ptr(infix_condition_expr,
            static_cast<int32_t>(strlen(infix_condition_expr)));
          ret = add_extra_expire_dependent_columns(cond_expr, org_param,
            table_schema, schema_mgr, decoded_param, schema_assis_in, rc);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to transfer infix expression to postfix "
                            "expression, infix_expr=%s",
              infix_condition_expr);
          }
        }

        // add extra expire where condition
        if (OB_SUCCESS == ret)
        {
          ret = org_param.add_where_cond(cond_expr, true);
        }
      }
    }

    return ret;
  }

  int ob_decode_scan_param_select_composite_columns(const ObScanParam & org_param,
    ObScanParam &decoded_param,
    ObMSSchemaDecoderAssis &schema_assis_in,
    ObResultCode *rc)
  {
    int err = OB_SUCCESS;
    ObPostfixExpression post_expr;
    int64_t return_info_column_idx = org_param.get_column_name_size();
    int64_t cur_column_idx = decoded_param.get_column_id_size();
    ObString infix_expr;
    ObString as_name;
    for (int64_t i = 0;
      (i < org_param.get_composite_columns().get_array_index()) && (OB_SUCCESS == err);
      i++, cur_column_idx ++, return_info_column_idx++)
    {
      TBSYS_LOG(DEBUG, "composite column: %ld", i);
      infix_expr = org_param.get_composite_columns().at(i)->get_infix_expr();
      as_name = org_param.get_composite_columns().at(i)->get_as_column_name();
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      //if ((OB_SUCCESS == err)
      //  && (OB_SUCCESS != (err = post_expr.set_expression(infix_expr,schema_assis_in.get_select_cname_to_idx_map(),
      //  schema_assis_in.get_post_expression_parser(),rc))))
      if ((OB_SUCCESS == err)
           && (OB_SUCCESS != (err = post_expr.set_expression(infix_expr,
                                                                                                            schema_assis_in.get_select_cname_to_idx_map(),
                                                                                                            schema_assis_in.get_post_expression_parser(),
                                                                                                            rc,
                                                                                                            org_param.get_composite_columns().at(i)->is_expire_info()))))
       //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
      {
        TBSYS_LOG(WARN,"fail to set infix expression [err:%d, expr:%.*s, as_name:%.*s, comp_column_idx:%ld]",
          err, infix_expr.length(), infix_expr.ptr(), as_name.length(), as_name.ptr(), i);
      }
      bool is_return = *org_param.get_return_infos().at(return_info_column_idx);
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS != (err = decoded_param.add_column(post_expr.get_expression(),is_return))))
      {
        TBSYS_LOG(WARN,"fail to add post expression to decoded scan param [err:%d]", err);
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = schema_assis_in.add_column(as_name,cur_column_idx, ObMSSchemaDecoderAssis::SELECT_COLUMN))))
      {
        TBSYS_LOG(WARN,"fail to add composite column return info to cname to idx map [err:%d]", err);
      }
    }
    return err;
  }

  int ob_decode_scan_param_select_where_conditions(const ObScanParam & org_param,
    const ObTableSchema &table_schema,
    ObScanParam &decoded_param,
    const ObMSSchemaDecoderAssis &schema_assis_in,
    ObResultCode *rc)
  {
    int err = OB_SUCCESS;

    const ObSimpleFilter & org_filter  = org_param.get_filter_info();
    ObSimpleFilter & decoded_filter = decoded_param.get_filter_info();
    int64_t cond_count = org_filter.get_count();
    const ObSimpleCond   *cond = NULL;
    ObString table_name;
    table_name.assign(const_cast<char*>(table_schema.get_table_name()), static_cast<int32_t>(strlen(table_schema.get_table_name())));
    for (int64_t i = 0; OB_SUCCESS == err && i < cond_count; i++)
    {
      cond = org_filter[i];
      if (NULL == cond)
      {
        TBSYS_LOG(ERROR,"unexpected error [org_filter.get_count():%ld,idx:%ld,cond:%p]", cond_count,
          i, cond);
        err = OB_ERR_UNEXPECTED;
      }

      if (OB_SUCCESS == err)
      {
        int64_t column_idx_in_scan_param  = -1;
        int hash_err = 0;
        if (hash::HASH_EXIST == (hash_err = schema_assis_in.get_select_cname_to_idx_map().get(cond->get_column_name(),column_idx_in_scan_param)))
        {
          if (column_idx_in_scan_param >= decoded_param.get_column_id_size() + decoded_param.get_composite_columns().get_array_index()
            || column_idx_in_scan_param < 0)
          {
            TBSYS_LOG(WARN,"filter column not in basic info [column_name:%.*s]", cond->get_column_name().length(),
              cond->get_column_name().ptr());
            err = OB_INVALID_ARGUMENT;
          }
          else
          {
            err = decoded_filter.add_cond(column_idx_in_scan_param,
              cond->get_logic_operator(),cond->get_right_operand());
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to add condition to decoded param [err:%d]", err);
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN,"fail to find condition column name in cname to idx hash map [column_name:%.*s,condition_idx:%ld]",
            cond->get_column_name().length(), cond->get_column_name().ptr(), i);
          if (NULL != rc)
          {
            snprintf(rc->message_.ptr(), rc->message_.length(), "where cond not declared [column_name:%.*s]",
              cond->get_column_name().length(), cond->get_column_name().ptr());
          }
          err = OB_INVALID_ARGUMENT;
        }
      }
    }
    return err;
  }

  int ob_decode_scan_param_groupby_info_groupby_columns(const ObScanParam & org_param,
    const ObTableSchema &table_schema,
    ObScanParam &decoded_param,
    const ObMSSchemaDecoderAssis &schema_assis_in,
    ObResultCode *rc)
  {
    int err = OB_SUCCESS;
    const ObGroupByParam &org_groupby_param = org_param.get_group_by_param();
    ObGroupByParam & decoded_groupby_param = decoded_param.get_group_by_param();
    ObString table_name;
    table_name.assign(const_cast<char*>(table_schema.get_table_name()), static_cast<int32_t>(strlen(table_schema.get_table_name())));
    const ObArrayHelper<ObGroupByParam::ColumnInfo> & groupby_columns = org_groupby_param.get_groupby_columns();
    int64_t return_info_idx = 0;
    for (int64_t i = 0; OB_SUCCESS == err && i < groupby_columns.get_array_index(); i++, return_info_idx ++)
    {
      int64_t column_idx_in_scan_param = -1;
      int hash_err = 0;
      if (hash::HASH_EXIST ==
        (hash_err = schema_assis_in.get_select_cname_to_idx_map().get(groupby_columns.at(i)->column_name_,column_idx_in_scan_param)))
      {
        int64_t select_clause_table_row_width = decoded_param.get_column_id_size()  +
          decoded_param.get_composite_columns().get_array_index();
        if (column_idx_in_scan_param >= select_clause_table_row_width
          || column_idx_in_scan_param < 0)
        {
          TBSYS_LOG(WARN,"group by column not in basic info [column_name:%.*s]", groupby_columns.at(i)->column_name_.length(),
            groupby_columns.at(i)->column_name_.ptr());
          err = OB_INVALID_ARGUMENT;
        }
        else
        {
          bool is_return = *org_groupby_param.get_return_infos().at(return_info_idx);
          err = decoded_groupby_param.add_groupby_column(column_idx_in_scan_param, is_return);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to add group by column to decoded param [err:%d]", err);
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN,"fail to find groupby column name in cname to idx hash map [column_name:%.*s,condition_idx:%ld]",
          groupby_columns.at(i)->column_name_.length(), groupby_columns.at(i)->column_name_.ptr(), i);
        if (NULL != rc)
        {
          snprintf(rc->message_.ptr(), rc->message_.length(), "groupby column not declared [column_name:%.*s]",
            groupby_columns.at(i)->column_name_.length(), groupby_columns.at(i)->column_name_.ptr());
        }
        err = OB_INVALID_ARGUMENT;
      }
    }
    return err;
  }

  int ob_decode_scan_param_groupby_having_conditions(const ObGroupByParam & org_param,
    ObGroupByParam &decoded_param,
    const ObMSSchemaDecoderAssis &schema_assis_in,
    ObResultCode *rc)
  {
    int err = OB_SUCCESS;
    const ObSimpleFilter & org_filter  = org_param.get_having_condition();
    ObSimpleFilter & decoded_filter = decoded_param.get_having_condition();
    int64_t cond_count = org_filter.get_count();
    const ObSimpleCond   *cond = NULL;
    for (int64_t i = 0; OB_SUCCESS == err && i < cond_count; i++)
    {
      cond = org_filter[i];
      if (NULL == cond)
      {
        TBSYS_LOG(ERROR,"unexpected error [org_filter.get_count():%ld,idx:%ld,cond:%p]", cond_count,
          i, cond);
        err = OB_ERR_UNEXPECTED;
      }

      if (OB_SUCCESS == err)
      {
        int64_t column_idx_in_scan_param  = -1;
        int hash_err = 0;
        int64_t groupby_row_width = org_param.get_groupby_columns().get_array_index() +
          org_param.get_return_columns().get_array_index() + org_param.get_aggregate_columns().get_array_index() +
          org_param.get_composite_columns().get_array_index();
        if (hash::HASH_EXIST == (hash_err = schema_assis_in.get_groupby_cname_to_idx_map().get(cond->get_column_name(),column_idx_in_scan_param)))
        {
          if (column_idx_in_scan_param >=  groupby_row_width
            || column_idx_in_scan_param < 0)
          {
            TBSYS_LOG(WARN,"filter column not in basic info [column_name:%.*s]", cond->get_column_name().length(),
              cond->get_column_name().ptr());
            err = OB_INVALID_ARGUMENT;
          }
          else
          {
            err = decoded_filter.add_cond(column_idx_in_scan_param,
              cond->get_logic_operator(),cond->get_right_operand());
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to add condition to decoded param [err:%d]", err);
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN,"fail to find condition column name in cname to idx hash map [column_name:%.*s,condition_idx:%ld]",
            cond->get_column_name().length(), cond->get_column_name().ptr(), i);
          if (NULL != rc)
          {
            snprintf(rc->message_.ptr(), rc->message_.length(), "having cond not ceclared [column_name:%.*s]",
              cond->get_column_name().length(), cond->get_column_name().ptr());
          }
          err = OB_INVALID_ARGUMENT;
        }
      }
    }
    return err;
  }

  int ob_decode_scan_param_groupby_info_return_columns(const ObScanParam & org_param,
    const ObTableSchema &table_schema,
    ObScanParam &decoded_param,
    const ObMSSchemaDecoderAssis &schema_assis_in,
    ObResultCode *rc)
  {
    int err = OB_SUCCESS;
    const ObGroupByParam &org_groupby_param = org_param.get_group_by_param();
    ObGroupByParam & decoded_groupby_param = decoded_param.get_group_by_param();
    ObString table_name;
    table_name.assign(const_cast<char*>(table_schema.get_table_name()), static_cast<int32_t>(strlen(table_schema.get_table_name())));
    const ObArrayHelper<ObGroupByParam::ColumnInfo> & return_columns = org_groupby_param.get_return_columns();
    int64_t return_info_idx = org_groupby_param.get_groupby_columns().get_array_index();

    for (int64_t i = 0; OB_SUCCESS == err && i < return_columns.get_array_index(); i++ , return_info_idx++)
    {
      int64_t column_idx_in_scan_param = -1;
      int hash_err = 0;
      if (hash::HASH_EXIST ==
        (hash_err = schema_assis_in.get_select_cname_to_idx_map().get(return_columns.at(i)->column_name_,column_idx_in_scan_param)))
      {
        int64_t select_clause_table_row_width = decoded_param.get_column_id_size() +
          decoded_param.get_composite_columns().get_array_index();
        if (column_idx_in_scan_param >= select_clause_table_row_width
          || column_idx_in_scan_param < 0)
        {
          TBSYS_LOG(WARN,"return column not in basic info [column_name:%.*s]", return_columns.at(i)->column_name_.length(),
            return_columns.at(i)->column_name_.ptr());
          err = OB_INVALID_ARGUMENT;
        }
        else
        {
          bool is_return = *org_groupby_param.get_return_infos().at(return_info_idx);
          err = decoded_groupby_param.add_return_column(column_idx_in_scan_param, is_return);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to add return column to decoded param [err:%d]", err);
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN,"fail to find return column name in cname to idx hash map [column_name:%.*s,condition_idx:%ld]",
          return_columns.at(i)->column_name_.length(), return_columns.at(i)->column_name_.ptr(), i);
        if (NULL != rc)
        {
          snprintf(rc->message_.ptr(), rc->message_.length(), "return column not declared [column_name:%.*s]",
            return_columns.at(i)->column_name_.length(), return_columns.at(i)->column_name_.ptr());
        }
        err = OB_INVALID_ARGUMENT;
      }
    }
    return err;
  }

  int ob_decode_scan_param_groupby_info_aggregate_columns(const ObScanParam & org_param,
    const ObTableSchema &table_schema,
    ObScanParam &decoded_param,
    const ObMSSchemaDecoderAssis &schema_assis_in,
    ObResultCode *rc)
  {
    int err = OB_SUCCESS;
    const ObGroupByParam &org_groupby_param = org_param.get_group_by_param();
    ObGroupByParam & decoded_groupby_param = decoded_param.get_group_by_param();
    ObString table_name;
    table_name.assign(const_cast<char*>(table_schema.get_table_name()), static_cast<int32_t>(strlen(table_schema.get_table_name())));
    const ObArrayHelper<ObAggregateColumn>          & aggregate_columns = org_groupby_param.get_aggregate_columns();
    int64_t return_info_idx = org_groupby_param.get_groupby_columns().get_array_index()
    + org_groupby_param.get_return_columns().get_array_index();

    for (int64_t i = 0; OB_SUCCESS == err && i < aggregate_columns.get_array_index(); i++, return_info_idx ++)
    {
      int64_t column_idx_in_scan_param = -1;
      if ((aggregate_columns.at(i)->get_org_column_name() == ObGroupByParam::COUNT_ROWS_COLUMN_NAME)
        && (aggregate_columns.at(i)->get_func_type() == COUNT))
      {
        column_idx_in_scan_param = 0;
      }
      else
      {
        if (hash::HASH_EXIST  !=
          schema_assis_in.get_select_cname_to_idx_map().get(aggregate_columns.at(i)->get_org_column_name(),column_idx_in_scan_param))
        {
          TBSYS_LOG(WARN,"fail to find aggregate column name in cname to idx hash map [column_name:%.*s,condition_idx:%ld]",
            aggregate_columns.at(i)->get_org_column_name().length(),
            aggregate_columns.at(i)->get_org_column_name().ptr(), i);
          err = OB_INVALID_ARGUMENT;
          if (NULL != rc)
          {
            snprintf(rc->message_.ptr(), rc->message_.length(), "aggregate column not declared [column_name:%.*s]",
              aggregate_columns.at(i)->get_org_column_name().length(),
              aggregate_columns.at(i)->get_org_column_name().ptr());
          }
        }
      }
      if (OB_SUCCESS == err)
      {
        int64_t select_clause_table_row_width = decoded_param.get_column_id_size() +
          decoded_param.get_composite_columns().get_array_index();
        if (column_idx_in_scan_param >= select_clause_table_row_width
          || column_idx_in_scan_param < 0)
        {
          TBSYS_LOG(WARN,"aggregate column not in basic info [column_name:%.*s]",
            aggregate_columns.at(i)->get_org_column_name().length(),
            aggregate_columns.at(i)->get_org_column_name().ptr());
          err = OB_INVALID_ARGUMENT;
        }
        else
        {
          bool is_return = *org_groupby_param.get_return_infos().at(return_info_idx);
          err = decoded_groupby_param.add_aggregate_column(column_idx_in_scan_param,
            aggregate_columns.at(i)->get_func_type(), is_return);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to add aggregate column to decoded param [err:%d]", err);
          }
        }
      }
    }
    return err;
  }

  int ob_decode_scan_param_groupby_composite_columns(const ObGroupByParam & org_param,
    ObGroupByParam &decoded_param,
    ObMSSchemaDecoderAssis &schema_assis_in,
    ObResultCode *rc)
  {
    int err = OB_SUCCESS;
    ObPostfixExpression post_expr;
    int64_t return_info_column_idx = org_param.get_groupby_columns().get_array_index()
    + org_param.get_return_columns().get_array_index() + org_param.get_aggregate_columns().get_array_index();
    int64_t cur_column_idx = decoded_param.get_groupby_columns().get_array_index()
    + decoded_param.get_return_columns().get_array_index() + decoded_param.get_aggregate_columns().get_array_index();
    ObString infix_expr;
    ObString as_name;
    for (int64_t i = 0;
      (i < org_param.get_composite_columns().get_array_index()) && (OB_SUCCESS == err);
      i++, cur_column_idx ++, return_info_column_idx++)
    {
      infix_expr = org_param.get_composite_columns().at(i)->get_infix_expr();
      as_name = org_param.get_composite_columns().at(i)->get_as_column_name();
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      //if ((OB_SUCCESS == err)
      //  && (OB_SUCCESS != (err = post_expr.set_expression(infix_expr,schema_assis_in.get_select_cname_to_idx_map(),
      //  schema_assis_in.get_post_expression_parser(),rc))))
      if ((OB_SUCCESS == err)
           && (OB_SUCCESS != (err = post_expr.set_expression(infix_expr,
                                                                                                            schema_assis_in.get_select_cname_to_idx_map(),
                                                                                                            schema_assis_in.get_post_expression_parser(),
                                                                                                            rc,
                                                                                                            org_param.get_composite_columns().at(i)->is_expire_info()))))
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      {
        TBSYS_LOG(WARN,"fail to set infix expression [err:%d, expr:%.*s, as_name:%.*s, comp_column_idx:%ld]",
          err, infix_expr.length(), infix_expr.ptr(), as_name.length(), as_name.ptr(), i);
      }
      bool is_return = *org_param.get_return_infos().at(return_info_column_idx);
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS != (err = decoded_param.add_column(post_expr.get_expression(),is_return))))
      {
        TBSYS_LOG(WARN,"fail to add post expression to decoded scan param [err:%d]", err);
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = schema_assis_in.add_column(as_name,cur_column_idx, ObMSSchemaDecoderAssis::GROUPBY_COLUMN))))
      {
        TBSYS_LOG(WARN,"fail to add composite column return info to cname to idx map [err:%d]", err);
      }
    }
    return err;
  }

  int ob_decode_scan_param_groupby_info(const ObScanParam & org_param,
    const ObTableSchema &table_schema,
    ObScanParam &decoded_param,
    ObMSSchemaDecoderAssis &schema_assis_in,
    ObResultCode *rc)
  {
    int err = OB_SUCCESS;

    const ObArrayHelper<ObGroupByParam::ColumnInfo> & groupby_columns = org_param.get_group_by_param().get_groupby_columns();
    const ObArrayHelper<ObGroupByParam::ColumnInfo> & return_columns = org_param.get_group_by_param().get_return_columns();
    const ObArrayHelper<ObAggregateColumn>   & aggregate_columns = org_param.get_group_by_param().get_aggregate_columns();
    const ObGroupByParam &org_groupby_param = org_param.get_group_by_param();
    ObGroupByParam &decoded_groupby_param = decoded_param.get_group_by_param();
    if (org_groupby_param.get_groupby_columns().get_array_index() +
      org_groupby_param.get_aggregate_columns().get_array_index() > 0)
    {
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS !=
        (err = ob_decode_scan_param_groupby_info_groupby_columns(org_param,table_schema,decoded_param,schema_assis_in, rc))))
      {
        TBSYS_LOG(WARN,"fail to decode groupby columns [err:%d]", err);
      }
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS !=
        (err = ob_decode_scan_param_groupby_info_return_columns(org_param,table_schema,decoded_param,schema_assis_in,rc))))
      {
        TBSYS_LOG(WARN,"fail to decode groupby columns [err:%d]", err);
      }
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS !=
        (err = ob_decode_scan_param_groupby_info_aggregate_columns(org_param,table_schema,decoded_param,schema_assis_in, rc))))
      {
        TBSYS_LOG(WARN,"fail to decode groupby columns [err:%d]", err);
      }
      //// after this, all computation will be base on groupby params
      ///  schema_assis_in.cname_to_idx_map_.clear();
      int32_t cur_column_idx = 0;
      for (int32_t idx = 0; (idx < groupby_columns.get_array_index()) && (OB_SUCCESS == err); idx++, cur_column_idx  ++)
      {
        if ((OB_SUCCESS == err)
          && (OB_SUCCESS != schema_assis_in.add_column(groupby_columns.at(idx)->column_name_, cur_column_idx, ObMSSchemaDecoderAssis::GROUPBY_COLUMN)))
        {
          TBSYS_LOG(WARN,"fail to add column name to idx map [err:%d, column_idx:%d]", err, cur_column_idx);
        }
      }

      for (int32_t idx = 0; (idx < return_columns.get_array_index()) && (OB_SUCCESS == err); idx++, cur_column_idx  ++)
      {
        if ((OB_SUCCESS == err)
          && (OB_SUCCESS != schema_assis_in.add_column(return_columns.at(idx)->column_name_, cur_column_idx, ObMSSchemaDecoderAssis::GROUPBY_COLUMN)))
        {
          TBSYS_LOG(WARN,"fail to add column name to idx map [err:%d, column_idx:%d]", err, cur_column_idx);
        }
      }

      for (int32_t idx = 0; (idx < aggregate_columns.get_array_index()) && (OB_SUCCESS == err); idx++, cur_column_idx  ++)
      {
        if ((OB_SUCCESS == err)
          && (OB_SUCCESS != schema_assis_in.add_column(aggregate_columns.at(idx)->get_as_column_name(), cur_column_idx, ObMSSchemaDecoderAssis::GROUPBY_COLUMN)))
        {
          TBSYS_LOG(WARN,"fail to add column name to idx map [err:%d, column_idx:%d]", err, cur_column_idx);
        }
      }

      if ((OB_SUCCESS == err)
        && (OB_SUCCESS !=
        (err = ob_decode_scan_param_groupby_composite_columns(org_groupby_param,decoded_groupby_param,schema_assis_in,rc))))
      {
        TBSYS_LOG(WARN,"fail to decode groupby composite columns [err:%d]", err);
      }
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS !=
        (err = ob_decode_scan_param_groupby_having_conditions(org_groupby_param,decoded_groupby_param,schema_assis_in, rc))))
      {
        TBSYS_LOG(WARN,"fail to decode having conditions [err:%d]", err);
      }
    }
    return err;
  }

  int ob_decode_scan_param_orderby_info(const ObScanParam & org_param,
    const ObTableSchema &table_schema,
    ObScanParam &decoded_param,
    const ObMSSchemaDecoderAssis &schema_assis_in,
    ObResultCode *rc)
  {
    int err = OB_SUCCESS;
    int64_t orderby_count = 0;
    ObString const* orderby_columns = NULL;
    uint8_t  const* order_desc = NULL;
    ObString table_name;
    table_name.assign(const_cast<char*>(table_schema.get_table_name()), static_cast<int32_t>(strlen(table_schema.get_table_name())));
    org_param.get_orderby_column(orderby_columns,order_desc,orderby_count);
    for (int64_t i = 0; OB_SUCCESS == err && i < orderby_count; i++)
    {
      if (org_param.get_group_by_param().get_aggregate_row_width() > 0)
      {
        int64_t column_idx_in_groupby_result = org_param.get_group_by_param().find_column(orderby_columns[i]);
        if (column_idx_in_groupby_result < 0)
        {
          TBSYS_LOG(WARN,"order by column was not in groupby columns [column_name:%.*s]",
            orderby_columns[i].length(), orderby_columns[i].ptr());
          if (NULL != rc)
          {
            snprintf(rc->message_.ptr(), rc->message_.length(), "orderby column not declared in groupby clause [column_name:%.*s]",
              orderby_columns[i].length(), orderby_columns[i].ptr());
          }
          err = OB_INVALID_ARGUMENT;
        }
        else
        {
          err = decoded_param.add_orderby_column(column_idx_in_groupby_result, static_cast<ObScanParam::Order>(order_desc[i]));
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to add orderby column to decoded param [err:%d]", err);
          }
        }
      }
      else
      {
        int64_t column_idx_in_scan_param  = -1;
        int hash_err = -1;
        if (hash::HASH_EXIST == (hash_err = schema_assis_in.get_select_cname_to_idx_map().get(orderby_columns[i], column_idx_in_scan_param)))
        {
          if (column_idx_in_scan_param >= decoded_param.get_column_id_size() + decoded_param.get_composite_columns().get_array_index()
            || column_idx_in_scan_param < 0)
          {
            TBSYS_LOG(WARN,"orderby column index outof range [column_name:%.*s, column_idx_in_scan_param:%ld, "
              "select_row_width:%ld]",
              orderby_columns[i].length(), orderby_columns[i].ptr(), column_idx_in_scan_param,
              decoded_param.get_column_id_size() + decoded_param.get_composite_columns().get_array_index());
            err = OB_INVALID_ARGUMENT;
          }
          else
          {
            err = decoded_param.add_orderby_column(column_idx_in_scan_param,
              static_cast<ObScanParam::Order>(order_desc[i]));
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to add orderby column to decoded param [err:%d]", err);
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN,"fail to find orderby column name in cname to idx hash map [column_name:%.*s,condition_idx:%ld]",
            orderby_columns[i].length(), orderby_columns[i].ptr(), i);
          if (NULL != rc)
          {
            snprintf(rc->message_.ptr(), rc->message_.length(), "orderby column not declared in select clause [column_name:%.*s]",
              orderby_columns[i].length(), orderby_columns[i].ptr());
          }
          err = OB_INVALID_ARGUMENT;
        }
      }
      if (ObScanParam::ASC != order_desc[i]
        && ObScanParam::DESC != order_desc[i])
      {
        TBSYS_LOG(WARN,"unrecogonized order [idx:%ld,order:%hhu]", i, order_desc[i]);
        err = OB_INVALID_ARGUMENT;
        if (NULL != rc)
        {
          snprintf(rc->message_.ptr(), rc->message_.length(), "unrecogonizable order [order_desc:%huu]", order_desc[i]);
        }
      }
    }
    return err;
  }

  bool check_scan_param_compatibility(const ObScanParam & scan_param_in , common::ObResultCode *rc)
  {
    bool result = true;
    if (ScanFlag::BACKWARD == scan_param_in.get_scan_direction())
    {
      if (scan_param_in.get_orderby_column_size() > 0)
      {
        TBSYS_LOG(WARN,"backward scan do not support orderby operation");
        if (NULL != rc)
        {
          snprintf(rc->message_.ptr(), rc->message_.length(), "backward scan do not support orderby");
        }
        result = false;
      }
      if (result  && scan_param_in.get_group_by_param().get_aggregate_row_width() > 0)
      {
        TBSYS_LOG(WARN,"backward scan do not support grouby operation");
        result = false;
        if (NULL != rc)
        {
          snprintf(rc->message_.ptr(), rc->message_.length(), "backward scan do not support groupby");
        }
      }
    }
    return result;
  }
}

int oceanbase::mergeserver::ob_decode_get_param(const oceanbase::common::ObGetParam & org_param,
  const oceanbase::common::ObSchemaManagerV2 & schema_mgr,
  oceanbase::common::ObGetParam &decoded_param,
  oceanbase::common::ObGetParam &org_param_with_name,
  common::ObResultCode *rc)
{
  int err = OB_SUCCESS;
  ObCellInfo cell;
  ObCellInfo cell_with_name;
  const ObCellInfo *cur_cell = NULL;
  const ObColumnSchemaV2 *column_schema = NULL;
  decoded_param.reset();
  org_param_with_name.reset(true);
  ObReadParam &read_param = decoded_param;
  read_param = org_param;
  for (int64_t i = 0; OB_SUCCESS == err && i < org_param.get_cell_size(); i++)
  {
    cur_cell = org_param[i];
    if (NULL == cur_cell)
    {
      TBSYS_LOG(WARN,"unexpected error [idx:%ld,org_param.get_cell_size():%ld,cur_cell:%p]",
        i, org_param.get_cell_size(), cur_cell);
      err = OB_INVALID_ARGUMENT;
    }
    if ((OB_SUCCESS == err) && (cur_cell->column_name_ == ObGetParam::OB_GET_ALL_COLUMN_NAME))
    {
      const ObColumnSchemaV2 *column_beg = NULL;
      int32_t column_size = 0;
      const ObTableSchema * table_schema = schema_mgr.get_table_schema(cur_cell->table_name_);
      if (NULL == table_schema)
      {
        TBSYS_LOG(WARN,"fail to get schema of table [table_name:%.*s]", cur_cell->table_name_.length(), cur_cell->table_name_.ptr());
        err = OB_SCHEMA_ERROR;
        if (NULL != rc)
        {
          snprintf(rc->message_.ptr(),rc->message_.length(), "undecodable [table_name:%.*s]",
            cur_cell->table_name_.length(), cur_cell->table_name_.ptr());
        }
      }
      if ((OB_SUCCESS == err)
        && ((NULL == (column_beg = schema_mgr.get_table_schema(table_schema->get_table_id(),column_size))) || (column_size <= 0)))
      {
        TBSYS_LOG(WARN,"fail to get schema of table [table_name:%.*s]", cur_cell->table_name_.length(), cur_cell->table_name_.ptr());
        err = OB_SCHEMA_ERROR;
        if (NULL != rc)
        {
          snprintf(rc->message_.ptr(),rc->message_.length(), "undecodable [table_name:%.*s]",
            cur_cell->table_name_.length(), cur_cell->table_name_.ptr());
        }
      }
      for (int32_t j = 0; (j < column_size) && (OB_SUCCESS == err); j ++)
      {
        cell.column_id_ = column_beg[j].get_id();
        cell.table_id_ = table_schema->get_table_id();
        cell.row_key_ = cur_cell->row_key_;
        cell_with_name.table_name_.assign((char*)table_schema->get_table_name(),static_cast<int32_t>(strlen(table_schema->get_table_name())));
        cell_with_name.column_name_.assign((char*)column_beg[j].get_name(), static_cast<int32_t>(strlen(column_beg[j].get_name())));
        cell_with_name.row_key_ = cur_cell->row_key_;
        if (OB_SUCCESS != (err = decoded_param.add_cell(cell)))
        {
          TBSYS_LOG(WARN,"fail to add decoded cell to decoded param [err:%d]", err);
          if (NULL != rc)
          {
            snprintf(rc->message_.ptr(),rc->message_.length(), "fail to add decoded cell to decoded param [err:%d]",  err);
          }
        }
        if((OB_SUCCESS == err) && (OB_SUCCESS != (err = org_param_with_name.add_cell(cell_with_name))))
        {
          TBSYS_LOG(WARN,"fail to add decoded cell to decoded param [err:%d]", err);
          if (NULL != rc)
          {
            snprintf(rc->message_.ptr(),rc->message_.length(), "fail to add decoded cell to decoded param [err:%d]",  err);
          }
        }
      }
    }
    else if (OB_SUCCESS == err)
    {
      column_schema = schema_mgr.get_column_schema(cur_cell->table_name_, cur_cell->column_name_);
      if (NULL == column_schema)
      {
        TBSYS_LOG(WARN,"fail to decode column [table_name:%.*s,column_name:%.*s]",
          cur_cell->table_name_.length(), cur_cell->table_name_.ptr(),
          cur_cell->column_name_.length(), cur_cell->column_name_.ptr()
          );
        if (NULL != rc)
        {
          snprintf(rc->message_.ptr(),rc->message_.length(), "undecodable [table_name:%.*s,column_name:%.*s]",
            cur_cell->table_name_.length(), cur_cell->table_name_.ptr(),
            cur_cell->column_name_.length(), cur_cell->column_name_.ptr());
        }
        err = OB_SCHEMA_ERROR;
      }
      if (OB_SUCCESS == err)
      {
        cell.column_id_ = column_schema->get_id();
        cell.table_id_ = column_schema->get_table_id();
        cell.row_key_ = cur_cell->row_key_;
        err = decoded_param.add_cell(cell);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to add cell to decoded param [err:%d]", err);
          if (NULL != rc)
          {
            snprintf(rc->message_.ptr(),rc->message_.length(), "fail to add decoded cell to decoded param [err:%d]",  err);
          }
        }
        if((OB_SUCCESS == err) && (OB_SUCCESS != (err = org_param_with_name.add_cell(*cur_cell))))
        {
          TBSYS_LOG(WARN,"fail to add decoded cell to decoded param [err:%d]", err);
          if (NULL != rc)
          {
            snprintf(rc->message_.ptr(),rc->message_.length(), "fail to add decoded cell to decoded param [err:%d]",  err);
          }
        }
      }
    }
  }
  return err;
}

int oceanbase::mergeserver::ob_decode_scan_param(ObScanParam & org_param,
  const ObSchemaManagerV2 & schema_mgr,
  ObScanParam &decoded_param,
  common::ObResultCode *rc)
{
  UNUSED(rc);
  int err = OB_SUCCESS;
  decoded_param.reset();
  ObReadParam &read_param = decoded_param;
  const ObTableSchema *table_schema = NULL;
  read_param = org_param;
  const ObString & table_name = org_param.get_table_name();
  table_schema = schema_mgr.get_table_schema(table_name);
  ObMSSchemaDecoderAssis *schema_assis = GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1);
  if (NULL == table_schema)
  {
    TBSYS_LOG(WARN,"fail to decode table name to table id [table_name:%.*s]",
      org_param.get_table_name().length(), org_param.get_table_name().ptr());
    if (NULL != rc)
    {
      snprintf(rc->message_.ptr(), rc->message_.length(), "undecodable [table_name:%.*s]",
        org_param.get_table_name().length(), org_param.get_table_name().ptr());
    }
    err = OB_SCHEMA_ERROR;
  }
  if (OB_SUCCESS == err)
  {
    const ObNewRange * range = org_param.get_range();
    if (NULL == range)
    {
      err = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "check scan param range failed:range[%p]", range);
    }
    else if (false == range->check())
    {
      err = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "%s", "check param range rowkey and border flag failed");
      if (NULL != rc)
      {
        snprintf(rc->message_.ptr(), rc->message_.length(), "scan range check error");
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    if (NULL == schema_assis)
    {
      TBSYS_LOG(WARN,"fail to allocate memory for ObMSSchemaDecoderAssis");
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = schema_assis->init())))
      {
        TBSYS_LOG(WARN,"fail to initialize schema assiss [err:%d]", err);
      }
    }
  }
  if ((OB_SUCCESS == err)
    && (!check_scan_param_compatibility(org_param, rc)))
  {
    TBSYS_LOG(WARN,"scan param compatibility check fail");
    err = OB_INVALID_ARGUMENT;
  }
  /// decode basic info
  if (OB_SUCCESS == err)
  {
    if (org_param.get_column_name_size() == 0)
    {/// select *
      err = ob_decode_scan_param_select_all_column(org_param,*table_schema,schema_mgr,
        decoded_param, *schema_assis, rc);
    }
    else
    {
      err = ob_decode_scan_param_basic_info(org_param,*table_schema,schema_mgr,
        decoded_param, *schema_assis, rc);
    }
  }

  /// add expire condition if necessary
  if (OB_SUCCESS == err)
  {
    err = ob_decode_scan_param_add_expire_condition(org_param,*table_schema,schema_mgr,
        decoded_param, *schema_assis, rc);
  }

  /// decode select composite columns
  if (OB_SUCCESS == err)
  {
    err = ob_decode_scan_param_select_composite_columns(org_param,decoded_param,*schema_assis, rc);
  }
  /// decode filter info
  if (OB_SUCCESS == err)
  {
    err = ob_decode_scan_param_select_where_conditions(org_param,*table_schema, decoded_param, *schema_assis, rc);
  }

  /// decode group by info
  if (OB_SUCCESS == err)
  {
    err = ob_decode_scan_param_groupby_info(org_param,*table_schema, decoded_param, *schema_assis, rc);
  }
  /// decode order by info
  if (OB_SUCCESS == err)
  {
    err = ob_decode_scan_param_orderby_info(org_param,*table_schema, decoded_param, *schema_assis,rc);
  }
  /// decode limit info
  if (OB_SUCCESS == err)
  {
    int64_t limit_offset = -1;
    int64_t limit_count = -1;
    int64_t sharding_minimum = -1;
    double precision = 0;
    org_param.get_limit_info(limit_offset,limit_count);
    org_param.get_topk_precision(sharding_minimum, precision);
    if (OB_SUCCESS != (err = decoded_param.set_limit_info(limit_offset, limit_count)))
    {
      TBSYS_LOG(WARN,"fail to set limit info [err:%d, limit_offset:%ld, limit_count:%ld]", err, limit_offset, limit_count);
    }
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = decoded_param.set_topk_precision(sharding_minimum,precision))))
    {
      TBSYS_LOG(WARN,"fail to set topk precision [err:%d, sharding_minimum:%ld, precision:%f]", err, sharding_minimum,
        precision);
    }
  }
  /// decode scan flag
  if (OB_SUCCESS == err)
  {
    decoded_param.set_scan_flag(org_param.get_scan_flag());
    if ((ScanFlag::ASYNCREAD != org_param.get_read_mode())
      &&(ScanFlag::SYNCREAD != org_param.get_read_mode()))
    {
      TBSYS_LOG(WARN,"unrecogonized read_mode [org_param.get_scan_flag().read_mode_:%ld]",
        org_param.get_scan_flag().read_mode_);
      err = OB_INVALID_ARGUMENT;
      if (NULL != rc)
      {
        snprintf(rc->message_.ptr(), rc->message_.length(), "unrecogonized [read_mode:%ld]", org_param.get_scan_flag().read_mode_);
      }
    }
    if ((OB_SUCCESS == err)
      && (ScanFlag::FORWARD != org_param.get_scan_direction())
      && (ScanFlag::BACKWARD != org_param.get_scan_direction()))
    {
      TBSYS_LOG(WARN,"unrecogonized scan_direction [org_param.get_scan_direction():%d]",
        org_param.get_scan_direction());
      if (NULL != rc)
      {
        snprintf(rc->message_.ptr(), rc->message_.length(), "unrecogonized [read_mode:%d]",
          static_cast<int32_t>(org_param.get_scan_direction()));
      }
      err = OB_INVALID_ARGUMENT;
    }
  }


  if ((OB_SUCCESS == err)
      && (decoded_param.get_returned_column_num() + decoded_param.get_group_by_param().get_returned_column_num() <= 0))
  {
    TBSYS_LOG(WARN,"scan param must return some columns. return_column_count:%ld", decoded_param.get_returned_column_num());
    err = OB_INVALID_ARGUMENT;
  }

  return err;
}
