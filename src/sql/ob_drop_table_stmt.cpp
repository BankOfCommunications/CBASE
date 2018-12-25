/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_drop_table_stmt.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_drop_table_stmt.h"
#include "ob_schema_checker.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObDropTableStmt::ObDropTableStmt(ObStringBuf* name_pool)
  : ObBasicStmt(ObBasicStmt::T_DROP_TABLE)
{
  name_pool_ = name_pool;
  if_exists_ = false;
}

ObDropTableStmt::~ObDropTableStmt()
{
}

int ObDropTableStmt::add_table_name_id(ResultPlan& result_plan, const ObString& table_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;

  ObSchemaChecker* schema_checker = NULL;
  schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
  if (schema_checker == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }

  if (ret == OB_SUCCESS && !if_exists_)
  {
    if ((table_id = schema_checker->get_table_id(table_name)) == OB_INVALID_ID)
    {
      ret = OB_ERR_TABLE_UNKNOWN;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "table '%.*s' does not exist", table_name.length(), table_name.ptr());
    }
  }

  if (ret == OB_SUCCESS && tables_.count() > 0)
  {
    for (int32_t i = 0; i < tables_.count(); i++)
    {
      if (tables_.at(i) == table_name)
      {
        ret = OB_ERR_TABLE_DUPLICATE;
        snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
            "Not unique table: '%.*s'", table_name.length(), table_name.ptr());
        break;
      }
    }
  }

  if (ret == OB_SUCCESS)
  {
    ObString str;
    if ((ret = ob_write_string(*name_pool_, table_name, str)) != OB_SUCCESS)
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Make space for %.*s failed", table_name.length(), table_name.ptr());
    }
    else if ((ret = tables_.push_back(str)) != OB_SUCCESS)
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Add table %.*s failed", table_name.length(), table_name.ptr());
    }
    else if (OB_SUCCESS != (ret = table_ids_.push_back(table_id)))
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Add table id %lu failed", table_id);
    }
  }

  return ret;
}

void ObDropTableStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "ObDropTableStmt %d Begin\n", index);
  if (if_exists_)
  {
    print_indentation(fp, level + 1);
    fprintf(fp, "if_exists_ = TRUE\n");
  }
  for (int64_t i = 0; i < tables_.count(); i++)
  {
    if (i == 0)
    {
      print_indentation(fp, level + 1);
      fprintf(fp, "Tables := %.*s", tables_.at(i).length(), tables_.at(i).ptr());
    }
    else
    {
      print_indentation(fp, level + 1);
      fprintf(fp, ", %.*s", tables_.at(i).length(), tables_.at(i).ptr());
    }
  }
  fprintf(fp, "\n");
  print_indentation(fp, level);
  fprintf(fp, "ObDropTableStmt %d End\n", index);
}
