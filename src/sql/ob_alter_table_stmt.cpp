/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_alter_table_stmt.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_alter_table_stmt.h"
#include "ob_schema_checker.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObAlterTableStmt::ObAlterTableStmt(ObStringBuf* name_pool)
  : ObBasicStmt(ObBasicStmt::T_ALTER_TABLE), name_pool_(name_pool)
  , table_id_(OB_INVALID_ID), max_column_id_(OB_INVALID_ID)
{
  name_pool_ = name_pool;
}

ObAlterTableStmt::ObAlterTableStmt()
  : ObBasicStmt(ObBasicStmt::T_ALTER_TABLE), name_pool_(NULL)
  , table_id_(OB_INVALID_ID), max_column_id_(OB_INVALID_ID)
{
}

ObAlterTableStmt::~ObAlterTableStmt()
{
}

int ObAlterTableStmt::init()
{
  int ret = OB_SUCCESS;
  has_table_rename_ = false;//Add By LiuJun.
  ret = columns_map_.create(hash::cal_next_prime(OB_MAX_USER_DEFINED_COLUMNS_COUNT));
  return ret;
}
int ObAlterTableStmt::set_db_name(ResultPlan& result_plan,const common::ObString& dbname)
{
  int ret = OB_SUCCESS;
  if ((ret = ob_write_string(*name_pool_, dbname, db_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "Allocate memory for db name failed");
  }
  return ret;
}
int ObAlterTableStmt::set_table_name(ResultPlan& result_plan, const ObString& table_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;

  ObSchemaChecker* schema_checker = NULL;
  const ObTableSchema* table_schema = NULL;
  //add dolphin [database manager]@20150617:b
  ObString dt;
  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  //modify liuxiao [database manager.dolphin_bug_fix] 20150723
  //dt.concat(db_name_,table_name);
  dt.write(db_name_.ptr(), db_name_.length());
  dt.write(".", 1);
  dt.write(table_name.ptr(), table_name.length());
  //modify e
  //add:e
  TBSYS_LOG(DEBUG,"DT:%.*s",dt.length(),dt.ptr());
  if ((schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)) == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }
  else if ((table_schema = schema_checker->get_table_schema(/** modify dolphin [database manager]@20150617 table_name*/dt)) == NULL)
  {
    ret = OB_ERR_TABLE_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Table '%.*s' doesn't exist", table_name.length(), table_name.ptr());
  }
  else if((table_id_ = table_schema->get_table_id()) == OB_INVALID_ID)
  {
    ret = OB_ERR_TABLE_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Table '%.*s' doesn't exist", table_name.length(), table_name.ptr());
  }
  else if ((max_column_id_ = table_schema->get_max_column_id()) == OB_INVALID_ID)
  {
    ret = OB_ERROR;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Get max column id of Table '%.*s' failed", table_name.length(), table_name.ptr());
  }
  else if ((ret = ob_write_string(*name_pool_, table_name, table_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Allocate memory for table name failed");
  }
  return ret;
}

int ObAlterTableStmt::set_new_table_name(ResultPlan& result_plan, const ObString& table_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  //add liuxiao [database manager bug fix]20150723
  //alter表名时，仿照建表时的操作添加对应的库名，实现方法仿照set_table_name
  ObString dt;
  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  dt.write(db_name_.ptr(), db_name_.length());
  dt.write(".", 1);
  dt.write(table_name.ptr(), table_name.length());
  //add:e
  if (name_pool_ == NULL)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Name pool need to be set, ret=%d", ret);
  }
  else if ((schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)) == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }
  //modif liuxiao [secondary index] 20150723
  //原方法中的table_name全都改为dt，拼接库名
  /*else if(schema_checker->get_table_id(table_name) != OB_INVALID_ID)
  {
    ret = OB_ERR_TABLE_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Table '%.*s' already exist", table_name.length(), table_name.ptr());
  }
  else if ((ret = ob_write_string(*name_pool_, table_name, new_table_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Allocate memory for table name failed");
  }*/
  else if(schema_checker->get_table_id(dt) != OB_INVALID_ID)
  {
    ret = OB_ERR_TABLE_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Table '%.*s' already exist", table_name.length(), table_name.ptr());
  }
  else if ((ret = ob_write_string(*name_pool_, table_name, new_table_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Allocate memory for table name failed");
  }
  //modif e
  return ret;
}

int ObAlterTableStmt::add_column(ResultPlan& result_plan, const ObColumnDef& column_def)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  ObColumnDef col = column_def;
  //add liumz, [database manager.dolphin_bug_fix]@20150707:b
  ObString dt;
  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  //modify liuxiao [database manager.dolphin_bug_fix] 20150723
  //dt.concat(db_name_,table_name_);
  dt.write(db_name_.ptr(), db_name_.length());
  dt.write(".", 1);
  dt.write(table_name_.ptr(), table_name_.length());
  //modify e
  //add:e
  if (name_pool_ == NULL)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Name pool need to be set, ret=%d", ret);
  }
  else if (table_name_.length() <= 0)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Alter table name must be set first");
  }
  else if (column_def.action_ != ADD_ACTION)
  {
    ret = OB_ERR_RESOLVE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Wrong action type '%d' of add column", column_def.action_);
  }
  else if (column_def.data_type_ == ObCreateTimeType
    || column_def.data_type_ == ObModifyTimeType)
  {
    ret = OB_ERR_CREAT_MODIFY_TIME_COLUMN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Add column '%.*s' with type ObCreateTimeType/ObModifyTimeType is not allowed",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if (columns_map_.get(column_def.column_name_) != NULL)
  {
    ret = OB_ERR_COLUMN_DUPLICATE;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Duplicate column name '%.*s'",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if ((schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)) == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }
  else if ((col.column_id_ = ++max_column_id_) <= OB_APP_MIN_COLUMN_ID)
  {
    ret = OB_ERR_PARSE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Error table '%.*s' status", table_name_.length(), table_name_.ptr());
  }
  //mod liumz, [database manager.dolphin_bug_fix]@20150707:b
  //else if (schema_checker->column_exists(table_name_, column_def.column_name_))
  else if (schema_checker->column_exists(dt, column_def.column_name_))
  //mod:e
  {
    ret = OB_ERR_COLUMN_DUPLICATE;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Column '%.*s' already exists",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if ((ret = ob_write_string(*name_pool_, column_def.column_name_, col.column_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Can not malloc space for column name '%.*s'", 
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if ((column_def.default_value_.get_type() == ObVarcharType 
    || column_def.default_value_.get_type() == ObDecimalType)
    && (ret = ob_write_obj(*name_pool_, column_def.default_value_, col.default_value_)) == OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Can not malloc default value string for column name '%.*s'",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if (columns_map_.set(col.column_name_, col, 0) != hash::HASH_INSERT_SUCC)
  {
    ret = OB_ERR_RESOLVE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Add new column '%.*s' failed", col.column_name_.length(), col.column_name_.ptr());
  }
  return ret;
}

int ObAlterTableStmt::drop_column(ResultPlan& result_plan, const ObColumnDef& column_def)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  ObColumnDef col = column_def;
  //add liumz, [database manager.dolphin_bug_fix]@20150707:b
  ObString dt;
  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  //modify liuxiao [database manager.dolphin_bug_fix] 20150723
  //dt.concat(db_name_,table_name_);
  dt.write(db_name_.ptr(), db_name_.length());
  dt.write(".", 1);
  dt.write(table_name_.ptr(), table_name_.length());
  //modify e
  //add:e
  if (name_pool_ == NULL)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Name pool need to be set, ret=%d", ret);
  }
  else if (table_name_.length() <= 0)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Alter table name must be set first");
  }
  else if (column_def.action_ != DROP_ACTION)
  {
    ret = OB_ERR_RESOLVE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Wrong action type '%d' of drop column", column_def.action_);
  }
  else if (columns_map_.get(column_def.column_name_) != NULL)
  {
    ret = OB_ERR_COLUMN_DUPLICATE;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Duplicate column name '%.*s'",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if ((schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)) == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }
  else if ((column_schema = schema_checker->get_column_schema(
                                                /*table_name_,*/ dt, //mod liumz, dolphin_bug_fix
                                                column_def.column_name_)) == NULL
    || (col.column_id_ = column_schema->get_id()) == OB_INVALID_ID)
  {
    ret = OB_ERR_COLUMN_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Column '%.*s' doesn't exist",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if (column_schema->get_type() == ObCreateTimeType
    || column_schema->get_type() == ObModifyTimeType)
  {
    ret = OB_ERR_CREAT_MODIFY_TIME_COLUMN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Drop column '%.*s' with type ObCreateTimeType/ObModifyTimeType is not allowed",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  //mod liumz, [database manager.dolphin_bug_fix]@20150707:b
  //else if (schema_checker->is_rowkey_column(table_name_, column_def.column_name_))
  else if (schema_checker->is_rowkey_column(dt, column_def.column_name_))
  //mod:e
  {
    ret = OB_ERR_RESOLVE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Primary key column can not be dropped, column name = '%.*s'",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if ((ret = ob_write_string(*name_pool_, column_def.column_name_, col.column_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Can not malloc space for column name '%.*s'", 
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if (columns_map_.set(col.column_name_, col, 0) != hash::HASH_INSERT_SUCC)
  {
    ret = OB_ERR_RESOLVE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Drop column '%.*s' failed", col.column_name_.length(), col.column_name_.ptr());
  }
  return ret;
}
int ObAlterTableStmt::rename_column(ResultPlan& result_plan, const ObColumnDef& column_def)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  ObColumnDef col = column_def;
  //add liumz, [database manager.dolphin_bug_fix]@20150707:b
  ObString dt;
  // mod by zhangcd [multi_database.secondary_index] 20150724:b
//  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
//  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  char buf[OB_MAX_COMPLETE_TABLE_NAME_LENGTH] = {0};
  dt.assign_buffer(buf, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
  // mod:e
  //modify liuxiao [database manager.dolphin_bug_fix] 20150723
  //provided by zhangcd
  //dt.concat(db_name_,table_name_);
  if(db_name_.length() >= OB_MAX_DATBASE_NAME_LENGTH)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "database name is too long!");
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "database name is too long!");
  }
  else if(table_name_.length() >= OB_MAX_TABLE_NAME_LENGTH)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "table name is too long!");
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "table name is too long!");
  }
  if(OB_SUCCESS == ret)
  {
    dt.write(db_name_.ptr(), db_name_.length());
    dt.write(".", 1);
    dt.write(table_name_.ptr(), table_name_.length());
  }
  //modify e
  //add:e
  if (name_pool_ == NULL)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Name pool need to be set, ret=%d", ret);
  }
  else if (table_name_.length() <= 0)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Alter table name must be set first");
  }
  else if (column_def.action_ != RENAME_ACTION)
  {
    ret = OB_ERR_RESOLVE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Wrong action type '%d' of rename column", column_def.action_);
  }
  else if (columns_map_.get(column_def.column_name_) != NULL)
  {
    ret = OB_ERR_COLUMN_DUPLICATE;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Duplicate column name '%.*s'",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if ((schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)) == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }
  //mod liumz, [database manager.dolphin_bug_fix]@20150707:b
  //else if ((col.column_id_ = schema_checker->get_column_id(table_name_, column_def.column_name_))
  else if ((col.column_id_ = schema_checker->get_column_id(dt, column_def.column_name_))
            == OB_INVALID_ID)
  //mod:e
  {
    ret = OB_ERR_COLUMN_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Column '%.*s' doesn't exist",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  //mod liumz, [database manager.dolphin_bug_fix]@20150707:b
  //else if (schema_checker->column_exists(table_name_, column_def.new_column_name_))
  else if (schema_checker->column_exists(dt, column_def.new_column_name_))
  //mod:e
  {
    ret = OB_ERR_COLUMN_DUPLICATE;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Column '%.*s' already exist",/*add liuj [Alter_Rename] [JHOBv0.1] 20150104*/
        column_def.new_column_name_.length(), column_def.new_column_name_.ptr());
  }
  else if ((ret = ob_write_string(
                      *name_pool_, 
                      column_def.column_name_, 
                      col.column_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Can not malloc space for column name '%.*s'", 
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if ((ret = ob_write_string(
                      *name_pool_, 
                      column_def.new_column_name_, 
                      col.new_column_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Can not malloc space for column name '%.*s'", 
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if (columns_map_.set(col.column_name_, col, 0) != hash::HASH_INSERT_SUCC)
  {
    ret = OB_ERR_RESOLVE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Rename column '%.*s' failed", col.column_name_.length(), col.column_name_.ptr());
  }
  return ret;
}

int ObAlterTableStmt::alter_column(ResultPlan& result_plan, const ObColumnDef& column_def)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  ObColumnDef col;
  //add liumz, [database manager.dolphin_bug_fix]@20150707:b
  ObString dt;
  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  //modify liuxiao [database manager.dolphin_bug_fix]20150723
  //dt.concat(db_name_,table_name_);
  dt.write(db_name_.ptr(), db_name_.length());
  dt.write(".", 1);
  dt.write(table_name_.ptr(), table_name_.length());
  //modify e
  //add:e
  /* only NOT NULL/DEFAULT can be altered */
  if (columns_map_.get(column_def.column_name_, col) != hash::HASH_EXIST)
  {
    col = column_def;
  }
  else
  {
    col.not_null_ = column_def.not_null_;
    col.default_value_ = column_def.default_value_;
  }
  
  if (name_pool_ == NULL)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Name pool need to be set, ret=%d", ret);
  }
  else if (table_name_.length() <= 0)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Alter table name must be set first");
  }
  //mod fyd [NotNULL_check] [JHOBv0.1] 20140108:b
  //else if (column_def.action_ != ALTER_ACTION)
  else if (column_def.action_ != ALTER_ACTION_NULL)
  //mod 20140108:e
  {
    ret = OB_ERR_RESOLVE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Wrong action type '%d' of alter column", column_def.action_);
  }
  //mod fyd [NotNULL_check] [JHOBv0.1] 20140108:b
  //else if (col.action_ != ALTER_ACTION)
  else if (col.action_ != ALTER_ACTION_NULL)
  //mod 20140108:e
  {
    ret = OB_ERR_COLUMN_DUPLICATE;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Duplicate column name '%.*s'",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if ((schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)) == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }
  //mod liumz, [database manager.dolphin_bug_fix]@20150707:b
  //else if ((col.column_id_ = schema_checker->get_column_id(table_name_, column_def.column_name_))
  else if ((col.column_id_ = schema_checker->get_column_id(dt, column_def.column_name_))
        == OB_INVALID_ID)
  //mod:e
  {
    ret = OB_ERR_COLUMN_DUPLICATE;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Column '%.*s' doesn't exist",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if ((ret = ob_write_string(
                      *name_pool_, 
                      column_def.column_name_, 
                      col.column_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Can not malloc space for column name '%.*s'", 
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if ((column_def.default_value_.get_type() == ObVarcharType 
    || column_def.default_value_.get_type() == ObDecimalType)
    && (ret = ob_write_obj(*name_pool_, column_def.default_value_, col.default_value_)) == OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Can not malloc default value string for column name '%.*s'",
        column_def.column_name_.length(), column_def.column_name_.ptr());
  }
  else if (columns_map_.set(col.column_name_, col, 0) != hash::HASH_INSERT_SUCC)
  {
    ret = OB_ERR_RESOLVE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Rename column '%.*s' failed", col.column_name_.length(), col.column_name_.ptr());
  }
  return ret;
}

void ObAlterTableStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "ObAlterTableStmt %d Begin\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "table_id = %lu\n", table_id_);
  print_indentation(fp, level + 1);
  fprintf(fp, "Table Name ::= %.*s\n", table_name_.length(), table_name_.ptr());
  if (new_table_name_.length() > 0)
  {
    print_indentation(fp, level + 1);
    fprintf(fp, "NEW Table Name ::= %.*s\n", new_table_name_.length(), new_table_name_.ptr());
  }
  print_indentation(fp, level + 1);
  fprintf(fp, "COLUMN DEFINITION(s) ::=\n");
  int32_t i = 1;
  hash::ObHashMap<common::ObString, ObColumnDef>::iterator iter;
  for (iter = column_begin(); iter != column_end(); iter++)
  {
    print_indentation(fp, level + 2);
    fprintf(fp, "Column(%d) ::=\n", i++);
    iter->second.print(fp, level + 3);
  }
  print_indentation(fp, level);
  fprintf(fp, "ObAlterTableStmt %d End\n", index);
}

