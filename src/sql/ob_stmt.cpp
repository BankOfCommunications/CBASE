#include "ob_stmt.h"
#include "ob_select_stmt.h"
#include "parse_malloc.h"
#include "ob_logical_plan.h"
#include "ob_schema_checker.h"
#include "common/utility.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObStmt::ObStmt(ObStringBuf* name_pool, StmtType type)
  : ObBasicStmt(type), name_pool_(name_pool), when_number_(OB_INVALID_INDEX)
  ,ud_where_parse_tree_(NULL)//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160425
{
  // means no where conditions
  //where_expr_id_ = OB_INVALID_ID;
  // placeholder of index 0
  tables_hash_.add_column_desc(OB_INVALID_ID, OB_INVALID_ID);
}

ObStmt::~ObStmt()
{
}

//add liumz, [multi_database.sql]:20150613
int ObStmt::write_db_table_name(const ObString &db_name, const ObString &table_name, ObString &db_table_name)
{
  int ret = OB_SUCCESS;
  if (db_name.length() != db_table_name.write(db_name.ptr(), db_name.length()))
  {
    ret = OB_ERROR;
  }
  else if (1 != db_table_name.write(".", 1))
  {
    ret = OB_ERROR;
  }
  else if (table_name.length() != db_table_name.write(table_name.ptr(), table_name.length()))
  {
    ret = OB_ERROR;
  }
  return ret;
}
//add:e

/**
 * using database db1;
 * select a.c1, t2.c1 from db1.t1 a, t2 where a.c1 = t2.c1;
 * (db1.a, a) -> a
 * (db1.t2, t2) -> db1.t2
 * select db1.t1.c1 from db1.t1 a;
 * (db1.t1, t1) -> db1.t1
 * select db1.t1.c1 from db1.t1 t1; syntax error
 */
//add liumz, [multi_database.sql]:20150615
uint64_t ObStmt::get_ref_table_name(
    const common::ObString& db_table_name,
    common::ObString& table_name)
{
  int32_t num = table_items_.size();
  for (int32_t i = 0; i < num; i++)
  {
    TableItem& item = table_items_[i];   
    if (table_name == item.alias_name_ || table_name == item.table_name_)
    {      
      return item.table_id_;
    }
  }
  for (int32_t i = 0; i < num; i++)
  {
    TableItem& item = table_items_[i];
    if (db_table_name == item.table_name_ || db_table_name == item.alias_name_)
    {
      table_name = db_table_name;
      return item.table_id_;
    }
  }
  return OB_INVALID_ID;
}
//add:e

int ObStmt::add_table_item(
  ResultPlan& result_plan,
  const ObString& db_name,
  ObString& table_name,
  //const ObString& table_name,
  const ObString& alias_name,
  uint64_t& table_id,
  const TableItem::TableType type,
  const uint64_t ref_id)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan.plan_tree_);
  if (logical_plan == NULL)
  {
    ret = OB_ERR_LOGICAL_PLAN_FAILD;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "Wrong invocation of ObStmt::add_table_item, logical_plan must exist!!!");
  }

  ObSchemaChecker* schema_checker = NULL;
  if (ret == OB_SUCCESS)
  {
    schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
    if (schema_checker == NULL)
    {
      ret = OB_ERR_SCHEMA_UNSET;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "Schema(s) are not set");
    }
  }

  TableItem item;
  //add liumz, [multi_database.sql]:20150613
  ObString db_table_name;
  int64_t buf_len = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1;
  char name_buf[buf_len];
  db_table_name.assign_buffer(name_buf, static_cast<ObString::obstr_size_t>(buf_len));
  //add:e
  if (ret == OB_SUCCESS)
  {
    switch (type)
    {
      case TableItem::ALIAS_TABLE:
        if (table_name == alias_name)
        {
          ret = OB_ERR_ILLEGAL_NAME;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "table '%.*s' must not alias the same name", table_name.length(), table_name.ptr());
          break;
        }
        /* go through */
      case TableItem::BASE_TABLE:
        item.ref_id_ = schema_checker->get_local_table_id(table_name);
        //add liumz, [multi_database.sql]:20150613        
        if (!IS_SYS_TABLE_ID(item.ref_id_))
        {          
          if(OB_SUCCESS != (ret = write_db_table_name(db_name, table_name, db_table_name)))
          {
            snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG, "Table name too long");
            break;
          }
          else
          {
            table_name = db_table_name;
            item.ref_id_ = schema_checker->get_local_table_id(table_name);
          }
        }
        //add:e
        if (item.ref_id_ == OB_INVALID_ID
          || (!is_show_stmt() && IS_SHOW_TABLE(item.ref_id_)))
        {
          ret = OB_ERR_TABLE_UNKNOWN;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "table '%.*s' does not exist", table_name.length(), table_name.ptr());
          break;
        }
        if (type == TableItem::BASE_TABLE)
          item.table_id_ = item.ref_id_;
        else
          item.table_id_ = logical_plan->generate_table_id();
        break;
      case TableItem::GENERATED_TABLE:
        if (ref_id == OB_INVALID_ID)
        {
          ret = OB_ERR_ILLEGAL_ID;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "illegal ref_id %lu", ref_id);
          break;
        }
        item.ref_id_ = ref_id;
        item.table_id_ = logical_plan->generate_table_id();
        break;
      default:
        /* won't be here */
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unknown table type");
        break;
    }
  }

  /* to check if the table is duplicated */
  if (ret == OB_SUCCESS)
  {
    TableItem old_item;
    int32_t size = table_items_.size();
    for (int32_t i = 0; ret == OB_SUCCESS && i < size; ++i)
    {
      old_item = table_items_[i];
      if (alias_name.length() == 0)
      {
        if (table_name == old_item.table_name_
            || table_name == old_item.alias_name_)
        {
          ret = OB_ERR_TABLE_DUPLICATE;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "table %.*s is ambiguous", table_name.length(), table_name.ptr());
          break;
        }
      }
      else if (old_item.alias_name_.length() == 0)
      {
        if (table_name == old_item.table_name_
            || alias_name == old_item.table_name_)
        {
          ret = OB_ERR_TABLE_DUPLICATE;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "table %.*s is ambiguous", old_item.table_name_.length(), old_item.table_name_.ptr());
          break;
        }
      }
      else
      {
        if (table_name == old_item.alias_name_)
        {
          ret = OB_ERR_TABLE_DUPLICATE;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "table %.*s is ambiguous", table_name.length(), table_name.ptr());
          break;
        }
        if (alias_name == old_item.table_name_
            || alias_name == old_item.alias_name_)
        {
          ret = OB_ERR_TABLE_DUPLICATE;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "table %.*s is ambiguous", alias_name.length(), alias_name.ptr());
          break;
        }
      }
    }
  }

  //add liumz, [multi_database.sql]:20150613
  if (ret == OB_SUCCESS)
  {
    if ((ret = ob_write_string(*name_pool_, db_name, item.db_name_)) != OB_SUCCESS)
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not make space for db name %.*s", db_name.length(), db_name.ptr());
    }
  }
  //add:e
  if (ret == OB_SUCCESS)
  {
    if ((ret = ob_write_string(*name_pool_, table_name, item.table_name_)) != OB_SUCCESS)
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not make space for table name %.*s", table_name.length(), table_name.ptr());
    }
  }
  if (ret == OB_SUCCESS)
  {
    if ((ret = ob_write_string(*name_pool_, alias_name, item.alias_name_)) != OB_SUCCESS)
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not make space for alias name %.*s", alias_name.length(), alias_name.ptr());
    }
  }
  if (ret == OB_SUCCESS)
  {
    item.type_ = type;
    item.has_scan_columns_ = false;
    if ((ret = table_items_.push_back(item)) != OB_SUCCESS)
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Add table item error");
  }

  // for tables bitset usage
  if (ret == OB_SUCCESS)
  {
    if ((ret = tables_hash_.add_column_desc(item.table_id_, OB_INVALID_ID)) != OB_SUCCESS)
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add table_id to hash table");
  }

  if (ret == OB_SUCCESS)
    table_id = item.table_id_;
  return ret;
}

uint64_t ObStmt::get_table_item(const ObString& table_name, TableItem** table_item) const
{
  // table name mustn't be empty
  int32_t num = table_items_.size();
  for (int32_t i = 0; i < num; i++)
  {
    TableItem& item = table_items_[i];
    if (table_name == item.table_name_ || table_name == item.alias_name_)
    {
      if (table_item)
        *table_item = &item;
      return item.table_id_;
    }
  }

  return OB_INVALID_ID;
}

TableItem* ObStmt::get_table_item_by_id(uint64_t table_id) const
{
  TableItem *table_item = NULL;
  int32_t num = table_items_.size();
  for (int32_t i = 0; i < num; i++)
  {
    if (table_items_[i].table_id_ == table_id)
    {
      table_item = &table_items_[i];
      break;
    }
  }
  return table_item;
}

//add dragon [common] 2016-8-24 09:18:29
int64_t TableItem::to_string(char* buf, const int64_t len) const
{
  int64_t pos = 0;
  char sb[64];
  switch (type_)
  {
  case 0:
    snprintf (sb, 64, "%s", "BASE_TABLE");
    break;
  case 1:
    snprintf (sb, 64, "%s", "ALIAS_TABLE");
    break;
  case 2:
    snprintf (sb, 64, "%s", "GENERATED_TABLE");
    break;
  default:
    snprintf (sb, 64, "%s", "UNKOWN_TABLE");
    break;
  }
  databuff_printf(buf, len, pos, "table_id[%ld], table_name[%.*s], ref_id[%ld], db_name[%.*s], "
                  "alias_name[%.*s], type[%s], has_scan_column[%s]",
                  table_id_, table_name_.length (), table_name_.ptr (), ref_id_,
                  db_name_.length (), db_name_.ptr (), alias_name_.length (),
                  alias_name_.ptr (), sb,
                  has_scan_columns_ ? "yes" : "no");
  return pos;
}
//add e

ColumnItem* ObStmt::get_column_item(
    const ObString* table_name,
    const ObString& column_name)
{
  uint64_t table_id = OB_INVALID_ID;

  if (table_name && table_name->length() != 0)
  {
    table_id = get_table_item(*table_name);
    if (table_id == OB_INVALID_ID)
      return NULL;
  }

  int32_t num = column_items_.size();
  for (int32_t i = 0; i < num; i++)
  {
    ColumnItem& column_item = column_items_[i];
    if (column_name == column_item.column_name_ &&
      (column_item.is_name_unique_ || table_id == column_item.table_id_))
    {
      return &column_item;
    }
  }

  return NULL;
}

ColumnItem* ObStmt::get_column_item_by_id(uint64_t table_id, uint64_t column_id) const
{
  ColumnItem *column_item = NULL;
  int32_t num = column_items_.size();
  for (int32_t i = 0; i < num; i++)
  {
    if (table_id == column_items_[i].table_id_ && column_id == column_items_[i].column_id_)
    {
      column_item = &column_items_[i];
      break;
    }
  }
  return column_item;
}

int ObStmt::add_column_item(const ColumnItem& column_item)
{
  int ret = OB_SUCCESS;
  // ugly implement
  TableItem* table_item = get_table_item_by_id(column_item.table_id_);
  if (table_item == NULL)
    ret = OB_ERROR;
  else
  {
    table_item->has_scan_columns_ = true;
    ret = column_items_.push_back(column_item);
  }
  return ret;
}

int ObStmt::add_column_item(
  ResultPlan& result_plan,
  const oceanbase::common::ObString& column_name,
  const oceanbase::common::ObString* table_name,
  ColumnItem** col_item)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ColumnItem column_item;
  TableItem* table_item = NULL;
  ColumnItem* ret_item = NULL;
  column_item.table_id_ = OB_INVALID_ID;
  column_item.column_id_= OB_INVALID_ID;

  if (table_name)
  {
    column_item.table_id_ = get_table_item(*table_name, &table_item);
    if (column_item.table_id_ == OB_INVALID_ID)
    {
      ret = OB_ERR_TABLE_UNKNOWN;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Unknown table name %.*s", table_name->length(), table_name->ptr());
      return ret;
    }
    column_item.is_name_unique_ = false;
  }
  else
  {
    column_item.is_name_unique_ = true;
  }

  if (column_item.table_id_ != OB_INVALID_ID)
  {
    ret = check_table_column(
              result_plan,
              column_name,
              *table_item,
              column_item.column_id_,
              column_item.data_type_);
    if (ret != OB_SUCCESS)
    {
      return ret;
    }
    else if (column_item.column_id_ == OB_INVALID_ID)
    {
      ret = OB_ERR_COLUMN_UNKNOWN;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Unknown column name %.*s", column_name.length(), column_name.ptr());
      return ret;
    }
    table_item->has_scan_columns_ = true;
  }
  else
  {
    int32_t num = table_items_.size();
    uint64_t  column_id = OB_INVALID_ID;
    ObObjType column_type = ObMinType;
    for (int32_t i = 0; i < num; i++)
    {
      TableItem& table_item = get_table_item(i);
      ret = check_table_column(result_plan, column_name, table_item, column_id, column_type);
      if (ret == OB_SUCCESS)
      {
        if (column_item.table_id_ != OB_INVALID_ID)
        {
          ret = OB_ERR_COLUMN_DUPLICATE;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "Column name %.*s is ambiguous", column_name.length(), column_name.ptr());
          return ret;
        }
        column_item.table_id_ = table_item.table_id_;
        column_item.column_id_ = column_id;
        column_item.data_type_ = column_type;
        table_item.has_scan_columns_ = true;
      }
      else if (ret != OB_ERR_COLUMN_UNKNOWN)
      {
        return ret;
      }
    }
    if (column_item.column_id_ == OB_INVALID_ID)
    {
      ret = OB_ERR_COLUMN_UNKNOWN;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Unknown column name %.*s", column_name.length(), column_name.ptr());
      return ret;
    }
  }

  ret = ob_write_string(*name_pool_, column_name, column_item.column_name_);
  if (ret != OB_SUCCESS)
  {
    ret = OB_ERR_PARSER_MALLOC_FAILED;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Malloc column name %.*s failed", column_name.length(), column_name.ptr());
    return ret;
  }
  // not be used now
  column_item.is_group_based_ = false;
  column_item.query_id_ = 0;

  if (table_name)
  {
    ret = column_items_.push_back(column_item);
    if (ret != OB_SUCCESS)
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add column %.*s", column_name.length(), column_name.ptr());
      return ret;
    }
    ret_item = column_items_.last();
  }
  else
  {
    bool bExist = false;
    int32_t num = column_items_.size();
    for (int32_t i = 0; i < num; i++)
    {
      ColumnItem& item = column_items_[i];
      if (column_name == item.column_name_)
      {
        item.is_name_unique_ = true;//comment liumz, false->true
        ret_item = &item;
        bExist = true;
        break;
      }
    }
    if (!bExist)
    {
      ret = column_items_.push_back(column_item);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
            "Can not add column %.*s", column_name.length(), column_name.ptr());
        return ret;
      }
      ret_item = column_items_.last();
    }
  }
  if (col_item != NULL)
    *col_item = ret_item;

  return ret;
}

int ObStmt::check_table_column(
    ResultPlan& result_plan,
    const ObString& column_name,
    const TableItem& table_item,
    uint64_t& column_id,
    ObObjType& column_type)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  column_id = OB_INVALID_ID;

  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan.plan_tree_);
  if (logical_plan == NULL)
  {
    ret = OB_ERR_LOGICAL_PLAN_FAILD;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Wrong invocation of ObStmt::check_table_column, logical_plan must exist!!!");
  }
  ObSchemaChecker* schema_checker = NULL;
  if (ret == OB_SUCCESS)
  {
    schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
    if (schema_checker == NULL)
    {
      ret = OB_ERR_SCHEMA_UNSET;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Schema(s) are not set");
    }
  }

  if (ret == OB_SUCCESS)
  {
    switch(table_item.type_)
    {
      case TableItem::BASE_TABLE:
        // get through
      case TableItem::ALIAS_TABLE:
      {
        const ObColumnSchemaV2 *col_schema = schema_checker->get_column_schema(table_item.table_name_, column_name);
        if (col_schema != NULL)
        {
          column_id = col_schema->get_id();
          column_type = col_schema->get_type();
        }
        break;
      }
      case TableItem::GENERATED_TABLE:
      {
        ObBasicStmt* stmt = logical_plan->get_query(table_item.ref_id_);
        if (stmt == NULL)
        {
          ret = OB_ERR_ILLEGAL_ID;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "Wrong query id %lu", table_item.ref_id_);
        }
        ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
        int32_t num = select_stmt->get_select_item_size();
        for (int32_t i = 0; i < num; i++)
        {
          const SelectItem& select_item = select_stmt->get_select_item(i);
          if (column_name == select_item.alias_name_)
          {
            if (column_id == OB_INVALID_ID)
            {
              column_id = i + OB_APP_MIN_COLUMN_ID;
              column_type = select_item.type_;
            }
            else
            {
              ret = OB_ERR_COLUMN_DUPLICATE;
              snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                  "column %.*s is ambiguous", column_name.length(), column_name.ptr());
              break;
            }
          }
        }
        break;
      }
      default:
        // won't be here
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unknown table type");
        break;
    }
    if (ret == OB_SUCCESS && column_id == OB_INVALID_ID)
    {
      ret = OB_ERR_COLUMN_UNKNOWN;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Unknown column");
    }
  }
  return ret;
}

int32_t ObStmt::get_table_bit_index(uint64_t table_id) const
{
  int64_t idx = tables_hash_.get_idx(table_id, OB_INVALID_ID);
  return static_cast<int32_t>(idx);
}

//add dragon 2016-8-24 11:09:28
void ObStmt::print_table_items () const
{
  if (table_items_.size() > 0)
  {
    TBSYS_LOG(INFO, "<TableItemList Begin>\n");
    for (int i = 0; i < table_items_.size(); ++i)
    {
      TableItem& item = table_items_[i];
      TBSYS_LOG(INFO, "{Num %d, TableId:%lu, TableName:%.*s, ",
                i, item.table_id_, item.table_name_.length(), item.table_name_.ptr());
      if (item.alias_name_.length() > 0)
        TBSYS_LOG(INFO, "AliasName:%.*s, ", item.alias_name_.length(), item.alias_name_.ptr());
      else
        TBSYS_LOG(INFO, "AliasName:NULL, ");
      if (item.type_ == TableItem::BASE_TABLE)
        TBSYS_LOG(INFO, "Type:BASE_TABLE, ");
      else if (item.type_ == TableItem::ALIAS_TABLE)
        TBSYS_LOG(INFO, "Type:ALIAS_TABLE, ");
      else if (item.type_ == TableItem::GENERATED_TABLE)
        TBSYS_LOG(INFO, "Type:GENERATED_TABLE, ");
      TBSYS_LOG(INFO,"RefId: %lu}\n", item.ref_id_);
    }
    TBSYS_LOG(INFO, "<TableItemList End>\n");
  }
}
//add e

void ObStmt::print(FILE* fp, int32_t level, int32_t index)
{
  OB_ASSERT(index >= 0);
  UNUSED(index);
  int32_t i;
  if (table_items_.size() > 0)
  {
    print_indentation(fp, level);
    fprintf(fp, "<TableItemList Begin>\n");
    for (i = 0; i < table_items_.size(); ++i)
    {
      TableItem& item = table_items_[i];
      print_indentation(fp, level + 1);
      fprintf(fp, "{Num %d, TableId:%lu, TableName:%.*s, ",
        i, item.table_id_, item.table_name_.length(), item.table_name_.ptr());
      if (item.alias_name_.length() > 0)
        fprintf(fp, "AliasName:%.*s, ", item.alias_name_.length(), item.alias_name_.ptr());
      else
        fprintf(fp, "AliasName:NULL, ");
      if (item.type_ == TableItem::BASE_TABLE)
        fprintf(fp, "Type:BASE_TABLE, ");
      else if (item.type_ == TableItem::ALIAS_TABLE)
        fprintf(fp, "Type:ALIAS_TABLE, ");
      else if (item.type_ == TableItem::GENERATED_TABLE)
        fprintf(fp, "Type:GENERATED_TABLE, ");
      fprintf(fp, "RefId: %lu}\n", item.ref_id_);
    }
    print_indentation(fp, level);
    fprintf(fp, "<TableItemList End>\n");
  }

  if (column_items_.size() > 0)
  {
    print_indentation(fp, level);
    fprintf(fp, "<ColumnItemList Begin>\n");
    for (i = 0; i < column_items_.size(); ++i)
    {
      ColumnItem& item = column_items_[i];
      print_indentation(fp, level + 1);
      fprintf(fp, "{Num %d, ColumnId:%lu, ColumnName:%.*s, TableRef:%lu}\n", i,
        item.column_id_, item.column_name_.length(), item.column_name_.ptr(),
        item.table_id_);
    }
    print_indentation(fp, level);
    fprintf(fp, "<ColumnItemList End>\n");
  }

  if (where_expr_ids_.size() > 0)
  {
    print_indentation(fp, level);
    fprintf(fp, "WHERE ::=");
    for (i = 0; i < where_expr_ids_.size(); i++)
      fprintf(fp, " <%lu>", where_expr_ids_[i]);
    fprintf(fp, "\n");
  }

  if (when_number_ != OB_INVALID_INDEX)
  {
    print_indentation(fp, level);
    fprintf(fp, "WHEN NUMBER ::= %ld\n", when_number_);
  }
}
