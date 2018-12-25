#include "common/ob_schema.h"
#include "common/ob_scan_param.h"
#include "common/ob_mutator.h"
#include "common/ob_row.h"
#include "common/ob_simple_tpl.h"
#include "updateserver/ob_sstable_mgr.h"
#if ROWKEY_IS_OBJ
#include "sql/ob_values.h"
#include "sql/ob_physical_plan.h"
#include "sql/ob_expr_values.h"
#include "sql/ob_ups_modify.h"
#include "sql/ob_multiple_get_merge.h"
#include "sql/ob_multiple_scan_merge.h"
#include "sql/ob_empty_row_filter.h"
#include "sql/ob_when_filter.h"
#include "sql/ob_project.h"
#include "sql/ob_row_count.h"
#include "sql/ob_mem_sstable_scan.h"
#include "sql/ob_sql_session_info.h"
#include "sql/ob_inc_scan.h"
#include "sql/ob_insert_dbsem_filter.h"
#else
namespace oceanbase
{
  namespace sql
  {};
};
#endif

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
using namespace oceanbase::sql;

int make_version_range(ObVersionRange& version_range, int64_t start_version)
{
  int err = OB_SUCCESS;
  if (start_version > 0)
  {
    version_range.border_flag_.set_inclusive_start();
    version_range.border_flag_.unset_inclusive_end();
    version_range.border_flag_.set_max_value();
    version_range.start_version_ = start_version;
    version_range.end_version_ = 0;
  }
  return err;
}

#if ROWKEY_IS_OBJ
int set_range(ObDataBuffer& buf, ObScanParam& scan_param, ObString& table_name, int64_t start_version, const char* start_key, const char* end_key)
{
  int err = OB_SUCCESS;
  ObNewRange range;
  ObVersionRange version_range;
  if (0 == strcmp(start_key, "min"))
  {
    TBSYS_LOG(INFO, "start_key is min");
    range.start_key_.set_min_row();
  }
  else if (OB_SUCCESS != (err = parse_rowkey(buf, range.start_key_, start_key, strlen(start_key))))
  {
    TBSYS_LOG(ERROR, "hex2bin()=>%d", err);
  }
  if (0 == strcmp(end_key, "max"))
  {
    TBSYS_LOG(INFO, "end_key is max");
    range.end_key_.set_max_row();
  }
  else if (OB_SUCCESS != (err = parse_rowkey(buf, range.end_key_, end_key, strlen(end_key))))
  {
    TBSYS_LOG(ERROR, "hex2bin()=>%d", err);
  }
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  make_version_range(version_range, start_version);
  scan_param.set_version_range(version_range);
  scan_param.set(OB_INVALID_ID, table_name, range);
  TBSYS_LOG(DEBUG, "scan_param{table_id=%ld, table_name=%.*s}", scan_param.get_table_id(),
            scan_param.get_table_name().length(), scan_param.get_table_name().ptr());
  return err;
}

int set_range2(ObScanParam& scan_param, ObString& table_name, int64_t start_version,
              ObRowkey& start_key, ObRowkey& end_key)
{
  int err = OB_SUCCESS;
  ObNewRange range;
  ObVersionRange version_range;
  range.start_key_ = start_key;
  range.end_key_ = end_key;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  make_version_range(version_range, start_version);
  scan_param.set_version_range(version_range);
  scan_param.set(OB_INVALID_ID, table_name, range);
  return err;
}

int choose_rowkey(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr,
                  const ObString& table_name, ObRowkey& rowkey, int64_t seed)
{
  int err = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
  {
    err = OB_SCHEMA_ERROR;
  }
  else if (OB_SUCCESS != (err = rand_rowkey(buf, rowkey, table_schema->get_rowkey_info(), seed)))
  {
    TBSYS_LOG(ERROR, "rand_rowkey()=>%d", err);
  }
  TBSYS_LOG(DEBUG, "choose_rowkey('%s')", to_cstring(rowkey));
  return err;
}

int64_t rand_choose_column(const ObRowkeyInfo& rowkey_info, const ObColumnSchemaV2* column_schema,
                           int64_t n_column, int64_t seed, const bool& exclude_rowkey)
{
  int64_t column_id = -1;
  int64_t max_retry_time = 1000;
  for(int64_t i = 0; i < max_retry_time && 0 > column_id; i++)
  {
    column_id = (seed = rand2(seed)) % n_column;
    if (column_schema[column_id].get_join_info())
    {
      column_id = -1;
    }
    else if (exclude_rowkey && rowkey_info.is_rowkey_column(column_schema[column_id].get_id()))
    {
      column_id = -1;
    }
    else
    {
      switch(column_schema[column_id].get_type())
      {
        case ObIntType:
          //case ObFloatType:
        case ObVarcharType:
          break;
        default:
          column_id = -1;
          break;
      }
    }
  }
  return column_id;
}

int choose_column(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr,
                  const ObString& table_name, ObString& column_name, int64_t seed, const bool exclude_rowkey=false)
{
  int err = OB_SUCCESS;
  int64_t n_column = 0;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  int64_t column_id = 0;
  if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
  {
    err = OB_SCHEMA_ERROR;
  }
  else if (NULL == (column_schema = schema_mgr.get_table_schema(table_schema->get_table_id(), (int32_t&)n_column)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(ERROR, "schema_mgr.get_table_schema(table_id=%ld)=>%d", table_schema->get_table_id(), err);
  }
  else if (0 > (column_id = rand_choose_column(table_schema->get_rowkey_info(), column_schema, n_column, seed, exclude_rowkey)))
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "choose_column(id=%ld)=>%d", column_id, err);
  }
  else if (OB_SUCCESS != (err = alloc_str(buf, column_name, column_schema[column_id].get_name())))
  {
    TBSYS_LOG(ERROR, "alloc_str()=>%d", err);
  }
  TBSYS_LOG(DEBUG, "choose_column('%*s')", column_name.length(), column_name.ptr());
  return err;
}

#else
int set_range(ObDataBuffer& buf, ObScanParam& scan_param, ObString& table_name, int64_t start_version, const char* start_key, const char* end_key)
{
  int err = OB_SUCCESS;
  ObRange range;
  ObVersionRange version_range;
  if (0 == strcmp(start_key, "min"))
  {
    TBSYS_LOG(INFO, "start_key is min");
    range.border_flag_.set_min_value();
  }
  else if (OB_SUCCESS != (err = hex2bin(buf, range.start_key_, start_key, strlen(start_key))))
  {
    TBSYS_LOG(ERROR, "hex2bin()=>%d", err);
  }
  if (0 == strcmp(end_key, "max"))
  {
    TBSYS_LOG(INFO, "end_key is max");
    range.border_flag_.set_max_value();
  }
  else if (OB_SUCCESS != (err = hex2bin(buf, range.end_key_, end_key, strlen(end_key))))
  {
    TBSYS_LOG(ERROR, "hex2bin()=>%d", err);
  }
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  make_version_range(version_range, start_version);
  scan_param.set_version_range(version_range);
  scan_param.set(OB_INVALID_ID, table_name, range);
  TBSYS_LOG(DEBUG, "scan_param{table_id=%ld, table_name=%.*s}", scan_param.get_table_id(),
            scan_param.get_table_name().length(), scan_param.get_table_name().ptr());
  return err;
}

int set_range2(ObScanParam& scan_param, ObString& table_name, int64_t start_version,
              ObString& start_key, ObString& end_key)
{
  int err = OB_SUCCESS;
  ObRange range;
  ObVersionRange version_range;
  range.start_key_ = start_key;
  range.end_key_ = end_key;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  make_version_range(version_range, start_version);
  scan_param.set_version_range(version_range);
  scan_param.set(OB_INVALID_ID, table_name, range);
  return err;
}

int choose_rowkey(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr,
                  const ObString& table_name, ObString& rowkey, int64_t seed)
{
  int err = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  char _rowkey[OB_MAX_ROW_KEY_LENGTH];
  if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
  {
    err = OB_SCHEMA_ERROR;
  }
  else if (OB_SUCCESS != (err = rand_str(_rowkey, table_schema->get_rowkey_max_length() + 1, seed)))
  {
    TBSYS_LOG(ERROR, "rand_str(len=%ld)=>%d", sizeof(_rowkey), err);
  }
  else if (OB_SUCCESS != (err = alloc_str(buf, rowkey, _rowkey)))
  {
    TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", _rowkey, err);
  }
  TBSYS_LOG(DEBUG, "choose_rowkey('%*s')", rowkey.length(), rowkey.ptr());
  return err;
}

int64_t rand_choose_column(const ObColumnSchemaV2* column_schema, int64_t n_column, int64_t seed, const bool exclude_rowkey=false)
{
  int64_t column_id = -1;
  UNUSED(exclude_rowkey);
  while(0 > column_id)
  {
    column_id = (seed = rand2(seed)) % n_column;
    if (column_schema[column_id].get_join_info())
    {
      column_id = -1;
    }
    else
    {
      switch(column_schema[column_id].get_type())
      {
        case ObIntType:
        case ObFloatType:
        case ObVarcharType:
          break;
        default:
          column_id = -1;
          break;
      }
    }
  }
  return column_id;
}

int choose_column(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr,
                  const ObString& table_name, ObString& column_name, int64_t seed, const bool exclude_rowkey=true)
{
  int err = OB_SUCCESS;
  int64_t n_column = 0;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  UNUSED(exclude_rowkey);
  if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
  {
    err = OB_SCHEMA_ERROR;
  }
  else if (NULL == (column_schema = schema_mgr.get_table_schema(table_schema->get_table_id(), (int32_t&)n_column)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(ERROR, "schema_mgr.get_table_schema(table_id=%ld)=>%d", table_schema->get_table_id(), err);
  }
  else if (OB_SUCCESS != (err = alloc_str(buf, column_name, column_schema[rand_choose_column(column_schema, n_column, seed)].get_name())))
  {
    TBSYS_LOG(ERROR, "alloc_str()=>%d", err);
  }
  TBSYS_LOG(DEBUG, "choose_column('%*s')", column_name.length(), column_name.ptr());
  return err;
}
#endif

int get_table_id(ObSchemaManagerV2& schema_mgr, ObString& table_name, int64_t& table_id)
{
  int err = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(ERROR, "schema_mgr->get_table_schema(table_name=%.*s)=>NULL", table_name.length(), table_name.ptr());
  }
  else
  {
    table_id = table_schema->get_table_id();
  }
  return err;
}

int get_column_id(ObSchemaManagerV2& schema_mgr, ObString& table_name, ObString& column_name, int64_t& column_id)
{
  int err = OB_SUCCESS;
  const ObColumnSchemaV2* column_schema = NULL;
  if (NULL == (column_schema = schema_mgr.get_column_schema(table_name, column_name)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(ERROR, "schema_mgr->get_table_schema(table_name=%.*s)=>NULL", table_name.length(), table_name.ptr());
  }
  else
  {
    column_id = column_schema->get_id();
  }
  return err;
}

int choose_table(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr,
                 ObString& table_name, const char* _table_name, int64_t seed)
{
  int err = OB_SUCCESS;
  int64_t w = -1;
  int64_t max_weight = -1;
  char cbuf[4096];
  const ObTableSchema* chosen_table = NULL;
  const char* selected_table_name = NULL;
  if (NULL == _table_name)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (0 != strcmp(_table_name, "any"))
  {
    int64_t pos = 0;
    if (OB_SUCCESS != (err = expand(cbuf, sizeof(cbuf), pos, _table_name)))
    {
      TBSYS_LOG(ERROR, "expand(spec=%s)=>%d", _table_name, err);
    }
    else if (NULL == (selected_table_name = choose(cbuf, seed)))
    {
      err = OB_ENTRY_NOT_EXIST;
      TBSYS_LOG(ERROR, "fail to choose_table(seed=%ld)", seed);
    }
    else if (OB_SUCCESS != (err = alloc_str(buf, table_name, selected_table_name)))
    {
      TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", selected_table_name, err);
    }
  }
  else
  {
    w = seed;
    for(const ObTableSchema* table_schema = schema_mgr.table_begin();
        table_schema != schema_mgr.table_end(); table_schema++)
    {
      //TBSYS_LOG(INFO, "choose_table(cur=%s[%ld])", table_schema->get_table_name(), table_schema->get_table_id());
      if (table_schema->get_table_id() >= OB_APP_MIN_TABLE_ID && table_schema->get_table_id() < OB_APP_MIN_TABLE_ID + 10000
          && (w = rand2(w)) > max_weight)
      {
        max_weight = w;
        chosen_table = table_schema;
        //break; // always choose first user table, otherwise build_rand_scan_param() will not work
      }
    }
    if (NULL == chosen_table)
    {
      err = OB_ENTRY_NOT_EXIST;
      TBSYS_LOG(ERROR, "no table been chosen");
    }
    else if (OB_SUCCESS != (err = alloc_str(buf, table_name, chosen_table->get_table_name())))
    {
      TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", chosen_table->get_table_name(), err);
    }
  }
  if (OB_SUCCESS == err)
  {
    TBSYS_LOG(DEBUG, "choosed_table=%.*s", table_name.length(), table_name.ptr());
  }
  return err;
}


int choose_value(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr,
                 const ObString& table_name, ObString& column_name, ObObj& value, int64_t seed)
{
  int err = OB_SUCCESS;
  const ObColumnSchemaV2* column_schema = NULL;
  if (NULL == (column_schema = schema_mgr.get_column_schema(table_name, column_name)))
  {
    err = OB_SCHEMA_ERROR;
  }
  else if (OB_SUCCESS != (err = rand_obj(buf, column_schema->get_type(), column_schema->get_size(), value, seed)))
  {
  }
  TBSYS_LOG(DEBUG, "choose_value(table=%*s, column=%*s)", table_name.length(), table_name.ptr(),
            column_name.length(), column_name.ptr());
  return err;
}

int add_cell_to_get_param(ObGetParam& get_param, ObString& table_name, ObRowkey& row_key, ObString& column_name)
{
  int err = OB_SUCCESS;
  ObCellInfo cell_info;
  cell_info.table_name_ = table_name;
  cell_info.row_key_ = row_key;
  cell_info.column_name_ = column_name;
  err = get_param.add_cell(cell_info);
  return err;
}

int add_cell_to_get_param(ObGetParam& get_param, const int64_t table_id, ObRowkey& row_key, const int64_t column_id)
{
  int err = OB_SUCCESS;
  ObCellInfo cell_info;
  cell_info.table_id_ = table_id;
  cell_info.row_key_ = row_key;
  cell_info.column_id_ = column_id;
  err = get_param.add_cell(cell_info);
  return err;
}

int build_rand_get_param(ObDataBuffer& buf, ObGetParam& get_param, int64_t seed, ObSchemaManagerV2& schema_mgr,
                       const char* _table_name)
{
  int err = OB_SUCCESS;
  ObString table_name;
  ObRowkey rowkey;
  ObString column_name;
  if (OB_SUCCESS != (err = choose_table(buf, schema_mgr, table_name, _table_name, seed)))
  {
    TBSYS_LOG(ERROR, "choose_table(table=%s)=>%d", _table_name, err);
  }
  else if (OB_SUCCESS != (err = choose_rowkey(buf, schema_mgr, table_name, rowkey, seed)))
  {
    TBSYS_LOG(ERROR, "choose_rowkey(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = choose_column(buf, schema_mgr, table_name, column_name, seed)))
  {
    TBSYS_LOG(ERROR, "choose_column(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = add_cell_to_get_param(get_param, table_name, rowkey, column_name)))
  {
    TBSYS_LOG(ERROR, "get_param.add_cell()=>%d", err);
  }
  return err;
}

int build_rand_mget_param(ObDataBuffer& buf, ObGetParam& get_param, int64_t start, int64_t end, ObSchemaManagerV2& schema_mgr,
                          const char* _table_name, int64_t start_version)
{
  int err = OB_SUCCESS;
  ObVersionRange version_range;
  if (0 >= start_version)
  {
    TBSYS_LOG(DEBUG, "no need to set start_version");
  }
  else if (OB_SUCCESS != (err = make_version_range(version_range, start_version)))
  {
    TBSYS_LOG(ERROR, "make_version_range(%ld)=>%d", start_version, err);
  }
  else
  {
    get_param.set_version_range(version_range);
  }
  for(int64_t i = start; OB_SUCCESS == err && i < end; i++)
  {
    err = build_rand_get_param(buf, get_param, i, schema_mgr, _table_name);
  }
  if (OB_SUCCESS == err)
  {
    get_param.set_is_read_consistency(false);
  }
  return err;
}

int build_rand_scan_param(ObDataBuffer& buf, ObScanParam& scan_param, int64_t start, int64_t end,
                          ObSchemaManagerV2& schema_mgr, const char* _table_name, int64_t start_version)
{
  int err = OB_SUCCESS;
  int64_t limit = 200;
  ObString table_name;
  ObString column_name;
  ObRowkey start_key;
  ObRowkey end_key;
  if (OB_SUCCESS != (err = choose_table(buf, schema_mgr, table_name, _table_name, start)))
  {
    TBSYS_LOG(ERROR, "choose_table(table=%s)=>%d", _table_name, err);
  }
  else if (OB_SUCCESS != (err = choose_rowkey(buf, schema_mgr, table_name, start_key, start)))
  {
    TBSYS_LOG(ERROR, "choose_rowkey(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = choose_rowkey(buf, schema_mgr, table_name, end_key, end)))
  {
    TBSYS_LOG(ERROR, "choose_rowkey(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = set_range2(scan_param, table_name, start_version, start_key, end_key)))
  {
    TBSYS_LOG(ERROR, "set_range(table_name=%.*s, start_version=%ld)=>%d",
              table_name.length(), table_name.ptr(), start_version, err);
  }
  else if (OB_SUCCESS != (err = scan_param.set_limit_info(0, limit)))
  {
    TBSYS_LOG(ERROR, "scan_param.set_limit_info(offset=%d, limit=%ld)=>%d", 0, limit, err);
  }
  else
  {
    scan_param.set_is_read_consistency(false);
  }
  return err;
}

int build_rand_mutator(ObDataBuffer& buf, ObMutator& mutator, int64_t seed, ObSchemaManagerV2& schema_mgr,
                       const char* _table_name)
{
  int err = OB_SUCCESS;
  ObString table_name;
  ObRowkey rowkey;
  int32_t column_num = 0;
  ObString column_name;
  ObObj cell_value;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  static bool is_delete = (0 == strcmp(_cfg("write_type", "mutator"), "delete"));
  static bool is_using_id = (0 == strcmp(_cfg("write_type", "mutator"), "msmutate"));
  if (OB_SUCCESS != (err = choose_table(buf, schema_mgr, table_name, _table_name, seed)))
  {
    TBSYS_LOG(ERROR, "choose_table(table=%s)=>%d", _table_name, err);
  }
  else if (OB_SUCCESS != (err = choose_rowkey(buf, schema_mgr, table_name, rowkey, seed)))
  {
    TBSYS_LOG(ERROR, "choose_rowkey(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(WARN, "sch_mgr.get_table_schema(table_id=%.*s)=>NULL", table_name.length(), table_name.ptr());
  }
  else if (NULL == (column_schema = schema_mgr.get_table_schema(table_schema->get_table_id(), column_num)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(ERROR, "sch_mgr.get_column_schema(table_id=%ld)", table_schema->get_table_id());
  }
  else if (is_delete)
  {
    if (OB_SUCCESS != (err = mutator.del_row(table_name, rowkey)))
    {
      TBSYS_LOG(ERROR, "mutator.del_row()=>%d", err);
    }
  }
  else
  {
    for(int32_t i = 0; OB_SUCCESS == err && i < column_num; i++)
    {
      #ifdef ROWKEY_IS_OBJ
      if (table_schema->get_rowkey_info().is_rowkey_column(column_schema[i].get_id()))
      {
      }
      #else
      if (false)
      {}
      #endif
      else if (OB_SUCCESS != (err = alloc_str(buf, column_name, column_schema[i].get_name())))
      {
        TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", column_schema[i].get_name(), err);
      }
      else if (OB_SUCCESS != (err = choose_value(buf, schema_mgr, table_name, column_name, cell_value, seed)))
      {
        TBSYS_LOG(ERROR, "choose_value(table=%*s, column=%*s)=>%d", table_name.length(), table_name.ptr(),
                  column_name.length(), column_name.ptr(), err);
      }
      else if (!is_using_id)
      {
        if (OB_SUCCESS != (err = mutator.update(table_name, rowkey, column_name, cell_value)))
        {
          TBSYS_LOG(ERROR, "mutator.update()=>%d", err);
        }
      }
      else
      {
        uint64_t table_id = table_schema->get_table_id();
        uint64_t column_id = column_schema[i].get_id();
        if (OB_SUCCESS != (err = mutator.update(table_id, rowkey, column_id, cell_value)))
        {
          TBSYS_LOG(ERROR, "mutator.update()=>%d", err);
        }
      }
    }
  }
  return err;
}

int build_rand_batch_mutator(ObDataBuffer& buf, ObMutator& mutator, int64_t start, int64_t end, ObSchemaManagerV2& schema_mgr,
                             const char* _table_name)
{
  int err = OB_SUCCESS;
  for(int64_t i = start; i < end; i++)
  {
    err = build_rand_mutator(buf, mutator, i, schema_mgr, _table_name);
  }
  return err;
}

int cell_info_resolve_table_name(ObSchemaManagerV2& sch_mgr, ObCellInfo& cell)
{
  int err = OB_SUCCESS;
  uint64_t table_id = cell.table_id_;
  const ObTableSchema* table_schema = NULL;
  const char* table_name = NULL;
  // `table_id == OB_INVALID_ID' is possible when cell.op_type == OB_USE_OB or cell.op_type == OB_USE_DB
  if (OB_INVALID_ID != table_id)
  {
    if (NULL == (table_schema = sch_mgr.get_table_schema(table_id)))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(WARN, "sch_mge.get_table_schema(table_id=%lu)=>NULL", table_id);
    }
    else if (NULL == (table_name = table_schema->get_table_name()))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(ERROR, "get_table_name(table_id=%lu) == NULL", table_id);
    }
    else
    {
      cell.table_name_.assign_ptr((char*)table_name, static_cast<int32_t>(strlen(table_name)));
      //cell.table_id_ = OB_INVALID_ID;
    }
  }
  return err;
}

int cell_info_resolve_column_name(ObSchemaManagerV2& sch_mgr, ObCellInfo& cell)
{
  int err = OB_SUCCESS;
  uint64_t table_id = cell.table_id_;
  uint64_t column_id = cell.column_id_;
  const ObColumnSchemaV2* column_schema = NULL;
  const char* column_name = NULL;
  // `table_id == OB_INVALID_ID' is possible when cell.op_type == OB_USE_OB or cell.op_type == OB_USE_DB
  // `column_id == OB_INVALID_ID' is possible when cell.op_type == OB_USE_OB or cell.op_type == OB_USE_DB
  //                                                        or cell.op_type == OB_DEL_ROW
  if (OB_INVALID_ID != table_id && OB_INVALID_ID != column_id)
  {
    if (NULL == (column_schema = sch_mgr.get_column_schema(table_id, column_id)))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(ERROR, "sch_mgr.get_column_schema(table_id=%lu, column_id=%lu) == NULL", table_id, column_id);
    }
    else if(NULL == (column_name = column_schema->get_name()))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(ERROR, "get_column_name(table_id=%lu, column_id=%lu) == NULL", table_id, column_id);
    }
    else
    {
      cell.column_name_.assign_ptr((char*)column_name, static_cast<int32_t>(strlen(column_name)));
      //cell.column_id_ = OB_INVALID_ID;
    }
  }
  return err;
}

static void dump_ob_mutator_cell(ObMutatorCellInfo& cell)
{
  uint64_t op = cell.op_type.get_ext();
  uint64_t table_id = cell.cell_info.table_id_;
  uint64_t column_id = cell.cell_info.column_id_;
  ObString table_name = cell.cell_info.table_name_;
  ObString column_name = cell.cell_info.column_name_;
  TBSYS_LOG(INFO, "cell{op=%lu, table=%lu[%*s], column=%lu[%*s]", op,
            table_id, table_name.length(), table_name.ptr(), column_id, column_name.length(), column_name.ptr());
}

int dump_ob_mutator(ObMutator& mut)
{
  int err = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "dump_ob_mutator");
  mut.reset_iter();
  while (OB_SUCCESS == err && OB_SUCCESS == (err = mut.next_cell()))
  {
    ObMutatorCellInfo* cell = NULL;
    if (OB_SUCCESS != (err = mut.get_cell(&cell)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else
    {
      dump_ob_mutator_cell(*cell);
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}

int ob_mutator_resolve_name(ObSchemaManagerV2& sch_mgr, ObMutator& mut)
{
  int err = OB_SUCCESS;
  while (OB_SUCCESS == err && OB_SUCCESS == (err = mut.next_cell()))
  {
    ObMutatorCellInfo* cell = NULL;
    if (OB_SUCCESS != (err = mut.get_cell(&cell)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else if (OB_SUCCESS != (err = cell_info_resolve_column_name(sch_mgr, cell->cell_info)))
    {
      TBSYS_LOG(ERROR, "resolve_column_name(table_id=%lu, column_id=%lu)=>%d",
                cell->cell_info.table_id_, cell->cell_info.column_id_, err);
    }
    else if (OB_SUCCESS != (err = cell_info_resolve_table_name(sch_mgr, cell->cell_info)))
    {
      TBSYS_LOG(ERROR, "resolve_table_name(table_id=%lu)=>%d", cell->cell_info.table_id_, err);
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}
int mutator_add_(ObMutator& dst, ObMutator& src)
{
  int err = OB_SUCCESS;
  src.reset_iter();
  while ((OB_SUCCESS == err) && (OB_SUCCESS == (err = src.next_cell())))
  {
    ObMutatorCellInfo* cell = NULL;
    if (OB_SUCCESS != (err = src.get_cell(&cell)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else if (OB_SUCCESS != (err = dst.add_cell(*cell)))
    {
      TBSYS_LOG(ERROR, "dst.add_cell()=>%d", err);
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}

int mutator_add(ObMutator& dst, ObMutator& src, int64_t size_limit)
{
  int err = OB_SUCCESS;
  if (dst.get_serialize_size() + src.get_serialize_size() > size_limit)
  {
    err = OB_SIZE_OVERFLOW;
    TBSYS_LOG(DEBUG, "mutator_add(): size overflow");
  }
  else if (OB_SUCCESS != (err = mutator_add_(dst, src)))
  {
    TBSYS_LOG(ERROR, "mutator_add()=>%d", err);
  }
  return err;
}

#if ROWKEY_IS_OBJ
int set_row_desc(const char* _table_name, ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr, ObRowDesc& row_desc, int64_t seed)
{
  int err = OB_SUCCESS;
  ObString table_name;
  uint64_t table_id = 0;
  uint64_t column_id = 0;
  int64_t max_column_num = 1;
  int64_t n_column = 0;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  const ObRowkeyInfo* rowkey_info = NULL;

  if (OB_SUCCESS != (err = choose_table(buf, schema_mgr, table_name, _table_name, seed)))
  {
    TBSYS_LOG(ERROR, "choose_table(table=%s)=>%d", _table_name, err);
  }
  else if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
  {
    err = OB_SCHEMA_ERROR;
  }
  else if (NULL == (column_schema = schema_mgr.get_table_schema(table_schema->get_table_id(), (int32_t&)n_column)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(ERROR, "schema_mgr.get_table_schema(table_id=%ld)=>%d", table_schema->get_table_id(), err);
  }
  else
  {
    table_id = table_schema->get_table_id();
    rowkey_info = &table_schema->get_rowkey_info();
  }
  for(int64_t i = 0; OB_SUCCESS == err && i < rowkey_info->get_size(); i++) {
    if (OB_SUCCESS != (err = rowkey_info->get_column_id(i, column_id)))
    {
      TBSYS_LOG(ERROR, "get_column_id(%ld)=>%d", i, err);
    }
    else if (OB_SUCCESS != (err = row_desc.add_column_desc(table_id, column_id)))
    {
      TBSYS_LOG(ERROR, "add_column_desc()=>%d", err);
    }
  }
  for(int64_t i = 0; OB_SUCCESS == err && i < min(max_column_num, n_column-rowkey_info->get_size()); i++) {
    int64_t idx = rand_choose_column(*rowkey_info, column_schema, n_column, seed + i, true);
    if (OB_SUCCESS != (err = row_desc.add_column_desc(table_id, column_schema[idx].get_id())))
    {
      TBSYS_LOG(ERROR, "add_column_desc()=>%d", err);
    }
  }
  return err;
}

int build_row_desc(ObRowDesc& row_desc, ObRowDescExt& row_desc_ext)
{
  int err = OB_SUCCESS;
  uint64_t table_id = 0;
  uint64_t column_id = 0;
  ObObj data_type;
  for(int64_t i = 0; OB_SUCCESS == err && i < row_desc_ext.get_column_num(); i++)
  {
    if (OB_SUCCESS != (err = row_desc_ext.get_by_idx(i, table_id, column_id, data_type)))
    {
      TBSYS_LOG(ERROR, "row_desc_ext.get_by_idx(i=%ld)=>%d", i, err);
    }
    else if (OB_SUCCESS != (err = row_desc.add_column_desc(table_id, column_id)))
    {
      TBSYS_LOG(ERROR, "row_desc.add_column_desc()=>%d", err);
    }
  }
  return err;
}

int set_row_desc(const char* _table_name, ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr, ObRowDescExt& row_desc, ObRowDesc& row_desc2, int64_t seed, int64_t max_column_num = 1)
{
  int err = OB_SUCCESS;
  ObString table_name;
  uint64_t table_id = 0;
  int64_t n_column = 0;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  const ObRowkeyInfo* rowkey_info = NULL;

  if (OB_SUCCESS != (err = choose_table(buf, schema_mgr, table_name, _table_name, seed)))
  {
    TBSYS_LOG(ERROR, "choose_table(table=%s)=>%d", _table_name, err);
  }
  else if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(ERROR, "get_table_schema()=>%d", err);
  }
  else if (NULL == (column_schema = schema_mgr.get_table_schema(table_schema->get_table_id(), (int32_t&)n_column)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(ERROR, "schema_mgr.get_table_schema(table_id=%ld)=>%d", table_schema->get_table_id(), err);
  }
  else
  {
    table_id = table_schema->get_table_id();
    rowkey_info = &table_schema->get_rowkey_info();
    row_desc2.set_rowkey_cell_count(rowkey_info->get_size());
  }
  for(int64_t i = 0; OB_SUCCESS == err && i < min(max_column_num + rowkey_info->get_size(), n_column); i++) {
    ObObj value;
    value.set_type(column_schema[i].get_type());
    if (OB_SUCCESS != (err = row_desc.add_column_desc(table_id, column_schema[i].get_id(), value)))
    {
      TBSYS_LOG(ERROR, "add_column_desc()=>%d", err);
    }
  }
  if (OB_SUCCESS != err)
  {}
  else if (OB_SUCCESS != (err = build_row_desc(row_desc2, row_desc)))
  {
    TBSYS_LOG(ERROR, "build_row_desc()=>%d", err);
  }
  return err;
}

// int set_row_desc(const char* _table_name, ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr, ObRowDescExt& row_desc, ObRowDesc& row_desc2, int64_t seed, int64_t max_column_num = 1)
// {
//   int err = OB_SUCCESS;
//   ObString table_name;
//   uint64_t table_id = 0;
//   uint64_t column_id = 0;
//   int64_t n_column = 0;
//   const ObTableSchema* table_schema = NULL;
//   const ObColumnSchemaV2* column_schema = NULL;
//   const ObRowkeyInfo* rowkey_info = NULL;

//   if (OB_SUCCESS != (err = choose_table(buf, schema_mgr, table_name, _table_name, seed)))
//   {
//     TBSYS_LOG(ERROR, "choose_table(table=%s)=>%d", _table_name, err);
//   }
//   else if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
//   {
//     err = OB_SCHEMA_ERROR;
//   }
//   else if (NULL == (column_schema = schema_mgr.get_table_schema(table_schema->get_table_id(), (int32_t&)n_column)))
//   {
//     err = OB_SCHEMA_ERROR;
//     TBSYS_LOG(ERROR, "schema_mgr.get_table_schema(table_id=%ld)=>%d", table_schema->get_table_id(), err);
//   }
//   else
//   {
//     table_id = table_schema->get_table_id();
//     rowkey_info = &table_schema->get_rowkey_info();
//     row_desc2.set_rowkey_cell_count(rowkey_info->get_size());
//   }
//   for(int64_t i = 0; OB_SUCCESS == err && i < rowkey_info->get_size(); i++) {
//     ObObj value;
//     const ObRowkeyColumn* rk_column = NULL;
//     if (OB_SUCCESS != (err = rowkey_info->get_column_id(i, column_id)))
//     {
//       TBSYS_LOG(ERROR, "get_column_id(%ld)=>%d", i, err);
//     }
//     else if (NULL == (rk_column = rowkey_info->get_column(i)))
//     {
//       err = OB_ERR_UNEXPECTED;
//       TBSYS_LOG(ERROR, "rowkey_info->get_column(%ld)=>NULL", i);
//     }
//     else
//     {
//       value.set_type(rk_column->type_);
//       if (OB_SUCCESS != (err = row_desc.add_column_desc(table_id, column_id, value)))
//       {
//         TBSYS_LOG(ERROR, "add_column_desc()=>%d", err);
//       }
//     }
//   }
//   for(int64_t i = 0; OB_SUCCESS == err && i < min(max_column_num, n_column-rowkey_info->get_size()); i++) {
//     int64_t idx = rand_choose_column(*rowkey_info, column_schema, n_column, seed + i, true);
//     ObObj value;
//     value.set_type(column_schema[idx].get_type());
//     if (OB_SUCCESS != (err = row_desc.add_column_desc(table_id, column_schema[idx].get_id(), value)))
//     {
//       TBSYS_LOG(ERROR, "add_column_desc()=>%d", err);
//     }
//   }
//   if (OB_SUCCESS != err)
//   {}
//   else if (OB_SUCCESS != (err = build_row_desc(row_desc2, row_desc)))
//   {
//     TBSYS_LOG(ERROR, "build_row_desc()=>%d", err);
//   }
//   return err;
// }

ObRow& build_row(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr, ObRowDesc& row_desc, ObRow& row, int64_t seed, bool has_data=true) {
  int err = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  ObObj value;
  const ObColumnSchemaV2* column_schema = NULL;
  row.set_row_desc(row_desc);
  for (int64_t j = 0; OB_SUCCESS == err && j < (has_data?row_desc.get_column_num():row_desc.get_rowkey_cell_count()); j++)
  {
    if (OB_SUCCESS != (err = row_desc.get_tid_cid(j, table_id, column_id)))
    {}
    else if (table_id == OB_INVALID_ID)
    {}
    else if (NULL == (column_schema = schema_mgr.get_column_schema(table_id, column_id)))
    {
      err = OB_SCHEMA_ERROR;
    }
    else if (OB_SUCCESS != (err = rand_obj(buf, column_schema->get_type(), value, seed)))
    {}
    else if (OB_SUCCESS != (err = row.set_cell(table_id, column_id, value)))
    {
      TBSYS_LOG(ERROR, "set_cell()=>%d", err);
    }
  }
  if (OB_SUCCESS != err || has_data)
  {}
  else
  {
    ObObj rne_obj;
    rne_obj.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    row.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, rne_obj);
  }
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(ERROR, "build_row()=>%d", err);
  }
  return row;
}

int test_values(ObValues& values)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = values.open()))
  {
    TBSYS_LOG(ERROR, "values.open()=>%d", err);
  }
  while(OB_SUCCESS == err)
  {
    const ObRow* row = NULL;
    if (OB_SUCCESS != (err = values.get_next_row(row)))
    {
      TBSYS_LOG(ERROR, "get_next_row()=>%d", err);
    }
  }
  return err;
}

int build_values(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr,
                 ObRowDesc& row_desc, ObValues& values, int64_t row_count, int64_t seed, bool has_value) {
  int err = OB_SUCCESS;
  values.set_row_desc(row_desc);
  for(int64_t i = 0; OB_SUCCESS == err && i < row_count; i++){
    ObRow row;
    if (OB_SUCCESS != (err = values.add_values(build_row(buf, schema_mgr, row_desc, row, seed + i, has_value))))
    {
      TBSYS_LOG(ERROR, "values.add_values()=>%d", err);
    }
  }
  return err;
}

ObCellInfo& set_read_cell_info(ObCellInfo& cell_info, uint64_t table_id, uint64_t column_id, ObRowkey& rowkey)
{
  cell_info.table_id_ = table_id;
  cell_info.column_id_ = column_id;
  cell_info.row_key_ = rowkey;
  return cell_info;
}

ObRowkey& set_rowkey(ObRowkey& rowkey, const ObObj* ptr, int64_t cnt)
{
  rowkey.assign((ObObj*)ptr, cnt);
  return rowkey;
}

int add_row(ObIAllocator* allocator, ObGetParam& get_param, const ObRow& row, int64_t rowkey_size)
{
  int err = OB_SUCCESS;
  uint64_t table_id = 0;
  uint64_t column_id = 0;
  ObRowkey rowkey;
  ObCellInfo cell_info;
  int64_t column_num = row.get_column_num();
  const ObObj* first_obj = NULL;
  const ObObj* cell = NULL;
  if (NULL == allocator)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = row.raw_get_cell(0L, first_obj, table_id, column_id)))
  {}
  else
  {
    ObRowkey tmp_rowkey;
    set_rowkey(tmp_rowkey, first_obj, rowkey_size);
    ob_write_rowkey(*allocator, tmp_rowkey, rowkey);
  }
  for(int64_t i = 0; OB_SUCCESS == err && i < column_num; i++){
    if (OB_SUCCESS != (err = row.raw_get_cell(i, cell, table_id, column_id)))
    {
      TBSYS_LOG(ERROR, "raw_get_cell(%ld)=>%d", i, err);
    }
    else if (OB_SUCCESS != (err = get_param.add_cell(set_read_cell_info(cell_info, table_id, column_id, rowkey))))
    {
      TBSYS_LOG(ERROR, "add_cell(table_id=%ld, column_id=%ld, rowkey_size=%ld)=>%d", table_id, column_id, rowkey_size, err);
    }
    char buffer[128];
    rowkey.to_string(buffer, sizeof(buffer));
  }
  return err;
}

int build_get_param(ObIAllocator* allocator, ObValues& values, ObGetParam& get_param, int64_t rowkey_size)
{
  int err = OB_SUCCESS;
  const ObRow* row = NULL;
  if (OB_SUCCESS != (err = values.open()))
  {
    TBSYS_LOG(ERROR, "values.open()=>%d", err);
  }
  while(OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = values.get_next_row(row)))
    {}
    else if (OB_SUCCESS != (err = add_row(allocator, get_param, *row, rowkey_size)))
    {
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  if (OB_SUCCESS == err)
  {
    ObVersionRange version_range;
    make_version_range(version_range, SSTableID::START_MAJOR_VERSION);
    get_param.set_version_range(version_range);
  }
  values.close();
  return err;
}

int obj2item(ExprItem& item, const ObObj& obj)
{
  int err = OB_SUCCESS;
  switch(obj.get_type())
  {
    case ObIntType:
      item.type_ = T_INT;
      err = obj.get_int(item.value_.int_);
      break;
    case ObVarcharType:
      item.type_ = T_STRING;
      err = obj.get_varchar(item.string_);
      break;
    default:
      err = OB_NOT_SUPPORTED;
      break;
  }
  return err;
}

int build_expr_values(ObIAllocator* allocator, ObValues& values, ObExprValues& expr_values)
{
  int err = OB_SUCCESS;
  const ObRow* row = NULL;
  UNUSED(allocator);
  if (OB_SUCCESS != (err = values.open()))
  {
    TBSYS_LOG(ERROR, "values.open()=>%d", err);
  }
  while(OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = values.get_next_row(row)))
    {}
    for(int64_t i = 0; OB_SUCCESS == err && i < row->get_column_num(); i++)
    {
      ObSqlExpression v;
      const ObObj* cell = NULL;
      ExprItem item;
      uint64_t table_id = 0;
      uint64_t column_id = 0;
      if (OB_SUCCESS != (err = row->raw_get_cell(i, cell, table_id, column_id)))
      {
        TBSYS_LOG(ERROR, "row.get_cell(i=%ld)=>%d", i, err);
      }
      else if (OB_INVALID_ID == table_id)
      {}
      else if (OB_SUCCESS != (err = obj2item(item, *cell)))
      {
        TBSYS_LOG(ERROR, "obj2item()=>%d", err);
      }
      else if (OB_SUCCESS != (err = v.add_expr_item(item)))
      {
        TBSYS_LOG(ERROR, "v.add_expr_item()=>%d", err);
      }
      else if (OB_SUCCESS != (err = v.add_expr_item_end()))
      {
        TBSYS_LOG(ERROR, "v.add_expr_item_end()=>%d", err);
      }
      else if (OB_SUCCESS != (err = expr_values.add_value(v)))
      {
        TBSYS_LOG(ERROR, "values.add_values()=>%d", err);
      }
      TBSYS_LOG(DEBUG, "add_cell(tid=%ld,cid=%ld)=>%d", table_id, column_id, err);
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  values.close();
  return err;
}

int build_rne_values(ObValues& target,  ObValues& values)
{
  int err = OB_SUCCESS;
  ObObj rne_obj;
  const ObRowDesc* row_desc = NULL;
  ObRowDesc new_row_desc;
  rne_obj.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  //rne_obj.set_ext(ObActionFlag::OP_NOP);
  if (OB_SUCCESS != (err = values.get_row_desc(row_desc)))
  {
    TBSYS_LOG(ERROR, "values.get_row_desc()=>%d", err);
  }
  else
  {
    new_row_desc = *row_desc;
    new_row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
    if (OB_SUCCESS != (err = target.set_row_desc(*row_desc)))
    {
      TBSYS_LOG(ERROR, "values.set_row_desc()=>%d", err);
    }
  }
  if (OB_SUCCESS != err)
  {}
  // else if (OB_SUCCESS != (err = target.get_row_desc(row_desc_with_rne)))
  // {
  //   TBSYS_LOG(ERROR, "target.get_row_desc()=>%d", err);
  // }
  else if (OB_SUCCESS != (err = values.open()))
  {
    TBSYS_LOG(ERROR, "values.open()=>%d", err);
  }
  while(OB_SUCCESS == err)
  {
    const ObRow* row = NULL;
    ObUpsRow new_row;
    if (OB_SUCCESS != (err = values.get_next_row(row)))
    {}
    else
    {
      TBSYS_LOG(INFO, "add_new_row()");
      new_row.assign(*row);
      new_row.set_is_delete_row(true);
      if (OB_SUCCESS != (err = target.add_values(new_row)))
      {
        TBSYS_LOG(ERROR, "add_values()=>%d", err);
      }
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  values.close();
  return err;
}

class SimpleAllocator: public SimpleBufAllocator
{
  public:
    const static int64_t buf_limit = 1<<21;
  public:
    SimpleAllocator() {
      void* cbuf = ob_tc_malloc(buf_limit, ObModIds::OB_RS_LOG_WORKER);
      buf_.set_data((char*)cbuf, buf_limit);
      SimpleBufAllocator::set_buf(&buf_);
    }
    ~SimpleAllocator(){ ob_tc_free(buf_.get_data()); }
  protected:
    ObDataBuffer buf_;
};

struct RowSpec
{
  RowSpec(): table_name_(NULL), seed_(0), count_(0), row_desc_(), row_desc_ext_(), values_(),
             tmp_table_id_(10489), tmp_column_id_(OB_MAX_TMP_COLUMN_ID) {}
  ~RowSpec() {}
  const char* table_name_;
  int64_t seed_;
  int64_t count_;
  ObRowDesc row_desc_;
  ObRowDesc row_desc_af_; // action_flag
  ObRowDescExt row_desc_ext_;
  ObValues values_;
  int64_t tmp_table_id_;
  int64_t tmp_column_id_;
};

class RowBuilder: public SimpleAllocator
{
  public:
    RowBuilder(ObSchemaManagerV2& schema_mgr): schema_mgr_(schema_mgr), spec_count_(0) {
    }
    ~RowBuilder(){}

    RowSpec* new_row_spec(const char* table_name, int64_t seed, int64_t row_count, int64_t max_column=128) {
      int err = OB_SUCCESS;
      int64_t idx = 0;
      RowSpec* spec = NULL;
      const char* real_table_name = NULL;
      ObString _table_name;
      if ((idx = __sync_fetch_and_add(&spec_count_, 1)) > (int64_t)array_len(spec_pool_))
      {
        __sync_fetch_and_add(&spec_count_, -1);
        err = OB_MEM_OVERFLOW;
      }
      else
      {
        spec = spec_pool_ + idx;
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = choose_table(buf_, schema_mgr_, _table_name, table_name, seed)))
      {
        TBSYS_LOG(ERROR, "choose_table(table=%s)=>%d", table_name, err);
      }
      else if (OB_SUCCESS != (err = obstring2cstr(buf_.get_data(), buf_.get_capacity(), buf_.get_position(), real_table_name, _table_name)))
      {
        TBSYS_LOG(ERROR, "obstring2cstr(%.*s)=>%d", _table_name.length(), _table_name.ptr(), err);
      }
      else if (OB_SUCCESS != (err = set_row_desc(real_table_name, buf_, schema_mgr_, spec->row_desc_ext_, spec->row_desc_, seed, max_column)))
      {
        TBSYS_LOG(ERROR, "set_row_desc()=>%d", err);
      }
      else
      {
        spec->row_desc_af_ = spec->row_desc_;
        spec->row_desc_af_.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
        spec->table_name_ = real_table_name;
        spec->seed_ = seed;
        spec->count_ = row_count;
      }
      return OB_SUCCESS == err? spec: NULL;
    }

    ObRow& build_row(RowSpec& spec, ObRow& row) {
      return ::build_row(buf_, schema_mgr_, spec.row_desc_, row, spec.seed_);
    }

    ObValues* build_values(RowSpec* spec, ObValues* values, bool has_data=true) {
      if (NULL != values && NULL != spec)
      {
        ::build_values(buf_, schema_mgr_, has_data? spec->row_desc_: spec->row_desc_af_, *values, spec->count_, spec->seed_, has_data);
      }
      return values;
    }
    ObExprValues* build_expr_values(RowSpec* spec, ObExprValues* expr_values, ObValues* values = NULL){
      if (NULL != expr_values && NULL != spec)
      {
        expr_values->set_row_desc(spec->row_desc_, spec->row_desc_ext_);
        ::build_expr_values(this, *(values?:&spec->values_), *expr_values);
      }
      return expr_values;
    }

    int build_get_param(RowSpec& spec, ObValues& values, ObGetParam& get_param){
      return ::build_get_param(this, values, get_param, get_rowkey_size(spec));
    }

    int64_t get_rowkey_size(RowSpec& spec) {
      int err = OB_SUCCESS;
      int64_t rk_size = 0;
      uint64_t table_id = 0;
      uint64_t column_id = 0;
      ObTableSchema* table_schema = NULL;
      if (OB_SUCCESS != (err = spec.row_desc_.get_tid_cid(0, table_id, column_id)))
      {
        TBSYS_LOG(ERROR, "get_tid_cid(0)=>%d", err);
      }
      else if (NULL == (table_schema = schema_mgr_.get_table_schema(table_id)))
      {
        TBSYS_LOG(ERROR, "get_table_schema(table_id=%ld)=>NULL", table_id);
      }
      else
      {
        rk_size = table_schema->get_rowkey_info().get_size();
      }
      return rk_size;
    }
  protected:
    ObSchemaManagerV2& schema_mgr_;
    RowSpec spec_pool_[2];
    int64_t spec_count_;
};

int get_first_int_col(ObRowDescExt& row_desc, int64_t start_idx, uint64_t& table_id, uint64_t& column_id)
{
  int err = OB_ENTRY_NOT_EXIST;
  ObObj data_type;
  int64_t column_num = row_desc.get_column_num();
  for(int64_t i = start_idx; i < column_num; i++)
  {
    if (OB_SUCCESS != (err = row_desc.get_by_idx(i, table_id, column_id, data_type)))
    {
      TBSYS_LOG(ERROR, "row_desc.get_by_idx(i=%ld)=>%d", i, err);
      break;
    }
    else if (data_type.get_type() == ObIntType)
    {
      break;
    }
  }
  return err;
}

int build_ref_expr(ObSqlExpression& expr, uint64_t table_id, uint64_t column_id)
{
  int err = OB_SUCCESS;
  ExprItem cref;
  cref.type_ = T_REF_COLUMN;
  cref.value_.cell_.tid = table_id;
  cref.value_.cell_.cid = column_id;

  if (OB_SUCCESS != (err = expr.add_expr_item(cref)))
  {
    TBSYS_LOG(ERROR, "v.add_expr_item()=>%d", err);
  }
  else if (OB_SUCCESS != (err = expr.add_expr_item_end()))
  {
    TBSYS_LOG(ERROR, "v.add_expr_item_end()=>%d", err);
  }
  return err;
}

int build_gt0_expr(ObSqlExpression& expr, uint64_t table_id, uint64_t column_id)
{
  int err = OB_SUCCESS;
  ExprItem gt0, cref, zero;
  gt0.type_ = T_OP_OR;
  gt0.value_.int_ = 2;
  cref.type_ = T_REF_COLUMN;
  cref.value_.cell_.tid = table_id;
  cref.value_.cell_.cid = column_id;

  zero.type_ = T_INT;
  zero.value_.int_ = 1;
  if (OB_SUCCESS != (err = expr.add_expr_item(cref)))
  {
    TBSYS_LOG(ERROR, "v.add_expr_item()=>%d", err);
  }
  else if (OB_SUCCESS != (err = expr.add_expr_item(zero)))
  {
    TBSYS_LOG(ERROR, "expr.add_expr_item(cref)=>%d", err);
  }
  else if (OB_SUCCESS != (err = expr.add_expr_item(gt0)))
  {
    TBSYS_LOG(ERROR, "expr.add_expr_item(cref)=>%d", err);
  }
  else if (OB_SUCCESS != (err = expr.add_expr_item_end()))
  {
    TBSYS_LOG(ERROR, "v.add_expr_item_end()=>%d", err);
  }
  return err;
}
/*
int build_add_expr(ObSqlExpression& expr, uint64_t table_id, uint64_t column_id)
{
  int err = OB_SUCCESS;
  ExprItem op, cref, zero;
  op.type_ = T_OP_ADD;
  op.value_.int_ = 2;
  cref.type_ = T_REF_COLUMN;
  cref.value_.cell_.tid = table_id;
  cref.value_.cell_.cid = column_id;

  zero.type_ = T_INT;
  zero.value_.int_ = 0;
  if (OB_SUCCESS != (err = expr.add_expr_item(cref)))
  {
    TBSYS_LOG(ERROR, "v.add_expr_item()=>%d", err);
  }
  else if (OB_SUCCESS != (err = expr.add_expr_item(zero)))
  {
    TBSYS_LOG(ERROR, "expr.add_expr_item(cref)=>%d", err);
  }
  else if (OB_SUCCESS != (err = expr.add_expr_item(op)))
  {
    TBSYS_LOG(ERROR, "expr.add_expr_item(cref)=>%d", err);
  }
  else if (OB_SUCCESS != (err = expr.add_expr_item_end()))
  {
    TBSYS_LOG(ERROR, "v.add_expr_item_end()=>%d", err);
  }
  return err;
}

ObPhyOperator* add_child(ObPhyOperator* parent, ObPhyOperator* child, ...)
{
  int err = OB_SUCCESS;
  va_list ap;
  va_start(ap, child);
  for(int32_t i = 0; NULL != parent && NULL != child; i++, child = va_arg(ap, ObPhyOperator*))
  {
    if (OB_SUCCESS != (err = parent->set_child(i, *child)))
    {
      TBSYS_LOG(ERROR, "set_child(%d)=>%d, parent=%s", i, err, to_cstring(*parent));
      parent = NULL;
    }
  }
  va_end(ap);
  return parent;
}
#define ADDC(parent, ...) add_child(parent, __VA_ARGS__, NULL)
#define NEW_OP(type) new_op<type>()
#define DEF_NEW_OP(type, value) type* value = NEW_OP(type)

struct SqlSessionCtx
{
  SqlSessionCtx() {
    session_info_.init(block_allocator_);
    result_set_.set_session(&session_info_);
  }
  ~SqlSessionCtx() {}
  DefaultBlockAllocator block_allocator_;
  ObSQLSessionInfo session_info_;
  ObResultSet result_set_; // The result set who owns this physical plan
};

struct SqlSessionCtxFactory
{
  typedef SqlSessionCtx InstanceType;
  int init_instance(InstanceType* inst){
    int err = OB_SUCCESS;
    UNUSED(inst);
    return err;
  }
};

class PhyPlanBuilder: public RowBuilder, public ObVersionProvider
{
  public:
    PhyPlanBuilder(ObSchemaManagerV2& schema_mgr):
      RowBuilder(schema_mgr), err_(OB_SUCCESS) {}
    ~PhyPlanBuilder(){}
  public:
    const ObVersion get_frozen_version() const {return 1;};

    template<typename op_type>
    op_type* alloc_op() {
      op_type* ret = (op_type*)alloc(sizeof(op_type));
      if (NULL == ret)
      {
        err_ = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "alloc(%ld) fail", sizeof(op_type));
      }
      else
      {
        new(ret)op_type();
        ret->set_phy_plan(&plan_);
        if (OB_SUCCESS != plan_.store_phy_operator(ret))
        {
          TBSYS_LOG(ERROR, "too many operator");
          ret = NULL;
        }
      }
      return ret;
    }

    template<typename op_type>
    op_type* new_op() {
      return alloc_op<op_type>();
    }

    ObPhysicalPlan* build_phy_plan(ObPhyOperator* op) {
      int err = OB_SUCCESS;
      static SqlSessionCtxFactory factory;
      static TSI<SqlSessionCtxFactory> tsi_sql_session(factory);
      SqlSessionCtx* session = tsi_sql_session.get();
      plan_.set_result_set(&session->result_set_);
      if (NULL == op)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "op = NULL");
      }
      else if (OB_SUCCESS != (err = plan_.add_phy_query(op, NULL, true)))
      {
        TBSYS_LOG(ERROR, "plan.add_phy_operator()=>%d", err);
      }
      return OB_SUCCESS == err? &plan_: NULL;
    }

    int build_insert_plan(ObPhysicalPlan*& plan, const char* table_name, int64_t seed, int64_t row_count) {
      int err = OB_SUCCESS;
      RowSpec* row_spec = NULL;
      if (NULL == (row_spec = new_row_spec(table_name, seed, row_count)))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "new_row_spec(%s,%ld)=>%d", table_name, seed, err);
      }
      else if (NULL == (plan = build_phy_plan(insert(row_spec))))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "build_phy_plan(op)=>%d", err);
      }
      return err;
    }

    int build_upcond_plan(ObPhysicalPlan*& plan, const char* table_name, const char* table_name2, int64_t seed, int64_t row_count) {
      int err = OB_SUCCESS;
      RowSpec* spec1 = NULL;
      RowSpec* spec2 = NULL;
      if (NULL == (spec1 = new_row_spec(table_name, seed, row_count)))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "new_row_spec(%s,%ld)=>%d", table_name, seed, err);
      }
      else if (NULL == (spec2 = new_row_spec(table_name2, seed, row_count)))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "new_row_spec(%s,%ld)=>%d", table_name, seed, err);
      }
      else if (NULL == (plan = build_phy_plan(compound_cond_insert(spec1, spec2))))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "build_phy_plan(op)=>%d", err);
      }
      return err;
    }

    ObFilter* build_gt0_filter(RowSpec* spec) {
      int err = OB_SUCCESS;
      DEF_NEW_OP(ObFilter, gt0_filter);
      uint64_t table_id = 0, column_id = 0;
      ObSqlExpression *gt0_expr = ObSqlExpression::alloc();
      if (OB_SUCCESS != err_)
      {
        err = err_;
      }
      else if (OB_SUCCESS != (err = get_first_int_col(spec->row_desc_ext_, get_rowkey_size(*spec), table_id, column_id)))
      {
        TBSYS_LOG(ERROR, "get_first_int_col()=>%d", err);
      }
      else if (OB_SUCCESS != (err = build_gt0_expr(*gt0_expr, table_id, column_id)))
      {
        TBSYS_LOG(ERROR, "build_gt0_expr()=>%d", err);
      }
      else if (OB_SUCCESS != (err = gt0_filter->add_filter(gt0_expr)))
      {
        TBSYS_LOG(ERROR, "gt0_filter.add_filetr()=>%d", err);
      }
      return OB_SUCCESS == err? gt0_filter:NULL;
    }

    ObWhenFilter* build_when_filter(RowSpec* spec) {
      int err = OB_SUCCESS;
      DEF_NEW_OP(ObWhenFilter, gt0_filter);
      ObSqlExpression gt0_expr;
      if (OB_SUCCESS != err_)
      {
        err = err_;
      }
      else if (OB_SUCCESS != (err = build_gt0_expr(gt0_expr, spec->tmp_table_id_, spec->tmp_column_id_)))
      {
        TBSYS_LOG(ERROR, "build_gt0_expr()=>%d", err);
      }
      else if (OB_SUCCESS != (err = gt0_filter->add_filter(gt0_expr)))
      {
        TBSYS_LOG(ERROR, "gt0_filter.add_filetr()=>%d", err);
      }
      return OB_SUCCESS == err? gt0_filter:NULL;
    }

    ObProject* build_project(RowSpec* spec) {
      int err = OB_SUCCESS;
      DEF_NEW_OP(ObProject, project);
      uint64_t table_id = 0, column_id = 0;
      ObSqlExpression add_expr;
      if (OB_SUCCESS != err_)
      {
        err = err_;
      }
      for(int64_t i = 0; OB_SUCCESS == err && i < get_rowkey_size(*spec); i++)
      {
        uint64_t rk_table_id = 0, rk_column_id = 0;
        ObSqlExpression rk_expr;
        if (OB_SUCCESS != (err = spec->row_desc_.get_tid_cid(i, rk_table_id, rk_column_id)))
        {}
        else if (OB_SUCCESS != (err = build_ref_expr(rk_expr, rk_table_id, rk_column_id)))
        {
          TBSYS_LOG(ERROR, "build_ref_expr()=>%d", err);
        }
        else
        {
          rk_expr.set_tid_cid(rk_table_id, rk_column_id);
          err = project->add_output_column(rk_expr);
        }
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = get_first_int_col(spec->row_desc_ext_, get_rowkey_size(*spec), table_id, column_id)))
      {
        TBSYS_LOG(ERROR, "get_first_int_col()=>%d", err);
      }
      else if (OB_SUCCESS != (err = build_add_expr(add_expr, table_id, column_id)))
      {
        TBSYS_LOG(ERROR, "build_gt0_expr()=>%d", err);
      }
      else
      {
        add_expr.set_tid_cid(table_id, column_id);
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = project->add_output_column(add_expr)))
      {
        TBSYS_LOG(ERROR, "project.add_filetr()=>%d", err);
      }
      return OB_SUCCESS == err? project:NULL;
    }

    ObPhyOperator* build_merger(RowSpec* spec, bool has_data=false) {
      int err = OB_SUCCESS;
      DEF_NEW_OP(ObValues, static_values);
      DEF_NEW_OP(ObMemSSTableScan, mem_sstable_scan);
      DEF_NEW_OP(ObIncScan, inc_scan);
      DEF_NEW_OP(ObMultipleGetMerge, merger);
      ObExprValues* get_param_values = NULL;
      if (OB_SUCCESS != err_)
      {
        err = err_;
      }
      else if (NULL == build_values(spec, &spec->values_))
      {
        err = OB_MEM_OVERFLOW;
      }
      else if (NULL == build_values(spec, static_values, has_data))
      {
        err = OB_MEM_OVERFLOW;
      }
      else if (NULL == (get_param_values = build_expr_values(spec, NEW_OP(ObExprValues))))
      {
        err = OB_MEM_OVERFLOW;
      }
      else
      {
        inc_scan->set_scan_type(ObIncScan::ST_MGET);
        inc_scan->set_values(get_param_values, false);
        inc_scan->set_write_lock_flag();
        mem_sstable_scan->set_tmp_table(static_values);
        merger->set_is_ups_row(false);
      }
      return OB_SUCCESS == err? ADDC(merger, mem_sstable_scan, inc_scan): NULL;
    }

    ObInsertDBSemFilter* build_insert_sem_filter(RowSpec* spec) {
      int err = OB_SUCCESS;
      DEF_NEW_OP(ObInsertDBSemFilter, insert_sem_filter);
      if (OB_SUCCESS != err_)
      {
        err = err_;
      }
      else if (NULL == build_values(spec, &spec->values_))
      {
        err = OB_MEM_OVERFLOW;
      }
      else if (NULL == build_expr_values(spec, &insert_sem_filter->get_insert_values()))
      {
        err = OB_MEM_OVERFLOW;
      }
      return OB_SUCCESS == err? insert_sem_filter: NULL;
    }

    ObRowCount* build_row_count(RowSpec* spec) {
      int err = OB_SUCCESS;
      DEF_NEW_OP(ObRowCount, row_count);
      if (OB_SUCCESS != err_)
      {
        err = err_;
      }
      else
      {
        row_count->set_tid_cid(spec->tmp_table_id_, spec->tmp_column_id_);
      }
      return OB_SUCCESS == err? row_count:NULL;
    }

    ObPhyOperator* insert_(RowSpec* spec) {
      return ADDC(build_insert_sem_filter(spec), ADDC(NEW_OP(ObEmptyRowFilter), build_merger(spec, false)));
    }

    ObPhyOperator* insert(RowSpec* spec) {
      return ADDC(NEW_OP(ObUpsModify), insert_(spec));
    }

    ObPhyOperator* cond_update_(RowSpec* spec) {
      return ADDC(build_project(spec), ADDC(build_gt0_filter(spec), build_merger(spec, true)));
    }

    ObPhyOperator* cond_update(RowSpec* spec) {
      return ADDC(NEW_OP(ObUpsModify), cond_update_(spec));
    }

    ObPhyOperator* compound_cond_insert(RowSpec* spec1, RowSpec* spec2) {
      // insert into spec1 when row_count(update spec2 where c>0)
      return ADDC(NEW_OP(ObUpsModify), ADDC(build_when_filter(spec2),
                                            insert_(spec1),
                                            ADDC(build_row_count(spec2), cond_update(spec2))));
    }
  private:
    int err_;
    ObPhysicalPlan plan_;
};
*/
#else
const int OB_ERR_PRIMARY_KEY_DUPLICATE = -5024;
#endif
