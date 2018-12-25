#include <stdint.h>
#include <ctype.h>
#include "common/page_arena.h"
#include "common/ob_common_param.h"
#include "common/ob_string_buf.h"
#include "common/ob_server.h"
#include "updateserver/ob_memtank.h"
#include "mock_client.h"
#include "utils.h"
#include "schema_compatible.h"
#include "cellinfo_builder.h"
#include "client_wrapper.h"
#include "row_checker.h"

using namespace oceanbase;
using namespace common;
using namespace updateserver;

uint64_t C_TIME_COLUMN_ID         = OB_INVALID_ID;
uint64_t M_TIME_COLUMN_ID         = OB_INVALID_ID;
uint64_t SEED_COLUMN_ID           = OB_INVALID_ID;
uint64_t ROWKEY_INFO_COLUMN_ID    = OB_INVALID_ID;
uint64_t CELL_NUM_COLUMN_ID       = OB_INVALID_ID;
uint64_t SUFFIX_LENGTH_COLUMN_ID  = OB_INVALID_ID;
uint64_t SUFFIX_NUM_COLUMN_ID     = OB_INVALID_ID;
uint64_t PREFIX_END_COLUMN_ID     = OB_INVALID_ID;

int64_t range_rand(int64_t start, int64_t end, int64_t rand)
{
  return (rand % (end - start + 1) + start);
}

void build_string(char *buffer, int64_t len, int64_t seed)
{
  if (NULL != buffer)
  {
    struct drand48_data rand_seed;
    srand48_r(seed, &rand_seed);
    for (int64_t i = 0; i < len;)
    {
      int64_t rand = 0;
      lrand48_r(&rand_seed, &rand);
      int8_t c = static_cast<int8_t>(range_rand(0, 255, rand));
      if (isalpha(c) || isalnum(c))
      {
        buffer[i++] = c;
      }
    }
  }
}

ObCellInfo *copy_cell(MemTank &mem_tank, const ObCellInfo *ci)
{
  ObCellInfo *ret = NULL;
  if (NULL != ci)
  {
    ret = (ObCellInfo*)mem_tank.node_alloc(sizeof(ObCellInfo));
    if (NULL != ret)
    {
      *ret = *ci;
      mem_tank.write_string(ci->table_name_, &(ret->table_name_));
      mem_tank.write_string(ci->row_key_, &(ret->row_key_));
      mem_tank.write_string(ci->column_name_, &(ret->column_name_));
      mem_tank.write_obj(ci->value_, &(ret->value_));
    }
  }
  return ret;
}

void trans_name2id(ObCellInfo &ci, const ObSchema &schema)
{
  ci.table_id_ = schema.get_table_id();
  if (NULL == ci.column_name_.ptr())
  {
    ci.column_id_ = OB_INVALID_ID;
  }
  else
  {
    ci.column_id_ = schema.find_column_info(ci.column_name_)->get_id();
  }
}

int fetch_schema(const char *schema_addr, const int64_t schema_port,
                oceanbase::common::ObSchemaManager &schema_mgr)
{
  int ret = OB_SUCCESS;
  ObServer dst_host;
  dst_host.set_ipv4_addr(schema_addr, static_cast<int32_t>(schema_port));
  MockClient client;
  client.init(dst_host);
  ret = client.fetch_schema(0, schema_mgr.get_v2(), TIMEOUT_US);
  client.destroy();
  return ret;
}

bool get_check_row(const ObSchema &schema,
                  const ObRowkey &row_key,
                  CellinfoBuilder &cb,
                  ClientWrapper &client,
                  const int64_t table_start_version,
                  const bool using_id)
{
  bool bret = false;

  ObGetParam get_param;
  const ObColumnSchema *iter = NULL;
  for (iter = schema.column_begin(); iter != schema.column_end(); iter++)
  {
    ObCellInfo ci;
    CharArena allocer;
    ci.row_key_ = row_key;
    if (using_id) 
    {
      ci.table_id_ = schema.get_table_id();
      ci.column_id_ = iter->get_id();
    }
    else
    {
      ci.table_name_.assign_ptr(const_cast<char*>(schema.get_table_name()), static_cast<int32_t>(strlen(schema.get_table_name())));
      ci.column_name_.assign_ptr(const_cast<char*>(iter->get_name()), static_cast<int32_t>(strlen(iter->get_name())));
    }
    get_param.add_cell(ci);
  }
  ObVersionRange version_range;
  version_range.start_version_ = table_start_version;
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  get_param.set_version_range(version_range);

  ObScanner scanner;
  int ret = client.get(get_param, scanner);
  if (OB_SUCCESS == ret)
  {
    if (scanner.get_cell_num() <= 0)
    {
      TBSYS_LOG(ERROR, "client.get(get_param.cell_size()=>%ld, scanner.cell_size()=>%ld)",
                get_param.get_cell_size(), scanner.get_cell_num());
      if (get_param.get_cell_size() > 0)
      {
        TBSYS_LOG(WARN, "%s", print_cellinfo(get_param[0]));
      }
      scanner.dump();
    }
    RowChecker rc;
    while (OB_SUCCESS == scanner.next_cell())
    {
      ObCellInfo *ci = NULL;
      if (OB_SUCCESS == scanner.get_cell(&ci))
      {
        if (!using_id)
        {
          trans_name2id(*ci, schema);
        }
        if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == ci->value_.get_ext())
        {
          bret = false;
          TBSYS_LOG(ERROR, "[%s] row not exist!", to_cstring(row_key));
          break;
        }
        rc.add_cell(ci);
      }
    }
    bret = rc.check_row(cb, schema);
  }
  else
  {
    TBSYS_LOG(WARN, "get ret=%d", ret);
  }
  return bret;
}

char* my_str_join(int n, char** parts)
{
  static char cmd[1<<12];
  cmd[0] = 0;
  for (int i = 0; i < n; i++)
  {
    strcat(cmd, parts[i]);
    strcat(cmd, " ");
  }
  return cmd;
}

