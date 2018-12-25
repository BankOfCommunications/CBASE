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

#include "tbsys.h"
#include "common/ob_malloc.h"
#include "updateserver/ob_ups_mutator.h"
#include "updateserver/ob_on_disk_log_locator.h"
#include "updateserver/ob_ups_table_mgr.h"
#include "common/ob_direct_log_reader.h"
#include "common/ob_repeated_log_reader.h"
#include "common/ob_log_writer.h"
#include "common/ob_schema.h"
//#include "common/ob_schema_service_impl.h"
#include <stdlib.h>
#include <set>
#include "file_utils.h"
#include "ob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

const char* _usages = "Usages:\n"
  "\t# You can set env var 'log_level' to 'DEBUG'/'WARN'...\n"
  "\t# Extra Env Vars: schema=path-to-schema-file, verbose=false...\n"
  "\t%1$s gen_nop log_file n_logs\n"
  "\t%1$s reset_id src_log_file dest_log_file start_id\n"
  "\t%1$s locate log_dir log_id\n"
  "\t%1$s dump log_file type=all # env: dump_header_only=false dump_all_data=true verbose=true\n"
  "\t%1$s find dir type=freeze\n"
  "\t%1$s cat log_file end_log_id # end_log_id = -n to drop last n log\n"
  "\t%1$s cat log_file 2>/dev/null |md5sum"
  "\t%1$s expand file size\n"
  "\t%1$s precreate log_dir du_percent\n"
  "\t%1$s merge src_log_file dest_log_file\n"
  "\t%1$s trans2to3 src_dir/log_file_id dest_dir/log_file_id\n"
  "\t%1$s a2i src_file dest_file\n"
  "\t%1$s dumpi file_of_binary_int\n"
  "\t%1$s read_file_test file_name offset\n"
  "\t%1$s sst2str sst_id\n"
  "\t%1$s select src_log_file dest_log_file id_seq_file\n";

#define getcfg(key) getenv(key)

int add_core_table_schema(ObSchemaManagerV2& schema_mgr)
{
  UNUSED(schema_mgr);
  return 0;
}

/*
int add_core_table_schema(ObSchemaManagerV2& schema_mgr)
{
  int err = OB_SUCCESS;
  ObSchemaServiceImpl schema_imp;
  TableSchema table1, table2, table3;
  ObArray<TableSchema> tables;
  
  if (OB_SUCCESS != (err = schema_imp.first_tablet_entry(table1)))
  {}
  else if (OB_SUCCESS != (err = schema_imp.all_all_column(table2)))
  {}
  else if (OB_SUCCESS != (err = schema_imp.all_join_info(table3)))
  {}
  else if (OB_SUCCESS != (err = tables.push_back(table1)))
  {}
  else if (OB_SUCCESS != (err = tables.push_back(table2)))
  {}
  else if (OB_SUCCESS != (err = tables.push_back(table3)))
  {}
  else if (OB_SUCCESS != (err = schema_mgr.add_new_table_schema(tables)))
  {}
  return err;
}
*/
struct Config
{
  Config(): schema_file_(NULL) {};
  ~Config() {};
  int init() {
    int err = OB_SUCCESS;
    tbsys::CConfig config_parser;
    if (false && OB_SUCCESS != (err = add_core_table_schema(schema_mgr_)))
    {
      TBSYS_LOG(ERROR, "add_core_table_schema()=>%d", err);
    }
    else if (NULL != (schema_file_ = getcfg("schema"))
        && !schema_mgr_.parse_from_file(schema_file_, config_parser))
    {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "parse_schema[%s] error!", schema_file_);
    }
    return err;
  }
  char* schema_file_;
  ObSchemaManagerV2 schema_mgr_;
};
Config g_cfg;

#if ROWKEY_IS_OBJ
int set_compatible_schema(ObUpsMutator& mutator)
{
  mutator.get_mutator().set_compatible_schema(g_cfg.schema_file_? &g_cfg.schema_mgr_: NULL);
  return 0;
}
#else
int set_compatible_schema(ObUpsMutator& mutator)
{
  UNUSED(mutator);
  return 0;
}
#endif

int dump_log_location(ObOnDiskLogLocator& log_locator, int64_t log_id)
{
  int err = OB_SUCCESS;
  ObLogLocation location;
  if (OB_SUCCESS != (err = log_locator.get_location(log_id, location)))
  {
    TBSYS_LOG(ERROR, "log_locator.get_location(%ld)=>%d", log_id, err);
  }
  else
  {
    printf("%ld -> [file_id=%ld, offset=%ld, log_id=%ld]\n", log_id, location.file_id_, location.offset_, location.log_id_);
  }
  return err;
}

int read_file_test(const char* path, int64_t offset)
{
  int err = OB_SUCCESS;
  ObString fname;
  ObFileReader file_reader;
  char buf[1<<21];
  int64_t len = sizeof(buf);
  bool dio = true;
  int64_t read_count = 0;
  if (NULL == path || 0 > offset || NULL == buf || 0 >= len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    fname.assign_ptr((char*)path, static_cast<int32_t>(strlen(path)));
  }

  if (OB_SUCCESS != err)
  {}
  else if (OB_SUCCESS != (err = file_reader.open(fname, dio)))
  {
    TBSYS_LOG(ERROR, "file_reader.open(%s)=>%d", path, err);
  }
  else if (OB_SUCCESS != (err = file_reader.pread(buf, len, offset, read_count)))
  {
    TBSYS_LOG(ERROR, "file_reader.pread(buf=%p[%ld], offset=%ld)=>%d", buf, len, offset, err);
  }

  if (file_reader.is_opened())
  {
    file_reader.close();
  }
  TBSYS_LOG(INFO, "read file '%s', offset=%ld, readcount=%ld", path, offset, read_count);
  return err;
}

int locate(const char* log_dir, int64_t log_id)
{
  int err = OB_SUCCESS;
  ObOnDiskLogLocator log_locator;
  ObLogLocation location;
  if (NULL == log_dir || 0 >= log_id)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "find_log(log_dir=%s, log_id=%ld): INVALID ARGUMENT.", log_dir, log_id);
  }
  else if (OB_SUCCESS != (err = log_locator.init(log_dir)))
  {
    TBSYS_LOG(ERROR, "log_locator.init(%s)=>%d", log_dir, err);
  }
  else if (OB_SUCCESS != (err = dump_log_location(log_locator, log_id)))
  {
    TBSYS_LOG(ERROR, "dump_log_location(%ld)=>%d", log_id, err);
  }
  return err;
}

static int file_expand_to(const int fd, const int64_t file_size)
{
  int err = OB_SUCCESS;
  struct stat st;
  static char buf[1<<20] __attribute__ ((aligned(DIO_ALIGN_SIZE)));
  int64_t count = 0;
  if (fd < 0 || file_size <= 0)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (0 != fstat(fd, &st))
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "fstat(%d):%s", fd, strerror(errno));
  }
  for(int64_t pos = st.st_size; OB_SUCCESS == err && pos < file_size; pos += count)
  {
    count = min(file_size - pos, sizeof(buf));
    if (unintr_pwrite(fd, buf, count, pos) != count)
    {
      err = OB_IO_ERROR;
      TBSYS_LOG(ERROR, "uniter_pwrite(pos=%ld, count=%ld)", pos, count);
    }
  }
  return err;
}

static int expand(const char* path, const int64_t size)
{
  int err = OB_SUCCESS;
  int fd = -1;
  if (NULL == path || size < 0)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if((fd = open(path, O_WRONLY | O_DIRECT | O_CREAT,
                     S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0)
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "open(%s):%s", path, strerror(errno));
  }
  else if (OB_SUCCESS != (err = file_expand_to(fd, size)))
  {
    TBSYS_LOG(ERROR, "file_expand_to(fd=%d, size=%ld)=>%d", fd, size, err);
  }
  if (fd >= 0)
  {
    close(fd);
  }
  return err;
}

int get_end_log_id(const char* log_data, int64_t data_len, int64_t& end_log_id, const bool skip_broken=true)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObLogEntry log_entry;
  const bool check_data_integrity = false;
  end_log_id = 0;
  if (NULL == log_data || data_len <= 0)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while(OB_SUCCESS == err && pos < data_len)
  {
    if (ObLogGenerator::is_eof(log_data + pos, data_len - pos))
    {
      err = OB_READ_NOTHING;
      TBSYS_LOG(WARN, "read eof(buf=%p, pos=%ld, len=%ld)", log_data, pos, data_len);
    }
    else if (OB_SUCCESS != (err = log_entry.deserialize(log_data, data_len, pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data, data_len, pos, err);
    }
    else if (pos + log_entry.get_log_data_len() > data_len)
    {
      err = OB_LAST_LOG_RUINNED;
      TBSYS_LOG(ERROR, "last log broken, last_log_id=%ld, offset=%ld", end_log_id, pos - log_entry.get_serialize_size());
    }
    else if (check_data_integrity && OB_SUCCESS != (err = log_entry.check_data_integrity(log_data + pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.check_data_integrity()=>%d", err);
    }
    else
    {
      pos += log_entry.get_log_data_len();
      end_log_id = (int64_t)log_entry.seq_;
    }
  }
  if (end_log_id > 0)
  {
    end_log_id++;
  }
  if (skip_broken && end_log_id > 0)
  {
    err = OB_SUCCESS;
  }
  return err;
}

int locate_in_buf(const char* log_data, int64_t data_len, int64_t end_log_id, int64_t& offset)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObLogEntry log_entry;
  const bool check_data_integrity = false;
  offset = 0;
  if (NULL == log_data || data_len <= 0)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while(OB_SUCCESS == err && pos < data_len)
  {
    int64_t cur_start = pos;
    if (OB_SUCCESS != (err = log_entry.deserialize(log_data, data_len, pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data, data_len, pos, err);
    }
    else if (pos + log_entry.get_log_data_len() > data_len)
    {
      err = OB_LAST_LOG_RUINNED;
      TBSYS_LOG(ERROR, "last log broken, log_id=%ld, offset=%ld", log_entry.seq_, cur_start);
    }
    else if (check_data_integrity && OB_SUCCESS != (err = log_entry.check_data_integrity(log_data + pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.check_data_integrity()=>%d", err);
    }
    else if ((int64_t)log_entry.seq_ == end_log_id)
    {
      offset = cur_start;
      break;
    }
    else if ((int64_t)log_entry.seq_ == end_log_id - 1)
    {
      offset = pos + log_entry.get_log_data_len();
      break;
    }
    else
    {
      pos += log_entry.get_log_data_len();
    }
  }
  return err;
}

static int cat(const char* log_file, const int64_t end_log_id)
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  int64_t len = 0;
  int64_t offset = 0;
  int64_t last_log_id = 0;
  fprintf(stderr, "cat(src=%s, end_log_id=%ld)\n", log_file, end_log_id);
  if (NULL == log_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(log_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(log_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", log_file, err);
  }
  else if (end_log_id <= 0 && OB_SUCCESS != (err = get_end_log_id(src_buf, len, last_log_id)))
  {
    TBSYS_LOG(ERROR, "get_last_end_log_id()=>%d", err);
  }
  else if (OB_SUCCESS != (err = locate_in_buf(src_buf, len, end_log_id > 0? end_log_id: last_log_id + end_log_id, offset)))
  {
    TBSYS_LOG(ERROR, "dump_log(buf=%p[%ld])=>%d", src_buf, len, err);
  }
  else if (offset != write(fileno(stdout), src_buf, offset))
  {
    TBSYS_LOG(ERROR, "write(stdout, count=%ld)=>%s", offset, strerror(errno));
  }
  else if (end_log_id <= 0)
  {
    fprintf(stderr, "dump: real_end_log_id=%ld\n", last_log_id + end_log_id);
  }
  return err;
}

static int is_clog_file(const struct dirent* a)
{
  return atoll(a->d_name) > 0;
}

int64_t calc_new_file_num_to_add(const char* dir, int64_t du_percent, int64_t file_size)
{
  int err = OB_SUCCESS;
  struct statfs fsst;
  int64_t num_file_to_add = 0;
  if (NULL == dir || du_percent < 0 || du_percent > 100)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if(0 != statfs(dir, &fsst))
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "statfs(dir=%s)=>%s", dir, strerror(errno));
  }
  else
  {
    num_file_to_add = (int64_t)fsst.f_bsize * ((int64_t)fsst.f_blocks * du_percent /100LL - (int64_t)(fsst.f_blocks - fsst.f_bavail))/file_size;
  }
  return num_file_to_add;
}

int precreate(const char* log_dir, int64_t du_percent, int64_t file_size)
{
  int err = OB_SUCCESS;
  int64_t max_id = 0;
  const int64_t align_size = ObLogGenerator::LOG_FILE_ALIGN_SIZE;
  static char log_buf[align_size] __attribute__ ((aligned(DIO_ALIGN_SIZE)));
  ObLogEntry entry;
  const int64_t header_size = entry.get_serialize_size();
  if (NULL == log_dir || du_percent < 0 || du_percent > 100 || file_size < 0)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    max_id = calc_new_file_num_to_add(log_dir, du_percent, file_size);
    printf("will create %ld new files.\n", max_id);
  }
  for(int64_t id = 1; OB_SUCCESS == err && id <= max_id; id++)
  {
    char fname[128];
    int64_t count = 0;
    int fd = -1;
    int64_t data_pos = entry.get_serialize_size();
    int64_t pos = 0;
    printf("create %ld, remain=%ld\n", id, max_id + 1 - id);
    if ((count = snprintf(fname, sizeof(fname), "%s/%ld", log_dir, id)) < 0
        || count >= (int64_t)sizeof(fname))
    {
      err = OB_ERR_UNEXPECTED;
    }
    else if((fd = open(fname, O_WRONLY | O_DIRECT | O_CREAT | O_EXCL,
                              S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0)
    {
      err = OB_IO_ERROR;
      TBSYS_LOG(ERROR, "open(%s):%s", fname, strerror(errno));
    }
    else if (OB_SUCCESS != (err = serialization::encode_i64(log_buf, sizeof(log_buf), data_pos, id + 1)))
    {
      TBSYS_LOG(ERROR, "encode_i64(id=%ld)=>%d", id + 1, err);
    }
    else
    {
      entry.set_log_seq(id);
      entry.set_log_command(OB_LOG_SWITCH_LOG);
    }
    if (OB_SUCCESS != err)
    {}
    else if (OB_SUCCESS != (err = entry.fill_header(log_buf + header_size, sizeof(log_buf) - header_size)))
    {
      TBSYS_LOG(ERROR, "entry.fill_header()=>%d", err);
    }
    else if (OB_SUCCESS != (err = entry.serialize(log_buf, sizeof(log_buf), pos)))
    {
      TBSYS_LOG(ERROR, "entry.serialize()=>%d", err);
    }
    else if (unintr_pwrite(fd, log_buf, sizeof(log_buf), 0) != sizeof(log_buf))
    {
      err = OB_IO_ERROR;
      TBSYS_LOG(ERROR, "unintr_pwrite(fd=%d,buf=%p[%ld])=>%s", fd, log_buf, sizeof(log_buf), strerror(errno));
    }
    else if (OB_SUCCESS != (err = file_expand_to(fd, file_size)))
    {
      TBSYS_LOG(ERROR, "file_expand_to(%ld)=>%d", file_size, err);
    }
    if (fd >= 0)
    {
      close(fd);
    }
  }
  return err;
}

static const char* LogCmdStr[1024];
int init_log_cmd_str()
{
  memset(LogCmdStr, 0x00, sizeof(LogCmdStr));
  LogCmdStr[OB_LOG_SWITCH_LOG]            = "SWITCH_LOG";
  LogCmdStr[OB_LOG_CHECKPOINT]            = "CHECKPOINT";
  LogCmdStr[OB_LOG_NOP]                   = "NOP";
  LogCmdStr[OB_LOG_UPS_MUTATOR]           = "UPS_MUTATOR";
  LogCmdStr[OB_UPS_SWITCH_SCHEMA]         = "UPS_SWITCH_SCHEMA";
  LogCmdStr[OB_RT_SCHEMA_SYNC]            = "OB_RT_SCHEMA_SYNC";
  LogCmdStr[OB_RT_CS_REGIST]              = "OB_RT_CS_REGIST";
  LogCmdStr[OB_RT_MS_REGIST]              = "OB_RT_MS_REGIST";
  LogCmdStr[OB_RT_SERVER_DOWN]            = "OB_RT_SERVER_DOWN";
  LogCmdStr[OB_RT_CS_LOAD_REPORT]         = "OB_RT_CS_LOAD_REPORT";
  LogCmdStr[OB_RT_CS_MIGRATE_DONE]        = "OB_RT_CS_MIGRATE_DONE";
  LogCmdStr[OB_RT_CS_START_SWITCH_ROOT_TABLE]
    = "OB_RT_CS_START_SWITCH_ROOT_TABLE";
  LogCmdStr[OB_RT_START_REPORT]           = "OB_RT_START_REPORT";
  LogCmdStr[OB_RT_REPORT_TABLETS]         = "OB_RT_REPORT_TABLETS";
  LogCmdStr[OB_RT_ADD_NEW_TABLET]         = "OB_RT_ADD_NEW_TABLET";
  LogCmdStr[OB_RT_CREATE_TABLE_DONE]      = "OB_RT_CREATE_TABLE_DONE";
  LogCmdStr[OB_RT_BEGIN_BALANCE]          = "OB_RT_BEGIN_BALANCE";
  LogCmdStr[OB_RT_BALANCE_DONE]           = "OB_RT_BALANCE_DONE";
  LogCmdStr[OB_RT_US_MEM_FRZEEING]        = "OB_RT_US_MEM_FRZEEING";
  LogCmdStr[OB_RT_US_MEM_FROZEN]          = "OB_RT_US_MEM_FROZEN";
  LogCmdStr[OB_RT_CS_START_MERGEING]      = "OB_RT_CS_START_MERGEING";
  LogCmdStr[OB_RT_CS_MERGE_OVER]          = "OB_RT_CS_MERGE_OVER";
  LogCmdStr[OB_RT_CS_UNLOAD_DONE]         = "OB_RT_CS_UNLOAD_DONE";
  LogCmdStr[OB_RT_US_UNLOAD_DONE]         = "OB_RT_US_UNLOAD_DONE";
  LogCmdStr[OB_RT_DROP_CURRENT_BUILD]     = "OB_RT_DROP_CURRENT_BUILD";
  LogCmdStr[OB_RT_DROP_LAST_CS_DURING_MERGE]
    = "OB_RT_DROP_LAST_CS_DURING_MERGE";
  LogCmdStr[OB_RT_SYNC_FROZEN_VERSION]    = "OB_RT_SYNC_FROZEN_VERSION";
  LogCmdStr[OB_RT_SET_UPS_LIST]           = "OB_RT_SET_UPS_LIST";
  LogCmdStr[OB_RT_SET_CLIENT_CONFIG]      = "OB_RT_SET_CLIENT_CONFIG";
  return 0;
}

const char* get_log_cmd_repr(const LogCommand cmd)
{
  const char* cmd_repr = NULL;
  if (cmd < 0 || cmd >= (int)ARRAYSIZEOF(LogCmdStr))
  {}
  else
  {
    cmd_repr = LogCmdStr[cmd];
  }
  if (NULL == cmd_repr)
  {
    cmd_repr = "unknown";
  }
  return cmd_repr;
}

static const char* ActionFlagStr[1024];
static int init_action_flag_str()
{
  memset(ActionFlagStr, 0x00, sizeof(ActionFlagStr));
  ActionFlagStr[ObActionFlag::OP_UPDATE]      = "UPDATE";
  ActionFlagStr[ObActionFlag::OP_INSERT]      = "INSERT";
  ActionFlagStr[ObActionFlag::OP_DEL_ROW]     = "DEL_ROW";
  ActionFlagStr[ObActionFlag::OP_DEL_TABLE]   = "DEL_TABLE";
  return 0;
}

const char* get_action_flag_str(int id)
{
  const char* repr = NULL;
  if (id < 0 || id >= (int)ARRAYSIZEOF(ActionFlagStr))
  {}
  else
  {
    repr = ActionFlagStr[id];
  }
  if (NULL == repr)
  {
    repr = "UNKNOWN";
  }
  return repr;
}

const char* format_time(int64_t time_us)
{
  static char time_str[1024];
  const char* format = "%Y-%m-%d %H:%M:%S";
  struct tm time_struct;
  int64_t time_s = time_us / 1000000;
  if(NULL != localtime_r(&time_s, &time_struct))
  {
    strftime(time_str, sizeof(time_str), format, &time_struct);
  }
  time_str[sizeof(time_str)-1] = 0;
  return time_str;
}

bool is_same_row(ObCellInfo& cell_1, ObCellInfo& cell_2)
{
  return cell_1.table_name_ == cell_2.table_name_ && cell_1.table_id_ == cell_2.table_id_ && cell_1.row_key_ == cell_2.row_key_;
}

int mutator_size(ObMutator& src, int64_t& n_cell, int64_t& n_row)
{
  int err = OB_SUCCESS;
  ObMutatorCellInfo* last_cell = NULL;
  n_cell = 0;
  n_row = 0;
  src.reset_iter();
  while ((OB_SUCCESS == err) && (OB_SUCCESS == (err = src.next_cell())))
  {
    ObMutatorCellInfo* cell = NULL;
    if (OB_SUCCESS != (err = src.get_cell(&cell)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else if (last_cell != NULL && is_same_row(last_cell->cell_info,cell->cell_info))
    {}
    else
    {
      n_row++;
    }
    if (OB_SUCCESS == err)
    {
      n_cell++;
      last_cell = cell;
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
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
      TBSYS_LOG(DEBUG, "sch_mge.get_table_schema(table_id=%lu)=>NULL", table_id);
    }
    else if (NULL == (table_name = table_schema->get_table_name()))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(DEBUG, "get_table_name(table_id=%lu) == NULL", table_id);
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
      TBSYS_LOG(DEBUG, "sch_mgr.get_column_schema(table_id=%lu, column_id=%lu) == NULL", table_id, column_id);
    }
    else if(NULL == (column_name = column_schema->get_name()))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(DEBUG, "get_column_name(table_id=%lu, column_id=%lu) == NULL", table_id, column_id);
    }
    else
    {
      cell.column_name_.assign_ptr((char*)column_name, static_cast<int32_t>(strlen(column_name)));
      //cell.column_id_ = OB_INVALID_ID;
    }
  }
  return err;
}

static int dump_ob_mutator_cell(ObMutatorCellInfo& cell,
                                const bool irc,
                                const bool irf,
                                const ObDmlType dml_type)
{
  int err = OB_SUCCESS;
  uint64_t op = cell.op_type.get_ext();
  uint64_t table_id = cell.cell_info.table_id_;
  uint64_t column_id = cell.cell_info.column_id_;
  ObString table_name = cell.cell_info.table_name_;
  ObString column_name = cell.cell_info.column_name_;
  char value_str[1<<20];
  int64_t pos = 0;
  if (OB_SUCCESS != (err = repr(value_str, sizeof(value_str), pos, cell.cell_info.value_)))
  {
    TBSYS_LOG(ERROR, "repr()=>%d", err);
  }
  else
  {
    printf("cell: op=%lu[%s], table=%lu[%*s], rowkey=%s, irc=%s, irf=%s, dml_type=%s, column=%lu[%*s], value=%s\n",
           op, get_action_flag_str(static_cast<int>(op)),
           table_id, table_name.length(), table_name.ptr(), to_cstring(cell.cell_info.row_key_),
           STR_BOOL(irc), STR_BOOL(irf), str_dml_type(dml_type),
           column_id, column_name.length(), column_name.ptr(), value_str);
  }
  return err;
}

int dump_ob_mutator(ObMutator& mut)
{
  int err = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "dump_ob_mutator");
  mut.reset_iter();
  while (OB_SUCCESS == err && OB_SUCCESS == (err = mut.next_cell()))
  {
    ObMutatorCellInfo* cell = NULL;
    bool irc = false;
    bool irf = false;
    ObDmlType dml_type = OB_DML_UNKNOW;
    if (OB_SUCCESS != (err = mut.get_cell(&cell, &irc, &irf, &dml_type)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else
    {
      dump_ob_mutator_cell(*cell, irc, irf, dml_type);
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
  mut.reset_iter();
  while (OB_SUCCESS == err && OB_SUCCESS == (err = mut.next_cell()))
  {
    ObMutatorCellInfo* cell = NULL;
    if (OB_SUCCESS != (err = mut.get_cell(&cell)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else if (OB_SUCCESS != (err = cell_info_resolve_column_name(sch_mgr, cell->cell_info))
             && OB_SCHEMA_ERROR != err)
    {
      TBSYS_LOG(ERROR, "resolve_column_name(table_id=%lu, column_id=%lu)=>%d",
                cell->cell_info.table_id_, cell->cell_info.column_id_, err);
    }
    else if (OB_SCHEMA_ERROR == err)
    {}
    else if (OB_SUCCESS != (err = cell_info_resolve_table_name(sch_mgr, cell->cell_info))
             && OB_SCHEMA_ERROR != err)
    {
      TBSYS_LOG(ERROR, "resolve_table_name(table_id=%lu)=>%d", cell->cell_info.table_id_, err);
    }
    else if (OB_SCHEMA_ERROR == err)
    {}
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}

const char* get_ups_mutator_type(ObUpsMutator& mutator)
{
  if (mutator.is_normal_mutator())
    return "NORMAL";
  if (mutator.is_freeze_memtable())
    return "FREEZE";
  if (mutator.is_drop_memtable())
    return "DROP";
  if (mutator.is_first_start())
    return "START";
  if (mutator.is_check_cur_version())
    return "CHECK_VERSION";
  return "UNKNOWN";
}

int dump_freeze_log(ObUpsMutator &ups_mutator)
{
  int err = OB_SUCCESS;
  ObMutatorCellInfo *mutator_ci = NULL;
  ObCellInfo *ci = NULL;
  ObString str_freeze_param;
  ObUpsTableMgr::FreezeParamHeader *header = NULL;

  ups_mutator.get_mutator().reset_iter();
  if (OB_SUCCESS != (err = ups_mutator.get_mutator().next_cell()))
  {
    TBSYS_LOG(ERROR, "next cell for freeze ups_mutator fail ret=%d", err);
  }
  else if (OB_SUCCESS != (err = ups_mutator.get_mutator().get_cell(&mutator_ci)))
  {
    TBSYS_LOG(ERROR, "get mutator cell fail, err=%d", err);
  }
  else if (NULL == mutator_ci)
  {
    TBSYS_LOG(WARN, "get NULL mutator cell");
    err = OB_ERR_UNEXPECTED;
  }
  else
  {
    ci = &(mutator_ci->cell_info);
    err = ci->value_.get_varchar(str_freeze_param);
    header = (ObUpsTableMgr::FreezeParamHeader*)str_freeze_param.ptr();
  }

  if (OB_SUCCESS != err || NULL == header || (int64_t)sizeof(ObUpsTableMgr::FreezeParamHeader) >= str_freeze_param.length())
  {
    TBSYS_LOG(ERROR, "get freeze_param from freeze ups_mutator fail ret=%d header=%p length=%d",
              err, header, str_freeze_param.length());
    err = (OB_SUCCESS == err) ? OB_ERR_UNEXPECTED : err;
  }
  else
  {
    switch (header->version)
    {
      case 1:
      {
        ObUpsTableMgr::FreezeParamV1 *freeze_param_v1 = (ObUpsTableMgr::FreezeParamV1*)(header->buf);
        fprintf(stdout, "freeze_param_v1 active_version=%ld new_log_file_id=%ld op_flag=%lx\n",
                  freeze_param_v1->active_version, freeze_param_v1->new_log_file_id, freeze_param_v1->op_flag);
        break;
      }
      case 2:
      {
        ObUpsTableMgr::FreezeParamV2 *param = (ObUpsTableMgr::FreezeParamV2*)(header->buf);
        fprintf(stdout, "param frozen=%s active=%s new_log_file_id=%ld op_flag=%lx\n",
                SSTableID::log_str(SSTableID::trans_format_v1(param->frozen_version)), SSTableID::log_str(SSTableID::trans_format_v1(param->active_version)),
                param->new_log_file_id, param->op_flag);
        break;
      }
      case 3:
      {
        ObUpsTableMgr::FreezeParamV3 *param = (ObUpsTableMgr::FreezeParamV3*)(header->buf);
        fprintf(stdout, "param frozen=%s active=%s new_log_file_id=%ld op_flag=%lx timestamp=%s[%lu]\n",
                SSTableID::log_str(SSTableID::trans_format_v1(param->frozen_version)), SSTableID::log_str(SSTableID::trans_format_v1(param->active_version)),
                param->new_log_file_id, param->op_flag,
                format_time(param->time_stamp), param->time_stamp);
        break;
      }
      case 4:
      {
        int tmp_ret = OB_SUCCESS;
        ObUpsTableMgr::FreezeParamV4 *param = (ObUpsTableMgr::FreezeParamV4*)(header->buf);
        CommonSchemaManagerWrapper schema_manager;
        char *data = str_freeze_param.ptr();
        int64_t length = str_freeze_param.length();
        int64_t pos = sizeof(ObUpsTableMgr::FreezeParamHeader) + sizeof(ObUpsTableMgr::FreezeParamV4);
        fprintf(stdout, "param frozen=%s active=%s new_log_file_id=%ld op_flag=%lx timestamp=%s[%lu]\n",
                SSTableID::log_str(param->frozen_version), SSTableID::log_str(param->active_version),
                param->new_log_file_id, param->op_flag,
                format_time(param->time_stamp), param->time_stamp);
        if (OB_SUCCESS != (tmp_ret = schema_manager.deserialize(data, length, pos)))
        {
          TBSYS_LOG(ERROR, "schema_manager.deserialize()=>%d", tmp_ret);
        }
        else
        {
          schema_manager.get_impl()->print(stdout);
        }
        break;
      }
      default:
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "freeze_param version error %d", header->version);
        break;
    }
  }
  ups_mutator.get_mutator().reset_iter();
  return err;
}

int dump_mutator_header(uint64_t seq, ObUpsMutator& mutator, int64_t& n_cell, int64_t& n_row)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = mutator_size(mutator.get_mutator(), n_cell, n_row)))
  {
    TBSYS_LOG(ERROR, "mutator_size()=>%d", err);
  }
  else
  {
    printf("#LogHeader# %lu Mutator %s, size %ld:%ld, MutationTime[%lu]: %s, CheckSum %lu:%lu\n",
           seq,
           get_ups_mutator_type(mutator),
           n_cell, n_row,
           mutator.get_mutate_timestamp(),
           format_time(mutator.get_mutate_timestamp()),
           mutator.get_memtable_checksum_before_mutate(),
           mutator.get_memtable_checksum_after_mutate());
    if (mutator.is_check_cur_version())
    {
      printf("cur_version=%lu:%lu load_bypass_checksum=%lu\n", mutator.get_cur_major_version(), mutator.get_cur_minor_version(), mutator.get_last_bypass_checksum());
    }
    if (mutator.is_freeze_memtable())
    {
      if (OB_SUCCESS != (err = dump_freeze_log(mutator)))
      {
        printf("dump_freeze_log() fail, err=%d\n", err);
      }
    }
  }
  return err;
}

int dump_mutator(uint64_t seq, ObUpsMutator& mutator, int64_t& n_cell, int64_t& n_row, bool verbose=true)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = dump_mutator_header(seq, mutator, n_cell, n_row)))
  {}
  else if (!verbose)
  {}
  else
  {
    if (g_cfg.schema_file_ && OB_SUCCESS != (err = ob_mutator_resolve_name(g_cfg.schema_mgr_, mutator.get_mutator()))
        && OB_SCHEMA_ERROR != err)
    {}
    if (OB_SUCCESS != (err = dump_ob_mutator(mutator.get_mutator())))
    {
    }
  }
  return err;
}

inline int64_t memNchr(const char* s, int64_t s_n, const char* c, int64_t c_n)
{
  if (NULL == s || NULL == c || s_n < 0 || c_n < 0)
  {
    TBSYS_LOG(ERROR, "memNchr() INVALID_ARGUMENT");
  }
  else
  {
    for(int64_t i = 0; i + c_n <= s_n; i++)
    {
      if (0 == memcmp(s + i, c, (int)c_n))
      {
        return i;
      }
    }
  }
  return -1;
}

int64_t next_header(const char* buf, int64_t len, int64_t pos)
{
  int64_t idx = -1;
  char magic_buf[2];
  int64_t magic_pos = 0;
  if (OB_SUCCESS != serialization::encode_i16(magic_buf, sizeof(magic_buf), magic_pos, ObLogEntry::MAGIC_NUMER))
  {
    TBSYS_LOG(ERROR, "encode_i16() FAIL");
  }
  else
  {
    idx = memNchr(buf + pos, len - pos, magic_buf, magic_pos);
  }
  return idx >= 0? pos + idx: len;
}

int dump_first_log_if_freeze(const char* log_file, const char* buf, const int64_t len)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObLogEntry entry;
  ObUpsMutator mutator;
  if (NULL == buf || 0 > len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (ObLogGenerator::is_eof(buf + pos, len - pos))
  {
    err = OB_READ_NOTHING;
    TBSYS_LOG(WARN, "read eof(buf=%p, pos=%ld, len=%ld)", buf, pos, len);
  }
  else if (OB_SUCCESS != (err = entry.deserialize(buf, len, pos)))
  {
    TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
  }
  else if (OB_SUCCESS != (err = entry.check_header_integrity(false)))
  {
    TBSYS_LOG(ERROR, "check_header_integrity()=>%d", err);
  }
  else if (pos + entry.get_log_data_len() > len || OB_SUCCESS != (err = entry.check_data_integrity(buf + pos, false)))
  {
    TBSYS_LOG(ERROR, "check_data_integrity()=>%d", err);
  }
  else if (OB_LOG_UPS_MUTATOR != entry.cmd_)
  {
    err = OB_READ_NOTHING;
  }
  else if (OB_SUCCESS != (err = mutator.deserialize(buf, pos + entry.get_log_data_len(), pos)))
  {
    TBSYS_LOG(ERROR, "deserialize()=>%d", err);
  }
  else if (!mutator.is_freeze_memtable())
  {
    err = OB_READ_NOTHING;
  }
  else
  {
    printf("find freeze: %s\n", log_file);
    if (OB_SUCCESS != (err = dump_freeze_log(mutator)))
    {
      TBSYS_LOG(ERROR, "dump_freeze_log()=>%d", err);
    }
  }
  return err;
}

int dump_tablets_list(const char* buf, int64_t len)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObServer server;
  ObTabletReportInfoList tablets;
  int64_t timestamp = 0;

  if (OB_SUCCESS != (err = serialization::decode_vi64(buf, len, pos, &timestamp)))
  {
    TBSYS_LOG(ERROR, "decode_vi64()=>%d", err);
  }
  else if (OB_SUCCESS != (err = server.deserialize(buf, len, pos)))
  {
    TBSYS_LOG(ERROR, "server.deserialize()=>%d", err);
  }
  else if (OB_SUCCESS != (err = tablets.deserialize(buf, len, pos)))
  {
    TBSYS_LOG(ERROR, "tablets.deserialize()=>%d", err);
  }
  printf("report_tablets: ts=%ld, server=%s, tablet_cnt=%ld\n", timestamp, to_cstring(server), tablets.get_tablet_size());
  return err;
}

int dump_log(const char* log_file, const char* buf, const int64_t len, const char* type)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObLogEntry entry;
  ObUpsMutator mutator;
  ObSchemaManagerV2 schema_mgr;
  bool dump_header_only = is_env_set("dump_header_only", "false");
  bool dump_all_data = is_env_set("dump_all_data", "true");
  bool verbose = is_env_set("verbose", "true");
  bool read_eof = false;
  int64_t n_header_broken = 0;
  int64_t n_body_broken = 0;
  int64_t n_deserialize_broken = 0;
  int64_t n_cell = 0;
  int64_t n_row = 0;
  int64_t total_cell = 0;
  int64_t total_row = 0;
  int64_t n_mutator = 0;

  if (strcmp(type, "freeze") == 0)
  {
    if (OB_SUCCESS != (err = dump_first_log_if_freeze(log_file, buf, len))
      && OB_READ_NOTHING != err)
    {
      TBSYS_LOG(ERROR, "dump_first_log_if_freeze()=>%d", err);
    }
    else if (OB_READ_NOTHING == err)
    {
      err = OB_SUCCESS;
    }
    return err;
  }
  set_compatible_schema(mutator);
  if (NULL == buf || 0 > len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while((dump_all_data || OB_SUCCESS == err) && pos < len)
  {
    if (ObLogGenerator::is_eof(buf + pos, len - pos))
    {
      TBSYS_LOG(INFO, "read eof(buf=%p, pos=%ld, len=%ld)", buf, pos, len);
      read_eof = true;
      break;
    }
    else if (OB_SUCCESS != (err = entry.deserialize(buf, len, pos)))
    {
      pos ++;
      TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
    }
    else if (OB_SUCCESS != (err = entry.check_header_integrity(false)))
    {
      TBSYS_LOG(ERROR, "check_header_integrity()=>%d", err);
    }
    if (OB_SUCCESS != err)
    {
      pos = next_header(buf, len, pos);
      n_header_broken++;
      fprintf(stdout, "0|%ld\t|%s\n", pos, "HEADER_BROKEN");
    }
    else
    {
      int64_t tmp_pos = 0;
      fprintf(stdout, "%lu|%ld\t|%ld\t%s[%d]\n", entry.seq_, pos + entry.get_log_data_len(), (int64_t)entry.get_log_data_len(), get_log_cmd_repr((LogCommand)entry.cmd_), entry.cmd_);
      if (pos + entry.get_log_data_len() > len || OB_SUCCESS != (err = entry.check_data_integrity(buf + pos, false)))
      {
        fprintf(stdout, "BODY_BROKEN ");
        n_body_broken++;
      }
      else if (dump_header_only)
      {}
      else if (OB_UPS_SWITCH_SCHEMA == entry.cmd_)
      {
        if (OB_SUCCESS != (err = schema_mgr.deserialize(buf + pos, entry.get_log_data_len(), tmp_pos)))
        {
          TBSYS_LOG(ERROR, "schema_mgr.deserialize()=>%d", err);
        }
        else
        {
          schema_mgr.print(stdout);
        }
      }
      else if (OB_LOG_UPS_MUTATOR == entry.cmd_)
      {
        if (OB_SUCCESS != (err = mutator.deserialize(buf + pos, entry.get_log_data_len(), tmp_pos)))
        {
          err = OB_SUCCESS;
          fprintf(stdout, "Corrupt.\n");
          //TBSYS_LOG(ERROR, "mutator.deserialize(seq=%ld)=>%d", (int64_t)entry.seq_, err);
        }
        else if (OB_SUCCESS != (err = dump_mutator(entry.seq_, mutator, n_cell, n_row, verbose)))
        {
          TBSYS_LOG(ERROR, "dump_mutator()=>%d", err);
        }
      }
      else if (OB_RT_REPORT_TABLETS == entry.cmd_)
      {
        if (OB_SUCCESS != (err = dump_tablets_list(buf + pos, entry.get_log_data_len())))
        {
          TBSYS_LOG(ERROR, "dump_tablets_list()=>%d", err);
        }
      }
      else if (OB_LOG_NOP == entry.cmd_)
      {
        DebugLog debug_log;
        if (OB_SUCCESS != debug_log.deserialize(buf + pos, entry.get_log_data_len(), tmp_pos))
        {
          TBSYS_LOG(DEBUG, "deserialize nop log fail: log_id=%ld, pos=%ld", entry.seq_, pos);
          fprintf(stdout, "#LogHeader# %lu NOP %d\n", entry.seq_, entry.get_log_data_len());
        }
        else
        {
          fprintf(stdout, "#LogHeader# %lu NOP %d %s\n", entry.seq_, entry.get_log_data_len(), to_cstring(debug_log));
        }
      }
      else if (OB_LOG_SWITCH_LOG == entry.cmd_)
      {
        int64_t file_id = 0;
        if (OB_SUCCESS != (err = serialization::decode_i64(buf + pos, entry.get_log_data_len(), tmp_pos, (int64_t*)&file_id)))
        {
          TBSYS_LOG(ERROR, "decode_i64 log_id error, err=%d", err);
        }
        else
        {
          fprintf(stdout, "switch_log: %ld\n", file_id);
        }
      }
      if (OB_SUCCESS != err)
      {
        n_deserialize_broken++;
        fprintf(stdout, "DESERIALIZE_BROKEN\n");
      }
      else
      {
        total_cell += n_cell;
        total_row += n_row;
        n_mutator++;
      }
      pos += entry.get_log_data_len();
    }
  }
  if (OB_SUCCESS == err && !read_eof && pos != len)
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "pos[%ld] != len[%ld]", pos, len);
  }
  printf("broken[header:body:deserialize]=[%ld:%ld:%ld], cell=%ld/%ld=%.2f, row=%ld/%ld=%.2f\n",
         n_header_broken, n_body_broken, n_deserialize_broken,
         total_cell, n_mutator, 1.0 * static_cast<double>(total_cell)/static_cast<double>(n_mutator),
         total_row, n_mutator, 1.0 * static_cast<double>(total_row)/static_cast<double>(n_mutator));
  return err;
}

int dump(const char* log_file, const char* type="all")
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  int64_t len = 0;
  TBSYS_LOG(DEBUG, "dump(src=%s)", log_file);
  if (NULL == log_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(log_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(log_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", log_file, err);
  }
  else if (OB_SUCCESS != (err = dump_log(log_file, src_buf, len, type)))
  {
    TBSYS_LOG(ERROR, "dump_log(buf=%p[%ld])=>%d", src_buf, len, err);
  }
  return err;
}


int find(const char* dir, const char* type="freeze")
{
  int err = OB_SUCCESS;
  char buf[256];
  struct dirent **list = NULL;
  int n = 0;
  int64_t len = 0;
  if (NULL == dir)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if ((n = scandir(dir, &list, is_clog_file, versionsort)) < 0)
  {
    TBSYS_LOG(ERROR, "scandir(%s)=>%s", dir, strerror(errno));
  }
  else if (0 == n)
  {
    TBSYS_LOG(WARN, "%s is empty, can not select a file", dir);
  }
  for(int64_t i = 0; OB_SUCCESS == err && i < n; i++)
  {
    if ((len = snprintf(buf, sizeof(buf), "%s/%s", dir, list[i]->d_name)) <= 0
        || len >= (int64_t)sizeof(buf))
    {
      TBSYS_LOG(ERROR, "generate file_name error, buf=%p[%ld], dir=%s, fname=%s",
                buf, sizeof(buf), dir, list[i]->d_name);
    }
    else if (OB_SUCCESS != (err = dump(buf, type))
             && OB_READ_NOTHING != err)
    {
      TBSYS_LOG(ERROR, "dump_freeze_log_in_file(%s)=>%d", buf, err);
    }
    else if (OB_READ_NOTHING == err)
    {
      err = OB_SUCCESS;
    }
  }
  if (NULL == list && n > 0)
  {
    while(n--)
    {
      free(list[n]);
    }
    free(list);
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

inline int64_t get_align_padding_size(const int64_t x, const int64_t mask)
{
  return -x & mask;
}

int serialize_nop_log(int64_t seq, char* buf, const int64_t limit, int64_t& pos)
{
  int err = OB_SUCCESS;
  ObLogEntry entry;
  int64_t data_len = get_align_padding_size(pos + entry.get_serialize_size() + 1, ObLogWriter::LOG_FILE_ALIGN_SIZE-1) + 1;
  entry.seq_ = seq;
  entry.cmd_ = OB_LOG_NOP;
  if (NULL == buf || 0 >= limit || pos >= limit)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (pos + entry.get_serialize_size() + data_len >= limit)
  {
    err = OB_BUF_NOT_ENOUGH;
  }
  else if (NULL == memset(buf + pos + entry.get_serialize_size(), 0, data_len))
  {
    err = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (err = entry.fill_header(buf + pos + entry.get_serialize_size(), data_len)))
  {
    TBSYS_LOG(ERROR, "entry.fill_header()=>%d", err);
  }
  else if (OB_SUCCESS != (err = entry.serialize(buf, limit, pos)))
  {
    TBSYS_LOG(ERROR, "entry.serialize()=>%d", err);
  }
  else
  {
    pos += data_len;
  }
  return err;
}

int serialize_mutator_log(int64_t seq, char* buf, const int64_t limit, int64_t& pos, ObUpsMutator& mutator)
{
  int err = OB_SUCCESS;
  ObLogEntry entry;
  int64_t tmp_pos = pos + entry.get_serialize_size();
  entry.seq_ = seq;
  entry.cmd_ = OB_LOG_UPS_MUTATOR;
  if (NULL == buf || 0 >= limit || pos >= limit)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = mutator.serialize(buf, limit, tmp_pos)))
  {
    TBSYS_LOG(ERROR, "mutator.serialize()=>%d", err);
  }
  else if (OB_SUCCESS != (err = entry.fill_header(buf + pos + entry.get_serialize_size(), mutator.get_serialize_size())))
  {
    TBSYS_LOG(ERROR, "entry.fill_header()=>%d", err);
  }
  else if (OB_SUCCESS != (err = entry.serialize(buf, limit, pos)))
  {
    TBSYS_LOG(ERROR, "entry.serialize()=>%d", err);
  }
  else if (tmp_pos - pos != mutator.get_serialize_size())
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "mutator.get_serialize_size()[%ld] != tmp_pos[%ld] - pos[%ld]", mutator.get_serialize_size(), tmp_pos, pos);
  }
  else
  {
    pos = tmp_pos;
  }
  return err;
}

int merge_mutator(const char* src_buf, const int64_t src_len, char* dest_buf, const int64_t dest_len, int64_t& ret_len)
{
  int err = OB_SUCCESS;
  int64_t src_pos = 0;
  int64_t dest_pos = 0;
  ObLogEntry entry;
  ObUpsMutator mutator;
  ObUpsMutator agg_mut;
  int64_t seq = 1;
  const int64_t size_limit = OB_MAX_LOG_BUFFER_SIZE;
  set_compatible_schema(mutator);
  set_compatible_schema(agg_mut);
  if (NULL == src_buf || NULL == dest_buf || 0 > src_len || 0 > dest_len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while(OB_SUCCESS == err && src_pos < src_len)
  {
    int64_t tmp_pos = 0;
    if (OB_SUCCESS != (err = entry.deserialize(src_buf, src_len, src_pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
    }
    else if (OB_LOG_UPS_MUTATOR != entry.cmd_)
    {}
    else if (OB_SUCCESS != (err = mutator.deserialize(src_buf + src_pos, entry.get_log_data_len(), tmp_pos)))
    {
      TBSYS_LOG(ERROR, "mutator.deserialize(seq=%ld)=>%d", (int64_t)entry.seq_, err);
    }
    else if (!mutator.is_normal_mutator())
    {
      TBSYS_LOG(DEBUG, "mutator[%ld] is not normal mutator", entry.seq_);
    }
    else if (OB_SUCCESS != (err = mutator_add(agg_mut.get_mutator(), mutator.get_mutator(), size_limit))
             && OB_SIZE_OVERFLOW != err)
    {
      TBSYS_LOG(ERROR, "mutator_add(%ld)=>%d", entry.seq_, err);
    }
    else if (OB_SUCCESS == err)
    {
      TBSYS_LOG(DEBUG, "mutator_add(%ld):succ", entry.seq_);
    }
    else if (OB_SUCCESS != (err = serialize_mutator_log(seq++, dest_buf, dest_len, dest_pos, agg_mut)))
    {
      TBSYS_LOG(ERROR, "serialize_mutator_log()=>%d", err);
    }
    else if (OB_SUCCESS != (err = serialize_nop_log(seq++, dest_buf, dest_len, dest_pos)))
    {
      TBSYS_LOG(ERROR, "serialize_nop_log()=>%d", err);
    }
    else if (OB_SUCCESS != (err = agg_mut.get_mutator().reset()))
    {
      TBSYS_LOG(ERROR, "agg_mut.reset()=>%d", err);
    }
    else if (OB_SUCCESS != (err = mutator_add(agg_mut.get_mutator(), mutator.get_mutator(), size_limit)))
    {
      TBSYS_LOG(ERROR, "mutator_add(%ld)=>%d", entry.seq_, err);
    }
    if (OB_SUCCESS == err)
    {
      src_pos += entry.get_log_data_len();
    }
  }
  if (OB_SUCCESS == err && OB_SUCCESS != (err = serialize_mutator_log(seq++, dest_buf, dest_len, dest_pos, agg_mut)))
  {
    TBSYS_LOG(ERROR, "serialize_mutator_log()=>%d", err);
  }
  else if (OB_SUCCESS != (err = serialize_nop_log(seq++, dest_buf, dest_len, dest_pos)))
  {
    TBSYS_LOG(ERROR, "serialize_nop_log()=>%d", err);
  }
  if (OB_SUCCESS == err && src_pos != src_len)
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", src_pos, src_len);
  }
  if (OB_SUCCESS == err)
  {
    ret_len = dest_pos;
  }
  return err;
}

int merge(const char* src_log_file, const char* dest_log_file)
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  char* dest_buf = NULL;
  int64_t src_len = 0;
  int64_t dest_len = 0;
  TBSYS_LOG(DEBUG, "merge(src=%s, dest=%s)", src_log_file, dest_log_file);
  if (NULL == src_log_file || NULL == dest_log_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(src_log_file, src_len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_log_file, src_len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_log_file, src_len, dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_log_file, err);
  }
  else if (OB_SUCCESS != (err = merge_mutator(src_buf, src_len, dest_buf, src_len, dest_len)))
  {
    TBSYS_LOG(ERROR, "merge_mutator(src_len=%ld, dest_len=%ld)=>%d", src_len, dest_len, err);
  }
  else if (OB_SUCCESS != (err = truncate(dest_log_file, dest_len)))
  {
    TBSYS_LOG(ERROR, "truncate(%s, len=%ld)=>%d", dest_log_file, dest_len, err);
  }
  return err;
}

// 每条log的seq = seq + file_id
int do_convert_log_seq_2to3(const int64_t file_id, char* buf, const int64_t len)
{
  int err = OB_SUCCESS;
  int64_t old_pos = 0;
  int64_t pos = 0;
  ObLogEntry log_entry;

  if (0 >= file_id || NULL == buf || 0 > len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while(OB_SUCCESS == err && pos < len)
  {
    old_pos = pos;
    if (OB_SUCCESS != (err = log_entry.deserialize(buf, len, pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
    }
    else
    {
      //TBSYS_LOG(DEBUG, "log_entry[seq=%ld, file_id=%ld]", log_entry.seq_, file_id);
      log_entry.set_log_seq(log_entry.seq_ + ((OB_LOG_SWITCH_LOG == log_entry.cmd_) ?file_id : file_id - 1));
    }

    if (OB_SUCCESS != err)
    {}
    else if (OB_SUCCESS != (err = log_entry.fill_header(buf + pos, log_entry.get_log_data_len())))
    {
      TBSYS_LOG(ERROR, "log_entry.fill_header()=>%d", err);
    }
    else if (OB_SUCCESS != (err = log_entry.serialize(buf, len, old_pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.serialize()=>%d", err);
    }
    if (OB_SUCCESS == err)
    {
      pos += log_entry.get_log_data_len();
    }
  }
  if (OB_SUCCESS == err && pos != len)
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", pos, len);
  }
  return err;
}

int reset_log_id(char* buf, int64_t len, const int64_t start_id)
{
  int err = OB_SUCCESS;
  int64_t real_log_id = start_id;
  int64_t old_pos = 0;
  int64_t pos = 0;
  ObLogEntry log_entry;

  if (0 >= start_id || NULL == buf || 0 > len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while(OB_SUCCESS == err && pos < len)
  {
    old_pos = pos;
    if (OB_SUCCESS != (err = log_entry.deserialize(buf, len, pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
    }
    else
    {
      TBSYS_LOG(DEBUG, "set_log_seq(%ld)", real_log_id);
      log_entry.set_log_seq(real_log_id++);
    }

    if (OB_SUCCESS != err)
    {}
    else if (OB_SUCCESS != (err = log_entry.fill_header(buf + pos, log_entry.get_log_data_len())))
    {
      TBSYS_LOG(ERROR, "log_entry.fill_header()=>%d", err);
    }
    else if (OB_SUCCESS != (err = log_entry.serialize(buf, len, old_pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.serialize()=>%d", err);
    }
    if (OB_SUCCESS == err)
    {
      pos += log_entry.get_log_data_len();
    }
  }
  if (OB_SUCCESS == err && pos != len)
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", pos, len);
  }
  return err;
}

int trans2to3(const char* src_log_file, const char* dest_log_file)
{
  int err = OB_SUCCESS;
  int64_t src_file_id = 0;
  const char* path_sep = NULL;
  const char* src_buf = NULL;
  char* dest_buf = NULL;
  int64_t len = 0;
  TBSYS_LOG(DEBUG, "trans2to3(src=%s, dest=%s)", src_log_file, dest_log_file);
  if (NULL == src_log_file || NULL == dest_log_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (0 >= (src_file_id = atoll((path_sep = strrchr(src_log_file, '/'))? path_sep+1: src_log_file)))
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "src_log_file[%s] should end with a number:path_sep=%s, strlen(path_sep)=%ld", src_log_file, path_sep, strlen(path_sep));
  }
  else if (OB_SUCCESS != (err = get_file_len(src_log_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_log_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_log_file, len, dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_log_file, err);
  }
  else if (NULL == memcpy(dest_buf, src_buf, len))
  {
    err = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (err = do_convert_log_seq_2to3(src_file_id, dest_buf, len)))
  {
    TBSYS_LOG(ERROR, "do_convert_log_seq_2to3(file_id=%ld)=>%d", src_file_id, err);
  }
  return err;
}

int fill_nop_log(char* buf, int64_t nop_size, int64_t n)
{
  int err = OB_SUCCESS;
  ObLogEntry entry;
  int64_t pos = 0;
  int64_t header_size = entry.get_serialize_size();
  if (NULL == buf || 0 >= nop_size || 0 >= n)
  {
    err = OB_INVALID_ARGUMENT;
  }
  for(int64_t i = 0; OB_SUCCESS == err && i < n; i++)
  {
    entry.set_log_seq(i + 1);
    entry.set_log_command(OB_LOG_NOP);
    memset(buf + pos, 0, nop_size);
    if (OB_SUCCESS != (err = entry.fill_header(buf + pos + header_size, nop_size - header_size)))
    {
      TBSYS_LOG(ERROR, "entry.fill_header()=>%d", err);
    }
    else if (OB_SUCCESS != (err = entry.serialize(buf, nop_size * n, pos)))
    {
      TBSYS_LOG(ERROR, "entry.serialize()=>%d", err);
    }
    else
    {
      pos += nop_size - header_size;
    }
  }
  return err;
}

int gen_nop(const char* dest_log_file, int64_t n_logs)
{
  int err = OB_SUCCESS;
  char* dest_buf = NULL;
  int64_t nop_size = OB_DIRECT_IO_ALIGN;
  TBSYS_LOG(DEBUG, "gen_nop(dest=%s, n_logs=%ld)", dest_log_file, n_logs);
  if (NULL == dest_log_file || 0 >= n_logs)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_log_file, nop_size * n_logs, dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_log_file, err);
  }
  else if (OB_SUCCESS != (err = fill_nop_log(dest_buf, nop_size, n_logs)))
  {
    TBSYS_LOG(ERROR, "fill_nop_log(n_logs=%ld)=>%d", n_logs, err);
  }
  return err;
}

int reset_id(const char* src_log_file, const char* dest_log_file, int64_t start_id)
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  char* dest_buf = NULL;
  int64_t len = 0;
  TBSYS_LOG(DEBUG, "reset_id(src=%s, dest=%s, start_id=%ld)", src_log_file, dest_log_file, start_id);
  if (NULL == src_log_file || NULL == dest_log_file || 0 >= start_id)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(src_log_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_log_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_log_file, len, dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_log_file, err);
  }
  else if (NULL == memcpy(dest_buf, src_buf, len))
  {
    err = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (err = reset_log_id(dest_buf, len, start_id)))
  {
    TBSYS_LOG(ERROR, "reset_log_id(start_id=%ld)=>%d", start_id, err);
  }
  return err;
}

int dumpi(const char* src_file)
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  int64_t len = 0;
  TBSYS_LOG(DEBUG, "dumpi(src=%s)", src_file);
  if (NULL == src_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(src_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_file, err);
  }
  if (0 != (len % sizeof(int64_t)))
  {
    TBSYS_LOG(WARN, "len[%ld] %% sizeof(int64_t)[%ld] != 0", len, sizeof(int64_t));
  }
  for(int64_t offset = 0; OB_SUCCESS == err && offset + (int64_t)sizeof(int64_t) <= len; offset += (int64_t)sizeof(int64_t))
  {
    printf("%ld\n", *(int64_t*)(src_buf + offset));
  }
  return err;
}

int sst2str(int64_t sstid)
{
  printf("%s\n", SSTableID::log_str(sstid));
  return 0;
}

int a2i(const char* src_file, const char* dest_file)
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  char* dest_buf = NULL;
  int64_t* dest_int_buf = NULL;;
  const char* start = NULL;
  char* end = NULL;
  int64_t len = 0;
  int64_t int_file_len = 0;
  TBSYS_LOG(DEBUG, "atoi(src=%s, dest=%s)", src_file, dest_file);
  if (NULL == src_file || NULL == dest_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(src_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_file, len * sizeof(int64_t), dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_file, err);
  }
  else
  {
    dest_int_buf = (int64_t*)dest_buf;
    start = src_buf;
    end = NULL;
    while(1)
    {
      int64_t id = strtol(start, &end, 10);
      if (end == start)
      {
        break;
      }
      else
      {
        *dest_int_buf++ = id;
        start = end;
      }
    }
    int_file_len = (int64_t)dest_int_buf - (int64_t)dest_buf;
  }
  if (OB_SUCCESS != err)
  {}
  else if (0 != truncate(dest_file, int_file_len))
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "truncate(file=%s, len=%ld): %s", dest_file, int_file_len, strerror(errno));
  }
  return err;
}

int select_log_by_id(char* dest_buf, const int64_t dest_limit, int64_t& dest_len, const char* src_buf, const int64_t src_len,
                     const int64_t* id_seq, int64_t n_id)
{
  int err = OB_SUCCESS;
  int64_t old_pos = 0;
  int64_t src_pos = 0;
  int64_t dest_pos = 0;
  ObLogEntry entry;
  int64_t size = 0;
  if (NULL == dest_buf || NULL == src_buf || NULL == id_seq || 0 >= dest_limit || 0 >= src_len || 0 >= n_id)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    std::set<int64_t> id_set(id_seq, id_seq + n_id);
    while(OB_SUCCESS == err && src_pos < src_len)
    {
      old_pos = src_pos;
      if (OB_SUCCESS != (err = entry.deserialize(src_buf, src_len, src_pos)))
      {
        TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
      }
      else if (id_set.find(entry.seq_) == id_set.end())
      {
        TBSYS_LOG(DEBUG, "ignore this log[seq=%ld]", entry.seq_);
      }
      else
      {
        size = entry.get_serialize_size() + entry.get_log_data_len();
        if (dest_pos + size > dest_limit)
        {
          err = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(ERROR, "dest_buf is not enough[dest_limit=%ld, cur_log_seq=%ld]", dest_limit, entry.seq_);
        }
        else
        {
          memcpy(dest_buf + dest_pos, src_buf + old_pos, size);
          dest_pos += size;
        }
      }

      if (OB_SUCCESS == err)
      {
        src_pos += entry.get_log_data_len();
      }
    }
  }
  if (OB_SUCCESS == err && src_pos != src_len)
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", src_pos, src_len);
  }
  if (OB_SUCCESS == err)
  {
    dest_len = dest_pos;
  }
  return err;
}

int select(const char* src_log_file, const char* dest_log_file, const char* id_seq_file)
{
  int err = OB_SUCCESS;
  int64_t src_len = 0;
  int64_t dest_len = 0;
  int64_t id_file_len = 0;
  const char* src_buf = NULL;
  char* dest_buf = NULL;
  const char* id_seq_buf = NULL;
  TBSYS_LOG(DEBUG, "select(src=%s, dest=%s, id_file=%s)", src_log_file, dest_log_file, id_seq_file);
  if (NULL == src_log_file || NULL == dest_log_file || NULL == id_seq_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(src_log_file, src_len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = get_file_len(id_seq_file, id_file_len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", id_seq_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_log_file, src_len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(id_seq_file, id_file_len, id_seq_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_log_file, src_len, dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_log_file, err);
  }
  else if (OB_SUCCESS != (err = select_log_by_id(dest_buf, src_len, dest_len, src_buf, src_len, (const int64_t*)id_seq_buf, id_file_len/sizeof(int64_t))))
  {
    TBSYS_LOG(ERROR, "select_log_by_id()=>%d", err);
  }
  if (0 != (truncate(dest_log_file, dest_len)))
  {
    TBSYS_LOG(ERROR, "truncate(file=%s, len=%ld):%s", dest_log_file, dest_len, strerror(errno));
  }
  return err;
}

#include "cmd_args_parser.h"
#define report_error(err, ...) if (OB_SUCCESS != err)TBSYS_LOG(ERROR, __VA_ARGS__);
__attribute__((constructor)) void init_mem_pool()
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
  {
    TBSYS_LOG(ERROR, "ob_init_memory_pool()=>%d", err);
  }
}

int main(int argc, char *argv[])
{
  int err = 0;
  TBSYS_LOGGER.setLogLevel(getenv("log_level")?:"WARN");
  init_log_cmd_str();
  init_action_flag_str();
  if (OB_SUCCESS != (err = g_cfg.init()))
  {
    TBSYS_LOG(ERROR, "g_cfg.init()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, read_file_test, StrArg(path), IntArg(offset)):OB_NEED_RETRY))
  {
    report_error(err, "read_file_test()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, gen_nop, StrArg(log_file), IntArg(n_logs)):OB_NEED_RETRY))
  {
    report_error(err, "gen_nop()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, reset_id, StrArg(src_log_file), StrArg(dest_log_file), IntArg(start_id)):OB_NEED_RETRY))
  {
    report_error(err, "reset()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, dump, StrArg(log_file), StrArg(type, "all")):OB_NEED_RETRY))
  {
    report_error(err, "dump()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, find, StrArg(dir), StrArg(type, "freeze")):OB_NEED_RETRY))
  {
    report_error(err, "find()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, merge, StrArg(src_log_file), StrArg(dest_log_file)):OB_NEED_RETRY))
  {
    report_error(err, "merge()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, cat, StrArg(log_file), IntArg(end_log_id, "0")):OB_NEED_RETRY))
  {
    report_error(err, "cat()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, locate, StrArg(log_dir), IntArg(log_id)):OB_NEED_RETRY))
  {
    report_error(err, "locate()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, expand, StrArg(path), IntArg(size)):OB_NEED_RETRY))
  {
    report_error(err, "expand()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, precreate, StrArg(path), IntArg(du_percent), IntArg(file_size, "67108864")):OB_NEED_RETRY))
  {
    report_error(err, "precreate()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, trans2to3, StrArg(src_log_file), StrArg(dest_log_file)):OB_NEED_RETRY))
  {
    report_error(err, "trans2to3()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, a2i, StrArg(src_file), StrArg(dest_file)):OB_NEED_RETRY))
  {
    report_error(err, "a2i()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, dumpi, StrArg(src_file)):OB_NEED_RETRY))
  {
    report_error(err, "dumpi()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, sst2str, IntArg(sst_id)):OB_NEED_RETRY))
  {
    report_error(err, "dumpi()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, select, StrArg(src_log_file), StrArg(dest_log_file), StrArg(id_seq_file)):OB_NEED_RETRY))
  {
    report_error(err, "select()=>%d", err);
  }
  else
  {
    fprintf(stderr, _usages, argv[0]);
  }
  return err;
}
