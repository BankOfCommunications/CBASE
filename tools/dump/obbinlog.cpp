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
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */
#include <limits.h>
#include "common/ob_repeated_log_reader.h"
#include "common/utility.h"
#include "common/hash/ob_hashmap.h"
#include "updateserver/ob_ups_mutator.h"
#include "updateserver/ob_ups_utils.h"

#include <string>
#include <sstream>

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::updateserver;

int64_t convert_time_string_us(const char *time)
{
  tm time_tm;
  strptime(time, "%Y/%m/%d/%H:%M:%S", &time_tm);
  return mktime(&time_tm);
}

static bool is_number(char *s)
{
  size_t i = 0;

  for(i=0; i<strlen(s); i++)
  {
    if(s[i]<'0' or s[i]>'9')
      return false;
  }

  return true;
}

int search_logs(const char *log_dir_path, const char *time_start, const char *time_end, 
                int64_t &log_start, int64_t &log_end)
{
#define DATE_MAX 64
  char file_path[256];
  DIR * log_dir = NULL;
  char file_ctime_s[DATE_MAX] = "";
  long log_upper = LONG_MAX;

  log_start = LONG_MAX;
  log_end = LONG_MAX;

  struct dirent * file_in_dir = NULL;
  struct stat file_stat;
  struct tm file_ctime;

  TBSYS_LOG(INFO, "TIME SECTION:%s %s", time_start, time_end);

  if (time_start == NULL || time_end == NULL || strcmp(time_end, time_start) < 0) 
  {
    TBSYS_LOG(ERROR, "Time section error");
    return -1;
  }

  log_dir = opendir(log_dir_path);
  if( NULL == log_dir )
  {
    TBSYS_LOG(ERROR, "Can't open path,%s", log_dir_path);
    return -1;
  }

  while( (file_in_dir=readdir(log_dir)) && NULL!=file_in_dir )
  {
    file_path[0] = '\0';
    strcat(file_path, log_dir_path);
    strcat(file_path, "/");
    strcat(file_path, file_in_dir->d_name);

    stat(file_path, &file_stat);
    if( true == S_ISREG(file_stat.st_mode) )
    {
      localtime_r(&file_stat.st_mtime, &file_ctime);
      strftime(file_ctime_s, DATE_MAX, "%Y/%m/%d/%H:%M:%S", &file_ctime);

      TBSYS_LOG(DEBUG, "LOGID:%s, LOGTIME:%s", file_path, file_ctime_s);
      if(is_number(file_in_dir->d_name) && strcmp(file_ctime_s, time_start) >= 0)
      {
        if( atoi(file_in_dir->d_name) < log_start)
          log_start = atoi(file_in_dir->d_name);

        if (log_upper == LONG_MAX)
          log_upper = atoi(file_in_dir->d_name);
        else if (atoi(file_in_dir->d_name) > log_upper && strcmp(time_end, file_ctime_s) >= 0)
          log_upper = atoi(file_in_dir->d_name);

        if( strcmp(file_ctime_s, time_end) >= 0 )
        {
          if( atoi(file_in_dir->d_name) < log_end )
            log_end = atoi(file_in_dir->d_name);
        }
      }
      else
      {
        continue;
      }

    }
  }

  if (log_end == LONG_MAX) {
    log_end = log_upper;
  }

  if (log_end == LONG_MAX || log_start == LONG_MAX) {
    TBSYS_LOG(INFO, "Time Interval Wrong");
    closedir(log_dir);
    return -1;
  }

  closedir(log_dir);
  return 0;
}

#define BINLOG_PARAM Param::get_instance()
class Param
{
public:
  typedef ObHashMap<uint64_t, std::string> IDMap;
  typedef ObHashMap<uint64_t, IDMap*> DIDMap;

public:
  ~Param()
  {
    if (out_stream_ != stdout)
    {
      fclose(out_stream_);
    }
  }

  static Param& get_instance()
  {
    static Param param_inst;
    return param_inst;
  }

  ObSchemaManagerV2& get_sch_mgr() {return sch_mgr_;}
  bool has_sub_row_key() {return sub_row_key_[0] != '\0';}
  bool has_schema() {return schema_file_[0] != '\0';}
  bool has_time_section() { return start_time_str_ != NULL; }
  bool need_print_content() { return print_content_; }
  char* get_sub_row_key() {return sub_row_key_;}
  char* get_schema_file() {return schema_file_;}
  const char* get_start_time_str() { return start_time_str_; }
  const char* get_end_time_str() { return end_time_str_; }
  FILE* get_out_stream() {return out_stream_;}
  DIDMap& get_sch_map() {return sch_map_;}
  IDMap& get_tab_map() {return tab_map_;}
  const char *get_commit_log_dir() { return commit_log_dir_; }
  int64_t get_log_id() { return start_log_id_; }
  uint64_t get_table_id() { return table_id_; }

  int64_t get_start_time() { return start_time_; }
  int64_t get_end_time() { return end_time_; }
  uint64_t get_column_id() { return column_id_; }

  void set_print_content(bool val) { print_content_ = val; }
  void set_time_section(const char *start, const char *end) 
  {
    start_time_str_ = start; 
    end_time_str_ = end; 
    start_time_ = convert_time_string_us(start_time_str_);
    end_time_  = convert_time_string_us(end_time_str_);
  }
  void set_commit_log_dir(const char *dir) { commit_log_dir_ = dir; }
  void set_log_id(int64_t log_id) { start_log_id_ = log_id; }
  void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  void set_out_stream(FILE *out_stream) { out_stream_ = out_stream; }
  void set_column_id(uint64_t cid) { column_id_ = cid; }

  void init_schema()
  {
    const ObTableSchema *table_iter = Param::get_instance().get_sch_mgr().table_begin();
    for (; table_iter != Param::get_instance().get_sch_mgr().table_end(); table_iter++)
    {
      Param::IDMap *new_tab = new Param::IDMap();
      new_tab->create(128);

      const ObColumnSchemaV2 *col_iter = Param::get_instance().get_sch_mgr().column_begin();
      for (; col_iter != Param::get_instance().get_sch_mgr().column_end(); col_iter++)
      {
        if (table_iter->get_table_id() == col_iter->get_table_id())
        {
          new_tab->set(col_iter->get_id(), std::string(col_iter->get_name()));
        }
      }

      Param::get_instance().get_sch_map().set(table_iter->get_table_id(), new_tab);
      Param::get_instance().get_tab_map().set(table_iter->get_table_id(), 
                                              std::string(table_iter->get_table_name()));
    }
  }

private:
  Param()
  {
    memset(sub_row_key_, 0x00, sizeof(sub_row_key_));
    memset(schema_file_, 0x00, sizeof(schema_file_));
    out_stream_ = stdout;

    sch_map_.create(64);
    tab_map_.create(64);

    start_time_str_ = end_time_str_ = NULL;
    start_time_ = end_time_ = -1;
    start_log_id_ = INT64_MAX;
    column_id_ = table_id_ = INT64_MAX;
    commit_log_dir_ = NULL;

    print_content_ = false;
  }

private:
  char sub_row_key_[64];
  char schema_file_[64];
  ObSchemaManagerV2 sch_mgr_;

  FILE* out_stream_;

  DIDMap sch_map_;
  IDMap tab_map_;

  bool print_content_;
  int64_t start_time_;
  const char *start_time_str_;
  int64_t end_time_;
  const char *end_time_str_;
  const char *commit_log_dir_;
  int64_t start_log_id_;
  uint64_t table_id_;
  uint64_t column_id_;
};

#define ARR_LEN(a) (sizeof(a)/sizeof(a[0]))

#define ID_TO_STR_FUNC(FUNC_NAME, ARRAY, TYPE) \
    static const char* FUNC_NAME(TYPE id) \
    { \
      static std::ostringstream osstream; \
      if (id < static_cast<TYPE>(ARR_LEN(ARRAY)) && 0 != ARRAY[id]) \
      { \
        return ARRAY[static_cast<int32_t>(id)]; \
      } \
      else \
      { \
        osstream.seekp(0); \
        osstream << "UNKNOWN" << id; \
        return osstream.str().c_str(); \
      } \
    }

class IDS
{
public:
  static void initialize()
  {
    init_log_command_str();
    init_action_flag_str();
    init_obj_type_str();
  }

  static void init_log_command_str()
  {
    memset(LogCommandStr, 0x00, sizeof(LogCommandStr));
    LogCommandStr[OB_LOG_SWITCH_LOG] = "SWITCH_LOG";
    LogCommandStr[OB_LOG_CHECKPOINT] = "CHECKPOINT";
    LogCommandStr[OB_LOG_UPS_MUTATOR] = "UPS_MUTATOR";
    LogCommandStr[OB_UPS_SWITCH_SCHEMA] = "UPS_SWITCH_SCHEMA";
    LogCommandStr[OB_RT_SCHEMA_SYNC] = "OB_RT_SCHEMA_SYNC";
    LogCommandStr[OB_RT_CS_REGIST] = "OB_RT_CS_REGIST";
    LogCommandStr[OB_RT_MS_REGIST] = "OB_RT_MS_REGIST";
    LogCommandStr[OB_RT_SERVER_DOWN] = "OB_RT_SERVER_DOWN";
    LogCommandStr[OB_RT_CS_LOAD_REPORT] = "OB_RT_CS_LOAD_REPORT";
    LogCommandStr[OB_RT_CS_MIGRATE_DONE] = "OB_RT_CS_MIGRATE_DONE";
    LogCommandStr[OB_RT_CS_START_SWITCH_ROOT_TABLE] = "OB_RT_CS_START_SWITCH_ROOT_TABLE";
    LogCommandStr[OB_RT_START_REPORT] = "OB_RT_START_REPORT";
    LogCommandStr[OB_RT_REPORT_TABLETS] = "OB_RT_REPORT_TABLETS";
    LogCommandStr[OB_RT_ADD_NEW_TABLET] = "OB_RT_ADD_NEW_TABLET";
    LogCommandStr[OB_RT_CREATE_TABLE_DONE] = "OB_RT_CREATE_TABLE_DONE";
    LogCommandStr[OB_RT_BEGIN_BALANCE] = "OB_RT_BEGIN_BALANCE";
    LogCommandStr[OB_RT_BALANCE_DONE] = "OB_RT_BALANCE_DONE";
    LogCommandStr[OB_RT_US_MEM_FRZEEING] = "OB_RT_US_MEM_FRZEEING";
    LogCommandStr[OB_RT_US_MEM_FROZEN] = "OB_RT_US_MEM_FROZEN";
    LogCommandStr[OB_RT_CS_START_MERGEING] = "OB_RT_CS_START_MERGEING";
    LogCommandStr[OB_RT_CS_MERGE_OVER] = "OB_RT_CS_MERGE_OVER";
    LogCommandStr[OB_RT_CS_UNLOAD_DONE] = "OB_RT_CS_UNLOAD_DONE";
    LogCommandStr[OB_RT_US_UNLOAD_DONE] = "OB_RT_US_UNLOAD_DONE";
    LogCommandStr[OB_RT_DROP_CURRENT_BUILD] = "OB_RT_DROP_CURRENT_BUILD";
  }

  static void init_action_flag_str()
  {
    memset(ActionFlagStr, 0x00, sizeof(ActionFlagStr));
    ActionFlagStr[ObActionFlag::OP_UPDATE] = "UPDATE";
    ActionFlagStr[ObActionFlag::OP_INSERT] = "INSERT";
    ActionFlagStr[ObActionFlag::OP_DEL_ROW] = "DEL_ROW";
    ActionFlagStr[ObActionFlag::OP_DEL_TABLE] = "DEL_TABLE";
  }

  static void init_obj_type_str()
  {
    memset(ObjTypeStr, 0x00, sizeof(ObjTypeStr));
    ObjTypeStr[ObNullType] = "NULL";
    ObjTypeStr[ObIntType] = "Int";
    ObjTypeStr[ObFloatType] = "Float";
    ObjTypeStr[ObDoubleType] = "Double";
    ObjTypeStr[ObDateTimeType] = "DateTime";
    ObjTypeStr[ObPreciseDateTimeType] = "PreciseDateTime";
    ObjTypeStr[ObVarcharType] = "Varchar";
    ObjTypeStr[ObSeqType] = "Seq";
    ObjTypeStr[ObCreateTimeType] = "CreateTime";
    ObjTypeStr[ObModifyTimeType] = "ModifyTime";
    ObjTypeStr[ObExtendType] = "Extend";
    ObjTypeStr[ObInt32Type] = "Int32";//add qianzm
    //add qianzm 20151231
    ObjTypeStr[ObDateType] = "Date";
    ObjTypeStr[ObTimeType] = "Time";
    //add e
}

  ID_TO_STR_FUNC(str_log_cmd, LogCommandStr, int)
  ID_TO_STR_FUNC(str_action_flag, ActionFlagStr, int64_t)
  ID_TO_STR_FUNC(str_obj_type, ObjTypeStr, int)

private:
  DISALLOW_COPY_AND_ASSIGN(IDS);

  static const char* LogCommandStr[1024];
  static const char* ActionFlagStr[1024];
  static const char* ObjTypeStr[ObMaxType + 1];
};

const char* IDS::LogCommandStr[1024];
const char* IDS::ActionFlagStr[1024];
const char* IDS::ObjTypeStr[ObMaxType + 1];

const char* get_table_name(uint64_t id)
{
  static std::string table_name;
  int ret = Param::get_instance().get_tab_map().get(id, table_name);
  if (HASH_NOT_EXIST == ret) table_name = "UNKNOWN";
  return table_name.c_str();
}

const char* get_column_name(uint64_t tab_id, uint64_t col_id)
{
  static std::string column_name;
  Param::IDMap *col_map = NULL;
  int ret = Param::get_instance().get_sch_map().get(tab_id, col_map);
  if (HASH_NOT_EXIST == ret) column_name = "UNKNOWN";
  else
  {
    ret = col_map->get(col_id, column_name);
    if (HASH_NOT_EXIST == ret) column_name = "UNKNOWN";
  }
  return column_name.c_str();
}

void print_ups_mutator_head(int64_t seq, int64_t len, const ObUpsMutator &mut, const char* op)
{
  time_t tm = mut.get_mutate_timestamp() / 1000000;
  char time_buf[64];
  ctime_r(&tm, time_buf);
  time_buf[strlen(time_buf) - 1] = '\0';

  fprintf(Param::get_instance().get_out_stream(), "SEQ: %lu\tPayload Length: %ld\tTYPE: %s\tMutatorTime: %s\t%s\n",
      seq, len, IDS::str_log_cmd(OB_LOG_UPS_MUTATOR), time_buf, op);
}

void print_mutator_body_simple(int cell_idx, ObUpsMutator *mut, ObMutatorCellInfo* cell, const char * row_key_buf, const char *time_buf, bool first_cell)
{
  static ObCellInfo last_cell;
  static int last_ext = -1;
  UNUSED(cell_idx);
  UNUSED(mut);

  int obj_ext = static_cast<int32_t>(cell->cell_info.value_.get_ext());
  if (obj_ext != ObActionFlag::OP_DEL_ROW)
    obj_ext = static_cast<int32_t>(cell->op_type.get_ext());

//  time_t tm = mut->get_mutate_timestamp() / 1000000;
//  char time_buf[64];
//  ctime_r(&tm, time_buf);
//  time_buf[strlen(time_buf) - 1] = '\0';

  if (first_cell || last_cell.row_key_ != cell->cell_info.row_key_ ||
      last_cell.table_id_ != cell->cell_info.table_id_ ||
      last_cell.table_name_ != cell->cell_info.table_name_ || obj_ext != last_ext)
  {
    last_cell = cell->cell_info;
    last_ext = obj_ext;

    if (BINLOG_PARAM.get_table_id() != INT64_MAX && 
        BINLOG_PARAM.get_table_id() != cell->cell_info.table_id_) /* table id filter */
    {
      return;
    }

    fprintf(BINLOG_PARAM.get_out_stream(), "table id=%ld rowkey=%s op=%s timestamp:%s\n", 
            cell->cell_info.table_id_, row_key_buf, 
            IDS::str_action_flag(obj_ext), time_buf);
  }
}

void print_ups_mutator_body(int cell_idx, ObMutatorCellInfo* cell, 
                            const char* row_key_buf, const char *time_str)
{
  static std::string iu_op;
  static std::string table_name;
  static std::string column_name;

  if (BINLOG_PARAM.get_table_id() != INT64_MAX && 
      BINLOG_PARAM.get_table_id() != cell->cell_info.table_id_) /* table id filter */
  {
    return;
  }

  if (BINLOG_PARAM.get_column_id() != INT64_MAX &&
      BINLOG_PARAM.get_column_id() != cell->cell_info.column_id_) /* column id filter */
  {
    return;
  }

  int obj_ext = static_cast<int32_t>(cell->cell_info.value_.get_ext());
  if (obj_ext != ObActionFlag::OP_DEL_ROW)
    obj_ext = static_cast<int32_t>(cell->op_type.get_ext());

  iu_op = IDS::str_action_flag(obj_ext);

  if (!Param::get_instance().has_schema()) table_name = "";
  else table_name = get_table_name(cell->cell_info.table_id_);

  if (ObExtendType == cell->cell_info.value_.get_type()) /* delete */
  {
    const char* row_op = IDS::str_action_flag(cell->cell_info.value_.get_ext());
    if (!Param::get_instance().has_schema())
      fprintf(Param::get_instance().get_out_stream(), "MTime=[%s]  Cell[%03d]: op=%s %s table[id=%lu] row_key=%s\n",
          time_str, cell_idx, iu_op.c_str(), row_op, cell->cell_info.table_id_, row_key_buf);
    else
      fprintf(Param::get_instance().get_out_stream(), "MTime=[%s]  Cell[%03d]: op=%s %s table[id=%lu name=%s] row_key=%s\n",
          time_str, cell_idx, iu_op.c_str(), row_op, cell->cell_info.table_id_, table_name.c_str(), row_key_buf);
  }
  else
  {
    if (!Param::get_instance().has_schema())
    {
      fprintf(Param::get_instance().get_out_stream(), "MTime=[%s]  Cell[%03d]: op=%s table[id=%lu] row_key=%s column[id=%lu]",
          time_str, cell_idx, iu_op.c_str(), cell->cell_info.table_id_, row_key_buf, cell->cell_info.column_id_);
    }
    else
    {
      column_name = get_column_name(cell->cell_info.table_id_, cell->cell_info.column_id_);

      fprintf(Param::get_instance().get_out_stream(), "MTime=[%s] Cell[%03d]: op=%s table[id=%lu name=%s] row_key=%s column[id=%lu name=%s]",
          time_str, cell_idx, iu_op.c_str(), cell->cell_info.table_id_, table_name.c_str(), row_key_buf,
          cell->cell_info.column_id_, column_name.c_str());
    }

    if (ObIntType == cell->cell_info.value_.get_type())
    {
      int64_t val = 0;
      cell->cell_info.value_.get_int(val);
      fprintf(Param::get_instance().get_out_stream(), " int_value=%ld\n", val);
    }
    else if (ObVarcharType == cell->cell_info.value_.get_type())
    {
      ObString val;
      cell->cell_info.value_.get_varchar(val);
      fprintf(Param::get_instance().get_out_stream(), " varchar_value=\"%.*s\"\n", val.length(), val.ptr());
    }
    else if (ObDateTimeType == cell->cell_info.value_.get_type())
    {
      int64_t val = 0;
      cell->cell_info.value_.get_datetime(val);
      fprintf(Param::get_instance().get_out_stream(), " datetime_value=%s\n", time2str(val * 1000000));
    }
    else
    {
      fprintf(Param::get_instance().get_out_stream(), " cell_type=%s\n", IDS::str_obj_type(cell->cell_info.value_.get_type()));
    }
  }
}

int print_ups_mutator(int64_t seq, const char* buf, int64_t len)
{
  int ret = 0;
  UNUSED(seq);

  ObUpsMutator mut;
  int64_t pos = 0;
  ret = mut.deserialize(buf, len, pos);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "Error occured when deserializing ObUpsMutator");
  }
  else
  {
    ObMutatorCellInfo* cell = NULL;
    int cell_idx = 0;

    const int row_key_len = OB_MAX_ROW_KEY_LENGTH;
    static char row_key_buf[row_key_len];
    row_key_buf[0] = '\0';

    const char* mutator_op = NULL;

//    bool print_flag = false;
    bool first_cell = true;

    time_t mut_time = mut.get_mutate_timestamp() / 1000000;
    char time_buf[64];

    ctime_r(&mut_time, time_buf);
    time_buf[strlen(time_buf) - 1] = '\0';

    if (BINLOG_PARAM.get_start_time_str() != NULL && 
        (mut_time < BINLOG_PARAM.get_start_time() || 
        mut_time > BINLOG_PARAM.get_end_time()))
    {
      TBSYS_LOG(DEBUG, "skiping log, timestamp=%ld, %s", mut_time, BINLOG_PARAM.get_start_time_str());
    }
    else
    {
      while (OB_SUCCESS == (ret = mut.next_cell()))
      {
        ret = mut.get_cell(&cell);
        if (OB_SUCCESS != ret || NULL == cell)
        {
          fprintf(stderr, "get cell error\n");
        }
        else
        {
          // figure out freeze/drop/normal operation of UpsMutator
          if (0 == cell_idx)
          {
            if (mut.is_freeze_memtable())
            {
              /*
              if (!Param::get_instance().has_sub_row_key())
              {
                print_ups_mutator_head(seq, len, mut, "Freeze Operation");
              }
              */
              continue;
            }
            else if (mut.is_drop_memtable())
            {
              /*
              if (!Param::get_instance().has_sub_row_key())
              {
                print_ups_mutator_head(seq, len, mut, "Drop Operation");
              }
              */
              continue;
            }
            else if (ObExtendType == cell->cell_info.value_.get_type())
            {
              if (ObActionFlag::OP_USE_OB_SEM == cell->cell_info.value_.get_ext())
              {
                mutator_op = "OB Operation";
              }
              else if (ObActionFlag::OP_USE_DB_SEM == cell->cell_info.value_.get_ext())
              {
                mutator_op = "DB Operation";
              }
              else if (ObActionFlag::OP_DEL_ROW == cell->cell_info.value_.get_ext())
              {
                mutator_op = "OB Operation";
              }
              else
              {
                fprintf(stderr, "OB/DB operation deserialize error, ext=%ld\n", cell->cell_info.value_.get_ext());
                ret = OB_ERROR;
                break;
              }
            }
            else
            {
              mutator_op = "OB Operation";
            }
          }

          hex_to_str(cell->cell_info.row_key_.ptr(), cell->cell_info.row_key_.length(), row_key_buf, row_key_len);

          if (!Param::get_instance().has_sub_row_key() || strstr(row_key_buf, Param::get_instance().get_sub_row_key()) != NULL)
          {
//            if (!print_flag)
//            {
//              print_ups_mutator_head(seq, len, mut, mutator_op);
//              print_flag = true;
//            }
//            else
//            {
              if (BINLOG_PARAM.need_print_content())
              {
                print_ups_mutator_body(cell_idx, cell, row_key_buf, time_buf);
              }
              else 
              {
                print_mutator_body_simple(cell_idx, &mut, cell, row_key_buf, time_buf, first_cell);
                first_cell = false;
              }
//            }
          }
        }

        ++cell_idx;
      }
    }
  }

  return ret;
}

int parse_args(int argc, char *argv[])
{
  int ret = -1;
  const char *begin_time = NULL;
  const char *end_time = NULL;
  const char *fname = NULL;
  int opt = -1;

  while ((opt = getopt(argc, argv, "l:k:s:t:d:b:e:co:m:")) != -1) 
  {
    switch (opt) 
    {
     case 'k':
       strcpy(Param::get_instance().get_sub_row_key(), optarg);
       break;
     case 's':
       strcpy(Param::get_instance().get_schema_file(), optarg);
       break;
     case 'b':
       begin_time = optarg;
       break;
     case 'e':
       end_time = optarg;
       break;
     case 'l':
       BINLOG_PARAM.set_log_id(atol(optarg));
       break;
     case 'c':
       BINLOG_PARAM.set_print_content(true);
       break;
     case 't':
       BINLOG_PARAM.set_table_id(atol(optarg));
       TBSYS_LOG(INFO, "accept table id [%ld]", BINLOG_PARAM.get_table_id());
       break;
     case 'd':
       ret = 0;
       BINLOG_PARAM.set_commit_log_dir(optarg); /* log dir must be set */
       break;
     case 'm':
       BINLOG_PARAM.set_column_id(atol(optarg));
       TBSYS_LOG(INFO, "using column id ,[%ld]", BINLOG_PARAM.get_column_id());
       break;
     case 'o':
       fname = optarg;
       break;
     default:
       ret = -1;
       fprintf(stderr, "error opt specified, [- %c] is not supported", opt);
    }
  }

  if (begin_time != NULL) 
  {
    if (end_time == NULL) 
    {
      tm time_tm;
      static char time_buf[128];
      time_t now = time(NULL);

      localtime_r(&now, &time_tm);
      strftime(time_buf, 128, "%Y/%m/%d/%H:%M:%S", &time_tm);
      end_time = time_buf;
    }

    BINLOG_PARAM.set_time_section(begin_time, end_time);
  }

  if (ret == 0)
  {
    FILE *file = stdout;
    if (fname != NULL)
    {
      file = fopen(fname, "w");
      if (file == NULL)
      {
        fprintf(stderr, "can't create output file %s\n", fname);
        ret = -1;
      }
    }

    if (ret == 0)
    {
      BINLOG_PARAM.set_out_stream(file);
    }
  }

  if (ret == 0 && Param::get_instance().has_schema())
  {
    tbsys::CConfig config;
    bool parse_ret = 
      BINLOG_PARAM.get_sch_mgr().parse_from_file(BINLOG_PARAM.get_schema_file(), config);

    if (!parse_ret)
    {
      fprintf(stderr, "parse schema file error\n");
      ret = OB_ERROR;
    }
    else
    {
      Param::get_instance().init_schema();
    }
  }

  return ret;
}

void print_usage(char* exe_name)
{
  fprintf(stderr,
      "Usage:" 
      "  %s -l [log id] -k [find key] -t [table id] -d [ commit log dir ]\n"
      "  \t-b [begin time] -e [end time] -s [schema file] [-c detail]\n", 
      exe_name);
}

int process_one_log(int64_t log_id)
{
  ObRepeatedLogReader reader;
  int ret = reader.init(BINLOG_PARAM.get_commit_log_dir());
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "Error occured when init, ret=%d\n", ret);
  }
  else
  {
    ret = reader.open(log_id);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "Error occured when opening, ret=%d\n", ret);
    }

    if (OB_SUCCESS == ret)
    {
      LogCommand cmd;
      uint64_t log_seq;
      char* log_data;
      int64_t data_len;

      ret = reader.read_log(cmd, log_seq, log_data, data_len);
      while (OB_SUCCESS == ret)
      {
        if (OB_LOG_UPS_MUTATOR == cmd)
        {
          ret = print_ups_mutator(log_seq, log_data, data_len);
        }

        ret = reader.read_log(cmd, log_seq, log_data, data_len);
      }

      if (OB_READ_NOTHING == ret)
      {
        ret = OB_SUCCESS;
      }
      else
      {
        fprintf(stderr, "Error occured when reading, ret=%d\n", ret);
      }
    }
  }

  return ret;
}

int handle_commitlogs()
{
  int64_t begin_id = 0;
  int64_t end_id = 0;

  int ret = OB_SUCCESS;
  if (search_logs(BINLOG_PARAM.get_commit_log_dir(), 
                  BINLOG_PARAM.get_start_time_str(),
                  BINLOG_PARAM.get_end_time_str(),
                  begin_id, end_id) == 0) 
  {
    TBSYS_LOG(INFO, "Log Section need to be scand[%ld, %ld]", begin_id, end_id);
    while (begin_id <= end_id) 
    {
      TBSYS_LOG(INFO, "processing commitlog [%ld]", begin_id);
      ret = process_one_log(begin_id);
      if (ret != OB_SUCCESS) 
      {
        fprintf(stderr, "Error occured when process one log, log_id=%ld, ret=%d\n", begin_id, ret);
      }

      begin_id++;
    }
  }
  else
  {
    ret = OB_ERROR;
    fprintf(stderr, "Error occured when search logs\n");
  }

  return ret;
}

int main(int argc, char* argv[])
{
  int ret = 0;
  TBSYS_LOGGER.setLogLevel("INFO");

  ob_init_memory_pool();
  IDS::initialize();

  ret = parse_args(argc ,argv);
  if (ret != 0) 
  {
    print_usage(argv[0]);
    ret = 1;
  }
  else
  {
    if (BINLOG_PARAM.get_start_time_str() != NULL)
    {
      TBSYS_LOG(INFO, "START:[%s--%ld], END:[%s--%ld]", BINLOG_PARAM.get_start_time_str(),
                BINLOG_PARAM.get_start_time(), BINLOG_PARAM.get_end_time_str(),
                BINLOG_PARAM.get_end_time());

      ret = handle_commitlogs();
    }
    else
    {
      if (BINLOG_PARAM.get_log_id() != INT64_MAX)
      {
        TBSYS_LOG(INFO, "processing one log [%ld]\n", BINLOG_PARAM.get_log_id());
        ret = process_one_log(BINLOG_PARAM.get_log_id());
      }
      else
      {
        ret = -1;
        fprintf(stderr, "no availabe log find\n");
      }
    }
  }

  return ret;
}
