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

#include "common/ob_repeated_log_reader.h"
#include "common/utility.h"
#include "common/hash/ob_hashmap.h"
#include "updateserver/ob_ups_mutator.h"
#include "updateserver/ob_ups_utils.h"
#include "updateserver/ob_big_log_writer.h"

#include <string>
#include <sstream>

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::updateserver;

void print_usage(char* exe_name)
{
  fprintf(stderr, "\n"
      "Usage: %s log_name [sub row key] [schema_file]\n\n", exe_name
      );
}

class Param
{
public:
  typedef ObHashMap<uint64_t, std::string> IDMap;
  typedef ObHashMap<uint64_t, IDMap*> DIDMap;

public:
  static Param& get_instance()
  {
    static Param param_inst;
    return param_inst;
  }

  ObSchemaManagerV2& get_sch_mgr() {return sch_mgr_;}
  bool has_sub_row_key() {return sub_row_key_[0] != '\0';}
  bool has_schema() {return schema_file_[0] != '\0';}
  char* get_sub_row_key() {return sub_row_key_;}
  char* get_schema_file() {return schema_file_;}
  FILE* get_out_stream() {return out_stream_;}
  DIDMap& get_sch_map() {return sch_map_;}
  IDMap& get_tab_map() {return tab_map_;}

  void init_schema()
  {
    const ObTableSchema *table_iter =
        Param::get_instance().get_sch_mgr().table_begin();
    for (; table_iter != Param::get_instance().get_sch_mgr().table_end();
        table_iter++)
    {
      Param::IDMap *new_tab = new Param::IDMap();
      new_tab->create(64);

      int32_t column_num = 0;
      const ObColumnSchemaV2 *col_iter =
          Param::get_instance().get_sch_mgr().get_table_schema(
              table_iter->get_table_id(), column_num);
      printf("column_num=%d\n", column_num);
      for (int32_t i = 0; i < column_num; i++)
      {
        new_tab->set(col_iter->get_id(), std::string(col_iter->get_name()));
        col_iter++;
      }

      Param::get_instance().get_sch_map().set(table_iter->get_table_id(), new_tab);
      Param::get_instance().get_tab_map().set(table_iter->get_table_id(), std::string(table_iter->get_table_name()));
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
  }

private:
  char sub_row_key_[64];
  char schema_file_[64];
  ObSchemaManagerV2 sch_mgr_;

  FILE* out_stream_;

  DIDMap sch_map_;
  IDMap tab_map_;

};

#define ARR_LEN(a) (sizeof(a)/sizeof(a[0]))

#define ID_TO_STR_FUNC(FUNC_NAME, ARRAY, TYPE) \
    static const char* FUNC_NAME(TYPE id) \
    { \
      static std::ostringstream osstream; \
      if (id < static_cast<TYPE>(ARR_LEN(ARRAY)) && 0 != ARRAY[id]) \
      { \
        return ARRAY[id]; \
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
    LogCommandStr[OB_LOG_SWITCH_LOG]            = "SWITCH_LOG";
    LogCommandStr[OB_LOG_CHECKPOINT]            = "CHECKPOINT";
    LogCommandStr[OB_LOG_NOP]                   = "NOP";
    LogCommandStr[OB_LOG_UPS_MUTATOR]           = "UPS_MUTATOR";
    LogCommandStr[OB_UPS_SWITCH_SCHEMA]         = "UPS_SWITCH_SCHEMA";
    LogCommandStr[OB_RT_SCHEMA_SYNC]            = "OB_RT_SCHEMA_SYNC";
    LogCommandStr[OB_RT_CS_REGIST]              = "OB_RT_CS_REGIST";
    LogCommandStr[OB_RT_MS_REGIST]              = "OB_RT_MS_REGIST";
    LogCommandStr[OB_RT_SERVER_DOWN]            = "OB_RT_SERVER_DOWN";
    LogCommandStr[OB_RT_CS_LOAD_REPORT]         = "OB_RT_CS_LOAD_REPORT";
    LogCommandStr[OB_RT_CS_MIGRATE_DONE]        = "OB_RT_CS_MIGRATE_DONE";
    LogCommandStr[OB_RT_CS_START_SWITCH_ROOT_TABLE]
        = "OB_RT_CS_START_SWITCH_ROOT_TABLE";
    LogCommandStr[OB_RT_START_REPORT]           = "OB_RT_START_REPORT";
    LogCommandStr[OB_RT_REPORT_TABLETS]         = "OB_RT_REPORT_TABLETS";
    LogCommandStr[OB_RT_ADD_NEW_TABLET]         = "OB_RT_ADD_NEW_TABLET";
    LogCommandStr[OB_RT_CREATE_TABLE_DONE]      = "OB_RT_CREATE_TABLE_DONE";
    LogCommandStr[OB_RT_BEGIN_BALANCE]          = "OB_RT_BEGIN_BALANCE";
    LogCommandStr[OB_RT_BALANCE_DONE]           = "OB_RT_BALANCE_DONE";
    LogCommandStr[OB_RT_US_MEM_FRZEEING]        = "OB_RT_US_MEM_FRZEEING";
    LogCommandStr[OB_RT_US_MEM_FROZEN]          = "OB_RT_US_MEM_FROZEN";
    LogCommandStr[OB_RT_CS_START_MERGEING]      = "OB_RT_CS_START_MERGEING";
    LogCommandStr[OB_RT_CS_MERGE_OVER]          = "OB_RT_CS_MERGE_OVER";
    LogCommandStr[OB_RT_CS_UNLOAD_DONE]         = "OB_RT_CS_UNLOAD_DONE";
    LogCommandStr[OB_RT_US_UNLOAD_DONE]         = "OB_RT_US_UNLOAD_DONE";
    LogCommandStr[OB_RT_DROP_CURRENT_BUILD]     = "OB_RT_DROP_CURRENT_BUILD";
    LogCommandStr[OB_RT_DROP_LAST_CS_DURING_MERGE]
        = "OB_RT_DROP_LAST_CS_DURING_MERGE";
    LogCommandStr[OB_RT_SYNC_FROZEN_VERSION]    = "OB_RT_SYNC_FROZEN_VERSION";
    LogCommandStr[OB_RT_SET_UPS_LIST]           = "OB_RT_SET_UPS_LIST";
    LogCommandStr[OB_RT_SET_CLIENT_CONFIG]      = "OB_RT_SET_CLIENT_CONFIG";
	LogCommandStr[OB_UPS_SWITCH_SCHEMA_MUTATOR]           = "OB_UPS_SWITCH_SCHEMA_MUTATOR";
	LogCommandStr[OB_UPS_SWITCH_SCHEMA_NEXT]           = "OB_UPS_SWITCH_SCHEMA_NEXT";
	LogCommandStr[OB_UPS_WRITE_SCHEMA_NEXT]           = "OB_UPS_WRITE_SCHEMA_NEXT";
    //add shili [LONG_TRANSACTION_LOG]  20160926:b
    LogCommandStr[OB_UPS_BIG_LOG_DATA]           = "UPS_BIG_LOG_DATA";
    //add e

  }

  static void init_action_flag_str()
  {
    memset(ActionFlagStr, 0x00, sizeof(ActionFlagStr));
    ActionFlagStr[ObActionFlag::OP_UPDATE]      = "UPDATE";
    ActionFlagStr[ObActionFlag::OP_INSERT]      = "INSERT";
    ActionFlagStr[ObActionFlag::OP_DEL_ROW]     = "DEL_ROW";
    ActionFlagStr[ObActionFlag::OP_DEL_TABLE]   = "DEL_TABLE";
  }

  static void init_obj_type_str()
  {
    memset(ObjTypeStr, 0x00, sizeof(ObjTypeStr));
    ObjTypeStr[ObNullType]                      = "NULL";
    ObjTypeStr[ObIntType]                       = "Int";
    ObjTypeStr[ObFloatType]                     = "Float";
    ObjTypeStr[ObDoubleType]                    = "Double";
    ObjTypeStr[ObDateTimeType]                  = "DateTime";
    ObjTypeStr[ObPreciseDateTimeType]           = "PreciseDateTime";
    ObjTypeStr[ObVarcharType]                   = "Varchar";
    ObjTypeStr[ObSeqType]                       = "Seq";
    ObjTypeStr[ObCreateTimeType]                = "CreateTime";
    ObjTypeStr[ObModifyTimeType]                = "ModifyTime";
    ObjTypeStr[ObExtendType]                    = "Extend";
    ObjTypeStr[ObDecimalType]                   = "Decimal";
  }

  ID_TO_STR_FUNC(str_log_cmd, LogCommandStr, int64_t)
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

//add shili [LONG_TRANSACTION_LOG]  20160926:b
void print_big_ups_mutator_head(uint64_t seq,int64_t len)
{
  fprintf(Param::get_instance().get_out_stream(), "SEQ: %lu\tPayload Length: %ld\tTYPE: %s\t\n",
          seq, len, IDS::str_log_cmd(OB_UPS_BIG_LOG_DATA));
}

void print_big_ups_mutator_head(ObArray<uint64_t> &seqs, int64_t len, const ObUpsMutator &mut, const char* op)
{
  time_t tm = mut.get_mutate_timestamp() / 1000000;
  int length=0;
  char time_buf[64];
  ctime_r(&tm, time_buf);
  time_buf[strlen(time_buf) - 1] = '\0';
  char seq_buf[200];
  sprintf(seq_buf,"BIG_LOG_SEQ_ARRAY:");
  strcat(seq_buf,"[");
  for (int64_t i=0;i<seqs.count();i++)
  {
    length = (int)strlen(seq_buf);
    sprintf(seq_buf+length,"%ld ",seqs.at(i));
  }
  strcat(seq_buf,"]");

  fprintf(Param::get_instance().get_out_stream(), "%s\tPayload Length: %ld\tTYPE: %s\tMutatorTime: %s\t%s %ld "
            "ChecksumBefore=%lu ChecksumAfter=%lu\n",
          seq_buf, len, IDS::str_log_cmd(OB_UPS_BIG_LOG_DATA), time_buf, op, mut.get_mutate_timestamp(),
          mut.get_memtable_checksum_before_mutate(), mut.get_memtable_checksum_after_mutate());
}
//add e

void print_ups_mutator_head(int64_t seq, int64_t len, const ObUpsMutator &mut, const char* op)
{
  time_t tm = mut.get_mutate_timestamp() / 1000000;
  char time_buf[64];
  ctime_r(&tm, time_buf);
  time_buf[strlen(time_buf) - 1] = '\0';

  fprintf(Param::get_instance().get_out_stream(), "SEQ: %lu\tPayload Length: %ld\tTYPE: %s\tMutatorTime: %s\t%s %ld "
          "ChecksumBefore=%lu ChecksumAfter=%lu\n",
          seq, len, IDS::str_log_cmd(OB_LOG_UPS_MUTATOR), time_buf, op, mut.get_mutate_timestamp(),
          mut.get_memtable_checksum_before_mutate(), mut.get_memtable_checksum_after_mutate());
}

void print_ups_mutator_body(int cell_idx, ObMutatorCellInfo* cell, const char* row_key_buf)
{
  static std::string iu_op;
  static std::string table_name;
  static std::string column_name;

  iu_op = IDS::str_action_flag(cell->op_type.get_ext());

  if (!Param::get_instance().has_schema()) table_name = "";
  else table_name = get_table_name(cell->cell_info.table_id_);

  if (ObExtendType == cell->cell_info.value_.get_type())
  {
    const char* row_op = IDS::str_action_flag(cell->cell_info.value_.get_ext());
    if (!Param::get_instance().has_schema())
      fprintf(Param::get_instance().get_out_stream(), "  Cell[%03d]: op=%s %s table[id=%lu] row_key=%s\n",
          cell_idx, iu_op.c_str(), row_op, cell->cell_info.table_id_, row_key_buf);
    else
      fprintf(Param::get_instance().get_out_stream(), "  Cell[%03d]: op=%s %s table[id=%lu name=%s] row_key=%s\n",
          cell_idx, iu_op.c_str(), row_op, cell->cell_info.table_id_, table_name.c_str(), row_key_buf);
  }
  else
  {
    if (!Param::get_instance().has_schema())
    {
      fprintf(Param::get_instance().get_out_stream(), "  Cell[%03d]: op=%s table[id=%lu] row_key=%s column[id=%lu]",
          cell_idx, iu_op.c_str(), cell->cell_info.table_id_, row_key_buf, cell->cell_info.column_id_);
    }
    else
    {
      column_name = get_column_name(cell->cell_info.table_id_, cell->cell_info.column_id_);

      fprintf(Param::get_instance().get_out_stream(), "  Cell[%03d]: op=%s table[id=%lu name=%s] row_key=%s column[id=%lu name=%s]",
          cell_idx, iu_op.c_str(), cell->cell_info.table_id_, table_name.c_str(), row_key_buf,
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
    else if (ObDecimalType == cell->cell_info.value_.get_type())
    {
      ObString val;
      cell->cell_info.value_.get_decimal(val);
      fprintf(Param::get_instance().get_out_stream(), " decimal_value=\"%.*s\"\n", val.length(), val.ptr());
    }
    else
    {
      fprintf(Param::get_instance().get_out_stream(), " cell_type=%s\n", IDS::str_obj_type(cell->cell_info.value_.get_type()));
    }
  }
}


//add shili [LONG_TRANSACTION_LOG]  20160926:b
void print_big_log_mutator(uint64_t seq,int64_t len)
{
  print_big_ups_mutator_head(seq,len);
}

int print_big_log_mutator(ObArray<uint64_t> &seqs, const char* buf, int64_t len)
{
  int ret = 0;
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

    const int row_key_len = 1024;
    char row_key_buf[row_key_len];
    row_key_buf[0] = '\0';

    const char* mutator_op = "OB Operation"; // default is OB Operation

    bool print_flag = false;

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
            if (!Param::get_instance().has_sub_row_key())
            {
              print_big_ups_mutator_head(seqs, len, mut, "Freeze Operation");
            }
            continue;
          }
          else if (mut.is_drop_memtable())
          {
            if (!Param::get_instance().has_sub_row_key())
            {
              print_big_ups_mutator_head(seqs, len, mut, "Drop Operation");
            }
            continue;
          }
          else if (ObExtendType == cell->cell_info.value_.get_type())
          {
            if (ObActionFlag::OP_USE_OB_SEM == cell->cell_info.value_.get_ext())
            {
              mutator_op = "OB Operation";
              continue;
            }
            else if (ObActionFlag::OP_USE_DB_SEM == cell->cell_info.value_.get_ext())
            {
              mutator_op = "DB Operation";
              continue;
            }
          }
        }

        if (!Param::get_instance().has_sub_row_key() || strstr(row_key_buf, Param::get_instance().get_sub_row_key()) != NULL)
        {
          if (!print_flag)
          {
            print_big_ups_mutator_head(seqs, len, mut, mutator_op);
            print_flag = true;
          }
          print_ups_mutator_body(cell_idx, cell, to_cstring(cell->cell_info.row_key_));
        }
      }

      ++cell_idx;
    }
    if(OB_ITER_END==ret)
    {
      ret= OB_SUCCESS;
    }
  }
  return ret;
}
//add e

int print_ups_mutator(int64_t seq, const char* buf, int64_t len)
{
  int ret = 0;

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

    const int row_key_len = 1024;
    char row_key_buf[row_key_len];
    row_key_buf[0] = '\0';

    const char* mutator_op = "OB Operation"; // default is OB Operation

    bool print_flag = false;

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
            if (!Param::get_instance().has_sub_row_key())
            {
              print_ups_mutator_head(seq, len, mut, "Freeze Operation");
            }
            continue;
          }
          else if (mut.is_drop_memtable())
          {
            if (!Param::get_instance().has_sub_row_key())
            {
              print_ups_mutator_head(seq, len, mut, "Drop Operation");
            }
            continue;
          }
          else if (ObExtendType == cell->cell_info.value_.get_type())
          {
            if (ObActionFlag::OP_USE_OB_SEM == cell->cell_info.value_.get_ext())
            {
              mutator_op = "OB Operation";
              continue;
            }
            else if (ObActionFlag::OP_USE_DB_SEM == cell->cell_info.value_.get_ext())
            {
              mutator_op = "DB Operation";
              continue;
            }
          }
        }

        if (!Param::get_instance().has_sub_row_key() || strstr(row_key_buf, Param::get_instance().get_sub_row_key()) != NULL)
        {
          if (!print_flag)
          {
            print_ups_mutator_head(seq, len, mut, mutator_op);
            print_flag = true;
          }
          print_ups_mutator_body(cell_idx, cell, to_cstring(cell->cell_info.row_key_));
        }
      }

      ++cell_idx;
    }
  }

  return ret;
}

int main(int argc, char* argv[])
{
  int ret = 0;

  char* log_dir = NULL;
  char* log_name = NULL;
  char* p = NULL;

  TBSYS_LOGGER.setLogLevel("WARN");

  ob_init_memory_pool();
  if (argc < 2)
  {
    print_usage(argv[0]);
    ret = 1;
  }
  else
  {
    IDS::initialize();

    p = strrchr(argv[1], '/');
    if (NULL == p)
    {
      log_dir = const_cast<char*> (".");
      log_name = argv[1];
    }
    else
    {
      log_dir = argv[1];
      *p = '\0';
      log_name = p + 1;
    }

    if (argc >= 3)
    {
      strcpy(Param::get_instance().get_sub_row_key(), argv[2]);
    }

    if (argc >= 4)
    {
      strcpy(Param::get_instance().get_schema_file(), argv[3]);
    }
  }

  // load schema file if needed
  if (0 == ret)
  {
    if (Param::get_instance().has_schema())
    {
      tbsys::CConfig config;
      bool parse_ret = Param::get_instance().get_sch_mgr().parse_from_file(Param::get_instance().get_schema_file(), config);
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
  }

  // read log
  if (0 == ret)
  {
    ObRepeatedLogReader reader;
    ret = reader.init(log_dir);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "Error occured when init, ret=%d\n", ret);
    }
    else
    {
      ret = reader.open(atoi(log_name));
      if (OB_SUCCESS != ret)
      {
        fprintf(stderr, "Error occured when opening, ret=%d\n", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      LogCommand cmd;
      uint64_t log_seq;
      char* log_data;
      int64_t data_len;
      //add shili [LONG_TRANSACTION_LOG]  20160926:b
      ObBigLogWriter big_log_writer_;
      bool read_next_file = false;
      bool is_reading_big_log = false;
      bool is_all_completed = false;
      uint64_t switch_log_seq = 0;
      int64_t switch_data_len = 0;
      big_log_writer_.init();
      ObArray<uint64_t>  big_log_seqs_;
      //add e
      ret = reader.read_log(cmd, log_seq, log_data, data_len);
      while (OB_SUCCESS == ret)
      {
        if (OB_LOG_UPS_MUTATOR == cmd)
        {
          ret = print_ups_mutator(log_seq, log_data, data_len);
        }
        //add shili [LONG_TRANSACTION_LOG]  20160926:b
        else if(OB_UPS_BIG_LOG_DATA == cmd)
        {
          is_all_completed = false;
          is_reading_big_log = true;
          if(OB_SUCCESS!=(ret=big_log_writer_.package_big_log(log_data,data_len,is_all_completed)))
          {
            fprintf(stderr, "package_big_log fail,ret=%d",ret);
          }
          else
          {
            big_log_seqs_.push_back(log_seq);
            if(is_all_completed)
            {
              char* big_log_data;
              int32_t big_data_len;
              big_log_writer_.get_buffer(big_log_data,big_data_len);
              print_big_log_mutator(log_seq,data_len);//打印日志头
              //打印big log总体日志
              if(OB_SUCCESS != (ret = print_big_log_mutator(big_log_seqs_,big_log_data,big_data_len)))
              {
                fprintf(stderr, "Error occured when print_big_log_mutator, ret=%d\n", ret);
              }
              else
              {
                big_log_seqs_.clear();
              }
              if(OB_SUCCESS==ret&&read_next_file) //切换文件并且 该条长日志读取完毕，结束
              {
                cmd = OB_LOG_SWITCH_LOG;
                fprintf(stdout, "SEQ: %lu\tPayload Length: %ld\tTYPE: %s\n", switch_log_seq, switch_data_len, IDS::str_log_cmd(cmd));
                read_next_file = false;
                break;
              }
              is_all_completed = false;
              is_reading_big_log = false;
              read_next_file = false;
            }
            else
            {
              print_big_log_mutator(log_seq,data_len);
              is_reading_big_log = true;
            }
          }
        }
        else if(OB_LOG_SWITCH_LOG == cmd && is_reading_big_log)  //切换文件
        {
          read_next_file = true;
          switch_log_seq = log_seq;
          switch_data_len = data_len;
          if(OB_SUCCESS!=(ret = reader.close())) //关闭上一个文件
          {
            fprintf(stderr, "Error occured when close, ret=%d\n", ret);
          }
          else if(OB_SUCCESS!=(ret = reader.open(atoi(log_name)+1))) //读取下一个文件
          {
            fprintf(stderr, "Error occured when opening, ret=%d\n", ret);
          }
        }
        //add e
        else
        {
          if (!Param::get_instance().has_sub_row_key())
          {
            fprintf(stdout, "SEQ: %lu\tPayload Length: %ld\tTYPE: %s\n", log_seq, data_len, IDS::str_log_cmd(cmd));
          }
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
