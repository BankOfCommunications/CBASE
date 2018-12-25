/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_ups_client.cpp,v 0.1 2011/01/07 16:55:10 xielun Exp $
 *
 * Authors:
 *     - some work details if you want
 *
 */

#include <getopt.h>
#include <string>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>
#include "../updateserver/mock_client.h"
#include "../updateserver/test_utils.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
#include "../common/test_rowkey_helper.h"

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
const int64_t TIMEOUT =  50000000L;
static const int64_t MAX_ROW_KEY = 1024ll*1024ll*16ll-1ll;
static const uint64_t MAX_COLUMN_ID = 255;
static const int64_t delete_row_interval = 10;
static CharArena allocator_;

int64_t get_cur_time_us()
{
  struct timeval now;
  gettimeofday(&now,NULL);
  return(now.tv_sec*1000000 + now.tv_usec);
}

struct CParam
{
  char *server_addr_;
  int32_t server_port_;
  int32_t interval_;
  char *schema_fname_;
  int64_t start_key_;
  int64_t end_key_;
  ObString left_table_;
  uint64_t left_table_id_;
  ObString right_table_;
  uint64_t right_table_id_;
  const ObTableSchema *left_schema_;
  const ObTableSchema *right_schema_;
  char *operation_;
  oceanbase::common::ObSchemaManagerV2 *schema_mgr_;
  int64_t cur_version_;
};

enum
{
  LEFT_TABLE_TYPE,
  RIGHT_TABLE_TYPE
};
struct rowkey_t
{
  union
  {
    unsigned int key_val_;
    struct
    {
      unsigned int type_:8;
      unsigned int val_:24;
    };
  };
};
struct left_rowkey_t
{
  rowkey_t  left_key_;
  rowkey_t  right_key_;
};

struct column_obj_t
{
  rowkey_t  key_;
  union
  {
    uint32_t column_;
    struct
    {
      uint32_t  column_id_:8;
      uint32_t  version_:24;
    };
  };
};



void print_usage()
{
  fprintf(stderr, "\n");
  fprintf(stderr, "batch_client [OPTION]\n");
  fprintf(stderr, "   -a|--addr server address\n");
  fprintf(stderr, "   -p|--port server port\n");
  fprintf(stderr, "   -m|--schema schema file\n");
  fprintf(stderr, "   -l|--left left table\n");
  fprintf(stderr, "   -r|--right left table\n");
  fprintf(stderr, "   -s|--start start rowkey must int type [0~%ld]\n", MAX_ROW_KEY);
  fprintf(stderr, "   -e|--end end rowkey must int type\n");
  fprintf(stderr, "   -o|--operation operation [scan,update]\n");
  fprintf(stderr, "   -v|--version specify current version of active memory table\n");
  fprintf(stderr, "   -h|--help print this help info\n");
  fprintf(stderr, "\n");
}

int check_schema(CParam &param)
{
  int err = OB_SUCCESS;
  const ObTableSchema * left_schema = NULL;
  const ObTableSchema * right_schema = NULL;
  left_schema = param.schema_mgr_->get_table_schema(param.left_table_);
  if (NULL == left_schema)
  {
    TBSYS_LOG(WARN,"left table not exist [table_name:%.*s]", param.left_table_.length(), param.left_table_.ptr());
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    param.left_table_id_ = left_schema->get_table_id();
    param.left_schema_ = left_schema;
  }
  if (OB_SUCCESS == err)
  {
    right_schema = param.schema_mgr_->get_table_schema(param.right_table_);
    if (NULL == right_schema)
    {
      TBSYS_LOG(WARN,"right table not exist [table_name:%.*s]", param.right_table_.length(), param.right_table_.ptr());
      err = OB_INVALID_ARGUMENT;
    }
    else
    {
      param.right_table_id_ = right_schema->get_table_id();
      param.right_schema_ = right_schema;
    }
  }
  if (OB_SUCCESS == err)
  {
    if (left_schema->get_rowkey_max_length() != static_cast<int64_t>(sizeof(left_rowkey_t)))
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN,"left table's rowkey len must be %zu", sizeof(left_rowkey_t));
    }
  }
  if (OB_SUCCESS == err)
  {
    if (!left_schema->is_row_key_fixed_len())
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN,"left table's rowkey len must be fixed");
    }
  }
  if (OB_SUCCESS == err)
  {
    int32_t column_count = 0;
    const ObColumnSchemaV2 *column_begin = param.schema_mgr_->get_table_schema(param.left_table_id_, column_count);
    const ObColumnSchemaV2 *column_end  = column_begin + column_count;
    const ObColumnSchemaV2::ObJoinInfo           *join_info = NULL;
    /*
    int32_t start_pos = -1;
    int32_t end_pos = -1;
    */
    for (;column_begin < column_end && OB_SUCCESS == err; column_begin++)
    {
      join_info = column_begin->get_join_info();
      if (NULL != join_info)
      {
        if (join_info->join_table_ != right_schema->get_table_id())
        {
          TBSYS_LOG(WARN,"join info not correct [left.column_id:%s,right.table_id:%lu,real_right.table_id:%lu]",
            column_begin->get_name(),join_info->join_table_,
            right_schema->get_table_id());
          err = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS == err)
        {
          /*
          start_pos = join_info->start_pos_;
          end_pos = join_info->end_pos_;
          if (start_pos != static_cast<int32_t>(sizeof(rowkey_t))
            || end_pos != static_cast<int32_t>(sizeof(left_rowkey_t))-1)
          {
            TBSYS_LOG(WARN,"wrong join rowkey range [start_pos:%d,end_pos:%d,expedted_start_pos:%zu,expected_end_pos:%zu]",
              start_pos,end_pos, sizeof(rowkey_t), sizeof(left_rowkey_t) - 1);
            err = OB_INVALID_ARGUMENT;
          }
          */
        }
        if (OB_SUCCESS == err)
        {
          uint64_t right_column_id = join_info->correlated_column_;
          if (right_column_id != column_begin->get_id())
          {
            TBSYS_LOG(WARN,"left id and right id not equal [left_id:%lu,right_id:%lu]", column_begin->get_id(), right_column_id);
            err = OB_INVALID_ARGUMENT;
          }
        }
      }
      if (OB_SUCCESS == err && column_begin->get_id() > MAX_COLUMN_ID)
      {
        TBSYS_LOG(WARN,"column id too max, MAX_COLUMN_ID:%ld", MAX_COLUMN_ID);
        err = OB_INVALID_ARGUMENT;
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    if (right_schema->get_rowkey_max_length() != static_cast<int64_t>(sizeof(rowkey_t)))
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN,"right table's rowkey len must be %zu", sizeof(rowkey_t));
    }
  }
  if (OB_SUCCESS == err)
  {
    if (!right_schema->is_row_key_fixed_len())
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN,"right table's rowkey len must be fixed");
    }
  }
  return err;
}

int parse_cmd_args(int argc, char **argv, CParam & param)
{
  int err = OB_SUCCESS;
  int opt = 0;
  memset(&param,0x00,sizeof(param));
  const char* opt_string = "a:p:m:l:r:s:e:o:i:v:h";
  struct option longopts[] =
  {
    {"addr", 1, NULL, 'a'},
    {"port", 1, NULL, 'p'},
    {"schema", 1, NULL, 'm'},
    {"left", 1, NULL, 'l'},
    {"right", 1, NULL, 'r'},
    {"start", 1, NULL, 's'},
    {"end", 1, NULL, 'e'},
    {"operation", 1, NULL, 'o'},
    {"interval", 1, NULL, 'i'},
    {"version", 1, NULL, 'v'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  param.interval_ = 1;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
    case 'a':
      param.server_addr_ = optarg;
      break;
    case 'p':
      param.server_port_ = atoi(optarg);
      break;
    case 'i':
      param.interval_ = atoi(optarg);
      break;
    case 'm':
      param.schema_fname_ = optarg;
      break;
    case 'l':
      param.left_table_.assign(optarg, static_cast<int32_t>(strlen(optarg)));
      break;
    case 'r':
      param.right_table_.assign(optarg, static_cast<int32_t>(strlen(optarg)));
      break;
    case 's':
      param.start_key_ = atol(optarg);
      break;
    case 'e':
      param.end_key_ = atol(optarg);
      break;
    case 'o':
      param.operation_ = optarg;
      break;
    case 'v':
      param.cur_version_ = atol(optarg);
      break;
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }

  if (NULL == param.server_addr_
    || 0 == param.server_port_
    || NULL == param.schema_fname_
    || NULL == param.left_table_.ptr()
    || NULL == param.right_table_.ptr()
    || NULL == param.operation_
    || MAX_ROW_KEY < param.start_key_
    || param.start_key_ > param.end_key_
    || 0 > param.start_key_
    || 2 > param.cur_version_
    )
  {
    print_usage();
    exit(1);
  }
  if (strcmp(param.operation_,"scan") != 0
    && strcmp(param.operation_, "update_wide") != 0
    && strcmp(param.operation_, "update_all") != 0)
  {
    print_usage();
    exit(1);
  }

  if (OB_SUCCESS == err)
  {
    param.schema_mgr_ = new ObSchemaManagerV2;
    if (NULL == param.schema_mgr_)
    {
      TBSYS_LOG(WARN, "%s", "fail to allocate memory for schema manager");
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  tbsys::CConfig config;
  if (OB_SUCCESS == err && !param.schema_mgr_->parse_from_file(param.schema_fname_,config))
  {
    TBSYS_LOG(WARN,"fail to load schema from file [fname:%s]", param.schema_fname_);
    err = OB_SCHEMA_ERROR;
  }
  if (OB_SUCCESS == err)
  {
    err = check_schema(param);
    if (OB_SUCCESS != err)
    {
      err = OB_SCHEMA_ERROR;
    }
  }
  return err;
}

void init_mock_client(const char *addr, int32_t port, MockClient &client)
{
  ObServer dst_host;
  dst_host.set_ipv4_addr(addr, port);
  client.init(dst_host);
}

int delete_rows(CParam &param, MockClient &client)
{
  int err = OB_SUCCESS;
  ObMutator mutator;
  ObString left_table_name;
  ObString right_table_name;
  ObString column_name;
  ObString left_row_key_str;
  ObString right_row_key_str;
  ObRowkey *rowkey = NULL;
  ObRowkey rowkey_object ;
  ObObj    val_obj;
  union
  {
    left_rowkey_t key_;
    uint32_t int_val_[2];
  }left_rowkey;
  left_table_name.assign(const_cast<char*>(param.left_schema_->get_table_name()),
    static_cast<int32_t>(strlen(param.left_schema_->get_table_name())));
  right_table_name.assign(const_cast<char*>(param.right_schema_->get_table_name()),
    static_cast<int32_t>(strlen(param.right_schema_->get_table_name())));
  ObString *table_name = NULL;
  int64_t each_update_row_num = 3000;
  bool req_sended = true;
  for (int64_t i = param.start_key_; OB_SUCCESS == err && i < param.end_key_; i++)
  {
    if (i % delete_row_interval != 0)
    {
      continue;
    }
    left_rowkey.key_.left_key_.key_val_ = 0;
    left_rowkey.key_.left_key_.key_val_ = static_cast<int32_t>(i << 8);
    left_rowkey.key_.left_key_.type_ = LEFT_TABLE_TYPE;
    left_rowkey.key_.right_key_.key_val_ = 0;
    left_rowkey.key_.right_key_.key_val_ = static_cast<int32_t>(i << 8);
    left_rowkey.key_.right_key_.type_ = RIGHT_TABLE_TYPE;
    for (uint32_t j = 0; j < sizeof(left_rowkey.int_val_)/sizeof(uint32_t); j++)
    {
      left_rowkey.int_val_[j] = htonl(left_rowkey.int_val_[j]);
    }
    left_row_key_str.assign((char*)&left_rowkey, sizeof(left_rowkey_t));
    right_row_key_str.assign((char*)&(left_rowkey.key_.right_key_), sizeof(rowkey_t));
    table_name = &left_table_name;
    rowkey_object = TestRowkeyHelper(left_row_key_str, &allocator_);
    rowkey = &rowkey_object;
    err = mutator.del_row(*table_name,*rowkey);
    if (OB_SUCCESS == err)
    {
      req_sended = false;
    }
    if (OB_SUCCESS == err && i > 0 && ( i % each_update_row_num == 0 || i == param.end_key_ - 1))
    {
      err = client.ups_apply(mutator,TIMEOUT);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to update ups [err:%d]", err);
      }
      else
      {
        TBSYS_LOG(WARN,"update [i:%ld,param.start_key_:%ld,param.end_key_:%ld]", i,
          param.start_key_, param.end_key_);
        req_sended = true;
        mutator.reset();
      }
    }
  }
  if (!req_sended)
  {
    err = client.ups_apply(mutator,TIMEOUT);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to update ups [err:%d]", err);
    }
    else
    {
      TBSYS_LOG(WARN,"update [param.start_key_:%ld,param.end_key_:%ld]",
        param.start_key_, param.end_key_);
      req_sended = true;
      mutator.reset();
    }
  }
  return err;
}

int update(CParam &param, MockClient &client)
{
  int err = OB_SUCCESS;
  int32_t column_count = 0;
  const ObColumnSchemaV2 *column_beg = param.schema_mgr_->get_table_schema(param.left_table_id_, column_count);
  const ObColumnSchemaV2 *column_end  = column_beg + column_count;
  ObMutator mutator;
  bool update_all = (strcmp(param.operation_, "update_all") == 0);
  ObString left_table_name;
  ObString right_table_name;
  ObString column_name;
  ObString left_row_key_str;
  ObString right_row_key_str;
  ObRowkey *rowkey = NULL;
  ObRowkey rowkey_object;
  ObObj    val_obj;
  union
  {
    left_rowkey_t key_;
    uint32_t int_val_[2];
  }left_rowkey;
  union
  {
    int64_t int_val;
    column_obj_t obj_val;
  }val_union;
  left_table_name.assign(const_cast<char*>(param.left_schema_->get_table_name()),
    static_cast<int32_t>(strlen(param.left_schema_->get_table_name())));
  right_table_name.assign(const_cast<char*>(param.right_schema_->get_table_name()),
    static_cast<int32_t>(strlen(param.right_schema_->get_table_name())));
  ObString *table_name = NULL;
  const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
  const ObColumnSchemaV2 *right_column_info = NULL;
  bool req_sended = false;
  int64_t each_update_row_num = 3000;
  for (int64_t i = param.start_key_; OB_SUCCESS == err && i < param.end_key_; i += param.interval_)
  {
    left_rowkey.key_.left_key_.key_val_ = 0;
    left_rowkey.key_.left_key_.key_val_ = static_cast<int32_t>(i << 8);
    left_rowkey.key_.left_key_.type_ = LEFT_TABLE_TYPE;
    left_rowkey.key_.right_key_.key_val_ = 0;
    left_rowkey.key_.right_key_.key_val_ = static_cast<int32_t>(i << 8);
    left_rowkey.key_.right_key_.type_ = RIGHT_TABLE_TYPE;
    for (uint32_t j = 0; j < sizeof(left_rowkey.int_val_)/sizeof(uint32_t); j++)
    {
      left_rowkey.int_val_[j] = htonl(left_rowkey.int_val_[j]);
    }
    left_row_key_str.assign((char*)&left_rowkey, sizeof(left_rowkey_t));
    right_row_key_str.assign((char*)&(left_rowkey.key_.right_key_), sizeof(rowkey_t));
    column_beg = param.schema_mgr_->get_table_schema(param.left_table_id_, column_count);
    while (OB_SUCCESS == err && column_beg < column_end)
    {
      if (column_beg->get_type() == ObIntType)
      {
        if ((join_info =  column_beg->get_join_info())!= NULL)
        {
          if (!update_all)
          {
            column_beg ++;
            continue;
          }
          table_name = &right_table_name;
          rowkey_object = TestRowkeyHelper(right_row_key_str, &allocator_);
          rowkey = &rowkey_object;
          val_union.obj_val.key_ = left_rowkey.key_.right_key_;
          right_column_info = param.schema_mgr_->get_column_schema(join_info->join_table_, join_info->correlated_column_);
          if (NULL == right_column_info)
          {
            TBSYS_LOG(WARN,"argumet error, cann't find right column info [columnid:%lu]", column_beg->get_id());
            err = OB_INVALID_ARGUMENT;
          }
          else
          {
            column_name.assign(const_cast<char*>(right_column_info->get_name()), static_cast<int32_t>(strlen(right_column_info->get_name())));
          }
        }
        else
        {
          table_name = &left_table_name;
          rowkey_object = TestRowkeyHelper(left_row_key_str);
          rowkey = &rowkey_object;
          val_union.obj_val.key_ = left_rowkey.key_.left_key_;
          column_name.assign(const_cast<char*>(column_beg->get_name()), static_cast<int32_t>(strlen(column_beg->get_name())));
        }
        val_union.obj_val.column_id_ = static_cast<uint8_t>(column_beg->get_id());
        val_union.obj_val.version_ = static_cast<uint8_t>(param.cur_version_);
        val_obj.set_int(val_union.int_val);
        if (OB_SUCCESS == err)
        {
          err = mutator.update(*table_name,*rowkey,column_name,val_obj);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to add muatator [err:%d]", err);
          }
          else
          {
            req_sended = false;
            column_beg ++;
          }
        }
      }
    }
    if (OB_SUCCESS == err && i > 0 && ( i % each_update_row_num == 0 || i == param.end_key_ - 1))
    {
      err = client.ups_apply(mutator,TIMEOUT);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to update ups [err:%d]", err);
      }
      else
      {
        TBSYS_LOG(WARN,"update [i:%ld,param.start_key_:%ld,param.end_key_:%ld]", i,
          param.start_key_, param.end_key_);
        mutator.reset();
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    err = delete_rows(param,client);
  }
  return err;
}

int check_result(CParam &param, ObScanner &result, uint32_t start_key, ObScanParam::Direction direction)
{
  int err = OB_SUCCESS;
  int32_t column_count = 0;
  const ObColumnSchemaV2 *column_beg = param.schema_mgr_->get_table_schema(param.left_table_id_, column_count);
  const ObColumnSchemaV2 *column_end  = column_beg + column_count;
  const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
  union
  {
    left_rowkey_t key_;
    uint32_t int_val_[2];
  }left_rowkey, first_rowkey;
  ObRowkey left_rowkey_str;
  ObString column_name;
  ObCellInfo *cur_cell = NULL;
  bool is_row_changed = false;
  ObObj val_obj;
  int64_t row_num = 0;
  union
  {
    int64_t int_val;
    column_obj_t obj_val;
  }val_union;
  err = result.next_cell();
  if (OB_SUCCESS != err && OB_ITER_END != err)
  {
    TBSYS_LOG(WARN,"fail to next cell from result [err:%d]", err);
  }
  if (OB_SUCCESS == err && ObScanParam::BACKWARD == direction)
  {
    ObCellInfo *first_cell = NULL;
    err = result.get_cell(&first_cell);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to get first cell from ObScanner [err:%d]", err);
    }
    else
    {
      memcpy(&first_rowkey, first_cell->row_key_.ptr(), first_cell->row_key_.length());
      for (uint32_t i = 0; i < sizeof(first_rowkey.int_val_)/sizeof(uint32_t); i++)
      {
        first_rowkey.int_val_[i] = ntohl(first_rowkey.int_val_[i]);
      }
      start_key = first_rowkey.key_.left_key_.val_;
    }
  }
  while (OB_SUCCESS == err)
  {
    if (start_key%delete_row_interval == 0)
    {
      start_key ++;
      continue;
    }
    left_rowkey.key_.left_key_.key_val_ = 0;
    left_rowkey.key_.left_key_.key_val_ = static_cast<int32_t>(start_key << 8);
    left_rowkey.key_.left_key_.type_ = LEFT_TABLE_TYPE;
    left_rowkey.key_.right_key_.key_val_ = 0;
    left_rowkey.key_.right_key_.key_val_ = static_cast<int32_t>(start_key << 8);
    left_rowkey.key_.right_key_.type_ = RIGHT_TABLE_TYPE;
    for (uint32_t i = 0; i < sizeof(left_rowkey.int_val_)/sizeof(uint32_t); i++)
    {
      left_rowkey.int_val_[i] = htonl(left_rowkey.int_val_[i]);
    }
    left_rowkey_str = make_rowkey((char*)&left_rowkey, sizeof(left_rowkey_t), &allocator_);
    for (column_beg = param.schema_mgr_->get_table_schema(param.left_table_id_, column_count);
      column_beg < column_end && OB_SUCCESS == err; column_beg ++)
    {
      if (column_beg->get_type() == ObIntType)
      {
        column_name.assign(const_cast<char*>(column_beg->get_name()), static_cast<int32_t>(strlen(column_beg->get_name())));
        if ((join_info =  column_beg->get_join_info()) != NULL)
        {
          val_union.obj_val.key_ = left_rowkey.key_.right_key_;
        }
        else
        {
          val_union.obj_val.key_ = left_rowkey.key_.left_key_;
        }
        val_union.obj_val.column_id_ = static_cast<uint8_t>(column_beg->get_id());
        val_union.obj_val.version_ = static_cast<uint8_t>(param.cur_version_);
        val_obj.set_int(val_union.int_val);
        err = result.get_cell(&cur_cell,&is_row_changed);
        if (OB_SUCCESS == err)
        {
          if (cur_cell->column_name_ != column_name
            || cur_cell->table_name_ != param.left_table_
            || cur_cell->row_key_ != left_rowkey_str
            || cur_cell->value_ != val_obj)
          {
            TBSYS_LOG(WARN,"unexpected error");
            err = OB_ERR_UNEXPECTED;
          }
        }
        else
        {
          TBSYS_LOG(WARN,"fail to get cell from scanner [err:%d]", err);
        }
        if (OB_SUCCESS == err)
        {
          err = result.next_cell();
          if (OB_SUCCESS != err && OB_ITER_END != err)
          {
            TBSYS_LOG(WARN,"fail to next cell from result [err:%d]", err);
          }
          if (OB_ITER_END == err && column_beg + 1 != column_end)
          {
            TBSYS_LOG(WARN,"result err, not row width");
            err = OB_ERR_UNEXPECTED;
          }
        }
      }
    }
    if (OB_SUCCESS == err)
    {
      start_key ++;
      row_num ++;
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  else
  {
    TBSYS_LOG(WARN,"unexpected error");
  }
  return err;
}

int scan(CParam &param, MockClient &client)
{
  int err = OB_SUCCESS;
  int32_t column_count = 0;
  const ObColumnSchemaV2 *column_beg = param.schema_mgr_->get_table_schema(param.left_table_id_, column_count);
  const ObColumnSchemaV2 *column_end  = column_beg + column_count;
  ObScanParam scan_param;
  ObString table_name;
  ObString column_name;
  ObString start_key_str;
  ObString end_key_str;
  ObObj    val_obj;
  union
  {
    left_rowkey_t key_;
    uint32_t int_val_[2];
  }start_key, end_key;
  ObNewRange       range;
  ObVersionRange version_range;
  version_range.border_flag_.set_min_value();
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  version_range.border_flag_.set_inclusive_end();
  table_name.assign(const_cast<char*>(param.left_schema_->get_table_name()),
    static_cast<int32_t>(strlen(param.left_schema_->get_table_name())));
  start_key.key_.left_key_.key_val_ = 0;
  start_key.key_.left_key_.key_val_ = static_cast<int32_t>(param.start_key_ << 8);
  start_key.key_.left_key_.type_ = LEFT_TABLE_TYPE;
  start_key.key_.right_key_.key_val_ = 0;
  start_key.key_.right_key_.key_val_ = static_cast<int32_t>(param.start_key_ << 8);
  start_key.key_.right_key_.type_ = RIGHT_TABLE_TYPE;
  for (uint32_t j = 0; j < sizeof(start_key.int_val_)/sizeof(uint32_t); j++)
  {
    start_key.int_val_[j] = htonl(start_key.int_val_[j]);
  }
  end_key_str.assign((char*)&start_key, sizeof(start_key));
  end_key.key_.left_key_.key_val_ = 0;
  end_key.key_.left_key_.key_val_ = static_cast<int32_t>(param.end_key_ << 8);
  end_key.key_.left_key_.type_ = LEFT_TABLE_TYPE;
  end_key.key_.right_key_.key_val_ = 0;
  end_key.key_.right_key_.key_val_ = static_cast<int32_t>(param.end_key_ << 8);
  end_key.key_.right_key_.type_ = RIGHT_TABLE_TYPE;
  for (uint32_t j = 0; j < sizeof(end_key.int_val_)/sizeof(uint32_t); j++)
  {
    end_key.int_val_[j] = htonl(end_key.int_val_[j]);
  }
  end_key_str.assign((char*)&end_key, sizeof(end_key));
  range.start_key_ = TestRowkeyHelper(start_key_str, &allocator_);
  range.end_key_ = TestRowkeyHelper(end_key_str, &allocator_);
  range.table_id_ = OB_INVALID_ID;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  uint32_t cur_key  = static_cast<uint32_t>(param.start_key_);
  scan_param.set(OB_INVALID_ID,table_name,range);
  scan_param.set_version_range(version_range);
  for (;column_beg < column_end && OB_SUCCESS == err; column_beg++)
  {
    if (column_beg->get_type() == ObIntType)
    {
      column_name.assign(const_cast<char*>(column_beg->get_name()),static_cast<int32_t>(strlen(column_beg->get_name())));
      err = scan_param.add_column(column_name);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to add column to scan param, [err:%d]", err);
      }
    }
  }
  ObRowkey last_row_key;
  ObScanner result;
  /// scan forward
  TBSYS_LOG(WARN,"scan forward");
  while (cur_key < param.end_key_ && OB_SUCCESS == err)
  {
    start_key.key_.left_key_.key_val_ = 0;
    start_key.key_.left_key_.key_val_ = static_cast<int32_t>(cur_key << 8);
    start_key.key_.left_key_.type_ = LEFT_TABLE_TYPE;
    start_key.key_.right_key_.key_val_ = 0;
    start_key.key_.right_key_.key_val_ = static_cast<int32_t>(cur_key << 8);
    start_key.key_.right_key_.type_ = RIGHT_TABLE_TYPE;
    for (uint32_t j = 0; j < sizeof(start_key.int_val_)/sizeof(uint32_t); j++)
    {
      start_key.int_val_[j] = htonl(start_key.int_val_[j]);
    }
    start_key_str.assign((char*)&start_key, sizeof(start_key));
    scan_param.set(OB_INVALID_ID,table_name,range);

    err = client.ups_scan(scan_param,result, TIMEOUT);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to scan from server [err:%d]", err);
    }
    else
    {
      err = check_result(param,result,cur_key, ObScanParam::FORWARD);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"check result fail");
      }
      else
      {
        err = result.get_last_row_key(last_row_key);
        if (OB_SUCCESS != err || last_row_key.length() != sizeof(left_rowkey_t))
        {
          TBSYS_LOG(WARN,"fail to get last key from scanner [err:%d,last_row_key.length():%ld]", err,
            last_row_key.length());
        }
        else
        {
          left_rowkey_t *last_left_key = (left_rowkey_t*)last_row_key.ptr();
          uint32_t next_key_val;
          memcpy(&next_key_val, &(last_left_key->left_key_), sizeof(last_left_key->left_key_));
          next_key_val =  ntohl(next_key_val);
          cur_key = ((rowkey_t*)&next_key_val)->val_ + 1;
        }
      }
    }
    TBSYS_LOG(WARN,"scan one round [cur_key:%d,param.start_key_:%ld,param.end_key_:%ld]", cur_key,
      param.start_key_, param.end_key_);
  }
  /// scan backward
  TBSYS_LOG(WARN,"scan backward");
  ObRowkey first_row_key;
  scan_param.set_scan_direction(ObScanParam::BACKWARD);
  start_key.key_.left_key_.key_val_ = 0;
  start_key.key_.left_key_.key_val_ = static_cast<int32_t>(param.start_key_ << 8);
  start_key.key_.left_key_.type_ = LEFT_TABLE_TYPE;
  start_key.key_.right_key_.key_val_ = 0;
  start_key.key_.right_key_.key_val_ = static_cast<int32_t>(param.start_key_ << 8);
  start_key.key_.right_key_.type_ = RIGHT_TABLE_TYPE;
  for (uint32_t j = 0; j < sizeof(start_key.int_val_)/sizeof(uint32_t); j++)
  {
    start_key.int_val_[j] = htonl(start_key.int_val_[j]);
  }
  start_key_str.assign((char*)&start_key, sizeof(start_key));
  cur_key = static_cast<uint32_t>(param.end_key_);
  while (cur_key > param.start_key_ && OB_SUCCESS == err)
  {
    end_key.key_.left_key_.key_val_ = 0;
    end_key.key_.left_key_.key_val_ = static_cast<int32_t>(cur_key << 8);
    end_key.key_.left_key_.type_ = LEFT_TABLE_TYPE;
    end_key.key_.right_key_.key_val_ = 0;
    end_key.key_.right_key_.key_val_ = static_cast<int32_t>(cur_key << 8);
    end_key.key_.right_key_.type_ = RIGHT_TABLE_TYPE;
    for (uint32_t j = 0; j < sizeof(end_key.int_val_)/sizeof(uint32_t); j++)
    {
      end_key.int_val_[j] = htonl(end_key.int_val_[j]);
    }
    end_key_str.assign((char*)&end_key, sizeof(end_key));
    scan_param.set(OB_INVALID_ID,table_name,range);
    static const int32_t backward_scan_count = 2048;
    scan_param.set_limit_info(0,backward_scan_count);

    err = client.ups_scan(scan_param,result, TIMEOUT);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to scan from server [err:%d]", err);
    }
    else
    {
      err = check_result(param,result,cur_key - 1, ObScanParam::BACKWARD);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"check result fail");
      }
      if (OB_SUCCESS == err)
      {
        err = result.get_last_row_key(last_row_key);
        if (OB_SUCCESS != err || last_row_key.length() != sizeof(left_rowkey_t))
        {
          TBSYS_LOG(WARN,"fail to get last key from scanner [err:%d,last_row_key.length():%ld]", err,
            last_row_key.length());
        }
        else
        {
          left_rowkey_t *last_left_key = (left_rowkey_t*)last_row_key.ptr();
          uint32_t last_key_val;
          memcpy(&last_key_val, &(last_left_key->left_key_), sizeof(last_left_key->left_key_));
          last_key_val =  ntohl(last_key_val);
          if (cur_key > 0)
          {
            cur_key --;
          }
          if (cur_key%delete_row_interval == 0 && cur_key > 0)
          {
            cur_key --;
          }
          if (cur_key % delete_row_interval != 0 && cur_key != ((rowkey_t*)&last_key_val)->val_)
          {
            TBSYS_LOG(WARN,"ms error [expect_last_rowkey:%u,real:%u]", cur_key,((rowkey_t*)&last_key_val)->val_);
            err = OB_ERR_UNEXPECTED;
          }
        }
      }
      if (OB_SUCCESS == err)
      {
        err = result.reset_iter();
        if (OB_SUCCESS == err)
        {
          ObCellInfo *tmp_cell = NULL;
          err = result.get_cell(&tmp_cell);
          if (OB_SUCCESS == err)
          {
            first_row_key = tmp_cell->row_key_;
            left_rowkey_t *first_left_key = (left_rowkey_t*)first_row_key.ptr();
            uint32_t next_key_val;
            memcpy(&next_key_val, &(first_left_key->left_key_), sizeof(first_left_key->left_key_));
            next_key_val =  ntohl(next_key_val);
            cur_key = ((rowkey_t*)&next_key_val)->val_;
            if (0 == cur_key)
            {
              break;
            }
          }
          else
          {
            TBSYS_LOG(WARN,"fail to get first cell from result [err:%d]", err);
          }
        }
        else
        {
          TBSYS_LOG(WARN,"fail to reset iterator of result [err:%d]", err);
        }
      }
    }
    TBSYS_LOG(WARN,"scan one round [cur_key:%d,param.start_key_:%ld,param.end_key_:%ld]", cur_key,
      param.start_key_, param.end_key_);
    if (cur_key == 1)
    {
      break;
    }
  }
  /// check count
  int64_t expect_count = 0;
  for (cur_key = static_cast<int32_t>(param.start_key_); cur_key < param.end_key_; cur_key ++)
  {
    if (cur_key % delete_row_interval != 0)
    {
      expect_count ++;
    }
  }
  if (OB_SUCCESS == err)
  {
    cur_key = static_cast<int32_t>(param.start_key_);
    start_key.key_.left_key_.key_val_ = 0;
    start_key.key_.left_key_.key_val_ = static_cast<int32_t>(cur_key << 8);
    start_key.key_.left_key_.type_ = LEFT_TABLE_TYPE;
    start_key.key_.right_key_.key_val_ = 0;
    start_key.key_.right_key_.key_val_ = static_cast<int32_t>(cur_key << 8);
    start_key.key_.right_key_.type_ = RIGHT_TABLE_TYPE;
    for (uint32_t j = 0; j < sizeof(start_key.int_val_)/sizeof(uint32_t); j++)
    {
      start_key.int_val_[j] = htonl(start_key.int_val_[j]);
    }
    start_key_str.assign((char*)&start_key, sizeof(start_key));
    cur_key = static_cast<int32_t>(param.end_key_);
    end_key.key_.left_key_.key_val_ = 0;
    end_key.key_.left_key_.key_val_ = static_cast<int32_t>(cur_key << 8);
    end_key.key_.left_key_.type_ = LEFT_TABLE_TYPE;
    end_key.key_.right_key_.key_val_ = 0;
    end_key.key_.right_key_.key_val_ = static_cast<int32_t>(cur_key << 8);
    end_key.key_.right_key_.type_ = RIGHT_TABLE_TYPE;
    for (uint32_t j = 0; j < sizeof(end_key.int_val_)/sizeof(uint32_t); j++)
    {
      end_key.int_val_[j] = htonl(end_key.int_val_[j]);
    }
    end_key_str.assign((char*)&end_key, sizeof(end_key));

    scan_param.reset();
    scan_param.set_scan_direction(ObScanParam::FORWARD);
    scan_param.set(OB_INVALID_ID,table_name,range);
    scan_param.add_column(ObGroupByParam::COUNT_ROWS_COLUMN_NAME);
    ObString as_column_name;
    as_column_name.assign(const_cast<char*>("count"),static_cast<int32_t>(strlen("count")));
    err = scan_param.get_group_by_param().add_aggregate_column(ObGroupByParam::COUNT_ROWS_COLUMN_NAME,as_column_name,COUNT);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to add aggregate column [err:%d]", err);
    }
    ObString filter_expr;
    filter_expr.assign(const_cast<char*>("`count` > 1"), static_cast<int32_t>(strlen("`count` > 1")));
    if((OB_SUCCESS == err) && (OB_SUCCESS != (err = scan_param.get_group_by_param().add_having_cond(filter_expr))))
    {
      TBSYS_LOG(WARN,"fail to add having condition [err:%d]", err);
    }
    int64_t time_us = get_cur_time_us();
    if (OB_SUCCESS == err)
    {
      err = client.ups_scan(scan_param,result, TIMEOUT);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to scan server [err:%d]", err);
      }
      else
      {
        time_us = get_cur_time_us() - time_us;
      }
    }
    if (OB_SUCCESS == err)
    {
      int64_t cell_num = 0;
      ObCellInfo *cur_cell = NULL;
      bool is_row_changed = false;
      int64_t count = 0;
      while (OB_SUCCESS == err)
      {
        err = result.next_cell();
        if (OB_SUCCESS != err && OB_ITER_END != err)
        {
          TBSYS_LOG(WARN,"fail to get next cell [err:%d]", err);
        }
        if (OB_SUCCESS == err)
        {
          err = result.get_cell(&cur_cell,&is_row_changed);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to get cell [err:%d]", err);
          }
          else
          {
            cell_num ++;
            if (cur_cell->value_.get_int(count) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"fail to get int val from result");
              err = OB_ERR_UNEXPECTED;
            }
            else
            {
              if (count != expect_count)
              {
                TBSYS_LOG(WARN,"count result error, [count:%ld,expect_count:%ld,param.end_key_:%ld,param.start_key_:%ld]", count,
                  expect_count, param.end_key_, param.start_key_);
                err = OB_ERR_UNEXPECTED;
              }
              else
              {
                TBSYS_LOG(WARN,"count:%ld,time_us:%ld", count,time_us);
              }
            }
          }
        }
      }
      if (cell_num != 1)
      {
        TBSYS_LOG(WARN,"result cell number wrong [cell_num:%ld]", cell_num);
      }
    }
    if (OB_ITER_END == err)
    {
      err = OB_SUCCESS;
    }
  }
  return err;
}

int main(int argc, char **argv)
{
  int err = OB_SUCCESS;
  ob_init_memory_pool();
  CParam param;
  err = parse_cmd_args(argc,argv, param);
  MockClient client;
  if (OB_SUCCESS == err)
  {
    init_mock_client(param.server_addr_, param.server_port_, client);
    if (strcmp(param.operation_,"scan") == 0)
    {
      err = scan(param,client);
    }
    else
    {
      err = update(param,client);
    }
    TBSYS_LOG(WARN,"result:%d", err);
  }
  client.destroy();
  delete param.schema_mgr_;
  return err;
}

