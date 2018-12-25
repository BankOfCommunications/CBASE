/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_mutate_client.cpp,v 0.1 2011/12/23 16:55:10 xielun Exp $
 *
 * Authors:
 *     - some work details if you want
 *
 */

#include <getopt.h>
#include <string>
#include <unistd.h>
#include "mock_client.h"
#include "common/utility.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
#include "../common/test_rowkey_helper.h"

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
const int64_t TIMEOUT =  10000000L;
static CharArena allocator_;

struct CParam
{
  char *server_addr_;
  int32_t server_port_;
  char *schema_fname_;
  char *table_name_;
  int64_t start_key_;
  int64_t end_key_;
  int64_t count_;
  int64_t fail_base_;
  oceanbase::common::ObSchemaManagerV2 *schema_mgr_;
};


void print_usage()
{
  fprintf(stderr, "\n");
  fprintf(stderr, "batch_client [OPTION]\n");
  fprintf(stderr, "   -a|--addr server address\n");
  fprintf(stderr, "   -p|--port server port\n");
  fprintf(stderr, "   -m|--schema schema file\n");
  fprintf(stderr, "   -c|--count add count\n");
  fprintf(stderr, "   -s|--start start rowkey must int type\n");
  fprintf(stderr, "   -e|--end end rowkey must int type\n");
  fprintf(stderr, "   -f|--mod base for update faile\n");
  fprintf(stderr, "   -t|--table apply update table name\n");
  fprintf(stderr, "   -h|--help print this help info\n");
  fprintf(stderr, "\n");
}

int parse_cmd_args(int argc, char **argv, CParam & param)
{
  int err = OB_SUCCESS;
  int opt = 0;
  memset(&param,0x00,sizeof(param));
  const char* opt_string = "a:p:c:m:t:s:e:f:h";
  struct option longopts[] =
  {
    {"addr", 1, NULL, 'a'},
    {"port", 1, NULL, 'p'},
    {"schema", 1, NULL, 'm'},
    {"table", 1, NULL, 't'},
    {"start", 1, NULL, 's'},
    {"count", 1, NULL, 'c'},
    {"end", 1, NULL, 'e'},
    {"fail", 1, NULL, 'f'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  param.fail_base_ = 10000;
  param.count_ = 1100;
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
      case 'm':
        param.schema_fname_ = optarg;
        break;
      case 't':
        param.table_name_ = optarg;
        break;
      case 's':
        param.start_key_ = atol(optarg);
        break;
      case 'e':
        param.end_key_ = atol(optarg);
        break;
      case 'f':
        param.fail_base_ = atol(optarg);
        break;
      case 'c':
        param.count_ = atol(optarg);
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
      || NULL == param.table_name_
      || 0 > param.count_
      || 0 > param.start_key_
      || 0 > param.fail_base_
      || param.start_key_ > param.end_key_
     )
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
  return err;
}

int check_result(const CParam & param, const int64_t count,
    const ObString & table_name, const ObRowkey& rowkey, ObScanner & scanner)
{
  int err = OB_SUCCESS;
  int64_t row_key = 0;
  uint64_t column_id = 0;
  int64_t value = 0;
  const ObColumnSchemaV2 * column = NULL;
  ObCellInfo *cur_cell = NULL;
  if (true == scanner.is_empty())
  {
    err = OB_ERROR;
    TBSYS_LOG(ERROR, "check return scanner result is empty");
  }
  while ((OB_SUCCESS == err) && (scanner.next_cell() == OB_SUCCESS))
  {
    err = scanner.get_cell(&cur_cell);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "get cell failed:ret[%d]", err);
      break;
    }
    else
    {
      if ((cur_cell->value_.get_type() != ObIntType)
          || (cur_cell->table_name_ != table_name)
          || (cur_cell->row_key_ != rowkey))
      {
        TBSYS_LOG(ERROR, "check table name rowkey or type failed");
        //hex_dump(cur_cell->row_key_.ptr(), cur_cell->row_key_.length());
        cur_cell->value_.dump();
        err = OB_ERROR;
        break;
      }
      else
      {
        column = param.schema_mgr_->get_column_schema(table_name, cur_cell->column_name_);
        if (NULL == column)
        {
          TBSYS_LOG(ERROR, "find column info failed");
          err = OB_ERROR;
          break;
        }
        column_id = column->get_id();
        int64_t pos = 0;
        ObString str_rowkey = TestRowkeyHelper(cur_cell->row_key_);
        serialization::decode_i64(str_rowkey.ptr(), str_rowkey.length(), pos, &row_key);
        cur_cell->value_.get_int(value);
        // TBSYS_LOG(INFO, "check value:value[%ld], rowkey[%ld], column[%lu]", value, row_key, column_id);
        if ((uint64_t)value != (((row_key << 24) | (column_id << 16)) | (count + 1)))
        {
          TBSYS_LOG(ERROR, "check value failed:cell[%ld], check[%ld], rowkey[%ld], column[%lu], count[%ld]",
              value, (((row_key << 24) | (column_id << 16)) | (count + 1)), row_key, column_id, count);
          err = OB_ERROR;
          break;
        }
      }
    }
  }
  return err;
}

int apply(CParam &param, MockClient &client)
{
  int err = OB_SUCCESS;
  ObString table_name;
  table_name.assign(param.table_name_, static_cast<int32_t>(strlen(param.table_name_)));
  const ObTableSchema * schema = param.schema_mgr_->get_table_schema(table_name);
  if (NULL == schema)
  {
    TBSYS_LOG(ERROR, "fail to find table:name[%s]", param.table_name_);
    err = OB_ERROR;
  }
  else
  {
    ObRowkey rowkey_str;
    ObString column_str;
    ObMutator mutator;
    ObScanner scanner;
    ObObj value_obj;
    int64_t column_id = 0;
    char buffer[128];
    int32_t size = 0;
    ObUpdateCondition cond;
    const ObColumnSchemaV2 * column_info = param.schema_mgr_->get_table_schema(schema->get_table_id(), size);
    for (int64_t i = param.start_key_; i <= param.end_key_; ++i)
    {
      scanner.reset();
      mutator.reset();
      int64_t pos = 0;
      serialization::encode_i64(buffer, sizeof(buffer), pos, i);
      rowkey_str = make_rowkey(buffer, pos);
      const ObColumnSchemaV2 * temp_column = column_info;
      for (int32_t j = 0; j < size; ++j)
      {
        if (NULL == temp_column)
        {
          TBSYS_LOG(ERROR, "check column info failed:table[%lu], rowkey[%lu]", schema->get_table_id(), i);
          err = OB_ERROR;
          break;
        }
        column_id = temp_column->get_id();
        column_str.assign(const_cast<char *>(temp_column->get_name()), static_cast<int32_t>(strlen(temp_column->get_name())));
        if (temp_column->get_type() == ObIntType)
        {
          value_obj.set_int(((i << 24) | (column_id << 16)) | (param.count_ + 1));
          // need return
          err = mutator.update(table_name, rowkey_str, column_str, value_obj, static_cast<int32_t>((i + j)% 2));
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "update table:%.*s rowkey:%ld column:%.*s err:%d",
                table_name.length(), table_name.ptr(), i,
                column_str.length(), column_str.ptr(), err);
            break;
          }
        }
        ++temp_column;
      }
      // condition failed
      if ((i != 0) && (0 == i % param.fail_base_))
      {
        err = cond.add_cond(table_name, rowkey_str, false);
        value_obj.set_int(-1);
        err = cond.add_cond(table_name, rowkey_str, column_str, LT, value_obj);
      }
      else
      {
        err = cond.add_cond(table_name, rowkey_str, true);
        value_obj.set_int(-1);
        err = cond.add_cond(table_name, rowkey_str, column_str, GT, value_obj);
      }

      if (OB_SUCCESS == err)
      {
        err = client.send_request(OB_MS_MUTATE, mutator, scanner, TIMEOUT);
        if ((i != 0) && (0 == i % param.fail_base_))
        {
          if (err != OB_SUCCESS)
          {
            err = OB_SUCCESS;
            continue;
          }
          else
          {
            err = OB_ERROR;
            TBSYS_LOG(ERROR, "update failed:talble[%.*s] rowkey[%ld] err[%d]",
                table_name.length(), table_name.ptr(), i, err);
          }
        }
        else
        {
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "update failed:talble[%.*s] rowkey[%ld] err[%d]",
                table_name.length(), table_name.ptr(), i, err);
          }
        }
      }
      if (OB_SUCCESS == err)
      {
        // check return value
        err = check_result(param, param.count_, table_name, rowkey_str, scanner);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "check result failed:table[%.*s], rowkey[%ld], ret[%d]",
              table_name.length(), table_name.ptr(), i, err);
          break;
        }
        else
        {
          TBSYS_LOG(INFO, "check reusult succ:table[%.*s], rowkey[%ld]",
              table_name.length(), table_name.ptr(), i);
        }
      }
      if (err != OB_SUCCESS)
      {
        break;
      }
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
    err = apply(param, client);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check apply mutation failed:ret[%d]", err);
    }
  }
  client.destroy();
  delete param.schema_mgr_;
  return err;
}
