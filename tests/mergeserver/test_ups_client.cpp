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
  fprintf(stderr, "   -t|--table apply update table name\n");
  fprintf(stderr, "   -h|--help print this help info\n");
  fprintf(stderr, "\n");
}

int parse_cmd_args(int argc, char **argv, CParam & param)
{
  int err = OB_SUCCESS;
  int opt = 0;
  memset(&param,0x00,sizeof(param));
  const char* opt_string = "a:p:c:m:t:s:e:h";
  struct option longopts[] =
  {
    {"addr", 1, NULL, 'a'},
    {"port", 1, NULL, 'p'},
    {"schema", 1, NULL, 'm'},
    {"table", 1, NULL, 't'},
    {"start", 1, NULL, 's'},
    {"count", 1, NULL, 'c'},
    {"end", 1, NULL, 'e'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
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
    ObObj value_obj;
    int64_t column_id = 0;
    char buffer[128];
    int32_t size = 0;
    const ObColumnSchemaV2 * column_info = param.schema_mgr_->get_table_schema(schema->get_table_id(), size);
    for (int64_t i = param.start_key_; i <= param.end_key_; ++i)
    {
      mutator.reset();
      int64_t pos = 0;
      serialization::encode_i64(buffer, sizeof(buffer), pos, i);
      rowkey_str = make_rowkey(buffer, pos, &allocator_);
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
          //value_obj.set_int(1, true);
          value_obj.set_int(((i << 24) | (column_id << 16)) | (param.count_ + 1));
          err = mutator.update(table_name, rowkey_str, column_str, value_obj);
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

      if (OB_SUCCESS == err)
      {
        err = client.send_request(OB_WRITE, mutator, TIMEOUT);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "update failed:table[%.*s], rowkey[%ld], err[%d]", 
              table_name.length(), table_name.ptr(), i, err);
        }
        else
        {
          TBSYS_LOG(INFO, "update succ:table[%.*s], rowkey[%ld]",
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

