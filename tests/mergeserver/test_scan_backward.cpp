/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_ms_client.cpp,v 0.1 2011/01/07 16:55:10 xielun Exp $
 *
 * Authors:
 *     - some work details if you want
 *
 */

#include <getopt.h>
#include <string>
#include <unistd.h>
#include "../updateserver/mock_client.h"
#include "../updateserver/test_utils.h"

#include "ob_ms_rpc_proxy.h"
#include "ob_read_param_modifier.h"
#include "common/ob_range.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
#include "../common/test_rowkey_helper.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

const int64_t TIMEOUT =  100000000L;
static CharArena allocator_;

struct CParam
{
  char *server_addr_;
  int32_t server_port_;
  char *schema_fname_;
  char *table_name_;
  int64_t add_count_;
  int64_t start_key_;
  int64_t end_key_;
  bool backward_;
  oceanbase::common::ObSchemaManagerV2 *schema_mgr_;
};


void print_usage()
{
  fprintf(stderr, "scan_test[OPTION]\n");
  fprintf(stderr, "   -a|--addr server address\n");
  fprintf(stderr, "   -p|--port server port\n");
  fprintf(stderr, "   -b|--scan backward\n");
  fprintf(stderr, "   -m|--schema schema file\n");
  fprintf(stderr, "   -s|--start start rowkey must int type\n");
  fprintf(stderr, "   -e|--end end rowkey must int type\n");
  fprintf(stderr, "   -t|--table check join table name\n");
  fprintf(stderr, "   -h|--help print this help info\n");
  fprintf(stderr, "\n");
}

int parse_cmd_args(int argc, char **argv, CParam & param)
{
  int err = OB_SUCCESS;
  int opt = 0;
  const char * opt_string = "a:p:c:m:t:s:e:bh";
  struct option longopts[] =
  {
    {"addr", 1, NULL, 'a'},
    {"port", 1, NULL, 'p'},
    {"back", 0, NULL, 'b'},
    {"count", 1, NULL, 'c'},
    {"schema", 1, NULL, 'm'},
    {"table", 1, NULL, 't'},
    {"start", 1, NULL, 's'},
    {"end", 1, NULL, 'e'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  memset(&param, 0, sizeof(param));
  param.backward_ = false;
  param.start_key_ = -1;
  param.end_key_ = -1;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
      case 'a':
        param.server_addr_ = optarg;
        break;
      case 'b':
        param.backward_ = true;
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
      case 'c':
        param.add_count_ = atol(optarg);
        break;
      case 's':
        param.start_key_ = atol(optarg);
        break;
      case 'e':
        param.end_key_ = atol(optarg);
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
      || 0 > param.add_count_
      || ((param.end_key_ != -1) && (param.start_key_ > param.end_key_))
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


int check_result(const CParam & param, const bool backward, const int64_t count,
    const ObString & table_name, ObScanner & scanner)
{
  UNUSED(backward);
  int err = OB_SUCCESS;
  int64_t row_key = 0;
  int64_t value = 0;
  int64_t column_id = 0;
  ObCellInfo *cur_cell = NULL;
  const ObColumnSchemaV2 * column = NULL;
  bool row_change = false;
  if (true == scanner.is_empty())
  {
    err = OB_ERROR;
    TBSYS_LOG(ERROR, "check read scanner result is empty");
  }
  while ((OB_SUCCESS == err) && (scanner.next_cell() == OB_SUCCESS))
  {
    err = scanner.get_cell(&cur_cell, &row_change);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "get cell failed:ret[%d]", err);
      break;
    }
    else
    {
      if ((cur_cell->value_.get_type() != ObIntType)
          || (cur_cell->table_name_ != table_name))
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
        ObString key = TestRowkeyHelper(cur_cell->row_key_, &allocator_);
        serialization::decode_i64(key.ptr(), key.length(), pos, &row_key);
        if (row_change)
        {
          TBSYS_LOG(INFO, "test scan succ:table[%lu], rowkey[%ld]", column->get_table_id(), row_key);
        }
        cur_cell->value_.get_int(value);
        // TBSYS_LOG(INFO, "check value:value[%ld], rowkey[%ld], column[%lu]", value, row_key, column_id);
        if (value != (((row_key << 24) | ((int64_t)column_id << 16)) | (count + 1)))
        {
          TBSYS_LOG(ERROR, "check value failed:value[%ld], new[%ld], rowkey[%ld], column[%lu], count[%ld]",
              value, (((row_key << 24) | (column_id << 16)) | (count + 1)), row_key, column_id, count);
          err = OB_ERROR;
          break;
        }
      }
    }
  }
  return err;
}

int get_first_row_key(ObScanner & scanner, ObRowkey & rowkey)
{
  scanner.reset_iter();
  ObCellInfo * cell = NULL;
  int ret = scanner.next_cell();
  if (OB_SUCCESS == ret)
  {
    ret = scanner.get_cell(&cell);
    if (OB_SUCCESS == ret)
    {
      rowkey = cell->row_key_;
    }
  }
  else
  {
    // empty scanner result
    ret = OB_SUCCESS;
  }
  return ret;
}

int scan(CParam &param, MockClient &client)
{
  int err = OB_SUCCESS;
  ObString table_name;
  table_name.assign(param.table_name_, static_cast<int32_t>(strlen(param.table_name_)));
  const ObTableSchema * schema = param.schema_mgr_->get_table_schema(table_name);
  if (NULL == schema)
  {
    TBSYS_LOG(ERROR, "check schema failed:id[%s]", param.table_name_);
    err = OB_ERROR;
  }
  else
  {
    ObRowkey start_rowkey_str;
    ObRowkey end_rowkey_str;
    ObString column_str;
    ObObj value_obj;
    ObScanner scanner;
    char buffer[128];
    char buffer_end[128];
    ObScanParam scan_param;
    ObNewRange range;
    int64_t pos = 0;
    if (param.start_key_ > 0)
    {
      serialization::encode_i64(buffer, sizeof(buffer), pos, param.start_key_);
      start_rowkey_str = make_rowkey(buffer, pos, &allocator_);
      range.start_key_ = start_rowkey_str;
    }
    else
    {
      range.start_key_.set_min_row();
    }

    if (param.end_key_ > 0)
    {
      pos = 0;
      serialization::encode_i64(buffer_end, sizeof(buffer), pos, param.end_key_);
      end_rowkey_str = make_rowkey(buffer_end, pos, &allocator_);
      range.end_key_ = end_rowkey_str;
    }
    else
    {
      range.end_key_.set_max_row();
    }

    // set borderflag
    if (!range.start_key_.is_min_row())
    {
      range.border_flag_.unset_inclusive_start();
    }
    else
    {
      range.border_flag_.set_inclusive_start();
    }

    if (range.end_key_.is_max_row())
    {
      range.border_flag_.unset_inclusive_end();
    }
    else
    {
      range.border_flag_.set_inclusive_end();
    }

    scan_param.set(OB_INVALID_ID, table_name, range);
    if (param.backward_)
    {
      scan_param.set_scan_direction(ObScanParam::BACKWARD);
    }

    ObVersionRange version_range;
    version_range.border_flag_.set_min_value();
    version_range.border_flag_.set_max_value();
    scan_param.set_version_range(version_range);

    int32_t size = 0;
    const ObColumnSchemaV2 * column_info = param.schema_mgr_->get_table_schema(schema->get_table_id(), size);
    for (int32_t j = 0; j < size; ++j)
    {
      if (NULL == column_info)
      {
        TBSYS_LOG(ERROR, "check column info failed:table[%lu]", schema->get_table_id());
        err = OB_ERROR;
        break;
      }
      column_str.assign(const_cast<char *>(column_info->get_name()), static_cast<int32_t>(strlen(column_info->get_name())));
      if (column_info->get_type() == ObIntType)
      {
        column_str.assign(const_cast<char *>(column_info->get_name()), static_cast<int32_t>(strlen(column_info->get_name())));
        err = scan_param.add_column(column_str);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "add column failed:table[%lu], ret[%d]", schema->get_table_id(), err);
          break;
        }
      }
      ++column_info;
    }

    ObScanParam new_param;
    new_param.safe_copy(scan_param);
    ObRowkey max_key;
    while (err == OB_SUCCESS)
    {
      err = client.ups_scan(new_param, scanner, TIMEOUT);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "check scan failed:table[%lu], ret[%d]", schema->get_table_id(), err);
      }
      else
      {
        err = check_result(param, param.backward_, param.add_count_, table_name, scanner);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "check result failed:table[%lu], ret[%d]", schema->get_table_id(), err);
        }
        else
        {
          TBSYS_LOG(INFO, "check result succ:table[%lu]", schema->get_table_id());
        }
      }

      if (OB_SUCCESS == err)
      {
        if (true == param.backward_)
        {
          err = get_first_row_key(scanner, max_key);
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "check get first rowkey failed:ret[%d]", err);
          }
        }
        else
        {
          err = scanner.get_last_row_key(max_key);
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "check get last rowkey failed:ret[%d]", err);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        ObNewRange rang = *new_param.get_range();
        if (true == param.backward_)
        {
          range.end_key_ = max_key;
          range.border_flag_.unset_inclusive_end();
        }
        else
        {
          range.start_key_ = max_key;
          range.border_flag_.unset_inclusive_start();
        }
        new_param.set(OB_INVALID_ID, table_name, range);
      }

      if (OB_SUCCESS == err)
      {
        bool is_fullfill = false;
        int64_t fullfilled_row_num = 0;
        err = scanner.get_is_req_fullfilled(is_fullfill, fullfilled_row_num);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get fullfilled status failed:ret[%d]", err);
        }
        else if (is_fullfill)
        {
          break;
        }
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
    err = scan(param, client);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check get failed:ret[%d]", err);
    }
  }
  client.destroy();
  delete param.schema_mgr_;
  return err;
}
