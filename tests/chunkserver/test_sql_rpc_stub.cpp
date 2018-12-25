/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_new_scan.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_base_client.h"
#include "chunkserver/ob_sql_rpc_stub.h"
#include "common/ob_client_helper.h"

using namespace oceanbase;
using namespace common;
using namespace chunkserver;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

#define DEF_STR(name, value) \
  const char *name##_ptr = value; \
  ObString name; \
  name.assign_ptr(const_cast<char *>(name##_ptr), (int32_t)strlen(name##_ptr));

class ObSqlRpcStubTest: public ::testing::Test
{
  public:
    ObSqlRpcStubTest();
    virtual ~ObSqlRpcStubTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSqlRpcStubTest(const ObSqlRpcStubTest &other);
    ObSqlRpcStubTest& operator=(const ObSqlRpcStubTest &other);
  protected:
    // data members
};

ObSqlRpcStubTest::ObSqlRpcStubTest()
{
}

ObSqlRpcStubTest::~ObSqlRpcStubTest()
{
}

void ObSqlRpcStubTest::SetUp()
{
}

void ObSqlRpcStubTest::TearDown()
{
}

TEST_F(ObSqlRpcStubTest, get)
{
  ObServer server;
  server.set_ipv4_addr("10.232.36.197", 2700);

  ObBaseClient client;
  client.initialize(server);

  common::ThreadSpecificBuffer rpc_buffer;

  ObSqlRpcStub rpc_stub;
  rpc_stub.init(&rpc_buffer, &client.get_client_mgr());

  ObGetParam get_param;

  ObVersionRange version_range;
  version_range.border_flag_.set_inclusive_start();
  version_range.border_flag_.set_max_value();
  ObVersion version(1);
  version_range.start_version_ = version;
  
  get_param.set_version_range(version_range);

  DEF_STR(table_name, "collect_info");
  DEF_STR(column_name, "field4");

  ObObj value;
  value.set_int(32);

  char rowkey_buf[100];
  ObString rowkey;
  sprintf(rowkey_buf, "rowkey_%05d", 101);
  rowkey.assign_ptr(rowkey_buf, (int32_t)strlen(rowkey_buf));

  ObMutator mutator;
  mutator.insert(table_name, rowkey, column_name, value);

  ObServer root_server;
  root_server.set_ipv4_addr("10.232.36.197", 2500);

  ObClientHelper client_helper;
  client_helper.init(&client.get_client_mgr(), &rpc_buffer, root_server, 2000 * 1000);
  OK(client_helper.apply(server, mutator));


  ObCellInfo cell;
  cell.row_key_ = rowkey;
  cell.table_id_ = 1001;
  cell.column_id_ = 4;

  get_param.add_cell(cell);
  
  ObNewScanner new_scanner;

  rpc_stub.get(2000 * 1000, server, get_param, new_scanner);

}

TEST_F(ObSqlRpcStubTest, scan)
{
  ObServer server;
  server.set_ipv4_addr("10.232.36.197", 2700);

  ObBaseClient client;
  client.initialize(server);

  common::ThreadSpecificBuffer rpc_buffer;

  ObSqlRpcStub rpc_stub;
  rpc_stub.init(&rpc_buffer, &client.get_client_mgr());

  ObScanParam scan_param;
  ObVersionRange version_range;
  version_range.border_flag_.set_inclusive_start();
  version_range.border_flag_.set_max_value();
  ObVersion version(1);
  version_range.start_version_ = version;


  DEF_STR(table_name, "collect_info");
  DEF_STR(column_name, "field4");

  ObObj value;
  value.set_int(32);

  char rowkey_buf[100];
  ObString rowkey;
  sprintf(rowkey_buf, "rowkey_%05d", 101);
  rowkey.assign_ptr(rowkey_buf, (int32_t)strlen(rowkey_buf));

  ObMutator mutator;
  mutator.insert(table_name, rowkey, column_name, value);

  ObServer root_server;
  root_server.set_ipv4_addr("10.232.36.197", 2500);

  ObClientHelper client_helper;
  client_helper.init(&client.get_client_mgr(), &rpc_buffer, root_server, 2000 * 1000);
  OK(client_helper.apply(server, mutator));


  char start_key[100];
  char end_key[100];

  sprintf(start_key, "rowkey_%05d", 100);
  sprintf(end_key, "rowkey_%05d", 123);

  ObRange range;
  range.start_key_.assign_ptr(start_key, (int32_t)strlen(start_key));
  range.end_key_.assign_ptr(end_key, (int32_t)strlen(end_key));

  ObString table_name2;

  scan_param.set(1001, table_name2, range);
  scan_param.set_version_range(version_range);

  ObNewScanner new_scanner;

  rpc_stub.scan(2000 * 1000, server, scan_param, new_scanner);

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

