/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_apply_with_condition.cpp,v 0.1 2011/07/15 17:34:54 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include <iostream>
#include <sstream>
#include <algorithm>
#include <gtest/gtest.h>
#include "tblog.h"
#include "test_helper.h"
#include "test_init.h"
#include "updateserver/ob_ups_table_mgr.h"
#include "updateserver/ob_ups_cache.h"
#include "updateserver/ob_client_wrapper.h"
#include "updateserver/ob_update_server_main.h"
#include "mock_server.h"
#include "mock_root_server2.h"
#include "mock_update_server2.h"
#include "mock_chunk_server2.h"
#include "test_ups_table_mgr_helper.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::updateserver;
using namespace oceanbase::updateserver::test;

namespace oceanbase
{
namespace tests
{
namespace updateserver
{

  static tbnet::Transport transport;
  static MockRootServer root;
  static MockChunkServer chunk;

  static ThreadSpecificBuffer buffer;
  static ObPacketFactory factory;
  static tbnet::DefaultPacketStreamer streamer; 

  static tbsys::CThread root_server_thread;
  static MockServerRunner* test_root_server;

  static tbsys::CThread chunk_server_thread;
  static MockServerRunner* test_chunk_server;

  static tbsys::CConfig c1;
  static ObSchemaManagerV2 schema_manager;
  static CommonSchemaManagerWrapper ups_schema_mgr;

class TestApplyWithCondition : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    test_root_server = new MockServerRunner(root);
    test_chunk_server = new MockServerRunner(chunk);

    int ret = 0;
    //const char *addr = "localhost";
    int64_t addr = 16777343;
    streamer.setPacketFactory(&factory);
    transport.start();
    ObUpdateServer& update_server = ObUpdateServerMain::get_instance()->get_update_server();
    ObClientManager& client_manager = (ObClientManager&) (update_server.get_client_manager());
    ret = client_manager.initialize(&transport, &streamer);
    ASSERT_EQ(0, ret);

    // start root server
    root_server_thread.start(test_root_server, NULL);

    // start chunk server
    chunk_server_thread.start(test_chunk_server, NULL);
    sleep(2);

    // init ups table mgr
    bool parse_ret = schema_manager.parse_from_file("./test1.ini", c1);
    ASSERT_EQ(true, parse_ret);
    ups_schema_mgr.set_impl(schema_manager);

    ObServer& root_server = (ObServer&) (update_server.get_root_server());
    root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);

    ObUpsCache& ups_cache = update_server.get_ups_cache();
    ups_cache.init();

    ObServer& self = (ObServer&) (update_server.get_self());
    self.set_ipv4_addr(addr, 2302);

    ObUpsTableMgr& ups_table_mgr = update_server.get_table_mgr();
    ups_table_mgr.init();
    ret = ups_table_mgr.set_schemas(ups_schema_mgr);
    ASSERT_EQ(0, ret);
    ret = ups_table_mgr.sstable_scan_finished(10);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObUpsLogMgr& ups_log_mgr = update_server.get_log_mgr();
    ObSlaveMgr& slave_mgr = update_server.get_slave_mgr();
    ObRoleMgr& role_mgr = update_server.get_role_mgr();
    role_mgr.set_role(ObRoleMgr::MASTER);
    role_mgr.set_state(ObRoleMgr::ACTIVE);
    ObCommonRpcStub rpc_stub;
    ret = slave_mgr.init(0, &rpc_stub, 1000000, 15000000, 12000000);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ups_log_mgr.init("./test_apply_tmp", 64 * 1024L * 1024L, &slave_mgr, &role_mgr, 1);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ups_log_mgr.start_log(1, 1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = update_server.init_client_wrapper();
    ASSERT_EQ(0, ret);
  }

  static void TearDownTestCase()
  {
    root.stop();
    chunk.stop();
    sleep(3);
    transport.stop();
    delete test_root_server;
    delete test_chunk_server;
  }

  virtual void SetUp()
  {
    ObUpsCache& ups_cache = ObUpdateServerMain::get_instance()->get_update_server().get_ups_cache();
    ups_cache.clear();
    ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();
    TestUpsTableMgrHelper test_helper(ups_table_mgr);

    TableMgr& table_mgr = test_helper.get_table_mgr();
    TableItem* active_memtable_item = table_mgr.get_active_memtable();
    MemTable& active_memtable = active_memtable_item->get_memtable();
    active_memtable.clear();
    system("mkdir test_apply_tmp");
  }

  virtual void TearDown()
  {
    system("rm -rf test_apply_tmp");
  }
};

TEST_F(TestApplyWithCondition, test_cond_apply_with_not_exist_key)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  
  ObUpdateCondition& update_condition = mutator.get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));

    ObCondInfo cond_info;
    cond_info.table_name_ = table_name;
    cond_info.row_key_ = row_key;
    cond_info.column_name_ = column_name;
    cond_info.op_type_ = EQ;
    cond_info.value_.set_int(1000 * (i+1));
    ASSERT_TRUE(OB_SUCCESS == update_condition.add_cond(
          cond_info.table_name_, cond_info.row_key_, cond_info.column_name_,
          cond_info.op_type_, cond_info.value_));
  }

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();
  ret = ups_table_mgr.apply(ups_mutator);
  EXPECT_EQ(OB_COND_CHECK_FAIL, ret);
}

TEST_F(TestApplyWithCondition, test_cond_apply_with_unequal_value)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  
  ObUpdateCondition& update_condition = mutator.get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));
  }

  ObString exist_row_key;
  exist_row_key.assign(row_key_str[4], strlen(row_key_str[4])); // "row_key_5";
  ObObj unequal_value;
  unequal_value.set_int(345678);
  ASSERT_TRUE(OB_SUCCESS == update_condition.add_cond(
        table_name, exist_row_key, column_name, EQ, unequal_value));

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();
  ret = ups_table_mgr.apply(ups_mutator);
  EXPECT_EQ(OB_COND_CHECK_FAIL, ret);
}

TEST_F(TestApplyWithCondition, test_cond_apply_with_equal_value)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  
  ObUpdateCondition& update_condition = mutator.get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));
  }

  ObString exist_row_key;
  exist_row_key.assign(row_key_str[4], strlen(row_key_str[4])); // "row_key_5";
  ObObj unequal_value;
  unequal_value.set_int(2237);
  ASSERT_TRUE(OB_SUCCESS == update_condition.add_cond(
        table_name, exist_row_key, column_name, EQ, unequal_value));

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();
  ret = ups_table_mgr.start_transaction();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.apply(ups_mutator);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.end_transaction(false);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestApplyWithCondition, test_cond_apply_with_data_in_memtable)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  
  ObUpsMutator ups_mutator1;
  ObUpdateCondition& update_condition = ups_mutator1.get_mutator().get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));

    ret = update_condition.add_cond(table_name, row_key, column_name,
        EQ, cell[i].value_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();

  // apply mutator
  ret = ups_table_mgr.start_transaction();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.apply(ups_mutator);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.end_transaction(false);
  EXPECT_EQ(OB_SUCCESS, ret);

  // apply mutator1
  ret = ups_table_mgr.start_transaction();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.apply(ups_mutator1);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.end_transaction(false);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestApplyWithCondition, test_row_exist_cond_apply)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObUpsMutator ups_mutator1;
  ObMutator& mutator = ups_mutator.get_mutator();
  ObMutator& mutator1 = ups_mutator1.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  
  ObUpdateCondition& update_condition = mutator.get_update_condition();
  ObUpdateCondition& update_condition1 = mutator1.get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));
    ASSERT_TRUE(OB_SUCCESS == mutator1.add_cell(mutator_cell));

    ObCondInfo cond_info;
    cond_info.table_name_ = table_name;
    cond_info.row_key_ = row_key;
    cond_info.column_name_ = column_name;
    cond_info.op_type_ = EQ;
    cond_info.value_.set_int(1000 * (i+1));
    ASSERT_TRUE(OB_SUCCESS == update_condition.add_cond(
          cond_info.table_name_, cond_info.row_key_, false));
    ASSERT_TRUE(OB_SUCCESS == update_condition1.add_cond(
          cond_info.table_name_, cond_info.row_key_, true));
  }

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();

  ret = ups_table_mgr.apply(ups_mutator);
  EXPECT_EQ(OB_COND_CHECK_FAIL, ret);
  ret = ups_table_mgr.start_transaction();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.apply(ups_mutator1);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.end_transaction(false);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestApplyWithCondition, test_row_exist_cond_apply_with_data_in_memtable)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  
  ObUpsMutator ups_mutator1;
  ObUpdateCondition& update_condition = ups_mutator1.get_mutator().get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "memtable_row_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));

    ret = update_condition.add_cond(table_name, row_key, true);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();

  // apply mutator
  ret = ups_table_mgr.start_transaction();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.apply(ups_mutator);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.end_transaction(false);
  EXPECT_EQ(OB_SUCCESS, ret);

  // apply mutator1
  ret = ups_table_mgr.start_transaction();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.apply(ups_mutator1);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.end_transaction(false);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestApplyWithCondition, test_preprocess_with_exist_cell)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  uint64_t table_id = 1001;
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  uint64_t column_id = 5;
  
  ObUpdateCondition& update_condition = mutator.get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));
  }

  ObString exist_row_key;
  exist_row_key.assign(row_key_str[4], strlen(row_key_str[4])); // "row_key_5";
  ObObj equal_value;
  equal_value.set_int(2237);
  ASSERT_TRUE(OB_SUCCESS == update_condition.add_cond(
        table_name, exist_row_key, column_name, EQ, equal_value));

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();
  ret = ups_table_mgr.pre_process(mutator);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObUpsCache& ups_cache = ObUpdateServerMain::get_instance()->get_update_server().get_ups_cache();
  ObBufferHandle buffer_handle;
  ObUpsCacheValue value;
  ret = ups_cache.get(table_id, exist_row_key, buffer_handle, column_id, value);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, value.version);
  EXPECT_TRUE(equal_value == value.value);
}

TEST_F(TestApplyWithCondition, test_preprocess_with_not_exist_cell)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  uint64_t table_id = 1001;
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  uint64_t column_id = 5;
  
  ObUpdateCondition& update_condition = mutator.get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));
  }

  // row exist, cell not exist
  ObString exist_row_key;
  exist_row_key.assign(row_key_str[3], strlen(row_key_str[3])); // "row_key_4";
  ObObj equal_value;
  equal_value.set_int(8888);
  ASSERT_TRUE(OB_SUCCESS == update_condition.add_cond(
        table_name, exist_row_key, column_name, EQ, equal_value));

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();
  ret = ups_table_mgr.pre_process(mutator);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObUpsCache& ups_cache = ObUpdateServerMain::get_instance()->get_update_server().get_ups_cache();
  ObBufferHandle buffer_handle;
  ObUpsCacheValue value;
  ret = ups_cache.get(table_id, exist_row_key, buffer_handle, column_id, value);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(value.version == 1);
  EXPECT_TRUE(value.value.get_type() == ObNullType);
}

TEST_F(TestApplyWithCondition, test_preprocess_with_exist_row)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  uint64_t table_id = 1001;
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  uint64_t column_id = OB_MAX_COLUMN_ID;
  
  ObUpdateCondition& update_condition = mutator.get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));
  }

  ObString exist_row_key;
  exist_row_key.assign(row_key_str[4], strlen(row_key_str[4])); // "row_key_5";
  ASSERT_TRUE(OB_SUCCESS == update_condition.add_cond(table_name, exist_row_key, true));

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();
  ret = ups_table_mgr.pre_process(mutator);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObUpsCache& ups_cache = ObUpdateServerMain::get_instance()->get_update_server().get_ups_cache();
  ObBufferHandle buffer_handle;
  ObUpsCacheValue value;
  ret = ups_cache.get(table_id, exist_row_key, buffer_handle, column_id, value);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, value.version);
  EXPECT_TRUE(value.value.get_type() == ObExtendType && value.value.get_ext() == ObActionFlag::OP_ROW_EXIST);
}

TEST_F(TestApplyWithCondition, test_preprocess_with_not_exist_row)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  uint64_t table_id = 1001;
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  uint64_t column_id = OB_MAX_COLUMN_ID;
  
  ObUpdateCondition& update_condition = mutator.get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));
  }

  ObString not_exist_row;
  not_exist_row.assign("not_exist_row", strlen("not_exist_row")); // "not_exist_row"
  ASSERT_TRUE(OB_SUCCESS == update_condition.add_cond(table_name, not_exist_row, true));

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();
  ret = ups_table_mgr.pre_process(mutator);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObUpsCache& ups_cache = ObUpdateServerMain::get_instance()->get_update_server().get_ups_cache();
  ObBufferHandle buffer_handle;
  ObUpsCacheValue value;
  ret = ups_cache.get(table_id, not_exist_row, buffer_handle, column_id, value);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, value.version);
  EXPECT_TRUE(value.value.get_type() == ObExtendType
      && value.value.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST);
}

TEST_F(TestApplyWithCondition, test_preprocess_with_data_in_memtable)
{
  int ret = 0;

  ObUpsMutator ups_mutator;
  ObMutator& mutator = ups_mutator.get_mutator();

  ObCellInfo cell[8];
  char row_key_str[8][30];
  char* table_str = "collect_info";
  char* column_str = "info_status";
  ObString table_name;
  table_name.assign(table_str, strlen(table_str));
  ObString column_name; //column_name="info_status", column_id=5
  column_name.assign(column_str, strlen(column_str));
  uint64_t table_id = 1001;
  uint64_t column_id = 5;
  
  ObUpsMutator ups_mutator1;
  ObUpsMutator ups_mutator2;
  ObUpdateCondition& update_condition1 = ups_mutator1.get_mutator().get_update_condition();
  ObUpdateCondition& update_condition2 = ups_mutator2.get_mutator().get_update_condition();

  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));
    cell[i].table_name_ = table_name;
    cell[i].row_key_ = row_key;
    cell[i].column_name_ = column_name;
    cell[i].value_.set_int(1000 * (i+1));
    ObMutatorCellInfo mutator_cell;
    mutator_cell.cell_info = cell[i];
    mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
    ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));

    ret = update_condition1.add_cond(table_name, row_key, column_name,
        EQ, cell[i].value_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = update_condition2.add_cond(table_name, row_key, column_name,
        EQ, cell[i].value_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();

  // apply mutator
  ret = ups_table_mgr.start_transaction();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.apply(ups_mutator);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.end_transaction(false);
  EXPECT_EQ(OB_SUCCESS, ret);

  // apply mutator1
  ret = ups_table_mgr.pre_process(ups_mutator1.get_mutator());
  EXPECT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 8; i++)
  {
    ObString row_key;
    snprintf(row_key_str[i], 30, "row_key_%ld",i+1);
    row_key.assign(row_key_str[i], strlen(row_key_str[i]));

    ObUpsCache& ups_cache = ObUpdateServerMain::get_instance()->get_update_server().get_ups_cache();
    ObBufferHandle buffer_handle;
    ObUpsCacheValue value;
    ret = ups_cache.get(table_id, row_key, buffer_handle, column_id, value);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(1, value.version);
  }

  ret = ups_table_mgr.start_transaction();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.apply(ups_mutator2);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ups_table_mgr.end_transaction(false);
  EXPECT_EQ(OB_SUCCESS, ret);
}

} // end namespace updateserver
} // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

