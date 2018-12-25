/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_sql_query_service.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/utility.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "chunkserver/ob_sql_query_service.h"
#include "ob_operator_factory_impl.h"
#include "ob_fake_ups_rpc_stub2.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace chunkserver;

#define TABLE_ID 1001
#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObSqlQueryServiceTest: public ::testing::Test
{
  public:
    ObSqlQueryServiceTest();
    virtual ~ObSqlQueryServiceTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSqlQueryServiceTest(const ObSqlQueryServiceTest &other);
    ObSqlQueryServiceTest& operator=(const ObSqlQueryServiceTest &other);
  protected:
    // data members
};

ObSqlQueryServiceTest::ObSqlQueryServiceTest()
{
}

ObSqlQueryServiceTest::~ObSqlQueryServiceTest()
{
}

void ObSqlQueryServiceTest::SetUp()
{
  system("python gen_tablet_scan_test_data.py");
}

void ObSqlQueryServiceTest::TearDown()
{
}

TEST_F(ObSqlQueryServiceTest, basic_test)
{
  ObSqlQueryService query_service;

  ObOperatorFactoryImpl op_factory;
  OK(query_service.set_operator_factory(&op_factory));

  ObFakeUpsRpcStub2 rpc_stub;
  rpc_stub.set_ups_scan("./tablet_scan_test_data/ups_table1.ini");
  rpc_stub.set_ups_multi_get("./tablet_scan_test_data/ups_table2.ini");

  OK(query_service.set_ups_rpc_stub(&rpc_stub));

  ObSchemaManagerV2 schema_mgr;
  tbsys::CConfig cfg;
  ASSERT_TRUE(schema_mgr.parse_from_file("./tablet_scan_test_data/schema.ini", cfg));

  OK(query_service.set_schema_manager(&schema_mgr));
  query_service.set_join_batch_count(3);

  ObSqlExpression expr1, expr2, expr3;
  ObSqlScanParam sql_scan_param;

  expr1.set_tid_cid(TABLE_ID, 4);
  ExprItem item_a;
  item_a.type_ = T_REF_COLUMN;
  item_a.value_.cell_.tid = TABLE_ID;
  item_a.value_.cell_.cid = 4;
  expr1.add_expr_item(item_a);
  expr1.add_expr_item_end();
  OK(sql_scan_param.add_output_column(expr1));

  expr2.set_tid_cid(TABLE_ID, 5);
  ExprItem item_b;
  item_b.type_ = T_REF_COLUMN;
  item_b.value_.cell_.tid = TABLE_ID;
  item_b.value_.cell_.cid = 5;
  expr2.add_expr_item(item_b);
  expr2.add_expr_item_end();
  OK(sql_scan_param.add_output_column(expr2));

  int64_t start = 100;
  int64_t end = 1000;
  
  ObNewRange range;
  range.table_id_ = TABLE_ID;
  CharArena arena;
  gen_new_range(start, end, arena, range);

  OK(sql_scan_param.set_range(range));

  ObRowDesc row_desc;
  row_desc.add_column_desc(TABLE_ID, 4);
  row_desc.add_column_desc(TABLE_ID, 5);


  ObFileTable result("./tablet_scan_test_data/result.ini");

  int err = OB_SUCCESS;
  int64_t count = 0;
  ObRow row;
  const ObRow *result_row = NULL;
  const ObObj *value = NULL;
  const ObObj *result_value = NULL;
  uint64_t table_id = 0;
  uint64_t column_id = 0;
  ObNewScanner new_scanner;
  new_scanner.set_mem_size_limit(1024);

  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;

  row.set_row_desc(row_desc);

  OK(query_service.open(sql_scan_param));
  OK(result.open());

  int run_times = 0;
  while(OB_SUCCESS == err)
  {
    OK(query_service.fill_scan_data(new_scanner));
    OK(new_scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num));

    run_times ++;

    while(OB_SUCCESS == err)
    {
      err = new_scanner.get_next_row(row);
      if(OB_SUCCESS != err)
      {
        ASSERT_TRUE(OB_ITER_END == err);
        err = OB_SUCCESS;
        break;
      }

      OK(result.get_next_row(result_row));
      count ++;

      ASSERT_TRUE(row.get_column_num() > 0);

      for(int64_t i=0;i<row.get_column_num();i++)
      {
        OK(row.raw_get_cell(i, value, table_id, column_id));
        OK(result_row->get_cell(table_id, column_id, result_value));

        if( *value != *result_value )
        {
          printf("row:[%ld], column[%ld]===========\n", count, i);
          printf("row: %s\n", print_obj(*value));
          printf("result: %s\n", print_obj(*result_value));
        }

        ASSERT_TRUE((*value) == (*result_value));
      }
    }

    if(is_fullfilled)
    {
      break;
    }
  }

  printf("I am runinng %d\n", run_times);
  printf("fullfilled_item_num %ld\n", fullfilled_item_num);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}



