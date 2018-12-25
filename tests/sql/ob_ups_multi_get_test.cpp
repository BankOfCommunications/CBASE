/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_multi_get_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "sql/ob_ups_multi_get.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_string_buf.h"
#include "ob_fake_sql_ups_rpc_proxy2.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace sql::test;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObUpsMultiGetTest: public ::testing::Test
{
  public:
    ObUpsMultiGetTest();
    virtual ~ObUpsMultiGetTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObUpsMultiGetTest(const ObUpsMultiGetTest &other);
    ObUpsMultiGetTest& operator=(const ObUpsMultiGetTest &other);
  protected:
    // data members
    ObStringBuf str_buf_;
    CharArena arena_;
};


ObUpsMultiGetTest::ObUpsMultiGetTest()
{
}

ObUpsMultiGetTest::~ObUpsMultiGetTest()
{
}

void ObUpsMultiGetTest::SetUp()
{
}

void ObUpsMultiGetTest::TearDown()
{
}

TEST_F(ObUpsMultiGetTest, basic_test)
{
  ObUpsMultiGet ups_multi_get;
  ObGetParam get_param;
  ObFakeSqlUpsRpcProxy2 rpc_proxy;
  int64_t column_count = 3;

  rpc_proxy.set_column_count(column_count);
  ups_multi_get.set_rpc_proxy(&rpc_proxy);

  int64_t now = tbsys::CTimeUtil::getTime();
  ups_multi_get.set_ts_timeout_us(1000 * 1000 + now);

  ObRowkey rowkey;

  ObCellInfo cell;
  for(int64_t i=0;i<1001;i++)
  {
    gen_rowkey(i, arena_, rowkey);

    for(int64_t j=0;j<column_count;j++)
    {
      cell.table_id_ = TABLE_ID;
      cell.row_key_ = rowkey;
      cell.column_id_ = j + 1;
      cell.value_.set_int(i * 1000 + j);

      OK(get_param.add_cell(cell));
    }
  }

  ObRowDesc row_desc;
  for(int i=1;i<=column_count;i++)
  {
    OK(row_desc.add_column_desc(TABLE_ID, i));
  }

  ups_multi_get.set_row_desc(row_desc);
  ups_multi_get.set_get_param(get_param);

  const ObRow *ups_row = NULL;
  const ObObj *value = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t int_value = 0;
  const ObRowkey *rowkey_ptr = NULL;

  OK(ups_multi_get.open());

  for(int i=0;i<1001;i++)
  {
    OK(ups_multi_get.get_next_row(rowkey_ptr, ups_row));
    for(int j=0;j<column_count;j++)
    {
      OK(ups_row->raw_get_cell(j, value, table_id, column_id));
      OK(value->get_int(int_value));
      ASSERT_EQ(i * 1000 + j, int_value);
    }
  }
  ASSERT_EQ(OB_ITER_END, ups_multi_get.get_next_row(rowkey_ptr, ups_row));

  OK(ups_multi_get.close());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

