/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_sql_ups_rpc_proxy_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include "common/ob_string_buf.h"
#include <gtest/gtest.h>
#include "ob_fake_sql_ups_rpc_proxy2.h"
#include "test_utility.h"

using namespace oceanbase;
using namespace common;
using namespace sql::test;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

namespace test
{
  class ObFakeSqlUpsRpcProxyTest2: public ::testing::Test
  {
    public:
      ObFakeSqlUpsRpcProxyTest2();
      virtual ~ObFakeSqlUpsRpcProxyTest2();
      virtual void SetUp();
      virtual void TearDown();
    private:
      // disallow copy
      ObFakeSqlUpsRpcProxyTest2(const ObFakeSqlUpsRpcProxyTest2 &other);
      ObFakeSqlUpsRpcProxyTest2& operator=(const ObFakeSqlUpsRpcProxyTest2 &other);

    protected:
      int gen_get_param(int64_t start, int64_t end, int64_t column_num, CharArena &arena, ObGetParam &get_param);
  };

  int ObFakeSqlUpsRpcProxyTest2::gen_get_param(int64_t start, int64_t end, int64_t column_num, CharArena &arena, ObGetParam &get_param)
  {
    int ret = OB_SUCCESS;
    ObCellInfo cell;
    ObRowkey rowkey;

    get_param.reset();

    for(int64_t i=start;OB_SUCCESS == ret && i<=end;i++)
    {
      ret = gen_rowkey(i, arena, rowkey);
      
      for(int64_t j=0;OB_SUCCESS == ret && j<column_num;j++)
      {
        cell.table_id_ = TABLE_ID;
        cell.row_key_ = rowkey;
        cell.column_id_ = j + 1;
        cell.value_.set_int(i * 1000 + j);

        ret = get_param.add_cell(cell);
      }
    }
    return ret;
  }

  ObFakeSqlUpsRpcProxyTest2::ObFakeSqlUpsRpcProxyTest2()
  {
  }

  ObFakeSqlUpsRpcProxyTest2::~ObFakeSqlUpsRpcProxyTest2()
  {
  }

  void ObFakeSqlUpsRpcProxyTest2::SetUp()
  {
  }

  void ObFakeSqlUpsRpcProxyTest2::TearDown()
  {
  }

  TEST_F(ObFakeSqlUpsRpcProxyTest2, get_test)
  {
    ObFakeSqlUpsRpcProxy2 rpc_proxy;

    int64_t column_count = 3;

    ObGetParam get_param;

    rpc_proxy.set_column_count(column_count);
    CharArena arena;

    ObNewScanner new_scanner;

    ObUpsRow ups_row;
    ObRowDesc row_desc;
    for(int i=1;i<=column_count;i++)
    {
      OK(row_desc.add_column_desc(TABLE_ID, i));
    }
    ups_row.set_row_desc(row_desc);

    const ObObj *value = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    int64_t int_value = 0;
    const ObRowkey *got_rowkey = NULL;
    bool is_fullfilled = false;
    int64_t fullfilled_num = 0;

    int64_t k = 0;
    for(k=0;k<2;k++)
    {
      gen_get_param(k * 100, 200, column_count, arena, get_param);
      OK(rpc_proxy.sql_ups_get(get_param, new_scanner, 1000 * 1000));
      for(int i=0;i<100;i++)
      {
        OK(new_scanner.get_next_row(got_rowkey, ups_row));
        for(int j=0;j<column_count;j++)
        {
          OK(ups_row.raw_get_cell(j, value, table_id, column_id));
          OK(value->get_int(int_value));
          ASSERT_EQ((i + k * 100) * 1000 + j, int_value);
        }
      }
      ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(got_rowkey, ups_row));

      OK(new_scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num));
      ASSERT_FALSE(is_fullfilled);
      ASSERT_EQ(100, fullfilled_num);
    }

    k = 2;
    int64_t row_count = 200 - k * 100 + 1; 
    
    gen_get_param(k * 100, 200, column_count, arena, get_param);
    OK(rpc_proxy.sql_ups_get(get_param, new_scanner, 1000 * 1000));
    for(int i=0;i<row_count;i++)
    {
      OK(new_scanner.get_next_row(got_rowkey, ups_row));
      for(int j=0;j<row_count;j++)
      {
        OK(ups_row.raw_get_cell(j, value, table_id, column_id));
        OK(value->get_int(int_value));
        ASSERT_EQ((i + k * 100) * 1000 + j, int_value);
      }
    }
    ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(got_rowkey, ups_row));

    OK(new_scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num));
    ASSERT_TRUE(is_fullfilled);
    ASSERT_EQ(row_count, fullfilled_num);
 
  }

  TEST_F(ObFakeSqlUpsRpcProxyTest2, gen_new_scanner_test2)
  {
    ObFakeSqlUpsRpcProxy2 rpc_proxy;
    ObNewScanner new_scanner;
    ObScanParam scan_param;
    const ObRowkey *rowkey = NULL;
    CharArena arena;

    int start = 5;
    int end = 1000;
    
    ObNewRange range;
    gen_new_range(start, end, arena, range);

    scan_param.set_range(range);

    rpc_proxy.gen_new_scanner(scan_param, new_scanner);
    ObUpsRow ups_row;
    const ObObj *cell = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    int64_t int_value = 0;

    ObNewRange res_range;
    OK(new_scanner.get_range(res_range));

    ObRowDesc row_desc;
    for(uint64_t i = 0;i<COLUMN_NUMS;i++)
    {
      row_desc.add_column_desc(TABLE_ID, i+OB_APP_MIN_COLUMN_ID);
    }
    ups_row.set_row_desc(row_desc);

    for(int i=start + 1;i<=start + 100;i++)
    {
      OK(new_scanner.get_next_row(rowkey, ups_row));
      for(int j=0;j<COLUMN_NUMS;j++)
      {
        OK(ups_row.raw_get_cell(j, cell, table_id, column_id));
        cell->get_int(int_value);
        ASSERT_EQ(i * 1000 + j, int_value);
      }
    }

    ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(rowkey, ups_row));

  }

  TEST_F(ObFakeSqlUpsRpcProxyTest2, gen_new_scanner_test1)
  {
    ObFakeSqlUpsRpcProxy2 rpc_proxy;
    ObNewScanner new_scanner;
    char rowkey_buf[100];

    int64_t start = 100;
    int64_t end = 1000;

    ObBorderFlag border_flag;
    border_flag.set_inclusive_start();
    border_flag.set_inclusive_end();

    rpc_proxy.gen_new_scanner(TABLE_ID, start, end, border_flag, new_scanner, true);

    ObUpsRow ups_row;
    const ObObj *cell = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    int64_t int_value = 0;
    const ObRowkey *rowkey = NULL;
    ObString rowkey_str;

    ObRowDesc row_desc;
    for(uint64_t i = 0;i<COLUMN_NUMS;i++)
    {
      row_desc.add_column_desc(TABLE_ID, i+OB_APP_MIN_COLUMN_ID);
    }
    ups_row.set_row_desc(row_desc);

    for(int64_t i=start;i<=end;i++)
    {
      OK(new_scanner.get_next_row(rowkey, ups_row));
      sprintf(rowkey_buf, "rowkey_%05ld", i);

      rowkey->ptr()[0].get_varchar(rowkey_str);
      ASSERT_EQ(0, strncmp(rowkey_buf, rowkey_str.ptr(), rowkey_str.length())); 

      for(int j=0;j<COLUMN_NUMS;j++)
      {
        OK(ups_row.raw_get_cell(j, cell, table_id, column_id));
        cell->get_int(int_value);
        ASSERT_EQ(i * 1000 + j, int_value);
      }
    }

    ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(rowkey, ups_row));

    bool is_fullfilled = false;
    int64_t fullfilled_row_num = 0;
    new_scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num);

    ASSERT_EQ( end - start + 1, fullfilled_row_num);

  }

  TEST_F(ObFakeSqlUpsRpcProxyTest2, get_int_test)
  {
    ObFakeSqlUpsRpcProxy2 rpc_proxy;
    ObString str;
    ObObj rowkey_obj;
    ObRowkey rowkey;

    char t[100];

    for(int64_t i=0;i<5000;i++)
    {
      sprintf(t, "rowkey_%05ld", i);
      str.assign_ptr(const_cast<char *>(t), (int32_t)strlen(t));
      rowkey_obj.set_varchar(str);
      rowkey.assign(&rowkey_obj, 1);
      ASSERT_EQ(i, rpc_proxy.get_int(rowkey));
    }
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

