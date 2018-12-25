/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * sql_get_merger_test.cpp is for what ...
 *
 * Version: $id: sql_get_merger_test.cpp,v 0.1 1/10/2013 4:54p wushi Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */

#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include <string>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <math.h>
#include <sys/syscall.h>
#include <unistd.h>
#include "common/ob_define.h"
#include "common/ob_cache.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_session_mgr.h"
#include "mergeserver/ob_ms_tsi.h"
#include "mergeserver/ob_ms_sql_sub_get_request.h"
#include "../common/test_rowkey_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
//using namespace oceanbase::mergeserver::test;
using namespace testing;
using namespace std;

static CharArena allocator_;

TEST(ObGetMerger, basic)
{
  ObSqlGetParam get_param;
  PageArena<int64_t, ModulePageAllocator> mem_allocator;
  get_param.reset();
  const int64_t sub_req_count = 2;
  const int64_t row_count = 6;
  ObMsSqlSubGetRequest sub_reqs[sub_req_count];
  ObNewScanner res_vec[sub_req_count];
  ObRowkey rowkey;
  ObRow    row;
  ObRowDesc desc;
  ObObj obj;
  obj.set_int(1111);

 
  const uint64_t TABLE_ID = 1001;
  /* init table */
  ASSERT_EQ(OB_SUCCESS, desc.add_column_desc(TABLE_ID, 10ul));
  ASSERT_EQ(OB_SUCCESS, desc.add_column_desc(TABLE_ID, 11ul));
  desc.set_rowkey_cell_count(1);
  row.set_row_desc(desc);
  
  for (int64_t i = 0; i < sub_req_count; i++)
  {
    sub_reqs[i].init(mem_allocator);
    sub_reqs[i].set_param(get_param);
    sub_reqs[i].set_row_desc(desc);
  }
  
  int64_t idx = 0;
  for (int64_t row_idx = 0; row_idx < row_count; row_idx++)
  {
    for (int64_t sub_req_idx = 0; sub_req_idx < sub_req_count; sub_req_idx ++)
    {
      char idx_buf[100];
      sprintf(idx_buf, "%ld", idx);
      rowkey = make_rowkey((char*)idx_buf, 10, &allocator_);
      row.set_cell(TABLE_ID, 10ul, rowkey.ptr()[0]);
      row.set_cell(TABLE_ID, 11ul, obj);
      EXPECT_EQ(get_param.add_rowkey(rowkey), OB_SUCCESS);
      EXPECT_EQ(res_vec[sub_req_idx].add_row(row), OB_SUCCESS);
      EXPECT_EQ(sub_reqs[sub_req_idx].add_row(idx), OB_SUCCESS);
      TBSYS_LOG(INFO, "==== set (sub[%ld],rowkey[%ld]) ====", sub_req_idx, idx);

      idx ++;
    }
  }

  for (int64_t sub_req_idx = 0; sub_req_idx < sub_req_count; sub_req_idx ++)
  {
    // TBSYS_LOG(INFO, "add result %ld", sub_req_idx);
    EXPECT_EQ(sub_reqs[sub_req_idx].add_result(res_vec[sub_req_idx]), OB_SUCCESS);
  }
  
  ObSqlGetMerger get_merger;
  EXPECT_EQ(get_merger.init(sub_reqs, sub_req_count, get_param), OB_SUCCESS);

  TBSYS_LOG(WARN, "init done");

  ObRow cur_row;
  cur_row.set_row_desc(desc);
  for (int64_t sub_req_idx = 0; sub_req_idx < sub_req_count; sub_req_idx ++)
  {
    for (int64_t i = 0; i < row_count; i++)
    {
      EXPECT_EQ(get_merger.get_next_row(cur_row), OB_SUCCESS);
      TBSYS_LOG(INFO, " >>>> get (rowkey=%s) <<<<", to_cstring(cur_row));
      //ObRowkey rowkey;
      //rowkey = make_rowkey((char*)&i,sizeof(i), &allocator_);
      //EXPECT_TRUE(rowkey == cur_row->row_key_);
    }
  }
  EXPECT_EQ(get_merger.get_next_row(cur_row), OB_ITER_END);
}

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

