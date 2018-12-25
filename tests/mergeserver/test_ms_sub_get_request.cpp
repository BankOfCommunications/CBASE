/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * test_read_param_decoder.cpp is for what ...
 *
 * Version: $id: test_read_param_decoder.cpp,v 0.1 3/29/2011 4:54p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
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
#include "common/ob_cell_array.h"
#include "common/ob_define.h"
#include "common/ob_cache.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_groupby.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_session_mgr.h"
#include "mergeserver/ob_groupby_operator.h"
#include "mergeserver/ob_read_param_decoder.h"
#include "mergeserver/ob_ms_tsi.h"
#include "ob_scan_param_loader.h"
#include "mergeserver/ob_ms_sub_get_request.h"
#include "../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::mergeserver::test;
using namespace testing;
using namespace std;
static CharArena allocator_;


TEST(ObMergerSubGetRequest, param_iterator)
{
  ObGetParam get_param;
  PageArena<int64_t, ModulePageAllocator> mem_allocator;
  get_param.reset(true);
  ObMergerSubGetRequest sub_get_req;
  sub_get_req.init(mem_allocator);
  ObGetParam *cur_get_param = NULL;

  const int64_t cell_count = 2048;
  ObCellInfo cell;
  for (int64_t i = 0; i < cell_count; i++)
  {
    cell.table_id_ = i + 1;
    cell.column_id_ = i + 1;
    cell.row_key_ = make_rowkey((char*)&i, sizeof(i), &allocator_);
    EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);
  }
  sub_get_req.set_param(get_param);
  for (int64_t i = 0; i < cell_count; i += 2)
  {
    EXPECT_EQ(sub_get_req.add_cell(i), OB_SUCCESS);
  }
  EXPECT_EQ(sub_get_req.get_cur_param(cur_get_param), OB_SUCCESS);
  EXPECT_EQ((*cur_get_param).get_cell_size(), cell_count/2);
  for (int64_t i = 0; i < cell_count; i += 2)
  {
    EXPECT_NE((*cur_get_param)[i/2], (void*)0);
    EXPECT_EQ((*cur_get_param)[i/2]->column_id_, static_cast<uint64_t>(i + 1));
    EXPECT_EQ((*cur_get_param)[i/2]->table_id_, static_cast<uint64_t>(i + 1));
    ObRowkey rowkey;
    rowkey = make_rowkey((char*)&i, sizeof(i), &allocator_);
    EXPECT_TRUE((*cur_get_param)[i/2]->row_key_ == rowkey);
  }

  //// add 1st result
  ObScanner res_1st;
  const int64_t first_result_count = 256;
  for (int64_t i = 0; i < first_result_count; i += 2)
  {
    cell.table_id_ = i + 1;
    cell.column_id_ = i + 1;
    cell.row_key_ = make_rowkey((char*)&i, sizeof(i), &allocator_);
    EXPECT_EQ(res_1st.add_cell(cell), OB_SUCCESS);
  }
  EXPECT_EQ(sub_get_req.add_result(res_1st), OB_SUCCESS);

  EXPECT_EQ(sub_get_req.get_cur_param(cur_get_param), OB_SUCCESS);
  for (int64_t i = first_result_count; i < cell_count; i += 2)
  {
    EXPECT_NE((*cur_get_param)[(i - first_result_count) /2], (void*)0);
    EXPECT_EQ((*cur_get_param)[(i - first_result_count)/2]->column_id_, static_cast<uint64_t>(i + 1));
    EXPECT_EQ((*cur_get_param)[(i - first_result_count)/2]->table_id_, static_cast<uint64_t>(i + 1));
    ObRowkey rowkey;
    rowkey = make_rowkey((char*)&i, sizeof(i), &allocator_);
    EXPECT_TRUE((*cur_get_param)[(i - first_result_count)/2]->row_key_ == rowkey);
  }


  //// add 2nd result
  ObScanner res_2nd;
  for (int64_t i = first_result_count; i < cell_count; i += 2)
  {
    cell.table_id_ = i + 1;
    cell.column_id_ = i + 1;
    cell.row_key_ = make_rowkey((char*)&i, sizeof(i), &allocator_);
    EXPECT_EQ(res_2nd.add_cell(cell), OB_SUCCESS);
  }
  EXPECT_EQ(sub_get_req.add_result(res_2nd), OB_SUCCESS);
  EXPECT_EQ(sub_get_req.get_cur_param(cur_get_param), OB_ITER_END);

  /// check result
  for (int64_t i = 0; i < cell_count; i+=2)
  {
    ObInnerCellInfo *cell = NULL;
    int64_t org_idx = 0;
    EXPECT_EQ(sub_get_req.has_next(), OB_SUCCESS);
    EXPECT_EQ(sub_get_req.next(cell, org_idx), OB_SUCCESS);
    EXPECT_EQ(org_idx, i);

    EXPECT_EQ(cell->column_id_, static_cast<uint64_t>(i + 1));
    EXPECT_EQ(cell->table_id_, static_cast<uint64_t>(i + 1));
    ObRowkey rowkey;
    rowkey = make_rowkey((char*)&i, sizeof(i), &allocator_);
    EXPECT_TRUE(cell->row_key_ == rowkey);
  }
  EXPECT_EQ(sub_get_req.has_next(), OB_ITER_END);
}

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

