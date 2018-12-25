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

TEST(ObGetMerger, basic)
{
  ObGetParam get_param;
  PageArena<int64_t, ModulePageAllocator> mem_allocator;
  get_param.reset(true);
  const int64_t sub_req_count = 5;
  const int64_t cell_count = sub_req_count*512;
  ObMergerSubGetRequest sub_reqs[sub_req_count];
  ObScanner res_vec[sub_req_count];
  for (int64_t i = 0; i < sub_req_count; i++)
  {
    sub_reqs[i].init(mem_allocator);
    sub_reqs[i].set_param(get_param);
  }
  ObCellInfo cell;
  for (int64_t cell_idx = 0; cell_idx < cell_count; cell_idx += sub_req_count)
  {
    for (int64_t sub_req_idx = 0; sub_req_idx < sub_req_count; sub_req_idx ++)
    {
      int64_t idx = cell_idx + sub_req_idx;
      cell.table_id_ = idx + 1;
      cell.column_id_ = idx + 1;
      cell.row_key_ = make_rowkey((char*)&idx, sizeof(idx), &allocator_);
      EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);
      EXPECT_EQ(res_vec[sub_req_idx].add_cell(cell), OB_SUCCESS);
      EXPECT_EQ(sub_reqs[sub_req_idx].add_cell(idx), OB_SUCCESS);
    }
  }
  for (int64_t sub_req_idx = 0; sub_req_idx < sub_req_count; sub_req_idx ++)
  {
    EXPECT_EQ(sub_reqs[sub_req_idx].add_result(res_vec[sub_req_idx]), OB_SUCCESS);
  }
  ObGetMerger get_merger;
  EXPECT_EQ(get_merger.init(sub_reqs, sub_req_count,get_param), OB_SUCCESS);

  bool row_changed = false;
  ObInnerCellInfo *cur_cell = NULL;
  for (int64_t i = 0; i < cell_count; i++)
  {
    EXPECT_EQ(get_merger.next_cell(), OB_SUCCESS);
    EXPECT_EQ(get_merger.get_cell(&cur_cell,&row_changed), OB_SUCCESS);
    EXPECT_EQ(cur_cell->table_id_, static_cast<uint64_t>(i+1));
    EXPECT_EQ(cur_cell->column_id_, static_cast<uint64_t>(i+1));
    ObRowkey rowkey;
    rowkey = make_rowkey((char*)&i,sizeof(i), &allocator_);
    EXPECT_TRUE(rowkey == cur_cell->row_key_);
  }
  EXPECT_EQ(get_merger.next_cell(), OB_ITER_END);
}

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

