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
#include <arpa/inet.h>
#include <vector>
#include <set>
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
#include "mergeserver/ob_scan_param_loader.h"
#include "mergeserver/ob_ms_sub_get_request.h"
#include "mergeserver/ob_ms_get_event_parellel.h"
#include "mergeserver/ob_ms_async_rpc.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "mergeserver/ob_ms_tablet_location.h"
#include "mergeserver/ob_location_list_cache_loader.h"
#include "../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace testing;
using namespace std;
static CharArena allocator_;

class MockAsyncRpcStub : public oceanbase::mergeserver::ObMergerAsyncRpcStub
{
public:
  MockAsyncRpcStub()
  {
    get_rpc_count_ = 0;
    session_rpc_count_ = 0;
  }
  int get(const int64_t , const common::ObServer & , const common::ObGetParam & , ObMergerRpcEvent & rpc_event) const
  {
    get_rpc_count_ ++;
    received_rpcs_.push_back(&rpc_event);
    return OB_SUCCESS;
  }

  virtual int get_session_next(const int64_t , const common::ObServer & ,  const int64_t , const int32_t , ObMergerRpcEvent & rpc_event)const
  {
    session_rpc_count_ ++;
    received_rpcs_.push_back(&rpc_event);
    return OB_SUCCESS;
  }
  inline int64_t get_rpc_count()const
  {
    return get_rpc_count_;
  }
  inline int64_t session_rpc_count()const
  {
    return session_rpc_count_;
  }

  inline const vector<ObMergerRpcEvent*> & get_received_rpcs()const
  {
    return received_rpcs_;
  }

private:
  mutable vector<ObMergerRpcEvent*> received_rpcs_;
  mutable int64_t get_rpc_count_;
  mutable int64_t session_rpc_count_;
};

struct MockEnv
{
  ObMergerRootRpcProxy *rs_proxy_;
  ObTabletLocationCache *loc_cache_;
  ObMergerLocationCacheProxy *cache_proxy_;

  MockEnv()
  {
    rs_proxy_ = NULL;
    loc_cache_ = NULL;
    cache_proxy_ = NULL;
  }

  ~MockEnv()
  {
    delete rs_proxy_ ;
    delete loc_cache_;
    delete cache_proxy_;
  }
};

MockEnv build_env()
{
  MockEnv env;
  ObServer root;
  const int64_t retry_times = 3;
  const int64_t timeout_us = 100000;
  const int64_t cache_timeout = 600000000;
  const int64_t cache_size = 512*1024*1024;
  const int64_t cache_slot_count = 10*1024*1024;
  ObMergerRootRpcProxy *rs_proxy = new ObMergerRootRpcProxy(retry_times, timeout_us, root);
  EXPECT_NE(rs_proxy, (void*)0);
  ObTabletLocationCache *loc_cache  = new ObMergerTabletLocationCache();
  EXPECT_NE(loc_cache, (void*)0);
  EXPECT_EQ(loc_cache->init(cache_size,cache_slot_count,cache_timeout), OB_SUCCESS);
  ObLocationListCacheLoader cache_loader;
  EXPECT_EQ(cache_loader.load("./pget_data/location_cache.txt", "tablet_location_cache"), OB_SUCCESS);
  EXPECT_EQ(cache_loader.get_decoded_location_list_cache(*loc_cache), OB_SUCCESS);
  ObMergerLocationCacheProxy *cache_proxy = new ObMergerLocationCacheProxy(root, rs_proxy, loc_cache);
  EXPECT_NE(cache_proxy, (void*)0);
  env.cache_proxy_ = cache_proxy;
  env.loc_cache_ = loc_cache;
  env.rs_proxy_ = rs_proxy;
  return env;
}

static  const int32_t g_svr_group_count = 4;
static  const uint64_t g_min_table_id = 1001;

void init_svr_groups(vector< set<int32_t> > & svr_groups)
{
  const int32_t node_count_per_group = 3;
  svr_groups.resize(g_svr_group_count);
  const char *ip_prefix = "10.232.35.";
  char ip_buf[32]= "";
  for (int32_t i = 0; i < g_svr_group_count; i++)
  {
    svr_groups[i].clear();
  }

  for (int32_t g_idx = 0; g_idx < g_svr_group_count; g_idx ++)
  {
    for (int32_t n_idx = 0; n_idx < node_count_per_group; n_idx ++)
    {
      struct in_addr ip_int;
      int32_t tmp;
      snprintf(ip_buf, sizeof(ip_buf), "%s%d", ip_prefix, g_idx*node_count_per_group + n_idx + 1);
      inet_aton(ip_buf,&ip_int);
      tmp = ip_int.s_addr;
      svr_groups[g_idx].insert(tmp);
    }
  }
}

TEST(ObMergerSubGetRequest, distribute_requests)
{
  vector< set<int32_t> > svr_groups;
  init_svr_groups(svr_groups);
  MockEnv env = build_env();
  const uint64_t column_id = 1111;
  const char *c_rowkey = "a";
  const int64_t cell_count = 100;
  ObRowkey rowkey;
  rowkey = make_rowkey(c_rowkey, &allocator_);
  ObGetParam get_param;
  ObCellInfo cell;
  get_param.reset(true);
  for (int64_t i = 0; i < cell_count; i++)
  {
    cell.table_id_ = i + g_min_table_id;
    cell.column_id_ = column_id;
    cell.row_key_ = rowkey;
    EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);
  }
  MockAsyncRpcStub async_rpc;
  ObMergerParellelGetEvent get_event(env.cache_proxy_, &async_rpc);
  int64_t timeout_us = 100000;
  EXPECT_EQ(get_event.set_request_param(get_param,timeout_us), OB_SUCCESS);

  int64_t sub_req_count = 0;
  const ObMergerSubGetRequest *sub_reqs  = NULL;
  get_event.TEST_get_sub_requests(sub_reqs, sub_req_count);
  EXPECT_EQ(sub_req_count, g_svr_group_count);

  for (int64_t s_idx = 0; s_idx < sub_req_count; s_idx ++)
  {
    ObGetParam *cur_param = NULL;
    EXPECT_EQ(sub_reqs[s_idx].get_cur_param(cur_param) ,OB_SUCCESS);
    int64_t first_table_id = (*cur_param)[0]->table_id_ - g_min_table_id;
    int64_t cur_g_idx = first_table_id%g_svr_group_count;
    EXPECT_TRUE(svr_groups[cur_g_idx].find(sub_reqs[s_idx].get_last_svr_ipv4()) != svr_groups[cur_g_idx].end());
    EXPECT_EQ(cur_param->get_cell_size(), cell_count/g_svr_group_count);
    for (int64_t cell_idx = 0;first_table_id < cell_count;first_table_id += g_svr_group_count, cell_idx ++)
    {
      EXPECT_EQ((*cur_param)[cell_idx]->table_id_, first_table_id + g_min_table_id);
    }
  }
  EXPECT_EQ(get_event.TEST_get_triggered_sub_req_count(), g_svr_group_count);
  EXPECT_EQ(async_rpc.get_rpc_count(), g_svr_group_count);
  EXPECT_EQ(async_rpc.get_received_rpcs().size(), (size_t)g_svr_group_count);
  EXPECT_EQ(async_rpc.session_rpc_count(), 0);
}

TEST(ObMergerSubGetRequest, get_multi_time)
{
  vector< set<int32_t> > svr_groups;
  init_svr_groups(svr_groups);
  MockEnv env = build_env();
  const uint64_t column_id = 1111;
  const char *c_rowkey = "a";
  const int64_t cell_count = 100;
  ObRowkey rowkey;
  rowkey = make_rowkey(c_rowkey, &allocator_);
  ObGetParam get_param;
  ObCellInfo cell;
  get_param.reset(true);
  for (int64_t i = 0; i < cell_count; i++)
  {
    cell.table_id_ = i + g_min_table_id;
    cell.column_id_ = column_id;
    cell.row_key_ = rowkey;
    EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);
  }
  MockAsyncRpcStub async_rpc;
  ObMergerParellelGetEvent get_event(env.cache_proxy_, &async_rpc);
  int64_t timeout_us = 100000;
  EXPECT_EQ(get_event.set_request_param(get_param,timeout_us), OB_SUCCESS);


  int64_t sub_req_count = 0;
  const ObMergerSubGetRequest *sub_reqs  = NULL;
  get_event.TEST_get_sub_requests(sub_reqs, sub_req_count);
  /// first rpc event finish
  int64_t sub_req_idx = 0;
  for (; sub_req_idx < sub_req_count; sub_req_idx ++)
  {
    if (sub_reqs[sub_req_idx].get_last_rpc_event() == async_rpc.get_received_rpcs()[0]->get_event_id())
    {
      break;
    }
  }
  EXPECT_LT(sub_req_idx, sub_req_count);
  ObGetParam *cur_param = NULL;
  EXPECT_EQ(sub_reqs[sub_req_idx].get_cur_param(cur_param) ,OB_SUCCESS);
  async_rpc.get_received_rpcs()[0]->get_result().add_cell(*((*cur_param)[0]));
  async_rpc.get_received_rpcs()[0]->set_result_code(OB_SUCCESS);
  int32_t prev_svr = sub_reqs[sub_req_idx].get_last_svr_ipv4();

  int64_t first_table_id = (*cur_param)[0]->table_id_ - g_min_table_id + g_svr_group_count;
  /// add result, and trigger next rpc
  bool finish = false;
  EXPECT_EQ(get_event.process_result(timeout_us, async_rpc.get_received_rpcs()[0],finish), OB_SUCCESS);
  EXPECT_FALSE(finish);
  EXPECT_EQ(async_rpc.get_rpc_count(), g_svr_group_count + 1);
  EXPECT_EQ(async_rpc.session_rpc_count(), 0);

  EXPECT_EQ(sub_reqs[sub_req_idx].get_cur_param(cur_param) ,OB_SUCCESS);
  int64_t cur_g_idx = first_table_id%g_svr_group_count;
  EXPECT_TRUE(svr_groups[cur_g_idx].find(sub_reqs[sub_req_idx].get_last_svr_ipv4()) != svr_groups[cur_g_idx].end());
  EXPECT_EQ(cur_param->get_cell_size(), cell_count/g_svr_group_count - 1);
  for (int64_t cell_idx = 0;first_table_id < cell_count;first_table_id += g_svr_group_count, cell_idx ++)
  {
    EXPECT_EQ((*cur_param)[cell_idx]->table_id_, first_table_id + g_min_table_id);
  }
  EXPECT_EQ(sub_reqs[sub_req_idx].get_last_svr_ipv4(), prev_svr);
  EXPECT_NE(sub_reqs[sub_req_idx].get_last_rpc_event(), async_rpc.get_received_rpcs()[0]->get_event_id());
}

TEST(ObMergerSubGetRequest, retry)
{
  vector< set<int32_t> > svr_groups;
  init_svr_groups(svr_groups);
  MockEnv env = build_env();
  const uint64_t column_id = 1111;
  const char *c_rowkey = "a";
  const int64_t cell_count = 100;
  ObRowkey rowkey;
  rowkey = make_rowkey(c_rowkey, &allocator_);
  ObGetParam get_param;
  ObCellInfo cell;
  get_param.reset(true);
  for (int64_t i = 0; i < cell_count; i++)
  {
    cell.table_id_ = i + g_min_table_id;
    cell.column_id_ = column_id;
    cell.row_key_ = rowkey;
    EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);
  }
  MockAsyncRpcStub async_rpc;
  ObMergerParellelGetEvent get_event(env.cache_proxy_, &async_rpc);
  int64_t timeout_us = 100000;
  EXPECT_EQ(get_event.set_request_param(get_param,timeout_us), OB_SUCCESS);


  int64_t sub_req_count = 0;
  const ObMergerSubGetRequest *sub_reqs  = NULL;
  get_event.TEST_get_sub_requests(sub_reqs, sub_req_count);
  /// first rpc event finish
  int64_t sub_req_idx = 0;
  for (; sub_req_idx < sub_req_count; sub_req_idx ++)
  {
    if (sub_reqs[sub_req_idx].get_last_rpc_event() == async_rpc.get_received_rpcs()[0]->get_event_id())
    {
      break;
    }
  }
  EXPECT_LT(sub_req_idx, sub_req_count);
  ObGetParam *cur_param = NULL;
  EXPECT_EQ(sub_reqs[sub_req_idx].get_cur_param(cur_param) ,OB_SUCCESS);
  async_rpc.get_received_rpcs()[0]->get_result().add_cell(*((*cur_param)[0]));
  async_rpc.get_received_rpcs()[0]->set_result_code(OB_RESPONSE_TIME_OUT);
  int32_t prev_svr = sub_reqs[sub_req_idx].get_last_svr_ipv4();

  int64_t first_table_id = (*cur_param)[0]->table_id_ - g_min_table_id;
  /// add result, and trigger next rpc
  bool finish = false;
  EXPECT_EQ(get_event.process_result(timeout_us, async_rpc.get_received_rpcs()[0],finish), OB_SUCCESS);
  EXPECT_FALSE(finish);
  EXPECT_EQ(async_rpc.get_rpc_count(), g_svr_group_count + 1);
  EXPECT_EQ(async_rpc.session_rpc_count(), 0);

  EXPECT_EQ(sub_reqs[sub_req_idx].get_cur_param(cur_param) ,OB_SUCCESS);
  int64_t cur_g_idx = first_table_id%g_svr_group_count;
  EXPECT_TRUE(svr_groups[cur_g_idx].find(sub_reqs[sub_req_idx].get_last_svr_ipv4()) != svr_groups[cur_g_idx].end());
  EXPECT_EQ(cur_param->get_cell_size(), cell_count/g_svr_group_count);
  for (int64_t cell_idx = 0;first_table_id < cell_count;first_table_id += g_svr_group_count, cell_idx ++)
  {
    EXPECT_EQ((*cur_param)[cell_idx]->table_id_, first_table_id + g_min_table_id);
  }
  EXPECT_NE(sub_reqs[sub_req_idx].get_last_svr_ipv4(), prev_svr);
  EXPECT_NE(sub_reqs[sub_req_idx].get_last_rpc_event(), async_rpc.get_received_rpcs()[0]->get_event_id());
}

TEST(ObMergerSubGetRequest, session_next)
{
  vector< set<int32_t> > svr_groups;
  init_svr_groups(svr_groups);
  MockEnv env = build_env();
  const uint64_t column_id = 1111;
  const char *c_rowkey = "a";
  const int64_t cell_count = 100;
  ObRowkey rowkey;
  rowkey = make_rowkey(c_rowkey, &allocator_);
  ObGetParam get_param;
  ObCellInfo cell;
  get_param.reset(true);
  for (int64_t i = 0; i < cell_count; i++)
  {
    cell.table_id_ = i + g_min_table_id;
    cell.column_id_ = column_id;
    cell.row_key_ = rowkey;
    EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);
  }
  MockAsyncRpcStub async_rpc;
  ObMergerParellelGetEvent get_event(env.cache_proxy_, &async_rpc);
  int64_t timeout_us = 100000;
  EXPECT_EQ(get_event.set_request_param(get_param,timeout_us), OB_SUCCESS);


  int64_t sub_req_count = 0;
  const ObMergerSubGetRequest *sub_reqs  = NULL;
  get_event.TEST_get_sub_requests(sub_reqs, sub_req_count);
  /// first rpc event finish
  int64_t sub_req_idx = 0;
  for (; sub_req_idx < sub_req_count; sub_req_idx ++)
  {
    if (sub_reqs[sub_req_idx].get_last_rpc_event() == async_rpc.get_received_rpcs()[0]->get_event_id())
    {
      break;
    }
  }
  EXPECT_LT(sub_req_idx, sub_req_count);
  ObGetParam *cur_param = NULL;
  EXPECT_EQ(sub_reqs[sub_req_idx].get_cur_param(cur_param) ,OB_SUCCESS);
  async_rpc.get_received_rpcs()[0]->get_result().add_cell(*((*cur_param)[0]));
  async_rpc.get_received_rpcs()[0]->get_result().set_is_req_fullfilled(false, 1);
  async_rpc.get_received_rpcs()[0]->set_result_code(OB_SUCCESS);
  int64_t session_id  = 5;
  async_rpc.get_received_rpcs()[0]->set_session_id(session_id);
  int32_t prev_svr = sub_reqs[sub_req_idx].get_last_svr_ipv4();

  int64_t first_table_id = (*cur_param)[0]->table_id_ - g_min_table_id + g_svr_group_count;
  /// add result, and trigger next rpc
  bool finish = false;
  EXPECT_EQ(get_event.process_result(timeout_us, async_rpc.get_received_rpcs()[0],finish), OB_SUCCESS);
  EXPECT_FALSE(finish);
  EXPECT_EQ(async_rpc.get_rpc_count(), g_svr_group_count);
  EXPECT_EQ(async_rpc.session_rpc_count(), 1);

  EXPECT_EQ(sub_reqs[sub_req_idx].get_cur_param(cur_param) ,OB_SUCCESS);
  int64_t cur_g_idx = first_table_id%g_svr_group_count;
  EXPECT_TRUE(svr_groups[cur_g_idx].find(sub_reqs[sub_req_idx].get_last_svr_ipv4()) != svr_groups[cur_g_idx].end());
  EXPECT_EQ(cur_param->get_cell_size(), cell_count/g_svr_group_count - 1);
  for (int64_t cell_idx = 0;first_table_id < cell_count;first_table_id += g_svr_group_count, cell_idx ++)
  {
    EXPECT_EQ((*cur_param)[cell_idx]->table_id_, first_table_id + g_min_table_id);
  }
  EXPECT_EQ(sub_reqs[sub_req_idx].get_last_svr_ipv4(), prev_svr);
  EXPECT_NE(sub_reqs[sub_req_idx].get_last_rpc_event(), async_rpc.get_received_rpcs()[0]->get_event_id());
  EXPECT_EQ(sub_reqs[sub_req_idx].get_last_session_id(), session_id);
}

TEST(ObMergerSubGetRequest, result_process)
{
  vector< set<int32_t> > svr_groups;
  init_svr_groups(svr_groups);
  MockEnv env = build_env();
  const uint64_t column_id = 1111;
  const char *c_rowkey = "a";
  const int64_t cell_count = 100;
  ObRowkey rowkey;
  rowkey = make_rowkey(c_rowkey, &allocator_);
  ObGetParam org_get_param;
  ObGetParam get_param;
  ObCellInfo cell;
  get_param.reset(true);
  ObString empty_str;
  org_get_param.reset(true);
  for (int64_t i = 0; i < cell_count; i++)
  {
    cell.table_name_ = empty_str;
    cell.column_name_ = empty_str;
    cell.table_id_ = i + g_min_table_id;
    cell.column_id_ = column_id;
    cell.row_key_ = rowkey;
    cell.value_.set_int(i);
    EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);
    cell.table_id_ = OB_INVALID_ID;
    cell.column_id_ = OB_INVALID_ID;
    cell.table_name_.assign((char*)&i,sizeof(i));
    cell.column_name_.assign((char*)&i,sizeof(i));
    EXPECT_EQ(org_get_param.add_cell(cell), OB_SUCCESS);
  }
  MockAsyncRpcStub async_rpc;
  ObMergerParellelGetEvent get_event(env.cache_proxy_, &async_rpc);
  int64_t timeout_us = 100000;
  EXPECT_EQ(get_event.set_request_param(get_param,timeout_us), OB_SUCCESS);

  int64_t sub_req_count = 0;
  const ObMergerSubGetRequest *sub_reqs  = NULL;
  get_event.TEST_get_sub_requests(sub_reqs, sub_req_count);
  /// first rpc event finish

  bool finish = false;
  for (int64_t rpc_idx = 0; rpc_idx < g_svr_group_count; rpc_idx ++)
  {
    int64_t sub_req_idx = 0;
    for (; sub_req_idx < sub_req_count; sub_req_idx ++)
    {
      if (sub_reqs[sub_req_idx].get_last_rpc_event() == async_rpc.get_received_rpcs()[rpc_idx]->get_event_id())
      {
        break;
      }
    }
    ObGetParam *cur_param = NULL;
    EXPECT_LT(sub_req_idx, g_svr_group_count);
    EXPECT_EQ(sub_reqs[sub_req_idx].get_cur_param(cur_param) ,OB_SUCCESS);
    for (int64_t cell_idx = 0; cell_idx < cur_param->get_cell_size(); cell_idx ++)
    {
      EXPECT_EQ(async_rpc.get_received_rpcs()[rpc_idx]->get_result().add_cell(*(*cur_param)[cell_idx]), OB_SUCCESS);
    }
    async_rpc.get_received_rpcs()[rpc_idx]->set_result_code(OB_SUCCESS);
    EXPECT_EQ(get_event.process_result(timeout_us, async_rpc.get_received_rpcs()[rpc_idx], finish), OB_SUCCESS);
    if (rpc_idx < g_svr_group_count - 1)
    {
      EXPECT_FALSE(finish);
    }
    else
    {
      EXPECT_TRUE(finish);
    }
  }

  for (int64_t cell_idx = 0;cell_idx < cell_count; cell_idx ++)
  {
    int64_t int_val = 0;
    ObInnerCellInfo *cell = NULL;
    bool row_changed = false;
    EXPECT_EQ(get_event.next_cell(), OB_SUCCESS);
    EXPECT_EQ(get_event.get_cell(&cell,&row_changed), OB_SUCCESS);
    EXPECT_TRUE(row_changed);
    EXPECT_EQ(cell->table_id_,  static_cast<uint64_t>(cell_idx + g_min_table_id));
    EXPECT_EQ(cell->column_id_, column_id);
    EXPECT_TRUE(cell->row_key_ == rowkey);
    EXPECT_EQ(cell->value_.get_int(int_val), OB_SUCCESS);
    EXPECT_EQ(int_val, cell_idx);
  }
  EXPECT_EQ(get_event.next_cell(), OB_ITER_END);

  EXPECT_EQ(get_event.reset_iterator(), OB_SUCCESS);
  for (int64_t cell_idx = 0;cell_idx < cell_count; cell_idx ++)
  {
    int64_t int_val = 0;
    ObInnerCellInfo *cell = NULL;
    bool row_changed = false;
    EXPECT_EQ(get_event.next_cell(), OB_SUCCESS);
    EXPECT_EQ(get_event.get_cell(&cell,&row_changed), OB_SUCCESS);
    EXPECT_TRUE(row_changed);
    EXPECT_EQ(cell->table_id_,  static_cast<uint64_t>(cell_idx + g_min_table_id));
    EXPECT_EQ(cell->column_id_, column_id);
    EXPECT_TRUE(cell->row_key_ == rowkey);
    EXPECT_EQ(cell->value_.get_int(int_val), OB_SUCCESS);
    EXPECT_EQ(int_val, cell_idx);
  }
  EXPECT_EQ(get_event.next_cell(), OB_ITER_END);
  EXPECT_EQ(get_event.reset_iterator(), OB_SUCCESS);
  ObScanner final_result;
  bool got_all = false;
  EXPECT_EQ(get_event.fill_result(final_result,org_get_param, got_all), OB_SUCCESS);
  EXPECT_TRUE(got_all);

  for (int64_t cell_idx = 0;cell_idx < cell_count; cell_idx ++)
  {
    int64_t int_val = 0;
    ObCellInfo *cell = NULL;
    bool row_changed = false;
    EXPECT_EQ(final_result.next_cell(), OB_SUCCESS);
    EXPECT_EQ(final_result.get_cell(&cell,&row_changed), OB_SUCCESS);
    EXPECT_TRUE(row_changed);
    //EXPECT_EQ(cell->table_id_,  static_cast<uint64_t>(cell_idx + g_min_table_id));
    //EXPECT_EQ(cell->column_id_, column_id);
    EXPECT_EQ(cell->table_name_.length(), (int32_t)sizeof(cell_idx));
    EXPECT_EQ(*(int64_t*)cell->table_name_.ptr(), cell_idx);
    EXPECT_EQ(cell->column_name_.length(), (int32_t)sizeof(cell_idx));
    EXPECT_EQ(*(int64_t*)cell->column_name_.ptr(), cell_idx);
    EXPECT_TRUE(cell->row_key_ == rowkey);
    EXPECT_EQ(cell->value_.get_int(int_val), OB_SUCCESS);
    EXPECT_EQ(int_val, cell_idx);
  }
}

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

