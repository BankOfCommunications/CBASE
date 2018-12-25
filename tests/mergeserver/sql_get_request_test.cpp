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
#include "common/ob_define.h"
#include "common/ob_cache.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "mergeserver/ob_ms_sql_sub_get_request.h"
#include "mergeserver/ob_ms_sql_get_request.h"
#include "mergeserver/ob_ms_async_rpc.h"
#include "common/location/ob_tablet_location_list.h"
#include "ob_location_list_cache_loader.h"
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
  int get(const int64_t , const common::ObServer & , const common::ObSqlGetParam & get_param, ObMsSqlRpcEvent & rpc_event) const
  {
    get_rpc_count_ ++;
    received_rpcs_.push_back(&rpc_event);
    TBSYS_LOG(INFO, "get_param=%s", to_cstring(get_param));
    return OB_SUCCESS;
  }

  virtual int get_session_next(const int64_t , const common::ObServer & ,  const int64_t , const int32_t , ObMsSqlRpcEvent & rpc_event)const
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

  inline const vector<ObMsSqlRpcEvent*> & get_received_rpcs()const
  {
    return received_rpcs_;
  }
private:
  mutable vector<ObMsSqlRpcEvent*> received_rpcs_;
  mutable int64_t get_rpc_count_;
  mutable int64_t session_rpc_count_;
  mutable tbsys::CThreadMutex lock_;
};

struct MockEnv
{
  ObGeneralRootRpcProxy *rs_proxy_;
  ObTabletLocationCache *loc_cache_;
  ObTabletLocationCacheProxy *cache_proxy_;

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

int build_env(MockEnv &env)
{
  ObServer root;
  const int64_t retry_times = 3;
  const int64_t timeout_us = 100000;
  const int64_t cache_timeout = 600000000;
  const int64_t cache_size = 512*1024*1024;
  const int64_t cache_slot_count = 10*1024*1024;
  ObGeneralRootRpcProxy *rs_proxy = new ObGeneralRootRpcProxy(retry_times, timeout_us, root);
  if (rs_proxy == NULL)
  {
    TBSYS_LOG(WARN, "fail to rs_proxy"); 
    EXPECT_EQ(OB_SUCCESS, OB_ERROR);
  }
  ObTabletLocationCache *loc_cache  = new ObTabletLocationCache();
  if (loc_cache == NULL)
  {
    EXPECT_EQ(OB_SUCCESS, OB_ERROR);
  }
  EXPECT_EQ(loc_cache->init(cache_size,cache_slot_count,cache_timeout), OB_SUCCESS);
  ObLocationListCacheLoader cache_loader;
  EXPECT_EQ(cache_loader.load("./pget_data/location_cache.txt", "tablet_location_cache"), OB_SUCCESS);
  EXPECT_EQ(cache_loader.get_decoded_location_list_cache(*loc_cache), OB_SUCCESS);
  ObTabletLocationCacheProxy *cache_proxy = new ObTabletLocationCacheProxy(root, rs_proxy, loc_cache);
  if (cache_proxy == NULL)
  {
    EXPECT_EQ(OB_SUCCESS, OB_ERROR);
  }
  env.cache_proxy_ = cache_proxy;
  env.loc_cache_ = loc_cache;
  env.rs_proxy_ = rs_proxy;
  cache_loader.dump_config();
  return OB_SUCCESS;
}

static  const int32_t g_svr_group_count = 2;
static  const uint64_t g_min_table_id = 1001;

void init_svr_groups(vector< set<int32_t> > & svr_groups)
{
  const int32_t node_count_per_group = 5;
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

TEST(ObMsSqlSubGetRequest, distributr_requests)
{
  //ObMsSqlGetRequest get_eventx(NULL, NULL);
  ObMsSqlGetRequest *get_event;
  get_event = new ObMsSqlGetRequest(NULL, NULL);
  ASSERT_TRUE(NULL != get_event);
}

TEST(ObMsSqlSubGetRequest, compile_test)
{
  MockEnv env;
  build_env(env);
}

TEST(ObMsSqlSubGetRequest, distribute_requests)
{
  TBSYS_LOG(INFO, "distribute_requests(0)");

  TBSYS_LOG(INFO, "distribute_requests(0.1)");
  MockEnv env;
  build_env(env);
  TBSYS_LOG(INFO, "distribute_requests(0.2)");
  const int64_t row_count = 100;
  ObRowkey rowkey;
  ObObj obj;
  ObSqlGetParam get_param;
  get_param.reset();
  get_param.set_table_id(1001);
  TBSYS_LOG(INFO, "distribute_requests(1)");
  for (int64_t i = 0; i < row_count; i++)
  {
    obj.set_int(i);
    rowkey.assign(&obj, 1);
    EXPECT_EQ(get_param.add_rowkey(rowkey, true), OB_SUCCESS);
  }
  MockAsyncRpcStub async_rpc;
  ObMsSqlGetRequest *get_event_tmp = new ObMsSqlGetRequest(env.cache_proxy_, &async_rpc);
  ObMsSqlGetRequest &get_event = *get_event_tmp;
  get_event.init(10, 10);
  int64_t timeout_us = 1000000;
  EXPECT_EQ(get_event.set_request_param(get_param,timeout_us, 5, 20), OB_SUCCESS);
  EXPECT_EQ(get_event.open(), OB_RESPONSE_TIME_OUT);
#if 0
  int64_t sub_req_count = 0;
  const ObMsSqlSubGetRequest *sub_reqs  = NULL;
  get_event.TEST_get_sub_requests(sub_reqs, sub_req_count);
  EXPECT_EQ(sub_req_count, g_svr_group_count);
  vector< set<int32_t> > svr_groups;
  init_svr_groups(svr_groups);

  for (int64_t s_idx = 0; s_idx < sub_req_count; s_idx ++)
  {
    ObSqlGetParam *cur_param = NULL;
    EXPECT_EQ(sub_reqs[s_idx].get_cur_param(cur_param) ,OB_SUCCESS);
  }
  EXPECT_EQ(get_event.TEST_get_triggered_sub_req_count(), g_svr_group_count);
  EXPECT_EQ(async_rpc.get_rpc_count(), g_svr_group_count);
  EXPECT_EQ(async_rpc.get_received_rpcs().size(), (size_t)g_svr_group_count);
  EXPECT_EQ(async_rpc.session_rpc_count(), 0);
#endif
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("INFO");
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

