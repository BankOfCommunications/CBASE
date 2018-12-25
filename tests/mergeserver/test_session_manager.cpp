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
#include "ob_scan_param_loader.h"
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
#include "../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace testing;
using namespace std;
static CharArena allocator_;

void init_decode_param(ObStringBuf &buf,  ObScanParam &org_scan_param)
{
  UNUSED(buf);
  const char *c_ptr;
  ObString str;
  ObRowkey key;
  ObNewRange q_range;
  q_range.set_whole_range();
  q_range.border_flag_.set_inclusive_start();
  q_range.border_flag_.set_inclusive_end();
  c_ptr = "collect_info";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  org_scan_param.reset();
  EXPECT_EQ(org_scan_param.set(OB_INVALID_ID, str,q_range), OB_SUCCESS);

  /// basic columns
  bool is_return = false;
  c_ptr = "item_collect_count";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_column(str,is_return), OB_SUCCESS );

  is_return = false;
  c_ptr = "item_category";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_column(str,is_return), OB_SUCCESS );

  is_return = true;
  c_ptr = "item_price";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_column(str,is_return), OB_SUCCESS );

  /// composite columns
  c_ptr = "`item_collect_count`*`item_price`";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_column(str,str, is_return), OB_SUCCESS );

  /// where condition
  c_ptr = "`item_price` > 10";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_where_cond(str), OB_SUCCESS );

  /// groupby::group by columns
  is_return = false;
  c_ptr = "item_category";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.get_group_by_param().add_groupby_column(str,is_return), OB_SUCCESS);

  /// aggregate columns
  is_return = true;
  c_ptr = "item_price";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.get_group_by_param().add_aggregate_column(str,str,  SUM, is_return), OB_SUCCESS);

  /// aggregate columns
  is_return = false;
  c_ptr = "item_collect_count";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.get_group_by_param().add_aggregate_column(str,str,  SUM, is_return), OB_SUCCESS);

  /// composite columns
  c_ptr = "`item_collect_count`*`item_price`";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.get_group_by_param().add_column(str, str,is_return), OB_SUCCESS );

  /// having condtion
  c_ptr = "`item_price` > 10";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.get_group_by_param().add_having_cond(str), OB_SUCCESS );

  /// orderby
  c_ptr = "item_price";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_orderby_column(str),OB_SUCCESS);
}

TEST(ObSessionManager, basic)
{
  ObGetParam get_param(true);
  ObSessionManager mgr;
  ObCellInfo cell;
  cell.row_key_ = make_rowkey("rowkey", &allocator_);
  cell.table_name_.assign((char*)"table", static_cast<int32_t>(strlen("table")));
  cell.column_name_.assign((char*)"column", static_cast<int32_t>(strlen("column")));
  EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);
  uint64_t session_id = 0;
  uint64_t prev_session_id = 0;
  uint64_t ipport = 0;
  EXPECT_EQ(mgr.session_begin(get_param,ipport,session_id, syscall(SYS_gettid), static_cast<pid_t>(pthread_self())), OB_SUCCESS);
  EXPECT_TRUE(mgr.session_alive(session_id));
  EXPECT_EQ(mgr.session_end(session_id), OB_SUCCESS);
  EXPECT_FALSE(mgr.session_alive(session_id));

  prev_session_id = session_id;
  EXPECT_EQ(mgr.session_begin(get_param,ipport,session_id, syscall(SYS_gettid), static_cast<pid_t>(pthread_self())), OB_SUCCESS);
  EXPECT_TRUE(mgr.session_alive(session_id));
  EXPECT_NE(prev_session_id, session_id);
  EXPECT_FALSE(mgr.session_alive(prev_session_id));
  mgr.kill_session(session_id);
  EXPECT_FALSE(mgr.session_alive(session_id));

  const int64_t buf_size = 4096;
  char *buf = new char[buf_size];
  int64_t pos = 0;
  mgr.get_sessions(buf, buf_size, pos);
  EXPECT_EQ(pos, 0);

  EXPECT_EQ(mgr.session_begin(get_param,ipport,session_id, syscall(SYS_gettid), static_cast<pid_t>(pthread_self())), OB_SUCCESS);
  EXPECT_TRUE(mgr.session_alive(session_id));
  pos = 0;
  mgr.get_sessions(buf, buf_size, pos);
  TBSYS_LOG(WARN,"sessions:%s", buf);

  EXPECT_EQ(mgr.session_end(session_id), OB_SUCCESS);

  ObScanParam scan_param;
  ObStringBuf str_buf;
  init_decode_param(str_buf,scan_param);
  EXPECT_EQ(mgr.session_begin(scan_param,ipport,session_id, syscall(SYS_gettid), static_cast<pid_t>(pthread_self())), OB_SUCCESS);
  EXPECT_TRUE(mgr.session_alive(session_id));

  sleep(1);
  pos = 0;
  mgr.get_sessions(buf, buf_size, pos);
  TBSYS_LOG(WARN,"sessions:%s", buf);

  delete [] buf;
}

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
