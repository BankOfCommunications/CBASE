/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: mock_client.cpp,v 0.1 2010/10/08 16:55:10 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "mock_client.h"
#include <gtest/gtest.h>
#include "test_helper.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_string.h"

int main(int argc, char** argv)
{
  const int32_t MOCK_SERVER_LISTEN_PORT = 8888;
  MockClient client;
  ObServer dst_host;
  const char* dst_addr = "localhost";
  dst_host.set_ipv4_addr(dst_addr, MOCK_SERVER_LISTEN_PORT);
  client.init(dst_host);

  // init cell info
  static const int64_t ROW_NUM = 1;
  static const int64_t COL_NUM = 10;

  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  char row_key_strs[ROW_NUM][50];
  uint64_t table_id = 10;
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_strs[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].row_key_.assign(row_key_strs[i], strlen(row_key_strs[i]));
      //cell_infos[i][j].op_info_.set_sem_ob();

      cell_infos[i][j].column_id_ = j + 1;

      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  // scan memtable
  ObScanner scanner;
  ObScanParam scan_param;
  //ObOperateInfo op_info;
  ObString table_name;
  ObRange range;
  const int64_t version = 999;
  range.start_key_ = cell_infos[0][0].row_key_;
  range.end_key_ = cell_infos[ROW_NUM - 1][0].row_key_;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  scan_param.set(table_id, table_name, range);
  //scan_param.set_timestamp(version);
  //scan_param.set_is_read_frozen_only(false);
  ObVersionRange version_range;
  version_range.start_version_ = version;
  version_range.end_version_ = version;
  version_range.border_flag_.set_inclusive_start();
  version_range.border_flag_.set_inclusive_end();
  scan_param.set_version_range(version_range);
  for (int64_t i = 0; i < COL_NUM; ++i)
  {
    scan_param.add_column(cell_infos[0][i].column_id_);
  }


  int64_t count = 0;
  const int64_t timeout = 10 * 1000L;

  int err = client.ups_scan(scan_param, scanner, timeout);
  fprintf(stderr, "ups_scan err=%d\n", err);

  // check result
  count = 0;
  ObScannerIterator iter;
  for (iter = scanner.begin(); iter != scanner.end(); iter++)
  {
    ObCellInfo ci;
    ObCellInfo expected = cell_infos[count / COL_NUM][count % COL_NUM];
    EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));
    check_cell(expected, ci);
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);
  fprintf(stderr, "cell_num=%ld\n", count);

  client.destroy();
  return 0;
}
