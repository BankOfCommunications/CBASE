/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */
#ifndef _OB_ROOT_STAT_KEY_H
#define _OB_ROOT_STAT_KEY_H

namespace oceanbase
{
  namespace rootserver
  {
    enum ObRootStatKey
    {
      OB_RS_STAT_RESERVE = 0,
      OB_RS_STAT_COMMON = 1,
      OB_RS_STAT_START_TIME = 2,
      OB_RS_STAT_PROGRAM_VERSION = 3,
      OB_RS_STAT_PID = 4,
      OB_RS_STAT_LOCAL_TIME = 5,
      OB_RS_STAT_MEM = 6,
      OB_RS_STAT_RS_STATUS = 7,
      OB_RS_STAT_FROZEN_VERSION = 8,
      OB_RS_STAT_SCHEMA_VERSION = 9,
      OB_RS_STAT_LOG_SEQUENCE = 10,
      OB_RS_STAT_LOG_FILE_ID = 11,
      OB_RS_STAT_TABLE_NUM = 12,
      OB_RS_STAT_TABLET_NUM = 13,
      OB_RS_STAT_REPLICAS_NUM = 14,
      OB_RS_STAT_CS_NUM = 15,
      OB_RS_STAT_MS_NUM = 16,
      OB_RS_STAT_CS = 17,
      OB_RS_STAT_MS = 18,
      OB_RS_STAT_UPS = 19,
      OB_RS_STAT_RS_SLAVE = 20,
      OB_RS_STAT_OPS_GET = 21,
      OB_RS_STAT_OPS_SCAN = 22,
      OB_RS_STAT_RS_SLAVE_NUM = 23,
      OB_RS_STAT_FROZEN_TIME = 24,
      OB_RS_STAT_CLIENT_CONF = 25,
      OB_RS_STAT_SSTABLE_DIST = 26,
      OB_RS_STAT_FAIL_GET_COUNT = 27,
      OB_RS_STAT_FAIL_SCAN_COUNT = 28,
      OB_RS_STAT_GET_OBI_ROLE_COUNT = 29,
      OB_RS_STAT_MIGRATE_COUNT = 30,
      OB_RS_STAT_COPY_COUNT = 31,
      OB_RS_STAT_MERGE = 32,
      OB_RS_STAT_UNUSUAL_TABLETS_NUM = 33,
      OB_RS_STAT_SHUTDOWN_CS = 34,
      OB_RS_STAT_ALL_SERVER = 35,
      OB_RS_STAT_TABLE_COUNT = 36,
      OB_RS_STAT_TABLET_COUNT = 37,
      OB_RS_STAT_ROW_COUNT = 38,
      OB_RS_STAT_DATA_SIZE = 39,
      //add pangtianze [paxos rs_election] 20150731:b
      OB_RS_STAT_PAXOS_CHANGED =40,
      //add:e
      //add chujiajia [Paxos rs_election] 20151229:b
      OB_RS_STAT_QUORUM_CHANGED = 41,
      //add:e
      //add jintianyang [paxos test] 20160530:b
      OB_RS_STAT_UPS_LEADER = 42,
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161024:b
      OB_RS_STAT_MASTER_CLUSTER_ID = 43,
      //add:e
      //add bingo [Paxos rs_admin all_server_in_clusters] 20170612:b
      OB_RS_STAT_ALL_SERVER_IN_CLUSTERS = 44,
      //add:e
      OB_RS_STAT_END
    };

    extern const char* OB_RS_STAT_KEYSTR[];

  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_STAT_KEY_H */

