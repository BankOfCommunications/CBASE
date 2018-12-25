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

#include "rootserver/ob_root_stat_key.h"
#include <cstdlib>

const char* oceanbase::rootserver::OB_RS_STAT_KEYSTR[]=
{
  "reserve",                    // 0
  "common",                     // 1
  "start_time",                 // 2
  "prog_version",               // 3
  "pid",                        // 4
  "local_time",                 // 5
  "mem",                        // 6
  "rs_status",                  // 7
  "frozen_version",             // 8
  "schema_version",             // 9
  "log_id",                     // 10
  "log_file_id",                // 11
  "table_num",                  // 12
  "tablet_num",                 // 13
  "replicas_num",               // 14
  "cs_num",                     // 15
  "ms_num",                     // 16
  "cs",                         // 17
  "ms",                         // 18
  "ups",                        // 19
  "rs_slave",                   // 20
  "ops_get",                    // 21
  "ops_scan",                   // 22
  "rs_slave_num",               // 23
  "frozen_time",                // 24
  "client_conf",                // 25
  "sstable_dist",               // 26
  "fail_get_count",             // 27
  "fail_scan_count",            // 28
  "get_obi_role_count",         // 29
  "migrate_count",              // 30
  "copy_count",                 // 31
  "merge",                      // 32
  "unusual_tablets_num",        // 33
  "shutdown_cs",                // 34
  "all_server",                 // 35
  "table_count",                // 36
  "tablet_count",               // 37
  "row_count",                  // 38
  "data_size",                  // 39
  //mod pangtianze [Paxos rs_election] 20161010:b
  /*
  //add pangtianze [Paxos rs_election] 20150731:b
  "paxos_num_changed",          //
  "rs_leader",                  //
  //add:e
  //add chujiajia [Paxos rs_election] 20151230:b
  "quorum_scale_changed",        //
  //add:e
  */
  "rs_paxos_num",          // 40
  "ups_quorum_scale",        // 41
  //mod:e
  //add jintianyang [paxos test] 20160530:b
  "ups_leader",        // 42
  //add:e
  //add jintianyang [paxos test] 20160530:b
  "master_cluster_id",        // 43
  //add:e
  //add bingo [Paxos rs_admin all_server_in_clusters] 20170612:b
  "all_server_in_clusters",    //44
  //add:e
  NULL
};

