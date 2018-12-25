/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-05-15 09:16:27 fufeng.syd>
 * Version: $Id$
 * Filename: ob_root_server_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 */

#ifndef _OB_ROOT_SERVER_CONFIG_H_
#define _OB_ROOT_SERVER_CONFIG_H_

#include "common/ob_server_config.h"
#include "common/ob_config.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootServerConfig
      : public common::ObServerConfig
    {
      public:
        int get_root_server(ObServer &server) const;
        int get_master_root_server(ObServer &server) const;

      protected:
        common::ObRole get_server_type() const
        { return OB_ROOTSERVER; }

      public:
        DEF_TIME(cs_lease_duration_time, "10s", "cs lease duration time");
        DEF_TIME(migrate_wait_time, "60s", "waiting time for balance thread to start work");
        DEF_TIME(safe_lost_one_time, "3600s", "safe duration while lost one copy");
        DEF_TIME(safe_wait_init_time, "60s", "time interval for build  root table");
        DEF_BOOL(create_table_in_init, "False", "create tablet switch while init");
        DEF_BOOL(ddl_system_table_switch, "False", "true: allow to create or drop system tables. false: not allow to create or drop system tables");
        DEF_BOOL(monitor_row_checksum, "True", "compare row checksum between master cluster and slave master");
        DEF_TIME(monitor_row_checksum_interval, "1800s", "compare row checksum between master cluster and slave master");
        DEF_TIME(monitor_row_checksum_timeout, "3s", "get master checksum timeout");
        DEF_TIME(monitor_drop_table_interval, "600s", "[1s,]", "delete droped table in daily merge check interval");
        //add liumz, [secondary index static_index_build] 20150323:b
        DEF_TIME(monitor_create_index_timeout, "3600s", "[1s,]", "create single static index timeout");
        //add:e
        DEF_BOOL(monitor_column_checksum, "True", "wether compare column checksum or not");
        DEF_INT(tablet_replicas_num, "3", "[1,3]", "tablet replicas num");
        DEF_INT(io_thread_count, "4", "[1,100]", "io thread count");
        DEF_INT(read_thread_count, "20", "[10,100]", "read thread count");
        DEF_INT(read_queue_size, "10000", "[10,100000]", "read queue size");
        DEF_INT(write_queue_size, "10000", "[10,100000]", "write queue size");
        DEF_INT(log_queue_size, "10000", "[10,100000]", "log queue size");
        //add pangtianze [Paxos rs_election] 20150619:b
        DEF_INT(election_queue_size, "10000", "[10,100000]", "election queue size");
        //add:e
        //add pangtianze [Paxos rs_election] 20150721:b
        DEF_INT(rs_paxos_number, "0", "[0,]", "rootserver paxos number, used in rs election");
        DEF_INT(ups_quorum_scale,   "0", "[0,]", "until log replicas are more than this number, master commit log");
        //add:e
        //add pangtianze [Paxos rs_election] 20161020:b
        DEF_BOOL(use_paxos, "True", "rootserver use paxos");
        //add:e
        DEF_TIME(network_timeout, "3s", "network timeout for rpc process");
        DEF_TIME(inner_table_network_timeout, "50s", "network timeout for interal table access process");
        DEF_BOOL(lease_on, "True", "lease switch between master and slave rootserver");
        //mod pangtianze [Paxos rs_election] 20160918:b
        //DEF_TIME(lease_interval_time, "15s", "lease interval time");
         DEF_TIME(lease_interval_time, "5s", "lease interval time");
        //mod:e
        DEF_TIME(lease_reserved_time, "10s", "lease reserved time");
        //mod:e
        //add pangtianze [Paxos rs_election] 20160918:b
        DEF_TIME(leader_lease_interval_time, "1s", "lease interval time");
        //no_hb_response_duration_time + leader_lease_interval_time < lease_interval_time
        DEF_TIME(no_hb_response_duration_time, "3s", "allowed duration time without recevie heartbeart responses");
        //add:e
        //add huangjianwei [Paxos rs_switch] 20160727:b
        //mod pangtianze [Paxos] 20170628, change 10s -> 30s
        DEF_TIME(server_status_refresh_time,"30s","server status refresh time");
        //add:e
        DEF_TIME(slave_register_timeout, "3s", "slave register rpc timeout");
        DEF_TIME(log_sync_timeout, "3s", "log sync rpc timeout");
        DEF_TIME(vip_check_period, "500ms", "vip check period");
        DEF_TIME(log_replay_wait_time, "100ms", "log replay wait time");
        DEF_CAP(log_sync_limit, "40MB", "log sync limit");
        DEF_TIME(max_merge_duration_time, "2h", "max merge duration time");
        DEF_TIME(cs_probation_period, "5s", "duration before cs can adopt migrate");

        DEF_TIME(ups_lease_time, "9s", "ups lease time");
        DEF_TIME(ups_lease_reserved_time, "8500ms", "ups lease reserved time");
        DEF_TIME(ups_renew_reserved_time, "7770ms", "ups renew reserved time");
        ///need greater than ups lease
        //mod pangtianze [Paxos] 20170427:b
        //DEF_TIME(ups_waiting_register_time, "25s", "rs select master ups, should wait the time after first ups regist");
        DEF_TIME(ups_waiting_register_time, "15s", "rs select master ups, should wait the time after first ups regist");
        //mod:e
        DEF_TIME(expected_request_process_time, "10ms", "expected request process time, check before pushing in queue");
        // balance related flags
        DEF_TIME(balance_worker_idle_time, "30s", "[1s,]", "balance worker idle wait time");
        DEF_TIME(import_rpc_timeout, "60s", "[30s,]", "import rpc timeout, used in fetch range list and import");
        DEF_INT(balance_tolerance_count, "10", "[1,1000]", "tolerance count for balance");
        DEF_TIME(balance_timeout_delta, "10s", "balance timeout delta");
        DEF_TIME(balance_max_timeout, "5m", "max timeout for one group of balance task");
        DEF_INT(balance_max_concurrent_migrate_num, "10", "[1,1024]", "max concurrent migrate num for balance");
        DEF_INT(balance_max_migrate_out_per_cs, "2", "[1,10]", "max migrate out per cs");
        DEF_INT(balance_max_migrate_in_per_cs, "2", "[1,10]", "max migrate in per cs");
        DEF_BOOL(enable_balance, "True", "balance switch");
        DEF_BOOL(enable_rereplication, "True", "rereplication switch");

        DEF_BOOL(enable_load_data, "False", "load data switch");
        DEF_TIME(load_data_check_status_interval, "30s", "load data switch");
        DEF_INT(data_source_max_fail_count, "10", "max data source fail count");
        DEF_TIME(load_data_max_timeout_per_range, "3m", "max timeout for cs fetch the data per range");

        DEF_TIME(tablet_migrate_disabling_period, "60s", "cs can participate in balance after regist");
        DEF_BOOL(enable_new_root_table, "False", "new root table switch");
        DEF_INT(obconnector_port, "5433", "obconnector port");
        DEF_TIME(delete_table_time, "10m", "time for cs to delete one table while bypass");
        DEF_TIME(load_sstable_time, "20m", "time for cs to load one table while bypass");
        DEF_TIME(report_tablet_time, "5m", "time for cs to report tablets while bypass");
        DEF_TIME(build_root_table_time, "5m", "time for cs to build new root_table while bypass");

        DEF_BOOL(enable_cache_schema, "True", "cache schema switch");
        DEF_BOOL(is_import, "False", "is import application");
        DEF_BOOL(is_ups_flow_control, "False", "ups flow control switch");
        DEF_INT(read_master_master_ups_percent, "50", "[0,100]", "master master ups read percent");
        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        /* no slave_master_ups anymore */
        //DEF_INT(read_slave_master_ups_percent, "50", "[0,100]", "slave master ups read percent");
        //del:e
        //add pangtianze [Paxos Cluster.Flow.UPS] 20170119:b
        DEF_INT(is_strong_consistency_read, "0", "[0,1]", "strong consistency read, cs only get data in master ups");
        //DEF_BOOL(is_strong_consistency_read, "False", "strong consistency read, cs only get data in master ups");
        //add:e
        DEF_CAP(max_commit_log_size, "64MB", "max commit log size");
        DEF_INT(commit_log_sync_type, "1", "commit log sync type");

        DEF_STR(rs_data_dir, "data/rs", "root server data directory");
        DEF_STR(commit_log_dir, "data/rs_commitlog", "root server commit log directory");
        DEF_STR(first_meta_filename, "first_tablet_meta", "first meta file name");
        DEF_STR(schema_filename, "etc/schema.ini", "schame file name");

        DEF_IP(master_root_server_ip, "0.0.0.0", "master OceanBase instance vip address");
        DEF_INT(master_root_server_port, "0", "master OceanBase instance listen port");

        //add zhaoqiong[roottable tablet management]20160223:b
        DEF_BOOL(enable_rereplication_in_table_level, "True", "table_level rereplication switch");
        //add:e
        //add pangtianze [Paxos Cluster.Balance] 20170620:b
        DEF_STR(cluster_replica_num, "0;0;0;0;0;0", "tablet replica number in each cluster");
        //add:e
    };
  }
}
#endif /* _OB_ROOT_SERVER_CONFIG_H_ */
