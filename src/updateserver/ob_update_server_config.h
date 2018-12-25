/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 15:59:47 fufeng.syd>
 * Version: $Id$
 * Filename: ob_update_server_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#ifndef _OB_UPDATE_SERVER_CONFIG_H_
#define _OB_UPDATE_SERVER_CONFIG_H_

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_server_config.h"
#include "ob_slave_sync_type.h"
#include "ob_ups_utils.h"

using namespace oceanbase::common;

namespace
{
  static const int64_t GB_UNIT = 1024L * 1024L * 1024L; // GB
  static const int64_t MB_UNIT = 1024L * 1024L; // MB
  //static const int64_t MIN_ACTIVE_MEMTABLE_WITH_HASH = 2L<<30;
  static const int64_t HASH_NODE_SIZE = 32;
}

namespace oceanbase
{
  namespace updateserver
  {
    class ObUpdateServerConfig
      : public common::ObServerConfig
    {
      public:
        ObUpdateServerConfig();
        virtual ~ObUpdateServerConfig();

        /* overwrite ObServerConfig functions */
        int read_config();
        int check_all();
        int init();

        int64_t get_total_memory_limit(const int64_t phy_mem_size);
        void auto_config_memory(const int64_t total_memory_limit_gb);

        const common::ObServer get_root_server() const
        { return ObServer(ObServer::IPV4, root_server_ip, (int32_t)root_server_port); }

      protected:
        common::ObRole get_server_type() const
        { return OB_UPDATESERVER; }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObUpdateServerConfig);
        int64_t total_memory_limit_bk_;

      public:
        DEF_INT(replay_worker_num, "0", "replay worker number"); /* calc later */
        DEF_INT(commit_end_thread_num, "4", "number of thread to end session and response to client");
        DEF_INT(trans_thread_num, "0", "number of thread to process read/write transaction");  /* calc later */
        DEF_CAP(memtable_hash_buckets_size, "0", "number of hash index buckets"); /* calc later */

        DEF_INT(low_prio_slow_query_time_limit, "1000000", "low priority query which execution time bigger than this will be considered as slow query");
        DEF_INT(normal_prio_slow_query_time_limit, "20000", "normal priority query which execution time bigger than this will be considered as slow query");
        DEF_INT(high_prio_quota_percent, "50", "[0,100]", "percent of CPU time quota for high priorioty task");
        DEF_INT(low_prio_quota_percent, "10", "[0,100]", "percent of CPU time quota for low priorioty task");
        DEF_INT(root_server_port, "2500", "(1024,65536)");

        DEF_STR(commit_log_dir, "data/ups_commitlog", "ups commit log directory");
        DEF_IP(lsync_ip, "0.0.0.0", "lsync ip address");
        DEF_INT(lsync_port, "3000", "(1024,65536)", "lsync listen port");

        DEF_INT(io_thread_count, "4", "io thread number for libeasy");
        DEF_INT(read_thread_count, "4", "read thread number");
        DEF_INT(store_thread_count, "3", "store thread number");
        DEF_INT(read_queue_size, "1000", "read queue size");
        DEF_INT(write_queue_size, "1000", "write queue size");
        DEF_INT(log_queue_size, "100", "log queue size");
        DEF_INT(lease_queue_size, "100", "lease queue size");
        DEF_INT(store_queue_size, "100", "store queue site");
        DEF_CAP(commit_log_size, "64MB", "commit log size");

        DEF_INT(write_thread_batch_num, "1024", "[1,]", "max wirte task count for batch");
        DEF_INT(fetch_schema_times, "10", "active fetch schema try times if fail");
        DEF_TIME(fetch_schema_timeout, "3s", "active fetch shema timeout");
        DEF_INT(resp_root_times, "20", "report frozen version to root server try times if fail");
        DEF_TIME(resp_root_timeout, "1s", "report frozen version to root server timeout");
        DEF_INT(register_times, "10", "register to root server try times if fail");
        DEF_TIME(register_timeout, "3s", "register to root server timeout");
        DEF_TIME(replay_wait_time, "100ms", "replay retry wait time");
        DEF_TIME(fetch_log_wait_time, "500ms", "fetch log retry wait time");
        DEF_TIME(log_sync_delay_warn_time_threshold, "500ms", "commit log delay beyond this value between master and slave ups will give an alarm");
        //mod wangdonghui [ups_replication_cannot_server_time configurable variable] 20170425 :b
        //DEF_TIME(log_sync_delay_tolerable_time_threshold, "5s", "commit log delay beyond this value between master and slave ups will be set to NOT_SYNC");
        DEF_TIME(log_sync_delay_tolerable_time_threshold, "10s", "commit log delay beyond this value between master and slave ups will be set to NOT_SYNC");
        //mod :e
        DEF_TIME(log_sync_delay_warn_report_interval, "10s", "commit log delay alarm given interval");
        DEF_INT(max_n_lagged_log_allowed, "10000", "commit log laged count beyond this value beyond this valud between master and slave ups will give an alarm");

        DEF_TIME(state_check_period, "50ms", "interval of slave to check sync-stat");
        DEF_INT(log_sync_type, "1", "sync log to disk");
        DEF_INT(log_sync_retry_times, "2", "log sync retry times");

        DEF_CAP(total_memory_limit, "0", "total memory limit"); /* calc later */
        DEF_CAP(table_memory_limit, "0", "table memory limit"); /* calc later */

        //mod wangdonghui [ups_replication] 500ms->600ms 20170712 :b:e
        DEF_TIME(log_sync_timeout, "600ms", "slave sync log timeout");
        DEF_TIME(packet_max_wait_time, "10s", "default rpc timeout if not timeout specified");
        DEF_TIME(trans_proc_time_warn, "1s", "if master process batch or slave write local log beyond this value, give an alarm");

        DEF_INT(inner_port, "2701", "(1024,65536)", "inner port for daily merge");
        DEF_CAP(low_priv_network_lower_limit, "30MB", "increase 1% probability to process low priority if low priority request network band less than this value and \\'low_priv_adjust_flag\\' is True");
        DEF_CAP(low_priv_network_upper_limit, "80MB", "decrease 1% probability to process low priority if low priority request network band beyond this value and \\'low_priv_adjust_flag\\' is True");
        DEF_BOOL(low_priv_adjust_flag, "True", "low priority task process probability auto adjust");
        DEF_INT(low_priv_cur_percent, "10", "[0,100]", "current low priority process probability");

        DEF_STR(store_root, "data/ups_data", "ups data directory");
        DEF_STR(raid_regex, "^raid[0-9]+$", "raid regex to find raid directory");
        DEF_STR(dir_regex, "^store[0-9]+$", "store regex to find store directory");

        DEF_CAP(blockcache_size, "0", "block cache size");       /* calc later */
        DEF_CAP(blockindex_cache_size, "0", "block index cache size"); /* calc later */
        DEF_INT(slave_sync_sstable_num, "1", "not used now ");

        DEF_CAP(active_mem_limit, "0", "active memtable memory limit"); /* calc later */
        DEF_INT(minor_num_limit, "0", "using major freeze instead if number minor version greater or equal to this value"); /* calc later */
        DEF_TIME(sstable_time_limit, "7d", "remove from memory and dump to trash directory if sstable stay in memory such time");
        DEF_STR(sstable_compressor_name, "none", "sstable compressor name");
        DEF_CAP(sstable_block_size, "64K", "sstable block size");
        DEF_MOMENT(major_freeze_duty_time, "Disable", OB_CONFIG_DYNAMIC, "major freeze duty time");
        DEF_TIME(min_major_freeze_interval, "3600s", "minimal time to generate major freeze version");
        DEF_BOOL(replay_checksum_flag, "True", "memtable checksum when replay");
        DEF_BOOL(allow_write_without_token, "True", "allow write without token");

        DEF_TIME(lsync_fetch_timeout, "5s", "fetch commit log timeout from lsync or master ups");
        DEF_TIME(refresh_lsync_addr_interval, "60s", "interval of slave to refresh lsyncserver-address");
        DEF_INT(max_row_cell_num, "32", "compact cell when cell of row beyond this value");
        DEF_CAP(table_available_warn_size, "0", "try drop frozen table if available table memory less than this value"); /* calc later */
        DEF_CAP(table_available_error_size, "0", "force drop frozen table and give an alarm if available table memory less than this value"); /* calc later */

        DEF_TIME(warm_up_time, "10m", "[10s,30m]", "sstable warm up time");
        DEF_BOOL(using_memtable_bloomfilter, "False", "using memtable bloomfilter");
        DEF_BOOL(write_sstable_use_dio, "True", "write sstable use dio");

        DEF_TIME(keep_alive_interval, "500ms", "write NOP interval");
        DEF_TIME(keep_alive_reregister_timeout, "800ms", "keep alive reregister timeout");
        DEF_TIME(keep_alive_timeout, "5s", "keep alive timeout");
        DEF_TIME(lease_timeout_in_advance, "500ms", "lease timeout in advance");
        DEF_BOOL(real_time_slave, "True", "whetch the server is a realtime slave ups");
        DEF_INT(consistency_type, "3", "[1,3]", "consistency type of log-sync, 1: strong, 2: normal, 3: weak");

        DEF_BOOL(using_static_cm_column_id, "False", "should treat 2 and 3 as create_time and modify_time column id");
        DEF_BOOL(using_hash_index, "True", "using hash index");

        DEF_INT(log_cache_n_block, "4", "number of blocks of log cache");
        DEF_CAP(log_cache_block_size, "32MB", "size of per-block of log cache");
        DEF_CAP(replay_log_buf_size, "10GB", "replay log buffer size");
        DEF_INT(replay_queue_len, "500", "replay queue size");
        //mod wangdonghui [Paxos ups_replication] 20170705:b:e 100ms->200ms
        DEF_TIME(wait_slave_sync_time, "200ms", "wait slave sync time");
//mod 0->2
        //mod pangtianze [Paxos ups_replication] 20161221:b
        //DEF_INT(wait_slave_sync_type, "2", "[0,2]", "0: response master ups before replay; 1: response master ups after replay before sync to disk; 2: response master ups after sync to disk");
        DEF_INT(wait_slave_sync_type, "1", "[0,1]", "0: response master ups before replay; 1: response master ups after sync to disk");
        //mod:e
        //add wangdonghui [ups_replication] 20170720 :b 3min
        DEF_TIME(delete_ups_wait_time, "180s", "delete slave ups wait time");
        //add :e
        DEF_TIME(disk_warn_threshold, "5ms", "disk warn threshold");
        DEF_TIME(net_warn_threshold, "5ms", "net worn threshold");

        DEF_STR(disk_delay_warn_param,  "40ms;800us;10;100000", "disk delay warn param");
        DEF_STR(net_delay_warn_param,   "50ms;5ms;5;10000",     "net delay warn param");

        DEF_INT(trans_thread_start_cpu, "-1", "start number of cpu, set affinity by transaction thread");
        DEF_INT(trans_thread_end_cpu,   "-1", "end number of cpu, set affinity by transaction thread");

        DEF_INT(io_thread_start_cpu, "-1", "start number of cpu, set affinity by io thread");
        DEF_INT(io_thread_end_cpu,   "-1", "end number of cpu, set affinity by io thread");

        DEF_INT(commit_bind_core_id, "-1", "commit thread will bind to this core(given configured core_id > 0)");

        DEF_INT(replay_thread_start_cpu, "-1", "start number of cpu, set affinity by replay thread");
        DEF_INT(replay_thread_end_cpu,   "-1", "end number of cpu, set affinity by replay thread");
        //add shili [LONG_TRANSACTION_LOG]  20160926:b
        DEF_CAP(max_mutator_size, "32MB", "max  mutator size");
        //add e
        //add wangjiahao [Paxos ups_replication] 20150601:b
        DEF_INT(quorum_scale,   "1", "the count of ups make a quorum group, 1 for sigle ups, larger than 1 will run quorum logging");
        //add:e
	    //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
        DEF_TIME(lock_wait_time, "3s", "lock wait time");
        //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e

    };
  }
}

#endif /* _OB_UPDATE_SERVER_CONFIG_H_ */
