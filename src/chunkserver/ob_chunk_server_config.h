/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 12:58:44 fufeng.syd>
 * Version: $Id$
 * Filename: ob_chunk_server_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#ifndef _OB_CHUNK_SERVER_CONFIG_H_
#define _OB_CHUNK_SERVER_CONFIG_H_

#include <stdint.h>
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_server_config.h"
#include "common/ob_config.h"
#include "common/ob_config_helper.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_blockcache.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace chunkserver
  {
    class ObChunkServerConfig
      : public common::ObServerConfig
    {
      public:
        ObChunkServerConfig();
        virtual ~ObChunkServerConfig();

        inline const common::ObServer get_root_server() const
        { return ObServer(ObServer::IPV4, root_server_ip, (int32_t)root_server_port); }

      protected:
        common::ObRole get_server_type() const
        { return OB_CHUNKSERVER; }

      public:
        DEF_STR(datadir, "./data", "sstable data path");
        DEF_STR(appname, "", "application name");
        DEF_STR(check_compress_lib, "snappy_1.0:none:lzo_1.0", "check compress lib as cs start");

        DEF_INT(root_server_port, "0", "root server port");

        DEF_BOOL(merge_migrate_concurrency, "False", "allow doing merge and migrate concurrently");
        DEF_INT(task_queue_size, "10000", "[1000,]", "task queue size");
        DEF_INT(task_thread_count, "32", "[1,]", "task thread number");
        DEF_INT(max_migrate_task_count, "2", "[1,]", "max migrate task number");

        DEF_INT(io_thread_count, "8", "[1,]", "io thread number for libeasy");
        DEF_TIME(network_timeout, "3s", "timeout when communication with other server");
        DEF_TIME(lease_check_interval, "5s", "[5s,5s]", "lease check interval, shouldn\\'t change");

        DEF_BOOL(lazy_load_sstable, "True", "lazy load sstable to speed up cs start");
        DEF_BOOL(unmerge_if_unchanged, "True", "merge sstable depend on it\\'s changed or not");
        DEF_INT(bypass_sstable_loader_thread_num, "0", "[0,10]", "bypass sstable loead thread number");
        DEF_CAP(compactsstable_cache_size, "0", "compacet sstable cache size");
        DEF_INT(compactsstable_cache_thread_num, "0", "[0,]", "compacet sstable cache thread number");

        DEF_CAP(migrate_band_limit_per_second, "50MB", "network band limit for migration");

        DEF_CAP(merge_mem_limit, "64MB", "memory usage to merge for each thread");
        DEF_INT(merge_thread_per_disk, "2", "[1,]", "merge thread per disk, increase the number will reduce daily merge time but increase response time");
        DEF_INT(max_merge_thread_num, "10", "[1,32]", "max merge thread number");
        DEF_INT(merge_threshold_load_high, "16", "[1,]", "suspend some merge threads if system load beyond this value");
        DEF_INT(merge_threshold_request_high, "3000", "[1,]", "suspend some merge threads if get/scan number beyond this value");
        DEF_TIME(merge_delay_interval, "600s", "(0,]", "sleep time before start merge");
        DEF_TIME(merge_delay_for_lsync, "5s", "(0,)", "sleep time wait for ups synchronise frozen version if merge should read slave ups");
        DEF_BOOL(merge_scan_use_preread, "True", "prepread sstable when doing daily merge");
        DEF_TIME(datasource_timeout, "30s", "(0,)", "timeout which fetch data from datasource");
        DEF_TIME(merge_timeout, "10s", "(0,)", "fetch ups data timeout in merge");
        //add wenghaixing [secondary index static_index_build.cs_scan]20150323
        DEF_TIME(index_timeout, "10s", "(0,)", "fetch_cs_data_timeout_in_build_total_index");
        //DEF_STR(datadir, "./data", "sstable data path");
        //add e
        //add wenghaixing, [secondary index static_index_build.time_out] 20150323:b
        DEF_TIME(build_index_timeout, "1800s", "[1s,]", "build static index timeout");
        //add:e

        DEF_INT(merge_pause_row_count, "2000", "merge check after how many rows");
        DEF_TIME(merge_pause_sleep_time, "0", "sleep time for each merge check");
        DEF_TIME(merge_highload_sleep_time, "2s", "sleep time if system load beyond \\'merge_threashold_load_high\\' in merge check");
        DEF_INT(merge_adjust_ratio, "80", "when the load is greater than this ratio of merge_load_high, slow down daily merge");
        DEF_INT(max_version_gap, "3", "[1,]", "use to judge if the seving version is too old, maybe need not to merge");
        DEF_TIME(min_merge_interval, "10s", "minimal merge interval between tow merges");
        DEF_TIME(min_drop_cache_wait_time, "300s", "waiting time before drop previous version cache after merge done");
        DEF_BOOL(switch_cache_after_merge, "False", "switch cache after merge");

        DEF_BOOL(each_tablet_sync_meta, "True", "sync tablet image to index file after merge each tablet");
        DEF_INT(over_size_percent_to_split, "50", "[0,100]", "over size percent to split sstable");
        DEF_INT(choose_disk_by_space_threshold, "60", "[0,100]", "choose disk by space threshold, percent of disk utilization");
        DEF_INT(merge_write_sstable_version, "2", "[1,]", "sstable version, 2 means old sstable format, 3 means new compact sstable");

        DEF_CAP(merge_mem_size, "8MB", "memory for each sub merge round, finish that round if cell array oversize");
        DEF_CAP(max_merge_mem_size, "16MB", "clear memory over this size after each sub merge");
        DEF_CAP(groupby_mem_size, "8MB", "maxmum memory used in groupby operator");
        DEF_CAP(max_groupby_mem_size, "16MB", "clear memory over this size after groupby");
        DEF_TIME(disk_check_interval, "3s", "check disk interval");

        DEF_TIME(fetch_ups_interval, "5s", "fetch ups list interval");
        DEF_INT(ups_fail_count, "100", "[1,]", "put ups to blacklist if fail count beyond this value");
        DEF_TIME(ups_blacklist_timeout, "5s", "remove ups if it stay in blacklist over this time");

        DEF_TIME(task_left_time, "10ms", "time left to ms, drop ahead if left time less than this value");
        DEF_BOOL(write_sstable_use_dio, "True", "write sstable use dio");

        DEF_TIME(slow_query_warn_time, "500ms", "beyond this value will treated as slow query");
        DEF_CAP(block_cache_size, "1GB", "(0,)", "block cache size");
        DEF_CAP(block_index_cache_size, "512MB", "(0,)", "block index cache size");
        DEF_CAP(join_cache_size, "512MB", "join cache size");
        DEF_CAP(sstable_row_cache_size, "2GB", "[0,]", "sstable row cache size");
        DEF_INT(file_info_cache_num, "4096", "(0,]", "file info cache number");
        DEF_INT(join_batch_count, "3000", "(0,]", "join row count per round");
    };
  }
}
#endif /* _OB_CHUNK_SERVER_CONFIG_H_ */
