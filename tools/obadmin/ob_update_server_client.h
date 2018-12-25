/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 
 *
 * Authors:
 *   Shi Yudi <fufeng.syd@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef _OCEANBASE_TOOLS_UPS_CLIENT_RPC_H_
#define _OCEANBASE_TOOLS_UPS_CLIENT_RPC_H_

#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_server.h"
#include "common/ob_schema.h"
#include "common/ob_result.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_mutator.h"
#include "common/thread_buffer.h"
#include "common/ob_bloomfilter.h"
#include "common/ob_login_mgr.h"
#include "common/ob_token.h"
#include "common/ob_log_cursor.h"
#include "updateserver/ob_ups_utils.h"
#include "updateserver/ob_ups_stat.h"
#include "updateserver/ob_store_mgr.h"
#include "updateserver/ob_log_sync_delay_stat.h"
#include "ob_server_client.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
extern const char *print_obj(const ObObj &obj);

namespace oceanbase {
  namespace tools {
    class ObUpsClientRpc : public ObServerClient
    {
      public:
        ObUpsClientRpc() {}
        ~ObUpsClientRpc() {}

      public:
        int ob_login(const ObLoginInfo &login_info, ObToken &token, const int64_t timeout) {
          return send_request(OB_LOGIN, login_info, token, timeout);
        }

        int delay_drop_memtable(const int64_t timeout) {
          return send_command(OB_UPS_DELAY_DROP_MEMTABLE, timeout);
        }
        int immediately_drop_memtable(const int64_t timeout) {
          return send_command(OB_UPS_IMMEDIATELY_DROP_MEMTABLE, timeout);
        }
        int enable_memtable_checksum(const int64_t timeout) {
          return send_command(OB_UPS_ENABLE_MEMTABLE_CHECKSUM, timeout);
        }
        int disable_memtable_checksum(const int64_t timeout) {
          return send_command(OB_UPS_DISABLE_MEMTABLE_CHECKSUM, timeout);
        }
        int get_table_time_stamp(const uint64_t major_version, int64_t &time_stamp, const int64_t timeout) {
          return send_request(OB_UPS_GET_TABLE_TIME_STAMP, major_version, time_stamp, timeout);
        }

        int switch_commit_log(uint64_t &new_log_file_id, const int64_t timeout) {
          return send_request(OB_UPS_SWITCH_COMMIT_LOG, new_log_file_id, timeout);
        }
        int reload_conf(const char* fname, const int64_t timeout) {
          ObString str;
          str.assign_ptr(const_cast<char*>(fname), static_cast<int32_t>(strlen(fname)));
          return send_command(OB_UPS_RELOAD_CONF, str, timeout);
        }
        int get_max_clog_id(ObLogCursor& log_cursor, const int64_t timeout) {
          return send_request(OB_GET_CLOG_CURSOR, log_cursor, timeout);
        }
        int get_clog_master(ObServer& server, const int64_t timeout) {
          return send_request(OB_GET_CLOG_MASTER, server, timeout);
        }
        int get_log_sync_delay_stat(ObLogSyncDelayStat& delay_stat, const int64_t timeout) {
          return send_request(OB_GET_LOG_SYNC_DELAY_STAT, delay_stat, timeout);
        }
        int get_bloomfilter(const int64_t version, TableBloomFilter &table_bf, const int64_t timeout) {
          return send_request(OB_UPS_GET_BLOOM_FILTER, (const uint64_t)version, table_bf, timeout);
        }
        int store_memtable(const int64_t store_all, const int64_t timeout) {
          return send_command(OB_UPS_STORE_MEM_TABLE, store_all, timeout);
        }
        int reload_store(const oceanbase::updateserver::StoreMgr::Handle store_handle, const int64_t timeout) {
          return send_command(OB_UPS_RELOAD_STORE, store_handle, timeout);
        }
        int drop(const int64_t timeout) {
          return send_command(OB_UPS_DROP_MEM_TABLE, timeout);
        }
        int fetch_schema(const int64_t version, ObSchemaManagerV2 &schema_mgr, const int64_t timeout) {
          return send_request(OB_FETCH_SCHEMA, version, schema_mgr, timeout);
        }
        int force_fetch_schema(const int64_t timeout) {
          return send_command(OB_UPS_FORCE_FETCH_SCHEMA, timeout);
        }
        int get_last_frozen_version(int64_t &version, const int64_t timeout) {
          return send_request(OB_UPS_GET_LAST_FROZEN_VERSION, version, timeout);
        }
        int fetch_ups_stat_info(oceanbase::updateserver::UpsStatMgr &stat_mgr, const int64_t timeout) {
          return send_request(OB_FETCH_STATS, stat_mgr, timeout);
        }
        int memory_watch(oceanbase::updateserver::UpsMemoryInfo &memory_info, const int64_t timeout) {
          return send_request(OB_UPS_MEMORY_WATCH, memory_info, timeout);
        }
        int memory_limit(const oceanbase::updateserver::UpsMemoryInfo &input,
                         oceanbase::updateserver::UpsMemoryInfo &output, const int64_t timeout) {
          return send_request(OB_UPS_MEMORY_LIMIT_SET, input, output, timeout);
        }
        int priv_queue_conf(const oceanbase::updateserver::UpsPrivQueueConf &input,
                            oceanbase::updateserver::UpsPrivQueueConf &output, const int64_t timeout) {
          return send_request(OB_UPS_PRIV_QUEUE_CONF_SET, input, output, timeout);
        }
        int reset_vip(const char* vip, const int64_t timeout) {
          ObString str;
          str.assign_ptr(const_cast<char*>(vip), static_cast<int32_t>(strlen(vip)));
          return send_command(OB_UPS_CHANGE_VIP_REQUEST, str, timeout);
        }
        int dump_memtable(const char *dump_dir, const int64_t timeout) {
          ObString str;
          str.assign_ptr(const_cast<char*>(dump_dir), static_cast<int32_t>(strlen(dump_dir)));
          return send_command(OB_UPS_DUMP_TEXT_MEMTABLE, str, timeout);
        }
        int umount_store(const char *store_dir, const int64_t timeout) {
          ObString str;
          str.assign_ptr(const_cast<char*>(store_dir), static_cast<int32_t>(strlen(store_dir)));
          return send_command(OB_UPS_UMOUNT_STORE, str, timeout);
        }
        int dump_schemas(const int64_t timeout) {
          return send_command(OB_UPS_DUMP_TEXT_SCHEMAS, timeout);
        }
        int clear_active_memtable(const int64_t timeout) {
          return send_command(OB_UPS_CLEAR_ACTIVE_MEMTABLE, timeout);
        }
        int load_new_store(const int64_t timeout) {
          return send_command(OB_UPS_LOAD_NEW_STORE, timeout);
        }
        int reload_all_store(const int64_t timeout) {
          return send_command(OB_UPS_RELOAD_ALL_STORE, timeout);
        }
        int erase_sstable(const int64_t timeout) {
          return send_command(OB_UPS_ERASE_SSTABLE, timeout);
        }
        int drop_memtable(const int64_t timeout) {
          return send_command(OB_UPS_DROP_MEM_TABLE, timeout);
        }
        int force_report_frozen_version(const int64_t timeout) {
          return send_command(OB_UPS_FORCE_REPORT_FROZEN_VERSION, timeout);
        }
        int freeze(const int64_t timeout, const bool major_freeze) {
          PacketCode pcode = OB_UPS_MINOR_FREEZE_MEMTABLE;
          if (major_freeze)
          {
            pcode = OB_FREEZE_MEM_TABLE;
          }
          return send_command(pcode, timeout);
        }
        int ups_apply(const ObMutator &mutator, const int64_t timeout) {
          return send_command(OB_WRITE, mutator, timeout);
        }
        int ups_get(const ObGetParam& get_param, ObScanner& scanner, const int64_t timeout) {
          return send_request(OB_GET_REQUEST, get_param, scanner, timeout);
        }
        int ups_scan(ObScanParam& scan_param, ObScanner& scanner, const int64_t timeout) {
          return send_request(OB_SCAN_REQUEST, scan_param, scanner, timeout);
        }
    }; // class ObUpsClientRpc

  } // end of namespace tools
} // end of namespace oceanbase

#endif // _OCEANBASE_TOOLS_UPS_CLIENT_RPC_H_
