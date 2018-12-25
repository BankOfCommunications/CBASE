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
#include <getopt.h>
#include <unistd.h>
#include <locale.h>
#include "tbsys.h"
#include "mock_client.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "common/ob_ms_list.h"
#include "common/ob_ups_info.h"
#include "updateserver/ob_log_sync_delay_stat.h"
#include "updateserver/ob_session_mgr.h"
#include "updateserver/ob_update_server_config.h"
#include "test_utils.h"

int64_t timeout = 30 * 1000 * 1000L;

void print_scanner(ObScanner &scanner)
{
  int64_t row_counter = 0;
  while (OB_SUCCESS == scanner.next_cell())
  {
    ObCellInfo *ci = NULL;
    bool is_row_changed = false;
    scanner.get_cell(&ci, &is_row_changed);
    fprintf(stdout, "%s\n", common::print_cellinfo(ci, "CLI_SCAN"));
    if (is_row_changed)
    {
      row_counter++;
    }
  }
  bool is_fullfilled = false;
  int64_t fullfilled_num = 0;
  ObRowkey last_rk;
  scanner.get_last_row_key(last_rk);
  scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
  fprintf(stdout, "is_fullfilled=%s fullfilled_num=%ld row_counter=%ld last_row_key=[%s]\n",
          STR_BOOL(is_fullfilled), fullfilled_num, row_counter, to_cstring(last_rk));
}

void print_scanner(const char *fname)
{
  int fd = open(fname, O_RDONLY);
  if (-1 == fd)
  {
    fprintf(stderr, "open file [%s] fail errno=%u\n", fname, errno);
  }
  else
  {
    struct stat st;
    fstat(fd, &st);
    char *buffer = new char[st.st_size];
    if (NULL == buffer)
    {
      fprintf(stderr, "new buffer fail size=%ld\n", st.st_size);
    }
    else
    {
      int64_t read_ret = read(fd, buffer, st.st_size);
      if (st.st_size != read_ret)
      {
        fprintf(stderr, "read file fail ret=%ld size=%ld errno=%u\n", read_ret, st.st_size, errno);
      }
      else
      {
        int64_t pos = 0;
        ObScanner scanner;
        int ret = scanner.deserialize(buffer, st.st_size, pos);
        if (OB_SUCCESS != ret)
        {
          fprintf(stderr, "deserialize fail ret=%d\n", ret);
        }
        else
        {
          print_scanner(scanner);
        }
      }
      delete[] buffer;
    }
    close(fd);
  }
}

void print_schema(const ObSchemaManagerV2 &sm)
{
  uint64_t cur_table_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *iter = NULL;
  for (iter = sm.column_begin(); iter != sm.column_end(); iter++)
  {
    if (NULL != iter)
    {
      if (iter->get_table_id() != cur_table_id)
      {
        const ObTableSchema *table_schema = sm.get_table_schema(iter->get_table_id());
        if (NULL != table_schema)
        {
          fprintf(stdout, "[TABLE_SCHEMA] table_id=%lu table_type=%d table_name=%s split_pos=%d rowkey_max_length=%d\n",
              iter->get_table_id(), table_schema->get_table_type(), table_schema->get_table_name(),
              table_schema->get_split_pos(), table_schema->get_rowkey_max_length());
        }
        else
        {
          fprintf(stdout, "[TABLE_SCHEMA] table_id=%lu\n", iter->get_table_id());
        }
        cur_table_id = iter->get_table_id();
      }
      fprintf(stdout, "              [COLUMN_SCHEMA] column_id=%lu column_name=%s column_type=%d size=%ld\n",
          iter->get_id(), iter->get_name(), iter->get_type(), iter->get_size());
    }
  }
}

void print_schema(const char *fname)
{
  int fd = open(fname, O_RDONLY);
  if (-1 == fd)
  {
    fprintf(stderr, "open file [%s] fail errno=%u\n", fname, errno);
  }
  else
  {
    struct stat st;
    fstat(fd, &st);
    char *buffer = new char[st.st_size];
    if (NULL == buffer)
    {
      fprintf(stderr, "new buffer fail size=%ld\n", st.st_size);
    }
    else
    {
      int64_t read_ret = read(fd, buffer, st.st_size);
      if (st.st_size != read_ret)
      {
        fprintf(stderr, "read file fail ret=%ld size=%ld errno=%u\n", read_ret, st.st_size, errno);
      }
      else
      {
        ObSchemaManagerV2 *sm = new(std::nothrow) ObSchemaManagerV2();
        if (NULL == sm)
        {
          fprintf(stdout, "[%s] new ObSchemaManagerV2 fail\n", __FUNCTION__);
        }
        else
        {
          int64_t pos = 0;
          int ret = sm->deserialize(buffer, st.st_size, pos);
          if (OB_SUCCESS != ret)
          {
            fprintf(stderr, "deserialize fail ret=%d\n", ret);
          }
          else
          {
            print_schema(*sm);
          }
          delete sm;
        }
      }
      delete[] buffer;
    }
    close(fd);
  }
}

void ups_show_sessions(MockClient &client)
{
  ObNewScanner scanner;
  int err = client.ups_show_sessions(scanner, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err)
  {
    SessionMgr sm;
    ShowSessions ss(sm, scanner);
    scanner.set_default_row_desc(&(ss.get_row_desc()));
    ObRow row;
    while (OB_SUCCESS == scanner.get_next_row(row))
    {
      fprintf(stdout, "%s\n", to_cstring(row));
    }
  }
}

void ups_kill_session(const uint32_t session_descriptor, MockClient &client)
{
  int err = client.ups_kill_session(session_descriptor, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

int minor_load_bypass(MockClient &client)
{
  int64_t loaded_num = 0;
  int err = client.minor_load_bypass(loaded_num, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  fprintf(stdout, "loaded_num=%ld\n", loaded_num);
  return err;
}

int major_load_bypass(MockClient &client)
{
  int64_t loaded_num = 0;
  int err = client.major_load_bypass(loaded_num, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  fprintf(stdout, "loaded_num=%ld\n", loaded_num);
  return err;
}

void delay_drop_memtable(MockClient &client)
{
  int err = client.delay_drop_memtable(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void immediately_drop_memtable(MockClient &client)
{
  int err = client.immediately_drop_memtable(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void enable_memtable_checksum(MockClient &client)
{
  int err = client.enable_memtable_checksum(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void disable_memtable_checksum(MockClient &client)
{
  int err = client.disable_memtable_checksum(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void get_table_time_stamp(MockClient &client, const uint64_t major_version)
{
  int64_t time_stamp = 0;
  int err = client.get_table_time_stamp(major_version, time_stamp, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err)
  {
    fprintf(stdout, "major_version=%lu time_stamp=%ld %s\n", major_version, time_stamp, time2str(time_stamp));
  }
}

void switch_commit_log(MockClient &client)
{
  uint64_t new_log_file_id = 0;
  int err = client.switch_commit_log(new_log_file_id, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err)
  {
    fprintf(stdout, "new_log_file_id=%lu\n", new_log_file_id);
  }
}

void load_new_store(MockClient &client)
{
  int err = client.load_new_store(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void reload_all_store(MockClient &client)
{
  int err = client.reload_all_store(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void reload_store(MockClient &client, int64_t store_handle)
{
  int err = client.reload_store(store_handle, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void umount_store(MockClient &client, const char *umount_dir)
{
  int err = client.umount_store(umount_dir, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void force_report_frozen_version(MockClient &client)
{
  int err = client.force_report_frozen_version(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void get_slave_ups_info(MockClient &client)
{
  char *buff = NULL;
  buff = new char[OB_MAX_PACKET_LENGTH];
  buff[OB_MAX_PACKET_LENGTH - 1] = '\0';
  if (NULL != buff)
  {
    int err = client.get_slave_ups_info(buff, OB_MAX_PACKET_LENGTH, timeout);
    fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
    delete [] buff;
  }
}
void erase_sstable(MockClient &client)
{
  int err = client.erase_sstable(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void store_memtable(MockClient &client, int64_t store_all)
{
  int err = client.store_memtable(store_all, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void get_bloomfilter(MockClient &client, int64_t version)
{
  oceanbase::common::TableBloomFilter table_bf;
  int err = client.get_bloomfilter(version, table_bf, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void fetch_ups_stat_info(MockClient &client)
{
  oceanbase::updateserver::UpsStatMgr stat_mgr;
  int err = client.fetch_ups_stat_info(stat_mgr, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err)
  {
    stat_mgr.print_info();
  }
}

void get_last_frozen_version(MockClient &client)
{
  int64_t version = 0;
  int err = client.get_last_frozen_version(version, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err)
  {
    fprintf(stdout, "last_frozen_version=%ld\n", version);
  }
}

void clear_active_memtable(MockClient &client)
{
  int err = client.clear_active_memtable(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void memory_watch(MockClient &client)
{
  UpsMemoryInfo memory_info;
  int err = client.memory_watch(memory_info, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err)
  {
    fprintf(stdout, "memory_watch err=%d\n", err);
    fprintf(stdout, "memory_info_version=%ld\n", memory_info.version);
    fprintf(stdout, "total_size\t=% '15ld\n", memory_info.total_size);
    fprintf(stdout, "cur_limit_size\t=% '15ld\n", memory_info.cur_limit_size);
    fprintf(stdout, "memtable_used\t=% '15ld\n", memory_info.table_mem_info.memtable_used);
    fprintf(stdout, "memtable_total\t=% '15ld\n", memory_info.table_mem_info.memtable_total);
    fprintf(stdout, "memtable_limit\t=% '15ld\n", memory_info.table_mem_info.memtable_limit);
  }
}

void memory_limit(const char *memory_limit, const char *memtable_limit, MockClient &client)
{
  UpsMemoryInfo param;
  UpsMemoryInfo memory_info;
  param.cur_limit_size = (NULL == memory_limit) ?  0 : atoll(memory_limit);
  param.table_mem_info.memtable_limit = (NULL == memtable_limit) ? 0 : atoll(memtable_limit);
  int err = client.memory_limit(param, memory_info, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err)
  {
    fprintf(stdout, "memory_limit err=%d\n", err);
    fprintf(stdout, "memory_info_version=%ld\n", memory_info.version);
    fprintf(stdout, "total_size\t=% '15ld\n", memory_info.total_size);
    fprintf(stdout, "cur_limit_size\t=% '15ld\n", memory_info.cur_limit_size);
    fprintf(stdout, "memtable_used\t=% '15ld\n", memory_info.table_mem_info.memtable_used);
    fprintf(stdout, "memtable_total\t=% '15ld\n", memory_info.table_mem_info.memtable_total);
    fprintf(stdout, "memtable_limit\t=% '15ld\n", memory_info.table_mem_info.memtable_limit);
  }
}

void priv_queue_conf(const char* conf_str, MockClient &client)
{
  UpsPrivQueueConf param;
  UpsPrivQueueConf priv_queue_conf;
  sscanf(conf_str, "%ld%ld%ld%ld", &param.low_priv_network_lower_limit,
      &param.low_priv_network_upper_limit, &param.low_priv_adjust_flag,
      &param.low_priv_cur_percent);

  int err = client.priv_queue_conf(param, priv_queue_conf, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err)
  {
    fprintf(stdout, "priv_queue_conf err=%d\n", err);
    fprintf(stdout, "low_priv_network_lower_limit=%ld\n", priv_queue_conf.low_priv_network_lower_limit);
    fprintf(stdout, "low_priv_network_upper_limit=%ld\n", priv_queue_conf.low_priv_network_upper_limit);
    fprintf(stdout, "low_priv_adjust_flag=%ld\n", priv_queue_conf.low_priv_adjust_flag);
    fprintf(stdout, "low_priv_cur_percent=%ld\n", priv_queue_conf.low_priv_cur_percent);
  }
}

void dump_memtable(const char *dump_dir, MockClient &client)
{
  if (NULL == dump_dir)
  {
    dump_dir = "/tmp";
  }
  int err = client.dump_memtable(dump_dir, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  fprintf(stdout, "dump dest=[%s]\n", dump_dir);
}

void dump_schemas(MockClient &client)
{
  int err = client.dump_schemas(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void force_fetch_schema(MockClient &client)
{
  int err = client.force_fetch_schema(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void get_clog_status(MockClient &client)
{
  int err = OB_SUCCESS;
  ObUpsCLogStatus stat;
  char buf[OB_MAX_RESULT_MESSAGE_LENGTH];
  TBSYS_LOG(ERROR, "timeout=%ld", timeout);
  if (OB_SUCCESS != (err = client.get_clog_status(stat, timeout)))
  {
    TBSYS_LOG(ERROR, "client.get_clog_status()=>%d", err);
  }
  else if (OB_SUCCESS != (err = stat.to_str(buf, sizeof(buf))))
  {
    TBSYS_LOG(ERROR, "stat.to_str()=>%d", err);
  }
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err)
  {
    fprintf(stdout, "clog status:\n%s\n", buf);
  }
  exit(err);
}

void get_max_log_seq(MockClient &client)
{
  int err = OB_SUCCESS;
  int64_t log_id = 0;
  if (OB_SUCCESS != (err = client.get_max_log_seq(log_id, timeout)))
  {
    TBSYS_LOG(ERROR, "client.get_max_log_id_replayable()=>%d", err);
  }
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  fprintf(stdout, "max_log_seq=%ld\n", log_id);
}

void get_clog_cursor(MockClient &client)
{
  int err = OB_SUCCESS;
  ObLogCursor log_cursor;
  if (OB_SUCCESS != (err = client.get_max_clog_id(log_cursor, timeout)))
  {
    TBSYS_LOG(ERROR, "client.get_max_clog_id()=>%d", err);
  }
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  fprintf(stdout, "log_file_id=%lu, log_seq_id=%lu, log_offset=%lu\n", log_cursor.file_id_, log_cursor.log_id_, log_cursor.offset_);
}

void get_clog_master(MockClient &client)
{
  ObServer server;
  int err = client.get_clog_master(server, timeout);
  char addr[256];
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  server.to_string(addr, sizeof(addr));
  fprintf(stdout, "%s\n", addr);
}

void get_log_sync_delay_stat(MockClient &client)
{
  ObLogSyncDelayStat delay_stat;
  int err = client.get_log_sync_delay_stat(delay_stat, timeout);

  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  time_t tm = delay_stat.get_last_replay_time_us()/1000000;
  char* str_time = ctime(&tm);
  str_time[strlen(str_time)-1] = 0;
  fprintf(stdout, "log_sync_delay: last_log_id=%ld, total_count=%ld, total_delay=%ldus, max_delay=%ldus, last_replay_time=%ldus [%s]\n",
          delay_stat.get_last_log_id(), delay_stat.get_mutator_count(), delay_stat.get_total_delay_us(),
          delay_stat.get_max_delay_us(), delay_stat.get_last_replay_time_us(),
          str_time);
}

void reload_conf(const char* fname, MockClient &client)
{
  if (NULL == fname)
  {
    fname = "./default.conf";
  }
  int err = client.reload_conf(fname, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  fprintf(stdout, "relaod fname=[%s]\n", fname);
}

void apply(const char *fname, PageArena<char> &allocer, MockClient &client)
{
  ObMutator mutator;
  ObMutator result;
  read_cell_infos(fname, CELL_INFOS_SECTION, allocer, mutator, result);
  int err = client.ups_apply(mutator, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void total_scan(const char *fname, PageArena<char> &allocer, MockClient &client, const char *version_range)
{
  ObScanner scanner;
  ObScanParam scan_param;
  read_scan_param(fname, SCAN_PARAM_SECTION, allocer, scan_param);
  scan_param.set_version_range(str2range(version_range));
  scan_param.set_is_read_consistency(false);

  int64_t total_fullfilled_num = 0;
  int64_t total_row_counter = 0;
  int64_t total_timeu = 0;
  while (true)
  {
    int64_t timeu = tbsys::CTimeUtil::getTime();
    int err = client.ups_scan(scan_param, scanner, timeout);
    timeu = tbsys::CTimeUtil::getTime() - timeu;
    if (OB_SUCCESS != err)
    {
      fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
      break;
    }
    else
    {
      int64_t row_counter = 0;
      while (OB_SUCCESS == scanner.next_cell())
      {
        ObCellInfo *ci = NULL;
        bool is_row_changed = false;
        scanner.get_cell(&ci, &is_row_changed);
        //fprintf(stdout, "%s\n", updateserver::print_cellinfo(ci, "CLI_SCAN"));
        if (is_row_changed)
        {
          row_counter++;
        }
      }
      bool is_fullfilled = false;
      int64_t fullfilled_num = 0;
      ObRowkey last_rk;
      scanner.get_last_row_key(last_rk);
      scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
      fprintf(stdout, "[SINGLE_SCAN] is_fullfilled=%s fullfilled_num=%ld row_counter=%ld timeu=%ld last_row_key=[%s]\n",
              STR_BOOL(is_fullfilled), fullfilled_num, row_counter, timeu, to_cstring(last_rk));
      total_fullfilled_num += fullfilled_num;
      total_row_counter += row_counter;
      total_timeu += timeu;
      if (is_fullfilled)
      {
        break;
      }
      else
      {
        const_cast<ObNewRange*>(scan_param.get_range())->start_key_ = last_rk;
        const_cast<ObNewRange*>(scan_param.get_range())->border_flag_.unset_min_value();
        const_cast<ObNewRange*>(scan_param.get_range())->border_flag_.unset_inclusive_start();
      }
    }
  }
  fprintf(stdout, "[TOTAL_SCAN] total_fullfilled_num=%ld total_row_counter=%ld total_timeu=%ld\n",
          total_fullfilled_num, total_row_counter, total_timeu);
}

int scan(const char *fname, PageArena<char> &allocer, MockClient &client, const char *version_range,
         const char * expect_result_fname, const char *schema_fname)
{
  ObScanner scanner;
  ObScanParam scan_param;
  read_scan_param(fname, SCAN_PARAM_SECTION, allocer, scan_param);
  scan_param.set_version_range(str2range(version_range));
  scan_param.set_is_read_consistency(false);
  int err = client.ups_scan(scan_param, scanner, timeout);
  UNUSED(expect_result_fname);
  UNUSED(schema_fname);
  // check result
  if (OB_SUCCESS == err)
  {
    print_scanner(scanner);
  }
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  return err;
}

int param_get(const char *fname, MockClient &client)
{
  int err = OB_SUCCESS;
  int fd = open(fname, O_RDONLY);
  if (-1 == fd)
  {
    fprintf(stdout, "open [%s] fail errno=%u\n", fname, errno);
    err = OB_ERROR;
  }
  else
  {
    struct stat st;
    fstat(fd, &st);
    char *buf = (char*)malloc(st.st_size);
    if (NULL == buf)
    {
      fprintf(stdout, "malloc buf fail size=%ld\n", st.st_size);
       err = OB_ERROR;
    }
    else
    {
      read(fd, buf, st.st_size);
      int64_t pos = 0;
      ObGetParam get_param;
      if (OB_SUCCESS != get_param.deserialize(buf, st.st_size, pos))
      {
        fprintf(stdout, "deserialize get_param fail\n");
        err = OB_ERROR;
      }
      else
      {
        get_param.set_is_read_consistency(false);
        ObScanner scanner;
        int err = client.ups_get(get_param, scanner, timeout);
        fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
        if (OB_SUCCESS == err)
        {
          int64_t row_counter = 0;
          while (OB_SUCCESS == scanner.next_cell())
          {
            ObCellInfo *ci = NULL;
            bool is_row_changed = false;
            scanner.get_cell(&ci, &is_row_changed);
            fprintf(stdout, "%s\n", common::print_cellinfo(ci, "CLI_GET"));
            if (is_row_changed)
            {
              row_counter++;
            }
          }
          bool is_fullfilled = false;
          int64_t fullfilled_num = 0;
          ObRowkey last_rk;
          scanner.get_last_row_key(last_rk);
          scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
          fprintf(stdout, "is_fullfilled=%s fullfilled_num=%ld row_counter=%ld last_row_key=[%s]\n",
                  STR_BOOL(is_fullfilled), fullfilled_num, row_counter, to_cstring(last_rk));
        }
      }
      free(buf);
    }
    close(fd);
  }
  return err;
}

int get(const char *fname, PageArena<char> &allocer, MockClient &client, const char *version_range,
        const char * expect_result_fname, const char * schema_fname)
{
  ObScanner scanner;
  ObGetParam get_param;
  read_get_param(fname, GET_PARAM_SECTION, allocer, get_param);
  get_param.set_is_read_consistency(false);
  get_param.set_version_range(str2range(version_range));
  int err = client.ups_get(get_param, scanner, timeout);
  UNUSED(expect_result_fname);
  UNUSED(schema_fname);
  if (OB_SUCCESS == err)
  {
    print_scanner(scanner);
  }
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  return err;
}

void minor_freeze(MockClient &client)
{
  int err = client.freeze(timeout, false);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void major_freeze(MockClient &client)
{
  int err = client.freeze(timeout, true);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void fetch_schema(MockClient &client, int64_t timestamp)
{
  ObSchemaManagerV2 *schema_mgr = new(std::nothrow) ObSchemaManagerV2();
  if (NULL == schema_mgr)
  {
    fprintf(stdout, "[%s] new ObSchemaManagerV2 fail\n", __FUNCTION__);
  }
  else
  {
    int err = client.fetch_schema(timestamp, *schema_mgr, timeout);
    fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
    if (OB_SUCCESS == err)
    {
      print_schema(*schema_mgr);
    }
    delete schema_mgr;
  }
}

void get_sstable_range_list(MockClient &client, int64_t timestamp, int64_t session_id)
{
  IntArray<2> vt;
  vt.v[0] = timestamp;
  vt.v[1] = session_id;
  ObTabletInfoList ti_list;
  int err = client.get_sstable_range_list(vt, ti_list, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
  if (OB_SUCCESS == err)
  {
    for (int64_t i = 0; i < ti_list.get_tablet_size(); i++)
    {
      const ObTabletInfo &ti = ti_list.get_tablet()[i];
      fprintf(stdout, "%s row_count=%ld occupy_size=%ld\n",
              scan_range2str(ti.range_), ti.row_count_, ti.occupy_size_);
    }
  }
}

void drop(MockClient &client)
{
  int err = client.drop_memtable(timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void change_log_level(MockClient &client, const char *sz_log_level)
{
  int log_level = -1;
  const int64_t timeout = 1000000;

  if (0 == strcmp(sz_log_level, "DEBUG"))
    log_level = TBSYS_LOG_LEVEL_DEBUG;
  else if (0 == strcmp(sz_log_level, "WARN"))
    log_level = TBSYS_LOG_LEVEL_WARN;
  else if (0 == strcmp(sz_log_level, "INFO"))
    log_level = TBSYS_LOG_LEVEL_INFO;
  else if (0 == strcmp(sz_log_level, "ERROR"))
    log_level = TBSYS_LOG_LEVEL_ERROR;
  else {
    printf("log_level can only set [DEBUG|WARN|INFO|ERROR]\n");
    return;
  }

  int err = client.change_log_level(log_level, timeout);
  fprintf(stdout, "[%s] err=%d\n", __FUNCTION__, err);
}

void init_mock_client(const char *addr, int32_t port, const char *login_type, MockClient &client)
{
  ObServer dst_host;
  dst_host.set_ipv4_addr(addr, port);
  client.init(dst_host);
  if (0 == strcmp(login_type, "direct"))
  {
    // do nothing
  }
  else if (0 == strcmp(login_type, "master_ups"))
  {
    ObUpsList ups_list;
    if (OB_SUCCESS == client.get_ups_list(ups_list, timeout))
    {
      for (int64_t i = 0; i < ups_list.ups_count_; i++)
      {
        if (UPS_MASTER == ups_list.ups_array_[i].stat_)
        {
          client.set_server(ups_list.ups_array_[i].addr_);
          break;
        }
      }
    }
  }
  else if (0 == strcmp(login_type, "random_ms"))
  {
    MsList ms_list;
    ms_list.init(dst_host, client.get_rpc());
    ms_list.runTimerTask();
    client.set_server(ms_list.get_one());
  }
  else
  {
    fprintf(stdout, "Invalid login_type [%s]\n", login_type);
    exit(-1);
  }
}

int execute_sql(MockClient &client, const char* sql_query)
{
  int ret = OB_SUCCESS;
  ObString query;
  query.assign_ptr(const_cast<char*>(sql_query), static_cast<int32_t>(strlen(sql_query)));
  printf("execute_sql, query=[%.*s]...\n", query.length(), query.ptr());
  ret = client.execute_sql(query, timeout);
  printf("ret=%d\n", ret);
  return ret;
}

struct CmdLineParam
{
  char *serv_addr;
  int32_t serv_port;
  char *login_type;
  char *cmd_type;
  char *ini_fname;
  int64_t timestamp;
  char *version_range;
  char *memory_limit;
  char *memtable_limit;
  char *priv_queue_conf;
  char *expected_result_fname;
  char *schema_fname;
  int64_t session_id;
  char *username;
  char *password;
  char *info;
  char *log_level;
  bool quickly_exit;
  char *sql_query;
  int64_t timeout;
};

void print_usage()
{
  fprintf(stdout, "\n");
  fprintf(stdout, "ups_admin [OPTION]\n");
  fprintf(stdout, "   -a|--serv_addr server address[default localhost]\n");
  fprintf(stdout, "   -p|--serv_port server port[default 2700]\n");
  fprintf(stdout, "   -o|--login_type server type[direct|master_ups|random_ms], default[direct]\n");
  fprintf(stdout, "   -t|--cmd_type command type[apply|login_apply|get|param_get|get_clog_status|get_max_log_seq|get_clog_cursor|get_clog_master|get_log_sync_delay_stat|scan|total_scan|minor_freeze|major_freeze|fetch_schema|drop|dump_memtable|dump_schemas|force_fetch_schema|"
          "reload_conf|memory_watch|memory_limit|priv_queue_conf|clear_active_memtable|get_last_frozen_version|fetch_ups_stat_info|get_bloomfilter|"
          "store_memtable|erase_sstable|load_new_store|reload_all_store|reload_store|umount_store|force_report_frozen_version|switch_commit_log|get_table_time_stamp|print_scanner|print_schema|"
          "enable_memtable_checksum|disable_memtable_checksum|get_slave_ups_info|"
          "delay_drop_memtable|immediately_drop_memtable|minor_load_bypass|major_load_bypass|change_log_level|list_sessions|kill_session|get_sstable_range_list|sql_query|ups_show_sessions|ups_kill_session]\n");
  fprintf(stdout, "   -f|--ini_fname ini file name\n");
  fprintf(stdout, "   -n|--session session_id or table_id, must be set while cmd_type is [kill_session|get_sstable_range_list|ups_kill_session]\n");
  fprintf(stdout, "   -s|--timestamp must be set while cmd_type is [fetch_schema|get_bloomfilter|reload_store|get_table_time_stamp|get_sstable_range_list], optional for [store_memtable]\n");
  fprintf(stdout, "   -r|--version_range must be set while cmd_type is [get|scan]\n");
  //fprintf(stdout, "   -l|--memory_limit could be set while cmd_type is memory_limit\n");
  //fprintf(stdout, "   -c|--memtable_limit could be set while cmd_type is memory_limit\n");
  //fprintf(stdout, "   -q|--priv_queue_conf network_lower_limit network_upper_limit adjust_flag low_priv_cur_percent\n");
  //fprintf(stdout, "   -e|--expect_result expected result of the read operation [optional]\n");
  //fprintf(stdout, "   -m|--schema schema of expect_result [must specify when -e was given]\n");
  fprintf(stdout, "   -k|--quickly_exit whether exit quickly[default not]\n");
  fprintf(stdout, "   -h|--help print this help info\n");
  fprintf(stdout, "   -z|--time2str micro time to string\n");
  fprintf(stdout, "   -i|--ip2str int ip_addr to string\n");
  fprintf(stdout, "   -x|--query SQL query string\n");
  fprintf(stdout, "   -T|--timeout timeout(us)\n");
  fprintf(stdout, "   -C|--config phy_memory_gb, list memory config\n");
  fprintf(stdout, "   -A|--config total_memory_limit, list memory config\n");
  fprintf(stdout, "\n");
}

void parse_cmd_line(int argc, char **argv, CmdLineParam &clp)
{
  int opt = 0;
  const char* opt_string = "a:p:o:t:f:s:n:z:i:r:l:q:c:e:m:u:w:g:khx:T:C:A:";
  struct option longopts[] =
  {
    {"serv_addr", 1, NULL, 'a'},
    {"serv_port", 1, NULL, 'p'},
    {"login_type", 1, NULL, 'o'},
    {"cmd_type", 1, NULL, 't'},
    {"ini_fname", 1, NULL, 'f'},
    {"timestamp", 1, NULL, 's'},
    {"session", 1, NULL, 'n'},
    {"time2str", 1, NULL, 'z'},
    {"ip2str", 1, NULL, 'i'},
    {"version_range", 1, NULL, 'r'},
    {"memory_limit", 1, NULL, 'l'},
    {"priv_queue_conf", 1, NULL, 'q'},
    {"active_limit", 1, NULL, 'c'},
    {"expect_result", 1, NULL, 'e'},
    {"schema", 1, NULL, 'm'},
    {"username", 1, NULL, 'u'},
    {"password", 1, NULL, 'w'},
    {"log_level", 1, NULL, 'g'},
    {"quickly_exit", 1, NULL, 'k'},
    {"help", 0, NULL, 'h'},
    {"query", 1, NULL, 'x'},
    {"timeout", 1, NULL, 'T'},
    {"config", 1, NULL, 'g'},
    {0, 0, 0, 0}
  };
  memset(&clp, 0, sizeof(clp));
  clp.serv_addr = (char*)"localhost";
  clp.serv_port = 2700;
  clp.login_type = (char*)"direct";
  clp.timeout = 0;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
    case 'a':
      clp.serv_addr = optarg;
      break;
    case 'p':
      clp.serv_port = atoi(optarg);
      break;
    case 'o':
      clp.login_type = optarg;
      break;
    case 't':
      clp.cmd_type = optarg;
      break;
    case 'f':
      clp.ini_fname = optarg;
      break;
    case 's':
      clp.timestamp = atol(optarg);
      break;
    case 'n':
      clp.session_id = atol(optarg);
      break;
    case 'r':
      clp.version_range = optarg;
      break;
    case 'l':
      clp.memory_limit = optarg;
      break;
    case 'q':
      clp.priv_queue_conf = optarg;
      break;
    case 'c':
      clp.memtable_limit = optarg;
      break;
    case 'z':
      {
        int64_t timestamp = atol(optarg);
        fprintf(stdout, "timestamp=%ld str=%s\n",
                timestamp, time2str(timestamp));
        exit(1);
      }
    case 'i':
      {
        int64_t ip = atoll(optarg);
        fprintf(stdout, "ip=%ld str=%s\n", ip, tbsys::CNetUtil::addrToString(ip).c_str());
        exit(1);
      }
    case 'e':
      clp.expected_result_fname = optarg;
      break;
    case 'm':
      clp.schema_fname = optarg;
      break;
    case 'u':
      clp.username = optarg;
      break;
    case 'w':
      clp.password = optarg;
      break;
    case 'g':
      clp.log_level = optarg;
      break;
    case 'x':
      clp.sql_query = optarg;
      break;
    case 'k':
      clp.quickly_exit = true;
      break;
    case 'T':
      clp.timeout = atol(optarg);
      break;
    case 'A':
      {
        updateserver::ObUpdateServerConfig conf;
        fprintf(stdout, "\n");
        conf.auto_config_memory(atol(optarg));
        exit(1);
      }
    case 'C':
      {
        updateserver::ObUpdateServerConfig conf;
        fprintf(stdout, "\n");
        conf.auto_config_memory(conf.get_total_memory_limit(atol(optarg)));
        exit(1);
      }
      break;
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }
  if (NULL == clp.serv_addr
      || 0 == clp.serv_port
      || NULL == clp.cmd_type
      || (NULL == clp.ini_fname && 0 == strcmp("apply", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("scan", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("total_scan", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("get", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("param_get", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("umount_store", clp.cmd_type))
      || (NULL == clp.version_range && 0 == strcmp("scan", clp.cmd_type))
      || (NULL == clp.version_range && 0 == strcmp("total_scan", clp.cmd_type))
      || (NULL == clp.version_range && 0 == strcmp("get", clp.cmd_type))
      || (0 == clp.session_id && 0 == strcmp("kill_session", clp.cmd_type))
      || (0 == clp.session_id && 0 == strcmp("ups_kill_session", clp.cmd_type))
      || (0 == clp.timestamp && 0 == strcmp("get_bloomfilter", clp.cmd_type))
      || (0 == clp.timestamp && 0 == strcmp("fetch_schema", clp.cmd_type))
      || (0 == clp.timestamp && 0 == clp.session_id && 0 == strcmp("get_sstable_range_list", clp.cmd_type))
      || (0 == clp.timestamp && 0 == strcmp("get_table_time_stamp", clp.cmd_type))
      || (0 == clp.timestamp && 0 == strcmp("reload_store", clp.cmd_type))
      || (NULL == clp.log_level && 0 == strcmp("change_log_level", clp.cmd_type))
      || (NULL == clp.sql_query && 0 == strcmp("sql_query", clp.cmd_type))
    )
  {
    print_usage();
    fprintf(stdout, "serv_addr=%s serv_port=%d cmd_type=%s ini_fname=%s timestamp=%ld version_range=%s\n",
            clp.serv_addr, clp.serv_port, clp.cmd_type, clp.ini_fname, clp.timestamp, clp.version_range);
    exit(-1);
  }
  if ((NULL == clp.ini_fname && 0 == strcmp("print_scanner", clp.cmd_type))
      || (NULL == clp.ini_fname && 0 == strcmp("print_schema", clp.cmd_type)))
  {
    print_usage();
    fprintf(stdout, "cmd_type=%s ini_fname=%s\n", clp.cmd_type, clp.ini_fname);
    exit(-1);
  }

  if(NULL != clp.expected_result_fname && NULL == clp.schema_fname)
  {
    print_usage();
    fprintf(stdout, "-e and -m must appear together");
    exit(-1);
  }
}

int main(int argc, char** argv)
{
  setlocale(LC_ALL, "");
  TBSYS_LOGGER.setFileName("ups_admin.log", true);
  TBSYS_LOG(INFO, "ups_admin start==================================================");
  ob_init_memory_pool();

  CmdLineParam clp;
  parse_cmd_line(argc, argv, clp);

  timeout = clp.timeout?: timeout;

  MockClient client;
  init_mock_client(clp.serv_addr, clp.serv_port, clp.login_type, client);

  PageArena<char> allocer;
  int rc = 0;

  if (0 == strcmp("apply", clp.cmd_type))
  {
    apply(clp.ini_fname, allocer, client);
  }
  else if (0 == strcmp("get_clog_status", clp.cmd_type))
  {
    get_clog_status(client);
  }
  else if (0 == strcmp("get_max_log_seq", clp.cmd_type))
  {
    get_max_log_seq(client);
  }
  else if (0 == strcmp("get_clog_cursor", clp.cmd_type))
  {
    get_clog_cursor(client);
  }
  else if (0 == strcmp("get_clog_master", clp.cmd_type))
  {
    get_clog_master(client);
  }
  else if (0 == strcmp("get_log_sync_delay_stat", clp.cmd_type))
  {
    get_log_sync_delay_stat(client);
  }
  else if (0 == strcmp("get", clp.cmd_type))
  {
    get(clp.ini_fname, allocer, client, clp.version_range,clp.expected_result_fname,
      clp.schema_fname);
  }
  else if (0 == strcmp("param_get", clp.cmd_type))
  {
    param_get(clp.ini_fname, client);
  }
  else if (0 == strcmp("scan", clp.cmd_type))
  {
    scan(clp.ini_fname, allocer, client, clp.version_range,clp.expected_result_fname,
      clp.schema_fname);
  }
  else if (0 == strcmp("total_scan", clp.cmd_type))
  {
    total_scan(clp.ini_fname, allocer, client, clp.version_range);
  }
  else if (0 == strcmp("minor_freeze", clp.cmd_type))
  {
    minor_freeze(client);
  }
  else if (0 == strcmp("major_freeze", clp.cmd_type))
  {
    major_freeze(client);
  }
  else if (0 == strcmp("fetch_schema", clp.cmd_type))
  {
    fetch_schema(client, clp.timestamp);
  }
  else if (0 == strcmp("get_sstable_range_list", clp.cmd_type))
  {
    get_sstable_range_list(client, clp.timestamp, clp.session_id);
  }
  else if (0 == strcmp("drop", clp.cmd_type))
  {
    drop(client);
  }
  else if (0 == strcmp("dump_memtable", clp.cmd_type))
  {
    dump_memtable(clp.ini_fname, client);
  }
  else if (0 == strcmp("dump_schemas", clp.cmd_type))
  {
    dump_schemas(client);
  }
  else if (0 == strcmp("force_fetch_schema", clp.cmd_type))
  {
    force_fetch_schema(client);
  }
  else if (0 == strcmp("reload_conf", clp.cmd_type))
  {
    reload_conf(clp.ini_fname, client);
  }
  else if (0 == strcmp("memory_watch", clp.cmd_type))
  {
    memory_watch(client);
  }
  else if (0 == strcmp("memory_limit", clp.cmd_type))
  {
    memory_limit(clp.memory_limit, clp.memtable_limit, client);
  }
  else if (0 == strcmp("priv_queue_conf", clp.cmd_type))
  {
    priv_queue_conf(clp.priv_queue_conf, client);
  }
  else if (0 == strcmp("clear_active_memtable", clp.cmd_type))
  {
    clear_active_memtable(client);
  }
  else if (0 == strcmp("get_last_frozen_version", clp.cmd_type))
  {
    get_last_frozen_version(client);
  }
  else if (0 == strcmp("fetch_ups_stat_info", clp.cmd_type))
  {
    fetch_ups_stat_info(client);
  }
  else if (0 == strcmp("get_bloomfilter", clp.cmd_type))
  {
    get_bloomfilter(client, clp.timestamp);
  }
  else if (0 == strcmp("store_memtable", clp.cmd_type))
  {
    store_memtable(client, clp.timestamp);
  }
  else if (0 == strcmp("erase_sstable", clp.cmd_type))
  {
    erase_sstable(client);
  }
  else if (0 == strcmp("load_new_store", clp.cmd_type))
  {
    load_new_store(client);
  }
  else if (0 == strcmp("reload_all_store", clp.cmd_type))
  {
    reload_all_store(client);
  }
  else if (0 == strcmp("reload_store", clp.cmd_type))
  {
    reload_store(client, clp.timestamp);
  }
  else if (0 == strcmp("umount_store", clp.cmd_type))
  {
    umount_store(client, clp.ini_fname);
  }
  else if (0 == strcmp("force_report_frozen_version", clp.cmd_type))
  {
    force_report_frozen_version(client);
  }
  else if (0 == strcmp("switch_commit_log", clp.cmd_type))
  {
    switch_commit_log(client);
  }
  else if (0 == strcmp("get_table_time_stamp", clp.cmd_type))
  {
    get_table_time_stamp(client, clp.timestamp);
  }
  else if (0 == strcmp("disable_memtable_checksum", clp.cmd_type))
  {
    disable_memtable_checksum(client);
  }
  else if (0 == strcmp("enable_memtable_checksum", clp.cmd_type))
  {
    enable_memtable_checksum(client);
  }
  else if (0 == strcmp("immediately_drop_memtable", clp.cmd_type))
  {
    immediately_drop_memtable(client);
  }
  else if (0 == strcmp("delay_drop_memtable", clp.cmd_type))
  {
    delay_drop_memtable(client);
  }
  else if (0 == strcmp("minor_load_bypass", clp.cmd_type))
  {
    rc = minor_load_bypass(client);
  }
  else if (0 == strcmp("major_load_bypass", clp.cmd_type))
  {
    rc = major_load_bypass(client);
  }
  else if(0 == strcmp("list_sessions", clp.cmd_type))
  {
    client.list_sessions(timeout);
  }
  else if(0 == strcmp("kill_session", clp.cmd_type))
  {
    client.kill_session(timeout, clp.session_id);
  }
  else if (0 == strcmp("change_log_level", clp.cmd_type))
  {
    change_log_level(client, clp.log_level);
  }
  else if (0 == strcmp("get_slave_ups_info", clp.cmd_type))
  {
    get_slave_ups_info(client);
  }
  else if (0 == strcmp("print_scanner", clp.cmd_type))
  {
    print_scanner(clp.ini_fname);
  }
  else if (0 == strcmp("print_schema", clp.cmd_type))
  {
    print_schema(clp.ini_fname);
  }
  else if (0 == strcmp("sql_query", clp.cmd_type))
  {
    execute_sql(client, clp.sql_query);
  }
  else if (0 == strcmp("ups_show_sessions", clp.cmd_type))
  {
    ups_show_sessions(client);
  }
  else if (0 == strcmp("ups_kill_session", clp.cmd_type))
  {
    ups_kill_session(static_cast<uint32_t>(clp.session_id), client);
  }
  else
  {
    print_usage();
  }

  if (!clp.quickly_exit)
  {
    client.destroy();
    TBSYS_LOG(INFO, "ups_admin end==================================================");
    return rc;
  }
  else
  {
    TBSYS_LOG(INFO, "ups_admin killed==================================================");
    fflush(stdout);
    fflush(stderr);
    _exit(rc);
  }
}
