/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_syschecker_param.h for parse parameter. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SYSCHECKER_PARAM_H_
#define OCEANBASE_SYSCHECKER_PARAM_H_

#include <stdint.h>
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_array.h"

namespace oceanbase 
{ 
  namespace syschecker 
  {

    class ObSyscheckerParam
    {
    public:
      static const int64_t OB_MAX_IP_SIZE               = 64;
      static const int64_t OB_MAX_MERGE_SERVER_STR_SIZE = 2048;
      static const int64_t DEFAULT_TIMEOUT              = 2000000; //2s
      static const int64_t DEFAULT_WRITE_THREAD_COUNT   = 2;
      static const int64_t DEFAULT_READ_THERAD_COUNT    = 20;
      static const int64_t DEFAULT_SYSCHECKER_COUNT     = 1;
      static const int64_t DEFAULT_STAT_DUMP_INTERVAL   = 0;
      static const int64_t DEFAULT_PERF_TEST            = 0;
      static const int64_t DEFAULT_SQL_READ             = 0;
      static const int64_t DEFAULT_READ_TABLE_TYPE      = 0;
      static const int64_t DEFAULT_WRITE_TABLE_TYPE     = 0;
      static const int64_t DEFAULT_GET_ROW_CNT          = 0;
      static const int64_t DEFAULT_SCAN_ROW_CNT         = 0;
      static const int64_t DEFAULT_UPDATE_ROW_CNT       = 0;
      static const int64_t DEFAULT_CHECK_RESULT         = 1;

      ObSyscheckerParam();
      ~ObSyscheckerParam();
    
    public:
      int load_from_config();
      void dump_param();

      inline const common::ObServer& get_root_server() const 
      {
        return root_server_; 
      }

      inline const common::ObServer& get_update_server() const 
      {
        return update_server_; 
      }

      common::ObArray<common::ObServer> & get_chunk_servers() { return chunk_servers_; }
      common::ObArray<common::ObServer> & get_merge_servers() { return merge_servers_; }

      inline const int64_t get_network_time_out() const 
      {
        return network_time_out_; 
      }

      inline const int64_t get_write_thread_count() const 
      {
        return write_thread_count_; 
      }

      inline const int64_t get_read_thread_count() const 
      {
        return read_thread_count_; 
      }

      inline const int64_t get_syschecker_count() const
      {
        return syschecker_count_;
      }

      inline const int64_t get_syschecker_no() const
      {
        return syschecker_no_;
      }

      inline const bool is_specified_read_param() const
      {
        return (specified_read_param_ == 0 ? false : true);
      }

      inline const bool is_operate_full_row() const
      {
        return (operate_full_row_ == 0 ? false : true);
      }

      inline const int64_t get_stat_dump_interval() const 
      {
        return stat_dump_interval_; 
      }  
      
      inline const int64_t is_perf_test() const
      {
        return (perf_test_ == 0) ? false : true;
      }

      inline const bool is_sql_read() const
      {
        return is_sql_read_;
      }

      inline const int64_t is_check_result() const
      {
        return (check_result_ == 0) ? false : true;
      }

      inline const int64_t get_read_table_type() const
      {
        return read_table_type_;
      }

      inline const int64_t get_write_table_type() const
      {
        return write_table_type_;
      }

      inline const int64_t get_get_row_cnt() const 
      {
        return get_row_cnt_;
      }

      inline const int64_t get_scan_row_cnt() const 
      {
        return scan_row_cnt_;
      }

      inline const int64_t get_update_row_cnt() const
      {
        return update_row_cnt_;
      }

    private:
      int load_string(char* dest, const int64_t size, 
                      const char* section, const char* name, bool not_null);
      int parse_server(char* str, common::ObArray<common::ObServer>& servers);
      int load_server();

    private:
      common::ObServer root_server_;
      common::ObServer update_server_;
      common::ObArray<common::ObServer> merge_servers_;
      common::ObArray<common::ObServer> chunk_servers_;
      int64_t network_time_out_;
      int64_t write_thread_count_;
      int64_t read_thread_count_;
      int64_t syschecker_count_;
      int64_t syschecker_no_;
      int64_t specified_read_param_;
      int64_t operate_full_row_;
      int64_t stat_dump_interval_;

      // preformance test param
      int64_t perf_test_;
      int64_t is_sql_read_;
      int64_t check_result_;
      int64_t read_table_type_;
      int64_t write_table_type_;
      int64_t get_row_cnt_;
      int64_t scan_row_cnt_;
      int64_t update_row_cnt_;
    };
  } // end namespace syschecker
} // end namespace oceanbase

#endif //OCEANBASE_SYSCHECKER_PARAM_H_
