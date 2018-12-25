/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive_param.h for parse parameter. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_PARAM_H
#define OCEANBASE_OLAPDRIVE_PARAM_H

#include <stdint.h>
#include "common/ob_define.h"
#include "common/ob_server.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {

    class ObOlapdriveParam
    {
    public:
      static const int64_t OB_MAX_IP_SIZE               = 64;
      static const int64_t OB_MAX_MERGE_SERVER_STR_SIZE = 2048;
      static const int64_t DEFAULT_TIMEOUT              = 2000000; //2s
      static const int64_t DEFAULT_WRITE_THREAD_COUNT   = 1;
      static const int64_t DEFAULT_READ_THERAD_COUNT    = 20;
      static const int64_t DEFAULT_STAT_DUMP_INTERVAL   = 0;
      static const int64_t DEFAULT_WRITE_DAY_COUNT      = 1;
      static const int64_t DEFAULT_DAILY_UNIT_COUNT     = 10000;
      static const int64_t DEFAULT_READ_DAY_COUNT       = 7;
      static const int64_t DEFAULT_LOG_LEVEL            = 1;

      ObOlapdriveParam();
      ~ObOlapdriveParam();
    
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

      inline const int64_t get_merge_server_count()
      {
        return merge_server_count_;
      }

      inline const common::ObServer* get_merge_server() const 
      {
        return merge_server_; 
      }

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

      inline const int64_t get_stat_dump_interval() const 
      {
        return stat_dump_interval_; 
      }     
      
      inline const int64_t get_write_day_count() const 
      {
        return write_day_count_; 
      }  

      inline const int64_t get_daily_unit_count() const 
      {
        return daily_unit_count_; 
      }  

      inline const int64_t get_read_day_count() const 
      {
        return read_day_count_; 
      } 

      inline const int64_t get_log_level() const 
      {
        return log_level_; 
      } 

    private:
      int load_string(char* dest, const int64_t size, 
                      const char* section, const char* name, bool not_null);
      int parse_merge_server(char* str);
      int load_merge_server();

    private:
      common::ObServer root_server_;
      common::ObServer update_server_;
      int64_t merge_server_count_;
      common::ObServer* merge_server_;
      int64_t network_time_out_;
      int64_t write_thread_count_;
      int64_t read_thread_count_;
      int64_t stat_dump_interval_;
      int64_t write_day_count_;
      int64_t daily_unit_count_;
      int64_t read_day_count_;
      int64_t log_level_;
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_PARAM_H
