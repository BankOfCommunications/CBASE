/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_msync_client_main.h
 * 
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#ifndef __OCEANBASE_MSYNC_OB_MSYNC_CLIENT_MAIN_H__
#define __OCEANBASE_MSYNC_OB_MSYNC_CLIENT_MAIN_H__

#include "common/base_main.h"
#include "ob_msync_client.h"

namespace oceanbase
{
  namespace msync
  {
    const static int64_t TIMEOUT = 10*1000*1000;
    const static int64_t RETRY_WAIT_TIME = 10*1000;
    const static int64_t MAX_RETRY_TIMES = 1<<30;
    
    const static int MAX_STR_BUF_SIZE = 1<<10;
    const static char* SEQ_MARKER_FILE = "seq";
    
    struct ObMsyncConfig
    {
      const char* schema;
      const char* host;
      const char* log_dir;
      int32_t port;
      int64_t log_file_start_id;
      int64_t last_log_seq_id;
      int64_t timeout;
      int64_t retry_wait_time;
      int64_t max_retry_times;
      const char* seq_marker;
      char str_buf_[MAX_STR_BUF_SIZE];

    ObMsyncConfig() : schema(NULL), host(NULL), log_dir(NULL), port(0), log_file_start_id(0), last_log_seq_id(0), timeout(TIMEOUT), retry_wait_time(RETRY_WAIT_TIME), max_retry_times(MAX_RETRY_TIMES), seq_marker(SEQ_MARKER_FILE)
        {}
      const char* to_str()
        {
          snprintf(str_buf_, sizeof(str_buf_),
                  "MsyncConfig {schema='%s', host='%s', port=%d, log-dir='%s', "
                  "log_file_start=%ld, last_log_seq_id=%ld, timeout=%ld, retry_wait_time=%ld, max_retry_times=%ld, seq_marker='%s'}",
                   schema, host, port, log_dir, log_file_start_id, last_log_seq_id, timeout,
                   retry_wait_time, max_retry_times, seq_marker);
          str_buf_[sizeof(str_buf_)-1] = 0;
          return str_buf_;
        }

      bool check()
        {
          return schema && host && log_dir && port && log_file_start_id && timeout;
        }
    };
    class ObMsyncClientMain : public common::BaseMain
    {
    protected:
      ObMsyncClientMain(){}
      ~ObMsyncClientMain(){}

    protected:
      virtual int do_work();
      virtual void do_signal(const int sig);
      virtual const char* parse_cmd_line(const int argc,  char *const argv[]);
      virtual void print_usage(const char *prog_name);
      virtual void print_version();

    public:
      static ObMsyncClientMain* get_instance();
    public:
      const ObMsyncClient& get_msync_client() const
      {
        return client_;
      }

      ObMsyncClient& get_msync_client()
      {
        return client_;
      }

    private:
      const char* app_name_;
      int log_level_;
      ObMsyncConfig config_;
      ObMsyncClient client_;
    };
  }
}

#endif //__OCEANBASE_MSYNC_OB_MSYNC_CLIENT_MAIN_H__

