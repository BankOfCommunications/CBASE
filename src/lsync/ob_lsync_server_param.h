/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_lsync_server_param.h
 * 
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */
#ifndef __OCEANBASE_LSYNC_OB_LSYNC_SERVER_PARAM_H__
#define __OCEANBASE_LSYNC_OB_LSYNC_SERVER_PARAM_H__

#include "common/ob_define.h"

using namespace oceanbase::common;

#define getter_define(type, lower) inline const type get_##lower() const { return lower##_; }
#define setter_define(type, lower) inline void set_##lower(const type lower) { lower##_ = lower; }
#define set_predictor_define(type, lower, upper) inline bool lower##_is_set() { return lower##_ != DEFAULT_##upper; }
#define config_item_define(type, lower, upper) getter_define(type, lower) setter_define(type, lower) set_predictor_define(type, lower, upper)

namespace oceanbase
{
  namespace lsync
  {
    const static int MAX_CONFIG_VALUE_LEN = 1<<10;
    const static char* DEFAULT_DEV_NAME = NULL;
    const static int DEFAULT_PORT = -1;
    const static int DEFAULT_CONVERT_SWITCH_LOG = -1;
    const static char* DEFAULT_COMMIT_LOG_DIR = NULL;
    const static int64_t DEFAULT_LOG_FILE_START_ID = -1;
    const static int64_t DEFAULT_TIMEOUT = -1;
    const static int64_t DEFAULT_LSYNC_RETRY_WAIT_TIME_US = -1;
    class ObLsyncServerParam
    {
    public:
      ObLsyncServerParam()
      {
        dev_name_ = DEFAULT_DEV_NAME;
        port_ = DEFAULT_PORT;
        commit_log_dir_ = DEFAULT_COMMIT_LOG_DIR;
        convert_switch_log_ = DEFAULT_CONVERT_SWITCH_LOG;
        log_file_start_id_ = DEFAULT_LOG_FILE_START_ID;
        timeout_ = DEFAULT_TIMEOUT;
        lsync_retry_wait_time_us_ = DEFAULT_LSYNC_RETRY_WAIT_TIME_US;
      }
      ~ObLsyncServerParam(){}

      int load_from_file(const char* config_file);
      void merge(ObLsyncServerParam& other);
      bool check();
      void print(const char* msg);
      
    public:
      config_item_define(char*, dev_name, DEV_NAME);
      config_item_define(int, port, PORT);
      config_item_define(int, convert_switch_log, CONVERT_SWITCH_LOG);
      config_item_define(char*, commit_log_dir, COMMIT_LOG_DIR);
      config_item_define(int64_t, log_file_start_id, LOG_FILE_START_ID);
      config_item_define(int64_t, timeout, TIMEOUT);
      config_item_define(int64_t, lsync_retry_wait_time_us, LSYNC_RETRY_WAIT_TIME_US);

    private:
      const char* dev_name_;
      int port_;
      int convert_switch_log_;
      const char* commit_log_dir_;
      int64_t log_file_start_id_;
      int64_t timeout_;
      int64_t lsync_retry_wait_time_us_;
      char dev_name_buf_[MAX_CONFIG_VALUE_LEN];
      char commit_log_dir_buf_[MAX_CONFIG_VALUE_LEN];
    };
  }
}

#endif // __OCEANBASE_LSYNC_OB_LSYNC_SERVER_PARAM_H__

