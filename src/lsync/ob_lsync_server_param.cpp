/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_lsync_server_param.cpp
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#include "config.h"
#include "ob_lsync_server_param.h"

namespace oceanbase
{
  namespace lsync
  {
    const static char* LSYNC_SECTION = "lsync_server";
    const static char* DEV_NAME_KEY = "dev_name";
    const static char* PORT_KEY = "port";
    const static char* CONVERT_SWITCH_LOG_KEY = "convert_switch_log";
    const static char* COMMIT_LOG_DIR_KEY = "ups_commit_log_dir";
    const static char* LOG_FILE_START_ID_KEY = "ups_commit_log_file_start_id";
    const static char* TIMEOUT_KEY = "timeout";
    const static char* LSYNC_RETRY_WAIT_TIME_US_KEY = "lsync_retry_wait_time_us";

#define get_lsync_config(name) TBSYS_CONFIG.getString(LSYNC_SECTION, name##_KEY, NULL)
#define merge_lsync_config(other, name) if(other.name##_is_set()) set_##name(other.get_##name())
    static char* _strncpy(char* dest, const char* src, int n)
    {
      strncpy(dest, src, n);
      dest[n-1] = 0;
      return dest;
    }

    int ObLsyncServerParam::load_from_file(const char* config_file)
    {
      int err = OB_SUCCESS;
      const char* val = NULL;
      if(OB_SUCCESS != (err = TBSYS_CONFIG.load(config_file)))
      {
        TBSYS_LOG(WARN, "TBSYS_CONFIG.load('%s')=>%d", config_file, err);
      }
      else
      {
        if((val = get_lsync_config(DEV_NAME)))
          set_dev_name(_strncpy(dev_name_buf_, val, ARRAYSIZEOF(dev_name_buf_)));
        if((val = get_lsync_config(PORT)))
          set_port(atoi(val));
        if((val = get_lsync_config(CONVERT_SWITCH_LOG)))
          set_convert_switch_log(atoi(val));
        if((val = get_lsync_config(COMMIT_LOG_DIR)))
          set_commit_log_dir(_strncpy(commit_log_dir_buf_, val, ARRAYSIZEOF(commit_log_dir_buf_)));
        if((val = get_lsync_config(LOG_FILE_START_ID)))
          set_log_file_start_id(atol(val));
        if((val = get_lsync_config(TIMEOUT)))
          set_timeout(atol(val));
        if((val = get_lsync_config(LSYNC_RETRY_WAIT_TIME_US)))
          set_lsync_retry_wait_time_us(atoll(val));
      }
      return err;
    }

    void ObLsyncServerParam::merge(ObLsyncServerParam& other)
    {
      merge_lsync_config(other, dev_name);
      merge_lsync_config(other, port);
      merge_lsync_config(other, convert_switch_log);
      merge_lsync_config(other, commit_log_dir);
      merge_lsync_config(other, log_file_start_id);
      merge_lsync_config(other, timeout);
      merge_lsync_config(other, lsync_retry_wait_time_us);
    }

    void ObLsyncServerParam::print(const char* msg)
    {
      TBSYS_LOG(INFO, "%s: ObLsyncServerParam{ log_dir: '%s', convert_switch_log: %d, log_file_start: %ld, timeout: %ld, dev: '%s', port: %d, lsync_retry_wait_time_us: %ld}\n",
                msg, commit_log_dir_, convert_switch_log_, log_file_start_id_, timeout_, dev_name_, port_, lsync_retry_wait_time_us_);
    }

    bool ObLsyncServerParam::check()
    {
      return dev_name_is_set() && port_is_set() && convert_switch_log_is_set() && commit_log_dir_is_set() && log_file_start_id_is_set() && timeout_is_set() && lsync_retry_wait_time_us_is_set();
    }
  } // end namespace lsync
} // end namespace oceanbase

