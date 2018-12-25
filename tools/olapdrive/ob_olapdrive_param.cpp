/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive_param.cpp for parse parameter. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <strings.h>
#include <tblog.h>
#include <tbsys.h>
#include "common/ob_malloc.h"
#include "ob_olapdrive_param.h"

namespace 
{
  static const char* OBRS_SECTION = "root_server";
  static const char* OBRS_IP = "vip";
  static const char* OBRS_PORT = "port";

  static const char* OBUPS_SECTION  = "update_server";
  static const char* OBUPS_IP = "vip";
  static const char* OBUPS_PORT = "port";

  static const char* OBMS_SECTION  = "merge_server";
  static const char* OBMS_MERGE_SERVER_COUNT = "merge_server_count";
  static const char* OBMS_MERGE_SERVER_STR = "merge_server_str";

  static const char* OBSC_SECTION = "olapdrive";
  static const char* OBSC_WRITE_THREAD_COUNT = "write_thread_count";
  static const char* OBSC_READ_THREAD_COUNT = "read_thread_count";
  static const char* OBSC_NETWORK_TIMEOUT = "network_timeout";
  static const char* OBSC_STAT_DUMP_INTERVAL = "stat_dump_interval";
  static const char* OBSC_WRITE_DAY_COUNT = "write_day_count";
  static const char* OBSC_DAILY_UNIT_COUNT = "daily_unit_count";
  static const char* OBSC_READ_DAY_COUNT = "read_day_count";
  static const char* OBSC_LOG_LEVEL = "log_level";
}

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace common;

    ObOlapdriveParam::ObOlapdriveParam()
    {
      memset(this, 0, sizeof(ObOlapdriveParam));
    }

    ObOlapdriveParam::~ObOlapdriveParam()
    {
      if (NULL != merge_server_)
      {
        ob_free(merge_server_);
        merge_server_ = NULL;
      }
    }

    int ObOlapdriveParam::load_string(char* dest, const int64_t size, 
                                      const char* section, const char* name, 
                                      bool not_null)
    {
      int ret           = OB_SUCCESS;
      const char* value = NULL;

      if (NULL == dest || 0 >= size || NULL == section || NULL == name)
      {
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        value = TBSYS_CONFIG.getString(section, name);
        if (not_null && (NULL == value || 0 >= strlen(value)))
        {
          TBSYS_LOG(WARN, "%s.%s has not been set.", section, name);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret && NULL != value)
      {
        if ((int32_t)strlen(value) >= size)
        {
          TBSYS_LOG(WARN, "%s.%s too long, length (%lu) > %ld", 
                    section, name, strlen(value), size);
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          memset(dest, 0, size);
          strncpy(dest, value, strlen(value));
        }
      }

      return ret;
    }

    int ObOlapdriveParam::parse_merge_server(char* str)
    {
      int ret           = OB_SUCCESS;
      char *str_ptr     = NULL;
      int32_t port      = 0;
      char *begin_ptr   = NULL;
      char *end_ptr     = str + strlen(str);
      char *ptr         = NULL;
      int64_t length    = 0;
      int64_t cur_index = 0;
      char buffer[OB_MAX_IP_SIZE];

      if (NULL == str)
      {
        TBSYS_LOG(WARN, "invalid param, str=%p", str);
        ret = OB_ERROR;
      }
    
      /**
       * Servers list format is like this. For examle:
       * "127.0.0.1:11108,127.0.0.1:11109"
       */
      for (begin_ptr = str; begin_ptr != end_ptr && OB_SUCCESS == ret;) 
      {
        port = 0;
        str_ptr = index(begin_ptr, ',');

        if (NULL != str_ptr) 
        {
          memcpy(buffer, begin_ptr, str_ptr - begin_ptr);
          buffer[str_ptr - begin_ptr]= '\0';
          begin_ptr = str_ptr + 1;
        } 
        else 
        {
          length= strlen(begin_ptr);
          memcpy(buffer, begin_ptr, length);
          buffer[length] = '\0';
          begin_ptr = end_ptr;
        }

        ptr = index(buffer, ':');
        if (NULL != ptr) {
          ptr[0] = '\0';
          ptr++;
          port = static_cast<int32_t>(strtol(ptr, (char**)NULL, 10));
        }

        if (NULL != merge_server_ && cur_index < merge_server_count_)
        {
          bool res = merge_server_[cur_index++].set_ipv4_addr(buffer, port);
          if (!res)
          {
            TBSYS_LOG(WARN, "invalid merge server index=%ld, ip=%s, port=%d.", 
                      cur_index, buffer, port);
            ret = OB_ERROR;
            break;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "no space to store merge server, merge_server_=%p, "
                          "merge_server_count_=%ld, cur_index=%ld", 
                    merge_server_, merge_server_count_, cur_index);
          ret = OB_ERROR;
          break;
        }

        if (isspace(*begin_ptr))
        {
          begin_ptr++;
        }
      }

      return ret;
    }

    int ObOlapdriveParam::load_merge_server()
    {
      int ret = OB_SUCCESS;
      char merge_server_str[OB_MAX_MERGE_SERVER_STR_SIZE];

      if (OB_SUCCESS == ret)
      {
        merge_server_count_ = TBSYS_CONFIG.getInt(OBMS_SECTION, 
                                                  OBMS_MERGE_SERVER_COUNT, 0);
        if (merge_server_count_ <= 0)
        {
          TBSYS_LOG(WARN, "olapdrive merge server count=%ld can't <= 0." ,
              merge_server_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        merge_server_ = reinterpret_cast<ObServer*>
          (ob_malloc(merge_server_count_ * sizeof(ObServer)));
        if (NULL == merge_server_)
        {
          TBSYS_LOG(ERROR, "failed allocate for merge servers array.");
          ret = OB_ERROR;
        }
        else
        {
          memset(merge_server_, 0, merge_server_count_ * sizeof(ObServer));
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = load_string(merge_server_str, OB_MAX_MERGE_SERVER_STR_SIZE, 
                          OBMS_SECTION, OBMS_MERGE_SERVER_STR, true);
      }

      if (OB_SUCCESS == ret)
      {
        ret = parse_merge_server(merge_server_str);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to parse merge server string");
        }
      }

      return ret;
    }

    int ObOlapdriveParam::load_from_config()
    {
      int ret                     = OB_SUCCESS;
      int32_t root_server_port    = 0;
      int32_t update_server_port  = 0;
      char root_server_ip[OB_MAX_IP_SIZE];
      char update_server_ip[OB_MAX_IP_SIZE];

      //load root server
      if (OB_SUCCESS == ret)
      {
        ret = load_string(root_server_ip, OB_MAX_IP_SIZE, 
                          OBRS_SECTION, OBRS_IP, true);
      }
      if (OB_SUCCESS == ret)
      {
        root_server_port = TBSYS_CONFIG.getInt(OBRS_SECTION, 
                                               OBRS_PORT, 0);
        if (root_server_port <= 0)
        {
          TBSYS_LOG(WARN, "root server port=%d cann't <= 0.", 
                    root_server_port);
          ret = OB_INVALID_ARGUMENT;
        }
      }   
      if (OB_SUCCESS == ret)
      {
        bool res = root_server_.set_ipv4_addr(root_server_ip, 
                                              root_server_port);
        if (!res)
        {
          TBSYS_LOG(WARN, "root server ip=%s, port=%d is invalid.", 
                    root_server_ip, root_server_port);
          ret = OB_ERROR;
        }
      }

      //load update server
      if (OB_SUCCESS == ret)
      {
        ret = load_string(update_server_ip, OB_MAX_IP_SIZE, 
                          OBUPS_SECTION, OBUPS_IP, true);
      }
      if (OB_SUCCESS == ret)
      {
        update_server_port = TBSYS_CONFIG.getInt(OBUPS_SECTION, 
                                                 OBUPS_PORT, 0);
        if (update_server_port <= 0)
        {
          TBSYS_LOG(WARN, "update server port=%d cann't <= 0.", 
                    update_server_port);
          ret = OB_INVALID_ARGUMENT;
        }
      }   
      if (OB_SUCCESS == ret)
      {
        bool res = update_server_.set_ipv4_addr(update_server_ip, 
                                                update_server_port);
        if (!res)
        {
          TBSYS_LOG(WARN, "update server ip=%s, port=%d is invalid.", 
                    update_server_ip, update_server_port);
          ret = OB_ERROR;
        }
      }

      // load merge server
      if (OB_SUCCESS == ret)
      {
        ret = load_merge_server();
      }

      if (OB_SUCCESS == ret)
      {
        write_thread_count_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                  OBSC_WRITE_THREAD_COUNT, 
                                                  DEFAULT_WRITE_THREAD_COUNT);
        if (write_thread_count_ < 0 || write_thread_count_ > 1)
        {
          TBSYS_LOG(WARN, "olapdrive write thread count=%ld can't <= 0 or > 1." ,
              write_thread_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        read_thread_count_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                 OBSC_READ_THREAD_COUNT, 
                                                 DEFAULT_READ_THERAD_COUNT);
        if (read_thread_count_ < 0)
        {
          TBSYS_LOG(WARN, "olapdrive read thread count=%ld can't <= 0." ,
              read_thread_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (read_thread_count_ <= 0 && write_thread_count_ <= 0)
      {
        TBSYS_LOG(WARN, "both read_thread_count_ and write_thread_count_ is "
                        "invalid, read_thread_count_=%ld, write_thread_count_=%ld",
                  read_thread_count_, write_thread_count_);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        network_time_out_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                OBSC_NETWORK_TIMEOUT, 
                                                DEFAULT_TIMEOUT);
        if (network_time_out_ <= 0)
        {
          TBSYS_LOG(WARN, "olapdrive network timeout=%ld can't <= 0." ,
                    network_time_out_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        stat_dump_interval_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                OBSC_STAT_DUMP_INTERVAL, 0);
        if (stat_dump_interval_ < 0)
        {
          TBSYS_LOG(WARN, "olapdrive stat_dump_interval_=%ld can't < 0." ,
              stat_dump_interval_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        write_day_count_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                               OBSC_WRITE_DAY_COUNT, 
                                               DEFAULT_WRITE_DAY_COUNT);
        if (write_day_count_ < 0)
        {
          TBSYS_LOG(WARN, "olapdrive write_day_count_=%ld can't < 0." ,
              write_day_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        daily_unit_count_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                                OBSC_DAILY_UNIT_COUNT, 
                                                DEFAULT_DAILY_UNIT_COUNT);
        if (daily_unit_count_ < 0)
        {
          TBSYS_LOG(WARN, "olapdrive daily_unit_count_=%ld can't < 0." ,
              daily_unit_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        read_day_count_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                              OBSC_READ_DAY_COUNT, 
                                              DEFAULT_READ_DAY_COUNT);
        if (read_day_count_ < 0)
        {
          TBSYS_LOG(WARN, "olapdrive read_day_count_=%ld can't < 0." ,
              read_day_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        log_level_ = TBSYS_CONFIG.getInt(OBSC_SECTION, 
                                         OBSC_LOG_LEVEL, 
                                         DEFAULT_LOG_LEVEL);
        if (log_level_ < 0)
        {
          TBSYS_LOG(WARN, "olapdrive log_level_=%ld can't < 0." ,
              log_level_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      return ret;
    }

    void ObOlapdriveParam::dump_param()
    {
      char server_str[OB_MAX_IP_SIZE];

      root_server_.to_string(server_str, OB_MAX_IP_SIZE);
      fprintf(stderr, "root_server: \n"
                      "    vip: %s \n"
                      "    port: %d \n",
              server_str, root_server_.get_port());

      update_server_.to_string(server_str, OB_MAX_IP_SIZE);
      fprintf(stderr, "update_server: \n"
                      "    vip: %s \n"
                      "    port: %d \n",
              server_str, update_server_.get_port());

      for (int64_t i = 0; i < merge_server_count_; ++i)
      {
        merge_server_[i].to_string(server_str, OB_MAX_IP_SIZE);
        fprintf(stderr, "merge_server[%ld]: \n"
                        "    vip: %s \n"
                        "    port: %d \n",
                i, server_str, merge_server_[i].get_port());
      }

      fprintf(stderr, "    network_time_out: %ld \n"
                      "    write_thread_count: %ld \n"
                      "    read_thread_count: %ld \n"
                      "    stat_dump_interval: %ld \n"
                      "    write_day_count: %ld \n"
                      "    daily_unit_count: %ld \n"
                      "    read_day_count: %ld \n"
                      "    log_level: %ld \n",
              network_time_out_, write_thread_count_, read_thread_count_,
              stat_dump_interval_, write_day_count_, daily_unit_count_, 
              read_day_count_, log_level_);
    }
  } // end namespace olapdrive
} // end namespace oceanbase
