/*                                                                              
 * (C) 2007-2010 Taobao Inc.                                                    
 *                                                                              
 * This program is free software; you can redistribute it and/or modify         
 * it under the terms of the GNU General Public License version 2 as            
 * published by the Free Software Foundation.                                   
 *                                                                              
 *                                                                              
 *                                                                              
 * Version: 0.1: ob_update_server.h,v 0.1 2010/09/28 13:39:26 chuanhui Exp $    
 *                                                                              
 * Authors:                                                                     
 *   rongxuan <rongxuan.lc@taobao.com>                                          
 *     - some work details if you want                                          
 *                                                                              
 */                                                                             
#ifndef OCEANBASE_CLUSTERMANAGER_OB_OCM_PARAM_H                     
#define OCEANBASE_CLUSTERMANAGER_OB_OCM_PARAM_H

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_string.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace clustermanager
  {
    const int64_t OB_MAX_DEV_NAME_SIZE = 64;
    const int64_t OB_MAX_LOCATION_LENGTH = 16;

    struct ObClusterInfo
    {
      char ip_addr[OB_IP_STR_BUFF];
      int64_t port;
      bool is_exist;

      ObClusterInfo()
      {
        ip_addr[0] = '\0';
        port = 0;
        is_exist = true;
      }
    };

    class ObOcmParam
    {
      public:
        ObOcmParam();
        ~ObOcmParam();

      public:
      int load_string(char* dest, const int32_t size,
          const char* section, const char* name, bool not_null);
      int load_from_config();
      int set_ocm_status(const ObString ocm_ip_addr, const bool is_exist);
      //whether to broadcast to the index cluster
      bool get_ocm_status(int64_t index)const;
      int set_ocm_status(const int64_t index, const bool is_exist);
      public:
      /*inline const  char* get_ocm_vip() const
      {
        return ocm_vip_;
      }
      inline char* get_ocm_vip()
      {
        return ocm_vip_;
      }*/
      inline int get_ip()
      {
        return tbsys::CNetUtil::getLocalAddr(dev_name_);
      }
      inline int64_t get_port() const 
      {
        return port_;
      }
      inline const char* get_ocm_location() const
      {
        return location_;
      }
      inline const char* get_hostname() const
      {
        return hostname_;
      }

      inline int64_t get_cluster_manager_count() const
      {
        return cluster_manager_count_;
      }

      inline const char* get_device_name() const
      {
        return dev_name_;
      }

      inline const ObClusterInfo* get_ocm_addr_array(int64_t index) const
      {
        return ocm_addr_array_ + index;
      }

      inline int64_t get_retry_times() const
      {
        return retry_times_;
      }

      inline int64_t get_broadcast_timeout_us() const
      {
        return broadcast_timeout_us_; 
      }

      private:
        static const int64_t DEFAULT_RETRY_TIMES = 3;
        static const int64_t DEFAULT_TIMEOUT_US = 1000000L;

      private:
        int64_t retry_times_;
       char dev_name_[OB_MAX_DEV_NAME_SIZE];
       char log_dir_path_[common::OB_MAX_FILE_NAME_LENGTH];
       int64_t log_size_mb_;
       char hostname_[OB_MAX_HOST_NAME_LENGTH];
       int64_t port_;
       char location_[OB_MAX_LOCATION_LENGTH];
       int64_t broadcast_timeout_us_;
       int64_t lease_on_;
       int64_t lease_reserved_time_rs_;
       int64_t lease_interval_rs_;
       int64_t replay_wait_time_rs_;
       int64_t state_check_period_rs_;
       int64_t log_sync_limit_kb_;
       int64_t register_times_;
       int64_t register_timeout_rs_;
       int64_t cluster_manager_count_;
       ObClusterInfo *ocm_addr_array_;
    };
  }
}
#endif

