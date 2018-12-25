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

#include "ob_ocm_param.h"
#include <stdlib.h>
#include <stdio.h>
namespace 
{
  const char* OBCM_SECTION = "cluster_manager";
  //const char* OBCM_VIP = "vip";
  const char* OBCM_PORT = "port";
  const char* OBCM_DEVNAME = "dev_name";
  const char* OBCM_LOG_DIR_PTAH = "log_dir_path";
  const char* OBCM_LOG_SIZE_MB = "log_size_mb";
  const char* OBCM_STATE_CHECK_PERIOD_RS = "state_check_period_rs";
  const char* OBCM_REPLAY_WAIT_TIME_RS = "replay_wait_time_rs";
  const char* OBCM_LOG_SYNC_LIMIT_KB = "log_sync_limit_kb";
  const char* OBCM_REGISTER_TIMES = "register_times";
  const char* OBCM_REGISTER_TIMEOUT_RS = "register_timeout_rs";
  const char* OBCM_LEASE_ON = "lease_on";
  const char* OBCM_LEASE_INTERVAL_RS = "lease_interval_rs";
  const char* OBCM_EASE_RESERVED_TIME_RS = "lease_reserved_time_rs";
  const char* OBCM_CLUSTER_MANAGER_COUNT = "cluster_manager_count";
  const char* OBCM_LOCATION = "location";
  const char* OBCM_HOST_NAME = "hostname";
}

namespace oceanbase
{
  namespace clustermanager
  {
    using namespace oceanbase::common;

    ObOcmParam::ObOcmParam()
    {
      memset(this, 0x00, sizeof(*this));
      retry_times_ = DEFAULT_RETRY_TIMES;
      broadcast_timeout_us_ = DEFAULT_TIMEOUT_US;
    }
    
    ObOcmParam::~ObOcmParam()
    {
    if(NULL != ocm_addr_array_)
    {
      delete [] ocm_addr_array_;
    ocm_addr_array_ = NULL;
    }
    }
  
  int ObOcmParam::load_string(char* dest, const int32_t size,
        const char* section, const char* name, bool not_null)
  {
    int ret = OB_SUCCESS;
      if (NULL == dest || 0 >= size || NULL == section || NULL == name)
      {
        ret = OB_ERROR;
      }     
      
      dest[0] = '\0';
      const char* value = NULL;
      if (OB_SUCCESS == ret)
      { 
        value = TBSYS_CONFIG.getString(section, name);
        if (not_null && (NULL == value || 0 >= strlen(value)))
        {
          TBSYS_LOG(ERROR, "%s.%s has not been set.", section, name);
          ret = OB_ERROR;
        }
    }

      if (OB_SUCCESS == ret && NULL != value)
      {
        if ((int32_t)strlen(value) >= size)
        {
          TBSYS_LOG(ERROR, "%s.%s too long, length (%ld) > %d",
              section, name, strlen(value), size);
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          strncpy(dest, value, strlen(value));
        }
      }

      return ret;
  }

    int ObOcmParam::load_from_config()
    {
      int err = OB_SUCCESS;

      port_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_PORT, 0);
      if(port_ <= 0)
      {
        TBSYS_LOG(ERROR, "ocm server port (%ld) cannot <= 0.", port_);
        err = OB_ERROR;
      }
      
      if(OB_SUCCESS == err)
      {
        err = load_string(dev_name_, OB_MAX_DEV_NAME_SIZE, OBCM_SECTION, OBCM_DEVNAME, true);
      }
    
      if(OB_SUCCESS == err)
      {
        err = load_string(log_dir_path_, OB_MAX_FILE_NAME_LENGTH, OBCM_SECTION, OBCM_LOG_DIR_PTAH, true);
      }

      if(OB_SUCCESS == err)
      {
        err = load_string(location_, OB_MAX_LOCATION_LENGTH, OBCM_SECTION, OBCM_LOCATION, true);
      }
    
    if(OB_SUCCESS == err)
    {
      err = load_string(hostname_, OB_MAX_HOST_NAME_LENGTH, OBCM_SECTION, OBCM_HOST_NAME, true);
    }

      if(OB_SUCCESS == err)
      {
        log_size_mb_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_LOG_SIZE_MB,0);
      }

    /* if(OB_SUCCESS == err)
     {
       err = load_string(ocm_vip_, OB_MAX_IP_SIZE, OBCM_SECTION, OBCM_VIP, true);
     }*/

     if(OB_SUCCESS == err)
     {
       lease_on_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_LEASE_ON, 0); 
     }

     if(OB_SUCCESS == err)
     {
       lease_reserved_time_rs_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_EASE_RESERVED_TIME_RS, 0);
     }

     if(OB_SUCCESS == err)
     {
       lease_interval_rs_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_LEASE_INTERVAL_RS, 0);
     }

     if(OB_SUCCESS == err)
     {
       replay_wait_time_rs_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_REPLAY_WAIT_TIME_RS, 0);
     }

     if(OB_SUCCESS == err)
     {
       state_check_period_rs_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_STATE_CHECK_PERIOD_RS, 0);
     }

     if(OB_SUCCESS == err)
     {
       log_sync_limit_kb_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_LOG_SYNC_LIMIT_KB, 0);
     }

     if(OB_SUCCESS == err)
     {
       register_times_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_REGISTER_TIMES, 0);
     }

     if(OB_SUCCESS == err)
     {
       register_timeout_rs_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_REGISTER_TIMEOUT_RS, 0);
     }

     if(OB_SUCCESS == err)
     {
       cluster_manager_count_ = TBSYS_CONFIG.getInt(OBCM_SECTION, OBCM_CLUSTER_MANAGER_COUNT, 0);
     }

     if(OB_SUCCESS == err)
     {
       ocm_addr_array_ = new(std::nothrow)ObClusterInfo[cluster_manager_count_];
       if(NULL == ocm_addr_array_)
       {
         TBSYS_LOG(WARN, "new ObclusterInfo array fail.");
         err = OB_ALLOCATE_MEMORY_FAILED;
       }
       else
       {
         char cluster_location_str[30] = "cluster_location_";
         char cluster_port_str[30] ="cluster_port_";
         int64_t i = 0; 
         int64_t location_len = strlen(cluster_location_str);
         int64_t port_len = strlen(cluster_port_str);
         char c = '1';
         for(; (i < cluster_manager_count_) && (OB_SUCCESS == err); i++)
         {
          cluster_location_str[location_len] = c;
          cluster_port_str[port_len] = c;
          cluster_location_str[location_len + 1] = '\0';
          cluster_port_str[port_len + 1] = '\0';
           err = load_string (ocm_addr_array_[i].ip_addr, OB_IP_STR_BUFF, OBCM_SECTION, cluster_location_str, true);      
           if(OB_SUCCESS == err)
           {  
             ocm_addr_array_[i].port = TBSYS_CONFIG.getInt(OBCM_SECTION, cluster_port_str, 0);
           }
           c = static_cast<char>(c + 1);
         }
       }
     }
     return err;
    }

  bool ObOcmParam::get_ocm_status(int64_t index) const 
  {
    return ocm_addr_array_[index].is_exist;
  }

  int ObOcmParam::set_ocm_status(const int64_t index, const bool is_exist)
  {
    int err = OB_SUCCESS;
    if(index >= cluster_manager_count_ || index <= 0)
    {
      TBSYS_LOG(WARN, "invalid argument.index=%ld, cluster_manager_count_=%ld", index, cluster_manager_count_);
      err = OB_INVALID_ARGUMENT;
    }
    else
    {
      ocm_addr_array_[index].is_exist = is_exist;
    }
    return err;
  }

  int ObOcmParam::set_ocm_status(const ObString ocm_ip_addr, const bool is_exist)
  {
    int err = OB_SUCCESS;
    if(NULL == ocm_addr_array_)
    {
      TBSYS_LOG(WARN, "ocm_addr_array not init.ocm_addr_array_=%p", ocm_addr_array_);
      err = OB_NOT_INIT;
    }
    else
    {
      int64_t i = 0;
      for(; i < cluster_manager_count_; i++)
      {
        if(0 == strcmp(ocm_ip_addr.ptr(), ocm_addr_array_[i].ip_addr))
        {
          ocm_addr_array_[i].is_exist = is_exist;
          break;
        }
      }

      if(cluster_manager_count_ == i)
      {
        TBSYS_LOG(WARN, "find ocm fail.ocm_ip_addr=%s", ocm_ip_addr.ptr());
        err = OB_ENTRY_NOT_EXIST;
      }
    }
    return err;
  }
  }
}
