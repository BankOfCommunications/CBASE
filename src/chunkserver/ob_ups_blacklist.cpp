/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_ups_blacklist.cpp for update server black list. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "tblog.h"
#include "tbsys.h"
#include "ob_ups_blacklist.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObUpsBlackList::ObUpsBlackList()
    {
      max_fail_count_ = DEFAULT_COUNT;
      black_timeout_ = DEFAULT_TIMEOUT;
      server_count_ = 0;
      memset(fail_counter_, 0, sizeof(fail_counter_));
    }
    
    ObUpsBlackList::~ObUpsBlackList()
    {
    }
    
    int ObUpsBlackList::init(const int32_t max_fail_count, const int64_t timeout,
        const ObServerType type, const ObUpsList & list)
    {
      int ret = OB_SUCCESS;
      if ((max_fail_count <= 0) || (timeout <= 0) || (0 == list.ups_count_)
          || (list.ups_count_ > MAX_LIST_COUNT))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "check max fail count or server list failed:fail[%d], timeout[%ld], count[%d]",
            max_fail_count, timeout, list.ups_count_);
      }
      else
      {
        black_timeout_ = timeout;
        max_fail_count_ = max_fail_count;
        ServerStatus server_info;
        for (int32_t i = 0; i < list.ups_count_; ++i)
        {
          server_info.fail_count_ = 0;
          server_info.fail_timestamp_ = 0;
          server_info.server_ = list.ups_array_[i].get_server(type);
          fail_counter_[i] = server_info;
        }
        server_count_ = list.ups_count_;
      }
      return ret;
    }
    
    bool ObUpsBlackList::check(const int32_t server_index)
    {
      // disallow fetch the real in_blacklist server
      bool ret = true;
      if ((server_index < server_count_) && (fail_counter_[server_index].fail_count_ >= max_fail_count_))
      {
        // blacklist timeout reset it to alive
        if (tbsys::CTimeUtil::getTime() - fail_counter_[server_index].fail_timestamp_ >= black_timeout_)
        {
          TBSYS_LOG(DEBUG, "pull the server to alive again:index[%d], count[%d], time[%ld], fail_count[%d]",
              server_index, server_count_, fail_counter_[server_index].fail_timestamp_,
              fail_counter_[server_index].fail_count_);
          fail_counter_[server_index].fail_count_ = 0;
          fail_counter_[server_index].fail_timestamp_ = 0;
        }
        else
        {
          TBSYS_LOG(WARN, "check server is in blacklist:server[%s]", to_cstring(fail_counter_[server_index].server_));
          ret = false;
        }
      }
      return ret;
    }
    
    void ObUpsBlackList::fail(const int32_t server_index, const common::ObServer & server)
    {
      if ((server_index < server_count_) && (fail_counter_[server_index].server_ == server))
      {
        // no need thread safe
        if (0 == fail_counter_[server_index].fail_count_)
        {
          fail_counter_[server_index].fail_timestamp_ = tbsys::CTimeUtil::getTime();
        }
        ++fail_counter_[server_index].fail_count_;
      }
      else
      {
        TBSYS_LOG(WARN, "check param failed:index[%d], count[%d], server[%u], port[%d]",
            server_index, server_count_, server.get_ipv4(), server.get_port());
      }
    }
    
    void ObUpsBlackList::reset(void)
    {
      for (int32_t i = 0; i < server_count_; ++i)
      {
        fail_counter_[i].fail_count_ = 0;
        fail_counter_[i].fail_timestamp_ = 0;
      }
    }
    
    int32_t ObUpsBlackList::get_valid_count(void) const
    {
      int32_t count = 0;
      int64_t cur_timestamp = tbsys::CTimeUtil::getTime();
      for (int32_t i = 0; i < server_count_; ++i)
      {
        if ((fail_counter_[i].fail_count_ < max_fail_count_)
          || (cur_timestamp - fail_counter_[i].fail_timestamp_ >= black_timeout_))
        {
          ++count;
        }
      }
      return count;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
