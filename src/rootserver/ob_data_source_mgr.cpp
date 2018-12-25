/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_data_source_mgr.cpp
 *
 * Authors:
 *  yongle.xh@alipay.com
 *
 */

#include "ob_data_source_mgr.h"
#include "ob_root_rpc_stub.h"
#include "sql/ob_sql_result_set.h"
#include "common/ob_mod_define.h"
#include "common/ob_string.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

int64_t ObDataSourceProxy::to_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;
  databuff_printf(buffer, length, pos, "server=");
  pos += server_.to_string(buffer+pos, length - pos);
  databuff_printf(buffer, length, pos, ",support_types=");
  for (ObList<ObString>::const_iterator it = support_types_.begin();
      it != support_types_.end(); ++it)
  {
    databuff_printf(buffer, length, pos, "%.*s|",
      it->length(), it->ptr());
  }
  databuff_printf(buffer, length, pos, "\",failed_count_=%ld,total_count_=%ld",
      failed_count_, total_count_);

  return pos;
}

bool ObDataSourceProxy::is_support(const ObString& data_source_name)
{
  bool ret = false;
  for (ObList<ObString>::const_iterator it = support_types_.begin();
      it != support_types_.end(); ++it)
  {
    if (*it == data_source_name)
    {
      ret = true;
      break;
    }
  }

  return ret;
}

ObDataSourceProxyList::ObDataSourceProxyList():
  proxys_(), mod_(ObModIds::OB_DATA_SOURCE_MANAGER), allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_)
{
}

ObDataSourceProxyList::~ObDataSourceProxyList()
{
  for (ObVector<ObDataSourceProxy*>::const_iterator it = proxys_.begin(); it != proxys_.end(); ++it)
  {
    ObDataSourceProxy* cur = *it;
    if (NULL == cur)
    {
      TBSYS_LOG(ERROR,"cur must not null");
    }
    else
    {
      cur->~ObDataSourceProxy();
    }
  }
}

int ObDataSourceProxyList::add_proxy(ObServer& server, ObString& support_types)
{
  int ret = OB_SUCCESS;
  void *buf = allocator_.alloc(sizeof(ObDataSourceProxy));
  if (NULL == buf)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(ERROR, "failed to alloc new proxy info");
  }
  else
  {
    ObDataSourceProxy* proxy = new(buf)ObDataSourceProxy();
    proxy->server_ = server;
    char table_name_buff[MAX_DATA_SOURCE_SUPPORT_NAME_STRING_LENGTH];
    int n = snprintf(table_name_buff, MAX_DATA_SOURCE_SUPPORT_NAME_STRING_LENGTH, "%.*s", support_types.length(), support_types.ptr());
    if (n<0 || n >=MAX_DATA_SOURCE_SUPPORT_NAME_STRING_LENGTH)
    {
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "failed to copy support_types[%.*s], its length[%u] must less than buf[%ld]",
          support_types.length(), support_types.ptr(), support_types.length(), MAX_DATA_SOURCE_SUPPORT_NAME_STRING_LENGTH);
    }
    else
    {
      char *remain = table_name_buff;
      char *token = NULL;
      while (NULL != (token = strsep(&remain, "|")))
      {
        ObString type;
        type.assign_ptr(token, static_cast<ObString::obstr_size_t>(strlen(token)));
        if (OB_SUCCESS != (ret = ob_write_string(allocator_, type, type)))
        {
          TBSYS_LOG(WARN, "failed to copy string %s, ret=%d", token, ret);
          break;
        }
        else if (OB_SUCCESS != (ret = proxy->support_types_.push_back(type)))
        {
          TBSYS_LOG(WARN, "failed to add type to proxy %s, ret=%d", to_cstring(server), ret);
          break;
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = proxys_.push_back(proxy);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to add proxy %s", to_cstring(*proxy));
      }
    }
  }

  return ret;
}

ObDataSourceMgr::ObDataSourceMgr():
  last_update_time_(0), proxy_list_(NULL), server_manager_(NULL), rpc_stub_(NULL), config_(NULL),
  data_source_lock_(),
  mod_(ObModIds::OB_DATA_SOURCE_MANAGER), allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_)
{
  srandom(static_cast<unsigned int>(time(NULL)));
  last_update_time_ = 0;
}

ObDataSourceMgr::~ObDataSourceMgr()
{
  if (NULL != proxy_list_)
  {
    proxy_list_->~ObDataSourceProxyList();
  }
}

void ObDataSourceMgr::print_data_source_info() const
{
  tbsys::CRLockGuard guard(data_source_lock_);
  int64_t i = 0;
  if (NULL != proxy_list_)
  {
    TBSYS_LOG(INFO, "print proxy list");
    ObVector<ObDataSourceProxy*>& proxy_list = proxy_list_->proxys_;
    for(ObVector<ObDataSourceProxy*>::const_iterator it = proxy_list.begin();
        it != proxy_list.end(); ++it)
    {
      ObDataSourceProxy* proxy = *it;
      TBSYS_LOG(INFO, "proxy %ld: %s", ++i, to_cstring(*proxy));
    }
  }
  else
  {
    TBSYS_LOG(INFO, "empty proxy list");
  }
}

int ObDataSourceMgr::update_data_source_proxy()
{
  int ret = OB_SUCCESS;
  const ObString table_name= ObString::make_string( "data_source_proxy");//TODO: make it use config
  ObDataBuffer msgbuf;
  sql::ObSQLResultSet rs;
  ObDataSourceProxyList* proxy_list = NULL;

  if (NULL == server_manager_ || NULL == rpc_stub_ || NULL == config_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "server_manager_=%p, rpc_stub_=%p and config_=%p must not null",
        server_manager_, rpc_stub_, config_);
  }
  else if (tbsys::CTimeUtil::getTime() > UPDATE_DATA_SOURCE_PERIOD  + last_update_time_)
  {
    ObServer ms_server;
    ObChunkServerManager::const_iterator it = server_manager_->get_serving_ms();
    if (server_manager_-> end() == it)
    {
      TBSYS_LOG(ERROR, "no serving ms found, failed to do update_data_source_proxy");
      ret = OB_MS_NOT_EXIST;
    }
    else
    {
      ms_server = it->server_;
      ms_server.set_port(it->port_ms_);
    }

    if (OB_SUCCESS == ret)
    {
      void* buf = ob_malloc(sizeof(ObDataSourceProxyList), ObModIds::OB_DATA_SOURCE_MANAGER);
      if (NULL == buf)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "failed to malloc ObDataSourceProxyList");
      }
      else
      {
        proxy_list = new(buf) ObDataSourceProxyList();
      }
    }

    if (OB_SUCCESS == ret)
    {
      const int64_t cluster_id = config_->cluster_id;
      const int64_t timeout = config_->inner_table_network_timeout;
      if (OB_SUCCESS != (ret = rpc_stub_->fetch_proxy_list(
              ms_server, table_name, cluster_id, *proxy_list, timeout)))
      {
        TBSYS_LOG(WARN, "failed to fetch_proxy_list, table_name=%.*s cluster_id=%ld, ret=%d",
            table_name.length(), table_name.ptr(), cluster_id, ret);
      }
      else if (proxy_list->proxys_.size() == 0)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "proxy_list size must larger than 0");
      }
    }

    if (OB_SUCCESS == ret)
    {
      tbsys::CWLockGuard guard(data_source_lock_);
      ObDataSourceProxyList* tmp = proxy_list_;
      proxy_list_ = proxy_list;
      proxy_list = tmp;
      if (NULL != proxy_list)
      {
        ObVector<ObDataSourceProxy*>& proxy_list_new = proxy_list_->proxys_;
        ObVector<ObDataSourceProxy*>& proxy_list_old = proxy_list->proxys_;
        for(ObVector<ObDataSourceProxy*>::const_iterator it = proxy_list_new.begin();
            it != proxy_list_new.end(); ++it)
        {
          ObDataSourceProxy* proxy = *it;
          for(ObVector<ObDataSourceProxy*>::const_iterator it2 = proxy_list_old.begin();
              it2 != proxy_list_old.end(); ++it2)
          {
            ObDataSourceProxy* proxy_old = *it2;
            if (NULL != proxy && NULL != proxy_old
                && proxy->server_ == proxy_old->server_)
            {
              proxy->failed_count_ = proxy_old->failed_count_ - 1;
              if (proxy->failed_count_ < 0)
              {
                proxy->failed_count_ = 0;
              }
              proxy->total_count_ = proxy_old->total_count_;
            }
          }
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      last_update_time_ = tbsys::CTimeUtil::getTime();
    }

    if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
    {
      print_data_source_info();
    }

    if (NULL != proxy_list)
    {
      proxy_list->~ObDataSourceProxyList();
      ob_free(proxy_list);
      proxy_list = NULL;
    }
  }

  return ret;
}

void ObDataSourceMgr::inc_failed_count(const ObServer& server)
{
  tbsys::CWLockGuard guard(data_source_lock_);
  if (NULL == proxy_list_)
  {
    TBSYS_LOG(ERROR, "can't get proxy_list_ yet");
  }
  else if (NULL == config_)
  {
    TBSYS_LOG(ERROR, "config must not null");
  }
  else
  {
    bool found = false;
    ObVector<ObDataSourceProxy*>& proxy_list = proxy_list_->proxys_;
    for (ObVector<ObDataSourceProxy*>::const_iterator it = proxy_list.begin();
        it != proxy_list.end() && !found; ++it)
    {
      ObDataSourceProxy* cur = *it;
      if (NULL == cur)
      {
        TBSYS_LOG(ERROR,"cur must not null");
        break;
      }
      else if (cur->server_ == server)
      {
        if (cur->failed_count_ < config_->data_source_max_fail_count)
        {
          ++cur->failed_count_;
          found  = true;
        }
        else
        {
          int64_t data_source_max_fail_count = config_->data_source_max_fail_count;
          TBSYS_LOG(WARN, "data source server %s is failed more than data_source_max_fail_count %ld",
              to_cstring(server), data_source_max_fail_count);
        }
      }
    }
    if (!found)
    {
      TBSYS_LOG(WARN, "data source server %s is not in proxy list", to_cstring(server));
    }
  }
}

void ObDataSourceMgr::dec_failed_count(const ObServer& server)
{
  tbsys::CWLockGuard guard(data_source_lock_);
  if (NULL == proxy_list_)
  {
    TBSYS_LOG(ERROR, "can't get proxy_list_ yet");
  }
  else if (NULL == config_)
  {
    TBSYS_LOG(ERROR, "config must not null");
  }
  else
  {
    bool found = false;
    ObVector<ObDataSourceProxy*>& proxy_list = proxy_list_->proxys_;
    for (ObVector<ObDataSourceProxy*>::const_iterator it = proxy_list.begin();
        it != proxy_list.end() && !found; ++it)
    {
      ObDataSourceProxy* cur = *it;
      if (NULL == cur)
      {
        TBSYS_LOG(ERROR,"cur must not null");
        break;
      }
      else if (cur->server_ == server)
      {
        if (cur->failed_count_ > 0)
        {
          --cur->failed_count_;
        }
        found  = true;
      }
    }
    if (!found)
    {
      TBSYS_LOG(WARN, "data source server %s is not in proxy list", to_cstring(server));
    }
  }
}

int ObDataSourceMgr::select_data_source(const ObDataSourceDesc::ObDataSourceType& type, ObString& uri,
    ObServer& dest_server, ObServer& data_source_server)
{
  int ret = OB_SUCCESS;
  ObDataSourceProxy* proxy = NULL;

  if (ObDataSourceDesc::DATA_SOURCE_PROXY != type)
  {
    //TODO: not support yet
    ret = OB_NOT_SUPPORTED;
    TBSYS_LOG(ERROR, "unsupported data source type, %d", type);
  }
  else if (NULL == config_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "config must not null");
  }
  else
  {
    tbsys::CRLockGuard guard(data_source_lock_);
    if (NULL == proxy_list_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "can't get proxy_list_ yet");
    }
    else
    {
      ObVector<ObDataSourceProxy*>& proxy_list = proxy_list_->proxys_;
      ObString data_source_name;
      const char* ptr = uri.find(':');
      if (NULL == ptr)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "unknown uri %.*s", uri.length(), uri.ptr());
      }
      else
      {
        data_source_name.assign_ptr(uri.ptr(), static_cast<ObString::obstr_size_t>(ptr - uri.ptr()));
        bool found = false;
        for (ObVector<ObDataSourceProxy*>::const_iterator it = proxy_list.begin();
            it != proxy_list.end() && !found; ++it)
        {
          ObDataSourceProxy* cur = *it;
          if (NULL == cur)
          {
            ret = OB_ERR_SYS;
            TBSYS_LOG(ERROR,"cur must not null");
            break;
          }
          else if (cur->server_.is_same_ip(dest_server) && cur->failed_count_ < config_->data_source_max_fail_count 
              && cur->is_support(data_source_name))
          {
            proxy = cur;
            proxy->total_count_++;
            data_source_server = proxy->server_;
            found = true;
            break;
          }
        }
      }
    }
  }

  if (NULL == proxy)
  {
    ret = select_data_source(type, uri, data_source_server);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to select data source, ret=%d", ret);
    }
  }

  return ret;
}

int ObDataSourceMgr::select_data_source(const ObDataSourceDesc::ObDataSourceType& type, ObString& uri,
    ObServer& data_source_server)
{
  int ret = OB_SUCCESS;
  ObDataSourceProxy* proxy = NULL;

  if (ObDataSourceDesc::DATA_SOURCE_PROXY != type)
  {
    //TODO: not support yet
    ret = OB_NOT_SUPPORTED;
    TBSYS_LOG(ERROR, "unsupported data source type, %d", type);
  }
  else if (ObDataSourceDesc::DATA_SOURCE_PROXY == type)
  {
    tbsys::CRLockGuard guard(data_source_lock_);
    if (NULL == proxy_list_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "can't get proxy_list_ yet");
    }
    else if (NULL == config_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "config must not null");
    }
    else
    {
      ObString data_source_name;
      ObVector<ObDataSourceProxy*>& proxy_list = proxy_list_->proxys_;
      int64_t proxy_list_size = proxy_list.size();
      const char* ptr = uri.find(':');
      if (NULL == ptr)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "unknown uri %.*s", uri.length(), uri.ptr());
      }
      else if (proxy_list_size == 0)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "no proxy in proxy list yet");
      }
      else
      {
        data_source_name.assign_ptr(uri.ptr(), static_cast<ObString::obstr_size_t>(ptr - uri.ptr()));
        int64_t seq = random() % proxy_list_size;
        bool found = false;
        bool is_supported = false;
        ObVector<ObDataSourceProxy*>::const_iterator it = proxy_list.begin();
        ObDataSourceProxy* cur = NULL;
        while (seq >= 0 && OB_SUCCESS == ret)
        {
          if (it == proxy_list.end())
          {
            if (found == false)
            {
              if (is_supported)
              {
                ret = OB_EAGAIN;
                TBSYS_LOG(WARN, "no availble proxy support type %.*s", data_source_name.length(), data_source_name.ptr());
              }
              else
              {
                ret = OB_INVALID_ARGUMENT;
                TBSYS_LOG(ERROR, "no proxy support type %.*s", data_source_name.length(), data_source_name.ptr());
              }
              print_data_source_info();
              break;
            }
            else
            {
              it = proxy_list.begin();
            }
          }

          cur = *it;
          if (NULL == cur)
          {
            ret = OB_ERR_SYS;
            TBSYS_LOG(ERROR,"cur must not null");
            break;
          }
          else if (cur->is_support(data_source_name))
          {
            is_supported = true;
            if (cur->failed_count_ < config_->data_source_max_fail_count)
            {
              --seq;
              proxy = cur;
              found = true;
              data_source_server = proxy->server_;
            }
          }
          ++it;
        }
      }
    }
  }

  return ret;
}

// get next data source not same as data_source_server
int ObDataSourceMgr::get_next_data_source(const ObDataSourceDesc::ObDataSourceType& type, ObString& uri,
    ObServer& data_source_server)
{
  int ret = OB_SUCCESS;
  ObServer server;

  if (ObDataSourceDesc::OCEANBASE_OUT == type)
  {
    //TODO: not support yet
    ret = OB_NOT_SUPPORTED;
  }
  else if (NULL == config_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "config must not null");
  }
  else if (ObDataSourceDesc::DATA_SOURCE_PROXY == type)
  {
    bool found = false;
    if (server == data_source_server)
    {// first time call this method
      ret = select_data_source(type, uri, data_source_server);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to random select data source, ret=%d", ret);
      }
      else
      {
        found = true;
      }
    }

    if (!found)
    {
      const char* ptr = uri.find(':');
      if (NULL == ptr)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "unknown uri %.*s", uri.length(), uri.ptr());
      }
      else
      {
        ObString data_source_name;
        data_source_name.assign_ptr(uri.ptr(), static_cast<ObString::obstr_size_t>(ptr - uri.ptr()));

        tbsys::CRLockGuard guard(data_source_lock_);
        if (NULL == proxy_list_)
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(ERROR, "can't get proxy_list_ yet");
        }
        else
        {
          ObVector<ObDataSourceProxy*>& proxy_list = proxy_list_->proxys_;
          int64_t proxy_list_size = proxy_list.size();
          if (proxy_list_size == 0)
          {
            ret = OB_NOT_INIT;
            TBSYS_LOG(ERROR, "no proxy in proxy list yet");
          }
          else
          {
            ObDataSourceProxy* cur = NULL;
            found = false;
            // find it of input data_source_server
            ObVector<ObDataSourceProxy*>::const_iterator cur_it = proxy_list.begin();
            get_proxy_it(data_source_server, cur_it);//ret is not care

            // get next
            ObVector<ObDataSourceProxy*>::const_iterator it = cur_it;
            if (it != proxy_list.end())
            {
              ++it;
            }
            else
            {
              it = proxy_list.begin();
            }

            for(;OB_SUCCESS == ret && it != cur_it; ++it)
            {
              if (it == proxy_list.end())
              {
                it = proxy_list.begin();
                if (it == cur_it)
                {
                  break;
                }
              }

              cur = *it;
              if (NULL == cur)
              {
                ret = OB_ERR_SYS;
                TBSYS_LOG(ERROR,"cur must not null");
                break;
              }
              if (data_source_server != cur->server_ && cur->failed_count_ < config_->data_source_max_fail_count 
                  && cur->is_support(data_source_name))
              {
                data_source_server = cur->server_;
                found = true;
                break;
              }
            }// end while
          }
        }
      }
    }

    if (!found)
    {
      get_next_proxy(uri, data_source_server);//ret is not care
    }
  }
  else
  {
    ret = OB_ERR_SYS;
    TBSYS_LOG(ERROR, "unkown ObDataSourceType %d", type);
  }

  return ret;
}

int ObDataSourceMgr::get_proxy_it(const ObServer& server, ObVector<ObDataSourceProxy*>::const_iterator& it)
{
  // need lock data_source_lock_ outside
  int ret = OB_SUCCESS;
  bool found = false;
  if (NULL == proxy_list_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "can't get proxy_list_ yet");
  }
  else
  {
    ObVector<ObDataSourceProxy*>& proxy_list = proxy_list_->proxys_;
    int64_t proxy_list_size = proxy_list.size();
    if (proxy_list_size == 0)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "no proxy in proxy list yet");
    }
    else
    {
      ObDataSourceProxy* cur = NULL;
      it = proxy_list.begin();
      for(;OB_SUCCESS == ret && it != proxy_list.end(); ++it)
      {
        cur = *it;
        if (NULL == cur)
        {
          ret = OB_ERR_SYS;
          TBSYS_LOG(ERROR,"cur must not null");
          break;
        }
        else if (cur->server_ == server)
        {
          found = true;
          break;
        }
      }
    }
  }

  if (!found)
  {
    ret = OB_ENTRY_EXIST;
    TBSYS_LOG(WARN, "server %s is not in proxy list", to_cstring(server));
  }
  return ret;
}

int ObDataSourceMgr::get_next_proxy(ObString& uri, ObServer& data_source_server)
{
  int ret = OB_SUCCESS;
  const char* ptr = uri.find(':');
  if (NULL == ptr)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "unknown uri %.*s", uri.length(), uri.ptr());
  }
  else
  {
    ObString data_source_name;
    data_source_name.assign_ptr(uri.ptr(), static_cast<ObString::obstr_size_t>(ptr - uri.ptr()));

    tbsys::CRLockGuard guard(data_source_lock_);
    if (NULL == proxy_list_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "can't get proxy_list_ yet");
    }
    else
    {
      ObVector<ObDataSourceProxy*>& proxy_list = proxy_list_->proxys_;
      int64_t proxy_list_size = proxy_list.size();
      if (proxy_list_size == 0)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "no proxy in proxy list yet");
      }
      else
      {
        ObDataSourceProxy* cur = NULL;
        bool found = false;
        // find it of input data_source_server
        ObVector<ObDataSourceProxy*>::const_iterator cur_it = proxy_list.begin();
        get_proxy_it(data_source_server, cur_it);//ret is not care
        ObVector<ObDataSourceProxy*>::const_iterator it = cur_it;
        if (it != proxy_list.end())
        {
          ++it;
        }
        else
        {
          it = proxy_list.begin();
        }

        for(;OB_SUCCESS == ret && it != cur_it; ++it)
        {
          if (it == proxy_list.end())
          {
            it = proxy_list.begin();
            if (it == cur_it)
            {
              break;
            }
          }
          cur = *it;
          if (NULL == cur)
          {
            ret = OB_ERR_SYS;
            TBSYS_LOG(ERROR,"cur must not null");
            break;
          }
          else if (cur->server_ != data_source_server && cur->is_support(data_source_name))
          {
            found = true;
            data_source_server = cur->server_;
            break;
          }
        }

        if (!found)
        {
          TBSYS_LOG(WARN, "no other proxy server found, use old previous %s", to_cstring(data_source_server));
        }
      }
    }
  }
  return ret;
}
